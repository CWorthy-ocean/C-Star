import asyncio
import logging
import os
import socket
import textwrap
import typing as t
import uuid
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.applications.core import RunnerRequest
from cstar.applications.hello_world import (
    HelloWorldBlueprint,
    HelloWorldRunner,
)
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.cli.blueprint.run import app as app_run_blueprint
from cstar.cli.workplan.run import app as app_run_workplan
from cstar.entrypoint.config import get_service_config
from cstar.entrypoint.runner import execution_facts
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.adapter import StepToPlaceholderAdapter
from cstar.orchestration.manifest import (
    STEP_RECORD_NAME,
    ExecutionFacts,
    read_step_record,
)
from cstar.orchestration.models import BlueprintIdentity, Workplan, WorkplanState
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import TrackingRepository
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.manager import AnvilEnvSettings
from cstar.system.scheduler import SlurmQOS, SlurmScheduler


@pytest.fixture
def hw_single_step_wp_path(
    tmp_path: Path,
    hello_world_bp_content: str,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
) -> Path:
    """Return the path to a workplan containing a single step that runs the hello_world application.

    Returns
    -------
    Path
    """
    bp_content = hello_world_bp_content
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    expected_name = "hw-workplan"
    expected_description = "This is a test workplan using the hello_world application"

    step_name = "Hello Devloper!"
    app_name = "hello_world"
    draft_state = "draft"

    wp_yaml = fill_workplan_template(
        {
            "name": expected_name,
            "description": expected_description,
            "state": draft_state,
            "steps": [
                {
                    "name": step_name,
                    "application": app_name,
                    "blueprint": bp_path.as_posix(),
                },
            ],
        }
    )
    wp_path = tmp_path / "hw-workplan.yaml"
    assert wp_path.write_text(wp_yaml)

    return wp_path


@pytest.fixture
def hw_single_step_wp(
    hw_single_step_wp_path: Path,
) -> Workplan:
    """Return a workplan containing a single step that runs the hello_world application.

    Returns
    -------
    Workplan
    """
    return deserialize(hw_single_step_wp_path, Workplan)


@pytest.fixture
def heterogeneous_workplan_path(
    tmp_path: Path,
    hw_single_step_wp: Workplan,
    single_step_workplan: Workplan,
) -> Path:
    """Return the path to a workplan containing steps triggering different applications.

    Uses pre-existing fixtures and combines their steps into a new workplan.

    Parameters
    ----------
    tmp_path : Path
        Path where temporary workplans and blueprints will be written during the test.
    hw_single_step_wp : Path
        Path to a workplan that contains a single hello_world step.
    single_step_workplan : Path
        Path to a workplan that contains a single ucla_roms/sleep step.

    Returns
    -------
    Path
    """
    wp = Workplan(
        name="heterogeneous-workplan",
        description="This is a test workplan containing steps triggering different applications",
        state=WorkplanState.Draft,
        compute_environment={"slurm": {"max_walltime": "00:10:00"}},
        runtime_vars=["var1", "var2"],
        steps=[
            hw_single_step_wp.steps[0],
            single_step_workplan.steps[0],
        ],
    )
    wp_path = tmp_path / f"{wp.name}.yaml"
    assert serialize(wp_path, wp)

    return wp_path


@pytest.mark.parametrize(
    "target",
    [
        "@ankona",
        "@ScottEilerman",
        "@NoraLoose",
    ],
)
@pytest.mark.asyncio
async def test_hello_world_runner_happy_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    target: str,
    hello_world_bp_content: str,
) -> None:
    """Test the execution of the "hello, {world}!" application

    This test verifies that the runner receives and parses the Blueprint, and
    executes the `Service` lifecycle.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    target : str
        Target to say hello to. Used to verify the runner triggers the
        `on_iteration` callback and that target is correctly parsed.
    hello_world_bp_content : str
        The content of a `HelloWorldBlueprint`.
    """
    bp_content = hello_world_bp_content.replace("@ankona", target)
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    request = RunnerRequest(str(bp_path), HelloWorldBlueprint)
    svc_cfg = get_service_config(log_level="INFO")

    runner = HelloWorldRunner(request, svc_cfg)

    await runner.execute()

    # Confirm the success disposition is set
    assert runner.state.status == ExecutionStatus.COMPLETED

    captured = capsys.readouterr()
    assert f"hello, {target}".lower() in captured.out.lower()


@pytest.mark.asyncio
async def test_hello_world_workplan(
    tmp_path: Path,
    hello_world_bp_content: str,
    fill_workplan_template: Callable[[dict[str, t.Any]], str],
) -> None:
    """Test deserialization of a workplan containing a non UCLA-ROMS application.

    Consider moving to a parametrized test that executes the same evaluation on all
    known application types.

    Parameters
    ----------
    tmp_path : Path
        Temporary output location for writing the test blueprint.
    hello_world_bp_content : str
        The content of a `HelloWorldBlueprint`.
    """
    bp_content = hello_world_bp_content
    bp_path = tmp_path / "hw.yaml"
    bp_path.write_text(bp_content)

    expected_name = "hw-workplan"
    expected_description = "This is a test workplan using the hello_world application"

    step_name = "Hello Devloper!"
    app_name = "hello_world"
    draft_state = "draft"

    wp_yaml = fill_workplan_template(
        {
            "name": expected_name,
            "description": expected_description,
            "state": draft_state,
            "steps": [
                {
                    "name": step_name,
                    "application": app_name,
                    "blueprint": bp_path,
                },
            ],
        }
    )
    wp_path = tmp_path / "hw-workplan.yaml"
    wp_path.write_text(wp_yaml)

    # ensure the read succeeds
    workplan = deserialize(wp_path, Workplan)
    assert workplan.name == expected_name
    assert workplan.description == expected_description
    assert workplan.state == draft_state
    assert len(workplan.steps) == 1
    assert workplan.steps[0].name == step_name
    assert workplan.steps[0].application == app_name
    assert workplan.steps[0].blueprint_path == str(bp_path)

    # ensure a write succeeds
    wp_copy = tmp_path / "hw-workplan-copy.yaml"
    assert serialize(wp_copy, workplan), "Failed to serialize workplan"

    # and sanity check the copy that was loaded
    workplan_copy = deserialize(wp_copy, Workplan)
    assert workplan_copy.name == workplan.name
    assert workplan_copy.description == workplan.description
    assert workplan_copy.state == workplan.state
    assert workplan_copy.steps[0].name == workplan.steps[0].name
    assert workplan_copy.steps[0].application == workplan.steps[0].application
    assert workplan_copy.steps[0].blueprint_path == workplan.steps[0].blueprint_path


def test_hello_world_workplan_scheduling(
    hw_single_step_wp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the execution of a workplan containing a non UCLA-ROMS application.

    Parameters
    ----------
    hw_single_step_wp_path : Path
        The path to the workplan containing a single step that runs the hello_world application.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to shorten the orchestrator's polling delays.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.01,))
    runner = CliRunner()
    custom_env = {
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:02:00",
        ENV_CSTAR_SLURM_QUEUE: "debug",
    }

    run_id = str(uuid.uuid4())
    with (
        mock.patch.dict(os.environ, custom_env),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
        mock.patch(
            "cstar.orchestration.adapter.StepToRunRequestAdapter",
            StepToPlaceholderAdapter,
        ),
    ):
        args = [hw_single_step_wp_path.as_posix(), "--run-id", run_id]

        result = runner.invoke(
            app_run_workplan,
            args,
            color=False,
        )

    assert "scheduling has completed" in result.stdout
    assert result.exit_code == 0


def test_workplan_run_unknown_clobber_step_fails_fast(
    hw_single_step_wp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an unrecognized `--clobber` step value fails fast as a CLI usage
    error (rather than a generic failure) and lists the valid step names.

    Parameters
    ----------
    hw_single_step_wp_path : Path
        The path to the workplan containing a single step that runs the
        hello_world application.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to shorten the orchestrator's polling delays.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.01,))
    runner = CliRunner()
    custom_env = {
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:02:00",
        ENV_CSTAR_SLURM_QUEUE: "debug",
    }

    with (
        mock.patch.dict(os.environ, custom_env),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
    ):
        result = runner.invoke(
            app_run_workplan,
            [
                hw_single_step_wp_path.as_posix(),
                "--run-id",
                str(uuid.uuid4()),
                "--clobber",
                "does-not-exist",
            ],
            color=False,
        )

    assert result.exit_code == 2
    assert "does-not-exist" in result.stderr


def test_heterogeneous_workplan(
    heterogeneous_workplan_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the execution of a workplan containing multiple applications.

    Parameters
    ----------
    heterogeneous_workplan_path : Path
        The path to the workplan containing a step relying on the hello_world application
        and a step relying on the UCLA-ROMS application.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to shorten the orchestrator's polling delays.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.01,))
    runner = CliRunner()
    custom_env = {
        ENV_CSTAR_SLURM_ACCOUNT: "abc123",
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:02:00",
        ENV_CSTAR_SLURM_QUEUE: "debug",
    }
    run_id = str(uuid.uuid4())
    with (
        mock.patch.dict(os.environ, custom_env),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
        mock.patch(
            "cstar.orchestration.adapter.StepToRunRequestAdapter",
            StepToPlaceholderAdapter,
        ),
    ):
        args = [heterogeneous_workplan_path.as_posix(), "--run-id", run_id]

        result = runner.invoke(
            app_run_workplan,
            args,
            color=False,
        )

    assert run_id in result.stdout
    assert "completed" in result.stdout
    assert result.exit_code == 0


def test_hw_runner_bp_only(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that a blueprint containing the sample application is executed
    correctly by the `cstar blueprint run` command.

    Parameters
    ----------
    hello_world_bp_path : Path
        A fixture that stores an HW blueprint and returns the path.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to shorten the orchestrator's polling delays.
    """
    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.01,))
    runner = CliRunner()
    custom_env: dict[str, str] = {
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:02:00",
        ENV_CSTAR_SLURM_QUEUE: "debug",
    }
    with (
        mock.patch.dict(os.environ, custom_env),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
        mock.patch(
            "cstar.orchestration.adapter.StepToRunRequestAdapter",
            StepToPlaceholderAdapter,
        ),
    ):
        args = [str(hello_world_bp_path)]

        print(f"Executing CliRunner with: {args}")
        result = runner.invoke(
            app_run_blueprint,
            args,
            color=False,
        )

        assert "Hello," in result.stdout
        assert result.exit_code == 0


def test_hello_world_blueprint_runs_without_slurm_settings(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a step that submits no scheduler job starts on a SLURM system whose
    required account and queue are unset, as a local-launcher step does on an
    HPC login node before the user has run `cstar init`.

    Parameters
    ----------
    hello_world_bp_path : Path
        A fixture that stores an HW blueprint and returns the path.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to unset the SLURM settings.
    """
    monkeypatch.delenv(ENV_CSTAR_SLURM_ACCOUNT, raising=False)
    monkeypatch.delenv(ENV_CSTAR_SLURM_QUEUE, raising=False)
    scheduler = SlurmScheduler(
        queues=[SlurmQOS(name="wholenode", query_name="wholenode")],
        primary_queue_name="wholenode",
    )
    with (
        mock.patch(
            "cstar.system.environment.CStarEnvironment.settings_klass",
            AnvilEnvSettings,
        ),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=scheduler),
        ),
    ):
        result = CliRunner().invoke(
            app_run_blueprint, [str(hello_world_bp_path)], color=False
        )

    assert result.exit_code == 0, result.output
    assert "Hello," in result.stdout


@pytest.mark.parametrize("run_from_workplan_dir", [True, False])
def test_hello_world_workplan_relative_blueprint_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hello_world_bp_content: str,
    run_from_workplan_dir: bool,
) -> None:
    """Verify a step whose `blueprint` is a path relative to the workplan file
    is launched with the absolute path, whether `cstar workplan submit` is
    invoked from the workplan's directory or from elsewhere.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the workplan, blueprint and run outputs
    monkeypatch : pytest.MonkeyPatch
        Fixture used to change the working directory and to shorten the
        orchestrator's polling delays
    hello_world_bp_content : str
        Fixture providing the content of a minimal hello-world blueprint
    run_from_workplan_dir : bool
        Whether the CLI is invoked from the directory holding the workplan
    """
    plans = tmp_path / "plans"
    plans.mkdir()
    (plans / "hw.yaml").write_text(hello_world_bp_content)
    wp_path = plans / "hw-workplan.yaml"
    wp_path.write_text(
        textwrap.dedent("""\
            name: hw-relative
            description: A hello_world step whose blueprint sits beside the workplan
            state: draft
            steps:
              - name: Say Hello
                application: hello_world
                blueprint: ./hw.yaml
            """)
    )
    if run_from_workplan_dir:
        monkeypatch.chdir(plans)
        wp_arg = wp_path.name
    else:
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        wp_arg = wp_path.as_posix()

    monkeypatch.setattr("cstar.orchestration.dag_runner.POLL_DELAYS", (0.01,))
    runner = CliRunner()
    custom_env = {
        ENV_CSTAR_SLURM_MAX_WALLTIME: "00:02:00",
        ENV_CSTAR_SLURM_QUEUE: "debug",
    }
    run_id = str(uuid.uuid4())
    with (
        mock.patch.dict(os.environ, custom_env),
        mock.patch(
            "cstar.system.manager.CStarSystemManager.scheduler",
            mock.PropertyMock(return_value=None),
        ),
    ):
        result = runner.invoke(
            app_run_workplan, [wp_arg, "--run-id", run_id], color=False
        )

    assert result.exit_code == 0, result.output
    assert "scheduling has completed" in result.stdout

    expected = (plans / "hw.yaml").resolve()
    wp_run = asyncio.run(TrackingRepository().get_workplan_run(run_id))
    assert wp_run is not None
    persisted = deserialize(wp_run.trx_workplan_path, LiveWorkplan)
    step = persisted.steps[0]
    assert Path(step.blueprint_path) == expected

    # the local launcher's proxy script carries the absolute path into the
    # step command, which runs with the task directory as its cwd
    script = step.script_path.read_text()
    assert f'BLUEPRINT_PATH="{expected}"' in script
    assert f"cstar blueprint run {expected}" in script


def _hw_runner(bp_path: Path) -> HelloWorldRunner:
    request = RunnerRequest(str(bp_path), HelloWorldBlueprint)
    return HelloWorldRunner(request, get_service_config(log_level="INFO"))


@pytest.mark.parametrize("run_id", ["", "run-1234"])
@pytest.mark.asyncio
async def test_runner_writes_step_record(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    run_id: str,
) -> None:
    """Verify a completed run leaves a step record in the working directory.

    Parameters
    ----------
    hello_world_bp_path : Path
        The path to a hello_world blueprint.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set or clear the run id.
    run_id : str
        The exported `CSTAR_RUNID`; empty means unset.
    """
    if run_id:
        monkeypatch.setenv(ENV_CSTAR_RUNID, run_id)
    else:
        monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)

    runner = _hw_runner(hello_world_bp_path)
    await runner.execute()

    blueprint = runner.blueprint
    record = read_step_record(blueprint.effective_working_dir)
    assert record is not None
    assert record.outcome == "completed"
    assert record.run_id == run_id
    assert record.step == blueprint.name  # a standalone run is its own step
    assert record.application == blueprint.application
    assert record.blueprint == BlueprintIdentity.of(blueprint)
    assert record.blueprint_schema_version == blueprint.schema_version
    assert record.start_at is not None
    assert record.start_at <= record.finished_at
    assert "cstar-ocean" in record.versions
    assert record.errors == []


def _leave_slurm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove SLURM's variables, so a test also passes inside an allocation."""
    for name in [k for k in os.environ if k.startswith("SLURM_")]:
        monkeypatch.delenv(name)


@pytest.mark.asyncio
async def test_runner_records_a_local_execution(
    hello_world_bp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a step run outside SLURM records that it ran locally, with no allocation."""
    _leave_slurm(monkeypatch)
    runner = _hw_runner(hello_world_bp_path)

    await runner.execute()

    record = read_step_record(runner.blueprint.effective_working_dir)
    assert record is not None
    assert record.execution == ExecutionFacts(
        scheduler="local", host=socket.gethostname(), pid=os.getpid()
    )


@pytest.mark.asyncio
async def test_runner_records_its_slurm_allocation(
    hello_world_bp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a step run in a SLURM job records the allocation the job was granted."""
    _leave_slurm(monkeypatch)
    for name, value in {
        "SLURM_JOB_ID": "4242",
        "SLURM_JOB_NUM_NODES": "2",
        "SLURM_NTASKS": "16",
        "SLURM_CPUS_ON_NODE": "8",
        "SLURM_JOB_PARTITION": "debug",
        "SLURM_JOB_ACCOUNT": "m1234",
    }.items():
        monkeypatch.setenv(name, value)
    runner = _hw_runner(hello_world_bp_path)

    await runner.execute()

    record = read_step_record(runner.blueprint.effective_working_dir)
    assert record is not None
    assert record.execution == ExecutionFacts(
        scheduler="slurm",
        job_id="4242",
        host=socket.gethostname(),
        pid=os.getpid(),
        nodes=2,
        tasks=16,
        cpus=8,
        partition="debug",
        account="m1234",
    )


def test_execution_facts_outside_slurm() -> None:
    """Verify variables of an allocation do not count without a job id."""
    facts = execution_facts({"SLURM_NTASKS": "4", "SLURM_JOB_PARTITION": "debug"})

    assert facts == ExecutionFacts(
        scheduler="local", host=socket.gethostname(), pid=os.getpid()
    )


def test_execution_facts_accept_the_older_job_id_spelling() -> None:
    facts = execution_facts({"SLURM_JOBID": "77"})

    assert (facts.scheduler, facts.job_id) == ("slurm", "77")


def test_execution_facts_leave_unreported_values_empty() -> None:
    """Verify a job that reports no partition or counts records none."""
    facts = execution_facts({"SLURM_JOB_ID": "77"})

    assert facts == ExecutionFacts(
        scheduler="slurm", job_id="77", host=socket.gethostname(), pid=os.getpid()
    )


@pytest.mark.parametrize("value", ["abc", "2(x3)", "1.5"])
def test_execution_facts_count_an_unparseable_value_as_zero(
    value: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify the record of what ran a step never fails the step: a count SLURM
    reports in a form this build cannot read is logged at debug and left at zero.
    """
    environ = {"SLURM_JOB_ID": "77", "SLURM_NTASKS": value, "SLURM_CPUS_ON_NODE": "4"}

    with caplog.at_level(logging.DEBUG, logger="cstar.entrypoint.runner"):
        facts = execution_facts(environ)

    assert (facts.tasks, facts.cpus) == (0, 4)
    assert [r.getMessage() for r in caplog.records if "SLURM_NTASKS" in r.message] == [
        f"Ignoring SLURM_NTASKS={value!r}: it is not a count"
    ]
    assert {r.levelno for r in caplog.records if "SLURM_NTASKS" in r.message} == {
        logging.DEBUG
    }


@pytest.mark.asyncio
async def test_runner_records_the_blueprint_file_it_ran(tmp_path: Path) -> None:
    """Verify the record names the blueprint the runner loaded relative to the
    step root when it lies there, as a file written by a step's directives does,
    and names none when it lies elsewhere.
    """
    step_root = tmp_path / "step"
    inside = step_root / "work" / "hello.ovrd.yaml"
    outside = tmp_path / "hello.yaml"
    inside.parent.mkdir(parents=True)
    for path in (inside, outside):
        blueprint = HelloWorldBlueprint(
            name="hello",
            description="says hello",
            target="world",
            working_dir=step_root,
        )
        assert serialize(path, blueprint)

    for path, expected in ((inside, "work/hello.ovrd.yaml"), (outside, "")):
        runner = _hw_runner(path)
        await runner.execute()

        record = read_step_record(step_root)
        assert record is not None
        assert record.blueprint_file == expected


def test_runner_records_no_blueprint_file_for_a_remote_blueprint(
    hello_world_bp_path: Path,
) -> None:
    """Verify a blueprint fetched from a URL is not recorded as a file."""
    runner = _hw_runner(hello_world_bp_path)
    root = runner.blueprint.effective_working_dir

    with mock.patch.object(
        type(runner),
        "blueprint_uri",
        new_callable=mock.PropertyMock,
        return_value="https://example.org/blueprints/hello.yaml",
    ):
        assert runner._blueprint_file(root) == ""


@pytest.mark.asyncio
async def test_runner_records_the_workplan_step_name(
    hello_world_bp_path: Path,
) -> None:
    """Verify a request that names its workplan step is recorded under that name."""
    request = RunnerRequest(str(hello_world_bp_path), HelloWorldBlueprint, step="A")
    runner = HelloWorldRunner(request, get_service_config(log_level="INFO"))

    await runner.execute()

    record = read_step_record(runner.blueprint.effective_working_dir)
    assert record is not None
    assert record.step == "A"
    assert record.blueprint.name == runner.blueprint.name


@pytest.mark.asyncio
async def test_runner_records_failure(hello_world_bp_path: Path) -> None:
    """Verify a run that raises leaves a failed record carrying the error."""
    runner = _hw_runner(hello_world_bp_path)

    with mock.patch.object(
        HelloWorldRunner, "run", mock.AsyncMock(side_effect=RuntimeError("kaboom"))
    ):
        await runner.execute()

    record = read_step_record(runner.blueprint.effective_working_dir)
    assert record is not None
    assert record.outcome == "failed"
    assert any("kaboom" in error for error in record.errors)


@pytest.mark.asyncio
async def test_runner_records_start_failure(hello_world_bp_path: Path) -> None:
    """Verify a run that fails to start leaves an `unsubmitted` record."""
    runner = _hw_runner(hello_world_bp_path)

    with mock.patch.object(
        HelloWorldRunner, "_on_start", mock.Mock(side_effect=ValueError("no start"))
    ):
        with pytest.raises(ValueError, match="no start"):
            await runner.execute()

    record = read_step_record(runner.blueprint.effective_working_dir)
    assert record is not None
    assert record.outcome == "unsubmitted"


@pytest.mark.asyncio
async def test_runner_start_failure_without_blueprint_warns(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify a blueprint that cannot load costs only a warning at shutdown."""
    runner = _hw_runner(tmp_path / "missing.yaml")

    with caplog.at_level(logging.WARNING):
        await runner.execute()

    assert "Unable to write the step record" in caplog.text
    assert runner.state.status != ExecutionStatus.COMPLETED
    assert not list(tmp_path.rglob(STEP_RECORD_NAME))
