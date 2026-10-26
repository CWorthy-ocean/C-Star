import json
import shutil
import typing as t
from collections.abc import Callable, Generator
from pathlib import Path
from unittest import mock

import pytest
import yaml
from typer.testing import CliRunner

from cstar.applications.core import (
    ApplicationDefinition,
    ArtifactKind,
    ArtifactSpec,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    get_application,
)
from cstar.applications.hello_world import (
    HelloWorldApplication,
    HelloWorldBlueprint,
    HelloWorldRunner,
)
from cstar.applications.ucla_roms.app import UclaRomsApplication, UclaRomsRunner
from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.base.env import (
    ENV_CSTAR_DISABLE_MIGRATION,
    ENV_CSTAR_RUNID,
    ENV_CSTAR_STATE_HOME,
    FLAG_ON,
)
from cstar.base.exceptions import CstarError
from cstar.cli.blueprint.run import app
from cstar.entrypoint.runner import BlueprintRunner
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_DIRECTIVES_URI_LONG,
    ARG_PRE_RUN,
    ARG_RESUME,
    ARG_STEP,
    ARG_VERBOSE,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.adapter import prepare_directive_file
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    AttemptStatus,
    StepRecord,
    read_run_manifest,
    read_step_record,
)
from cstar.orchestration.models import Application, Blueprint
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import ApplyOverridesDirective
from cstar.roms.simulation import ROMSSimulation


@pytest.fixture(autouse=True)
def _run_from_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run every test from its own directory.

    The fixture blueprints name working directories relative to the current
    directory (`.` and `temp_out_dir`), and a standalone run now records itself
    there; without this the checkout would collect their manifests.
    """
    monkeypatch.chdir(tmp_path)


def test_blueprint_run_file_dne(
    tmp_path: Path,
    flatten_cli_output: Callable[[str], str],
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify that a path to a non-existent blueprint fails to be started due
    to validation and the error identifies the missing file.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    flatten_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for phrase matching
    squash_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for path matching
    """
    bp_path = tmp_path / "blueprint-dne.yml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert "not found" in flatten_cli_output(result.stderr)
    # the message must name the missing blueprint
    assert bp_path.name in squash_cli_output(result.stderr)


def test_blueprint_run_migrated_blueprint_invalid(
    tmp_path: Path,
    plotter_v1_0_0_model: dict[str, t.Any],
    flatten_cli_output: Callable[[str], str],
    squash_cli_output: Callable[[str], str],
) -> None:
    """Verify a blueprint whose migrated content fails model validation is
    rejected with a usage error naming the blueprint, instead of a raw
    traceback.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    plotter_v1_0_0_model : dict[str, t.Any]
        Fixture providing the raw content of a plotter blueprint at schema 1.0.0
    flatten_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for phrase matching
    squash_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for path matching
    """
    # drop fields the migrated model requires so post-migration validation fails
    model = {
        k: v
        for k, v in plotter_v1_0_0_model.items()
        if k not in ("input_dir", "grid_file_path")
    }
    bp_path = tmp_path / "plotter_incomplete_1.0.0.json"
    bp_path.write_text(json.dumps(model))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path.as_posix()],
        color=False,
    )

    assert result.exit_code == 2
    stderr_flat = flatten_cli_output(result.stderr)
    assert "is invalid" in stderr_flat
    assert "Details:" in stderr_flat
    assert bp_path.name in squash_cli_output(result.stderr)


def test_blueprint_run_remote_blueprint_dne() -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is not executed if the URL is invalid.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint-X.yaml"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [bp_path],
        color=False,
    )

    assert "not found" in result.stderr


def test_blueprint_run_remote_blueprint(
    serve_remote_files: dict[str, Path], bp_templates_dir: Path
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.

    The URL serves a schema 2.0.0 blueprint (as the published wales_toy one
    still is); automatic migration brings it to the current schema during `run`.
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"
    serve_remote_files[bp_path] = bp_templates_dir / "roms_marbl/blueprint.2.0.0.yaml"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            app_config.runner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path],
            color=False,
        )

    mock_exec_runner.assert_called_once()


def _write_2_1_0_blueprint(source: Path, dest_dir: Path) -> tuple[Path, int]:
    """Reshape the (3.0.0) complete blueprint fixture into its 2.1.0 form.

    Returns the path to the written file and the time step it carries in the
    legacy `model_params.time_step` field.
    """
    data = yaml.safe_load(source.read_text())
    data.pop("$schema", None)
    time_step = data.pop("namelist_overrides")["time_stepping"]["dt"]
    use_pio = data["partitioning"].pop("use_pio", False)
    data["schema_version"] = "2.1.0"
    data["model_params"] = {"time_step": time_step, "use_pio": use_pio}

    bp_path = dest_dir / "blueprint_2_1_0.yaml"
    bp_path.write_text(yaml.safe_dump(data))
    return bp_path, time_step


def test_blueprint_run_auto_migrates_2_1_0_blueprint(
    tmp_path: Path,
    complete_blueprint_path: Path,
    mock_xdg_dirs: dict[str, Path],
) -> None:
    """`run` migrates a 2.1.0 blueprint to the current schema by default and
    executes it: `model_params.time_step` lands in
    `namelist_overrides.time_stepping.dt` and `use_pio` moves under
    `partitioning` in the persisted, migrated blueprint.
    """
    bp_path, time_step = _write_2_1_0_blueprint(complete_blueprint_path, tmp_path)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            app_config.runner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [bp_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()

    state_home = mock_xdg_dirs[ENV_CSTAR_STATE_HOME]
    current = get_application(UclaRomsApplication.name).schema_version
    migrated_path = next(state_home.rglob(f"{bp_path.stem}_{current}*"))
    migrated = yaml.safe_load(migrated_path.read_text())
    assert "model_params" not in migrated
    # serialize() excludes model defaults, so validate to see effective values
    bp = UclaRomsBlueprint.model_validate(migrated)
    assert bp.schema_version == current
    assert bp.partitioning.use_pio is False
    assert bp.namelist_overrides["time_stepping"]["dt"] == time_step


def test_blueprint_run_disable_migration_outdated_rejected(
    tmp_path: Path,
    complete_blueprint_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `CSTAR_DISABLE_MIGRATION` set, an out-of-date blueprint fails
    early instead of being migrated, and is not executed.
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)
    bp_path, _ = _write_2_1_0_blueprint(complete_blueprint_path, tmp_path)

    with mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    # rich wraps the message at the terminal width, so a line break may land
    # inside the phrase; normalize whitespace before matching.
    assert "migration is disabled" in " ".join(result.stdout.split())
    mock_exec.assert_not_called()


def test_blueprint_run_disable_migration_current_blueprint_ok(
    complete_blueprint_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `CSTAR_DISABLE_MIGRATION` set, an up-to-date blueprint is
    unaffected and executes normally.
    """
    monkeypatch.setenv(ENV_CSTAR_DISABLE_MIGRATION, FLAG_ON)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            app_config.runner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [complete_blueprint_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()


def test_blueprint_run_current_blueprint_not_persisted(
    complete_blueprint_path: Path,
    mock_xdg_dirs: dict[str, Path],
) -> None:
    """An up-to-date blueprint runs as-is: no migrated copy is written to the
    state directory.
    """
    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    app_config: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
        get_application("roms_marbl")
    )

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            app_config.runner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [complete_blueprint_path.as_posix()],
            color=False,
        )

    mock_exec_runner.assert_called_once()
    state_home = mock_xdg_dirs[ENV_CSTAR_STATE_HOME]
    assert not list(state_home.rglob(f"{complete_blueprint_path.stem}_*"))


def test_blueprint_run_invalid_blueprint_exits_nonzero(
    tmp_path: Path,
    complete_blueprint_path: Path,
) -> None:
    """A blueprint that is schema-current but fails content validation exits
    with a non-zero code and is not executed.
    """
    data = yaml.safe_load(complete_blueprint_path.read_text())
    data.pop("$schema", None)
    # violate the model validator: end_date beyond the valid range
    data["runtime_params"]["end_date"] = "2200-01-01T00:00:00"
    bp_path = tmp_path / "blueprint_invalid.yaml"
    bp_path.write_text(yaml.safe_dump(data))

    with mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [bp_path.as_posix()],
            color=False,
        )

    assert result.exit_code != 0
    mock_exec.assert_not_called()


@pytest.mark.parametrize(
    "directive_path",
    [
        "directive-dne.json",
        "https://www.google.com/directive-dne.json",
    ],
)
def test_blueprint_run_apply_directive_dne(
    directive_path: str,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify that an exception is raised if a path to a non-existent directive file is passed.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture providing a helper preparing CLI output for phrase matching
    """
    bp_path = "https://raw.githubusercontent.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example/refs/heads/main/wales-toy-domain/wales_toy_blueprint.yaml"

    with mock.patch(
        "cstar.applications.ucla_roms.app.UclaRomsRunner.execute",
        return_value=RunnerResult(
            RunnerRequest(bp_path, UclaRomsBlueprint),
            RunnerState(ExecutionStatus.COMPLETED),
        ),
    ) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_path,
            ],
            color=False,
        )

    # Depending on the installed typer/rich versions, usage errors render as a
    # rich panel whose box borders and width-dependent wrapping can split the
    # phrase across lines -- collapse the decoration before matching.
    stderr_flat = flatten_cli_output(result.stderr)
    assert "file not found" in stderr_flat
    mock_exec.assert_not_called()


def test_blueprint_run_apply_directive_empty(
    tmp_path: Path,
    package_path: Path,
) -> None:
    """Verify that an exception is raised if an empty directive file is passed."""
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
    directive_file_path = tmp_path / "directive-dne.json"
    directive_file_path.touch()

    with mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_file_path.as_posix(),
            ],
            color=False,
        )

    assert "malformed" in result.stderr


@pytest.fixture
def directive_run_harness(
    tmp_path: Path,
    mocked_simulation_outputs: tuple[Path, Path, Path],
    package_path: Path,
) -> Generator[tuple[str, LiveStep, Path, mock.Mock]]:
    """Build a step with a `continue-from` directive, write its directive
    file, and mock blueprint execution so `run` can be invoked against it.

    Shared by tests that only differ in what happens to the directive file
    (and how the recorded workplan is mocked) before the CLI is invoked.

    Yields
    ------
    tuple[str, LiveStep, Path, mock.Mock]
        The blueprint path, the step, its directive file path, and the mock
        tracking calls to `UclaRomsRunner.execute`.
    """
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
    _, step_dir, _ = mocked_simulation_outputs

    temp_step = LiveStep(
        name="step-for-testing",
        application=Application.UCLA_ROMS.value,
        blueprint=bp_path,
        working_dir=tmp_path,
        directives={
            "continue-from": {"path": step_dir.as_posix()},
        },
    )
    directive_path = prepare_directive_file(temp_step)

    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        """Mock the main execution method to avoid `real work` and ensure the result
        attribute is updated.
        """
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            UclaRomsRunner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
    ):
        yield bp_path, temp_step, directive_path, mock_exec_runner


def test_blueprint_run_apply_directives(
    directive_run_harness: tuple[str, LiveStep, Path, mock.Mock],
) -> None:
    """Verify that a URL to a remote blueprint is handled properly and the
    blueprint is executed.
    """
    bp_path, temp_step, directive_path, mock_exec_runner = directive_run_harness

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(
            return_value=LiveWorkplan(
                name="test-workplan",
                description="a live workplan used to create a `WorkplanRun` to test directives",
                steps=[temp_step],
            )
        ),
    ):
        runner = CliRunner()
        _ = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_path.as_posix(),
            ],
            color=False,
        )

    mock_exec_runner.assert_called_once()


def test_blueprint_run_restores_missing_directive_file(
    directive_run_harness: tuple[str, LiveStep, Path, mock.Mock],
    mock_run_id: str,
) -> None:
    """Verify the CLI restores a directive file missing at submit time from
    the workplan recorded for the run, and the runner still executes.
    """
    bp_path, temp_step, directive_path, mock_exec_runner = directive_run_harness

    # the work directory is cleaned while the step waits in the queue
    shutil.rmtree(directive_path.parent)
    assert not directive_path.exists()

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(
            return_value=LiveWorkplan(
                name="test-workplan",
                description="a live workplan used to restore a missing directive file",
                steps=[temp_step],
            )
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                directive_path.as_posix(),
            ],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_exec_runner.assert_called_once()
    assert directive_path.exists()


def test_blueprint_run_restore_directive_file_runtime_error_fails_cleanly(
    tmp_path: Path,
    package_path: Path,
    mock_run_id: str,
) -> None:
    """Verify the CLI fails cleanly, without executing the runner, when a
    missing directive file cannot be restored from the recorded workplan.
    """
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
    missing_directive_path = tmp_path / "directives.yaml"
    assert not missing_directive_path.exists()

    with (
        mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec,
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(side_effect=RuntimeError("no run context available")),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                bp_path,
                ARG_DIRECTIVES_URI_LONG,
                missing_directive_path.as_posix(),
            ],
            color=False,
        )

    assert result.exit_code != 0
    mock_exec.assert_not_called()

    # rich wraps the message at the terminal width, so a line break may land
    # inside the phrase; normalize whitespace before matching.
    stderr_flat = " ".join(result.stderr.replace("│", " ").split())
    assert "could not be restored" in stderr_flat


def test_blueprint_run_deferred_blueprint(
    tmp_path: Path,
    hello_world_bp_content: str,
) -> None:
    """Verify a `step://` URI is resolved against the producing step's output
    directory and the generated blueprint is executed with the packaged
    overrides applied.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    hello_world_bp_content : str
        The content of a minimal hello-world blueprint.
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=(tmp_path / "unused.yaml").as_posix(),
        working_dir=tmp_path / "producer",
    )
    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)
    generated = producer.fsm.output_dir / "generated.yaml"
    generated.write_text(hello_world_bp_content)

    consumer = LiveStep.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer", "filename": "generated.yaml"},
            "depends_on": ["producer"],
            "working_dir": tmp_path / "consumer",
            "directives": {
                ApplyOverridesDirective.key(): {
                    ApplyOverridesDirective.KEY_OVERRIDES: {
                        "target": "@overridden-target",
                        "working_dir": (tmp_path / "consumer").as_posix(),
                    },
                    ApplyOverridesDirective.KEY_APPLICATION: "hello_world",
                },
            },
        },
    )
    directive_path = prepare_directive_file(consumer)

    live_plan = LiveWorkplan(
        name="deferred-run",
        description="a live workplan providing producer-step context",
        steps=[producer],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                str(consumer.blueprint_path),
                ARG_DIRECTIVES_URI_LONG,
                directive_path.as_posix(),
            ],
            color=False,
        )

    assert result.exit_code == 0, result.output
    assert "Hello, @overridden-target" in result.stdout


def test_blueprint_run_deferred_blueprint_unresolvable(
    tmp_path: Path,
) -> None:
    """Verify a `step://` URI that cannot be resolved fails with a clear
    error and a non-zero exit code.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to read/write test inputs and outputs
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=(tmp_path / "unused.yaml").as_posix(),
        working_dir=tmp_path / "producer",
    )
    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)

    live_plan = LiveWorkplan(
        name="deferred-run",
        description="a live workplan providing producer-step context",
        steps=[producer],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["step://producer/never-generated.yaml"],
            color=False,
        )

    assert result.exit_code == 1
    assert "Unable to resolve deferred blueprint" in result.stdout


def _write_newer_step_record(step_root: Path) -> None:
    """Write a step record stamped with a schema major newer than this build reads."""
    current = StepRecord.model_fields["schema_version"].default
    newer = f"{int(current.split('.')[0]) + 1}.0.0"
    step_root.mkdir(parents=True, exist_ok=True)
    (step_root / STEP_RECORD_NAME).write_text(f"schema_version: {newer}\n")


def _assert_refused_for_upgrade(result: t.Any) -> None:
    """Assert the CLI refused the input with the upgrade guidance, not a traceback."""
    assert isinstance(result.exception, SystemExit), result.exception
    assert result.exit_code == 1
    assert "Upgrade cstar-ocean" in result.stdout


def test_blueprint_run_deferred_blueprint_newer_record_refused(
    tmp_path: Path,
) -> None:
    """A deferred blueprint whose producer's record was written by a newer
    cstar is refused with the upgrade guidance, not a traceback.
    """
    producer = LiveStep(
        name="producer",
        application="hello_world",
        blueprint=(tmp_path / "unused.yaml").as_posix(),
        working_dir=tmp_path / "producer",
    )
    producer.fsm.output_dir.mkdir(parents=True, exist_ok=True)
    _write_newer_step_record(producer.fsm.root_dir)

    live_plan = LiveWorkplan(
        name="deferred-run",
        description="a live workplan providing producer-step context",
        steps=[producer],
    )

    with mock.patch(
        "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
        mock.Mock(return_value=live_plan),
    ):
        result = CliRunner().invoke(app, ["step://producer"], color=False)

    _assert_refused_for_upgrade(result)
    assert "Unable to resolve deferred blueprint" in result.stdout


def test_blueprint_run_directive_newer_record_refused(
    tmp_path: Path,
    package_path: Path,
    mock_run_id: str,
) -> None:
    """A `continue-from` directive whose source step's record was written by a
    newer cstar is refused with the upgrade guidance, not a traceback, and the
    runner never starts.
    """
    bp_path = str(package_path / "docs/tutorials/wales_toy_blueprint.yaml")
    producer = LiveStep(
        name="producer",
        application=Application.UCLA_ROMS.value,
        blueprint=bp_path,
        working_dir=tmp_path / "producer",
    )
    _write_newer_step_record(producer.fsm.root_dir)

    consumer = LiveStep(
        name="consumer",
        application=Application.UCLA_ROMS.value,
        blueprint=bp_path,
        working_dir=tmp_path / "consumer",
        depends_on=["producer"],
        directives={"continue-from": {"step": "producer"}},
    )
    directive_path = prepare_directive_file(consumer)

    live_plan = LiveWorkplan(
        name="continue-run",
        description="a live workplan providing source-step context",
        steps=[producer, consumer],
    )

    with (
        mock.patch(
            "cstar.orchestration.transforms.DirectiveConfig.load_workplan",
            mock.Mock(return_value=live_plan),
        ),
        mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec,
    ):
        result = CliRunner().invoke(
            app,
            [bp_path, ARG_DIRECTIVES_URI_LONG, directive_path.as_posix()],
            color=False,
        )

    _assert_refused_for_upgrade(result)
    mock_exec.assert_not_called()


def test_blueprint_run_resume_non_resumable_app_fails_fast(
    complete_blueprint_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--resume` against an application that does not declare itself
    resumable fails fast with a usage error naming the application, and the
    runner is never invoked.
    """
    with (
        mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec,
        mock.patch.object(UclaRomsApplication, "resumable", False),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 2
    stderr_flat = flatten_cli_output(result.stderr)
    assert "ucla_roms" in stderr_flat
    assert "does not support" in stderr_flat
    mock_exec.assert_not_called()


def test_blueprint_run_resume_and_clobber_fails_fast(
    complete_blueprint_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--resume` combined with `--clobber` fails fast with a usage
    error, and the runner is never invoked.
    """
    with mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_RESUME, ARG_CLOBBER],
            color=False,
        )

    assert result.exit_code == 2
    stderr_flat = flatten_cli_output(result.stderr)
    assert "--resume" in stderr_flat
    assert "--clobber" in stderr_flat
    mock_exec.assert_not_called()


def test_blueprint_run_runner_error_exits_cleanly(
    hello_world_bp_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a `CstarError` raised while the runner executes is reported as a
    message with exit code 1, not a traceback.
    """
    error = "The runner could not start"
    with mock.patch.object(
        HelloWorldRunner, "execute", mock.AsyncMock(side_effect=CstarError(error))
    ):
        runner = CliRunner()
        result = runner.invoke(app, [hello_world_bp_path.as_posix()], color=False)

    assert result.exit_code == 1, result.output
    assert f"Unable to run the blueprint: {error}" in flatten_cli_output(result.output)
    assert not isinstance(result.exception, CstarError)


def test_blueprint_run_runner_construction_error_exits_cleanly(
    hello_world_bp_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify a `CstarError` raised while the runner is constructed (a launch
    refused on a login node is raised there) is reported as a message with exit
    code 1, not a traceback.
    """
    refusal = "Refusing to launch ROMS outside a scheduler job"
    with mock.patch.object(
        HelloWorldRunner, "__init__", mock.Mock(side_effect=CstarError(refusal))
    ):
        runner = CliRunner()
        result = runner.invoke(app, [hello_world_bp_path.as_posix()], color=False)

    assert result.exit_code == 1, result.output
    flat = flatten_cli_output(result.output)
    assert f"Unable to run the blueprint: {refusal}" in flat
    assert not isinstance(result.exception, CstarError)


def test_blueprint_run_resume_reaches_runner_request(
    complete_blueprint_path: Path,
) -> None:
    """Verify `--resume` against an application patched to declare itself
    resumable reaches the runner via a `RunnerRequest` with `resume=True`.
    """
    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    with (
        mock.patch.object(UclaRomsApplication, "resumable", True),
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            UclaRomsRunner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
        mock.patch(
            "cstar.cli.blueprint.run.RunnerRequest",
            wraps=RunnerRequest,
        ) as mock_request_cls,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_exec_runner.assert_called_once()
    mock_request_cls.assert_called_once()
    assert mock_request_cls.call_args.kwargs["resume"] is True


def test_blueprint_run_pre_run_reaches_runner_request(
    complete_blueprint_path: Path,
) -> None:
    """Verify `--pre-run` reaches the runner via a `RunnerRequest` with
    `pre_run=True`.
    """
    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            UclaRomsRunner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
        mock.patch(
            "cstar.cli.blueprint.run.RunnerRequest",
            wraps=RunnerRequest,
        ) as mock_request_cls,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_PRE_RUN],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_exec_runner.assert_called_once()
    mock_request_cls.assert_called_once()
    assert mock_request_cls.call_args.kwargs["pre_run"] is True


@pytest.mark.parametrize("clobber", [False, True])
def test_blueprint_run_clobber_reaches_runner_request(
    complete_blueprint_path: Path,
    clobber: bool,
) -> None:
    """Verify `--clobber` reaches the runner via a `RunnerRequest` with
    `clobber=True`, and its absence with `clobber=False`.

    Parameters
    ----------
    complete_blueprint_path : Path
        The path to a valid blueprint.
    clobber : bool
        Whether `--clobber` is passed.
    """
    mock_sim_instance = mock.Mock()
    mock_sim_instance.name = "test simulation"

    async def modify_runner(
        self: BlueprintRunner[UclaRomsBlueprint],
    ) -> RunnerResult[UclaRomsBlueprint]:
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    with (
        mock.patch.object(
            ROMSSimulation,
            "from_blueprint",
            return_value=mock_sim_instance,
        ),
        mock.patch.object(
            UclaRomsRunner,
            "execute",
            side_effect=modify_runner,
            autospec=True,
        ) as mock_exec_runner,
        mock.patch(
            "cstar.cli.blueprint.run.RunnerRequest",
            wraps=RunnerRequest,
        ) as mock_request_cls,
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), *([ARG_CLOBBER] * clobber)],
            color=False,
        )

    assert result.exit_code == 0, result.output
    mock_exec_runner.assert_called_once()
    mock_request_cls.assert_called_once()
    assert mock_request_cls.call_args.kwargs["clobber"] is clobber


@pytest.mark.parametrize("verbose", [False, True])
def test_blueprint_run_verbose_reaches_the_migration(
    complete_blueprint_path: Path,
    verbose: bool,
) -> None:
    """Verify `--verbose`, wherever it appears relative to the blueprint path,
    selects the detailed display of the blueprint's migration.

    Parameters
    ----------
    complete_blueprint_path : Path
        The path to a valid blueprint.
    verbose : bool
        Whether `--verbose` is passed (after the path).
    """
    with (
        mock.patch(
            "cstar.cli.blueprint.run.localize_and_migrate",
            return_value=(complete_blueprint_path, False),
        ) as mock_migrate,
        mock.patch.object(UclaRomsRunner, "execute", autospec=True),
        mock.patch.object(ROMSSimulation, "from_blueprint"),
    ):
        CliRunner().invoke(
            app,
            [complete_blueprint_path.as_posix(), *([ARG_VERBOSE] * verbose)],
            color=False,
        )

    mock_migrate.assert_called_once()
    assert mock_migrate.call_args.kwargs["verbose"] is verbose


def test_blueprint_run_pre_run_and_resume_fails_fast(
    complete_blueprint_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--pre-run` combined with `--resume` fails fast with a usage
    error, and the runner is never invoked.
    """
    with mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_PRE_RUN, ARG_RESUME],
            color=False,
        )

    assert result.exit_code == 2
    stderr_flat = flatten_cli_output(result.stderr)
    assert ARG_PRE_RUN in stderr_flat
    assert ARG_RESUME in stderr_flat
    mock_exec.assert_not_called()


def test_blueprint_run_pre_run_non_pre_runnable_app_fails_fast(
    complete_blueprint_path: Path,
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify `--pre-run` against an application that does not declare itself
    pre-runnable fails fast with a usage error naming the application, and the
    runner is never invoked.
    """
    with (
        mock.patch.object(UclaRomsRunner, "execute", mock.AsyncMock()) as mock_exec,
        mock.patch.object(UclaRomsApplication, "pre_runnable", False),
    ):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [complete_blueprint_path.as_posix(), ARG_PRE_RUN],
            color=False,
        )

    assert result.exit_code == 2
    stderr_flat = flatten_cli_output(result.stderr)
    assert "ucla_roms" in stderr_flat
    assert "does not support" in stderr_flat
    mock_exec.assert_not_called()


def _run_hello_world(bp_path: Path, *args: str) -> t.Any:
    """Run a hello_world blueprint through the CLI with a runner that shuts down."""

    async def execute(self: BlueprintRunner[Blueprint]) -> None:
        self.add_state(ExecutionStatus.COMPLETED)
        self._on_shutdown()

    with mock.patch.object(HelloWorldRunner, "execute", execute):
        return CliRunner().invoke(app, [bp_path.as_posix(), *args], color=False)


def test_blueprint_run_writes_run_manifest(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A standalone run leaves a step record and a one-step run manifest."""
    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)

    result = _run_hello_world(hello_world_bp_path)

    assert result.exit_code == 0, result.output
    bp = deserialize(hello_world_bp_path, HelloWorldBlueprint)
    root = bp.effective_working_dir
    assert (root / STEP_RECORD_NAME).exists()
    assert (root / RUN_MANIFEST_NAME).exists()

    manifest = read_run_manifest(root)
    assert manifest is not None
    assert manifest.kind == "blueprint"
    assert len(manifest.attempts) == 1
    assert manifest.attempts[0].status == AttemptStatus.done
    assert manifest.attempts[0].finished_at is not None
    assert len(manifest.steps) == 1
    entry = manifest.steps[0]
    assert entry.working_dir == "."
    assert entry.status == Status.Done.name
    assert entry.record is not None
    assert entry.record.outcome == "completed"


def test_blueprint_run_records_declared_products(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The step record lists the files the application's declarations match."""
    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
    root = deserialize(hello_world_bp_path, HelloWorldBlueprint).effective_working_dir
    (root / "logs").mkdir(parents=True, exist_ok=True)
    (root / "logs" / "a.out").write_bytes(b"hello")
    spec = ArtifactSpec(kind=ArtifactKind.log, pattern="logs/*.out")

    with mock.patch.object(HelloWorldApplication, "artifacts", return_value=[spec]):
        result = _run_hello_world(hello_world_bp_path)

    assert result.exit_code == 0, result.output
    record = read_step_record(root)
    assert record is not None
    assert [(p.kind, p.path, p.size_bytes) for p in record.products] == [
        ("log", "logs/a.out", 5)
    ]


def test_blueprint_run_keeps_the_record_when_declarations_fail(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declaration that raises costs the products, never the record."""
    monkeypatch.delenv(ENV_CSTAR_RUNID, raising=False)
    root = deserialize(hello_world_bp_path, HelloWorldBlueprint).effective_working_dir

    with mock.patch.object(
        HelloWorldApplication, "artifacts", side_effect=RuntimeError("bad")
    ):
        result = _run_hello_world(hello_world_bp_path)

    assert result.exit_code == 0, result.output
    record = read_step_record(root)
    assert record is not None
    assert record.products == []


def test_blueprint_run_with_step_writes_no_run_manifest(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A step of a workplan run is recorded by the orchestrator, not the CLI."""
    monkeypatch.setenv(ENV_CSTAR_RUNID, "a-run")

    result = _run_hello_world(hello_world_bp_path, ARG_STEP, "A")

    assert result.exit_code == 0, result.output
    root = deserialize(hello_world_bp_path, HelloWorldBlueprint).effective_working_dir
    assert (root / STEP_RECORD_NAME).exists()
    assert not (root / RUN_MANIFEST_NAME).exists()


def test_blueprint_run_step_option_names_the_recorded_step(
    hello_world_bp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--step` is what the step record names; without it, the blueprint is."""
    monkeypatch.setenv(ENV_CSTAR_RUNID, "a-run")
    root = deserialize(hello_world_bp_path, HelloWorldBlueprint).effective_working_dir

    result = _run_hello_world(hello_world_bp_path, ARG_STEP, "A")

    assert result.exit_code == 0, result.output
    record = read_step_record(root)
    assert record is not None
    assert record.step == "A"
