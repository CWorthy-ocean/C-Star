import asyncio
import contextlib
import datetime
import shlex
import subprocess
import time
import typing as t
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import psutil
import pytest
from psutil import NoSuchProcess

from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RESUME
from cstar.orchestration.launch.local import (
    LocalComputeSpec,
    LocalHandle,
    LocalLauncher,
    ProxiedRunRequestFormatter,
    TimeConstrainedRunRequestEnricher,
)
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.models import KEY_CLOBBER, KEY_PRE_RUN, KEY_RESUME, Step
from cstar.orchestration.orchestration import (
    LiveStep,
    RunRequest,
    RunRequestCommandFormatter,
    Status,
    Task,
    Workplan,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.state import StateRepository

WHICH = "cstar.orchestration.launch.local.shutil.which"
"""Where the timeout executable is looked up."""

T0 = datetime.datetime(2026, 10, 15, 12, 0, tzinfo=datetime.UTC)
"""When the prior attempt of the re-entry tests was submitted."""


@pytest.mark.parametrize(
    ("exit_code", "expected"),
    [
        pytest.param(0, Status.Done, id="clean exit"),
        pytest.param(3, Status.Failed, id="failure exit"),
    ],
)
async def test_locallauncher_query_status_observes_exit(
    exit_code: int,
    expected: Status,
) -> None:
    """Verify a process that exits on its own is reported as terminal.

    Popen only records an exit when it is polled; a status query that reads
    `returncode` without polling reports the task as running forever.
    """
    process = subprocess.Popen(["sh", "-c", f"exit {exit_code}"])
    handle = LocalHandle(
        pid=str(process.pid),
        name="observed",
        run_id="test-run",
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Running,
    )
    handle.process = process

    status = Status.Running
    for _ in range(100):
        status = await LocalLauncher.query_status(handle)
        if status != Status.Running:
            break
        await asyncio.sleep(0.05)

    assert status == expected


@pytest.mark.parametrize(
    ("dep_status", "exp_rc", "exp_status", "exp_ran"),
    [
        pytest.param(Status.Done, 0, Status.Done, True, id="done dependency runs"),
        pytest.param(
            Status.Failed, 1, Status.Failed, False, id="failed dependency aborts"
        ),
        pytest.param(
            Status.Running, 1, Status.Failed, False, id="vanished dependency aborts"
        ),
    ],
)
def test_proxy_script_propagates_dependency_outcome(
    tmp_path: Path,
    wp_templates_dir: Path,
    dep_status: Status,
    exp_rc: int,
    exp_status: Status,
    exp_ran: bool,
) -> None:
    """Verify the proxy script inspects a finished dependency's recorded outcome.

    A dependency whose process exited without reaching `Done` (it failed, or
    died before recording a terminal status) must abort the step instead of
    running it against incomplete upstream output.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    dep_process = subprocess.Popen(["sh", "-c", "exit 0"])
    dep_process.wait()
    dep_handle = LocalHandle(
        pid=str(dep_process.pid),
        name="dep",
        run_id="fake-run-id",
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=dep_status,
    )

    for name, status in (("dep", dep_status), (live_step.name, Status.Submitted)):
        sentinel = StateRepository.sentinel_path(name)
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text(f"name: {name}\nstatus: {status.value}\n")

    marker = tmp_path / "step-ran.txt"
    script = ProxiedRunRequestFormatter(live_step, [dep_handle]).format(
        RunRequest(command=["touch", str(marker)])
    )
    script_path = tmp_path / "script.sh"
    script_path.write_text(script)

    result = subprocess.run(["sh", str(script_path)], capture_output=True, timeout=30)

    assert result.returncode == exp_rc
    assert marker.exists() == exp_ran
    own_sentinel = StateRepository.sentinel_path(live_step.name)
    assert f"status: {exp_status.value}" in own_sentinel.read_text()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"local": {}}, id="empty local overrides"),
        pytest.param({}, id="no overrides"),
        pytest.param(
            {"slurm": {"num_cpus": 4}},
            id="foreign launcher overrides ignored",
        ),
    ],
)
def test_locallauncher_adapt_step_formatter_selection(
    wp_templates_dir: Path,
    overrides: dict[str, dict[str, str]],
) -> None:
    """Verify that the `LocalLauncher` converts a step into a proxied command script.

    The compute overrides are parameterized to ensure that empty overrides and not
    specifying overrides result in the same behavior.

    TODO: consider evaluating an empty overrides block as a configuration failure, instead.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={
            "compute_overrides": overrides,
        },
    )

    step_command = LocalLauncher.adapt_step(live_step, [])

    assert "update_status" in step_command

    # confirm that compute overrides are required to modify the command
    assert "timeout" not in step_command


@pytest.mark.parametrize(
    ("overrides", "exp_timeout", "exp_fk_timeout"),
    [
        pytest.param(
            {"local": {"max_walltime": "01:30"}},
            "90s",
            "2s",
            id="default fk-timeout",
        ),
        pytest.param(
            {"local": {"force_kill_timeout": "00:00:05"}},
            "600s",
            "5s",
            id="default duration",
        ),
        pytest.param(
            {"local": {"max_walltime": "00:01:00", "force_kill_timeout": "00:00:42"}},
            "60s",
            "42s",
            id="no defaults",
        ),
        pytest.param(
            {"local": {"max_walltime": "00:60:00", "force_kill_timeout": "00:00:00"}},
            "3600s",
            "0s",
            id="zero fk-timeout",
        ),
    ],
)
def test_locallauncher_adapt_step_with_compute_overrides(
    wp_templates_dir: Path,
    overrides: dict[str, dict[str, str]],
    exp_timeout: str,
    exp_fk_timeout: str,
) -> None:
    """Verify that the `LocalLauncher.adapt` results in the appropriate change
    to the underlying command and user-specified overrides are honored.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={
            "compute_overrides": overrides,
        },
    )

    with mock.patch(
        WHICH, side_effect=lambda exe: f"/usr/bin/{exe}" if exe == "timeout" else None
    ):
        step_command = LocalLauncher.adapt_step(live_step, [])

    # confirm that compute overrides are required to modify the command
    assert f"timeout -k {exp_fk_timeout} {exp_timeout}" in step_command


@pytest.mark.parametrize(
    ("available", "expected"),
    [
        pytest.param({"timeout": "/bin/timeout"}, "/bin/timeout", id="timeout"),
        pytest.param({"gtimeout": "/brew/gtimeout"}, "/brew/gtimeout", id="gtimeout"),
        pytest.param(
            {"timeout": "/bin/timeout", "gtimeout": "/brew/gtimeout"},
            "/bin/timeout",
            id="timeout preferred",
        ),
    ],
)
def test_timeout_enricher_uses_resolved_executable(
    available: dict[str, str], expected: str
) -> None:
    """Verify the enricher prefixes the command with whichever executable resolves."""
    enricher = TimeConstrainedRunRequestEnricher(LocalComputeSpec())

    with mock.patch(WHICH, side_effect=available.get):
        request = enricher.enrich(RunRequest(command=["echo", "hi"]))

    assert request.command[:2] == [expected, "-k"]
    assert request.command[-2:] == ["echo", "hi"]


def test_timeout_enricher_executable_path_survives_rendering() -> None:
    """Verify a path with a space survives the quoting of the rendered command."""
    enricher = TimeConstrainedRunRequestEnricher(LocalComputeSpec())
    path = "/Users/First Last/envs/cstar-env/bin/timeout"

    with mock.patch(WHICH, side_effect={"timeout": path}.get):
        request = enricher.enrich(RunRequest(command=["echo", "hi"]))

    assert request.command[0] == path
    rendered = RunRequestCommandFormatter().format(request)
    assert shlex.split(rendered)[0] == path


def test_timeout_enricher_raises_without_executable() -> None:
    """Verify the enricher refuses to build a command when no timeout resolves."""
    enricher = TimeConstrainedRunRequestEnricher(LocalComputeSpec())

    with (
        mock.patch(WHICH, return_value=None),
        pytest.raises(CstarExpectationFailed, match="coreutils"),
    ):
        enricher.enrich(RunRequest(command=["echo", "hi"]))


def _workplan(overrides_by_step: dict[str, dict[str, dict[str, str]]]) -> Workplan:
    """Build a workplan whose steps carry the given compute overrides."""
    return Workplan(
        name="timeout-check",
        description="steps with and without a local walltime",
        steps=[
            Step(
                name=name,
                application="hello_world",
                blueprint="blueprint.yaml",
                compute_overrides=overrides,
            )
            for name, overrides in overrides_by_step.items()
        ],
    )


WALLTIME = {"local": {"max_walltime": "00:05:00"}}


def test_check_preconditions_names_every_walltime_step() -> None:
    """Verify a missing timeout is reported once, naming all steps that need it."""
    workplan = _workplan({"a": WALLTIME, "b": {}, "c": WALLTIME})

    with (
        mock.patch(WHICH, return_value=None),
        pytest.raises(CstarExpectationFailed) as exc_info,
    ):
        LocalLauncher.check_preconditions(workplan)

    msg = str(exc_info.value)
    assert "'a'" in msg
    assert "'b'" not in msg
    assert "'c'" in msg
    assert "coreutils" in msg


def test_check_preconditions_names_force_kill_only_step() -> None:
    """Verify a step overriding only the grace period is named: it gets the default walltime."""
    workplan = _workplan(
        {"kill-only": {"local": {"force_kill_timeout": "00:00:05"}}, "none": {}}
    )

    with (
        mock.patch(WHICH, return_value=None),
        pytest.raises(CstarExpectationFailed, match="'kill-only'") as exc_info,
    ):
        LocalLauncher.check_preconditions(workplan)

    assert "'none'" not in str(exc_info.value)


def test_check_preconditions_ignores_malformed_local_overrides() -> None:
    """Verify a step whose local overrides are invalid is not named: it runs without timeout."""
    workplan = _workplan({"bad": {"local": {"max_walltime": "not-a-time"}}})

    with mock.patch(WHICH, return_value=None):
        LocalLauncher.check_preconditions(workplan)


@pytest.mark.parametrize(
    ("overrides_by_step", "available"),
    [
        pytest.param({"a": WALLTIME}, {"gtimeout": "/brew/gtimeout"}, id="resolves"),
        pytest.param({"a": {}, "b": {"slurm": {"num_cpus": 4}}}, {}, id="no walltime"),
    ],
)
def test_check_preconditions_passes(
    overrides_by_step: dict[str, dict[str, dict[str, str]]],
    available: dict[str, str],
) -> None:
    """Verify no error when a timeout resolves or no step needs one."""
    with mock.patch(WHICH, side_effect=available.get):
        LocalLauncher.check_preconditions(_workplan(overrides_by_step))


def test_check_preconditions_without_workplan() -> None:
    """Verify the absence of a workplan is a no-op."""
    with mock.patch(WHICH, return_value=None):
        LocalLauncher.check_preconditions(None)


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_reuses_done_prior_handle(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a prior handle that terminated successfully is adopted instead
    of resubmitting the step.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    prior_handle = LocalHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Done,
    )
    await StateRepository().put_sentinel(prior_handle)

    with mock.patch.object(LocalLauncher, "_submit", mock.AsyncMock()) as mock_submit:
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    mock_submit.assert_not_awaited()
    assert task.handle.pid == prior_handle.pid
    assert task.handle.status == Status.Done


@pytest.mark.parametrize(
    ("prior_uuid", "adopted"),
    [
        pytest.param("earlier", False, id="another-run"),
        pytest.param("this", True, id="this-run"),
        pytest.param("", True, id="sentinel-before-uuids"),
    ],
)
@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_ignores_the_sentinel_of_another_run(
    wp_templates_dir: Path,
    mock_run_id: str,
    prior_uuid: str,
    adopted: bool,
) -> None:
    """Verify an expired Done prior handle recording another run's manifest uuid
    is not adopted and the step is submitted again; a handle of this run, or
    one whose run cannot be told, is adopted.
    """
    workplan = deserialize(wp_templates_dir / "single_step.yaml", Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])
    prior_handle = LocalHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Done,
        run_uuid=prior_uuid,
    )
    await StateRepository().put_sentinel(prior_handle)
    new_handle = LocalHandle(
        pid="999",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Submitted,
    )

    with mock.patch.object(
        LocalLauncher, "_submit", mock.AsyncMock(return_value=new_handle)
    ) as mock_submit:
        task = await LocalLauncher.launch(live_step, [], run_uuid="this")

    assert mock_submit.await_count == (0 if adopted else 1)
    assert task.handle.pid == ("12345" if adopted else "999")
    if not adopted:
        # a stale pre-run or failure must not steer the new submission
        assert mock_submit.await_args is not None
        assert mock_submit.await_args.kwargs["after_pre_run"] is False
        assert KEY_CLOBBER not in mock_submit.await_args.args[0].workflow_overrides


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_resubmits_failed_prior_handle_with_clobber(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a failed prior handle causes a resubmission with clobber set,
    when the step is not marked for resume.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    prior_handle = LocalHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Failed,
    )
    await StateRepository().put_sentinel(prior_handle)

    fake_new_handle = LocalHandle(
        pid="999",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Submitted,
    )
    with mock.patch.object(
        LocalLauncher, "_submit", mock.AsyncMock(return_value=fake_new_handle)
    ) as mock_submit:
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    mock_submit.assert_awaited_once()
    assert mock_submit.await_args is not None
    submitted_step = mock_submit.await_args.args[0]
    assert submitted_step.workflow_overrides.get(KEY_CLOBBER, False) is True
    assert task.handle is fake_new_handle


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_failed_prior_with_resume_no_clobber(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a failed prior handle for a step marked for resume is resubmitted
    without clobber, and the rendered command includes `--resume`.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={"workflow_overrides": {KEY_RESUME: True}},
    )

    prior_handle = LocalHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Failed,
    )
    await StateRepository().put_sentinel(prior_handle)

    fake_process = mock.Mock(pid=999999)
    with mock.patch.object(subprocess, "Popen", return_value=fake_process):
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    submitted_step = task.step
    assert submitted_step.workflow_overrides.get(KEY_CLOBBER, False) is False
    assert ARG_RESUME in submitted_step.script_path.read_text()


@pytest.mark.parametrize("pre_run", [True, False])
@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_submit_records_pre_run_on_handle(
    wp_templates_dir: Path,
    mock_run_id: str,
    pre_run: bool,
) -> None:
    """Verify the submitted handle records whether the step ran in pre-run mode,
    and that the flag survives a round-trip through the sentinel file.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={"workflow_overrides": {KEY_PRE_RUN: True} if pre_run else {}},
    )

    fake_process = mock.Mock(pid=777777)
    with mock.patch.object(subprocess, "Popen", return_value=fake_process):
        handle = await LocalLauncher._submit(live_step, [])

    assert handle.pre_run is pre_run
    assert handle.launcher_name == LocalLauncher.name
    assert (ARG_PRE_RUN in live_step.script_path.read_text()) is pre_run

    await StateRepository().put_sentinel(handle)
    reloaded = await StateRepository().get_sentinel(live_step.name, LocalHandle)
    assert reloaded is not None
    assert reloaded.pre_run is pre_run
    assert reloaded.launcher_name == LocalLauncher.name


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_submit_records_when_the_process_was_created(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify the submission is the process creation time the launcher reads,
    and survives the sentinel as a time, whatever form `start_at` takes later.
    """
    live_step = LiveStep.from_step(
        deserialize(wp_templates_dir / "single_step.yaml", Workplan).steps[0]
    )
    created = 1_800_000_000.5

    with (
        mock.patch.object(subprocess, "Popen", return_value=mock.Mock(pid=777777)),
        mock.patch(
            "cstar.orchestration.launch.local.PsProcess",
            return_value=mock.Mock(create_time=mock.Mock(return_value=created)),
        ),
    ):
        handle = await LocalLauncher._submit(live_step, [])

    expected = datetime.datetime.fromtimestamp(created, tz=datetime.UTC)
    assert handle.submitted_at == expected
    assert handle.start_ts == created  # turns `start_at` into a float, not the stamp
    assert handle.submitted_at == expected

    await StateRepository().put_sentinel(handle)
    reloaded = await StateRepository().get_sentinel(live_step.name, LocalHandle)
    assert reloaded is not None
    assert reloaded.submitted_at == expected


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_submit_falls_back_to_now_for_an_unknown_process(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a process that exits before its creation time is read is submitted
    at the moment it was started.
    """
    live_step = LiveStep.from_step(
        deserialize(wp_templates_dir / "single_step.yaml", Workplan).steps[0]
    )

    before = datetime.datetime.now(tz=datetime.UTC)
    with (
        mock.patch.object(subprocess, "Popen", return_value=mock.Mock(pid=777777)),
        mock.patch(
            "cstar.orchestration.launch.local.PsProcess",
            side_effect=NoSuchProcess(777777),
        ),
    ):
        handle = await LocalLauncher._submit(live_step, [])
    after = datetime.datetime.now(tz=datetime.UTC)

    assert handle.submitted_at is not None
    assert before <= handle.submitted_at <= after


@pytest.mark.parametrize(
    "recorded",
    [datetime.datetime(2026, 10, 15, 12, 0, 3, 123456, tzinfo=datetime.UTC), None],
    ids=["recorded", "sentinel-before-submission-times"],
)
@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_adopts_the_submission_of_a_prior_handle(
    wp_templates_dir: Path,
    mock_run_id: str,
    recorded: datetime.datetime | None,
) -> None:
    """Verify an adopted handle keeps the submission time it was persisted with,
    or none: the launcher stamps nothing when it submits nothing.
    """
    live_step = LiveStep.from_step(
        deserialize(wp_templates_dir / "single_step.yaml", Workplan).steps[0]
    )
    await StateRepository().put_sentinel(
        LocalHandle(
            pid="12345",
            name=live_step.name,
            run_id=mock_run_id,
            launcher_name=LocalLauncher.name,
            start_at=datetime.datetime.now(tz=datetime.UTC),
            status=Status.Done,
            submitted_at=recorded,
        )
    )

    with mock.patch.object(LocalLauncher, "_submit", mock.AsyncMock()) as mock_submit:
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    mock_submit.assert_not_awaited()
    assert task.handle.pid == "12345"
    assert task.handle.submitted_at == recorded


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_attaches_to_done_pre_run(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a completed pre-run prior handle leads a real (non pre-run) launch
    to resubmit the step for resume, without clobber, so it attaches to the
    prepared working directory.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    prior_handle = LocalHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Done,
        pre_run=True,
    )
    await StateRepository().put_sentinel(prior_handle)

    fake_process = mock.Mock(pid=999999)
    with mock.patch.object(subprocess, "Popen", return_value=fake_process):
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    assert task.handle.pid != prior_handle.pid
    assert task.handle.pre_run is False
    assert task.step.workflow_overrides.get(KEY_CLOBBER, False) is False
    script = task.step.script_path.read_text()
    assert ARG_RESUME in script
    assert ARG_PRE_RUN not in script


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_submit_rotates_prior_log(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a prior log is rotated to `.1` and the fresh log's first line
    mentions it, when a step is (re)submitted.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    live_step.fsm.prepare()
    live_step.log_path.write_text("prior attempt output\n")

    fake_process = mock.Mock(pid=888888)
    with mock.patch.object(subprocess, "Popen", return_value=fake_process):
        await LocalLauncher._submit(live_step, [])

    rotated = live_step.log_path.with_name(f"{live_step.log_path.name}.1")
    assert rotated.exists()
    assert rotated.read_text() == "prior attempt output\n"

    new_content = live_step.log_path.read_text()
    assert rotated.name in new_content.splitlines()[0]


@pytest.mark.parametrize(
    ("persisted", "alive", "expected"),
    [
        pytest.param(Status.Running, True, "RUNNING", id="process_alive"),
        pytest.param(Status.Running, False, "FAILED", id="process_gone"),
        pytest.param(Status.Submitted, True, "PENDING", id="queued_process_alive"),
        pytest.param(Status.Submitted, False, "FAILED", id="queued_process_gone"),
    ],
)
async def test_status_expired_nonterminal_handle_checks_process_liveness(
    mock_run_id: str, persisted: Status, alive: bool, expected: str
) -> None:
    """A deserialized handle whose sentinel was never finalized is RUNNING only
    while its OS process is alive, or PENDING if it was persisted as submitted
    (its proxy has not yet recorded that it is running); a dead process is
    reported as FAILED so the step is re-run instead of adopted forever.
    """
    handle = LocalHandle(
        pid="12345",
        name="step",
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=persisted,
    )

    with mock.patch.object(LocalLauncher, "_is_alive", return_value=alive):
        assert await LocalLauncher._status(handle) == expected


@pytest.mark.parametrize(
    ("persisted", "expected"),
    [
        pytest.param(Status.Submitted, Status.Submitted, id="queued"),
        pytest.param(Status.Running, Status.Running, id="running"),
    ],
)
async def test_query_status_of_a_deserialized_handle_with_a_live_process(
    mock_run_id: str, persisted: Status, expected: Status
) -> None:
    """Verify a step whose proxy is alive but still waiting on its dependencies
    reads as Submitted, so a launcher can tell it from one that is running.
    """
    handle = LocalHandle(
        pid="12345",
        name="step",
        run_id=mock_run_id,
        launcher_name=LocalLauncher.name,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=persisted,
    )

    with mock.patch.object(LocalLauncher, "_is_alive", return_value=True):
        assert await LocalLauncher.query_status(handle) is expected


def test_is_alive_matches_pid_and_start_time(mock_run_id: str) -> None:
    """`_is_alive` requires a live, non-zombie process whose creation time
    matches the handle, so a recycled PID is not mistaken for the step.
    """
    start = datetime.datetime.now(tz=datetime.UTC)
    handle = LocalHandle(
        pid="12345",
        name="step",
        run_id=mock_run_id,
        start_at=start,
        status=Status.Running,
    )

    ps_process = mock.Mock()
    ps_process.status.return_value = "running"
    ps_process.create_time.return_value = start.timestamp()
    with mock.patch(
        "cstar.orchestration.launch.local.PsProcess", return_value=ps_process
    ):
        assert LocalLauncher._is_alive(handle) is True

    ps_process.create_time.return_value = start.timestamp() + 3600
    with mock.patch(
        "cstar.orchestration.launch.local.PsProcess", return_value=ps_process
    ):
        assert LocalLauncher._is_alive(handle) is False

    with mock.patch(
        "cstar.orchestration.launch.local.PsProcess", side_effect=NoSuchProcess(12345)
    ):
        assert LocalLauncher._is_alive(handle) is False


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_clobbers_failed_slurm_sentinel_for_pre_run(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a pre-run reads a failed SLURM attempt's sentinel using its
    persisted status (a SLURM job id is no local pid) and relaunches the step
    clobbered.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={"workflow_overrides": {KEY_PRE_RUN: True}},
    )

    prior_handle = SlurmHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        launcher_name=SlurmLauncher.name,
        status=Status.Failed,
    )
    await StateRepository().put_sentinel(prior_handle)

    fake_process = mock.Mock(pid=999999)
    with (
        mock.patch.object(subprocess, "Popen", return_value=fake_process),
        mock.patch.object(LocalLauncher, "_status", mock.AsyncMock()) as mock_status,
    ):
        task = await LocalLauncher.launch(live_step, [], run_uuid="")

    mock_status.assert_not_awaited()
    assert task.handle.pid != prior_handle.pid
    assert task.handle.launcher_name == LocalLauncher.name
    script = task.step.script_path.read_text()
    assert ARG_CLOBBER in script
    assert ARG_PRE_RUN in script


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_refuses_running_slurm_sentinel(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a pre-run neither adopts nor duplicates a SLURM attempt that is
    still in progress.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={"workflow_overrides": {KEY_PRE_RUN: True}},
    )

    prior_handle = SlurmHandle(
        pid="12345",
        name=live_step.name,
        run_id=mock_run_id,
        launcher_name=SlurmLauncher.name,
        status=Status.Running,
    )
    await StateRepository().put_sentinel(prior_handle)

    with (
        mock.patch.object(LocalLauncher, "_submit", mock.AsyncMock()) as mock_submit,
        pytest.raises(CstarExpectationFailed, match="another launcher"),
    ):
        await LocalLauncher.launch(live_step, [], run_uuid="")

    mock_submit.assert_not_awaited()


@pytest.mark.parametrize(
    ("launcher_name", "run_id", "same_run"),
    [
        pytest.param("local", "spinup", False, id="local step of another run"),
        pytest.param("slurm", "", True, id="slurm job of this run"),
    ],
)
@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_refuses_dependency_it_cannot_wait_on(
    wp_templates_dir: Path,
    mock_run_id: str,
    launcher_name: str,
    run_id: str,
    same_run: bool,
) -> None:
    """Verify the local launcher refuses to wait on a handle created by another
    run or by another launcher, rather than waiting on an unrelated pid.
    """
    workplan = deserialize(wp_templates_dir / "single_step.yaml", Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])
    foreign = LocalHandle(
        pid="12345",
        name="outer",
        run_id=mock_run_id if same_run else run_id,
        launcher_name=launcher_name,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Running,
    )

    with (
        mock.patch.object(LocalLauncher, "_submit", mock.AsyncMock()) as mock_submit,
        pytest.raises(CstarError, match="another run"),
    ):
        await LocalLauncher.launch(live_step, [foreign], run_uuid="")

    mock_submit.assert_not_awaited()


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_locallauncher_launch_waits_on_dependency_of_this_run(
    wp_templates_dir: Path,
    mock_run_id: str,
) -> None:
    """Verify a dependency created by this run's local launcher is still accepted."""
    workplan = deserialize(wp_templates_dir / "single_step.yaml", Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])
    dependency = LocalHandle(
        pid="12345",
        name="outer",
        run_id=mock_run_id,
        launcher_name=LocalLauncher.name,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Running,
    )
    submitted = LocalHandle(pid="777", name=live_step.name, run_id=mock_run_id)

    with (
        mock.patch.object(
            LocalLauncher, "query_status", mock.AsyncMock(return_value=Status.Running)
        ),
        mock.patch.object(
            LocalLauncher, "_submit", mock.AsyncMock(return_value=submitted)
        ) as mock_submit,
    ):
        task = await LocalLauncher.launch(live_step, [dependency], run_uuid="")

    assert task.handle.pid == "777"
    assert mock_submit.await_args is not None
    assert mock_submit.await_args.args[1] == [dependency]


def test_localhandle_start_ts_requires_start_time(mock_run_id: str) -> None:
    """A handle deserialized from another launcher's sentinel has no start
    time, and must not report a made-up one.
    """
    handle = LocalHandle(pid="12345", name="step", run_id=mock_run_id)

    assert handle.start_at is None
    with pytest.raises(CstarExpectationFailed, match="no start time"):
        _ = handle.start_ts


@pytest.fixture
def quick_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the grace a cancelled step's processes get, and the wait for a
    cancelled prior attempt to end.
    """
    monkeypatch.setattr("cstar.orchestration.launch.local.CANCEL_GRACE", 2)
    monkeypatch.setattr("cstar.orchestration.launch.common.CANCEL_TIMEOUT", 0)
    monkeypatch.setattr("cstar.orchestration.launch.common.CANCEL_POLL_DELAY", 0)


@pytest.fixture
def single_step(wp_templates_dir: Path, read_yaml_intercept: None) -> LiveStep:
    """A step whose blueprint exists."""
    return LiveStep.from_step(
        deserialize(wp_templates_dir / "single_step.yaml", Workplan).steps[0]
    )


@pytest.fixture
def process_tree() -> Iterator[tuple[subprocess.Popen[bytes], int]]:
    """A shell and the `sleep` it started, both running until the test ends."""
    shell = subprocess.Popen(["sh", "-c", "sleep 60 & wait"])
    sleeper = 0
    try:
        for _ in range(100):
            if children := psutil.Process(shell.pid).children():
                sleeper = children[0].pid
                break
            time.sleep(0.05)
        assert sleeper, "the shell did not start its child"
        yield shell, sleeper
    finally:
        with contextlib.suppress(NoSuchProcess):
            psutil.Process(sleeper).kill()
        shell.kill()
        shell.wait()


def _is_gone(pid: int) -> bool:
    """Report whether a process has exited, though its parent may not have
    collected it yet.
    """
    try:
        return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except NoSuchProcess:
        return True


def _expired_handle(run_id: str, *, status: Status = Status.Running) -> LocalHandle:
    """Build a handle as it is read from a sentinel, of a process no
    one has looked up.
    """
    return LocalHandle(
        pid="12345",
        name="step",
        run_id=run_id,
        launcher_name=LocalLauncher.name,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=status,
        submitted_at=T0,
    )


def _handle_of(
    shell: subprocess.Popen[bytes],
    run_id: str,
    *,
    status: Status = Status.Running,
    live: bool = False,
) -> LocalHandle:
    """Build the handle of a shell, as the launcher would have: with its
    creation time, and the `Popen` only when `live`; one read from a sentinel
    has none.
    """
    created = psutil.Process(shell.pid).create_time()
    handle = LocalHandle(
        pid=str(shell.pid),
        name="step",
        run_id=run_id,
        launcher_name=LocalLauncher.name,
        start_at=datetime.datetime.fromtimestamp(created, tz=datetime.UTC),
        status=status,
    )
    if live:
        handle.process = shell
        handle.status = status
    return handle


@pytest.mark.parametrize("live", [False, True], ids=["from-sentinel", "live-popen"])
async def test_locallauncher_cancel_stops_the_proxy_and_what_it_started(
    single_step: LiveStep,
    mock_run_id: str,
    process_tree: tuple[subprocess.Popen[bytes], int],
    quick_cancel: None,
    live: bool,
) -> None:
    """Verify cancelling a step ends its proxy and every process the proxy
    started, whether the handle was created by this process or deserialized
    from a sentinel, and marks the task cancelled.
    """
    shell, sleeper = process_tree
    handle = _handle_of(shell, mock_run_id, live=live)
    assert handle.is_expired is not live

    task = await LocalLauncher.cancel(Task(step=single_step, handle=handle))

    assert task.status is Status.Cancelled
    assert _is_gone(shell.pid)
    assert _is_gone(sleeper)
    if live:
        assert shell.returncode is not None  # reaped through the Popen


@pytest.mark.usefixtures("quick_cancel")
async def test_locallauncher_cancel_leaves_a_finished_live_process_alone(
    single_step: LiveStep, mock_run_id: str
) -> None:
    """Verify nothing is terminated, and the status stays, for a task whose
    process this launcher started and has since exited.
    """
    shell = subprocess.Popen(["sh", "-c", "exit 0"])
    shell.wait()
    handle = _expired_handle(mock_run_id)
    handle.pid = str(shell.pid)
    handle.process = shell
    assert not handle.is_expired

    with mock.patch.object(LocalLauncher, "_terminate") as terminate:
        task = await LocalLauncher.cancel(Task(step=single_step, handle=handle))

    terminate.assert_not_called()
    assert task.status is Status.Submitted


async def test_locallauncher_cancel_of_a_process_that_is_gone(
    single_step: LiveStep, mock_run_id: str
) -> None:
    """Verify a process that is no longer running cannot be stopped, so the
    task is not marked cancelled.
    """
    handle = LocalHandle(
        pid="12345",
        name="step",
        run_id=mock_run_id,
        launcher_name=LocalLauncher.name,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Running,
    )

    with mock.patch.object(LocalLauncher, "_process_of", return_value=None):
        assert LocalLauncher._terminate(handle) is False
        task = await LocalLauncher.cancel(Task(step=single_step, handle=handle))

    assert task.status is Status.Running


def test_locallauncher_terminate_stops_the_proxy_before_its_children(
    mock_run_id: str,
) -> None:
    """Verify the proxy is suspended, its descendants listed, and it is killed
    before any descendant is signalled: they exiting first would let it record
    the step failed in the sentinel.
    """
    manager = mock.Mock()
    proxy, first, second = manager.proxy, manager.first, manager.second
    proxy.children.return_value = [first, second]
    manager.wait_procs.return_value = ([first, second], [])

    with (
        mock.patch.object(LocalLauncher, "_process_of", return_value=proxy),
        mock.patch("cstar.orchestration.launch.local.wait_procs", manager.wait_procs),
    ):
        stopped = LocalLauncher._terminate(_expired_handle(mock_run_id))

    assert stopped is True
    proxy.children.assert_called_once_with(recursive=True)
    assert [c[0] for c in manager.mock_calls] == [
        "proxy.suspend",
        "proxy.children",
        "proxy.kill",
        "first.terminate",
        "second.terminate",
        "wait_procs",
        "wait_procs",
    ]


@pytest.mark.parametrize("survives_kill", [False, True])
def test_locallauncher_terminate_kills_a_child_that_ignores_terminate(
    mock_run_id: str, survives_kill: bool
) -> None:
    """Verify a descendant still running after the grace period is killed, and
    the step is reported stopped only once it has exited.
    """
    manager = mock.Mock()
    proxy, stubborn = manager.proxy, manager.stubborn
    proxy.children.return_value = [stubborn]
    manager.wait_procs.side_effect = [
        ([], [stubborn]),
        ([], [stubborn]) if survives_kill else ([stubborn], []),
    ]

    with (
        mock.patch.object(LocalLauncher, "_process_of", return_value=proxy),
        mock.patch("cstar.orchestration.launch.local.wait_procs", manager.wait_procs),
    ):
        stopped = LocalLauncher._terminate(_expired_handle(mock_run_id))

    stubborn.kill.assert_called_once_with()
    assert stopped is not survives_kill


@pytest.mark.usefixtures("quick_cancel")
async def test_locallauncher_launch_cancels_the_prior_process_of_a_clobbered_step(
    single_step: LiveStep, mock_run_id: str
) -> None:
    """Verify a clobbered step whose prior process is still running has it
    stopped first, then is submitted as a new process.
    """
    single_step.workflow_overrides[KEY_CLOBBER] = True
    prior = _expired_handle(mock_run_id)
    prior.name = single_step.name
    await StateRepository().put_sentinel(prior)
    events: list[str] = []

    def terminate(handle: LocalHandle) -> bool:
        events.append("terminate")
        return True

    def popen(*args: t.Any, **kwargs: t.Any) -> t.Any:
        events.append("submit")
        return mock.Mock(pid=999999)

    with (
        mock.patch.object(LocalLauncher, "_is_alive", return_value=True),
        mock.patch.object(
            LocalLauncher, "_terminate", mock.Mock(side_effect=terminate)
        ) as mock_terminate,
        mock.patch.object(subprocess, "Popen", side_effect=popen),
    ):
        task = await LocalLauncher.launch(single_step, [], run_uuid="")

    mock_terminate.assert_called_once()
    assert mock_terminate.call_args.args[0].pid == prior.pid
    assert events == ["terminate", "submit"]
    assert task.handle.pid == "999999"


@pytest.mark.usefixtures("quick_cancel")
async def test_locallauncher_launch_refuses_when_the_prior_process_survives(
    single_step: LiveStep, mock_run_id: str
) -> None:
    """Verify a clobbered step is not submitted while its prior process cannot
    be stopped, and the process is named.
    """
    single_step.workflow_overrides[KEY_CLOBBER] = True
    prior = _expired_handle(mock_run_id)
    prior.name = single_step.name
    await StateRepository().put_sentinel(prior)

    with (
        mock.patch.object(LocalLauncher, "_is_alive", return_value=True),
        mock.patch.object(LocalLauncher, "_terminate", return_value=False),
        mock.patch.object(subprocess, "Popen") as popen,
        pytest.raises(CstarExpectationFailed, match="12345"),
    ):
        await LocalLauncher.launch(single_step, [], run_uuid="")

    popen.assert_not_called()


@pytest.mark.parametrize(
    ("dependency_at", "requeued"),
    [
        pytest.param(T0 + datetime.timedelta(hours=1), True, id="newer-dependency"),
        pytest.param(T0 - datetime.timedelta(hours=1), False, id="older-dependency"),
    ],
)
@pytest.mark.usefixtures("quick_cancel")
async def test_locallauncher_launch_requeues_a_process_behind_a_replaced_dependency(
    single_step: LiveStep,
    mock_run_id: str,
    dependency_at: datetime.datetime,
    requeued: bool,
) -> None:
    """Verify a prior process still waiting on its dependencies is stopped and
    submitted again, though the step is not clobbered, when a dependency was
    submitted after it, and is adopted when none was.
    """
    prior = _expired_handle(mock_run_id, status=Status.Submitted)
    prior.name = single_step.name
    await StateRepository().put_sentinel(prior)
    dependency = _expired_handle(mock_run_id, status=Status.Submitted)
    dependency.pid = "555"
    dependency.name = "upstream"
    dependency.submitted_at = dependency_at

    with (
        mock.patch.object(LocalLauncher, "_is_alive", return_value=True),
        mock.patch.object(
            LocalLauncher, "_terminate", return_value=True
        ) as mock_terminate,
        mock.patch.object(
            subprocess, "Popen", return_value=mock.Mock(pid=999999)
        ) as popen,
    ):
        task = await LocalLauncher.launch(single_step, [dependency], run_uuid="")

    assert not single_step.clobber
    if requeued:
        mock_terminate.assert_called_once()
        assert mock_terminate.call_args.args[0].pid == prior.pid
        popen.assert_called_once()
        assert task.handle.pid == "999999"
        assert '"555"' in task.step.script_path.read_text()
    else:
        mock_terminate.assert_not_called()
        popen.assert_not_called()
        assert task.handle.pid == prior.pid
