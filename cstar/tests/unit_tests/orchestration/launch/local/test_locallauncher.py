import asyncio
import datetime
import subprocess
from pathlib import Path
from unittest import mock

import pytest
from psutil import NoSuchProcess

from cstar.entrypoint.utils import ARG_RESUME
from cstar.orchestration.launch.local import (
    LocalHandle,
    LocalLauncher,
    ProxiedRunRequestFormatter,
)
from cstar.orchestration.models import KEY_CLOBBER, KEY_RESUME
from cstar.orchestration.orchestration import LiveStep, RunRequest, Status, Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.state import StateRepository


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

    step_command = LocalLauncher.adapt_step(live_step, [])

    # confirm that compute overrides are required to modify the command
    assert f"timeout {exp_timeout} -k {exp_fk_timeout}" in step_command


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
        task = await LocalLauncher.launch(live_step, [])

    mock_submit.assert_not_awaited()
    assert task.handle.pid == prior_handle.pid
    assert task.handle.status == Status.Done


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
        task = await LocalLauncher.launch(live_step, [])

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
        task = await LocalLauncher.launch(live_step, [])

    submitted_step = task.step
    assert submitted_step.workflow_overrides.get(KEY_CLOBBER, False) is False
    assert ARG_RESUME in submitted_step.script_path.read_text()


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
    ("alive", "expected"),
    [
        pytest.param(True, "RUNNING", id="process_alive"),
        pytest.param(False, "FAILED", id="process_gone"),
    ],
)
async def test_status_expired_nonterminal_handle_checks_process_liveness(
    mock_run_id: str, alive: bool, expected: str
) -> None:
    """A deserialized handle whose sentinel was never finalized is RUNNING only
    while its OS process is alive; a dead process is reported as FAILED so the
    step is re-run instead of adopted forever.
    """
    handle = LocalHandle(
        pid="12345",
        name="step",
        run_id=mock_run_id,
        start_at=datetime.datetime.now(tz=datetime.UTC),
        status=Status.Running,
    )

    with mock.patch.object(LocalLauncher, "_is_alive", return_value=alive):
        assert await LocalLauncher._status(handle) == expected


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
