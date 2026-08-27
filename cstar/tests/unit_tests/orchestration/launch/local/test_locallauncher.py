import asyncio
import datetime
import subprocess
from pathlib import Path

import pytest

from cstar.orchestration.launch.local import (
    LocalHandle,
    LocalLauncher,
    ProxiedRunRequestFormatter,
)
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
