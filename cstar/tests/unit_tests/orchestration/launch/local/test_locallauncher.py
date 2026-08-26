import asyncio
import datetime
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.orchestration import LiveStep, Status, Workplan
from cstar.orchestration.serialization import deserialize


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
    ("use_proxy", "overrides"),
    [
        pytest.param(True, {"local": {}}, id="Proxied; empty local overrides"),
        pytest.param(True, {}, id="Proxied; no overrides"),
        pytest.param(False, {"local": {}}, id="Unproxied; empty local overrides"),
        pytest.param(False, {}, id="Proxied; no overrides"),
    ],
)
def test_locallauncher_adapt_step_formatter_selection(
    wp_templates_dir: Path,
    use_proxy: bool,
    overrides: dict[str, dict[str, str]],
) -> None:
    """Verify that the `LocalLauncher` correctly converts a step into a proxied (or
    non-proxied) command script based on the value of `LocalLauncher.use_proxy`.

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

    with mock.patch.object(
        LocalLauncher, "use_proxy", mock.PropertyMock(return_value=use_proxy)
    ):
        step_command = LocalLauncher.adapt_step(live_step, [])

    if use_proxy:
        assert "update_status" in step_command
    else:
        # confirm the command does not contain proxy-specific script elements
        assert "update_status" not in step_command

    # confirm that compute overrides are required to modify the command
    assert "timeout" not in step_command


@pytest.mark.parametrize(
    ("use_proxy", "overrides", "exp_timeout", "exp_fk_timeout"),
    [
        pytest.param(
            True,
            {"local": {"max_walltime": "01:30"}},
            "90s",
            "2s",
            id="Proxied; default fk-timeout",
        ),
        pytest.param(
            True,
            {"local": {"force_kill_timeout": "00:00:05"}},
            "600s",
            "5s",
            id="Proxied; default duration",
        ),
        pytest.param(
            True,
            {"local": {"max_walltime": "00:01:00", "force_kill_timeout": "00:00:42"}},
            "60s",
            "42s",
            id="Proxied; no defaults",
        ),
        pytest.param(
            False,
            {"local": {"max_walltime": "00:60"}},
            "60s",
            "2s",
            id="Unproxied; default fk-timeout",
        ),
        pytest.param(
            False,
            {"local": {"force_kill_timeout": "00:01:00"}},
            "600s",
            "60s",
            id="Unproxied; default duration",
        ),
        pytest.param(
            False,
            {"local": {"max_walltime": "00:60:00", "force_kill_timeout": "00:00:00"}},
            "3600s",
            "0s",
            id="Unproxied; no defaults",
        ),
    ],
)
def test_locallauncher_adapt_step_with_compute_overrides(
    wp_templates_dir: Path,
    use_proxy: bool,
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

    with mock.patch.object(
        LocalLauncher, "use_proxy", mock.PropertyMock(return_value=use_proxy)
    ):
        step_command = LocalLauncher.adapt_step(live_step, [])

    # confirm that compute overrides are required to modify the command
    assert f"timeout {exp_timeout} -k {exp_fk_timeout}" in step_command
