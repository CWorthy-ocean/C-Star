from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.launch.local import LocalComputeSpec, LocalLauncher
from cstar.orchestration.orchestration import LiveStep, Workplan
from cstar.orchestration.serialization import deserialize


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
            {"local": {"max_walltime": "90s"}},
            "90s",
            LocalComputeSpec.DEFAULT_FK_TIMEOUT,
            id="Proxied; default fk-to",
        ),
        pytest.param(
            True,
            {"local": {"force_kill_timeout": "5s"}},
            LocalComputeSpec.DEFAULT_MAX_WALLTIME,
            "5s",
            id="Proxied; default to",
        ),
        pytest.param(
            True,
            {"local": {"max_walltime": "90s", "force_kill_timeout": "5s"}},
            "90s",
            "5s",
            id="Proxied; no defaults",
        ),
        pytest.param(
            False,
            {"local": {"max_walltime": "90s"}},
            "90s",
            LocalComputeSpec.DEFAULT_FK_TIMEOUT,
            id="Unproxied; default fk-to",
        ),
        pytest.param(
            False,
            {"local": {"force_kill_timeout": "5s"}},
            LocalComputeSpec.DEFAULT_MAX_WALLTIME,
            "5s",
            id="Unproxied; default to",
        ),
        pytest.param(
            False,
            {"local": {"max_walltime": "90s", "force_kill_timeout": "5s"}},
            "90s",
            "5s",
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
