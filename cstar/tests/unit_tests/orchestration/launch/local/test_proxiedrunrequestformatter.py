import os
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_ORCH_LOCAL_DELAY
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.local import LocalHandle, ProxiedRunRequestFormatter
from cstar.orchestration.orchestration import LiveStep, RunRequest, Status, Workplan
from cstar.orchestration.serialization import deserialize


def test_proxiedrunrequestformatter_init_no_step() -> None:
    """Verify that an appropriate error is raised if the required step is not provided."""
    step: LiveStep | None = None

    with pytest.raises(CstarExpectationFailed, match="Step is required for formatting"):
        _ = ProxiedRunRequestFormatter(step)  # type: ignore


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_init_step_only(wp_templates_dir: Path) -> None:
    """Verify that a formatter is initialized correctly when only a step is supplied."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    formatter = ProxiedRunRequestFormatter(live_step)

    # verify the step is referenced in the formatter
    assert formatter.step == live_step


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_init_empty_deps(wp_templates_dir: Path) -> None:
    """Verify that a formatter handles an empty dependency list correctly."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    formatter = ProxiedRunRequestFormatter(live_step, [])

    # verify dependencies is non-null
    assert isinstance(formatter.dependencies, list)
    # verify no dependencies are appearing unexpectedly
    assert not formatter.dependencies


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_init_empty_updates(wp_templates_dir: Path) -> None:
    """Verify that a formatter handles an empty update map correctly."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    formatter = ProxiedRunRequestFormatter(live_step, updates={})

    # verify updates is non-null
    assert isinstance(formatter.updates, dict)
    # verify no updates are appearing unexpectedly
    assert not formatter.updates


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_init_nonempty_deps(
    wp_templates_dir: Path, mock_run_id: str
) -> None:
    """Verify that a formatter handles a non-empty dependency list correctly."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    exp_pid = "12345"

    deps = [
        LocalHandle(
            start_at=datetime.now(),
            pid=exp_pid,
            name="local-test-handle",
            run_id=mock_run_id,
        )
    ]
    formatter = ProxiedRunRequestFormatter(live_step, deps)

    # verify dependencies is non-null
    assert isinstance(formatter.dependencies, list)

    # verify list contains the expected handle
    assert len(formatter.dependencies) == len(deps)
    assert next(h for h in formatter.dependencies if h.pid == exp_pid)


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_init_nonempty_updates(
    wp_templates_dir: Path,
) -> None:
    """Verify that a formatter handles a non-empty update map correctly."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])
    exp_updates = {
        "a": "aaa",
        "b": "bbb",
    }

    formatter = ProxiedRunRequestFormatter(
        live_step,
        updates=exp_updates,
    )

    # verify updates is non-null
    assert isinstance(formatter.updates, dict)

    # verify the supplied updates are found in the formatter
    assert len(formatter.updates) == len(exp_updates)
    assert formatter.updates["a"] == "aaa"
    assert formatter.updates["b"] == "bbb"


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_minimal_tpl_fill(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that a formatter correctly fills a template with the minimum allowed
    arguments (step).
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    formatter = ProxiedRunRequestFormatter(live_step)
    cmd = ["python", "-m", "venv", ".venv"]

    tokens = [
        'SENTINEL_PATH="{sentinel_path}"',
        'BLUEPRINT_PATH="{blueprint_path}"',
        "DEP_PIDS=({pids})",
        "RUNNING={running}",
        "DONE={done}",
        "FAILED={failed}",
        "sleep {delay}",
        "{env_vars}",
    ]

    run_request = RunRequest(
        command=cmd,
    )

    mock_spath = tmp_path / "mock-sentinel.yaml"
    mock_delay = "42"

    with (
        mock.patch(
            "cstar.orchestration.state.StateRepository.sentinel_path",
            mock.Mock(return_value=mock_spath),
        ),
        mock.patch.dict(
            os.environ,
            {ENV_CSTAR_ORCH_LOCAL_DELAY: mock_delay},
        ),
    ):
        script = formatter.format(run_request)

    # confirm all tokens were replaced
    for token in tokens:
        assert token not in script

    # confirm all the default replacements are done.
    exp_replacements = [
        f'SENTINEL_PATH="{mock_spath}"',
        f'BLUEPRINT_PATH="{live_step.blueprint_path}"',
        f"RUNNING={Status.Running.value}",
        f"DONE={Status.Done.value}",
        f"FAILED={Status.Failed.value}",
        f"sleep {mock_delay}",
    ]
    for replacement in exp_replacements:
        assert replacement in script


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_environment_formatting(
    wp_templates_dir: Path,  # , mock_run_id: str
) -> None:
    """Verify that a formatter correctly adds environment variables into the template."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    formatter = ProxiedRunRequestFormatter(live_step)
    cmd = ["python", "-m", "venv", ".venv"]
    env = {"a": "aaa", "b": "bbb"}

    exp_replacement = [
        "export a='aaa'",
        "export b='bbb'",
    ]

    run_request = RunRequest(
        command=cmd,
        environment=env,
    )
    script = formatter.format(run_request)

    # confirm the requested environment variables were exported in the script
    for replacement in exp_replacement:
        assert replacement in script


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_dependency_formatting(
    wp_templates_dir: Path, mock_run_id: str
) -> None:
    """Verify that a formatter correctly adds the dependency array into the template."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    deps = [
        LocalHandle(
            start_at=datetime.now(),
            pid=pid,
            name=f"local-test-handle-{pid}",
            run_id=mock_run_id,
        )
        for pid in ["12345", "54321"]
    ]
    formatter = ProxiedRunRequestFormatter(live_step, deps)
    cmd = ["python", "-m", "venv", ".venv"]

    deps_array = 'DEP_PIDS=("12345" "54321")\n'

    run_request = RunRequest(
        command=cmd,
    )
    script = formatter.format(run_request)

    # confirm the dependencies are listed
    assert deps_array in script
