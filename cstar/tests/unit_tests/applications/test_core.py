# ruff: noqa: S101
import textwrap
from pathlib import Path

import pytest

from cstar.applications.core import (
    RunnerRequest,
    RunnerResult,
    RunnerState,
    _registry,
    get_application,
)
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.base.env import discover_env_vars
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.utils import ENV_CSTAR_APP_MODULES


def test_get_application_unknown_name_raises_value_error() -> None:
    """Verify that an unregistered application name raises ValueError."""
    with pytest.raises(ValueError, match="No application for"):
        get_application("no_such_application")


def test_app_modules_env_var_is_registered() -> None:
    """Verify that CSTAR_APP_MODULES is discoverable for CLI display and docs."""
    assert ENV_CSTAR_APP_MODULES in discover_env_vars()


def test_get_application_from_external_module(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that applications defined outside `cstar.applications` are discovered
    via the CSTAR_APP_MODULES environment variable.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs. Used to create the
        external application module.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the environment variable and import path.
    """
    app_name = "external_test_app"
    module = tmp_path / f"{app_name}_module.py"
    module.write_text(
        textwrap.dedent(
            f"""
            from cstar.applications.core import (
                ApplicationDefinition,
                register_application,
            )
            from cstar.applications.hello_world import (
                HelloWorldBlueprint,
                HelloWorldRunner,
            )


            @register_application
            class ExternalApplication(
                ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner]
            ):
                name: str = "{app_name}"
                long_name: str = "Externally Defined App"
                runner = HelloWorldRunner
                blueprint = HelloWorldBlueprint
                applicable_transforms = ()
            """
        )
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(ENV_CSTAR_APP_MODULES, f"{app_name}_module")

    try:
        app = get_application(app_name)
        assert app.name == app_name
        assert app.blueprint.__name__ == "HelloWorldBlueprint"
    finally:
        _registry.pop(app_name, None)


def test_runnerresult_initial_state(tmp_path: Path) -> None:
    """Verify that the RunnerResult returns the initial status, as expected."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        RomsMarblBlueprint,
        f"test-{RomsMarblBlueprint.__name__}-request",
    )
    result = RunnerResult(request, RunnerState(ExecutionStatus.UNSUBMITTED))

    assert result.state.status == ExecutionStatus.UNSUBMITTED


@pytest.mark.parametrize(
    "updates",
    [
        pytest.param(
            [
                ExecutionStatus.PENDING,
            ],
            id="1 update",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
            ],
            id="2 updates",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.COMPLETED,
            ],
            id="3 updates, success state",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.CANCELLED,
            ],
            id="3 updates, fail state",
        ),
    ],
)
def test_runnerresult_state_updates(
    tmp_path: Path,
    updates: list[ExecutionStatus],
) -> None:
    """Verify that the RunnerResult stores multiple states and returns the latest value.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs. Used to create a fake
        blueprint file.
    updates : list[ExecutionStatus]
        A collection of status updates to pass to the RunnerResult
    """
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    states = [RunnerState(status) for status in updates]

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        RomsMarblBlueprint,
        f"test-{RomsMarblBlueprint.__name__}-request",
    )
    result = RunnerResult(request, states)

    # confirm the order is honored
    assert result.state.status == updates[-1]


@pytest.mark.parametrize(
    "updates",
    [
        pytest.param(
            [
                ExecutionStatus.PENDING,
            ],
            id="1 update",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
            ],
            id="2 updates",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.COMPLETED,
            ],
            id="3 updates, success state",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.CANCELLED,
            ],
            id="3 updates, fail state",
        ),
    ],
)
def test_runnerresult_state_add_state(
    tmp_path: Path,
    updates: list[ExecutionStatus],
) -> None:
    """Verify that the RunnerResult properly updates the stored state transitions.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs. Used to create a fake
        blueprint file.
    updates : list[ExecutionStatus]
        A collection of status updates to apply iteratively to the RunnerResult
    """
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        RomsMarblBlueprint,
        f"test-{RomsMarblBlueprint.__name__}-request",
    )

    result = RunnerResult(request, RunnerState())
    for status in updates:
        result.add_state(RunnerState(status))

    # confirm the order is honored
    assert result.state.status == updates[-1]
    # confirm the RunnerResult has an initial state
    assert len(result.states) == len(updates) + 1
    # ... and that it is the expected default
    assert result.states[0].status == ExecutionStatus.UNSUBMITTED


@pytest.mark.parametrize(
    ("updates", "exp_num_states"),
    [
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
            ],
            2,
            id="leading dupe",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.RUNNING,
            ],
            2,
            id="trailing dupe",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.COMPLETED,
                ExecutionStatus.COMPLETED,
            ],
            3,
            id="interleaved dupes",
        ),
        pytest.param(
            [
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
                ExecutionStatus.PENDING,
            ],
            1,
            id="mega-dupe!",
        ),
    ],
)
def test_runnerresult_state_add_duplicate_status(
    tmp_path: Path,
    updates: list[ExecutionStatus],
    exp_num_states: int,
) -> None:
    """Verify that the RunnerResult does not store sequential, duplicate states.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs. Used to create a fake
        blueprint file.
    updates : list[ExecutionStatus]
        A collection of status updates to apply iteratively to the RunnerResult
    """
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        RomsMarblBlueprint,
        f"test-{RomsMarblBlueprint.__name__}-request",
    )

    all_states = [RunnerState(s) for s in updates]

    # ensure constructor catches dupes
    result = RunnerResult(request, all_states)

    # confirm the RunnerResult has an initial state
    assert len(result.states) == exp_num_states

    # ensure constructor catches dupes
    result = RunnerResult(request, RunnerState(ExecutionStatus.UNKNOWN))
    for item in all_states:
        result.add_state(item)

    # confirm the expected number via `add_state` (allowing for the initial state)
    assert len(result.states) == exp_num_states + 1


@pytest.mark.parametrize(
    ("errors", "exp_num_errors"),
    [
        pytest.param(
            [
                ["error 1"],
            ],
            1,
            id="base-case with a single error list",
        ),
        pytest.param(
            [
                ["error 1"],
                ["error 2", "error 3"],
            ],
            3,
            id="verify list chaining",
        ),
        pytest.param(
            [
                ["error 1"],
                ["error 2", "error 3"],
                ["error 4"],
                ["error 5", "error 6"],
            ],
            6,
            id="extended chaining",
        ),
    ],
)
def test_runnerresult_state_add_errors(
    tmp_path: Path,
    errors: list[list[str]],
    exp_num_errors: int,
) -> None:
    """Verify that the RunnerResult properly updates the stored state transitions.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture for writing per-test outputs. Used to create a fake
        blueprint file.
    errors : list[str]
        A collection of errors that will be added to the result.
    exp_num_errors : int
        The number of errors the RunnerResult should produce.
    """
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        RomsMarblBlueprint,
        f"test-{RomsMarblBlueprint.__name__}-request",
    )

    result = RunnerResult(request, RunnerState())
    for error_list in errors:
        result.add_state(RunnerState(ExecutionStatus.RUNNING, error_list))

    # confirm error results
    assert len(result.errors) == exp_num_errors
