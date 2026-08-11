# ruff: noqa: S101
import sys
import textwrap
from pathlib import Path

import pytest

from cstar.applications.core import (
    APP_PLUGIN_GROUP,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    _registry,
    get_application,
)
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.execution.handler import ExecutionStatus


def _external_app_module_source(app_name: str) -> str:
    """Return source for a throwaway module that registers a HelloWorld-based
    application under *app_name* when imported.

    Parameters
    ----------
    app_name : str
        The unique application name the module will register.

    Returns
    -------
    str
    """
    return textwrap.dedent(
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


def _write_dist_info(
    tmp_path: Path,
    dist_name: str,
    version: str,
    entry_name: str,
    module: str,
) -> None:
    """Write a minimal ``*.dist-info`` directory so ``importlib.metadata``
    discovers a ``cstar.applications`` entry point pointing at *module*.

    Parameters
    ----------
    tmp_path : Path
        Directory (expected to already be on ``sys.path``) in which to create
        the dist-info directory.
    dist_name : str
        The distribution name used for the dist-info directory and METADATA.
    version : str
        The distribution version used for the dist-info directory and METADATA.
    entry_name : str
        The entry point name (the application name it will register under).
    module : str
        The importable module path the entry point resolves to.
    """
    dist_info = tmp_path / f"{dist_name}-{version}.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {dist_name}\nVersion: {version}\n"
    )
    (dist_info / "entry_points.txt").write_text(
        f"[{APP_PLUGIN_GROUP}]\n{entry_name} = {module}\n"
    )


def test_get_application_unknown_name_raises_value_error() -> None:
    """Verify that an unregistered application name raises ValueError."""
    with pytest.raises(ValueError, match="No application for"):
        get_application("no_such_application")


def test_get_application_via_installed_entry_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that applications registered via the ``cstar.applications``
    entry-point group are discovered through real ``importlib.metadata``
    dist-info discovery.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture used to host both the module and a dist-info
        directory so it is discoverable via ``sys.path``.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the import path.
    """
    app_name = "entrypoint_disco_app"
    module = f"{app_name}_module"
    (tmp_path / f"{module}.py").write_text(_external_app_module_source(app_name))
    _write_dist_info(tmp_path, app_name, "1.0.0", app_name, module)

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        app = get_application(app_name)
        assert app.name == app_name
        assert app.blueprint.__name__ == "HelloWorldBlueprint"
    finally:
        _registry.pop(app_name, None)


def test_get_application_via_entry_point_does_not_warn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that resolving an application registered via the
    ``cstar.applications`` entry-point group does not also attempt (and warn
    about) the in-tree ``cstar.applications`` import.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture used to host both the module and a dist-info
        directory so it is discoverable via ``sys.path``.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the import path.
    caplog : pytest.LogCaptureFixture
        Fixture used to assert no spurious "Unable to load" warning is logged.
    """
    app_name = "entrypoint_disco_app_no_warn"
    module = f"{app_name}_module"
    (tmp_path / f"{module}.py").write_text(_external_app_module_source(app_name))
    _write_dist_info(tmp_path, app_name, "1.0.0", app_name, module)

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        with caplog.at_level("WARNING"):
            app = get_application(app_name)
        assert app.name == app_name
        assert "Unable to load C-Star application" not in caplog.text
    finally:
        _registry.pop(app_name, None)


def test_entry_point_cannot_shadow_builtin_application(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that an entry point named after a built-in application (e.g.
    ``hello_world``) never replaces the built-in: the resolved application
    must be the real, in-tree class, and the decoy module must never even be
    imported.

    The decoy is a real, importable module that *would* register itself under
    the built-in's name, so this fails if built-in precedence ever regresses.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture used to host both the decoy module and a
        dist-info directory so it is discoverable via ``sys.path``.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the import path.
    """
    app_name = "hello_world"
    module = "shadow_decoy_module"
    (tmp_path / f"{module}.py").write_text(_external_app_module_source(app_name))
    _write_dist_info(tmp_path, "shadow-decoy", "1.0.0", app_name, module)

    monkeypatch.syspath_prepend(str(tmp_path))

    # Force resolution through the loaders rather than a registry hit left
    # behind by an earlier test, and drop the cached module so the in-tree
    # import actually re-runs its @register_application decorator.
    registered = _registry.pop(app_name, None)
    builtin = sys.modules.pop(f"cstar.applications.{app_name}", None)

    try:
        app = get_application(app_name)

        assert app.long_name != "Externally Defined App"
        assert app.blueprint.__name__ == "HelloWorldBlueprint"
        assert module not in sys.modules
    finally:
        if builtin is not None:
            sys.modules[f"cstar.applications.{app_name}"] = builtin
        if registered is not None:
            _registry[app_name] = registered
        else:
            _registry.pop(app_name, None)


def test_get_application_broken_entry_point_does_not_abort_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify that an entry-point module which raises on import is skipped
    (with a warning) rather than aborting resolution, and that resolution
    still ends in the normal ``ValueError`` when nothing else registers the
    application.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture used to host both the module and a dist-info
        directory so it is discoverable via ``sys.path``.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the import path.
    caplog : pytest.LogCaptureFixture
        Fixture used to assert the expected warning is logged.
    """
    app_name = "broken_entry_point_app"
    module = f"{app_name}_module"
    (tmp_path / f"{module}.py").write_text(
        'raise RuntimeError("this module cannot be imported")\n'
    )
    _write_dist_info(tmp_path, app_name, "1.0.0", app_name, module)

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        with caplog.at_level("WARNING"):
            with pytest.raises(ValueError, match="No application for"):
                get_application(app_name)
        assert (
            f"Unable to load application plugin {app_name!r} from {module!r}"
            in caplog.text
        )
    finally:
        _registry.pop(app_name, None)


def test_get_application_entry_point_registers_nothing_raises_value_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that an entry point which imports successfully but does not
    register the requested application name still ends resolution in the
    normal ``ValueError``, rather than silently succeeding with a wrong
    answer.

    Parameters
    ----------
    tmp_path : Path
        Temporary path fixture used to host both the module and a dist-info
        directory so it is discoverable via ``sys.path``.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to set the import path.
    """
    app_name = "empty_entry_point_app"
    module = f"{app_name}_module"
    (tmp_path / f"{module}.py").write_text(
        "# importable module that registers no application\n"
    )
    _write_dist_info(tmp_path, app_name, "1.0.0", app_name, module)

    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        with pytest.raises(ValueError, match="No application for"):
            get_application(app_name)
    finally:
        _registry.pop(app_name, None)


def test_broken_builtin_application_propagates_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that an in-tree application module which exists but fails to
    import surfaces the real ``ImportError`` rather than being reported as a
    missing application.

    ``find_spec`` is stubbed so the module appears to exist; the subsequent
    ``import_module`` then fails for real, standing in for an in-tree
    application whose own imports are broken.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to stub out ``find_spec``.
    """
    app_name = "broken_builtin_app"

    monkeypatch.setattr(
        "cstar.applications.core.importlib.util.find_spec",
        lambda target: object() if target == f"cstar.applications.{app_name}" else None,
    )

    with pytest.raises(ModuleNotFoundError, match=app_name):
        get_application(app_name)


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
