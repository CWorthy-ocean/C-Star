import sys
import textwrap
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from cstar.applications.core import (
    APP_PLUGIN_GROUP,
    BUILTIN_APP_PACKAGE,
    EmittedBlueprint,
    RunnerRequest,
    RunnerResult,
    RunnerState,
    _registry,
    get_application,
)
from cstar.applications.ucla_roms.models import UclaRomsBlueprint
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.transforms import DirectiveConfig


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


def test_get_application_resolves_alias_to_current_application() -> None:
    """An earlier application name returns the application that replaced it."""
    current = get_application("ucla_roms")

    assert current.name == "ucla_roms"
    assert type(get_application("roms_marbl")) is type(current)


def test_ucla_roms_blueprint_loads_earlier_application_name_as_current(
    complete_blueprint_path: Path,
) -> None:
    """A blueprint authored under the earlier name loads under the current one.

    Loads through `UclaRomsBlueprint` itself: the subclass redeclares
    `application`, which must not drop the canonicalisation.
    """
    data: dict[str, Any] = yaml.safe_load(complete_blueprint_path.read_text())
    assert data["application"] == "roms_marbl"

    blueprint = UclaRomsBlueprint.model_validate(data)

    assert blueprint.application == "ucla_roms"


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
    builtin = sys.modules.pop(f"{BUILTIN_APP_PACKAGE}.{app_name}", None)

    try:
        app = get_application(app_name)

        assert app.long_name != "Externally Defined App"
        assert app.blueprint.__name__ == "HelloWorldBlueprint"
        assert module not in sys.modules
    finally:
        if builtin is not None:
            sys.modules[f"{BUILTIN_APP_PACKAGE}.{app_name}"] = builtin
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
        lambda target: (
            object() if target == f"{BUILTIN_APP_PACKAGE}.{app_name}" else None
        ),
    )

    with pytest.raises(ModuleNotFoundError, match=app_name):
        get_application(app_name)


def test_runnerrequest_resume_defaults_false(tmp_path: Path) -> None:
    """Verify `RunnerRequest.resume` defaults to `False` when not supplied."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(fake_bp_path.as_posix(), UclaRomsBlueprint)

    assert request.resume is False


def test_runnerrequest_resume_can_be_set_true(tmp_path: Path) -> None:
    """Verify `RunnerRequest.resume` is stored when explicitly supplied."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(fake_bp_path.as_posix(), UclaRomsBlueprint, resume=True)

    assert request.resume is True


def test_runnerrequest_clobber_defaults_false(tmp_path: Path) -> None:
    """Verify `RunnerRequest.clobber` defaults to `False` when not supplied."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(fake_bp_path.as_posix(), UclaRomsBlueprint)

    assert request.clobber is False


def test_runnerrequest_clobber_can_be_set_true(tmp_path: Path) -> None:
    """Verify `RunnerRequest.clobber` is stored when explicitly supplied."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(fake_bp_path.as_posix(), UclaRomsBlueprint, clobber=True)

    assert request.clobber is True


def test_runnerrequest_rejects_resume_with_clobber(tmp_path: Path) -> None:
    """Verify a request cannot both resume a prior attempt and clobber its
    working directory.
    """
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    with pytest.raises(ValueError, match="clobber"):
        RunnerRequest(
            fake_bp_path.as_posix(), UclaRomsBlueprint, resume=True, clobber=True
        )


def test_runnerrequest_blueprint_anchors_relative_locations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bp_templates_dir: Path
) -> None:
    """Verify the blueprint a request loads resolves relative locations against the
    blueprint file's directory rather than the working directory.
    """
    bp_dir = tmp_path / "blueprints"
    bp_dir.mkdir()
    bp_path = bp_dir / "bp.yaml"
    bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("http://mockdoc.com/partitioning1.nc", "../data/boundary.nc")
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    request = RunnerRequest(bp_path.as_posix(), UclaRomsBlueprint)

    assert str(request.blueprint.forcing.boundary.data[0].location) == str(
        tmp_path / "data" / "boundary.nc"
    )


def test_runnerrequest_blueprint_leaves_remote_locations_as_authored(
    bp_templates_dir: Path,
) -> None:
    """Verify a blueprint fetched from a URL keeps its relative locations as written:
    its temporary local copy is no directory to be relative to.
    """
    text = (
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("http://mockdoc.com/partitioning1.nc", "../data/boundary.nc")
    )
    response = mock.Mock(status_code=200, text=text)

    with mock.patch("cstar.execution.file_system.request", return_value=response):
        request = RunnerRequest("https://example.com/bp.yaml", UclaRomsBlueprint)
        blueprint = request.blueprint

    assert str(blueprint.forcing.boundary.data[0].location) == "../data/boundary.nc"


def test_application_definition_resumable_defaults_false() -> None:
    """Verify an `ApplicationDefinition` subclass that does not declare
    `resumable` defaults to `False` (e.g. `hello_world`, which does not
    support resume).
    """
    app_config = get_application("hello_world")

    assert app_config.resumable is False


def test_runnerresult_initial_state(tmp_path: Path) -> None:
    """Verify that the RunnerResult returns the initial status, as expected."""
    fake_bp_path = tmp_path / "fake.yaml"
    fake_bp_path.touch()

    request = RunnerRequest(
        fake_bp_path.as_posix(),
        UclaRomsBlueprint,
        f"test-{UclaRomsBlueprint.__name__}-request",
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
        UclaRomsBlueprint,
        f"test-{UclaRomsBlueprint.__name__}-request",
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
        UclaRomsBlueprint,
        f"test-{UclaRomsBlueprint.__name__}-request",
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
        UclaRomsBlueprint,
        f"test-{UclaRomsBlueprint.__name__}-request",
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
        UclaRomsBlueprint,
        f"test-{UclaRomsBlueprint.__name__}-request",
    )

    result = RunnerResult(request, RunnerState())
    for error_list in errors:
        result.add_state(RunnerState(ExecutionStatus.RUNNING, error_list))

    # confirm error results
    assert len(result.errors) == exp_num_errors


def test_ucla_roms_directives_registered_in_directive_map() -> None:
    """Verify every directive `ucla_roms` declares is registered in
    `DirectiveConfig.directive_map` under its own `key()`.

    The application's `directives` and the global registry are populated
    independently (declaration vs. `DirectiveConfig.register` calls at
    import time); this guards against the two drifting apart.
    """
    app = get_application("roms_marbl")
    for directive in app.directives:
        assert DirectiveConfig.directive_map.get(directive.key()) is directive


def test_emitted_blueprint_defaults_to_none() -> None:
    """An application that does not override the hook emits no blueprint."""
    app = get_application("hello_world")
    assert app.emitted_blueprint(app.blueprint.model_construct()) is None


def test_artifacts_defaults_to_empty() -> None:
    """An application that does not override the hook declares no products."""
    app = get_application("hello_world")
    assert not app.artifacts(app.blueprint.model_construct())


_EMITTED_BLUEPRINT_FIELDS = {
    "filename": "B_x.yaml",
    "application": "roms_marbl",
    "producer": BlueprintIdentity(kind="Blueprint", application="forge", name="x"),
    "cpus_needed": 1,
    "start_date": "2012-01-01",
    "end_date": "2012-01-02",
    "use_pio": False,
    "grid_filename": "x_grid.nc",
}
"""Every field `EmittedBlueprint` requires, with valid values."""


def test_emitted_blueprint_forbids_unknown_fields() -> None:
    """`EmittedBlueprint` rejects fields it does not declare."""
    with pytest.raises(ValidationError) as exc_info:
        EmittedBlueprint(**_EMITTED_BLUEPRINT_FIELDS, bogus=1)  # type: ignore[arg-type]

    assert [(e["type"], e["loc"]) for e in exc_info.value.errors()] == [
        ("extra_forbidden", ("bogus",))
    ]


def test_emitted_blueprint_requires_a_producer() -> None:
    """`EmittedBlueprint` needs the reference its blueprint records for its producer."""
    fields = {k: v for k, v in _EMITTED_BLUEPRINT_FIELDS.items() if k != "producer"}

    with pytest.raises(ValidationError) as exc_info:
        EmittedBlueprint(**fields)  # type: ignore[arg-type]

    assert [(e["type"], e["loc"]) for e in exc_info.value.errors()] == [
        ("missing", ("producer",))
    ]
