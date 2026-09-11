import importlib
import importlib.util
import typing as t
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import entry_points
from itertools import chain
from pathlib import Path

from cstar.base.adapter import SchemaAdapter
from cstar.base.env import ENV_CSTAR_DISABLE_MIGRATION
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.entrypoint.config import JOBFILE_DATE_FORMAT
from cstar.execution.file_system import local_copy
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint, BlueprintCore
from cstar.orchestration.serialization import SerializableModel, deserialize
from cstar.system.migration import (
    BlueprintMigration,
    CstarMigrationError,
    CstarUnsupportedMigrationError,
)

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration

APP_PLUGIN_GROUP: t.Final[str] = "cstar.applications"
"""Entry-point group third-party packages use to register applications.

A published packaging contract: installed plugins name this group in their own
``pyproject.toml``, so it must not change when the package layout does. It
coincides with :data:`BUILTIN_APP_PACKAGE` today only by convention.
"""

BUILTIN_APP_PACKAGE: t.Final[str] = "cstar.applications"
"""Package holding the in-tree application modules, one per application name."""

log = get_logger(__name__)


class HasApplication(SerializableModel, t.Protocol):
    @property
    def application(self) -> str: ...


TBlueprint = t.TypeVar("TBlueprint", bound=HasApplication)
TTransformable = t.TypeVar("TTransformable", bound=SerializableModel)


class RunnerRequest(t.Generic[TBlueprint]):
    """Generic request containing configuration required to execute an application
    via Blueprint.
    """

    name: str | None = None
    """User-friendly name for the process (or job) handling the request."""
    blueprint_uri: str
    """The URI of a blueprint to be used to parameterize the application."""
    directive_uri: str
    """The URI of a file containing directive configuration."""
    bp_type: type[TBlueprint]
    """The type of blueprint that the URI will be deserialized into."""
    _bp: TBlueprint | None = None
    """The deserialized blueprint."""

    def __init__(
        self,
        uri: str,
        bp_type: type[TBlueprint],
        name: str = "",
        directive_uri: str = "",
    ) -> None:
        """Initialize the request instance.

        Parameters
        ----------
        name : str
            User-friendly name for the process (or job) handling the request.
        uri : str
            The URI of a blueprint to be used to parameterize the application.
        bp_type : type[TBlueprint]
            The type of blueprint that the path will be deserialized into.
        directive_uri : str
            The URI of a file containing directive configuration for a runner.
        """
        self.blueprint_uri = uri.strip()
        self.bp_type = bp_type
        self.name = name.strip() or RunnerRequest._generate_job_name()
        self.directive_uri = directive_uri.strip()

    @property
    def application(self) -> str:
        """Return the string identifying the application that will be executed.

        Returns
        -------
        str
        """
        return self.blueprint.application

    @property
    def blueprint(self) -> TBlueprint:
        """Return the deserialized blueprint instance.

        Returns
        -------
        TBlueprint
        """
        if self._bp is None:
            with local_copy(str(self.blueprint_uri)) as local_path:
                self._bp = deserialize(local_path, self.bp_type)
        return self._bp

    @classmethod
    def _generate_job_name(cls) -> str:
        """Generate a unique job name based on the current date and time.

        Returns
        -------
        str
        """
        now_utc = datetime.now(UTC)
        formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
        return f"cstar_worker_{formatted_now_utc}"


@dataclass
class RunnerState:
    """The state of a runner task at a given point in time."""

    status: ExecutionStatus = field(default=ExecutionStatus.UNSUBMITTED)
    """The final status of the application."""
    errors: list[str] = field(default_factory=list[str])
    """The error messages produced by the application."""
    timestamp: t.Final[str] = field(
        default_factory=lambda: datetime.now(UTC).strftime("%Y%m%d%H%M%S"),
        init=False,
        compare=False,
    )
    """When the state occurred."""


class RunnerResult(t.Generic[TBlueprint]):
    """Specifies details about the result of running an application."""

    request: RunnerRequest[TBlueprint]
    """The request that was handled and produced the result."""
    _states: list[RunnerState]
    """State transitions for the blueprint process recorded during the runner lifecycle."""

    def __init__(
        self,
        request: RunnerRequest[TBlueprint],
        state: Sequence[RunnerState] | RunnerState,
        errors: list[str] | None = None,
    ) -> None:
        """Initialize the result instance.

        Parameters
        ----------
        request : RunnerRequest[TBlueprint]
            The request that was handled and produced the result.
        status : ExecutionStatus | None
            The final status of the application.
        errors : list[str] | None
            The error messages produced by the appplication.
        state : Sequence[RunnerState] | RunnerState
            The state(s) of the application.
        """
        self.request = request
        self._errors = errors or []
        self._states = []

        if isinstance(state, RunnerState):
            self._states.append(state)
        else:
            list(map(self.add_state, state))

    @property
    def states(self) -> Sequence[RunnerState]:
        """Return all unique states encountered by the runner."""
        return self._states

    @property
    def state(self) -> RunnerState:
        """Return the latest state encountered by the runner."""
        return self._states[-1]

    @property
    def errors(self) -> Sequence[str]:
        """Return all recorded error messages."""
        return list(
            chain.from_iterable(item.errors for item in self._states if item.errors),
        )

    def add_state(self, state: RunnerState) -> bool:
        """Add the state to the state history, while dropping duplicate states.

        Parameters
        ----------
        state : RunnerState
            A state to be added to the history.

        Returns
        -------
        bool
            `True` when the state is stored, `False` when duplicated.
        """
        last_state = self._states[-1] if self._states else None

        old = last_state.status if last_state else None
        new = state.status

        unseen_status = old is None or old != new
        seen_errors = set(self.errors)
        unseen_errors = set(state.errors).difference(seen_errors)

        if unseen_status or unseen_errors:
            old = last_state.status if last_state else None
            new = state.status

            self._states.append(state)

            msg = f"Runner transitioned from {old} to {new}"
            log.trace(msg)
            return True

        return False


class XRunner(t.Protocol, t.Generic[TBlueprint]):
    """Core API required to be a hosted BlueprintRunner."""

    def __init__(
        self,
        request: RunnerRequest[TBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize a runner instance.

        Parameters
        ----------
        request : RunnerRequest[TBlueprint]
            The request containing configuration for executing an application.
        service_cfg : ServiceConfiguration
            Configuration options for the execution of an application in a service.
        job_cfg : JobConfig
            Configuration required to submit jobs on an HPC.
        """
        ...

    @property
    def blueprint(self) -> TBlueprint:
        """Return the deserialized blueprint instance."""
        ...

    @property
    def request(self) -> RunnerRequest[TBlueprint]:
        """Return the request containing configuration for executing the application."""
        ...

    @property
    def result(self) -> RunnerResult[TBlueprint] | None:
        """Return the runner result object used to record state transitions of
        the executing blueprint.

        Returns
        -------
        RunnerResult[TBlueprint]
        """
        ...

    @property
    def state(self) -> RunnerState:
        """Return the current state of the application."""
        ...

    async def run(self) -> RunnerResult[TBlueprint]:
        """Execute the application.

        Returns
        -------
        RunnerResult[TBlueprint]
        """
        ...


class Transform(t.Protocol, t.Generic[TTransformable]):
    """Protocol for a class that transforms a step into one or more
    new steps.
    """

    def __call__(self, step: TTransformable) -> Sequence[TTransformable]:
        """Apply the transform to a step.

        Parameters
        ----------
        step : Step
            The step to be transformed

        Returns
        -------
        Iterable[Step]
            Zero-to-many steps resulting from applying the transform.
        """
        ...

    @staticmethod
    def suffix() -> str:
        """Return the standard prefix to be used when persisting
        a resource modified by this transform.
        """
        ...

    @classmethod
    def is_active(cls) -> bool:
        """Return `True` when the transform will modify steps under the
        current configuration.

        Transforms gated behind feature flags override this to report
        their effective state.

        Returns
        -------
        bool
        """
        return True


TRunner = t.TypeVar("TRunner")


class ApplicationDefinition(t.Protocol, t.Generic[TBlueprint, TRunner]):
    """The contract establishing the metadata needed by the system
    to orchestrate tasks using their blueprints.
    """

    name: str
    """A short, unique name used to identify the application."""
    long_name: str
    """A user-friendly display name for the application."""
    runner: type[TRunner]
    """The runner that executes the application blueprints."""
    blueprint: type[TBlueprint]
    """The blueprint containing the application configuration."""
    applicable_transforms: Sequence[type[Transform[t.Any]]]
    """Transforms that must be executed prior to execution."""
    migrations: Sequence[type[SchemaAdapter]] | None = None
    """The available adapters for performing schema migrations."""


_TAnyApp: t.TypeAlias = ApplicationDefinition[t.Any, t.Any]
_registry: dict[str, type[_TAnyApp]] = {}
_AppDef = t.TypeVar("_AppDef", bound=_TAnyApp)


def get_application_name(path: Path) -> str:
    """Retrieve the application name from a blueprint file.

    Parameters
    ----------
    path : Path
        The path to a file containing a blueprint.

    Returns
    -------
    str
    """
    if not path.exists():
        msg = f"Blueprint file not found at {str(path)!r}"
        raise FileNotFoundError(msg)

    base_bp = deserialize(path, BlueprintCore)
    return base_bp.application


def register_application(
    klass: type[_AppDef],
) -> type[_AppDef]:
    """Register the decorated type as an available Application."""
    _registry[klass.name] = klass
    log.trace(f"Registered {klass.__name__!r} application context")
    return klass


def _load_builtin_application(name: str) -> None:
    """Import the in-tree ``{BUILTIN_APP_PACKAGE}.{name}`` module, if it exists.

    Parameters
    ----------
    name : str
        The application name being resolved.

    Notes
    -----
    A missing module is not an error -- the name may belong to an installed
    plugin, which the caller tries next. A module that exists but fails to
    import *is* an error: the ``ImportError`` propagates rather than being
    reported as a missing application.
    """
    module = f"{BUILTIN_APP_PACKAGE}.{name}"

    if importlib.util.find_spec(module) is None:
        log.trace(f"No built-in application module {module!r} for {name!r}")
        return

    importlib.import_module(module)


def _load_app_entry_point(name: str) -> None:
    """Load a third-party application via the ``cstar.applications`` entry-point group.

    Parameters
    ----------
    name : str
        The application name being resolved.

    Notes
    -----
    Reached only once the in-tree lookup has come up empty, so an installed
    plugin can never shadow a built-in application.
    """
    for ep in entry_points(group=APP_PLUGIN_GROUP, name=name):
        try:
            ep.load()
        except Exception:
            log.warning(f"Unable to load application plugin {name!r} from {ep.value!r}")

        if name in _registry:
            return


def get_application(name: str) -> ApplicationDefinition[t.Any, t.Any]:
    """Get an application from the application registry.

    Returns
    -------
    Application
        The application matching the supplied name

    Raises
    ------
    ValueError
        if no registered application is associated with this classification

    Notes
    -----
    Applications are discovered, in order, from:

    1. The in-tree ``{BUILTIN_APP_PACKAGE}.{name}`` module.
    2. The :data:`APP_PLUGIN_GROUP` entry-point group -- installed third-party
       plugins.

    Each step is attempted only while *name* is still unregistered. A plugin
    therefore can never shadow a built-in application: a name that resolves in
    step 1 never reaches step 2, and the plugin claiming it is never imported.
    """
    if name not in _registry:
        _load_builtin_application(name)

    if name not in _registry:
        _load_app_entry_point(name)

    if application := _registry.get(name):
        log.trace(f"Located application context {application.__name__!r} for {name!r}")
        return application()

    msg = f"No application for {name!r}"
    raise ValueError(msg)


def get_app_for_blueprint(path: Path) -> ApplicationDefinition[t.Any, t.Any]:
    """Retrieve the appropriate application for a blueprint.

    Parameters
    ----------
    path : Path
        The path to a file containing a blueprint.

    Returns
    -------
    ApplicationDefinition[t.Any, t.Any]
    """
    name = get_application_name(path)
    return get_application(name)


def load_blueprint(path: Path) -> Blueprint:
    """Load a blueprint from disk, migrating its schema in memory if needed.

    Unlike :func:`deserialize`, which validates a file straight into the
    application's strict blueprint type, this reads the file leniently (via
    :class:`BlueprintCore`), runs any registered schema migrations for the
    application in memory, and only then validates against the current schema.
    Nothing is written to disk. Callers that must parse a blueprint before
    execution (schedule-time transforms) use this so a pre-current-schema
    blueprint is upgraded rather than raising a raw ``ValidationError``.

    Parameters
    ----------
    path : Path
        The path to a file containing a blueprint.

    Returns
    -------
    Blueprint
        The blueprint validated against the application's current schema.

    Raises
    ------
    CstarMigrationError
        If a required migration cannot be planned or completed, or is required
        but disabled via ``CSTAR_DISABLE_MIGRATION``.
    """
    # A single lenient read serves both the application lookup and the migration
    # input, so a blueprint on a network path is read once, not per-consumer.
    core = deserialize(path, BlueprintCore)
    app = get_application(core.application)
    dumped = core.model_dump()

    adapters = app.migrations or []
    if adapters:
        migrator = BlueprintMigration(adapters=adapters)
        if app.name in migrator.schema_bounds:
            dumped = _migrate_blueprint_dict(path, migrator, dumped)
        else:
            # The registered adapters target a different application (e.g. an
            # app inheriting another's migrations). Nothing to migrate here;
            # strict validation below handles current-schema blueprints.
            log.debug(
                f"Skipping schema migration for {str(path)!r}: registered adapters "
                f"do not cover application {app.name!r}"
            )

    return t.cast("Blueprint", app.blueprint.model_validate(dumped))


def _migrate_blueprint_dict(
    path: Path,
    migrator: BlueprintMigration,
    dumped: dict[str, t.Any],
) -> dict[str, t.Any]:
    """Plan and apply an in-memory schema migration for a blueprint dict.

    Returns
    -------
    dict[str, t.Any]
        The (possibly) migrated blueprint dict; unchanged if already current.

    Raises
    ------
    CstarMigrationError
        If migration cannot be planned or completed, or is required but disabled
        via ``CSTAR_DISABLE_MIGRATION``.
    """
    try:
        plan = migrator.plan(dumped)
    except CstarUnsupportedMigrationError as ex:
        msg = f"Unable to migrate blueprint {str(path)!r}: {ex}"
        raise CstarMigrationError(msg) from ex

    if plan.adapters and is_flag_enabled(ENV_CSTAR_DISABLE_MIGRATION):
        msg = (
            f"Blueprint {str(path)!r} requires schema migration from "
            f"{plan.source!r} to {plan.target!r}, but migration is disabled "
            f"({ENV_CSTAR_DISABLE_MIGRATION}=1). Unset {ENV_CSTAR_DISABLE_MIGRATION} "
            "or run `cstar blueprint migrate` to update the blueprint."
        )
        raise CstarMigrationError(msg)

    if plan.is_latest:
        return dumped

    try:
        return migrator.migrate(dumped, plan)
    except CstarMigrationError as ex:
        msg = f"Unable to migrate blueprint {str(path)!r}: {ex}"
        raise CstarMigrationError(msg) from ex
