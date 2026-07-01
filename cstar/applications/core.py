import typing as t
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path

from cstar.base.adapter import SchemaAdapter
from cstar.base.log import get_logger
from cstar.entrypoint.config import JOBFILE_DATE_FORMAT
from cstar.execution.file_system import local_copy
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import BlueprintMeta
from cstar.orchestration.serialization import SerializableModel, deserialize

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


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
        now_utc = datetime.now(timezone.utc)
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
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
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
    base_bp = deserialize(path, BlueprintMeta)
    return base_bp.application


def register_application(
    klass: type[_AppDef],
) -> type[_AppDef]:
    """Register the decorated type as an available Application."""
    _registry[klass.name] = klass
    log.trace(f"Registered {klass.__name__!r} application context")
    return klass


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
    """
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
