import typing as t

from cstar.base.log import get_logger
from cstar.orchestration.models import ApplicationDefinition

log = get_logger(__name__)


_TAnyApp: t.TypeAlias = ApplicationDefinition[t.Any, t.Any]
_registry: dict[str, type[_TAnyApp]] = {}

_AppDef = t.TypeVar("_AppDef", bound=_TAnyApp)


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
