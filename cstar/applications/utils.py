import typing as t

from cstar.orchestration.models import ApplicationDefinition

_TAnyApp: t.TypeAlias = ApplicationDefinition[t.Any, t.Any]
_registry: dict[str, type[_TAnyApp]] = {}

_AppDef = t.TypeVar("_AppDef", bound=_TAnyApp)


def register_application(
    wrapped_cls: type[_AppDef],
) -> type[_AppDef]:
    """Register the decorated type as an available Application."""
    _registry[wrapped_cls.name] = wrapped_cls  # type: ignore[reportArgumentType,index] # pydantic + decorated property

    return wrapped_cls


def get_application(name: str) -> _TAnyApp:
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
        return application()

    msg = f"No application for {name!r}"
    raise ValueError(msg)
