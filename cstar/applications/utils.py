import typing as t

from cstar.orchestration.models import ApplicationDefinition

_registry: dict[str, type[ApplicationDefinition]] = {}

_AppDef = t.TypeVar("_AppDef", bound=ApplicationDefinition)


def register_application(
    wrapped_cls: type[_AppDef],
) -> type[_AppDef]:
    """Register the decorated type as an available Application."""
    _registry[wrapped_cls.name] = wrapped_cls

    return wrapped_cls


def get_application(name: str) -> ApplicationDefinition:
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

    raise ValueError(f"No application for {name}")
