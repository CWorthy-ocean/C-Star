import typing as t
from enum import StrEnum, auto

from cstar.system.registration import Registry


class ApplicationRegistry(StrEnum):
    """Constants identifying the registry keys used for types that
    are registered to handle a portion of the application life-cycle.
    """

    BLUEPRINT = auto()
    """Used to register types that contain blueprints for an application."""
    RUNNER = auto()
    """Used to register types that contain runners for an application."""


THandler = t.TypeVar("THandler")
"""Type variable representing handlers for application life cycle segments"""


def register_app_handler(
    category: str,
    application: str,
) -> t.Callable[[type[THandler]], type[THandler]]:
    """Decorator used to register a type as the handler of some portion of an
    application's lifecycle.

    Parameters
    ----------
    category : str
        A string identifying the lifecycle segment, e.g. "blueprint" or "runner".
    application : str
        The name of the application the handler will be registered for, e.g. "roms_marbl"

    Returns
    -------
        A function that will add the handler to the registry then return it.
    """

    def register_handler_direct(klass: type[THandler]) -> type[THandler]:
        """Register the specified type as a blueprint.

        Parameters
        ----------
        klass : type[TR]
            The type to be registered
        """
        registry = Registry(category)
        registry.put(application, klass)

        return klass

    return register_handler_direct
