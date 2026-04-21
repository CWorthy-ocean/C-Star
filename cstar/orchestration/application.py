import typing as t
from collections.abc import Callable

from cstar.system.registration import register_handler

THandler = t.TypeVar("THandler")

APP_CAT_BLUEPRINTS: t.Literal["blueprints"] = "blueprints"
APP_CAT_RUNNERS: t.Literal["runners"] = "runners"


def register_blueprint(
    application: str,
) -> Callable[[type[THandler]], type[THandler]]:
    """Decorator used to register a Blueprint for a given application.

    Parameters
    ----------
    application : str
        The name of the application the handler will be registered for, e.g. "roms_marbl"

    Returns
    -------
        A decorator that registers the decorated class as a Blueprint
    """
    return register_handler(APP_CAT_BLUEPRINTS, application)


def register_runner(
    application: str,
) -> Callable[[type[THandler]], type[THandler]]:
    """Decorator used to register a Runner for a given application.

    Parameters
    ----------
    application : str
        The name of the application the handler will be registered for, e.g. "roms_marbl"

    Returns
    -------
        A decorator that registers the decorated class as a Runner
    """
    return register_handler(APP_CAT_RUNNERS, application)
