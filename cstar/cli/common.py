import importlib
import os
import typing as t
from collections.abc import Callable
from pathlib import Path

import typer

import cstar
from cstar.base.env import (
    ENV_CSTAR_LOG_LEVEL,
    FLAG_ON,
)
from cstar.base.log import LogLevelChoices, reset_log_level
from cstar.execution.file_system import is_remote_resource

app = typer.Typer()

HELP_SHORT = "Print the current version of the C-Star package and exit."


BoolCallback: t.TypeAlias = Callable[[typer.Context, bool], bool]
StrCallback: t.TypeAlias = Callable[[typer.Context, str], str]


def version_callback(value: bool) -> bool:
    """Print the version of C-Star and exit."""
    if value:
        typer.echo(cstar.__version__)
        raise typer.Exit(0)

    return value


def common_callback(
    ctx: typer.Context,
    version: t.Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            is_eager=True,
            callback=version_callback,
            help=HELP_SHORT,
        ),
    ] = False,
) -> None:
    pass


P = t.ParamSpec("P")
A = t.TypeVar("A")


def cb_pipeline(
    *callbacks: Callable[[typer.Context, A], A],
) -> Callable[[typer.Context, A], A]:
    """Create a typer Callback function composed of a series of individual callbacks.

    Parameters
    ----------
    callbacks : Callable[[typer.Context, A], A]
        The sequence of callbacks to be executed

    Returns
    -------
    Callable[[typer.Context, A], A]
    """

    def _callback(ctx: typer.Context, value: A) -> A:
        """Callback wrapper that executes all specified callbacks."""
        for callback in callbacks:
            value = callback(ctx, value)

        return value

    return _callback


def set_flag(
    key: str,
    extra: BoolCallback | None = None,
) -> BoolCallback:
    """Create a typer Callback function that sets the value of an environment
    variable to the value of the argument supplied by the user.

    Parameters
    ----------
    key : str
        The environment variable to be set
    extra : StrCallback
        Additional logic called after setting the environment variable.

    Returns
    -------
    StrCallback
    """

    def _callback(ctx: typer.Context, value: bool) -> bool:
        """Callback that sets the specified environment variable to `FLAG_ON`
        if `value` is `True`.
        """
        if value:
            os.environ[key] = FLAG_ON

        if extra:
            value = extra(ctx, value)

        return value

    return _callback


def set_env(
    key: str,
    extra: StrCallback | None = None,
) -> StrCallback:
    """Create a typer Callback function that sets the value of an environment
    variable to the value of the argument supplied by the user.

    Parameters
    ----------
    key : str
        The environment variable to be set
    extra : StrCallback
        Additional logic called after setting the environment variable.

    Returns
    -------
    StrCallback
    """

    def _callback(ctx: typer.Context, value: str) -> str:
        """Callback that sets the specified environment variable to the specified value."""
        value = value.strip()
        os.environ[key] = value
        if extra:
            value = extra(ctx, value)
        return value

    return _callback


def update_loggers(ctx: typer.Context, value: str) -> str:
    """Perform a log-level reset on all loggers if the log level is updated via CLI."""
    reset_log_level(LogLevelChoices[value])
    return value


log_level_callback = cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers)


def path_callback(value: str | None) -> str | None:
    """Ensure a path that was provided by a user has been expanded and resolved.

    Returns
    -------
    Path
    """
    if value and not is_remote_resource(value):
        return Path(value).expanduser().resolve().as_posix()
    return value


def locate_app_modules() -> list[str]:
    """Return a list of absolute import strings where the module matches the
    application module naming standard of ending with `app.py`
    (e.g. cstar/applications/hello_app.py).

    Returns
    -------
    str
        The absolute import path, e.g. `cstar.applications.hello_app`
    """
    root = Path(__file__).parent.parent  # <path>/cstar
    location = root / "applications"
    apps = [
        f.relative_to(root.parent) for f in location.rglob("*app.py") if f.is_file()
    ]
    return [str(f.with_suffix("")).replace("/", ".") for f in apps]


def autoimport_apps(module_names: list[str]) -> None:
    for module_name in module_names:
        importlib.import_module(module_name)
