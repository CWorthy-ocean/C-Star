from collections.abc import Callable
import importlib
import os
import typing as t
from collections.abc import Callable
from pathlib import Path

import typer

import cstar
from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    FLAG_ON,
)
from cstar.base.log import LogLevelChoices, reset_log_level

app = typer.Typer()

HELP_SHORT = "Print the current version of the C-Star package and exit."


def version_callback(value: bool) -> bool:
    """Print the version of C-Star and exit."""
    if value:
        typer.echo(cstar.__version__)
        raise typer.Exit()

    return value


def log_level_callback(value: str) -> str:
    """Set the log level in the environment for C-Star."""
    value = value.strip().upper()
    if value:
        os.environ[ENV_CSTAR_LOG_LEVEL] = value

    return value


def clobber_callback(value: bool) -> bool:
    """Callback for the clobber option."""
    if value:
        os.environ[ENV_CSTAR_CLOBBER_WORKING_DIR] = FLAG_ON
    return value


def verbose_callback(value: bool) -> bool:
    """Callback for the verbose option."""
    if value:
        os.environ[ENV_CSTAR_CLI_VERBOSE] = FLAG_ON
    return value


def dryrun_callback(ctx: typer.Context, value: bool | None) -> bool | None:
    """Callback for the dry-run option."""
    if value:
        os.environ[ENV_CSTAR_CLI_DRY_RUN] = FLAG_ON
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


def update_loggers(ctx: typer.Context, value: str) -> str:
    """Perform a log-level reset on all loggers if the log level is updated via CLI."""
    reset_log_level(LogLevelChoices[value])
    return value


log_level_callback = cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers)


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
