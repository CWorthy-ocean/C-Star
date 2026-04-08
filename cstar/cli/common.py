import os
import typing as t

import typer

import cstar
from cstar.base.env import ENV_CSTAR_CLOBBER_WORKING_DIR, ENV_CSTAR_LOG_LEVEL

app = typer.Typer()

HELP_SHORT = "Print the current version of the C-Star package and exit."


def version_callback(value: bool) -> None:
    """Print the current version of the C-Star package and exit."""
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
        os.environ[ENV_CSTAR_CLOBBER_WORKING_DIR] = "1"
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
