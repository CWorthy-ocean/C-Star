import os
import typing as t

import typer

import cstar
from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    FLAG_ON,
)

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
