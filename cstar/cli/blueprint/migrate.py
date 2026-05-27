import typing as t
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_CLI_VERBOSE
from cstar.cli.common import (
    cb_pipeline,
    execute_migration,
    set_env,
    set_flag,
)
from cstar.entrypoint.utils import (
    ARG_DRY_RUN,
    ARG_OUTPUT_LONG,
    ARG_OUTPUT_SHORT,
    ARG_VERBOSE,
    ARG_VERBOSE_HELP,
)
from cstar.execution.file_system import is_remote_resource

app = typer.Typer()


HELP_SHORT = "Migrate the schema of a blueprint file."
HELP_LONG = f"""\
{HELP_SHORT}

The schema will be updated to the latest available version. If an output
path is not provided, it will be written next to the existing
file with the version number appended to the file name.
"""


def path_callback(value: str | None) -> str | None:
    """Ensure a path that was provided by a user has been expanded and resolved.

    Returns
    -------
    Path
    """
    if value and not is_remote_resource(value):
        return Path(value).expanduser().resolve().as_posix()
    return value


def migrate_dryrun_callback(ctx: typer.Context, value: bool | None) -> bool | None:
    """Display informational message to user if a parameter conflict is found."""
    output: str | None = ctx.params.get("output", None)

    if value is not None and value and output is not None:
        print(f"Ignoring output path {output!r} during dry-run")

    return value


@app.command(name="migrate", help=HELP_LONG, short_help=HELP_SHORT)
def migrate(
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a blueprint file.",
            callback=path_callback,
        ),
    ],
    output: t.Annotated[
        str | None,
        typer.Option(
            ARG_OUTPUT_LONG,
            ARG_OUTPUT_SHORT,
            help="Path to the output file",
            callback=path_callback,
        ),
    ] = None,
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Generate the migration plan without executing it.",
            callback=cb_pipeline(
                set_env(ENV_CSTAR_CLI_DRY_RUN),
                migrate_dryrun_callback,
            ),
            envvar=ENV_CSTAR_CLI_DRY_RUN,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help=ARG_VERBOSE_HELP,
            callback=set_flag(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Migrate the schema of an old blueprint to the latest version."""
    execute_migration(path, output, dry_run)
