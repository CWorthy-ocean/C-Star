import typing as t
from pathlib import Path

import typer
from pydantic import ValidationError

from cstar.base.env import (
    ENV_CSTAR_CLI_DRY_RUN,
    ENV_CSTAR_CLI_VERBOSE,
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
)
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import (
    MigrationRequest,
    cb_pipeline,
    execute_migration,
    print_validation_errors,
    set_env,
    set_flag,
    update_loggers,
)
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_CLOBBER_HELP,
    ARG_DRY_RUN,
    ARG_LOGLEVEL_HELP,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_OUTPUT_LONG,
    ARG_OUTPUT_SHORT,
    ARG_VERBOSE,
    ARG_VERBOSE_HELP,
)
from cstar.execution.file_system import (
    DirectoryManager,
    is_remote_resource,
    write_local_copy,
)

app = typer.Typer()
log = get_logger(__name__)

CMD_NAME: t.Final[str] = "migrate"
HELP_SHORT = "Migrate the schema of a blueprint file."
HELP_LONG = f"""\
{HELP_SHORT}

The schema will be updated to the latest available version. If an output
path is not provided, it will be written next to the existing
file with the version number appended to the file name.
"""


def path_callback(value: str) -> str:
    """Ensure the user provided a non-empty path.

    Resulting path has been expanded and resolved.

    Returns
    -------
    Path
    """
    value = value.strip() if value else ""

    if not value:
        msg = "path is an empty string."
        raise typer.BadParameter(msg)

    if not is_remote_resource(value):
        path = Path(value).expanduser().resolve()
        if not path.exists():
            msg = f"{str(path)!r} was not found"
            raise typer.BadParameter(msg)
        return path.as_posix()

    try:
        state_dir = DirectoryManager.state_home()
        local_path = write_local_copy(value, state_dir)
    except FileNotFoundError as ex:
        msg = f"{ex.strerror}: {ex.filename!r}"
        raise typer.BadParameter(msg) from ex
    else:
        msg = f"Remote file retrieved into: {local_path.as_posix()!r}"
        log.debug(msg)
        return local_path.as_posix()


def target_callback(value: str) -> str:
    """Ensure the user provided a non-empty path.

    Resulting path has been expanded and resolved.

    Returns
    -------
    Path
    """
    if not value or not value.strip():
        return value.strip()

    path = Path(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.as_posix()


def dryrun_notify(ctx: typer.Context, value: bool) -> bool:
    """Display informational message to user if a parameter conflict is found."""
    output = ctx.params.get("output", "")

    if value and output:
        print(f"Output path {output!r} will be ignored during dry-run")

    return value


def clobber_output(ctx: typer.Context, value: bool) -> bool:
    """Callback for clobber parameter that removes a pre-existing output file
    and mitigates a FileExistsError.
    """
    output = ctx.params.get("output", "")
    if value and output:
        path = Path(output)
        if path.exists():
            path.unlink()

    return value


@app.command(name=CMD_NAME, help=HELP_LONG, short_help=HELP_SHORT)
def migrate(
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a file containing a serialized blueprint.",
            callback=path_callback,
        ),
    ],
    output: t.Annotated[
        str,
        typer.Option(
            ARG_OUTPUT_LONG,
            ARG_OUTPUT_SHORT,
            help="Path where the migrated blueprint will be serialized",
            callback=target_callback,
        ),
    ] = "",
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Generate the migration plan without executing it.",
            callback=cb_pipeline(
                set_flag(ENV_CSTAR_CLI_DRY_RUN),
                dryrun_notify,
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
    clobber: t.Annotated[
        bool,
        typer.Option(
            ARG_CLOBBER,
            help=ARG_CLOBBER_HELP,
            callback=cb_pipeline(
                set_flag(ENV_CSTAR_CLOBBER_WORKING_DIR),
                clobber_output,
            ),
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
        ),
    ] = False,
    log_level: t.Annotated[
        LogLevelChoices,
        typer.Option(
            ARG_LOGLEVEL_LONG,
            ARG_LOGLEVEL_SHORT,
            callback=cb_pipeline(set_env(ENV_CSTAR_LOG_LEVEL), update_loggers),
            help=ARG_LOGLEVEL_HELP,
            envvar=ENV_CSTAR_LOG_LEVEL,
            is_eager=True,
        ),
    ] = LogLevelChoices.INFO,
) -> None:
    """Migrate the schema of an old blueprint to the latest version."""
    global log
    log = get_logger(__name__)

    try:
        request = MigrationRequest(
            path=Path(path),
            output=Path(output) if output else None,
        )
    except ValidationError as ex:
        print_validation_errors(ex)
        raise typer.Exit(1) from ex

    result = execute_migration(request)
    if not result.result.plan:
        print("Migration failed to produce a plan.")

    print(f"Migrated blueprint persisted to {str(result.target)!r}")
