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
from cstar.base.feature import is_flag_enabled
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import (
    MigrationRequest,
    cb_pipeline,
    console,
    execute_migration,
    format_validation_errors,
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
    ARG_VERBOSE,
    ARG_VERBOSE_HELP,
)
from cstar.execution.file_system import (
    DirectoryManager,
    is_remote_resource,
    write_local_copy,
)
from cstar.system.migration import (
    CstarMigrationError,
    CStarMigrationNotRegisteredError,
    CstarUnsupportedMigrationError,
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

    Parameters
    ----------
    value : str
        The value of the path parameter.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        If the path is empty, was not found, or could not be retrieved.
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


def target_callback(ctx: typer.Context, value: str) -> str:
    """Ensure the user provided a non-empty path and resolve conflicts with
    parameters processed earlier.

    An output path is reported as ignored when in-place or dry-run mode is
    requested; otherwise a pre-existing output file is removed when clobber
    is enabled, mitigating a FileExistsError.

    Resulting path has been expanded and resolved.

    Parameters
    ----------
    ctx : typer.Context
        The typer context object.
    value : str
        The value of the output parameter.

    Returns
    -------
    str
    """
    if not value or not value.strip():
        return value.strip()

    # combine each parameter with its environment variable: a flag enabled
    # only through the environment is processed after this callback and is
    # not yet present in ctx.params
    if ctx.params.get("in_place", False):
        console.print(f"Output path {value!r} will be ignored in in-place mode")
    elif ctx.params.get("dry_run", False) or is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN):
        console.print(f"Output path {value!r} will be ignored during dry-run")
    elif ctx.params.get("clobber", False) or is_flag_enabled(
        ENV_CSTAR_CLOBBER_WORKING_DIR
    ):
        path = Path(value)
        if path.exists():
            log.debug(f"Clobbering output path: {value}")
            path.unlink()
        else:
            log.debug(f"No output file to clobber: {value}")

    path = Path(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.as_posix()


def report_inplace_conflicts(in_place: bool, clobber: bool) -> None:
    """Cancel execution when the user attempts to clobber in-place.

    Parameters
    ----------
    in_place : bool
        The value of the in-place parameter.
    clobber : bool
        The value of the clobber parameter.

    Raises
    ------
    typer.BadParameter
        If in-place and clobber are both enabled.
    """
    if in_place and clobber:
        msg = "Clobbering in-place will result in loss of the input file. Cancelling."
        raise typer.BadParameter(msg)


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
        typer.Argument(
            help="Path where the migrated blueprint will be serialized",
            callback=target_callback,
            dir_okay=False,
            exists=False,
        ),
    ] = "",
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Generate the migration plan without executing it.",
            callback=set_flag(ENV_CSTAR_CLI_DRY_RUN),
            envvar=ENV_CSTAR_CLI_DRY_RUN,
        ),
    ] = False,
    in_place: t.Annotated[
        bool,
        typer.Option(
            "--inplace",
            help="Migrate the blueprint and replace the source file content.",
            is_eager=True,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help=ARG_VERBOSE_HELP,
            callback=set_flag(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
            is_eager=True,
        ),
    ] = False,
    clobber: t.Annotated[
        bool,
        typer.Option(
            ARG_CLOBBER,
            help=ARG_CLOBBER_HELP,
            callback=set_flag(ENV_CSTAR_CLOBBER_WORKING_DIR),
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
            is_eager=True,
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
    report_inplace_conflicts(in_place, clobber)

    try:
        target = Path(path) if in_place else (Path(output) if output else None)
        request = MigrationRequest(
            path=Path(path),
            output=target,
            in_place=in_place,
        )
    except ValidationError as ex:
        errors = format_validation_errors(ex)
        raise typer.BadParameter(errors)

    try:
        result = execute_migration(request)
    except CstarUnsupportedMigrationError as ex:
        msg = f"Unable to migrate blueprint: {str(path)!r}"
        log.exception(msg)
        raise typer.Exit(1) from ex
    except CStarMigrationNotRegisteredError as ex:
        msg = f"No schema migrations available for {str(path)!r}"
        log.exception(msg)
        raise typer.Exit(0) from ex
    except CstarMigrationError as ex:
        msg = "Migration failed"
        raise typer.BadParameter(msg) from ex

    if not result.migration_result.plan:
        console.print("Migration failed to produce a plan.")
        raise typer.Exit(2)

    if not result.migration_result.plan.is_latest:
        console.print(f"Migrated blueprint persisted to {str(result.target)!r}")
