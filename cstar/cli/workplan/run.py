import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_CLOBBER_WORKING_DIR, ENV_CSTAR_LOG_LEVEL
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import clobber_callback, log_level_callback
from cstar.cli.workplan.shared import (
    check_and_capture_kvps,
    list_runs,
)
from cstar.entrypoint.worker.worker import (
    ARG_CLOBBER,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
)
from cstar.execution.file_system import local_copy
from cstar.orchestration.dag_runner import build_and_run_dag
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import validate_serialized_entity
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

app = typer.Typer()
log = get_logger(__name__)

HELP_SHORT = "Execute a workplan."
HELP_LONG = f"""\
{HELP_SHORT}

Specify a previously used `run_id` to re-start a prior run.
"""


def preprocess_vars(
    ctx: typer.Context,
    user_variables: list[str],
) -> list[str] | None:
    """Perform validation and formatting on user-supplied variables.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    user_variables : list[str]
        A list of key-value pairs supplied by a user.

    Returns
    -------
    list[str] | None
        The original input with leading/trailing whitespace stripped from the keys
        and values.

    Raises
    ------
    typer.BadParameter
        - If the key-value pair does not meet `key=value` convention
    """
    if not user_variables:
        return None

    ctx.obj = check_and_capture_kvps(user_variables)

    return user_variables


def preprocess_varfile(
    ctx: typer.Context,
    user_varfile_path: Path | None,
) -> Path | None:
    """Perform validation and formatting on user-supplied variables
    supplied through a path to a variables file.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    user_varfile_path : Path | None
        A path to a file containing the variable configuration.

    Returns
    -------
    Path | None

    Raises
    ------
    typer.BadParameter
        - If the source file does not exist
        - If any individual key-value pair is malformed
    """
    if user_varfile_path is None:
        return None

    with user_varfile_path.open("r") as fp:
        lines = [x.strip() for x in fp.readlines() if x.strip()]

    ctx.obj = check_and_capture_kvps(lines)

    return user_varfile_path


def preprocess_runid(ctx: typer.Context, run_id: str) -> str:
    """Perform validation and formatting of the run-id argument.

    - verify blueprint path or run-id is supplied
    - generate a default run id from a blueprint if run-id is not supplied

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    run_id : str
        The user-supplied run-id.

    Returns
    -------
    str
    """
    if run_id := run_id.strip():
        return run_id

    path: str | None = ctx.params.get("path", None)
    if isinstance(path, str):
        path = path.strip()

    if not path:
        msg = "A run-id or workplan path is required"
        raise typer.BadParameter(msg)

    run_id = WorkplanRun.get_default_run_id(path)

    msg = f"Generated a default run-id `{run_id}` from `{path}`"
    log.debug(msg)

    return run_id


def preprocess_path(workplan_path: str | None) -> str | None:
    """Perform validation related to the workplan path.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    workplan_path : str | None
        The user-supplied path to a workplan file.

    Returns
    -------
    str
    """
    if workplan_path:
        try:
            with local_copy(workplan_path) as local_path:
                if not local_path.exists():
                    msg = f"Workplan not found at path: {workplan_path}"
                    raise typer.BadParameter(msg)

                validation_result = validate_serialized_entity(local_path, Workplan)
                if not validation_result.item:
                    msg = f"The workplan file in `{workplan_path}` is improperly formatted"
                    raise typer.BadParameter(msg)

        except FileNotFoundError as ex:
            msg = f"Workplan not found at path: {workplan_path}"
            raise typer.BadParameter(msg) from ex

    return workplan_path


def handle_run_reloading(run_id: str) -> str:
    """Locate a prior run for the run ID and update the `RunCmdContext` with
    the correct `Workplan`.

    Parameters
    ----------
    run_id : str
        The run-id to reload
    """
    repo = TrackingRepository()
    wp_run = asyncio.run(repo.get_workplan_run(run_id))
    if wp_run is None:
        msg = f"No runs with the id `{run_id}` could be found."
        raise typer.BadParameter(msg)

    # ensure the environment matches the prior run
    os.environ.update(wp_run.environment)

    path = wp_run.workplan_path
    msg = f"Re-starting run-id `{run_id}` with workplan originating in `{path}`"
    log.info(msg)

    return wp_run.trx_workplan_path.as_posix()


@app.command(name="run", help=HELP_LONG, short_help=HELP_SHORT)
def run(
    ctx: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            help="The unique identifier for an execution of the workplan.",
            autocompletion=list_runs,
            callback=preprocess_runid,
        ),
    ] = "",
    user_variables: t.Annotated[
        list[str] | None,
        typer.Option(
            "--var",
            "-v",
            help=(
                "Specify 0-to-many replacements as key-value pairs in "
                "the form `key=value`."
            ),
            callback=preprocess_vars,
        ),
    ] = None,
    user_variables_path: t.Annotated[
        Path | None,
        typer.Option(
            "--varfile",
            "-f",
            help=(
                "Specify the path to a file containing one replacements per line "
                "as key-value pairs in the form `key=value`."
            ),
            callback=preprocess_varfile,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    path: t.Annotated[
        str,
        typer.Argument(
            help="Path to a workplan file.",
            callback=preprocess_path,
        ),
    ] = "",
    dry_run: t.Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Generate the execution plan without executing the workplan.",
        ),
    ] = False,
    log_level: t.Annotated[
        LogLevelChoices,
        typer.Option(
            ARG_LOGLEVEL_LONG,
            ARG_LOGLEVEL_SHORT,
            callback=log_level_callback,
            help="Set the log level for C-Star.",
            envvar=ENV_CSTAR_LOG_LEVEL,
        ),
    ] = LogLevelChoices.INFO,
    clobber: t.Annotated[
        bool,
        typer.Option(
            ARG_CLOBBER,
            callback=clobber_callback,
            help="Clobber the working directory if it exists.",
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
        ),
    ] = False,
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    if user_variables is not None and user_variables_path is not None:
        msg = "`--var` and `--varfile` must not be supplied together"
        raise typer.BadParameter(msg)

    if not path:
        path = handle_run_reloading(run_id)

    try:
        with local_copy(path) as wp_path:
            asyncio.run(
                build_and_run_dag(
                    wp_path,
                    run_id,
                    user_variables=t.cast("t.Mapping[str, str]", ctx.obj),
                    dry_run=dry_run,
                ),
            )
    except CstarExpectationFailed as ex:
        msg = f"An invalid request was made: {ex}"
        print(msg)
        raise typer.Exit(1) from ex
    except ValueError as ex:
        raise typer.BadParameter(ex.args[0]) from ex
    except Exception as ex:
        msg = f"Workplan run `{run_id}` has completed unsuccessfully: {ex}"
        print(msg)
        raise typer.Exit(3) from ex
    else:
        print(f"Workplan run `{run_id}` has completed")


if __name__ == "__main__":
    typer.run(run)
