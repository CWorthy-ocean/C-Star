import asyncio
import os
import typing as t
from pathlib import Path

import typer
from pydantic import BaseModel, Field

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.cli.workplan.shared import (
    check_and_capture_kvps,
    list_runs,
    upsert_command_context,
)
from cstar.execution.file_system import local_copy
from cstar.orchestration.dag_runner import build_and_run_dag
from cstar.orchestration.models import NamedConfigurationBuilder, Workplan
from cstar.orchestration.serialization import deserialize, validate_serialized_entity
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

app = typer.Typer()
log = get_logger(__name__)


class RunContext(BaseModel):
    user_variables: t.Mapping[str, str] = Field(default_factory=dict)
    workplan: Workplan | None = None
    workplan_source: str | Path | None = None

    def compute_user_variables(self) -> t.Mapping[str, str]:
        if self.workplan is None:
            msg = f"Unable to load workplan from {self.workplan_source}"
            raise typer.BadParameter(msg)

        config_builder = NamedConfigurationBuilder(
            keys=set(self.workplan.runtime_vars),
            mapping=self.user_variables,
            require_coverage=False,
            require_declaration=True,
        )
        if error := config_builder.error:
            raise typer.BadParameter(error)

        return config_builder.mapping


def preprocess_vars(
    ctx: typer.Context,
    var: list[str],
) -> list[str] | None:
    """Perform validation and formatting on user-supplied runtime variables.

    Places the processed variables into a `RunContext` object stored
    in the user data slot of the typer context.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    var : list[str]
        A list of key-value pairs supplied by a user.

    Returns
    -------
    list[str] | None
        The original input with leading/trailing whitespace stripped from the keys
        and values.

    Raises
    ------
    ValueError
    """
    if not var:
        return None

    varfile = ctx.params.get("varfile", None)

    if var is not None and varfile is not None:
        msg = "`--var` and `--varfile` must not be supplied together"
        raise typer.BadParameter(msg)

    try:
        mapping = check_and_capture_kvps(var)
        _ = upsert_command_context(ctx, RunContext, user_variables=mapping)

    except ValueError as ex:
        log.exception("Runtime variables failed validation")
        raise typer.BadParameter(ex.args[0]) from ex

    return var


def preprocess_varfile(
    ctx: typer.Context,
    varfile: Path | None,
) -> Path | None:
    """Perform validation and formatting on user-supplied runtime variables
    supplied through a path to a variables file.

    Places the processed variables into a `RunContext` object stored
    in the user data slot of the typer context.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    varfile : Path | None
        A path to a file containing the variable configuration

    Returns
    -------
    Path | None

    Raises
    ------
    typer.BadParameter
        - If `--var` and `--varfile` are supplied simultaneously
        - If the source file does not exist
    """
    if varfile is None:
        return None

    var = ctx.params.get("var", None)

    if varfile is not None and var is not None:
        msg = "`--var` and `--varfile` must not be supplied together"
        raise typer.BadParameter(msg)

    try:
        if isinstance(varfile, str):
            varfile = Path(varfile)

        if not varfile.exists():
            msg = f"File not found at: {varfile}"
            raise typer.BadParameter(msg)

        with varfile.open("r") as fp:
            lines = [x.strip() for x in fp.readlines() if x.strip()]

        mapping = check_and_capture_kvps(lines)
        _ = upsert_command_context(ctx, RunContext, user_variables=mapping)

    except ValueError as ex:
        log.exception("Runtime variables failed validation")
        raise typer.BadParameter(ex.args[0]) from ex

    return varfile


def preprocess_runid(ctx: typer.Context, run_id: str) -> str:
    """Perform validation and formatting of the run-id argument.

    - verify blueprint path or run-id is supplied
    - generate a default run id from a blueprint if run-id is not supplied

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    run_id : str
        The user-supplied run-id

    Returns
    -------
    str
    """
    if run_id is not None:
        if run_id := run_id.strip():
            return run_id

        msg = "An empty run-id was supplied"
        raise typer.BadParameter(msg)

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


def preprocess_path(ctx: typer.Context, path: str | None) -> str | None:
    """Perform validation related to the workplan path.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    path : str | None
        The user-supplied path to a workplan file.

    Returns
    -------
    str
    """
    if path:
        try:
            with local_copy(path) as local_path:
                if not local_path.exists():
                    msg = f"Workplan not found at path: {path}"
                    raise typer.BadParameter(msg)

                validation_result = validate_serialized_entity(local_path, Workplan)
                if not validation_result.item:
                    msg = f"The workplan file in `{path}` is improperly formatted"
                    raise typer.BadParameter(msg)

                _ = upsert_command_context(
                    ctx,
                    RunContext,
                    workplan_source=path,
                    workplan=validation_result.item,
                )

        except FileNotFoundError as ex:
            msg = f"Workplan not found at path: {path}"
            raise typer.BadParameter(msg) from ex

    return path


@app.command()
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
    var: t.Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Specify 0-to-many replacements as key-value pairs in "
                "the form `<key>=<value>`."
            ),
            callback=preprocess_vars,
        ),
    ] = None,
    varfile: t.Annotated[
        Path | None,
        typer.Option(
            help=(
                "Specify the path to a file containing one replacements per line "
                "as key-value pairs in the form `<key>=<value>`."
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
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    if not path:
        repo = TrackingRepository()
        wp_run = asyncio.run(repo.get_workplan_run(run_id))
        if wp_run is None:
            msg = f"No runs with the id `{run_id}` could be found."
            raise typer.BadParameter(msg)

        # if var is None and varfile is None and wp_run.runtime_vars:
        #     # assume user is re-running when no variables are passed
        #     log.info(f"Reusing runtime variables from prior run")
        #     runtime_vars = wp_run.runtime_vars

        # ensure the environment matches the prior run
        os.environ.update(wp_run.environment)

        path = str(wp_run.workplan_path)
        msg = f"Re-starting run-id `{run_id}` with workplan at `{path}`"
        log.info(msg)

        run_context = upsert_command_context(
            ctx,
            RunContext,
            workplan_source=wp_run.workplan_path,
            workplan=deserialize(wp_run.trx_workplan_path, Workplan),
        )
    else:
        run_context = upsert_command_context(ctx, RunContext)

    if run_context.workplan is None:
        msg = f"Unable to load workplan from {path}"
        raise typer.BadParameter(msg)

    user_variables = run_context.compute_user_variables()

    try:
        with local_copy(path) as wp_path:
            asyncio.run(
                build_and_run_dag(
                    wp_path,
                    run_id,
                    user_variables=user_variables,
                ),
            )
    except CstarExpectationFailed as ex:
        msg = f"An invalid request was made: {ex}"
        print(msg)
        raise typer.Exit(1) from ex
    except Exception as ex:
        msg = f"Workplan run `{run_id}` has completed unsuccessfully"
        print(msg)
        raise typer.Exit(2) from ex
    else:
        print(f"Workplan run `{run_id}` has completed")


if __name__ == "__main__":
    typer.run(run)
