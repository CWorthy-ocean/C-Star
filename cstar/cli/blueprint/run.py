import asyncio
import typing as t

import typer
from pydantic import ValidationError

from cstar.base.env import (
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    get_env_item,
)
from cstar.base.log import LogLevelChoices, get_logger
from cstar.cli.common import clobber_callback, log_level_callback
from cstar.cli.workplan.shared import create_xrunner, get_registered_bp
from cstar.entrypoint.utils import (
    ARG_CLOBBER,
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
)
from cstar.entrypoint.worker.worker import (
    SimulationStages,
    get_job_config,
    get_request,
    get_service_config,
)
from cstar.entrypoint.worker.worker import execute_runner as exec_romsmarbl_runner
from cstar.entrypoint.xrunner import XRunnerRequest
from cstar.execution.file_system import local_copy
from cstar.orchestration.models import Application, Blueprint
from cstar.orchestration.serialization import deserialize, validate_serialized_entity

app = typer.Typer()
log = get_logger(__name__)


def path_callback(
    ctx: typer.Context,
    path: str,
) -> str:
    """Validate the blueprint content after typer has parsed the path.

    Additionally, loads the blueprint and stores in the context for later use.

    Parameters
    ----------
    ctx : typer.Context
        The context of the command.
    path : str
        The path to the blueprint.

    Returns
    -------
    str
        The path to the blueprint.
    """
    try:
        with local_copy(path) as local_path:
            if not local_path.exists():
                msg = f"Blueprint not found at path: {path}"
                raise typer.BadParameter(msg)

            # use the core blueprint fields to identify the application type
            base_bp = deserialize(local_path, Blueprint)
            bp_type = get_registered_bp(base_bp.application)

            ctx.obj = bp_type(**base_bp.model_dump())

    except FileNotFoundError as ex:
        msg = f"Blueprint file not found: {path}"
        raise typer.BadParameter(msg) from ex
    except ValidationError as ex:
        msg = f"Blueprint file is malformed: {ex}"
        raise typer.BadParameter(msg) from ex

    return path


@app.command(name="run", help="Execute a blueprint in a local worker service.")
def run(
    ctx: typer.Context,
    uri: t.Annotated[
        str,
        typer.Argument(
            help="The URI (or path) to the blueprint to execute",
            callback=path_callback,
        ),
    ],
    stage: t.Annotated[
        list[SimulationStages] | None,
        typer.Option(
            help="The stages to execute. If not specified, all stages will be executed.",
            case_sensitive=False,
        ),
    ] = None,
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
    """Execute a blueprint in a local worker service."""
    bp = t.cast("Blueprint", ctx.obj)
    name = f"{bp.application}_runner"

    job_cfg = get_job_config()
    service_cfg = get_service_config(get_env_item(ENV_CSTAR_LOG_LEVEL).value, name=name)

    msg = f"Executing {bp.application!r} blueprint in a {type(bp)} service"
    log.debug(msg)

    result = validate_serialized_entity(uri, type(bp))
    if not result.is_valid:
        print(result.error_msg)
        return

    if bp.application == Application.ROMS_MARBL.value:
        # NOTE: temporary conditional to use old runner until it is converted to XRunner
        rm_request = get_request(uri, stage)
        rc = asyncio.run(exec_romsmarbl_runner(job_cfg, service_cfg, rm_request))
    else:
        request = XRunnerRequest(uri, type(bp))

        runner = create_xrunner(request, service_cfg, job_cfg)
        xresult = asyncio.run(runner.execute_xrunner())

        if errors := xresult.errors:
            print(f"Errors occurred: {', '.join(errors)}")

        rc = len(errors)

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
