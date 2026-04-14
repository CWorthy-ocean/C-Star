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
from cstar.cli.workplan.shared import create_xrunner
from cstar.entrypoint.worker.worker import (
    SimulationStages,
    get_job_config,
    get_request,
    get_service_config,
)
from cstar.entrypoint.worker.worker import execute_runner as exec_romsmarbl_runner
from cstar.entrypoint.xrunner import XRunnerRequest
from cstar.execution.file_system import local_copy
from cstar.orchestration.application import ApplicationRegistry
from cstar.orchestration.models import Blueprint
from cstar.orchestration.serialization import deserialize, validate_serialized_entity
from cstar.system.registration import Registry

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
            bp_core = deserialize(local_path, Blueprint)

            reg_bp = Registry(ApplicationRegistry.BLUEPRINT)
            bp_type = reg_bp.get(bp_core.application)

            ctx.obj = deserialize(local_path, bp_type)

    except FileNotFoundError:
        raise typer.BadParameter(f"Blueprint file not found: {path}")
    except ValidationError as ex:
        raise typer.BadParameter(f"Blueprint file is malformed: {ex}")

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
            "--log-level",
            "-l",
            callback=log_level_callback,
            help="Set the log level for C-Star.",
            envvar=ENV_CSTAR_LOG_LEVEL,
        ),
    ] = LogLevelChoices.INFO,
    clobber: t.Annotated[
        bool,
        typer.Option(
            "--clobber",
            callback=clobber_callback,
            help="Clobber the working directory if it exists.",
            envvar=ENV_CSTAR_CLOBBER_WORKING_DIR,
        ),
    ] = False,
) -> None:
    """Execute a blueprint in a local worker service."""
    bp: Blueprint = ctx.obj
    application = bp.application
    runner_name = f"{application.capitalize()}Runner"
    bp_type = type(bp)

    job_cfg = get_job_config()
    service_cfg = get_service_config(
        get_env_item(ENV_CSTAR_LOG_LEVEL).value, name=runner_name
    )

    log.debug(f"Executing {application!r} blueprint in a {bp_type} service")

    validation_result_rm = validate_serialized_entity(uri, bp_type)
    if not validation_result_rm.is_valid:
        print(validation_result_rm.error_msg)
        return

    if application == "roms_marbl":
        # NOTE: temporary conditional to use old runner until it is converted to XRunner
        # TODO: stages must be moved to the blueprint instead of a CLI parameter
        rm_request = get_request(uri, stage)
        rc = asyncio.run(exec_romsmarbl_runner(job_cfg, service_cfg, rm_request))
    else:
        request = XRunnerRequest(uri, type(bp))

        runner = create_xrunner(request, service_cfg, job_cfg)
        result = asyncio.run(runner.execute_xrunner())

        if errors := list(result.errors):
            print(f"Errors occurred: {', '.join(errors)}")

        rc = len(errors)

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
