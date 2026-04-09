import asyncio
import typing as t

import typer
from pydantic import BaseModel, Field, ValidationError

from cstar.base.env import (
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_LOG_LEVEL,
    get_env_item,
)
from cstar.base.log import LogLevelChoices
from cstar.cli.common import clobber_callback, log_level_callback
from cstar.entrypoint.worker.hello_app import (
    HelloWorldBlueprint,
    HelloWorldRunner,
    get_base_request,
)
from cstar.entrypoint.worker.hello_app import execute_runner as execute_runner_bp
from cstar.entrypoint.worker.worker import (
    SimulationStages,
    get_job_config,
    get_request,
    get_service_config,
)
from cstar.entrypoint.worker.worker import (
    execute_runner as execute_runner_rm,
)
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import (
    deserialize_discriminated,
)

app = typer.Typer()

AllBlueprints = RomsMarblBlueprint | HelloWorldBlueprint


class BlueprintDiscriminator(BaseModel):
    """A utility used to enable pydantic to deserialize an unknown
    blueprint based on the value of the discriminator field (application).
    """

    blueprint: t.Annotated[AllBlueprints, Field(discriminator="application")]


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
        loader = deserialize_discriminated(path, BlueprintDiscriminator, "blueprint")
    except FileNotFoundError:
        raise typer.BadParameter(f"Blueprint file not found: {path}")
    except ValidationError as ex:
        raise typer.BadParameter(f"Blueprint file is malformed: {ex}")

    ctx.obj = loader.blueprint

    return path


@app.command(name="run", help="Execute a blueprint in a local worker service.")
def run(
    ctx: typer.Context,
    path: t.Annotated[
        str,
        typer.Argument(
            help="The path to the blueprint to execute",
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
    job_cfg = get_job_config()
    service_cfg = get_service_config(get_env_item(ENV_CSTAR_LOG_LEVEL).value)

    bp: AllBlueprints = ctx.obj
    application = bp.application

    print(f"Executing {application!r} blueprint in a worker service")

    if application == "roms_marbl":
        # validation_result_rm = validate_serialized_entity(path, RomsMarblBlueprint)
        # if not validation_result_rm.is_valid:
        #     print(validation_result_rm.error_msg)
        #     return

        rm_request = get_request(path, stage)
        rc = asyncio.run(execute_runner_rm(job_cfg, service_cfg, rm_request))
    elif application == "hello_world":
        hw_request = get_base_request(path)
        # validation_result_hw = validate_serialized_entity(path, HelloWorldBlueprint)
        # if not validation_result_hw.is_valid:
        #     print(validation_result_hw.error_msg)
        #     return

        bp_result = asyncio.run(
            execute_runner_bp(HelloWorldRunner, job_cfg, service_cfg, hw_request)
        )
        rc = 0 if not bp_result.errors else 1

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
