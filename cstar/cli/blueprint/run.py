import asyncio
import typing as t

import typer

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.log import LogLevelChoices
from cstar.cli.common import log_level_callback
from cstar.entrypoint.worker.worker import (
    SimulationStages,
    execute_runner,
    get_job_config,
    get_request,
    get_service_config,
)
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import validate_serialized_entity

app = typer.Typer()


@app.command(name="run", help="Execute a blueprint in a local worker service.")
def run(
    path: t.Annotated[
        str,
        typer.Argument(help="The path to the blueprint to execute"),
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
) -> None:
    """Execute a blueprint in a local worker service."""
    result = validate_serialized_entity(path, RomsMarblBlueprint)
    if not result.is_valid:
        print(result.error_msg)
        return

    print("Executing blueprint in a worker service")
    job_cfg = get_job_config()
    service_cfg = get_service_config(get_env_item(ENV_CSTAR_LOG_LEVEL).value)
    request = get_request(path, stage)

    rc = asyncio.run(execute_runner(job_cfg, service_cfg, request))

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
