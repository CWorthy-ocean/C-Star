import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.cli.blueprint.check import check
from cstar.entrypoint.worker.worker import (
    SimulationStages,
    execute_runner,
    get_job_config,
    get_request,
    get_service_config,
)

app = typer.Typer()


@app.command()
def run(
    path: t.Annotated[
        Path, typer.Argument(help="The path to the blueprint to execute")
    ],
    stage: t.Annotated[
        list[SimulationStages] | None,
        typer.Option(
            help="The stages to execute. If not specified, all stages will be executed.",
            case_sensitive=False,
        ),
    ] = None,
) -> None:
    """Execute a blueprint in a local worker service."""
    if not check(path):
        return

    print("Executing blueprint in a worker service")
    job_cfg = get_job_config()
    service_cfg = get_service_config(get_env_item(ENV_CSTAR_LOG_LEVEL).value)
    request = get_request(path.as_posix(), stage)

    rc = asyncio.run(execute_runner(job_cfg, service_cfg, request))

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
