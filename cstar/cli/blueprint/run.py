import asyncio
import logging
import typing as t
from pathlib import Path

import typer

from cstar.entrypoint.worker.worker import (
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
) -> None:
    """Execute a blueprint in a local worker service."""
    print("Executing blueprint in a worker service")
    job_cfg = get_job_config()
    service_cfg = get_service_config(logging.DEBUG)
    request = get_request(path.as_posix())

    rc = asyncio.run(execute_runner(job_cfg, service_cfg, request))

    if rc:
        print("Blueprint execution failed")
        raise typer.Exit(code=rc)

    print("Blueprint execution completed")


if __name__ == "__main__":
    typer.run(run)
