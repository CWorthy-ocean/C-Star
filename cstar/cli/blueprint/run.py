import os
import typing as t
from multiprocessing import Process
from pathlib import Path
from time import sleep

import typer

from cstar.entrypoint.service import ServiceConfiguration
from cstar.entrypoint.worker.worker import BlueprintRequest, JobConfig, SimulationRunner

app = typer.Typer()


def configure_simulation_runner(path: Path) -> SimulationRunner:
    """Create a `SimulationRunner` to execute the blueprint.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute

    Returns
    -------
    SimulationRunner
        A simulation runner configured to execute the blueprint
    """
    account_id = os.getenv("CSTAR_SLURM_ACCOUNT", "")
    walltime = os.getenv("CSTAR_SLURM_WALLTIME", "48:00:00")

    request = BlueprintRequest(path.as_posix())
    print(f"Configured request: {request}")

    service_config = ServiceConfiguration(
        loop_delay=0, health_check_frequency=300, health_check_log_threshold=25
    )
    print(f"Configured service: {service_config}")

    job_config = JobConfig(account_id, walltime)
    print(f"Job configuration: {job_config}")

    return SimulationRunner(request, service_config, job_config)


@app.command()
def run(
    path: t.Annotated[
        Path, typer.Argument(help="The path to the blueprint to execute")
    ],
) -> None:
    """Execute a blueprint in a local worker service."""
    print("Executing blueprint in a worker service.")
    runner = configure_simulation_runner(path)

    process = Process(target=runner.execute, kwargs={"path": path})
    process.start()

    # delay briefly in case the process dies during startup
    sleep(1)

    print(f"Worker process `{process.pid}`")
    if process.exitcode:
        print(f"Worker processs failed prematurely with code `{process.exitcode}`")
