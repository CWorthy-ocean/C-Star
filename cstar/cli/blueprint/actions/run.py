import argparse
import asyncio
import typing as t
from multiprocessing import Process
from pathlib import Path

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.entrypoint.service import ServiceConfiguration
from cstar.entrypoint.worker.worker import BlueprintRequest, JobConfig, SimulationRunner


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
    account_id = "m4632"
    walltime = "48:00:00"

    request = BlueprintRequest(path.as_posix())
    service_config = ServiceConfiguration(
        loop_delay=0, health_check_frequency=300, health_check_log_threshold=25
    )
    job_config = JobConfig(account_id, walltime)

    return SimulationRunner(request, service_config, job_config)


def run_worker(path: Path) -> None:
    """Execute a blueprint synchronously using a worker service.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    print("Executing blueprint via blocking worker service.")
    runner = configure_simulation_runner(path)
    asyncio.run(runner.execute())


async def run_worker_process(path: Path) -> None:
    """Execute a blueprint in a non-blocking worker service.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    print("Executing blueprint via non-blocking worker service.")

    process = Process(target=run_worker, kwargs={"path": path}, daemon=True)
    process.start()

    # delay briefly in case the process dies during startup
    await asyncio.sleep(4)

    print(f"Worker process started in PID: {process.pid}")
    if process.exitcode:
        print(f"Worker processs failed prematurely with code: {process.exitcode}")


async def handle(ns: argparse.Namespace) -> None:
    """The action handler for the blueprint-run action.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    if ns.blocking:
        run_worker(ns.path)
    else:
        await run_worker_process(ns.path)


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the blueprint-run command into the CLI.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI

    Returns
    -------
    RegistryResult
        A 2-tuple containing ((command name, action name), parser function)
    """
    command: t.Literal["blueprint"] = "blueprint"
    action: t.Literal["run"] = "run"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar blueprint run path/to/blueprint.yaml`"""
        parser = sp.add_parser(
            action,
            help="Execute a blueprint",
            description="Path to the blueprint (YAML)",
        )
        parser.add_argument(
            dest="path",
            help="Path to the blueprint (YAML)",
            action=PathConverterAction,
        )
        parser.add_argument(
            "-b",
            "--blocking",
            action="store_true",
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
