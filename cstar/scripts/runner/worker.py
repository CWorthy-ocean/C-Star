import argparse
import asyncio
import dataclasses as dc
import logging
import pathlib
import shutil
import sys
from datetime import datetime, timezone
from typing import override

from cstar import Simulation
from cstar.base.exceptions import BlueprintError, CstarException
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.roms import ROMSSimulation
from cstar.scripts.service import Service, ServiceConfiguration

CSTAR_USER_ENV_PATH = "~/.cstar.env"
CSTAR_EXTERNALS_ROOT = "~/code/cstar/cstar/externals"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def create_parser() -> argparse.ArgumentParser:
    """Creates a parser for command line arguments expected by the c-star Worker."""
    parser = argparse.ArgumentParser(
        description="Run a c-star simulation.",
    )
    parser.add_argument(
        "--blueprint-uri",
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        required=False,
        help="Logging level for the simulation.",
        choices=[
            logging._levelToName[i]
            for i in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
        ],
    )
    parser.add_argument(
        "--output-dir",
        default="~/code/cstar/examples/",
        type=str,
        required=False,
        help="Local path to write simulation outputs to",
    )
    parser.add_argument(
        "--start-date",
        default="2012-01-03 12:00:00",
        type=str,
        required=False,
        help=f"The date on which to begin the simulation, formatted as `{DATE_FORMAT}`",
    )
    parser.add_argument(
        "--end-date",
        default="2012-01-04 12:00:00",
        type=str,
        required=False,
        help=f"The date on which to end the simulation, formatted as `{DATE_FORMAT}`",
    )
    return parser


def get_unique_path(root_path: pathlib.Path) -> pathlib.Path:
    """Create a unique path name to avoid collisions."""
    current_time = datetime.now(timezone.utc)
    return root_path / f"{current_time.strftime('%Y%m%d_%H%M%S')}"


@dc.dataclass
class BlueprintRequest:
    """Represents a request to run a c-star simulation."""

    blueprint_uri: str = dc.field(
        metadata={"description": "The path to the blueprint."},
    )
    output_dir: pathlib.Path = dc.field(
        metadata={"description": "The directory to write simulation outputs to"},
    )
    start_date: datetime = dc.field(
        metadata={"description": "The date on which to begin the simulation"},
    )
    end_date: datetime = dc.field(
        metadata={"description": "The date on which to end the simulation"},
    )


class SimulationRunner(Service):
    """Worker class to run c-star simulations."""

    def __init__(self, request: BlueprintRequest, service_cfg: ServiceConfiguration):
        """Initialize the worker with a request."""
        super().__init__(service_cfg)

        self._blueprint_uri = request.blueprint_uri
        self._output_root = request.output_dir.expanduser()
        self._output_dir = get_unique_path(self._output_root)
        self._simulation: Simulation = ROMSSimulation.from_blueprint(
            blueprint=self._blueprint_uri,
            directory=self._output_dir,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        self._handler: ExecutionHandler | None = None
        self._simulation.interactive = False
        # TODO: get this from the cstar env
        self._user_env_path = pathlib.Path(CSTAR_USER_ENV_PATH).expanduser()
        # TODO: get this from the cstar env
        self._externals_path = pathlib.Path(CSTAR_EXTERNALS_ROOT).expanduser()

    def _prepare_file_system(self) -> None:
        """Clean up old directories to avoid collisions and create new locations for the
        new run.

        NOTE: this may be unnecessary if the worker starts
        up a new container w/fresh directories every time.
        """
        # a leftover .cstar.env cause empty repo/compilation errors; remove it.
        if self._user_env_path.exists():
            self._user_env_path.unlink()

        # a leftover root_dir may have files in it, breaking download; remove it.
        if self._output_root.exists():
            shutil.rmtree(self._output_root)

        # leftover external code folder causes non-empty repo errors; remove it.
        if self._externals_path.exists():
            shutil.rmtree(self._externals_path)
        self._externals_path.mkdir(parents=True, exist_ok=False)

        # create a clean location to write outputs.
        if not self._output_dir.exists():
            self._output_dir.mkdir(parents=True, exist_ok=True)

    def _log_disposition(self) -> None:
        """Helper method to log the status of the simulation at time of shutdown."""
        disposition: ExecutionStatus = (
            self._handler.status if self._handler else ExecutionStatus.UNKNOWN
        )

        if self._handler:
            self.log.info(f"Completed simulation logs at: {self._handler.output_file}")

        if disposition == ExecutionStatus.COMPLETED:
            self.log.info("Simulation completed successfully.")
        elif disposition == ExecutionStatus.FAILED:
            self.log.error("Simulation failed.")
        else:
            self.log.warning(f"Simulation ended with status: {disposition}")

    @override
    def _on_start(self) -> None:
        """Prepare the simulation for execution by verifying the simulation loaded
        properly, configuring the file system, retrieving remote resources, and building
        third-party codebases."""
        self._prepare_file_system()

        if self._blueprint_uri is None:
            raise BlueprintError("No blueprint URI provided")

        if self._simulation is None:
            raise BlueprintError(f"Unable to load the blueprint: {self._blueprint_uri}")

        try:
            self._simulation.setup()
            self._simulation.build()
            self._simulation.pre_run()
        except ValueError as ex:
            raise CstarException("Failed to prepare simulation") from ex

    @override
    def _on_shutdown(self) -> None:
        """Perform shutdown of the worker service and log the final disposition of the
        simulation."""
        if not self._simulation:
            self.log.warning("No simulation available at shutdown")
            return

        # Ensure simulation status has been logged (handler updates may be suppressed)
        self._log_disposition()

    @override
    def _on_iteration(self) -> None:
        """Execute the c-star simulation."""

        try:
            self._handler = self._simulation.run()
        except Exception:
            logging.exception("An error occurred while running the simulation")

    @override
    def _on_iteration_complete(self) -> None:
        """Perform post-simulation behaviors."""
        # Check for updates... handler.updates is blocking w/0
        # - consider allowing main iteration to loop for more control of output
        if self._handler:
            self._handler.updates(0, interactive=False)

        self._simulation.post_run()

    @override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown."""
        if self._simulation is None:
            self.log.error("Simulation is not set. Allowing shutdown.")
            return True

        if not self._handler:
            self.log.error("Execution handler is not set. Allowing shutdown.")
            return True

        if self._handler.status != ExecutionStatus.RUNNING:
            self.log.info("Simulation is no longer running. Allowing shutdown.")
            return True

        return False


def get_service_config(args: argparse.Namespace) -> ServiceConfiguration:
    """Create a ServiceConfiguration instance using CLI arguments to configure the
    behavior of the service."""
    return ServiceConfiguration(
        health_check_frequency=60,
        log_level=logging._nameToLevel[args.log_level],
        name="SimulationRunner",
    )


def config_from_args(args: argparse.Namespace) -> BlueprintRequest:
    """Creates a WorkerConfig instance from CLI arguments supplied to the entrypoint
    script."""

    return BlueprintRequest(
        blueprint_uri=args.blueprint_uri,
        output_dir=pathlib.Path(args.output_dir),
        start_date=datetime.strptime(args.start_date, DATE_FORMAT),
        end_date=datetime.strptime(args.end_date, DATE_FORMAT),
    )


async def main() -> int:
    """Main entry point for the c-star worker script.

    Trigger
    the `Service` lifecycle for a Worker.
    """

    try:
        parser = create_parser()
        args = parser.parse_args()
    except SystemExit:
        # If the argument parsing fails, we exit with a non-zero status
        return 1
    else:
        service_cfg = get_service_config(args)
        blueprint_req = config_from_args(args)

    log_level = service_cfg.log_level
    logging.basicConfig(level=log_level, format="%(message)s")
    log = logging.getLogger(__name__)
    log.setLevel(log_level)

    try:
        worker = SimulationRunner(blueprint_req, service_cfg)
        await worker.execute()
    except CstarException:
        log.exception("An error occurred during the simulation")
        return 1
    except Exception:
        log.exception("An unexpected exception occurred during the simulation")
        return 1

    return 0


if __name__ == "__main__":
    rc = asyncio.run(main())
    sys.exit(rc)
