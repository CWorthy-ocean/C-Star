import argparse
import asyncio
import dataclasses as dc
import logging
import os
import pathlib
import shutil
import sys
from datetime import datetime, timezone
from typing import override

from cstar import Simulation
from cstar.base.exceptions import BlueprintError, CstarException
from cstar.base.log import get_logger
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.roms import ROMSSimulation
from cstar.scripts.service import Service, ServiceConfiguration

CSTAR_USER_ENV_PATH = pathlib.Path("~/.cstar.env").expanduser()
CSTAR_EXTERNALS_ROOT = "~/code/cstar/cstar/externals"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
WORKER_LOG_FILE_TPL = "cstar-worker.{0}.log"


@dc.dataclass
class BlueprintRequest:
    """Represents a request to run a c-star simulation."""

    blueprint_uri: str
    """The path to the blueprint."""
    output_dir: pathlib.Path
    """The directory where simulation outputs will be written."""
    start_date: datetime
    """The date on which to begin the simulation."""
    end_date: datetime
    """The date on which to end the simulation."""


@dc.dataclass
class JobConfig:
    # TODO: does this already exist?
    account_id: str = "m4746"
    """HPC account used for billing."""
    walltime: str = "01:00:00"
    """Maximum walltime allowed for job."""
    job_name: str = f"cstar_worker_{datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')}"
    """User-friendly job name."""
    priority: str = "regular"
    """"""


class SimulationRunner(Service):
    """Worker class to run c-star simulations."""

    def __init__(
        self,
        request: BlueprintRequest,
        service_cfg: ServiceConfiguration,
        job_cfg: JobConfig,
    ):
        """Initialize the worker with a request."""
        super().__init__(service_cfg)

        self._blueprint_uri = request.blueprint_uri
        self._output_root = request.output_dir.expanduser()
        self._output_dir = self._get_unique_path(self._output_root)
        self._simulation: Simulation = ROMSSimulation.from_blueprint(
            blueprint=self._blueprint_uri,
            directory=self._output_dir,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        self._handler: ExecutionHandler | None = None
        # TODO: get this from the cstar env
        self._user_env_path = pathlib.Path(CSTAR_USER_ENV_PATH).expanduser()
        # TODO: get this from the cstar env
        self._externals_path = pathlib.Path(CSTAR_EXTERNALS_ROOT).expanduser()
        self._job_config = job_cfg

    @staticmethod
    def _get_unique_path(root_path: pathlib.Path) -> pathlib.Path:
        """Create a unique path name to avoid collisions."""
        current_time = datetime.now(timezone.utc)
        return root_path / f"{current_time.strftime('%Y%m%d_%H%M%S')}"

    def _prepare_file_system(self) -> None:
        """Clean up old directories to avoid collisions and create new locations for the
        new run.

        NOTE: this may be unnecessary if the worker starts
        up a new container w/fresh directories every time.
        """
        # a leftover .cstar.env cause empty repo/compilation errors; remove it.
        if self._user_env_path.exists():
            self.log.debug(f"Removing existing user env: {self._user_env_path}")
            self._user_env_path.unlink()

        # a leftover root_dir may have files in it, breaking download; remove it.
        if self._output_root.exists():
            self.log.debug(f"Removing existing output dir: {self._output_root}")
            shutil.rmtree(self._output_root)

        # leftover external code folder causes non-empty repo errors; remove it.
        if self._externals_path.exists():
            self.log.debug(f"Removing existing externals dir: {self._externals_path}")
            shutil.rmtree(self._externals_path)
        self._externals_path.mkdir(parents=True, exist_ok=False)

        # create a clean location to write outputs.
        if not self._output_dir.exists():
            self.log.debug(f"Creating clean output dir: {self._output_dir}")
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
            self.log.debug("Setting up simulation")
            self._simulation.setup()

            self.log.debug("Building simulation")
            self._simulation.build()

            self.log.debug("Executing simulation pre-run")
            self._simulation.pre_run()
        except RuntimeError as ex:
            raise CstarException("Failed to build simulation") from ex
        except ValueError as ex:
            raise CstarException("Failed to prepare simulation") from ex

    @override
    def _on_shutdown(self) -> None:
        """Perform shutdown of the worker service and log the final disposition of the
        simulation."""
        if not self._simulation:
            self.log.warning("No simulation available at shutdown")
            return

        # perform simulation cleanup activities when possible
        if self._handler and self._handler.status == ExecutionStatus.COMPLETED:
            # note: calling post_run on any status but completed fails.
            self.log.debug("Executing simulation post-run")
            self._simulation.post_run()
        else:
            self.log.debug("Skipping simulation post-run.")

        # Ensure simulation status has been logged (handler updates may be suppressed)
        self._log_disposition()

    @override
    def _on_iteration(self) -> None:
        """Execute the c-star simulation."""

        try:
            if not self._handler:
                self.log.debug("Running simulation.")

                ## TODO: WARNING - more intelligent job config required.
                # run_params = {}
                # if os.environ.get("SLURM_JOB_ID", True):
                run_params = {
                    "account_key": self._job_config.account_id,
                    "walltime": self._job_config.walltime,
                    "job_name": self._job_config.job_name,
                }

                self._handler = self._simulation.run(**run_params)
            else:
                self._handler.updates(1.0)
                self._send_hc_update({"status": str(self._handler.status)})
        except Exception:
            logging.exception("An error occurred while running the simulation")

    def _is_status_complete(self) -> bool:
        if self._handler is None:
            return True

        return self._handler.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
            ExecutionStatus.UNKNOWN,
        ]

    @override
    def _can_shutdown(self) -> bool:
        """Determine if the service can shutdown."""
        if self._simulation is None:
            self.log.error("Simulation is not set. Allowing shutdown.")
            return True

        if not self._handler:
            self.log.error("Execution handler is not set. Allowing shutdown.")
            return True

        status = self._handler.status
        if self._is_status_complete():
            self.log.info(
                f"Simulation is no longer running ({status}). Allowing shutdown."
            )
            return True

        return False


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


def get_service_config(args: argparse.Namespace) -> ServiceConfiguration:
    """Create a ServiceConfiguration instance using CLI arguments."""
    return ServiceConfiguration(
        as_service=True,
        loop_delay=5,
        health_check_frequency=10,
        log_level=logging._nameToLevel[args.log_level],
        name="SimulationRunner",
    )


def config_from_args(args: argparse.Namespace) -> BlueprintRequest:
    """Creates a WorkerConfig instance from CLI arguments."""

    return BlueprintRequest(
        blueprint_uri=args.blueprint_uri,
        output_dir=pathlib.Path(args.output_dir),
        start_date=datetime.strptime(args.start_date, DATE_FORMAT),
        end_date=datetime.strptime(args.end_date, DATE_FORMAT),
    )


def configure_environment(log: logging.Logger) -> None:
    """Configure the environment variables required by the worker.

    NOTE: The worker checks for CSTAR_ROMS_PREBUILT and CSTAR_MARBL_PREBUILT
    to indicate that pre-built modeling binaries should be used.
    """
    # ensure no human interaction is required
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] = "1"

    # TODO: consider modifying the cstar_sysmgr to look
    # at environment variables on the first write.
    vars = os.environ

    if os.environ.get("CSTAR_ROMS_PREBUILT", None):
        # cstar_sysmgr.environment.set_env_var("ROMS_ROOT", os.environ["ROMS_ROOT"])
        ext_root = vars.get("ROMS_ROOT", None)
        log.debug(f"Using prebuilt ROMS at: {ext_root}")

    if os.environ.get("CSTAR_MARBL_PREBUILT", None):
        ext_root = vars.get("MARBL_ROOT", None)
        log.debug(f"Using prebuilt MARBL at: {ext_root}")


async def main() -> int:
    """Main entry point for the c-star worker script.

    Triggers the `Service` lifecycle of a Worker and runs a blueprint
    from parameters.
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
        job_cfg = JobConfig()  # TODO: parameterize... env vars? use defaults for now...

    log_file = (
        blueprint_req.output_dir
        / "logs"
        / WORKER_LOG_FILE_TPL.format(datetime.now(timezone.utc))
    )
    log = get_logger(__name__, level=service_cfg.log_level, filename=log_file)

    try:
        configure_environment(log)
        worker = SimulationRunner(blueprint_req, service_cfg, job_cfg)
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
