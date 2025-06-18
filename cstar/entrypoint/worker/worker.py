import argparse
import asyncio
import dataclasses as dc
import logging
import os
import pathlib
import shutil
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, override

from cstar.base.exceptions import BlueprintError, CstarError
from cstar.base.log import get_logger
from cstar.entrypoint.service import Service, ServiceConfiguration
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.roms import ROMSSimulation
from cstar.system.manager import cstar_sysmgr

if TYPE_CHECKING:
    from cstar import Simulation

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
WORKER_LOG_FILE_TPL = "cstar-worker.{0}.log"
JOBFILE_DATE_FORMAT = "%Y%m%d_%H%M%S"


def _generate_job_name() -> str:
    """Generate a unique job name based on the current date and time."""
    now_utc = datetime.now(timezone.utc)
    formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
    return f"cstar_worker_{formatted_now_utc}"


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
    """Configuration required to submit HPC jobs."""

    account_id: str = "m4746"
    """HPC account used for billing."""
    walltime: str = "01:00:00"
    """Maximum walltime allowed for job."""
    job_name: str = _generate_job_name()
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
    ) -> None:
        """Initialize the SimulationRunner with the supplied configuration.

        Parameters
        ----------
        request: BlueprintRequest
            A request containing information about the simulation to run

        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.

        job_cfg: JobConfig
            Configuration for submitting jobs to an HPC, such as account ID,
            walltime, job name, and priority.
        """
        super().__init__(service_cfg)

        self._blueprint_uri = request.blueprint_uri
        """The URI of the blueprint to run."""
        self._output_root = request.output_dir.expanduser()
        """The root directory where simulation outputs will be written."""
        self._output_dir = self._get_unique_path(self._output_root)
        """A unique directory for this simulation run to write outputs."""
        self._simulation: Simulation = ROMSSimulation.from_blueprint(
            blueprint=self._blueprint_uri,
            directory=self._output_dir,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        """The simulation instance created from the blueprint."""
        self._handler: ExecutionHandler | None = None
        """The execution handler for the simulation."""
        self._job_config = job_cfg
        """Configuration for submitting jobs to an HPC."""

    @staticmethod
    def _get_unique_path(root_path: pathlib.Path) -> pathlib.Path:
        """Create a unique path name to avoid collisions.

        Parameters
        ----------
        root_path: pathlib.Path
            The parent directory where the unique directory will be created.

        Returns
        -------
        pathlib.Path
            A unique path based on the current date and time.
        """
        current_time = datetime.now(timezone.utc)
        return root_path / f"{current_time.strftime('%Y%m%d_%H%M%S')}"

    def _prepare_file_system(self) -> None:
        """Ensure fresh directories exist for the simulation outputs.

        Removes any pre-existing directories and creates empty directories to avoid
        collisions.
        """
        # a leftover .cstar.env cause empty repo/compilation errors; remove.
        user_env_path = cstar_sysmgr.environment.user_env_path
        if user_env_path.exists():
            self.log.debug(f"Removing existing user env: {user_env_path}")
            user_env_path.unlink()

        # a leftover root_dir may have files in it, breaking download; remove.
        if self._output_root.exists():
            self.log.debug(f"Removing existing output dir: {self._output_root}")
            shutil.rmtree(self._output_root)

        # leftover external code folder causes non-empty repo errors; remove.
        externals_path = cstar_sysmgr.environment.package_root / "externals"
        if externals_path.exists():
            self.log.debug(f"Removing existing externals dir: {externals_path}")
            shutil.rmtree(externals_path)
        externals_path.mkdir(parents=True, exist_ok=False)

        # create a clean location to write outputs.
        if not self._output_dir.exists():
            self.log.debug(f"Creating clean output dir: {self._output_dir}")
            self._output_dir.mkdir(parents=True, exist_ok=True)

    def _log_disposition(self) -> None:
        """Log the status of the simulation at shutdown time."""
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
        """Prepare the simulation for execution.

        Verifyies the simulation loaded properly, configuring the file system,
        retrieving remote resources, and building third-party codebases.
        """
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
            raise CstarError("Failed to build simulation") from ex
        except ValueError as ex:
            raise CstarError("Failed to prepare simulation") from ex

    @override
    def _on_shutdown(self) -> None:
        """Perform activities required for clean shutdown.

        Execute simulation post-run behavior and log the final disposition of the
        simulation.
        """
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

        # ensure simulation status is reliably logged if/when self._handler
        # updates are suppressed.
        self._log_disposition()

    @override
    def _on_iteration(self) -> None:
        """Execute the c-star simulation."""
        try:
            if not self._handler:
                self.log.debug("Running simulation.")

                # if os.environ.get("SLURM_JOB_ID", True):
                run_params = {
                    "account_key": self._job_config.account_id,
                    "walltime": self._job_config.walltime,
                    "job_name": self._job_config.job_name,
                }

                self._handler = self._simulation.run(**run_params)
            else:
                self._handler.updates(1.0)
                self._send_update_to_hc({"status": str(self._handler.status)})
        except Exception:
            self.log.exception("An error occurred while running the simulation")

    def _is_status_complete(self) -> bool:
        """Determine if the simulation has completed.

        Returns
        -------
        bool
            `True` if the simulation is in a completed state, `False` otherwise.
        """
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
        """Determine if the service can shutdown.

        Returns
        -------
        bool
            `True` if the service can shutdown, `False` otherwise.
        """
        if self._simulation is None:
            self.log.error("Simulation is not set. Allowing shutdown.")
            return True

        if not self._handler:
            self.log.error("Execution handler is not set. Allowing shutdown.")
            return True

        status = self._handler.status
        if self._is_status_complete():
            self.log.info(f"Simulation is not running ({status}). Allowing shutdown.")
            return True

        return False


def create_parser() -> argparse.ArgumentParser:
    """Create a parser for CLI arguments expected by a SimulationRunner.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured with the expected arguments for the
        SimulationRunner service.
    """
    parser = argparse.ArgumentParser(
        description="Run a c-star simulation.",
        # prefix_chars="--",
        # allow_abbrev=True,
        exit_on_error=True,
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
            logging.getLevelName(i)
            for i in [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
            ]
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
        help=(f"The simulation start date, formatted `{DATE_FORMAT}`"),
    )
    parser.add_argument(
        "--end-date",
        default="2012-01-04 12:00:00",
        type=str,
        required=False,
        help=(f"The simulation end date, formatted `{DATE_FORMAT}`"),
    )
    return parser


def get_service_config(args: argparse.Namespace) -> ServiceConfiguration:
    """Create a ServiceConfiguration instance using CLI arguments.

    Parameters
    ----------
    args: argparse.Namespace
        The arguments parsed from the command line, including log level and

    Returns
    -------
    ServiceConfiguration
        The configuration for a service.
    """
    return ServiceConfiguration(
        as_service=True,
        loop_delay=5,
        health_check_frequency=10,
        log_level=logging.getLevelNamesMapping()[args.log_level],
        name="SimulationRunner",
    )


def _format_date(date_str: str) -> datetime:
    """Convert a date string to a datetime object using the default format.

    Parameters
    ----------
    date_str : str
        The date string to convert.

    Returns
    -------
    datetime
        The converted datetime.
    """
    return datetime.strptime(  # noqa: DTZ007
        date_str,
        DATE_FORMAT,
    )


def get_request(args: argparse.Namespace) -> BlueprintRequest:
    """Create a BlueprintRequest instance from CLI arguments.

    Parameters
    ----------
    args : argparse.Namespace
        The arguments parsed from the command line.

    Returns
    -------
    BlueprintRequest
        A request configured to run a c-star simulation via a blueprint.
    """
    return BlueprintRequest(
        blueprint_uri=args.blueprint_uri,
        output_dir=pathlib.Path(args.output_dir),
        start_date=_format_date(args.start_date),
        end_date=_format_date(args.end_date),
    )


def configure_environment(log: logging.Logger) -> None:
    """Configure the environment variables required by the worker.

    NOTE: The worker checks for CSTAR_ROMS_PREBUILT and CSTAR_MARBL_PREBUILT
    to indicate that pre-built modeling binaries should be used.

    Parameters
    ----------
    log : logging.Logger
        A logger to log configuration details.

    Returns
    -------
    None
    """
    # ensure no human interaction is required
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["GIT_DISCOVERY_ACROSS_FILESYSTEM"] = "1"

    is_roms_prebuilt = os.environ.get("CSTAR_ROMS_PREBUILT", None) == "1"
    is_marbl_prebuilt = os.environ.get("CSTAR_MARBL_PREBUILT", None) == "1"

    if is_roms_prebuilt:
        ext_root = os.environ.get("ROMS_ROOT", None)
        log.debug("Using prebuilt ROMS at: %s", ext_root)

    if is_marbl_prebuilt:
        ext_root = os.environ.get("MARBL_ROOT", None)
        log.debug("Using prebuilt MARBL at: %s", ext_root)


async def main(raw_args: list[str]) -> int:
    """Run the c-star worker script.

    Triggers the `Service` lifecycle of a Worker and runs a blueprint based on
    any supplied parameters.

    Returns
    -------
    int
        The exit code of the worker script. Returns 0 on success, 1 on failure.
    """
    try:
        parser = create_parser()
        args = parser.parse_args(raw_args)
    except SystemExit:
        return 1
    else:
        service_cfg = get_service_config(args)
        blueprint_req = get_request(args)
        job_cfg = JobConfig()  # use default HPC config

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
    except CstarError as ex:
        log.exception("An error occurred during the simulation", exc_info=ex)
        return 1
    except Exception as ex:
        log.exception(
            "An unexpected exception occurred during the simulation", exc_info=ex
        )
        return 1

    return 0


if __name__ == "__main__":
    rc = asyncio.run(main(sys.argv[1:]))
    sys.exit(rc)
