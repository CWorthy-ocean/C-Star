import argparse
import asyncio
import dataclasses as dc
import enum
import logging
import os
import pathlib
import shutil
import sys
from datetime import datetime, timezone
from typing import Final, override

from cstar.base.exceptions import BlueprintError, CstarError
from cstar.base.log import get_logger
from cstar.entrypoint.service import Service, ServiceConfiguration
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.orchestration.adapter import BlueprintAdapter
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize
from cstar.roms import ROMSSimulation
from cstar.system.manager import cstar_sysmgr

DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
WORKER_LOG_FILE_TPL: Final[str] = "cstar-worker.{0}.log"
JOBFILE_DATE_FORMAT: Final[str] = "%Y%m%d_%H%M%S"
LOGS_DIRECTORY: Final[str] = "logs"


def _generate_job_name() -> str:
    """Generate a unique job name based on the current date and time."""
    now_utc = datetime.now(timezone.utc)
    formatted_now_utc = now_utc.strftime(JOBFILE_DATE_FORMAT)
    return f"cstar_worker_{formatted_now_utc}"


class SimulationStages(enum.StrEnum):
    """The stages in the simulation pipeline."""

    SETUP = enum.auto()
    """Execute simulation setup. See `Simulation.setup`"""
    BUILD = enum.auto()
    """Execute builds of simulation depdencies. See `Simulation.build`"""
    PRE_RUN = enum.auto()
    """Execute hooks before the simulation starts. See `Simulation.pre_run`"""
    RUN = enum.auto()
    """Execute the simulation. See `Simulation.run`"""
    POST_RUN = enum.auto()
    """Execute hooks after the simulation completes. See `Simulation.post_run`"""


@dc.dataclass(frozen=True)
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
    stages: tuple[SimulationStages, ...] = dc.field(default=())
    """The simulation stages to execute."""


@dc.dataclass(frozen=True)
class JobConfig:
    """Configuration required to submit HPC jobs."""

    account_id: str = "m4746"
    """HPC account used for billing."""
    walltime: str = "01:00:00"
    """Maximum walltime allowed for job."""
    job_name: str = _generate_job_name()
    """User-friendly job name."""
    priority: str = "regular"
    """Job priority."""


class SimulationRunner(Service):
    """Worker class to run c-star simulations."""

    _blueprint_uri: Final[str]
    """The URI of the blueprint to run."""
    _output_root: Final[pathlib.Path]
    """The root directory where simulation outputs will be written."""
    _output_dir: Final[pathlib.Path]
    """A unique directory for this simulation run to write outputs."""
    _simulation: Final[ROMSSimulation]
    """The simulation instance created from the blueprint."""
    _stages: Final[tuple[SimulationStages, ...]]
    """The simulation stages that should be executed."""
    _handler: ExecutionHandler | None
    """The execution handler for the simulation."""
    _job_config: Final[JobConfig]
    """Configuration for submitting jobs to an HPC."""

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

        bp = deserialize(pathlib.Path(self._blueprint_uri), RomsMarblBlueprint)
        self._output_root = bp.runtime_params.output_dir.expanduser()
        self._output_dir = self._get_unique_path(self._output_root)
        self._simulation: ROMSSimulation = BlueprintAdapter(bp).adapt()
        self._stages = tuple(request.stages)

        roms_root = os.environ.get("ROMS_ROOT", None)
        self._simulation.exe_path = pathlib.Path(roms_root) if roms_root else None
        self._handler = None
        self._job_config = job_cfg

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

        Raises
        ------
        ValueError
            If the output directory exists and contains
        """
        # ensure that log files don't cause startup to fail.
        outputs = next(
            (p for p in self._output_root.glob("*") if LOGS_DIRECTORY not in str(p)),
            None,
        )

        if self._output_dir.exists() and outputs:
            msg = f"Output directory {self._output_root} is not empty."
            raise ValueError(msg)

        # leftover external code folder causes non-empty repo errors; remove.
        externals_path = cstar_sysmgr.environment.package_root / "externals"
        if externals_path.exists():
            msg = f"Removing existing externals dir: {externals_path}"
            self.log.debug(msg)
            shutil.rmtree(externals_path)
        externals_path.mkdir(parents=True, exist_ok=False)

        # create a clean location to write outputs.
        if not self._output_dir.exists():
            msg = f"Creating clean output dir: {self._output_dir}"
            self.log.debug(msg)
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

        Verifies the simulation loaded properly, configuring the file system, retrieving
        remote resources, and building third-party codebases.
        """
        if self._blueprint_uri is None:
            msg = "No blueprint URI provided"
            raise BlueprintError(msg)

        if self._simulation is None:
            msg = f"Unable to load the blueprint: {self._blueprint_uri}"
            raise BlueprintError(msg)

        self._prepare_file_system()

        try:
            if SimulationStages.SETUP in self._stages:
                self.log.debug("Setting up simulation")
                self._simulation.setup()
            else:
                self.log.debug("Skipping simulation setup")

            if SimulationStages.BUILD in self._stages:
                self.log.debug("Building simulation")
                self._simulation.build()
            else:
                self.log.debug("Skipping simulation build")

            if SimulationStages.PRE_RUN in self._stages:
                self.log.debug("Executing simulation pre-run")
                self._simulation.pre_run()
            else:
                self.log.debug("Skipping simulation pre_run")

        except RuntimeError as ex:
            msg = "Failed to build simulation"
            raise CstarError(msg) from ex
        except ValueError as ex:
            msg = "Failed to prepare simulation"
            raise CstarError(msg) from ex

    @override
    def _on_shutdown(self) -> None:
        """Perform activities required for clean shutdown.

        Execute simulation post-run behavior and log the final disposition of the
        simulation.
        """
        # perform simulation cleanup activities only when required
        stage_enabled = SimulationStages.POST_RUN in self._stages

        try:
            if not self._simulation:
                self.log.warning("No simulation available at shutdown")
                return

            # note: calling post_run on any status but completed fails.
            if not self._handler:
                self.log.debug("Skipping simulation post-run; handler not found.")
                return

            if self._handler.status != ExecutionStatus.COMPLETED:
                self.log.debug(
                    "Skipping simulation post-run; simulation is not complete."
                )
                return

            if stage_enabled:
                self._simulation.post_run()
                self.log.debug("Executing simulation post-run")
            else:
                self.log.debug("Skipping simulation post-run")
        except RuntimeError:
            self.log.exception("Simulation post_run failed.")
        finally:
            # ensure status is logged even if _handler updates are suppressed.
            self._log_disposition()

    @override
    def _on_iteration(self) -> None:
        """Execute the c-star simulation."""
        try:
            if not self._handler:
                # if os.environ.get("SLURM_JOB_ID", True):
                run_params = {
                    "account_key": self._job_config.account_id,
                    "walltime": self._job_config.walltime,
                    "job_name": self._job_config.job_name,
                }

                if SimulationStages.RUN in self._stages:
                    self.log.debug("Running simulation.")
                    self._handler = self._simulation.run(**run_params)
                else:
                    self.log.debug("Skipping simulation run")
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
        exit_on_error=True,
    )
    parser.add_argument(
        "-b",
        "--blueprint-uri",
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        "-l",
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
        "-o",
        "--output-dir",
        default="~/code/cstar/examples/",
        type=str,
        required=False,
        help="Local path to write simulation outputs to.",
    )
    parser.add_argument(
        "-s",
        "--start-date",
        default="2012-01-03 12:00:00",
        type=str,
        required=False,
        help=(f"Simulation start date, formatted `{DATE_FORMAT}`"),
    )
    parser.add_argument(
        "-e",
        "--end-date",
        default="2012-01-04 12:00:00",
        type=str,
        required=False,
        help=(f"Simulation end date, formatted `{DATE_FORMAT}`"),
    )
    parser.add_argument(
        "-g",
        "--stage",
        default=tuple(x for x in SimulationStages),
        type=str,
        required=False,
        action="append",
        dest="stages",
        help=("Simulation stages to execute."),
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
        health_check_log_threshold=10,
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
        stages=args.stages,
    )


def configure_environment(log: logging.Logger) -> None:
    """Configure the environment variables required by the worker.

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

    # TODO: re-run tests now that prebuilt is gone and rebase on develop is in.
    # is_roms_prebuilt = os.environ.get("CSTAR_ROMS_PREBUILT", None) == "1"
    # is_marbl_prebuilt = os.environ.get("CSTAR_MARBL_PREBUILT", None) == "1"

    # if is_roms_prebuilt:
    #     ext_root = os.environ.get("ROMS_ROOT", None)
    #     log.debug("Using prebuilt ROMS at: %s", ext_root)

    # if is_marbl_prebuilt:
    #     ext_root = os.environ.get("MARBL_ROOT", None)
    #     log.debug("Using prebuilt MARBL at: %s", ext_root)


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

    # log_file = (
    #     blueprint_req.output_dir
    #     / LOGS_DIRECTORY
    #     / WORKER_LOG_FILE_TPL.format(datetime.now(timezone.utc))
    # )
    log = get_logger(__name__, level=service_cfg.log_level)  # , filename=log_file)

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
