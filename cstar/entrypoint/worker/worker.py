import argparse
import asyncio
import dataclasses as dc
import enum
import logging
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Literal, override

from cstar.base.env import ENV_CSTAR_LOG_LEVEL, get_env_item
from cstar.base.exceptions import BlueprintError, CstarError
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.entrypoint.config import (
    configure_environment,
    get_job_config,
    get_service_config,
)
from cstar.entrypoint.service import Service
from cstar.entrypoint.utils import (
    ARG_LOGLEVEL_LONG,
    ARG_LOGLEVEL_SHORT,
    ARG_URI_LONG,
    ARG_URI_SHORT,
)
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.roms import ROMSSimulation

if TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


ARG_STAGE_LONG: Literal["--stage"] = "--stage"
ARG_STAGE_SHORT: Literal["-g"] = "-g"


class SimulationStages(enum.StrEnum):
    """The stages in the simulation pipeline."""

    SETUP = enum.auto()
    """Execute simulation setup. See `Simulation.setup`"""
    BUILD = enum.auto()
    """Execute builds of simulation dependencies. See `Simulation.build`"""
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
    stages: list[SimulationStages] = dc.field(default_factory=list)
    """The simulation stages to execute.

    Defaults to all stages.
    """


class SimulationRunner(Service):
    """Worker class to run c-star simulations."""

    _blueprint_uri: Final[str]
    """The URI of the blueprint to run."""
    _output_root: Final[pathlib.Path]
    """The root directory where simulation outputs will be written."""
    _simulation: Final[ROMSSimulation]
    """The simulation instance created from the blueprint."""
    _stages: Final[tuple[SimulationStages, ...]]
    """The simulation stages that should be executed."""
    _handler: ExecutionHandler | None
    """The execution handler for the simulation."""
    _job_config: Final["JobConfig"]
    """Configuration for submitting jobs to an HPC."""

    def __init__(
        self,
        request: BlueprintRequest,
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
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

        self._simulation = ROMSSimulation.from_blueprint(self._blueprint_uri)
        self._simulation.name = slugify(self._simulation.name)
        self._output_root = self._simulation.directory.expanduser()
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

    def _log_disposition(self, treat_as_failure: bool = False) -> None:
        """Log the status of the simulation at shutdown time."""
        disposition: ExecutionStatus = (
            self._handler.status if self._handler else ExecutionStatus.UNKNOWN
        )

        if self._handler:
            self.log.info(f"Completed simulation logs at: {self._handler.output_file}")

        if disposition == ExecutionStatus.COMPLETED and not treat_as_failure:
            self.log.info("Simulation completed successfully.")
        elif disposition == ExecutionStatus.FAILED or treat_as_failure:
            self.log.error("Simulation failed.")
        else:
            self.log.warning(f"Simulation ended with status: {disposition}.")

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

        try:
            if SimulationStages.SETUP in self._stages:
                self.log.trace("Setting up simulation")
                self._simulation.setup()
            else:
                self.log.trace("Skipping simulation setup")

            if SimulationStages.BUILD in self._stages:
                self.log.trace("Building simulation")
                self._simulation.build()
            else:
                self.log.trace("Skipping simulation build")

            if SimulationStages.PRE_RUN in self._stages:
                self.log.trace("Executing simulation pre-run")
                self._simulation.pre_run()
            else:
                self.log.trace("Skipping simulation pre_run")

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
        treat_as_failure = False
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
            treat_as_failure = True
            self.log.exception("Simulation post_run failed.")
        finally:
            # ensure status is logged even if _handler updates are suppressed.
            self._log_disposition(treat_as_failure=treat_as_failure)

    @override
    async def _on_iteration(self) -> None:
        """Execute the c-star simulation."""
        try:
            if not self._handler:
                run_params = {
                    "account_key": self._job_config.account_id,
                    "walltime": self._job_config.walltime,
                    "job_name": self._job_config.job_name,
                }

                if SimulationStages.RUN in self._stages:
                    self.log.trace("Running simulation.")
                    self._handler = self._simulation.run(**run_params)
                else:
                    self.log.trace("Skipping simulation run")
            else:
                await self._handler.updates(seconds=1.0)
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


def create_simrunner_parser() -> argparse.ArgumentParser:
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
        ARG_URI_SHORT,
        ARG_URI_LONG,
        type=str,
        required=True,
        help="The URI of a blueprint.",
    )
    parser.add_argument(
        ARG_LOGLEVEL_SHORT,
        ARG_LOGLEVEL_LONG,
        default=get_env_item(ENV_CSTAR_LOG_LEVEL).value,
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
        ARG_STAGE_SHORT,
        ARG_STAGE_LONG,
        choices=[x.value for x in SimulationStages],
        type=str,
        required=False,
        action="append",
        dest="stages",
        help=("Simulation stages to execute."),
    )
    return parser


def get_request(
    blueprint_uri: str, stages: list[SimulationStages] | None = None
) -> BlueprintRequest:
    """Create a BlueprintRequest instance from CLI arguments.

    Parameters
    ----------
    blueprint_uri : str
        The path to a blueprint file
    stages : list[SimulationStages] | None
        The set of stages to be executed. Defaults to all stages, if empty or None.

    Returns
    -------
    BlueprintRequest
        A request configured to run a c-star simulation via a blueprint.
    """
    if not stages:
        stages = list(SimulationStages)

    return BlueprintRequest(
        blueprint_uri=blueprint_uri,
        stages=stages,
    )


async def execute_runner(
    job_cfg: "JobConfig",
    service_cfg: "ServiceConfiguration",
    request: BlueprintRequest,
) -> int:
    """Execute a blueprint with a SimulationRunner.

    Parameters
    ----------
    job_cfg : JobConfig
        Configuration applied to the scheduler
    service_cfg : ServiceConfiguration
        Configuration applied to the service
    request : BlueprintRequest
        A request specifying the blueprint to be executed
    """
    log = get_logger(__name__, level=service_cfg.log_level)

    log.debug(f"Job config: {job_cfg!r}")
    log.debug(f"Service config: {service_cfg!r}")
    log.debug(f"Request: {request!r}")
    log.trace(f"Environment: {os.environ}")

    try:
        configure_environment(log)

        worker = SimulationRunner(request, service_cfg, job_cfg)
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


def main() -> int:
    """Parse CLI arguments and run a c-star worker.

    Triggers the `Service` lifecycle of a `Worker` and runs a blueprint based on
    any supplied parameters.

    Returns
    -------
    int
        The exit code of the worker script. Returns 0 on success, 1 on failure.
    """
    parser = create_simrunner_parser()
    args = parser.parse_args()

    job_cfg = get_job_config()
    service_cfg = get_service_config(args.log_level, name="SimulationRunner")
    request = get_request(args.blueprint_uri, args.stages)

    return asyncio.run(execute_runner(job_cfg, service_cfg, request))


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
