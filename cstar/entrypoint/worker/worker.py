import asyncio
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, override

from cstar.base.exceptions import BlueprintError, CstarError
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.entrypoint.config import (
    configure_environment,
    get_job_config,
    get_service_config,
)
from cstar.entrypoint.service import Service
from cstar.entrypoint.xrunner import XRunnerRequest, create_parser
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.transforms import DirectiveConfig
from cstar.roms import ROMSSimulation

if TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


class SimulationRunner(Service):
    """Worker class to run c-star simulations."""

    _blueprint_uri: Final[str]
    """The URI of the blueprint to run."""
    _output_root: Final[pathlib.Path]
    """The root directory where simulation outputs will be written."""
    _simulation: Final[ROMSSimulation]
    """The simulation instance created from the blueprint."""
    _handler: ExecutionHandler | None
    """The execution handler for the simulation."""
    _job_config: Final["JobConfig"]
    """Configuration for submitting jobs to an HPC."""

    def __init__(
        self,
        request: XRunnerRequest[RomsMarblBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize the SimulationRunner with the supplied configuration.

        Parameters
        ----------
        request: XRunnerRequest[RomsMarblBlueprint]
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
            self.log.trace("Setting up simulation")
            self._simulation.setup()

            self.log.trace("Building simulation")
            self._simulation.build()

            self.log.trace("Executing simulation pre-run")
            self._simulation.pre_run()

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

            self._simulation.post_run()
            self.log.debug("Executing simulation post-run")
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

                self.log.trace("Running simulation.")
                self._handler = self._simulation.run(**run_params)
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


def get_request(
    blueprint_uri: str,
) -> XRunnerRequest[RomsMarblBlueprint]:
    """Create a XRunnerRequest[RomsMarblBlueprint] instance from CLI arguments.

    Parameters
    ----------
    blueprint_uri : str
        The path to a blueprint file

    Returns
    -------
    XRunnerRequest[RomsMarblBlueprint]
        A request configured to run a c-star simulation via a blueprint.
    """
    return XRunnerRequest(
        blueprint_uri,
        RomsMarblBlueprint,
    )


async def execute_runner(
    job_cfg: "JobConfig",
    service_cfg: "ServiceConfiguration",
    request: XRunnerRequest[RomsMarblBlueprint],
) -> int:
    """Execute a blueprint with a SimulationRunner.

    Parameters
    ----------
    job_cfg : JobConfig
        Configuration applied to the scheduler
    service_cfg : ServiceConfiguration
        Configuration applied to the service
    request : XRunnerRequest[RomsMarblBlueprint]
        A request specifying the blueprint to be executed
    """
    log = get_logger(__name__, level=service_cfg.log_level)

    log.debug(f"Job config: {job_cfg!r}")
    log.debug(f"Service config: {service_cfg!r}")
    log.debug(f"Request: {request!r}")
    log.trace(f"Environment: {os.environ}")
    log.info(f"Creating simulation runner for {request.blueprint_uri}")

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
    parser = create_parser()
    args = parser.parse_args()

    job_cfg = get_job_config()
    service_cfg = get_service_config(args.log_level, name="SimulationRunner")

    blueprint_uri = args.blueprint_uri
    if args.directives:
        blueprint_uri = DirectiveConfig.apply_directives(args.directives, blueprint_uri)

    request = get_request(blueprint_uri)

    return asyncio.run(execute_runner(job_cfg, service_cfg, request))


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
