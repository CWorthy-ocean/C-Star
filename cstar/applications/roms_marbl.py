import asyncio
import os
import sys
import typing as t
from typing import TYPE_CHECKING, Final, override

from cstar.applications.utils import register_application
from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.entrypoint.config import (
    configure_environment,
    get_job_config,
    get_service_config,
)
from cstar.entrypoint.xrunner import (
    XBlueprintRunner,
    XRunnerRequest,
    XRunnerResult,
    create_parser,
)
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.orchestration.models import (
    ApplicationDefinition,
    RomsMarblBlueprint,
)
from cstar.orchestration.transforms import (
    DirectiveConfig,
    OverrideTransform,
    RomsMarblTimeSplitter,
)
from cstar.roms import ROMSSimulation

if TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


_APP_NAME: t.Literal["roms_marbl"] = "roms_marbl"
_APP_NAME_LONG: t.Literal["ROMS-MARBL simulation runner"] = (
    "ROMS-MARBL simulation runner"
)


class RomsMarblRunner(XBlueprintRunner[RomsMarblBlueprint]):
    """Worker class to run c-star simulations."""

    _simulation: Final[ROMSSimulation]
    """The simulation instance created from the blueprint."""
    _handler: ExecutionHandler | None = None
    """The execution handler for the simulation."""

    def __init__(
        self,
        request: XRunnerRequest[RomsMarblBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize the RomsMarblRunner with the supplied configuration.

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
        super().__init__(request, job_cfg, service_cfg)

        self._simulation = ROMSSimulation.from_blueprint(self.request.blueprint_uri)
        self._simulation.name = slugify(self._simulation.name)

    @override
    def _on_start(self) -> None:
        super()._on_start()

        if not self._simulation:
            msg = "Simulation creation failed. Unable to execute simulation runner"
            raise RuntimeError(msg)

    @override
    async def run(self) -> XRunnerResult[RomsMarblBlueprint]:
        """Execute the c-star simulation."""
        treat_as_failure = False

        try:
            if not self._handler:
                run_params = {
                    "account_key": self._job_cfg.account_id,
                    "walltime": self._job_cfg.walltime,
                    "job_name": self._job_cfg.job_name,
                }
                self.log.trace("Setting up simulation")
                self._simulation.setup()
                self.log.trace("Building simulation")
                self._simulation.build()
                self.log.trace("Executing simulation pre-run")
                self._simulation.pre_run()

                self.log.trace("Starting simulation.")
                self._handler = self._simulation.run(**run_params)
            else:
                await self._handler.updates(seconds=1.0)

            self.set_result(self._handler.status)

        except Exception:
            msg = "An error occurred while running the simulation"
            self.log.exception(msg)
            treat_as_failure = True
            self.set_result(ExecutionStatus.FAILED, [msg])

        if self.status == ExecutionStatus.COMPLETED:
            self._simulation.post_run()
        else:
            msg = "Skipping simulation post-run; simulation is not complete."
            self.log.debug(msg)

        self._log_disposition(treat_as_failure=treat_as_failure)
        return self.set_result(self.status)


@register_application
class RomsMarblApplication(ApplicationDefinition[RomsMarblBlueprint, RomsMarblRunner]):
    name: str = _APP_NAME
    long_name: str = _APP_NAME_LONG
    runner = RomsMarblRunner
    blueprint = RomsMarblBlueprint
    applicable_transforms = (RomsMarblTimeSplitter, OverrideTransform)


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
    """Execute a blueprint with a RomsMarblRunner.

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
    log.info(f"Creating simulation runner for {request.blueprint_uri!r}")

    try:
        configure_environment(log)
        worker = RomsMarblRunner(request, service_cfg, job_cfg)
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
    service_cfg = get_service_config(args.log_level, name="RomsMarblRunner")

    blueprint_uri = args.blueprint_uri
    if args.directives:
        blueprint_uri = DirectiveConfig.apply_directives(args.directives, blueprint_uri)

    request = get_request(blueprint_uri)

    return asyncio.run(execute_runner(job_cfg, service_cfg, request))


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
