import asyncio
import sys
import typing as t
from typing import TYPE_CHECKING, Final, override

from cstar.applications.core import ApplicationDefinition, register_application
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.transforms import RomsMarblTimeSplitter
from cstar.base.exceptions import CstarError
from cstar.base.utils import slugify
from cstar.entrypoint.config import (
    get_job_config,
    get_service_config,
)
from cstar.entrypoint.runner import (
    XBlueprintRunner,
    XRunnerRequest,
    XRunnerResult,
    create_parser,
)
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.orchestration.models import (
    Application,
)
from cstar.orchestration.serialization import register_representer, strenum_representer
from cstar.orchestration.transforms import (
    DirectiveConfig,
    OverrideTransform,
)
from cstar.roms import ROMSSimulation

if TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


APP_NAME: t.Literal["roms_marbl"] = "roms_marbl"
_APP_NAME_LONG: t.Literal["ROMS-MARBL simulation runner"] = (
    "ROMS-MARBL simulation runner"
)


class RomsMarblRunner(XBlueprintRunner[RomsMarblBlueprint]):
    """Worker class to run c-star simulations."""

    simulation: Final[ROMSSimulation]
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
        super().__init__(request, service_cfg, job_cfg)

        self.simulation = ROMSSimulation.from_blueprint(self.request.blueprint_uri)
        self.simulation.name = slugify(self.simulation.name)

    @property
    def application(self) -> str:
        """The application identifier."""
        return APP_NAME

    @override
    def _on_start(self) -> None:
        super()._on_start()

        if not self.simulation:
            msg = "Simulation creation failed. Unable to execute simulation runner"
            raise RuntimeError(msg)

    @override
    async def run(self) -> XRunnerResult[RomsMarblBlueprint]:
        """Execute the c-star simulation."""
        treat_as_failure = False

        try:
            if not self._handler:
                self.log.trace("Setting up simulation")
                self.simulation.setup()
                self.log.trace("Building simulation")
                self.simulation.build()
                self.log.trace("Executing simulation pre-run")
                self.simulation.pre_run()

                self.log.trace("Starting simulation.")
                self._handler = self.simulation.run(
                    account_key=self._job_cfg.account_id,
                    walltime=self._job_cfg.walltime,
                    job_name=self._job_cfg.job_name,
                )
            else:
                await self._handler.updates(seconds=1.0)

            self.set_result(self._handler.status)

        except Exception:
            msg = "An error occurred while running the simulation"
            self.log.exception(msg)
            treat_as_failure = True
            self.set_result(ExecutionStatus.FAILED, [msg])

        if self.status == ExecutionStatus.COMPLETED:
            self.simulation.post_run()
        else:
            msg = "Skipping simulation post-run; simulation is not complete."
            self.log.debug(msg)

        self._log_disposition(treat_as_failure=treat_as_failure)
        return self.set_result(self.status)


@register_application
class RomsMarblApplication(ApplicationDefinition[RomsMarblBlueprint, RomsMarblRunner]):
    name: str = APP_NAME
    long_name: str = _APP_NAME_LONG
    runner = RomsMarblRunner
    blueprint = RomsMarblBlueprint
    applicable_transforms = (RomsMarblTimeSplitter, OverrideTransform)


def main() -> int:
    """Parse CLI arguments and run a c-star worker.

    Triggers the `Service` lifecycle of a `Worker` and runs a blueprint based on
    any supplied parameters.

    Returns
    -------
    int
        The exit code of the worker script. Returns 0 on success, 1 on failure.
    """
    try:
        parser = create_parser()
        args = parser.parse_args()
    except SystemExit as ex:
        print(str(ex))
        return 1

    job_cfg = get_job_config()
    service_cfg = get_service_config(args.log_level, name="RomsMarblRunner")

    blueprint_uri = str(args.blueprint_uri)
    if args.directives:
        blueprint_uri = DirectiveConfig.apply_directives(args.directives, blueprint_uri)

    request = XRunnerRequest(blueprint_uri, RomsMarblBlueprint)
    runner = RomsMarblRunner(request, service_cfg, job_cfg)

    try:
        asyncio.run(runner.execute())
        result = runner.result
        if result and result.errors:
            print(f"Errors occurred: {', '.join(result.errors)}")
            return 1
    except CstarError as ex:
        print(f"An error occurred during the simulation: {ex}")
        return 1
    except Exception as ex:
        print(f"An unexpected exception occurred during the simulation: {ex}")
        return 1

    return 0


register_representer(Application, strenum_representer)


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
