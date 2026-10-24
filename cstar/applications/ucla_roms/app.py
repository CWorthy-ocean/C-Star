import asyncio
import sys
import typing as t
from pathlib import Path
from typing import TYPE_CHECKING, Final, override

from cstar.applications.core import (
    ApplicationDefinition,
    ArtifactKind,
    ArtifactSpec,
    RunnerRequest,
    RunnerResult,
    register_application,
)
from cstar.roms.file_system import (
    GENERATED_NAMELIST_NAME,
    ROMS_EXECUTABLE_NAME,
    RomsFileSystemManager,
)
from cstar.applications.ucla_roms.migration import (
    UclaRomsSchemaAdapter2025v1,
    UclaRomsSchemaAdapterV2V21,
    UclaRomsSchemaAdapterV21V3,
)
from cstar.applications.ucla_roms.models import APP_NAME, UclaRomsBlueprint
from cstar.roms.outputs import (
    OUTPUT_FILE_PATTERN,
    OUTPUT_STREAMS,
    warn_on_restart_start_date_mismatch,
)
from cstar.applications.ucla_roms.resume import prepare_resume_blueprint
from cstar.applications.ucla_roms.transforms import (
    CarbonateSensitivityDirective,
    ContinuanceDirective,
    NestingDirective,
    RomsTimeSplitter,
)
from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.entrypoint.config import (
    get_job_config,
    get_service_config,
)
from cstar.entrypoint.runner import BlueprintRunner, create_parser
from cstar.execution.handler import ExecutionHandler, ExecutionStatus
from cstar.orchestration.manifest import Product
from cstar.orchestration.models import (
    Application,
)
from cstar.orchestration.serialization import register_representer, strenum_representer
from cstar.orchestration.transforms import (
    DirectiveConfig,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from cstar.entrypoint.config import JobConfig, ServiceConfiguration
    from cstar.roms.simulation import ROMSSimulation


_APP_NAME_LONG: t.Literal["UCLA-ROMS simulation runner"] = "UCLA-ROMS simulation runner"
log = get_logger(__name__)


def output_products(output_dir: Path, root: Path, *, use_pio: bool) -> list[Product]:
    """Describe the ROMS output files in a directory by stream, timestamp and format.

    Only classifies by file name: the declared patterns own each file's size, and
    any file this skips is still declared by the catch-all output pattern.

    Parameters
    ----------
    output_dir : Path
        The directory holding the joined output files.
    root : Path
        The step root that product paths are relative to.
    use_pio : bool
        Whether the run wrote its output through ParallelIO.

    Returns
    -------
    list[Product]
        One product per file named like a ROMS output, sorted by name.
    """
    # both sides resolved, so a symlinked step root still contains its output
    root = root.resolve()
    products: list[Product] = []
    for path in sorted(output_dir.resolve().glob("*.nc")):
        match = OUTPUT_FILE_PATTERN.match(path.name)
        if match is None:
            continue
        if not path.is_relative_to(root):
            log.debug(f"Skipping {path}: it is not under the step root {root}")
            continue
        row = next((r for r in OUTPUT_STREAMS if r[0] == match["stem"]), None)
        kind, base = (row[1], row[2]) if row else (ArtifactKind.output, {})
        products.append(
            Product(
                kind=kind.value,
                path=path.relative_to(root).as_posix(),
                metadata={
                    **base,
                    "stream": match["stream"],
                    "timestamp": match["timestamp"],
                    "format": "pio" if use_pio else "joined",
                },
            )
        )
    return products


class UclaRomsRunner(BlueprintRunner[UclaRomsBlueprint]):
    """Worker class to run c-star simulations."""

    simulation: Final["ROMSSimulation"]
    """The simulation instance created from the blueprint."""
    _handler: ExecutionHandler | None = None
    """The execution handler for the simulation."""

    def __init__(
        self,
        request: RunnerRequest[UclaRomsBlueprint],
        service_cfg: "ServiceConfiguration",
        job_cfg: "JobConfig",
    ) -> None:
        """Initialize the UclaRomsRunner with the supplied configuration.

        Parameters
        ----------
        request: RunnerRequest[UclaRomsBlueprint]
            A request containing information about the simulation to run

        service_cfg: ServiceConfiguration
            Configuration for modifying behavior of the service process.

        job_cfg: JobConfig
            Configuration for submitting jobs to an HPC, such as account ID,
            walltime, job name, and priority.
        """
        super().__init__(request, service_cfg, job_cfg)

        from cstar.roms.simulation import ROMSSimulation

        # a resumed attempt runs from a derived blueprint that continues from
        # the interrupted attempt's last usable restart; the original is kept
        blueprint_uri = self.request.blueprint_uri
        if self.request.resume:
            blueprint_uri = prepare_resume_blueprint(blueprint_uri)

        self.simulation = ROMSSimulation.from_blueprint(blueprint_uri)
        self.simulation.name = slugify(self.simulation.name)

    @override
    def _on_start(self) -> None:
        super()._on_start()

        if not self.simulation:
            msg = "Simulation creation failed. Unable to execute simulation runner"
            raise RuntimeError(msg)

        ic = self.simulation.initial_conditions
        if ic is not None and self.simulation.start_date is not None:
            warn_on_restart_start_date_mismatch(
                ic.source.location, self.simulation.start_date, log=self.log
            )

        if self.request.resume:
            self.log.info("Resuming simulation from its existing working directory")
            self.simulation.attach()
        else:
            self.log.trace("Setting up simulation")
            self.simulation.setup(clobber=self.request.clobber)
            self.log.trace("Building simulation")
            self.simulation.build()
            self.log.trace("Executing simulation pre-run")
            self.simulation.pre_run()

            if self.request.pre_run:
                plan = self.simulation.prepare_launch(job_name=self._job_cfg.job_name)
                self.log.info(
                    "Pre-run complete; the model was not launched. "
                    "Prepared command: %s (in %s)",
                    plan.command,
                    plan.run_path,
                )
                # a terminal state stops the service before its event loop
                # starts, so neither `run` nor post-run is reached
                self.add_state(ExecutionStatus.COMPLETED)
                return

        self.log.trace("Starting simulation.")
        self._handler = self.simulation.run(
            account_key=self._job_cfg.account_id,
            walltime=self._job_cfg.walltime,
            queue_name=self._job_cfg.priority,
            job_name=self._job_cfg.job_name,
        )

    @override
    async def run(self) -> RunnerResult[UclaRomsBlueprint]:
        """Execute the c-star simulation."""
        if self._handler is None:
            msg = "Simulation did not start up successfully"
            self.add_state(ExecutionStatus.FAILED, msg)
            raise CstarError(msg)

        try:
            await self._handler.updates(seconds=1.0)
            status = self._handler.status
            if status == ExecutionStatus.FAILED:
                self.add_state(status, ["ROMS execution reported a failed status"])
            else:
                self.add_state(status)
        except Exception as ex:
            msg = "An error occurred while running the simulation"
            self.log.exception(msg)
            self.add_state(ExecutionStatus.FAILED, [msg, str(ex)])

        return self.result

    @override
    def _on_iteration_complete(self) -> None:
        """Perform post-processing after each iteration of the main event loop."""
        super()._on_iteration_complete()
        if self.state.status == ExecutionStatus.COMPLETED:
            self.simulation.post_run()
            # reporting never changes the outcome: the declared patterns still
            # list the files at shutdown, without this metadata
            try:
                self.products.extend(
                    output_products(
                        self.simulation.fs_manager.output_dir,
                        self.blueprint.effective_working_dir,
                        use_pio=self.simulation.use_pio,
                    )
                )
            except Exception as ex:
                self.log.warning(f"Unable to describe the output files: {ex}")
        elif ExecutionStatus.is_terminal(self.state.status):
            msg = "Skipping simulation post-run; simulation did not complete."
            self.log.debug(msg)


@register_application
class UclaRomsApplication(ApplicationDefinition[UclaRomsBlueprint, UclaRomsRunner]):
    name: str = APP_NAME
    long_name: str = _APP_NAME_LONG
    runner = UclaRomsRunner
    blueprint = UclaRomsBlueprint
    applicable_transforms = (RomsTimeSplitter,)
    directives = (
        ContinuanceDirective,
        NestingDirective,
        CarbonateSensitivityDirective,
    )
    resumable = True
    pre_runnable = True
    migrations = (
        UclaRomsSchemaAdapter2025v1,
        UclaRomsSchemaAdapterV2V21,
        UclaRomsSchemaAdapterV21V3,
    )

    @override
    def artifacts(self, blueprint: UclaRomsBlueprint) -> "Sequence[ArtifactSpec]":
        """Declare the files a UCLA-ROMS run leaves in its step root.

        Parameters
        ----------
        blueprint : UclaRomsBlueprint
            The blueprint of the UCLA-ROMS step.

        Returns
        -------
        Sequence[ArtifactSpec]
            The joined output files by stream (the streams other steps consume
            under their own kinds), the namelist, cppdefs and MARBL configuration
            the model read, the executable and the run log.
        """
        # a manager on a placeholder root only spells the layout; it touches no disk
        fs = RomsFileSystemManager(Path("/"))

        def rel(directory: Path, name: str) -> str:
            return (directory / name).relative_to(fs.root_dir).as_posix()

        streams = [
            ArtifactSpec(
                kind=kind,
                pattern=rel(fs.output_dir, f"{stem}.*.nc"),
                metadata=dict(metadata),
            )
            for stem, kind, metadata in OUTPUT_STREAMS
        ]
        return [
            *streams,
            ArtifactSpec(kind=ArtifactKind.output, pattern=rel(fs.output_dir, "*.nc")),
            ArtifactSpec(
                kind=ArtifactKind.config,
                pattern=rel(fs.run_dir, GENERATED_NAMELIST_NAME),
                metadata={"role": "namelist"},
            ),
            ArtifactSpec(
                kind=ArtifactKind.config,
                pattern=rel(fs.compile_time_code_dir, "cppdefs.opt"),
                metadata={"role": "cppdefs"},
            ),
            ArtifactSpec(
                kind=ArtifactKind.config,
                pattern=rel(fs.runtime_code_dir, "marbl_in"),
                metadata={"role": "marbl_in"},
            ),
            ArtifactSpec(
                kind=ArtifactKind.build,
                pattern=rel(fs.compile_time_code_dir, ROMS_EXECUTABLE_NAME),
            ),
            ArtifactSpec(kind=ArtifactKind.log, pattern=rel(fs.logs_dir, "*.out")),
        ]


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
    service_cfg = get_service_config(args.log_level, name="UclaRomsRunner")

    blueprint_uri = str(args.blueprint_uri)
    if args.directives:
        blueprint_uri = DirectiveConfig.apply_directives(args.directives, blueprint_uri)

    request = RunnerRequest(
        blueprint_uri,
        UclaRomsBlueprint,
        resume=args.resume,
        pre_run=args.pre_run,
        clobber=args.clobber,
        step=args.step,
    )
    runner = UclaRomsRunner(request, service_cfg, job_cfg)

    try:
        asyncio.run(runner.execute())
        if runner.state.status == ExecutionStatus.FAILED or runner.result.errors:
            if runner.result.errors:
                print(f"Errors occurred: {', '.join(runner.result.errors)}")
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
