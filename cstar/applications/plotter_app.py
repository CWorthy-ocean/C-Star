import typing as t
from pathlib import Path

from pydantic import ValidationInfo, field_validator

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.base.adapter import SchemaAdapter
from cstar.base.log import get_logger
from cstar.base.utils import lazy_import
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint
from cstar.system.migration import SchemaBounds

roms_tools = lazy_import("roms_tools")


APP_NAME: t.Final[str] = "plotter"
"""The unique identifier for the plotter application type."""
_APP_NAME_LONG: t.Final[str] = "Plotting Demo Application"
"""The long-form application name."""

log = get_logger(__name__)


class PlotterBlueprint(Blueprint):
    """A blueprint for an example plotting application."""

    application: str = APP_NAME
    """The application identifier."""
    input_dir: Path
    """The location of the inputs for this step (the outputs from ROMS)."""
    variables: list[str]
    """The variables that the application will plot."""
    time_indices: list[int]
    """The time indices to plot"""
    s_indices: list[int]
    """The s (depth) indices to plot"""
    grid_file_path: Path
    """The path to the grid file matching the outputs."""
    file_glob: str = "*rst*.nc"
    """The glob pattern to match the file names to open for plotting."""
    schema_version: str = "2.0.0"
    """The current schema version for this blueprint."""

    @field_validator("input_dir", "grid_file_path", mode="after")
    @classmethod
    def _resolve_input_dir(
        cls,
        value: Path,
        _info: "ValidationInfo",
    ) -> Path:
        return value.expanduser().resolve()


class PlotterRunner(BlueprintRunner[PlotterBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    @t.override
    async def run(self) -> RunnerResult[PlotterBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        msg = f"Running plotter application for {self.blueprint}"
        self.log.info(msg)

        self.make_plots()
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    def make_plots(self) -> None:
        """
        Make 2D plots of all combinations of the provided variables, time indices, and depth indices.

        Plots will be written to the output directory specified in the blueprint.

        Returns
        -------
        None
        """
        blueprint = self.request.blueprint

        input_dir = blueprint.input_dir.expanduser().resolve()
        grid = roms_tools.Grid(filename=blueprint.grid_file_path)
        roms = roms_tools.ROMSOutput(
            path=input_dir / blueprint.file_glob, grid=grid, use_dask=True
        )
        working_dir = blueprint.working_dir
        working_dir.mkdir(parents=True, exist_ok=True)

        for var in blueprint.variables:
            for s in blueprint.s_indices:
                for time in blueprint.time_indices:
                    out_path = working_dir / f"{var}_s{s}_t{time}.png"
                    try:
                        roms.ds.variables[var][s][time].compute()
                        roms.plot(var, time=time, s=s, save_path=str(out_path))
                    except Exception:
                        msg = f"Exception while plotting var {var}, s {s}, time {time}"
                        log.exception(msg)


APP_PLOTTER_SCHEMA_1_0_0: t.Final[str] = "1.0.0"
APP_PLOTTER_SCHEMA_2_0_0: t.Final[str] = "2.0.0"

plotter_bounds: SchemaBounds = {
    "min": APP_PLOTTER_SCHEMA_1_0_0,
    "max": APP_PLOTTER_SCHEMA_2_0_0,
}
"""Schema bounds for the plotter blueprint schema.

The schema bounds enable the migration tool to:
- automatically set version to  minimum version for a blueprint that predated versioning
- configure which version it will target for updates
"""


class PlotterSchemaAdapterV1V2(SchemaAdapter):
    """Schema migration from schema version `1.0.0` to `2.0.0`.

    Adapting `1.0.0` to `2.0.0`:
    - use `working_dir` attribute from `Blueprint` base class instead of `output_dir`
    """

    @classmethod
    def application(cls) -> str:
        return APP_NAME

    @classmethod
    def source(cls) -> str:
        return APP_PLOTTER_SCHEMA_1_0_0

    @classmethod
    def target(cls) -> str:
        return APP_PLOTTER_SCHEMA_2_0_0

    @classmethod
    def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
        # Rename output_dir attribute to match base blueprint 2.0.0 working_dir
        print(model)
        output_dir = model.pop("output_dir")
        model["working_dir"] = output_dir
        return model


@register_application
class PlotterApplication(
    ApplicationDefinition[PlotterBlueprint, PlotterRunner],
):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = PlotterRunner
    blueprint = PlotterBlueprint
    applicable_transforms = ()
    migrations = (PlotterSchemaAdapterV1V2,)
