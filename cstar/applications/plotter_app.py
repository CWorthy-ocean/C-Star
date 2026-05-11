import typing as t
from pathlib import Path

from roms_tools import Grid, ROMSOutput

from cstar.applications.core import ApplicationDefinition, register_application
from cstar.base.log import get_logger
from cstar.entrypoint.runner import (
    XBlueprintRunner,
    XRunnerResult,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint
from cstar.orchestration.transforms import OverrideTransform

APP_NAME: t.Literal["plotter"] = "plotter"
"""The unique identifier for the plotter application type."""
_APP_NAME_LONG: t.Literal["Plotting Demo Application"] = "Plotting Demo Application"
"""The long-form application name."""

log = get_logger(__name__)


class PlotterBlueprint(Blueprint):
    """A blueprint for an example plotting application."""

    application: str = APP_NAME
    """The application identifier."""
    input_dir: Path
    """The location of the inputs for this step (the outputs from ROMS)."""
    output_dir: Path
    """The location to save the plots."""
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


class PlotterRunner(XBlueprintRunner[PlotterBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    @property
    def application(self) -> str:
        """The short, unique application identifier."""
        return APP_NAME

    @t.override
    async def run(self) -> XRunnerResult[PlotterBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        msg = f"Running plotter application for {self.blueprint}"
        self.log.info(msg)

        self.make_plots()
        return self.set_state(ExecutionStatus.COMPLETED)

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
        grid = Grid(filename=blueprint.grid_file_path)
        roms = ROMSOutput(
            path=input_dir / blueprint.file_glob, grid=grid, use_dask=True
        )
        output_dir = blueprint.output_dir.expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        for var in blueprint.variables:
            for s in blueprint.s_indices:
                for time in blueprint.time_indices:
                    out_path = output_dir / f"{var}_s{s}_t{time}.png"
                    try:
                        roms.ds.variables[var][s][time].compute()
                        roms.plot(var, time=time, s=s, save_path=str(out_path))
                    except Exception:
                        log.exception(
                            f"Exception while plotting var {var}, s {s}, time {time}"
                        )


@register_application
class PlotterApplication(
    ApplicationDefinition[PlotterBlueprint, PlotterRunner],
):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = PlotterRunner
    blueprint = PlotterBlueprint
    applicable_transforms = (OverrideTransform,)
