import typing as t
from pathlib import Path

from roms_tools import Grid, ROMSOutput

from cstar.base.log import get_logger
from cstar.entrypoint.xrunner import (
    XBlueprintRunner,
    XRunnerResult,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.application import register_blueprint, register_runner
from cstar.orchestration.models import Blueprint

APP_PLOTTER: t.Literal["plotter"] = "plotter"
"""The unique identifier for the plotter application type."""

log = get_logger(__name__)


@register_blueprint(APP_PLOTTER)
class PlotterBlueprint(Blueprint):
    """A blueprint for an example plotting application."""

    application: str = APP_PLOTTER
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


@register_runner(APP_PLOTTER)
class PlotterRunner(XBlueprintRunner[PlotterBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    application: str = APP_PLOTTER
    """The application identifier."""

    @t.override
    def __call__(self) -> XRunnerResult[PlotterBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.debug("Executing handler function on blueprint runner")
        if not isinstance(self.blueprint, PlotterBlueprint):
            raise ValueError(
                f"PlotterBlueprint expected. Received: {type(self.blueprint)}"
            )

        make_plots(**self.blueprint.dict())
        return self.set_result(ExecutionStatus.COMPLETED)


def make_plots(
    input_dir: Path,
    output_dir: Path,
    variables: list[str],
    time_indices: list[int],
    s_indices: list[int],
    grid_file_path: Path,
    file_glob: str,
    **kwargs,
) -> None:
    """
    Make 2D plots of all combinations of the provided variables, time indices, and depth indices.

    Parameters
    ----------
    input_dir: location of ROMS output files
    output_dir: location to save the plots
    variables: variables to plot
    time_indices: time indices to plot
    s_indices: depth indices to plot
    grid_file_path: location of the grid file corresponding to the ROMS outputs
    file_glob: glob pattern to match the file names to open for plotting
    kwargs: unused; intended to capture any unused blueprint params that are unpacked

    Returns
    -------
    None
    """
    input_dir = input_dir.expanduser().resolve()
    grid = Grid(filename=grid_file_path)
    roms = ROMSOutput(path=input_dir / file_glob, grid=grid, use_dask=True)
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for var in variables:
        for s in s_indices:
            for time in time_indices:
                out_path = output_dir / f"{var}_s{s}_t{time}.png"
                try:
                    roms.ds.variables[var][s][time].compute()
                    roms.plot(var, time=time, s=s, save_path=str(out_path))
                except Exception:
                    log.exception(
                        f"Exception while plotting var {var}, s {s}, time {time}"
                    )
