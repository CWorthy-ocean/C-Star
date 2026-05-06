import typing as t
from pathlib import Path

import xarray as xr
from roms_tools import Grid, InitialConditions

from cstar.base.log import get_logger
from cstar.entrypoint.xrunner import (
    XBlueprintRunner,
    XRunnerResult,
)
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.application import register_blueprint, register_runner
from cstar.orchestration.models import Blueprint
from cstar.orchestration.transforms import RestartFile

APP_NEST_IC: t.Literal["nest_ic"] = "nest_ic"
"""The unique identifier for the nest_ic application type."""

log = get_logger(__name__)


@register_blueprint(APP_NEST_IC)
class NestIcBlueprint(Blueprint):
    """A blueprint for an example plotting application."""

    application: str = APP_NEST_IC
    """The application identifier."""
    parent_rst: Path
    parent_grid: Path
    child_grid: Path
    output_dir: Path


@register_runner(APP_NEST_IC)
class NestIcRunner(XBlueprintRunner[NestIcBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    application: str = APP_NEST_IC
    """The application identifier."""

    @t.override
    def __call__(self) -> XRunnerResult[NestIcBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.debug("Executing handler function on blueprint runner")
        if not isinstance(self.blueprint, NestIcBlueprint):
            raise ValueError(
                f"NestIcBlueprint expected. Received: {type(self.blueprint)}"
            )
        self.log.info(f"Running nest ic application for {self.blueprint}")

        _create_initial_conditions(
            self.blueprint.output_dir,
            self.blueprint.parent_rst,
            self.blueprint.child_grid,
            self.blueprint.parent_grid,
        )
        return self.set_result(ExecutionStatus.COMPLETED)


def _has_bgc(filepath):
    """Check if the parent restart file has BGC tracers."""
    with xr.open_dataset(filepath) as ds:
        # Example: check if nitrate, DIC exist
        # These are specific to MARBL but other than capitalization are likely to be in any BGC or mCDR model
        bgc_vars = ["NO3", "DIC", "no3", "dic"]
        return any(var in ds.variables for var in bgc_vars)


def _create_initial_conditions(
    output_dir, restart_file, child_grid_path, parent_grid_path
):

    rst = RestartFile(path=restart_file)
    restart_date = rst.timestamp

    parent_grid = Grid(filename=parent_grid_path)

    # build InitialConditions kwargs
    ic_kwargs = dict(
        grid=Grid(filename=child_grid_path),
        ini_time=restart_date,
        source={"name": "ROMS", "grid": parent_grid, "path": restart_file},
        use_dask=True,
        # model_reference_date=model_reference_date
    )

    # only add bgc_source if parent has BGC
    if _has_bgc(restart_file):
        ic_kwargs["bgc_source"] = {
            "name": "ROMS",
            "grid": parent_grid,
            "path": restart_file,
        }

    fname = f"ic_from_parent_rst.{rst.formatted_timestamp}.nc"
    path = Path(output_dir).expanduser() / fname
    path.parent.mkdir(parents=True, exist_ok=True)

    ic = InitialConditions(**ic_kwargs)

    # Write out initial conditions file for child

    ic.save(path)

    return path
