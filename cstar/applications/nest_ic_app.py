import typing as t
from pathlib import Path

import xarray as xr

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.applications.roms_marbl.transforms import RestartFile
from cstar.base.log import get_logger
from cstar.base.utils import lazy_import
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint

roms_tools = lazy_import("roms_tools")

APP_NAME: t.Final[str] = "nest_ic"
"""The unique identifier for the nest_ic application type."""
_APP_NAME_LONG: t.Final[str] = "Nesting Data Processor"
"""The long-form application name."""

log = get_logger(__name__)


class NestIcBlueprint(Blueprint):
    """A blueprint for an example plotting application."""

    application: str = APP_NAME
    """The application identifier."""
    parent_rst: Path
    parent_grid: Path
    """Path to a netCDF file defining the parent grid attributes."""
    child_grid: Path
    """Path to a netCDF file defining the child grid attributes."""
    output_dir: Path


class NestIcRunner(BlueprintRunner[NestIcBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    application: str = APP_NAME
    """The application identifier."""

    @t.override
    async def run(self) -> RunnerResult[NestIcBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.trace("Executing handler function on blueprint runner")
        self.log.info(f"Running nest ic application for {self.blueprint}")

        self._create_initial_conditions()
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result

    @staticmethod
    def _has_bgc(filepath: Path) -> bool:
        """Check if the parent restart file has BGC tracers."""
        with xr.open_dataset(filepath) as ds:
            # Example: check if nitrate, DIC exist
            # These are specific to MARBL but other than capitalization are likely to be in any BGC or mCDR model
            bgc_vars = ["NO3", "DIC", "no3", "dic"]
            return any(var in ds.variables for var in bgc_vars)

    def _create_initial_conditions(
        self,
    ) -> Path:
    """Create initial conditions for a simulation using the parent grid and restart file.
    
    Returns
    -------
    Path
        The path to the persisted `InitialConditions`
    """

        rst = RestartFile(path=self.blueprint.parent_rst)
        restart_date = rst.timestamp

        parent_grid = roms_tools.Grid(filename=self.blueprint.parent_grid)

        # build InitialConditions kwargs
        ic_kwargs = dict(
            grid=roms_tools.Grid(filename=self.blueprint.child_grid),
            ini_time=restart_date,
            source={
                "name": "ROMS",
                "grid": parent_grid,
                "path": self.blueprint.parent_rst,
            },
            use_dask=True,
        )

        # only add bgc_source if parent has BGC
        if self._has_bgc(self.blueprint.parent_rst):
            ic_kwargs["bgc_source"] = {
                "name": "ROMS",
                "grid": parent_grid,
                "path": self.blueprint.parent_rst,
            }

        fname = f"ic_from_parent_rst.{rst.formatted_timestamp}.nc"
        path = Path(self.blueprint.output_dir).expanduser() / fname
        path.parent.mkdir(parents=True, exist_ok=True)

        ic = roms_tools.InitialConditions(**ic_kwargs)

        # Write out initial conditions file for child

        ic.save(path)

        return path


@register_application
class NestIcApplication(
    ApplicationDefinition[NestIcBlueprint, NestIcRunner],
):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = NestIcRunner
    blueprint = NestIcBlueprint
    applicable_transforms = ()
