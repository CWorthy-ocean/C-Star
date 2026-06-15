import typing as t
from pathlib import Path

import numpy as np
import xarray as xr

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerResult,
    register_application,
)
from cstar.base.log import get_logger
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint

APP_NAME: t.Final[str] = "upscaler"
"""The unique identifier for the upscaler application type."""
_APP_NAME_LONG: t.Literal["Nesting Data Processor"] = "Nesting Data Processor"
"""The long-form application name."""

log = get_logger(__name__)


class UpscalerBlueprint(Blueprint):
    """A blueprint used to perform upscaling of a high resolution nested grid to a lower resolution parent grid."""

    application: str = APP_NAME
    """The application identifier."""

    uscl_file_location: str
    output_dir: Path


class UpscalerRunner(BlueprintRunner[UpscalerBlueprint]):
    """Worker class to execute a simple plotting application.

    This application is intended primarily as an example of how to build a functioning
    application to perform a custom task, rather than a fully-featured plotting utility
    intended for scientific use.
    """

    application: str = APP_NAME
    """The application identifier."""

    @t.override
    async def run(self) -> RunnerResult[UpscalerBlueprint]:
        """Process the blueprint.

        Returns
        -------
        RunnerResult
            The result of the blueprint processing.
        """
        self.log.trace("Executing handler function on blueprint runner")
        self.log.info(f"Running upscaler application for {self.blueprint}")

        uscl_dir = Path(self.blueprint.uscl_file_location)
        if not Path(uscl_dir).exists() or not uscl_dir.is_dir():
            msg = f"Specified path of upscale files doesn't exist or isn't a directory: {uscl_dir}"
            raise FileNotFoundError(msg)

        files = list(uscl_dir.glob("*_uscl.??????????????.nc"))
        if len(files) == 0:
            msg = f"No uscl files found in {uscl_dir}"
            raise FileNotFoundError(msg)

        out_path = Path(self.blueprint.output_dir / "output")
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / "upscaled_cdr.nc"

        cdr_upscaler = CDRUpscaler(files)
        cdr_upscaler.create_cdr_dataset()
        cdr_upscaler.populate_cdr_dataset()
        cdr_upscaler.save(str(out_file))
        self.add_state(ExecutionStatus.COMPLETED)
        return self.result


@register_application
class UpscalerApplication(
    ApplicationDefinition[UpscalerBlueprint, UpscalerRunner],
):
    name = APP_NAME
    long_name = _APP_NAME_LONG
    runner = UpscalerRunner
    blueprint = UpscalerBlueprint
    applicable_transforms = ()


class CDRUpscaler:
    """
    Class for transforming CDR-relevant boundary tracers to CDR forcing profiles.

    Use
    ---
    1. Initialize with a list of _joined_ `_uscl` files, e.g.
      `cu = CDRUpscaler(["roms_uscl.19920211170000.nc","roms_uscl.19920211180000.nc"])`
    This will open an xarray multi-file dataset with these files.

    2. A CDR forcing dataset based on the properties of these files can be initialized with
    `cu.create_cdr_dataset()`

    3. The CDR forcing dataset can then be populated based on the `uscl` data using
    `cu.populate_cdr_dataset()`

    4. The CDR forcing dataset can then be saved with
    `cu.save()`
    """

    def __init__(self, files: list):
        """
        Generate a CDRUpscaler instance from a list of joined, time-evolving `_uscl` files.

        Sets simulation- and domain-related attributes eta_rho, xi_rho, s_rho and time.
        Determines how many boundary columns are present to be transformed into CDR profiles.
        """
        self.files = files

        self.validate()

        print(f"Opening {len(self.files)} `_uscl` files...")
        self.uscl_dataset = xr.open_mfdataset(
            self.files,
            concat_dim="time",
            combine="nested",
            data_vars="minimal",
            coords="minimal",
        )
        self.eta_rho: xr.DataArray = self.uscl_dataset.eta_rho
        self.xi_rho: xr.DataArray = self.uscl_dataset.xi_rho
        self.s_rho: xr.DataArray = self.uscl_dataset.s_rho
        self.time: xr.DataArray = self.uscl_dataset.time

        # Determine active boundaries and add up all columns to get no. profiles
        self.child_boundaries: dict = {}
        self.n_profiles: int = 0
        for bry in ["north", "south", "east", "west"]:
            v = f"ALK_add_{bry}"
            if v in self.uscl_dataset.variables:
                self.child_boundaries[bry] = True
                rho_var = "eta_rho" if bry.endswith("st") else "xi_rho"
                self.n_profiles = self.n_profiles + self.uscl_dataset.sizes.get(rho_var)
        print(
            f"Found {self.n_profiles} boundary tracer profiles to convert to CDR forcing profiles"
        )

        # Initialize CDR dataset
        self.cdr_dataset: xr.Dataset | None = None

    def validate(self) -> None:
        """Run validation checks on input files."""
        for i, f in enumerate(self.files):
            ds = xr.open_dataset(f)
            if i == 0:
                reference_sizes = {k: v for k, v in ds.sizes.items() if k != "time"}
                continue
            ds_sizes = {k: v for k, v in ds.sizes.items() if k != "time"}
            if ds_sizes != reference_sizes:
                raise ValueError(
                    "Dimensions of input files do not match. "
                    + "HINT: use ncjoin to join files before calling uscl_to_cdr"
                )

    @property
    def filename_prefix(self) -> str:
        """
        Determine the common prefix of input `_uscl` files.

        Used to create default output filename for CDRUpscaler.save()

        Returns
        -------
        str: the common filename prefix of the CDRUpscaler inputs

        Examples
        --------
        CDRUpscaler(["output/roms_run_uscl.20250102130000.nc",
                     "output/roms_run_uscl.20250103130000.nc"
                    ]).filename_prefix()
        >> "output/roms_run"
        """
        prefixes = [f.split("_uscl")[0] for f in self.files]
        if not all([prefix == prefixes[0] for prefix in prefixes]):
            raise ValueError("Filenames do not share a common prefix")
        return prefixes[0]

    def create_cdr_dataset(self) -> None:
        """
        Initialize the CDR forcing dataset based on boundary geometry.

        Updates the CDRUpscaler.cdr_dataset attribute.

        Creates the following variables:
        - cdr_trcflx_profile: Series of time-evolving tracer flux profiles flux(time,depth,profile no.)
        - cdr_lon, cdr_lat: 1D arrays specifying where each tracer flux profile is located
        - cdr_layer_thickness: Series of time-evolving layer heights used to remap the profiles onto the parent grid
        """
        ds = xr.Dataset()

        ds["cdr_trcflx_profile"] = xr.DataArray(
            np.zeros((len(self.time), len(self.s_rho), 2, self.n_profiles)),
            dims=("cdr_time", "s_rho", "two", "ncdr_prof"),
            attrs={
                "long_name": "tracer flux [mmol/s]",
                "units": "mmol/s",
            },
        )

        ds["cdr_time"] = xr.DataArray(
            self.uscl_dataset.ocean_time.values / 86400.0,
            dims=("cdr_time",),
            attrs={
                "long_name": "CDR forcing time",
                "units": "days since 2000/01/01",
            },
        )

        ds["cdr_lon"] = xr.DataArray(
            np.zeros(self.n_profiles),
            dims=("ncdr_prof",),
            attrs={
                "long_name": "longitude of CDR release [degrees East]",
                "units": "deg E",
            },
        )

        ds["cdr_lat"] = xr.DataArray(
            np.zeros(self.n_profiles),
            dims=("ncdr_prof",),
            attrs={
                "long_name": "latitude of CDR release [degrees North]",
                "units": "deg N",
            },
        )

        ds["cdr_layer_thickness"] = xr.DataArray(
            np.zeros((len(self.time), len(self.s_rho), self.n_profiles)),
            dims=("cdr_time", "s_rho", "ncdr_prof"),
            attrs={
                "long_name": "layer thicknesses of CDR release given in a vertical profile [m]",
                "units": "m",
            },
        )
        self.cdr_dataset = ds

    def populate_cdr_dataset(self):
        """
        Populates the CDRUpscaler.cdr_forcing() xarray Dataset created by CDRUpscaler.create_cdr_dataset().

        Remaps the time-evolving tracer flux columns of each boundary in the `_uscl` dataset to a depth
        profile in the CDR forcing dataset with a corresponding lat and lon.
        """

        def boundaries_to_profiles(var_prefix) -> np.ndarray:
            """Convert boundary columns to a sequence of profiles.

            Concatenates all boundary columns into a single list along
            the final axis (local lateral dimension).

            Note
            ----
            This formulation uses `.values` which triggers compute.
            See boundaries_to_profiles_xr for lazy implementation.
            """
            return np.concatenate(
                [
                    self.uscl_dataset[f"{var_prefix}_{bry}"].values
                    for bry in ["north", "east", "south", "west"]
                    if self.child_boundaries.get(bry)
                ],
                axis=-1,
            )

        def boundaries_to_profiles_xr(var_prefix) -> xr.DataArray:
            """boundaries_to_profiles using xarray/lazy concatenation.

            Should be faster, but isn't. Huge bottleneck during save().
            """
            data_arrays = []
            for bry in ["north", "east", "south", "west"]:
                if self.child_boundaries.get(bry):
                    rho_var = "eta_rho" if bry.endswith("st") else "xi_rho"
                    da = self.uscl_dataset[f"{var_prefix}_{bry}"].rename(
                        {rho_var: "ncdr_prof"}
                    )
                    if "time" in da.dims:
                        da = da.rename({"time": "cdr_time"})
                    data_arrays.append(da)
            return xr.concat(data_arrays, dim="ncdr_prof")

        print("Populating `cdr_lat`...")
        self.cdr_dataset["cdr_lat"][:] = boundaries_to_profiles("lat")
        print("Populating `cdr_lon`...")
        self.cdr_dataset["cdr_lon"][:] = boundaries_to_profiles("lon")
        print("Populating `cdr_trcflx_profile` for tracer ALK...")
        self.cdr_dataset["cdr_trcflx_profile"][:, :, 0, :] = boundaries_to_profiles(
            "ALK_add"
        )
        print("Populating `cdr_trcflx_profile` for tracer DIC...")
        self.cdr_dataset["cdr_trcflx_profile"][:, :, 1, :] = boundaries_to_profiles(
            "DIC_add"
        )
        print("Populating `cdr_layer_thickness`...")
        self.cdr_dataset["cdr_layer_thickness"][:] = boundaries_to_profiles("h")

    def save(self, filename: str | None = None) -> None:
        """
        Save the CDRForcing dataset to a netCDF file to be used in ROMS.

        Parameters
        ----------
        filename (str, optional): the filename of the saved netCDF file.
            Defaults to `CDR_forcing_from_<prefix>` where prefix is the
            shared prefix of the input file list found by
            `CDRUpscaler.filename_prefix()`
        """
        if not self.cdr_dataset:
            raise ValueError(
                "No CDR forcing dataset to save. "
                + "Call CDRUpscaler.create_cdr_dataset() and "
                + "CDRUpscaler.populate_cdr_dataset() first"
            )

        if not filename:
            basename = Path(self.filename_prefix).name
            parent = Path(self.filename_prefix).parent
            filepath = parent / f"cdr_release_profiles_from_{basename}.nc"
        else:
            filepath = Path(filename)

        print(f"Saving output to {filepath}")
        self.cdr_dataset.to_netcdf(filepath)
        print("Done.")
