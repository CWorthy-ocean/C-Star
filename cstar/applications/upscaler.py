import re
import typing as t
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import xarray as xr
from pydantic import Field

from cstar.applications.core import (
    ApplicationDefinition,
    ArtifactKind,
    ArtifactSpec,
    RunnerResult,
    register_application,
)
from cstar.applications.roms_marbl.outputs import UPSCALING_SUFFIX
from cstar.base.log import get_logger
from cstar.base.utils import convert_to_cdf5
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.file_system import JobFileSystemManager
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Blueprint

APP_NAME: t.Final[str] = "upscaler"
"""The unique identifier for the upscaler application type."""
_APP_NAME_LONG: t.Final[str] = "Nesting Data Processor"
"""The long-form application name."""

log = get_logger(__name__)

UPSCALED_CDR_NAME: t.Final[str] = "upscaled_cdr.nc"
"""The name of the upscaled CDR forcing file."""

BASE_TRACER_NAMES: t.Final[tuple[str, ...]] = ("ALK", "DIC")
"""Tracers always written to ``cdr_trcflx_profile``: MARBL's ALK and DIC
additionality (versus their ``ALT_CO2`` analogues), the first two columns."""

CDR_ADD_VAR_RE: t.Final[re.Pattern[str]] = re.compile(
    r"^(?P<tracer>CDR_(?P<family>OAE_ALK|OAE_DIC|DOR_DIC)(?P<index>\d+))"
    r"_add_(?P<bry>north|south|east|west)$"
)
"""``_uscl`` variables holding the boundary fluxes of dedicated CDR-lite tracers
(ucla-roms >= 0.11.0 writes ``<tracer>_add_<side>`` for each)."""

_CDR_FAMILY_ORDER: t.Final[dict[str, tuple[int, int]]] = {
    "OAE_ALK": (0, 0),
    "OAE_DIC": (0, 1),
    "DOR_DIC": (1, 0),
}
"""(group, slot) of each CDR-lite tracer family in ROMS tracer order: OAE ALK/DIC
pairs by index, then DOR DICs."""


def discover_cdr_tracer_names(
    dataset: xr.Dataset, child_boundaries: dict[str, bool]
) -> list[str]:
    """CDR-lite tracers whose fluxes are present on every active boundary, in ROMS
    tracer order. A tracer missing from an active boundary is left out, with a
    warning: ucla-roms writes every CDR-lite tracer on every open boundary, so a
    gap means the `_uscl` files are incomplete.
    """
    active = {bry for bry, is_active in child_boundaries.items() if is_active}
    if not active:
        return []
    present: dict[str, set[str]] = {}
    order: dict[str, tuple[int, int, int]] = {}
    for varname in dataset.variables:
        m = CDR_ADD_VAR_RE.fullmatch(str(varname))
        if m:
            tracer = m.group("tracer")
            present.setdefault(tracer, set()).add(m.group("bry"))
            group, slot = _CDR_FAMILY_ORDER[m.group("family")]
            order[tracer] = (group, int(m.group("index")), slot)
    complete = []
    for tracer, brys in present.items():
        if missing := sorted(active - brys):
            log.warning(
                f"Leaving {tracer} out of the CDR forcing: the `_uscl` files have "
                f"no {tracer}_add_<side> for the active boundaries {missing}."
            )
        else:
            complete.append(tracer)
    return sorted(complete, key=order.__getitem__)


class UpscalerBlueprint(Blueprint):
    """A blueprint used to perform upscaling of a high resolution nested grid to a lower resolution parent grid."""

    application: str = APP_NAME
    """The application identifier."""
    uscl_file_location: str
    """Path to the .uscl file."""
    pio: bool = Field(default=True)
    """Whether the target ROMS build reads its inputs via ParallelIO, which requires
    CDF-5 (``NETCDF3_64BIT_DATA``) files. When True (the default), the CDR forcing
    file is written in xarray's default NETCDF4 format (fast) under an
    ``_nc4``-suffixed name, then converted to CDF-5 at the intended filename via an
    ``nccopy -k cdf5`` subprocess. When False, the NETCDF4 file is written directly."""


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

        files: list[Path | str] = list(
            uscl_dir.glob(f"*{UPSCALING_SUFFIX}.??????????????.nc")
        )
        if len(files) == 0:
            msg = f"No uscl files found in {uscl_dir}"
            raise FileNotFoundError(msg)

        out_path = JobFileSystemManager(self.blueprint.effective_working_dir).output_dir
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / UPSCALED_CDR_NAME

        cdr_upscaler = CDRUpscaler(files)
        cdr_upscaler.create_cdr_dataset()
        cdr_upscaler.populate_cdr_dataset()
        if self.blueprint.pio:
            nc4_file = out_file.with_name(out_file.stem + "_nc4" + out_file.suffix)
            cdr_upscaler.save(nc4_file)
            convert_to_cdf5(nc4_file, out_file)
        else:
            cdr_upscaler.save(out_file)
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

    @t.override
    def artifacts(self, blueprint: UpscalerBlueprint) -> Sequence[ArtifactSpec]:
        """Declare the upscaled CDR forcing file an upscaler run leaves.

        Parameters
        ----------
        blueprint : UpscalerBlueprint
            The blueprint of the upscaler step.

        Returns
        -------
        Sequence[ArtifactSpec]
            The CDR forcing pattern.
        """
        # a manager on a placeholder root only spells the layout; it touches no disk
        fs = JobFileSystemManager(Path("/"))
        output = fs.output_dir.relative_to(fs.root_dir)
        return [
            ArtifactSpec(
                kind=ArtifactKind.cdr_forcing,
                pattern=(output / UPSCALED_CDR_NAME).as_posix(),
            )
        ]


class CDRUpscaler:
    """
    Class for transforming CDR-relevant boundary tracers to CDR forcing profiles.

    ALK and DIC additionality are always written to ``cdr_trcflx_profile``. If the
    ``_uscl`` files also hold dedicated CDR-lite tracers (``CDR_OAE_ALK*``,
    ``CDR_OAE_DIC*``, ``CDR_DOR_DIC*``, written by ucla-roms >= 0.11.0), their
    fluxes follow along the tracer axis, and ``cdr_trc_name`` records which tracer
    each column is. ROMS (>= 0.11.0) uses the names to apply each column to the
    tracer of that name, skipping tracers the parent run does not have (e.g. ALK
    and DIC without MARBL); older releases read the first two columns as ALK and
    DIC. Without CDR-lite tracers the file is unchanged: two columns, no names.

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

    files: list[Path | str]
    """Paths to data files used by the upscaler."""

    def __init__(self, files: list[Path | str]) -> None:
        """
        Generate a CDRUpscaler instance from a list of joined, time-evolving `_uscl` files.

        Sets simulation- and domain-related attributes eta_rho, xi_rho, s_rho and time.
        Determines how many boundary columns are present to be transformed into CDR profiles.
        """
        self.files = files
        self.validate()

        log.debug(f"Opening {len(self.files)} `_uscl` files...")
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
        log.debug(
            f"Found {self.n_profiles} boundary tracer profiles to convert to CDR forcing profiles"
        )

        cdr_tracer_names = discover_cdr_tracer_names(
            self.uscl_dataset, self.child_boundaries
        )
        self.tracer_names: list[str] = list(BASE_TRACER_NAMES) + cdr_tracer_names
        if cdr_tracer_names:
            log.info(
                "Found CDR tracers in `_uscl` files: " + ", ".join(cdr_tracer_names)
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
        prefixes = [str(f).split("_uscl")[0] for f in self.files]
        if not all([prefix == prefixes[0] for prefix in prefixes]):
            raise ValueError(
                "Files must have a common prefix. Received: {', '.join(set(prefixes))}"
            )
        return prefixes[0]

    def create_cdr_dataset(self) -> None:
        """
        Initialize the CDR forcing dataset based on boundary geometry.

        Updates the CDRUpscaler.cdr_dataset attribute.

        Creates the following variables:
        - cdr_trcflx_profile: Series of time-evolving tracer flux profiles flux(time,depth,tracer,profile no.)
          ALK and DIC are the first two tracers; CDR-lite tracers, if any, follow.
        - cdr_trc_name: Tracer names along the tracer axis (only with CDR-lite tracers)
        - cdr_lon, cdr_lat: 1D arrays specifying where each tracer flux profile is located
        - cdr_layer_thickness: Series of time-evolving layer heights used to remap the profiles onto the parent grid
        """
        ds = xr.Dataset()
        n_trc = len(self.tracer_names)
        named = n_trc > len(BASE_TRACER_NAMES)
        trc_dim = "ntrc" if named else "two"

        ds["cdr_trcflx_profile"] = xr.DataArray(
            np.zeros((len(self.time), len(self.s_rho), n_trc, self.n_profiles)),
            dims=("cdr_time", "s_rho", trc_dim, "ncdr_prof"),
            attrs={
                "long_name": "tracer flux [mmol/s]",
                "units": "mmol/s",
            },
        )
        if named:
            ds["cdr_trc_name"] = xr.DataArray(
                np.array(self.tracer_names),
                dims=(trc_dim,),
                attrs={"long_name": "names of tracers stored in cdr_trcflx_profile"},
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

    def populate_cdr_dataset(self) -> None:
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

        if self.cdr_dataset is None:
            msg = "Unable to populate null dataset."
            raise RuntimeError(msg)

        log.info("Populating `cdr_lat`...")
        self.cdr_dataset["cdr_lat"][:] = boundaries_to_profiles("lat")
        log.info("Populating `cdr_lon`...")
        self.cdr_dataset["cdr_lon"][:] = boundaries_to_profiles("lon")
        for i_trc, name in enumerate(self.tracer_names):
            log.info(f"Populating `cdr_trcflx_profile` for tracer {name}...")
            self.cdr_dataset["cdr_trcflx_profile"][:, :, i_trc, :] = (
                boundaries_to_profiles(f"{name}_add")
            )
        log.info("Populating `cdr_layer_thickness`...")
        self.cdr_dataset["cdr_layer_thickness"][:] = boundaries_to_profiles("h")

    def save(self, filename: str | Path | None = None) -> Path:
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

        msg = f"Saving output to {filepath}"
        log.info(msg)

        # ROMS (Fortran) reads tracer names as fixed-length characters
        # (NC_CHAR); xarray would otherwise write variable-length NC_STRING.
        encoding = {}
        if "cdr_trc_name" in self.cdr_dataset:
            encoding["cdr_trc_name"] = {"dtype": "S1", "char_dim_name": "trc_name_len"}
        self.cdr_dataset.to_netcdf(filepath, encoding=encoding)

        msg = "CDRUpscaler.save is done."
        log.info(msg)

        return filepath
