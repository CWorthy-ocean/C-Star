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
from cstar.orchestration.manifest import recorded_files_in, require_recorded_files
from cstar.orchestration.models import Blueprint

APP_NAME: t.Final[str] = "upscaler"
"""The unique identifier for the upscaler application type."""
_APP_NAME_LONG: t.Final[str] = "Nesting Data Processor"
"""The long-form application name."""

log = get_logger(__name__)

UPSCALED_CDR_NAME: t.Final[str] = "upscaled_cdr.nc"
"""The name of the upscaled CDR forcing file."""

_USCL_GLOB: t.Final[str] = f"*{UPSCALING_SUFFIX}.??????????????.nc"
"""The name of a joined `_uscl` file: the stream's suffix and a 14-digit timestamp."""
_USCL_TIMESTAMP: t.Final[re.Pattern[str]] = re.compile(
    rf"{re.escape(UPSCALING_SUFFIX)}\.(\d{{14}})\.nc$"
)
"""The `YYYYMMDDHHMMSS` stamp at the end of a name matched by `_USCL_GLOB`."""

_AVG_BEGIN: t.Final[str] = "avg_begin_time"
"""`_uscl` variable holding the start (s) of each record's averaging window."""
_AVG_END: t.Final[str] = "avg_end_time"
"""`_uscl` variable holding the end (s) of each record's averaging window."""
_WINDOW_TOLERANCE_S: t.Final[float] = 1.0
"""How far (s) a window may start from the end of the one before it, or differ in
length from the first (legacy files)."""
USCL_WINDOWS_FROM_BOUNDS: t.Final[str] = f"{_AVG_BEGIN}/{_AVG_END}"
"""`uscl_windows` attribute of a file whose windows were read from the `_uscl` files."""
USCL_WINDOWS_INFERRED: t.Final[str] = (
    "inferred from consecutive ocean_time stamps (ucla-roms <= 0.12.0)"
)
"""`uscl_windows` attribute of a file whose windows were inferred from the stamps."""
CDR_TIME_CONVENTION: t.Final[str] = (
    "start of interval; the last record closes the final interval"
)
"""`convention` attribute of ``cdr_time`` in ``upscaled_cdr.nc``."""

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


def _uscl_file(path: Path) -> Path:
    """Accept a path named like a joined `_uscl` file.

    Parameters
    ----------
    path : Path
        The path to check.

    Returns
    -------
    Path
        The path.

    Raises
    ------
    ValueError
        If the name does not match `_USCL_GLOB`.
    """
    if not path.match(_USCL_GLOB):
        msg = f"{path.name} is not named like a joined `_uscl` file"
        raise ValueError(msg)
    return path


def _find_uscl_files(uscl_dir: Path) -> list[Path]:
    """Find the upscaling files of a directory.

    The files the record beside `uscl_dir` lists as `upscaling` products are
    used when any of them is a `_uscl` file in `uscl_dir` itself; the record of
    an unrelated step, or none, leaves the directory to be searched.

    Parameters
    ----------
    uscl_dir : Path
        The directory holding the `_uscl` files, a step's `output` directory.

    Returns
    -------
    list[Path]
        The `_uscl` files, empty if there are none.

    Raises
    ------
    CstarError
        If a `_uscl` file the record lists is missing.
    """
    # resolved, so a linked directory reads the record of the step it links to
    resolved = uscl_dir.resolve()
    recorded = recorded_files_in(resolved, ArtifactKind.upscaling, _uscl_file)
    if recorded:
        # the glob would skip a missing file and upscale fluxes with a gap
        require_recorded_files(resolved.parent, recorded)
        return recorded

    log.debug("No recorded upscaling files in %s; searching the directory", uscl_dir)
    return list(uscl_dir.glob(_USCL_GLOB))


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

        files: list[Path | str] = list(_find_uscl_files(uscl_dir))
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


def _uscl_sort_key(file: Path | str) -> str:
    """Order `_uscl` files chronologically by the timestamp in their file name.

    Returns the 14-digit timestamp, or an empty string for a name without one.
    Files without a timestamp therefore sort before every timestamped file, and
    since Python's sort is stable they keep the relative order they were given in.
    """
    match = _USCL_TIMESTAMP.search(Path(file).name)
    return match.group(1) if match else ""


def _file_of_record(files: Sequence[Path | str], index: int) -> Path | str:
    """Return the file holding record `index` of `files` joined along ``time``."""
    counts = []
    for file in files:
        with xr.open_dataset(file) as ds:
            counts.append(ds.sizes["time"])
    return files[int(np.searchsorted(np.cumsum(counts), index, side="right"))]


class _Windows(t.NamedTuple):
    """The averaging windows of the `_uscl` records, as `_averaging_windows` finds them."""

    begin: np.ndarray
    """Start of each window (s)."""
    end: np.ndarray
    """End of each window (s)."""
    first_record: int
    """Index of the `_uscl` record holding the first window's data."""
    attribute: str
    """How the windows were found, as the `uscl_windows` attribute spells it."""


def _averaging_windows(dataset: xr.Dataset, files: Sequence[Path | str]) -> _Windows:
    """Find the averaging windows of the records of the concatenated `_uscl` files.

    Files with ``avg_begin_time``/``avg_end_time`` give their windows directly.
    Without them (ucla-roms <= 0.12.0), each record is the average over the hour
    ending at its ``ocean_time``, and the first is a one-step start-up sample
    that covers no window: the windows run between consecutive stamps and the
    first record is skipped.

    Parameters
    ----------
    dataset : xr.Dataset
        The `_uscl` files, concatenated along ``time``, with ``ocean_time``
        strictly increasing.
    files : Sequence[Path | str]
        The files of `dataset`, in order, to name in errors.

    Returns
    -------
    _Windows
        The windows, in the time units of ``ocean_time``.

    Raises
    ------
    ValueError
        If there is no window, or a window is empty. With bounds, if a window does
        not start where the one before it ends. Without, if a window other than
        the last is not as long as the first, or the last is longer.
    """
    from_bounds = _AVG_BEGIN in dataset.variables
    if from_bounds:
        begin = dataset[_AVG_BEGIN].values
        end = dataset[_AVG_END].values
        first_record, attribute = 0, USCL_WINDOWS_FROM_BOUNDS
    else:
        stamps = dataset.ocean_time.values
        begin, end = stamps[:-1], stamps[1:]
        first_record, attribute = 1, USCL_WINDOWS_INFERRED
    if begin.size == 0:
        msg = "The `_uscl` files hold no averaging window: at least one is needed."
        raise ValueError(msg)

    def offender(window: int) -> str:
        record = first_record + window
        return (
            f"window {window} ({begin[window]:.0f}-{end[window]:.0f} s, in "
            f"{Path(_file_of_record(files, record)).name})"
        )

    empty = np.flatnonzero(~(end > begin))
    if empty.size:
        msg = f"The averaging window ends before it begins: {offender(int(empty[0]))}."
        raise ValueError(msg)
    if from_bounds:
        broken = np.flatnonzero(np.abs(begin[1:] - end[:-1]) > _WINDOW_TOLERANCE_S)
        if broken.size:
            window = int(broken[0]) + 1
            shift = begin[window] - end[window - 1]
            msg = (
                f"The averaging windows are not contiguous: {offender(window)} begins "
                f"{abs(shift):.0f} s {'after' if shift > 0 else 'before'} the window "
                f"before it ends ({end[window - 1]:.0f} s), a "
                f"{'gap' if shift > 0 else 'overlap'} between the `_uscl` files."
            )
            raise ValueError(msg)
    else:
        # The windows are the stamp spacing, so a missing `_uscl` file shows only as
        # one window longer than the first; a partial final window is shorter.
        length = end - begin
        wrong = np.abs(length[:-1] - length[0]) > _WINDOW_TOLERANCE_S
        wrong = np.append(wrong, length[-1] > length[0] + _WINDOW_TOLERANCE_S)
        if wrong.any():
            window = int(np.flatnonzero(wrong)[0])
            msg = (
                f"The averaging windows are not of equal length: {offender(window)} "
                f"is {length[window]:.0f} s long, where {length[0]:.0f} s is "
                "expected. This suggests a missing or out-of-run `_uscl` file."
            )
            raise ValueError(msg)
    return _Windows(begin, end, first_record, attribute)


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

    Time convention: each `_uscl` record is the child's average export over a
    window, and ROMS holds each ``cdr_trcflx_profile`` record from its ``cdr_time``
    until the next (start of interval), aborting without a record at or after the
    last model time. ``cdr_time`` is therefore the start of each window plus the
    end of the last, and the file is one record longer than there are windows: the
    flux and layer thickness of each window, then a closing record (zero flux, the
    last thickness repeated) that ROMS never applies. Windows are read from
    ``avg_begin_time``/``avg_end_time`` when the files have them. ucla-roms
    <= 0.12.0 writes neither: each record is the average over the hour *ending*
    at its ``ocean_time`` and the first is a start-up sample, so the windows are
    inferred from consecutive stamps and the first record is dropped. Copying
    stamps and records one to one would apply every hour an hour late and never
    apply the last.

    Use
    ---
    1. Initialize with a list of _joined_ `_uscl` files, e.g.
      `cu = CDRUpscaler(["roms_uscl.19920211170000.nc","roms_uscl.19920211180000.nc"])`
    This will open an xarray multi-file dataset with these files, concatenated in
    chronological order of the timestamps in their names regardless of the order given
    (files without a timestamp come first, in the order given).

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

        Raises
        ------
        ValueError
            If the dimensions of the files do not match, only some files have
            averaging bounds, the concatenated `ocean_time` is not strictly
            increasing, or the averaging windows are not contiguous (or, without
            bounds, not of equal length).
        """
        self.files = sorted(files, key=_uscl_sort_key)
        self.validate()

        log.debug(f"Opening {len(self.files)} `_uscl` files...")
        self.uscl_dataset = xr.open_mfdataset(
            self.files,
            concat_dim="time",
            combine="nested",
            data_vars="minimal",
            coords="minimal",
        )
        ocean_time = self.uscl_dataset.ocean_time.values
        not_increasing = np.flatnonzero(~(np.diff(ocean_time) > 0))
        if not_increasing.size:
            self.uscl_dataset.close()
            record = int(not_increasing[0]) + 1
            previous_file = Path(_file_of_record(self.files, record - 1)).name
            record_file = Path(_file_of_record(self.files, record)).name
            msg = (
                "`ocean_time` of the concatenated `_uscl` files is not strictly "
                f"increasing: record {record} ({ocean_time[record]:.0f} s, in "
                f"{record_file}) does not follow record {record - 1} "
                f"({ocean_time[record - 1]:.0f} s, in {previous_file}). The files "
                "contain repeated or out-of-order records."
            )
            raise ValueError(msg)
        try:
            self.windows = _averaging_windows(self.uscl_dataset, self.files)
        except ValueError:
            self.uscl_dataset.close()
            raise
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
        reference_sizes: dict[str, int] = {}
        has_bounds, partial = [], []
        for i, f in enumerate(self.files):
            with xr.open_dataset(f) as ds:
                ds_sizes = {k: v for k, v in ds.sizes.items() if k != "time"}
                present = [v for v in (_AVG_BEGIN, _AVG_END) if v in ds.variables]
            if i == 0:
                reference_sizes = ds_sizes
            elif ds_sizes != reference_sizes:
                raise ValueError(
                    "Dimensions of input files do not match. "
                    + "HINT: use ncjoin to join files before calling uscl_to_cdr"
                )
            if len(present) == 1:
                partial.append(f"{Path(f).name} (only {present[0]})")
            has_bounds.append(len(present) == 2)
        if partial:
            msg = f"Averaging bounds are incomplete in {', '.join(partial)}."
            raise ValueError(msg)
        if 0 < sum(has_bounds) < len(has_bounds):
            without = [
                Path(f).name for f, has in zip(self.files, has_bounds) if not has
            ]
            msg = (
                f"Only some `_uscl` files have `{_AVG_BEGIN}`/`{_AVG_END}`; "
                f"missing from {', '.join(without)}. Do not mix files of "
                "ucla-roms <= 0.12.0 with newer ones."
            )
            raise ValueError(msg)

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

        One record per averaging window plus a closing record (see the class
        docstring for the time convention).

        Creates the following variables:
        - cdr_trcflx_profile: Series of time-evolving tracer flux profiles flux(time,depth,tracer,profile no.)
          ALK and DIC are the first two tracers; CDR-lite tracers, if any, follow.
        - cdr_trc_name: Tracer names along the tracer axis (only with CDR-lite tracers)
        - cdr_lon, cdr_lat: 1D arrays specifying where each tracer flux profile is located
        - cdr_layer_thickness: Series of time-evolving layer heights used to remap the profiles onto the parent grid
        """
        ds = xr.Dataset(attrs={"uscl_windows": self.windows.attribute})
        n_rec = self.windows.begin.size + 1
        n_trc = len(self.tracer_names)
        named = n_trc > len(BASE_TRACER_NAMES)
        trc_dim = "ntrc" if named else "two"

        ds["cdr_trcflx_profile"] = xr.DataArray(
            np.zeros((n_rec, len(self.s_rho), n_trc, self.n_profiles)),
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
            np.append(self.windows.begin, self.windows.end[-1]) / 86400.0,
            dims=("cdr_time",),
            attrs={
                "long_name": "CDR forcing time",
                "units": "days since 2000/01/01",
                "convention": CDR_TIME_CONVENTION,
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
            np.zeros((n_rec, len(self.s_rho), self.n_profiles)),
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
        profile in the CDR forcing dataset with a corresponding lat and lon. The
        averaging windows fill all but the last record, which stays a zero flux
        and repeats the last layer thickness (ucla-roms divides by it).
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

        def window_profiles(var_prefix) -> np.ndarray:
            """boundaries_to_profiles of the records that hold a window's data."""
            return boundaries_to_profiles(var_prefix)[self.windows.first_record :]

        if self.cdr_dataset is None:
            msg = "Unable to populate null dataset."
            raise RuntimeError(msg)

        log.info("Populating `cdr_lat`...")
        self.cdr_dataset["cdr_lat"][:] = boundaries_to_profiles("lat")
        log.info("Populating `cdr_lon`...")
        self.cdr_dataset["cdr_lon"][:] = boundaries_to_profiles("lon")
        for i_trc, name in enumerate(self.tracer_names):
            log.info(f"Populating `cdr_trcflx_profile` for tracer {name}...")
            self.cdr_dataset["cdr_trcflx_profile"][:-1, :, i_trc, :] = window_profiles(
                f"{name}_add"
            )
        log.info("Populating `cdr_layer_thickness`...")
        thickness = window_profiles("h")
        self.cdr_dataset["cdr_layer_thickness"][:-1] = thickness
        self.cdr_dataset["cdr_layer_thickness"][-1] = thickness[-1]

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
