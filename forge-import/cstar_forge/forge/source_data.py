import logging
import shutil
import stat
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import MISSING, dataclass
from dataclasses import fields as dc_fields
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

import copernicusmarine
import gdown
import roms_tools as rt

logger = logging.getLogger(__name__)

# -----------------------------------------
# Dataset registry (name -> handler + metadata)
# -----------------------------------------


class DatasetHandler:
    """Container for a dataset handler and its required SourceData attributes."""

    def __init__(self, func: Callable[["SourceData"], Path], requires: list[str]):
        self.func = func
        self.requires = requires


DATASET_REGISTRY: dict[str, DatasetHandler] = {}


def _add_group_read(path: Path) -> None:
    """Best-effort ``chmod g+rX``: add group-read (plus group-execute for directories).

    Only ever *adds* group bits (never write, never other), and silently tolerates
    failure: in a group-shared cache, files staged by a different user cannot be
    chmodded by this one (chmod is owner-only) — and don't need to be.
    """
    try:
        mode = path.stat().st_mode
        extra = stat.S_IRGRP | (stat.S_IXGRP if stat.S_ISDIR(mode) else 0)
        if (mode & extra) != extra:
            path.chmod(mode | extra)
    except OSError:
        pass


def _iter_concrete_paths(obj) -> Iterator[Path]:
    """Yield concrete Paths from a handler result (Path, glob-pattern Path, list, dict, None)."""
    if obj is None:
        return
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            yield from _iter_concrete_paths(item)
    elif isinstance(obj, dict):
        for item in obj.values():
            yield from _iter_concrete_paths(item)
    elif isinstance(obj, Path):
        # WOA/RIVR2O handlers return wildcard patterns for roms-tools; expand them.
        if "*" in obj.name:
            yield from obj.parent.glob(obj.name)
        else:
            yield obj


def register_dataset(name: str, requires: list[str] | None = None) -> Callable:
    """
    Decorator to register a dataset handler.

    Parameters
    ----------
    name : str
        Dataset name (e.g. "GLORYS_REGIONAL", "UNIFIED_BGC", "SRTM15").
        Stored in upper case.
    requires : list of str, optional
        Names of SourceData attributes that must be non-None for this
        dataset to be prepared (e.g. ["grid", "grid_name", "start_time", "end_time"]).

    Usage
    -----
        @register_dataset("GLORYS_REGIONAL", requires=["grid", "grid_name", "start_time", "end_time"])
        def _prepare_glorys_regional(self): ...
    """
    if requires is None:
        requires = []

    def decorator(func: Callable[["SourceData"], Path]) -> Callable:
        DATASET_REGISTRY[name.upper()] = DatasetHandler(func=func, requires=requires)
        return func

    return decorator


# -----------------------------------------
# Source-name registry / metadata constants
#
# The alias map, streamable list, dataset ids, and download URLs live in the
# lightweight ``source_registry`` module (single source of truth, importable
# without the heavy acquisition deps). Re-exported here for existing consumers.
# -----------------------------------------
from cstar_forge.forge.source_registry import (  # noqa: E402,F401  (re-export)
    GLOFAS_CDS_URL,
    GLOFAS_FILENAME,
    GLORYS_DATASET_ID,
    MBL_CO2_URL,
    SOURCE_ALIAS,
    SRTM15_URL,
    SRTM15_VERSION,
    STREAMABLE_SOURCES,
    UNIFIED_BGC_FILENAME,
    UNIFIED_BGC_URL,
    UNIFIED_BGC_VERSION,
    UNSTAGED_DATASETS,
    WOA_DOWNLOAD_URL,
    map_source_to_dataset_key,
)

# Back-compat alias (handlers reference the lowercase name).
glorys_dataset_id: str = GLORYS_DATASET_ID

WOA_FILENAMES: list[str] = [f"woa*_decav_s{month:02d}_*.nc" for month in range(1, 13)]


# -----------------------------------------
# SourceData
# -----------------------------------------


@dataclass
class SourceData:
    """
    Handles creation and caching of source data files
    (GLORYS_REGIONAL, UNIFIED_BGC, SRTM15, etc.) for ROMS preprocessing.

    Parameters
    ----------
    datasets : list of str
        Names of datasets to prepare, e.g. ["GLORYS_REGIONAL", "UNIFIED_BGC", "SRTM15"].
    clobber : bool, optional
        If True, re-download/rebuild datasets even if files exist.
    grid, grid_name, start_time, end_time : optional
        Only required for datasets whose handlers declare them via
        `requires=[...]` in the @register_dataset decorator.
        For example, GLORYS_REGIONAL needs all four.
    """

    datasets: list[str]
    clobber: bool = False

    # Optional attributes — only required if a dataset handler declares them
    grid: object | None = None
    grid_name: str | None = None
    start_time: object | None = None
    end_time: object | None = None
    # Injected by the caller (executor). Root dir under which datasets are cached.
    # Host-independent: source_data no longer resolves paths from cstar_forge.config,
    # so this can be supplied by C-Star when the forge application relocates.
    source_data_dir: Path | None = None
    # Authoritative logical-name -> {dataset_key, dataset_id, url, streamable} snapshot
    # (ForgeBlueprint.forcing.resolved_datasets, frozen at blueprint-build time). When
    # present, key/streamable resolution in dataset_key_for_source/streamable_for_source
    # reads this first; source_registry.resolve_dataset_key is the fallback for names
    # not in the snapshot (e.g. a hand-built SourceData outside the ForgeBlueprint path).
    # This is what makes the executor behave deterministically across hosts/forge
    # versions even if source_registry's tables drift after the blueprint was built.
    resolved_datasets: dict[str, dict] | None = None

    def __post_init__(self):
        # Normalize dataset names through SOURCE_ALIAS (if not found, use uppercased name)
        normalized = []
        for ds in self.datasets:
            ds_upper = ds.upper()
            normalized.append(SOURCE_ALIAS.get(ds_upper, ds_upper))
        self.datasets = normalized

        # Case-normalize snapshot keys so lookups by logical name (any case) match.
        if self.resolved_datasets:
            self.resolved_datasets = {
                k.upper(): v for k, v in self.resolved_datasets.items()
            }

        # Validate requested datasets. `known` = datasets Forge stages (have a handler);
        # `UNSTAGED_DATASETS` = recognized keys Forge legitimately does not stage (ETOPO5 is
        # fetched by roms-tools; DAI is streamed). Anything else is a genuine typo → raise.
        known = set(DATASET_REGISTRY.keys())
        unknown = set(self.datasets) - known - UNSTAGED_DATASETS
        if unknown:
            raise ValueError(
                f"Unknown dataset(s) requested: {', '.join(sorted(unknown))}. "
                f"Known datasets: {', '.join(sorted(known | UNSTAGED_DATASETS))}"
            )

        if self.source_data_dir is not None:
            self.source_data_dir = Path(self.source_data_dir)

        # Per-dataset paths (generic) + convenience attrs
        self.paths: dict[str, Path] = {}
        self.srtm15_path: Path | None = None

    # -----------------------------------------
    # Public API
    # -----------------------------------------

    def prepare_all(self, include_streamable: bool = False):
        """
        Prepare all requested source datasets and populate `self.paths`.

        Parameters
        ----------
        include_streamable : bool, optional
            If True, also prepare streamable datasets. If False (default),
            streamable datasets are skipped.
        """
        try:
            for name in self.datasets:
                # Datasets Forge doesn't stage: provided by roms-tools (ETOPO5) or streamed
                # at run time with no handler (DAI). Always skipped here, never staged.
                if name in UNSTAGED_DATASETS:
                    continue
                if name in STREAMABLE_SOURCES and not include_streamable:
                    continue
                # raise error if not in registry (shouldn't happen after validation, but be safe)
                if name not in DATASET_REGISTRY:
                    raise ValueError(f"Unknown dataset: {name}")

                handler = DATASET_REGISTRY[name]
                # Make sure required attributes are provided
                missing_attrs = [
                    attr for attr in handler.requires if getattr(self, attr) is None
                ]
                if missing_attrs:
                    raise ValueError(
                        f"Dataset '{name}' requires attributes {missing_attrs}, "
                        "but they were not provided to SourceData()."
                    )
                if self.source_data_dir is None:
                    raise ValueError(
                        f"SourceData.source_data_dir must be set to prepare '{name}' — the "
                        "caller must inject the dataset cache root (source_data no longer "
                        "reads cstar_forge.config)."
                    )

                path = handler.func(self)  # call handler with this instance
                self.paths[name] = path  # store generically
        finally:
            # Even on a mid-run failure, share whatever was already staged.
            self.share_with_group()

        return self

    def share_with_group(self) -> None:
        """Make staged datasets group-readable (``g+r`` files, ``g+rx`` directories).

        On HPC the source-data cache typically lives in a group-shared allocation,
        but downloads land user-private: ``tempfile.NamedTemporaryFile`` creates
        0600 files regardless of umask (SRTM15, MBL_CO2), and a restrictive umask
        makes the copernicusmarine/gdown downloads private too. This sweep walks
        every prepared path (plus its parent directories up to and including
        ``source_data_dir``) and adds group-read bits. Best-effort: paths owned by
        another user are skipped silently (see ``_add_group_read``).
        """
        if self.source_data_dir is None:
            return
        root = self.source_data_dir
        _add_group_read(root)
        for result in self.paths.values():
            for p in _iter_concrete_paths(result):
                _add_group_read(p)
                # Group-read on a file is useless if an ancestor dir blocks traversal;
                # fix dirs between the file and the cache root (root handled above).
                for parent in p.parents:
                    if parent == root or not parent.is_relative_to(root):
                        break
                    _add_group_read(parent)

    # -----------------------------------------
    # Helpers for model.py (logical source → path)
    # -----------------------------------------

    def dataset_key_for_source(
        self,
        logical_name: str,
        glorys_layout: str | None = None,
    ) -> str:
        """
        Given a logical source name (e.g. "GLORYS", "UNIFIED"), return the
        dataset key (e.g. "GLORYS_GLOBAL", "GLORYS_REGIONAL", "UNIFIED_BGC")
        used in `self.paths`.

        For logical "GLORYS", pass ``glorys_layout`` from SourceSpec
        (``"global"`` or ``"regional"``). If omitted, defaults to regional.

        Prefers the injected ``resolved_datasets`` snapshot (frozen at ForgeBlueprint
        build time) over live ``source_registry`` resolution, so a blueprint resolves
        the same dataset key regardless of registry drift on the processing host.
        GLORYS with an explicit ``glorys_layout`` is the one exception: that
        resolution is a hardcoded, per-item branch (not a table lookup) and the
        snapshot is keyed by logical name only (one entry per name, see
        ``forge_blueprint_resolve._build_forcing``), so it can't disambiguate two
        GLORYS items with different layouts -- always resolve that case live.
        """
        from cstar_forge.forge.source_registry import resolve_dataset_key

        if not (logical_name.upper() == "GLORYS" and glorys_layout is not None):
            entry = (self.resolved_datasets or {}).get(logical_name.upper())
            if entry and entry.get("dataset_key"):
                return entry["dataset_key"]
        return resolve_dataset_key(logical_name, glorys_layout)

    def streamable_for_source(
        self,
        logical_name: str,
        glorys_layout: str | None = None,
    ) -> bool:
        """
        Whether ``logical_name`` is a streamable source (not staged locally unless
        explicitly requested). Prefers the ``resolved_datasets`` snapshot (see
        ``dataset_key_for_source``) so streamability is pinned alongside the dataset
        key; falls back to the live ``STREAMABLE_SOURCES`` check.
        """
        if not (logical_name.upper() == "GLORYS" and glorys_layout is not None):
            entry = (self.resolved_datasets or {}).get(logical_name.upper())
            if entry is not None and "streamable" in entry:
                return bool(entry["streamable"])
        key = self.dataset_key_for_source(logical_name, glorys_layout=glorys_layout)
        upper_streamable = {s.upper() for s in STREAMABLE_SOURCES}
        return (
            logical_name.upper() in upper_streamable or key.upper() in upper_streamable
        )

    def path_for_source(
        self,
        logical_name: str,
        glorys_layout: str | None = None,
    ) -> Path:
        """
        Return the prepared file path associated with a logical source name.

        Parameters
        ----------
        logical_name : str
            Logical source name, e.g. "GLORYS", "UNIFIED", "DAI".
        glorys_layout : str, optional
            For ``GLORYS`` only: ``"global"`` or ``"regional"`` (default regional).

        Returns
        -------
        Path
            Path to the corresponding dataset file.

        Raises
        ------
        KeyError
            If the mapped dataset key was not among `self.datasets` or has
            not been prepared (i.e., `prepare_all()` has not been called
            or the dataset was omitted).
        """
        key = self.dataset_key_for_source(logical_name, glorys_layout=glorys_layout)
        try:
            return self.paths[key]
        except KeyError:
            if self.streamable_for_source(logical_name, glorys_layout=glorys_layout):
                return None
            else:
                raise KeyError(
                    f"Source '{logical_name}' maps to dataset '{key}', "
                    f"but that dataset was not prepared. Available datasets: "
                    f"{', '.join(sorted(self.paths.keys()))}"
                )

    # -----------------------------------------
    # Internals / helpers
    # -----------------------------------------

    def _construct_glorys_path(self, date: datetime, is_regional: bool) -> Path:
        """Construct filename for a single day of GLORYS data."""
        date_str = date.strftime("%Y%m%d")
        dataset_name = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
        if is_regional:
            fn = f"{glorys_dataset_id}_REGIONAL_{self.grid_name}_{date_str}.nc"
        else:
            fn = f"{glorys_dataset_id}_GLOBAL_{date_str}.nc"
        dataset_dir = self.source_data_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir / fn

    def _prepare_glorys_daily(
        self, is_regional: bool, bounds: dict[str, float | None]
    ) -> list[Path]:
        """
        Download or reuse daily GLORYS subsets.

        Parameters
        ----------
        is_regional : bool
            True for regional (grid-based), False for global.
        bounds : dict
            Dictionary with keys minimum_longitude, maximum_longitude,
            minimum_latitude, maximum_latitude. For global, all values are None.

        Notes
        -----
        The GLORYS fetch window is padded by 1 day on each side
        (start_time - 1 day to end_time + 1 day) to ensure boundary/initial
        condition interpolation has temporal context.
        """
        paths = []

        # Iterate over each day with a ±1 day temporal padding window.
        current_date = datetime(
            self.start_time.year, self.start_time.month, self.start_time.day
        )
        end_date = datetime(self.end_time.year, self.end_time.month, self.end_time.day)
        # Pad the range by 1 day on each side to ensure boundary/initial condition can be interpolated
        current_date, end_date = _pad_date_range(current_date, end_date, pad_days=1)

        while current_date <= end_date:
            # Construct path for this day
            path = self._construct_glorys_path(current_date, is_regional)
            paths.append(path)

            needs_download = self.clobber or (not path.exists())

            if needs_download:
                if path.exists():
                    dataset_type = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
                    print(
                        f"⚠️  Clobber=True: removing existing {dataset_type} file {path.name}"
                    )
                    path.unlink()

                dataset_type = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
                date_str = current_date.strftime("%Y-%m-%d")
                print(f"⬇️  Downloading {dataset_type} for {date_str} → {path.name}")

                copernicusmarine.subset(
                    dataset_id=glorys_dataset_id,
                    variables=["thetao", "so", "uo", "vo", "zos"],
                    coordinates_selection_method="outside",
                    start_datetime=current_date,
                    end_datetime=current_date,
                    output_filename=path.name,
                    output_directory=path.parent,
                    overwrite=True,
                    **bounds,
                )
            else:
                dataset_type = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
                date_str = current_date.strftime("%Y-%m-%d")
                print(
                    f"✔️  Using existing {dataset_type} file for {date_str}: {path.name}"
                )

            # Move to next day
            current_date += timedelta(days=1)

        return paths


def _pad_date_range(
    sd: datetime, ed: datetime, pad_days: int = 1
) -> tuple[datetime, datetime]:
    """Return a new date range with padding added to both ends."""
    if sd > ed:
        raise ValueError("Start date must precede end date")

    pad_days = abs(pad_days)
    delta = timedelta(days=pad_days)
    return sd - delta, ed + delta


# ---------------------------
# GLORYS_REGIONAL handler
# ---------------------------


@register_dataset(
    "GLORYS_REGIONAL",
    requires=["grid", "grid_name", "start_time", "end_time"],
)
def _prepare_glorys_regional(self: SourceData) -> list[Path]:
    """Download or reuse daily regional GLORYS subsets for this grid and time range."""
    is_regional = True
    bounds = rt.get_glorys_bounds(self.grid)
    paths = self._prepare_glorys_daily(is_regional, bounds)
    # Store paths under the dataset key
    self.paths["GLORYS_REGIONAL"] = paths[0] if len(paths) == 1 else paths
    return paths


# ---------------------------
# GLORYS_GLOBAL handler
# ---------------------------


@register_dataset(
    "GLORYS_GLOBAL",
    requires=["start_time", "end_time"],
)
def _prepare_glorys_global(self: SourceData) -> list[Path]:
    """Download or reuse daily global GLORYS subsets for this time range."""
    is_regional = False
    bounds = {
        "minimum_longitude": None,
        "maximum_longitude": None,
        "minimum_latitude": None,
        "maximum_latitude": None,
    }
    paths = self._prepare_glorys_daily(is_regional, bounds)
    # Store paths under the dataset key
    self.paths["GLORYS_GLOBAL"] = paths[0] if len(paths) == 1 else paths
    return paths


# ---------------------------
# UNIFIED BGC handler
# ---------------------------


def _roms_tools_reads_unified_v2_1() -> bool:
    """Whether the installed roms-tools can read a v2.1+ unified BGC file.

    Forward compatibility here is one-way. roms-tools learned to read *both* file
    generations after the 4.0.1 release (it renames a pre-v2.1 file's dimensions and
    warns); earlier roms-tools renames ``lon``/``lat``/``dep`` unconditionally, so
    handing it a v2.1 file — whose dimensions are already ``longitude``/``latitude``/
    ``depth`` — raises deep inside ``UnifiedDataset.clean_up`` instead of at staging
    time. Since Forge now pins the v2.1 download, check up front.

    The class default for ``dim_names`` is the behavior that actually breaks, so it is
    read directly rather than parsing a version string (the capability currently has
    no released roms-tools version to compare against).
    """
    try:
        from roms_tools.datasets.lat_lon_datasets import UnifiedBGCDataset
    except ImportError:  # pragma: no cover - roms-tools layout changed; don't block
        return True
    for f in dc_fields(UnifiedBGCDataset):
        if f.name == "dim_names" and f.default_factory is not MISSING:
            return f.default_factory().get("longitude") == "longitude"
    return True  # pragma: no cover - field vanished; don't block staging


@register_dataset("UNIFIED_BGC")
def _prepare_unified_bgc_dataset(self: SourceData) -> Path:
    """Ensure the UNIFIED_BGC dataset exists locally."""
    if not _roms_tools_reads_unified_v2_1():
        raise RuntimeError(
            f"Forge stages unified BGC {UNIFIED_BGC_VERSION} "
            f"({UNIFIED_BGC_FILENAME}), but the installed roms-tools "
            f"({rt.__version__}) predates v2.1 support and would fail to read it "
            "(it renames lon/lat/dep unconditionally). Upgrade roms-tools to a build "
            "that includes unified-BGC v2.1 support, or stage a pre-v2.1 file "
            "yourself and point the UNIFIED source at it via an explicit path."
        )

    url_bgc_forcing = UNIFIED_BGC_URL
    dataset_dir = self.source_data_dir / "UNIFIED_BGC"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / UNIFIED_BGC_FILENAME
    needs_download = self.clobber or (not path.exists())

    stale = dataset_dir / "BGCdataset.nc"
    if stale.exists():
        print(
            f"ℹ️  A pre-v2.1 BGC file is still cached at {stale} and is no longer "
            "used; delete it to reclaim the space."
        )

    if needs_download:
        if path.exists():
            print(f"⚠️  Clobber=True: removing existing BGC file {path.name}")
            path.unlink()

        print(f"⬇️  Downloading BGC dataset → {path}")
        gdown.download(url_bgc_forcing, str(path), quiet=False)
    else:
        print(f"✔️  Using existing BGC dataset: {path}")

    self.bgc_forcing_path = path
    self.paths["UNIFIED_BGC"] = path
    return path


# ---------------------------
# SRTM15+ handler
# ---------------------------


@register_dataset("SRTM15")
def _prepare_srtm15(self: SourceData) -> Path:
    """
    Ensure the SRTM15 bathymetry dataset exists locally.

    Download if:
      - the file does not exist, or
      - clobber=True.

    The file is stored under self.source_data_dir / "SRTM15" / "SRTM15_{SRTM15_VERSION}.nc".
    """
    dataset_dir = self.source_data_dir / "SRTM15"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / f"SRTM15_{SRTM15_VERSION}.nc"

    needs_download = self.clobber or (not path.exists())

    if needs_download:
        if path.exists():
            print(f"⚠️  Clobber=True: removing existing SRTM15 file {path.name}")
            path.unlink()

        print(f"⬇️  Downloading SRTM15+ {SRTM15_VERSION} bathymetry → {path}")

        with tempfile.NamedTemporaryFile(delete=False, dir=str(dataset_dir)) as tmpfile:
            with urlopen(SRTM15_URL) as r:
                shutil.copyfileobj(r, tmpfile)
            tmp_path = Path(tmpfile.name)

        tmp_path.replace(path)
        print(f"✔️  SRTM15+ download complete: {path}")
    else:
        print(f"✔️  Using existing SRTM15+ dataset: {path}")

    self.srtm15_path = path
    return path


# ---------------------------
# MBL_CO2 handler
# ---------------------------


@register_dataset("MBL_CO2")
def _prepare_mblco2(self: SourceData) -> Path:
    """
    Ensure the MBL xco2 dataset exists locally.

    Download if:
      - the file does not exist, or
      - clobber=True.

    The file is stored under self.source_data_dir / "MBL_CO2" / "co2_GHGreference.1785677502_surface.txt".
    """
    dataset_dir = self.source_data_dir / "MBL_CO2"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / "co2_GHGreference.1785677502_surface.txt"

    needs_download = self.clobber or (not path.exists())

    if needs_download:
        if path.exists():
            print(f"⚠️  Clobber=True: removing existing MBL_CO2 file {path.name}")
            path.unlink()

        print(f"⬇️  Downloading MBL_CO2, 1979-2025 xco2 surface data → {path}")

        with tempfile.NamedTemporaryFile(delete=False, dir=str(dataset_dir)) as tmpfile:
            with urlopen(MBL_CO2_URL) as r:
                shutil.copyfileobj(r, tmpfile)
            tmp_path = Path(tmpfile.name)

        tmp_path.replace(path)
        print(f"✔️  MBL_CO2 download complete: {path}")
    else:
        print(f"✔️  Using existing MBL_CO2 dataset: {path}")

    self.mblco2_path = path
    return path


@register_dataset("ERA5")
def _prepare_era5(self: SourceData) -> None:
    """
    No-op handler for ERA5.

    ERA5 is a ``STREAMABLE_SOURCES`` entry: it is read directly at run time by
    roms-tools rather than staged to a local file by Forge. ``prepare_all()``
    skips streamable datasets by default (``include_streamable=False``), so this
    handler only runs when a caller explicitly opts in with
    ``include_streamable=True``. In that case there is still nothing for Forge
    to stage locally, so we log that fact and return ``None`` rather than
    fabricating a path. ``prepare_all`` stores this ``None`` in ``self.paths["ERA5"]``,
    which matches what ``path_for_source`` already returns for a streamable
    source with no staged path.
    """
    logger.info(
        "ERA5 is a streamable source (read directly by roms-tools at run time); "
        "no local file is staged by Forge."
    )


# ---------------------------
# TPXO handler (user-provided dataset)
# ---------------------------


@register_dataset("TPXO")
def _prepare_tpxo(self: SourceData) -> dict[str, Path]:
    """
    Verify that the user has provided TPXO tidal data files.

    This is a USER_DATASET that must be downloaded by the user.
    The handler checks that all required files exist at the expected location:
    - self.source_data_dir / "TPXO/TPXO10.v2a/grid_tpxo10v2a.nc"
    - self.source_data_dir / "TPXO/TPXO10.v2a/h_tpxo10.v2a.nc"
    - self.source_data_dir / "TPXO/TPXO10.v2a/u_tpxo10.v2a.nc"

    Returns
    -------
    dict[str, Path]
        The TPXO files keyed ``"grid"`` / ``"h"`` / ``"u"`` (this is what
        ``prepare_all()`` stores in ``paths["TPXO"]``).

    Raises
    ------
    FileNotFoundError
        If the TPXO directory or any required files are missing.
    """
    tpxo_path = self.source_data_dir / "TPXO" / "TPXO10.v2a"

    tpxo_dict = {
        "grid": tpxo_path / "grid_tpxo10v2a.nc",
        "h": tpxo_path / "h_tpxo10.v2a.nc",
        "u": tpxo_path / "u_tpxo10.v2a.nc",
    }

    # Check that the base directory exists
    if not tpxo_path.exists():
        raise FileNotFoundError(
            f"TPXO dataset directory not found at: {tpxo_path}\n"
            f"Please download TPXO data and place it in the expected location."
        )

    # Check that all required files exist
    missing_files = []
    for key, file_path in tpxo_dict.items():
        if not file_path.exists():
            missing_files.append(f"  - {key}: {file_path}")

    if missing_files:
        raise FileNotFoundError(
            "TPXO dataset is incomplete. Missing files:\n"
            + "\n".join(missing_files)
            + "\n"
            f"Please ensure all TPXO files are present in: {tpxo_path}"
        )

    print(f"✔️  TPXO dataset verified at: {tpxo_path}")
    self.paths["TPXO"] = tpxo_path
    return tpxo_dict


# ---------------------------
# WOA handler (user-provided dataset)
# ---------------------------


@register_dataset("WOA")
def _prepare_woa(self: SourceData) -> Path:
    """
    Verify that the user has provided 12 monthly WOA climatology files (s01..s12).

    This is a USER_DATASET that must be downloaded by the user from either:
        https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/
        https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/salinity/netcdf/decav/0.25/

    Expected layout:
        self.source_data_dir / "WOA" / "woa{YY}_decav_s{MM}_{gr}.nc"
        for MM in 01..12. YY is atlas year (18, 23). gr is grid resolution - 04 is quarter deg.

    Returns
    -------
    Path
        Base directory path to the TPXO dataset.

    Raises
    ------
    FileNotFoundError
        If the WOA directory or any of the 12 monthly files are missing.
    """
    woa_path = self.source_data_dir / "WOA"

    woa_dict = {
        f"s{m:02d}": woa_path / f"woa*_decav_s{m:02d}_*.nc" for m in range(1, 13)
    }

    # Check that the base directory exists
    if not woa_path.exists():
        raise FileNotFoundError(
            f"WOA dataset directory not found at: {woa_path}\n"
            f"2018 WOA data can be downloaded from: {WOA_DOWNLOAD_URL}.\n"
            f"Please download files woa18_decav_s01_04.nc through woa18_decav_s12_04.nc "
            f"from that website and place them in {woa_path}. "
            f"Do not fetch files s00 or s13-s16 into that directory."
        )

    # Check that all required files exist
    # Check that all required files exist (resolving globs)
    missing_files = []
    resolved: dict[str, Path] = {}
    for key, file_path in woa_dict.items():
        matches = list(file_path.parent.glob(file_path.name))
        if not matches:
            missing_files.append(f" - {key}: {file_path}")
        else:
            resolved[key] = matches[0]

    if missing_files:
        raise FileNotFoundError(
            "WOA dataset is incomplete. Missing files:\n"
            + "\n".join(missing_files)
            + "\n"
        )

    print(f"✔️  WOA dataset verified at: {woa_path}")
    self.paths["WOA"] = woa_path
    return woa_path / "woa*_decav_s*.nc"


# ---------------------------
# GLOFAS handler (user-provided dataset)
# ---------------------------


@register_dataset("GLOFAS")
def _prepare_glofas(self: SourceData) -> Path:
    """
    Verify that the user has provided a preprocessed GloFAS v4.0 river discharge file.

    Unlike DAI (streamed automatically by roms-tools), GloFAS requires manual access
    to the Copernicus Climate Data Store and preprocessing with the GloFAS
    Large-scale Drainage Direction (LDD) algorithm to place river mouths on coastal
    cells, so Forge cannot download or build it. This is a USER_DATASET, like TPXO/WOA:
    the file must already exist at the expected location.

    Expected file: self.source_data_dir / "GLOFAS" / GLOFAS_FILENAME

    Returns
    -------
    Path
        Path to the preprocessed GloFAS NetCDF file.

    Raises
    ------
    FileNotFoundError
        If the file is missing at the expected location.
    """
    glofas_path = self.source_data_dir / "GLOFAS" / GLOFAS_FILENAME

    if not glofas_path.exists():
        raise FileNotFoundError(
            f"GloFAS river discharge dataset not found at: {glofas_path}\n"
            "GloFAS v4.0 must be downloaded manually from the Copernicus Climate "
            f"Data Store ({GLOFAS_CDS_URL}) and preprocessed with the GloFAS "
            "Large-scale Drainage Direction (LDD) algorithm to place river mouths on "
            f"coastal cells. Place the resulting file at {glofas_path}."
        )

    print(f"✔️  GloFAS dataset verified at: {glofas_path}")
    self.paths["GLOFAS"] = glofas_path
    return glofas_path


# ---------------------------
# EMOD handler (user-provided dataset)
# ---------------------------


@register_dataset("EMOD")
def _prepare_emod(self: SourceData) -> Path:
    """
    Verify that the user has provided an EMODnet bathymetry/topography file.

    Unlike ETOPO5 (fetched automatically by roms-tools at grid-build time), EMODnet
    has no roms-tools auto-download and no single canonical filename, so this is a
    USER_DATASET, like TPXO/WOA/GLOFAS: the file must already exist at the expected
    location.

    Expected location: self.source_data_dir / "EMOD" / "*.nc" (any NetCDF file;
    EMODnet exports do not have a fixed filename).

    Returns
    -------
    Path
        Path to the EMODnet NetCDF file.

    Raises
    ------
    FileNotFoundError
        If the EMOD directory or no matching NetCDF file is found.
    """
    emod_dir = self.source_data_dir / "EMOD"
    matches = sorted(emod_dir.glob("*.nc")) if emod_dir.exists() else []

    if not matches:
        raise FileNotFoundError(
            f"EMOD (EMODnet) topography dataset not found at: {emod_dir}\n"
            "EMODnet bathymetry must be downloaded manually (e.g. from "
            "https://emodnet.ec.europa.eu/geoviewer/) and placed as a .nc file in "
            f"{emod_dir}."
        )

    path = matches[0]
    print(f"✔️  EMOD dataset verified at: {path}")
    self.paths["EMOD"] = path
    return path


# ---------------------------
# RIVR2O handler (user-provided dataset)
# ---------------------------


@register_dataset("RIVR2O")
def _prepare_rivr2o(self: SourceData) -> Path:
    """
    Verify that the user has provided RIVR2O river biogeochemistry export files.

    RIVR2O has no roms-tools auto-download (unlike the CONSTANTS river-BGC default,
    which roms-tools downloads itself), so this is a USER_DATASET, like TPXO/WOA/GLOFAS:
    the files must already exist at the expected location. The product ships one
    NetCDF file per year (1903-2024); roms-tools accepts a wildcard pattern spanning
    multiple years.

    Expected location: self.source_data_dir / "RIVR2O" / "*.nc"

    Returns
    -------
    Path
        Wildcard pattern matching the staged RIVR2O NetCDF file(s).

    Raises
    ------
    FileNotFoundError
        If the RIVR2O directory or no matching NetCDF file is found.
    """
    rivr2o_dir = self.source_data_dir / "RIVR2O"
    matches = sorted(rivr2o_dir.glob("*.nc")) if rivr2o_dir.exists() else []

    if not matches:
        raise FileNotFoundError(
            f"RIVR2O river biogeochemistry dataset not found at: {rivr2o_dir}\n"
            "RIVR2O must be obtained separately and placed as one or more .nc files "
            f"(one per year) in {rivr2o_dir}."
        )

    pattern = rivr2o_dir / "*.nc"
    print(f"✔️  RIVR2O dataset verified at: {rivr2o_dir} ({len(matches)} file(s))")
    self.paths["RIVR2O"] = pattern
    return pattern
