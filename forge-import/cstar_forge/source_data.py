from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional
from datetime import datetime, timedelta

import shutil
import tempfile
from urllib.request import urlopen

import copernicusmarine
import gdown
import roms_tools as rt

from . import config


# -----------------------------------------
# Dataset registry (name -> handler + metadata)
# -----------------------------------------


class DatasetHandler:
    """Container for a dataset handler and its required SourceData attributes."""

    def __init__(self, func: Callable["SourceData", Path], requires: List[str]):
        self.func = func
        self.requires = requires


DATASET_REGISTRY: Dict[str, DatasetHandler] = {}


def register_dataset(name: str, requires: Optional[List[str]] = None) -> Callable:
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

    def decorator(func: Callable["SourceData", Path]) -> Callable:
        DATASET_REGISTRY[name.upper()] = DatasetHandler(func=func, requires=requires)
        return func

    return decorator


# -----------------------------------------
# Constants (SRTM15 versioning)
# -----------------------------------------

SRTM15_VERSION = "V2.7"
SRTM15_URL = f"https://topex.ucsd.edu/pub/srtm15_plus/SRTM15_{SRTM15_VERSION}.nc"


# -----------------------------------------
# constants: GLORYS
# -----------------------------------------

glorys_dataset_id: str = "cmems_mod_glo_phy_my_0.083deg_P1D-m"


# -----------------------------------------
# Constants (WOA versioning)
# -----------------------------------------

WOA_FILENAMES: List[str] = [f"woa*_decav_s{month:02d}_*.nc" for month in range(1, 13)]
WOA_DOWNLOAD_URL = f"https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/"


# -----------------------------------------
# Logical source-name → dataset key mapping
# -----------------------------------------


SOURCE_ALIAS: Dict[str, str] = {
    # ERA5 surface forcing
    "ERA5": "ERA5",
    # GLORYS defaults to regional when only the logical name is known (see SourceSpec.glorys_layout)
    "GLORYS": "GLORYS_REGIONAL",
    "GLORYS_GLOBAL": "GLORYS_GLOBAL",
    "GLORYS_REGIONAL": "GLORYS_REGIONAL",
    # UNIFIED biogeochemistry
    "UNIFIED": "UNIFIED_BGC",
    "UNIFIED_BGC": "UNIFIED_BGC",
    # SRTM15 bathymetry
    "SRTM15": f"SRTM15_{SRTM15_VERSION}".upper(),
    # TPXO tidal data (user-provided)
    "TPXO": "TPXO",
    # WOA salinity data (user-provided)
    "WOA": "WOA",
    # Rivers, etc. (placeholder – add real dataset handlers as needed)
    "DAI": "DAI",  # expected to correspond to a DAI dataset if/when added
}

# List of source names that are streamable and do not need to be prepared unless explicitly requested.
STREAMABLE_SOURCES: List[str] = [
    "ERA5",
    "DAI",
]


def map_source_to_dataset_key(name: str) -> str:
    """
    Map a logical source name (e.g. 'GLORYS', 'UNIFIED') to a dataset key
    used in DATASET_REGISTRY / SourceData.paths.

    If no alias is defined, the uppercased name is returned as-is.
    """
    return SOURCE_ALIAS.get(name.upper(), name.upper())


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

    datasets: List[str]
    clobber: bool = False

    # Optional attributes — only required if a dataset handler declares them
    grid: Optional[object] = None
    grid_name: Optional[str] = None
    start_time: Optional[object] = None
    end_time: Optional[object] = None

    def __post_init__(self):
        # Normalize dataset names through SOURCE_ALIAS (if not found, use uppercased name)
        normalized = []
        for ds in self.datasets:
            ds_upper = ds.upper()
            normalized.append(SOURCE_ALIAS.get(ds_upper, ds_upper))
        self.datasets = normalized
        
        # Validate requested datasets
        known = set(DATASET_REGISTRY.keys())
        unknown = set(self.datasets) - known
        if unknown:
            raise ValueError(
                f"Unknown dataset(s) requested: {', '.join(sorted(unknown))}. "
                f"Known datasets: {', '.join(sorted(known))}"
            )

        # Per-dataset paths (generic) + convenience attrs
        self.paths: Dict[str, Path] = {}
        self.srtm15_path: Optional[Path] = None

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
        for name in self.datasets:
            if name in STREAMABLE_SOURCES and not include_streamable:
                continue
            # raise error if not in registry (shouldn't happen after validation, but be safe)
            if name not in DATASET_REGISTRY:
                raise ValueError(f"Unknown dataset: {name}")

            handler = DATASET_REGISTRY[name]
            # Make sure required attributes are provided
            missing_attrs = [attr for attr in handler.requires if getattr(self, attr) is None]
            if missing_attrs:
                raise ValueError(
                    f"Dataset '{name}' requires attributes {missing_attrs}, "
                    "but they were not provided to SourceData()."
                )

            path = handler.func(self)  # call handler with this instance
            self.paths[name] = path  # store generically

        return self

    # -----------------------------------------
    # Helpers for model.py (logical source → path)
    # -----------------------------------------

    def dataset_key_for_source(
        self,
        logical_name: str,
        glorys_layout: Optional[str] = None,
    ) -> str:
        """
        Given a logical source name (e.g. "GLORYS", "UNIFIED"), return the
        dataset key (e.g. "GLORYS_GLOBAL", "GLORYS_REGIONAL", "UNIFIED_BGC")
        used in `self.paths`.

        For logical "GLORYS", pass ``glorys_layout`` from SourceSpec
        (``"global"`` or ``"regional"``). If omitted, defaults to regional.
        """
        u = logical_name.upper()
        if u == "GLORYS":
            layout = (glorys_layout or "regional").lower()
            return "GLORYS_GLOBAL" if layout == "global" else "GLORYS_REGIONAL"
        return map_source_to_dataset_key(logical_name)

    def path_for_source(
        self,
        logical_name: str,
        glorys_layout: Optional[str] = None,
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
            if key in STREAMABLE_SOURCES:
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
        date_str = date.strftime('%Y%m%d')
        dataset_name = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
        if is_regional:
            fn = f"{glorys_dataset_id}_REGIONAL_{self.grid_name}_{date_str}.nc"
        else:
            fn = f"{glorys_dataset_id}_GLOBAL_{date_str}.nc"
        dataset_dir = config.paths.source_data / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir / fn

    def _prepare_glorys_daily(self, is_regional: bool, bounds: Dict[str, Optional[float]]) -> List[Path]:
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
        end_date = datetime(
            self.end_time.year, self.end_time.month, self.end_time.day
        )
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
                    print(f"⚠️  Clobber=True: removing existing {dataset_type} file {path.name}")
                    path.unlink()
                
                dataset_type = "GLORYS_REGIONAL" if is_regional else "GLORYS_GLOBAL"
                date_str = current_date.strftime('%Y-%m-%d')
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
                date_str = current_date.strftime('%Y-%m-%d')
                print(f"✔️  Using existing {dataset_type} file for {date_str}: {path.name}")
            
            # Move to next day
            current_date += timedelta(days=1)
        
        return paths


def _pad_date_range(sd: datetime, ed: datetime, pad_days: int=1) -> tuple[datetime, datetime]:
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
def _prepare_glorys_regional(self: SourceData) -> List[Path]:
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
def _prepare_glorys_global(self: SourceData) -> List[Path]:
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


@register_dataset("UNIFIED_BGC")
def _prepare_unified_bgc_dataset(self: SourceData) -> Path:
    """Ensure the UNIFIED_BGC dataset exists locally."""
    url_bgc_forcing = (
        "https://drive.google.com/uc?id=1wUNwVeJsd6yM7o-5kCx-vM3wGwlnGSiq"
    )
    dataset_dir = config.paths.source_data / "UNIFIED_BGC"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / "BGCdataset.nc"
    needs_download = self.clobber or (not path.exists())

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

    The file is stored under config.paths.source_data / "SRTM15" / "SRTM15_{SRTM15_VERSION}.nc".
    """
    dataset_dir = config.paths.source_data / "SRTM15"
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


@register_dataset("ERA5")
def _prepare_era5(self: SourceData) -> Path:
    pass

# ---------------------------
# TPXO handler (user-provided dataset)
# ---------------------------


@register_dataset("TPXO")
def _prepare_tpxo(self: SourceData) -> Path:
    """
    Verify that the user has provided TPXO tidal data files.

    This is a USER_DATASET that must be downloaded by the user.
    The handler checks that all required files exist at the expected location:
    - config.paths.source_data / "TPXO/TPXO10.v2/grid_tpxo10v2.nc"
    - config.paths.source_data / "TPXO/TPXO10.v2/h_tpxo10.v2.nc"
    - config.paths.source_data / "TPXO/TPXO10.v2/u_tpxo10.v2.nc"

    Returns
    -------
    Path
        Base directory path to the TPXO dataset.

    Raises
    ------
    FileNotFoundError
        If the TPXO directory or any required files are missing.
    """
    tpxo_path = config.paths.source_data / "TPXO" / "TPXO10.v2"

    tpxo_dict = {
        "grid": tpxo_path / "grid_tpxo10v2.nc",
        "h": tpxo_path / "h_tpxo10.v2.nc",
        "u": tpxo_path / "u_tpxo10.v2.nc",
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
            f"TPXO dataset is incomplete. Missing files:\n" + "\n".join(missing_files) + "\n"
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
        config.paths.source_data / "WOA" / "woa{YY}_decav_s{MM}_{gr}.nc"
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
    woa_path = config.paths.source_data / "WOA"

    woa_dict = {
        f"s{m:02d}": woa_path / f"woa*_decav_s{m:02d}_*.nc"
        for m in range(1, 13)
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
    resolved: Dict[str, Path] = {}
    for key, file_path in woa_dict.items():
        matches = list(file_path.parent.glob(file_path.name))
        if not matches:
            missing_files.append(f" - {key}: {file_path}")
        else:
            resolved[key] = matches[0]

    if missing_files:
        raise FileNotFoundError(
            f"WOA dataset is incomplete. Missing files:\n" + "\n".join(missing_files) + "\n"

        )

    #import pdb;pdb.set_trace()
    print(f"✔️  WOA dataset verified at: {woa_path}")
    self.paths["WOA"] = woa_path
    return woa_path / "woa*_decav_s*.nc"
