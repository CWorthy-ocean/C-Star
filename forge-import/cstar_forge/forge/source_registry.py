"""
Lightweight, dependency-free source-data registry: the logical-name → dataset-key
alias map, the streamable list, the per-dataset provenance metadata (dataset id /
download URL), and the resolution helpers.

This module holds ONLY pure data + functions (stdlib/typing) so it can be imported
by both:
  * ``cstar_forge.forge.source_data`` (the heavy acquisition layer — copernicusmarine /
    gdown / roms_tools), which re-exports these names for its existing consumers, and
  * ``cstar_forge.spec_config_resolve`` (the dependency-light Phase-1 resolver),
    which previously carried a hand-copied duplicate of this table.

Single source of truth: edit dataset identifiers / URLs / aliases here. When the
processing layer eventually moves to C-Star, this module travels with ``source_data``.
"""

from __future__ import annotations

# --- versioned constants -----------------------------------------------------
SRTM15_VERSION = "V2.7"
SRTM15_URL = f"https://topex.ucsd.edu/pub/srtm15_plus/SRTM15_{SRTM15_VERSION}.nc"
GLORYS_DATASET_ID = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
MBL_CO2_URL = (
    "https://gml.noaa.gov/ccgg/mbl/tmp/co2_GHGreference.1785677502_surface.txt"
)
WOA_DOWNLOAD_URL = (
    "https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/"
)
UNIFIED_BGC_URL = "https://drive.google.com/uc?id=1wUNwVeJsd6yM7o-5kCx-vM3wGwlnGSiq"

# --- logical source-name -> dataset key --------------------------------------
SOURCE_ALIAS: dict[str, str] = {
    "ERA5": "ERA5",
    # GLORYS defaults to regional when only the logical name is known
    # (see SourceSpec.glorys_layout / resolve_dataset_key).
    "GLORYS": "GLORYS_REGIONAL",
    "GLORYS_GLOBAL": "GLORYS_GLOBAL",
    "GLORYS_REGIONAL": "GLORYS_REGIONAL",
    "UNIFIED": "UNIFIED_BGC",
    "UNIFIED_BGC": "UNIFIED_BGC",
    "SRTM15": f"SRTM15_{SRTM15_VERSION}".upper(),
    "MBL_CO2": "MBL_CO2",
    "TPXO": "TPXO",
    "WOA": "WOA",
    "DAI": "DAI",  # placeholder until a real DAI handler exists
}

# Sources streamed at run time (not staged unless explicitly requested).
STREAMABLE_SOURCES = ["ERA5", "DAI"]

# Per-key provenance metadata (snapshotted into SpecConfig.sources.resolved_datasets).
DATASET_METADATA: dict[str, dict[str, str]] = {
    "GLORYS_REGIONAL": {"dataset_id": GLORYS_DATASET_ID},
    "GLORYS_GLOBAL": {"dataset_id": GLORYS_DATASET_ID},
    "UNIFIED_BGC": {"url": UNIFIED_BGC_URL},
    f"SRTM15_{SRTM15_VERSION}".upper(): {"url": SRTM15_URL},
    "MBL_CO2": {"url": MBL_CO2_URL},
    "WOA": {"url": WOA_DOWNLOAD_URL},
    "TPXO": {},
    "ETOPO5": {},
    "ERA5": {},
    "DAI": {},
}

_STREAMABLE_UPPER = {s.upper() for s in STREAMABLE_SOURCES}


def map_source_to_dataset_key(name: str) -> str:
    """Map a logical source name to a dataset key; uppercased name if no alias."""
    return SOURCE_ALIAS.get(name.upper(), name.upper())


def resolve_dataset_key(name: str, glorys_layout: str | None = None) -> str:
    """Layout-aware key resolution. For logical ``GLORYS``, ``glorys_layout``
    selects ``GLORYS_GLOBAL`` vs ``GLORYS_REGIONAL`` (defaults to regional).
    """
    if name.upper() == "GLORYS":
        return (
            "GLORYS_GLOBAL"
            if (glorys_layout or "regional").lower() == "global"
            else "GLORYS_REGIONAL"
        )
    return map_source_to_dataset_key(name)


def resolve_source(name: str, glorys_layout: str | None = None) -> dict[str, object]:
    """Resolve a logical source to ``{dataset_key, dataset_id, url, streamable}``
    (plain dict — callers wrap it into their own model).
    """
    key = resolve_dataset_key(name, glorys_layout)
    meta = DATASET_METADATA.get(key, {})
    return {
        "dataset_key": key,
        "dataset_id": meta.get("dataset_id"),
        "url": meta.get("url"),
        "streamable": name.upper() in _STREAMABLE_UPPER
        or key.upper() in _STREAMABLE_UPPER,
    }
