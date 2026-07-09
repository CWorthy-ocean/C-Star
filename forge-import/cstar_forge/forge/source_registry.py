"""
Lightweight, dependency-free source-data registry: the logical-name → dataset-key
alias map, the streamable list, the per-dataset provenance metadata (dataset id /
download URL), and the resolution helpers.

This module holds ONLY pure data + functions (stdlib/typing) so it can be imported
by both:
  * ``cstar_forge.forge.source_data`` (the heavy acquisition layer — copernicusmarine /
    gdown / roms_tools), which re-exports these names for its existing consumers, and
  * ``cstar_forge.forge_blueprint_resolve`` (the dependency-light Phase-1 resolver),
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
GLOFAS_CDS_URL = "https://ewds.climate.copernicus.eu/datasets/cems-glofas-historical"
GLOFAS_FILENAME = "glofas_v4_rivers_daily.nc"

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
    # SRTM15 maps to the un-versioned handler key (the version lives in the URL/
    # filename constant, mirroring how GLORYS keeps its version in metadata). This
    # matches the ``@register_dataset("SRTM15")`` handler so the topo file actually stages.
    "SRTM15": "SRTM15",
    "MBL_CO2": "MBL_CO2",
    "TPXO": "TPXO",
    "WOA": "WOA",
    "DAI": "DAI",  # placeholder until a real DAI handler exists
    "GLOFAS": "GLOFAS",  # alternative river-discharge dataset (roms-tools rt>=4, PR #625)
}

# Sources streamed at run time (not staged unless explicitly requested).
STREAMABLE_SOURCES = ["ERA5", "DAI"]

# Recognized dataset keys that Forge does NOT stage locally — they have no SourceData
# handler because something else provides them:
#   - "ETOPO5": roms-tools fetches this topography itself at grid-build time (the
#               SRTM15 alternative IS staged by Forge and injected into grid_kwargs).
#   - "DAI":    river dataset streamed at run time (placeholder; no handler yet).
# `SourceData` treats these as valid-but-skipped, distinct from a genuinely unknown key
# (a typo), which still raises. NOTE: "GLOFAS" (the other river source) is NOT here —
# unlike DAI it has no roms-tools auto-download, so it IS staged (verified) by Forge via
# a real @register_dataset("GLOFAS") handler, same as the TPXO/WOA user-provided pattern.
UNSTAGED_DATASETS: set[str] = {"ETOPO5", "DAI"}

# Per-key provenance metadata (snapshotted into ForgeBlueprint.sources.resolved_datasets).
DATASET_METADATA: dict[str, dict[str, str]] = {
    "GLORYS_REGIONAL": {"dataset_id": GLORYS_DATASET_ID},
    "GLORYS_GLOBAL": {"dataset_id": GLORYS_DATASET_ID},
    "UNIFIED_BGC": {"url": UNIFIED_BGC_URL},
    "SRTM15": {"url": SRTM15_URL},
    "MBL_CO2": {"url": MBL_CO2_URL},
    "WOA": {"url": WOA_DOWNLOAD_URL},
    "TPXO": {},
    "ETOPO5": {},
    "ERA5": {},
    "DAI": {},
    "GLOFAS": {"url": GLOFAS_CDS_URL},
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
