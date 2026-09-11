"""
Lightweight, dependency-free source-data registry: the logical-name → dataset-key
alias map, the streamable list, the per-dataset provenance metadata (dataset id /
download URL), and the resolution helpers.

This module holds ONLY pure data + functions (stdlib/typing) so it can be imported
by both:
  * ``cstar_forge.forge.source_data`` (the heavy acquisition layer — copernicusmarine /
    gdown / roms_tools), which re-exports these names for its existing consumers, and
  * ``cstar_forge.forge_blueprint_resolve`` (the dependency-light resolver),
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
# Quarter-degree salinity, used by the SSS-restoring "WOA" source. Still WOA18 by
# default because that is what existing runs staged; WOA23 has the same layout at
# .../WOA23/DATA/salinity/netcdf/decav/0.25/ and is drop-in.
WOA_DOWNLOAD_URL = (
    "https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/"
)

# --- WOA23 1-degree BGC climatology ("WOA_BGC") -------------------------------
# The gridded BGC source: nutrients and oxygen, plus the temperature/salinity that
# roms-tools uses for the umol/kg -> mmol/m3 conversion and for density / density_mld
# vertical interpolation. WOA23 publishes nutrients and oxygen on the 1-degree grid
# only (0.25 degree exists for T/S alone), so everything here is 1 degree.
#
# In the NCEI layout the "decav"/"all" token is the averaging period *over years*, not
# the period within a year -- "decav" pools 1955-2022 for T/S and "all" is the
# equivalent all-years token for nutrients/oxygen (1965-2022). The month is carried by
# the two-digit filename suffix: 01-12 monthly, 00 annual.
WOA23_BASE_URL = "https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA"

# internal key -> (NCEI directory, decade token, one-letter file code)
WOA23_BGC_VARIABLES: dict[str, tuple[str, str, str]] = {
    "NO3": ("nitrate", "all", "n"),
    "PO4": ("phosphate", "all", "p"),
    "SiO3": ("silicate", "all", "i"),
    "O2": ("oxygen", "all", "o"),
    "temp_bgc": ("temperature", "decav", "t"),
    "salt_bgc": ("salinity", "decav", "s"),
}

# Monthly WOA fields are shallow -- 800 m for the nutrients, 1500 m for oxygen and
# T/S -- so the full-depth annual (period 00) files are staged too. roms-tools splices
# them underneath the monthly data for its default "annual_blend" deep fill.
WOA23_PERIODS = (*range(1, 13), 0)
WOA23_GRID = "01"
# The unified BGC climatology. From v2.1 on, the file names its dimensions
# ``longitude``/``latitude``/``depth`` and stores ``month`` as an integer index 1-12;
# roms-tools still reads earlier files but logs a "predates v2.1" warning, and the
# oldest ones lack the ``temp_WOA``/``salt_WOA`` fields that density-space BGC
# interpolation and SSS restoring need. The version is not derivable from the Drive
# id, so both constants are literal — bump them together.
UNIFIED_BGC_VERSION = "v2_1"
UNIFIED_BGC_URL = "https://drive.google.com/uc?id=1NKbAe1ARtU68Np3bcwdd7nadeEUgdcef"
# Versioned filename so bumping the URL actually re-downloads: the handler skips the
# download when the target path already exists, so an unversioned name would leave
# every already-staged host silently on the old file.
UNIFIED_BGC_FILENAME = f"BGCdataset_{UNIFIED_BGC_VERSION}.nc"
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
    "WOA": "WOA",  # SSS-restoring salinity (0.25 deg); user-staged
    "WOA_BGC": "WOA_BGC",  # WOA23 1-deg gridded BGC source; auto-downloaded
    "DAI": "DAI",  # placeholder until a real DAI handler exists
    "GLOFAS": "GLOFAS",  # alternative river-discharge dataset (roms-tools rt>=4, PR #625)
    "EMOD": "EMOD",  # alternative topography source (EMODnet); user-staged, like TPXO/WOA
    "RIVR2O": "RIVR2O",  # river biogeochemistry source; user-staged, like TPXO/WOA/GLOFAS
    "GLODAP": "GLODAP",  # GLODAPv2.2016b mapped BGC climatology; user-staged, like EMOD/RIVR2O
    "CONSTANTS": "CONSTANTS",  # river-BGC default (roms-tools' own auto-download)
}

# Forge logical source name -> the name roms-tools expects in its source dict, for
# the few cases where they differ. Forge needs "WOA_BGC" to disambiguate the gridded
# BGC source from the SSS-restoring "WOA" source (different file sets, different
# staged path shape), but roms-tools has only one WOA BGC dataset and registers it
# under the bare name "WOA".
ROMS_TOOLS_SOURCE_NAME: dict[str, str] = {"WOA_BGC": "WOA"}

# Sources streamed at run time (not staged unless explicitly requested).
# CONSTANTS (the river-BGC default) belongs here too, alongside DAI: roms-tools
# auto-downloads its own file (river_tracer_defaults.nc) at generation time, so
# Forge must never try to resolve/verify a staged path for it (there is no
# @register_dataset("CONSTANTS") handler and none is needed).
STREAMABLE_SOURCES = ["ERA5", "DAI", "CONSTANTS"]

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

# Pseudo-source names that are computed/derived at generation time, not fetched or staged
# by Forge at all -- unlike UNSTAGED_DATASETS above, these aren't datasets in any sense
# (no @register_dataset handler exists or ever will), so they must never reach SourceData
# or land in ForgeBlueprint.datasets/resolved_datasets in the first place:
#   - "CONSTANTS": depth-invariant constant value(s) supplied inline in the blueprint
#                  (SourceSpec.constants), or roms-tools' own auto-downloaded river-BGC
#                  default -- either way, nothing for Forge to stage.
#   - "ESPER":     BGC fields derived from physics T/S via PyESPER at generation time
#                  (SourceSpec.esper_method/esper_equation); see input_data.py's ESPER
#                  handling, which already recognizes this by name, not by dataset lookup.
# Consulted by forge_blueprint_resolve.py's source-collection code (_note() and the
# river-bgc-source carve-out) to exclude these before they ever reach _resolved_dataset().
DERIVED_BGC_SOURCES: set[str] = {"CONSTANTS", "ESPER"}

# Per-key provenance metadata (snapshotted into ForgeBlueprint.sources.resolved_datasets).
DATASET_METADATA: dict[str, dict[str, str]] = {
    "GLORYS_REGIONAL": {"dataset_id": GLORYS_DATASET_ID},
    "GLORYS_GLOBAL": {"dataset_id": GLORYS_DATASET_ID},
    "UNIFIED_BGC": {"url": UNIFIED_BGC_URL},
    "SRTM15": {"url": SRTM15_URL},
    "MBL_CO2": {"url": MBL_CO2_URL},
    "WOA": {"url": WOA_DOWNLOAD_URL},
    "WOA_BGC": {"url": WOA23_BASE_URL},
    "TPXO": {},
    "ETOPO5": {},
    "ERA5": {},
    "DAI": {},
    "GLOFAS": {"url": GLOFAS_CDS_URL},
    "EMOD": {},  # user-staged (EMODnet has no canonical download URL Forge can pin)
    "RIVR2O": {},  # user-staged (no roms-tools auto-download; annual files, 1903-2024)
    "GLODAP": {},  # user-staged (GLODAPv2.2016b mapped climatology; no auto-download)
    "CONSTANTS": {},  # roms-tools auto-download (river_tracer_defaults.nc); streamable
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
