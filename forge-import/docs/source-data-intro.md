# Source Data Overview
The `cstar_forge/forge/source_data.py` module manages the acquisition, preparation, and caching of model input datasets required for ROMS/MARBL domain generation and simulation.

These datasets are documented in ROMS Tools [here](https://roms-tools.readthedocs.io/en/latest/datasets.html).

`source_data.py` provides a registry-driven system for handling diverse data sources, allowing for flexible workflows whether datasets are streamed or locally cached. The alias map, streamable-source list, and per-dataset provenance metadata it uses live in the lighter-weight sibling module `cstar_forge/forge/source_registry.py` (see the [developer guide](source-data-developer.md)).

:::{important} Register for dataset access
The `source_data.py` module provides automated downloading of data assets used to force the model; however, some of these require registration to permit access.
- GLORYS data is provided via the Copernicus Marine Service. 
Learn how to register for access [here](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service).
That process should result in a .copernicusmarine or .copernicusmarine-credentials file in your home directory
- Access to the TPXO Global Tidal Model data requires registration, available [here](https://www.tpxo.net/global).
:::

## Dataset Preparation Logic

- **SRTM15**: Downloads topography from Scripps (version controlled, e.g. `SRTM15_V2.7`). Returns a single Path.
- **GLORYS**: Global or regional ocean initial conditions; subset and time-extract logic depends on whether the request is regional (grid-based, dataset key `GLORYS_REGIONAL`) or global (`GLORYS_GLOBAL`). Returns a `List[Path]` (one file per day, with the window padded ±1 day); `src.paths["GLORYS_REGIONAL"]` is always a list after `prepare_all()`.
- **UNIFIED_BGC**: [Unified biogeochemistry forcing & initial conditions from ROMS Tools](https://roms-tools.readthedocs.io/en/latest/initial_conditions.html#Adding-Biogeochemical-(BGC)-Initial-Conditions). Downloaded from Google Drive; version controlled like SRTM15, so the staged filename carries the version (`BGCdataset_v2_1.nc`) and a version bump re-downloads rather than reusing the stale cached file. **Requires a roms-tools newer than 4.0.1**: v2.1 files name their dimensions `longitude`/`latitude`/`depth`, which older roms-tools renames unconditionally and chokes on, so the handler refuses to stage and says so. Get a capable build with `./dev-setup.sh --roms-tools-ref main` until it releases. Returns a single Path.
- **MBL_CO2**: NOAA marine boundary-layer xCO2 surface reference data. Downloaded once and cached; returns a single Path.
- **ERA5**: Atmospheric surface forcing (streamable, no local download needed). Handler is an intentional no-op: it logs and returns `None` (so `paths["ERA5"]` is `None`).
- **TPXO**, **WOA**, **GLOFAS**, **EMOD**, **RIVR2O**: User-provided datasets (tidal harmonics, climatology, river discharge/BGC, alternative topography). Forge cannot download these itself; each handler only verifies that the expected files already exist under the dataset's cache directory and raises `FileNotFoundError` with instructions if they don't. TPXO's handler returns a dictionary with keys `"grid"`, `"h"`, and `"u"` mapping to file paths, stored in `src.paths["TPXO"]`.
- **CONSTANTS**, **DAI**: Streamed/auto-downloaded by roms-tools itself at generation time — Forge never stages a local path for these — `CONSTANTS` additionally has no registry entry at all: the resolver never places it in a blueprint's `datasets` list, and requesting it from `SourceData` directly raises `ValueError`.
- **ETOPO5**: The default topography source; like CONSTANTS/DAI, roms-tools fetches it itself (at grid-build time), so Forge does not stage it either.

Each preparation routine ensures datasets exist locally and are subsetted for the target domain/grid (handlers check existence, never freshness).



