# Source Data Overview
The `source_data.py` module manages the acquisition, preparation, and caching of model input datasets required for ROMS/MARBL domain generation and simulation. 

These datasets are documented in ROMS Tools [here](https://roms-tools.readthedocs.io/en/latest/datasets.html).

`source_data.py` provides a registry-driven system for handling diverse data sources, allowing for flexible workflows whether datasets are streamed or locally cached.

:::{important} Register for dataset access
The `source_data.py` module provides automated downloading of data assets used to force the model; however, some of these require registration to permit access.
- GLORYS data is provided via the Copernicus Marine Service. 
Learn how to register for access [here](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service).
That process should result in a .copernicusmarine or .copernicusmarine-credentials file in your home directory
- Access to the TPXO Global Tidal Model data requires registration, available [here](https://www.tpxo.net/global).
:::

## Dataset Preparation Logic

- **SRTM15**: Downloads topography from Scripps (version controlled, e.g. `SRTM15_V2.7`). Returns a single Path.
- **GLORYS**: Global or regional ocean initial conditions; subset and time-extract logic depends on system type. Returns a single Path (for single day) or List[Path] (for multiple days).
- **UNIFIED_BGC**: [Unified biogeochemistry forcing & initial conditions from ROMS Tools](https://roms-tools.readthedocs.io/en/latest/initial_conditions.html#Adding-Biogeochemical-(BGC)-Initial-Conditions). Returns a single Path.
- **ERA5**: Atmospheric surface forcing (streamable, no local download needed). Handler is a placeholder.
- **TPXO**: Tidal harmonics data must be provided by user. The handler verifies required files exist and returns a dictionary with keys `"grid"`, `"h"`, and `"u"` mapping to file paths. This dictionary is stored in `src.paths["TPXO"]`.

Each preparation routine ensures datasets are up to date and correctly subsetted for the target domain/grid.



