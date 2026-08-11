# Registering with data sources

Forge pulls forcing data from open datasets ([documented in
roms-tools](https://roms-tools.readthedocs.io/en/latest/datasets.html)). Most
access is automatic and anonymous — ERA5 atmospheric forcing, for example,
needs no account at all. Two sources require a one-time (free) registration,
each with a small setup step afterwards.

(data-access-glorys)=
## Copernicus Marine (GLORYS)

GLORYS ocean-state data provides initial and boundary conditions, and Forge
downloads it on demand through the Copernicus Marine toolbox. **Required for
essentially every domain**, including the Getting Started example.

1. [Sign up for the Copernicus Marine Service](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service).
2. Authenticate once, using the `copernicusmarine` CLI that ships with Forge's
   dependencies:

   ```bash
   copernicusmarine login    # prompts for your username and password
   ```

   This stores a credentials file under `~/.copernicusmarine/` that every
   future run picks up automatically.

For non-interactive contexts (HPC batch jobs, CI), set the environment
variables `COPERNICUSMARINE_SERVICE_USERNAME` and
`COPERNICUSMARINE_SERVICE_PASSWORD` instead of running the login step.

(data-access-tpxo)=
## TPXO (tidal forcing)

TPXO tidal constituents are needed **only for domains with tidal forcing** —
not for the Getting Started example. TPXO's license doesn't permit automated
fetching, so Forge cannot download it for you.

1. [Sign up for TPXO access](https://www.tpxo.net/global).
2. You'll receive download instructions from the TPXO team. Download the
   **TPXO10.v2a** netCDF files and place them under your source-data directory
   in exactly this layout:

   ```text
   <source_data>/TPXO/TPXO10.v2a/grid_tpxo10v2a.nc
   <source_data>/TPXO/TPXO10.v2a/h_tpxo10.v2a.nc
   <source_data>/TPXO/TPXO10.v2a/u_tpxo10.v2a.nc
   ```

Find your `source_data` directory with `python -m cstar_forge.config
show-paths`. If anything is missing at processing time, Forge fails with a
message listing the exact expected paths.

## Other user-staged datasets

A few optional datasets follow the same "you download, Forge verifies"
pattern as TPXO — for example WOA climatology, GLOFAS river discharge, and
EMOD topography. Each handler checks a documented location under
`<source_data>/` and raises with instructions if files are missing; see the
[SourceData documentation](source-data-intro.md) for the full inventory.
