.. _data-access:

Registering for datasets
========================

Forge builds a domain's input files from public observational and reanalysis
datasets, most of which it downloads for you. The datasets themselves are
documented in `ROMS-Tools <https://roms-tools.readthedocs.io/en/latest/datasets.html>`__,
which does the regridding. Two of them require a free registration before
Forge can use them, and a few must be downloaded by hand because their licenses
do not allow automated fetching.

Where the files go
------------------

Downloaded and user-staged datasets live in a shared source-data directory
that C-Star chooses per machine, so several domains reuse one copy. Find it
with:

.. code-block:: console

   cstar forge show-paths

On a laptop it is ``~/cstar-forge-data/source-data``; on supported HPC
systems it sits under the project or scratch file system (see :doc:`hpc`).

.. _forge-data-access-glorys:

Copernicus Marine (GLORYS)
--------------------------

GLORYS ocean reanalysis supplies initial and boundary conditions for
essentially every domain, including the toy example in the tutorials. Forge
downloads it on demand through the Copernicus Marine toolbox.

1. `Sign up for the Copernicus Marine Service <https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service>`__.
2. Log in once with the ``copernicusmarine`` command, which is installed with
   C-Star:

   .. code-block:: console

      copernicusmarine login

   This stores a credentials file under ``~/.copernicusmarine/`` that every
   later run picks up.

For non-interactive use, such as batch jobs, set
``COPERNICUSMARINE_SERVICE_USERNAME`` and
``COPERNICUSMARINE_SERVICE_PASSWORD`` in the environment instead of running
the login step.

.. _forge-data-access-tpxo:

TPXO (tides)
------------

TPXO tidal constituents are needed only for domains with tidal forcing. The
TPXO license does not permit automated downloads, so you fetch the files
yourself.

1. `Register for TPXO access <https://www.tpxo.net/global>`__ and follow the
   download instructions you receive.
2. Place the **TPXO10.v2a** netCDF files under your source-data directory in
   this layout:

   .. code-block:: text

      <source_data>/TPXO/TPXO10.v2a/grid_tpxo10v2a.nc
      <source_data>/TPXO/TPXO10.v2a/h_tpxo10.v2a.nc
      <source_data>/TPXO/TPXO10.v2a/u_tpxo10.v2a.nc

If a file is missing when a domain is processed, Forge stops with a message
listing the exact paths it expected.

Other user-staged datasets
--------------------------

A few optional sources follow the same pattern: you download, Forge checks.
Each expects its files at a documented location under the source-data
directory and stops with instructions if they are absent.

- **WOA** (0.25 degree World Ocean Atlas salinity) for sea-surface salinity
  restoring.
- **GLOFAS** river discharge and **RIVR2O** river biogeochemistry.
- **EMOD** bathymetry as an alternative topography source.
- **GLODAP** mapped climatology as a biogeochemical initial and boundary
  source.

:doc:`forge/source_datasets` lists every source Forge knows about, which are
downloaded automatically, which stream at generation time, and the expected
file layout for each user-staged one.

Everything else, including ERA5 atmospheric forcing, the unified
biogeochemical climatology, World Ocean Atlas nutrients and the default
topography, is fetched without an account.
