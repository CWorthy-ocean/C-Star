.. _forge-source-datasets:

Source datasets
===============

Forge builds a domain's inputs from public datasets. Before generating
inputs it stages each source the blueprint lists (the blueprint's
``datasets`` field): downloading it into the shared source-data cache,
verifying that a file you were asked to provide is present, or, for sources
ROMS-Tools reads directly, doing nothing. The cache location is per machine;
``cstar forge show-paths`` prints it, and :doc:`../hpc` lists the defaults.
Handlers check that files exist, never that they are current, so a cached
dataset is reused until you remove it.

The datasets themselves, their variables and their citations are documented
in `ROMS-Tools <https://roms-tools.readthedocs.io/en/latest/datasets.html>`__.
Registration steps for GLORYS and TPXO are in :doc:`../data_access`.

Downloaded automatically
------------------------

GLORYS
   Global ocean reanalysis for physical initial and boundary conditions,
   fetched through the Copernicus Marine toolbox (registration required).
   Forge downloads the days covering the run window, padded by one day on
   each side, as a regional subset for the domain (``GLORYS_REGIONAL``) or
   the global fields (``GLORYS_GLOBAL``) depending on the blueprint.
   Multi-file GLORYS sources are read through a just-in-time reference index
   by default (``cstar forge run --no-subchunk`` turns this off).
SRTM15
   Bathymetry from Scripps, version pinned (for example ``SRTM15_V2.7``).
UNIFIED_BGC
   The unified biogeochemical climatology from ROMS-Tools, for MARBL initial
   and boundary conditions. The staged filename carries the version, so a
   version bump re-downloads rather than reusing a stale file.
WOA_BGC
   World Ocean Atlas 2023 nutrients and oxygen (NO3, PO4, SiO3, O2) with the
   matching monthly temperature and salinity, as a gridded biogeochemical
   source: 78 files, about 3 GB, staged into ``<source_data>/WOA/`` and
   resumable. WOA has no DIC, alkalinity or iron, so a MARBL run pairs it
   with GLODAP, ESPER or constants.
MBL_CO2
   NOAA marine boundary layer xCO2 surface reference.

Streamed or fetched by ROMS-Tools
---------------------------------

Nothing is staged for these; ROMS-Tools reads them at generation time.

ERA5
   Atmospheric surface forcing, streamed from the cloud.
DAI
   Dai river discharge climatology.
ETOPO5
   The default topography source, fetched when the grid is built.
constants
   Uniform biogeochemical values you specify in the blueprint.
ESPER *(experimental)*
   Biogeochemical fields estimated from the temperature and salinity of the
   physical source at generation time, using the CWorthy fork of PyESPER
   (either installed in the environment or pointed to with the source's
   ``path``). Large domains may need the source's ``serialize_dask`` option
   to bound memory.

Provided by you
---------------

These cannot be downloaded automatically. Place the files under the
source-data directory in the layout shown; Forge stops with the expected
paths if any are missing.

TPXO
   Tidal constituents (registration required):

   .. code-block:: text

      <source_data>/TPXO/TPXO10.v2a/grid_tpxo10v2a.nc
      <source_data>/TPXO/TPXO10.v2a/h_tpxo10.v2a.nc
      <source_data>/TPXO/TPXO10.v2a/u_tpxo10.v2a.nc

WOA
   The 0.25 degree World Ocean Atlas salinity climatology used for
   sea-surface salinity restoring, distinct from the auto-downloaded
   ``WOA_BGC`` above. Both live in ``<source_data>/WOA/``; the restoring
   files carry the ``_04`` suffix and the nutrient files ``_01``.
GLODAP
   The GLODAPv2.2016b mapped climatology (ALK, DIC, NO3, PO4, SiO3, O2), one
   file per variable under ``<source_data>/GLODAP/``:
   ``GLODAPv2.2016b.{TAlk,TCO2,NO3,PO4,silicate,oxygen}.nc``. The
   ``temperature`` and ``salinity`` files are optional but recommended;
   without them ROMS-Tools uses a uniform density for the unit conversion.
GLOFAS, RIVR2O
   River discharge and river biogeochemistry.
EMOD
   EMODnet bathymetry, as an alternative topography source.

Staging outside a full run
--------------------------

Normally staging is the first stage of processing a blueprint. To pre-stage
data on its own, for example on a login node with network access before
submitting a job, run the blueprint with the later stages skipped:

.. code-block:: console

   cstar forge run forge_blueprint.yaml --no-generate --no-configure

The Python interface behind this is
:class:`~cstar.applications.forge.source_datasets.SourceDatasets`; see
:doc:`../developers/forge_source_data` for how sources are registered and
how to add one.
