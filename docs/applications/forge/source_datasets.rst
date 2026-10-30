.. _forge-source-datasets:

Source datasets
===============

Forge builds a domain's inputs from public datasets. Before generating
inputs it stages each source the blueprint lists (the blueprint's
``datasets`` field): downloading it into the shared source-data cache,
verifying that a file you were asked to provide is present, or, for sources
ROMS-Tools reads directly, doing nothing. The cache location is per machine;
``cstar forge show-paths`` prints it, and :doc:`../../deployments/hpc` explains where it goes.
Handlers check that files exist, never that they are current, so a cached
dataset is reused until you remove it.

The datasets themselves, their variables and their citations are documented
in `ROMS-Tools <https://roms-tools.readthedocs.io/en/latest/datasets.html>`__.
Registration steps for GLORYS and TPXO are in :doc:`../../getting_started/data_access`.

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
WOA_SALINITY
   The single World Ocean Atlas 2023 annual-mean salinity file (about 25 MB)
   that ESPER's salinity conditioning reads (see ESPER below). Downloaded
   only when an ESPER source has ``esper_salinity_conditioning`` on, into
   ``<source_data>/WOA/``; ``WOA_BGC`` stages the same file, so whichever
   runs first downloads it.
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
   Uniform biogeochemical values you specify in the blueprint, for initial
   and boundary conditions.
CONSTANTS (river biogeochemistry)
   The default river biogeochemistry: ROMS-Tools downloads its own file of
   default river tracer concentrations when the river forcing is generated.
ESPER *(experimental)*
   Biogeochemical fields estimated from the temperature and salinity of the
   physical source at generation time, using the CWorthy fork of PyESPER
   (either installed in the environment or pointed to with the source's
   ``path``). Large domains may need the source's ``serialize_dask`` option
   to bound memory. ESPER's nets have no training data at river-plume
   salinities and extrapolate badly there (silicate above 100 µmol/kg, negative
   nutrients, DIC above alkalinity); set the source's
   ``esper_salinity_conditioning: true`` to have PyESPER evaluate them at a
   salinity blended toward the WOA23 annual climatology below 31-34 PSU and
   dilute alkalinity and DIC back to the model salinity. Forge then downloads
   the one WOA23 file this needs (``WOA_SALINITY``, into the ``WOA`` folder).
   This needs the CWorthy PyESPER fork at tag ``v1.1.1+cworthy.1`` or later
   (``git clone --branch v1.1.1+cworthy.1 https://github.com/CWorthy-ocean/PyESPER``
   then ``pip install -e .``; the fork is not on PyPI or conda); the bundled
   ``glorys-era5-esper`` forcing spec has it on. ESPER oxygen is floored at
   5.0 mmol/m³ (MARBL's default ``parm_o2_min``) by roms-tools regardless of
   this switch. ESPER tracers are also groomed by default: ROMS-Tools detects
   implausible values and refills them from the surrounding water. Turn this
   off with ``groom: false`` on ``initial_conditions`` or ``boundary`` (the
   wizard's "Groom ESPER tracers" checkbox); the thresholds can be overridden
   by hand with ``groom_settings`` in the forge blueprint (see the ROMS-Tools
   datasets overview).

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
   The 0.25 degree World Ocean Atlas monthly salinity climatology used for
   sea-surface salinity restoring, distinct from the auto-downloaded
   ``WOA_BGC`` and ``WOA_SALINITY`` above. Download the twelve monthly files
   from the `WOA18 0.25 degree salinity directory
   <https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/>`__
   (WOA23 works the same way) and place them as:

   .. code-block:: text

      <source_data>/WOA/woa18_decav_s01_04.nc
      ...
      <source_data>/WOA/woa18_decav_s12_04.nc

   Leave out the ``s00`` and ``s13`` to ``s16`` files. All the WOA files
   share ``<source_data>/WOA/``: the restoring files carry the ``_04``
   (quarter-degree) suffix and the nutrient files ``_01``.
GLODAP
   The GLODAPv2.2016b mapped climatology (ALK, DIC, NO3, PO4, SiO3, O2), one
   file per variable under ``<source_data>/GLODAP/``:
   ``GLODAPv2.2016b.{TAlk,TCO2,NO3,PO4,silicate,oxygen}.nc``. The
   ``temperature`` and ``salinity`` files are optional but recommended;
   without them ROMS-Tools uses a uniform density for the unit conversion.
GLOFAS
   GloFAS v4.0 daily river discharge, an alternative to Dai. It comes from
   the Copernicus Early Warning Data Store and must be preprocessed to place
   river mouths on coastal cells and add RIVR2O river concentrations, so
   Forge expects one prepared file:

   .. code-block:: text

      <source_data>/GLOFAS/glofas_v4_rivers_daily_w_rivr2o.nc

RIVR2O
   River biogeochemistry, an alternative to the default constants. One
   netCDF file per year, any names:

   .. code-block:: text

      <source_data>/RIVR2O/*.nc

EMOD
   EMODnet bathymetry, as an alternative topography source. Export a
   netCDF file for your region from the `EMODnet viewer
   <https://emodnet.ec.europa.eu/geoviewer/>`__ and place it, under any name,
   in ``<source_data>/EMOD/``.

Your own files
--------------

Any source can be pointed at your own copy of the data instead. Give the
source a ``path`` in the forge blueprint (in the wizard, the path field next
to the source), and Forge skips staging for it and hands that path to
ROMS-Tools as it is:

.. code-block:: yaml

   forcing:
     initial_conditions:
       bgc_sources:
         - source:
             name: GLODAP
             path: /project/shared/data/GLODAPv2.2016b

Two sources work only this way:

CESM_REGRIDDED
   Biogeochemical fields from CESM output on a regular latitude-longitude
   grid, for surface forcing and initial and boundary conditions. Forge has
   nothing to download or check for it, so it needs a ``path``.
A pre-made river forcing file
   A river source named ``CUSTOM_FILE`` uses a river forcing netCDF you
   already have in place of building one from Dai or GloFAS. The file goes
   in the river item's ``custom_file`` (a ``location`` and a
   ``content_hash``), which the wizard fills in when you attach the file.

Staging outside a full run
--------------------------

Normally staging is the first stage of processing a blueprint. To pre-stage
data on its own, for example on a login node with network access before
submitting a job, run the blueprint with the later stages skipped:

.. code-block:: console

   cstar forge run forge_blueprint.yaml --no-generate --no-configure

The Python interface behind this is
:class:`~cstar.applications.forge.source_datasets.SourceDatasets`; see
:doc:`../../developers/forge_source_data` for how sources are registered and
how to add one.
