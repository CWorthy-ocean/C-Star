.. _forge-source-data:

Source data
===========

Overview
--------

The ``cstar.applications.forge.source_datasets`` module manages the
acquisition, preparation, and caching of model input datasets required for
ROMS/MARBL domain generation and simulation.

These datasets are documented in `ROMS Tools
<https://roms-tools.readthedocs.io/en/latest/datasets.html>`__.

``source_datasets.py`` provides a registry-driven system for handling diverse
data sources, allowing for flexible workflows whether datasets are streamed
or locally cached. The alias map, streamable-source list, and per-dataset
provenance metadata it uses live in the lighter-weight sibling module
``cstar.applications.forge.source_registry`` (see `Developer's guide`_
below).

.. important:: Register for dataset access

   The ``source_datasets.py`` module provides automated downloading of data
   assets used to force the model; however, some of these require
   registration to permit access.

   - GLORYS data is provided via the Copernicus Marine Service. Learn how to
     register for access `here
     <https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service>`__.
     That process should result in a ``.copernicusmarine`` or
     ``.copernicusmarine-credentials`` file in your home directory.
   - Access to the TPXO Global Tidal Model data requires registration,
     available `here <https://www.tpxo.net/global>`__.

Dataset preparation logic
--------------------------

- **SRTM15**: Downloads topography from Scripps (version controlled, e.g.
  ``SRTM15_V2.7``). Returns a single Path.
- **GLORYS**: Global or regional ocean initial conditions; subset and
  time-extract logic depends on whether the request is regional (grid-based,
  dataset key ``GLORYS_REGIONAL``) or global (``GLORYS_GLOBAL``). Returns a
  ``List[Path]`` (one file per day, with the window padded +/-1 day);
  ``src.paths["GLORYS_REGIONAL"]`` is always a list after ``prepare_all()``.
- **UNIFIED_BGC**: `Unified biogeochemistry forcing & initial conditions from
  ROMS Tools
  <https://roms-tools.readthedocs.io/en/latest/initial_conditions.html#Adding-Biogeochemical-(BGC)-Initial-Conditions>`__.
  Downloaded from Google Drive; version controlled like SRTM15, so the staged
  filename carries the version (``BGCdataset_v2_1.nc``) and a version bump
  re-downloads rather than reusing the stale cached file. **Requires a
  roms-tools newer than 4.0.1**: v2.1 files name their dimensions
  ``longitude``/``latitude``/``depth``, which older roms-tools renames
  unconditionally and chokes on, so the handler refuses to stage and says so.
  Returns a single Path.
- **MBL_CO2**: NOAA marine boundary-layer xCO2 surface reference data.
  Downloaded once and cached; returns a single Path.
- **WOA_BGC**: World Ocean Atlas 2023 nutrients and oxygen (``NO3``,
  ``PO4``, ``SiO3``, ``O2``) as a gridded BGC source for initial and boundary
  conditions, plus the matching monthly temperature and salinity that
  roms-tools needs to convert umol/kg to mmol/m3 and to supply the source
  density coordinate for ``density``/``density_mld`` interpolation.
  Downloaded from NCEI: 78 files (12 monthly + 1 annual per variable, ~3 GB)
  staged flat into ``source_data_dir / "WOA"``, the same directory as the
  restoring **WOA** source. The two do not collide -- BGC files carry the
  ``_01`` (1 degree) suffix and the restoring files ``_04`` (0.25 degree) --
  and staging is resumable, skipping files already present. Returns the
  **directory**, which is what roms-tools' ``WOABGCDataset`` expects as its
  ``path``. Note that WOA has no DIC, alkalinity or iron, so a MARBL run must
  pair this with GLODAP, ESPER or a constants source. Blueprints name it
  ``WOA_BGC``; Forge renames it to ``WOA`` on the way to roms-tools (see
  ``ROMS_TOOLS_SOURCE_NAME``).
- **ERA5**: Atmospheric surface forcing (streamable, no local download
  needed). Handler is an intentional no-op: it logs and returns ``None`` (so
  ``paths["ERA5"]`` is ``None``).
- **TPXO**, **WOA**, **GLOFAS**, **EMOD**, **RIVR2O**: User-provided datasets
  (tidal harmonics, restoring salinity climatology, river discharge/BGC,
  alternative topography). Note **WOA** here is the 0.25 degree
  sea-surface-salinity restoring source, distinct from the auto-downloaded
  **WOA_BGC** above. Forge cannot download these itself; each handler only
  verifies that the expected files already exist under the dataset's cache
  directory and raises ``FileNotFoundError`` with instructions if they don't.
  TPXO's handler returns a dictionary with keys ``"grid"``, ``"h"``, and
  ``"u"`` mapping to file paths, stored in ``src.paths["TPXO"]``.
- **GLODAP**: The GLODAPv2.2016b mapped climatology (``ALK``, ``DIC``,
  ``NO3``, ``PO4``, ``SiO3``, ``O2``), usable as an
  ``InitialConditions``/``BoundaryForcing`` ``bgc_sources`` entry alongside
  ``WOA_BGC`` or ``ESPER``. User-staged, like EMOD/RIVR2O: place one file per
  variable,
  ``GLODAPv2.2016b.{TAlk,TCO2,NO3,PO4,silicate,oxygen}.nc``, under
  ``<source_data_dir>/GLODAP/``; the ``GLODAP`` handler in
  ``source_datasets.py`` verifies them and hands roms-tools the directory.
  ``GLODAPv2.2016b.temperature.nc``/``.salinity.nc`` are optional but
  recommended: roms-tools uses them for the umol/kg to mmol/m3 conversion
  (in-situ density) and warns and falls back to a uniform 1025 kg/m3 without
  them. It is a static field with no time axis, so the wizard hides the
  ``climatology`` checkbox for it, as for ``constants``/``ESPER``.
- **ESPER** *(experimental)*: BGC fields (e.g. DIC/alkalinity) estimated
  from the physics temperature/salinity at generation time via PyESPER,
  rather than read from any staged dataset -- ``SourceSpec.esper_method``/
  ``esper_equation`` configure the estimator. Needs the CWorthy fork of
  PyESPER; either point ``SourceSpec.path`` at a checkout (containing
  ``Mat_fullgrid/``/``NeuralNetworks/``) or have it pip-installed with
  ``pip install -e`` into the environment so it locates its own data
  directories. Like
  ``constants``, it carries no time axis, so no ``climatology`` option. Large
  domains may need a source's ``serialize_dask`` set (see ``BgcSourceItem``)
  -- PyESPER's own numba kernels already use every core, so the ordinary
  concurrent NetCDF write can exhaust memory.
- **CONSTANTS** (blueprints spell it lowercase ``constants``), **DAI**:
  Streamed/auto-downloaded by roms-tools itself at generation time -- Forge
  never stages a local path for these -- ``CONSTANTS`` additionally has no
  registry entry at all: the resolver never places it in a blueprint's
  ``datasets`` list, and requesting it from ``SourceDatasets`` directly
  raises ``ValueError``.
- **ETOPO5**: The default topography source; like CONSTANTS/DAI, roms-tools
  fetches it itself (at grid-build time), so Forge does not stage it either.

Each preparation routine ensures datasets exist locally and are subsetted for
the target domain/grid (handlers check existence, never freshness).

Example
-------

Normally you don't call ``SourceDatasets`` directly: ``cstar forge run
<forge_blueprint.yaml>`` (or ``cstar blueprint run ...``) runs source-data
preparation as one step of executing a ``ForgeBlueprint``, auto-detecting the
host's shared download cache for you. The snippet below is the lower-level
API that ``ForgeExecutor`` calls internally -- useful for pre-staging data
outside of a full blueprint run.

``SourceDatasets`` does not resolve its cache location from
``cstar.applications.forge.config`` internally; the caller must inject it via
``source_data_dir``. ``cstar.applications.forge.config.resolve_host()``
builds the same ``HostPaths`` the forge application would use, whose
``source_data_cache`` is the shared download cache root.

.. code-block:: python

   from datetime import datetime
   from cstar.applications.forge.source_datasets import SourceDatasets
   from cstar.applications.forge.config import resolve_host

   host = resolve_host(working_dir="~/cstar/_forge_bp_runs/my_domain")

   start_time = datetime(2012, 1, 1)
   end_time = datetime(2012, 1, 2)

   domain_grid = roms_tools.Grid(...)  # the domain's Grid (as built by the executor)

   src = SourceDatasets(
       datasets=["GLORYS", "SRTM15", "UNIFIED_BGC"],
       clobber=True,
       grid=domain_grid,
       grid_name="my_domain",
       start_time=start_time,
       end_time=end_time,
       source_data_dir=host.source_data_cache,
   )

   # Prepares and caches the datasets needed
   src.prepare_all()
   # Paths to prepared files are available as: src.paths[<DATASET_KEY>]
   # Note: src.paths["GLORYS_REGIONAL"] is a List[Path] (one file per day, window padded +/-1 day)
   # For streamable sources (e.g., ERA5), use: src.prepare_all(include_streamable=True)
   # (ERA5 still stages no file; paths["ERA5"] is None)
   # You can also use: src.path_for_source("GLORYS") to get the path using the logical name

Developer's guide
------------------

Module design philosophy
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``cstar.applications.forge.source_datasets`` module provides a
**registry-based framework** for managing heterogeneous source datasets used
in ROMS preprocessing. The design emphasizes:

- **Extensibility**: New datasets can be added by registering handler
  functions without modifying core logic.
- **Dependency management**: Each dataset declares its requirements (grid,
  time range, etc.) explicitly.
- **Caching**: Datasets are downloaded/cached locally and reused across runs,
  under a root directory injected by the caller (not resolved internally).
- **Abstraction**: User-facing logical names (e.g., "GLORYS") map to
  implementation-specific dataset keys (e.g., "GLORYS_REGIONAL").

The module is split across two files:

- ``cstar/applications/forge/source_datasets.py`` -- the ``SourceDatasets``
  dataclass, the ``@register_dataset`` decorator, ``DATASET_REGISTRY``, and
  every dataset handler function. This is the "heavy" layer: it imports
  ``copernicusmarine``, ``gdown``, and ``roms_tools``.
- ``cstar/applications/forge/source_registry.py`` -- a dependency-light,
  stdlib-only module holding ``SOURCE_ALIAS``, ``STREAMABLE_SOURCES``,
  ``UNSTAGED_DATASETS``, ``DATASET_METADATA``, and the resolution helpers
  (``map_source_to_dataset_key``, ``resolve_dataset_key``,
  ``resolve_source``). It exists so the alias/metadata tables can be
  imported by lightweight callers (e.g. the blueprint resolver) without
  pulling in the acquisition dependencies. It also holds the versioned
  dataset-id/URL constants (``SRTM15_URL``, ``GLORYS_DATASET_ID``,
  ``MBL_CO2_URL``, ``WOA_DOWNLOAD_URL``,
  ``UNIFIED_BGC_URL``/``UNIFIED_BGC_VERSION``/``UNIFIED_BGC_FILENAME``,
  ``GLOFAS_CDS_URL``). ``source_datasets.py`` re-exports the alias map, the
  streamable/unstaged sets, the versioned URL constants, and
  ``map_source_to_dataset_key`` for existing consumers, so ``from
  cstar.applications.forge.source_datasets import SOURCE_ALIAS`` still
  works; ``DATASET_METADATA``, ``resolve_dataset_key``, and
  ``resolve_source`` must be imported from
  ``cstar.applications.forge.source_registry`` directly.

Core architecture
~~~~~~~~~~~~~~~~~~

The module consists of three main components:

1. **Registry system**: Decorator-based registration of dataset handlers
   (``source_datasets.py``).
2. **SourceDatasets class**: Main interface for preparing and accessing
   datasets (``source_datasets.py``).
3. **Source name mapping**: Translation layer between logical names and
   dataset keys (``source_registry.py``, re-exported from
   ``source_datasets.py``).

.. code-block:: text

   User Request ("GLORYS")
       |
       v
   SOURCE_ALIAS mapping
       |
       v
   Dataset Key ("GLORYS_REGIONAL")
       |
       v
   DATASET_REGISTRY lookup
       |
       v
   DatasetHandler (function + requirements)
       |
       v
   Handler execution -> Path(s) to prepared data

Core objects
~~~~~~~~~~~~~

``SourceDatasets`` (dataclass)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The main interface for preparing and accessing source datasets.

Constructor:

.. code-block:: python

   SourceDatasets(
       datasets: list[str],                        # Dataset names to prepare
       clobber: bool = False,                       # Force re-download if True
       grid: object | None = None,
       grid_name: str | None = None,
       start_time: object | None = None,
       end_time: object | None = None,
       source_data_dir: Path | None = None,         # Cache root -- injected by the caller
       resolved_datasets: dict[str, dict] | None = None,  # Optional frozen key/metadata snapshot
   )

``SourceDatasets`` does not resolve a cache directory from
``cstar.applications.forge.config`` internally -- the caller (typically
``ForgeExecutor``) must inject ``source_data_dir``, e.g. from a
``HostPaths.source_data_cache`` built by
``cstar.applications.forge.config.resolve_host()``. If ``source_data_dir``
is ``None``, ``prepare_all()`` raises ``ValueError`` for the first dataset it
actually stages (unstaged/streamable skips happen first).

Key attributes:

- ``datasets``: Normalized list of dataset keys (after alias resolution).
- ``paths``: Dictionary mapping dataset keys to prepared file paths.
- ``source_data_dir``: Cache root under which all datasets are staged
  (host-injected; required to call ``prepare_all()``).
- ``resolved_datasets``: Optional
  ``{logical_name: {dataset_key, dataset_id, url, streamable}}`` snapshot,
  typically frozen into a ``ForgeBlueprint`` at build time. When present,
  ``dataset_key_for_source()`` / ``streamable_for_source()`` prefer it over
  live ``source_registry`` lookups, so a blueprint resolves identically even
  if the registry drifts later on a different host/forge version.
- Optional attributes (``grid``, ``start_time``, etc.) are only required if
  a dataset handler declares them.

Key methods:

- ``prepare_all(include_streamable=False)``: Prepare all requested datasets.
- ``path_for_source(logical_name, glorys_layout=None)``: Get path for a
  logical source name (e.g., "GLORYS").
- ``dataset_key_for_source(logical_name, glorys_layout=None)``: Map logical
  name to dataset key.
- ``streamable_for_source(logical_name, glorys_layout=None)``: Whether a
  logical name is streamable (see below).

Lifecycle:

1. **Initialization**: Normalizes dataset names through ``SOURCE_ALIAS``,
   validates against ``DATASET_REGISTRY | UNSTAGED_DATASETS``.
2. **Preparation**: ``prepare_all()`` iterates through datasets, skips
   ``UNSTAGED_DATASETS`` (e.g. ``ETOPO5``, ``DAI`` -- provided by roms-tools
   itself, never staged by Forge) and, unless ``include_streamable=True``,
   skips ``STREAMABLE_SOURCES``; for the rest it checks requirements and
   calls handler functions.
3. **Storage**: Handler return values (``Path``, ``List[Path]``, or
   ``Dict``) are stored in ``self.paths``. ``prepare_all()`` overwrites
   ``self.paths[key]`` with the handler's return value, so a handler's own
   ``self.paths[...]`` assignment only matters when the handler is called
   directly.

``DatasetHandler`` (class)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Container for a dataset preparation function and its dependency
requirements.

.. code-block:: python

   class DatasetHandler:
       def __init__(self, func: Callable[["SourceDatasets"], Path], requires: List[str]):
           self.func = func      # Handler function
           self.requires = requires  # Required SourceDatasets attributes

Purpose:

- Encapsulates handler function and metadata.
- Enables dependency checking before handler execution.
- Stored in ``DATASET_REGISTRY`` keyed by dataset name.

Registry framework
~~~~~~~~~~~~~~~~~~~~

Registration decorator
^^^^^^^^^^^^^^^^^^^^^^^^

The ``@register_dataset`` decorator registers dataset preparation functions:

.. code-block:: python

   @register_dataset(
       name: str,                    # Dataset key (e.g., "GLORYS_REGIONAL")
       requires: Optional[List[str]] = None  # Required SourceDatasets attributes
   )
   def _prepare_dataset(self: SourceDatasets) -> Union[Path, List[Path], Dict]:
       """Handler function that prepares the dataset."""
       # Implementation...
       return path_or_paths

Registration process:

1. Decorator captures function and requirements.
2. Creates ``DatasetHandler`` instance.
3. Stores in ``DATASET_REGISTRY`` with uppercase key.

Example:

.. code-block:: python

   @register_dataset(
       "GLORYS_REGIONAL",
       requires=["grid", "grid_name", "start_time", "end_time"]
   )
   def _prepare_glorys_regional(self: SourceDatasets) -> List[Path]:
       """Download daily regional GLORYS subsets."""
       bounds = rt.get_glorys_bounds(self.grid)
       paths = self._prepare_glorys_daily(is_regional=True, bounds=bounds)
       self.paths["GLORYS_REGIONAL"] = paths[0] if len(paths) == 1 else paths
       return paths

Registry dictionary
^^^^^^^^^^^^^^^^^^^^^

``DATASET_REGISTRY: Dict[str, DatasetHandler]`` (defined in
``source_datasets.py``, alongside the handlers it registers) maps dataset
keys to their handlers.

- Keys are **uppercase** (normalized during registration).
- Values are ``DatasetHandler`` instances.
- Populated at module import time via decorators.

Handler function signature
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Handler functions must:

- Accept ``self: SourceDatasets`` as first parameter, which ``prepare_all``
  passes explicitly (``handler.func(self)``) -- these are module-level
  functions, not methods.
- Return ``Path``, ``List[Path]``, or ``Dict[str, Path]`` (stored in
  ``self.paths[dataset_key]``).
- Access required attributes via ``self`` (e.g., ``self.grid``,
  ``self.start_time``).
- Use ``self.clobber`` to determine if re-download is needed.
- Store result in ``self.paths[dataset_key]`` (convention, not required).

Source name mapping
~~~~~~~~~~~~~~~~~~~~~

Logical names vs. dataset keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Users specify **logical source names** in a **ForcingSpec**
(``catalog/ForcingSpec/<name>/Forcing.yaml``); the topography source is
Domain-level (``model.yaml`` no longer carries source selection):

- ``"GLORYS"`` -> maps to ``"GLORYS_REGIONAL"`` or ``"GLORYS_GLOBAL"``
  (platform-dependent)
- ``"UNIFIED"`` -> maps to ``"UNIFIED_BGC"``
- ``"SRTM15"`` -> ``"SRTM15"`` (un-versioned key; the version appears in the
  staged filename ``SRTM15_V2.7.nc``)

``SOURCE_ALIAS`` dictionary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defined in ``cstar/applications/forge/source_registry.py`` (re-exported from
``source_datasets.py``). Maps logical names to dataset registry keys:

.. code-block:: python

   SOURCE_ALIAS: dict[str, str] = {
       "ERA5": "ERA5",
       "GLORYS": "GLORYS_REGIONAL",       # defaults to regional; see resolve_dataset_key
       "GLORYS_GLOBAL": "GLORYS_GLOBAL",
       "GLORYS_REGIONAL": "GLORYS_REGIONAL",
       "UNIFIED": "UNIFIED_BGC",
       "UNIFIED_BGC": "UNIFIED_BGC",
       "SRTM15": "SRTM15",
       "MBL_CO2": "MBL_CO2",
       "TPXO": "TPXO",
       "WOA": "WOA",
       "DAI": "DAI",
       "GLOFAS": "GLOFAS",
       "EMOD": "EMOD",
       "RIVR2O": "RIVR2O",
       "CONSTANTS": "CONSTANTS",
   }

Mapping function:

.. code-block:: python

   def map_source_to_dataset_key(name: str) -> str:
       """Map logical name to dataset key, or return uppercased name if no alias."""
       return SOURCE_ALIAS.get(name.upper(), name.upper())

``resolve_dataset_key(name, glorys_layout=None)`` wraps this and
additionally handles the one case the table can't: logical ``"GLORYS"``
disambiguated by an explicit ``glorys_layout`` (``"global"`` vs.
``"regional"``, defaulting to regional).

Normalization:

- ``SourceDatasets.__post_init__()`` normalizes all dataset names through
  ``SOURCE_ALIAS``.
- Unknown names are uppercased and used as-is (must exist in
  ``DATASET_REGISTRY`` or ``UNSTAGED_DATASETS``, or ``__post_init__`` raises
  ``ValueError``).

Streamable and unstaged sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some datasets don't require local caching (e.g., ERA5, DAI, CONSTANTS).
Listed in ``STREAMABLE_SOURCES`` (``source_registry.py``):

.. code-block:: python

   STREAMABLE_SOURCES = ["ERA5", "DAI", "CONSTANTS"]

- Skipped by default in ``prepare_all()`` unless ``include_streamable=True``.
- ``path_for_source()`` returns ``None`` for streamable sources if not
  prepared.
- ``CONSTANTS`` is streamable but has **no registry entry at all** -- the
  resolver strips it upstream; passing it to ``SourceDatasets`` raises
  ``ValueError: Unknown dataset(s) requested``.

A distinct set, ``UNSTAGED_DATASETS = {"ETOPO5", "DAI"}``, covers recognized
keys that Forge never stages *at all* -- no ``@register_dataset`` handler
exists because something else supplies the file (roms-tools auto-fetches
``ETOPO5`` at grid-build time; ``DAI`` is streamed).
``SourceDatasets.__post_init__()`` treats these as valid-but-skipped,
distinguishing them from a genuinely unknown/typo'd name, which still
raises. ``prepare_all()`` always skips them, even with
``include_streamable=True``.

Adding a new dataset
~~~~~~~~~~~~~~~~~~~~~~

Step 1: Implement handler function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   @register_dataset("MY_DATASET", requires=["grid", "grid_name"])
   def _prepare_my_dataset(self: SourceDatasets) -> Path:
       """
       Prepare MY_DATASET for the given grid.

       Returns
       -------
       Path
           Path to the prepared dataset file.
       """
       dataset_dir = self.source_data_dir / "MY_DATASET"
       dataset_dir.mkdir(parents=True, exist_ok=True)
       path = dataset_dir / f"my_dataset_{self.grid_name}.nc"

       needs_download = self.clobber or (not path.exists())

       if needs_download:
           if path.exists():
               path.unlink()
           # Download/prepare logic here...
       self.paths["MY_DATASET"] = path
       return path

Step 2: Add source alias (if needed)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If users should reference it by a logical name, add the entry to
``SOURCE_ALIAS`` in ``cstar/applications/forge/source_registry.py``:

.. code-block:: python

   "MY_SOURCE": "MY_DATASET",

Step 3: Add to streamable sources (if applicable)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the dataset doesn't need local caching, add it to ``STREAMABLE_SOURCES``
in ``cstar/applications/forge/source_registry.py``:

.. code-block:: python

   "MY_DATASET",

Step 4: Add a ``DATASET_METADATA`` entry
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add a provenance entry to ``DATASET_METADATA`` in
``cstar/applications/forge/source_registry.py`` (``{"dataset_id": ...}`` or
``{"url": ...}``; ``{}`` for user-staged datasets) so provenance is
snapshotted into ``ForgeBlueprint.forcing.resolved_datasets``.

Design patterns
~~~~~~~~~~~~~~~~~

Dependency injection
^^^^^^^^^^^^^^^^^^^^^^

Required attributes are injected into ``SourceDatasets`` and accessed by
handlers via ``self``. This enables:

- Lazy evaluation (attributes only needed if dataset is requested).
- Clear dependency declaration via ``requires`` parameter.
- Runtime validation before handler execution.

Caching strategy
^^^^^^^^^^^^^^^^^^

- Files are cached in ``self.source_data_dir / {dataset_name} /``, where
  ``source_data_dir`` is injected by the caller (e.g. ``ForgeExecutor``,
  from ``HostPaths.source_data_cache``) -- ``source_datasets.py`` does not
  import ``cstar.applications.forge.config`` to resolve this path itself
  (some handlers nest one level deeper or use a fixed filename -- e.g.
  ``TPXO/TPXO10.v2a/``, ``GLOFAS/glofas_v4_rivers_daily_w_rivr2o.nc``).
- Existence check: ``if self.clobber or (not path.exists())``.
- Clobber mode: Remove existing file before download.

Return value flexibility
^^^^^^^^^^^^^^^^^^^^^^^^^^

Handlers can return:

- ``Path``: Single file.
- ``List[Path]``: Multiple files (e.g., daily time series).
- ``Dict[str, Path]``: Named file collection (e.g., TPXO with grid/h/u
  files).

All are stored in ``self.paths[dataset_key]`` for uniform access.
