.. _forge-source-data-internals:

Forge internals: source datasets
====================================

Module design philosophy
--------------------------

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
------------------

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
-------------

``SourceDatasets`` (dataclass)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
--------------------

Registration decorator
~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

``DATASET_REGISTRY: Dict[str, DatasetHandler]`` (defined in
``source_datasets.py``, alongside the handlers it registers) maps dataset
keys to their handlers.

- Keys are **uppercase** (normalized during registration).
- Values are ``DatasetHandler`` instances.
- Populated at module import time via decorators.

Handler function signature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
---------------------

Logical names vs. dataset keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users specify **logical source names** in a **ForcingSpec**
(``catalog/ForcingSpec/<name>/Forcing.yaml``); the topography source is
Domain-level (``model.yaml`` no longer carries source selection):

- ``"GLORYS"`` -> ``"GLORYS_REGIONAL"`` by default, or ``"GLORYS_GLOBAL"``
  when an explicit ``glorys_layout="global"`` is given (see
  ``resolve_dataset_key``)
- ``"UNIFIED"`` -> maps to ``"UNIFIED_BGC"``
- ``"SRTM15"`` -> ``"SRTM15"`` (un-versioned key; the version appears in the
  staged filename ``SRTM15_V2.7.nc``)

``SOURCE_ALIAS`` dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
       "WOA": "WOA",           # SSS-restoring salinity (0.25 deg); user-staged
       "WOA_BGC": "WOA_BGC",   # WOA23 1-deg gridded BGC source; auto-downloaded
       "DAI": "DAI",
       "GLOFAS": "GLOFAS",
       "EMOD": "EMOD",
       "RIVR2O": "RIVR2O",
       "GLODAP": "GLODAP",     # GLODAPv2.2016b mapped BGC climatology; user-staged
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
----------------------

Step 1: Implement handler function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If users should reference it by a logical name, add the entry to
``SOURCE_ALIAS`` in ``cstar/applications/forge/source_registry.py``:

.. code-block:: python

   "MY_SOURCE": "MY_DATASET",

Step 3: Add to streamable sources (if applicable)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the dataset doesn't need local caching, add it to ``STREAMABLE_SOURCES``
in ``cstar/applications/forge/source_registry.py``:

.. code-block:: python

   "MY_DATASET",

Step 4: Add a ``DATASET_METADATA`` entry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a provenance entry to ``DATASET_METADATA`` in
``cstar/applications/forge/source_registry.py`` (``{"dataset_id": ...}`` or
``{"url": ...}``; ``{}`` for user-staged datasets) so provenance is
snapshotted into ``ForgeBlueprint.forcing.resolved_datasets``.

Design patterns
-----------------

Dependency injection
~~~~~~~~~~~~~~~~~~~~~~

Required attributes are injected into ``SourceDatasets`` and accessed by
handlers via ``self``. This enables:

- Lazy evaluation (attributes only needed if dataset is requested).
- Clear dependency declaration via ``requires`` parameter.
- Runtime validation before handler execution.

Caching strategy
~~~~~~~~~~~~~~~~~~

- Files are cached in ``self.source_data_dir / {dataset_name} /``, where
  ``source_data_dir`` is injected by the caller (e.g. ``ForgeExecutor``,
  from ``HostPaths.source_data_cache``) -- ``source_datasets.py`` does not
  import ``cstar.applications.forge.config`` to resolve this path itself
  (some handlers nest one level deeper or use a fixed filename -- e.g.
  ``TPXO/TPXO10.v2a/``, ``GLOFAS/glofas_v4_rivers_daily_w_rivr2o.nc``).
- Existence check: ``if self.clobber or (not path.exists())``.
- Clobber mode: Remove existing file before download.

Return value flexibility
~~~~~~~~~~~~~~~~~~~~~~~~~~

Handlers can return:

- ``Path``: Single file.
- ``List[Path]``: Multiple files (e.g., daily time series).
- ``Dict[str, Path]``: Named file collection (e.g., TPXO with grid/h/u
  files).

All are stored in ``self.paths[dataset_key]`` for uniform access.
