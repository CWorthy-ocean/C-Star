.. _forge-source-data-internals:

Forge internals: source datasets
================================

Before Forge generates any input, it stages the source datasets the forge
blueprint lists: it downloads them into the shared source-data cache, checks that
files a user must provide are in place, or does nothing for sources ROMS-Tools reads
itself. The user-facing list of sources is on :doc:`../applications/forge/source_datasets`;
this page is about how staging is put together and how to add a source.

The code is split in two:

- ``cstar/applications/forge/source_registry.py`` holds the name tables and
  dataset constants: ``SOURCE_ALIAS``, ``STREAMABLE_SOURCES``,
  ``UNSTAGED_DATASETS``, ``DERIVED_BGC_SOURCES``, ``DATASET_METADATA`` and the
  URLs and versions. It imports nothing heavy, so the resolver and the wizard can
  use it.
- ``cstar/applications/forge/source_datasets.py`` holds ``SourceDatasets``, the
  ``@register_dataset`` decorator, ``DATASET_REGISTRY`` and one handler per
  stageable dataset. This is the layer that imports ``copernicusmarine`` and
  ``roms_tools``.

How staging works
-----------------

.. code-block:: text

   source name in the blueprint ("GLORYS")
       -> SOURCE_ALIAS / resolve_dataset_key   -> dataset key ("GLORYS_REGIONAL")
       -> DATASET_REGISTRY                      -> handler + the attributes it requires
       -> SourceDatasets.prepare_all()          -> path(s) in SourceDatasets.paths

``ForgeExecutor`` builds a ``SourceDatasets`` with the blueprint's ``datasets``
list, the grid and run window the handlers may need, and ``source_data_dir``: the
cache root, which the caller supplies from the host paths (the module never
resolves it itself). ``prepare_all()`` runs each dataset's handler and records its
result in ``paths``; input generation then asks ``path_for_source()`` and
``streamable_for_source()`` for each source it reads. When the blueprint carries a
``resolved_datasets`` snapshot, those lookups use it rather than the live tables,
so a blueprint resolves the same way on another host or a later C-Star.

Source names fall into four groups:

Staged
   Has a handler in ``DATASET_REGISTRY``: downloaded (GLORYS, SRTM15, UNIFIED_BGC,
   WOA_BGC, WOA_SALINITY, MBL_CO2) or checked for user-provided files (TPXO, WOA, GLODAP,
   GLOFAS, RIVR2O, EMOD).
Streamable
   ``STREAMABLE_SOURCES`` (ERA5, DAI, the default river CONSTANTS): read by
   ROMS-Tools at generation time and skipped by ``prepare_all()``.
Unstaged
   ``UNSTAGED_DATASETS`` (ETOPO5, DAI): recognized names with no handler, because
   something else supplies the file.
Derived
   ``DERIVED_BGC_SOURCES`` (constants, ESPER): computed during generation; they
   never reach ``SourceDatasets``.

A name in none of these groups is an error, so a typo in a blueprint fails at
staging rather than later.

Adding a new dataset
--------------------

1. **Write a handler** in ``source_datasets.py`` and register it under its dataset
   key, listing the ``SourceDatasets`` attributes it needs. A handler receives the
   ``SourceDatasets`` instance, stages under ``self.cache_root / <KEY>``, honors
   ``self.clobber``, stores its result in ``self.paths[<KEY>]`` and returns a
   ``Path``, a list of paths, or a dict of named paths. A user-provided dataset
   only checks that its files exist, and raises with the expected layout if not.

   .. code-block:: python

      @register_dataset("MY_DATASET", requires=["grid", "grid_name"])
      def _prepare_my_dataset(self: SourceDatasets) -> Path:
          """Download MY_DATASET for the grid, or reuse the cached copy."""
          dataset_dir = self.cache_root / "MY_DATASET"
          dataset_dir.mkdir(parents=True, exist_ok=True)
          path = dataset_dir / f"my_dataset_{self.grid_name}.nc"
          if self.clobber or not path.exists():
              ...  # download into a temporary file, then move it to path
          self.paths["MY_DATASET"] = path
          return path

2. **Add a ``SOURCE_ALIAS`` entry** in ``source_registry.py`` if users name the
   source differently from its dataset key.
3. **Add a ``DATASET_METADATA`` entry** (``{"dataset_id": ...}``, ``{"url": ...}``,
   or ``{}`` for a user-provided dataset), so its provenance is recorded in the
   blueprint's ``resolved_datasets``.
4. **Add it to ``STREAMABLE_SOURCES``** instead of writing a handler if ROMS-Tools
   reads it remotely and nothing needs staging.
5. **List it on the user page**, :doc:`../applications/forge/source_datasets`,
   with the layout of any files users provide.
