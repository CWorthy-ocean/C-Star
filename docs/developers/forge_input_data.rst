.. _forge-input-data:

Forge internals: input data generation
=========================================

.. note::

   **This subsystem is driven by the forge application** (``cstar forge run
   <forge_blueprint.yaml>``, or the app-framework ``cstar blueprint run``
   with defaults only). Both load a ``ForgeBlueprint`` and call
   ``ForgeExecutor.generate_inputs()`` (``cstar/applications/forge/executor.py``),
   which constructs a ``RomsMarblInputData`` instance and calls
   ``generate_all()`` on it. Constructing ``RomsMarblInputData`` directly (as
   shown later on this page) is for developers debugging or extending input
   generation -- normal usage goes through the forge application.

The ``input_data`` module (``cstar/applications/forge/input_data.py``)
generates model-ready input files from prepared source datasets. It uses a
**registry-based framework** similar to the ``source_data`` module (see
:doc:`forge_source_data`): input steps are decorator-registered functions run
in a fixed order.

Module purpose
---------------

The input data generation process transforms prepared source datasets into
model-ready input files:

- **Grid files**: ROMS grid NetCDF files
- **Initial conditions**: Temperature, salinity, and biogeochemical fields
- **Forcing data**: Surface, boundary, tidal, and river forcing
- **CDR forcing**: Carbon dioxide removal forcing (optional)
- **Corrections**: Forcing corrections (registered but unwired: the resolver
  never emits a ``corrections`` category, and the handler raises
  ``NotImplementedError``)

Base class: ``InputData``
----------------------------

An abstract dataclass defining the interface for input data generation:
``domain_name``, ``start_date``, ``end_date``, and an ``input_data_dir``
(injected by the caller -- the executor -- rather than derived from
``cstar.applications.forge.config``, keeping the class host-independent).
Subclasses implement ``generate_all()``. The base class also creates
``input_data_dir`` in ``__post_init__``, provides a filename-construction
helper (``_forcing_filename``), and handles clobber logic for existing files
(``_ensure_empty_or_clobber``).

Registry system
-----------------

Input generation steps are registered with the ``@register_input(name, order,
label)`` decorator onto ``INPUT_REGISTRY: dict[str, InputStep]``, each entry
holding the handler function, its execution order, and a progress label.

Execution order (steps run in order, lowest ``order`` value first):

- ``grid`` (order=10)
- ``initial_conditions`` (order=20)
- ``forcing.surface`` (order=30)
- ``forcing.boundary`` (order=40)
- ``forcing.tidal`` (order=50)
- ``forcing.river`` (order=60)
- ``cdr_forcing`` (order=80)
- ``forcing.corrections`` (order=90; registered but unwired -- never emitted
  by the resolver)

``RomsMarblInputData``
-------------------------

``RomsMarblInputData`` (a dataclass subclass of ``InputData``, both defined
in ``input_data.py``) implements ROMS-MARBL-specific input generation: grid,
initial conditions, and every forcing category.

There is no ``model_spec`` field -- the class is host-/model-spec-independent.
Beyond the fields inherited from ``InputData``, the constructor takes the
injected ``grid``/``grid_parent``/``grid_child`` objects, ``boundaries``,
``source_data``, ``partitioning``, and ``roms_marbl_blueprint_dir``; the CDR
selection (``cdr_mode``, plus ``cdr_forcing`` or a pre-made
``cdr_forcing_file`` -- mutually exclusive, enforced upstream by the
``ForgeBlueprint`` schema); ``settings_compile_time``/``settings_run_time``
(the executor's own live settings dicts, bound **by reference**, not copied
-- see `Settings initialization`_ below); and tuning flags (``use_dask``,
``dask_num_workers``, ``use_pio``, ``subchunk``, ``verbose``, ``has_bgc``).
See ``input_data.py`` for the full field list and docstrings; it changes
often enough that reproducing it here would drift.

``forcing_override`` (injected by the caller, typically built by
``sources_to_forcing_override()`` in ``engine.py`` from a ``ForgeBlueprint``)
is what drives which inputs get generated; ``grid`` is always generated from
the injected ``grid`` object regardless of ``forcing_override``.

Initialization
~~~~~~~~~~~~~~~~

Input list derivation
^^^^^^^^^^^^^^^^^^^^^^^

During ``__post_init__()``, the class builds ``input_list`` from
``forcing_override`` (not a model spec):

1. **Grid**: Always appended as ``("grid", {})`` -- the grid handler ignores
   kwargs and uses the injected ``grid`` (and, if present,
   ``grid_child``/``metadata_child``) object directly.
2. **Initial conditions**: ``forcing_override["initial_conditions"]``, if
   present -> ``("initial_conditions", kwargs)``.
3. **Forcing**: Iterates over ``forcing_override["forcing"]`` categories.
   ``surface``/``tidal``/``river`` are lists of items ->
   ``("forcing.{category}", kwargs)`` for each item. ``boundary`` is a
   single ``BoundaryForcing``-shaped dict (``source`` + ``bgc_sources`` list,
   mirroring ``initial_conditions``) -> one ``("forcing.boundary", kwargs)``
   entry for the whole section.
4. **CDR forcing**: If either the ``cdr_forcing`` or ``cdr_forcing_file``
   constructor kwarg is set -> ``("cdr_forcing", {"cdr_kwargs": ...,
   "custom_file": ...})``. Neither is set for CDR mode ``"upscaled"`` -- see
   the CDR forcing handler below.

A missing ``forcing_override`` raises ``ValueError`` -- it is required
whenever the blueprint path is used (the resolver always fills it, from the
model default or an authored selection).

Example input list:

.. code-block:: python

   [
       ("grid", {}),
       ("initial_conditions", {"source": {"name": "GLORYS"}, "bgc_sources": [{"source": {"name": "UNIFIED_BGC"}}]}),
       ("forcing.surface", {"source": {"name": "ERA5"}, "type": "physics", ...}),
       ("forcing.surface", {"source": {"name": "UNIFIED"}, "type": "bgc", ...}),
       ("forcing.boundary", {"source": {"name": "GLORYS"}, "bgc_sources": [{"source": {"name": "GLODAP"}}]}),
       ("forcing.tidal", {"source": {"name": "TPXO"}, "ntides": 15}),
       ("forcing.river", {"source": {"name": "DAI"}, "include_bgc": True}),
   ]

Registry validation
^^^^^^^^^^^^^^^^^^^^^

The class validates that every key in ``input_list`` has a registered
handler in ``INPUT_REGISTRY``; a missing handler raises ``ValueError``.

ROMS-MARBL blueprint elements initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``__post_init__`` also creates ``roms_marbl_blueprint_elements``
(``RomsMarblBlueprintInputData``), pre-populating an empty ``Dataset`` for
each planned category so downstream steps only ever append to it:

- **Surface forcing is always required** once any forcing category is
  present -- a missing ``surface`` entry raises ``ValueError``.
- **Boundary forcing is required unless the domain is a nested child**
  (``grid_parent is not None``): a child domain gets its boundary values
  from the parent's ``nesting.nc`` extraction instead, so the resolver
  deliberately emits no boundary items; the class satisfies C-Star's
  ``ForcingConfiguration`` schema with an empty ``Dataset`` in that case
  rather than raising.
- **Initial conditions are required unless the domain is a nested child.** A
  child domain that generates no IC gets a schema-valid *placeholder*
  ``Resource`` instead (C-Star's ``RomsMarblBlueprint`` requires
  ``initial_conditions`` non-empty, and its orchestrator validates the
  emitted blueprint before the runtime ``nest-from`` directive gets a chance
  to replace the placeholder with the parent-derived initial state).
- **CDR forcing is optional.** When CDR mode is ``"upscaled"``, no
  ``cdr_forcing`` generation step is scheduled (see the CDR forcing handler
  below), but a placeholder ``Resource`` is still emitted onto
  ``roms_marbl_blueprint_elements.cdr_forcing`` for the runtime path to
  replace, mirroring the child-IC placeholder.

Settings initialization
^^^^^^^^^^^^^^^^^^^^^^^^^

``_settings_compile_time``/``_settings_run_time`` are bound directly to the
``settings_compile_time``/``settings_run_time`` constructor args (no copy) --
in the normal ``ForgeExecutor`` path these ARE the executor's own live
settings dicts, so generation steps mutate the executor's dicts in place and
there is no merge-back step. When either arg is omitted (standalone/test
use), a fresh empty dict is created instead.

- ``_settings_compile_time``: ``cppdefs`` only, populated by generation
  steps (open boundary flags, ``sal_restore``, ``co2_tvarying``,
  ``cdr_forcing``).
- ``_settings_run_time``: populated per-section (a flat dict of sections:
  ``grid``, ``param``, ``s_coord``, ``initial``, ``forcing``,
  ``extract_data``, ``bgc``, ``blk_frc``, ...).

``generate_all()``
~~~~~~~~~~~~~~~~~~~~

Main entry point for generating all input files. Returns
``roms_marbl_blueprint_elements: RomsMarblBlueprintInputData`` (or ``None``
if the input directory is non-empty and ``clobber`` is ``False``). Settings
are NOT returned: they are the executor-owned
``_settings_compile_time``/``_settings_run_time`` dicts, mutated in place --
the caller already holds the up-to-date dicts through its own reference.

``only`` restricts generation to a subset of canonical ``INPUT_REGISTRY``
keys (see ``resolve_input_selection()``, which maps user-facing aliases like
``"ic"``/``"bry"``/``"tides"`` onto them); the ``grid`` step always runs
regardless, since every other step depends on the in-memory grid object.

Process:

1. **Clobber check**: With ``clobber=False``, existing ``.nc`` files in
   ``input_data_dir`` are left in place (an informational count is printed)
   and reused per-step per the planned-output list; with ``clobber=True``,
   all existing ``.nc`` files are deleted first
   (``_ensure_empty_or_clobber()``).
2. **Build step list**: Creates a list of ``(step, kwargs)`` tuples from
   ``input_list``, sorted by order.
3. **Plan outputs**: Computes the planned NetCDF outputs for the run up
   front (``_planned_netcdf_outputs``) and records which already exist on
   disk, so each step can decide whether to reuse an existing file instead
   of regenerating it.
4. **Dask/thread guards**: When ``use_dask`` is True, caps dask's worker
   count (``dask_num_workers``) and pins BLAS/OpenMP to 1 thread for the
   duration of the loop, to avoid thread oversubscription on high-core HPC
   nodes.
5. **Execute handlers**: For each step (skipping boundary forcing when all
   open boundaries are disabled, and skipping any step not in ``only`` when
   ``only`` is given), calls the handler with ``key`` and ``kwargs``.
6. **Partitioning**: Optionally partitions files across tiles if
   ``partition_files=True``.
7. **Return**: Returns ``roms_marbl_blueprint_elements`` (settings dicts are
   mutated in place, not returned -- see above).

Registered input handlers
----------------------------

Each handler is registered via ``@register_input(name=..., order=...,
label=...)`` on a method ``def _generate_<x>(self, key: str = "<name>",
**kwargs)``; ``kwargs`` are the input's entry from ``input_list``. A
handler's side effects are: write NetCDF file(s) to ``input_data_dir``,
write a YAML metadata sidecar to ``roms_marbl_blueprint_dir``, append
``Resource`` object(s) to the matching
``roms_marbl_blueprint_elements`` field, and update
``_settings_compile_time``/``_settings_run_time``.

Grid (``grid``, order=10)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: Grid NetCDF file (``{domain_name}_grid.nc``) and YAML
metadata. If ``grid_child`` is set (nesting): also a child grid NetCDF and a
nesting-info NetCDF (``{domain_name}_nesting.nc``, built via
``rt.make_nesting_info()``, with ``include_bgc=True`` passed when the model
has MARBL/BGC compiled in).

**Settings**: Compile-time open boundary flags
(``obc_west``/``obc_east``/``obc_north``/``obc_south``); run-time ``grid``
(``grid_file``), ``param`` (grid dimensions and partitioning -- run-time, not
compile-time, lowercase keys: ``llm``, ``mmm``, ``n``, ``np_xi``, ``np_eta``),
``s_coord`` (``tcline``, ``theta_b``, ``theta_s``); and, only with a child
grid, ``extract_data`` (``do_extract``, ``extract_file``, ``n_chd``,
``theta_s_chd``, ``theta_b_chd``, ``hc_chd``).

Initial conditions (``initial_conditions``, order=20)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: Initial conditions NetCDF (``{domain_name}_initial_conditions.nc``)
and YAML metadata.

**Source resolution**: ``source`` and optional ``bgc_sources`` (zero or more
``BgcSourceItem``-shaped entries), resolved via
``_resolve_source_block()``/``_resolve_bgc_sources_list()`` (see `Source
resolution`_ below). Per-day source file lists are trimmed to
``[start_date, start_date + 1 day]`` (roms-tools only needs the day-of and
next-day files for ``ini_time``) via ``filter_paths_by_time_window()``.

**Settings**: Run-time ``initial`` (``initial_file`` -- first file in list;
there is no ``nrrec`` key here, that is a template default, not something
this handler sets).

Surface forcing (``forcing.surface``, order=30)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: One NetCDF per item in ``forcing.surface``:
``{domain_name}_surface-{type}_YYYYMM.nc`` for physics/restoring; bgc items
instead get ``surface-bgc-{source suffix}_YYYYMM.nc`` (e.g.
``surface-bgc-unified.nc``, ``surface-bgc-mbl_co2.nc``), disambiguated by
source name (+ ``use_vars`` when the same source is split across multiple
items -- see ``_forcing_detail_suffix``/``_bgc_output_suffix``). Requires
``type``: ``"physics"``, ``"bgc"``, or ``"restoring"``.

**Source resolution**: ``source``, resolved via ``_resolve_source_block()``.

**Settings**: Compile-time ``sal_restore = True`` when ``type == "restoring"``
and ``"sss"`` is in ``restoring_forces``; ``co2_tvarying = True`` when
``type == "bgc"`` and ``source.name == "MBL_co2"``. Run-time ``interp_frc``
(1 if the coarse grid was used, else 0) on ``blk_frc`` for physics/restoring,
on ``bgc`` for bgc (only when the model has MARBL/BGC compiled in) -- a
mismatch between forcing types raises ``ValueError``. Run-time ``forcing``:
each physics/restoring item sets (overwrites) ``surface_forcing_path``; each
bgc item *appends* to the ``surface_forcing_bgc_path`` list, so every bgc
surface file reaches ROMS's ``frcfiles``.

Boundary forcing (``forcing.boundary``, order=40)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: One boundary physics NetCDF
(``{domain_name}_boundary-physics_YYYYMM.nc``) and one boundary bgc NetCDF
per ``bgc_sources`` entry (``{domain_name}_boundary-bgc-{source suffix}_YYYYMM.nc``,
e.g. ``boundary-bgc-glodap.nc``), disambiguated the same way as surface bgc
files. Unlike initial conditions (which merge every bgc source into one
file), boundary bgc sources are never merged -- ROMS's ``frcfiles`` namelist
key accepts a list, so each source keeps its own file.

**Key features**: The ``forcing.boundary`` kwargs are a single
``BoundaryForcing``-shaped dict (structurally mirroring
``initial_conditions``), not a list of type-discriminated items: ``source``
(physics) + zero or more ``bgc_sources`` entries (each ``{source, use_vars,
bgc_interpolation_method, serialize_dask}``, see
``forge.blueprint.BgcSourceItem``). One ``rt.BoundaryForcing`` call builds
the physics object plus one bgc companion per ``bgc_sources`` entry
internally (via ``physics_forcing=``, reusing the physics object's
temp/salt), completing them together via
``BGCMarbl().process_bgc_fields()`` -- an all-or-nothing unit; reuse of
existing output requires the physics file AND every bgc file to already
exist. Skipped entirely for child/nested domains (``grid_parent is not
None``) -- a child domain's boundaries come from the parent's data
extraction (``nesting.nc``), not from reanalysis. When a bgc source's
(effective) ``bgc_interpolation_method`` is ``"density"``/``"density_mld"``,
the section's own physics object already anchors the density-space
interpolation (all bgc sources share it via ``physics_forcing=``), so no
separate companion-building step is needed.

**Source resolution**: ``source`` and ``bgc_sources``, via
``_resolve_source_block()``/``_resolve_bgc_sources_list()``.

**Settings**: Run-time ``forcing``: physics sets (overwrites)
``boundary_forcing_path``; each bgc source *appends* to the
``boundary_forcing_bgc_path`` list, so every boundary bgc file reaches
ROMS's ``frcfiles``. Compile-time settings are not populated by this
handler.

Tidal forcing (``forcing.tidal``, order=50)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: ``{domain_name}_tidal.nc`` and YAML metadata.

**Key features**: Uses ``ntides`` and other parameters from kwargs;
``model_reference_date`` when configured. On reuse (existing NetCDF + YAML
sidecar), reads ``ntides`` back out of the roms-tools multi-document YAML
sidecar instead of reconstructing ``TidalForcing``.

**Source resolution**: ``source`` (typically TPXO), via
``_resolve_source_block()``.

**Settings**: Run-time ``tides``: only the actually-generated
tidal-constituent count (``ntides``). ``bry_tides``/``pot_tides``/``ana_tides``
are **not** set here -- those are static booleans owned by the
resolver/model settings (so a child grid's ``bry_tides=False`` override
isn't clobbered by this handler). Run-time ``forcing``:
``tidal_forcing_path``.

River forcing (``forcing.river``, order=60)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: ``{domain_name}_river.nc`` and YAML metadata.

**Key features**: Passes ``include_bgc`` and other kwargs through to
``rt.RiverForcing``; extracts the number of rivers from the generated
dataset. If ``rt.RiverForcing`` raises the roms-tools ``ValueError`` whose
message contains *no relevant rivers found* (no river mouths survive the
domain filters), the step logs at INFO, clears
``roms_marbl_blueprint_elements.forcing.river``, and returns; any other
``ValueError`` propagates. If the domain simply has no rivers
(``river.ds.sizes["nriver"] == 0``), also clears
``roms_marbl_blueprint_elements.forcing.river`` without treating it as an
error.

**Source resolution**: ``source`` (typically DAI), via
``_resolve_source_block()``.

**Settings**: Run-time ``river_frc`` (note: run-time, not compile-time,
despite the section name): ``river_source``, ``analytical``, ``nriv`` (from
the generated dataset), ``rvol_vname``/``rvol_tname``,
``rtrc_vname``/``rtrc_tname``. Run-time ``forcing``: ``river_path``.

CDR forcing (``cdr_forcing``, order=80)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Generates**: ``{domain_name}_cdr.nc`` (the basename must contain the
literal substring ``cdr.nc`` so C-Star's ROMS build check on ``cdr_frc.opt``
passes, see ``CDR_FORCING_NETCDF_STEM``) and YAML metadata.

**Key features**: Optional: only appears in ``input_list`` when
``cdr_forcing`` or ``cdr_forcing_file`` is set (CDR mode ``"upscaled"`` sets
neither -- see `ROMS-MARBL blueprint elements initialization`_ above); the
handler itself also no-ops if the merged kwargs are empty. ``cdr_kwargs`` is
merged via ``_build_input_args()`` and passed to ``rt.CDRForcing(**input_args)``.
Output paths are normalized to absolute strings.

**Settings**: Compile-time ``cdr_forcing = True``. Run-time ``cdr_frc``:
``cdr_file="cdr.nc"`` (the executor/blueprint symlinks to the real path),
``cdr_source=True``, ``ncdr_parm=len(cdr.releases)``,
``forcing_parameterized=True``, ``cdr_volume=(cdr.releases.release_type ==
"volume")``. Run-time ``cdr_output``: ``do_cdr_output = True``. Does NOT
touch ``cdr_tracer_output``/``cdr_gas_exch_output`` (ucla-roms >= 0.7.0) --
unlike ``cdr_output``, those two streams are never forced on by CDR forcing;
a user enables them explicitly (see the OutputSpec).

Corrections forcing (``forcing.corrections``, order=90)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Status**: Registered but unwired -- the resolver never emits a
``corrections`` category, so the step never enters ``input_list`` in
production; the handler raises ``NotImplementedError``.

Source resolution
--------------------

``_resolve_source_block()`` normalizes a ``source``/``bgc_source`` block (a
plain string or a dict) and injects a resolved file ``path``:

1. Normalize to dict: a string becomes ``{"name": str}``.
2. Extract ``name`` (raises if a dict block has no ``name``).
3. Check streamability: ``SourceDatasets.streamable_for_source(name,
   glorys_layout=...)`` -- if streamable (e.g. ERA5), no path is added
   unless one was explicitly provided.
4. Otherwise get the path: ``SourceDatasets.path_for_source(name,
   glorys_layout=...)``.
5. **Either** time-window trim (when a time window is given and the path is
   a multi-file list, via ``filter_paths_by_time_window()``) **or**
   subchunking (when ``subchunk`` is on and the source is a multi-file
   GLORYS path: the path is replaced with a memoized kerchunk-subchunked
   reference, ``_subchunked_glorys_path()``) -- never both. A trimmed list
   deliberately skips the subchunk branch so the memoized reference is only
   ever built from the full file list.

Examples:

.. code-block:: python

   "GLORYS" -> {"name": "GLORYS", "path": Path("/path/to/GLORYS_REGIONAL_file.nc")}
   {"name": "UNIFIED", "climatology": True} -> {"name": "UNIFIED", "climatology": True, "path": Path("/path/to/UNIFIED_BGC_file.nc")}
   "ERA5" -> {"name": "ERA5"}  # No path (streamable)

``_build_input_args()`` merges per-input defaults with runtime overrides:
``base_kwargs`` (always supplied from an ``input_list`` entry -- there is no
model-spec fallback) is resolved through ``_resolve_source_block()`` for its
``source``/``bgc_source``/``surface_forcing_source`` fields; an optional
``options`` passthrough dict is popped out and forwarded verbatim to the
roms-tools constructor; the result is merged as ``cfg`` (base kwargs) <
``item_options`` < ``extra`` (runtime injections like dates always win). If
subchunking swapped a source path for a kerchunk reference and the merged
args don't already specify ``chunks``, ``chunks={}`` is set so xarray/dask
honors the reference's native layout.

``SourceDatasets.dataset_key_for_source()`` is used elsewhere (subchunk-
reference memoization), not inside this method.

ROMS-MARBL blueprint element updates
----------------------------------------

Each handler appends a ``Resource`` (``location=str(out_path),
partitioned=False``, from ``cstar.orchestration.models``) to the matching
``roms_marbl_blueprint_elements`` field: ``grid.data``,
``initial_conditions.data``, ``forcing.{surface,boundary,tidal,river}.data``,
or ``cdr_forcing.data``.

File partitioning
--------------------

``_partition_files()`` partitions whole-field input files across tiles,
using ``rt.partition_netcdf()`` for each ``Resource.location``, with
``np_eta``/``np_xi`` from ``self.partitioning``, ``output_dir=input_data_dir``,
and ``include_coarse_dims`` (set during surface forcing generation). The
original whole-field files are left unchanged; partitioned files are created
in ``input_data_dir``; ``roms_marbl_blueprint_elements`` is updated with
partitioned ``Resource`` objects, and each one's ``partitioned`` flag is set
to ``True``.

File outputs
---------------

All input files are written to::

   {input_data_dir}/{domain_name}_{input_name}.nc

``domain_name`` has any ``.`` replaced with ``_`` (via
``netcdf_filename_component()``), since generated NetCDF basenames must not
contain a ``.`` except the final ``.nc`` suffix. For example, a domain named
``cson_roms-marbl_v0.1_test-tiny`` produces:

- ``cson_roms-marbl_v0_1_test-tiny_grid.nc``
- ``cson_roms-marbl_v0_1_test-tiny_initial_conditions.nc``
- ``cson_roms-marbl_v0_1_test-tiny_surface-physics_201201.nc``
- ``cson_roms-marbl_v0_1_test-tiny_boundary-physics_201201.nc``

Return value
---------------

``generate_all()`` returns just ``roms_marbl_blueprint_elements:
RomsMarblBlueprintInputData`` (or ``None``; see ``generate_all()`` above).
The settings dicts are not returned -- they are the executor-owned
``_settings_compile_time``/``_settings_run_time``, mutated in place by
generation:

- ``roms_marbl_blueprint_elements``: Merged into the in-memory
  ``RomsMarblBlueprint`` by ``generate_inputs()``; persisted in
  ``configure_build()``.
- ``_settings_compile_time``: Merged with template defaults, used to render
  ``cppdefs.opt``.
- ``_settings_run_time``: Merged with template defaults, used to write
  ``namelist.nml`` (via ``write_roms_namelist``).

``ForgeExecutor.generate_inputs()`` (``cstar/applications/forge/executor.py``)
passes its own ``self._settings_compile_time``/``self._settings_run_time``
in by reference, so it already holds the up-to-date settings after
``generate_all()`` returns -- no merge-back step.

Usage pattern
----------------

Direct construction is for developers; normal usage goes through the forge
application (``cstar forge run``, see the note at the top of this page).
Construct ``RomsMarblInputData`` with the injected ``grid``/``boundaries``/
``source_data``/``partitioning``/``roms_marbl_blueprint_dir`` and a
``forcing_override`` (typically produced by ``sources_to_forcing_override()``
from a ``ForgeBlueprint``); omit ``settings_compile_time``/``settings_run_time``
for standalone use, since they default to fresh empty dicts. Then call
``generate_all(clobber=..., partition_files=..., test=...)``.

Integration with ``ForgeExecutor``
--------------------------------------

``RomsMarblInputData`` is used internally by
``ForgeExecutor.generate_inputs()`` (``cstar/applications/forge/executor.py``;
see :doc:`forge_internals` for the architecture), called in turn by
``process_forge_blueprint()`` (``cstar/applications/forge/engine.py``), which
is what ``cstar forge run`` invokes:

1. Creates ``RomsMarblInputData``, passing ``self.forcing_override``, the
   other executor-resolved fields, and
   ``self._settings_compile_time``/``self._settings_run_time`` BY REFERENCE
   (as ``settings_compile_time=``/``settings_run_time=``) and
   ``has_bgc=self._has_bgc``.
2. Calls ``generate_all()`` to create input files -- generation steps mutate
   the executor's settings dicts in place; the executor already holds the
   up-to-date values through its own reference, so there is no merge-back
   step.
3. Updates the in-memory blueprint with ``roms_marbl_blueprint_elements``.
4. Persists blueprint and settings to disk (in ``configure_build()``).

This completes the ``generate_inputs`` stage; the blueprint and settings are
persisted in ``configure_build()``.
