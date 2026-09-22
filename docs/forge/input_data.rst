.. _forge-input-data:

Input data generation
======================

.. note::

   **This subsystem is driven by the forge application** (``cstar forge run
   <forge_blueprint.yaml>``, or equivalently ``cstar blueprint run``), which
   loads a ``ForgeBlueprint`` and calls ``ForgeExecutor.generate_inputs()``
   (``cstar/applications/forge/executor.py``). That method constructs a
   ``RomsMarblInputData`` instance and calls ``generate_all()`` on it.
   Constructing ``RomsMarblInputData`` directly (as shown later on this page)
   is for developers debugging or extending input generation -- normal usage
   goes through the forge application.

The ``input_data`` module (``cstar/applications/forge/input_data.py``)
provides classes and utilities for generating input data files for ocean
models. It uses a **registry-based framework** similar to the
``source_data`` module (see :doc:`source_data`), allowing extensible input
generation through decorator-based registration.

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

Abstract dataclass defining the interface for input data generation:

.. code-block:: python

   @dataclass
   class InputData:
       domain_name: str
       start_date: Any
       end_date: Any
       input_data_dir: Path = field(kw_only=True)  # Output directory, injected by the caller

       def generate_all(self):
           """Generate all input files. Must be implemented by subclasses."""
           raise NotImplementedError

``input_data_dir`` is injected by the caller (the executor) rather than
derived from ``cstar.applications.forge.config`` -- this keeps the class
host-independent.

Key features:

- Manages output directory (``input_data_dir``), creating it in
  ``__post_init__``.
- Provides filename construction helpers (``_forcing_filename``).
- Handles clobber logic for existing files (``_ensure_empty_or_clobber``).

Registry system
-----------------

Input generation steps are registered using the ``@register_input``
decorator:

.. code-block:: python

   @register_input(name="grid", order=10, label="Writing ROMS grid")
   def _generate_grid(self, key: str = "grid", **kwargs):
       """Generate grid input file."""
       # Implementation...

Registry components:

- ``INPUT_REGISTRY``: Dictionary mapping input keys to ``InputStep``
  instances.
- ``InputStep``: Container for handler function, order, and label.
- ``@register_input``: Decorator to register handler functions.

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

``RomsMarblInputData`` is a dataclass (subclass of ``InputData``, both
defined in ``cstar/applications/forge/input_data.py``) that implements
ROMS-MARBL specific input data generation. It handles the creation of all
input files required for a ROMS simulation, including grid, initial
conditions, and all types of forcing data.

Class definition
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   @dataclass
   class RomsMarblInputData(InputData):
       """ROMS-MARBL specific input data generation."""

       # Inherited from InputData: domain_name, start_date, end_date, input_data_dir (kw_only)

       grid: rt.Grid
       boundaries: OpenBoundaries
       source_data: source_datasets.SourceDatasets
       roms_marbl_blueprint_dir: Path
       partitioning: cstar_models.PartitioningParameterSet
       cdr_forcing: dict | None = None
       forcing_override: dict[str, Any] | None = None
       model_reference_date: datetime | None = None
       grid_parent: rt.Grid | None = None
       grid_child: rt.Grid | None = None
       metadata_child: dict[str, Any] | None = None
       settings_compile_time: dict[str, Any] | None = None  # executor-owned, bound by reference
       settings_run_time: dict[str, Any] | None = None  # executor-owned, bound by reference
       use_dask: bool = True
       dask_num_workers: int = 8
       use_pio: bool = False
       subchunk: bool = True
       verbose: bool = False
       has_bgc: bool = False  # mirrors ForgeExecutor._has_bgc (cppdefs.marbl)

       roms_marbl_blueprint_elements: RomsMarblBlueprintInputData  # Auto-initialized
       _settings_compile_time: dict  # bound to `settings_compile_time`, or {} if not given
       _settings_run_time: dict  # bound to `settings_run_time`, or {} if not given
       include_coarse_dims: bool | None = None  # Set during surface forcing generation

There is no ``model_spec`` field -- the class is host-/model-spec-independent.
``forcing_override`` (injected by the caller, typically built by
``sources_to_forcing_override()`` in ``engine.py`` from a ``ForgeBlueprint``)
is what drives which inputs get generated; ``grid`` is always generated from
the injected ``grid`` object regardless of ``forcing_override``. See
``cstar/applications/forge/input_data.py`` for the full field list, including
private bookkeeping fields (``_subchunk_refs``, ``_clobber``,
``_existing_planned_outputs``, ``_planned_output_paths``) not listed above.

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
4. **CDR forcing**: If the ``cdr_forcing`` constructor kwarg is set ->
   ``("cdr_forcing", {"cdr_kwargs": self.cdr_forcing})``.

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

The class validates that all keys in ``input_list`` have registered handlers
in ``INPUT_REGISTRY``. Missing handlers raise a ``ValueError``.

ROMS-MARBL blueprint elements initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates ``RomsMarblBlueprintInputData`` instance with empty datasets:

- ``grid``: Empty dataset if "grid" in input_list.
- ``initial_conditions``: Empty dataset if "initial_conditions" in
  input_list.
- ``forcing``: ForcingConfiguration with datasets for each category
  (boundary, surface, tidal, river).
- ``cdr_forcing``: Empty dataset if "cdr_forcing" in input_list.

Validation:

- Requires "boundary" forcing if any forcing is specified.
- Requires "surface" forcing if any forcing is specified.

Settings initialization
^^^^^^^^^^^^^^^^^^^^^^^^^

``_settings_compile_time``/``_settings_run_time`` are bound directly to the
``settings_compile_time``/``settings_run_time`` constructor args (no copy) --
in the normal ``ForgeExecutor`` path these ARE the executor's own live
settings dicts, so generation steps mutate the executor's dicts in place and
there is no merge-back step. When either arg is omitted (standalone/test
use), a fresh empty dict ``{}`` is created instead.

- ``_settings_compile_time``: ``cppdefs`` only, populated by generation
  steps (open boundary flags, ``sal_restore``, ``co2_tvarying``,
  ``cdr_forcing``).
- ``_settings_run_time``: populated per-section (a flat dict of sections:
  ``grid``, ``param``, ``s_coord``, ``initial``, ``forcing``,
  ``extract_data``, ``bgc``, ``blk_frc``, ...).

Registry framework
~~~~~~~~~~~~~~~~~~~~

Input registry
^^^^^^^^^^^^^^^^

The ``INPUT_REGISTRY`` dictionary maps input keys to ``InputStep``
instances:

.. code-block:: python

   INPUT_REGISTRY: Dict[str, InputStep] = {
       "grid": InputStep(name="grid", order=10, label="Writing ROMS grid", handler=_generate_grid),
       "initial_conditions": InputStep(name="initial_conditions", order=20, label="Generating initial conditions", handler=_generate_initial_conditions),
       "forcing.surface": InputStep(name="forcing.surface", order=30, label="Generating surface forcing", handler=_generate_surface_forcing),
       "forcing.boundary": InputStep(name="forcing.boundary", order=40, label="Generating boundary forcing", handler=_generate_boundary_forcing),
       "forcing.tidal": InputStep(name="forcing.tidal", order=50, label="Generating tidal forcing", handler=_generate_tidal_forcing),
       "forcing.river": InputStep(name="forcing.river", order=60, label="Generating river forcing", handler=_generate_river_forcing),
       "cdr_forcing": InputStep(name="cdr_forcing", order=80, label="Generating CDR forcing", handler=_generate_cdr_forcing),
       "forcing.corrections": InputStep(name="forcing.corrections", order=90, label="Generating corrections forcing", handler=_generate_corrections),
   }

Registration decorator
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   @register_input(name: str, order: int, label: str | None = None)

Parameters:

- ``name``: Input key (e.g., "grid", "forcing.surface").
- ``order``: Execution order (lower numbers run first).
- ``label``: Human-readable label for progress messages.

Example:

.. code-block:: python

   @register_input(name="forcing.surface", order=30, label="Generating surface forcing")
   def _generate_surface_forcing(self, key: str = "forcing.surface", **kwargs):
       """Generate surface forcing input files."""
       # Implementation...

``generate_all()``
~~~~~~~~~~~~~~~~~~~~

Main entry point for generating all input files:

.. code-block:: python

   def generate_all(
       self,
       clobber: bool = False,
       partition_files: bool = False,
       test: bool = False,
       only: set[str] | None = None,
   ) -> RomsMarblBlueprintInputData | None:
       """
       Generate all ROMS input files.

       Returns
       -------
       RomsMarblBlueprintInputData | None
           Blueprint subset with generated input file paths, or None if the input
           directory is non-empty and clobber is False. Settings are NOT returned:
           `_settings_compile_time`/`_settings_run_time` are the executor-owned dicts
           passed in via the `settings_compile_time`/`settings_run_time` constructor
           args and mutated in place by generation steps -- the caller already holds
           the up-to-date dicts through its own reference.
       """

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
2. **Build step list**: Creates list of ``(step, kwargs)`` tuples from
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

Handler function signature:

.. code-block:: python

   @register_input(name="input_key", order=ORDER, label="Label")
   def _generate_input(self, key: str = "input_key", **kwargs):
       """
       Generate input file(s) for this input type.

       Parameters
       ----------
       key : str
           Input key (matches registered name)
       **kwargs
           Input-specific arguments from input_list

       Side Effects
       ------------
       - Creates NetCDF file(s) in input_data_dir
       - Creates YAML metadata file in roms_marbl_blueprint_dir
       - Appends Resource(s) to roms_marbl_blueprint_elements
       - Updates _settings_compile_time and/or _settings_run_time
       """

Registered input handlers
----------------------------

Grid (``grid``, order=10)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_grid()``

**Generates**:

- Grid NetCDF file: ``{domain_name}_grid.nc``
- Grid YAML metadata: ``_grid.yaml`` (in ``roms_marbl_blueprint_dir``)
- If ``grid_child`` is set (nesting): also a child grid NetCDF
  (``{domain_name}_grid_child.nc`` + ``_grid_child.yaml``) and a
  nesting-info NetCDF (``{domain_name}_nesting.nc``, built via
  ``rt.make_nesting_info()``, with ``include_bgc=True`` passed when the
  model has MARBL/BGC compiled in).

**Updates ROMS-MARBL blueprint**:

- Appends ``Resource`` to ``roms_marbl_blueprint_elements.grid.data``.
- When nesting is present, also sets
  ``roms_marbl_blueprint_elements.nesting_info``.

**Populates settings**:

- Compile-time (``cppdefs``): Open boundary flags
  (``obc_west``/``obc_east``/``obc_north``/``obc_south``).
- Run-time (``grid``): Grid file path (``self._settings_run_time["grid"] =
  {"grid_file": out_path}``).
- Run-time (``param``): Grid dimensions and partitioning -- note this is
  run-time, not compile-time, and the keys are lowercase (``llm``, ``mmm``,
  ``n``, ``np_xi``, ``np_eta``).
- Run-time (``s_coord``): Vertical stretching parameters (``tcline``,
  ``theta_b``, ``theta_s``).
- Run-time (``extract_data``): Only when a child grid is present (nesting) --
  ``do_extract``, ``extract_file``, ``n_chd``, ``theta_s_chd``,
  ``theta_b_chd``, ``hc_chd``.

Initial conditions (``initial_conditions``, order=20)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_initial_conditions()``

**Generates**: Initial conditions NetCDF file(s)
(``{domain_name}_initial_conditions.nc``) and YAML metadata
(``_initial_conditions.yaml``).

**Source resolution**: Uses ``source`` and optional ``bgc_sources`` (zero or
more ``BgcSourceItem``-shaped entries) from kwargs; resolves paths via
``_resolve_source_block()``/``_resolve_bgc_sources_list()`` ->
``SourceDatasets.path_for_source()``. Per-day source file lists are trimmed
to ``[start_date, start_date + 1 day]`` (roms-tools only needs the day-of
and next-day files for ``ini_time``) -- see ``filter_paths_by_time_window()``.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.initial_conditions.data``.

**Populates settings**: Run-time (``initial``): Initial conditions file path
(``self._settings_run_time["initial"] = dict(initial_file=paths[0])`` --
first file in list; there is no ``nrrec`` key here, that would be a
template default, not something this handler sets).

Surface forcing (``forcing.surface``, order=30)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_surface_forcing()``

**Generates**: Surface forcing NetCDF file(s):
``{domain_name}_surface-{type}_YYYYMM.nc`` for physics/restoring; bgc items
instead get ``surface-bgc-{source suffix}_YYYYMM.nc`` (e.g.
``surface-bgc-unified.nc``, ``surface-bgc-mbl_co2.nc``) -- one file per bgc
surface item, disambiguated by source name (+ ``use_vars`` when the same
source is split across multiple items). This replaced the old ad hoc
``surface-bgc-co2`` special case, which left every other bgc source
undisambiguated (see ``_forcing_detail_suffix``/``_bgc_output_suffix``).
Also writes surface forcing YAML metadata:
``_forcing.surface-{type}.yaml`` (or ``-bgc-{source suffix}`` for bgc
items).

**Key features**: Supports multiple surface forcing sources (physics, bgc,
and restoring); each item in ``forcing.surface`` list generates a separate
file; requires ``type`` parameter: ``"physics"``, ``"bgc"``, or
``"restoring"``.

**Source resolution**: Uses ``source`` from kwargs; resolves path via
``_resolve_source_block()``.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.forcing.surface.data``.

**Populates settings**:

- Compile-time (``cppdefs``): ``sal_restore = True`` when ``type ==
  "restoring"`` and ``"sss"`` is in ``restoring_forces``; ``co2_tvarying =
  True`` when ``type == "bgc"`` and ``source.name == "MBL_co2"``.
- Run-time (``blk_frc``/``bgc``): ``interp_frc`` (1 if the coarse grid was
  used, else 0) -- set on ``blk_frc`` for physics/restoring, on ``bgc`` for
  bgc (only when the model has MARBL/BGC compiled in); a mismatch between
  forcing types raises ``ValueError``.
- Run-time (``forcing``): Surface forcing file paths.
  ``surface_forcing_bgc_path`` is a *list* -- each bgc surface item appends
  its own path rather than overwriting the last one, so every bgc surface
  file reaches ROMS's ``frcfiles`` (previously a last-write-wins scalar
  silently dropped all but one surface bgc file):

  .. code-block:: python

     if "bgc" in type:
         self._settings_run_time["forcing"].setdefault("surface_forcing_bgc_path", [])
         self._settings_run_time["forcing"]["surface_forcing_bgc_path"].append(paths[0])
     else:  # physics or restoring
         self._settings_run_time["forcing"]["surface_forcing_path"] = paths[0]

Boundary forcing (``forcing.boundary``, order=40)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_boundary_forcing()``

**Generates**: Boundary physics NetCDF file
(``{domain_name}_boundary-physics_YYYYMM.nc``); one boundary bgc NetCDF
file per ``bgc_sources`` entry
(``{domain_name}_boundary-bgc-{source suffix}_YYYYMM.nc``, e.g.
``boundary-bgc-glodap.nc``, ``boundary-bgc-unified_bgc.nc`` --
disambiguated by source name (+ ``use_vars`` when the same source is split
across multiple entries), the same ``_forcing_detail_suffix``/
``_bgc_output_suffix`` mechanism ``_generate_surface_forcing`` uses. Unlike
initial conditions (which merge every bgc source into one file), boundary
bgc sources are never merged -- ROMS's ``frcfiles`` namelist key accepts a
list, so each source keeps its own file. Also writes boundary physics/bgc
YAML metadata: ``_forcing.boundary-physics.yaml`` /
``_forcing.boundary-bgc-{source suffix}.yaml``.

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
exist. Uses ``boundaries`` configuration for open boundary specification.
Skipped entirely for child/nested domains (``grid_parent is not None``) --
a child domain's boundaries come from the parent's data extraction
(``nesting.nc``), not from reanalysis. When a bgc source's (effective)
``bgc_interpolation_method`` is ``"density"``/``"density_mld"``, the
section's own physics object already anchors the density-space
interpolation (all bgc sources share it via ``physics_forcing=``), so no
separate companion-building step is needed.

**Source resolution**: Uses ``source`` and ``bgc_sources`` from kwargs;
resolves paths via
``_resolve_source_block()``/``_resolve_bgc_sources_list()``.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.forcing.boundary.data``.

**Populates settings**: Run-time (``forcing``): Boundary forcing file
paths. ``boundary_forcing_bgc_path`` is a *list* -- each bgc source appends
its own path so every boundary bgc file reaches ROMS's ``frcfiles``
(previously a last-write-wins scalar silently dropped all but one boundary
bgc file):

.. code-block:: python

   if type_ == "bgc":
       self._settings_run_time["forcing"].setdefault("boundary_forcing_bgc_path", [])
       self._settings_run_time["forcing"]["boundary_forcing_bgc_path"].append(path_list[0])
   else:  # physics
       self._settings_run_time["forcing"]["boundary_forcing_path"] = path_list[0]

Compile-time settings are not populated by the boundary handler.

Tidal forcing (``forcing.tidal``, order=50)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_tidal_forcing()``

**Generates**: Tidal forcing NetCDF file(s) (``{domain_name}_tidal.nc``) and
YAML metadata (``_forcing.tidal.yaml``).

**Key features**: Uses ``ntides`` and other parameters from
``forcing_override`` kwargs; uses ``model_reference_date`` when configured.
On reuse (existing NetCDF + YAML sidecar), reads ``ntides`` back out of the
roms-tools multi-document YAML sidecar instead of reconstructing
``TidalForcing``.

**Source resolution**: Uses ``source`` from kwargs (typically TPXO);
resolves path via ``_resolve_source_block()``.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.forcing.tidal.data``.

**Populates settings**:

- Run-time (``tides``): only the actually-generated tidal-constituent count
  (``self._settings_run_time.setdefault("tides", {})["ntides"] =
  tidal.ntides``). ``bry_tides``/``pot_tides``/``ana_tides`` are **not** set
  here -- those are static booleans owned by the resolver/model settings (so
  a child grid's ``bry_tides=False`` override isn't clobbered by this
  handler).
- Run-time (``forcing``): Tidal forcing file path
  (``self._settings_run_time["forcing"]["tidal_forcing_path"] = paths[0]``).

River forcing (``forcing.river``, order=60)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_river_forcing()``

**Generates**: River forcing NetCDF file(s) (``{domain_name}_river.nc``) and
YAML metadata (``_forcing.river.yaml``).

**Key features**: Passes ``include_bgc`` and other kwargs through to
``rt.RiverForcing``; extracts number of rivers from the generated dataset.
If ``rt.RiverForcing`` raises the roms-tools ``ValueError`` whose message
contains *no relevant rivers found* (no river mouths survive the domain
filters), the step logs at INFO, clears
``roms_marbl_blueprint_elements.forcing.river``, and returns; any other
``ValueError`` propagates. If the domain simply has no rivers
(``river.ds.sizes["nriver"] == 0``), also clears
``roms_marbl_blueprint_elements.forcing.river`` without treating it as an
error.

**Source resolution**: Uses ``source`` from kwargs (typically DAI); resolves
path via ``_resolve_source_block()``.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.forcing.river.data``.

**Populates settings**: note this is run-time, not compile-time, despite the
``river_frc`` section name.

- Run-time (``river_frc``): ``river_source``, ``analytical``, ``nriv``
  (from generated dataset), ``rvol_vname``/``rvol_tname``,
  ``rtrc_vname``/``rtrc_tname``.
- Run-time (``forcing``): River forcing file path
  (``self._settings_run_time["forcing"]["river_path"] = paths[0]``).

CDR forcing (``cdr_forcing``, order=80)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_cdr_forcing()``

**Generates**: CDR forcing NetCDF file(s) (``{domain_name}_cdr.nc`` -- the
basename must contain the literal substring ``cdr.nc`` so C-Star's ROMS
build check on ``cdr_frc.opt`` passes, see ``CDR_FORCING_NETCDF_STEM``) and
YAML metadata (``_cdr_forcing.yaml``).

**Key features**: Optional input: only appears in ``input_list`` (as
``("cdr_forcing", {"cdr_kwargs": ...})``) when the ``cdr_forcing``
constructor kwarg is set; the handler itself also no-ops if ``cdr_kwargs``
is empty. ``cdr_kwargs`` is merged via ``_build_input_args()`` and passed to
``rt.CDRForcing(**input_args)``. Output paths are normalized to absolute
strings.

**Updates ROMS-MARBL blueprint**: Appends ``Resource(s)`` to
``roms_marbl_blueprint_elements.cdr_forcing.data``.

**Populates settings**:

- Compile-time (``cppdefs``): ``cdr_forcing = True``.
- Run-time (``cdr_frc``): ``cdr_file="cdr.nc"`` (the executor/blueprint
  symlinks to the real path), ``cdr_source=True``,
  ``ncdr_parm=len(cdr.releases)``, ``forcing_parameterized=True``,
  ``cdr_volume=(cdr.releases.release_type == "volume")``.
- Run-time (``cdr_output``): ``do_cdr_output = True``.
- Does NOT touch ``cdr_tracer_output``/``cdr_gas_exch_output`` (ucla-roms
  >= 0.7.0) -- unlike ``cdr_output``, those two streams are never forced on
  by CDR forcing; a user enables them explicitly (see the OutputSpec).

Corrections forcing (``forcing.corrections``, order=90)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Handler**: ``_generate_corrections()``. **Status**: Registered but
unwired -- the resolver never emits a ``corrections`` category, so the step
never enters ``input_list`` in production; the handler raises
``NotImplementedError``.

Source resolution
--------------------

``_resolve_source_block()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Normalizes source blocks and injects file paths:

.. code-block:: python

   def _resolve_source_block(
       self,
       block: str | dict[str, Any],
       time_window: tuple[datetime, datetime] | None = None,
   ) -> dict[str, Any]:
       """
       Normalize a "source"/"bgc_source" block and inject a 'path' based on SourceDatasets.

       Parameters
       ----------
       block : str or dict
           Source specification (e.g., "GLORYS" or {"name": "GLORYS", "climatology": True})
       time_window : tuple[datetime, datetime], optional
           When given and the resolved path is a per-day file list, trims it to the files
           covering that window (e.g. initial conditions only need `ini_time`'s day).

       Returns
       -------
       dict
           Source block with 'name' and optional 'path' fields
       """

Process:

1. Normalize to dict: If string, convert to ``{"name": str}``.
2. Extract name: Get ``name`` field from dict (raises if a dict block has no
   ``name``).
3. Check streamability:
   ``SourceDatasets.streamable_for_source(name, glorys_layout=...)`` -- if
   streamable (e.g. ERA5), don't add a path unless one was explicitly
   provided.
4. Get path: ``SourceDatasets.path_for_source(name, glorys_layout=...)`` for
   non-streamable sources.
5. **Either** time-window trim (when ``time_window`` is given and the path
   is a multi-file list, via ``filter_paths_by_time_window()``) **or**
   subchunking (when ``subchunk`` is on and the source is a multi-file
   GLORYS path: the path is replaced with a memoized kerchunk-subchunked
   reference, ``_subchunked_glorys_path()``) -- never both. A trimmed list
   deliberately skips the subchunk branch so the memoized reference is only
   ever built from the full file list.
6. Return: Dict with ``name`` and optional ``path``.

``SourceDatasets.dataset_key_for_source()`` is used elsewhere (subchunk-
reference memoization), not inside this method.

Examples:

.. code-block:: python

   # String input
   "GLORYS" -> {"name": "GLORYS", "path": Path("/path/to/GLORYS_REGIONAL_file.nc")}

   # Dict input
   {"name": "UNIFIED", "climatology": True} -> {"name": "UNIFIED", "climatology": True, "path": Path("/path/to/UNIFIED_BGC_file.nc")}

   # Streamable source
   "ERA5" -> {"name": "ERA5"}  # No path (streamable)

``_build_input_args()``
~~~~~~~~~~~~~~~~~~~~~~~~~

Merges default arguments with runtime overrides:

.. code-block:: python

   def _build_input_args(
       self,
       key: str,
       extra: dict[str, Any] | None = None,
       base_kwargs: dict[str, Any] | None = None,
       time_window: tuple[datetime, datetime] | None = None,
   ) -> dict[str, Any]:
       """
       Merge per-input defaults with runtime arguments.

       Uses base_kwargs (always provided from input_list).
       Resolves "source", "bgc_source", and "surface_forcing_source" through
       SourceDatasets.
       Merges with extra, where extra overrides defaults.
       """

Process:

1. Get base config: ``base_kwargs``, always supplied from an ``input_list``
   entry (there is no model-spec fallback).
2. Resolve source blocks: Convert ``source``, ``bgc_source``, and
   ``surface_forcing_source`` Pydantic models to dicts with paths via
   ``_resolve_source_block()``.
3. Unpack ``options``: an optional ``options`` passthrough dict in the item
   config is popped out and forwarded verbatim to the roms-tools
   constructor.
4. Merge: ``cfg`` (base kwargs) < ``item_options`` < ``extra`` (extra always
   wins -- it carries runtime injections like dates).
5. If subchunking swapped a source path for a kerchunk reference and the
   merged args don't already specify ``chunks``, sets ``chunks={}`` so
   xarray/dask honors the reference's native layout.

ROMS-MARBL blueprint element updates
----------------------------------------

Each handler appends ``Resource`` objects to the appropriate
``roms_marbl_blueprint_elements`` field:

.. code-block:: python

   resource = Resource(              # from cstar.orchestration.models
       location=str(out_path),       # path to generated NetCDF file
       partitioned=False,            # set to True after partitioning
   )

- **Grid**: ``roms_marbl_blueprint_elements.grid.data.append(resource)``
- **Initial conditions**:
  ``roms_marbl_blueprint_elements.initial_conditions.data.append(resource)``
- **Forcing categories**:
  ``roms_marbl_blueprint_elements.forcing.{category}.data.append(resource)``
  (``forcing.surface`` -> ``forcing.surface.data``, ``forcing.boundary`` ->
  ``forcing.boundary.data``, ``forcing.tidal`` -> ``forcing.tidal.data``,
  ``forcing.river`` -> ``forcing.river.data``)
- **CDR forcing**:
  ``roms_marbl_blueprint_elements.cdr_forcing.data.append(resource)``

File partitioning
--------------------

``_partition_files()`` partitions whole-field input files across tiles,
using ``rt.partition_netcdf()`` for each ``Resource.location``:

.. code-block:: python

   def _partition_files(self, **kwargs):
       """
       Partition whole input files across tiles using roms_tools.partition_netcdf.

       Uses the paths stored in roms_marbl_blueprint_elements to build the list of whole-field files,
       and records the partitioned paths in the Resource objects.
       """

Partitioning arguments:

.. code-block:: python

   input_args = dict(
       np_eta=self.partitioning.n_procs_y,
       np_xi=self.partitioning.n_procs_x,
       output_dir=self.input_data_dir,
       include_coarse_dims=self.include_coarse_dims,  # set during surface forcing generation
   )

Result: original whole-field files remain unchanged; partitioned files are
created in ``input_data_dir``; ``roms_marbl_blueprint_elements`` is updated
with partitioned ``Resource`` objects; the ``partitioned`` flag is set to
``True``.

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

.. code-block:: python

   from cstar.applications.forge.input_data import RomsMarblInputData

   # Create input data generator -- host-independent: paths and the resolved forcing
   # selection are injected by the caller (ForgeExecutor), not derived from a model_spec.
   input_gen = RomsMarblInputData(
       domain_name="test-tiny",
       start_date=datetime(2012, 1, 1),
       end_date=datetime(2012, 1, 2),
       input_data_dir=input_data_dir,
       grid=grid,
       boundaries=boundaries,
       source_data=source_data,
       roms_marbl_blueprint_dir=roms_marbl_blueprint_dir,
       partitioning=partitioning,
       forcing_override=forcing_override,  # from ForgeBlueprint.forcing via sources_to_forcing_override
   )

   # Generate all inputs. Settings dicts are executor-owned: pass them in (or omit
   # for fresh empty dicts, as here) and they are mutated in place by generation --
   # no return value carries them back.
   roms_marbl_blueprint_elements = input_gen.generate_all(
       clobber=False,
       partition_files=False,
       test=False,
   )

Integration with ``ForgeExecutor``
--------------------------------------

The ``RomsMarblInputData`` class is used internally by
``ForgeExecutor.generate_inputs()`` (``cstar/applications/forge/executor.py``;
see :doc:`internals` for the architecture). That method is in turn called by
``process_forge_blueprint()`` (``cstar/applications/forge/engine.py``), which
is what ``cstar forge run`` invokes:

1. Creates ``RomsMarblInputData`` instance, passing ``self.forcing_override``,
   the other executor-resolved fields, and
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
