# RomsMarblInputData Class Documentation

> **This subsystem is driven by the forge application** (`python -m cstar_forge.run`), which
> calls `ForgeExecutor.generate_inputs()` (`cstar_forge/forge/executor.py`), which constructs a
> `RomsMarblInputData` and calls `generate_all()` on it. Direct construction, as shown in this
> document, is for developers debugging or extending input generation.

## Overview

`RomsMarblInputData` is a dataclass (subclass of `InputData`, both defined in
`cstar_forge/forge/input_data.py`) that implements ROMS-MARBL specific input data generation. It
handles the creation of all input files required for a ROMS simulation, including grid, initial
conditions, and all types of forcing data.

## Class Definition

```python
@dataclass
class RomsMarblInputData(InputData):
    """ROMS-MARBL specific input data generation."""

    # Inherited from InputData: domain_name, start_date, end_date, input_data_dir (kw_only)

    grid: rt.Grid
    boundaries: OpenBoundaries
    source_data: source_data.SourceData
    roms_marbl_blueprint_dir: Path
    partitioning: cstar_models.PartitioningParameterSet
    cdr_forcing: dict | None = None
    forcing_override: dict[str, Any] | None = None
    model_reference_date: datetime | None = None
    grid_parent: rt.Grid | None = None
    grid_child: rt.Grid | None = None
    metadata_child: dict[str, Any] | None = None
    use_dask: bool = True
    dask_num_workers: int = 8
    use_pio: bool = False
    subchunk: bool = True
    verbose: bool = False

    roms_marbl_blueprint_elements: RomsMarblBlueprintInputData  # Auto-initialized
    _settings_compile_time: dict  # Auto-initialized
    _settings_run_time: dict  # Auto-initialized
    include_coarse_dims: bool | None = None  # Set during surface forcing generation
```

There is no `model_spec` field — the class is host-/model-spec-independent. `forcing_override`
(injected by the caller, typically built by `sources_to_forcing_override()` in
`forge_blueprint_engine.py` from a `ForgeBlueprint`) is what drives which inputs get generated;
`grid` is always generated from the injected `grid` object regardless of `forcing_override`. See
`cstar_forge/forge/input_data.py` for the full field list, including private bookkeeping fields
(`_subchunk_refs`, `_clobber`, `_existing_planned_outputs`, `_planned_output_paths`) not listed
above.

## Initialization

### Input List Derivation

During `__post_init__()`, the class builds `input_list` from `forcing_override` (not a model
spec):

1. **Grid**: Always appended as `("grid", {})` — the grid handler ignores kwargs and uses the
   injected `grid` (and, if present, `grid_child`/`metadata_child`) object directly.
2. **Initial Conditions**: `forcing_override["initial_conditions"]`, if present → `("initial_conditions", kwargs)`
3. **Forcing**: Iterates over `forcing_override["forcing"]` categories (surface, boundary, tidal, river) and items within each → `("forcing.{category}", kwargs)` for each item
4. **CDR Forcing**: If the `cdr_forcing` constructor kwarg is set → `("cdr_forcing", {"cdr_kwargs": self.cdr_forcing})`

A missing `forcing_override` raises `ValueError` — it is required whenever the blueprint path is
used (the resolver always fills it, from the model default or an authored selection).

**Example Input List:**
```python
[
    ("grid", {}),
    ("initial_conditions", {"source": {"name": "GLORYS"}, "bgc_source": {...}}),
    ("forcing.surface", {"source": {"name": "ERA5"}, "type": "physics", ...}),
    ("forcing.surface", {"source": {"name": "UNIFIED"}, "type": "bgc", ...}),
    ("forcing.boundary", {"source": {"name": "GLORYS"}, "type": "physics", ...}),
    ("forcing.tidal", {"source": {"name": "TPXO"}, "ntides": 15}),
    ("forcing.river", {"source": {"name": "DAI"}, "include_bgc": True}),
]
```

### Registry Validation

The class validates that all keys in `input_list` have registered handlers in `INPUT_REGISTRY`. Missing handlers raise a `ValueError`.

### ROMS-MARBL Blueprint Elements Initialization

Creates `RomsMarblBlueprintInputData` instance with empty datasets:
- `grid`: Empty dataset if "grid" in input_list
- `initial_conditions`: Empty dataset if "initial_conditions" in input_list
- `forcing`: ForcingConfiguration with datasets for each category (boundary, surface, tidal, river)
- `cdr_forcing`: Empty dataset if "cdr_forcing" in input_list

**Validation:**
- Requires "boundary" forcing if any forcing is specified
- Requires "surface" forcing if any forcing is specified

### Settings Initialization

- `_settings_compile_time`: Empty dictionary `{}`
- `_settings_run_time`: Empty dictionary `{}`, populated per-section (a flat dict of sections: `grid`, `param`, `s_coord`, `initial`, `forcing`, `extract_data`, `bgc`, `blk_frc`, ...)

## Registry Framework

### Input Registry

The `INPUT_REGISTRY` dictionary maps input keys to `InputStep` instances:

```python
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
```

### Registration Decorator

```python
@register_input(name: str, order: int, label: str | None = None)
```

**Parameters:**
- `name`: Input key (e.g., "grid", "forcing.surface")
- `order`: Execution order (lower numbers run first)
- `label`: Human-readable label for progress messages

**Example:**
```python
@register_input(name="forcing.surface", order=30, label="Generating surface forcing")
def _generate_surface_forcing(self, key: str = "forcing.surface", **kwargs):
    """Generate surface forcing input files."""
    # Implementation...
```

## Input Generation Process

### `generate_all()` Method

Main entry point for generating all input files:

```python
def generate_all(
    self,
    clobber: bool = False,
    partition_files: bool = False,
    test: bool = False,
    only: set[str] | None = None,
) -> tuple[RomsMarblBlueprintInputData, dict, dict]:
    """
    Generate all ROMS input files.

    Returns
    -------
    roms_marbl_blueprint_elements: RomsMarblBlueprintInputData
        Blueprint subset with generated input file paths
    compile_time_settings: dict
        Compile-time settings dictionary
    run_time_settings: dict
        Run-time settings dictionary
    """
```

`only` restricts generation to a subset of canonical `INPUT_REGISTRY` keys (see
`resolve_input_selection()`, which maps user-facing aliases like `"ic"`/`"bry"`/`"tides"` onto
them); the `grid` step always runs regardless, since every other step depends on the in-memory
grid object.

**Process:**
1. **Clobber Check**: With `clobber=False`, existing `.nc` files in `input_data_dir` are left in
   place (an informational count is printed) and reused per-step per the planned-output list;
   with `clobber=True`, all existing `.nc` files are deleted first (`_ensure_empty_or_clobber()`)
2. **Build Step List**: Creates list of `(step, kwargs)` tuples from `input_list`, sorted by order
3. **Plan Outputs**: Computes the planned NetCDF outputs for the run up front
   (`_planned_netcdf_outputs`) and records which already exist on disk, so each step can decide
   whether to reuse an existing file instead of regenerating it
4. **Dask/Thread Guards**: When `use_dask` is True, caps dask's worker count
   (`dask_num_workers`) and pins BLAS/OpenMP to 1 thread for the duration of the loop, to avoid
   thread oversubscription on high-core HPC nodes
5. **Execute Handlers**: For each step (skipping boundary forcing when all open boundaries are
   disabled, and skipping any step not in `only` when `only` is given), calls the handler with
   `key` and `kwargs`
6. **Partitioning**: Optionally partitions files across tiles if `partition_files=True`
7. **Return**: Returns `roms_marbl_blueprint_elements` and the settings dictionaries

### Handler Function Signature

All registered handlers follow this pattern:

```python
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
```

## Registered Input Handlers

### Grid (`grid`, order=10)

**Handler**: `_generate_grid()`

**Generates:**
- Grid NetCDF file: `{domain_name}_grid.nc`
- Grid YAML metadata: `_grid.yaml` (in `roms_marbl_blueprint_dir`)
- If `grid_child` is set (nesting): also a child grid NetCDF (`{domain_name}_grid_child.nc` +
  `_grid_child.yaml`) and a nesting-info NetCDF (`{domain_name}_nesting.nc`, built via
  `rt.make_nesting_info()`, with `include_bgc=True` passed when the model has MARBL/BGC compiled in)

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource` to `roms_marbl_blueprint_elements.grid.data`
- When nesting is present, also sets `roms_marbl_blueprint_elements.nesting_info`

**Populates Settings:**
- **Compile-time (`cppdefs`)**: Open boundary flags
  ```python
  self._settings_compile_time["cppdefs"]["obc_west"] = self.boundaries.west
  self._settings_compile_time["cppdefs"]["obc_east"] = self.boundaries.east
  self._settings_compile_time["cppdefs"]["obc_north"] = self.boundaries.north
  self._settings_compile_time["cppdefs"]["obc_south"] = self.boundaries.south
  ```
- **Run-time (`grid`)**: Grid file path
  ```python
  self._settings_run_time["grid"] = {"grid_file": out_path}
  ```
- **Run-time (`param`)**: Grid dimensions and partitioning — note this is run-time, not
  compile-time, and the keys are lowercase
  ```python
  self._settings_run_time["param"]["llm"] = self.grid.nx
  self._settings_run_time["param"]["mmm"] = self.grid.ny
  self._settings_run_time["param"]["n"] = self.grid.N
  self._settings_run_time["param"]["np_xi"] = self.partitioning.n_procs_x
  self._settings_run_time["param"]["np_eta"] = self.partitioning.n_procs_y
  ```
- **Run-time (`s_coord`)**: Vertical stretching parameters
  ```python
  self._settings_run_time["s_coord"] = dict(
      tcline=self.grid.hc, theta_b=self.grid.theta_b, theta_s=self.grid.theta_s,
  )
  ```
- **Run-time (`extract_data`)**: Only when a child grid is present (nesting)
  ```python
  self._settings_run_time["extract_data"] = dict(
      do_extract=True, extract_file="nesting.nc",
      n_chd=self.grid_child.N, theta_s_chd=..., theta_b_chd=..., hc_chd=...,
  )
  ```

### Initial Conditions (`initial_conditions`, order=20)

**Handler**: `_generate_initial_conditions()`

**Generates:**
- Initial conditions NetCDF file(s): `{domain_name}_initial_conditions.nc`
- Initial conditions YAML metadata: `_initial_conditions.yaml`

**Source Resolution:**
- Uses `source` and optional `bgc_source` from kwargs
- Resolves paths via `_resolve_source_block()` → `SourceData.path_for_source()`
- Per-day source file lists are trimmed to `[start_date, start_date + 1 day]` (roms-tools only
  needs the day-of and next-day files for `ini_time`) — see `filter_paths_by_time_window()`

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.initial_conditions.data`

**Populates Settings:**
- **Run-time (`initial`)**: Initial conditions file path (there is no `nrrec` key here — that
  would be a template default, not something this handler sets)
  ```python
  self._settings_run_time["initial"] = dict(initial_file=paths[0])  # First file in list
  ```

### Surface Forcing (`forcing.surface`, order=30)

**Handler**: `_generate_surface_forcing()`

**Generates:**
- Surface forcing NetCDF file(s): `{domain_name}_surface-{type}_YYYYMM.nc` (bgc items sourced
  from `MBL_co2` instead get `surface-{type}-co2` in the stem)
- Surface forcing YAML metadata: `_forcing.surface-{type}.yaml` (or `-{type}-co2` for MBL_co2)

**Key Features:**
- Supports multiple surface forcing sources (physics, bgc, and restoring)
- Each item in `forcing.surface` list generates a separate file
- Requires `type` parameter: `"physics"`, `"bgc"`, or `"restoring"`

**Source Resolution:**
- Uses `source` from kwargs
- Resolves path via `_resolve_source_block()`

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.forcing.surface.data`

**Populates Settings:**
- **Compile-time (`cppdefs`)**:
  - `sal_restore = True` when `type == "restoring"` and `"sss"` is in `restoring_forces`
  - `co2_tvarying = True` when `type == "bgc"` and `source.name == "MBL_co2"`
- **Run-time (`blk_frc`/`bgc`)**: `interp_frc` (1 if the coarse grid was used, else 0) — set on
  `blk_frc` for physics/restoring, on `bgc` for bgc (only when the model has MARBL/BGC compiled
  in); a mismatch between forcing types raises `ValueError`
- **Run-time (`forcing`)**: Surface forcing file paths
  ```python
  if "bgc" in type:
      self._settings_run_time["forcing"]["surface_forcing_bgc_path"] = paths[0]
  else:  # physics or restoring
      self._settings_run_time["forcing"]["surface_forcing_path"] = paths[0]
  ```

### Boundary Forcing (`forcing.boundary`, order=40)

**Handler**: `_generate_boundary_forcing()`

**Generates:**
- Boundary forcing NetCDF file(s): `{domain_name}_boundary-{type}_YYYYMM.nc`
- Boundary forcing YAML metadata: `_forcing.boundary-{type}.yaml`

**Key Features:**
- Supports multiple boundary forcing sources (physics and bgc)
- Each item in `forcing.boundary` list generates a separate file
- Requires `type` parameter: `"physics"` or `"bgc"`
- Uses `boundaries` configuration for open boundary specification
- Skipped entirely for child/nested domains (`grid_parent is not None`) — a child domain's
  boundaries come from the parent's data extraction (`nesting.nc`), not from reanalysis
- When `type == "bgc"` and `bgc_interpolation_method` is `"density"`/`"density_mld"`, builds a
  companion physics `rt.BoundaryForcing` (`_build_physics_boundary_companion()`) and passes it as
  `physics_forcing=` so roms-tools can interpolate in density space instead of depth space; if no
  physics boundary item exists to anchor it, a warning is issued and roms-tools falls back to
  depth-space interpolation

**Source Resolution:**
- Uses `source` from kwargs
- Resolves path via `_resolve_source_block()`

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.forcing.boundary.data`

**Populates Settings:**
- **Run-time (`forcing`)**: Boundary forcing file paths
  ```python
  if type == "bgc":
      self._settings_run_time["forcing"]["boundary_forcing_bgc_path"] = paths[0]
  else:  # physics
      self._settings_run_time["forcing"]["boundary_forcing_path"] = paths[0]
  ```

**Note**: Compile-time settings are not populated by the boundary handler.

### Tidal Forcing (`forcing.tidal`, order=50)

**Handler**: `_generate_tidal_forcing()`

**Generates:**
- Tidal forcing NetCDF file(s): `{domain_name}_tidal.nc`
- Tidal forcing YAML metadata: `_forcing.tidal.yaml`

**Key Features:**
- Uses `ntides` and other parameters from `forcing_override` kwargs
- Uses `model_reference_date` when configured
- On reuse (existing NetCDF + YAML sidecar), reads `ntides` back out of the roms-tools
  multi-document YAML sidecar instead of reconstructing `TidalForcing`

**Source Resolution:**
- Uses `source` from kwargs (typically TPXO)
- Resolves path via `_resolve_source_block()`

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.forcing.tidal.data`

**Populates Settings:**
- **Run-time (`tides`)**: only the actually-generated tidal-constituent count
  ```python
  self._settings_run_time.setdefault("tides", {})["ntides"] = tidal.ntides
  ```
  `bry_tides`/`pot_tides`/`ana_tides` are **not** set here — those are static booleans owned by
  the resolver/model settings (so a child grid's `bry_tides=False` override isn't clobbered by
  this handler)
- **Run-time (`forcing`)**: Tidal forcing file path
  ```python
  self._settings_run_time["forcing"]["tidal_forcing_path"] = paths[0]
  ```

### River Forcing (`forcing.river`, order=60)

**Handler**: `_generate_river_forcing()`

**Generates:**
- River forcing NetCDF file(s): `{domain_name}_river.nc`
- River forcing YAML metadata: `_forcing.river.yaml`

**Key Features:**
- Passes `include_bgc` and other kwargs through to `rt.RiverForcing`
- Extracts number of rivers from the generated dataset
- If `rt.RiverForcing` raises the roms-tools `ValueError` whose message contains *no relevant
  rivers found* (no river mouths survive the domain filters), the step logs at INFO, clears
  `roms_marbl_blueprint_elements.forcing.river`, and returns; any other `ValueError` propagates
- If the domain simply has no rivers (`river.ds.sizes["nriver"] == 0`), also clears
  `roms_marbl_blueprint_elements.forcing.river` without treating it as an error

**Source Resolution:**
- Uses `source` from kwargs (typically DAI)
- Resolves path via `_resolve_source_block()`

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.forcing.river.data`

**Populates Settings:** note this is run-time, not compile-time, despite the `river_frc`
section name
- **Run-time (`river_frc`)**: River forcing configuration
  ```python
  self._settings_run_time["river_frc"] = {
      "river_source": True,
      "analytical": False,
      "nriv": river.ds.sizes["nriver"],  # From generated dataset
      "rvol_vname": "river_volume",
      "rvol_tname": "river_time",
      "rtrc_vname": "river_tracer",
      "rtrc_tname": "river_time",
  }
  ```
- **Run-time (`forcing`)**: River forcing file path
  ```python
  self._settings_run_time["forcing"]["river_path"] = paths[0]
  ```

### CDR Forcing (`cdr_forcing`, order=80)

**Handler**: `_generate_cdr_forcing()`

**Generates:**
- CDR forcing NetCDF file(s): `{domain_name}_cdr.nc` (the basename must contain the literal
  substring `cdr.nc` so C-Star's ROMS build check on `cdr_frc.opt` passes — see
  `CDR_FORCING_NETCDF_STEM`)
- CDR forcing YAML metadata: `_cdr_forcing.yaml`

**Key Features:**
- Optional input: only appears in `input_list` (as `("cdr_forcing", {"cdr_kwargs": ...})`) when
  the `cdr_forcing` constructor kwarg is set; the handler itself also no-ops if `cdr_kwargs` is
  empty
- `cdr_kwargs` is merged via `_build_input_args()` and passed to `rt.CDRForcing(**input_args)`
- Output paths are normalized to absolute strings

**Updates ROMS-MARBL Blueprint:**
- Appends `Resource(s)` to `roms_marbl_blueprint_elements.cdr_forcing.data`

**Populates Settings:**
- **Compile-time (`cppdefs`)**: `cdr_forcing = True`
- **Run-time (`cdr_frc`)**: `cdr_file="cdr.nc"` (the executor/blueprint symlinks to the real
  path), `cdr_source=True`, `ncdr_parm=len(cdr.releases)`, `forcing_parameterized=True`,
  `cdr_volume=(cdr.releases.release_type == "volume")`
- **Run-time (`cdr_output`)**: `do_cdr = True`

### Corrections Forcing (`forcing.corrections`, order=90)

**Handler**: `_generate_corrections()`

**Status**: Registered but unwired — the resolver never emits a `corrections` category, so the step never enters `input_list` in production; the handler raises `NotImplementedError`

## Source Resolution

### `_resolve_source_block()` Method

Normalizes source blocks and injects file paths:

```python
def _resolve_source_block(
    self,
    block: str | dict[str, Any],
    time_window: tuple[datetime, datetime] | None = None,
) -> dict[str, Any]:
    """
    Normalize a "source"/"bgc_source" block and inject a 'path' based on SourceData.

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
```

**Process:**
1. Normalize to dict: If string, convert to `{"name": str}`
2. Extract name: Get `name` field from dict (raises if a dict block has no `name`)
3. Check streamability: `SourceData.streamable_for_source(name, glorys_layout=...)` — if
   streamable (e.g. ERA5), don't add a path unless one was explicitly provided
4. Get path: `SourceData.path_for_source(name, glorys_layout=...)` for non-streamable sources
5. **Either** time-window trim (when `time_window` is given and the path is a multi-file list,
   via `filter_paths_by_time_window()`) **or** subchunking (when `subchunk` is on and the source
   is a multi-file GLORYS path: the path is replaced with a memoized kerchunk-subchunked
   reference, `_subchunked_glorys_path()`) — never both. A trimmed list deliberately skips the
   subchunk branch so the memoized reference is only ever built from the full file list
7. Return: Dict with `name` and optional `path`

`SourceData.dataset_key_for_source()` is used elsewhere (subchunk-reference memoization), not
inside this method.

**Examples:**
```python
# String input
"GLORYS" → {"name": "GLORYS", "path": Path("/path/to/GLORYS_REGIONAL_file.nc")}

# Dict input
{"name": "UNIFIED", "climatology": True} → {"name": "UNIFIED", "climatology": True, "path": Path("/path/to/UNIFIED_BGC_file.nc")}

# Streamable source
"ERA5" → {"name": "ERA5"}  # No path (streamable)
```

### `_build_input_args()` Method

Merges default arguments with runtime overrides:

```python
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
    Resolves "source" and "bgc_source" through SourceData.
    Merges with extra, where extra overrides defaults.
    """
```

**Process:**
1. Get base config: `base_kwargs`, always supplied from an `input_list` entry (there is no
   model-spec fallback)
2. Resolve source blocks: Convert `source` and `bgc_source` Pydantic models to dicts with paths
   via `_resolve_source_block()`
3. Unpack `options`: an optional `options` passthrough dict in the item config is popped out and
   forwarded verbatim to the roms-tools constructor
4. Merge: `cfg` (base kwargs) < `item_options` < `extra` (extra always wins — it carries runtime
   injections like dates)
5. If subchunking swapped a source path for a kerchunk reference and the merged args don't
   already specify `chunks`, sets `chunks={}` so xarray/dask honors the reference's native layout

## Settings Population for Forcing

### Compile-Time Settings

**Surface Forcing:**
- `cppdefs.sal_restore`: set when `type == "restoring"` and `"sss"` is in `restoring_forces`
- `cppdefs.co2_tvarying`: set when `type == "bgc"` and `source.name == "MBL_co2"`

**CDR Forcing:**
- `cppdefs.cdr_forcing`: `True` when CDR forcing is generated

**Boundary Forcing:**
- Compile-time settings are not populated by the boundary handler.

**Tidal/River Forcing:**
- `tides` and `river_frc` are **run-time** sections, not compile-time — see below.

### Run-Time Settings

**Surface Forcing:**
- `forcing.surface_forcing_path`: Path to physics/restoring surface forcing file
- `forcing.surface_forcing_bgc_path`: Path to bgc surface forcing file
- `blk_frc.interp_frc` / `bgc.interp_frc`: 1 if the coarse grid was used, else 0

**Boundary Forcing:**
- `forcing.boundary_forcing_path`: Path to physics boundary forcing file
- `forcing.boundary_forcing_bgc_path`: Path to bgc boundary forcing file

**Tidal Forcing:**
- `tides.ntides`: Number of tidal constituents actually generated (`bry_tides`/`pot_tides`/
  `ana_tides` are owned by the resolver/model settings, not this handler)
- `forcing.tidal_forcing_path`: Path to tidal forcing file

**River Forcing:**
- `river_frc.river_source`: Enable river source flag
- `river_frc.analytical`: Analytical river flag
- `river_frc.nriv`: Number of rivers (from generated dataset)
- `river_frc.rvol_vname`, `river_frc.rvol_tname`: River volume variable/time names
- `river_frc.rtrc_vname`, `river_frc.rtrc_tname`: River tracer variable/time names
- `forcing.river_path`: Path to river forcing file

**CDR Forcing:**
- `cdr_frc.cdr_file`, `cdr_frc.cdr_source`, `cdr_frc.ncdr_parm`, `cdr_frc.forcing_parameterized`,
  `cdr_frc.cdr_volume`
- `cdr_output.do_cdr`: `True` when CDR forcing is generated

## ROMS-MARBL Blueprint Element Updates

Each handler appends `Resource` objects to the appropriate roms_marbl_blueprint element:

**Resource Creation:**
```python
resource = Resource(              # from cstar.orchestration.models
    location=str(out_path),       # path to generated NetCDF file
    partitioned=False,            # set to True after partitioning
)
```

**Blueprint Updates:**
- **Grid**: `roms_marbl_blueprint_elements.grid.data.append(resource)`
- **Initial Conditions**: `roms_marbl_blueprint_elements.initial_conditions.data.append(resource)`
- **Forcing Categories**: `roms_marbl_blueprint_elements.forcing.{category}.data.append(resource)`
  - `forcing.surface` → `forcing.surface.data`
  - `forcing.boundary` → `forcing.boundary.data`
  - `forcing.tidal` → `forcing.tidal.data`
  - `forcing.river` → `forcing.river.data`
- **CDR Forcing**: `roms_marbl_blueprint_elements.cdr_forcing.data.append(resource)`

## File Partitioning

### `_partition_files()` Method

Partitions whole-field input files across tiles:

```python
def _partition_files(self, **kwargs):
    """
    Partition whole input files across tiles using roms_tools.partition_netcdf.
    
    Uses the paths stored in roms_marbl_blueprint_elements to build the list of whole-field files,
    and records the partitioned paths in the Resource objects.
    """
```

**Process:**
1. **Iterate over input_list**: For each input key, get corresponding dataset from `roms_marbl_blueprint_elements`
2. **Partition each Resource**: Call `rt.partition_netcdf()` for each `Resource.location`
3. **Create partitioned Resources**: Replace original resources with partitioned ones
4. **Update partitioned flag**: Set `partitioned=True` on new resources

**Partitioning Arguments:**
```python
input_args = dict(
    np_eta=self.partitioning.n_procs_y,
    np_xi=self.partitioning.n_procs_x,
    output_dir=self.input_data_dir,
    include_coarse_dims=self.include_coarse_dims,  # set during surface forcing generation
)
```

**Result:**
- Original whole-field files remain unchanged
- Partitioned files created in `input_data_dir`
- `roms_marbl_blueprint_elements` updated with partitioned `Resource` objects
- `partitioned` flag set to `True`

## Return Values

`generate_all()` returns a tuple:

```python
(
    roms_marbl_blueprint_elements: RomsMarblBlueprintInputData,
    compile_time_settings: dict,
    run_time_settings: dict
)
```

**Usage:**
- `roms_marbl_blueprint_elements`: Merged into the in-memory `RomsMarblBlueprint` by `generate_inputs()`; persisted in `configure_build()`
- `compile_time_settings`: Merged with template defaults, used to render `cppdefs.opt`
- `run_time_settings`: Merged with template defaults, used to write `namelist.nml` (via `write_roms_namelist`)

These are used by `ForgeExecutor.generate_inputs()` (`cstar_forge/forge/executor.py`) to
update the blueprint and settings before persisting.

