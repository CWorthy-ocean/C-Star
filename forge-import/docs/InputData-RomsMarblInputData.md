# RomsMarblInputData Class Documentation

## Overview

`RomsMarblInputData` is a dataclass that implements ROMS-MARBL specific input data generation. It handles the creation of all input files required for a ROMS simulation, including grid, initial conditions, and all types of forcing data.

## Class Definition

```python
@dataclass
class RomsMarblInputData(InputData):
    """ROMS-MARBL specific input data generation."""
    
    model_spec: cson_models.ModelSpec
    grid: rt.Grid
    boundaries: cson_models.OpenBoundaries
    source_data: source_data.SourceData
    blueprint_dir: Path
    partitioning: cstar_models.PartitioningParameterSet
    use_dask: bool = True
    
    blueprint_elements: RomsMarblBlueprintInputData  # Auto-initialized
    _settings_compile_time: dict  # Auto-initialized
    _settings_run_time: dict  # Auto-initialized
```

## Initialization

### Input List Derivation

During `__post_init__()`, the class builds `input_list` from `model_spec.inputs`:

1. **Grid**: Extracts grid specifications → `("grid", kwargs)`
2. **Initial Conditions**: Extracts initial conditions specs → `("initial_conditions", kwargs)`
3. **Forcing**: Iterates over forcing categories (surface, boundary, tidal, river) and items within each → `("forcing.{category}", kwargs)` for each item

**Example Input List:**
```python
[
    ("grid", {"topography_source": "ETOPO5"}),
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

### Blueprint Elements Initialization

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
- `_settings_run_time`: Dictionary with `{"roms.in": {}}`

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
    test: bool = False
) -> Tuple[RomsMarblBlueprintInputData, dict, dict]:
    """
    Generate all ROMS input files.
    
    Returns
    -------
    blueprint_elements: RomsMarblBlueprintInputData
        Blueprint subset with generated input file paths
    compile_time_settings: dict
        Compile-time settings dictionary
    run_time_settings: dict
        Run-time settings dictionary
    """
```

**Process:**
1. **Clobber Check**: Ensures output directory is empty or removes existing files if `clobber=True`
2. **Build Step List**: Creates list of `(step, kwargs)` tuples from `input_list`, sorted by order
3. **Execute Handlers**: Calls each handler with `key` and `kwargs`
4. **Partitioning**: Optionally partitions files across tiles if `partition_files=True`
5. **Return**: Returns `blueprint_elements` and settings dictionaries

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
    - Creates YAML metadata file in blueprint_dir
    - Appends Resource(s) to blueprint_elements
    - Updates _settings_compile_time and/or _settings_run_time
    """
```

## Registered Input Handlers

### Grid (`grid`, order=10)

**Handler**: `_generate_grid()`

**Generates:**
- Grid NetCDF file: `{model_name}_{grid_name}_grid.nc`
- Grid YAML metadata: `_{grid_name}.yml` (in blueprint_dir)

**Updates Blueprint:**
- Appends `Resource` to `blueprint_elements.grid.data`

**Populates Settings:**
- **Compile-time (`cppdefs`)**: Open boundary flags
  ```python
  self._settings_compile_time["cppdefs"]["obc_west"] = self.boundaries.west
  self._settings_compile_time["cppdefs"]["obc_east"] = self.boundaries.east
  self._settings_compile_time["cppdefs"]["obc_north"] = self.boundaries.north
  self._settings_compile_time["cppdefs"]["obc_south"] = self.boundaries.south
  ```
- **Compile-time (`param`)**: Grid dimensions and partitioning
  ```python
  self._settings_compile_time["param"]["LLm"] = self.grid.nx
  self._settings_compile_time["param"]["MMm"] = self.grid.ny
  self._settings_compile_time["param"]["N"] = self.grid.N
  self._settings_compile_time["param"]["NP_XI"] = self.partitioning.n_procs_x
  self._settings_compile_time["param"]["NP_ETA"] = self.partitioning.n_procs_y
  ```
- **Run-time (`roms.in.grid`)**: Grid file path
  ```python
  self._settings_run_time["roms.in"]["grid"] = {"grid_file": out_path}
  ```

### Initial Conditions (`initial_conditions`, order=20)

**Handler**: `_generate_initial_conditions()`

**Generates:**
- Initial conditions NetCDF file(s): `{model_name}_{grid_name}_initial_conditions.nc`
- Initial conditions YAML metadata: `_initial_conditions.yml`

**Source Resolution:**
- Uses `source` and optional `bgc_source` from kwargs
- Resolves paths via `_resolve_source_block()` → `SourceData.path_for_source()`

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.initial_conditions.data`

**Populates Settings:**
- **Run-time (`roms.in.initial`)**: Initial conditions file path
  ```python
  self._settings_run_time["roms.in"]["initial"] = {
      "nrrec": 1,
      "initial_file": paths[0]  # First file in list
  }
  ```

### Surface Forcing (`forcing.surface`, order=30)

**Handler**: `_generate_surface_forcing()`

**Generates:**
- Surface forcing NetCDF file(s): `{model_name}_{grid_name}_surface-{type}_YYYYMM.nc`
- Surface forcing YAML metadata: `_forcing.surface-{type}.yml`

**Key Features:**
- Supports multiple surface forcing sources (physics and bgc)
- Each item in `forcing.surface` list generates a separate file
- Requires `type` parameter: `"physics"` or `"bgc"`

**Source Resolution:**
- Uses `source` from kwargs
- Resolves path via `_resolve_source_block()`

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.forcing.surface.data`

**Populates Settings:**
- **Run-time (`roms.in.forcing`)**: Surface forcing file paths
  ```python
  if type == "bgc":
      self._settings_run_time["roms.in"]["forcing"]["surface_forcing_bgc_path"] = paths[0]
  else:  # physics
      self._settings_run_time["roms.in"]["forcing"]["surface_forcing_path"] = paths[0]
  ```

**Note**: Compile-time settings for surface forcing are not yet populated (TODO in code).

### Boundary Forcing (`forcing.boundary`, order=40)

**Handler**: `_generate_boundary_forcing()`

**Generates:**
- Boundary forcing NetCDF file(s): `{model_name}_{grid_name}_boundary-{type}_YYYYMM.nc`
- Boundary forcing YAML metadata: `_forcing.boundary-{type}.yml`

**Key Features:**
- Supports multiple boundary forcing sources (physics and bgc)
- Each item in `forcing.boundary` list generates a separate file
- Requires `type` parameter: `"physics"` or `"bgc"`
- Uses `boundaries` configuration for open boundary specification

**Source Resolution:**
- Uses `source` from kwargs
- Resolves path via `_resolve_source_block()`

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.forcing.boundary.data`

**Populates Settings:**
- **Run-time (`roms.in.forcing`)**: Boundary forcing file paths
  ```python
  if type == "bgc":
      self._settings_run_time["roms.in"]["forcing"]["boundary_forcing_bgc_path"] = paths[0]
  else:  # physics
      self._settings_run_time["roms.in"]["forcing"]["boundary_forcing_path"] = paths[0]
  ```

**Note**: Compile-time settings for boundary forcing are not yet populated (TODO in code).

### Tidal Forcing (`forcing.tidal`, order=50)

**Handler**: `_generate_tidal_forcing()`

**Generates:**
- Tidal forcing NetCDF file(s): `{model_name}_{grid_name}_tidal.nc`
- Tidal forcing YAML metadata: `_forcing.tidal.yml`

**Key Features:**
- Uses `ntides` parameter from kwargs (default from model_spec.inputs)
- Uses `model_reference_date` (set to `start_date`)

**Source Resolution:**
- Uses `source` from kwargs (typically TPXO)
- Resolves path via `_resolve_source_block()`

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.forcing.tidal.data`

**Populates Settings:**
- **Compile-time (`tides`)**: Tidal forcing configuration
  ```python
  self._settings_compile_time["tides"] = {
      "ntides": 10,  # Default, may be overridden by kwargs
      "bry_tides": True,
      "pot_tides": True,
      "ana_tides": False
  }
  ```

**Note**: Run-time settings for tidal forcing are not yet populated (TODO in code).

### River Forcing (`forcing.river`, order=60)

**Handler**: `_generate_river_forcing()`

**Generates:**
- River forcing NetCDF file(s): `{model_name}_{grid_name}_river.nc`
- River forcing YAML metadata: `_forcing.river.yml`

**Key Features:**
- Uses `include_bgc` parameter from kwargs
- Extracts number of rivers from generated dataset

**Source Resolution:**
- Uses `source` from kwargs (typically DAI)
- Resolves path via `_resolve_source_block()`

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.forcing.river.data`

**Populates Settings:**
- **Compile-time (`river_frc`)**: River forcing configuration
  ```python
  self._settings_compile_time["river_frc"] = {
      "river_source": True,
      "analytical": False,
      "nriv": river.ds.sizes["nriver"],  # From generated dataset
      "rvol_vname": "river_volume",
      "rvol_tname": "river_time",
      "rtrc_vname": "river_tracer",
      "rtrc_tname": "river_time",
  }
  ```
- **Run-time (`roms.in.forcing`)**: River forcing file path
  ```python
  self._settings_run_time["roms.in"]["forcing"]["river_path"] = paths[0]
  ```

### CDR Forcing (`cdr_forcing`, order=80)

**Handler**: `_generate_cdr_forcing()`

**Generates:**
- CDR forcing NetCDF file(s): `{model_name}_{grid_name}_cdr_forcing.nc`
- CDR forcing YAML metadata: `_cdr_forcing.yml`

**Key Features:**
- Optional input (only generates if `cdr_list` is provided)
- Uses `releases` parameter for CDR release specifications

**Updates Blueprint:**
- Appends `Resource(s)` to `blueprint_elements.cdr_forcing.data`

**Populates Settings:**
- Settings for CDR forcing are not yet implemented (TODO in code).

### Corrections Forcing (`forcing.corrections`, order=90)

**Handler**: `_generate_corrections()`

**Status**: Not yet implemented (raises `NotImplementedError`)

## Source Resolution

### `_resolve_source_block()` Method

Normalizes source blocks and injects file paths:

```python
def _resolve_source_block(self, block: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize a "source"/"bgc_source" block and inject a 'path' based on SourceData.
    
    Parameters
    ----------
    block : str or dict
        Source specification (e.g., "GLORYS" or {"name": "GLORYS", "climatology": True})
        
    Returns
    -------
    dict
        Source block with 'name' and optional 'path' fields
    """
```

**Process:**
1. Normalize to dict: If string, convert to `{"name": str}`
2. Extract name: Get `name` field from dict
3. Map to dataset key: Use `SourceData.dataset_key_for_source(name)`
4. Check streamability: If streamable (ERA5, DAI), don't add path unless explicitly provided
5. Get path: Use `SourceData.path_for_source(name)` for non-streamable sources
6. Return: Dict with `name` and optional `path`

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
    extra: Optional[Dict[str, Any]] = None,
    base_kwargs: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Merge per-input defaults with runtime arguments.
    
    Uses base_kwargs if provided (from input_list), otherwise looks up in model_spec.inputs.
    Resolves "source" and "bgc_source" through SourceData.
    Merges with extra, where extra overrides defaults.
    """
```

**Process:**
1. Get base config: Use `base_kwargs` if provided, otherwise lookup in `model_spec.inputs`
2. Resolve source blocks: Convert `source` and `bgc_source` Pydantic models to dicts with paths
3. Merge extra: `extra` overrides defaults (for runtime additions like `start_time`, `use_dask`)

## Settings Population for Forcing

### Compile-Time Settings

**Tidal Forcing:**
- `tides.ntides`: Number of tidal constituents
- `tides.bry_tides`: Boundary tides flag
- `tides.pot_tides`: Potential tides flag
- `tides.ana_tides`: Analytical tides flag

**River Forcing:**
- `river_frc.river_source`: Enable river source flag
- `river_frc.analytical`: Analytical river flag
- `river_frc.nriv`: Number of rivers (from generated dataset)
- `river_frc.rvol_vname`, `river_frc.rvol_tname`: River volume variable/time names
- `river_frc.rtrc_vname`, `river_frc.rtrc_tname`: River tracer variable/time names

**Surface/Boundary Forcing:**
- Compile-time settings for surface and boundary forcing are not yet populated (TODO in code).

### Run-Time Settings

**Surface Forcing:**
- `roms.in.forcing.surface_forcing_path`: Path to physics surface forcing file
- `roms.in.forcing.surface_forcing_bgc_path`: Path to bgc surface forcing file

**Boundary Forcing:**
- `roms.in.forcing.boundary_forcing_path`: Path to physics boundary forcing file
- `roms.in.forcing.boundary_forcing_bgc_path`: Path to bgc boundary forcing file

**River Forcing:**
- `roms.in.forcing.river_path`: Path to river forcing file

**Tidal Forcing:**
- Run-time settings for tidal forcing are not yet populated (TODO in code).

## Blueprint Element Updates

Each handler appends `Resource` objects to the appropriate blueprint element:

**Resource Creation:**
```python
resource = cstar_models.Resource(
    location=out_path,  # Path to generated NetCDF file
    partitioned=False   # Set to True after partitioning
)
```

**Blueprint Updates:**
- **Grid**: `blueprint_elements.grid.data.append(resource)`
- **Initial Conditions**: `blueprint_elements.initial_conditions.data.append(resource)`
- **Forcing Categories**: `blueprint_elements.forcing.{category}.data.append(resource)`
  - `forcing.surface` → `forcing.surface.data`
  - `forcing.boundary` → `forcing.boundary.data`
  - `forcing.tidal` → `forcing.tidal.data`
  - `forcing.river` → `forcing.river.data`
- **CDR Forcing**: `blueprint_elements.cdr_forcing.data.append(resource)`

## File Partitioning

### `_partition_files()` Method

Partitions whole-field input files across tiles:

```python
def _partition_files(self, **kwargs):
    """
    Partition whole input files across tiles using roms_tools.partition_netcdf.
    
    Uses the paths stored in blueprint_elements to build the list of whole-field files,
    and records the partitioned paths in the Resource objects.
    """
```

**Process:**
1. **Iterate over input_list**: For each input key, get corresponding dataset from `blueprint_elements`
2. **Partition each Resource**: Call `rt.partition_netcdf()` for each `Resource.location`
3. **Create partitioned Resources**: Replace original resources with partitioned ones
4. **Update partitioned flag**: Set `partitioned=True` on new resources

**Partitioning Arguments:**
```python
input_args = {
    "np_eta": self.partitioning.n_procs_y,
    "np_xi": self.partitioning.n_procs_x,
    "output_dir": self.input_data_dir,
    "include_coarse_dims": False,
}
```

**Result:**
- Original whole-field files remain unchanged
- Partitioned files created in `input_data_dir`
- `blueprint_elements` updated with partitioned `Resource` objects
- `partitioned` flag set to `True`

## Return Values

`generate_all()` returns a tuple:

```python
(
    blueprint_elements: RomsMarblBlueprintInputData,
    compile_time_settings: dict,
    run_time_settings: dict
)
```

**Usage:**
- `blueprint_elements`: Merged into main blueprint during POSTCONFIG stage
- `compile_time_settings`: Merged with template defaults, used to render `*.opt` files
- `run_time_settings`: Merged with template defaults, used to render `roms.in`

These are used by `CstarSpecBuilder.generate_inputs()` to update the blueprint and settings before persisting.

