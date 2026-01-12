# Input Data Generation Overview

The `input_data` module provides classes and utilities for generating input data files for ocean models. It uses a **registry-based framework** similar to the `source_data` module, allowing extensible input generation through decorator-based registration.

## Module Purpose

The input data generation process transforms prepared source datasets into model-ready input files:

- **Grid files**: ROMS grid NetCDF files
- **Initial conditions**: Temperature, salinity, and biogeochemical fields
- **Forcing data**: Surface, boundary, tidal, and river forcing
- **CDR forcing**: Carbon dioxide removal forcing (optional)
- **Corrections**: Forcing corrections (not yet implemented)

## Core Components

### Base Class: `InputData`

Abstract base class defining the interface for input data generation:

```python
class InputData:
    model_name: str
    grid_name: str
    start_date: Any
    end_date: Any
    input_data_dir: Path  # Output directory for input files
    
    def generate_all(self):
        """Generate all input files. Must be implemented by subclasses."""
        raise NotImplementedError
```

**Key Features:**
- Manages output directory (`input_data_dir`)
- Provides filename construction helpers
- Handles clobber logic for existing files

### Registry System

Input generation steps are registered using the `@register_input` decorator:

```python
@register_input(name="grid", order=10, label="Writing ROMS grid")
def _generate_grid(self, key: str = "grid", **kwargs):
    """Generate grid input file."""
    # Implementation...
```

**Registry Components:**
- `INPUT_REGISTRY`: Dictionary mapping input keys to `InputStep` instances
- `InputStep`: Container for handler function, order, and label
- `@register_input`: Decorator to register handler functions

**Execution Order:**
Steps are executed in order (lowest `order` value first):
- `grid` (order=10)
- `initial_conditions` (order=20)
- `forcing.surface` (order=30)
- `forcing.boundary` (order=40)
- `forcing.tidal` (order=50)
- `forcing.river` (order=60)
- `cdr_forcing` (order=80)
- `forcing.corrections` (order=90)

### ROMS-MARBL Implementation: `RomsMarblInputData`

The `RomsMarblInputData` class provides ROMS-MARBL specific input generation:

**Key Attributes:**
- `model_spec`: Model specification from `models.yml`
- `grid`: ROMS grid object
- `boundaries`: Open boundary configuration
- `source_data`: Prepared source datasets
- `blueprint_elements`: Blueprint subset containing input data fields
- `_settings_compile_time`: Compile-time settings dictionary
- `_settings_run_time`: Run-time settings dictionary
- `input_list`: List of (key, kwargs) tuples derived from `model_spec.inputs`

**Workflow:**
1. **Initialization**: Builds `input_list` from `model_spec.inputs`, validates against registry
2. **Generation**: `generate_all()` executes registered handlers in order
3. **Blueprint Updates**: Each handler appends `Resource` objects to `blueprint_elements`
4. **Settings Updates**: Handlers populate compile-time and run-time settings dictionaries
5. **Partitioning**: Optional step to partition files across tiles

## Input Generation Process

### Step 1: Build Input List

The `input_list` is derived from `model_spec.inputs`:
- Grid specifications → `("grid", kwargs)`
- Initial conditions → `("initial_conditions", kwargs)`
- Forcing items → `("forcing.surface", kwargs)`, `("forcing.boundary", kwargs)`, etc.

### Step 2: Execute Handlers

For each item in `input_list`:
1. Look up handler in `INPUT_REGISTRY`
2. Build input arguments from defaults + kwargs
3. Resolve source paths via `SourceData`
4. Call handler function
5. Update blueprint and settings

### Step 3: Source Resolution

Source blocks (e.g., `{"name": "GLORYS"}`) are resolved:
- Map logical name to dataset key via `SourceData.dataset_key_for_source()`
- Get prepared file path via `SourceData.path_for_source()`
- Handle streamable sources (no local path needed)

### Step 4: Settings Population

Handlers populate settings dictionaries:
- **Compile-time**: `cppdefs`, `param`, `tides`, `river_frc`, etc.
- **Run-time**: `roms.in` sections (`grid`, `initial`, `forcing`, etc.)

These settings are used later to render configuration templates.

## Input Types

### Grid (`grid`)
- **Handler**: `_generate_grid()`
- **Output**: Grid NetCDF file and YAML metadata
- **Settings**: Updates `param` (grid dimensions), `cppdefs` (open boundaries)

### Initial Conditions (`initial_conditions`)
- **Handler**: `_generate_initial_conditions()`
- **Output**: Initial conditions NetCDF file(s)
- **Settings**: Updates `roms.in.initial` (initial file path)

### Surface Forcing (`forcing.surface`)
- **Handler**: `_generate_surface_forcing()`
- **Output**: Surface forcing NetCDF file(s) (physics or bgc)
- **Settings**: Updates `roms.in.forcing` (surface forcing paths)

### Boundary Forcing (`forcing.boundary`)
- **Handler**: `_generate_boundary_forcing()`
- **Output**: Boundary forcing NetCDF file(s) (physics or bgc)
- **Settings**: Updates `roms.in.forcing` (boundary forcing paths)

### Tidal Forcing (`forcing.tidal`)
- **Handler**: `_generate_tidal_forcing()`
- **Output**: Tidal forcing NetCDF file(s)
- **Settings**: Updates `tides` compile-time settings (ntides, flags)

### River Forcing (`forcing.river`)
- **Handler**: `_generate_river_forcing()`
- **Output**: River forcing NetCDF file(s)
- **Settings**: Updates `river_frc` compile-time settings (nriv, variable names)

### CDR Forcing (`cdr_forcing`)
- **Handler**: `_generate_cdr_forcing()`
- **Output**: CDR forcing NetCDF file(s) (optional)
- **Settings**: Updates CDR-related settings (not yet implemented)

## Blueprint Integration

Each input handler updates `blueprint_elements`, a subset of the blueprint containing:
- `grid`: Grid dataset
- `initial_conditions`: Initial conditions dataset
- `forcing`: Forcing configuration (surface, boundary, tidal, river, corrections)
- `cdr_forcing`: CDR forcing dataset

**Resource Objects:**
Each generated file is represented as a `Resource` object with:
- `location`: Path to the NetCDF file
- `partitioned`: Boolean indicating if file is partitioned

## Settings Integration

Handlers populate two settings dictionaries:

### Compile-Time Settings (`_settings_compile_time`)
- **cppdefs**: Open boundary flags (`obc_west`, `obc_east`, etc.)
- **param**: Grid dimensions, partitioning (`LLm`, `MMm`, `N`, `NP_XI`, `NP_ETA`)
- **tides**: Tidal forcing configuration (`ntides`, `bry_tides`, `pot_tides`)
- **river_frc**: River forcing configuration (`nriv`, variable names)

### Run-Time Settings (`_settings_run_time`)
- **roms.in.grid**: Grid file path
- **roms.in.initial**: Initial conditions file path
- **roms.in.forcing**: Forcing file paths (surface, boundary, river)

These settings are later merged with template defaults and used to render configuration files.

## File Outputs

All input files are written to:
```
{input_data_dir}/{model_name}_{input_name}.nc
```

For example:
- `cson_roms-marbl_v0.1_test-tiny_grid.nc`
- `cson_roms-marbl_v0.1_test-tiny_initial_conditions.nc`
- `cson_roms-marbl_v0.1_test-tiny_surface-physics_201201.nc`
- `cson_roms-marbl_v0.1_test-tiny_boundary-physics_201201.nc`

## Usage Pattern

```python
from cson_forge.input_data import RomsMarblInputData

# Create input data generator
input_gen = RomsMarblInputData(
    model_name="cson_roms-marbl_v0.1",
    grid_name="test-tiny",
    start_date=datetime(2012, 1, 1),
    end_date=datetime(2012, 1, 2),
    model_spec=model_spec,
    grid=grid,
    boundaries=boundaries,
    source_data=source_data,
    blueprint_dir=blueprint_dir,
    partitioning=partitioning,
)

# Generate all inputs
blueprint_elements, compile_time_settings, run_time_settings = input_gen.generate_all(
    clobber=False,
    partition_files=False,
    test=False
)
```

## Integration with CstarSpecBuilder

The `RomsMarblInputData` class is used internally by `CstarSpecBuilder.generate_inputs()`:

1. Creates `RomsMarblInputData` instance
2. Calls `generate_all()` to create input files
3. Updates blueprint with `blueprint_elements`
4. Merges settings dictionaries with template defaults
5. Persists blueprint and settings to disk

This completes the POSTCONFIG stage of the workflow.

