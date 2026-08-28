# Input Data Generation Overview

> **This subsystem is driven by the forge application** (`cstar forge run <forge_blueprint.yaml>`, or equivalently
> `python -m cstar_forge.run`), which loads a `ForgeBlueprint` and calls `ForgeExecutor.generate_inputs()`
> (`cstar_forge/forge/executor.py`). That method constructs a `RomsMarblInputData`
> instance and calls `generate_all()` on it. Constructing `RomsMarblInputData` directly
> (as shown later in this doc) is for developers debugging or extending input generation
> — normal usage goes through the forge application.

The `input_data` module (`cstar_forge/forge/input_data.py`) provides classes and utilities for generating input data files for ocean models. It uses a **registry-based framework** similar to the `source_data` module, allowing extensible input generation through decorator-based registration.

## Module Purpose

The input data generation process transforms prepared source datasets into model-ready input files:

- **Grid files**: ROMS grid NetCDF files
- **Initial conditions**: Temperature, salinity, and biogeochemical fields
- **Forcing data**: Surface, boundary, tidal, and river forcing
- **CDR forcing**: Carbon dioxide removal forcing (optional)
- **Corrections**: Forcing corrections (registered but unwired: the resolver never emits a `corrections` category, and the handler raises `NotImplementedError`)

## Core Components

### Base Class: `InputData`

Abstract dataclass defining the interface for input data generation:

```python
@dataclass
class InputData:
    domain_name: str
    start_date: Any
    end_date: Any
    input_data_dir: Path = field(kw_only=True)  # Output directory, injected by the caller

    def generate_all(self):
        """Generate all input files. Must be implemented by subclasses."""
        raise NotImplementedError
```

`input_data_dir` is injected by the caller (the executor) rather than derived from
`cstar_forge.config` — this keeps the class host-independent.

**Key Features:**
- Manages output directory (`input_data_dir`), creating it in `__post_init__`
- Provides filename construction helpers (`_forcing_filename`)
- Handles clobber logic for existing files (`_ensure_empty_or_clobber`)

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
- `forcing.corrections` (order=90; registered but unwired — never emitted by the resolver)

### ROMS-MARBL Implementation: `RomsMarblInputData`

The `RomsMarblInputData` class provides ROMS-MARBL specific input generation:

**Key Attributes** (dataclass fields; see `docs/InputData-RomsMarblInputData.md` for the full list):
- `domain_name`, `start_date`, `end_date`, `input_data_dir`: core config, inherited from `InputData`
- `grid`: ROMS grid object (`rt.Grid`), plus optional `grid_parent`/`grid_child`/`metadata_child` for nesting
- `boundaries`: Open boundary configuration (`OpenBoundaries`)
- `source_data`: Prepared source datasets (`SourceData`)
- `forcing_override`: the fully-resolved initial-conditions + forcing selection driving generation
  (keys mirror the blueprint's `inputs` block: `initial_conditions`, `forcing.surface`, `forcing.boundary`, etc.)
- `cdr_forcing`: optional user-provided CDR forcing dict
- `roms_marbl_blueprint_elements`: `RomsMarblBlueprintInputData` subset, auto-initialized
- `_settings_compile_time` / `_settings_run_time`: settings dictionaries. Bound directly (no copy)
  from the `settings_compile_time`/`settings_run_time` constructor args when given -- the
  executor's own live dicts, in the normal `ForgeExecutor` path -- or a fresh empty dict
  when omitted (standalone/test use)
- `has_bgc`: whether the model build includes MARBL BGC (mirrors `ForgeExecutor._has_bgc`);
  gates `include_bgc` on `make_nesting_info` and the run-time `bgc` section
- `input_list`: list of `(key, kwargs)` tuples derived from `forcing_override` (a plain attribute set in `__post_init__`, not a declared dataclass field; see below)

**Workflow:**
1. **Initialization**: Builds `input_list` from `forcing_override` (plus the always-present `grid` entry and any
   `cdr_forcing`), validates against registry
2. **Generation**: `generate_all()` executes registered handlers in order
3. **Blueprint Updates**: Each handler appends `Resource` objects to `roms_marbl_blueprint_elements`
4. **Settings Updates**: Handlers populate compile-time and run-time settings dictionaries
5. **Partitioning**: Optional step to partition files across tiles

## Input Generation Process

### Step 1: Build Input List

`__post_init__` derives `input_list` from `forcing_override` (the resolved initial-conditions +
forcing selection injected by the caller — see `cstar_forge.forge.forge_blueprint_engine` /
`sources_to_forcing_override`), not from a model spec:
- `grid` is always appended (its handler ignores kwargs; the grid comes from the injected `grid` object)
- `forcing_override["initial_conditions"]` → `("initial_conditions", kwargs)`
- `forcing_override["forcing"][category]` items → `("forcing.{category}", kwargs)` for each item
- an optional `cdr_forcing` constructor kwarg → `("cdr_forcing", {"cdr_kwargs": ...})`

### Step 2: Execute Handlers

For each item in `input_list`:
1. Look up handler in `INPUT_REGISTRY`
2. Build input arguments from defaults + kwargs
3. Resolve source paths via `SourceData`
4. Call handler function
5. Update blueprint and settings

### Step 3: Source Resolution

Source blocks (e.g., `{"name": "GLORYS"}`) are resolved:
- Check streamability via `SourceData.streamable_for_source()` (no local path needed for
  streamable sources, e.g. ERA5)
- Get prepared file path via `SourceData.path_for_source()` for non-streamable sources
- `SourceData.dataset_key_for_source()` is used elsewhere (subchunk-reference memoization for
  multi-file GLORYS sources), not in this resolution step itself

### Step 4: Settings Population

Handlers populate settings dictionaries:
- **Compile-time** (`_settings_compile_time`): `cppdefs` only (open boundary flags,
  `sal_restore`, `co2_tvarying`, `cdr_forcing`)
- **Run-time** (`_settings_run_time`): flat sections — `grid`, `param`, `s_coord`, `initial`,
  `forcing`, `extract_data`, `blk_frc`, `bgc`, `tides`, `river_frc`, `cdr_frc`, `cdr_output`, etc.
  (note: `param`, `tides`, and `river_frc` are run-time, not compile-time, despite sounding like
  compile-time concerns)

These settings are used later to render configuration templates.

## Input Types

### Grid (`grid`)
- **Handler**: `_generate_grid()`
- **Output**: Grid NetCDF file and YAML metadata (plus a child grid + nesting-info NetCDF when
  `grid_child` is set)
- **Settings**: Updates the run-time `grid` (grid file path), `param` (grid dimensions), `s_coord`,
  and (when nesting) `extract_data`; updates the compile-time `cppdefs` (open boundaries)

### Initial Conditions (`initial_conditions`)
- **Handler**: `_generate_initial_conditions()`
- **Output**: Initial conditions NetCDF file(s)
- **Settings**: Updates the run-time `initial` settings (initial file path)

### Surface Forcing (`forcing.surface`)
- **Handler**: `_generate_surface_forcing()`
- **Output**: Surface forcing NetCDF file(s) (physics, bgc, or restoring)
- **Settings**: Updates the run-time `forcing` settings (surface forcing paths) and `blk_frc`/
  `bgc` (`interp_frc`); updates compile-time `cppdefs` (`sal_restore`, `co2_tvarying`)

### Boundary Forcing (`forcing.boundary`)
- **Handler**: `_generate_boundary_forcing()`
- **Output**: Boundary forcing NetCDF file(s) (physics or bgc)
- **Settings**: Updates the run-time `forcing` settings (boundary forcing paths)

### Tidal Forcing (`forcing.tidal`)
- **Handler**: `_generate_tidal_forcing()`
- **Output**: Tidal forcing NetCDF file(s)
- **Settings**: Updates the run-time `tides.ntides` (constituent count actually generated) and
  `forcing.tidal_forcing_path`; `bry_tides`/`pot_tides`/`ana_tides` are owned by the resolver, not
  this handler

### River Forcing (`forcing.river`)
- **Handler**: `_generate_river_forcing()`
- **Output**: River forcing NetCDF file(s)
- **Settings**: Updates the run-time `river_frc` settings (nriv, variable names) and
  `forcing.river_path`

### CDR Forcing (`cdr_forcing`)
- **Handler**: `_generate_cdr_forcing()`
- **Output**: CDR forcing NetCDF file(s) (optional)
- **Settings**: Updates compile-time `cppdefs.cdr_forcing`; updates run-time `cdr_frc` and
  `cdr_output.do_cdr_output`

## Blueprint Integration

Each input handler updates `roms_marbl_blueprint_elements` (a `RomsMarblBlueprintInputData`), a subset of the blueprint containing:
- `grid`: Grid dataset
- `initial_conditions`: Initial conditions dataset
- `forcing`: Forcing configuration (surface, boundary, tidal, river, corrections)
- `cdr_forcing`: CDR forcing dataset
- `nesting_info`: Nesting-info dataset, only set when a child grid is present

**Resource Objects:**
Each generated file is represented as a `Resource` object with:
- `location`: Path to the NetCDF file
- `partitioned`: Boolean indicating if file is partitioned

## Settings Integration

Handlers populate two settings dictionaries:

### Compile-Time Settings (`_settings_compile_time`)
- **cppdefs**: Open boundary flags (`obc_west`, `obc_east`, etc.), plus `sal_restore`,
  `co2_tvarying`, and `cdr_forcing` when those forcing types are generated

This is the only compile-time section this class populates — grid dimensions, tides, and river
settings are all run-time (see below), not compile-time.

### Run-Time Settings (`_settings_run_time`)
- **grid**: Grid file path
- **param**: Grid dimensions and partitioning (`llm`, `mmm`, `n`, `np_xi`, `np_eta`)
- **s_coord**: Vertical stretching parameters (`tcline`, `theta_b`, `theta_s`)
- **extract_data**: Nesting-extraction settings, only when a child grid is present
- **initial**: Initial conditions file path
- **forcing**: Forcing file paths (surface, boundary, tidal, river)
- **blk_frc**/**bgc**: `interp_frc` (coarse-grid usage flag)
- **tides**: `ntides` (constituent count)
- **river_frc**: River forcing configuration (`nriv`, variable names)
- **cdr_frc**/**cdr_output**: CDR forcing configuration

These settings are later merged with template defaults and used to render configuration files.

## File Outputs

All input files are written to:
```
{input_data_dir}/{domain_name}_{input_name}.nc
```

`domain_name` has any `.` replaced with `_` (via `netcdf_filename_component()`), since generated
NetCDF basenames must not contain a `.` except the final `.nc` suffix. For example, a domain
named `cson_roms-marbl_v0.1_test-tiny` produces:
- `cson_roms-marbl_v0_1_test-tiny_grid.nc`
- `cson_roms-marbl_v0_1_test-tiny_initial_conditions.nc`
- `cson_roms-marbl_v0_1_test-tiny_surface-physics_201201.nc`
- `cson_roms-marbl_v0_1_test-tiny_boundary-physics_201201.nc`

## Usage Pattern

Direct construction is for developers; normal usage goes through the forge application
(`cstar forge run`, see the note at the top of this document).

```python
from cstar_forge.forge.input_data import RomsMarblInputData

# Create input data generator — host-independent: paths and the resolved forcing
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
```

## Integration with ForgeExecutor

The `RomsMarblInputData` class is used internally by `ForgeExecutor.generate_inputs()`
(`cstar_forge/forge/executor.py`; see `docs/architecture-details.md` for the architecture). That
method is in turn called by `process_forge_blueprint()`
(`cstar_forge/forge/forge_blueprint_engine.py`), which is what `cstar forge run`
invokes:

1. Creates `RomsMarblInputData` instance, passing `self.forcing_override`, the other
   executor-resolved fields, and `self._settings_compile_time`/`self._settings_run_time`
   BY REFERENCE (as `settings_compile_time=`/`settings_run_time=`) and `has_bgc=self._has_bgc`
2. Calls `generate_all()` to create input files -- generation steps mutate the executor's
   settings dicts in place; the executor already holds the up-to-date values through its
   own reference, so there is no merge-back step
3. Updates the in-memory blueprint with `roms_marbl_blueprint_elements`
4. Persists blueprint and settings to disk (in `configure_build()`)

This completes the `generate_inputs` stage; the blueprint and settings are persisted in `configure_build()`.

