# Model Specification

The `ModelSpec` abstraction is designed to formalize and preserve a notion of a trusted model configuration by aggregrating the information required to build and configure a particular model as a named entity. 

Model specifications are defined in `cson_forge/models.yml` (see [here](reference-models-yml.md)). 

Each model includes:

- Template configurations (compile-time and run-time)
- Settings specifications (compile-time and run-time)
- Code repository configurations (ROMS, MARBL)
- Input dataset defaults (a list of required source datasets is derived from inputs)


## `models.yml` Schema

Here's a view of the schema:
```yaml
model_name:
  templates:
    compile_time:
      location: "{{ config.path.model_configs }}/{{ model.name }}/templates/compile-time"
      filter:
        files:
          - cppdefs.opt.j2
          - param.opt.j2
          - tracers.opt.j2
          - Makefile
    run_time:
      location: "{{ config.path.model_configs }}/{{ model.name }}/templates/run-time"
      filter:
        files:
          - roms.in.j2
          - marbl_in

  settings:
    properties:
      n_tracers: 34
    compile_time:
      _default_config_yaml: "{{ config.path.model_configs }}/{{ model.name }}/templates/compile-time-defaults.yml"
    run_time:
      _default_config_yaml: "{{ config.path.model_configs }}/{{ model.name }}/templates/run-time-defaults.yml"

  code:
    roms:
      location: https://github.com/org/repo.git
      branch: main  # or use 'commit: <hash>' instead
    marbl:  # optional
      location: https://github.com/org/marbl.git
      commit: v1.0.0

  inputs: # default keyword arguments to input generation functions
    grid:
      topography_source: ETOPO5  # or SRTM15

    initial_conditions:
      source:
        name: GLORYS
      bgc_source:  # optional
        name: UNIFIED
        climatology: true

    forcing:
      surface:
        - source:
            name: ERA5
          type: physics
          correct_radiation: true
        - source:
            name: UNIFIED
            climatology: true
          type: bgc
      boundary:
        - source:
            name: GLORYS
          type: physics
        - source:
            name: UNIFIED
            climatology: true
          type: bgc
      tidal:  # optional
        - source:
            name: TPXO
          ntides: 15
      river:  # optional
        - source:
            name: DAI
            climatology: false
          include_bgc: true
```


### Field Descriptions:

- `templates`  
  Template specifications for compile-time and run-time stages. Each stage specifies:
  - `location`: Path to template directory (may contain template variables)
  - `filter.files`: List of template files to process: 
     `*.j2` files have Jinja2 templating applied; files without this extension are simply copied to build directories.

- `settings`  
  Settings specifications containing:
  - `properties`: Model properties (e.g., `n_tracers`)
  - `compile_time`: Compile-time settings stage with `_default_config_yaml` pointing to default configuration YAML
  - `run_time`: Run-time settings stage with `_default_config_yaml` pointing to default configuration YAML

- `code`  
  Code repository specifications:
  - `roms`: ROMS source code repository (required; specify `location` and `branch` or `commit`)
  - `marbl`: MARBL source code repository (optional; specify `location` and `branch` or `commit`)

- `inputs`  
  Model input specifications:
  - `grid`: Grid configuration with `topography_source` (e.g., "ETOPO5")
  - `initial_conditions`: Initial conditions with `source` (physics) and optional `bgc_source` (biogeochemistry)
  - `forcing`: Forcing data organized by type:
    - `surface`: List of surface forcing items (each with `source`, `type` ["physics"|"bgc"], and optional `correct_radiation`)
    - `boundary`: List of boundary forcing items (each with `source` and `type`)
    - `tidal`: List of tidal forcing items (each with `source` and optional `ntides`)
    - `river`: List of river forcing items (each with `source`, optional `climatology`, and optional `include_bgc`)

- `datasets`  
  (Derived field, not specified in YAML) List of SourceData dataset keys required for this model, automatically derived from the `inputs` configuration.

**Source Specifications:**

Each `source` in the inputs can be:
- A string (source name)
- An object with:
  - `name`: Source name (e.g., "GLORYS", "ERA5", "UNIFIED", "TPXO", "DAI")
  - `climatology`: Boolean indicating whether to use climatology data (default: `false`)

You can add new models by creating a new top-level key in the YAML file with the same schema as above.


