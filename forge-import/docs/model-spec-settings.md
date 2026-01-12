# Settings

C-SON Forge curates default settings for each model configuration.
These defaults are used in the templating engine to generate source code and input files with the correct parameters.

Settings are managed in `forge` using 
1. Templated code files
2. YAML dictionaries specifying defaults 
  - Example [compile-time-settings.yml](reference-settings-compile-time.md)
  - Example [run-time-settings.yml](reference-settings-run-time.md)  
3. User override settings


### Templates

A model specification in `models.yml` will include a list of code templates. For example,
```yaml
cson_roms-marbl_v0.1:
  templates:
    compile_time:
      location: "{{ config.path.model_configs }}/{{ model.name }}/templates/compile-time"
      filter:
        files:
        - bgc.opt.j2
        - blk_frc.opt.j2
        - cdr_frc.opt.j2
        - cppdefs.opt.j2
    run_time:
      location: "{{ config.path.model_configs }}/{{ model.name }}/templates/run-time"
      filter:
        files:
        - roms.in.j2        
        - marbl_in        
```


Templates look like this:

- `cdr_frc.opt.j2`:
  ```fortran
  logical,parameter,public :: cdr_source  = {{ cdr_frc.cdr_source|lower }} 
  logical,parameter,public :: cdr_volume  = {{ cdr_frc.cdr_volume|lower }}
  ```


- `roms.in.j2`:
  ```fortran 
  title:
    {{ title.casename }}

  time_stepping: NTIMES   dt[sec]  NDTFAST  NINFO
        {{ time_stepping.ntimes }}    {{ time_stepping.dt }}    {{ time_stepping.ndtfast }}    {{ time_stepping.ninfo }}

  S-coord: THETA_S,   THETA_B,    TCLINE (m)
            {{ "%.1fD0"|format(s_coord.theta_s) }}        {{ "%.1fD0"|format(s_coord.theta_b) }}    {{ "%.0fD0"|format(s_coord.tcline) }}

  grid:  filename
        {{ grid.grid_file }}

  forcing: filename
        {{ forcing.surface_forcing_path }}
        {{ forcing.surface_forcing_bgc_path }}
        {{ forcing.boundary_forcing_path }}
        {{ forcing.boundary_forcing_bgc_path }}
        {{ forcing.river_path }}
  ```

When `forge` configures and builds the model for a new domain, it uses the `jinja2` templating engine to replace keys in the template files with values in `CStarSpecBuilder._compile_time_settings` dictionary. 

### Defaults

This dictionary is initialized to the defaults curated in YAML files. 

For example, `model-configs/cson_roms-marbl_v0.1/templates/compile-time-defaults.yml` includes the following:
```yaml
cdr_frc:
  cdr_source: false
  cdr_volume: true
```

### User override

User additions are permitted when building model domains in `forge`. For example, a user can pass in parameter values to override defaults:
```python
CStarSpecBuilder.configure_build(compile_time_settings={"cdr_source": True})
```

The settings used saved with the model's `blueprint` file.
