# Settings

C-STAR Forge curates default settings for each model configuration.
These defaults are used in the templating engine to generate source code and input files with the correct parameters.

Settings are managed in `forge` using 
1. Templated code files
2. YAML dictionaries specifying defaults 
  - Example [compile-time-settings.yml](reference-settings-compile-time.md)
  - Example [run-time-settings.yml](reference-settings-run-time.md)  
3. User override settings


### Templates

A model specification in `model.yml` will include a list of code templates. For example,
```yaml
templates:
  compile_time:
    location: "templates/compile-time"
    filter:
      files:
      - cppdefs.opt.j2
  run_time:
    location: "templates/run-time"
    filter:
      files:
      - marbl_in
```


Compile-time options still use a Jinja2 template, `cppdefs.opt.j2`, which renders the ROMS CPP defines. For example:

- `cppdefs.opt.j2`:
  ```jinja
  {% if cppdefs.cdr_forcing|default(false) %}#define CDR_FORCING
  {% else %}#undef CDR_FORCING
  {% endif %}
  ```

Run-time options are no longer rendered from Jinja2 templates. Instead they are written to a single `namelist.nml` (via `f90nml`, in `write_roms_namelist`) from the run-time settings, whose defaults live in `templates/run-time-defaults.yml`. The `marbl_in` file is copied as-is.

When `forge` configures and builds the model for a new domain, it uses the `jinja2` templating engine to replace keys in `cppdefs.opt.j2` with values from the `CStarSpecBuilder._compile_time_settings` dictionary. 

### Defaults

This dictionary is initialized to the defaults curated in YAML files. 

For example, `catalog/ModelSpec/cson_roms-marbl_v0.1/templates/compile-time-defaults.yml` includes the following:
```yaml
cppdefs:
  obc_west: true
  obc_east: true
  marbl: true
  cdr_forcing: false
```

### User override

User additions are permitted when building model domains in `forge`. For example, a user can pass in parameter values to override defaults:
```python
CStarSpecBuilder.configure_build(compile_time_settings={"cdr_forcing": True})
```

The settings used saved with the model's `blueprint` file.
