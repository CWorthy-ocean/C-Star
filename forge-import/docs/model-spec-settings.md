# Settings

C-Star Forge curates default settings for each model configuration.
These defaults are used in the templating engine to generate source code and input files with the correct parameters.

Settings are managed in `forge` using 
1. Templated code files
2. A `model_settings` dict specifying defaults, consolidated into each model's
   [`model.yaml`](reference-model-yaml.md)
3. User override settings, merged in by the resolver (`build_forge_blueprint`)


### Templates

A model specification in `model.yaml` references its code templates under `code.templates_compile_time`
and `code.templates_run_time`. `directory` is relative to the forge repo root (these templates live at
`templates/` in the forge repo, decoupled from any one `ModelSpec`); `code.templates_commit` pins the forge
commit they're fetched from (defaults to branch `main` if omitted). For example:
```yaml
code:
  templates_commit: 4208f4384431852daccea1ccad01d03c0cb473d9
  templates_compile_time:
    directory: "templates/compile-time"
    files:
    - cppdefs.opt.j2
  templates_run_time:
    directory: "templates/run-time"
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

Run-time options are no longer rendered from Jinja2 templates. Instead they are written to a single `namelist.nml` by `write_roms_namelist`, which validates the settings into `RunTimeSettings` and serializes via C-Star's `cstar.roms.namelist.RomsNamelist` (itself f90nml-backed). The `marbl_in` file is copied as-is.

When `forge` configures and builds the model for a new domain, `render_roms_settings` (in
`cstar_forge/forge/settings.py`) uses the `jinja2` templating engine to replace keys in `cppdefs.opt.j2` with
values from the resolved `model_settings` dict (the same dict that ends up on `ForgeBlueprint.model_settings`).

### Defaults

The `model_settings` dict is initialized from the defaults curated directly in each model's `model.yaml` —
there are no longer separate `compile-time-defaults.yaml`/`run-time-defaults.yaml` files; everything is
consolidated into one `model.yaml` per model.

For example, `catalog/ModelSpec/cson_roms-marbl_v0.1/model.yaml` includes a `model_settings.cppdefs` section
with model-level defaults not otherwise derived by the resolver:
```yaml
model_settings:
  cppdefs:
    sponge_tune: false
    nhy_forcing: true
    nox_forcing: true
```
(`cppdefs.obc_*`/`marbl`/`co2_tvarying`/`sal_restore`/`tides`/`cdr_forcing`/`use_pio` are resolver-derived
from the Domain/Forcing selection and the model's `bgc_mode`/`use_pio` toggles, so they're intentionally
absent from `model.yaml`.)

### User override

User additions are permitted when building model domains in `forge`. A user can pass parameter values to
`build_forge_blueprint` (the resolver) to override the model's defaults, e.g.:
```python
build_forge_blueprint(
    ...,
    compile_time_overrides={"cppdefs": {"sponge_tune": True}},
)
```

The settings actually used are saved with the model's `ForgeBlueprint` (`model_settings`), not just the
`ModelSpec` defaults.
