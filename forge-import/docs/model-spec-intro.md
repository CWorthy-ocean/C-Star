# Model Specification

The `ModelSpec` abstraction is designed to formalize and preserve a notion of a trusted model configuration by aggregrating the information required to build and configure a particular model as a named entity. 

Model specifications are defined per-model in `cstar_forge/catalog/ModelSpec/<model>/model.yaml` (see [here](reference-model-yaml.md)). Models are discovered by scanning `catalog/ModelSpec/*/model.yaml`. 

Each model includes:

- Code repository configurations (ROMS, MARBL, PIO) and template refs (compile-time and run-time)
- Per-run build-mode toggles (`bgc_mode`, `use_pio`)
- Model-specific physics/numerics settings defaults (`model_settings`)

Everything a Domain/Forcing/Output spec already owns (grid/IC/forcing source selection, output write-lists,
open-boundary and tidal/river presence, grid partitioning, etc.) is deliberately *not* duplicated here — those
values come from the selected `DomainSpec/`/`ForcingSpec/`/`OutputSpec/` catalog entries (directories read as plain dicts; of the specs, only `ModelSpec` is also a Python class) and are merged in by the resolver
(`build_forge_blueprint`) when it assembles a `ForgeBlueprint`.


## `model.yaml` Schema

Here's a view of the schema:
```yaml
bgc_mode: marbl  # marbl|none -- prepopulates the wizard; resolver derives cppdefs.marbl from it
use_pio: false  # prepopulates the wizard's PIO checkbox; resolver derives cppdefs.use_pio from it

code:
  roms:
    location: https://github.com/org/repo.git
    commit: <hash>  # or 'branch: main' instead

  marbl:  # optional
    location: https://github.com/marbl-ecosys/MARBL.git
    commit: marbl0.45.0

  pio:  # optional; required if use_pio can be set true
    location: https://github.com/NCAR/ParallelIO.git
    commit: pio2_7_0

  # Render templates live at the forge repo root (templates/), decoupled from this
  # ModelSpec. `directory` is relative to the repo root; `templates_commit` pins the
  # forge commit they're fetched from (defaults to branch `main` if omitted).
  templates_commit: <forge-commit-sha>
  templates_compile_time:
    directory: "templates/compile-time"
    files:
      - cppdefs.opt.j2
  templates_run_time:
    directory: "templates/run-time"
    files:
      - marbl_in

model_settings:
  cppdefs:
    sponge_tune: false
    nhy_forcing: true
    nox_forcing: true
  # ...one section per model_settings namelist key (lateral_visc, vertical_mixing,
  # tracer_diff2, bottom_drag, param, bgc, blk_frc, tides, marbl_bgc, etc.)
```


### Field Descriptions:

- `bgc_mode`  
  Per-run BGC toggle (`marbl` or `none`). Prepopulates the wizard's BGC dropdown; the resolver uses it to
  derive `model_settings.cppdefs.marbl` (and gate `nhy_forcing`/`nox_forcing`) and to decide whether
  `code.marbl` is populated. Not itself part of `model_settings` — it's a build mode, not a namelist section.

- `use_pio`  
  Per-run ParallelIO (PIO) build toggle. Prepopulates the wizard's PIO checkbox; the resolver uses it to
  derive `model_settings.cppdefs.use_pio` and to decide whether `code.pio` is populated (raising if PIO is
  requested but the model has no `code.pio` pin).

- `code`  
  Code repository and template specifications:
  - `roms`: ROMS source code repository (required; specify `location` and `branch` or `commit`)
  - `marbl`: MARBL source code repository (optional; specify `location` and `branch` or `commit`)
  - `pio`: ParallelIO source code repository (optional; specify `location` and `branch` or `commit`)
  - `templates_commit`: forge-repo commit that `templates_compile_time`/`templates_run_time` are fetched
    from (defaults to branch `main` when omitted)
  - `templates_compile_time` / `templates_run_time`: each a `directory` (relative to the forge repo root)
    plus a `files` list. `*.j2` files have Jinja2 templating applied; files without that extension (e.g.
    `marbl_in`) are copied as-is.

- `model_settings`  
  A flat dict of model-specific physics/numerics defaults, mirroring `ForgeBlueprint.model_settings` 1:1
  (each top-level key is a namelist section or a scalar namelist value, e.g. `cppdefs`, `param`, `tides`, `marbl_bgc`; `gamma2`, `ubind`). Every
  compile-time `.j2` template file listed under `code.templates_compile_time.files` must have a
  corresponding top-level key here (e.g. `cppdefs.opt.j2` requires a `cppdefs:` section) — this is
  enforced by a `ModelSpec` validator. Many fields within these sections are still overwritten by the
  resolver at build time from Domain/Forcing/Output selections (e.g. `param`'s grid-partitioning fields,
  `cppdefs.obc_*`/`marbl`/`tides`, `tides.ntides`); they're included in `model.yaml` only where the
  *other* fields in that same section are real, model-level defaults.

You can add new models by creating a new directory under `cstar_forge/catalog/ModelSpec/<model>/` containing
a `model.yaml` with the schema above.
