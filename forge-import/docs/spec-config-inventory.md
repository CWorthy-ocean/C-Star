# Inventory: inputs that drive `configure_build` and `generate_inputs`

This document catalogs **every input, setting, and hardcoded value** that affects
the results of `CstarSpecBuilder.generate_inputs()` and
`CstarSpecBuilder.configure_build()`. It exists to support a planned refactor that
separates the **collection/curation of options** (assemble → review → write a single
authoritative file) from the **heavy processing** (ingest the file → generate inputs
and configure the build, possibly on a different machine).

A concrete starting-point Pydantic model for that authoritative file lives in
`cstar_forge/spec_config.py` (`SpecConfig`).

---

## 1. The six sources of truth (provenance)

Everything that influences the two methods comes from one of six layers. *Where* a
value comes from matters more for the refactor than *what* it controls, because it
determines whether the value can be frozen into the authoritative file at config
time or must be produced by processing.

| Layer | Source | User-mutable? | Resolved when | In authoritative file? |
|---|---|---|---|---|
| **L1 Run inputs** | `CstarSpecBuilder` constructor args | yes | construction | ✅ verbatim |
| **L2 ModelSpec** | `model.yml` + `compile/run-time-defaults.yml` + code pins + input source defaults | via override files / editing `model.yml` | `_load_model_spec` at construction | ✅ resolved & inlined |
| **L3 Machine/env** | `config.py` `DataPaths`, `catalog/Machines/*.yml`, env vars | via env / machine | import + construction | ⚠️ partial — re-resolvable at ingest |
| **L4 Hardcoded** | literals in `source_data.py`, `input_data.py`, `_core.py`, `util.py` | no (code change only) | baked in | ✅ snapshot as explicit, overridable values |
| **L5 Derived** | computed in `_init_settings_*` and the input handlers | indirectly | config-time **or** process-time | 🔶 split (see §3) |
| **L6 External lib** | `roms_tools` defaults (nesting period, `Grid` behavior) | no | call-time | ✅ snapshot the resolved value |

---

## 2. Catalog by domain category

### A. Grid / domain geometry
- **L1:** `grid_kwargs`, `grid_kwargs_parent`, `grid_kwargs_child` (incl. optional `metadata`), `grid_name`
  → build `rt.Grid` objects (`grid`, `grid_parent`, `grid_child`, `metadata_child`) in `model_post_init` (`_core.py:374–446`).
- **L2:** `inputs.grid.topography_source` (`ETOPO5`) — `model.yml`.
- **L4 hardcoded:** `param.nsub_x = nsub_e = 1` (`input_data.py:661–662`); grid / `grid_child` / `nesting` filename stems.
- **L5 derived:** `param.{llm,mmm,n}` (from grid `nx`/`ny`/`N`); `cppdefs.obc_{west,east,north,south}` (from boundaries present); child `extract_data.{n_chd,theta_s_chd,theta_b_chd,hc_chd}`.
- **L5 derived — artifact-dependent (process-time only):** `s_coord.{theta_s,theta_b,tcline}` (read from the generated grid file); `grid.grid_file` path.

### B. Forcing & source data
- **L1:** `open_boundaries` (`north/south/east/west`), `cdr_forcing`, `start_date`/`end_date` (temporal subsetting).
- **L2:** `inputs.forcing.{surface,boundary,tidal,river}` items — `source.name`, `type` (physics/bgc), `climatology`, `glorys_layout`, `correct_radiation`, `coarse_grid_mode`, `ntides`, `include_bgc`; `inputs.initial_conditions.{source,bgc_source}`.
- **L4 hardcoded (`source_data.py`):** dataset registry + alias map (`GLORYS→GLORYS_REGIONAL`, `UNIFIED→UNIFIED_BGC`, …); download URLs (SRTM15 `V2.7`, MBL_CO2, WOA18); GLORYS `dataset_id`; TPXO `TPXO10.v2` file paths; streamable set (`ERA5`, `DAI`).
- **L4 hardcoded (`input_data.py`):** forcing filename stems; tides `bry_tides=True/pot_tides=True/ana_tides=False`; river `river_source`/`analytical`/vname/tname; `nrrec=1`; CDR `cdr_file="cdr.nc"`; coarse-dim (`interp_frc`) inference; `cppdefs.{sal_restore,co2_tvarying}` triggers.
- **L5 derived:** `tides.ntides`, `river_frc.nriv`, `cdr_frc.ncdr_parm`.
- **L5 derived — artifact-dependent (process-time only):** all `forcing.*_path`, `initial.initial_file`.

### C. Model / physics / numerics
- **L2 (`run-time-defaults.yml`, ~25 sections):** `lateral_visc`, `vertical_mixing`, `tracer_diff2`, `bottom_drag`, `v_sponge`, `gamma2`, `ubind`, `lin_rho_eos`, `sss_correction`, `sst_correction`.
- **L2 (`settings.properties`):** `n_tracers=34`, `marbl=true`.
- **L4 hardcoded:** `time_stepping.ndtfast=60`, `ninfo=1` (`_core.py` timestepping defaults).
- **L5 derived (pure functions of config inputs):** `time_stepping.dt` (CFL from grid size/spacing); `ntimes` (duration ÷ dt); `v_sponge.v_sponge` (grid spacing in m ÷ 10).

### D. BGC / MARBL / CDR
- **L2:** `bgc` section; `marbl_bgc` (`marbl_config_file`, tracer/diagnostic write-lists, `marbl_timestep_ratio`); `cdr_frc` / `cdr_output`.
- **L4:** `compile-time-defaults cppdefs.{marbl,cdr_forcing,co2_tvarying}`; the static `marbl_in` run-time file copied verbatim.

### E. Output / diagnostics
- **L2:** `ocean_vars` (~33 flags), `surf_flux`, `frc_output`, `diagnostics`, `stdout_diag`, `zslice`, `upscale_output`, `random_output`, `particles`, `sponge_tune`, `ts_output`, `calc_pflx`, `extract_data`.
- **L5 derived:** `title.casename`, `output_root_name` (from `casename` / `run_output_dir`); `extract_data.extract_period` (child metadata `period`, else `roms_tools` default `3600.0` from `roms_tools_default_nesting_period_seconds()`).

### F. Execution / build / code
- **L1:** `partitioning` (`n_procs_x`, `n_procs_y`) → `param.{np_xi,np_eta}`; `ensemble_id`; `description`; (derived) `run_output_dir`.
- **L2:** `code.roms` / `code.marbl` (location + commit/branch pins); `templates` (compile/run-time locations + filter files: `cppdefs.opt.j2`, `marbl_in`).
- **L3:** machine tag (`MacOS` / `RCAC_anvil` / `NERSC_perlmutter`), `account` (`m4632` / `ees250129`), `pes_per_node`, `queues`, cluster type, dask interface; all `DataPaths` (`here`, `source_data`, `input_data`, `scratch`, `catalog`, `blueprints`, `*_yaml`); env vars (`USER`, `NERSC_HOST`, `WORK`, `SCRATCH`, `ENV_CSTAR_*`).
- **L1 (process-only flags):** `generate_inputs(clobber, use_dask, partition_files, test, prompt_if_files_exist)`; `configure_build(compile_time_settings=…, run_time_settings=…)`.
- **L1/L2 (overrides):** the `override` list of YAML files merged into both settings dicts.

---

## 3. The config / processing boundary

**Governing principle:** the authoritative `SpecConfig` stores ONLY
host-independent, single-source-of-truth inputs. Everything mechanically derivable —
from the host, from identity, or from generated artifacts — is computed at
**processing** time and never stored. This keeps the file portable (generate on
machine A, process on machine B) and avoids drift between duplicated values.

**Stored in `SpecConfig`** (curated inputs + reviewable settings):
- atomic identity (`model_name`, `grid_name`, `ensemble_id`, `description`), run dates
- `domain.grid_kwargs` (the single source for grid geometry, incl. `theta_s`/`theta_b`/`hc`)
  and `domain.partitioning` (a host-independent decomposition choice)
- `sources` (resolved datasets) and `code` pins — including `templates_compile_time` /
  `templates_run_time`, which are repo references (`location` + `commit`/`branch` +
  `files`) exactly like `code.roms` / `code.marbl`, not local paths
- `model_settings` — a **flat** mapping: `cppdefs` alongside every namelist section,
  *including* the pure-derived numerics (`time_stepping.{dt,ntimes}`, `v_sponge`,
  `param.{llm,mmm,n,np_xi,np_eta}`, `cppdefs.obc_*`). These are computed at config
  time but kept inline because they carry scientific review value and may be edited.

**Not stored — deterministic implementation details** (set by the processing step,
no review/override value): the fixed `cdr.nc` / `nesting.nc` filenames, `nrrec`, and
the tide flags (`bry_tides`/`pot_tides`/`ana_tides`) applied during generation. These
have no config-level home; where one is genuinely a namelist value (e.g. the tide
flags) it lives in the relevant `model_settings` section, which processing overwrites.

**Derived at processing — NOT stored:**

| Group | Values | Source at processing |
|---|---|---|
| **Host / machine** | machine tag, `account`, `queues`, `pes_per_node`; data paths `source_data`/`input_data`/`scratch`/`catalog` | `cstar_forge.config` on the run host |
| **Host-dependent paths** | `run_output_dir` (= `scratch/casename`), namelist `output_root_name` | derived from host scratch + `casename` |
| **Naming** | `name`, `casename`, namelist `title.casename` | `f(model_name, grid_name, ensemble_id, dates, n_procs)` — `SpecConfig` properties |
| **Artifacts** | `s_coord.{theta_s,theta_b,tcline}`; `grid`/`initial`/`forcing` file paths | generated grid file / generated NetCDF |

> **Design rule:** `SpecConfig` is **input-only**. The existing blueprint YAML remains
> the **output** that captures the host- and artifact-derived values. This avoids a
> chicken-and-egg where the authoritative file would contain values that require
> running the very thing it configures.

**Naming is single-source.** Only `model_name` + `grid_name` (+ `ensemble_id`, dates,
`n_procs`) are stored. `name`, `casename`, `title`, `output_root_name`, and
`run_output_dir` are deterministic functions exposed as `SpecConfig` properties /
helpers (`.name`, `.casename`, `.run_output_dir(scratch)`, `.output_root_name(scratch)`),
so there is exactly one place to change a name and everything else follows.

### Composable pieces

The config is assembled from a few independently-selectable pieces — the goal is a
catalog/UI where a user picks (or authors) each and reviews the assembled result:

| Piece | Catalog home | Contributes to `SpecConfig` |
|---|---|---|
| **Model** | `catalog/ModelSpec/<name>/` | `code`, `properties`, `model_settings` defaults |
| **Domain** | `catalog/DomainSpec/<name>/` | `domain` (grid kwargs, partitioning, boundaries) |
| **Forcing** | *(future)* `catalog/ForcingSpec/` | `sources` (ICs + surface/boundary/tidal/river + CDR) |
| **Run** | per-run (not cataloged) | `run` (dates), `identity.ensemble_id` |

A top-level `composition` block records, per piece, its `name`, `origin`
(`catalog` / `custom` / `model_default`), and whether it was `modified` after
selection — so a UI/review can show provenance without re-deriving it.

> Deferred ideas (noted, not built): splitting CDR into its own "intervention"
> piece; tracking per-key default-vs-override provenance inside `model_settings`.

---

## 4. Proposed two-phase flow

```
   [Phase 1: COLLECTION / CURATION]          [authoritative file]      [Phase 2: PROCESSING]
 user args ─┐                                                    Phase 2 resolves the
 model.yml ─┤                                                    HOST (machine + paths)
 defaults  ─┼─► resolve + merge + validate ─► spec_config.yml ─► from cstar_forge.config,
 hardcoded ─┘   (+ compute pure-derived)       (reviewable)      then Engine.run(spec_config)
                                                                 → blueprint + NetCDF + namelist
```

Phase 1 is cheap, reviewable, diffable, and **host-independent** (no `rt.Grid`, no
downloads, no file I/O, no machine/paths). Phase 2 becomes a near-pure function
`(SpecConfig, host) → artifacts`, where `host` (machine config + data paths) is
resolved on the run machine.

Two choices that make "review here, process elsewhere" work:
1. **Inline resolved values, don't reference them.** Hardcoded registries/URLs (L4)
   and `roms_tools` defaults (L6) are *snapshotted* into the file, so a reviewer sees
   the real GLORYS `dataset_id`/URL and the processing host can't silently drift.
2. **Keep machine/paths OUT of the file entirely.** The same `spec_config.yml` is
   portable by construction: the processing host resolves its own machine tag, data
   paths, and output dirs from `cstar_forge.config`, so nothing host-specific is
   baked in at config time.

---

## 5. Suggested implementation sequencing (low-risk)

1. **[DONE — draft]** Define `SpecConfig` + `to_yaml`/`from_yaml`
   (`cstar_forge/spec_config.py`) and a dependency-light Phase-1 resolver
   `build_spec_config` (`cstar_forge/spec_config_resolve.py`), validated against the
   `test-tiny` demo (`tests/test_spec_config.py`, `docs/spec-config-example.test-tiny.yml`).
   The resolver reads the ModelSpec YAML directly and needs no ROMS/C-Star/roms_tools
   (only `dt` via CFL is optional/lazy), so a UI backend can call it.
2. **[DONE — draft]** Phase-2 processing engine
   (`cstar_forge/spec_config_engine.py`, `tests/test_spec_config.py`): ingest a
   `SpecConfig`, resolve the **host** (machine + data paths) from `cstar_forge.config`
   on the run machine (`resolve_host` / `host_summary`), reconstruct a
   `CstarSpecBuilder` from the config's atomic inputs, run
   `ensure_source_data` → `generate_inputs`, and **overlay** the reviewed
   `model_settings` via `configure_build(...)` so config edits win over re-derived
   defaults. CLI: `python -m cstar_forge.run spec_config.yml`
   (`--host-only`, `--clobber`, `--no-{data,generate,configure,dask}`). The Forge-side
   `cstar_forge.run` auto-detects the host and injects it; the app engine
   (`cstar_forge.forge.spec_config_engine`) is host-independent.
2b. **[DONE — schema identity]** ``SpecConfig`` carries an ``application`` discriminator
   (default ``"roms_marbl"`` — the target C-Star app) and ``spec_config_version``;
   ``from_yaml`` rejects files declaring a newer version. A portability guard test keeps
   ``spec_config.py`` free of ``cstar_forge``/``cstar`` imports (so it stays
   C-Star-relocatable), and a schema round-trip test pins config↔YAML identity. Version
   is bumped only on *breaking* changes; additive fields (with defaults) stay loadable.

   **[TODO — at the C-Star migration, NOT before]** Add a *byte-exact golden namelist*
   test: run ``process_spec_config`` on the example and assert the generated
   ``namelist.nml`` matches a committed fixture. Deferred deliberately — it churns on
   every schema/default change, so it earns its keep only as a behavior-preservation
   snapshot right before moving the engine into C-Star. (See the skipped stub in
   ``tests/test_spec_config.py``.) Until then, behavior is guarded by the resolver↔builder
   parity test + round-trip/validation invariants (which don't churn on field changes).

3. Reconcile the Phase-1 resolver with the live builder so the two paths can't drift.
   - **[DONE — parity net]** `TestResolverBuilderParity` (`tests/test_spec_config.py`,
     `@pytest.mark.integration`) builds a `SpecConfig` two ways for several domains —
     the resolver vs. a real `CstarSpecBuilder` (no mocks: real ModelSpec defaults +
     real geometric grid, persistence isolated to a temp catalog) — and asserts the
     **CFL `dt`/`ntimes`, `v_sponge`, and every shared default section are identical**.
     Compared at construction; `param`/`cppdefs` obc (set by the builder's grid handler
     during generation) and the host/artifact sections are excluded there but were
     confirmed equal by the real test-tiny end-to-end run.
   - **[TODO — consolidate]** Have `CstarSpecBuilder.model_post_init` *delegate* to
     `build_spec_config` (one source of truth) rather than re-derive in parallel.
     *(Riskiest step — the live derived logic is split between `_init_settings_run_time`
     and the `input_data` grid handler and entangled with `self.grid` + blueprint
     persistence. Until then, the parity net guards drift and Phase 2 treats the
     config's `model_settings` as authoritative via the `configure_build` overlay.)*
4. Keep `CstarSpecBuilder` as a thin facade for back-compat: `builder.spec_config`
   returns the resolved `SpecConfig`; `builder.generate_inputs()` delegates to
   `process_spec_config`.
5. Add `ForcingSpec` to the catalog and wire the composable-piece selection (model /
   domain / forcing) + the UI on top of `build_spec_config`.

---

## 6. Key hardcoded constants to surface (today buried in code)

| Constant | Value | File |
|---|---|---|
| GLORYS `dataset_id` | `cmems_mod_glo_phy…0.083deg_P1D-m` | `source_data.py` |
| SRTM15 version / URL | `V2.7` / topex.ucsd.edu | `source_data.py` |
| MBL_CO2 URL | gml.noaa.gov/ccgg/mbl | `source_data.py` |
| WOA download URL | ncei.noaa.gov WOA18 | `source_data.py` |
| TPXO version | `TPXO10.v2` | `source_data.py` |
| source alias map | `GLORYS→GLORYS_REGIONAL`, `UNIFIED→UNIFIED_BGC`, … | `source_data.py` |
| CDR stem / file | `cdr` / `cdr.nc` | `input_data.py` |
| nesting/extract file | `nesting.nc` | `input_data.py` |
| `nsub_x`, `nsub_e` | `1`, `1` | `input_data.py` |
| `nrrec` (initial) | `1` | `input_data.py` |
| tides flags | `bry_tides=True`, `pot_tides=True`, `ana_tides=False` | `input_data.py` |
| `ndtfast`, `ninfo` | `60`, `1` | `_core.py` |
| nesting period fallback | `3600.0` | `util.py` |
| NERSC / RCAC account | `m4632` / `ees250129` | `catalog/Machines/*.yml` |
| `pes_per_node` (both HPC) | `128` | `catalog/Machines/*.yml` |

These split three ways:

* **Snapshot into `SpecConfig`** — the data-source identifiers (GLORYS `dataset_id`,
  URLs, version pins, alias map) become module-level named constants the Phase-1
  resolver reads and writes into `sources.resolved_datasets`, so the processing host
  uses exactly those values.
* **Land in a `model_settings` section** — `nsub_x`/`nsub_e` (→ `param`),
  `ndtfast`/`ninfo` (→ `time_stepping`), tide flags (→ `tides`). They are reviewable
  there; processing may overwrite the generation-specific ones.
* **Stay as deterministic processing constants (not stored)** — the `cdr.nc` /
  `nesting.nc` filenames, `nrrec`, the nesting-period fallback (from `roms_tools`),
  and the machine account / `pes_per_node` (resolved on the run host).
