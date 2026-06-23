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

## 3. The config / processing boundary (the L5 split)

Derived (L5) values fall into two classes, and the split between them **is** the
boundary between the two refactored phases:

**Pure functions of config inputs** — resolvable at config time, frozen into the
authoritative file:
- `time_stepping.{dt,ntimes,ndtfast,ninfo}`
- `v_sponge.v_sponge`
- `extract_data.extract_period`
- `param.{llm,mmm,n,np_xi,np_eta,nsub_x,nsub_e}`
- `cppdefs.obc_{west,east,north,south}`
- `title.casename`, `output_root_name`

These need only `grid_kwargs` + dates + partitioning (+ a lightweight `rt.Grid` for
grid spacing `ds`). No source downloads, no generated NetCDF.

**Functions of materialized artifacts** — outputs of processing, **must not** be in
the authoritative input file (they belong in the resulting blueprint):
- `s_coord.{theta_s,theta_b,tcline}` (read from the generated grid file)
- `grid.grid_file`, `initial.initial_file`, all `forcing.*_path`

> **Design rule:** the authoritative `SpecConfig` is **input-only**. The existing
> blueprint YAML remains the **output** that captures artifact-derived values. This
> avoids a chicken-and-egg where the authoritative file would contain values that
> require running the very thing it configures.

---

## 4. Proposed two-phase flow

```
   [Phase 1: COLLECTION / CURATION]          [authoritative file]      [Phase 2: PROCESSING]
 user args ─┐
 model.yml ─┤
 defaults  ─┼─► resolve + merge + validate ─► spec_config.yml ─► Engine.run(spec_config)
 machine   ─┤   (+ compute pure-derived L5)    (reviewable)       generate_inputs + configure_build
 hardcoded ─┘                                                     → blueprint + NetCDF + namelist
```

Phase 1 is cheap, reviewable, diffable, and portable (no `rt.Grid`, no downloads, no
file I/O). Phase 2 becomes a near-pure function `(SpecConfig, filesystem) → artifacts`.

Two choices that make "review here, process elsewhere" work:
1. **Inline resolved values, don't reference them.** Hardcoded registries/URLs (L4)
   and `roms_tools` defaults (L6) are *snapshotted* into the file, so a reviewer sees
   the real GLORYS `dataset_id`/URL and the processing host can't silently drift.
2. **Make `execution.paths`/`machine` re-resolvable at ingest.** The file is generated
   on machine A but processed on machine B; a `--paths-from-env` switch lets the same
   `spec_config.yml` be portable.

---

## 5. Suggested implementation sequencing (low-risk)

1. Define `SpecConfig` + `to_yaml`/`from_yaml` (done as a draft in
   `cstar_forge/spec_config.py`); have `model_post_init` populate it *alongside*
   current behavior. Add a golden-file test that the serialized YAML is stable.
2. Split `_init_settings_run_time` into `_settings_static()` (defaults ⊕ overrides,
   no I/O) and `_settings_derived(grid_kwargs, dates, partitioning)` (the CFL /
   `v_sponge` / `ntimes` / `param` / `obc` math). Prove byte-identical settings via
   the existing test suite. *(This is the riskiest step — the current logic is
   entangled with `self.grid` and persistence.)*
3. Route `generate_inputs` / `configure_build` to read exclusively from `SpecConfig`.
4. Add a standalone `forge process spec_config.yml` entrypoint; flip path/machine
   resolution to ingest-time.
5. Keep `CstarSpecBuilder` as a thin facade for back-compat: `builder.spec_config`
   returns the resolved `SpecConfig`; `builder.generate_inputs()` becomes
   `SpecEngine.run(self.spec_config)`.

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

These should become module-level named constants the Phase-1 resolver reads and
writes into `SpecConfig` (`sources.resolved_datasets` / a `conventions` block), so
they are visible and overridable instead of implicit.
