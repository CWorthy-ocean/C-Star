# Executor portability plan — make the forge application fully relocatable

**Status:** decided; executing. Follows the decomposition (Phases 0/B/C/D + host seam,
allowlist 5→0). This closes the last coupling: `ForgeExecutor` still reads
`cstar_forge.config` and the authoring `DomainCatalog`/`ModelSpec`. Goal: the executor
reads **only** the `SpecConfig` + an injected runtime location, so the entire
`cstar_forge/forge/` package relocates into C-Star untouched.

## Governing principle (from the design discussion)

- **Everything the executor PRODUCES** (input netCDFs, `namelist.nml`, `cppdefs.opt`, the
  emitted roms_marbl blueprint, build artifacts) lands under one **`working_dir`** — the
  per-run artifact root, matching how C-Star defines where a run's outputs go.
- **Everything the executor READS** (ModelSpec/catalog content) is **fully resolved into
  the `SpecConfig`** by the Phase-1 resolver, so the executor never opens the catalog.
- **Division:** the **resolver (Phase 1, authoring, Forge-side)** is the *only* thing that
  reads the catalog/ModelSpec; it bakes everything into the `SpecConfig`. The **executor
  (Phase 2)** reads only the `SpecConfig` + injected host. The catalog/ModelSpec is an
  authoring-time input, never a processing-time one.

## Decisions (all settled)

1. **`working_dir`** — a **stored** `SpecConfig` field with a wizard default
   (`~/cstar-forge-data`), **overridden at runtime** by C-Star / Forge's executor. It is
   host/location, not results-affecting → **excluded from `content_hash`** (a host-swapped
   `working_dir` must not change the hash). Today's split outputs (`config.paths.input_data`
   / `scratch/casename` / `catalog/blueprints` / `catalog/builds`) collapse under it.
2. **`datasets`** — store an explicit resolved list of host-independent dataset **keys**
   (`GLORYS_REGIONAL`, `ETOPO5`, …); cache paths resolve at processing from the injected
   `source_data_cache`. **Results-affecting → stays IN the hash.**
3. **`source_data` cache** — a separate injected host cache (shared across runs), not under
   `working_dir`.
4. **Templates** — fetched from the repo refs already in `cfg.code.templates_*` (like code),
   not embedded.
5. **Settings** — `cfg.model_settings` is authoritative; the executor uses it directly and
   stops re-deriving defaults from `model_spec.settings` + overlaying.
6. **`code`** — build the cstar `ROMSCompositeCodeRepository` from `cfg.code` at processing
   (shape-mapping, not new data).
7. **Drop + delete**: `override` (settings-override files), `dump`/`load` (superseded by
   `SpecConfig.to_yaml`/`from_yaml`), and the now-obsolete `catalog_root` /
   `initialize_catalog_from` / `initialize_catalog_clobber` / `suppress_catalog_validation`.
8. **Breadcrumbs** — the spec stays self-contained for producing results, **and** retains
   `composition` (PieceRefs: which model/domain/forcing/output catalog pieces + origin) and
   `provenance` as catalog-source metadata (both already `_HASH_EXCLUDE`d). The executor
   ignores them; they ride along for a *later, detached* C-Star→catalog callback. Extending
   them (catalog location/commit + item ids) is that future work item — leave the hook.
9. **Reduced `HostPaths`** = `{working_dir, source_data_cache, system, machine_config}`
   (`input_data`/`scratch`/`catalog` fold into `working_dir`).

## Phased steps (verify green at each)

1. **Schema** (`forge/spec_config.py`): add `working_dir` (default `~/cstar-forge-data`) and
   `datasets: list[str]`; `working_dir` → `_HASH_EXCLUDE`; `datasets` stays hashed. Wizard
   seeds the `working_dir` default.
2. **Resolver** (`spec_config_resolve.py`): emit `datasets` (forcing/IC sources +
   topography). Forcing/settings/code already resolved.
3. **`HostPaths`** (`forge/host.py`): reduce to the 4-field shape; `config.resolve_host` +
   `run.py` build it (working_dir default, overridable).
4. **Executor** (`forge/executor.py`): inject host; all outputs under `working_dir`; use
   `cfg.model_settings` directly; build cstar code from `cfg.code`;
   `SourceData(datasets=cfg.datasets, source_data_dir=host.source_data_cache)`; drop
   `_get_catalog`/`_load_model_spec`/`resolve_catalog_dir`/`config`; delete `override` /
   `dump` / `load` / `catalog_root` / `initialize_catalog_*` / `suppress_catalog_validation`.
5. **Bridge** (`forge/spec_config_engine.py`): `sources_to_forcing_override` always returns
   the dict (drop the `model_default` short-circuit) so the executor never needs
   `model_spec.inputs`.
6. **Guard** (`tests/test_forge_app_boundary.py`): add `executor` to `_FORGE_APP_MODULES`;
   confirm zero violations → whole `forge/` package is config/authoring-free and relocatable.
7. **Tests**: migrate `test_core` `config.paths` patches → injected host; parity tests drop
   `catalog_root`/`initialize_catalog_from` for an injected temp host; delete `dump`/`load`
   + `override` tests.

## Field-by-field checks to make while implementing
- `cfg.code` carries roms + marbl + both template repos (location/commit/files) needed to
  build the cstar `ROMSCompositeCodeRepository`.
- `cfg.forcing` on the model-default path is byte-equivalent to `model_spec.inputs`
  (parity test covers derived settings; confirm forcing items match).
- The `datasets` derivation includes topography and IC bgc sources, not just the forcing
  categories.
