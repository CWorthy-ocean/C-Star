# Executor portability plan — make the forge application fully relocatable

**Status:** DONE — this portability work has been executed; `ForgeExecutor` now reads
only the `ForgeBlueprint` + an injected `HostPaths`, and `tests/test_forge_app_boundary.py`
enforces it with an empty violation allowlist. Kept as a historical record of the plan;
for the current architecture see `docs/developer-guide.md`. (Original goal: the executor
reads **only** the `ForgeBlueprint` + an injected runtime location, so the entire
`cstar_forge/forge/` package relocates into C-Star untouched.)

## Governing principle (from the design discussion)

- **Everything the executor PRODUCES** (input netCDFs, `namelist.nml`, `cppdefs.opt`, the
  emitted roms_marbl blueprint, build artifacts) lands under one **`working_dir`** — the
  per-run artifact root, matching how C-Star defines where a run's outputs go.
- **Everything the executor READS** (ModelSpec/catalog content) is **fully resolved into
  the `ForgeBlueprint`** by the Phase-1 resolver, so the executor never opens the catalog.
- **Division:** the **resolver (Phase 1, authoring, Forge-side)** is the *only* thing that
  reads the catalog/ModelSpec; it bakes everything into the `ForgeBlueprint`. The **executor
  (Phase 2)** reads only the `ForgeBlueprint` + injected host. The catalog/ModelSpec is an
  authoring-time input, never a processing-time one.

## Decisions (all settled)

1. **`working_dir`** — a **stored** `ForgeBlueprint` field with a wizard default
   (`~/cstar-forge-data`), **overridden at runtime** by C-Star / Forge's executor. It is
   host/location, not results-affecting → **excluded from `content_hash`** (a host-swapped
   `working_dir` must not change the hash). Today's split outputs (`config.paths.input_data`
   / `scratch/casename` / `catalog/blueprints` / `catalog/builds`) collapse under it.
2. **`datasets`** — store an explicit resolved list of host-independent dataset **keys**
   (`GLORYS_REGIONAL`, `ETOPO5`, …); cache paths resolve at processing from the injected
   `source_data_cache`. **Results-affecting → stays IN the hash.**
3. **`source_data` cache** — a separate injected host cache (shared across runs), not under
   `working_dir`.
4. **Templates** — fetched from the repo refs in `cfg.code.templates_*` (like code), not
   embedded. **DONE** (2026-07-08): the render templates (`cppdefs.opt.j2`, `marbl_in`)
   moved out of the bundled ModelSpec to repo-root `templates/` (decoupled — they track a
   ROMS version and may move into ucla-roms later). `ForgeExecutor._stage_templates(stage)`
   materializes them at processing via C-Star's `AdditionalCode` (remote repo → raw-file
   fetch; local dir → copy) under `host.working_dir/templates/<stage>`; the old
   `_template_dir` bundled-catalog read (`import cstar_forge` for `__file__`) is gone, so
   the executor no longer touches the package/catalog. Tests stage offline from the working
   tree via a conftest seam that redirects `location` (real `AdditionalCode` local-copy,
   no network/clone/mock). **Deferred reproducibility follow-up:** the resolver still pins
   the template repo by `branch` (`main`), not a commit, and `code.templates_*.location`
   participates in `content_hash` — so a template edit changes build output without a hash
   bump, and a local test `location` perturbs the (unasserted) hash. Model.yml has a
   `templates.commit:` pin hook; the principled fix is to pin a commit and hash the
   template *version* (commit/directory/files) rather than the fetch `location`.
5. **Settings** — `cfg.model_settings` is authoritative; the executor uses it directly and
   stops re-deriving defaults from `model_spec.settings` + overlaying.
6. **`code`** — build the cstar `ROMSCompositeCodeRepository` from `cfg.code` at processing
   (shape-mapping, not new data).
7. **Drop + delete**: `override` (settings-override files), `dump`/`load` (superseded by
   `ForgeBlueprint.to_yaml`/`from_yaml`), and the now-obsolete `catalog_root` /
   `initialize_catalog_from` / `initialize_catalog_clobber` / `suppress_catalog_validation`.
8. **Breadcrumbs** — the spec stays self-contained for producing results, **and** retains
   `composition` (PieceRefs: which model/domain/forcing/output catalog pieces + origin) and
   `provenance` as catalog-source metadata (both already `_HASH_EXCLUDE`d). The executor
   ignores them; they ride along for a *later, detached* C-Star→catalog callback. Extending
   them (catalog location/commit + item ids) is that future work item — leave the hook.
9. **Reduced `HostPaths`** = `{working_dir, source_data_cache, system, machine_config}`
   (`input_data`/`scratch`/`catalog` fold into `working_dir`).

## Phased steps (verify green at each)

1. **Schema** (`forge/forge_blueprint.py`): add `working_dir` (default `~/cstar-forge-data`) and
   `datasets: list[str]`; `working_dir` → `_HASH_EXCLUDE`; `datasets` stays hashed. Wizard
   seeds the `working_dir` default.
2. **Resolver** (`forge_blueprint_resolve.py`): emit `datasets` (forcing/IC sources +
   topography). Forcing/settings/code already resolved.
3. **`HostPaths`** (`forge/host.py`): reduce to the 4-field shape; `config.resolve_host` +
   `run.py` build it (working_dir default, overridable).
4. **Executor** (`forge/executor.py`): inject host; all outputs under `working_dir`; use
   `cfg.model_settings` directly; build cstar code from `cfg.code`;
   `SourceData(datasets=cfg.datasets, source_data_dir=host.source_data_cache)`; drop
   `_get_catalog`/`_load_model_spec`/`resolve_catalog_dir`/`config`; delete `override` /
   `dump` / `load` / `catalog_root` / `initialize_catalog_*` / `suppress_catalog_validation`.
5. **Bridge** (`forge/forge_blueprint_engine.py`): `sources_to_forcing_override` always returns
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

## Session review (2026-07-08) — decision #4 landed; remaining to-dos

Decision #4 (templates via git) is implemented and green (475 tests, ruff clean, boundary
guard green, hash provably unchanged). Reviewed for consistency with the decisions above; no
defects, only the to-dos below. Separated by origin so a later reader knows what this session
introduced vs. what predates it.

### Session-introduced — worth resolving
1. **Flat-staging cross-repo contract is unguarded in CI (highest value).** The remote path
   depends on C-Star's `AdditionalCode` staging filtered files *flat* into `local_dir`
   (`dest/cppdefs.opt.j2`, not `dest/<subdir>/…`), because `render_roms_settings` reads
   `template_dir/<file>`. Verified once manually against the real `REMOTE_REPOSITORY` path;
   the offline seam forces `subdir=""`, so CI never exercises subdir-preserving behavior. If
   C-Star changes its stager layout, rendering breaks with file-not-found and no test fails.
   Documented as an explicit assumption in `ForgeExecutor._stage_templates`. TODO: add one
   `@pytest.mark.slow`/network integration test that stages from the real remote once.
2. **Template staging ignores the decision-#3 cache pattern.** Templates are immutable-per-
   commit external refs — same category as source data and code — but `_stage_templates`
   puts them in a per-run `working_dir/templates/<stage>` subdir that is `rmtree`'d and
   re-fetched every `configure_build` (a branch-pinned remote fetch re-hits the network each
   run). Decision #3 puts external fetches in a *shared, cross-run cache, not under
   `working_dir`*. TODO (single fix collapses three concerns): a **commit-keyed template
   cache** (à la `source_data_cache`) would align decision #3, remove the re-fetch, and set
   up the reproducibility pin below.
3. **Post-merge validation.** The production raw URL
   (`raw.githubusercontent.com/CWorthy-ocean/cstar-forge/main/templates/compile-time/…`)
   only resolves once `refactor` merges to `main` (that is where repo-root `templates/`
   lands). TODO: after merge, confirm the real remote fetch serves the files.

### Pre-existing tensions (not regressions) — decide deliberately
4. **`code.location` is in `content_hash`.** Not a decision-#1 violation in practice: the
   resolver only ever persists the GitHub URL (host-independent), and the local-path override
   is transient/test-only (never saved, never asserted). But it isn't the principled end-
   state — the hash should capture the template *version* (commit/directory/files), not the
   fetch *location*. Ties to to-do #2: pin `templates.commit:` (hook already in model.yml)
   and refine the hash to exclude `location`.
5. **`examples/forge_blueprint*.yml` carry stale `application: roms_marbl`** (should be `forge`).
   Confirmed **not load-tested** and referenced only as a doc follow-up → dead docs. TODO:
   regenerate or delete (low urgency). Their template blocks (`directory: templates/…`) are
   already forward-compatible.
6. **Stale `.ipynb_checkpoints/*.yml`** — `cstar_forge/.ipynb_checkpoints/models-checkpoint.yml`
   references the deleted `models.yml`; `…/ModelSpec/…/.ipynb_checkpoints/model-checkpoint.yml`
   has the pre-move template block. Not loaded by anything. TODO: delete + gitignore
   `.ipynb_checkpoints/`.

### Nits (no action needed)
- `docs/overview.md` line 57 labels `catalog/ModelSpec/` as "Model templates and defaults" —
  now only the `*-defaults.yml` live there (render templates moved out).
- The offline test seam's `_local_args` omits the `or ""` guard the real `_template_repo_args`
  has (test-only; resolver always sets `directory`).

### Follow-up filed 2026-07-08 — SRTM15 dataset-key aliasing (topography)
Surfaced while unblocking a real wizard spec (`ValueError: Unknown dataset(s): DAI, ETOPO5`).
Root cause was decision-#2's `datasets` derivation being too broad: it emits *every* resolved
dataset key, including keys Forge doesn't stage. **Fixed (part a):** `source_registry`
declares `UNSTAGED_DATASETS = {"ETOPO5", "DAI"}` (ETOPO5 is fetched by roms-tools at grid
build; DAI is a streamed placeholder with no handler); `SourceData` validation and
`prepare_all` skip these instead of rejecting them, while genuine typos still raise. Tests in
`tests/test_source_data.py` exercise the unmocked path.

**RESOLVED (part b, task #16) — implemented 2026-07-09 via option (a).** SRTM15 topography
had never been functional (the "fix" was a first implementation, not a key rename). Wired up
as three coupled changes:
1. **Key reconciliation** — `SOURCE_ALIAS["SRTM15"]` and the `DATASET_METADATA` key now use
   the un-versioned `"SRTM15"` (matching the `@register_dataset("SRTM15")` handler); the
   version stays in the URL/filename constant, mirroring GLORYS. Note `content_hash`: `datasets`
   is hashed, so SRTM15-topo specs' key changed `SRTM15_V2.7`→`SRTM15` (intentional — no
   functional SRTM15 specs existed to migrate); ETOPO5 specs are unaffected.
2. **Injection** — `ForgeExecutor` gained a `topography_source: str` field (fed from
   `cfg.domain.topography_source`, coerced enum→value at the `forge_blueprint_engine` boundary). A
   new `_resolve_topography_source()` stages the topo file and returns
   `{"name":"SRTM15","path": <staged>}` (or `None` for ETOPO5). `name` is a plain string, never
   the `TopographySource` enum, so the injected dict is safe when `grid.to_yaml` runs.
3. **Ordering (option a)** — the topo dict is injected into every `grid_kwargs`/`_parent`/
   `_child` at the top of `model_post_init`, before the `rt.Grid` calls. The chosen justification
   (better than the doc's original framing): `rt.Grid` *already* downloads topography during
   construction (ETOPO5 via pooch), so staging SRTM15 there introduces no new behavior class.
   Option (b) (lazy grid-build) was rejected as a larger touch fighting the existing design.

Tests: `test_srtm15_key_reconciles_to_handler` (replaces the old landmine guard),
`test_model_post_init_srtm15_injects_topography_source` and
`test_model_post_init_etopo5_leaves_grid_kwargs_untouched` prove the wiring at `rt.Grid`
(the silent-failure point). 480 tests pass. NOT verified end-to-end against a live SRTM15
download (multi-GB) — the rt.Grid call is asserted with staging mocked.

### Still-open larger items (unchanged from prior sessions)
- ForgeBlueprint → C-Star `forge` application migration (executor + engine + ForgeBlueprint-as-
  blueprint). Seams are in place (portability, executor factory, ForgeBlueprintExecutor Protocol).
- Detached: C-Star → catalog callback breadcrumbs (extend `composition`/`provenance`).
