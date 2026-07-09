# Developer Guide (current state, 2026-07-09, branch `refactor`)

This supersedes the mental model in `docs/overview.md` / `docs/CStarSpecBuilder-overview.md`
/ `docs/machine-config.md` / `docs/spec-config-inventory.md`, which describe the
pre-refactor `_core.py` / `CstarSpecBuilder` design. Those files are stale (see
"Stale docs" at the end) — start here.

## 1. The big picture

Forge is splitting into two layers along a hard boundary, in preparation for moving
the execution half into C-Star as an "application":

- **Authoring** (stays in this repo): the catalog of reusable pieces (Model/Domain/
  Forcing/Output specs), a Phase-1 **resolver** that assembles them into a single
  reviewable file, and a wizard UI.
- **Execution** (`cstar_forge/forge/`, target: relocates into C-Star wholesale):
  a Phase-2 **engine** that turns that file into ROMS-MARBL input NetCDFs, a
  namelist, and a downstream blueprint. It never touches the catalog.

The file that crosses the boundary is `SpecConfig` — **the forge application's own
blueprint**. Terminology trap to avoid: C-Star also has an existing, unrelated
`roms_marbl` application whose blueprint (`RomsMarblBlueprint`) forge *emits as an
output artifact*. "Building a blueprint" in old code/docs means producing that
downstream artifact, not forge's own input.

```
 catalog pieces ─┐
 (Model/Domain/  ├─► build_spec_config() ─► SpecConfig ─► process_spec_config(cfg, host)
  Forcing/Output)│      (Phase 1, resolver)   (.yml,          (Phase 2, engine)
                 │                             portable)           │
 wizard UI ──────┘                                                 ▼
                                                     input NetCDFs, namelist.nml,
                                                     cppdefs.opt, roms_marbl blueprint
```

`CstarSpecBuilder`/`CstarSpecEngine`/`_core.py` **no longer exist** — fully deleted
and replaced by the above. `git log` for that deletion: the branch carries ~20
commits of decomposition work (see `docs/architecture-decomposition-plan.md` and
`docs/executor-portability-plan.md`, which record the actual decisions made).

## 2. Directory map

```
cstar_forge/
  catalog.py                 thin back-compat shim: BlueprintCatalog → DomainCatalog
  domain_catalog.py           DomainCatalog: scans catalog/{ModelSpec,DomainSpec,
                               ForcingSpec,OutputSpec,Machines,blueprints}, exposes
                               *_data()/*_path() accessors + blueprintDF()
  domain_catalog_sketch.py    dead prototype, unreferenced anywhere — candidate for deletion
  spec_config_resolve.py      Phase-1 resolver: build_spec_config(...)
  spec_config_wizard.py       SpecConfigWizard (ipywidgets UI), thin shell over the resolver
  models.py                   Pydantic wrappers for model.yml (ModelSpec, GridInput,
                               ForcingInput, load_models_yaml); imports its forcing/IC/
                               OpenBoundaries item models FROM forge.spec_config (single
                               source, no duplicates — see §5)
  config.py                   DataPaths / MachineConfig / resolve_host() — authoring-side
                               host detection (NERSC/RCAC/macOS), used by run.py only
  run.py                      CLI: `python -m cstar_forge.run spec_config.yml`; resolves
                               host, calls forge.spec_config_engine.process_spec_config
  catalog/                    ModelSpec/, DomainSpec/, ForcingSpec/, OutputSpec/,
                               Machines/, blueprints/ — the YAML data the catalog scans

  forge/                      THE FORGE APPLICATION — execution engine, target: relocates
                               into C-Star as one unit. Nothing here reads the catalog.
    spec_config.py            SpecConfig (the blueprint) + item models + enums.
                               Dependency-light: stdlib + pydantic + yaml only (portability
                               guard test enforces this — no cstar_forge/cstar imports)
    spec_config_engine.py      process_spec_config(); SpecConfigExecutor Protocol (the
                               C-Star substitution seam); sources_to_forcing_override();
                               spec_config_to_builder_kwargs(); verify_content_hash()
    executor.py                ForgeExecutor (ex-CstarSpecBuilder, ~2000 lines) — the
                               actual work: ensure_source_data, generate_inputs,
                               configure_build, blueprint persistence, template staging
    input_data.py               RomsMarblInputData — grid/IC/forcing/river/CDR/nesting
                               NetCDF generation (roms_tools calls live here)
    source_data.py              SourceData — dataset download/staging (GLORYS, ERA5,
                               SRTM15, WOA, TPXO, GLOFAS, ...)
    source_registry.py          single source of truth for dataset aliases/URLs/versions/
                               UNSTAGED_DATASETS/STREAMABLE_SOURCES; source_data.py re-exports it
    settings.py                 Jinja2 cppdefs.opt rendering + write_roms_namelist()
    namelist_model.py            RunTimeSettings (validates the namelist vocabulary) +
                               build_namelist() → cstar.roms.namelist.RomsNamelist
    util.py                      CFL dt calc, nesting-period writer
    host.py                     HostPaths — frozen 4-field injected host contract
                               (working_dir, source_data_cache, system, machine_config)
    _yaml_representers.py       registers a PyYAML Enum representer globally (side-effect
                               import) so Forge enums survive roms-tools' SafeDumper
```

## 3. `SpecConfig` — the forge blueprint

Defined in `cstar_forge/forge/spec_config.py`. Top-level shape:
`spec_config_version` (int, bump only on breaking change) · `application` (=`"forge"`,
C-Star app discriminator) · `identity` (model_name, grid_name, ensemble_id, description)
· `run` (start/end date, model_reference_date) · `domain` (grid_kwargs, topography_source,
open_boundaries, partitioning, nesting) · `forcing` (flat: initial_conditions,
surface/boundary/tidal/river lists, cdr_forcing, resolved_datasets) · `datasets`
(host-independent list of resolved dataset keys) · `model_settings` (flat dict: cppdefs +
~35 namelist sections) · `code` (roms/marbl repos + `templates_compile_time`/`_run_time`
repo refs) · `composition` (which catalog pieces produced this + overrides layer) ·
`provenance` (generated_at, content_hash, notes).

- **`working_dir`** (default `~/cstar-forge-data`) is the single per-run artifact root —
  everything the executor *produces* lands under it. It's host/location, not
  results-affecting, so it's excluded from `content_hash`.
- **`content_hash()`** — sha256 over everything *except* `spec_config_version`,
  `identity`, `composition`, `provenance`, `working_dir`, plus (as of 2026-07-09) each
  code repo's `location` field (`code.roms`/`marbl`/`templates_compile_time`/
  `templates_run_time` — the fetch address, not the pinned commit/branch/files). Stamped
  on `to_yaml`; `verify_content_hash` warns (doesn't block) on a mismatched hand-edit at
  load. See §6 item 4 for the remaining piece (templates still pinned by branch, not
  commit).

## 4. The call chain end to end

**Authoring (Phase 1):**
1. `wiz = SpecConfigWizard()` (spec_config_wizard.py) — scans the catalog via
   `domain_catalog.default_catalog`, populates dropdowns.
2. User picks a domain → `_on_domain()` prefills grid/boundaries/partitioning/dates from
   `catalog.domain_data(name)`.
3. Every edit → `_rebuild()` → `build_spec_config(**self._gather())`
   (spec_config_resolve.py) — reads `model.yml` + `*-defaults.yml` directly as dicts (no
   Pydantic here), resolves dataset keys via `source_registry`, computes pure-derived
   settings (CFL `dt`, `v_sponge`, etc.), returns a `SpecConfig`.
4. `wiz.config.to_yaml(path)` writes the portable `spec_config.yml`.

**Execution (Phase 2), same machine or a different one:**
5. `python -m cstar_forge.run spec_config.yml` (run.py) — resolves the host via
   `cstar_forge.config.resolve_host()` (machine tag, `source_data_cache`,
   `working_dir` override).
6. `forge.spec_config_engine.process_spec_config(cfg, host, ...)` builds a
   `ForgeExecutor` via `ForgeExecutor.from_spec_config(cfg, host)` and drives:
   `ensure_source_data()` → `generate_inputs()` → `configure_build()`.
7. Outputs land under `host.working_dir`: input NetCDFs, `namelist.nml`, `cppdefs.opt`,
   the blueprint YAML (`B_{name}_{stage}.yml`), and the emitted downstream `roms_marbl`
   blueprint.

`ForgeExecutor` never imports `cstar_forge.config`/`catalog`/`domain_catalog`/
`spec_config_resolve`/`spec_config_wizard` — verified both by grep and by
`tests/test_forge_app_boundary.py` (an AST-based guard with an empty, actively-enforced
violation allowlist).

`namelist_model.py` and `util.py` were moved into `cstar_forge/forge/` (2026-07-09, see §6
item 1) — `forge/settings.py`/`forge/spec_config_engine.py` import
`cstar_forge.forge.namelist_model` and `forge/input_data.py` imports `cstar_forge.forge.util`
as same-package siblings now, not top-level reach-ups. Both modules are also now in the
boundary guard's `_FORGE_APP_MODULES` list.

## 5. `models.py` vs `forge/spec_config.py` (Phase D — done)

Earlier project memory described two parallel item-model schemas kept in sync by a
"lockstep drift guard" test. **That has been resolved**: `models.py` now imports its
forcing/IC item models (`BoundaryForcingItem`, `SurfaceForcingItem`, `InitialConditions`,
etc.) directly from `forge.spec_config` — single source of truth, no duplication, no
drift guard needed for item models. What `models.py` still owns is the `model.yml`
*wrapper* shape (`ModelSpec`, `GridInput`, `ForcingInput`, `load_models_yaml`) used by
`domain_catalog.load_model_spec()` for full Pydantic validation at catalog-registration
time — a heavier, separate path from the resolver's plain-dict read of the same file.

What does still guard drift: `tests/test_roms_tools_coverage.py` (roms-tools option
coverage) and a resolver/executor settings-parity assertion in `test_spec_config.py` —
different concern, not the retired item-model duplication.

**Fixed (2026-07-09):** `OpenBoundaries` used to be defined twice — `models.py`
(`BaseModel`, `extra="forbid"`) and `forge/spec_config.py` (`_Section`) — with
`ForgeExecutor`/`RomsMarblInputData` depending on the `models.py` copy for no good reason
(it was the one place either module reached into `cstar_forge.models`, an
authoring-flavored module that also carries catalog-facing `load_models_yaml`). Both
`forge/executor.py` and `forge/input_data.py` now import `OpenBoundaries` straight from
`forge.spec_config`; `models.py` re-exports it from there like the other 6 item models.
`cstar_forge.forge` no longer imports `cstar_forge.models` at all.

## 6. Known gaps / open items (see also §7 for stale docs)

Ranked roughly by what's worth doing next:

1. ~~Two top-level utility modules block a clean relocation.~~ **DONE (2026-07-09):**
   `namelist_model.py` and `util.py` moved into `cstar_forge/forge/`; all imports across
   `cstar_forge/`, `tests/`, and docs updated; both added to the boundary guard's
   `_FORGE_APP_MODULES`. `cstar_forge/forge/` is now a clean, self-contained relocation
   unit for these two as well.
2. **Template staging has no CI coverage for the cross-repo flat-staging contract**
   (`ForgeExecutor._stage_templates`, executor.py ~L1017). Rendering silently assumes
   C-Star's `AdditionalCode` stages filtered files *flat*; only manually verified once
   against the real remote. A `@pytest.mark.slow` network test staging from the real repo
   would close this — flagged in-code and in `docs/executor-portability-plan.md`, still
   open.
3. **Templates are re-fetched every `configure_build`** (rmtree + re-clone under
   `working_dir/templates/<stage>`), not cached like source data / code. A commit-keyed
   template cache (mirroring `source_data_cache`) would fix this and set up item 4.
4. ~~`code.location` participates in `content_hash`.~~ **PARTIALLY DONE (2026-07-09):**
   `content_hash()` now scrubs `location` (the fetch address) from every code repo
   (`code.roms`, `code.marbl`, `code.templates_compile_time`, `code.templates_run_time`)
   before hashing — only `commit`/`branch`/`directory`/`files` are results-affecting, so a
   mirror/local-path change no longer perturbs the hash (test:
   `test_content_hash_ignores_code_repo_location`). **Still open:** templates are still
   pinned by `branch` (`main`), not commit — `_build_code` in `spec_config_resolve.py`
   already reads a `templates.commit:` hook from `model.yml` when set (the wiring exists),
   but no ModelSpec has actually set it yet; that's a data/coordination decision for
   post-merge (a real commit SHA can't be pinned before `refactor` lands on `main`), not a
   code gap.
5. **`refactor` has not been merged to `main` yet** (currently `main`+38 commits, 0 behind
   — a clean fast-forward candidate per `docs/executor-portability-plan.md` and prior
   audit). The raw-URL template-fetch path
   (`raw.githubusercontent.com/.../main/templates/...`) only resolves post-merge and is
   unverified against the real remote.
6. **Two compile-time-settings TODOs in `input_data.py`** (~L843, ~L1016): surface and
   boundary forcing generation don't yet update `self._settings_compile_time` with their
   related cppdefs — flagged in-code as real unfinished wiring, not just a note.
7. **Stale docstring** in `forge/spec_config.py` (L42-44) still says "this module is not
   yet wired into `ForgeExecutor`" — it has been for a while now; delete/update.
8. **`examples/spec_config.yml` / `spec_config2.yml` / `spec_config3.yml`** still stamp
   `application: roms_marbl` (pre-rename); `spec_config_new.yml` and
   `docs/spec-config-example.test-tiny.yml` already say `forge`. Regenerate or delete the
   stale ones — not load-tested by anything, low urgency.
9. **`domain_catalog_sketch.py`** (167 lines) — dead prototype, zero references anywhere
   (code, tests, notebooks, docs). Safe to delete.
10. **Architecture doc headers understate progress.** `docs/architecture-decomposition-plan.md`
    still says "Status: proposal… Not started" — Phases 0/B/C/D described in that doc are
    actually complete. Worth a status update so a new reader isn't misled.

**Good news / already resolved that older memory implied was still open:**
- A **settings-level** golden test exists now: `test_golden_model_settings_test_tiny`
  (test_spec_config.py:201) diffs resolved `model_settings` against a committed JSON
  fixture (`tests/fixtures/golden_model_settings_test-tiny.json`). This is a real,
  currently-running behavior-preservation check. **It is not** the specifically deferred
  *byte-exact `namelist.nml`* golden that `docs/spec-config-inventory.md` and
  `docs/architecture-decomposition-plan.md` describe (that one was explicitly deferred
  "until the C-Star migration" to avoid churn) — no skipped stub for it was found, and no
  test writes/diffs an actual `namelist.nml` file byte-for-byte. Treat the namelist-level
  golden as **still open**, with a lighter-weight settings-level proxy already in place.
- `.gitignore` now excludes `.ipynb_checkpoints` (untracked stale checkpoint files still
  exist on disk locally but aren't committed — harmless, can delete opportunistically).
- Full test suite: **487 passed, 0 failed** as of this audit.

## 7. Docs that referenced deleted `_core.py`/`CstarSpecBuilder`/`CstarSpecEngine` — FIXED (2026-07-09)

All 7 were live pages in the published MyST docs site (`myst.yml` TOC), so none were
deleted (that would need TOC restructuring); each was fixed in place instead:

- `docs/CStarSpecBuilder-overview.md` — added a banner noting the class is gone
  (decomposed into `SpecConfig` + `ForgeExecutor`); updated the mermaid diagram and stage
  descriptions to show `build_spec_config` → `SpecConfig` → `ForgeExecutor.from_spec_config`.
- `docs/InputData-intro.md`, `docs/InputData-RomsMarblInputData.md` — only ~4 lines of
  actual drift in 803 combined lines (the conceptual content was still accurate). Fixed
  the import path (`cstar_forge.forge.input_data`), the `RomsMarblInputData` constructor
  example (verified current fields against `tests/test_input_data.py` — `model_spec`/
  `model_name`/`grid_name` are gone, replaced by `domain_name` + `forcing_override`), and
  the `CstarSpecBuilder.generate_inputs()` → `ForgeExecutor.generate_inputs()` references.
- `docs/overview.md` — updated the project-structure tree to show `cstar_forge/forge/`
  and the resolver/wizard modules; added a pointer to this guide.
- `docs/machine-config.md` — only 2 lines were stale (the rest — `DataPaths`,
  `config.paths.*`, system detection — is still accurate, verified against current
  `config.py`); replaced the dead `CstarSpecBuilder(catalog_root=...)` example with an
  accurate description of `config.resolve_host()` → `HostPaths`.
- `docs/roms-tools-options-integration.md` — intentionally a historical record (per its
  own banner); added a note that its file paths/class names have since moved, without
  rewriting the historical narrative.
- `docs/spec-config-inventory.md` — the original planning doc that motivated this whole
  refactor; added a "historical/superseded" banner pointing here, kept the content (the
  six-sources-of-truth model and hardcoded-constants table still have reference value).
