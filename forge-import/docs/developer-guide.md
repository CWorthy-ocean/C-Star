# Developer Guide (current state, 2026-07-17, branch `refactor`)

This supersedes the mental model in `docs/overview.md` / `docs/domain-generation-overview.md`
/ `docs/machine-config.md` / `docs/dev-notes/forge-blueprint-inventory.md`, which describe the
pre-refactor `_core.py` / `CstarSpecBuilder` design. Those files are stale (see
"Stale docs" at the end) — start here.

## 1. The big picture

Forge is split into two layers along a hard boundary, in preparation for moving
the execution half into C-Star as an "application":

- **Authoring** (stays in this repo): the catalog of reusable pieces (Model/Domain/
  Forcing/Output specs), a **resolver** that assembles them into a single
  reviewable file, and a **wizard** UI.
- **Execution** (`cstar_forge/forge/`, target: relocates into C-Star wholesale):
  an **engine** that turns that file into ROMS-MARBL input NetCDFs, a
  namelist, and a downstream blueprint, plus the **executor** that does the actual
  work. Execution never touches the catalog.

The file that crosses the boundary is `ForgeBlueprint` — **the forge application's own
blueprint**. Terminology trap to avoid: C-Star also has an existing, unrelated
`roms_marbl` application whose blueprint (`RomsMarblBlueprint`) forge *emits as an
output artifact*. "Building a blueprint" in old code/docs means producing that
downstream artifact, not forge's own input.

```
 catalog pieces ─┐
 (Model/Domain/  ├─► build_forge_blueprint() ─► ForgeBlueprint ─► process_forge_blueprint(cfg, host)
  Forcing/Output)│         (resolver)         (.yaml,               (engine → executor)
                 │                             portable)           │
 wizard UI ──────┘                                                 ▼
                                                     input NetCDFs, namelist.nml,
                                                     cppdefs.opt, roms_marbl blueprint
```

`CstarSpecBuilder`/`CstarSpecEngine`/`_core.py` **no longer exist** — fully deleted
and replaced by the above. The `docs/dev-notes/architecture-decomposition-plan.md` and
`docs/dev-notes/executor-portability-plan.md` planning docs record the history of how that
decomposition was carried out and are kept as historical reference; this guide is
the current-state description.

## 2. Directory map

```
cstar_forge/
  catalog.py                 thin back-compat shim: BlueprintCatalog → DomainCatalog
  domain_catalog.py           DomainCatalog: scans catalog/{ModelSpec,DomainSpec,
                               ForcingSpec,OutputSpec,Machines,blueprints}, exposes
                               *_data()/*_path() accessors + blueprintDF(); register_output/
                               register_forcing/register_domain_from_dict/
                               register_model_from_settings write new catalog entries (the
                               wizard's "save modified pieces to catalog" panel)
  domain_catalog_sketch.py    dead prototype, unreferenced anywhere — candidate for deletion
  forge_blueprint_resolve.py      resolver: build_forge_blueprint(...)
  forge_blueprint_wizard.py       ForgeBlueprintWizard (ipywidgets UI), thin shell over the resolver;
                               ForgeBlueprintWizardApp wraps it with a catalog-location bar
                               (defaults to the bundled catalog; Reload rebuilds against a
                               different local path/"local"/GitHub URL/http URL)
  models.py                   Pydantic wrappers for model.yaml (ModelSpec, ModelCode,
                               ModelTemplates, load_models_yaml); imports its forcing/IC/
                               OpenBoundaries item models FROM forge.forge_blueprint (single
                               source, no duplicates — see §5)
  config.py                   DataPaths / MachineConfig / resolve_host() — authoring-side
                               host detection (NERSC/RCAC/macOS), used by run.py only
  run.py                      CLI: `python -m cstar_forge.run forge_blueprint.yaml`; resolves
                               host, calls forge.forge_blueprint_engine.process_forge_blueprint
  catalog/                    ModelSpec/, DomainSpec/, ForcingSpec/, OutputSpec/,
                               Machines/, blueprints/ — the YAML data the catalog scans

  forge/                      THE FORGE APPLICATION — execution engine, target: relocates
                               into C-Star as one unit. Nothing here reads the catalog.
    forge_blueprint.py            ForgeBlueprint (the blueprint) + item models + enums.
                               Dependency-light: stdlib + pydantic + yaml, plus
                               cstar.orchestration.models.Blueprint (see §3a) — a
                               portability guard test enforces no cstar_forge imports
                               and no other cstar import
    app.py                        ForgeRunner + ForgeApplication (see §3a) — makes forge
                               a real, C-Star-discoverable application. NOT part of the
                               relocatable boundary above (like run.py, it's disposable
                               host-resolution glue: imports cstar_forge.config)
    forge_blueprint_engine.py      process_forge_blueprint(); ForgeBlueprintExecutor Protocol (the
                               C-Star substitution seam); sources_to_forcing_override();
                               forge_blueprint_to_builder_kwargs(); verify_content_hash()
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

## 3. `ForgeBlueprint` — the forge blueprint

Defined in `cstar_forge/forge/forge_blueprint.py`, which subclasses
`cstar.orchestration.models.Blueprint` (as of 2026-07-23) — this is what makes forge a
real C-Star application (see §3a below), not just a Pydantic model that happens to
carry an `application` string. It's still the ONLY `cstar` import in this module —
everything else in `forge/` stays free of `cstar_forge`'s authoring/host layer (see §4)
— so it remains lightweight (no ROMS/MARBL build, no roms-tools) even though it's no
longer literally cstar-free; `cstar-ocean` is a required pip dependency of this package
regardless (see `pyproject.toml`).

Top-level shape: `forge_blueprint_version` (int, bump only on breaking change;
currently 4) · `application` (=`"forge"`, C-Star app discriminator, required by the
`Blueprint` base) · `name`/`description` (required top-level fields on the `Blueprint`
base; `name` is the single user-editable canonical name — `casename`/`working_dir`/
`B_{name}.yaml`/netCDF stems all derive from it. Pre-2026-07-23 these lived under a
now-removed `identity` sub-model; a v3→v4 migration in `ForgeBlueprint`'s
`model_validator(mode="before")` flattens old files automatically — see
`migrate_forge_blueprint_data`. `model_name`/`grid_name` live in
`composition.model.name`/`domain.grid_name` since `grid_name` is results-affecting —
`SourceData` keys cache filenames off it. `ensemble_id` was removed entirely long ago,
a dead concept with no effect beyond a cosmetic name suffix; a v2→v3 migration
(same before-validator) reproduces the old derived name bit-for-bit for those files.)
· `run` (start/end date, model_reference_date) · `domain` (`grid_name`, grid_kwargs,
topography_source, open_boundaries, partitioning, nesting) · `forcing` (flat: initial_conditions,
surface/boundary/tidal/river lists, cdr_forcing, resolved_datasets) · `datasets`
(host-independent list of resolved dataset keys) · `model_settings` (flat dict: cppdefs +
~35 namelist sections) · `code` (roms/marbl repos + `templates_compile_time`/`_run_time`
repo refs) · `composition` (which catalog pieces produced this + overrides layer) ·
`provenance` (generated_at, content_hash, notes). The `Blueprint` base also adds
`state`/`schema_version` (its own versioning metadata, distinct from
`forge_blueprint_version`) and injects a `$schema` key on serialization (stripped back
out on load by the same before-validator).

- **`working_dir`** (default `~/cstar-forge-data`) is the single per-run artifact root —
  everything the executor *produces* lands under it. It's host/location, not
  results-affecting, so it's excluded from `content_hash`. Redeclared as `str` (the
  `Blueprint` base's is `Path`) to preserve the sentinel-expansion behavior below —
  see `ForgeBlueprint._resolve_out_dir`, which overrides the base's eager
  `expanduser()`/`resolve()` for exactly this reason.
- **`content_hash()`** — sha256 over everything *except* `forge_blueprint_version`,
  `name`, `description`, `composition`, `provenance`, `working_dir`, `state`,
  `schema_version`, `$schema` (see `_HASH_EXCLUDE`), plus (as of 2026-07-09) each
  code repo's `location` field (`code.roms`/`marbl`/`templates_compile_time`/
  `templates_run_time` — the fetch address, not the pinned commit/branch/files). Stamped
  on `to_yaml`; `verify_content_hash` warns (doesn't block) on a mismatched hand-edit at
  load. See §6 item 4 for the remaining piece (templates still pinned by branch, not
  commit).

### 3a. Forge as a real C-Star application

`cstar_forge/forge/app.py` (NOT part of the `forge/` boundary guarded by §4/
`test_forge_app_boundary.py` — like `run.py`, it's disposable host-resolution glue)
defines the three pieces the
[C-Star custom-applications contract](https://c-star.readthedocs.io/en/latest/custom_applications.html)
requires:

- `ForgeRunner(BlueprintRunner[ForgeBlueprint])` — `run()` delegates to
  `cstar_forge.run.process` (host resolution) → `process_forge_blueprint` →
  `ensure_source_data`/`generate_inputs`/`configure_build`, then reports
  `ExecutionStatus.COMPLETED`. Scope (2026-07, first cut): generates inputs and emits
  the downstream `roms_marbl` blueprint (`B_{name}.yaml`), then stops — it does not
  chain into actually running the ROMS-MARBL simulation; the existing `roms_marbl`
  application consumes that blueprint separately.
- `ForgeApplication` — `@register_application`-decorated `ApplicationDefinition`
  wiring `ForgeBlueprint` + `ForgeRunner` together under `name = "forge"`.

Discoverable via C-Star's `CSTAR_APP_MODULES` environment variable (comma-separated
importable module paths, each imported before app lookup):

```
export CSTAR_APP_MODULES=cstar_forge.forge.app
```

which lets C-Star's own entrypoint run a forge blueprint directly (`cstar blueprint
run forge_blueprint.yaml`), in addition to the existing `python -m cstar_forge.run`
CLI. The app currently lives in this repo (not relocated into the C-Star repo) —
deliberate, per §1's target: the blueprint/executor design is still iterating, so
relocation stays a later step.

## 4. The call chain end to end

**Authoring (catalog → resolver/wizard → blueprint):**
1. `wiz = ForgeBlueprintWizard()` (forge_blueprint_wizard.py) — scans the catalog via
   `domain_catalog.default_catalog`, populates dropdowns. The notebook entry point is
   actually `ForgeBlueprintWizardApp()`, a thin wrapper that shows a catalog-location
   bar above the wizard (auto-loads the bundled catalog; Reload rebuilds a fresh
   `ForgeBlueprintWizard(catalog=DomainCatalog(catalog_root=...))` against a different
   local path/`"local"`/GitHub URL/http URL, keeping the previous wizard on failure).
2. User picks a domain → `_on_domain()` prefills grid/boundaries/partitioning/dates from
   `catalog.domain_data(name)`.
3. Every edit → `_rebuild()` → `build_forge_blueprint(**self._gather())`
   (forge_blueprint_resolve.py) — reads the single consolidated `model.yaml` directly as a
   dict (no Pydantic here; `code` + flat `model_settings`, no embedded forcing/output
   defaults — a ForcingSpec and OutputSpec must always be supplied explicitly), resolves
   dataset keys via `source_registry`, computes pure-derived settings (CFL `dt`,
   `v_sponge`, etc.), returns a `ForgeBlueprint`.
4. `wiz.config.to_yaml(path)` writes the portable `forge_blueprint.yaml`.

**Execution (blueprint → engine → executor), same machine or a different one:**
5. `python -m cstar_forge.run forge_blueprint.yaml` (run.py) — resolves the host via
   `cstar_forge.config.resolve_host()` (machine tag, `source_data_cache`,
   `working_dir` override).
6. `forge.forge_blueprint_engine.process_forge_blueprint(cfg, host, ...)` builds a
   `ForgeExecutor` via `ForgeExecutor.from_forge_blueprint(cfg, host)` and drives:
   `ensure_source_data()` → `generate_inputs()` → `configure_build()`.
7. Outputs land under `host.working_dir`: input NetCDFs, `namelist.nml`, `cppdefs.opt`,
   and the emitted downstream `roms_marbl` blueprint YAML (`B_{name}.yaml`, persisted
   once by `configure_build()` — there is no per-stage blueprint file).

`ForgeExecutor` never imports `cstar_forge.config`/`catalog`/`domain_catalog`/
`forge_blueprint_resolve`/`forge_blueprint_wizard` — verified both by grep and by
`tests/test_forge_app_boundary.py` (an AST-based guard with an empty, actively-enforced
violation allowlist).

`namelist_model.py` and `util.py` were moved into `cstar_forge/forge/` (2026-07-09, see §6
item 1) — `forge/settings.py`/`forge/forge_blueprint_engine.py` import
`cstar_forge.forge.namelist_model` and `forge/input_data.py` imports `cstar_forge.forge.util`
as same-package siblings now, not top-level reach-ups. Both modules are also now in the
boundary guard's `_FORGE_APP_MODULES` list.

## 5. `models.py` vs `forge/forge_blueprint.py` (item-model unification — done)

Earlier project memory described two parallel item-model schemas kept in sync by a
"lockstep drift guard" test. **That has been resolved**: `models.py` now imports its
forcing/IC item models (`BoundaryForcingItem`, `SurfaceForcingItem`, `InitialConditions`,
etc.) directly from `forge.forge_blueprint` — single source of truth, no duplication, no
drift guard needed for item models. What `models.py` still owns is the `model.yaml`
*wrapper* shape (`ModelSpec`, `ModelCode`, `ModelTemplates`, `load_models_yaml`) used by
`domain_catalog.load_model_spec()` for full Pydantic validation at catalog-registration
time — a heavier, separate path from the resolver's plain-dict read of the same file.
ModelSpec no longer has an `inputs`/split `templates`+`settings` shape (consolidated,
2026-07; see the catalog's `ModelSpec/*/model.yaml` for the current `code` +
`model_settings` shape) — `GridInput`/`ForcingInput`/`ModelInputs`/`SettingsStage`/
`SettingsSpec`/`TemplatesSpec` etc. were removed.

What does still guard drift: `tests/test_roms_tools_coverage.py` (roms-tools option
coverage) and a resolver/executor settings-parity assertion in `test_forge_blueprint.py` —
different concern, not the retired item-model duplication.

**Fixed (2026-07-09):** `OpenBoundaries` used to be defined twice — `models.py`
(`BaseModel`, `extra="forbid"`) and `forge/forge_blueprint.py` (`_Section`) — with
`ForgeExecutor`/`RomsMarblInputData` depending on the `models.py` copy for no good reason
(it was the one place either module reached into `cstar_forge.models`, an
authoring-flavored module that also carries catalog-facing `load_models_yaml`). Both
`forge/executor.py` and `forge/input_data.py` now import `OpenBoundaries` straight from
`forge.forge_blueprint`; `models.py` re-exports it from there like the other 6 item models.
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
   would close this — flagged in-code and in `docs/dev-notes/executor-portability-plan.md`, still
   open.
3. **Templates are re-fetched every `configure_build`** (rmtree + re-clone under
   `working_dir/templates/<stage>`), not cached like source data / code. A commit-keyed
   template cache (mirroring `source_data_cache`) would fix this and set up item 4.
4. ~~`code.location` participates in `content_hash`.~~ **PARTIALLY DONE (2026-07-09):**
   `content_hash()` now scrubs `location` (the fetch address) from every code repo
   (`code.roms`, `code.marbl`, `code.templates_compile_time`, `code.templates_run_time`)
   before hashing — only `commit`/`branch`/`directory`/`files` are results-affecting, so a
   mirror/local-path change no longer perturbs the hash (test:
   `test_content_hash_ignores_code_repo_location`). **DONE:** `_build_code` in
   `forge_blueprint_resolve.py` reads a `code.templates_commit:` pin from `model.yaml`
   when set, and the bundled `cson_roms-marbl_v0.1` ModelSpec now sets it to a real
   commit SHA (post-`refactor`-merge) rather than tracking branch `main`.
5. **`refactor` has not been merged to `main` yet** (currently `main`+38 commits, 0 behind
   — a clean fast-forward candidate per `docs/dev-notes/executor-portability-plan.md` and prior
   audit). The raw-URL template-fetch path
   (`raw.githubusercontent.com/.../main/templates/...`) only resolves post-merge and is
   unverified against the real remote.
6. ~~Two compile-time-settings TODOs in `input_data.py`~~ **INVESTIGATED AND REMOVED
   (2026-07-09):** both comments predated (2026-01-12/21) the code that resolves them.
   The surface-forcing one sits directly above code (added 2026-06-05) that already sets
   `cppdefs.sal_restore`/`co2_tvarying` for the only two cases that exist today — the
   comment was just never deleted. The boundary-forcing one has no evident target: every
   boundary-related cppdefs flag in `templates/compile-time/cppdefs.opt.j2` (`OBC_*`,
   `M2_FRC_BRY`, `M3_FRC_BRY`, `T_FRC_BRY`, `Z_FRC_BRY`) is either a static default or
   driven by `open_boundaries` at the domain level, not by boundary-forcing type
   (physics/bgc) — inferred from the current template, not confirmed with a ROMS-MARBL
   domain expert, but no missing wiring was found. Both comments deleted; 488 tests still
   green.
7. ~~Stale docstring in `forge/forge_blueprint.py`~~ **DONE** (found already resolved
   during the 2026-07-23 real-C-Star-application work, §3a): the module docstring
   already says "fully wired into `ForgeExecutor`", not the old "not yet wired" text
   this item described.
8. **`examples/forge_blueprint2.yaml` / `forge_blueprint3.yaml`** still stamp
   `application: roms_marbl` (pre-rename). Regenerate or delete the stale ones — not
   load-tested by anything, low urgency. (The docs example was regenerated 2026-08-06 as
   `docs/forge-blueprint-example.wio-toy.yaml`, schema v4, load-tested by
   `test_committed_example_validates`.)
9. **`domain_catalog_sketch.py`** (167 lines) — dead prototype, zero references anywhere
   (code, tests, notebooks, docs). Safe to delete.
10. ~~Architecture doc headers understated progress.~~ **DONE (2026-07-17):**
    `docs/dev-notes/architecture-decomposition-plan.md` and `docs/dev-notes/executor-portability-plan.md` had
    stale "proposal"/"executing" status headers even though the decomposition they describe
    is complete (see this guide for current state) — both status lines now point here.
11. **Forge app relocation into C-Star not yet done** (2026-07-23, see §3a): `ForgeBlueprint`
    is a real `cstar.orchestration.models.Blueprint` subclass and `cstar_forge/forge/app.py`
    registers it with C-Star via `CSTAR_APP_MODULES`, but the app still lives in this repo
    rather than in the C-Star repo's `cstar/applications/` — a deliberate, explicit choice
    (the blueprint/executor design is still iterating); relocation is a follow-on once that
    settles. Also open: `ForgeRunner.run()` calls the synchronous, heavy
    `process_forge_blueprint` inline on the event loop (blocking it for the duration) rather
    than via `asyncio.to_thread` — fine for a first cut, candidate refinement later.

**Good news / already resolved that older memory implied was still open:**
- A **settings-level** golden test exists: `test_golden_model_settings_test_tiny`
  (test_forge_blueprint.py:201) diffs resolved `model_settings` against a committed JSON
  fixture (`tests/fixtures/golden_model_settings_test-tiny.json`).
- The specifically deferred **byte-exact `namelist.nml`** golden (2026-07-16) is now also
  in place: `tests/test_core.py::TestGoldenNamelist::test_golden_namelist_test_tiny`
  drives the real `generate_inputs()` → `configure_build()` chain (real
  `write_roms_namelist`; only roms-tools construction classes are mocked) and diffs the
  rendered `namelist.nml` against `tests/fixtures/golden_namelist_test-tiny.nml`
  (host-rooted absolute paths normalized to a `<WORKDIR>` token). Regenerate via
  `UPDATE_GOLDEN=1 pytest tests/test_core.py -k golden_namelist_test_tiny`. Still open:
  a real-generated-data integration test (actual GLORYS/ERA5/TPXO/DAI network fetch, no
  roms-tools mocking) — a heavier test that doesn't exist yet.
- `.gitignore` now excludes `.ipynb_checkpoints` (untracked stale checkpoint files still
  exist on disk locally but aren't committed — harmless, can delete opportunistically).
- Full test suite: **487 passed, 0 failed** as of this audit.

## 7. Docs that referenced deleted `_core.py`/`CstarSpecBuilder`/`CstarSpecEngine` — FIXED (2026-07-09)

All 7 were live pages in the published MyST docs site (`myst.yml` TOC), so none were
deleted (that would need TOC restructuring); each was fixed in place instead:

- `docs/domain-generation-overview.md` — added a banner noting the class is gone
  (decomposed into `ForgeBlueprint` + `ForgeExecutor`); updated the mermaid diagram and stage
  descriptions to show `build_forge_blueprint` → `ForgeBlueprint` → `ForgeExecutor.from_forge_blueprint`.
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
- `docs/dev-notes/roms-tools-options-integration.md` — intentionally a historical record (per its
  own banner); added a note that its file paths/class names have since moved, without
  rewriting the historical narrative.
- `docs/dev-notes/forge-blueprint-inventory.md` — the original planning doc that motivated this whole
  refactor; added a "historical/superseded" banner pointing here, kept the content (the
  six-sources-of-truth model and hardcoded-constants table still have reference value).
