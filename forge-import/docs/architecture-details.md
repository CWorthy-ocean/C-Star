# Developer Guide

The primary architecture reference for C-Star Forge, describing the current
state of the code. Historical notes — how the repo arrived here, resolved follow-ups, planning
documents — live in the repo's git history (the former `docs/dev-notes/`
directory); the distilled design rationale agents need is in the claude-docs
repo (`cstar-forge/DESIGN-RATIONALE.md`).

## 1. The big picture

Forge is split into two layers along a hard boundary, in preparation for moving
the execution half into C-Star as an "application":

- **Authoring** (stays in this repo): the catalog of reusable specs (Model/Domain/
  Forcing/Output specs), a **resolver** that assembles them into a single
  reviewable file, and a **wizard** UI.
- **Execution** (`cstar_forge/forge/`, target: relocates into C-Star wholesale):
  an **engine** that turns that file into ROMS-MARBL input NetCDFs, a
  namelist, and a downstream blueprint, plus the **executor** that does the actual
  work. Execution never touches the catalog.

The file that crosses the boundary is `ForgeBlueprint` — **the forge application's own
blueprint**. Terminology trap to avoid: C-Star also has an existing, unrelated
`roms_marbl` application whose blueprint (`RomsMarblBlueprint`) forge *emits as an
output artifact*. "Building a blueprint" means producing that downstream artifact,
not forge's own input.

```
 catalog specs  ─┐
 (Model/Domain/  ├─► build_forge_blueprint() ─► ForgeBlueprint ─► process_forge_blueprint(cfg, host)
  Forcing/Output)│         (resolver)         (.yaml,               (engine → executor)
                 │                             portable)           │
 wizard UI ──────┘                                                 ▼
                                                     input NetCDFs, namelist.nml,
                                                     cppdefs.opt, roms_marbl blueprint
```

## 2. Directory map

```
cstar-forge/
├── cstar_forge/                 # Main package directory
│   ├── forge_blueprint_resolve.py  # resolver: build_forge_blueprint(...)
│   ├── forge_blueprint_wizard.py   # ForgeBlueprintWizard (ipywidgets UI) +
│   │                               # ForgeBlueprintWizardApp (adds catalog-location bar)
│   ├── forge-blueprint-wizard.ipynb     # user-facing wizard notebook (run in Jupyter)
│   ├── models.py               # Spec classes (ModelSpec, etc.)
│   ├── domain_catalog.py       # DomainCatalog: scans the catalog, exposes accessors;
│   │                           # LayeredCatalog stacks a writable user layer
│   │                           # (default_catalog_stack(): ~/cstar-forge-data/catalog,
│   │                           # or CSTAR_FORGE_CATALOG) over the read-only bundled
│   │                           # catalog — this stack is the module's default_catalog
│   ├── config.py               # Path management and system detection
│   ├── run.py                  # CLI entry point: python -m cstar_forge.run forge_blueprint.yaml
│   ├── cli.py                  # 'cstar forge run'/'wizard'/'register-kernel' typer sub-app (cstar.cli entry point)
│   ├── register_kernel.py      # Jupyter kernelspec + activation wrapper (backs 'cstar forge register-kernel')
│   ├── ui/                     # Wizard presentation layer (Voilà app front-end)
│   │   ├── _voila_app.ipynb    # Voilà app notebook — internal; served via
│   │   │                       # run-wizard-app.sh / 'cstar forge wizard'
│   │   ├── branding.py         # [C]Worthy header bar, favicon, page title
│   │   └── assets/cworthy-logo.png  # bundled logo (embedded as a data URI)
│   ├── forge/                  # The forge application (execution engine —
│   │   │                       # relocates into C-Star as one unit)
│   │   ├── app.py                  # ForgeRunner/ForgeApplication (C-Star application)
│   │   ├── forge_blueprint.py      # ForgeBlueprint — the forge application's blueprint
│   │   ├── forge_blueprint_engine.py # process_forge_blueprint(); ForgeBlueprintExecutor Protocol
│   │   ├── executor.py         # ForgeExecutor — the processing engine
│   │   ├── host.py             # HostPaths — frozen host-boundary contract injected into the executor
│   │   ├── input_data.py       # Input file generation
│   │   ├── source_data.py      # Dataset download and preparation
│   │   ├── source_registry.py  # Dataset alias map / provenance metadata (stdlib-only)
│   │   ├── glorys_subchunk.py  # Just-in-time kerchunk subchunking for GLORYS
│   │   ├── settings.py         # Template rendering
│   │   └── namelist_model.py   # RunTimeSettings + build_namelist
│   └── catalog/                # Bundled spec catalog (+ BlueprintCatalog API)
│       ├── ModelSpec/{model}/model.yaml    # Code repos, templates, settings, defaults
│       ├── DomainSpec/{grid}/Domain.yaml   # Grid definitions
│       ├── ForcingSpec/{name}/Forcing.yaml # Forcing source configurations
│       ├── CdrSpec/{name}/Cdr.yaml         # CDR-forcing configurations (mode + config)
│       ├── OutputSpec/{name}/Output.yaml   # Output configurations
│       └── blueprints/                     # Example blueprints (bundled, read-only layer;
│                                            # user saves go to the user catalog layer instead)
├── templates/                  # Render templates (cppdefs.opt.j2, marbl_in), decoupled
│                                # from ModelSpec — fetched by ForgeExecutor via C-Star's
│                                # AdditionalCode
├── legacy/                    # Deprecated pre-wizard tooling: notebook workflows,
│                                # the nb_engine runner, and legacy-layout blueprints
├── docs/                      # Documentation
└── README.md
```

Note `glorys_subchunk.py` is live (called from `input_data.py`) but is **not**
in the boundary guard's `_FORGE_APP_MODULES` list — a known guard gap (§6).

## 3. `ForgeBlueprint` — the forge blueprint

Defined in `cstar_forge/forge/forge_blueprint.py`, which subclasses
`cstar.orchestration.models.Blueprint` — this is what makes forge a real C-Star
application (see §3a), not just a Pydantic model that happens to carry an
`application` string. It's the ONLY `cstar` import in this module — everything
else in `forge/` stays free of `cstar_forge`'s authoring/host layer (see §4) —
so it remains lightweight (no ROMS/MARBL build, no roms-tools); `cstar-ocean` is
a required pip dependency of this package regardless (see `pyproject.toml`).

Top-level shape: `forge_blueprint_version` (int, bump only on breaking change;
currently 8) · `application` (=`"forge"`, C-Star app discriminator, required by the
`Blueprint` base) · `name`/`description` (required top-level fields on the `Blueprint`
base; `name` is the single user-editable canonical name — `casename`/`working_dir`/
`B_{name}.yaml`/netCDF stems all derive from it) · `run` (start/end date,
model_reference_date) · `domain` (`grid_name`, grid_kwargs, topography_source,
open_boundaries, partitioning, nesting) · `forcing` (flat: initial_conditions,
a single boundary section (`BoundaryForcing`, mirroring `InitialConditions`),
surface/tidal/river lists, resolved_datasets) · `cdr` (`CdrSpec`: a 5-mode
CDR-forcing selection — none/simple/yaml/netcdf/upscaled — carrying the compiled
roms-tools `CDRForcing` kwargs or a user-provided netCDF ref; its own composable
catalog spec, independent of `forcing`) · `datasets`
(host-independent list of resolved dataset keys) · `model_settings` (flat dict: cppdefs +
~35 namelist sections) · `code` (roms/marbl repos + `templates_compile_time`/`_run_time`
repo refs) · `composition` (which catalog specs produced this + overrides layer) ·
`provenance` (generated_at, content_hash, notes). The `Blueprint` base also adds
`state`/`schema_version` (its own versioning metadata, distinct from
`forge_blueprint_version`) and injects a `$schema` key on serialization (stripped back
out on load).

Older blueprint files load transparently: a `model_validator(mode="before")`
(`migrate_forge_blueprint_data`) migrates v2/v3 layouts (removed `identity`
sub-model, removed `ensemble_id`), the v4→v5 `do_cdr`→`do_cdr_output` rename,
the v6→v7 CDR move (`forcing.cdr_forcing`/`cdr_forcing_file` → the
top-level `cdr` section, mode inferred), and the v7→v8 BGC-sources move
(`initial_conditions.bgc_source` rewrapped as a one-item `bgc_sources` list;
`forcing.boundary`'s flat, type-discriminated `BoundaryForcingItem` list split
into a single `BoundaryForcing` section with `source` + `bgc_sources`, mirroring
`InitialConditions`) to the current shape, reproducing derived names
bit-for-bit. `model_name`/`grid_name` live in
`composition.model.name`/`domain.grid_name`; `grid_name` is results-affecting —
`SourceData` keys cache filenames off it.

- **`working_dir`** (default `~/cstar/_forge_bp_runs`) is the single per-run artifact root —
  everything the executor *produces* lands under it. It's host/location, not
  results-affecting, so it's excluded from `content_hash`. Redeclared as `str` (the
  `Blueprint` base's is `Path`) to preserve sentinel expansion — see
  `ForgeBlueprint._resolve_out_dir`, which overrides the base's eager
  `expanduser()`/`resolve()` for exactly this reason.
- **`content_hash()`** — sha256 over everything *except* `forge_blueprint_version`,
  `name`, `description`, `composition`, `provenance`, `working_dir`, `state`,
  `schema_version`, `$schema` (see `_HASH_EXCLUDE`), plus each code repo's
  `location` field (the fetch address; only `commit`/`branch`/`directory`/`files`
  are results-affecting). Stamped on `to_yaml`; `verify_content_hash` warns
  (doesn't block) on a mismatched hand-edit at load.

### 3a. Forge as a real C-Star application

`cstar_forge/forge/app.py` (NOT part of the `forge/` boundary guarded by §4/
`test_forge_app_boundary.py` — like `run.py` and `cli.py`, it's disposable
host-resolution glue) defines the pieces the
[C-Star custom-applications contract](https://c-star.readthedocs.io/en/latest/custom_applications.html)
requires:

- `ForgeRunner(BlueprintRunner[ForgeBlueprint])` — `run()` delegates to
  `cstar_forge.run.process` (host resolution) → `process_forge_blueprint` →
  `ensure_source_data`/`generate_inputs`/`configure_build`, then reports
  `ExecutionStatus.COMPLETED`. Scope: generates inputs and emits the downstream
  `roms_marbl` blueprint (`B_{name}.yaml`), then stops — the existing
  `roms_marbl` application consumes that blueprint separately.
- `ForgeApplication` — `@register_application`-decorated `ApplicationDefinition`
  wiring `ForgeBlueprint` + `ForgeRunner` together under `name = "forge"`.

Discovered through the `cstar.applications` entry-point group that cstar-forge
declares in `pyproject.toml`:

```toml
[project.entry-points."cstar.applications"]
forge = "cstar_forge.forge.app"
```

C-Star imports that module the first time an `application: forge` blueprint is
resolved, so an installed cstar-forge is the whole requirement — no environment
variables, and it holds in spawned scheduler jobs too. This is C-Star's only
mechanism for out-of-tree applications (the older `CSTAR_APP_MODULES` env var was
removed); a name already used by a built-in C-Star application cannot be claimed
this way.

Three ways to run a forge blueprint:

1. `cstar blueprint run forge_blueprint.yaml` — the app-framework path
   (defaults only; no forge-specific options), the no-frills front door.
   Resolves `application: forge` via the entry point above, so it needs a C-Star
   release that consults that group; on an older C-Star use one of the entries
   below.
2. `cstar forge run forge_blueprint.yaml` — the `cli.py` typer sub-app,
   registered via the `cstar.cli` entry-point group (requires a C-Star
   release with the discovery hook); a full-option argv passthrough to
   `run.main`. Reach for this for per-run options `cstar blueprint run`
   doesn't expose (stage selection, `--clobber`, dask tuning,
   `--only-inputs`, verbosity).
3. `python -m cstar_forge.run forge_blueprint.yaml` — the module CLI both of
   the above ultimately reach; always available.

The app lives in this repo (not relocated into the C-Star repo) — deliberate,
per §1's target: the blueprint/executor design is still iterating, so
relocation stays a later step.

## 4. The call chain end to end

**Authoring (catalog → resolver/wizard → blueprint):**
1. `wiz = ForgeBlueprintWizard()` (forge_blueprint_wizard.py) — scans the catalog via
   `domain_catalog.default_catalog`, populates dropdowns; entries from lower layers
   (e.g. the bundled catalog) are shown with a `(bundled)` badge. The notebook entry
   point is actually `ForgeBlueprintWizardApp()`, a thin wrapper that shows a
   catalog-location bar above the wizard (auto-loads the default layered stack —
   your writable `~/cstar-forge-data/catalog`/`CSTAR_FORGE_CATALOG` layer over the
   read-only bundled catalog; Reload rebuilds a fresh wizard against a different
   single local path/`"local"`/GitHub URL/http URL, or several `os.pathsep`-separated
   locations to build a new `LayeredCatalog`, keeping the previous wizard on failure).
   Saves (blueprints, workplans) and catalog registrations land in the stack's
   writable top layer — never inside the installed package.
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
5. `cstar blueprint run forge_blueprint.yaml` (or `cstar forge run …`) —
   resolves the host via `cstar_forge.config.resolve_host()` (machine tag,
   `source_data_cache`, `working_dir` override).
6. `forge.forge_blueprint_engine.process_forge_blueprint(cfg, host, ...)` builds a
   `ForgeExecutor` via `ForgeExecutor.from_forge_blueprint(cfg, host)` and drives:
   `ensure_source_data()` → `generate_inputs()` → `configure_build()`.
7. Outputs land under `host.working_dir`: input NetCDFs, `namelist.nml`, `cppdefs.opt`,
   and the emitted downstream `roms_marbl` blueprint YAML (`B_{name}.yaml`, persisted
   once by `configure_build()` — there is no per-stage blueprint file).

`ForgeExecutor` never imports `cstar_forge.config`/`catalog`/`domain_catalog`/
`forge_blueprint_resolve`/`forge_blueprint_wizard` — verified both by grep and by
`tests/test_forge_app_boundary.py` (an AST-based guard with an empty, actively-enforced
violation allowlist). `namelist_model.py` and `util.py` are same-package siblings
inside `forge/` and are covered by the guard's `_FORGE_APP_MODULES` list.

### 4a. Versioned namelist schemas (ucla-roms 0.5.0+)

ucla-roms 0.5.0 made its first breaking namelist change (`nrpf_rst` removed from
`&BASIC_OUTPUT_SETTINGS`; `&PARTICLES_SETTINGS` `output_period`/`nrpf` renamed to
`output_period_particles`/`nrpf_particles`). C-Star versions the namelist schema
by ucla-roms release (`cstar.roms.namelist`: `RomsNamelist` for < 0.5.0,
`RomsNamelistV0_5_0` for >= 0.5.0, selected by `namelist_schema_for_ref(ref)` —
semver tags select exactly; branch names/hashes warn and fall back to the latest
schema). Forge mirrors this in `namelist_model.py`: `RunTimeSettings` (legacy)
vs `RunTimeSettingsV0_5_0`, selected by `run_time_settings_for_ref(roms_ref)`,
where `roms_ref` is the blueprint's pinned `code.roms.commit` (threaded
resolver → executor → `write_roms_namelist`). C-Star's registry is the single
source of version-boundary truth — forge only maps its result to the matching
settings class. The forge **settings vocabulary is version-stable**: YAML keys
(`particles.output_period`, `particles.nrpf`) don't change; only the
`serialization_alias` to namelist names differs per version, and `nrpf_rst`
(still present in the shared `OutputSpec/standard`) is silently ignored for
0.5.0+ models via `extra="ignore"`. One ModelSpec per tagged ucla-roms release:
`roms-marbl-0.5-default` pins `0.5.0`; older specs stay fixed and keep emitting
byte-identical legacy namelists.

ucla-roms 0.5.0 also added a run-start precheck (`check_output_divides_rst`):
each enabled output stream's `nrpf × output_period` must evenly divide
`output_period_rst` (vacuous for monthly restarts / a 0 period). Three bundled
OutputSpecs conform for every stream — `daily-restarts` (the wizard default,
see `_DEFAULT_OUTPUT_SPEC`), `weekly-restarts`, and `monthly-restarts`
(upstream's own convention: `monthly_restarts=T`, `output_period_rst=0`).
`OutputSpec/standard` predates the precheck and is kept unchanged for
blueprints that reference it — enabling its his/avg streams under a 0.5.0+
model trips the precheck. A guard test
(`test_bundled_output_specs_satisfy_roms_divides_rst_precheck`) pins the
conforming specs, including `roms-marbl-0.5-default`'s ModelSpec-owned
sponge/particles streams. The nesting extract stream is resolve-time-derived
(child DomainSpec metadata `period` × a seeded `nrpf`), so it's enforced at
authoring time instead: `check_extract_divides_rst` (namelist_model.py), called
from the resolver and gated to >= 0.5.0 pins.

## 5. `models.py` vs `forge/forge_blueprint.py`

The forcing/IC item models (`BoundaryForcing`, `SurfaceForcingItem`,
`InitialConditions`, `BgcSourceItem`, `OpenBoundaries`, etc.) are defined once, in
`forge/forge_blueprint.py`; `models.py` imports and re-exports them — single
source of truth, no duplication. What `models.py` owns is the `model.yaml`
*wrapper* shape (`ModelSpec`, `ModelCode`, `ModelTemplates`, `load_models_yaml`)
used by `domain_catalog.load_model_spec()` for full Pydantic validation at
catalog-registration time — a heavier, separate path from the resolver's
plain-dict read of the same file. `cstar_forge.forge` never imports
`cstar_forge.models`.

What guards drift today: `tests/test_roms_tools_coverage.py` (roms-tools option
coverage) and a resolver/executor settings-parity assertion in
`test_forge_blueprint.py`.

## 6. Known gaps / open items

1. **Template staging has no CI coverage for the cross-repo flat-staging contract**
   (`ForgeExecutor._stage_templates`, executor.py ~L1036). Rendering silently assumes
   C-Star's `AdditionalCode` stages filtered files *flat*; only manually verified
   against the real remote. A `@pytest.mark.slow` network test staging from the real
   repo would close this.
2. **Templates are re-fetched every `configure_build`** (rmtree + re-clone under
   `working_dir/templates/<stage>`), not cached like source data / code. A commit-keyed
   template cache (mirroring `source_data_cache`) would fix this.
3. **`glorys_subchunk.py` is outside the §4 boundary guard** — live code called
   from `input_data.py` but absent from `_FORGE_APP_MODULES`, so boundary
   violations there would go uncaught.
4. **Forge app relocation into C-Star not yet done** (see §3a) — deliberate;
   relocation is a follow-on once the blueprint/executor design settles. Also
   open: `ForgeRunner.run()` calls the synchronous, heavy
   `process_forge_blueprint` inline on the event loop rather than via
   `asyncio.to_thread` — fine for a first cut, candidate refinement later.
5. **No real-generated-data integration test** (actual GLORYS/ERA5/TPXO network
   fetch with no roms-tools mocking) — the golden tests below mock roms-tools
   construction classes.

## 7. Golden fixtures

Two committed goldens pin the resolved-settings and namelist contracts (treat
any diff as a behavior change to justify, not noise):

- **Settings-level**: `test_golden_model_settings_test_tiny`
  (test_forge_blueprint.py) diffs resolved `model_settings` against
  `tests/fixtures/golden_model_settings_test-tiny.json`. No regeneration hook —
  update manually.
- **Byte-exact namelist**: `tests/test_core.py::TestGoldenNamelist::
  test_golden_namelist_test_tiny` drives the real `generate_inputs()` →
  `configure_build()` chain (real `write_roms_namelist`; only roms-tools
  construction classes are mocked) and diffs the rendered `namelist.nml` against
  `tests/fixtures/golden_namelist_test-tiny.nml` (host-rooted absolute paths
  normalized to a `<WORKDIR>` token). Two sibling tests pin the versioned-namelist
  schemas against the same test-tiny domain/forcing/output setup:
  `test_golden_namelist_test_tiny_roms050` (`roms-marbl-0.5-default`,
  `golden_namelist_test-tiny-roms050.nml`) and
  `test_golden_namelist_test_tiny_roms060` (`roms-marbl-0.6-default`, adds
  `&PIO_SETTINGS`, `golden_namelist_test-tiny-roms060.nml`). Regenerate one at a
  time via `UPDATE_GOLDEN=1 pytest tests/test_core.py -k <test name>` (the run
  intentionally fails after writing; rerun without the env var to confirm). To
  select *only* the legacy test, use
  `-k "golden_namelist_test_tiny and not roms050 and not roms060"` -- a bare
  `-k golden_namelist_test_tiny` matches all three.

Both fixtures resolve paths via `cstar_forge.__file__`, so they need the
editable install.
