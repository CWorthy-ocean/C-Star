# Decomposition plan: `CstarSpecBuilder` → the forge application (ForgeBlueprint-native executor)

**Status:** DONE — this decomposition has been executed and merged into `refactor`.
Kept as a historical record of the plan; for the current architecture see
`docs/developer-guide.md`.

## Terminology (read first — the target model)

C-Star hosts multiple **applications**; each has its own **blueprint** (a YAML of all its
inputs) that the application reproducibly executes. Two applications matter here:

- **roms_marbl application** — *exists, unchanged, NOT ours.* Blueprint = the
  `RomsMarblBlueprint` YAML. Runs the ROMS-MARBL simulation.
- **forge application** — *what we are building.* Blueprint = the **`ForgeBlueprint`**. When it
  runs, it emits the same artifacts as today: input netCDFs, the namelist, **and the
  roms_marbl blueprint**. That downstream blueprint is an *output artifact* of the forge
  application, consumed by the (separate) roms_marbl application.

So: **forge blueprint = ForgeBlueprint** (our input); **roms_marbl blueprint = an emitted
artifact** (not our application's blueprint). Every "make a blueprint" reference in the
legacy `_core.py` code is about producing that *downstream* roms_marbl blueprint.

## Goal & principle

Break the transitional `CstarSpecBuilder` god-object (`_core.py`, ~2260 lines) into the
**forge application** at its C-Star target boundary, so that:
- the eventual C-Star move is a *relocation* (`git mv` + imports), not a re-design; and
- collaborators develop against the final shape, not a structure about to be ripped up.

Two separable operations — this plan is **D only**:
- **D (decomposition, this plan):** carve out the forge application; done once, stable
  against schema churn.
- **R (relocation into C-Star):** move the forge-application package into the C-Star repo;
  deferred; cheap once D is done.

Schema iteration is orthogonal: it changes field *definitions inside* `ForgeBlueprint`, not
the schema/executor/resolver/UI boundaries D establishes.

The seams already exist and were built for this: `ForgeBlueprintExecutor` Protocol +
injectable `executor_factory` + `process_forge_blueprint` (`forge_blueprint_engine.py`),
`forge_blueprint.py` portability guard, `application` discriminator.

---

## Target module layout

Working package name `forge_app/` below is a placeholder — see "Naming decisions".

```
cstar_forge/                         # AUTHORING (stays in Forge)
  catalog.py, domain_catalog.py, catalog/…   # pieces + discovery
  forge_blueprint_resolve.py                       # Phase-1 resolver (build_forge_blueprint)
  forge_blueprint_wizard.py                        # wizard UI
  config.py                                    # host / paths / machine resolution
  _core.py                                     # CstarSpecEngine ONLY (slimmed orchestrator)

  forge_app/                         # THE FORGE APPLICATION (relocatable to C-Star as one unit)
    forge_blueprint.py                   # the forge blueprint (already portable) + unified item models
    executor.py                      # ForgeBlueprint-native executor (ex-CstarSpecBuilder guts)
    engine.py                        # process_forge_blueprint + ForgeBlueprintExecutor Protocol
    input_data.py                    # RomsMarblInputData (input netCDF generation)
    source_data.py                   # SourceData / DatasetHandler
    source_registry.py
    settings.py                      # cppdefs / namelist rendering
    roms_marbl_blueprint.py          # EMITS the downstream roms_marbl blueprint artifact
```

Dependency rule (enforced by test): `forge_app/*` must NOT import the authoring modules
(`catalog*`, `forge_blueprint_resolve`, `forge_blueprint_wizard`, `config`). Forge authoring
imports `forge_app` one-way. This is why execution moves into the forge-application
package (relocatable to C-Star), not Forge into C-Star.

---

## Where `CstarSpecBuilder` goes (method-group → destination)

| Current (in `_core.py`) | Destination |
|---|---|
| Constructor fields / schema; `from_domain` | **Delete** — `ForgeBlueprint` is the input; `from_forge_blueprint` is the ctor |
| `_validate_dates`, `model_post_init` | `executor.py` init |
| Path props (`input_data_dir`, `blueprint_dir`, `*_code_dir`, `run_output_dir`, `resolved_catalog_dir`) | Host resolution (`config.resolve_host`, already in engine) → passed into executor |
| Persistence (`persist`, `_persist_settings`, `_load_settings*`, `_convert_paths*`, `dump`, `load`) | `ForgeBlueprint.to_yaml/from_yaml` is the canonical **forge** blueprint; **retire `dump`/`load`** |
| `path_blueprint`, blueprint build (`_initialize_blueprint`, `_load_blueprint_file`, `blueprint_from_file`) | `roms_marbl_blueprint.py` — these *emit the downstream roms_marbl blueprint artifact* (not the forge app's own blueprint) |
| Data (`datasets`, `get_ds`, `ensure_source_data`) | `executor.py` → delegates to `source_data.py` |
| `generate_inputs` | `executor.py` → delegates to `input_data.py` |
| Settings (`_init_settings_*`, `_update_settings_*`, `_merge_settings_override_*`, `_apply_v_sponge_default_from_grid`, `_set_…_timestepping_defaults`) | `executor.py` / `settings.py` |
| `configure_build`, `prep_cstar_environment` | `executor.py` (the Protocol surface) |
| `name`, `n_procs`, `casename`, `datestr` | Already on `ForgeBlueprint`; executor reads them |

`CstarSpecEngine` (catalog orchestration: `_create_builder`, `generate_domain`,
`generate_all`, `run_all`) stays in Forge but is repointed: **catalog pick →
`build_forge_blueprint` → `ForgeBlueprint` → `process_forge_blueprint`** (drops the `from_domain`
path at `_core.py:2609` and `domain_catalog.py:697`).

---

## Phased sequence

### Phase 0 — dependency-direction guard (non-breaking, start here)
- Add a test asserting no `forge_app`-bound module imports an authoring module.
- Extend the existing `forge_blueprint.py` portability guard to the whole future package.
- **Value:** free safety net + documents the target boundary for the invitees.
- **Risk:** none. **Test impact:** +1 guard test.

### Phase B — builder becomes ForgeBlueprint-native (the commitment point)
1. Add `CstarSpecBuilder.from_forge_blueprint(cfg)` as canonical ctor; construct directly
   from `ForgeBlueprint` (drop the lossy `forge_blueprint_to_builder_kwargs` bridge).
2. Point `_default_executor_factory` and `CstarSpecEngine` at it; **delete `from_domain`**
   and the domain-dict derivation.
3. Remove now-dead transitional fields (`override`, `catalog_root`,
   `initialize_catalog_*`, `suppress_catalog_validation`) and `dump`/`load` if unused.
4. **Retire the parity test** (`test_forge_blueprint.py:771`) — one derivation now.
- **Risk:** high blast radius (central code). **Test impact:** the 69 `test_core.py`
  instantiations (see strategy below).

### Phase C — carve the forge-application package
- Create `cstar_forge/forge_app/`; move `input_data.py`, `source_data.py`,
  `source_registry.py`, `settings.py`, `forge_blueprint.py`, `forge_blueprint_engine.py`
  (→ `engine.py`), and the executor (rename `CstarSpecBuilder` → `ForgeExecutor`, or the
  name chosen below). Split the downstream-blueprint emission into `roms_marbl_blueprint.py`.
- `absolufy-imports` rewrites imports; leave thin re-export shims at old paths for one
  cycle if external notebooks import them.
- **Risk:** mechanical once B is done (import churn). **Test impact:** import-path
  updates only.

### Phase D — unify the two item schemas (folds in checklist item 3)
- Fold `models.py` forcing item models into `forge_blueprint.py` (single source). Reconcile:
  - **`extra` policy:** `models.py` items use `extra="allow"` (legacy passthrough);
    `forge_blueprint` uses `forbid`. Move to `forbid` now that `options` is the sanctioned
    hatch — *after* confirming `model.yml`/resolver emit no stray keys.
  - **Naming:** `InitialConditionsInput` → `InitialConditions`.
  - **`SourceSpec` validator:** move the `Literal` typing + `glorys_layout`-only-for-GLORYS
    validator into `forge_blueprint`'s `SourceSpec` (stays stdlib/pydantic-only).
- Delete `models.py` duplicates; update `input_data`, resolver, `ModelInputs`.
- Replace the lockstep guard with a single-definition assertion (or delete it).
- **Risk:** medium, contained. **Test impact:** `test_models.py`, a few resolver tests.

---

## `test_core.py` strategy (the bulk of Phase B effort)

69 direct `CstarSpecBuilder(` instantiations, brittle because they mock *builder-private*
method boundaries. Triage into three buckets:

1. **Behavior now covered by `test_forge_blueprint.py`** (resolver/schema/derivation) →
   delete or thin to a smoke check.
2. **Genuinely executor-specific** (path resolution, blueprint writing, settings merge,
   input-generation orchestration) → re-point to an executor built via `from_forge_blueprint`,
   and **mock at stable seams** (`SourceData.get`, `RomsMarblInputData.generate_all`, the
   `rt.*` calls) — *not* builder-private methods. This is what makes them durable.
3. **Tests of removed surface** (`dump`/`load`, dual derivation; `from_domain` already 0)
   → delete.

Introduce one test helper — `make_executor(**overrides)` = `build_forge_blueprint(...)` →
`from_forge_blueprint(...)` — so future signature changes touch one site, not 69.

---

## Naming decisions (RESOLVED)

The application is named **`forge`**. The `application` discriminator identifies the
*consumer* — the forge application that runs this blueprint (not the downstream
`roms_marbl` it produces for).

1. **`application` discriminator** — `DEFAULT_APPLICATION = "forge"` (done in
   `forge_blueprint.py`). C-Star routes the ForgeBlueprint blueprint to the `forge` application.
2. **Package** for the forge application — `cstar_forge/forge/` (used in the layout above
   in place of the `forge_app/` placeholder).
3. **Executor class** — `ForgeExecutor` (replaces `CstarSpecBuilder`).

> Follow-up: `examples/forge_blueprint*.yml` and `docs/forge-blueprint-example.test-tiny.yml` were
> stamped under the old value; the doc example is re-stamped to `forge`. The `examples/*`
> snapshots should be regenerated (they may carry other catalog drift too).

## Sequencing (Option A selected — invitees touch engine internals)

Phase 0 → B → C → D, **then** invite. Phase 0 is non-breaking and can start immediately.
(Option B — invite roms-tools contributors first onto the guarded current surface — was
the alternative; not chosen because the invitees work on the internals D reshapes.)

## Guardrails / definition of done
- Suite green at every phase boundary; `refactor → main` stays a clean fast-forward.
- Dependency-direction + portability guards green.
- No change to generated outputs (netCDFs, namelist, **downstream roms_marbl blueprint**) —
  the deferred byte-golden namelist test (`test_forge_blueprint.py:146`) is the eventual proof;
  until then the parity test guards Phase B, and existing behavior tests guard C/D.

> Note: the file paths above are pre-relocation. They move under `forge_app/` in Phase C
> and again into the C-Star repo at R; don't treat these paths as permanent.
