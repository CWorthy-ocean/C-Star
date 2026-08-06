# ROMS-Tools Options Integration — Implementation Summary

> **Looking to add/operate a roms-tools parameter?** See the forward-looking contract in
> [`roms-tools-contributor-guide.md`](roms-tools-contributor-guide.md). This document is
> the *historical record* of how the seams below were built.
>
> **Note:** file paths below reflect the state at the time this was written and have
> since moved: `cstar_forge/_core.py` was deleted (decomposed into `ForgeBlueprint` +
> `ForgeExecutor`); `cstar_forge/input_data.py` and `cstar_forge/forge_blueprint_engine.py`
> are now `cstar_forge/forge/input_data.py` and `cstar_forge/forge/forge_blueprint_engine.py`;
> `CstarSpecBuilder` is now `ForgeExecutor`. See `docs/developer-guide.md` for the current
> module map.

This document records the changes made to expose roms-tools constructor options through
the Forge ForgeBlueprint, resolver, processing engine, and wizard UI.

## Background

A gap analysis compared every roms-tools rt constructor parameter against what Forge
previously exposed. The main findings:

- Most option knobs (scientific/behavioral choices) were **blocked** by `extra="forbid"` on
  the `models.py` item models, or simply **un-surfaced** in the config and UI.
- `model_reference_date` recurred on 5 rt objects (IC, Surface, Boundary, Tidal, CDR),
  all defaulting to 2000-01-01 — should be a single run-level field.
- The wizard's `ForcingSpec` selection / forced edits were **never reaching `input_data`**
  (the processing engine passed only grid/partitioning/cdr/nesting; `input_data.__post_init__`
  always read from `model_spec.inputs`). This was the Phase 0 unlock.
- A **drift guard test** was missing — new roms-tools params silently become uncovered.

## Changes by Phase

### Phase 0 — Propagate `cfg.sources` to `input_data` (the unlock)

**Files:** `cstar_forge/input_data.py`, `cstar_forge/_core.py`, `cstar_forge/forge_blueprint_engine.py`

- `RomsMarblInputData` gained `forcing_override: Optional[Dict]` and
  `model_reference_date: Optional[datetime]`. When `forcing_override` is provided,
  `__post_init__` uses it (instead of `model_spec.inputs`) to build `input_list` for IC
  and forcing categories.
- `CstarSpecBuilder` gained `forcing_override` and `model_reference_date` fields, both
  threaded through to `RomsMarblInputData`.
- `forge_blueprint_engine.py` gained `sources_to_forcing_override(cfg)`: returns `None`
  when `composition.forcing.origin == "model_default"` (no-op, preserves old behavior),
  otherwise converts `cfg.sources` → the `forcing_override` dict format. Called in
  `forge_blueprint_to_builder_kwargs`.

**Effect:** The wizard's `ForcingSpec` selection and per-item edits now actually reach
input file generation instead of being silently ignored.

---

### Phase 1a — `model_reference_date` as a run-level field

**Files:** `cstar_forge/forge_blueprint.py` (`RunWindow`), `cstar_forge/input_data.py`

- Added `model_reference_date: datetime = datetime(2000, 1, 1)` to `RunWindow` in
  `ForgeBlueprint`. Default matches the roms-tools default.
- Added `_mrd_extra()` helper on `RomsMarblInputData` that returns
  `{"model_reference_date": self.model_reference_date}` when set. Injected into the
  `extra` dicts of all 5 handler methods (IC, surface, boundary, tidal, river), so it
  propagates to every rt object that accepts it.
- `CstarSpecBuilder.model_reference_date` threads it from the engine's
  `forge_blueprint_to_builder_kwargs`.

---

### Phase 1b — Typed option fields on item models

**Files:** `cstar_forge/models.py`, `cstar_forge/forge_blueprint.py`

New validated fields added to **both** the legacy `models.py` item models (what
`input_data` consumes) and the `forge_blueprint.py` forcing item models (resolver/wizard):

| Class | New fields |
|---|---|
| `InitialConditionsInput` / `InitialConditions` | `use_density_interpolation`, `allow_flex_time` |
| `SurfaceForcingItem` | `wind_dropoff` |
| `BoundaryForcingItem` | `apply_2d_horizontal_fill`, `use_density_interpolation` |
| `RiverForcingItem` | `convert_to_climatology`, `bgc_source` |
| `Domain` (`forge_blueprint.py`) | `nesting_include_pressure_fluxes` |

`nesting_include_pressure_fluxes` is injected by the engine into `metadata_child`
(which the grid handler passes to the nesting writer as `**nesting_kwargs`).

Also fixed: `_build_input_args` now skips `None` values when resolving source blocks,
so optional `bgc_source = None` on `RiverForcingItem` doesn't error.

---

### Phase 1c — `options` passthrough dict

**Files:** `cstar_forge/models.py`, `cstar_forge/input_data.py`

Added `options: Dict[str, Any] = {}` to all 5 item models (`InitialConditionsInput`,
`SurfaceForcingItem`, `BoundaryForcingItem`, `TidalForcingItem`, `RiverForcingItem`).

`_build_input_args` now unpacks `options` from the item config before merging. Precedence:
`typed defaults → options passthrough → extra (hardcoded run-time injections)`.

This makes future roms-tools knobs available immediately via the `options` dict without
model changes. Covers: `bypass_validation`, `chunks`, `initial_slice_bounds`,
`allow_flex_time` (if not already typed), etc.

---

### Phase 2 — Wizard UI updates

**File:** `cstar_forge/forge_blueprint_wizard.py`

New controls surfaced in the wizard:

**Grid section:**
- `hmin` (float, default 5.0 m) — minimum ocean depth
- `close_narrow_channels` (checkbox) — close narrow water channels in mask
- `mask_shapefile` (text) — path to custom land-mask shapefile

**Nesting section:**
- `include pressure fluxes` (checkbox) — include baroclinic pressure fluxes in the
  nesting extraction file (`nesting_include_pressure_fluxes`)

**Run window section:**
- `model_reference_date` (date picker, default 2000-01-01) — ROMS t=0 reference date

**Forcing editor — per-item new fields:**
- IC: `density interp` checkbox (`use_density_interpolation`), `flex time` checkbox
  (`allow_flex_time`)
- Surface: `wind_dropoff` checkbox
- Boundary: `2d_fill` checkbox (`apply_2d_horizontal_fill`), `dens_interp` checkbox
- River: `convert_to_climatology` dropdown (if_any_missing / never / always)

IC widgets now trigger `on_change` directly (were previously un-observed).

---

### Phase 3 — roms-tools options drift guard

**File:** `tests/test_roms_tools_coverage.py`

6 `@pytest.mark.integration` parametrized tests — one per rt class
(`InitialConditions`, `SurfaceForcing`, `BoundaryForcing`, `TidalForcing`,
`RiverForcing`, `Grid`).

**Mechanism:** Introspects the *installed* roms-tools constructor parameters at test
time. For each parameter, asserts it is either:
1. A "data/run input" Forge provides programmatically (grid, dates, source, boundaries), OR
2. A typed field in the Forge `models.py` item model (`_FORGE_FIELDS`), OR
3. On the documented `SKIP_LIST` for that class (with reason).

Any roms-tools param in none of these three categories → **failing test**, not silent drift.

**Skip-list highlights:**
- `use_dask` — hardcoded from `RomsMarblInputData.use_dask` for all classes
- `chunks`, `initial_slice_bounds`, `bypass_validation` — advanced Dask/debug; accessible
  via `options` passthrough
- `indices` (RiverForcing) — manual river grid placement; expose in a later pass
- `physics_forcing` (BoundaryForcing) — internal object for density interp wiring
- `verbose`, `filename` (Grid) — debug only / loading existing grids

---

## What is still on the skip-list (potential future work)

| Class | Param | Notes |
|---|---|---|
| All | `chunks` | Advanced Dask chunk sizes; expose via `options` dict |
| All (most) | `bypass_validation` | Dev/debug; expose via `options` dict |
| IC, Surface, Boundary | `initial_slice_bounds` | Advanced spatial Dask subsetting |
| Boundary | `physics_forcing` | Auto-wired for density interp (complex object dep) |
| River | `indices` | Manual river-grid placement; planned future typed field |
| Grid | `verbose` | Debug only |
| Grid | `filename` | For loading an existing grid file, not generating |
| CDR / nesting | various | CDR is still free-form dict; nesting prefix/boundaries/verbose via `metadata_child` |

## Test counts

Before this work: 500 passed, 1 skipped  
After this work: **506 passed, 1 skipped** (+6 new drift guard tests)
