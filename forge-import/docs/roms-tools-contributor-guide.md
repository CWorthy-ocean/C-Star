# Operating a roms-tools capability in Forge

**Audience:** contributors adding functionality to **roms-tools** who need Forge to
drive that functionality. This is the contract for *how a roms-tools constructor
parameter becomes usable through Forge* — first immediately, then (optionally) as a
first-class, validated, UI-surfaced option.

It is deliberately narrow: it does **not** cover authoring a whole SpecConfig, the
catalog, or the execution engine. See `docs/spec-config-inventory.md` for the full
input model and `docs/roms-tools-options-integration.md` for the historical record of
how these seams were built.

---

## The one principle

**The SpecConfig is the single source of truth, and it stays reproducible.** Every
value that affects results — including the escape hatch below — lives *inside* the
SpecConfig, is serialized to its YAML, and is covered by `content_hash()`. There is no
side channel. The same SpecConfig always produces the same inputs.

So the choice below is **not** "reproducible vs. not." It is "validated + discoverable
(typed field) vs. quick + unvalidated (passthrough)." Both reproduce identically.

---

## The seam (what you're plugging into)

`input_data.py` constructs every roms-tools object the same way:

```python
frc = rt.BoundaryForcing(grid=self.grid, **input_args)
```

`input_args` is assembled by `RomsMarblInputData._build_input_args`, which merges three
layers (later wins):

```
typed item-model fields   ←  options passthrough   ←  run-time injections (dates, use_dask)
   (validated defaults)         (raw rt kwargs)              (hardcoded by Forge)
```

An authored SpecConfig reaches this via the engine bridge
(`spec_config_engine.sources_to_forcing_override`), which `model_dump()`s each
`spec_config` forcing item into the dict `input_data` consumes. **The bridge is
generic** — it forwards every field, so you never edit it when adding a knob.

Grid is the one exception: grid parameters are not a typed item model but a free
`grid_kwargs` dict fed straight to `rt.Grid(**grid_kwargs)`. New grid params need no
Forge change — put them in `grid_kwargs`.

---

## Tier 1 — operate a new roms-tools param *today* (zero Forge changes)

You are never blocked waiting for a Forge release.

**Forcing / initial conditions** — use the `options` passthrough on the item. In the
SpecConfig YAML:

```yaml
forcing:
  boundary:
    - source: {name: GLORYS}
      type: physics
      options:                 # raw rt.BoundaryForcing kwargs, forwarded verbatim
        some_new_rt_param: 42
```

In the wizard, each forcing item (and initial conditions) has an **options** JSON editor
for exactly this. Whatever you put there is stored, hashed, and reproduces.

**Grid** — put the param directly in `grid_kwargs`.

Precedence: `options` overrides the typed defaults but loses to Forge's run-time
injections (`model_reference_date`, `use_dask`, dates). It is forwarded to roms-tools
**unvalidated** — a typo (`prefil=`) reaches the constructor and fails there, not at
authoring time. That's the tradeoff, and the reason for Tier 2.

---

## Tier 2 — promote a param to a typed field (validated + in the UI)

When a knob is stable and worth surfacing, promote it. Typed fields get Pydantic
validation, enum dropdowns, tooltips, and discoverability. Checklist:

1. **Add the field once, to the item model in `cstar_forge/forge/spec_config.py`.** The
   item models (`SurfaceForcingItem`, `BoundaryForcingItem`, `TidalForcingItem`,
   `RiverForcingItem`, `InitialConditions`, `SourceSpec`) are **single-sourced** there and
   re-exported by `cstar_forge/models.py` (with `InitialConditions` aliased to the legacy
   name `InitialConditionsInput`), so one edit covers both the authoring and processing
   sides. For a constrained set of values, define a `str, Enum` in `spec_config.py` too
   (it stays import-light and relocatable). Items are `extra="forbid"` — unknown kwargs
   must go through `options`, not as loose fields.
2. **Record it in the drift guard** — add the field name to the class's entry in
   `_FORGE_FIELDS` in `tests/test_roms_tools_coverage.py`.
3. **Surface it in the wizard** — add a control in `spec_config_wizard.py`
   (`_ForcingEditor._make_row` / `gather`, plus the load path `_sources_to_inputs` so it
   round-trips). Add a tooltip via `HELP_TEXT`.

The bridge needs no change (it's generic). Emitting only non-default values in the
wizard keeps authored specs clean — follow the surrounding pattern in `_gather_item`.

---

## Guards (what stops silent drift)

Both run in CI (`pytest tests/` with no marker filter):

| Guard | File | Fails when |
|---|---|---|
| **roms-tools coverage** | `test_roms_tools_coverage.py::test_all_rt_params_are_exposed_or_skipped` | A new rt constructor param is neither a typed Forge field, a data/run input, nor on the documented `_SKIP` list. Forces a decision on every new roms-tools parameter. |
| **single-source** | `test_roms_tools_coverage.py::test_forge_item_models_are_single_sourced` | `cstar_forge.models` stops re-exporting the exact `forge.spec_config` item class (i.e. someone re-introduced a divergent copy in `models.py`). |

If the coverage guard fails on a param you don't want to type yet, add it to `_SKIP`
with a one-line reason (it remains usable via `options`). If the single-source guard
fails, delete the duplicate definition in `models.py` and re-export from
`forge/spec_config.py` instead.

---

## Rule of thumb

- New roms-tools param, need it now → `options` (or `grid_kwargs`). Reproducible,
  unvalidated, invisible in the UI beyond the raw JSON editor.
- Param is here to stay → promote to a typed field (one edit in `forge/spec_config.py`
  + drift guard + wizard). Validated, discoverable, first-class.
- `options` is the pressure valve for the window between "roms-tools shipped it" and
  "Forge typed it" — in a healthy repo it stays near-empty, because the coverage guard
  won't let a new param hide there unnoticed.
