"""
Phase 3: roms-tools option drift guard.

Introspects each rt constructor's parameters against Forge's item models (models.py).
Every rt parameter that is NOT a "data/run input" (provided programmatically by Forge)
must either:
  (a) be a typed field in the Forge item model, OR
  (b) be in the explicit SKIP_LIST for that class (documented as "intentionally deferred").

A new roms-tools parameter that appears in neither will cause this test to FAIL — making
drift visible in CI rather than silent.

Run with: pytest tests/test_roms_tools_coverage.py -m integration -v
"""

import inspect

import pytest

pytest.importorskip("roms_tools")
import roms_tools as rt

# ── data/run inputs ──────────────────────────────────────────────────────────
# Forge provides these programmatically (grid object, dates, source dicts,
# boundaries, CDR releases) and they are NOT user-settable option knobs.
_RT_DATA_INPUTS = {
    "grid",
    "start_time",
    "end_time",
    "ini_time",
    "source",
    "bgc_source",
    "boundaries",
    "filepath",
    "releases",
}

# ── option fields in each Forge item model ───────────────────────────────────
# These are the typed fields added to models.py for each item type.
_FORGE_FIELDS = {
    "InitialConditions": {
        "bgc_interpolation_method",
        "allow_flex_time",
        # model_reference_date is handled run-level; options dict is passthrough
        "model_reference_date",
        "options",
    },
    "SurfaceForcing": {
        "type",
        "correct_radiation",
        "wind_dropoff",
        "restoring_forces",
        "coarse_grid_mode",
        "model_reference_date",
        "options",
    },
    "BoundaryForcing": {
        "type",
        "bgc_interpolation_method",
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
        "model_reference_date",
        "options",
    },
    "TidalForcing": {
        "ntides",
        "model_reference_date",
        "options",
    },
    "RiverForcing": {
        "include_bgc",
        "convert_to_climatology",
        "bgc_source",
        "coast_snap_buffer_km",
        "domain_edge_buffer",
        "model_reference_date",
        "options",
    },
    "Grid": {
        "nx",
        "ny",
        "size_x",
        "size_y",
        "center_lon",
        "center_lat",
        "rot",
        "N",
        "theta_s",
        "theta_b",
        "hc",
        "topography_source",
        "hmin",
        "close_narrow_channels",
        "mask_shapefile",
    },
}

# ── intentional skip-lists ────────────────────────────────────────────────────
# Params deliberately NOT typed into the Forge schema. Document the reason.
_SKIP = {
    # All rt classes: use_dask is hardcoded from RomsMarblInputData.use_dask.
    "*": {"use_dask"},
    "InitialConditions": {
        "chunks",  # advanced Dask tuning; expose via options passthrough
        "initial_slice_bounds",  # advanced spatial Dask subsetting
        "bypass_validation",  # dev/debug knob; expose via options passthrough
    },
    "SurfaceForcing": {
        "chunks",
        "initial_slice_bounds",
        "bypass_validation",
    },
    "BoundaryForcing": {
        "chunks",
        "initial_slice_bounds",
        "bypass_validation",
        "physics_forcing",  # internal object for density interp wiring (set by Forge, not user)
        "apply_2d_horizontal_fill",  # deprecated in rt>=4 in favor of `prefill`; Forge exposes prefill instead
    },
    "TidalForcing": {
        "bypass_validation",
    },
    "RiverForcing": {
        "indices",  # manual river grid placement; advanced, expose later
    },
    "Grid": {
        "verbose",  # debug/dev only
        "filename",  # for loading an existing grid file, not generating
    },
}


def _skip_for(cls_name: str) -> set:
    return _SKIP.get("*", set()) | _SKIP.get(cls_name, set())


@pytest.mark.integration
@pytest.mark.parametrize(
    "cls_name,forge_cls_name",
    [
        ("InitialConditions", "InitialConditions"),
        ("SurfaceForcing", "SurfaceForcing"),
        ("BoundaryForcing", "BoundaryForcing"),
        ("TidalForcing", "TidalForcing"),
        ("RiverForcing", "RiverForcing"),
        ("Grid", "Grid"),
    ],
)
def test_all_rt_params_are_exposed_or_skipped(cls_name, forge_cls_name):
    """Every rt constructor parameter must be either:
    (a) a "data/run input" Forge provides programmatically,
    (b) a typed field in the Forge item model, OR
    (c) on the SKIP_LIST with a documented reason.
    Any parameter in none of these three categories fails the test, surfacing drift.
    """
    cls = getattr(rt, cls_name)
    try:
        params = set(inspect.signature(cls).parameters.keys())
    except (ValueError, TypeError):
        params = set(cls.model_fields.keys()) if hasattr(cls, "model_fields") else set()

    exposed_in_forge = _FORGE_FIELDS[forge_cls_name]
    skipped = _skip_for(cls_name)
    accounted_for = _RT_DATA_INPUTS | exposed_in_forge | skipped

    uncovered = params - accounted_for
    assert not uncovered, (
        f"rt.{cls_name} has parameters NOT accounted for in Forge: {sorted(uncovered)}. "
        "Either add them as typed fields in models.py/spec_config.py "
        "or add them to the SKIP_LIST in tests/test_roms_tools_coverage.py with a reason."
    )


# ── models.py ↔ spec_config.py lockstep ───────────────────────────────────────
# The option knobs live in TWO parallel Forge schemas: the processing-side item
# models in ``cstar_forge.models`` (consumed by ``RomsMarblInputData``) and the
# authoring-side item models in ``cstar_forge.forge.spec_config`` (the SpecConfig / wizard).
# The engine bridge (``sources_to_forcing_override``) ``model_dump``s the spec_config
# item and feeds it to the models.py item, so a typed field added to one but not the
# other silently breaks the round trip: a value set in the UI never reaches
# ``input_data``, or a processing field is un-authorable. These pairs must expose
# identical field sets. See docs/roms-tools-contributor-guide.md.
_ITEM_MODEL_PAIRS = [
    ("InitialConditionsInput", "InitialConditions"),
    ("SurfaceForcingItem", "SurfaceForcingItem"),
    ("BoundaryForcingItem", "BoundaryForcingItem"),
    ("TidalForcingItem", "TidalForcingItem"),
    ("RiverForcingItem", "RiverForcingItem"),
]


@pytest.mark.parametrize("models_name,spec_name", _ITEM_MODEL_PAIRS)
def test_forge_item_models_in_lockstep(models_name, spec_name):
    """The processing-side (``models.py``) and authoring-side (``spec_config.py``)
    item models must expose identical field sets, so every option knob is both
    authorable in the SpecConfig/UI and forwarded to ``input_data``. Drift here means
    a UI value is silently dropped by the bridge (or an ``input_data`` field is
    un-authorable) — exactly the class of bug this guard exists to prevent.
    """
    from cstar_forge import models
    from cstar_forge.forge import spec_config

    models_fields = set(getattr(models, models_name).model_fields.keys())
    spec_fields = set(getattr(spec_config, spec_name).model_fields.keys())

    missing_in_spec = models_fields - spec_fields
    missing_in_models = spec_fields - models_fields
    assert not (missing_in_spec or missing_in_models), (
        "Forge item models drifted (add the field to BOTH, with a matching type):\n"
        f"  models.{models_name} has fields absent from spec_config.{spec_name}: "
        f"{sorted(missing_in_spec)}\n"
        f"  spec_config.{spec_name} has fields absent from models.{models_name}: "
        f"{sorted(missing_in_models)}\n"
        "See docs/roms-tools-contributor-guide.md (promoting a roms-tools option)."
    )
