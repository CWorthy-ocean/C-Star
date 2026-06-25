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
import roms_tools as rt  # noqa: E402

# ── data/run inputs ──────────────────────────────────────────────────────────
# Forge provides these programmatically (grid object, dates, source dicts,
# boundaries, CDR releases) and they are NOT user-settable option knobs.
_RT_DATA_INPUTS = {
    "grid", "start_time", "end_time", "ini_time",
    "source", "bgc_source", "boundaries", "filepath", "releases",
}

# ── option fields in each Forge item model ───────────────────────────────────
# These are the typed fields added to models.py for each item type.
_FORGE_FIELDS = {
    "InitialConditions": {
        "use_density_interpolation", "allow_flex_time",
        # model_reference_date is handled run-level; options dict is passthrough
        "model_reference_date", "options",
    },
    "SurfaceForcing": {
        "type", "correct_radiation", "wind_dropoff",
        "restoring_forces", "coarse_grid_mode",
        "model_reference_date", "options",
    },
    "BoundaryForcing": {
        "type", "apply_2d_horizontal_fill", "use_density_interpolation",
        "model_reference_date", "options",
    },
    "TidalForcing": {
        "ntides", "model_reference_date", "options",
    },
    "RiverForcing": {
        "include_bgc", "convert_to_climatology", "bgc_source",
        "model_reference_date", "options",
    },
    "Grid": {
        "nx", "ny", "size_x", "size_y", "center_lon", "center_lat",
        "rot", "N", "theta_s", "theta_b", "hc",
        "topography_source", "hmin", "close_narrow_channels", "mask_shapefile",
    },
}

# ── intentional skip-lists ────────────────────────────────────────────────────
# Params deliberately NOT typed into the Forge schema. Document the reason.
_SKIP = {
    # All rt classes: use_dask is hardcoded from RomsMarblInputData.use_dask.
    "*": {"use_dask"},
    "InitialConditions": {
        "chunks",              # advanced Dask tuning; expose via options passthrough
        "initial_slice_bounds",  # advanced spatial Dask subsetting
        "bypass_validation",   # dev/debug knob; expose via options passthrough
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
        "physics_forcing",     # internal object for density interp wiring
    },
    "TidalForcing": {
        "bypass_validation",
    },
    "RiverForcing": {
        "indices",             # manual river grid placement; advanced, expose later
    },
    "Grid": {
        "verbose",             # debug/dev only
        "filename",            # for loading an existing grid file, not generating
    },
}


def _skip_for(cls_name: str) -> set:
    return _SKIP.get("*", set()) | _SKIP.get(cls_name, set())


@pytest.mark.integration
@pytest.mark.parametrize("cls_name,forge_cls_name", [
    ("InitialConditions", "InitialConditions"),
    ("SurfaceForcing", "SurfaceForcing"),
    ("BoundaryForcing", "BoundaryForcing"),
    ("TidalForcing", "TidalForcing"),
    ("RiverForcing", "RiverForcing"),
    ("Grid", "Grid"),
])
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
