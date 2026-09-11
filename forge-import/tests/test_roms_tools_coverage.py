"""
roms-tools option drift guard.

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
    # BGCMarbl.process_bgc_fields: the already-built forcing objects it completes
    # in place -- pure data, not a user-configurable option.
    "forcings",
    # InitialConditions/BoundaryForcing (roms-tools >=5 monolithic wrapper): the
    # resolved bgc_sources list (forge_blueprint.BgcSourceItem, each already
    # resolved through SourceData) and the BGCModel class Forge always passes
    # (rt.BGCMarbl) when bgc_sources is non-empty -- both pure data, never a raw
    # user-facing option knob.
    "bgc_sources",
    "bgc_model",
}

# ── option fields in each Forge item model ───────────────────────────────────
# These are the typed fields added to models.py for each item type.
_FORGE_FIELDS = {
    "InitialConditions": {
        "bgc_interpolation_method",
        "allow_flex_time",
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
        # model_reference_date is handled run-level; options dict is passthrough
        "model_reference_date",
        "options",
        # wizard "validate" checkbox (checked = bypass_validation=False, the
        # roms-tools default); promoted from the SKIP_LIST after a real
        # production crash traced to _validate() running unprotected.
        "bypass_validation",
    },
    "SurfaceForcing": {
        "type",
        "correct_radiation",
        "wind_dropoff",
        "restoring_forces",
        "coarse_grid_mode",
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
        "model_reference_date",
        "options",
    },
    "BoundaryForcing": {
        # `type` dropped entirely in the roms-tools >=5 wrapper (physics is the
        # required `source`, bgc is the `bgc_sources` list -- no discriminator).
        "bgc_interpolation_method",
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
        "model_reference_date",
        "options",
        # wizard "validate" checkbox (checked = bypass_validation=False, the
        # roms-tools default); promoted from the SKIP_LIST after a real
        # production crash traced to _validate() running unprotected.
        "bypass_validation",
    },
    "TidalForcing": {
        "ntides",
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
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
    # serialize_dask: forge-only write flag (a `.save()` kwarg, not a
    # constructor param of any class here) -- RomsMarblInputData.serialize_dask_write
    # / BgcSourceItem.serialize_dask control it; never a user-facing rt option.
    "*": {"use_dask", "serialize_dask"},
    "InitialConditions": {
        "chunks",  # advanced Dask tuning; expose via options passthrough
        "initial_slice_bounds",  # advanced spatial Dask subsetting
        # legacy single-bgc-source convenience (forge never emits this directly,
        # but rt.InitialConditions' own wrapper constructor still accepts it);
        # use_vars lives on BgcSourceItem, forwarded per item.
        "use_vars",
    },
    "SurfaceForcing": {
        "chunks",
        "initial_slice_bounds",
        "bypass_validation",  # dev/debug knob; expose via options passthrough
        # padding an extra time record before/after the run window; roms-tools
        # defaults (True/True) are correct for the normal case, not yet exposed
        # as a Forge option (same gap as BoundaryForcing's).
        "start_time_pad",
        "end_time_pad",
    },
    "BoundaryForcing": {
        "chunks",
        "initial_slice_bounds",
        "apply_2d_horizontal_fill",  # deprecated in rt>=4 in favor of `prefill`; Forge exposes prefill instead
        # padding an extra time record before/after the run window; roms-tools
        # defaults (True/True) are correct for the normal ROMS boundary-interp
        # case, not yet exposed as a Forge option.
        "start_time_pad",
        "end_time_pad",
    },
    "TidalForcing": {
        "bypass_validation",
    },
    "RiverForcing": {
        "indices",  # manual river grid placement; advanced, expose later
        "surface_forcing_source",  # ERA5 source for river temperature sampling; not yet exposed
        "river_temp_smoothing_window_days",  # smoothing window for river temp estimate; not yet exposed
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
        "Either add them as typed fields in models.py/forge_blueprint.py "
        "or add them to the SKIP_LIST in tests/test_roms_tools_coverage.py with a reason."
    )


@pytest.mark.integration
def test_bgc_marbl_process_bgc_fields_params_are_data_inputs():
    """``BGCMarbl.process_bgc_fields`` isn't a constructor (so it isn't covered by
    ``test_all_rt_params_are_exposed_or_skipped`` above) but is still user-facing
    roms-tools surface Forge drives (batched boundary/IC bgc completion + save --
    see ``RomsMarblInputData._generate_boundary_forcing``/``_generate_initial_conditions``).
    Both its params (``forcings``, the already-built objects; ``filepath``, output
    path(s)) are pure data/output-path inputs, not user-configurable Forge fields.
    """
    params = set(
        inspect.signature(rt.BGCMarbl.process_bgc_fields).parameters.keys()
    ) - {"self"}
    uncovered = params - _RT_DATA_INPUTS
    assert not uncovered, (
        f"rt.BGCMarbl.process_bgc_fields has parameters NOT accounted for as data "
        f"inputs: {sorted(uncovered)}. Either add them to _RT_DATA_INPUTS (if "
        "they're pure data/output-path inputs) or give them proper typed-field/"
        "SKIP_LIST coverage."
    )


# ── single-source item models ──────────────────────────────────────
# The forcing/IC item models are now defined ONCE in ``cstar_forge.forge.forge_blueprint``
# and re-exported by ``cstar_forge.models`` (with ``InitialConditions`` aliased to the
# legacy name ``InitialConditionsInput``). This guard asserts they are literally the same
# class, so the "two parallel schemas" duplication cannot silently re-appear — adding a
# roms-tools option field is a one-place edit. See docs/roms-tools-contributor-guide.md.
_ITEM_MODEL_PAIRS = [
    ("InitialConditionsInput", "InitialConditions"),
    ("SurfaceForcingItem", "SurfaceForcingItem"),
    ("BoundaryForcing", "BoundaryForcing"),
    ("TidalForcingItem", "TidalForcingItem"),
    ("RiverForcingItem", "RiverForcingItem"),
    ("BgcSourceItem", "BgcSourceItem"),
]


@pytest.mark.parametrize("models_name,spec_name", _ITEM_MODEL_PAIRS)
def test_forge_item_models_are_single_sourced(models_name, spec_name):
    """``cstar_forge.models`` must re-export the exact ``forge.forge_blueprint`` item class
    (single source of truth) — not a divergent copy. If someone re-introduces a separate
    definition in models.py, these stop being the same object and this guard fails.
    """
    from cstar_forge import models
    from cstar_forge.forge import forge_blueprint

    assert getattr(models, models_name) is getattr(forge_blueprint, spec_name), (
        f"models.{models_name} is not the same class as forge_blueprint.{spec_name} — "
        "the item models must be single-sourced in forge/forge_blueprint.py and re-exported "
        "by models.py. See docs/roms-tools-contributor-guide.md."
    )
