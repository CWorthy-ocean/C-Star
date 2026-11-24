"""Tests for the Forcing card's editor (cstar.wizard.forcing_editor).

These build a ``_ForcingEditor`` without a wizard: its rows, sources and options, and
the seed it is given by ``ForgeBlueprintWizard._sources_to_inputs``.
"""

import copy
from types import SimpleNamespace

import pytest

from cstar.applications.forge.blueprint import (
    BgcSourceItem,
    Forcing,
    SurfaceBlend,
    SurfaceBlendFallback,
    SurfaceForcingItem,
)
from cstar.tests.unit_tests.wizard.helpers import (
    _BLEND_SECTIONS,
    _display,
    _iter_widget_tree,
)
from cstar.wizard.forcing_editor import _BOUNDARY_NONE, _IC_NONE, _ForcingEditor
from cstar.wizard.wizard import ForgeBlueprintWizard


def test_surface_row_visibility_by_type(editor):
    """Item 2/8: restore/corr_rad/wind_dropoff only show for their relevant type."""
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    assert _display(w["correct_radiation"]) == ""  # physics -> shown
    assert _display(w["wind_dropoff"]) == ""
    assert _display(w["restoring_forces"]) == "none"  # not restoring -> hidden

    w["type"].value = "restoring"
    assert _display(w["restoring_forces"]) == ""  # restoring -> shown
    assert _display(w["correct_radiation"]) == "none"  # no longer physics -> hidden
    assert _display(w["wind_dropoff"]) == "none"

    w["type"].value = "bgc"
    assert _display(w["restoring_forces"]) == "none"
    assert _display(w["correct_radiation"]) == "none"
    assert _display(w["wind_dropoff"]) == "none"


def test_boundary_row_layout_visibility_by_source_name(editor):
    """Item 7: glorys_layout only shows when the source name is GLORYS. Boundary's
    physics source is a required scalar now (self.boundary_name/boundary_layout,
    mirroring IC's), not a row -- see __init__/_ROW_CATEGORIES. BGC boundary
    sources (UNIFIED/CESM_REGRIDDED/GLODAP/constants/ESPER) live in their own
    "boundary_bgc" row-list/pane instead, which never offers GLORYS and has no
    glorys_layout widget at all (not just hidden -- absent).
    """
    assert editor.boundary_name.value == "GLORYS"
    assert _display(editor.boundary_layout) == ""

    bgc_w = editor._make_row("boundary_bgc", {"source": {"name": "UNIFIED"}})
    assert bgc_w["name"].value != "GLORYS"
    assert "glorys_layout" not in bgc_w


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
@pytest.mark.parametrize("name", ["constants", "ESPER", "GLODAP"])
def test_climatology_hidden_for_static_bgc_sources(editor, cat, name):
    """None of these sources has a time axis of its own -- constants is inline
    values, ESPER is derived from the physics T/S, and GLODAP is a single static
    field -- so none can be a climatology, and roms-tools raises ValueError if one
    is handed climatology=True. Hiding the checkbox keeps that unreachable.
    """
    w = editor._make_row(cat, {"source": {"name": name}})
    assert _display(w["climatology"]) == "none"


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_climatology_shown_for_dataset_backed_bgc_sources(editor, cat):
    """The gate is only about the static sources -- a regridded, time-varying
    dataset still gets the checkbox.
    """
    for name in ("UNIFIED", "WOA_BGC"):
        w = editor._make_row(cat, {"source": {"name": name}})
        assert _display(w["climatology"]) == "", name


def test_glodap_keeps_regrid_knobs_unlike_constants_and_esper(editor):
    """GLODAP has a wider climatology gate than the regrid gate: it IS a regridded
    dataset, so hiding its climatology must not sweep away the regrid knobs the way
    the derived pseudo-sources do.
    """
    w = editor._make_row("boundary_bgc", {"source": {"name": "GLODAP"}})
    assert _display(w["climatology"]) == "none"
    assert _display(w["bgc_interpolation_method"]) == ""  # still regridded

    w = editor._make_row("boundary_bgc", {"source": {"name": "constants"}})
    assert _display(w["bgc_interpolation_method"]) == "none"  # not a dataset at all


def test_stale_climatology_not_emitted_after_switch_to_static_source(editor):
    """The checkbox is hidden rather than destroyed, so a tick left over from a
    previously selected source would otherwise be gathered into the blueprint and
    trip roms-tools' validation on a source that can never be a climatology.
    """
    w = editor._make_row(
        "boundary_bgc", {"source": {"name": "UNIFIED", "climatology": True}}
    )
    assert editor._gather_item("boundary_bgc", w)["source"]["climatology"] is True

    w["name"].value = "GLODAP"
    editor._apply_row_visibility(w)
    assert w["climatology"].value is True  # widget state survives the switch
    assert "climatology" not in editor._gather_item("boundary_bgc", w)["source"]


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_esper_dropdown_is_labelled_experimental_but_stores_plain_name(editor, cat):
    """PyESPER support is experimental, and the dropdown says so -- but the label is
    presentation only: the blueprint must still carry the bare "ESPER" source name.
    """
    w = editor._make_row(cat, {"source": {"name": "ESPER"}})
    assert ("ESPER (experimental)", "ESPER") in w["name"].options
    assert w["name"].value == "ESPER"
    assert editor._gather_item(cat, w)["source"]["name"] == "ESPER"


def test_unlabelled_sources_keep_their_bare_name_in_the_dropdown(editor):
    """Only ESPER carries a display label; every other source is its own label, so
    `(label, value)` options must not change what the rest of the UI sees.
    """
    w = editor._make_row("boundary_bgc", {"source": {"name": "UNIFIED"}})
    assert ("UNIFIED", "UNIFIED") in w["name"].options
    assert [value for _label, value in w["name"].options] == [
        "UNIFIED",
        "CESM_REGRIDDED",
        "GLODAP",
        "WOA_BGC",
        "constants",
        "ESPER",
    ]


def test_type_switch_rebuilds_labelled_options(editor):
    """Changing a surface row's type rewrites the source dropdown; membership there
    has to be tested against the bare names, not the (label, value) pairs, or the
    value reset would fire on every switch.
    """
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    w["type"].value = "bgc"
    # MBL_co2 is the "Atmospheric CO2" block's, not a row source.
    assert [value for _label, value in w["name"].options] == [
        "UNIFIED",
        "CESM_REGRIDDED",
    ]
    assert w["name"].value == "UNIFIED"


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
@pytest.mark.parametrize("name", ["ESPER", "UNIFIED"])
def test_serialize_dask_is_not_offered_as_a_widget(editor, cat, name):
    """`serialize_dask` is deliberately unexposed, for every source including ESPER.

    PyESPER serialises entry into its own numba kernels with a per-process
    semaphore, so the flag no longer buys the protection it was added for -- it
    only forces the rest of that write onto the one-task-at-a-time path. It stays
    a blueprint field and a CLI flag for manual troubleshooting.
    """
    w = editor._make_row(cat, {"source": {"name": name}})
    assert "serialize_dask" not in w
    item = editor._gather_item(cat, w)
    assert "serialize_dask" not in item
    assert "serialize_dask" not in item["source"]


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_serialize_dask_round_trips_when_the_blueprint_already_set_it(editor, cat):
    """Loading a blueprint that sets it and re-saving must not drop it.

    The wizard has no widget for it, but silently rewriting someone's blueprint on
    an unrelated edit is worse than leaving a field they can delete deliberately --
    and `_build_bgc_section` has already been bitten once by dropping exactly this
    field. It lands at item level, never inside `source`, where roms-tools'
    extra="forbid" validation would reject it.
    """
    w = editor._make_row(cat, {"source": {"name": "ESPER"}, "serialize_dask": True})
    item = editor._gather_item(cat, w)
    assert item["serialize_dask"] is True
    assert "serialize_dask" not in item["source"]


def test_serialize_dask_not_invented_for_rows_that_lacked_it(editor):
    """An untouched row stays absent, so the source keeps inheriting the CLI flag
    rather than pinning a hard value.
    """
    w = editor._make_row("ic_bgc", {"source": {"name": "ESPER"}})
    assert "serialize_dask" not in editor._gather_item("ic_bgc", w)


def test_serialize_dask_not_offered_on_surface_rows(editor):
    """Surface bgc is not a BgcSourceItem -- its schema has no `serialize_dask`, and
    `_Section` is extra="forbid", so emitting one there would fail validation.
    """
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "UNIFIED"}})
    assert "serialize_dask" not in w
    assert "serialize_dask" not in editor._gather_item("surface", w)


def test_surface_row_never_shows_layout(editor):
    """Surface sources never include GLORYS, so the layout box is always hidden."""
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    assert _display(w["glorys_layout"]) == "none"


def test_ic_layout_visibility_initial_state():
    """Item 7 (IC side): glorys_layout is shown for the (currently sole) IC source,
    GLORYS. InitialConditionsSource has only one member today, so the hidden branch
    can't be exercised via the dropdown; this pins the visible/default case.
    """
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {"initial_conditions": {"source": {"name": "GLORYS"}}},
        on_change=lambda: None,
    )
    assert ed.ic_name.value == "GLORYS"
    assert _display(ed.ic_layout) == ""


def test_row_box_puts_remove_button_first_then_type(editor):
    """The remove button is always left-most (never clipped off-screen by a wide
    row -- see _row_box); `type` (when present) comes right after it.
    """
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "ERA5"}})
    box = editor._row_box(w)
    assert box.children[0] is w["_remove_btn"]
    assert box.children[1] is w["type"]


def test_row_box_without_type_unaffected(editor):
    """tidal/river rows have no `type`; ordering must not error or reorder oddly."""
    w = editor._make_row("tidal", {"ntides": 15, "source": {"name": "TPXO"}})
    box = editor._row_box(w)
    assert w["ntides"] in box.children


def test_boundary_bgc_add_and_remove_buttons_always_present(editor):
    """The "boundary_bgc" row-list (the only boundary row-list now -- physics is a required
    scalar, see __init__) keeps its add/remove buttons regardless of row count,
    like every other bgc row-list ("ic_bgc").
    """
    assert editor._rows["boundary_bgc"] == []
    editor._render("boundary_bgc")
    assert any(
        getattr(c, "description", "") == "add bgc source"
        for c in _iter_widget_tree(editor._containers["boundary_bgc"])
    )

    editor._add("boundary_bgc")
    assert len(editor._rows["boundary_bgc"]) == 1
    remove_btn = editor._rows["boundary_bgc"][0]["_remove_btn"]
    assert remove_btn.layout.display == ""
    assert any(
        getattr(c, "description", "") == "add bgc source"
        for c in _iter_widget_tree(editor._containers["boundary_bgc"])
    )


@pytest.mark.parametrize("cat", ["surface", "tidal"])
def test_regrid_widgets_present_and_gathered(editor, cat):
    """prefill/regrid_method/extrap_method dropdowns (roms-tools >=4) are built for
    surface, boundary, and tidal rows alike, and a non-blank selection round-trips
    through ``_gather_item``.
    """
    seed = {"source": {"name": "TPXO" if cat == "tidal" else "ERA5"}}
    if cat != "tidal":
        seed["type"] = "physics"
    w = editor._make_row(cat, seed)
    assert w["prefill"].value == ""  # blank sentinel = leave unset
    assert w["regrid_method"].value == ""
    assert w["extrap_method"].value == ""

    w["prefill"].value = "inverse_dist"
    w["regrid_method"].value = "xesmf"
    w["extrap_method"].value = "nearest_s2d"
    item = editor._gather_item(cat, w)
    assert item["prefill"] == "inverse_dist"
    assert item["regrid_method"] == "xesmf"
    assert item["extrap_method"] == "nearest_s2d"


def test_ic_regrid_widgets_seed_gather_and_layout():
    """IC prefill/regrid_method/extrap_method dropdowns seed from a loaded config,
    gather back into the authored dict, and are actually placed in the displayed
    ic_box (a widget built but never laid out renders invisibly).
    """
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "prefill": "nearest_neighbor",
                "regrid_method": "scipy",
            }
        },
        on_change=lambda: None,
    )
    assert ed.ic_prefill.value == "nearest_neighbor"
    assert ed.ic_regrid_method.value == "scipy"
    assert ed.ic_extrap_method.value == ""

    ed.ic_extrap_method.value = "nearest_s2d"
    gathered = ed.gather()
    ic = gathered["initial_conditions"]
    assert ic["prefill"] == "nearest_neighbor"
    assert ic["regrid_method"] == "scipy"
    assert ic["extrap_method"] == "nearest_s2d"

    # layout check: the widgets must actually be reachable from the rendered widget
    all_children = []

    def _walk(node):
        all_children.append(node)
        for c in getattr(node, "children", []):
            _walk(c)

    _walk(ed.widget)
    assert ed.ic_prefill in all_children
    assert ed.ic_regrid_method in all_children
    assert ed.ic_extrap_method in all_children


def test_boundary_regrid_widgets_seed_gather_and_layout():
    """Boundary's prefill/regrid_method/extrap_method dropdowns (a scalar group,
    mirroring IC's -- see __init__/gather()) seed from a loaded config, gather
    back into the authored dict, and are actually placed in the rendered widget.
    """
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {
            "forcing": {
                "boundary": {
                    "source": {"name": "GLORYS"},
                    "prefill": "nearest_neighbor",
                    "regrid_method": "scipy",
                }
            }
        },
        on_change=lambda: None,
    )
    assert ed.boundary_prefill.value == "nearest_neighbor"
    assert ed.boundary_regrid_method.value == "scipy"
    assert ed.boundary_extrap_method.value == ""

    ed.boundary_extrap_method.value = "nearest_s2d"
    gathered = ed.gather()
    boundary = gathered["forcing"]["boundary"]
    assert boundary["prefill"] == "nearest_neighbor"
    assert boundary["regrid_method"] == "scipy"
    assert boundary["extrap_method"] == "nearest_s2d"

    all_children = []

    def _walk(node):
        all_children.append(node)
        for c in getattr(node, "children", []):
            _walk(c)

    _walk(ed.widget)
    assert ed.boundary_prefill in all_children
    assert ed.boundary_regrid_method in all_children
    assert ed.boundary_extrap_method in all_children


def _blend_seed(section: str, **block) -> dict:
    """A forcing-editor seed with a GLORYS IC or boundary block carrying ``block``."""
    block = {"source": {"name": "GLORYS"}, **block}
    return (
        {"initial_conditions": block}
        if section == "ic"
        else {"forcing": {"boundary": block}}
    )


def _blend_block(gathered: dict, section: str) -> dict:
    return (
        gathered["initial_conditions"]
        if section == "ic"
        else gathered["forcing"]["boundary"]
    )


@pytest.mark.parametrize("section", _BLEND_SECTIONS)
@pytest.mark.parametrize(
    ("loaded", "shown"),
    [
        (None, ""),
        (100.0, "100"),
        (12.5, "12.5"),
        (0.0, "0"),
        (float("inf"), "inf"),
        # shown exactly, so a re-save never changes the value
        (12.3456789, "12.3456789"),
        (1234567.0, "1234567"),
    ],
)
def test_bottom_fill_blend_m_seeds_as_text(section, loaded, shown):
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        _blend_seed(section, bottom_fill_blend_m=loaded),
        on_change=lambda: None,
    )
    assert getattr(ed, f"{section}_bottom_fill_blend_m").value == shown


@pytest.mark.parametrize("section", _BLEND_SECTIONS)
def test_bottom_fill_blend_m_absent_seeds_blank_and_gathers_no_key(section):
    import ipywidgets as W

    ed = _ForcingEditor(W, _blend_seed(section), on_change=lambda: None)
    box = getattr(ed, f"{section}_bottom_fill_blend_m")
    assert box.value == ""
    assert "bottom_fill_blend_m" not in _blend_block(ed.gather(), section)

    box.value = "   "  # whitespace alone is blank too
    assert "bottom_fill_blend_m" not in _blend_block(ed.gather(), section)


@pytest.mark.parametrize("section", _BLEND_SECTIONS)
@pytest.mark.parametrize(
    ("text", "gathered"),
    [
        ("12.5", 12.5),
        (" 100 ", 100.0),
        ("0", 0.0),  # zero is a real setting, not "unset"
        ("inf", float("inf")),
        # Unparseable text is kept as typed, so the blueprint's own validation
        # reports it instead of the wizard silently dropping it.
        ("deep", "deep"),
        (" 25 m ", "25 m"),
    ],
)
def test_bottom_fill_blend_m_gathers_typed_text(section, text, gathered):
    import ipywidgets as W

    ed = _ForcingEditor(W, _blend_seed(section), on_change=lambda: None)
    getattr(ed, f"{section}_bottom_fill_blend_m").value = text
    got = _blend_block(ed.gather(), section)["bottom_fill_blend_m"]
    assert got == gathered
    assert type(got) is type(gathered)


@pytest.mark.parametrize(
    ("section", "none_sentinel"),
    [("ic", _IC_NONE), ("boundary", _BOUNDARY_NONE)],
)
def test_bottom_fill_blend_m_widget_is_laid_out_and_hidden_with_no_source(
    section, none_sentinel
):
    """The text box is reachable from the rendered widget (one built but never laid
    out renders invisibly), and goes with the rest of the section when its source is
    "(none)".
    """
    import ipywidgets as W

    ed = _ForcingEditor(W, _blend_seed(section), on_change=lambda: None)
    box = getattr(ed, f"{section}_bottom_fill_blend_m")

    all_children = []

    def _walk(node):
        all_children.append(node)
        for c in getattr(node, "children", []):
            _walk(c)

    _walk(ed.widget)
    assert box in all_children
    assert _display(box) == ""

    getattr(ed, f"{section}_name").value = none_sentinel
    assert _display(box) == "none"
    getattr(ed, f"{section}_name").value = "GLORYS"
    assert _display(box) == ""


def test_sources_to_inputs_round_trips_bottom_fill_blend_m():
    """A loaded blueprint's ``bottom_fill_blend_m`` (including zero, which is not
    "unset", and ``inf``) reaches the text boxes and gathers back unchanged.
    """
    forcing = Forcing.model_validate(
        {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bottom_fill_blend_m": 0.0,
            },
            "boundary": {
                "source": {"name": "GLORYS"},
                "bottom_fill_blend_m": float("inf"),
            },
        }
    )
    seed = ForgeBlueprintWizard._sources_to_inputs(SimpleNamespace(forcing=forcing))

    import ipywidgets as W

    ed = _ForcingEditor(W, seed, on_change=lambda: None)
    assert ed.ic_bottom_fill_blend_m.value == "0"
    assert ed.boundary_bottom_fill_blend_m.value == "inf"
    gathered = ed.gather()
    assert gathered["initial_conditions"]["bottom_fill_blend_m"] == 0.0
    assert gathered["forcing"]["boundary"]["bottom_fill_blend_m"] == float("inf")


def test_ic_bypass_validation_round_trips_through_editor():
    """Item 7: the "validate" checkbox is checked (validation on) by default;
    unchecking it must round-trip as ``bypass_validation: True`` and reload
    still unchecked.
    """
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bypass_validation": True,
            }
        },
        on_change=lambda: None,
    )
    assert ed.ic_validate.value is False
    gathered = ed.gather()
    assert gathered["initial_conditions"]["bypass_validation"] is True

    ed2 = _ForcingEditor(W, gathered, on_change=lambda: None)
    assert ed2.ic_validate.value is False


def test_boundary_bypass_validation_round_trips_through_editor():
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {
            "forcing": {
                "boundary": {
                    "source": {"name": "GLORYS"},
                    "bypass_validation": True,
                }
            }
        },
        on_change=lambda: None,
    )
    assert ed.boundary_validate.value is False
    gathered = ed.gather()
    assert gathered["forcing"]["boundary"]["bypass_validation"] is True

    ed2 = _ForcingEditor(W, gathered, on_change=lambda: None)
    assert ed2.boundary_validate.value is False


def test_remove_middle_row_preserves_order(editor):
    """Nit: removing a middle row must not reorder or drop its neighbors."""
    for name in ("UNIFIED", "GLODAP", "WOA_BGC"):
        editor._rows["ic_bgc"].append(
            editor._make_row("ic_bgc", {"source": {"name": name}})
        )
    editor._render("ic_bgc")
    assert len(editor._rows["ic_bgc"]) == 3

    editor._remove("ic_bgc", editor._rows["ic_bgc"][1])

    gathered = editor.gather()
    assert [
        b["source"]["name"] for b in gathered["initial_conditions"]["bgc_sources"]
    ] == ["UNIFIED", "WOA_BGC"]


# ===========================================================================
# F1: a surface bgc row must never offer/emit `use_vars`/`constants`/`esper_*`
# (SurfaceForcingItem has no `use_vars` field at all, and BgcSurfaceSource
# never offers "constants"/"ESPER").
# ===========================================================================


def test_surface_bgc_row_never_gets_use_vars_or_bgc_source_widgets(editor):
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "UNIFIED"}})
    assert "use_vars" not in w
    assert "constants" not in w
    assert "esper_method" not in w
    assert "esper_equation" not in w
    assert "esper_salinity_conditioning" not in w


def test_surface_bgc_row_gathers_a_validating_surface_forcing_item(editor):
    """A surface bgc row (even with use_vars unavailable to type into) must
    still gather to a dict that validates as ``SurfaceForcingItem`` --
    ``use_vars`` is not part of that schema (``extra='forbid'``), so it must
    never be emitted for a surface row the way it correctly is for ic_bgc/
    boundary_bgc rows.
    """
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "UNIFIED"}})
    item = editor._gather_item("surface", w)
    assert "use_vars" not in item
    SurfaceForcingItem(**item)


# ===========================================================================
# Surface blend: a regional primary layered over a global fallback.
# ===========================================================================


_BLENDED_ITEM = {
    "type": "physics",
    "source": {"name": "CONUS404"},
    "coarse_grid_mode": "never",
    "blend": {
        "fallback": {
            "source": {"name": "ERA5"},
            "correct_radiation": True,
            "options": {"use_dask": True},
        },
        "width_km": 150,
        "profile": "linear",
    },
}


def test_wizard_handles_every_blend_field():
    """The surface row reads and writes the blend field by field (load-back, widgets,
    carried data, gather). A new blend field fails here until the row handles it.
    """
    assert set(SurfaceBlend.model_fields) == {"fallback", "width_km", "profile"}
    assert set(SurfaceBlendFallback.model_fields) == {
        "source",
        "correct_radiation",
        "options",
    }


def test_blend_round_trips_through_sources_to_inputs_and_gather(editor):
    """A blended item (fallback ``options`` included) survives load-back into the
    row widgets and gathers to the same ``blend``.
    """
    forcing = Forcing.model_validate({"surface": [_BLENDED_ITEM]})
    seed = ForgeBlueprintWizard._sources_to_inputs(SimpleNamespace(forcing=forcing))
    blend_seed = seed["forcing"]["surface"][0]["blend"]
    assert isinstance(blend_seed, dict)  # plain data, not the model object

    w = editor._make_row("surface", seed["forcing"]["surface"][0])
    assert w["blend_fallback"].value == "ERA5"
    assert w["blend_fallback_correct_radiation"].value is True
    assert w["blend_width_km"].value == 150
    assert w["blend_profile"].value == "linear"
    assert _display(w["blend_width_km"]) == ""

    item = editor._gather_item("surface", w)
    assert SurfaceForcingItem(**item).blend == forcing.surface[0].blend
    assert item["blend"]["fallback"]["options"] == {"use_dask": True}


def test_blend_fallback_source_fields_round_trip(editor):
    """Source fields beyond the name ride along, but only for the same source."""
    seed = copy.deepcopy(_BLENDED_ITEM)
    seed["blend"]["fallback"]["source"] = {"name": "ERA5", "path": "/data/era5"}
    w = editor._make_row("surface", seed)
    assert editor._gather_item("surface", w)["blend"]["fallback"]["source"] == {
        "name": "ERA5",
        "path": "/data/era5",
    }


def test_physics_row_gathers_blend_only_with_a_fallback(editor):
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    assert "blend" not in editor._gather_item("surface", w)
    assert _display(w["blend_fallback"]) == ""
    assert _display(w["blend_width_km"]) == "none"

    w["blend_fallback"].value = "ERA5"
    assert _display(w["blend_width_km"]) == ""
    item = editor._gather_item("surface", w)
    # Width, profile and the fallback's radiation flag come from the model defaults.
    default = SurfaceBlend(fallback={"source": "ERA5"})
    assert item["blend"] == {
        "fallback": {"source": {"name": "ERA5"}},
        "width_km": default.width_km,
        "profile": default.profile.value,
    }
    assert SurfaceForcingItem(**{**item, "coarse_grid_mode": "never"}).blend == default

    w["blend_fallback"].value = ""
    assert "blend" not in editor._gather_item("surface", w)
    assert _display(w["blend_width_km"]) == "none"


def test_bgc_row_hides_and_never_gathers_blend(editor):
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "UNIFIED"}})
    assert _display(w["blend_fallback"]) == "none"
    w["blend_fallback"].value = "ERA5"
    assert "blend" not in editor._gather_item("surface", w)
    assert _display(w["blend_width_km"]) == "none"


def test_regional_source_disables_radiation_and_defaults_the_fallback(editor):
    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    w["correct_radiation"].value = True

    w["name"].value = "CONUS404"
    assert w["correct_radiation"].value is False
    assert w["correct_radiation"].disabled is True
    assert w["blend_fallback"].value == "ERA5"
    assert "correct_radiation" not in editor._gather_item("surface", w)

    # The user may clear the fallback (a domain wholly inside the source).
    w["blend_fallback"].value = ""
    assert w["blend_fallback"].value == ""

    w["blend_fallback"].value = "ERA5"
    w["name"].value = "ERA5"
    assert w["correct_radiation"].disabled is False
    assert w["blend_fallback"].value == "ERA5"  # left as the user had it


def test_regional_switch_moves_the_radiation_correction_to_the_fallback(editor):
    w = editor._make_row(
        "surface",
        {"type": "physics", "source": {"name": "ERA5"}, "correct_radiation": True},
    )
    w["name"].value = "CONUS404"
    assert w["blend_fallback"].value == "ERA5"
    assert w["blend_fallback_correct_radiation"].value is True
    assert w["correct_radiation"].value is False
    assert w["correct_radiation"].disabled is True


def test_loaded_invalid_regional_row_is_not_mutated(editor):
    w = editor._make_row(
        "surface",
        {
            "type": "physics",
            "source": {"name": "CONUS404"},
            "correct_radiation": True,
        },
    )
    assert w["correct_radiation"].value is True


def test_choosing_a_fallback_pins_auto_coarse_grid_modes(editor):
    """Every surface row on ``auto`` goes to ``never``, so the blueprint validates."""
    editor._rows["surface"] = []
    editor._add("surface")
    editor._add("surface")
    bgc = editor._rows["surface"][1]
    bgc["type"].value = "bgc"
    editor._rows["surface"][0]["name"].value = "CONUS404"  # fallback -> ERA5
    assert bgc["coarse_grid_mode"].value == "never"
    assert editor._rows["surface"][0]["coarse_grid_mode"].value == "never"
    Forcing.model_validate({"surface": editor.gather()["forcing"]["surface"]})


def test_choosing_a_fallback_pins_auto_rows_to_the_blended_rows_mode(editor):
    """A blended row already on ``always`` takes the ``auto`` rows with it."""
    editor._rows["surface"] = []
    editor._add("surface")
    editor._add("surface")
    primary, bgc = editor._rows["surface"]
    bgc["type"].value = "bgc"
    primary["coarse_grid_mode"].value = "always"
    primary["blend_fallback"].value = "ERA5"
    assert bgc["coarse_grid_mode"].value == "always"
    Forcing.model_validate({"surface": editor.gather()["forcing"]["surface"]})


# ===========================================================================
# F2: constants/esper_method/esper_equation/per-row bgc_interpolation_method
# must never leak from a hidden widget after the row's source name switches
# away from the source they belong to; a 'constants' source takes no `path`.
# ===========================================================================


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_stale_constants_not_emitted_after_switch_to_static_source(editor, cat):
    w = editor._make_row(
        cat, {"source": {"name": "constants", "constants": {"Fe": 0.003}}}
    )
    assert editor._gather_item(cat, w)["source"]["constants"] == {"Fe": 0.003}

    w["name"].value = "UNIFIED"
    editor._apply_row_visibility(w)
    item = editor._gather_item(cat, w)
    assert "constants" not in item["source"]
    BgcSourceItem(**item)


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_stale_esper_fields_not_emitted_after_switch_to_static_source(editor, cat):
    w = editor._make_row(
        cat,
        {
            "source": {
                "name": "ESPER",
                "esper_method": "lir",
                "esper_equation": 16,
                "esper_salinity_conditioning": True,
            }
        },
    )
    item = editor._gather_item(cat, w)
    assert item["source"]["esper_method"] == "lir"
    assert item["source"]["esper_equation"] == 16
    assert item["source"]["esper_salinity_conditioning"] is True

    w["name"].value = "GLODAP"
    editor._apply_row_visibility(w)
    item = editor._gather_item(cat, w)
    assert "esper_method" not in item["source"]
    assert "esper_equation" not in item["source"]
    assert "esper_salinity_conditioning" not in item["source"]
    BgcSourceItem(**item)


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_esper_salinity_conditioning_unchecked_is_not_emitted(editor, cat):
    """Unchecked means "roms-tools default" (off) and must not emit ``False``: the
    resolver treats the field as a switch and ``False`` would still be a value.
    """
    w = editor._make_row(cat, {"source": {"name": "ESPER"}})
    assert w["esper_salinity_conditioning"].value is False
    assert "esper_salinity_conditioning" not in editor._gather_item(cat, w)["source"]


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_esper_rows_have_no_groom_control(editor, cat):
    """Grooming is a section setting (one checkbox per IC/boundary pane), not a
    per-source one: an ESPER row neither offers nor emits a ``groom`` key.
    """
    w = editor._make_row(cat, {"source": {"name": "ESPER"}})
    assert "esper_groom" not in w and "groom" not in w
    item = editor._gather_item(cat, w)
    assert "groom" not in item["source"] and "esper_groom" not in item["source"]
    assert "groom" not in item


def _groom_section(gathered, section):
    return (
        gathered["initial_conditions"]
        if section == "ic"
        else gathered["forcing"]["boundary"]
    )


def _groom_seed(section, block):
    block = {"source": {"name": "GLORYS"}, **block}
    if section == "ic":
        return {"initial_conditions": block}
    return {
        "initial_conditions": {"source": {"name": "GLORYS"}},
        "forcing": {"boundary": block},
    }


@pytest.mark.parametrize("section", ["ic", "boundary"])
def test_groom_checkbox_round_trips_through_editor(section):
    """The "Groom ESPER tracers" box is checked by default and then writes nothing
    (roms-tools' default, on); unchecking writes ``groom: False``, which reloads
    unchecked; an explicit ``groom: True`` loads checked and, being the default,
    is not written back (it hashes like unset).
    """
    import ipywidgets as W

    ed = _ForcingEditor(W, _groom_seed(section, {}), on_change=lambda: None)
    box = getattr(ed, f"{section}_groom")
    assert box.value is True
    assert "groom" not in _groom_section(ed.gather(), section)

    box.value = False
    gathered = ed.gather()
    assert _groom_section(gathered, section)["groom"] is False
    ed2 = _ForcingEditor(W, gathered, on_change=lambda: None)
    assert getattr(ed2, f"{section}_groom").value is False

    ed3 = _ForcingEditor(
        W, _groom_seed(section, {"groom": True}), on_change=lambda: None
    )
    assert getattr(ed3, f"{section}_groom").value is True
    assert "groom" not in _groom_section(ed3.gather(), section)


def test_groom_checkbox_is_in_each_section_pane(editor):
    found = set()

    def _walk(node):
        found.add(id(node))
        for c in getattr(node, "children", []):
            _walk(c)

    _walk(editor.widget)
    assert id(editor.ic_groom) in found
    assert id(editor.boundary_groom) in found


def _build_with_forcing(inputs):
    """A blueprint-shaped stand-in carrying just the resolver-built ``forcing``."""
    from cstar.applications.forge.resolve import _build_forcing

    return SimpleNamespace(forcing=_build_forcing(copy.deepcopy(inputs)))


def test_groom_settings_survive_wizard_load_and_resave():
    """``groom_settings`` has no widget, but a blueprint hand-edited to carry it
    must keep it through the wizard: blueprint -> ``_sources_to_inputs`` ->
    ``_ForcingEditor`` -> ``gather()`` -> resolver.
    """
    import ipywidgets as W

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = cat.forcing_data("glorys-era5-unified")
    fdata["initial_conditions"]["groom_settings"] = {"passes": 2}
    fdata["forcing"]["boundary"]["groom"] = False
    cfg = _build_with_forcing(fdata)

    seed = ForgeBlueprintWizard._sources_to_inputs(cfg)
    ed = _ForcingEditor(W, seed, on_change=lambda: None)
    assert ed.ic_groom.value is True
    assert ed.boundary_groom.value is False
    gathered = ed.gather()
    assert gathered["initial_conditions"]["groom_settings"] == {"passes": 2}
    assert "groom" not in gathered["initial_conditions"]
    assert gathered["forcing"]["boundary"]["groom"] is False
    assert "groom_settings" not in gathered["forcing"]["boundary"]

    cfg2 = _build_with_forcing(gathered)
    for sec in ("initial_conditions", "boundary"):
        a, b = getattr(cfg.forcing, sec), getattr(cfg2.forcing, sec)
        assert (a.groom, a.groom_settings) == (b.groom, b.groom_settings)


@pytest.mark.parametrize("section", ["ic", "boundary"])
def test_unchecking_groom_leaves_carried_groom_settings_out(section):
    """roms-tools applies ``groom_settings`` only while grooming is on, and the section
    refuses the pair: an unchecked box leaves carried settings out of the gather, so
    it still resolves, and re-checking the box brings them back.
    """
    import ipywidgets as W

    from cstar.catalog.domain_catalog import default_catalog as cat

    fdata = cat.forcing_data("glorys-era5-unified")
    block = (
        fdata["initial_conditions"] if section == "ic" else fdata["forcing"]["boundary"]
    )
    block["groom_settings"] = {"passes": 2}
    seed = ForgeBlueprintWizard._sources_to_inputs(_build_with_forcing(fdata))
    ed = _ForcingEditor(W, seed, on_change=lambda: None)
    box = getattr(ed, f"{section}_groom")

    box.value = False
    gathered = ed.gather()
    off = _groom_section(gathered, section)
    assert off["groom"] is False and "groom_settings" not in off
    _build_with_forcing(gathered)

    box.value = True
    assert _groom_section(ed.gather(), section)["groom_settings"] == {"passes": 2}


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_path_hidden_and_not_emitted_for_constants_source(editor, cat):
    w = editor._make_row(cat, {"source": {"name": "UNIFIED", "path": "/x/y.nc"}})
    assert _display(w["path"]) == ""
    assert editor._gather_item(cat, w)["source"]["path"] == "/x/y.nc"

    w["name"].value = "constants"
    editor._apply_row_visibility(w)
    assert _display(w["path"]) == "none"
    w["constants"].value = "Fe=0.003"
    item = editor._gather_item(cat, w)
    assert "path" not in item["source"]
    BgcSourceItem(**item)


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_stale_per_row_interp_not_emitted_for_derived_bgc_sources(editor, cat):
    """constants/ESPER are derived/inline, not a regridded dataset -- a per-row
    ``bgc_interpolation_method`` left over from a previously selected dataset-
    backed source must not leak through once the row switches to one of them.
    """
    w = editor._make_row(
        cat, {"source": {"name": "UNIFIED"}, "bgc_interpolation_method": "density"}
    )
    assert editor._gather_item(cat, w)["bgc_interpolation_method"] == "density"

    w["name"].value = "constants"
    w["constants"].value = "Fe=0.003"
    editor._apply_row_visibility(w)
    item = editor._gather_item(cat, w)
    assert "bgc_interpolation_method" not in item
    BgcSourceItem(**item)


# ===========================================================================
# F4: a pre-v8 ("old-shaped") ForcingSpec dict, read through ``read_spec`` as the
# catalog does, must seed ``_ForcingEditor`` instead of crashing it or silently
# dropping data.
# ===========================================================================


def test_forcing_editor_seeds_from_a_migrated_singular_ic_bgc_source():
    import ipywidgets as W

    from cstar.catalog.spec_schema import read_spec

    old_shaped = {
        "initial_conditions": {
            "source": {"name": "GLORYS"},
            "bgc_source": {"name": "UNIFIED"},
        },
        "forcing": {},
    }
    ed = _ForcingEditor(
        W, read_spec("ForcingSpec", old_shaped, "old.yaml"), on_change=lambda: None
    )
    assert len(ed._rows["ic_bgc"]) == 1
    assert ed._rows["ic_bgc"][0]["name"].value == "UNIFIED"
    gathered = ed.gather()
    assert gathered["initial_conditions"]["bgc_sources"] == [
        {"source": {"name": "UNIFIED"}}
    ]
    # the migration leaves the caller's dict untouched
    assert old_shaped["initial_conditions"]["bgc_source"] == {"name": "UNIFIED"}
    assert "bgc_sources" not in old_shaped["initial_conditions"]


def test_forcing_editor_seeds_from_a_migrated_list_shaped_boundary():
    import ipywidgets as W

    from cstar.catalog.spec_schema import read_spec

    old_shaped = {
        "initial_conditions": {"source": {"name": "GLORYS"}},
        "forcing": {
            "boundary": [
                {"type": "physics", "source": {"name": "GLORYS"}},
                {"type": "bgc", "source": {"name": "UNIFIED"}, "use_vars": ["ALK"]},
            ]
        },
    }
    ed = _ForcingEditor(
        W, read_spec("ForcingSpec", old_shaped, "old.yaml"), on_change=lambda: None
    )
    assert ed.boundary_name.value == "GLORYS"
    assert len(ed._rows["boundary_bgc"]) == 1
    gathered = ed.gather()
    boundary = gathered["forcing"]["boundary"]
    assert boundary["source"] == {"name": "GLORYS"}
    assert boundary["bgc_sources"] == [
        {"source": {"name": "UNIFIED"}, "use_vars": ["ALK"]}
    ]
    # the migration leaves the caller's dict untouched
    assert isinstance(old_shaped["forcing"]["boundary"], list)


# ===========================================================================
# F9: "(none)" boundary sentinel -- a non-child domain must be able to express
# "no boundary forcing at all" (``Forcing.boundary is None``), mirroring the
# existing IC "(none)" sentinel.
# ===========================================================================


def test_boundary_none_option_present_in_dropdown(editor):
    assert _BOUNDARY_NONE in editor.boundary_name.options


def test_boundary_none_sentinel_gathers_null_boundary_and_hides_widgets(editor):
    editor.boundary_name.value = _BOUNDARY_NONE
    gathered = editor.gather()
    assert gathered["forcing"]["boundary"] is None
    assert _display(editor.boundary_path) == "none"
    assert _display(editor.boundary_bgc_interp) == "none"
    assert _display(editor._bgc_sections["boundary_bgc"]) == "none"

    # switching back to a real source restores both the gathered dict and the
    # widgets' visibility
    editor.boundary_name.value = "GLORYS"
    gathered2 = editor.gather()
    assert gathered2["forcing"]["boundary"]["source"]["name"] == "GLORYS"
    assert _display(editor.boundary_path) == ""
    assert _display(editor._bgc_sections["boundary_bgc"]) == ""


def test_boundary_none_sentinel_preserves_boundary_bgc_rows_across_switch(editor):
    """Rows are hidden, not destroyed -- switching "(none)" on and back off must
    not lose whatever boundary_bgc rows were already configured.
    """
    editor._rows["boundary_bgc"] = [
        editor._make_row("boundary_bgc", {"source": {"name": "UNIFIED"}})
    ]
    editor._render("boundary_bgc")

    editor.boundary_name.value = _BOUNDARY_NONE
    assert len(editor._rows["boundary_bgc"]) == 1

    editor.boundary_name.value = "GLORYS"
    gathered = editor.gather()
    assert gathered["forcing"]["boundary"]["bgc_sources"] == [
        {"source": {"name": "UNIFIED"}}
    ]


def test_boundary_none_seeded_from_explicit_null_forcing_input():
    import ipywidgets as W

    ed = _ForcingEditor(
        W,
        {
            "initial_conditions": {"source": {"name": "GLORYS"}},
            "forcing": {"boundary": None},
        },
        on_change=lambda: None,
    )
    assert ed.boundary_name.value == _BOUNDARY_NONE
    assert ed.gather()["forcing"]["boundary"] is None


def test_sources_to_inputs_seeds_boundary_none_sentinel_for_missing_boundary():
    """`_sources_to_inputs` must emit an explicit ``forcing["boundary"] = None``
    (not merely omit the key) so a resolved config with no boundary forcing at
    all reloads with the "(none)" sentinel selected, instead of falling back to
    the fresh-wizard GLORYS default.
    """
    forcing = Forcing.model_validate(
        {"initial_conditions": {"source": {"name": "GLORYS"}}, "boundary": None}
    )
    cfg = SimpleNamespace(forcing=forcing)
    seed = ForgeBlueprintWizard._sources_to_inputs(cfg)
    assert "boundary" in seed["forcing"]
    assert seed["forcing"]["boundary"] is None

    import ipywidgets as W

    ed = _ForcingEditor(W, seed, on_change=lambda: None)
    assert ed.boundary_name.value == _BOUNDARY_NONE
    assert ed.gather()["forcing"]["boundary"] is None


# ===========================================================================
# F7: "Copy IC bgc -> Boundary" / "Copy Boundary bgc -> IC" must also copy the
# section-level default BGC interpolation method, not just the rows -- a
# copied row's blank (inherit-the-default) per-row bgc_interpolation_method
# would otherwise silently change meaning if the two panels' defaults differ.
# ===========================================================================


def test_sync_ic_to_boundary_copies_default_interp_alongside_rows(editor):
    editor.ic_bgc_interp.value = "density"
    editor._rows["ic_bgc"] = [
        editor._make_row(
            "ic_bgc",
            {
                "source": {"name": "UNIFIED", "climatology": True},
                "use_vars": ["ALK"],
                "serialize_dask": True,
            },
        )
    ]
    editor._render("ic_bgc")

    editor._sync_bgc("ic_bgc", "boundary_bgc")
    gathered = editor.gather()
    assert gathered["forcing"]["boundary"]["bgc_interpolation_method"] == "density"
    assert (
        gathered["forcing"]["boundary"]["bgc_sources"]
        == gathered["initial_conditions"]["bgc_sources"]
    )


def test_sync_boundary_to_ic_copies_default_interp_alongside_rows(editor):
    editor.boundary_bgc_interp.value = "density_mld"
    editor._rows["boundary_bgc"] = [
        editor._make_row("boundary_bgc", {"source": {"name": "GLODAP"}})
    ]
    editor._render("boundary_bgc")

    editor._sync_bgc("boundary_bgc", "ic_bgc")
    gathered = editor.gather()
    assert gathered["initial_conditions"]["bgc_interpolation_method"] == "density_mld"
    assert (
        gathered["initial_conditions"]["bgc_sources"]
        == gathered["forcing"]["boundary"]["bgc_sources"]
    )


# ===========================================================================
# F5: `_sources_to_inputs`'s `bgc_section()`/`src()` now build on the generic
# `plain()` helper instead of a hand-listed field set -- must still round-trip
# every field losslessly.
# ===========================================================================


def test_sources_to_inputs_round_trips_ic_bgc_sources_losslessly():
    forcing = Forcing.model_validate(
        {
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bgc_sources": [
                    {
                        "source": {"name": "UNIFIED", "climatology": True},
                        "use_vars": ["ALK", "DIC"],
                        "bgc_interpolation_method": "density",
                    },
                    {
                        "source": {
                            "name": "ESPER",
                            "esper_method": "lir",
                            "esper_equation": 16,
                        },
                        "use_vars": ["NO3"],
                        "serialize_dask": True,
                    },
                    {
                        "source": {
                            "name": "constants",
                            "constants": {"Fe": 0.003},
                        },
                        "use_vars": ["PO4"],
                    },
                ],
                "bgc_interpolation_method": "density_mld",
                "bypass_validation": True,
            },
            "boundary": None,
        }
    )
    cfg = SimpleNamespace(forcing=forcing)
    seed = ForgeBlueprintWizard._sources_to_inputs(cfg)

    import ipywidgets as W

    ed = _ForcingEditor(W, seed, on_change=lambda: None)
    gathered = ed.gather()
    assert gathered["initial_conditions"] == seed["initial_conditions"]


def test_river_bgc_widgets_visible_only_when_include_bgc_checked(editor):
    """The river-BGC source/path widgets only take effect when include_bgc=True
    (roms-tools ignores bgc_source otherwise), so they stay hidden until checked.
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    assert _display(w["bgc_source_name"]) == "none"
    assert _display(w["bgc_source_path"]) == "none"

    w["include_bgc"].value = True
    assert _display(w["bgc_source_name"]) == ""
    assert _display(w["bgc_source_path"]) == ""

    w["include_bgc"].value = False
    assert _display(w["bgc_source_name"]) == "none"
    assert _display(w["bgc_source_path"]) == "none"


def test_river_bgc_source_seeded_from_existing_item(editor):
    """Loading an item with a pre-set bgc_source (e.g. RIVR2O) seeds the dropdown/path
    and shows the widgets immediately (include_bgc already True).
    """
    w = editor._make_row(
        "river",
        {
            "source": {"name": "DAI"},
            "include_bgc": True,
            "bgc_source": {"name": "RIVR2O", "path": "/data/rivr2o/*.nc"},
        },
    )
    assert w["bgc_source_name"].value == "RIVR2O"
    assert w["bgc_source_path"].value == "/data/rivr2o/*.nc"
    assert _display(w["bgc_source_name"]) == ""
    assert _display(w["bgc_source_path"]) == ""


def test_gather_item_river_includes_bgc_source_only_when_include_bgc_checked(editor):
    """_gather_item must not emit bgc_source when include_bgc is unchecked (matches
    the RiverForcingItem validator, which rejects bgc_source without include_bgc).
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    w["bgc_source_name"].value = "RIVR2O"
    w["bgc_source_path"].value = "/data/rivr2o/*.nc"

    item = editor._gather_item("river", w)
    assert "bgc_source" not in item

    w["include_bgc"].value = True
    item = editor._gather_item("river", w)
    assert item["bgc_source"] == {"name": "RIVR2O", "path": "/data/rivr2o/*.nc"}


def test_gather_item_river_bgc_source_omits_path_when_blank(editor):
    """A blank bgc path means 'derive the default staged location' — omit the key
    rather than emitting an empty string.
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    w["include_bgc"].value = True
    w["bgc_source_name"].value = "CONSTANTS"

    item = editor._gather_item("river", w)
    assert item["bgc_source"] == {"name": "CONSTANTS"}


def test_river_temp_source_widgets_visible_only_when_source_selected(editor):
    """The temperature-source path/smoothing-window widgets only matter once a
    source (ERA5) is picked, so they stay hidden until then (mirrors the bgc pair).
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    assert _display(w["surface_forcing_source_path"]) == "none"
    assert _display(w["river_temp_smoothing_window_days"]) == "none"

    w["surface_forcing_source_name"].value = "ERA5"
    assert _display(w["surface_forcing_source_path"]) == ""
    assert _display(w["river_temp_smoothing_window_days"]) == ""

    w["surface_forcing_source_name"].value = ""
    assert _display(w["surface_forcing_source_path"]) == "none"
    assert _display(w["river_temp_smoothing_window_days"]) == "none"


def test_river_temp_source_seeded_from_existing_item(editor):
    """Loading an item with a pre-set surface_forcing_source (ERA5) seeds the
    dropdown/path/window and shows the widgets immediately.
    """
    w = editor._make_row(
        "river",
        {
            "source": {"name": "DAI"},
            "surface_forcing_source": {"name": "ERA5", "path": "/x/era5"},
            "river_temp_smoothing_window_days": 7.0,
        },
    )
    assert w["surface_forcing_source_name"].value == "ERA5"
    assert w["surface_forcing_source_path"].value == "/x/era5"
    assert w["river_temp_smoothing_window_days"].value == 7.0
    assert _display(w["surface_forcing_source_path"]) == ""
    assert _display(w["river_temp_smoothing_window_days"]) == ""


def test_gather_item_river_omits_temp_source_when_blank(editor):
    """_gather_item must not emit surface_forcing_source when no temperature
    source is selected. The smoothing window follows the domain_edge_buffer rule
    instead -- emitted whenever non-default, source or not -- so a loaded
    blueprint carrying the value round-trips with an unchanged content_hash.
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    item = editor._gather_item("river", w)
    assert "surface_forcing_source" not in item
    assert "river_temp_smoothing_window_days" not in item

    w["river_temp_smoothing_window_days"].value = 45.0
    item = editor._gather_item("river", w)
    assert "surface_forcing_source" not in item
    assert item["river_temp_smoothing_window_days"] == 45.0


def test_gather_item_river_drops_stale_invalid_window_when_hidden(editor):
    """Pick ERA5, type an invalid window, then blank the source again: the
    hidden widget keeps the stale 0. Emitting it would make the schema's `> 0`
    check reject the blueprint over a field the user can no longer see, so the
    stale invalid value is dropped instead. With ERA5 still selected the value
    is emitted as typed and validation reports it against a visible widget.
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    w["surface_forcing_source_name"].value = "ERA5"
    w["river_temp_smoothing_window_days"].value = 0.0
    assert editor._gather_item("river", w)["river_temp_smoothing_window_days"] == 0.0

    w["surface_forcing_source_name"].value = ""
    item = editor._gather_item("river", w)
    assert "surface_forcing_source" not in item
    assert "river_temp_smoothing_window_days" not in item


def test_river_temp_source_seed_normalizes_name_case(editor):
    """RiverForcingItem accepts any case ("era5"); the dropdown options are
    upper-case, so seeding must normalize or the source is silently dropped.
    """
    w = editor._make_row(
        "river",
        {"source": {"name": "DAI"}, "surface_forcing_source": {"name": "era5"}},
    )
    assert w["surface_forcing_source_name"].value == "ERA5"
    assert editor._gather_item("river", w)["surface_forcing_source"] == {"name": "ERA5"}


def test_gather_item_river_temp_source_omits_path_when_blank(editor):
    """A blank ERA5 path means 'read the remote ARCO archive' — omit the key
    rather than emitting an empty string. The window also stays omitted at its
    30.0 default.
    """
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    w["surface_forcing_source_name"].value = "ERA5"

    item = editor._gather_item("river", w)
    assert item["surface_forcing_source"] == {"name": "ERA5"}
    assert "river_temp_smoothing_window_days" not in item


def test_gather_item_river_temp_source_includes_path_and_changed_window(editor):
    w = editor._make_row("river", {"source": {"name": "DAI"}})
    w["surface_forcing_source_name"].value = "ERA5"
    w["surface_forcing_source_path"].value = "/x/era5/*.nc"
    w["river_temp_smoothing_window_days"].value = 14.0

    item = editor._gather_item("river", w)
    assert item["surface_forcing_source"] == {"name": "ERA5", "path": "/x/era5/*.nc"}
    assert item["river_temp_smoothing_window_days"] == 14.0


# ---------------------------------------------------------------------------
# WP3 (inner editors): IC pane field_row/field_grid layout, forcing-row labels.
# ---------------------------------------------------------------------------


def test_ic_pane_first_field_row_is_ic_name(editor):
    """The IC pane opens with the Physics subsection, whose ``field_grid`` holds
    name, layout, path in that order (see ``_ForcingEditor._section_pane``).
    """
    acc = editor.widget
    # `open_accordion` returns a VBox of collapsible panes (`.panes`).
    ic_box = acc.panes[0].body  # _FORCING_PANES[0] == initial_conditions + ic_bgc
    physics_grid = ic_box.children[0].children[1]
    assert [r.forge_key for r in physics_grid.children] == [
        "ic.ic_name",
        "ic.ic_layout",
        "ic.ic_path",
    ]


def test_ic_layout_display_none_hides_its_row(editor):
    """``field_row`` mirrors ``widget.layout.display`` onto the row -- setting
    ``ic_layout.layout.display = "none"`` (as ``_sync_ic_layout_visibility``
    does for a non-GLORYS source) hides the whole row, not just the dropdown.
    """
    acc = editor.widget
    ic_box = acc.panes[0].body
    physics_grid = ic_box.children[0].children[1]
    layout_row = next(r for r in physics_grid.children if r.forge_key == "ic.ic_layout")
    assert layout_row.layout.display == _display(editor.ic_layout)

    editor.ic_layout.layout.display = "none"
    assert layout_row.layout.display == "none"


def test_surface_row_name_description_starts_with_glossary_label(editor):
    """A per-row widget's description is sourced from the ``forcing.row.<key>``
    glossary entry (``label_for``, see ``_row_desc``), not the old hardcoded
    ``"src:"``/``"path:"``/etc. caption.
    """
    from cstar.wizard.ui.labels import label_for

    w = editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})
    label = label_for("forcing.row.name", default="src").label
    assert w["name"].description.startswith(label)
