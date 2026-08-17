"""Tests for the ipywidgets ForgeBlueprintWizard UI (cstar_forge.forge_blueprint_wizard).

These target the wizard-feedback fixes: conditional field visibility, forcing-row
option ordering, the ntides sync into model_settings, and the nesting-section
plot_nesting wiring. Widget construction is lightweight (no grid/network I/O — the
live preview resolves via ``dt``, never building a roms_tools.Grid), so these run
as fast unit tests.
"""

from datetime import date
from pathlib import Path

import pytest

from cstar_forge.forge_blueprint_wizard import (
    ForgeBlueprintWizard,
    _drain_stream_buffer,
    _ForcingEditor,
)

try:
    from cstar.orchestration.models import DeferredBlueprintRef  # noqa: F401

    _CSTAR_HAS_DEFERRED_BLUEPRINT = True
except ImportError:
    _CSTAR_HAS_DEFERRED_BLUEPRINT = False

requires_workplan_support = pytest.mark.skipif(
    not _CSTAR_HAS_DEFERRED_BLUEPRINT,
    reason=(
        "installed C-Star lacks workplan deferred-blueprint support "
        "(DeferredBlueprintRef not in cstar.orchestration.models)"
    ),
)


@pytest.fixture
def editor():
    import ipywidgets as W

    return _ForcingEditor(W, {}, on_change=lambda: None)


def _display(widget) -> str:
    return getattr(widget.layout, "display", "") or ""


def _find_section(w, title_fragment):
    """Recursively find the VBox whose direct HTML title child contains the given
    fragment (mirrors the wizard's ``section()`` layout helper).
    """
    for c in getattr(w, "children", []):
        html = getattr(c, "value", None)
        if isinstance(html, str) and title_fragment in html:
            return w
        found = _find_section(c, title_fragment)
        if found is not None:
            return found
    return None


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
    """Item 7: glorys_layout only shows when the source name is GLORYS. Switching
    the boundary `type` to bgc resets `name` away from GLORYS (bgc boundary sources
    are UNIFIED/CESM_REGRIDDED) and must hide the layout box via the same cascade
    that resets the name dropdown's options (_on_type_change).
    """
    w = editor._make_row("boundary", {"type": "physics", "source": {"name": "GLORYS"}})
    assert w["name"].value == "GLORYS"
    assert _display(w["glorys_layout"]) == ""

    w["type"].value = "bgc"
    assert w["name"].value != "GLORYS"
    assert _display(w["glorys_layout"]) == "none"


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


def test_row_box_puts_type_first(editor):
    """Item 3: `type` (when present) is the left-most widget in the row."""
    w = editor._make_row("surface", {"type": "bgc", "source": {"name": "ERA5"}})
    box = editor._row_box(w)
    assert box.children[0] is w["type"]


def test_row_box_without_type_unaffected(editor):
    """tidal/river rows have no `type`; ordering must not error or reorder oddly."""
    w = editor._make_row("tidal", {"ntides": 15, "source": {"name": "TPXO"}})
    box = editor._row_box(w)
    assert w["ntides"] in box.children


@pytest.mark.parametrize("cat", ["surface", "boundary", "tidal"])
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


def test_topo_source_dropdown_includes_emod():
    """The topography-source dropdown must offer EMOD alongside ETOPO5/SRTM15."""
    wiz = ForgeBlueprintWizard()
    assert "EMOD" in wiz.topo_source.options


def test_wizard_smoke_assembles_widget():
    """Reordered sections (item 4b) and relocated dropdowns (item 5) assemble cleanly."""
    wiz = ForgeBlueprintWizard()
    root = wiz.widget  # must not raise
    # crude structural check: section titles appear in the expected relative order
    titles = []

    def walk(w):
        html = getattr(w, "value", None)
        if isinstance(html, str) and html.startswith("<b>"):
            titles.append(html)
        for c in getattr(w, "children", []):
            walk(c)

    walk(root)
    order = [t for t in titles]
    grid_i = next(i for i, t in enumerate(order) if "Grid" in t and "Child" not in t)
    obc_i = next(i for i, t in enumerate(order) if "Open boundaries" in t)
    nest_i = next(i for i, t in enumerate(order) if "Child grid" in t)
    assert grid_i < obc_i < nest_i


def test_pieces_section_has_forcing_and_output_dropdowns():
    """Item 5: Forcing/Output selectors live in the first 'Pieces' box."""
    wiz = ForgeBlueprintWizard()
    pieces_box = _find_section(wiz.widget, "<b>Pieces</b>")
    assert pieces_box is not None
    assert wiz.forcing_dd in pieces_box.children
    assert wiz.output_dd in pieces_box.children


def test_roms_ref_prefilled_and_placed_next_to_model_dropdown():
    """ucla-roms ref is prefilled from the selected model's pinned default (stays
    editable) and lives right next to the Model dropdown in the Pieces section.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.roms_ref.value == wiz._model_default_roms_ref()
    assert wiz.roms_ref.value  # this model.yaml pins a concrete commit

    pieces_box = _find_section(wiz.widget, "<b>Pieces</b>")
    assert pieces_box is not None
    model_row = next(
        c for c in pieces_box.children if wiz.model_dd in getattr(c, "children", [])
    )
    assert wiz.roms_ref in model_row.children  # same row as the Model dropdown


def test_roms_ref_repopulates_on_model_change(monkeypatch):
    """Switching models refreshes ucla-roms ref to the new model's pinned default."""
    wiz = ForgeBlueprintWizard()
    monkeypatch.setattr(wiz, "_model_default_roms_ref", lambda: "some-other-ref")
    wiz._on_model_change(None)
    assert wiz.roms_ref.value == "some-other-ref"


def test_co2_tvarying_is_not_user_editable():
    """co2_tvarying is controlled solely by the presence of an MBL_co2 bgc surface
    source; the wizard must not expose a checkbox letting the user override it --
    that derivation happens behind the scenes in the resolver/input_data.
    """
    wiz = ForgeBlueprintWizard()
    assert not hasattr(wiz, "co2_tvarying_chk")

    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    # The default model's forcing includes an MBL_co2 bgc surface source, so the
    # resolver still auto-derives co2_tvarying=True with no UI toggle involved.
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True


def test_resolver_derived_cppdefs_fields_have_no_accordion_widget():
    """obc_*/marbl/use_pio/cdr_forcing/co2_tvarying/sal_restore/tides are all fully
    resolver-derived (like co2_tvarying above) -- the advanced settings accordion
    must never expose a competing editor for any of them.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    for field in (
        "obc_west",
        "obc_east",
        "obc_north",
        "obc_south",
        "marbl",
        "use_pio",
        "cdr_forcing",
        "co2_tvarying",
        "sal_restore",
        "tides",
    ):
        assert ("cppdefs", field) not in wiz.editor._widgets


def test_sponge_tune_editable_via_advanced_settings_accordion():
    """Unlike co2_tvarying, SPONGE_TUNE has no resolver-side derivation -- it's a
    plain ModelSpec default (False) reachable only through the advanced settings
    accordion's generic (section, field) override mechanism.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["sponge_tune"] is False
    assert ("cppdefs", "sponge_tune") in wiz.editor._widgets

    wiz._overrides[("cppdefs", "sponge_tune")] = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["sponge_tune"] is True


def test_nhy_nox_forcing_editable_in_bgc_advanced_settings_pane():
    """NHY_FORCING/NOX_FORCING default True from the ModelSpec and are editable as
    checkboxes in the Biogeochemistry (BGC / MARBL) advanced settings pane.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is True
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is True
    assert ("cppdefs", "nhy_forcing") in wiz.editor._widgets
    assert ("cppdefs", "nox_forcing") in wiz.editor._widgets

    wiz._overrides[("cppdefs", "nhy_forcing")] = False
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is False
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is True  # untouched


def test_bgc_dd_none_forces_nhy_nox_forcing_off_in_the_wizard():
    """Flipping BGC mode to 'none' in the wizard must force NHY_FORCING/NOX_FORCING
    off through the real compose -> override -> sync pipeline, not just at the
    resolver. Strip every BGC signal from the bundled ForcingSpec's widgets first
    (mirrors _PHYSICS_ONLY_FORCING in test_forge_blueprint.py) so bgc_mode="none"
    doesn't raise.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    fe = wiz._forcing_editor
    for cat in ("surface", "boundary"):
        for ws in list(fe._rows[cat]):
            if ws.get("type") is not None and ws["type"].value == "bgc":
                fe._remove(cat, ws)
    for ws in list(fe._rows["river"]):
        if "include_bgc" in ws:
            ws["include_bgc"].value = False
    fe.ic_bgc_name.value = ""

    wiz.bgc_dd.value = "none"
    wiz._rebuild()
    assert wiz.config is not None, wiz.derived.value
    assert wiz.config.model_settings["cppdefs"]["nhy_forcing"] is False
    assert wiz.config.model_settings["cppdefs"]["nox_forcing"] is False


def test_bgc_dd_default_and_placement():
    """BGC mode defaults to 'marbl' and lives in the same Pieces row as Model."""
    wiz = ForgeBlueprintWizard()
    assert wiz.bgc_dd.value == "marbl"
    assert set(wiz.bgc_dd.options) == {"marbl", "none"}

    pieces_box = _find_section(wiz.widget, "<b>Pieces</b>")
    assert pieces_box is not None
    model_row = next(
        c for c in pieces_box.children if wiz.model_dd in getattr(c, "children", [])
    )
    assert wiz.bgc_dd in model_row.children


def test_bgc_dd_marbl_gathers_into_cppdefs():
    """The default 'marbl' choice flows through _gather()/build_forge_blueprint into
    cppdefs.marbl -- the happy path (the bundled ForcingSpec carries BGC forcing, so
    switching to 'none' without changing forcing is expected to raise; that's
    covered at the resolver level, see test_resolver_bgc_mode_none_raises_with_bgc_forcing).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["marbl"] is True
    assert wiz.config.code.marbl is not None


def test_bgc_dd_none_with_default_bgc_forcing_surfaces_error_legibly():
    """Flipping to 'none' while the bundled (BGC-carrying) ForcingSpec is still
    selected must not crash the wizard -- _rebuild()'s existing exception handling
    should catch the resolver's ValueError and surface it in the status area.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.bgc_dd.value = "none"
    wiz._rebuild()
    assert wiz.config is None
    assert "Invalid" in wiz.derived.value


def test_use_pio_chk_default_seeded_from_model_spec():
    """use_pio_chk mirrors bgc_dd: it is seeded from the selected ModelSpec's
    top-level use_pio (True for pio-dev, the wizard's default model; False for
    cson_roms-marbl_v0.1), and reseeded on a model switch.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.model_dd.value == "pio-dev"
    assert wiz.use_pio_chk.value is True
    assert wiz._model_default_use_pio() is True

    wiz.model_dd.value = "cson_roms-marbl_v0.1"
    assert wiz.use_pio_chk.value is False
    assert wiz._model_default_use_pio() is False


def test_use_pio_chk_emit_is_unconditional():
    """The wizard must send an explicit use_pio=False to the resolver when the
    checkbox is unchecked (not simply omit the kwarg) -- otherwise a ModelSpec
    that declares use_pio: true could never be turned off in the UI, since the
    resolver's None fallback re-reads the ModelSpec default.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    # pio-dev (the default model) declares use_pio: true -- exactly the
    # ModelSpec this test guards against: unchecking must emit an explicit
    # False, not fall back to the ModelSpec default.
    assert wiz.use_pio_chk.value is True
    wiz.use_pio_chk.value = False
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is False
    assert wiz.config.code.pio is None

    wiz.use_pio_chk.value = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is True
    assert wiz.config.code.pio is not None


_CDR_SAMPLE_YAML = Path(__file__).parent / "fixtures" / "cdr_forcing_sample.yaml"


def _upload_change(content: bytes):
    """Build an ipywidgets FileUpload-style ``change`` dict for a single file."""
    return {"new": ({"name": "cdr.yaml", "content": content},)}


def test_cdr_upload_valid_yaml_gathers_into_config():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))

    assert wiz._cdr_forcing is not None
    assert "✓ CDR" in wiz.cdr_status.value
    assert "cdr_forcing" in wiz._gather()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["cdr_forcing"] is True
    assert wiz.config.forcing.cdr_forcing["releases"]


def test_cdr_upload_invalid_yaml_surfaces_error_and_does_not_set_config():
    """Not a roms-tools CDRForcing document at all -- caught by
    ``read_cdr_forcing_yaml``'s own parse check, before roms-tools is ever imported.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    wiz._on_cdr_upload(_upload_change(b"---\nSomeOtherThing:\n  foo: bar\n"))

    assert wiz._cdr_forcing is None
    assert "invalid" in wiz.cdr_status.value.lower()
    # no CDR was gathered -- the rest of the config still resolves fine
    assert "cdr_forcing" not in wiz._gather()
    assert wiz.config is not None


def test_cdr_upload_semantically_broken_yaml_caught_by_eager_rt_construction():
    """A structurally-valid CDRForcing document (parses fine, has the right keys)
    but with start_time >= end_time. ``read_cdr_forcing_yaml`` has no opinion on
    this -- only the eager ``rt.CDRForcing(**parsed)`` construction in
    ``_on_cdr_upload`` catches it (roms-tools' own validator raises). This is the
    scenario the eager-validation design exists for: without it, this would embed
    silently into the blueprint and only fail much later, during blueprint processing.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)

    text = _CDR_SAMPLE_YAML.read_text()
    # swap start_time/end_time so parsing succeeds but roms-tools' own
    # `start_time < end_time` validator raises.
    swapped = text.replace(
        "start_time: '2012-01-01T00:00:00'\n  end_time: '2012-01-02T00:00:00'",
        "start_time: '2012-01-02T00:00:00'\n  end_time: '2012-01-01T00:00:00'",
    )
    assert swapped != text, "fixture format changed -- update this test's replace()"

    wiz._on_cdr_upload(_upload_change(swapped.encode("utf-8")))

    assert wiz._cdr_forcing is None
    assert "invalid" in wiz.cdr_status.value.lower()
    assert "cdr_forcing" not in wiz._gather()


def test_cdr_clear_resets_state():
    wiz = ForgeBlueprintWizard()
    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz._cdr_forcing is not None

    wiz._on_cdr_clear(None)

    assert wiz._cdr_forcing is None
    assert wiz.cdr_status.value == ""
    assert "cdr_forcing" not in wiz._gather()


def test_cdr_forcing_round_trips_through_load(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
    assert wiz.config is not None
    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2._cdr_forcing is not None

    # cdr_forcing is stored as a plain dict (no typed CDR model, by design -- see the
    # plan's "no typed CDR Pydantic model" note), so a bare YAML timestamp parses to
    # an in-memory datetime but re-serializes to an ISO string on the blueprint's own
    # to_yaml/from_yaml round trip. Compare with that normalized away.
    def _iso(t):
        return t.isoformat() if hasattr(t, "isoformat") else str(t)

    orig = [dict(r) for r in wiz._cdr_forcing["releases"]]
    back = [dict(r) for r in wiz2._cdr_forcing["releases"]]
    for r in (orig, back):
        for release in r:
            release["times"] = [_iso(t) for t in release["times"]]
    assert back == orig
    assert "✓ CDR loaded" in wiz2.cdr_status.value


def test_ntides_syncs_from_tidal_forcing_into_model_settings():
    """Item 6: the tidal forcing item's ntides drives model_settings['tides']['ntides'],
    not just the run-time-defaults placeholder (10).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    tidal_rows = wiz._forcing_editor._rows["tidal"]
    assert tidal_rows, "expected a default tidal row from the model's forcing inputs"
    # Default model forcing sets ntides=15 -> must already have synced, not left at 10.
    assert wiz.config.model_settings["tides"]["ntides"] == 15

    tidal_rows[0]["ntides"].value = 8
    wiz._rebuild()
    assert wiz.config.model_settings["tides"]["ntides"] == 8

    # A manual Advanced-settings override still wins over the tidal-forcing value.
    wiz._overrides[("tides", "ntides")] = 42
    wiz._rebuild()
    assert wiz.config.model_settings["tides"]["ntides"] == 42


def test_domain_modified_reflects_deviation_from_catalog_pick():
    """composition.domain.modified follows "deviate" semantics: editing a
    domain-defining widget after a catalog Domain pick sets it True; reverting the
    edit exactly clears it back to False (audit follow-up: domain never used to
    track modification at all).
    """
    wiz = ForgeBlueprintWizard()
    wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[
        1
    ]  # first real catalog domain
    assert wiz.config.composition.domain.origin == "catalog"
    assert wiz.config.composition.domain.modified is False

    orig_npx = wiz.npx.value
    wiz.npx.value = orig_npx + 1
    assert wiz.config.composition.domain.modified is True
    assert wiz.config.composition.domain.origin == "catalog"  # never flips to custom

    wiz.npx.value = orig_npx  # revert exactly -> deviation clears
    assert wiz.config.composition.domain.modified is False


def test_domain_modified_false_when_hand_authored():
    """A from-scratch (non-catalog) domain has nothing to deviate from -> never
    modified, regardless of what its widgets hold.
    """
    wiz = ForgeBlueprintWizard()
    assert wiz.domain_dd.value == "<custom>"
    wiz.npx.value = wiz.npx.value + 1
    assert wiz.config.composition.domain.origin == "custom"
    assert wiz.config.composition.domain.modified is False


def test_forcing_modified_reflects_deviation_from_catalog_pick():
    """composition.forcing.modified follows the same "deviate" semantics, and
    (post-unification) origin no longer flips to "custom" on edit.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False
    assert wiz.config.composition.forcing.origin == "catalog"

    tidal_rows = wiz._forcing_editor._rows["tidal"]
    orig_ntides = tidal_rows[0]["ntides"].value
    tidal_rows[0]["ntides"].value = orig_ntides + 1
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is True
    assert wiz.config.composition.forcing.origin == "catalog"  # never flips to custom

    tidal_rows[0]["ntides"].value = orig_ntides  # revert exactly -> deviation clears
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False


def test_model_and_output_modified_from_accordion_overrides():
    """Model/output share the accordion overrides layer; modified is derived per-
    piece by whether a deviating override key belongs to OUTPUT_SECTIONS/
    PARTIAL_OUTPUT_SECTIONS (audit follow-up: these two pieces never set `modified`
    at all before this fix).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is False

    # A non-output section override -> model.modified only.
    wiz._overrides[("lateral_visc", "visc2")] = 99.0
    wiz._rebuild()
    assert wiz.config.composition.model.modified is True
    assert wiz.config.composition.output.modified is False

    # An OUTPUT_SECTIONS override -> output.modified only.
    del wiz._overrides[("lateral_visc", "visc2")]
    wiz._overrides[("ocean_vars", "wrt_z")] = False
    wiz._rebuild()
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is True


def test_composition_modified_survives_save_and_load_round_trip(tmp_path):
    """A saved deviation on model/output/domain/forcing must reload with the same
    `modified` flags (composition is meant to reliably answer "did the user touch
    this catalog piece" even after a save/load cycle).
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[1]
    wiz.npx.value = wiz.npx.value + 1  # deviate domain
    wiz._overrides[("lateral_visc", "visc2")] = 99.0  # deviate model
    wiz._rebuild()
    assert wiz.config.composition.domain.modified is True
    assert wiz.config.composition.model.modified is True

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    # Domain always loads as origin="custom" (the file, not a catalog entry, is
    # authoritative) so domain.modified is moot on load; model.modified must survive
    # via the reconstructed overrides layer.
    assert wiz2.config.composition.domain.origin == "custom"
    assert wiz2.config.composition.model.modified is True


def test_composition_modified_all_false_on_pristine_save_and_load_round_trip(
    tmp_path,
):
    """A file saved with no edits must reload with every piece unmodified -- the
    forcing comparison in particular round-trips through `_sources_to_inputs` /
    `build_forge_blueprint` before being re-gathered, so a lossy resolve/reconstruct
    cycle (e.g. an omitted-vs-null field) could otherwise report a false positive
    for a file the user never touched.
    """
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config.composition.forcing.modified is False
    assert wiz.config.composition.model.modified is False
    assert wiz.config.composition.output.modified is False

    saved = tmp_path / "forge_blueprint.yaml"
    wiz.config.to_yaml(saved)

    wiz2 = ForgeBlueprintWizard()
    wiz2.load_path.value = str(saved)
    wiz2._on_load_path(None)

    assert wiz2.config.composition.forcing.modified is False
    assert wiz2.config.composition.model.modified is False
    assert wiz2.config.composition.output.modified is False


def test_parent_plot_is_always_grid_plot_only(monkeypatch):
    """The Grid section's plot is parent-only regardless of nesting state -- the
    parent+child overlay lives in its own Nesting-section plot (see _on_nest_plot).
    """
    import roms_tools

    calls = []

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def plot(self):
            calls.append("plot")

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)

    wiz = ForgeBlueprintWizard()
    for nest_enabled in (False, True):
        calls.clear()
        wiz.nest_enable.value = nest_enabled
        wiz._on_plot(None)
        assert calls == ["plot"], (nest_enabled, wiz.plot_status.value)


def test_nest_plot_button_renders_parent_and_child_via_plot_nesting(monkeypatch):
    """Item 1 (revised): a dedicated 'Refresh plot' button in the Nesting section
    builds both grids and renders them via plot_nesting, independent of the parent
    Grid section's plot.
    """
    import roms_tools

    calls = []

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def plot(self):
            calls.append(("plot", self.kwargs))

    def _fake_plot_nesting(parent, child, **kw):
        calls.append(("plot_nesting", parent.kwargs, child.kwargs))

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)
    monkeypatch.setattr(roms_tools, "plot_nesting", _fake_plot_nesting, raising=False)

    wiz = ForgeBlueprintWizard()
    wiz._on_nest_plot(None)

    assert any(c[0] == "plot_nesting" for c in calls), wiz.nest_plot_status.value
    assert not any(c[0] == "plot" for c in calls)  # doesn't touch the parent plot
    assert len(wiz.nest_plot_img.value) > 0
    # The parent plot/status are untouched by the nesting-section refresh.
    assert wiz.plot_status.value == ""
    assert wiz.plot_img.value == b""


def test_nest_plot_figure_survives_inline_backend_show(monkeypatch):
    """Regression: plot_nesting calls plt.show() internally (no way to suppress it,
    no return value). Under Jupyter's inline backend, show() renders-and-closes the
    current figure immediately, so a plain plt.gcf() call right after plot_nesting
    returns would grab a fresh *blank* figure instead of the one just drawn -- the
    bug report ("existing plot disappears"). _on_nest_plot must neutralize
    plt.show for the duration of the plot_nesting call so the real figure survives
    to be saved.
    """
    import matplotlib.pyplot as plt
    import roms_tools

    class _FakeGrid:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    def _fake_plot_nesting(parent, child, **kw):
        fig, ax = plt.subplots()
        ax.plot([0, 1], [0, 1])  # something identifiable on the "real" figure
        plt.show()  # exercises the real plt.show, patched below

    def _inline_backend_show(*a, **kw):
        # Mimic Jupyter's inline backend: render then close the current figure.
        plt.close(plt.gcf())

    captured = {}
    real_savefig = plt.Figure.savefig

    def _spy_savefig(self, *a, **kw):
        captured["axes"] = list(self.axes)
        return real_savefig(self, *a, **kw)

    monkeypatch.setattr(roms_tools, "Grid", _FakeGrid)
    monkeypatch.setattr(roms_tools, "plot_nesting", _fake_plot_nesting, raising=False)
    monkeypatch.setattr(plt, "show", _inline_backend_show)
    monkeypatch.setattr(plt.Figure, "savefig", _spy_savefig)

    wiz = ForgeBlueprintWizard()
    wiz._on_nest_plot(None)

    assert not wiz.nest_plot_status.value.startswith("<span style='color:#b00'>"), (
        wiz.nest_plot_status.value
    )
    # A blank new figure (created after a premature close) would have zero axes;
    # the real drawn figure has one axis with the plotted line.
    assert captured.get("axes"), "expected the drawn figure's axes, got a blank figure"
    assert captured["axes"][0].lines, "the plotted line did not survive plt.show()"


def test_build_run_command_uses_cstar_blueprint_run_from_this_env():
    """The Run button invokes `cstar blueprint run <path>` -- the one command the docs
    give for both pipeline steps -- via the `cstar` script installed next to the
    interpreter already running the wizard's kernel, not a bare `cstar` from PATH or a
    `conda run` invocation (avoids conda/micromamba env-discovery issues). Not
    `cstar forge run`: the button exposes no per-run flags, so that passthrough's only
    advantage does not apply here.
    """
    import sys

    wiz = ForgeBlueprintWizard()
    cmd = wiz._build_run_command("/tmp/some_blueprint.yaml")
    cstar_exe = Path(sys.executable).with_name("cstar")
    if cstar_exe.exists():
        assert cmd == [str(cstar_exe), "blueprint", "run", "/tmp/some_blueprint.yaml"]
    else:
        assert cmd == [
            sys.executable,
            "-m",
            "cstar_forge.run",
            "/tmp/some_blueprint.yaml",
        ]


def test_workplan_path_strips_forge_blueprint_suffix():
    f = ForgeBlueprintWizard._workplan_path
    assert f(Path("/x/foo.forge_blueprint.yaml")) == Path("/x/foo.workplan.yaml")
    assert f(Path("/x/foo.yaml")) == Path("/x/foo.workplan.yaml")


@requires_workplan_support
def test_build_workplan_two_steps_with_deferred_blueprint(tmp_path):
    """The workplan pairs a `forge` step (the saved blueprint) with a `roms_marbl`
    step consuming the B_{name}.yaml that step 1 generates -- a deferred blueprint
    reference, since the file does not exist until the forge step has run.
    """
    from cstar.orchestration.models import DeferredBlueprintRef

    wiz = ForgeBlueprintWizard()
    assert wiz.config is not None
    bp_path = tmp_path / f"{wiz.config.name}.forge_blueprint.yaml"
    bp_path.write_text("placeholder")  # Step's FilePath branch requires existence

    wp = wiz._build_workplan(bp_path)

    assert wp.name == wiz.config.name
    forge_step, roms_step = wp.steps
    assert (forge_step.name, forge_step.application) == ("forge", "forge")
    assert Path(str(forge_step.blueprint_path)) == bp_path.resolve()
    assert (roms_step.name, roms_step.application) == ("roms_marbl", "roms_marbl")
    assert list(roms_step.depends_on) == ["forge"]
    ref = roms_step.blueprint_path
    assert isinstance(ref, DeferredBlueprintRef)
    assert ref.from_step == "forge"
    assert ref.filename == f"B_{wiz.config.name}.yaml"
    # a deferred blueprint can't be inspected at submit time (SLURM would default
    # to 1 CPU) -- the step must carry the partitioning size explicitly
    assert roms_step.compute_overrides["cpus"] == wiz.config.n_procs
    # the forge step carries no cpus override: the scheduler falls back to
    # ForgeBlueprint.cpus_needed, the grid-sized forge estimate
    assert "cpus" not in forge_step.compute_overrides
    assert wiz.config.cpus_needed >= 16


def test_on_save_workplan_guards_on_invalid_config(tmp_path):
    wiz = ForgeBlueprintWizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz.config = None
    wiz.save_path.value = str(tmp_path / "never.forge_blueprint.yaml")
    wiz._on_save_workplan(None)
    assert "invalid" in wiz.workplan_status.value
    assert not list(tmp_path.iterdir())


@requires_workplan_support
def test_on_save_workplan_writes_to_catalog_workplans_dir(tmp_path):
    """With a local catalog, the workplan lands in catalog/workplans/ (not next
    to the blueprint in catalog/blueprints/).
    """
    import shutil

    from cstar.orchestration.models import Workplan
    from cstar.orchestration.serialization import deserialize

    from cstar_forge.domain_catalog import _DEFAULT_CATALOG_ROOT, DomainCatalog

    root = tmp_path / "catalog"
    # Copy the BUNDLED catalog (not default_catalog.catalog_root, which is now
    # the writable *user* layer -- empty/nonexistent in tests, see conftest.py).
    shutil.copytree(_DEFAULT_CATALOG_ROOT, root)
    wiz = ForgeBlueprintWizard(catalog=DomainCatalog(catalog_root=root))
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    assert wiz.config is not None

    wiz._on_save_workplan(None)

    assert "color:#080" in wiz.workplan_status.value, wiz.workplan_status.value
    bp_path = Path(wiz.save_path.value)
    wp_path = root / "workplans" / f"{wiz.config.name}.workplan.yaml"
    assert bp_path.exists() and wp_path.exists()
    assert bp_path.parent == root / "blueprints"
    # the saved YAML round-trips through C-Star's own workplan loader, including
    # its producer-must-be-a-dependency validation of the deferred reference
    wp = deserialize(wp_path, Workplan)
    assert [s.name for s in wp.steps] == ["forge", "roms_marbl"]
    assert wp.steps[1].is_deferred
    assert "cstar workplan run" in wiz.workplan_status.value
    # The printed command carries no env-var prefix: the forge app reaches C-Star's
    # registry through cstar-forge's `cstar.applications` entry point.
    assert "CSTAR_APP_MODULES" not in wiz.workplan_status.value


@requires_workplan_support
def test_on_save_workplan_falls_back_to_blueprint_sibling(tmp_path, monkeypatch):
    """When the catalog isn't a writable local filesystem, the workplan is saved
    next to the blueprint (mirroring the blueprint save-path fallback).
    """
    wiz = ForgeBlueprintWizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    assert wiz.config is not None
    monkeypatch.setattr(type(wiz.catalog), "_is_local", False)
    wiz.save_path.value = str(tmp_path / f"{wiz.config.name}.forge_blueprint.yaml")

    wiz._on_save_workplan(None)

    assert "color:#080" in wiz.workplan_status.value, wiz.workplan_status.value
    assert (tmp_path / f"{wiz.config.name}.workplan.yaml").exists()


def test_on_run_guards_on_invalid_config(monkeypatch):
    """Clicking Run with no resolved config shows an error and spawns nothing."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz.config = None

    def _boom(*a, **kw):
        raise AssertionError("must not spawn a subprocess for an invalid config")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)
    assert "invalid" in wiz.run_status.value
    assert wiz.run_output.outputs == ()


class _FakeStdout:
    """Minimal async stand-in for asyncio.StreamReader's ``read(n)``.

    Serves a flat byte buffer in small, deliberately line-*un*aligned chunks (7
    bytes by default), so a test exercises the wizard's chunk-to-line reassembly
    rather than getting one whole line per read (which would prove nothing). Returns
    ``b""`` at EOF, like the real reader.

    Deliberately implements only ``read(n)`` and no ``__aiter__``: the wizard must
    not go back to ``async for line in proc.stdout`` (StreamReader.readline), whose
    64 KiB line limit is the bug this change removed -- doing so would fail here.
    """

    def __init__(self, data, chunk_size=7):
        self._data = data if isinstance(data, bytes) else b"".join(data)
        self._pos = 0
        self._chunk_size = chunk_size

    async def read(self, n=-1):
        if self._pos >= len(self._data):
            return b""
        take = len(self._data) - self._pos if n < 0 else min(n, self._chunk_size)
        chunk = self._data[self._pos : self._pos + take]
        self._pos += len(chunk)
        return chunk


class _FakeProcess:
    def __init__(self, data, returncode=0, chunk_size=7):
        self.stdout = _FakeStdout(data, chunk_size=chunk_size)
        self._returncode = returncode

    async def wait(self):
        return self._returncode


def test_on_run_streams_subprocess_output_and_reports_success(monkeypatch, tmp_path):
    """Run auto-saves the current blueprint, launches the built command with
    stderr merged into stdout, and streams each line into run_output. There's no
    running event loop in a plain test function, so _schedule_coroutine's
    asyncio.run(...) fallback runs the whole thing to completion synchronously --
    no pytest.mark.asyncio needed.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    captured_cmd = {}

    async def _fake_create_subprocess_exec(*args, **kwargs):
        captured_cmd["args"] = args
        captured_cmd["kwargs"] = kwargs
        return _FakeProcess([b"line one\n", b"line two\n"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert (tmp_path / "bp.yaml").exists()  # auto-saved before running
    assert captured_cmd["kwargs"]["stderr"] == asyncio.subprocess.STDOUT
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert "line one" in text
    assert "line two" in text
    assert "✓ finished" in wiz.run_status.value
    # ...and the success message hands the user their next command, since the
    # app-framework path prints no "run it with" trailer of its own
    assert "cstar blueprint run" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def test_on_run_reports_nonzero_exit_code(monkeypatch, tmp_path):
    """A failing subprocess is reported as an error status, not a silent success."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake_create_subprocess_exec(*args, **kwargs):
        return _FakeProcess([b"uh oh\n"], returncode=1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert "exited with code 1" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def _run_wiz_with_output(monkeypatch, tmp_path, data, *, returncode=0, chunk_size=7):
    """Drive _on_run with a fake process emitting ``data`` (bytes); return the wizard."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake(*args, **kwargs):
        return _FakeProcess(data, returncode=returncode, chunk_size=chunk_size)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    wiz._on_run(None)
    return wiz


def test_on_run_survives_line_longer_than_stream_limit(monkeypatch, tmp_path):
    r"""A child that emits far more than 64 KiB with no newline (the classic
    \r-less/progress-less long line) must NOT raise the asyncio StreamReader
    "Separator is not found, and chunk exceed the limit" ValueError that the old
    ``async for line in proc.stdout`` loop did -- the content still lands, split
    across multiple appends by the memory-bounding flush.
    """
    from cstar_forge.forge_blueprint_wizard import _STREAM_MAX_LINE

    giant = b"x" * (_STREAM_MAX_LINE * 3 + 17) + b"\ndone\n"
    # Read in chunks well below the flush threshold and unaligned to it, so the
    # buffer must *accumulate across many reads* before each flush -- the real
    # production path (asyncio read() returns whatever is buffered, usually far less
    # than the threshold), not one oversized read that drains immediately.
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, giant, chunk_size=3000)

    assert "✓ finished" in wiz.run_status.value  # no exception surfaced
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert text.count("x") == _STREAM_MAX_LINE * 3 + 17  # every byte preserved
    assert "done\n" in text
    # bounded memory => the giant run was flushed as several appends, not held whole
    assert len(wiz.run_output.outputs) > 3


def test_on_run_splits_carriage_return_progress_into_lines(monkeypatch, tmp_path):
    r"""\r-redrawn progress (git clone / tqdm) surfaces as successive log lines
    instead of one accumulating line.
    """
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, b"10%\r20%\r30%\n")

    texts = [o["text"] for o in wiz.run_output.outputs]
    assert texts == ["10%\n", "20%\n", "30%\n"]


def test_on_run_error_status_names_the_command(monkeypatch, tmp_path):
    """An exception during the run is reported WITH the command that was running,
    not as a context-free error string.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True

    async def _boom(*args, **kwargs):
        raise OSError("no such executable")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)

    assert "OSError: no such executable" in wiz.run_status.value
    assert "while running:" in wiz.run_status.value
    assert "bp.yaml" in wiz.run_status.value  # the built command is named
    assert wiz.run_btn.disabled is False


def test_drain_stream_buffer_crlf_split_across_reads():
    r"""A \r\n straddling a read boundary is one line break, not \r + blank line:
    the trailing \r is held back for the next chunk.
    """
    lines, rem = _drain_stream_buffer(b"abc\ndef\r")
    assert lines == ["abc\n"]
    assert rem == b"def\r"  # \r held, not emitted as a terminator yet
    lines2, rem2 = _drain_stream_buffer(rem + b"\nghi")
    assert lines2 == ["def\n"]  # the held \r + \n = a single CRLF break
    assert rem2 == b"ghi"


def test_drain_stream_buffer_mixed_terminators_and_eof():
    r"""\r, \n and \r\n all cut lines (normalised to \n); EOF flushes the
    unterminated remainder.
    """
    lines, rem = _drain_stream_buffer(b"a\rb\nc\r\nd")
    assert lines == ["a\n", "b\n", "c\n"]
    assert rem == b"d"
    flushed, rem2 = _drain_stream_buffer(rem, at_eof=True)
    assert flushed == ["d"]  # no trailing newline added at EOF
    assert rem2 == b""


# ===========================================================================
# User-provided-netCDF attach flows (grid / CDR forcing / river custom_file)
# ===========================================================================
#
# Real (tiny) netCDFs are used wherever ``hash_netcdf_contents`` runs for real
# (it opens the file with xarray) -- only ``roms_tools.Grid`` itself is stubbed
# (real Grid *generation* is broken in this env; ``Grid(filename=...)`` loading
# is also stubbed here for speed/determinism, mirroring
# test_parent_plot_is_always_grid_plot_only's monkeypatch pattern).


def _write_tiny_netcdf(path: Path) -> Path:
    """A minimal real netCDF, just for ``hash_netcdf_contents`` to hash."""
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {"temp": (["y", "x"], np.zeros((2, 2), dtype=np.float64))},
        attrs={"title": "tiny test file"},
    )
    ds.to_netcdf(path)
    return path


def _write_tiny_cdr_netcdf(path: Path, with_ncdr_dim: bool = True) -> Path:
    """A minimal real netCDF with (or without) the ``ncdr`` dimension the CDR
    attach flow's light validation checks for.
    """
    import numpy as np
    import xarray as xr

    dim = "ncdr" if with_ncdr_dim else "n_other"
    ds = xr.Dataset({"cdr_volume": ([dim], np.zeros(3, dtype=np.float64))})
    ds.to_netcdf(path)
    return path


class _FakeLoadedGrid:
    """Stands in for ``rt.Grid(filename=...)``'s return value: only the
    attributes the wizard/resolver actually read off a loaded grid file
    (nx/ny/N/center_lon/center_lat/rot/size_x/size_y/theta_s/theta_b/hc).

    Mirrors real roms-tools I/O behavior for a missing ``filename`` (raises)
    rather than silently succeeding -- needed so a test simulating a
    since-deleted grid_file also sees the resolver's own independent reload
    attempt (``build_forge_blueprint``'s ``rt.Grid(filename=grid_file_obj.location)``
    when no ``grid=`` is passed) fail the same way a real missing file would.
    """

    def __init__(self, **kwargs):
        filename = kwargs.get("filename")
        if filename is not None and not Path(filename).exists():
            raise FileNotFoundError(f"no such file: {filename}")
        self.kwargs = kwargs
        self.nx = 10
        self.ny = 8
        self.N = 5
        self.center_lon = 12.0
        self.center_lat = 34.0
        self.rot = 0.0
        self.size_x = 300.0
        self.size_y = 250.0
        self.theta_s = 6.0
        self.theta_b = 3.0
        self.hc = 200.0


@pytest.fixture
def fake_grid(monkeypatch):
    """Stub ``roms_tools.Grid`` for the duration of a test."""
    import roms_tools

    monkeypatch.setattr(roms_tools, "Grid", _FakeLoadedGrid)
    return _FakeLoadedGrid


def _new_wizard():
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    return wiz


class TestGridFileAttach:
    def test_attach_locks_and_populates_widgets(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")

        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)

        assert wiz._grid_file == {
            "location": str(p),
            "content_hash": wiz._grid_file["content_hash"],
        }
        assert isinstance(wiz._grid_file_grid, _FakeLoadedGrid)
        assert wiz._grid_file_grid.kwargs == {
            "filename": str(p)
        }  # loaded from this path
        assert wiz.grid_w["nx"].value == 10
        assert wiz.grid_w["center_lon"].value == 12.0
        assert wiz.scoord_chk.value is True  # theta_s/theta_b/hc all present

        for w in (
            *wiz.grid_w.values(),
            wiz.scoord_chk,
            wiz.hmin,
            wiz.close_narrow_chk,
            wiz.mask_shapefile,
            wiz.topo_source,
            wiz.topo_path,
            wiz.nest_enable,
            wiz.parent_enable,
        ):
            assert w.disabled is True
        assert "attached" in wiz.grid_file_status.value
        assert "exact path" in wiz.grid_file_status.value  # persistent warning

    def test_upload_fallback_stages_and_attaches(
        self, fake_grid, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        wiz = _new_wizard()
        src = _write_tiny_netcdf(tmp_path / "uploaded.nc")
        change = {"new": ({"name": "uploaded.nc", "content": src.read_bytes()},)}

        wiz._on_grid_file_upload(change)

        staged = tmp_path / "forge_user_files" / "uploaded.nc"
        assert staged.exists()
        assert wiz._grid_file == {
            "location": str(staged),
            "content_hash": wiz._grid_file["content_hash"],
        }
        assert wiz.grid_w["nx"].disabled is True

    def test_attach_error_shown_in_status_not_raised(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        wiz.grid_file_path.value = str(tmp_path / "does-not-exist.nc")

        wiz._on_grid_file_attach(None)  # must not raise

        assert wiz._grid_file is None
        assert "FileNotFoundError" in wiz.grid_file_status.value

    def test_detach_restores_widgets(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        pre_attach_nx = wiz.grid_w["nx"].value
        pre_attach_center_lon = wiz.grid_w["center_lon"].value
        assert pre_attach_nx != 10  # sanity: differs from _FakeLoadedGrid's nx
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz.grid_w["nx"].disabled is True
        assert wiz.grid_w["nx"].value == 10  # overwritten by the attached file

        wiz._on_grid_file_detach(None)

        assert wiz._grid_file is None
        assert wiz._grid_file_grid is None
        assert wiz.grid_file_status.value == ""
        assert wiz.grid_file_path.value == ""
        for w in (*wiz.grid_w.values(), wiz.scoord_chk, wiz.nest_enable):
            assert w.disabled is False
        # The user's own pre-attach geometry is restored, not left at the
        # detached file's (now meaningless) values.
        assert wiz.grid_w["nx"].value == pre_attach_nx
        assert wiz.grid_w["center_lon"].value == pre_attach_center_lon

    def test_reattach_and_detach_restores_original_pre_attach_geometry(
        self, fake_grid, tmp_path
    ):
        """Re-attaching a second file without detaching first must not clobber
        the snapshot with the first file's values -- Detach must still give
        back the ORIGINAL pre-any-attach geometry.
        """
        wiz = _new_wizard()
        pre_attach_nx = wiz.grid_w["nx"].value
        p1 = _write_tiny_netcdf(tmp_path / "grid1.nc")
        wiz.grid_file_path.value = str(p1)
        wiz._on_grid_file_attach(None)
        assert wiz.grid_w["nx"].value == 10

        p2 = _write_tiny_netcdf(tmp_path / "grid2.nc")
        wiz.grid_file_path.value = str(p2)
        wiz._on_grid_file_attach(None)  # re-attach without detaching
        assert wiz.grid_w["nx"].value == 10  # still the (only) fake grid's nx

        wiz._on_grid_file_detach(None)

        assert wiz.grid_w["nx"].value == pre_attach_nx

    def test_gather_emits_grid_file_and_empty_grid_kwargs(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)

        kw = wiz._gather()

        assert kw["grid_file"] == wiz._grid_file
        assert kw["grid"] is wiz._grid_file_grid
        assert kw["grid_kwargs"] == {}
        assert "grid_kwargs_child" not in kw
        assert "grid_kwargs_parent" not in kw

    def test_rebuild_does_not_rehash_after_attach(
        self, fake_grid, tmp_path, monkeypatch
    ):
        """The one hash computation happens at Attach time; every subsequent
        _rebuild() (triggered here by an unrelated widget edit) must reuse the
        cached dict, never recomputing the digest.
        """
        import cstar_forge.forge.user_files as user_files_mod
        import cstar_forge.forge_blueprint_wizard as wizard_mod

        calls = {"n": 0}
        real_hash = user_files_mod.hash_netcdf_contents

        def _counting_hash(path):
            calls["n"] += 1
            return real_hash(path)

        monkeypatch.setattr(wizard_mod, "hash_netcdf_contents", _counting_hash)
        monkeypatch.setattr(user_files_mod, "hash_netcdf_contents", _counting_hash)

        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert calls["n"] == 1

        wiz.description.value = "edited after attach"  # triggers _rebuild()
        wiz._rebuild()

        assert calls["n"] == 1  # never rehashed
        assert wiz.config is not None

    def test_config_round_trips_attached_and_locked_through_populate_from(
        self, fake_grid, tmp_path
    ):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz.config is not None
        cfg = wiz.config

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._grid_file is not None
        assert wiz2._grid_file["content_hash"] == wiz._grid_file["content_hash"]
        assert wiz2.grid_w["nx"].disabled is True
        assert wiz2.config is not None
        assert "attached" in wiz2.grid_file_status.value

    def test_reattach_failure_keeps_locked_and_surfaces_error(
        self, fake_grid, tmp_path
    ):
        """A missing file at reload time must not silently fall back to the
        default/generic grid_kwargs (which would gather a different blueprint) --
        the grid_file dict + locked widgets stay in place, and _gather()/
        _rebuild() surface the failure loudly (config goes Invalid).
        """
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        cfg = wiz.config

        p.unlink()  # the file is now missing at "reload" time

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._grid_file is not None  # kept, not cleared
        assert wiz2._grid_file_grid is None
        assert wiz2.grid_w["nx"].disabled is True  # still locked
        assert "could not re-attach" in wiz2.grid_file_status.value
        assert wiz2.config is None  # surfaced loudly, not silently substituted

        # Plot/Derive/the Save-Run safety net must likewise refuse to silently
        # build a grid from the (locked, stale) grid_w values instead of the
        # missing file -- not just build_forge_blueprint().
        with pytest.raises(RuntimeError, match="failed to"):
            wiz2._build_grid_from_widgets()
        # _populate_from freezes a loaded file's boundaries as touched (a
        # deliberate, already-resolved choice -- see _populate_from), which
        # would short-circuit _ensure_boundaries_derived() before it ever
        # reaches _build_grid_from_widgets(); force the untouched path here to
        # actually exercise the safety net's own grid-build attempt.
        wiz2._boundaries_touched = False
        assert wiz2._ensure_boundaries_derived() is False
        assert "failed to" in wiz2.derive_status.value

    def test_domain_pick_detaches_attached_grid_file(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        assert wiz._grid_file is not None

        wiz.domain_dd.value = ForgeBlueprintWizard._dd_values(wiz.domain_dd)[
            1
        ]  # first real catalog domain

        assert wiz._grid_file is None
        assert wiz.grid_w["nx"].disabled is False

    def test_save_domain_guards_while_grid_file_attached(self, fake_grid, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_netcdf(tmp_path / "grid.nc")
        wiz.grid_file_path.value = str(p)
        wiz._on_grid_file_attach(None)
        wiz.save_domain_name.value = "some-new-domain"

        wiz._on_save_domain(None)

        assert "Detach the grid file first" in wiz.save_domain_status.value


class TestCdrFileAttach:
    def test_attach_populates_and_clears_yaml_upload(self, tmp_path):
        wiz = _new_wizard()
        wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))
        assert wiz._cdr_forcing is not None

        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        assert wiz._cdr_forcing_file == {
            "location": str(p),
            "content_hash": wiz._cdr_forcing_file["content_hash"],
        }
        assert wiz._cdr_forcing is None
        assert "cleared" in wiz.cdr_file_status.value.lower()
        assert "attached" in wiz.cdr_file_status.value.lower()

    def test_upload_fallback_stages_and_attaches(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        wiz = _new_wizard()
        src = _write_tiny_cdr_netcdf(tmp_path / "uploaded_cdr.nc")
        change = {"new": ({"name": "uploaded_cdr.nc", "content": src.read_bytes()},)}

        wiz._on_cdr_file_upload(change)

        staged = tmp_path / "forge_user_files" / "uploaded_cdr.nc"
        assert staged.exists()
        assert wiz._cdr_forcing_file == {
            "location": str(staged),
            "content_hash": wiz._cdr_forcing_file["content_hash"],
        }

    def test_yaml_upload_clears_attached_cdr_file(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz._cdr_forcing_file is not None

        wiz._on_cdr_upload(_upload_change(_CDR_SAMPLE_YAML.read_bytes()))

        assert wiz._cdr_forcing is not None
        assert wiz._cdr_forcing_file is None
        assert "cleared" in wiz.cdr_status.value.lower()

    def test_attach_warns_but_does_not_block_when_ncdr_dim_missing(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc", with_ncdr_dim=False)
        wiz.cdr_file_path.value = str(p)

        wiz._on_cdr_file_attach(None)

        assert wiz._cdr_forcing_file is not None  # not blocked
        assert "ncdr" in wiz.cdr_file_status.value.lower()

    def test_gather_emits_cdr_forcing_file(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        kw = wiz._gather()

        assert kw["cdr_forcing_file"] == wiz._cdr_forcing_file
        assert "cdr_forcing" not in kw

    def test_clear_resets_state(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)

        wiz._on_cdr_file_clear(None)

        assert wiz._cdr_forcing_file is None
        assert wiz.cdr_file_status.value == ""
        assert "cdr_forcing_file" not in wiz._gather()

    def test_round_trips_through_populate_from(self, tmp_path):
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz.config is not None
        cfg = wiz.config

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(cfg)

        assert wiz2._cdr_forcing_file == wiz._cdr_forcing_file
        assert "attached" in wiz2.cdr_file_status.value.lower()
        assert wiz2.config is not None

    def test_forcing_spec_carrying_cdr_clears_attached_cdr_file(
        self, tmp_path, monkeypatch
    ):
        """Picking a ForcingSpec whose own CDR forcing is non-empty must clear an
        attached CDR file (Forcing's own validator forbids both at once). Stubs
        catalog.forcing_data with an embedded cdr_forcing block, independent of
        whether the bundled default ForcingSpec happens to carry one.
        """
        wiz = _new_wizard()
        p = _write_tiny_cdr_netcdf(tmp_path / "cdr.nc")
        wiz.cdr_file_path.value = str(p)
        wiz._on_cdr_file_attach(None)
        assert wiz._cdr_forcing_file is not None

        fake_spec = dict(wiz.catalog.forcing_data(wiz.forcing_dd.value))
        fake_spec["cdr_forcing"] = {"releases": []}
        monkeypatch.setattr(wiz.catalog, "forcing_data", lambda _name: fake_spec)

        wiz._on_forcing_spec(None)

        assert wiz._cdr_forcing == {"releases": []}
        assert wiz._cdr_forcing_file is None


class TestRiverCustomFileAttach:
    def test_selecting_custom_file_toggles_visibility(self, editor):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        assert _display(w["custom_file_path"]) == "none"
        assert _display(w["climatology"]) == ""
        assert _display(w["path"]) == ""

        w["name"].value = "CUSTOM_FILE"

        assert _display(w["custom_file_path"]) == ""
        assert _display(w["custom_file_attach_btn"]) == ""
        assert _display(w["custom_file_upload"]) == ""
        assert _display(w["custom_file_status"]) == ""
        assert _display(w["climatology"]) == "none"
        assert _display(w["include_bgc"]) == "none"
        assert _display(w["convert_to_climatology"]) == "none"
        assert _display(w["coast_snap_buffer_km"]) == "none"
        assert _display(w["domain_edge_buffer"]) == "none"
        assert _display(w["bgc_source_name"]) == "none"
        assert _display(w["bgc_source_path"]) == "none"
        assert _display(w["path"]) == "none"

        w["name"].value = "DAI"
        assert _display(w["custom_file_path"]) == "none"
        assert _display(w["climatology"]) == ""
        assert _display(w["path"]) == ""

    def test_include_bgc_visibility_still_works_after_leaving_custom_file(self, editor):
        """Switching CUSTOM_FILE -> DAI must hand bgc-widget visibility back to
        include_bgc's own sync, not leave it stuck from the custom-file branch.
        """
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["name"].value = "DAI"
        assert _display(w["bgc_source_name"]) == "none"  # include_bgc still False

        w["include_bgc"].value = True
        assert _display(w["bgc_source_name"]) == ""

    def test_attach_and_gather_emits_custom_file_omits_standard_fields(
        self, editor, tmp_path
    ):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["include_bgc"].value = True  # would-be leftover state; must be ignored
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)

        w["custom_file_attach_btn"].click()

        assert w["_custom_file"] == {
            "location": str(p),
            "content_hash": w["_custom_file"]["content_hash"],
        }
        assert "attached" in w["custom_file_status"].value.lower()

        item = editor._gather_item("river", w)

        assert item == {
            "source": {"name": "CUSTOM_FILE"},
            "custom_file": w["_custom_file"],
        }

    def test_upload_fallback_stages_and_attaches(self, editor, tmp_path, monkeypatch):
        """Unlike the grid/CDR upload-fallback tests (which call the wizard's
        ``_on_*_upload`` handler directly, bypassing the widget), the river
        row's upload handler is a closure with no externally-reachable name --
        this must go through the real ``FileUpload.value`` trait, so the
        change item needs every key ipywidgets' own (de)serializer requires
        (name/type/size/content/last_modified), not just the two the handler
        itself reads.
        """
        import datetime as dt

        monkeypatch.chdir(tmp_path)  # forge_user_files/ lands under Path.cwd()
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        src = _write_tiny_netcdf(tmp_path / "uploaded_river.nc")
        content = src.read_bytes()

        w["custom_file_upload"].value = (
            {
                "name": "uploaded_river.nc",
                "type": "application/x-netcdf",
                "size": len(content),
                "content": content,
                "last_modified": dt.datetime.now(dt.UTC),
            },
        )

        staged = tmp_path / "forge_user_files" / "uploaded_river.nc"
        assert staged.exists()
        assert w["_custom_file"] == {
            "location": str(staged),
            "content_hash": w["_custom_file"]["content_hash"],
        }

    def test_attach_error_shown_in_status(self, editor, tmp_path):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"
        w["custom_file_path"].value = str(tmp_path / "missing.nc")

        w["custom_file_attach_btn"].click()  # must not raise

        assert w["_custom_file"] is None
        assert "FileNotFoundError" in w["custom_file_status"].value

    def test_gather_item_hints_when_nothing_attached_yet(self, editor):
        w = editor._make_row("river", {"source": {"name": "DAI"}})
        w["name"].value = "CUSTOM_FILE"

        item = editor._gather_item("river", w)

        assert item == {"source": {"name": "CUSTOM_FILE"}}  # no custom_file key

    def test_custom_file_round_trips_through_populate_from(self, tmp_path):
        wiz = _new_wizard()
        fe = wiz._forcing_editor
        assert fe._rows["river"], "expected a default river row from ForcingSpec"
        w = fe._rows["river"][0]
        w["name"].value = "CUSTOM_FILE"
        p = _write_tiny_netcdf(tmp_path / "river.nc")
        w["custom_file_path"].value = str(p)
        w["custom_file_attach_btn"].click()
        wiz._rebuild()
        assert wiz.config is not None, wiz.derived.value
        assert any(it.source.name == "CUSTOM_FILE" for it in wiz.config.forcing.river)

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)  # must not raise ValueError

        assert wiz2.config is not None
        fe2 = wiz2._forcing_editor
        custom_rows = [
            ws for ws in fe2._rows["river"] if ws["name"].value == "CUSTOM_FILE"
        ]
        assert len(custom_rows) == 1
        assert custom_rows[0]["_custom_file"]["location"] == str(p)
        assert _display(custom_rows[0]["custom_file_path"]) == ""
        assert _display(custom_rows[0]["climatology"]) == "none"

    def test_generic_source_path_round_trips_through_populate_from(self):
        """Non-custom river categories already carry SourceSpec.path (a WP3 fix);
        this pins the wizard's load-back side: w["path"] must repopulate too.
        """
        wiz = _new_wizard()
        fe = wiz._forcing_editor
        w = fe._rows["river"][0]
        w["name"].value = "DAI"
        w["path"].value = "/custom/river/source.nc"
        wiz._rebuild()
        assert wiz.config is not None
        dai_item = next(
            it for it in wiz.config.forcing.river if it.source.name == "DAI"
        )
        assert dai_item.source.path == "/custom/river/source.nc"

        wiz2 = ForgeBlueprintWizard()
        wiz2._populate_from(wiz.config)

        fe2 = wiz2._forcing_editor
        w2 = next(ws for ws in fe2._rows["river"] if ws["name"].value == "DAI")
        assert w2["path"].value == "/custom/river/source.nc"
