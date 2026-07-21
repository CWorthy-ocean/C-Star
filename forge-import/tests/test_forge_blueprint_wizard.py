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

import cstar_forge
from cstar_forge.forge_blueprint_wizard import (
    ForgeBlueprintWizard,
    _ForcingEditor,
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
    top-level use_pio (default False for the bundled catalog model).
    """
    wiz = ForgeBlueprintWizard()
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
    assert wiz.use_pio_chk.value is False
    wiz._rebuild()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is False
    assert wiz.config.code.pio is None

    wiz.use_pio_chk.value = True
    wiz._rebuild()
    assert wiz.config.model_settings["cppdefs"]["use_pio"] is True
    assert wiz.config.code.pio is not None


_CDR_SAMPLE_YAML = (
    Path(cstar_forge.__file__).parent
    / "catalog"
    / "blueprints"
    / "MacOS"
    / "cson_roms-marbl_v0.1_test-tiny_1procs"
    / "_cdr_forcing.yaml"
)


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
    silently into the blueprint and only fail much later, during Phase-2 execution.
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
    wiz.domain_dd.value = wiz.domain_dd.options[1]  # first real catalog domain
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
    wiz.domain_dd.value = wiz.domain_dd.options[1]
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


def test_build_run_command_uses_current_interpreter():
    """The Run button invokes `sys.executable -m cstar_forge.run <path>` -- the
    interpreter already running the wizard's kernel -- not a bare `python` or
    `conda run` invocation (avoids conda/micromamba env-discovery issues).
    """
    import sys

    wiz = ForgeBlueprintWizard()
    cmd = wiz._build_run_command("/tmp/some_blueprint.yaml")
    assert cmd == [sys.executable, "-m", "cstar_forge.run", "/tmp/some_blueprint.yaml"]


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
    """Minimal async-iterable mimicking asyncio.StreamReader's line iteration."""

    def __init__(self, lines):
        self._lines = iter(lines)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._lines)
        except StopIteration:
            raise StopAsyncIteration


class _FakeProcess:
    def __init__(self, lines, returncode=0):
        self.stdout = _FakeStdout(lines)
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
    assert wiz.run_status.value == "<span style='color:#080'>✓ finished</span>"
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
