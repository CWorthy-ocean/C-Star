"""Forcing-row option disclosures, output readouts, variable-list panes, the derived
``v_sponge`` display, MARBL list boxes and the shared layout components of the wizard.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("ipywidgets")

import ipywidgets as W

from cstar.applications.forge.blueprint import (
    RiverForcingItem,
    RiverSource,
)
from cstar.wizard.ui import components
from cstar.wizard.wizard import (
    _ROW_OPTION_DEFAULTS,
    ForgeBlueprintWizard,
    _display_round,
    _ForcingEditor,
    _make_field_widget,
    _model_time_text,
    _read_field_widget,
    _SettingsEditor,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_HIS = "his"
_AVG = "avg"
_OPTION_KEYS = (
    "coarse_grid_mode",
    "blend_fallback",
    "blend_fallback_correct_radiation",
    "blend_width_km",
    "blend_profile",
    "prefill",
    "regrid_method",
    "extrap_method",
    "options",
)
_PRIMARY_KEYS = (
    "type",
    "name",
    "path",
    "climatology",
    "correct_radiation",
    "wind_dropoff",
)
_DAY = 86400


@pytest.fixture(scope="module")
def shared_wizard() -> ForgeBlueprintWizard:
    """A wizard built once, for tests that only read it."""
    return ForgeBlueprintWizard()


def _editor(wiz: ForgeBlueprintWizard) -> _ForcingEditor:
    assert wiz._forcing_editor is not None
    return wiz._forcing_editor


def _settings(wiz: ForgeBlueprintWizard) -> _SettingsEditor:
    assert wiz.editor is not None
    return wiz.editor


def _physics_row(editor: _ForcingEditor) -> dict[str, Any]:
    return editor._make_row("surface", {"type": "physics", "source": {"name": "ERA5"}})


def _bgc_row(editor: _ForcingEditor) -> dict[str, Any]:
    return editor._make_row("surface", {"type": "bgc", "source": {"name": "UNIFIED"}})


def _river_row(editor: _ForcingEditor, name: str = "DAI") -> dict[str, Any]:
    return editor._make_row("river", {"source": {"name": name}})


def _walk(widget: Any) -> Iterator[Any]:
    yield widget
    for child in getattr(widget, "children", ()):
        yield from _walk(child)


def _collapsibles(root: Any) -> list[Any]:
    return [x for x in _walk(root) if "forge-coll" in getattr(x, "_dom_classes", ())]


def _history_pane(wiz: ForgeBlueprintWizard) -> Any:
    panes = [
        p
        for p in _collapsibles(_settings(wiz).output_box)
        if p.title.title == "Variables in history files"
    ]
    assert len(panes) == 1
    return panes[0]


def _pills(pane: Any) -> list[str]:
    return [p.text for p in pane.title.pills]


# ---- 1. forcing-row options disclosure ---------------------------------------


def test_surface_option_fields_live_inside_the_disclosure(shared_wizard):
    """A physics row keeps its option keys in the disclosure and the primary keys inline."""
    editor = _editor(shared_wizard)
    w = _physics_row(editor)
    box = editor._row_box(w)
    children = list(box.children)
    body = list(w["_options"].body.children)
    for key in _OPTION_KEYS:
        assert w[key] not in children
        assert w[key] in body
    for key in _PRIMARY_KEYS:
        assert w[key] in children
    assert children[-1] is w["_options"]


def test_option_title_counts_only_shown_widgets(shared_wizard):
    """The disclosure's count drops by one for a bgc row, which hides the blend fallback."""
    editor = _editor(shared_wizard)
    physics = _physics_row(editor)["_options"].get_title()
    bgc = _bgc_row(editor)["_options"].get_title()
    n_physics = int(re.search(r"(\d+) settings", physics).group(1))
    n_bgc = int(re.search(r"(\d+) settings", bgc).group(1))
    assert n_bgc == n_physics - 1


def test_option_title_tracks_non_default_values(shared_wizard):
    """The title says defaults, then 1 non-default after a change, then defaults again."""
    editor = _editor(shared_wizard)
    w = _physics_row(editor)
    assert "defaults" in w["_options"].get_title()
    option = next(o for o in w["prefill"].options if o and o != "")
    w["prefill"].value = option
    assert "1 non-default" in w["_options"].get_title()
    w["prefill"].value = ""
    assert "defaults" in w["_options"].get_title()
    assert "non-default" not in w["_options"].get_title()


def test_custom_file_river_hides_the_disclosure(shared_wizard):
    """A custom-file river has nothing to show, so the disclosure is hidden."""
    editor = _editor(shared_wizard)
    custom = _river_row(editor, RiverSource.CUSTOM_FILE.value)
    assert custom["_options"].layout.display == "none"
    assert _river_row(editor)["_options"].layout.display != "none"
    # Back to a dataset source, the options (passthrough box included) return.
    custom["name"].value = RiverSource.DAI.value
    assert custom["options"].layout.display != "none"
    assert custom["_options"].layout.display != "none"


def test_river_disclosure_is_titled_river_options(shared_wizard):
    """A river row's disclosure is titled River options."""
    editor = _editor(shared_wizard)
    assert "River options" in _river_row(editor)["_options"].get_title()


@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_bgc_source_rows_have_no_disclosure(shared_wizard, cat):
    """The BGC source rows have no options disclosure."""
    editor = _editor(shared_wizard)
    w = editor._make_row(cat, {"source": {"name": "UNIFIED"}})
    assert "_options" not in w


def test_open_state_survives_render():
    """A re-render keeps the same disclosure widget, still open."""
    editor = _editor(ForgeBlueprintWizard())
    w = editor._rows["surface"][0]
    pane = w["_options"]
    pane.selected_index = 0
    editor._add("surface")
    same = editor._rows["surface"][0]
    assert same is w
    assert same["_options"] is pane
    assert pane.selected_index == 0
    assert pane in editor._row_box(same).children


def test_river_defaults_are_single_sourced():
    """The river defaults table reads the model's field defaults."""
    assert (
        _ROW_OPTION_DEFAULTS["river"]["domain_edge_buffer"]
        == RiverForcingItem.model_fields["domain_edge_buffer"].default
    )
    assert (
        _ROW_OPTION_DEFAULTS["river"]["river_temp_smoothing_window_days"]
        == RiverForcingItem.model_fields["river_temp_smoothing_window_days"].default
    )


def test_river_defaults_are_omitted_when_gathered(shared_wizard):
    """A river row at its defaults gathers without the two option keys; an edit adds one."""
    editor = _editor(shared_wizard)
    w = _river_row(editor)
    item = editor._gather_item("river", w)
    assert "domain_edge_buffer" not in item
    assert "river_temp_smoothing_window_days" not in item
    w["domain_edge_buffer"].value = 21
    assert editor._gather_item("river", w)["domain_edge_buffer"] == 21


@pytest.mark.parametrize(
    "make", [_physics_row, _bgc_row, _river_row], ids=["physics", "bgc", "river"]
)
def test_no_row_label_ends_with_a_colon(shared_wizard, make):
    """No row widget's label ends with a colon."""
    w = make(_editor(shared_wizard))
    labels = [
        (k, v.description)
        for k, v in w.items()
        if isinstance(getattr(v, "description", None), str) and v.description
    ]
    assert labels
    assert [k for k, d in labels if d.rstrip().endswith(":")] == []


# ---- 2. output readouts ---------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (86400, "1 day"),
        (172800, "2 days"),
        (3600, "1 hour"),
        (5400, "90 minutes"),
        (45, ""),
    ],
)
def test_model_time_text(seconds, text):
    """A period reads in the largest whole unit, or blank below a minute."""
    assert _model_time_text(seconds) == text


def _stream(wiz: ForgeBlueprintWizard, suffix: str):
    """``(readout, write, period, records)`` of the ``his``/``avg`` output-table row."""
    editor = _settings(wiz)
    period = editor._widgets[("ocean_vars", f"output_period_{suffix}")][0]
    write = editor._widgets[("ocean_vars", f"wrt_file_{suffix}")][0]
    records = editor._widgets[("ocean_vars", f"nrpf_{suffix}")][0]
    (readout,) = [r for r in editor._readouts if r.period is period]
    return readout, write, period, records


def _configure(wiz, suffix, *, per_file, days=30):
    readout, write, period, records = _stream(wiz, suffix)
    write.value = True
    period.value = _DAY
    records.value = per_file
    _settings(wiz).set_run_window(days * _DAY)
    return readout


@pytest.mark.parametrize(
    ("suffix", "per_file", "files"),
    [(_HIS, 1, 31), (_AVG, 1, 30), (_HIS, 10, 4), (_AVG, 10, 3)],
)
def test_file_count_readout(suffix, per_file, files):
    """History counts the initial record, averages do not, and files hold per_file records."""
    readout = _configure(ForgeBlueprintWizard(), suffix, per_file=per_file)
    assert f"{files} files" in readout.files_html.value
    if per_file == 1:
        assert "each file holds 1 day" in readout.files_html.value
    assert readout.period_html.value == "= 1 day"


def test_file_readout_is_blank_with_the_write_flag_off():
    """A stream that is not written has no file readout."""
    wiz = ForgeBlueprintWizard()
    readout = _configure(wiz, _HIS, per_file=1)
    assert readout.files_html.value
    _stream(wiz, _HIS)[1].value = False
    assert readout.files_html.value == ""


def test_readouts_are_blank_when_the_period_is_hidden_or_disabled():
    """A hidden or disabled period box blanks both readouts."""
    wiz = ForgeBlueprintWizard()
    readout = _configure(wiz, _HIS, per_file=1)
    period = readout.period
    period.layout.display = "none"
    assert (readout.period_html.value, readout.files_html.value) == ("", "")
    period.layout.display = ""
    assert readout.period_html.value
    period.disabled = True
    assert (readout.period_html.value, readout.files_html.value) == ("", "")


def test_file_readout_needs_a_run_window():
    """With no run window the file readout is blank, and it returns with one."""
    wiz = ForgeBlueprintWizard()
    readout = _configure(wiz, _HIS, per_file=1)
    _settings(wiz).set_run_window(None)
    assert readout.files_html.value == ""
    _settings(wiz).set_run_window(30 * _DAY)
    assert readout.files_html.value


def test_run_window_seconds(shared_wizard):
    """The run window is the default dates' span in seconds."""
    wiz = ForgeBlueprintWizard()
    expected = (
        wiz._run_datetime("end", wiz.end.value)
        - wiz._run_datetime("start", wiz.start.value)
    ).total_seconds()
    assert expected > 0
    assert wiz._run_window_seconds() == expected


def test_run_window_seconds_is_none_for_an_empty_window():
    """An end date equal to the start gives no run window."""
    wiz = ForgeBlueprintWizard()
    wiz.end.value = wiz.start.value
    assert wiz._run_window_seconds() is None


# ---- 3. variable-list panes -------------------------------------------------------


def test_history_variables_are_a_collapsible_grid(shared_wizard):
    """The history list is a collapsible whose body is a forge-var-grid."""
    pane = _history_pane(shared_wizard)
    assert "forge-var-grid" in pane.body._dom_classes


def test_history_pane_title_counts_checked_boxes():
    """The pane title's count matches the checked boxes and follows a toggle."""
    wiz = ForgeBlueprintWizard()
    pane = _history_pane(wiz)
    boxes = list(pane.body.children)
    assert len(boxes) == 15
    assert f"{sum(bool(b.value) for b in boxes)} of 15 selected" in _pills(pane)
    z = _settings(wiz)._widgets[("ocean_vars", "wrt_z")][0]
    assert z in boxes
    before = sum(bool(b.value) for b in boxes)
    z.value = not z.value
    after = sum(bool(b.value) for b in boxes)
    assert after != before
    assert f"{after} of 15 selected" in _pills(pane)


def test_history_pane_notes_when_the_stream_is_off():
    """The off pill shows while the history stream is not written and goes when it is."""
    wiz = ForgeBlueprintWizard()
    pane = _history_pane(wiz)
    write = _settings(wiz)._widgets[("ocean_vars", "wrt_file_his")][0]
    write.value = False
    assert "not written: this stream is off" in _pills(pane)
    write.value = True
    assert "not written: this stream is off" not in _pills(pane)


def test_surface_flux_list_is_not_collapsible(shared_wizard):
    """The four-box Fluxes written list is a flat grid, not a collapsible pane."""
    root = _settings(shared_wizard).output_box
    assert "Fluxes written" not in [p.title.title for p in _collapsibles(root)]
    flux = _settings(shared_wizard)._widgets[("surf_flux", "wrt_smflx")][0]
    grids = [
        x
        for x in _walk(root)
        if "forge-var-grid" in getattr(x, "_dom_classes", ()) and flux in x.children
    ]
    assert len(grids) == 1
    assert len(grids[0].children) == 4


# ---- 4. v_sponge display ----------------------------------------------------------


def test_derived_v_sponge_shows_rounded_and_saves_full():
    """The widget shows one decimal while the derived and saved values stay unrounded."""
    wiz = ForgeBlueprintWizard()
    shown = wiz.v_sponge.value
    assert round(shown, 1) == shown
    derived = wiz._v_sponge_value()
    assert derived == wiz._domain_snapshot()["v_sponge"]
    assert wiz.config is not None
    assert wiz.config.domain.v_sponge == derived
    assert derived != shown


def test_edited_v_sponge_is_read_back_as_typed():
    """After a user edit the widget's own value is used."""
    wiz = ForgeBlueprintWizard()
    wiz.v_sponge.value = 1234.5
    assert wiz._v_sponge_value() == 1234.5


def test_display_round_keeps_small_values():
    """Values below one keep three significant digits rather than rounding to zero."""
    assert _display_round(0.04) == 0.04
    assert _display_round(8333.333333333332) == 8333.3


# ---- 5. MARBL lists ------------------------------------------------------------------


def test_marbl_tracer_list_is_a_textarea(shared_wizard):
    """The MARBL tracers-to-write box is a wrapping text area."""
    widget = _settings(shared_wizard)._widgets[("marbl_bgc", "marbl_tracers_to_write")][
        0
    ]
    assert isinstance(widget, W.Textarea)


def test_multiline_list_field_splits_on_commas_and_lines():
    """A text area list reads back split on commas and line breaks, stripped."""
    widget = _make_field_widget(W, "t", list, ["PO4"], multiline=True)
    assert isinstance(widget, W.Textarea)
    widget.value = "PO4,\nNO3\n, Fe"
    assert _read_field_widget(widget, list) == ["PO4", "NO3", "Fe"]


# ---- 6. components ----------------------------------------------------------------------


def test_field_grid_default_columns_come_from_the_stylesheet():
    """A two-column grid carries the class and no inline column template."""
    grid = components.field_grid(W, [W.HTML("a"), W.HTML("b")])
    assert "forge-fgrid" in grid._dom_classes
    assert grid.layout.grid_template_columns is None


def test_field_grid_other_column_counts_stay_inline():
    """A grid with another column count keeps its inline template."""
    grid = components.field_grid(W, [W.HTML("a")], cols=3)
    assert "forge-fgrid" in grid._dom_classes
    assert grid.layout.grid_template_columns == "repeat(3, minmax(0, 1fr))"


def test_stylesheet_rules_for_rows_tables_and_grids_avoid_important():
    """The row, output-table and field-grid rules do not use !important."""
    rules = re.findall(r"([^{}]+)\{([^{}]*)\}", components.WIZARD_CSS)
    scoped = [
        (sel, body)
        for sel, body in rules
        if re.search(r"forge-frow|forge-out-table|forge-fgrid", sel)
    ]
    assert scoped
    assert [sel.strip() for sel, body in scoped if "!important" in body] == []


def test_stylesheet_has_no_has_child_gridbox_selector():
    """The stylesheet no longer uses the :has(> .widget-gridbox > .forge-field) selector."""
    assert ":has(> .widget-gridbox > .forge-field)" not in components.WIZARD_CSS


# ---- 7. narrow cards ---------------------------------------------------------------


def _at_rule_block(prelude: str) -> str:
    """The bodies of every ``WIZARD_CSS`` at-rule that starts with ``prelude``."""
    css = components.WIZARD_CSS
    bodies = []
    for match in re.finditer(re.escape(prelude + " {"), css):
        depth, start = 1, match.end()
        for i in range(start, len(css)):
            depth += {"{": 1, "}": -1}.get(css[i], 0)
            if depth == 0:
                bodies.append(css[start:i])
                break
    assert bodies, f"no {prelude} block"
    return "\n".join(bodies)


def _with_class(root: Any, css_class: str) -> list[Any]:
    return [x for x in _walk(root) if css_class in getattr(x, "_dom_classes", ())]


def test_output_tables_take_their_columns_from_the_stylesheet(shared_wizard):
    """An inline column template would beat the narrow-card rule, so the tables
    carry none; each row's last cell (the file readout and extras) is the one a
    narrow card moves to a line of its own.
    """
    tables = _with_class(shared_wizard.widget, "forge-out-table")
    assert tables
    for table in tables:
        assert table.layout.grid_template_columns is None
        cells = list(table.children)
        assert len(cells) % 5 == 0
        assert all("forge-out-x" in c._dom_classes for c in cells[4::5])
        assert not any("forge-out-x" in c._dom_classes for i, c in enumerate(cells) if i % 5 != 4)
    assert "grid-template-columns:" in _declarations_of(".forge-app .forge-out-table")
    narrow = _at_rule_block("@container (max-width: 720px)")
    assert ".forge-app .forge-out-table > .forge-out-x { grid-column: 2 / -1; }" in narrow
    assert ".forge-out-th.forge-out-x { display: none; }" in narrow


def test_variable_grids_drop_columns_in_narrower_cards(shared_wizard):
    """Four columns ellipsised labels ("Sea surface hei…") in a narrow card: the
    stylesheet sets four, three and two by card width, so no grid sets its own.
    """
    grids = _with_class(shared_wizard.widget, "forge-var-grid")
    assert grids
    assert all(g.layout.grid_template_columns is None for g in grids)
    assert "repeat(4, minmax(0, 1fr))" in _declarations_of(".forge-app .forge-var-grid")
    assert "repeat(3, minmax(0, 1fr))" in _at_rule_block("@container (max-width: 1150px)")
    assert ".forge-var-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }" in (
        _at_rule_block("@container (max-width: 760px)")
    )


def test_cdr_plot_moves_below_the_release_panels_in_a_narrow_card(shared_wizard):
    """Like the Grid card's preview, the CDR plot column wraps below the panels."""
    rows = _with_class(shared_wizard.widget, "forge-cdr-row")
    assert len(rows) == 1
    assert rows[0].children[1] is shared_wizard.cdr_plot_box
    assert ".forge-app .forge-cdr-row { flex-wrap: wrap" in _at_rule_block(
        "@container (max-width: 1000px)"
    ).replace(".forge-app .forge-geo-row, ", "")


def test_nesting_checkboxes_size_to_their_labels(shared_wizard):
    """The default 300px cut "This grid is a parent (enter child grid info below)." off."""
    assert shared_wizard.nest_enable.layout.width == "auto"
    assert shared_wizard.parent_enable.layout.width == "auto"


def _declarations_of(selector: str) -> str:
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", components.WIZARD_CSS)
    assert match, f"no rule for {selector}"
    return match.group(1)

