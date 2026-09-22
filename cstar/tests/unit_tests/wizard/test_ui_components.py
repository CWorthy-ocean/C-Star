"""Tests for the wizard's reusable ipywidgets building blocks (cstar.wizard.ui.components)."""

from __future__ import annotations

import pytest

pytest.importorskip("ipywidgets")

import ipywidgets as W

from cstar.wizard.ui import components as C


def test_wizard_css_contains_every_documented_class():
    expected_classes = [
        "forge-app",
        "forge-card",
        "forge-card-h",
        "forge-card-b",
        "forge-field",
        "forge-hint",
        "forge-unit",
        "forge-msg-err",
        "forge-msg-ok",
        "forge-msg-warn",
        "forge-sub",
        "forge-sticky",
        "forge-chip",
        "forge-banner",
        "forge-banner-w",
        "forge-rail",
    ]
    for cls in expected_classes:
        assert f".{cls}" in C.WIZARD_CSS, f"missing CSS for .{cls}"
    for kind in ("req", "opt", "ok", "warn", "info", "def"):
        assert f".forge-chip.{kind}" in C.WIZARD_CSS, f"missing chip kind {kind}"
    for kind in ("info", "warn", "err", "ok"):
        assert f".forge-banner.{kind}" in C.WIZARD_CSS, f"missing banner kind {kind}"


def test_wizard_css_has_no_unresolved_braces():
    # Every literal CSS brace must have been doubled and resolved by the
    # f-string; nothing should leak through unresolved.
    assert "{{" not in C.WIZARD_CSS
    assert "}}" not in C.WIZARD_CSS


def test_style_widget_wraps_css_in_style_tag():
    widget = C.style_widget(W)
    assert widget.value.startswith("<style>")
    assert C.WIZARD_CSS in widget.value


def test_chip_contains_kind_class_and_text():
    html = C.chip("Required", "req")
    assert "forge-chip req" in html
    assert "Required" in html


def test_banner_contains_kind_class_and_html():
    html = C.banner("warn", "watch out")
    assert "forge-banner warn" in html
    assert "watch out" in html


def test_banner_widget_has_marker_class():
    widget = C.banner_widget(W, "ok", "all good")
    assert "forge-banner-w" in widget._dom_classes
    assert "all good" in widget.value


def test_field_row_clears_description_and_sets_forge_attrs():
    widget = W.Text(description="should be cleared")
    row = C.field_row(W, "v_sponge", widget)
    assert widget.description == ""
    assert row.forge_key == "v_sponge"
    assert row.forge_widget is widget
    assert "forge-field" in row._dom_classes


def test_field_row_uses_glossary_label_symbol_unit_hint():
    widget = W.FloatText()
    row = C.field_row(W, "v_sponge", widget)
    label_html = row.children[0].value
    assert "Sponge viscosity" in label_html
    assert "v_sponge" in label_html
    input_hbox = row.children[1].children[0]
    unit_html = input_hbox.children[-1].value
    assert "m²/s" in unit_html
    hint_html = row.children[1].children[-1].value
    assert "sponge layer" in hint_html


def test_field_row_falls_back_to_default_hint():
    widget = W.Text()
    row = C.field_row(W, "totally_unknown_key", widget, default_hint="fallback hint")
    body = row.children[1]
    hint_widgets = [
        c for c in body.children if "fallback hint" in getattr(c, "value", "")
    ]
    assert hint_widgets


def test_field_row_mirrors_display_at_construction():
    widget = W.Text()
    widget.layout.display = "none"
    row = C.field_row(W, "start", widget)
    assert row.layout.display == "none"


def test_field_row_mirrors_display_on_change():
    widget = W.Text()
    row = C.field_row(W, "start", widget)
    assert row.layout.display != "none"
    widget.layout.display = "none"
    assert row.layout.display == "none"
    widget.layout.display = ""
    assert row.layout.display == ""


def test_field_row_required_marker_present_for_required_field():
    widget = W.Text()
    row = C.field_row(W, "grid_name", widget)
    assert "req" in row.children[0].value


def test_field_grid_sets_columns_and_gap():
    rows = [C.field_row(W, "start", W.Text()), C.field_row(W, "end", W.Text())]
    grid = C.field_grid(W, rows, cols=3)
    assert grid.layout.grid_template_columns == "repeat(3, minmax(0, 1fr))"
    assert grid.layout.grid_gap == "0 28px"
    assert list(grid.children) == rows


def test_subsection_header_contains_title():
    box = C.subsection(W, "grid.geometry", W.HTML("body"))
    header = box.children[0]
    assert "forge-sub" in header._dom_classes
    assert "Grid geometry" in header.value


def test_subsection_with_trailing_widget():
    trailing = W.Button(description="Go")
    box = C.subsection(W, "grid.geometry", W.HTML("body"), trailing=trailing)
    header = box.children[0]
    assert trailing in header.children


def test_card_header_has_title_desc_and_required_chip():
    box = C.card(W, "model", W.HTML("body"), num=1)
    header_html = box.forge_header.value
    assert "Model" in header_html
    assert "forge-chip req" in header_html
    assert "Required" in header_html


def test_card_header_has_optional_chip_for_non_required_section():
    box = C.card(W, "start", W.HTML("body"))
    header_html = box.forge_header.value
    assert "forge-chip opt" in header_html
    assert "Optional" in header_html


def test_card_stores_forge_key_and_title():
    box = C.card(W, "grid", W.HTML("body"))
    assert box.forge_key == "grid"
    assert box.forge_title == "Domain and grid"
    assert "forge-card" in box._dom_classes


def test_card_includes_anchor_by_default():
    box = C.card(W, "grid", W.HTML("body"))
    anchor_html = box.children[0].value
    assert "forge-sec-grid" in anchor_html


def test_card_can_omit_anchor():
    box = C.card(W, "grid", W.HTML("body"), anchor=False)
    # No anchor -> first child is the header row, not an <a> tag.
    first = box.children[0]
    assert getattr(first, "value", "").strip().startswith("<a") is False


def test_card_required_chip_false_omits_chip():
    box = C.card(W, "model", W.HTML("body"), required_chip=False)
    header_html = box.forge_header.value
    assert "forge-chip" not in header_html
    assert "Required" not in header_html
    assert "Optional" not in header_html


def test_card_appends_chips_widget_in_header_row():
    chips = W.HTML("<span>live status</span>")
    box = C.card(W, "grid", W.HTML("body"), chips_widget=chips)
    # header row becomes an HBox([header_widget, chips_widget]) when given.
    header_row = box.children[1]  # anchor at [0] by default
    assert chips in header_row.children


def test_accordion_title_joins_non_empty_parts():
    assert (
        C.accordion_title("Title", "summary", "chip")
        == "Title   ·   summary   ·   chip"
    )


def test_accordion_title_skips_empty_parts():
    assert C.accordion_title("Title") == "Title"
    assert C.accordion_title("Title", "", "chip") == "Title   ·   chip"


def test_open_accordion_panes_are_independent_and_retitleable():
    """open_accordion: one single-pane Accordion per pane, all may stay open,
    and set_title/get_title address panes by index like a plain Accordion.
    """
    box = C.open_accordion(W, [W.HTML("a"), W.HTML("b")], ["A", "B"])
    assert "forge-open-acc" in box._dom_classes
    assert len(box.panes) == 2
    box.panes[0].selected_index = 0
    box.panes[1].selected_index = 0
    assert (box.panes[0].selected_index, box.panes[1].selected_index) == (0, 0)
    box.set_title(1, "B · summary")
    assert box.get_title(1) == "B · summary"
    assert box.get_title(0) == "A"


def test_open_accordion_rejects_mismatched_panes_and_titles():
    with pytest.raises(ValueError, match="2 panes but 1 titles"):
        C.open_accordion(W, [W.HTML("a"), W.HTML("b")], ["A"])
