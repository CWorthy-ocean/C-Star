"""Tests for the wizard's reusable ipywidgets building blocks (cstar.wizard.ui.components)."""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

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


def _declarations(selector: str) -> str:
    """The declaration block of the ``WIZARD_CSS`` rule for ``selector``."""
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", C.WIZARD_CSS)
    assert match, f"no rule for {selector}"
    return match.group(1)


def test_no_wrap_status_lines_do_not_set_the_page_width():
    """The sticky bar's run summary and the catalog bar's status are no-wrap
    lines; without inline-size containment their text set the whole page's
    minimum width (~1450px), so narrower windows scrolled sideways.
    """
    for selector in (
        ".forge-app .forge-sticky > .widget-html:first-child",
        ".forge-app .forge-catalog-bar > .widget-html:last-child",
    ):
        assert "contain: inline-size;" in _declarations(selector)
    assert "min-width: 0;" in _declarations(".forge-app .forge-stack > .widget-box")


def test_style_widget_wraps_css_in_style_tag():
    widget = C.style_widget(W)
    assert widget.value.startswith("<style ")
    assert C.WIZARD_CSS in widget.value


def test_number_boxes_have_no_arrows_and_ignore_the_mouse_wheel():
    assert "input[type=number]::-webkit-inner-spin-button" in C.WIZARD_CSS
    html = C.style_widget(W).value
    assert f'<style onload="{C._NO_WHEEL_JS}">' in html
    assert "'wheel'" in C._NO_WHEEL_JS and "blur()" in C._NO_WHEEL_JS
    assert '"' not in C._NO_WHEEL_JS  # it sits inside a double-quoted attribute


def test_mouse_wheel_guard_is_scoped_to_the_wizard_root():
    # The listener sits on `document`; without this check it would blur every
    # number box in a JupyterLab tab, not just the wizard's.
    assert "closest('.forge-app')" in C._NO_WHEEL_JS


def test_chip_contains_kind_class_and_text():
    html = C.chip("Required", "req")
    assert "forge-chip req" in html
    assert "Required" in html


def test_chip_title_becomes_a_tooltip():
    assert "title='why'" in C.chip("Experimental", "warn", title="why")
    assert "title=" not in C.chip("Required", "req")


def test_banner_contains_kind_class_and_html():
    html = C.banner("warn", "watch out")
    assert "forge-banner warn" in html
    assert "watch out" in html


class _RootChildren(HTMLParser):
    """Collects what sits directly inside the first element: child tags and text."""

    _VOID = frozenset({"br", "hr", "img", "input", "wbr"})

    def __init__(self) -> None:
        super().__init__()
        self.depth = 0
        self.children: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if self.depth == 1:
            self.children.append(f"<{tag}>")
        if tag not in self._VOID:
            self.depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag not in self._VOID:  # ``<br/>`` reports an end tag too
            self.depth -= 1

    def handle_data(self, data: str) -> None:
        if self.depth == 1 and data.strip():
            self.children.append(data)


def test_banner_content_is_one_flex_item():
    # .forge-banner is a flex box: text, <b> and <br> left loose inside it would
    # each be a flex item, laid out side by side as columns
    parser = _RootChildren()
    parser.feed(C.banner("info", "Defaults come from <b>x</b>. Expand<br>a section."))
    assert parser.children == ["<div>"]


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


def test_field_row_experimental_chip_only_for_flagged_field():
    flagged = C.field_row(W, "auto_tiling_chk", W.Checkbox())
    plain = C.field_row(W, "use_pio_chk", W.Checkbox())
    assert C.EXPERIMENTAL_CHIP in flagged.children[0].value
    assert "Experimental" not in plain.children[0].value


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


def test_accordion_title_returns_parts_and_skips_empty_pills():
    title = C.accordion_title("Title", "GLORYS", "", C.Pill("default path", "off"))
    assert title == C.PaneTitle(
        "Title", (C.Pill("GLORYS"), C.Pill("default path", "off"))
    )
    assert str(title) == "Title · GLORYS · default path"
    assert str(C.accordion_title("Title", chip="Required")) == "Title · Required"


def test_pane_title_html_escapes_and_marks_pill_kinds():
    """Header markup: escaped title (workplan step names are user input), one
    span per pill carrying its kind, and the quiet Required/Optional text.
    """
    markup = C.accordion_title(
        "<b>step</b>", C.Pill("1 modified", "mod"), chip="Optional"
    ).html()
    assert "&lt;b&gt;step&lt;/b&gt;" in markup
    assert "<b>" not in markup
    assert "<span class='forge-pill mod'>1 modified</span>" in markup
    assert "<span class='forge-coll-req'>Optional</span>" in markup


def test_collapsible_toggles_its_body_and_keeps_the_body_layout():
    body = W.HTML("body")
    pane = C.collapsible(W, body, C.accordion_title("T", "v"))
    assert pane.body is body
    assert pane.selected_index is None
    assert pane._body_box.layout.display == "none"
    pane._hit.value = True  # a click on the header
    assert pane.selected_index == 0
    assert pane._body_box.layout.display == ""
    assert "forge-coll-open" in pane._dom_classes
    pane.selected_index = None
    assert pane._hit.value is False
    assert "forge-coll-open" not in pane._dom_classes
    assert body.layout.display is None  # never touched


def test_collapsible_set_title_skips_an_unchanged_title():
    pane = C.collapsible(W, W.HTML("x"), "A")
    before = pane._head_html.value
    pane._head_html.value = "sentinel"
    pane.set_title("A")
    assert pane._head_html.value == "sentinel"
    pane.set_title(C.accordion_title("A", "b"))
    assert pane._head_html.value != before
    assert pane._hit.description == pane.get_title() == "A · b"


def test_open_accordion_panes_are_independent_and_retitleable():
    """open_accordion: one collapsible per pane, all may stay open, and
    set_title/get_title address panes by index.
    """
    box = C.open_accordion(W, [W.HTML("a"), W.HTML("b")], ["A", "B"])
    assert "forge-open-acc" in box._dom_classes
    assert len(box.panes) == 2
    box.panes[0].selected_index = 0
    box.panes[1].selected_index = 0
    assert (box.panes[0].selected_index, box.panes[1].selected_index) == (0, 0)
    box.set_title(1, C.accordion_title("B", "summary"))
    assert box.get_title(1) == "B · summary"
    assert box.get_title(0) == "A"


def test_open_accordion_rejects_mismatched_panes_and_titles():
    with pytest.raises(ValueError, match="2 panes but 1 titles"):
        C.open_accordion(W, [W.HTML("a"), W.HTML("b")], ["A"])


def _at_rule_block(prelude: str, css: str = C.WIZARD_CSS) -> str:
    """The bodies of every at-rule in ``css`` that starts with ``prelude``."""
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


def _rule(selector: str, css: str) -> str:
    """The declaration block of the rule for ``selector`` in ``css``."""
    match = re.search(r"(?:^|\n)\s*" + re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert match, f"no rule for {selector}"
    return match.group(1)


def test_wizard_follows_the_host_theme():
    """``.forge-app`` declares the palette and the host colours ipywidgets' own
    controls read (re-declaring the ``--jp-widgets-*`` colours ipywidgets resolves
    on ``:root``): the light values by default, the dark ones in a dark
    JupyterLab and in Voila when the browser prefers dark.
    """
    table = {**C.branding.PALETTE, **C._HOST_COLORS}
    light = _rule(".forge-app", C.WIZARD_CSS)
    dark_lab = _rule('body[data-jp-theme-light="false"] :is(.forge-app)', C.WIZARD_CSS)
    dark_voila = _rule(
        "body[data-voila] :is(.forge-app)",
        _at_rule_block("@media (prefers-color-scheme: dark)"),
    )
    assert "color-scheme: light;" in light
    for block in (dark_lab, dark_voila):
        assert "color-scheme: dark;" in block
    for name, (light_value, dark_value) in table.items():
        assert f"{name}: {light_value};" in light
        for dark in (dark_lab, dark_voila):
            # a value the same in both themes comes from the light block alone
            assert (f"{name}:" in dark) == (dark_value != light_value)
            if dark_value != light_value:
                assert f"{name}: {dark_value};" in dark
    for name in (
        "--jp-widgets-label-color",
        "--jp-widgets-input-color",
        "--jp-widgets-input-background-color",
        "--jp-content-font-color1",
        "--jp-content-link-color",
        "--jp-layout-color1",
    ):
        assert name in C._HOST_COLORS


def test_light_values_are_the_brand_palette():
    """Light looks as it did before the dark theme: the tokens' light values are the
    brand constants, and the ``--jp-*`` light values JupyterLab 4's light theme's.
    """
    B = C.branding
    for name, constant in {
        "--forge-text": B.SOFT_BLACK,
        "--forge-red": B.BRAND_RED,
        "--forge-blue": B.BLUE,
        "--forge-navy": B.NAVY,
        "--forge-page": B.GREY,
        "--forge-line": B.LINE,
        "--forge-tint-blue": B.TINT_BLUE,
    }.items():
        assert B.PALETTE[name][0] == constant
    assert C._HOST_COLORS["--jp-layout-color1"] == ("#fff", "#212121")


def test_only_voila_follows_the_browsers_dark_preference():
    """JupyterLab marks its own theme on ``body``: its light theme stays light on a
    dark OS, so every rule under the dark media query is scoped to Voila.
    """
    for css in (C.WIZARD_CSS, C.branding.header_css()):
        block = _at_rule_block("@media (prefers-color-scheme: dark)", css)
        selectors = re.findall(r"([^{}]+)\{", block)
        assert selectors
        for selector in selectors:
            assert selector.strip().startswith("body[data-voila]"), selector


def test_voila_page_background_follows_the_theme():
    page_light, page_dark = C.branding.PALETTE["--forge-page"]
    assert f"background: {page_light};" in _rule("body[data-voila]", C.WIZARD_CSS)
    media = _at_rule_block("@media (prefers-color-scheme: dark)")
    assert f"background: {page_dark};" in _rule("body[data-voila]", media)
    assert f"background: {page_dark};" in _rule(
        'body[data-voila][data-jp-theme-light="false"]', C.WIZARD_CSS
    )


def test_header_declares_its_own_palette():
    """``branding_display()`` can show the header without the wizard's stylesheet,
    so its CSS declares the palette tokens it reads, in both themes.
    """
    header = C.branding.header_css()
    assert C.branding.themed_rule(".cworthy-forge-header", C.branding.PALETTE) in header
    assert "var(--forge-red)" in header


def _has_colour_literal(value: str) -> bool:
    """Whether a CSS value names a colour itself (a ``var(--forge-…)`` does not)."""
    return bool(_COLOUR_LITERAL.search(re.sub(r"var\([^)]*\)", "", value)))


_COLOUR_LITERAL = re.compile(
    r"#[0-9a-fA-F]{3,8}\b|\brgba?\(|\b(?:white|black|red|green|blue|gr[ae]y|orange"
    r"|yellow|purple|navy|silver|maroon|teal|crimson|darkred|lightgr[ae]y)\b",
    re.IGNORECASE,
)


@pytest.mark.parametrize("name", ["WIZARD_CSS", "header_css"])
def test_stylesheet_colours_come_from_tokens(name):
    """Every colour in the stylesheets is a ``var(--forge-…)`` token, so it follows
    the theme; literal colours appear only in the custom-property declarations
    (and in Voila's page background, outside the wizard's root).
    """
    css = C.WIZARD_CSS if name == "WIZARD_CSS" else C.branding.header_css()
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    for selector, block in re.findall(r"([^{};]+)\{([^{}]*)\}", css):
        for declaration in block.split(";"):
            prop, _, value = declaration.partition(":")
            if prop.strip().startswith("--") or not _has_colour_literal(value):
                continue
            assert selector.strip().startswith("body[data-voila]"), (
                f"{selector.strip()} {{ {declaration.strip()} }}"
            )
            assert value.strip() in C.branding.PALETTE["--forge-page"]


def test_status_span_colours_follow_the_theme():
    assert C.status_span("err", "bad") == "<span class='forge-st-err'>bad</span>"
    for kind in ("err", "ok", "warn", "note"):
        assert f".forge-app .{C.status_class(kind)} {{" in C.WIZARD_CSS


#: CSS properties and ipywidgets style traits that carry a colour.
_COLOUR_SETTING = re.compile(
    r"\b(?:color|background(?:-color)?|border(?:-[a-z]+)?|fill|stroke|outline(?:-color)?"
    r"|button_color|text_color|bar_color|handle_color)\s*[:=]\s*['\"]?([^'\";}>)]*)",
)


def test_wizard_modules_write_no_colours_of_their_own():
    """A colour written into Python-built HTML, SVG or a ``W.Layout`` stays light
    on a dark page; it belongs in the stylesheet (a class plus a palette token).
    """
    root = Path(C.__file__).resolve().parents[1]
    stylesheets = {root / "ui" / "components.py", root / "ui" / "branding.py"}
    found = []
    for path in sorted(root.rglob("*.py")):
        if path in stylesheets:
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            for value in _COLOUR_SETTING.findall(line):
                if _has_colour_literal(value):
                    found.append(f"{path.relative_to(root)}:{number}: {line.strip()}")
    assert not found, "\n".join(found)


def test_sticky_step_links_wrap_instead_of_hiding():
    """In a narrow window the step links wrap onto another line (they used to be
    cut off past "6 Advanced"); the run summary takes what room is left and
    ellipsises, and goes when there is too little room to read it.
    """
    assert "flex-wrap: wrap;" in _declarations(
        ".forge-app .forge-sticky .widget-html-content"
    )
    summary = _declarations(".forge-app .forge-sticky .summary")
    assert "flex: 1 1 0;" in summary
    assert "text-overflow: ellipsis;" in summary
    assert "container: forge-sticky / inline-size;" in _declarations(
        ".forge-app .forge-sticky > .widget-html:first-child"
    )
    assert ".forge-app.forge-wizard .forge-sticky .summary { display: none; }" in (
        _at_rule_block("@container forge-sticky (max-width: 900px)")
    )


def test_pinned_top_preview_docks_below_a_wrapped_sticky_bar():
    """Below ~820px the Workplan page's bar may take two lines, so a pinned top
    preview docks below that height instead of sliding under the bar.
    """
    block = _at_rule_block("@media (max-width: 820px)")
    assert f"top: {C.STICKY_BAR_HEIGHT_WRAPPED};" in block
    assert "body[data-voila] .forge-app .forge-side-top.forge-pinned" in block
