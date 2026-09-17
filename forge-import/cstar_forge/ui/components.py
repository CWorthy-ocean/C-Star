"""Reusable ipywidgets building blocks for the redesigned wizard UI.

Every helper that builds a widget takes the ``ipywidgets`` module as its
first parameter, ``W`` -- the same lazy-import idiom
``_make_field_widget(W, ...)`` uses in :mod:`cstar_forge.forge_blueprint_wizard`
-- so this module itself never imports ``ipywidgets`` at module scope (it is
an optional dependency of the package).

:data:`WIZARD_CSS` is the single stylesheet for the redesigned wizard,
scoped entirely under the ``.forge-app`` shell root class so it never leaks
into the host JupyterLab/Voila page. It is built from the palette tokens in
:mod:`cstar_forge.ui.branding`.
"""

from __future__ import annotations

from typing import Any

from cstar_forge.ui import branding
from cstar_forge.ui.labels import label_for, section_for

#: The wizard's stylesheet (no leading/trailing ``<style>`` tags -- wrap with
#: :func:`style_widget` or ``f"<style>{WIZARD_CSS}</style>"`` yourself).
WIZARD_CSS = f"""
/* page background only in Voila (body[data-voila]); JupyterLab keeps its theme */
body[data-voila] {{ background: {branding.GREY}; }}
/* the page column: shell root only -- the wizard root also carries .forge-app
   (so the notebook path is styled) and must not get its own max-width/padding */
body[data-voila] .forge-shell {{ max-width: 1180px; margin: 0 auto; padding: 0 24px; }}

.forge-app, .forge-app .widget-label, .forge-app .widget-html-content,
.forge-app .jupyter-button, .forge-app input, .forge-app select,
.forge-app textarea {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    color: {branding.SOFT_BLACK};
}}

/* section card */
.forge-app .forge-card {{
    background: #fff;
    border: 1px solid {branding.LINE};
    border-radius: 6px;
    margin: 10px 0;
    overflow: visible;
}}
/* the header ROW (an HBox: the header HTML widget + an optional live-status
   chips widget) carries the padding/hairline; the HTML widget's own class
   only targets its rendered content below, since ipywidgets renders a
   W.HTML's value inside a child div.widget-html-content -- flex rules on the
   widget's own node do nothing for that inner markup. */
.forge-app .forge-card-hrow {{
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 18px;
    border-bottom: 1px solid {branding.LINE_2};
}}
.forge-app .forge-card-h .widget-html-content {{
    display: flex;
    align-items: center;
    gap: 12px;
}}
.forge-app .forge-card-b {{ padding: 14px 18px 16px; }}
.forge-app .forge-card .num {{
    width: 26px;
    height: 26px;
    border-radius: 50%;
    background: {branding.SOFT_BLACK};
    color: #fff;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 12.5px;
    font-weight: 700;
}}
.forge-app .forge-card .ttl {{ font-size: 15.5px; font-weight: 600; }}
.forge-app .forge-card .desc {{ font-size: 12.5px; color: {branding.INK_2}; }}
/* generic flex-spacer marker, reused inside the card header, sticky bar, etc. */
.forge-app .sp {{ flex: 1 1 auto; }}

/* chips */
.forge-app .forge-chip {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 22px;
    padding: 0 9px;
    border-radius: 11px;
    font-size: 11.5px;
    font-weight: 600;
    white-space: nowrap;
}}
.forge-app .forge-chip.req {{ background: {branding.TINT_RED}; color: {branding.BRAND_RED}; }}
.forge-app .forge-chip.opt {{ background: {branding.LINE_2}; color: {branding.INK_2}; }}
.forge-app .forge-chip.ok {{ background: {branding.TINT_GREEN}; color: {branding.GREEN}; }}
.forge-app .forge-chip.warn {{ background: {branding.TINT_AMBER}; color: {branding.AMBER}; }}
.forge-app .forge-chip.info {{ background: {branding.TINT_BLUE}; color: {branding.BLUE}; }}
.forge-app .forge-chip.def {{ background: {branding.LINE_2}; color: {branding.SOFT_BLACK}; }}

/* banners */
.forge-app .forge-banner {{
    display: flex;
    gap: 10px;
    padding: 9px 12px;
    border-radius: 4px;
    border: 1px solid {branding.LIGHT_BLUE};
    background: {branding.TINT_BLUE};
    font-size: 13px;
}}
.forge-app .forge-banner.info {{ border-color: {branding.LIGHT_BLUE}; background: {branding.TINT_BLUE}; }}
.forge-app .forge-banner.warn {{ border-color: {branding.AMBER}; background: {branding.TINT_AMBER}; }}
.forge-app .forge-banner.err {{ border-color: {branding.BRAND_RED}; background: {branding.TINT_RED}; }}
.forge-app .forge-banner.ok {{ border-color: {branding.GREEN}; background: {branding.TINT_GREEN}; }}
.forge-app .forge-banner-w .widget-html-content {{ display: block; }}

/* field rows: label column is an HTML widget, the input itself has description='' */
.forge-app .forge-field .flab .widget-html-content {{ padding-top: 5px; line-height: 1.25; }}
.forge-app .forge-field .w {{ font-size: 13px; font-weight: 500; }}
.forge-app .forge-field .w .req {{ color: {branding.BRAND_RED}; font-weight: 700; margin-left: 4px; }}
.forge-app .forge-field .sym {{
    font-family: Menlo, Consolas, monospace;
    font-size: 11.5px;
    color: {branding.INK_3};
}}
.forge-app .forge-hint {{
    font-size: 12px;
    color: {branding.INK_2};
    line-height: 1.35;
    max-width: 520px;
}}
.forge-app .forge-unit {{ font-size: 12.5px; color: {branding.INK_2}; padding-top: 6px; }}
.forge-app .forge-msg-err {{ font-size: 12.5px; color: {branding.BRAND_RED}; }}
.forge-app .forge-msg-ok {{ font-size: 12.5px; color: {branding.GREEN}; }}
.forge-app .forge-msg-warn {{ font-size: 12.5px; color: {branding.AMBER}; }}
.forge-app .widget-text input, .forge-app .widget-dropdown select {{
    border: 1px solid {branding.LINE};
    border-radius: 4px;
}}
.forge-app .widget-text input:focus, .forge-app .widget-dropdown select:focus {{
    border-color: {branding.BLUE};
    box-shadow: 0 0 0 2px {branding.LIGHT_BLUE};
    outline: none;
}}

/* Advanced-settings section sub-header (namelist section name kept as symbol) */
.forge-app .forge-settings-sec {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 8px 0 2px;
}}
.forge-app .forge-settings-sec .ttl {{ font-size: 13px; font-weight: 600; }}
.forge-app .forge-settings-sec .sym {{
    font-family: Menlo, Consolas, monospace;
    font-size: 11.5px;
    color: {branding.INK_3};
}}

/* subsection header */
.forge-app .forge-sub {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 14px 0 8px;
    padding-bottom: 4px;
    border-bottom: 1px solid {branding.LINE_2};
}}
.forge-app .forge-sub .widget-html-content {{ display: flex; align-items: center; gap: 10px; }}
.forge-app .forge-sub-ttl {{ font-size: 13.5px; font-weight: 600; }}
.forge-app .forge-sub-sym {{
    font-family: Menlo, Consolas, monospace;
    font-size: 11.5px;
    color: {branding.INK_3};
}}

/* sticky bar (sticks in Voila's page scroll; harmless no-op inside a Lab cell).
   No negative margins here: ipywidgets boxes scroll their overflow, so a bar
   wider than the content column shows up as a horizontal scrollbar. */
.forge-app .forge-sticky {{
    position: sticky;
    top: 0;
    z-index: 5;
    background: #fff;
    border-bottom: 1px solid {branding.LINE};
    margin: 0;
    padding: 8px 0;
    display: flex;
    align-items: center;
    gap: 12px;
}}
/* the sticky bar's own HTML content (step links, run summary, spacer, status
   chip) and the download link both render inside a child
   div.widget-html-content -- flex/nowrap must target that, not the widget's
   own node (see the card header comment above for why). */
.forge-app .forge-sticky .widget-html-content {{
    display: flex;
    align-items: center;
    gap: 10px;
    white-space: nowrap;
}}
.forge-app .forge-sticky a.step {{
    display: inline-flex;
    align-items: center;
    height: 26px;
    padding: 0 10px;
    border-radius: 13px;
    font-size: 12.5px;
    font-weight: 600;
    color: {branding.INK_2};
    text-decoration: none;
    white-space: nowrap;
}}
.forge-app .forge-sticky a.step:hover {{ background: {branding.TINT_BLUE}; }}
.forge-app .forge-sticky .summary {{ white-space: nowrap; }}
/* the status HTML shrinks (and ellipsises) before the download button does */
.forge-app .forge-sticky > .widget-html:first-child {{ flex: 1 1 0; min-width: 0; }}
.forge-app .forge-sticky > .widget-html:first-child .widget-html-content {{
    overflow: hidden;
    text-overflow: ellipsis;
}}
.forge-app .forge-sticky > .widget-html:last-child {{ flex: 0 0 auto; }}

/* catalog-location bar */
.forge-app .forge-catalog-bar {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0;
}}
.forge-app .forge-catalog-bar .widget-html-content {{
    font-size: 12px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.forge-app .forge-catalog-bar > .widget-html {{ min-width: 0; }}

/* buttons: neutral by default, brand-blue primary, red-outline danger.
   width:auto lets captions like "Compute from CFL" size themselves instead of
   truncating at ipywidgets' fixed default button width. */
.forge-app .jupyter-button {{
    border-radius: 4px;
    font-weight: 600;
    border: 1px solid {branding.LINE};
    background: #fff;
    color: {branding.SOFT_BLACK};
    width: auto;
    min-width: 0;
    padding: 0 12px;
}}
.forge-app .jupyter-button.mod-primary {{
    background: {branding.BLUE};
    border-color: {branding.BLUE};
    color: #fff;
}}
.forge-app .jupyter-button.mod-danger {{
    background: #fff;
    border-color: {branding.BRAND_RED};
    color: {branding.BRAND_RED};
}}

/* the sticky bar's download link, styled like a mod-primary button */
.forge-app .forge-dl-btn {{
    display: inline-flex;
    align-items: center;
    height: 32px;
    padding: 0 12px;
    border-radius: 4px;
    background: {branding.BLUE};
    color: #fff;
    font-weight: 600;
    text-decoration: none;
}}
.forge-app .forge-dl-btn code {{ color: #fff; }}

/* accordion restyle */
.forge-app .jupyter-widget-Collapse-header, .forge-app .p-Collapse-header {{
    background: #FAFAFB;
    font-weight: 600;
    font-size: 13px;
    border: 1px solid {branding.LINE};
    border-radius: 4px;
    padding: 8px 12px;
}}

/* tab bar: current-tab highlight */
.forge-app .lm-TabBar-tab.lm-mod-current, .forge-app .p-TabBar-tab.p-mod-current {{
    border-bottom: 2px solid {branding.BLUE};
    color: {branding.BLUE};
    font-weight: 600;
}}

/* left-aligned vertical rail nav (shell) */
.forge-app .forge-rail .widget-toggle-button {{
    width: 220px;
    text-align: left;
    justify-content: flex-start;
    border: 0;
    background: transparent;
    border-radius: 6px;
    font-size: 13px;
}}
.forge-app .forge-rail .widget-toggle-button.mod-active {{
    background: {branding.TINT_BLUE};
    color: {branding.BLUE};
    font-weight: 600;
    box-shadow: none;
}}

/* Advanced-settings output-stream table (grouped write/period/records rows).
   Header cells share one fixed height so their underline forms a single
   guideline (the empty corner cells carry &nbsp; and the same rule). */
.forge-app .forge-out-table {{ margin: 6px 0 12px; }}
.forge-app .forge-out-th {{ align-self: end; }}
.forge-out-th .widget-html-content {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .05em;
    color: {branding.INK_2};
    height: 22px;
    line-height: 18px;
    padding-bottom: 4px;
    border-bottom: 1px solid {branding.LINE};
    box-sizing: border-box;
}}
.forge-out-center .widget-html-content {{ text-align: center; }}
.forge-app .forge-out-write-stack .widget-checkbox {{ width: auto; margin: 0; }}
.forge-app .forge-out-table .widget-checkbox.forge-out-center {{
    width: auto;
    justify-content: center;
    margin: 0;
    padding: 0;
}}
/* bold divider between namelist sections inside one Advanced pane; the pane's
   first header has none (widget-level class set in _SettingsEditor) */
.forge-app .forge-settings-sec-w {{
    border-top: 2px solid {branding.BLUE};
    margin-top: 18px;
    padding-top: 12px;
}}
.forge-app .forge-settings-sec-w:first-child {{ border-top: 0; margin-top: 0; padding-top: 0; }}
/* wrapped forcing rows: breathing room and a hairline between rows */
.forge-app .forge-frow {{
    padding: 8px 0 10px;
    row-gap: 6px;
    border-bottom: 1px solid {branding.LINE_2};
}}
.forge-app .forge-frow:last-of-type {{ border-bottom: 0; }}
.forge-out-label .widget-html-content {{ font-size: 13px; }}
.forge-out-label .sym {{
    display: block;
    font-family: Menlo, Consolas, monospace;
    font-size: 11px;
    color: {branding.INK_3};
}}
.forge-var-title {{ font-size: 12.5px; font-weight: 600; margin: 10px 0 4px; }}
.forge-app .forge-var-grid .widget-checkbox {{ width: auto; }}
"""


def style_widget(W: Any) -> Any:
    """A ``W.HTML`` widget carrying :data:`WIZARD_CSS` in a ``<style>`` tag."""
    widget = W.HTML(f"<style>{WIZARD_CSS}</style>")
    widget.add_class("forge-style")
    return widget


def chip(text: str, kind: str) -> str:
    """An inline ``<span class='forge-chip {kind}'>`` HTML chip."""
    return f"<span class='forge-chip {kind}'>{text}</span>"


def banner(kind: str, html: str) -> str:
    """A ``<div class='forge-banner {kind}'>`` HTML banner."""
    return f"<div class='forge-banner {kind}'>{html}</div>"


def banner_widget(W: Any, kind: str, html: str) -> Any:
    """A ``W.HTML`` widget wrapping :func:`banner`, carrying class ``forge-banner-w``."""
    widget = W.HTML(banner(kind, html))
    widget.add_class("forge-banner-w")
    return widget


def _field_label_html(label) -> str:
    """The label-column HTML for one :func:`field_row` (word + symbol)."""
    req = "<span class='req'>*</span>" if label.required else ""
    html = f"<div class='w'>{label.label}{req}</div>"
    if label.symbol:
        html += f"<div class='sym'>{label.symbol}</div>"
    return html


def field_row(
    W: Any,
    key: str,
    widget: Any,
    *,
    default: str | None = None,
    default_hint: str | None = None,
    width: str | None = None,
    extra: tuple = (),
    label_width: str = "200px",
    page: str = "blueprint-wizard",
) -> Any:
    """A labeled field row: a two-column ``W.GridBox`` (label | widget + hint).

    ``label_width`` sets the label column (default 200px; dense grids such as
    the grid-geometry block pass a narrower value).

    ``widget.description`` is cleared (the label column replaces it). The
    label, symbol, unit, and required marker come from
    :func:`cstar_forge.ui.labels.label_for`; ``hint`` falls back to
    ``default_hint`` when the glossary has none. ``widget.layout.display`` is
    mirrored onto the row's own layout (both now and on every future change),
    so existing code that hides ``widget`` (``widget.layout.display =
    "none"``) hides the whole row, including the label.
    """
    label = label_for(key, default, page=page)

    if hasattr(widget, "description"):
        widget.description = ""
    if width is not None:
        widget.layout.width = width

    label_widget = W.HTML(_field_label_html(label))
    label_widget.add_class("flab")

    input_row = [widget]
    if label.unit:
        input_row.append(W.HTML(f"<span class='forge-unit'>{label.unit}</span>"))
    input_row.extend(extra)
    right_children = [W.HBox(input_row)]
    hint_text = label.hint or default_hint
    if hint_text:
        right_children.append(W.HTML(f"<div class='forge-hint'>{hint_text}</div>"))

    row = W.GridBox(
        [label_widget, W.VBox(right_children)],
        layout=W.Layout(
            grid_template_columns=f"{label_width} 1fr",
            grid_gap="2px 16px",
            margin="0 0 10px 0",
        ),
    )
    row.add_class("forge-field")
    row.forge_key = key
    row.forge_widget = widget

    def _mirror_display(change: dict) -> None:
        row.layout.display = change["new"]

    widget.layout.observe(_mirror_display, names="display")
    row.layout.display = widget.layout.display

    return row


def field_grid(W: Any, rows: list, cols: int = 2) -> Any:
    """A ``W.GridBox`` laying out ``rows`` (e.g. :func:`field_row` results) in ``cols`` columns."""
    return W.GridBox(
        rows,
        layout=W.Layout(
            grid_template_columns=f"repeat({cols}, minmax(0, 1fr))",
            grid_gap="0 28px",
        ),
    )


def subsection(
    W: Any,
    key: str,
    *children: Any,
    default_title: str = "",
    symbols: str | None = None,
    trailing: Any = None,
    page: str = "blueprint-wizard",
) -> Any:
    """A subsection header (``forge-sub``, from :func:`~cstar_forge.ui.labels.section_for`) plus ``children``."""
    section = section_for(key, default_title, page=page)
    header_html = f"<span class='forge-sub-ttl'>{section.title}</span>"
    if symbols:
        header_html += f"<span class='forge-sub-sym'>{symbols}</span>"
    header_widget = W.HTML(header_html)
    header_row = (
        W.HBox([header_widget, trailing]) if trailing is not None else header_widget
    )
    header_row.add_class("forge-sub")
    return W.VBox([header_row, *children])


def card(
    W: Any,
    key: str,
    *children: Any,
    num: int | None = None,
    chips_widget: Any = None,
    default_title: str = "",
    anchor: bool = True,
    required_chip: bool = True,
    page: str = "blueprint-wizard",
) -> Any:
    """A section card (``forge-card``): numbered header (title/desc/Required-or-Optional chip) plus a body.

    ``chips_widget``, when given, is an extra ``W.HTML`` the caller can keep
    updating (e.g. a live validation-status chip) shown alongside the header.
    ``required_chip`` omits the Required/Optional chip entirely when false --
    for a card that is neither (e.g. Review, which is a destination rather
    than an optional step). Returns a ``W.VBox`` carrying ``forge_key``,
    ``forge_title``, and ``forge_header`` (the header ``W.HTML`` widget,
    independent of ``chips_widget``) for callers/tests to inspect.
    """
    section = section_for(key, default_title, page=page)

    header_parts = []
    if num is not None:
        header_parts.append(f"<span class='num'>{num}</span>")
    header_parts.append(f"<div><div class='ttl'>{section.title}</div>")
    header_parts.append(f"<div class='desc'>{section.desc}</div></div>")
    header_parts.append("<span class='sp'></span>")
    if required_chip:
        header_parts.append(
            chip("Required", "req") if section.required else chip("Optional", "opt")
        )
    header_widget = W.HTML("".join(header_parts))
    header_widget.add_class("forge-card-h")
    # The header ROW (not the HTML widget itself) carries the padding/hairline
    # and the flex split with `chips_widget` -- see the WIZARD_CSS comment on
    # `.forge-card-hrow` for why this can't live on the widget's own class.
    header_widget.layout.flex = "1 1 auto"
    header_children = [header_widget]
    if chips_widget is not None:
        chips_widget.layout.flex = "0 0 auto"
        header_children.append(chips_widget)
    header_row = W.HBox(header_children)
    header_row.layout.align_items = "center"
    header_row.add_class("forge-card-hrow")

    body = W.VBox(list(children))
    body.add_class("forge-card-b")

    box_children = []
    if anchor:
        box_children.append(W.HTML(f"<a id='forge-sec-{key}'></a>"))
    box_children.append(header_row)
    box_children.append(body)

    box = W.VBox(box_children)
    box.add_class("forge-card")
    box.forge_key = key
    box.forge_title = section.title
    box.forge_header = header_widget
    return box


def accordion_title(title: str, summary: str = "", chip_text: str = "") -> str:
    """Join ``title``/``summary``/``chip_text`` with " · " separators, skipping empties."""
    parts = [p for p in (title, summary, chip_text) if p]
    return "   ·   ".join(parts)


def open_accordion(W: Any, panes: list, titles: list[str]) -> Any:
    """A stack of independently collapsible panes (class ``forge-open-acc``).

    ``W.Accordion`` allows one open pane at a time, so opening a second pane
    collapses the first and the page jumps. This returns a ``W.VBox`` of
    single-pane ``W.Accordion`` widgets instead -- any number may stay open --
    while keeping the ``set_title(i, title)`` / ``get_title(i)`` surface callers
    use to refresh titles. ``box.panes`` lists the inner accordions.
    """
    if len(panes) != len(titles):
        raise ValueError(f"open_accordion: {len(panes)} panes but {len(titles)} titles")
    inner = []
    for pane, title in zip(panes, titles, strict=True):
        acc = W.Accordion(children=[pane], selected_index=None)
        acc.set_title(0, title)
        inner.append(acc)
    box = W.VBox(inner)
    box.add_class("forge-open-acc")
    box.panes = inner
    box.set_title = lambda i, title: inner[i].set_title(0, title)
    box.get_title = lambda i: inner[i].get_title(0)
    return box
