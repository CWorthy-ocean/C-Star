"""Reusable ipywidgets building blocks for the redesigned wizard UI.

Every helper that builds a widget takes the ``ipywidgets`` module as its
first parameter, ``W`` -- the same lazy-import idiom
``_make_field_widget(W, ...)`` uses in :mod:`cstar.wizard.wizard`
-- so this module itself never imports ``ipywidgets`` at module scope (it is
an optional dependency of the package).

:data:`WIZARD_CSS` is the single stylesheet for the redesigned wizard,
scoped entirely under the ``.forge-app`` shell root class so it never leaks
into the host JupyterLab/Voila page. It is built from the palette tokens in
:mod:`cstar.wizard.ui.branding`.
"""

from __future__ import annotations

import functools
import html
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal

from cstar.wizard.ui import branding
from cstar.wizard.ui.labels import label_for, section_for

if TYPE_CHECKING:
    from collections.abc import Sequence

#: The width of the page column in Voila.
SHELL_MAX_WIDTH = "1600px"
#: Height of the sticky status bar: where a pinned preview pane docks beneath it.
STICKY_BAR_HEIGHT = "56px"
#: The same once the bar's step links wrap onto a second line (one more 26px row).
STICKY_BAR_HEIGHT_WRAPPED = "82px"

#: The host colours ipywidgets' own controls read (input boxes, checkbox and radio
#: labels, the run log's output areas), at JupyterLab 4's light-theme values. The
#: wizard's palette assumes a light page, so ``.forge-app`` sets these and stays
#: light inside a dark JupyterLab theme too. ipywidgets declares the
#: ``--jp-widgets-*`` colours on ``:root`` from the theme colours, so they resolve
#: there; they are declared again here to resolve against the values above them.
_LIGHT_HOST_COLORS = {
    "--jp-layout-color0": "#fff",
    "--jp-layout-color1": "#fff",
    "--jp-layout-color2": "#eee",
    "--jp-layout-color3": "#bdbdbd",
    "--jp-layout-color4": "#757575",
    "--jp-ui-font-color0": "#000",
    "--jp-ui-font-color1": "rgba(0, 0, 0, 0.87)",
    "--jp-ui-font-color2": "rgba(0, 0, 0, 0.54)",
    "--jp-ui-font-color3": "rgba(0, 0, 0, 0.38)",
    "--jp-ui-inverse-font-color0": "#fff",
    "--jp-ui-inverse-font-color1": "#fff",
    "--jp-content-font-color0": "#000",
    "--jp-content-font-color1": "rgba(0, 0, 0, 0.87)",
    "--jp-content-font-color2": "rgba(0, 0, 0, 0.54)",
    "--jp-content-font-color3": "rgba(0, 0, 0, 0.38)",
    "--jp-content-link-color": "#0d47a1",
    "--jp-content-link-visited-color": "#7b1fa2",
    # the light theme leaves this unset, so links fall back to the link colour
    "--jp-content-link-hover-color": "var(--jp-content-link-color)",
    "--jp-border-color0": "#bdbdbd",
    "--jp-border-color1": "#bdbdbd",
    "--jp-border-color2": "#e0e0e0",
    "--jp-border-color3": "#eee",
    "--jp-brand-color0": "#0d47a1",
    "--jp-brand-color1": "#1976d2",
    "--jp-brand-color2": "#64b5f6",
    "--jp-error-color0": "#b71c1c",
    "--jp-error-color1": "#d32f2f",
    "--jp-warn-color0": "#e65100",
    "--jp-warn-color1": "#f57c00",
    "--jp-success-color0": "#1b5e20",
    "--jp-success-color1": "#388e3c",
    "--jp-info-color0": "#006064",
    "--jp-info-color1": "#0097a7",
    "--jp-rendermime-error-background": "#fdd",
    "--jp-widgets-color": "var(--jp-content-font-color1)",
    "--jp-widgets-label-color": "var(--jp-widgets-color)",
    "--jp-widgets-readout-color": "var(--jp-widgets-color)",
    "--jp-widgets-input-color": "var(--jp-ui-font-color1)",
    "--jp-widgets-input-background-color": "var(--jp-layout-color1)",
    "--jp-widgets-input-border-color": "var(--jp-border-color1)",
    "--jp-widgets-input-focus-border-color": "var(--jp-brand-color2)",
    "--jp-widgets-slider-handle-border-color": "var(--jp-border-color1)",
    "--jp-widgets-slider-handle-background-color": "var(--jp-layout-color1)",
    "--jp-widgets-slider-active-handle-color": "var(--jp-brand-color1)",
}
_LIGHT_HOST_DECLARATIONS = "\n".join(
    f"    {name}: {value};" for name, value in _LIGHT_HOST_COLORS.items()
)

#: The wizard's stylesheet (no leading/trailing ``<style>`` tags -- wrap it with
#: :func:`style_widget`, which also installs the number-box mouse-wheel guard that
#: a bare ``f"<style>{WIZARD_CSS}</style>"`` would lack).
WIZARD_CSS = f"""
/* page background only in Voila (body[data-voila]); JupyterLab keeps its theme */
body[data-voila] {{ background: {branding.GREY}; }}
/* the page column: shell root only -- the wizard root also carries .forge-app
   (so the notebook path is styled) and must not get its own max-width/padding */
body[data-voila] .forge-shell {{ max-width: {SHELL_MAX_WIDTH}; margin: 0 auto; padding: 0 24px; }}

/* the wizard is light in any host theme: native controls and scrollbars too
   (color-scheme), and ipywidgets' controls through the host colours. The header
   too, for when branding_display() shows it above a wizard, outside .forge-app */
.{branding.HEADER_CLASS},
.forge-app {{
    color-scheme: light;
{_LIGHT_HOST_DECLARATIONS}
}}
/* in a dark JupyterLab, the outermost wizard root brings Voila's light page with
   it, so the text that sits on the page (header, intro, catalog bar) reads */
body[data-jp-theme-light="false"] .forge-app:not(.forge-app .forge-app) {{
    background: {branding.GREY};
    border-radius: 6px;
    padding: 0 12px 12px;
}}

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
/* the card body is the size container for the narrow-window @container rules */
.forge-app .forge-card-b {{ padding: 14px 18px 16px; container-type: inline-size; }}
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
/* in a card header, Required/Optional are quiet text, not a red pill that reads
   like an error: the live status chip beside them is the header's one colour */
.forge-app .forge-card-h .forge-chip.req,
.forge-app .forge-card-h .forge-chip.opt {{
    background: transparent;
    color: {branding.INK_3};
    font-weight: 500;
    padding: 0 2px;
}}

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
.forge-app .forge-field .exp {{ margin-top: 3px; }}
.forge-app .forge-field .sym {{
    font-family: Menlo, Consolas, monospace;
    font-size: 11.5px;
    color: {branding.INK_3};
}}
/* fixed input widths (110-420px) and 300px-wide checkboxes inside a two-column
   field_grid gave the card body a ~1470px min-content width: let the row's
   tracks shrink, cap inputs at their cell, and wrap checkbox rows */
.forge-app .forge-field > * {{ min-width: 0; }}
.forge-app .forge-field .widget-hbox > .widget-inline-hbox {{ max-width: 100%; }}
.forge-app .forge-field .widget-hbox:has(> .widget-checkbox) {{ flex-wrap: wrap; }}
.forge-app .forge-field .widget-hbox > .widget-text {{ flex: 0 1 auto; min-width: 0; }}
.forge-app .forge-field .widget-hbox > .jupyter-button {{ flex: none; }}
.forge-app .forge-field .widget-hbox > .widget-checkbox {{ width: auto; margin-right: 22px; }}
/* field_grid: two columns, one in a narrow card body. Rows keep their own height
   at the top when the grid is stretched taller (beside a drawn grid preview, the
   geometry row stretches it to the image's height and the rows spread apart) */
.forge-app .forge-fgrid {{
    grid-template-columns: repeat(2, minmax(0, 1fr));
    align-content: start;
}}
@container (max-width: 760px) {{
    .forge-app .forge-fgrid {{ grid-template-columns: minmax(0, 1fr); }}
}}
/* the Grid geometry fields beside the grid preview: the fields keep the ~680px
   their two columns need (label, box, unit and hint), the preview column gives
   way first, down to its image's 400px floor; under 1100px the preview moves
   below the fields */
.forge-app .forge-geo-row > .forge-fgrid {{ flex: 1 1 680px; min-width: 680px; }}
.forge-app .forge-geo-row > :last-child {{ flex: 0 1 620px; }}
@container (max-width: 1100px) {{
    .forge-app .forge-geo-row {{ flex-wrap: wrap; row-gap: 8px; }}
    .forge-app .forge-geo-row > .forge-fgrid {{ flex: 1 1 100%; min-width: 0; }}
}}
/* under 1000px the CDR plot moves below the CDR release panels */
@container (max-width: 1000px) {{
    .forge-app .forge-cdr-row {{ flex-wrap: wrap; row-gap: 8px; }}
    .forge-app .forge-cdr-row > :first-child {{ flex: 1 1 100%; }}
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
/* number boxes: no up/down arrows (the mouse wheel is handled in style_widget) */
.forge-app input[type=number] {{ -moz-appearance: textfield; appearance: textfield; }}
.forge-app input[type=number]::-webkit-inner-spin-button,
.forge-app input[type=number]::-webkit-outer-spin-button {{ -webkit-appearance: none; margin: 0; }}
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
.forge-app .forge-settings-sec .ttl {{ font-size: 15px; font-weight: 650; color: {branding.NAVY}; }}
.forge-app .forge-settings-sec .sym {{
    font-family: Menlo, Consolas, monospace;
    font-size: 11.5px;
    color: {branding.INK_3};
}}

/* subsection header: the space and hairline go ABOVE the title, so the title
   sits with the fields it names instead of hanging off the previous group */
.forge-app .forge-sub {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 22px 0 12px;
    padding-top: 16px;
    border-top: 1px solid {branding.LINE_2};
}}
/* ...except for a subsection that opens its card body or pane. Hidden siblings
   (a card body starts with its error banner, display:none until there is a
   problem) don't count, or Run setup's first subsection got the rule */
.forge-app .widget-vbox:nth-child(1 of :not([style*="display: none"])) > .forge-sub:first-child {{
    border-top: 0;
    margin-top: 2px;
    padding-top: 0;
}}
.forge-app .forge-sub .widget-html-content {{ display: flex; align-items: center; gap: 10px; }}
.forge-app .forge-sub-ttl {{ font-size: 15px; font-weight: 650; color: {branding.NAVY}; }}
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
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 10px;
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
/* in a narrow window the step links wrap onto a second line, so every card stays
   one click away; the run summary takes what room is left on its line (a zero
   basis: it never pushes anything onto another line) and ellipsises */
.forge-app .forge-sticky .summary {{
    flex: 1 1 0;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
/* the status chip keeps to the right edge, even on a line of its own */
.forge-app .forge-sticky .widget-html-content > :last-child {{ margin-left: auto; }}
/* once the blueprint page's eight step links and the chip leave the run summary
   too little room to read, it goes (the cards say the same) */
@container forge-sticky (max-width: 900px) {{
    .forge-app.forge-wizard .forge-sticky .summary {{ display: none; }}
}}
/* the status HTML shrinks (and ellipsises) before the download button does.
   min-width:0 alone leaves its no-wrap run summary in the page's min-content
   width (~1450px, more with long names); contain:inline-size takes it out. */
.forge-app .forge-sticky > .widget-html:first-child {{
    flex: 1 1 0;
    min-width: 0;
    contain: inline-size;
    container: forge-sticky / inline-size;
}}
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
/* the no-wrap "Loaded …" status: same min-content fix as the sticky bar's summary */
.forge-app .forge-catalog-bar > .widget-html:last-child {{ contain: inline-size; }}
/* with those minimums gone, each page fills the stack instead of sizing to content */
.forge-app .forge-stack > .widget-box {{ flex: 1 1 0%; min-width: 0; }}

/* buttons: neutral by default, brand-blue primary, red-outline danger.
   width:auto lets captions like "Use CFL value" size themselves instead of
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
.forge-app .forge-dl-btn code {{ color: #fff; margin-left: 0.5em; }}

/* the preview pane (a live view of the draft) sits beside the cards (right), or
   below or above them, and optionally sticks to the page scroll (forge-pinned).
   Right: the columns share the row 3 : 2 and stack below ~1100px. */
.forge-app .forge-two-col {{ gap: 24px; align-items: flex-start; }}
.forge-app .forge-two-col > .forge-left {{ flex: 3 1 0; min-width: 0; }}
.forge-app .forge-two-col > .forge-side-right {{ flex: 2 1 0; min-width: 0; }}
.forge-app .forge-side-right.forge-pinned {{
    position: sticky;
    top: {STICKY_BAR_HEIGHT};
    max-height: calc(100vh - {STICKY_BAR_HEIGHT} - 16px);
    overflow-y: auto;
}}
.forge-app .forge-side-top, .forge-app .forge-side-bottom {{
    box-sizing: border-box;
    background: #fff;
    border: 1px solid {branding.LINE};
    border-radius: 6px;
    padding: 6px 14px 10px;
    margin: 10px 0;
    max-height: 40vh;
    overflow-y: auto;
    z-index: 4;
}}
.forge-app .forge-side-top.forge-pinned {{
    position: sticky;
    top: {STICKY_BAR_HEIGHT};
    box-shadow: 0 6px 12px -8px rgba(0, 0, 0, 0.35);
}}
.forge-app .forge-side-bottom.forge-pinned {{
    position: sticky;
    bottom: 0;
    box-shadow: 0 -6px 12px -8px rgba(0, 0, 0, 0.35);
}}
/* in a narrow window the Workplan page's sticky bar can wrap onto a second line
   (its six step links, status chip and download button need ~780px of window), so
   a pinned top preview docks below two lines: a gap while the bar still fits one
   line beats covering the preview's controls. Sticky is Voila-only (see below),
   where the page follows the window. */
@media (max-width: 820px) {{
    body[data-voila] .forge-app .forge-side-top.forge-pinned {{ top: {STICKY_BAR_HEIGHT_WRAPPED}; }}
}}
.forge-app .forge-pane-controls {{ align-items: center; gap: 14px; flex-wrap: wrap; }}
.forge-app .forge-side-right .forge-pane-body {{ flex-direction: column !important; }}
.forge-app .forge-side-top .forge-pane-body,
.forge-app .forge-side-bottom .forge-pane-body {{ gap: 18px; }}
.forge-app .forge-side-top .forge-pane-body > *,
.forge-app .forge-side-bottom .forge-pane-body > * {{ flex: 1 1 0; min-width: 0; }}
/* side by side, both of the pane's subsections open the row: no rule above either */
.forge-app .forge-side-top .forge-pane-body > .widget-vbox > .forge-sub:first-child,
.forge-app .forge-side-bottom .forge-pane-body > .widget-vbox > .forge-sub:first-child {{
    border-top: 0;
    margin-top: 2px;
    padding-top: 0;
}}
.forge-app .forge-dag .widget-html-content {{ display: block; }}
@media (max-width: 1100px) {{
    .forge-app .forge-two-col {{ flex-direction: column !important; align-items: stretch; }}
    .forge-app .forge-two-col > .forge-left,
    .forge-app .forge-two-col > .forge-side-right {{
        flex: 0 0 auto !important;
        position: static !important;
        max-height: none;
        overflow-y: visible;
    }}
}}

/* Sticky needs every ancestor up to Voila's scroller to stop clipping: ipywidgets
   boxes and Voila's output areas default to overflow:auto, which makes each the
   sticky element's scroll container (a box as tall as its content), so nothing
   sticks. Let them overflow visibly so the status bar and the pane stick to the
   page scroll. Voila only; JupyterLab cells keep their own scrolling. */
body[data-voila] .forge-shell,
body[data-voila] .forge-shell .widget-box,
body[data-voila] .forge-shell .forge-stack,
body[data-voila] .forge-app,
body[data-voila] .forge-two-col,
body[data-voila] .forge-left,
body[data-voila] .forge-side,
body[data-voila] .jp-OutputArea,
body[data-voila] .jp-OutputArea-child,
body[data-voila] .jp-OutputArea-output {{ overflow: visible; }}

/* an editable code view (a Textarea): monospace, scrolls, full width */
.forge-app .forge-code, .forge-app .forge-code textarea {{
    width: 100%;
    box-sizing: border-box;
}}
.forge-app .forge-code textarea {{
    font-family: Menlo, Consolas, monospace;
    font-size: 12px;
    max-height: 380px;
    overflow: auto;
    white-space: pre;
}}

/* collapsible panes (components.collapsible): an HTML header -- chevron, title,
   value pills, quiet Required/Optional -- under a transparent toggle button that
   takes the click, and a body shown while open */
.forge-app .forge-coll {{ margin: 0 0 8px; overflow: visible; }}
.forge-app .forge-coll-h {{ position: relative; overflow: visible; }}
.forge-app .forge-coll-h > .forge-coll-hit {{
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    margin: 0;
    padding: 0;
    border: 0;
    opacity: 0;
    cursor: pointer;
}}
.forge-app .forge-coll-h:has(> .forge-coll-hit:focus-visible) {{
    outline: 2px solid {branding.LIGHT_BLUE};
    outline-offset: 1px;
}}
.forge-app .forge-coll-ht {{ flex: 1 1 auto; min-width: 0; margin: 0; }}
.forge-app .forge-coll-ht .widget-html-content {{
    display: flex;
    align-items: center;
    gap: 10px;
    line-height: 20px;
}}
.forge-app .forge-coll-ht .chev {{
    flex: none;
    width: 12px;
    font-size: 12px;
    color: {branding.INK_2};
    transition: transform .12s;
}}
.forge-app .forge-coll-open > .forge-coll-h .chev {{ transform: rotate(90deg); }}
.forge-app .forge-coll-t {{ font-size: 13.5px; font-weight: 600; color: {branding.SOFT_BLACK}; white-space: nowrap; }}
.forge-app .forge-coll-pills {{ display: inline-flex; flex-wrap: wrap; gap: 6px; min-width: 0; }}
.forge-app .forge-coll-req {{ font-size: 12px; font-weight: 500; color: {branding.INK_3}; white-space: nowrap; }}
.forge-app .forge-pill {{
    display: inline-flex;
    align-items: center;
    height: 20px;
    padding: 0 7px;
    border-radius: 4px;
    background: #EEF0F3;
    color: #3D3F46;
    font-size: 12px;
    font-weight: 500;
    white-space: nowrap;
}}
.forge-app .forge-pill.mod {{ background: {branding.TINT_BLUE}; color: {branding.BLUE}; }}
.forge-app .forge-pill.warn {{ background: {branding.TINT_AMBER}; color: {branding.AMBER}; }}
.forge-app .forge-pill.off {{ background: transparent; color: {branding.INK_3}; padding: 0 2px; }}
/* a pane: white closed header; open, its tinted header joins the bordered body */
.forge-app .forge-coll-pane > .forge-coll-h {{
    background: #fff;
    border: 1px solid {branding.LINE};
    border-radius: 6px;
    padding: 8px 14px;
}}
.forge-app .forge-coll-pane.forge-coll-open > .forge-coll-h {{
    background: #FAFAFB;
    border-radius: 6px 6px 0 0;
    border-bottom-color: {branding.LINE_2};
}}
.forge-app .forge-coll-pane > .forge-coll-b {{
    border: 1px solid {branding.LINE};
    border-top: 0;
    border-radius: 0 0 6px 6px;
    padding: 14px 18px 16px;
}}
/* inline: a one-line link (a forcing row's options), its body under a dashed rule */
.forge-app .forge-coll-inline {{ flex: 1 1 100%; margin: 0; }}
.forge-app .forge-coll-inline > .forge-coll-h {{ align-self: flex-start; }}
.forge-app .forge-coll-inline .forge-coll-t {{ font-size: 12.5px; color: {branding.BLUE}; }}
.forge-app .forge-coll-inline > .forge-coll-b {{
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px dashed {branding.LINE};
}}
/* a pane-level options link (initial conditions, boundary forcing) applies to
   the whole pane, so it is set off from the subsection above it like one */
.forge-app .forge-coll-inline.forge-sec-opts {{
    margin-top: 18px;
    padding-top: 12px;
    border-top: 1px solid {branding.LINE_2};
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

/* Advanced-settings output-stream table (grouped write/period/records rows from
   _build_output_table). One column template (label | Write | Period | Records |
   the stream's file readout and extras), so Write/Period/Records line up down the
   whole card: the middle columns sit at their maximum until the card is too
   narrow, then shrink. Header cells share one fixed height and are quiet: no
   per-cell underline. */
.forge-app .forge-out-table {{
    margin: 4px 0 8px;
    grid-template-columns: minmax(160px, 1.3fr) minmax(70px, 150px) minmax(130px, 190px) minmax(130px, 150px) minmax(120px, 1.2fr);
}}
.forge-app .forge-out-th {{ align-self: end; }}
/* in a narrow card (the five columns need ~660px, more inside a pane) each row's
   readout and extras move to a line of their own, under Write..Records */
@container (max-width: 720px) {{
    .forge-app .forge-out-table {{
        grid-template-columns: minmax(120px, 1.3fr) minmax(120px, 150px) minmax(130px, 190px) minmax(120px, 150px);
    }}
    .forge-app .forge-out-table > .forge-out-x {{ grid-column: 2 / -1; }}
    .forge-app .forge-out-table > .forge-out-th.forge-out-x {{ display: none; }}
}}
/* a stream's extras (e.g. CDR output's Averaging dropdown) fit the last column,
   their label ellipsised */
.forge-app .forge-out-table > .widget-hbox {{ min-width: 0; flex-wrap: wrap; }}
.forge-app .forge-out-table > .widget-hbox > .widget-inline-hbox {{ max-width: 100%; }}
.forge-app .forge-out-table > .widget-hbox > .widget-inline-hbox > .widget-label {{
    flex: 0 1 auto;
    min-width: 0;
}}
.forge-app .forge-out-table > .widget-hbox > .widget-inline-hbox > :is(input, select) {{ min-width: 0; }}
/* the period box with its model-time readout, and the per-stream file readout */
.forge-app .forge-out-period {{ align-items: center; column-gap: 8px; }}
.forge-app .forge-out-per .widget-html-content,
.forge-app .forge-out-files .widget-html-content {{
    font-size: 12px;
    color: {branding.INK_2};
    line-height: 1.3;
}}
.forge-app .forge-out-per .widget-html-content {{ white-space: nowrap; }}
.forge-app .forge-out-files {{ min-width: 0; margin-right: 14px; }}
.forge-out-th .widget-html-content {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .05em;
    color: {branding.INK_3};
    height: 18px;
    line-height: 18px;
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
/* space and a hairline between namelist sections inside one Advanced pane, like
   a subsection; the pane's first header has none (widget-level class set in
   _SettingsEditor) */
.forge-app .forge-settings-sec-w {{
    border-top: 1px solid {branding.LINE_2};
    margin-top: 18px;
    padding-top: 12px;
}}
.forge-app .forge-settings-sec-w:first-child {{ border-top: 0; margin-top: 0; padding-top: 0; }}
/* a namelist section's settings fill the pane in two columns, labels
   left-aligned like field_row's (output tables, variable grids and the "More
   output streams" panes keep their own layout) */
.forge-app .forge-settings-sec-w ~ .widget-vbox:not(.forge-open-acc):not(:has(.forge-out-table, .forge-var-grid)) {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(min(460px, 100%), 1fr));
    column-gap: 32px;
    row-gap: 2px;
}}
.forge-app .forge-settings-sec-w ~ .widget-vbox .widget-inline-hbox > .widget-label {{ text-align: left; }}
/* a wide setting (_make_field_widget's 560px) is capped at its column and its
   input shrinks, instead of running past the card in a narrow window */
.forge-app .forge-settings-sec-w ~ .widget-vbox > .widget-inline-hbox {{ max-width: 100%; }}
.forge-app .forge-settings-sec-w ~ .widget-vbox > .widget-inline-hbox > :is(input, select, textarea) {{ min-width: 0; }}
/* repeatable forcing rows: each row is a tile with its labels above the inputs
   (so a field lines up from row to row), checkboxes on the input baseline, and
   the remove button in the top-right corner. A row's option fields
   (_ForcingEditor._add_row_options) sit the same way under its disclosure. */
.forge-app .forge-frow {{
    position: relative;
    background: #FAFAFB;
    border: 1px solid {branding.LINE_2};
    border-radius: 6px;
    padding: 10px 44px 12px 14px;
    margin: 0 0 10px;
    column-gap: 16px;
    row-gap: 12px;
}}
.forge-app .forge-frow-opts {{ column-gap: 16px; row-gap: 12px; }}
/* a fixed value shown as text in a row's field position (the time-varying CO2
   source): label above, text on the input line */
.forge-app .forge-frow .forge-frow-static .widget-html-content {{ display: block; }}
.forge-app .forge-frow-static-l {{
    font-size: 11.5px;
    font-weight: 600;
    color: {branding.INK_2};
    line-height: 16px;
    margin: 0 0 3px;
}}
.forge-app .forge-frow-static-v {{ font-size: 13px; line-height: 28px; }}
/* a row list's "add" button and its copy button, side by side */
.forge-app .forge-row-btns {{ gap: 8px; flex-wrap: wrap; }}
.forge-app .forge-frow > .widget-inline-hbox:not(.widget-checkbox),
.forge-app .forge-frow-opts > .widget-inline-hbox:not(.widget-checkbox) {{
    flex-direction: column;
    align-items: stretch;
    margin: 0;
    height: auto;
}}
.forge-app .forge-frow .widget-inline-hbox:not(.widget-checkbox) > .widget-label {{
    max-width: none;
    text-align: left;
    font-size: 11.5px;
    font-weight: 600;
    color: {branding.INK_2};
    line-height: 16px;
    height: 16px;
    margin: 0 0 3px;
    padding: 0;
}}
.forge-app .forge-frow .widget-inline-hbox > select,
.forge-app .forge-frow .widget-inline-hbox > input {{ flex: none; height: 28px; }}
.forge-app .forge-frow .widget-inline-hbox > textarea {{ flex: none; height: 52px; }}
.forge-app .forge-frow .widget-checkbox {{
    height: 28px;
    margin: 0;
    align-items: center;
    width: auto;
}}
.forge-app .forge-frow > .widget-button:first-child {{
    position: absolute;
    top: 8px;
    right: 8px;
    height: 28px;
    padding: 0;
    border-color: transparent;
    background: transparent;
    color: {branding.INK_3};
}}
.forge-app .forge-frow > .widget-button:first-child:hover {{
    color: {branding.BRAND_RED};
    border-color: {branding.LINE_2};
    background: #fff;
}}
/* a forcing-row box its pane's note asks the user to fill (refresh_bgc_notes) */
.forge-app .forge-need input {{
    border-color: {branding.AMBER};
    background: {branding.TINT_AMBER};
}}
/* the sticky bar's Invalid chip, a link to the card that owns the problem */
.forge-app .forge-status-link {{ text-decoration: none; }}
.forge-out-label .widget-html-content {{ font-size: 13px; padding: 3px 0; line-height: 1.3; }}
.forge-out-label .sym {{
    display: block;
    font-family: Menlo, Consolas, monospace;
    font-size: 11px;
    color: {branding.INK_3};
}}
.forge-var-title {{ font-size: 12.5px; font-weight: 600; margin: 10px 0 4px; }}
.forge-app .forge-var-grid .widget-checkbox {{ width: auto; }}
/* variable checkboxes: four columns, three in a narrower card, two in a narrow
   one, so a typical label ("Sea surface height (zeta)") fits on its line */
.forge-app .forge-var-grid {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
@container (max-width: 1150px) {{
    .forge-app .forge-var-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
}}
@container (max-width: 760px) {{
    .forge-app .forge-var-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
}}
/* a longer label wraps under itself instead of ellipsising (variable grids and
   the output table's stacked write flags); a one-line label keeps the 28px row */
.forge-app :is(.forge-var-grid, .forge-out-write-stack) .widget-checkbox {{
    height: auto;
    min-height: 28px;
}}
.forge-app :is(.forge-var-grid, .forge-out-write-stack) .widget-label-basic {{
    display: flex;
    align-items: baseline;
    height: auto;
    padding: 5px 0;
    line-height: 18px;
    white-space: normal;
}}
.forge-app :is(.forge-var-grid, .forge-out-write-stack) .widget-label-basic > input {{ flex: none; }}
"""


#: Installed once per page: a wheel event over a number box takes its focus away,
#: which is what stops the browser from stepping its value. Only boxes under the
#: ``.forge-app`` root are touched, like the CSS, so the rest of a JupyterLab tab
#: keeps its own wheel behavior.
_NO_WHEEL_JS = (
    "if(!window.forgeNoWheel){window.forgeNoWheel=1;"
    "document.addEventListener('wheel',function(e){var t=e.target;"
    "if(t.type==='number'&&t.closest('.forge-app'))t.blur();},true);}"
)


def style_widget(W: Any) -> Any:
    """A ``W.HTML`` widget carrying :data:`WIZARD_CSS` in a ``<style>`` tag.

    The tag's ``onload`` (scripts are not executed, see
    :func:`cstar.wizard.ui.branding._favicon_js`) stops the mouse wheel from
    changing the number in a focused number box: the box loses focus instead.
    """
    widget = W.HTML(f'<style onload="{_NO_WHEEL_JS}">{WIZARD_CSS}</style>')
    widget.add_class("forge-style")
    return widget


def chip(text: str, kind: str, *, title: str = "") -> str:
    """An inline ``<span class='forge-chip {kind}'>`` HTML chip; ``title`` is its tooltip."""
    title_attr = f" title='{title}'" if title else ""
    return f"<span class='forge-chip {kind}'{title_attr}>{text}</span>"


#: The chip a glossary entry with ``experimental: true`` puts under its label.
EXPERIMENTAL_CHIP = chip(
    "Experimental",
    "warn",
    title="This feature is experimental and may not work correctly.",
)


def banner(kind: str, html: str) -> str:
    """A ``<div class='forge-banner {kind}'>`` HTML banner.

    ``.forge-banner`` is a flex box, so *html* goes in one inner ``<div>``:
    otherwise each loose run of it (text, ``<b>…</b>``, ``<br>``) becomes its
    own flex item, and they lay out side by side as columns.
    """
    return f"<div class='forge-banner {kind}'><div>{html}</div></div>"


def banner_widget(W: Any, kind: str, html: str) -> Any:
    """A ``W.HTML`` widget wrapping :func:`banner`, carrying class ``forge-banner-w``."""
    widget = W.HTML(banner(kind, html))
    widget.add_class("forge-banner-w")
    return widget


def _field_label_html(label) -> str:
    """The label-column HTML for one :func:`field_row` (word + symbol + experimental chip)."""
    req = "<span class='req'>*</span>" if label.required else ""
    html = f"<div class='w'>{label.label}{req}</div>"
    if label.symbol:
        html += f"<div class='sym'>{label.symbol}</div>"
    if label.experimental:
        html += f"<div class='exp'>{EXPERIMENTAL_CHIP}</div>"
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
    :func:`cstar.wizard.ui.labels.label_for`, plus an "Experimental" chip
    for an entry flagged ``experimental``; ``hint`` falls back to
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
    """A ``W.GridBox`` (class ``forge-fgrid``) laying out ``rows`` (e.g.
    :func:`field_row` results) in ``cols`` columns.

    Two columns, the usual case, come from the stylesheet, which drops them to
    one in a narrow card; any other count is set on the grid and kept at every
    width.
    """
    grid = W.GridBox(rows, layout=W.Layout(grid_gap="0 28px"))
    grid.add_class("forge-fgrid")
    if cols != 2:
        grid.layout.grid_template_columns = f"repeat({cols}, minmax(0, 1fr))"
    return grid


def subsection(
    W: Any,
    key: str,
    *children: Any,
    default_title: str = "",
    symbols: str | None = None,
    trailing: Any = None,
    page: str = "blueprint-wizard",
) -> Any:
    """A subsection header (``forge-sub``, from :func:`~cstar.wizard.ui.labels.section_for`) plus ``children``."""
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


@dataclass(frozen=True)
class Pill:
    """One value in a collapsible pane's header (see :class:`PaneTitle`).

    ``kind`` is how it reads: ``""`` a current choice, ``"mod"`` changed from
    its default, ``"off"`` a default or nothing (muted text, no pill), and
    ``"warn"`` something to fix.
    """

    text: str
    kind: Literal["", "mod", "off", "warn"] = ""


@dataclass(frozen=True)
class PaneTitle:
    """A collapsible pane's header: the title, the current values as pills, and a
    quiet ``Required``/``Optional`` at the right (``chip``, or none).

    ``str()`` gives the plain-text form (parts joined with " · "), which is also
    the header's accessible name and tooltip.
    """

    title: str
    pills: tuple[Pill, ...] = ()
    chip: str = ""

    def __str__(self) -> str:
        parts = [self.title, *(p.text for p in self.pills), self.chip]
        return " · ".join(p for p in parts if p)

    def with_pill(self, pill: Pill) -> PaneTitle:
        """This title with ``pill`` added after its other pills."""
        return replace(self, pills=(*self.pills, pill))

    def html(self) -> str:
        """The header markup: a chevron, then the escaped title, pills and chip."""
        pills = "".join(
            f"<span class='forge-pill {p.kind}'>{html.escape(p.text)}</span>"
            for p in self.pills
        )
        chip = (
            f"<span class='forge-coll-req'>{html.escape(self.chip)}</span>"
            if self.chip
            else ""
        )
        return (
            "<span class='chev'>▸</span>"
            f"<span class='forge-coll-t'>{html.escape(self.title)}</span>"
            f"<span class='forge-coll-pills'>{pills}</span>"
            f"<span class='sp'></span>{chip}"
        )


def accordion_title(title: str, *pills: str | Pill, chip: str = "") -> PaneTitle:
    """A :class:`PaneTitle` from ``title``, its value ``pills`` (a bare string is a
    plain pill; empty ones are skipped) and an optional ``chip``.
    """
    parts = tuple(p if isinstance(p, Pill) else Pill(p) for p in pills)
    return PaneTitle(title, tuple(p for p in parts if p.text), chip)


def _as_pane_title(title: str | PaneTitle) -> PaneTitle:
    return title if isinstance(title, PaneTitle) else PaneTitle(title)


@functools.cache
def _collapsible_class(W: Any) -> type:
    """The widget class :func:`collapsible` builds, made once per ``ipywidgets``
    module so this module never imports ``ipywidgets`` itself.
    """
    import traitlets

    class Collapsible(W.VBox):  # type: ignore[misc, name-defined]
        """A pane with an HTML header that opens and closes its body.

        ``selected_index`` is ``0`` while open and ``None`` while closed, as on a
        one-pane ``W.Accordion``; the header's transparent toggle button (the
        part that takes the click) and the body follow it.
        """

        selected_index = traitlets.Int(None, allow_none=True)

        def __init__(self, body: Any, title: PaneTitle, variant: str) -> None:
            self.body = body
            self.title = title
            self._head_html = W.HTML(title.html())
            self._head_html.add_class("forge-coll-ht")
            self._hit = W.ToggleButton(
                value=False, description=str(title), tooltip=str(title)
            )
            self._hit.add_class("forge-coll-hit")
            head = W.Box([self._head_html, self._hit])
            head.add_class("forge-coll-h")
            # The body sits in its own box, so opening and closing never touches
            # the body's own layout.display (callers hide some bodies themselves).
            self._body_box = W.VBox([body], layout=W.Layout(display="none"))
            self._body_box.add_class("forge-coll-b")
            super().__init__([head, self._body_box])
            self.add_class("forge-coll")
            self.add_class(f"forge-coll-{variant}")
            self._hit.observe(self._on_hit, names="value")
            self.observe(self._on_index, names="selected_index")

        def _on_hit(self, change: dict) -> None:
            self.selected_index = 0 if change["new"] else None

        def _on_index(self, change: dict) -> None:
            is_open = change["new"] is not None
            self._hit.value = is_open
            self._body_box.layout.display = "" if is_open else "none"
            if is_open:
                self.add_class("forge-coll-open")
            else:
                self.remove_class("forge-coll-open")

        def set_title(self, title: str | PaneTitle) -> None:
            """Show ``title`` in the header; a title equal to the current one
            changes nothing (the wizard retitles every pane on each rebuild).
            """
            title = _as_pane_title(title)
            if title == self.title:
                return
            self.title = title
            self._head_html.value = title.html()
            self._hit.description = self._hit.tooltip = str(title)

        def get_title(self) -> str:
            """The header's plain text (see :class:`PaneTitle`)."""
            return str(self.title)

    return Collapsible


def collapsible(
    W: Any, body: Any, title: str | PaneTitle = "", *, variant: str = "pane"
) -> Any:
    """A collapsible pane (class ``forge-coll``), closed, whose header is HTML.

    ipywidgets 8 writes ``W.Accordion`` titles with ``textContent``, so a title
    there can only be plain text. Here the header is a ``W.HTML`` (chevron, title,
    value pills, quiet Required/Optional: see :class:`PaneTitle`) under a
    transparent ``W.ToggleButton`` that takes the click. ``variant`` picks the
    look: ``"pane"`` (a bordered panel, the forcing and Advanced panes) or
    ``"inline"`` (a one-line link, a forcing row's options). The widget has
    ``selected_index`` (``0`` open, ``None`` closed), ``set_title``,
    ``get_title`` and ``body``.
    """
    return _collapsible_class(W)(body, _as_pane_title(title), variant)


def open_accordion(W: Any, panes: list, titles: Sequence[str | PaneTitle]) -> Any:
    """A stack of independently collapsible panes (class ``forge-open-acc``).

    ``W.Accordion`` allows one open pane at a time, so opening a second pane
    collapses the first and the page jumps. This returns a ``W.VBox`` of
    :func:`collapsible` panes instead -- any number may stay open -- with a
    ``set_title(i, title)`` / ``get_title(i)`` surface for refreshing titles.
    ``box.panes`` lists the panes (each with ``selected_index`` and ``body``).
    """
    if len(panes) != len(titles):
        raise ValueError(f"open_accordion: {len(panes)} panes but {len(titles)} titles")
    inner = [
        collapsible(W, pane, title) for pane, title in zip(panes, titles, strict=True)
    ]
    box = W.VBox(inner)
    box.add_class("forge-open-acc")
    box.panes = inner
    box.set_title = lambda i, title: inner[i].set_title(title)
    box.get_title = lambda i: inner[i].get_title()
    return box
