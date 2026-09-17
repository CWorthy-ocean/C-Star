"""[C]Worthy brand palette and the wizard's logo + title header bar.

This module owns the palette tokens shared across the wizard UI (brand colors
plus the neutral/tint scale used by the redesigned components) and emits the
logo + title header bar (with the brand-red bottom rule), which in full-page
Voila contexts also sets the browser tab's favicon and title. The wizard's own
CSS (cards, chips, banners, fields, etc.) lives in
:mod:`cstar_forge.ui.components`, built from the tokens defined here.
"""

import base64
from functools import lru_cache
from pathlib import Path

# [C]Worthy brand palette (brand guidelines).
BRAND_RED = "#ED523E"  # header divider rule, required/error accents
SOFT_BLACK = "#28292E"  # header title text, primary body text

# Neutral scale (borders, secondary/tertiary text, page background).
GREY = "#F0F0F0"  # Voila page background
INK_2 = "#5B5D66"  # secondary text (descriptions, hints)
INK_3 = "#8A8C94"  # tertiary text (symbols, placeholders)
LINE = "#D9DADF"  # card/field borders
LINE_2 = "#E8E9EC"  # lighter dividers (card header rule)

# Accent colors.
LIGHT_BLUE = "#C0D3ED"  # focus ring
BLUE = "#004182"  # primary action / current-tab color
GREEN = "#2E7D4F"  # success / ok
AMBER = "#B7791F"  # warning

# Tint backgrounds (paired with the accent colors above for chips/banners).
TINT_BLUE = "#EAF0F9"
TINT_RED = "#FDECE9"
TINT_GREEN = "#E9F4EE"
TINT_AMBER = "#FBF3E4"

#: Browser tab title (Voila otherwise shows the notebook filename, e.g. "_voila_app").
PAGE_TITLE = "C-Star Blueprint Wizard"

#: Class on the header bar; the header CSS is scoped under it.
HEADER_CLASS = "cworthy-forge-header"

_LOGO_PATH = Path(__file__).parent / "assets" / "cworthy-logo.png"


@lru_cache(maxsize=1)
def logo_data_uri() -> str:
    """The bundled [C]Worthy logo as a ``data:`` URI (128px PNG, ~5.5 kB)."""
    return "data:image/png;base64," + base64.b64encode(_LOGO_PATH.read_bytes()).decode(
        "ascii"
    )


def header_css() -> str:
    """CSS for the header bar only (layout, logo size, red divider, title text)."""
    return f"""
.{HEADER_CLASS} {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 4px;
    border-bottom: 3px solid {BRAND_RED};
    margin-bottom: 8px;
}}
.{HEADER_CLASS} img {{
    width: 40px;
    height: 40px;
}}
.{HEADER_CLASS} .title {{
    font-size: 20px;
    font-weight: 700;
    color: {SOFT_BLACK};
}}
.{HEADER_CLASS} .subtitle {{
    font-size: 13px;
    color: #666;
}}
"""


def _favicon_js() -> str:
    """One-line favicon/title script, run from the header logo's ``onload``.

    Neither JupyterLab nor Voila executes ``<script>``/``Javascript`` outputs, so
    this rides the logo ``<img onload=...>`` instead. Guarded on Voila's
    ``data-voila`` body attribute so it never hijacks the JupyterLab tab.
    """
    return (
        "if(document.body.dataset.voila){"
        f"document.title='{PAGE_TITLE}';"
        "var l=document.querySelector('link[rel*=icon]');"
        "if(!l){l=document.createElement('link');l.rel='icon';"
        "document.head.appendChild(l);}"
        "l.type='image/png';l.href=this.src;}"
    )


def header_html(
    title: str = PAGE_TITLE,
    subtitle: str = (
        "Assemble a reproducible <code>forge_blueprint.yaml</code> for a "
        "ROMS-MARBL regional domain"
    ),
) -> str:
    """The logo + title header bar (also sets the favicon/title in Voila)."""
    return (
        f"<div class='{HEADER_CLASS}'>"
        f"<img src='{logo_data_uri()}' alt='[C]Worthy logo' "
        f'onload="{_favicon_js()}"/>'
        f"<div><div class='title'>{title}</div>"
        f"<div class='subtitle'>{subtitle}</div></div>"
        f"</div>"
    )


def branding_display() -> None:
    """Emit the header bar (and, in Voila, set the tab favicon and title).

    The Voilà app gets its header from :class:`cstar_forge.ui.shell.AppShell`,
    which calls :func:`header_html` directly. This helper remains for notebook
    users who display :class:`~cstar_forge.forge_blueprint_wizard.ForgeBlueprintWizardApp`
    on its own and want the same header above it: call it once, immediately
    before ``app.display()``.
    """
    from IPython.display import HTML, display

    display(HTML(f"<style>{header_css()}</style>{header_html()}"))
