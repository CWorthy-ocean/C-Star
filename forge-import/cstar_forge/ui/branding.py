"""Minimal [C]Worthy header for the wizard (logo, red divider, favicon, title).

Scope is deliberately narrow: :func:`branding_display` emits the logo + title
header bar (with the brand-red bottom rule) and, in full-page Voila contexts only,
sets the browser tab's favicon and title. It intentionally does *not* restyle the
wizard's widgets, fonts, links, or backgrounds — those keep the host theme.
"""

import base64
from functools import lru_cache
from pathlib import Path

# [C]Worthy brand palette (brand guidelines) — only the two the header uses.
BRAND_RED = "#ED523E"  # header divider rule
SOFT_BLACK = "#28292E"  # header title text

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
    title: str = PAGE_TITLE, subtitle: str = "ForgeBlueprint builder"
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

    Call once, immediately before displaying the wizard widget.
    """
    from IPython.display import HTML, display

    display(HTML(f"<style>{header_css()}</style>{header_html()}"))
