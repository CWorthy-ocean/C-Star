"""[C]Worthy brand palette and the wizard's logo + title header bar.

This module owns the palette tokens shared across the wizard UI (brand colors
plus the neutral/tint scale used by the redesigned components, each with a
light-theme and a dark-theme value, see :data:`PALETTE`) and emits the logo +
title header bar (with the brand-red bottom rule), which in full-page Voila
contexts also sets the browser tab's favicon and title. The wizard's own CSS
(cards, chips, banners, fields, etc.) lives in
:mod:`cstar.wizard.ui.components`, built from the tokens defined here.
"""

import base64
from collections.abc import Mapping
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
NAVY = "#00376F"  # subsection and namelist-section titles (a touch darker than BLUE)
GREEN = "#2E7D4F"  # success / ok
AMBER = "#B7791F"  # warning

# Tint backgrounds (paired with the accent colors above for chips/banners).
TINT_BLUE = "#EAF0F9"
TINT_RED = "#FDECE9"
TINT_GREEN = "#E9F4EE"
TINT_AMBER = "#FBF3E4"

#: The wizard's colours as CSS custom properties, each a ``(light, dark)`` pair.
#: The light values are the constants above and the few whites and greys the
#: stylesheets need besides; the stylesheets read every colour through these
#: tokens (``var(--forge-…)``), and :func:`themed_rule` declares the column that
#: fits the host's theme. Python-built HTML never writes a colour of its own: it
#: uses a class (e.g. :func:`cstar.wizard.ui.components.status_span`), or its
#: colour would stay light-theme text on a dark page.
PALETTE: dict[str, tuple[str, str]] = {
    # text and surfaces
    "--forge-text": (SOFT_BLACK, "#E4E5E9"),
    "--forge-ink-2": (INK_2, "#A8AAB3"),
    "--forge-ink-3": (INK_3, "#8B8E97"),
    "--forge-page": (GREY, "#16171B"),
    "--forge-surface": ("#fff", "#1F2025"),  # cards, panes, buttons, the sticky bar
    "--forge-surface-2": ("#FAFAFB", "#25262C"),  # open pane header, forcing rows
    "--forge-line": (LINE, "#3A3C44"),
    "--forge-line-2": (LINE_2, "#2E3037"),
    "--forge-shadow": ("rgba(0, 0, 0, 0.35)", "rgba(0, 0, 0, 0.6)"),
    # accents
    "--forge-red": (BRAND_RED, "#F47A68"),
    "--forge-blue": (BLUE, "#6CA6E8"),
    "--forge-navy": (NAVY, "#9CC2EE"),
    "--forge-light-blue": (LIGHT_BLUE, "#34557F"),
    "--forge-green": (GREEN, "#5CC78A"),
    "--forge-amber": (AMBER, "#E0A84A"),
    "--forge-primary": (BLUE, "#2B66AD"),  # filled buttons
    "--forge-on-primary": ("#fff", "#fff"),
    # tints (chips, banners, pills)
    "--forge-tint-blue": (TINT_BLUE, "#1B2B40"),
    "--forge-tint-red": (TINT_RED, "#3A1F1C"),
    "--forge-tint-green": (TINT_GREEN, "#17301F"),
    "--forge-tint-amber": (TINT_AMBER, "#362B17"),
    "--forge-pill": ("#EEF0F3", "#30323A"),
    "--forge-pill-text": ("#3D3F46", "#D0D2D8"),
    # status text in Python-built HTML (components.status_span), side notes, the
    # header subtitle, quieter hints and notes, and the run logs' border
    "--forge-err": ("#b00", "#FF8A80"),
    "--forge-ok": ("#080", "#6FD08C"),
    "--forge-warn": ("#b58900", "#E6B450"),
    "--forge-note": ("#666", "#A8AAB3"),
    "--forge-quiet": ("#888", "#8B8E97"),
    "--forge-faint": ("#777", "#8B8E97"),
    "--forge-log-border": ("#ccc", "#4A4C54"),
    # the workplan's dependency graph (workplan_builder.dag_svg): node fills by
    # application, node outline and text, edges and their labels
    "--forge-dag-ucla-roms": ("#cfe3f7", "#1E3A5A"),
    "--forge-dag-forge": ("#d5ecd0", "#23402A"),
    "--forge-dag-nest-ic": ("#f6e3c4", "#4A3A1E"),
    "--forge-dag-upscaler": ("#e6d4f2", "#3B2A4D"),
    "--forge-dag-hello-world": ("#f4d6d6", "#4A2626"),
    "--forge-dag-other": ("#e4e7ea", "#34373D"),
    "--forge-dag-external": ("#f1f1f1", "#2A2B30"),
    "--forge-dag-outline": ("#667", "#8A8FA0"),
    "--forge-dag-title": ("#222", "#E8E9EC"),
    "--forge-dag-subtitle": ("#555", "#B6B8C0"),
    "--forge-dag-edge": ("#555", "#9A9DA6"),
    "--forge-dag-label": ("#444", "#C4C6CD"),
}

#: Where the dark column applies: a dark JupyterLab theme (JupyterLab marks its
#: theme on ``body``; Voila run with ``--theme=dark`` does too), and Voila in a
#: browser that prefers dark. JupyterLab's light theme stays light on a dark OS,
#: and a host that marks neither (VS Code, Colab) gets the light column.
DARK_JUPYTERLAB = 'body[data-jp-theme-light="false"]'
DARK_VOILA_MEDIA = "@media (prefers-color-scheme: dark)"
VOILA_BODY = "body[data-voila]"


def themed_rule(selector: str, table: Mapping[str, tuple[str, str]]) -> str:
    """CSS declaring ``table``'s light values on ``selector`` and its dark ones where the host is dark.

    The dark blocks hold only the entries whose dark value differs; the rest
    (and a ``var()`` reference, which resolves against the dark values on the
    same element) come from the light block. Each block also sets
    ``color-scheme``, so native controls and scrollbars match. ``selector`` may
    be a selector list.
    """

    def block(sel: str, scheme: str, column: int, indent: str = "") -> str:
        lines = [f"{indent}{sel} {{", f"{indent}    color-scheme: {scheme};"]
        lines += [
            f"{indent}    {name}: {pair[column]};"
            for name, pair in table.items()
            if column == 0 or pair[1] != pair[0]
        ]
        return "\n".join([*lines, f"{indent}}}"])

    return "\n".join(
        [
            block(selector, "light", 0),
            block(f"{DARK_JUPYTERLAB} :is({selector})", "dark", 1),
            f"{DARK_VOILA_MEDIA} {{",
            block(f"{VOILA_BODY} :is({selector})", "dark", 1, "    "),
            "}",
        ]
    )


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
    """CSS for the header bar only (layout, logo size, red divider, title text).

    It declares the palette itself: :func:`branding_display` can show the
    header without the wizard's stylesheet.
    """
    return f"""
{themed_rule(f".{HEADER_CLASS}", PALETTE)}
.{HEADER_CLASS} {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 4px;
    border-bottom: 3px solid var(--forge-red);
    margin-bottom: 8px;
}}
.{HEADER_CLASS} img {{
    width: 40px;
    height: 40px;
}}
.{HEADER_CLASS} .title {{
    font-size: 20px;
    font-weight: 700;
    color: var(--forge-text);
}}
.{HEADER_CLASS} .subtitle {{
    font-size: 13px;
    color: var(--forge-note);
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
        "UCLA-ROMS regional domain"
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

    The Voilà app gets its header from :class:`cstar.wizard.ui.shell.AppShell`,
    which calls :func:`header_html` directly. This helper remains for notebook
    users who display :class:`~cstar.wizard.app.ForgeBlueprintWizardApp`
    on its own and want the same header above it: call it once, immediately
    before ``app.display()``.
    """
    from IPython.display import HTML, display

    display(HTML(f"<style>{header_css()}</style>{header_html()}"))
