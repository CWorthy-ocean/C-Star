"""The wizard's catalog-location bar, extracted from ``ForgeBlueprintWizardApp``.

This is a pure extraction of the widgets and status-message formatting that
``ForgeBlueprintWizardApp.__init__``/``_load``/``_reload`` (see
:mod:`cstar.wizard.wizard`) build for choosing/reloading the
domain catalog. It does not change catalog-loading behavior -- wiring
:class:`CatalogBar` up to the App's actual ``_load`` is left to a later work
package; ``on_reload`` here is simply invoked with the current input text.
"""

from __future__ import annotations

import asyncio
import html
import re
import threading
from typing import TYPE_CHECKING, Any

from cstar.wizard.ui import branding

if TYPE_CHECKING:
    from collections.abc import Callable

#: Strips HTML tags from a status string to build its ``title=`` plain-text.
_TAG_RE = re.compile(r"<[^>]+>")

#: Seconds the "Confirm reload" state stays armed before resetting.
_CONFIRM_TIMEOUT = 5.0


class CatalogBar:
    """A catalog-location text box + reload button + status line.

    Reload is a two-step confirm (it discards any in-progress edits): the
    first click arms a "Confirm reload (discards edits)" danger-styled
    button for :data:`_CONFIRM_TIMEOUT` seconds; a second click within that
    window calls ``on_reload(current_input_text)``. The arming timer resets
    automatically if it isn't confirmed in time.
    """

    def __init__(self, W: Any, on_reload: Callable[[str], None]) -> None:
        """Build the bar's widgets; ``on_reload`` is called with the input text on confirm."""
        self.W = W
        self._on_reload = on_reload
        self._confirm_timer: Any = None  # threading.Timer or asyncio.TimerHandle
        self._awaiting_confirm = False

        label = W.HTML("<b>Catalog</b>")
        self._cat_input = W.Text(
            value="",
            placeholder="catalog path(s), ':'-separated top-first, or GitHub URL "
            "(blank = your catalog over the bundled one)",
            description="",
            layout=W.Layout(width="420px"),
        )
        self._cat_reload_btn = W.Button(
            description="Reload catalog", icon="refresh", layout=W.Layout(width="auto")
        )
        self._cat_reload_btn.on_click(self._on_reload_click)
        self._cat_status = W.HTML("")
        self._cat_status.layout.flex = "1 1 0"

        self._widget = W.HBox(
            [label, self._cat_input, self._cat_reload_btn, self._cat_status]
        )
        self._widget.add_class("forge-catalog-bar")

    @property
    def widget(self) -> Any:
        """The bar's root widget (an ``HBox``: label, input, button, status)."""
        return self._widget

    def _set_status_html(self, html_value: str) -> None:
        """Set the status line, carrying a stripped-tags plain-text ``title=``
        for hover -- CSS truncates the rendered line with an ellipsis when it
        doesn't fit on one line.
        """
        plain = _TAG_RE.sub("", html_value)
        self._cat_status.value = (
            f'<span title="{html.escape(plain, quote=True)}">{html_value}</span>'
        )

    def set_status_for(self, cat: Any) -> None:
        """Set the status line for a successfully loaded catalog ``cat``.

        Mirrors ``ForgeBlueprintWizardApp._load``'s three cases: a layered
        stack, a single writable store, and a single read-only store (which
        adds an amber note that saves fall back to the current directory).
        """
        from cstar.catalog.domain_catalog import LayeredCatalog

        if isinstance(cat, LayeredCatalog):
            layers = " over ".join(
                f"{store.label} {store.catalog_root} ({len(store.domain_names)} domains)"
                if store is cat.top
                else f"{store.label} ({len(store.domain_names)} domains)"
                for store in cat.stores
            )
            self._set_status_html(
                f"<span style='color:{branding.GREEN}'>Loaded {layers} -- "
                f"{len(cat.model_names)} models, "
                f"{len(cat.roms_marbl_blueprint_names)} blueprints</span>"
            )
            return

        ro_note = (
            f" <span style='color:{branding.AMBER}'>(read-only catalog -- saves "
            "default to the current directory)</span>"
            if getattr(cat, "read_only", False)
            else ""
        )
        self._set_status_html(
            f"<span style='color:{branding.GREEN}'>Loaded {cat.catalog_root} -- "
            f"{len(cat.model_names)} models, "
            f"{len(cat.roms_marbl_blueprint_names)} blueprints</span>{ro_note}"
        )

    def set_error(self, val: str, exc: Exception) -> None:
        """Set the status line to a load failure for input ``val``."""
        self._set_status_html(
            f"<span style='color:{branding.BRAND_RED}'>Failed to load catalog "
            f"{val or '(default)'!r}: {exc}</span>"
        )

    def _reset_confirm(self) -> None:
        """Restore the reload button to its unarmed state."""
        self._awaiting_confirm = False
        self._cat_reload_btn.description = "Reload catalog"
        self._cat_reload_btn.button_style = ""
        self._confirm_timer = None

    def _schedule_reset(self) -> Any:
        """Arrange for `_reset_confirm` to run after ``_CONFIRM_TIMEOUT`` seconds.

        Widget state must be mutated on the kernel's event-loop thread for the
        update to reach the frontend reliably, so when a loop is running (the
        normal ipykernel case) the reset is scheduled with ``call_later`` on
        it. Only when no loop is running (plain Python, tests) does this fall
        back to a daemon ``threading.Timer``. Both returned handles expose
        ``cancel()``.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            timer = threading.Timer(_CONFIRM_TIMEOUT, self._reset_confirm)
            timer.daemon = True
            timer.start()
            return timer
        return loop.call_later(_CONFIRM_TIMEOUT, self._reset_confirm)

    def _on_reload_click(self, _btn: Any) -> None:
        """Handle a reload-button click: arm on the first click, confirm on the second."""
        if not self._awaiting_confirm:
            self._awaiting_confirm = True
            self._cat_reload_btn.description = "Confirm reload (discards edits)"
            self._cat_reload_btn.button_style = "danger"
            if self._confirm_timer is not None:
                self._confirm_timer.cancel()
            self._confirm_timer = self._schedule_reset()
            return

        if self._confirm_timer is not None:
            self._confirm_timer.cancel()
        self._reset_confirm()
        self._on_reload(self._cat_input.value)
