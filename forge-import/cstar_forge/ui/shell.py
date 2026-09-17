"""The wizard's outer application shell: header, page nav, and page stack.

:class:`AppShell` is UI-only scaffolding -- it lays out whatever pages it is
given behind a single-select nav (hidden for a single page) and links the nav
to the visible page with ``W.jslink`` (no custom JavaScript). Page objects
may be raw ipywidgets widgets or any object exposing a ``.widget`` property
(duck-typed via ``getattr(page, "widget", page)``).
"""

from __future__ import annotations

from typing import Any

from cstar_forge.ui import branding, components


def _carries_style(widgets: list[Any]) -> bool:
    """True if any widget (or a descendant) is a ``forge-style`` stylesheet widget."""
    stack = list(widgets)
    seen: set[int] = set()
    while stack:
        w = stack.pop()
        if id(w) in seen:
            continue
        seen.add(id(w))
        if "forge-style" in getattr(w, "_dom_classes", ()):
            return True
        stack.extend(getattr(w, "children", ()))
    return False


class AppShell:
    """The outer shell: brand header, a page nav rail, and the page stack.

    ``pages`` is a list of ``(title, page)`` pairs. ``page`` may be a plain
    ipywidgets widget, or an object exposing a ``.widget`` attribute (used in
    preference to the object itself). The nav (a ``W.ToggleButtons`` styled
    via the ``forge-rail`` CSS class) is hidden when there is only one page.
    """

    def __init__(self, pages: list[tuple[str, Any]], *, W: Any = None) -> None:
        """Build the shell from ``pages``; imports ``ipywidgets`` lazily if ``W`` is omitted."""
        if W is None:
            import ipywidgets as W
        self.W = W
        self.pages = pages

        titles = [title for title, _ in pages]
        page_widgets = [getattr(page, "widget", page) for _, page in pages]

        # Pages that already carry the stylesheet (the wizard root embeds it so
        # the Jupyter-notebook path is styled too) mean the shell need not add a
        # second copy of the same <style> block.
        style_children = (
            [] if _carries_style(page_widgets) else [components.style_widget(W)]
        )
        header = W.HTML(
            f"<style>{branding.header_css()}</style>{branding.header_html()}"
        )

        self.nav = W.ToggleButtons(options=titles)
        self.nav.add_class("forge-rail")
        if len(pages) <= 1:
            self.nav.layout.display = "none"

        self.stack = W.Stack(page_widgets, selected_index=0)
        self._link = W.jslink((self.nav, "index"), (self.stack, "selected_index"))

        self.root = W.VBox([*style_children, header, self.nav, self.stack])
        self.root.add_class("forge-app")
        self.root.add_class("forge-shell")

    def display(self) -> None:
        """Display the shell's root widget."""
        from IPython.display import display

        display(self.root)


def blueprint_app(catalog_root: str | None = None) -> AppShell:
    """Build an :class:`AppShell` with a single "Blueprint" page.

    Imports ``ForgeBlueprintWizardApp`` lazily (it, in turn, lazily imports
    ipywidgets).
    """
    from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizardApp

    app = ForgeBlueprintWizardApp(catalog_root)
    return AppShell([("Blueprint", app)])
