"""The wizard's outer application shell: header, page nav, and page stack.

:class:`AppShell` is UI-only scaffolding -- it lays out whatever pages it is
given behind a single-select nav (hidden for a single page) and links the nav
to the visible page with ``W.jslink`` (no custom JavaScript). Page objects
may be raw ipywidgets widgets or any object exposing a ``.widget`` property
(duck-typed via ``getattr(page, "widget", page)``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cstar.wizard.ui import branding, components

if TYPE_CHECKING:
    from pathlib import Path


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
            import ipywidgets

            W = ipywidgets
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
        self.stack.add_class("forge-stack")
        self._link = W.jslink((self.nav, "index"), (self.stack, "selected_index"))

        self.root = W.VBox([*style_children, header, self.nav, self.stack])
        self.root.add_class("forge-app")
        self.root.add_class("forge-shell")

    def show(self, title: str) -> None:
        """Bring the page called ``title`` to the front.

        Sets the nav and the stack both: ``jslink`` only syncs them in a
        browser, and a kernel with no frontend attached must switch too.
        """
        index = [t for t, _ in self.pages].index(title)
        self.nav.index = index
        self.stack.selected_index = index

    def display(self) -> None:
        """Display the shell's root widget."""
        from IPython.display import display

        display(self.root)


def queue_iopub_instead_of_dropping() -> None:
    """Make the running kernel queue IOPub messages instead of dropping them.

    Building the wizard opens about 1,350 widgets: some 6,500 IOPub messages in
    a couple of seconds. ipykernel's IOPub socket keeps libzmq's default send
    high-water mark of 1,000 messages and silently drops messages past it once
    a subscriber falls behind, as voila does on a busy machine. When the cell's
    final ``idle`` status is among them, voila waits out its IOPub timeout and
    serves a blank page; no timeout brings a dropped message back. SNDHWM=0
    (no limit) makes the socket queue them instead.

    The option is set on ipykernel's IOPub thread, which owns the socket (zmq
    sockets are not thread-safe), through ``IOPubThread.schedule``. That queue
    runs in order with the queued sends, so the limit is lifted before any
    widget message goes out. libzmq applies a changed high-water mark to
    pipes that are already connected (checked on libzmq 4.3.5), so voila's
    existing subscription is covered.

    A no-op outside an ipykernel kernel (tests, plain Python). It never raises:
    the wizard must still build if ipykernel's internals change.
    """
    try:
        import zmq
        from IPython import get_ipython

        iopub_thread = getattr(
            getattr(get_ipython(), "kernel", None), "iopub_thread", None
        )
        if iopub_thread is None:
            return

        def lift_limit() -> None:
            # Runs on the IOPub thread; an exception here would stop it from
            # processing the sends queued behind this call.
            try:
                iopub_thread.socket.setsockopt(zmq.SNDHWM, 0)
            except Exception:
                pass

        iopub_thread.schedule(lift_limit)
    except Exception:
        return


def blueprint_app(catalog_root: str | None = None) -> AppShell:
    """Build an :class:`AppShell` with a single "Blueprint" page.

    Imports ``ForgeBlueprintWizardApp`` lazily (it, in turn, lazily imports
    ipywidgets).
    """
    from cstar.wizard.wizard import ForgeBlueprintWizardApp

    queue_iopub_instead_of_dropping()
    app = ForgeBlueprintWizardApp(catalog_root)
    return AppShell([("Blueprint", app)])


def app(catalog_root: str | None = None) -> AppShell:
    """Build the two-page wizard :class:`AppShell`: "Blueprint" and "Workplan".

    The Workplan page shares the Blueprint page's app (and so its catalog).
    Imports are lazy (both pages, in turn, lazily import ipywidgets).
    """
    from cstar.wizard.wizard import ForgeBlueprintWizardApp, WorkplanHooks
    from cstar.wizard.workplan_builder import WorkplanBuilderPage

    queue_iopub_instead_of_dropping()
    blueprint_page = ForgeBlueprintWizardApp(catalog_root)
    workplan_page = WorkplanBuilderPage(blueprint_page)
    shell = AppShell([("Blueprint", blueprint_page), ("Workplan", workplan_page)])

    def open_steps(blueprint_path: Path, name: str) -> None:
        workplan_page.add_forge_then_run(blueprint_path, name)
        shell.show("Workplan")

    blueprint_page.set_workplan_hooks(
        WorkplanHooks(open_steps=open_steps, saved=workplan_page.refresh_saved)
    )
    return shell
