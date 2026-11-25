"""The wizard behind a catalog-location bar (:class:`ForgeBlueprintWizardApp`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cstar.base.env import (
    is_bundled_catalog_entry,
    is_remote_catalog_entry,
    split_catalog,
)
from cstar.wizard.ui.catalog_bar import CatalogBar

if TYPE_CHECKING:
    from cstar.wizard.review_card import WorkplanHooks
    from cstar.wizard.wizard import ForgeBlueprintWizard


class ForgeBlueprintWizardApp:
    """Thin wrapper around :class:`ForgeBlueprintWizard` that adds a catalog-location
    bar above it. Blank input loads the default layered stack (your writable
    ``~/cstar/catalog`` -- or ``CSTAR_CATALOG`` -- layer over the
    read-only bundled in-repo catalog, see
    :func:`~cstar.catalog.domain_catalog.default_catalog_stack`). Entering one or more
    ``os.pathsep``-separated local paths builds a
    :class:`~cstar.catalog.domain_catalog.LayeredCatalog` exactly like the same value
    in ``CSTAR_CATALOG`` would (first = writable top, rest read-only, bundled
    catalog appended at the bottom). Entering a single GitHub/http URL or the
    literal ``"local"`` loads exactly that one store, read-only, for browsing
    (saves then default to CWD-relative filenames, and the status line says so).
    Clicking Reload rebuilds the wizard against the new catalog.

    Usage (in a Jupyter notebook)::

        from cstar.wizard.wizard import ForgeBlueprintWizardApp
        app = ForgeBlueprintWizardApp()
        app.display()
        # ... optionally enter a different catalog path/URL above and click Reload ...
        cfg = app.inner.config       # the current resolved ForgeBlueprint (or None)
    """

    def __init__(self, catalog_root: str | None = None):
        import ipywidgets as W

        self.W = W
        self.inner: ForgeBlueprintWizard | None = None
        self._workplan_hooks: WorkplanHooks | None = None

        self._bar = CatalogBar(W, on_reload=self._load)

        self._outer = W.VBox([])
        self._load(catalog_root)

    @property
    def widget(self) -> Any:
        """The app's root widget: the catalog bar above the wizard's own widget."""
        return self._outer

    def set_workplan_hooks(self, hooks: WorkplanHooks | None) -> None:
        """Connect the wizard (now and after each Reload) to a Workplan page."""
        self._workplan_hooks = hooks
        if self.inner is not None:
            self.inner.set_workplan_hooks(hooks)

    def _load(self, catalog_root_value: str | None) -> None:
        from cstar.catalog.domain_catalog import (
            DomainCatalog,
            LayeredCatalog,
            build_catalog_stack,
            default_catalog_stack,
        )

        # Imported here, not at module level: cstar.wizard.wizard re-exports
        # this class, so a module-level import would be circular.
        from cstar.wizard.wizard import ForgeBlueprintWizard

        val = (catalog_root_value or "").strip()
        try:
            cat: DomainCatalog | LayeredCatalog
            if not val:
                # Blank -> the default layered stack (writable user layer over
                # the read-only bundled catalog), not the bundled catalog alone.
                cat = default_catalog_stack()
            else:
                entries = split_catalog(val)
                if len(entries) == 1 and (
                    is_remote_catalog_entry(entries[0])
                    or is_bundled_catalog_entry(entries[0])
                ):
                    # A remote URL or the literal "local" (bundled catalog)
                    # can never be a writable top layer, so load it as exactly
                    # one read-only store for browsing.
                    cat = DomainCatalog(catalog_root=entries[0])
                else:
                    # Same builder as the CSTAR_CATALOG env handling
                    # (first entry writable top, rest read-only, bundled
                    # appended at the bottom) so the two paths cannot drift --
                    # a single local path gets the bundled layer underneath,
                    # exactly like the same value in the env var.
                    cat = build_catalog_stack(entries)
            inner = ForgeBlueprintWizard(catalog=cat)
        except Exception as exc:
            self._bar.set_error(val, exc)
            return

        inner.set_workplan_hooks(self._workplan_hooks)
        self.inner = inner
        self._bar.set_status_for(cat)
        self._outer.children = [self._bar.widget, inner.widget]

    def display(self):
        from IPython.display import display

        display(self._outer)
