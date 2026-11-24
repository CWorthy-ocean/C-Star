"""Tests for the wizard behind its catalog bar (cstar.wizard.app), and for the
Blueprint page the shell builds around it (``cstar.wizard.ui.shell.blueprint_app``).
"""

from cstar.wizard.app import ForgeBlueprintWizardApp
from cstar.wizard.wizard import ForgeBlueprintWizard, WorkplanHooks


def test_wizard_app_widget_is_outer_and_holds_a_catalog_bar():
    from cstar.wizard.app import ForgeBlueprintWizardApp

    app = ForgeBlueprintWizardApp()
    assert app.widget is app._outer
    assert app._bar.widget in app._outer.children
    assert "forge-catalog-bar" in app._bar.widget._dom_classes


def test_catalog_bar_loads_a_single_url_as_one_read_only_store(monkeypatch):
    """A URL typed in the catalog bar is one store, not split at its ``:``."""
    import ipywidgets as w

    from cstar.wizard import wizard as wizard_module
    from cstar.wizard.app import ForgeBlueprintWizardApp

    app = ForgeBlueprintWizardApp()
    store_calls = []
    stack_calls = []
    store = object()

    def fake_store(**kwargs):
        store_calls.append(kwargs)
        return store

    def fake_stack(entries):
        stack_calls.append(entries)
        return object()

    class FakeWizard:
        def __init__(self, catalog):
            self.catalog = catalog
            self.widget = w.Label("inner")

        def set_workplan_hooks(self, hooks):
            pass

    monkeypatch.setattr("cstar.catalog.domain_catalog.DomainCatalog", fake_store)
    monkeypatch.setattr("cstar.catalog.domain_catalog.build_catalog_stack", fake_stack)
    monkeypatch.setattr(wizard_module, "ForgeBlueprintWizard", FakeWizard)
    monkeypatch.setattr(app._bar, "set_status_for", lambda cat: None)

    app._load("https://github.com/o/r")

    assert store_calls == [{"catalog_root": "https://github.com/o/r"}]
    assert stack_calls == []
    assert app.inner.catalog is store


def test_blueprint_app_from_shell_builds():
    from cstar.wizard.ui.shell import blueprint_app

    shell = blueprint_app()
    assert shell.stack.children  # must not raise, and must hold the page


def test_open_in_builder_is_hidden_without_hooks(tmp_path):
    assert ForgeBlueprintWizard().open_workplan_btn.layout.display == "none"
    app = ForgeBlueprintWizardApp(str(tmp_path / "catalog"))
    assert app.inner is not None
    assert app.inner.open_workplan_btn.layout.display == "none"
    hooks = WorkplanHooks(open_steps=lambda p, n: None, saved=lambda: None)
    app.set_workplan_hooks(hooks)
    assert app.inner.open_workplan_btn.layout.display == ""
    app._load(str(tmp_path / "catalog"))
    assert app.inner._workplan_hooks is hooks
    assert app.inner.open_workplan_btn.layout.display == ""
