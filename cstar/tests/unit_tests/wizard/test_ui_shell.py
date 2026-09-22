"""Tests for the wizard's outer application shell (cstar_forge.ui.shell)."""

from __future__ import annotations

import pytest

pytest.importorskip("ipywidgets")

import ipywidgets as W

from cstar_forge.ui.shell import AppShell, blueprint_app


class _DummyPage:
    """A page object exposing ``.widget`` (duck-typed, not a raw widget)."""

    def __init__(self, text: str):
        self.widget = W.HTML(text)


def test_single_page_hides_nav():
    shell = AppShell([("Only", _DummyPage("hello"))])
    assert shell.nav.layout.display == "none"
    assert len(shell.stack.children) == 1


def test_two_pages_show_nav_and_two_stack_children():
    shell = AppShell(
        [("A", _DummyPage("page a")), ("B", _DummyPage("page b"))],
    )
    assert shell.nav.layout.display != "none"
    assert len(shell.stack.children) == 2
    assert list(shell.nav.options) == ["A", "B"]


def test_jslink_object_exists():
    shell = AppShell([("A", _DummyPage("a")), ("B", _DummyPage("b"))])
    assert isinstance(shell._link, W.widget_link.Link)


def test_page_widget_is_duck_typed_via_widget_attribute():
    page = _DummyPage("hello")
    shell = AppShell([("Only", page)])
    assert shell.stack.children[0] is page.widget


def test_page_accepts_a_raw_widget_directly():
    raw = W.HTML("raw widget, no .widget attribute")
    shell = AppShell([("Only", raw)])
    assert shell.stack.children[0] is raw


def test_root_has_forge_app_class():
    shell = AppShell([("Only", _DummyPage("hello"))])
    assert "forge-app" in shell.root._dom_classes


def test_root_contains_style_header_nav_and_stack():
    shell = AppShell([("Only", _DummyPage("hello"))])
    assert len(shell.root.children) == 4
    style, header, nav, stack = shell.root.children
    assert nav is shell.nav
    assert stack is shell.stack
    assert "<style>" in style.value
    assert "<style>" in header.value  # header carries header_css


def test_appshell_lazy_imports_ipywidgets_when_w_omitted():
    shell = AppShell([("Only", _DummyPage("hello"))], W=None)
    # No W was passed in, yet the shell still built real ipywidgets widgets --
    # proof it imported the module itself rather than requiring the caller to.
    assert isinstance(shell.root, W.VBox)
    assert shell.W.__name__ == "ipywidgets"


def test_blueprint_app_builds_without_error():
    shell = blueprint_app()
    assert isinstance(shell, AppShell)
    assert shell.nav.layout.display == "none"
    assert len(shell.stack.children) == 1


def test_shell_skips_its_own_stylesheet_when_a_page_carries_one():
    """The wizard root embeds WIZARD_CSS itself; the shell must not add a duplicate."""
    import ipywidgets as W

    from cstar_forge.ui import components
    from cstar_forge.ui.shell import AppShell

    page = W.VBox([components.style_widget(W), W.HTML("body")])
    shell = AppShell([("Only", page)], W=W)
    styles = [c for c in shell.root.children if "forge-style" in c._dom_classes]
    assert styles == []
    assert len(shell.root.children) == 3  # header, nav, stack

    bare = AppShell([("Only", W.HTML("body"))], W=W)
    assert sum("forge-style" in c._dom_classes for c in bare.root.children) == 1
