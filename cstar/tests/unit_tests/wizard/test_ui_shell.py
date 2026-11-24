"""Tests for the wizard's outer application shell (cstar.wizard.ui.shell)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("ipywidgets")

import ipywidgets as W
import zmq

from cstar.base.utils import slugify
from cstar.wizard.ui import shell as shell_module
from cstar.wizard.ui.shell import (
    AppShell,
    app,
    blueprint_app,
    queue_iopub_instead_of_dropping,
)


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
    assert "<style " in style.value  # the tag carries an onload attribute
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

    from cstar.wizard.ui import components
    from cstar.wizard.ui.shell import AppShell

    page = W.VBox([components.style_widget(W), W.HTML("body")])
    shell = AppShell([("Only", page)], W=W)
    styles = [c for c in shell.root.children if "forge-style" in c._dom_classes]
    assert styles == []
    assert len(shell.root.children) == 3  # header, nav, stack

    bare = AppShell([("Only", W.HTML("body"))], W=W)
    assert sum("forge-style" in c._dom_classes for c in bare.root.children) == 1


def test_app_builds_blueprint_and_workplan_pages():
    shell = app()
    assert isinstance(shell, AppShell)
    assert shell.nav.layout.display != "none"
    assert list(shell.nav.options) == ["Blueprint", "Workplan"]
    assert len(shell.stack.children) == 2
    assert shell.pages[1][1].blueprint_app is shell.pages[0][1]


def _fake_kernel_shell() -> SimpleNamespace:
    """A stand-in for ``get_ipython()`` in a kernel, down to the IOPub socket."""
    iopub_thread = SimpleNamespace(schedule=MagicMock(), socket=MagicMock())
    return SimpleNamespace(kernel=SimpleNamespace(iopub_thread=iopub_thread))


def test_iopub_limit_lifted_on_the_iopub_thread():
    # The wizard's ~6,500 widget messages overrun libzmq's default 1,000-message
    # send limit and the kernel drops some, including the final idle that voila
    # waits for (blank page). The limit must be lifted from the IOPub thread,
    # which owns the socket, not set directly from the calling thread.
    ip = _fake_kernel_shell()
    iopub_thread = ip.kernel.iopub_thread
    with patch("IPython.get_ipython", return_value=ip):
        queue_iopub_instead_of_dropping()
    iopub_thread.socket.setsockopt.assert_not_called()
    iopub_thread.schedule.assert_called_once()
    scheduled = iopub_thread.schedule.call_args.args[0]
    scheduled()
    iopub_thread.socket.setsockopt.assert_called_once_with(zmq.SNDHWM, 0)


def test_iopub_limit_is_a_noop_outside_a_kernel():
    with patch("IPython.get_ipython", return_value=None):
        queue_iopub_instead_of_dropping()
    with patch("IPython.get_ipython", return_value=SimpleNamespace()):
        queue_iopub_instead_of_dropping()


def test_iopub_limit_never_raises():
    # The wizard must still build if ipykernel's internals change; and the
    # scheduled call must not raise on the IOPub thread, where it would stop
    # the sends queued behind it.
    ip = _fake_kernel_shell()
    iopub_thread = ip.kernel.iopub_thread
    iopub_thread.schedule.side_effect = RuntimeError("no thread")
    with patch("IPython.get_ipython", return_value=ip):
        queue_iopub_instead_of_dropping()

    ip = _fake_kernel_shell()
    iopub_thread = ip.kernel.iopub_thread
    iopub_thread.socket.setsockopt.side_effect = zmq.ZMQError()
    with patch("IPython.get_ipython", return_value=ip):
        queue_iopub_instead_of_dropping()
    iopub_thread.schedule.call_args.args[0]()


@pytest.mark.parametrize("build", [app, blueprint_app])
def test_app_lifts_iopub_limit(build):
    with patch.object(shell_module, "queue_iopub_instead_of_dropping") as lift:
        build()
    lift.assert_called_once_with()


def test_open_in_the_workplan_builder_adds_the_steps_and_switches_page(tmp_path):
    shell = app(catalog_root=str(tmp_path / "catalog"))
    blueprint_page = shell.pages[0][1]
    wiz = blueprint_page.inner
    assert Path(wiz.catalog.workplans_dir).is_relative_to(tmp_path)
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True
    wiz._on_save(None)
    wiz._on_open_workplan(None)
    assert shell.nav.index == 1
    assert shell.stack.selected_index == 1
    page = shell.pages[1][1]
    assert [p.name.value for p in page.panes] == ["forge", "ucla_roms"]
    assert page.name.value == slugify(wiz.config.name)
