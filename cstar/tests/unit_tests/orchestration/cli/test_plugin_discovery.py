"""Tests for third-party CLI plugin discovery via the ``cstar.cli`` entry-point group."""

from unittest.mock import patch

import typer

from cstar.cli.cli import attach_plugin_subcommands


class FakeEntryPoint:
    """Stand-in for importlib.metadata.EntryPoint with a controllable load()."""

    def __init__(self, name, loaded=None, error=None):
        self.name = name
        self.value = f"fake_pkg.cli:{name}"
        self._loaded = loaded
        self._error = error

    def load(self):
        if self._error is not None:
            raise self._error
        return self._loaded


def registered_names(app: typer.Typer) -> set[str]:
    return {g.name for g in app.registered_groups}


def test_plugin_is_attached():
    app = typer.Typer()
    plugin = typer.Typer()

    @plugin.command()
    def hello():  # pragma: no cover - registration is what matters
        pass

    eps = [FakeEntryPoint("forge", loaded=plugin)]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken={"blueprint"})

    assert registered_names(app) == {"forge"}


def test_colliding_plugin_is_skipped():
    app = typer.Typer()
    eps = [FakeEntryPoint("blueprint", loaded=typer.Typer())]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken={"blueprint"})

    assert registered_names(app) == set()


def test_failing_plugin_does_not_break_cli():
    app = typer.Typer()
    good = typer.Typer()
    eps = [
        FakeEntryPoint("broken", error=ImportError("boom")),
        FakeEntryPoint("good", loaded=good),
    ]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken=set())

    assert registered_names(app) == {"good"}


def test_non_typer_plugin_is_skipped():
    app = typer.Typer()
    eps = [FakeEntryPoint("notatyper", loaded=object())]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken=set())

    assert registered_names(app) == set()


def test_duplicate_plugin_names_both_dropped():
    # Two plugins claiming the same name are BOTH skipped: discovery iterates a
    # set (no meaningful "first"), so dropping both is the deterministic policy.
    app = typer.Typer()
    first, second = typer.Typer(), typer.Typer()
    eps = [
        FakeEntryPoint("forge", loaded=first),
        FakeEntryPoint("forge", loaded=second),
    ]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken=set())

    assert registered_names(app) == set()
