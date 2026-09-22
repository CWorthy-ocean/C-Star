"""Tests for third-party CLI plugin discovery via the ``cstar.cli`` entry-point group."""

from unittest.mock import patch

import typer

from cstar.cli.cli import attach_plugin_subcommands
from cstar.tests.unit_tests.orchestration.cli.conftest import FakeEntryPoint


def registered_names(app: typer.Typer) -> set[str]:
    return {g.name for g in app.registered_groups}


def test_plugin_is_attached():
    app = typer.Typer()
    plugin = typer.Typer()

    @plugin.command()
    def hello():  # pragma: no cover - registration is what matters
        pass

    eps = [FakeEntryPoint("widget", loaded=plugin)]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken={"blueprint"})

    assert registered_names(app) == {"widget"}


def test_colliding_plugin_is_skipped():
    app = typer.Typer()
    eps = [FakeEntryPoint("blueprint", loaded=typer.Typer())]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken={"blueprint"})

    assert registered_names(app) == set()


def test_forge_plugin_now_collides_with_the_core_command():
    # `forge` moved from an entry-point plugin to a core subcommand (see
    # `attach_subcommands`); a third-party plugin trying to claim the name
    # must be skipped just like any other core-name collision.
    app = typer.Typer()
    eps = [FakeEntryPoint("forge", loaded=typer.Typer())]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken={"forge"})

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
        FakeEntryPoint("widget", loaded=first),
        FakeEntryPoint("widget", loaded=second),
    ]
    with patch("cstar.cli.cli.entry_points", return_value=eps):
        attach_plugin_subcommands(app, taken=set())

    assert registered_names(app) == set()
