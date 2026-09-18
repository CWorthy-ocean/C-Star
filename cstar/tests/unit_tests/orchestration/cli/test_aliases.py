"""Tests for the CLI command aliases: `wp`, `bp`, and `list`."""

from collections.abc import Callable, Generator
from unittest.mock import patch

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner

from cstar.cli.blueprint import ALIAS as ALIAS_BLUEPRINT
from cstar.cli.cli import attach_subcommands
from cstar.cli.workplan import ALIAS as ALIAS_WORKPLAN
from cstar.cli.workplan.ls import ALIAS as ALIAS_LS
from cstar.cli.workplan.ls import app as app_ls
from cstar.tests.unit_tests.orchestration.cli.conftest import FakeEntryPoint

SUBCOMMAND_ALIASES = (("workplan", ALIAS_WORKPLAN), ("blueprint", ALIAS_BLUEPRINT))


@pytest.fixture
def build_app() -> Generator[Callable[[], typer.Typer]]:
    """Build a root app with only the core subcommands and their aliases.

    Entry-point discovery is stubbed out so a third-party `cstar.cli` plugin
    installed alongside C-Star cannot change what is attached.
    """
    with patch("cstar.cli.cli.entry_points", return_value=[]):

        def _build() -> typer.Typer:
            app = typer.Typer()
            attach_subcommands(app)
            return app

        yield _build


def test_subcommand_aliases_resolve_to_the_same_app(build_app):
    """`wp`/`bp` expose the same subcommand tree as `workplan`/`blueprint`."""
    root = get_command(build_app())

    for primary, alias in SUBCOMMAND_ALIASES:
        assert set(root.commands[alias].commands) == set(
            root.commands[primary].commands
        )


def test_subcommand_aliases_are_hidden_from_help(build_app):
    """Aliases stay out of `--help` so the listing shows one entry per command."""
    root = get_command(build_app())

    for primary, alias in SUBCOMMAND_ALIASES:
        assert root.commands[alias].hidden
        assert not root.commands[primary].hidden


@pytest.mark.parametrize(
    ("primary", "alias"),
    [*SUBCOMMAND_ALIASES, ("ls", ALIAS_LS)],
    ids=lambda v: v,
)
def test_help_text_advertises_the_registered_alias(build_app, primary, alias):
    """Aliases are hidden from `--help`, so each is named in its own help text.

    That text is written by hand, so assert it matches the alias actually
    registered rather than a stale copy of it.
    """
    root = get_command(build_app())
    workplan = root.commands["workplan"]
    command = workplan.commands[primary] if primary == "ls" else root.commands[primary]

    assert f"(alias: {alias})" in (command.help or "")


def test_ls_alias_is_the_same_command():
    """`list` is a second registration of `ls`, not a wrapper, so the options match."""
    registered = {c.name: c for c in app_ls.registered_commands}

    assert registered["list"].callback is registered["ls"].callback
    assert registered["list"].hidden
    assert not registered["ls"].hidden


@pytest.mark.parametrize(
    "argv",
    [["workplan", "ls"], ["workplan", "list"], ["wp", "ls"], ["wp", "list"]],
    ids=lambda argv: " ".join(argv),
)
def test_aliases_dispatch(build_app, argv):
    """Every alias path reaches the same underlying command."""

    def close_unawaited(coro):
        """Close the unawaited pipeline coroutine to keep the mock warning-free."""
        coro.close()
        return []

    with patch("cstar.cli.workplan.ls.asyncio.run", side_effect=close_unawaited) as run:
        result = CliRunner().invoke(build_app(), argv)

    assert result.exit_code == 0, result.output
    run.assert_called_once()


def test_alias_names_are_reserved_against_plugins():
    """A plugin may not shadow an alias any more than it may shadow a command."""
    plugin = FakeEntryPoint(ALIAS_WORKPLAN, loaded=typer.Typer())

    app = typer.Typer()
    with patch("cstar.cli.cli.entry_points", return_value=[plugin]):
        attach_subcommands(app)

    aliased = [g for g in app.registered_groups if g.name == ALIAS_WORKPLAN]
    assert len(aliased) == 1, f"the plugin shadowed the `{ALIAS_WORKPLAN}` alias"

    workplan = next(g for g in app.registered_groups if g.name == "workplan")
    assert aliased[0].typer_instance is workplan.typer_instance
