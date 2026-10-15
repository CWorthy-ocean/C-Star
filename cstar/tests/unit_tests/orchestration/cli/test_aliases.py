"""Tests for the CLI command aliases (`wp`, `bp`, `list`) and the `run` group."""

from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner, Result

from cstar.cli.blueprint import ALIAS as ALIAS_BLUEPRINT
from cstar.cli.cli import attach_subcommands
from cstar.cli.run import NAME as NAME_RUN
from cstar.cli.run import RUN_LEVEL_VERBS
from cstar.cli.run import app as app_run
from cstar.cli.run.check import COMMAND_CHECK
from cstar.cli.run.check import app as app_run_check
from cstar.cli.workplan import ALIAS as ALIAS_WORKPLAN
from cstar.cli.workplan import app as app_workplan
from cstar.cli.workplan.check import app as app_workplan_check
from cstar.cli.workplan.gather import app as app_gather
from cstar.cli.workplan.log import app as app_log
from cstar.cli.workplan.ls import ALIAS as ALIAS_LS
from cstar.cli.workplan.ls import app as app_ls
from cstar.cli.workplan.path import app as app_path
from cstar.cli.workplan.run import app as app_workplan_run
from cstar.cli.workplan.status import app as app_status
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.tracking import WorkplanRun
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


RUN_LEVEL_APPS = (app_status, app_log, app_gather, app_path, app_ls)
"""The sub-apps that implement the run-level verbs, shared by both groups."""


def attached(group: typer.Typer) -> set[typer.Typer]:
    """The sub-apps merged into a group."""
    return {g.typer_instance for g in group.registered_groups if g.typer_instance}


def test_run_and_workplan_share_the_run_level_sub_apps():
    """`run` serves the run-level verbs with the very objects `workplan` does,
    so the two spellings cannot drift apart.
    """
    assert set(RUN_LEVEL_APPS) <= attached(app_run)
    assert set(RUN_LEVEL_APPS) <= attached(app_workplan)


def test_run_attaches_check_but_not_the_workplan_verbs():
    """`cstar run check` inspects a run; launching and validating a workplan file
    stay under `workplan`.
    """
    assert app_run_check in attached(app_run)
    assert app_run_check not in attached(app_workplan)
    assert app_workplan_check not in attached(app_run)
    assert app_workplan_run not in attached(app_run)


def test_run_level_verbs_are_what_the_groups_attach():
    """The constant the workplan group warns on is exactly the verbs `run` serves
    besides `check`, and `workplan` still serves each of them.
    """
    run_commands = set(get_command(app_run).commands)
    workplan_commands = set(get_command(app_workplan).commands)

    assert run_commands == RUN_LEVEL_VERBS | {COMMAND_CHECK}
    assert RUN_LEVEL_VERBS <= workplan_commands
    assert {COMMAND_CHECK, NAME_RUN} <= workplan_commands


def test_run_group_is_attached_to_the_root(build_app):
    """`run` is a visible root command with no alias."""
    root = get_command(build_app())

    assert NAME_RUN in root.commands
    assert not root.commands[NAME_RUN].hidden
    assert set(root.commands[NAME_RUN].commands) == RUN_LEVEL_VERBS | {COMMAND_CHECK}


def test_run_name_is_reserved_against_plugins():
    """A plugin may not shadow `run` any more than another core command."""
    plugin = FakeEntryPoint(NAME_RUN, loaded=typer.Typer())

    app = typer.Typer()
    with patch("cstar.cli.cli.entry_points", return_value=[plugin]):
        attach_subcommands(app)

    named = [g for g in app.registered_groups if g.name == NAME_RUN]
    assert len(named) == 1, f"the plugin shadowed `{NAME_RUN}`"
    assert named[0].typer_instance is app_run


@pytest.fixture
def invoke(build_app, tmp_path: Path) -> Generator[Callable[[list[str]], Result]]:
    """Run an argv through the root app with the run lookups stubbed out."""

    def close_unawaited(coro):
        """Close the unawaited pipeline coroutine to keep the mock warning-free."""
        coro.close()
        return []

    # `status x` finds a record through the shared argument callback, then stops
    # at the missing transformed workplan, which it reports without failing.
    # The `ls` stub replaces `asyncio.run` on the shared module object, so the
    # record lookup gets its own loop stub.
    record = WorkplanRun(
        workplan_path=tmp_path / "x.yaml",
        trx_workplan_path=tmp_path / "x_transformed.yaml",
        output_path=tmp_path,
        run_id="x",
    )
    shared_asyncio = MagicMock()
    shared_asyncio.run.return_value = record

    with (
        patch("cstar.cli.workplan.ls.asyncio.run", side_effect=close_unawaited),
        patch("cstar.cli.workplan.shared.TrackingRepository"),
        patch("cstar.cli.workplan.shared.asyncio", shared_asyncio),
        patch.object(LiveWorkplan, "load", side_effect=FileNotFoundError),
    ):
        yield lambda argv: CliRunner().invoke(build_app(), argv)


@pytest.mark.parametrize(
    "argv",
    [
        [group, *verb]
        for group in ("run", "workplan", "wp")
        for verb in (["status", "x"], ["ls"], ["list"])
    ],
    ids=lambda argv: " ".join(argv),
)
def test_run_level_verbs_are_silent_under_every_group(invoke, argv):
    """Both spellings of a run-level verb run the same command with nothing on
    stderr: neither is deprecated.
    """
    result = invoke(argv)

    assert result.exit_code == 0, result.output
    assert result.stderr == ""


def test_workplan_help_names_the_run_group(build_app):
    """The group help says where else the run-level commands are found."""
    root = get_command(build_app())

    assert "`cstar run <command>`" in (root.commands["workplan"].help or "")
