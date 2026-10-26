"""Tests for the CLI command aliases (`wp`, `bp`, `list`, `run`) and the `run` group."""

import inspect
import logging
import warnings
from collections.abc import Callable, Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner, Result

from cstar.base.deprecation import CstarDeprecationWarning, deprecated
from cstar.cli.blueprint import ALIAS as ALIAS_BLUEPRINT
from cstar.cli.cli import attach_subcommands
from cstar.cli.common import common_callback
from cstar.cli.run import NAME as NAME_RUN
from cstar.cli.run import RUN_LEVEL_VERBS
from cstar.cli.run import app as app_run
from cstar.cli.run.attach import COMMAND_ATTACH
from cstar.cli.run.check import COMMAND_CHECK
from cstar.cli.run.check import app as app_run_check
from cstar.cli.run.promote import COMMAND_PROMOTE
from cstar.cli.run.show import COMMAND_SHOW
from cstar.cli.workplan import ALIAS as ALIAS_WORKPLAN
from cstar.cli.workplan import app as app_workplan
from cstar.cli.workplan.check import app as app_workplan_check
from cstar.cli.workplan.gather import app as app_gather
from cstar.cli.workplan.log import app as app_log
from cstar.cli.workplan.ls import ALIAS as ALIAS_LS
from cstar.cli.workplan.ls import app as app_ls
from cstar.cli.workplan.path import app as app_path
from cstar.cli.workplan.run import ALIAS as ALIAS_SUBMIT
from cstar.cli.workplan.run import COMMAND_SUBMIT
from cstar.cli.workplan.run import app as app_workplan_run
from cstar.cli.workplan.status import app as app_status
from cstar.entrypoint.utils import ARG_RUN_ID, ARG_VAR_LONG, ARG_VARFILE_LONG
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.cli.conftest import FakeEntryPoint

RUN_ONLY_COMMANDS = {COMMAND_ATTACH, COMMAND_CHECK, COMMAND_PROMOTE, COMMAND_SHOW}
"""The commands `cstar run` serves that `cstar workplan` does not."""

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
    [*SUBCOMMAND_ALIASES, ("ls", ALIAS_LS), (COMMAND_SUBMIT, ALIAS_SUBMIT)],
    ids=lambda v: v,
)
def test_help_text_advertises_the_registered_alias(build_app, primary, alias):
    """Aliases are hidden from `--help`, so each is named in its own help text.

    That text is written by hand, so assert it matches the alias actually
    registered rather than a stale copy of it.
    """
    root = get_command(build_app())
    workplan = root.commands["workplan"]
    group = workplan if primary in {"ls", COMMAND_SUBMIT} else root
    command = group.commands[primary]

    assert f"(alias: {alias})" in (command.help or "")


def test_ls_alias_is_the_same_command():
    """`list` is a second registration of `ls`, not a wrapper, so the options match."""
    registered = {c.name: c for c in app_ls.registered_commands}

    assert registered["list"].callback is registered["ls"].callback
    assert registered["list"].hidden
    assert not registered["ls"].hidden


def test_submit_alias_is_the_same_command():
    """`run` is a second registration of `submit`, wrapped only to announce its
    deprecation, so `--run-id`, `--resume` and the re-entry forms work under both
    names.
    """
    (listed,) = app_workplan_run.registered_commands
    alias = next(c for c in app_workplan.registered_commands if c.name == ALIAS_SUBMIT)

    assert listed.name == COMMAND_SUBMIT
    assert alias.callback is not None
    assert inspect.unwrap(alias.callback) is listed.callback
    assert inspect.signature(alias.callback) == inspect.signature(listed.callback)
    assert alias.hidden
    assert not listed.hidden


@pytest.mark.parametrize(
    "argv",
    [["workplan", "ls"], ["workplan", "list"], ["wp", "ls"], ["wp", "list"]],
    ids=lambda argv: " ".join(argv),
)
@pytest.mark.filterwarnings("ignore::cstar.base.deprecation.CstarDeprecationWarning")
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
"""The sub-apps that implement the run-level verbs."""


def attached(group: typer.Typer) -> set[typer.Typer]:
    """The sub-apps merged into a group."""
    return {g.typer_instance for g in group.registered_groups if g.typer_instance}


def test_run_attaches_the_run_level_sub_apps():
    """`run` is the listed home of the run-level verbs."""
    assert set(RUN_LEVEL_APPS) <= attached(app_run)


@pytest.mark.parametrize("verb", sorted(RUN_LEVEL_VERBS))
def test_workplan_spelling_of_a_run_level_verb_is_the_same_command(verb):
    """`cstar workplan <verb>` runs the very callback `cstar run <verb>` does, with
    the same help, so the two spellings cannot drift apart.
    """
    run_command = get_command(app_run).commands[verb]
    workplan_command = get_command(app_workplan).commands[verb]

    assert workplan_command.callback is not None
    assert run_command.callback is not None
    assert inspect.unwrap(workplan_command.callback) is inspect.unwrap(
        run_command.callback
    )
    assert workplan_command.help == run_command.help
    assert [p.name for p in workplan_command.params] == [
        p.name for p in run_command.params
    ]


def test_workplan_lists_only_its_own_commands():
    """`cstar workplan --help` lists `submit` and `check`; the run-level verbs and
    the `run` alias stay registered but unlisted, while `cstar run` lists the
    verbs.
    """
    workplan = get_command(app_workplan).commands
    run = get_command(app_run).commands
    listed = {name for name, command in workplan.items() if not command.hidden}

    assert {COMMAND_SUBMIT, "check"} <= listed
    assert listed.isdisjoint(RUN_LEVEL_VERBS | {ALIAS_SUBMIT})
    assert {name for name in RUN_LEVEL_VERBS if not run[name].hidden} == (
        RUN_LEVEL_VERBS - {ALIAS_LS}
    )


def test_workplan_help_output_omits_the_hidden_spellings(build_app):
    """The rendered `cstar workplan --help` names the listed commands only."""
    result = CliRunner().invoke(build_app(), ["workplan", "--help"])

    assert result.exit_code == 0, result.output
    assert COMMAND_SUBMIT in result.output
    for hidden in ("status", "gather", "log", "ls"):
        assert f" {hidden} " not in result.output


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
    besides `check` and `show`, and `workplan` still serves each of them.
    """
    run_commands = set(get_command(app_run).commands)
    workplan_commands = set(get_command(app_workplan).commands)

    assert run_commands == RUN_LEVEL_VERBS | RUN_ONLY_COMMANDS
    assert RUN_LEVEL_VERBS <= workplan_commands
    assert {COMMAND_CHECK, COMMAND_SUBMIT, ALIAS_SUBMIT} <= workplan_commands


def test_run_group_is_attached_to_the_root(build_app):
    """`run` is a visible root command with no alias."""
    root = get_command(build_app())

    assert NAME_RUN in root.commands
    assert not root.commands[NAME_RUN].hidden
    assert set(root.commands[NAME_RUN].commands) == RUN_LEVEL_VERBS | RUN_ONLY_COMMANDS


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
    # at the transformed workplan, which it fails to load and reports without failing.
    # The `ls` stub replaces `asyncio.run` on the shared module object, so the
    # record lookup gets its own loop stub.
    (tmp_path / "x_transformed.yaml").touch()
    record = WorkplanRun(
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


RUN_LEVEL_ARGV = (["status", "x"], ["ls"], ["list"])
"""Run-level verbs with arguments that run them to completion under `invoke`."""


@pytest.mark.parametrize("argv", RUN_LEVEL_ARGV, ids=" ".join)
def test_run_level_verbs_are_silent_under_run(invoke, argv, caplog):
    """`cstar run <verb>` runs with no deprecation."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", CstarDeprecationWarning)
        with caplog.at_level(logging.WARNING):
            result = invoke(["run", *argv])

    assert result.exit_code == 0, result.output
    assert not caplog.records


@pytest.mark.parametrize(
    "argv",
    [[group, *verb] for group in ("workplan", "wp") for verb in RUN_LEVEL_ARGV],
    ids=" ".join,
)
def test_workplan_spelling_of_a_run_level_verb_is_deprecated(invoke, argv, caplog):
    """`cstar workplan <verb>` still runs the command, after one warning that names
    the spelling typed, `cstar run <verb>` and the removal release.
    """
    group, verb = argv[:2]
    with pytest.warns(CstarDeprecationWarning), caplog.at_level(logging.WARNING):
        result = invoke(argv)

    assert result.exit_code == 0, result.output
    [record] = caplog.records
    assert record.getMessage() == (
        f"`cstar {group} {verb}` is deprecated and stops working in cstar-ocean "
        f"0.20.0; use `cstar run {verb}` instead."
    )


@pytest.mark.parametrize("group", ["workplan", "wp"])
def test_run_alias_of_submit_is_deprecated(build_app, group, tmp_path, caplog):
    """`cstar workplan run` names `cstar workplan submit` before `submit` runs (here
    to its first check, which refuses `--var` with `--varfile`).
    """
    varfile = tmp_path / "vars.env"
    varfile.write_text("a=b\n")
    argv = [group, ALIAS_SUBMIT, ARG_RUN_ID, "x", ARG_VAR_LONG, "a=b"]
    argv += [ARG_VARFILE_LONG, str(varfile)]
    with pytest.warns(CstarDeprecationWarning), caplog.at_level(logging.WARNING):
        result = CliRunner().invoke(build_app(), argv)

    assert result.exit_code == 2
    assert "must not be supplied together" in result.output
    [record] = caplog.records
    assert record.getMessage() == (
        f"`cstar {group} {ALIAS_SUBMIT}` is deprecated and stops working in "
        f"cstar-ocean 0.20.0; use `cstar workplan {COMMAND_SUBMIT}` instead."
    )


def test_cli_reports_a_deprecation_by_its_log_line_alone(monkeypatch, caplog):
    """The root callback silences the Python warning `deprecated` emits beside its
    log line, so the terminal shows one line.
    """
    monkeypatch.setattr("cstar.cli.common.load_user_config", lambda: None)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        common_callback(MagicMock())
        with caplog.at_level(logging.WARNING):
            deprecated("The 'x' key", "'y'", removal="0.20.0")

    assert not record
    assert len(caplog.records) == 1


def test_workplan_help_names_the_run_group(build_app):
    """The group help says where else the run-level commands are found."""
    root = get_command(build_app())

    assert "`cstar run <command>`" in (root.commands["workplan"].help or "")
