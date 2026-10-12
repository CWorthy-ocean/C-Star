"""Tests for the `cstar run check` command."""

import os
import re
import shutil
import typing as t
from pathlib import Path
from unittest import mock

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.cli.run import app as app_run
from cstar.cli.run.check import COMMAND_CHECK
from cstar.entrypoint.utils import ARG_ALL, ARG_REPAIR
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.run_tree import (
    FakeRun,
    build_fake_run,
    link,
    write,
)

LOOKUP: t.Final[str] = "cstar.cli.workplan.shared.TrackingRepository.get_workplan_run"
"""The tracking lookup, replaced so that a run-id resolves to a chosen record."""

EXIT_CLEAN: t.Final[int] = 0
EXIT_FINDINGS: t.Final[int] = 1
EXIT_BAD_ARGS: t.Final[int] = 2

HEADINGS: t.Final[tuple[str, ...]] = (
    "Absolute paths in run files (",
    f"Symlinks with an absolute target inside the run (fixed by {ARG_REPAIR}) (",
    "Symlinks whose target does not exist (",
    "Steps without a directory (",
    "Symlinks with an absolute target outside the run (informational) (",
    "Task directories no step accounts for (informational) (",
    "Sentinel files no step accounts for (informational) (",
)
"""The sections of a report, in the order they are listed."""


@pytest.fixture
def run(tmp_path: Path, hello_world_bp_path: Path) -> FakeRun:
    """The fake run tree with one of everything."""
    return build_fake_run(hello_world_bp_path, tmp_path)


def invoke(*args: str, record: WorkplanRun | None = None) -> Result:
    """Run `cstar run check` against a repository that holds only `record`."""
    with mock.patch(LOOKUP, mock.AsyncMock(return_value=record)):
        return CliRunner().invoke(app_run, [COMMAND_CHECK, *args], color=False)


def small_run(root: Path) -> Path:
    """A run directory with two steps and no problem."""
    for name in ("spin-up", "run"):
        (root / "tasks" / name / "output").mkdir(parents=True)
    write(root / "tasks" / "run" / "output" / "x.yaml", "where: ../spin-up\n")
    return root


def test_a_run_id_reports_every_kind_of_finding(run: FakeRun) -> None:
    """Verify a tracked run is checked through its record, every section is
    listed in order with one line per finding, and the exit code says it failed.
    """
    result = invoke(run.record.run_id, record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    positions = [result.stdout.index(heading) for heading in HEADINGS]
    assert positions == sorted(positions)
    assert (
        f"{run.spin_up}/output/result.yaml:2: {run.spin_up}/output/a.nc"
        in result.stdout
    )
    assert (
        f"  {run.spin_up}/input/abs_file -> {run.spin_up}/output/a.nc" in result.stdout
    )
    assert f"  {run.spin_up}/input/rel_dangling -> ../nothing" in result.stdout
    assert f"  {run.root}/tasks/run\n" in result.stdout
    assert f"  {run.root}/tasks/stale\n" in result.stdout
    assert re.search(
        rf"\d+ problem\(s\) found in {re.escape(str(run.root))}, 3 informational",
        result.stdout,
    )
    assert "Skipped" not in result.stdout
    assert result.stderr == ""


def test_a_directory_is_checked_without_the_run_record(
    run: FakeRun, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a path, absolute or `.`, needs no tracking, finds the transformed
    workplan beside the tasks, and skips the checks that need a run-id.
    """
    absolute = invoke(str(run.root))
    monkeypatch.chdir(run.root)
    relative = invoke(".")

    for result in (absolute, relative):
        assert result.exit_code == EXIT_FINDINGS, result.output
        assert f"  {run.root}/tasks/run\n" in result.stdout
        assert f"  {run.root}/tasks/stale\n" in result.stdout
        assert "Sentinel files" not in result.stdout
        assert "Skipped orphaned sentinels: they are found by run-id" in result.stdout
    assert absolute.stdout == relative.stdout


def test_all_adds_the_launch_artifacts(run: FakeRun) -> None:
    """Verify the artifacts a launcher regenerates are scanned only on request."""
    default = invoke(str(run.root))
    everything = invoke(str(run.root), ARG_ALL)

    for result in (default, everything):
        assert result.exit_code == EXIT_FINDINGS, result.output
    artifacts = [f"{run.spin_up}/{x}:2:" for x in ("script.sh", "work/directives.yaml")]
    assert not any(a in default.stdout for a in artifacts)
    assert all(a in everything.stdout for a in artifacts)


def test_repair_makes_links_relative_and_keeps_failing_on_the_rest(
    run: FakeRun,
) -> None:
    """Verify `--repair` lists what it rewrote, reports only what remains,
    rebuilds `gathered_output` and still exits 1 for the dangling links.
    """
    result = invoke(str(run.root), ARG_REPAIR)

    inputs = run.spin_up / "input"
    gathered = run.root / "gathered_output"
    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Rewrote 3 symlink(s) as relative:" in result.stdout
    assert f"  {inputs}/abs_file -> ../output/a.nc" in result.stdout
    assert "Symlinks with an absolute target inside the run" not in result.stdout
    assert "Symlinks whose target does not exist (2):" in result.stdout
    assert os.readlink(inputs / "abs_file") == "../output/a.nc"
    assert os.readlink(inputs / "abs_dir") == "../output"
    assert os.readlink(inputs / "abs_dangling") == f"{run.root}/tasks/gone"
    assert f"Rebuilt {gathered} with 2 link(s)." in result.stdout
    assert {p.name: os.readlink(p) for p in gathered.iterdir()} == {
        "a.nc": "../tasks/spin-up/output/a.nc",
        "result.yaml": "../tasks/spin-up/output/result.yaml",
    }


def test_a_directory_is_checked_as_spelled_not_as_resolved(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a run directory given through a symlinked alias has the links
    spelled through that alias repaired, as on data homes placed on scratch.
    """
    alias = tmp_path / "alias"
    alias.symlink_to(run.root)
    via_alias = link(
        run.spin_up / "input" / "via_alias", alias / "tasks/spin-up/output/a.nc"
    )

    result = invoke(str(alias), ARG_REPAIR)

    assert f"  {via_alias} -> ../output/a.nc" in result.stdout
    assert os.readlink(via_alias) == "../output/a.nc"
    gathered = list((run.root / "gathered_output").iterdir())
    assert len(gathered) == 2
    assert all(p.resolve().exists() for p in gathered)
    assert all(os.readlink(p).startswith("../tasks/spin-up/output/") for p in gathered)


def test_repair_that_fixes_everything_exits_clean(tmp_path: Path) -> None:
    """Verify a run whose only problem is an absolute link inside it passes once repaired."""
    root = small_run(tmp_path / "small")
    abs_link = link(
        root / "tasks" / "run" / "input" / "x", root / "tasks/run/output/x.yaml"
    )

    before = invoke(str(root))
    after = invoke(str(root), ARG_REPAIR)

    assert before.exit_code == EXIT_FINDINGS, before.output
    assert after.exit_code == EXIT_CLEAN, after.output
    assert f"No problems found in {root.resolve()}" in after.stdout
    assert os.readlink(abs_link) == "../output/x.yaml"
    assert not (root / "gathered_output").exists()


def test_repair_rebuilds_gathered_output_even_when_no_link_needed_fixing(
    run: FakeRun,
) -> None:
    """Verify `--repair` always rebuilds an existing `gathered_output`, so a
    second repair, with nothing left to rewrite, still refreshes it.
    """
    gathered = run.root / "gathered_output"
    invoke(str(run.root), ARG_REPAIR)
    (gathered / "a.nc").unlink()

    result = invoke(str(run.root), ARG_REPAIR)

    assert "No symlink to rewrite." in result.stdout
    assert f"Rebuilt {gathered} with 2 link(s)." in result.stdout
    assert sorted(p.name for p in gathered.iterdir()) == ["a.nc", "result.yaml"]


def test_repair_without_a_workplan_leaves_gathered_output_alone(tmp_path: Path) -> None:
    """Verify a run directory with no transformed workplan has nothing to gather
    from: the skip is said, a real file in `gathered_output` survives and the run
    still passes.
    """
    root = small_run(tmp_path / "small")
    kept = write(root / "gathered_output" / "mine.txt", "mine")

    result = invoke(str(root), ARG_REPAIR)

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "no transformed workplan could be loaded" in result.stdout
    assert "Rebuilt" not in result.stdout
    assert kept.read_text() == "mine"


def test_repair_does_not_rebuild_gathered_output_from_another_machines_workplan(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a copy of a run, whose workplan still names the original
    directories, has its links rewritten but its gathered output left as it was.
    """
    moved = tmp_path / "archive" / "my-run"
    shutil.copytree(run.root, moved, symlinks=True)
    fresh = link(
        moved / "tasks" / "spin-up" / "input" / "fresh",
        moved.resolve() / "tasks" / "spin-up" / "output" / "a.nc",
    )
    stale = os.readlink(moved / "gathered_output" / "a.nc")

    result = invoke(str(moved), ARG_REPAIR)

    assert "Rewrote 1 symlink(s) as relative:" in result.stdout
    assert os.readlink(fresh) == "../output/a.nc"
    assert f"Did not rebuild {moved.resolve()}/gathered_output" in result.stdout
    assert "describes a different directory" in result.stdout
    assert os.readlink(moved / "gathered_output" / "a.nc") == stale
    assert "Rebuilt" not in result.stdout


def test_a_clean_run_exits_zero(tmp_path: Path) -> None:
    """Verify a run with nothing to report says so and exits 0."""
    root = small_run(tmp_path / "small")

    result = invoke(str(root), ARG_ALL)

    assert result.exit_code == EXIT_CLEAN, result.output
    assert f"No problems found in {root.resolve()}." in result.stdout
    assert "Skipped step directories and orphans" in result.stdout


def test_informational_findings_do_not_fail_the_run(
    tmp_path: Path, hello_world_bp_path: Path
) -> None:
    """Verify an absolute link leaving the run, and a task directory no step
    accounts for, are listed but exit 0.
    """
    root = small_run(tmp_path / "small")
    outside = write(tmp_path / "outside" / "data.nc", "data")
    link(root / "tasks" / "run" / "input" / "data", outside)

    result = invoke(str(root))

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "(informational) (1):" in result.stdout
    assert f"No problems found in {root.resolve()}, 1 informational." in result.stdout


def test_an_unloadable_workplan_is_named_and_its_checks_skipped(run: FakeRun) -> None:
    """Verify a transformed workplan that cannot be read is reported on stderr,
    the checks that need it are skipped, and the rest still run.
    """
    run.trx_path.write_text("not: [a, workplan")

    result = invoke(run.record.run_id, record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert f"unable to load the transformed workplan {run.trx_path}" in result.stderr
    assert "Skipped step directories and orphans" in result.stdout
    assert "Steps without a directory" not in result.stdout
    assert "Symlinks whose target does not exist (2):" in result.stdout


@pytest.mark.parametrize("target", ["wp.yaml", "dir/wp.YML", "./wp.yml"])
def test_a_workplan_file_is_pointed_at_workplan_check(
    target: str, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a workplan file is refused with the command that validates it."""
    result = invoke(target)

    assert result.exit_code == EXIT_BAD_ARGS
    assert f"cstar workplan check {target}" in flatten_cli_output(result.stderr)


def test_an_unknown_run_id_is_a_bad_argument(
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run-id no record exists for exits 2."""
    result = invoke("no-such-run")

    assert result.exit_code == EXIT_BAD_ARGS
    assert (
        "Unable to locate run with unknown run-id: no-such-run"
        in flatten_cli_output(result.stderr)
    )


def test_an_unknown_run_id_that_names_a_directory_suggests_the_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a bare name that is a directory here is not silently taken for one."""
    small_run(tmp_path / "small")
    monkeypatch.chdir(tmp_path)

    result = invoke("small")

    assert result.exit_code == EXIT_BAD_ARGS
    assert "pass it as a path" in flatten_cli_output(result.stderr)


def test_a_missing_directory_is_a_bad_argument(
    tmp_path: Path, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a path that is not a directory exits 2 and names the path."""
    target = tmp_path / "missing"

    result = invoke(str(target))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "is not a directory" in flatten_cli_output(result.stderr)


def test_a_directory_without_tasks_is_not_a_run(
    tmp_path: Path, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a directory that holds no `tasks` directory is refused."""
    result = invoke(str(tmp_path))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "has no `tasks` directory" in flatten_cli_output(result.stderr)


def test_a_directory_with_two_transformed_workplans_is_ambiguous(
    run: FakeRun, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify the check does not guess which transformed workplan describes the run."""
    shutil.copy(run.trx_path, run.root / "other_transformed.yaml")

    result = invoke(str(run.root))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "more than one transformed workplan" in flatten_cli_output(result.stderr)


def test_a_run_whose_directory_is_gone_suggests_the_path(
    run: FakeRun, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a tracked run whose recorded directory is missing says how to check a moved one."""
    record = run.record.model_copy(update={"output_path": run.root / "gone"})

    result = invoke(record.run_id, record=record)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "pass its directory as a path" in flatten_cli_output(result.stderr)


def test_the_help_names_the_options_and_the_workplan_command(
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify the options come from the shared constants and the help redirects
    workplan files.
    """
    result = CliRunner().invoke(app_run, [COMMAND_CHECK, "--help"], color=False)
    text = flatten_cli_output(result.stdout)

    assert result.exit_code == 0
    assert ARG_REPAIR in text
    assert ARG_ALL in text
    assert "cstar workplan check <file>" in text
