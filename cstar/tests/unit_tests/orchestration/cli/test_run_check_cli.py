"""Tests for the `cstar run check` command."""

import os
import re
import shutil
import typing as t
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.run import app as app_run
from cstar.cli.run.check import COMMAND_CHECK, format_findings, summarize
from cstar.entrypoint.utils import ARG_ALL, ARG_NO_REFRESH, ARG_REPAIR, ARG_VERIFY
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.index import run_uri
from cstar.orchestration.manifest import (
    ATTEMPTS_DIR_NAME,
    RUN_MANIFEST_NAME,
    Attempt,
    Product,
    RunManifest,
    StepEntry,
    StepRecord,
    carried_digest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.run_check import check_run
from cstar.orchestration.tracking import (
    KEY_ACCESS,
    KEY_ATTACHED,
    KEY_RUN_UUID,
    Access,
    WorkplanRun,
)
from cstar.tests.unit_tests.orchestration.manifest_runs import BuiltRun
from cstar.tests.unit_tests.orchestration.run_tree import (
    FakeRun,
    build_fake_run,
    link,
    write,
)

LOOKUP: t.Final[str] = (
    "cstar.orchestration.index.TrackingRepository.get_workplan_run_sync"
)
"""The tracking lookup the run resolver makes, replaced so that a run-id resolves
to a chosen record."""

EXIT_CLEAN: t.Final[int] = 0
EXIT_FINDINGS: t.Final[int] = 1
EXIT_BAD_ARGS: t.Final[int] = 2

HEADINGS: t.Final[tuple[str, ...]] = (
    "Absolute paths in run files (",
    f"Symlinks with an absolute target inside the run (fixed by {ARG_REPAIR}) (",
    "Symlinks whose target does not exist (",
    "Steps without a directory (",
    "Products missing from the run (",
    "Products whose size differs from the record (",
    f"Products whose content differs from the record ({ARG_VERIFY}) (",
    "Carried inputs whose content differs from the manifest (",
    "Symlinks with a target outside the run (informational) (",
    "Steps that are done without a step record (informational) (",
    "Latest attempt not finished (informational) (",
    "Carried inputs missing from the run (informational) (",
    "Step directories no step accounts for (informational) (",
    "Sentinel files no step accounts for (informational) (",
)
"""The sections of a report, in the order they are listed."""


NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


@pytest.fixture
def run(tmp_path: Path, hello_world_bp_path: Path) -> FakeRun:
    """The fake run tree with one of everything."""
    return build_fake_run(hello_world_bp_path, tmp_path)


class Refresh(t.NamedTuple):
    """The calls a run-id target makes to bring the manifest up to date."""

    status: mock.AsyncMock
    refresh: mock.Mock


@pytest.fixture(autouse=True)
def refresh() -> t.Iterator[Refresh]:
    """Replace what refreshing the manifest of a tracked run reaches for."""
    status = mock.AsyncMock(return_value=DagStatus({}))
    with (
        mock.patch("cstar.cli.run.check.get_launcher"),
        mock.patch("cstar.cli.run.check.load_run_state", status),
        mock.patch("cstar.cli.run.check.refresh_run_manifest") as refresh_manifest,
    ):
        yield Refresh(status, refresh_manifest)


def entry(
    name: str,
    working_dir: str,
    *products: Product,
    status: str = "",
    record: bool = True,
) -> StepEntry:
    """A step entry whose record, when it has one, lists `products`."""
    step_record = StepRecord(
        step=name,
        application="hello_world",
        blueprint=BlueprintIdentity(
            kind="Blueprint", application="hello_world", name="bp", content_hash="x"
        ),
        outcome="completed",
        finished_at=NOW,
        products=list(products),
    )
    return StepEntry(
        name=name,
        application="hello_world",
        working_dir=working_dir,
        status=status,
        record=step_record if record else None,
    )


def write_manifest(
    root: Path,
    *steps: StepEntry,
    finished: bool = True,
    attempts: t.Sequence[Attempt] = (),
) -> RunManifest:
    """Write the manifest of a run of `steps`.

    Its one attempt is `finished` or not, unless `attempts` replaces it.
    """
    manifest = RunManifest(
        run_id="small",
        name="small",
        kind="workplan",
        created_at=NOW,
        attempts=list(attempts)
        or [Attempt(start_at=NOW, finished_at=NOW if finished else None)],
        steps=list(steps),
    )
    write_run_manifest(root, manifest)
    return manifest


def carry(
    root: Path, name: str, files: dict[str, str], *, finished: bool = True
) -> Attempt:
    """An attempt that carried `files`, by relative path, into `attempts/<name>`."""
    directory = f"{ATTEMPTS_DIR_NAME}/{name}"
    carried = {
        relative: carried_digest(write(root / directory / relative, text))
        for relative, text in files.items()
    }
    return Attempt(
        start_at=NOW,
        finished_at=NOW if finished else None,
        directory=directory,
        carried=carried,
    )


def product(path: str, size: int = 0, sha256: str = "") -> Product:
    """A product of unclassified kind."""
    return Product(kind="other", path=path, size_bytes=size, sha256=sha256)


def invoke(*args: str, record: WorkplanRun | None = None) -> Result:
    """Run `cstar run check` against a repository that holds only `record`."""
    with mock.patch(LOOKUP, mock.Mock(return_value=record)):
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
    older = carry(run.root, "20261018T090000", {"workplan.yaml": "name: wp\n"})
    shutil.rmtree(run.root / older.directory)
    latest = carry(
        run.root,
        "20261019T120000",
        {"blueprints/run/bp.yaml": "a: 1\n"},
        finished=False,
    )
    edited = write(run.root / latest.directory / "blueprints/run/bp.yaml", "a: 2\n")
    write_manifest(
        run.root,
        entry(
            "Spin Up",
            "tasks/spin-up",
            product("output/gone.nc", 4),
            product("output/a.nc", 9),
            product("output/result.yaml", sha256="0" * 64),
        ),
        entry("run", "tasks/run", status="Done", record=False),
        attempts=[older, latest],
    )

    result = invoke(run.record.run_id, ARG_VERIFY, record=run.record)

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
    assert f"  {run.spin_up}/output/gone.nc\n" in result.stdout
    assert f"  {run.spin_up}/output/a.nc: 4 bytes, recorded 9\n" in result.stdout
    assert re.search(
        rf"  {re.escape(str(run.spin_up))}/output/result.yaml: [0-9a-f]{{64}}\n",
        result.stdout,
    )
    assert f"  {run.root}/tasks/run/cstar-step.yaml\n" in result.stdout
    assert f"  {run.root}/{RUN_MANIFEST_NAME}\n" in result.stdout
    assert f"  {edited}: {carried_digest(edited)}\n" in result.stdout
    assert f"  {run.root}/{older.directory}\n" in result.stdout
    assert re.search(
        rf"\d+ problem\(s\) found in {re.escape(str(run.root))}, 6 informational",
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
    assert "No run found for run-id no-such-run" in flatten_cli_output(result.stderr)


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
    """Verify a directory with neither `tasks` nor a manifest is refused, and
    the message names both.
    """
    result = invoke(str(tmp_path))

    assert result.exit_code == EXIT_BAD_ARGS
    assert (
        "has neither a `tasks` directory nor a `cstar-run.yaml`"
        in flatten_cli_output(result.stderr)
    )


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
    assert ARG_VERIFY in text
    assert ARG_NO_REFRESH in text
    assert "cstar workplan check <file>" in text


def test_a_missing_product_fails_the_check(tmp_path: Path) -> None:
    """Verify a product the record lists that the run lacks is named and exits 1,
    while one that is there passes.
    """
    root = small_run(tmp_path / "small")
    write(root / "tasks" / "run" / "output" / "here.nc", "data")
    write_manifest(
        root,
        entry(
            "run", "tasks/run", product("output/here.nc", 4), product("output/gone.nc")
        ),
    )

    result = invoke(str(root))

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Products missing from the run (1):" in result.stdout
    assert f"  {root.resolve()}/tasks/run/output/gone.nc\n" in result.stdout
    assert "here.nc" not in result.stdout
    assert "Skipped products" not in result.stdout


def test_a_changed_size_fails_the_check(tmp_path: Path) -> None:
    """Verify a product of another size than recorded exits 1 with both sizes."""
    root = small_run(tmp_path / "small")
    write(root / "tasks" / "run" / "output" / "x.nc", "data")
    write_manifest(root, entry("run", "tasks/run", product("output/x.nc", 10)))

    result = invoke(str(root))

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Products whose size differs from the record (1):" in result.stdout
    assert "/tasks/run/output/x.nc: 4 bytes, recorded 10\n" in result.stdout


def test_verify_compares_content_only_where_a_hash_is_recorded(tmp_path: Path) -> None:
    """Verify `--verify` flags a differing hash, and without it nothing is hashed."""
    root = small_run(tmp_path / "small")
    write(root / "tasks" / "run" / "output" / "x.nc", "data")
    write_manifest(root, entry("run", "tasks/run", product("output/x.nc", 4, "f" * 64)))

    plain = invoke(str(root))
    verified = invoke(str(root), ARG_VERIFY)

    assert plain.exit_code == EXIT_CLEAN, plain.output
    assert verified.exit_code == EXIT_FINDINGS, verified.output
    assert f"Products whose content differs from the record ({ARG_VERIFY}) (1):" in (
        verified.stdout
    )


def test_verify_says_when_no_product_records_a_hash(tmp_path: Path) -> None:
    """Verify `--verify` over records without hashes passes and names the skip."""
    root = small_run(tmp_path / "small")
    write(root / "tasks" / "run" / "output" / "x.nc", "data")
    write_manifest(root, entry("run", "tasks/run", product("output/x.nc", 4)))

    result = invoke(str(root), ARG_VERIFY)

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "Skipped hash verification: no product records a hash" in result.stdout


def test_informational_manifest_findings_alone_exit_zero(tmp_path: Path) -> None:
    """Verify a done step without a record and an unfinished attempt are listed,
    counted as informational, and do not fail the check.
    """
    root = small_run(tmp_path / "small")
    write_manifest(
        root,
        entry("run", "tasks/run"),
        entry("later", "tasks/later", status="Done", record=False),
        finished=False,
    )

    result = invoke(str(root))

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "Steps that are done without a step record (informational) (1):" in (
        result.stdout
    )
    assert f"  {root.resolve()}/tasks/later/cstar-step.yaml\n" in result.stdout
    assert "Latest attempt not finished (informational) (1):" in result.stdout
    assert f"  {root.resolve()}/{RUN_MANIFEST_NAME}\n" in result.stdout
    assert f"No problems found in {root.resolve()}, 2 informational." in result.stdout


def test_an_edited_carried_input_fails_the_check(tmp_path: Path) -> None:
    """Verify a carried blueprint that changed since the attempt is named with
    the digest now found and exits 1, while an untouched carried file is not.
    """
    root = small_run(tmp_path / "small")
    attempt = carry(
        root,
        "20261019T120000",
        {"workplan.yaml": "name: wp\n", "blueprints/run/bp.yaml": "a: 1\n"},
    )
    write_manifest(root, entry("run", "tasks/run"), attempts=[attempt])
    edited = write(root / attempt.directory / "blueprints/run/bp.yaml", "a: 2\n")

    result = invoke(str(root))

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Carried inputs whose content differs from the manifest (1):" in (
        result.stdout
    )
    assert f"  {edited.resolve()}: {carried_digest(edited)}\n" in result.stdout
    assert "workplan.yaml" not in result.stdout
    assert "Carried inputs missing" not in result.stdout


def test_deleted_carried_inputs_are_informational(tmp_path: Path) -> None:
    """Verify an older attempt deleted by hand is listed once, by its directory,
    a carried file gone from the newer attempt is listed too, and the run still
    exits 0.
    """
    root = small_run(tmp_path / "small")
    older = carry(
        root,
        "20261018T090000",
        {"workplan.yaml": "name: wp\n", "blueprints/run/bp.yaml": "a: 1\n"},
    )
    latest = carry(
        root,
        "20261019T120000",
        {"workplan.yaml": "name: wp\n", "workplan.vars": "x=1\n"},
    )
    write_manifest(root, entry("run", "tasks/run"), attempts=[older, latest])
    shutil.rmtree(root / older.directory)
    (root / latest.directory / "workplan.vars").unlink()

    result = invoke(str(root))

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "Carried inputs missing from the run (informational) (2):" in result.stdout
    assert f"  {root.resolve()}/{older.directory}\n" in result.stdout
    assert f"  {root.resolve()}/{latest.directory}/workplan.vars\n" in result.stdout
    assert "bp.yaml" not in result.stdout
    assert f"No problems found in {root.resolve()}, 2 informational." in result.stdout


def test_intact_carried_inputs_are_not_reported(tmp_path: Path) -> None:
    """Verify a run whose attempt carried inputs that are all as recorded is clean."""
    root = small_run(tmp_path / "small")
    attempt = carry(root, "20261019T120000", {"workplan.yaml": "name: wp\n"})
    write_manifest(root, entry("run", "tasks/run"), attempts=[attempt])

    result = invoke(str(root))

    assert result.exit_code == EXIT_CLEAN, result.output
    assert "Carried inputs" not in result.stdout
    assert f"No problems found in {root.resolve()}." in result.stdout


def test_the_help_names_the_carried_inputs(
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify the help says the carried inputs are compared with the manifest."""
    result = CliRunner().invoke(app_run, [COMMAND_CHECK, "--help"], color=False)

    assert result.exit_code == 0
    assert "carried" in flatten_cli_output(result.stdout)


def test_a_standalone_run_directory_is_accepted(tmp_path: Path) -> None:
    """Verify a directory holding `cstar-run.yaml` and no `tasks/` is checked:
    its symlinks and the products of the step record beside it, which is folded
    in memory, and the step-directory checks are skipped.
    """
    root = tmp_path / "standalone"
    write(root / "output" / "kept.nc", "data")
    link(root / "output" / "broken", "../nothing")
    write_manifest(root, entry("run", ".", record=False))
    recorded = entry(
        "run", ".", product("output/kept.nc", 4), product("output/lost.nc")
    )
    assert recorded.record is not None
    write_step_record(root, recorded.record)

    result = invoke(str(root))

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Symlinks whose target does not exist (1):" in result.stdout
    assert "Products missing from the run (1):" in result.stdout
    assert f"  {root.resolve()}/output/lost.nc\n" in result.stdout
    assert "kept.nc" not in result.stdout
    assert "Skipped step directories and orphans" in result.stdout
    assert not (root / "tasks").exists()
    assert result.stderr == ""


def test_an_unreadable_manifest_is_a_warning_and_the_other_checks_run(
    tmp_path: Path,
) -> None:
    """Verify a corrupt manifest is named on stderr, the product checks are
    skipped, and the checks that need no manifest still decide the exit code.
    """
    root = small_run(tmp_path / "small")
    link(root / "tasks" / "run" / "input" / "broken", "../nothing")
    (root / RUN_MANIFEST_NAME).write_text("{not: [valid")

    result = invoke(str(root))

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert result.stderr.startswith("Warning:")
    assert RUN_MANIFEST_NAME in result.stderr
    assert "Symlinks whose target does not exist (1):" in result.stdout
    assert "Skipped products and carried inputs: no run manifest" in result.stdout


def test_a_run_id_refreshes_the_manifest_before_checking_it(
    run: FakeRun, refresh: Refresh
) -> None:
    """Verify a tracked run's scheduler state is loaded and folded into the
    manifest, as `cstar run show` does, before the products are compared.
    """
    write_manifest(
        run.root, entry("Spin Up", "tasks/spin-up", product("output/a.nc", 4))
    )

    result = invoke(run.record.run_id, record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    refresh.status.assert_awaited_once()
    assert refresh.status.await_args is not None
    assert refresh.status.await_args.args[0] == run.record.run_id
    refresh.refresh.assert_called_once_with(
        run.root, run.record.run_id, refresh.status.return_value
    )
    assert "Skipped products" not in result.stdout


def test_no_refresh_reads_the_manifest_as_recorded(
    run: FakeRun, refresh: Refresh
) -> None:
    """Verify `--no-refresh` neither polls the scheduler nor rewrites the manifest."""
    write_manifest(
        run.root, entry("Spin Up", "tasks/spin-up", product("output/gone.nc"))
    )

    result = invoke(run.record.run_id, ARG_NO_REFRESH, record=run.record)

    refresh.status.assert_not_called()
    refresh.refresh.assert_not_called()
    assert "Products missing from the run (1):" in result.stdout


def test_a_directory_is_never_refreshed(run: FakeRun, refresh: Refresh) -> None:
    """Verify a directory has no run-id to poll the scheduler for."""
    write_manifest(run.root, entry("Spin Up", "tasks/spin-up"))

    invoke(str(run.root))

    refresh.status.assert_not_called()
    refresh.refresh.assert_not_called()


def test_a_run_id_without_a_loadable_workplan_is_not_refreshed(
    run: FakeRun, refresh: Refresh
) -> None:
    """Verify a tracked run whose workplan cannot be read is checked against the
    manifest as recorded.
    """
    run.trx_path.write_text("not: [a, workplan")
    write_manifest(
        run.root, entry("Spin Up", "tasks/spin-up", product("output/gone.nc"))
    )

    result = invoke(run.record.run_id, record=run.record)

    refresh.status.assert_not_called()
    assert "Products missing from the run (1):" in result.stdout


def test_a_refresh_that_cannot_run_is_a_warning(run: FakeRun, refresh: Refresh) -> None:
    """Verify a run whose workplan names a scheduler this machine lacks is still
    checked, with the manifest as last recorded.
    """
    write_manifest(
        run.root, entry("Spin Up", "tasks/spin-up", product("output/gone.nc"))
    )
    failure = CstarExpectationFailed("no scheduler on this system")

    with mock.patch("cstar.cli.run.check.get_launcher", side_effect=failure):
        result = invoke(run.record.run_id, record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Warning: the manifest was not refreshed: no scheduler" in result.output
    assert "Products missing from the run (1):" in result.stdout
    refresh.refresh.assert_not_called()


@pytest.mark.parametrize("handle", ["run-id", "uuid"])
def test_an_untracked_run_is_checked_as_its_directory(
    local_run: t.Callable[..., BuiltRun], refresh: Refresh, handle: str
) -> None:
    """Verify a run only its manifest names is checked by its run-id or its uuid
    as its directory is: never refreshed, the manifest left as it is, and the
    checks that need a tracked run skipped.
    """
    built = local_run("lonely")
    before = built.manifest_bytes

    result = invoke(
        built.run_id if handle == "run-id" else run_uri(built.uuid), record=None
    )

    # the transformed workplan names the run's own directories, which is a finding
    assert result.exit_code == EXIT_FINDINGS, result.output
    assert f"problem(s) found in {built.root}" in result.stdout
    assert "Skipped orphaned sentinels: they are found by run-id" in result.stdout
    refresh.status.assert_not_called()
    refresh.refresh.assert_not_called()
    assert built.manifest_bytes == before


def test_a_run_in_the_project_area_is_checked_by_its_uuid(
    project_run: t.Callable[..., BuiltRun], refresh: Refresh
) -> None:
    """Verify a run kept in the project's runs area is found by its uuid."""
    built = project_run("shared-run")

    result = invoke(run_uri(built.uuid), record=None)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert f"problem(s) found in {built.root}" in result.stdout
    refresh.refresh.assert_not_called()


def test_a_standalone_blueprint_run_is_checked_by_its_uuid(
    build_run: t.Callable[..., BuiltRun], refresh: Refresh
) -> None:
    """Verify a run that holds a manifest and no `tasks/` is accepted."""
    root = StateDirectoryManager.blueprint_runs_dir() / "hello_world" / "solo"
    built = build_run(root, "solo", kind="blueprint", steps=("solo",))

    result = invoke(run_uri(built.uuid), record=None)

    assert result.exit_code == EXIT_CLEAN, result.output
    assert f"No problems found in {built.root}" in result.stdout


def test_the_uuid_of_a_tracked_run_refreshes_it(run: FakeRun, refresh: Refresh) -> None:
    """Verify a run tracked on this machine is refreshed however it is named."""
    manifest = write_manifest(
        run.root, entry("Spin Up", "tasks/spin-up", product("output/a.nc", 4))
    )
    manifest.id = "0f8c2ba4-6d57-4a25-9c1e-3b52f0d7a911"
    write_run_manifest(run.root, manifest)

    result = invoke(run_uri(manifest.id), record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    refresh.status.assert_awaited_once()
    refresh.refresh.assert_called_once()


def test_an_unknown_uuid_is_a_bad_argument(
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a uuid no manifest holds is a usage error that names it."""
    wanted = "6f1c3a58-1b2e-4c6d-8e0f-0a1b2c3d4e5f"

    result = invoke(run_uri(wanted), record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert f"No run has the uuid {wanted}" in flatten_cli_output(result.stderr)


def test_a_malformed_uuid_handle_is_a_bad_argument(
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a `run://` handle without a uuid is a usage error naming the form."""
    result = invoke("run://nonsense", record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "run://<uuid>" in flatten_cli_output(result.stderr)


def add_absolute_link(built: BuiltRun) -> Path:
    """Put in the first step of a built run a link, with an absolute target, to a file in it."""
    output = write(built.step_dir("Spin Up") / "output" / "a.nc", "data")
    return link(built.step_dir("Spin Up") / "input" / "abs_file", output)


@pytest.mark.parametrize("handle", ["run-id", "uuid"])
@pytest.mark.parametrize("kept", ["local_run", "project_run"])
def test_repair_refuses_a_run_found_only_by_its_manifest(
    request: pytest.FixtureRequest,
    refresh: Refresh,
    squash_cli_output: t.Callable[[str], str],
    handle: str,
    kept: str,
) -> None:
    """Verify `--repair` writes nothing into a run that only its manifest names,
    however it is named, and says how to repair it deliberately.
    """
    built = request.getfixturevalue(kept)("lonely")
    absolute = add_absolute_link(built)
    target = os.readlink(absolute)
    before = built.manifest_bytes

    result = invoke(
        built.run_id if handle == "run-id" else run_uri(built.uuid),
        ARG_REPAIR,
        record=None,
    )

    assert result.exit_code == EXIT_BAD_ARGS, result.output
    text = squash_cli_output(result.stderr)
    assert (
        squash_cli_output(f"The run at {built.root} was found by its manifest") in text
    )
    assert squash_cli_output("is not tracked here") in text
    assert squash_cli_output(f"cstar run check {built.root} {ARG_REPAIR}") in text
    assert result.stdout == ""
    assert os.readlink(absolute) == target
    assert built.manifest_bytes == before
    assert not (built.root / "gathered_output").exists()
    refresh.refresh.assert_not_called()


def test_repair_still_repairs_a_directory_the_user_names(
    project_run: t.Callable[..., BuiltRun],
) -> None:
    """Verify naming the directory of a run that only its manifest names is the
    deliberate way to repair it.
    """
    built = project_run("shared-run")
    absolute = add_absolute_link(built)

    result = invoke(str(built.root), ARG_REPAIR)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Rewrote 1 symlink(s) as relative:" in result.stdout
    assert os.readlink(absolute) == "../output/a.nc"


def test_repair_still_repairs_a_run_tracked_here(
    run: FakeRun, refresh: Refresh
) -> None:
    """Verify a run tracked on this machine is repaired by its run-id."""
    result = invoke(run.record.run_id, ARG_REPAIR, record=run.record)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert "Rewrote 3 symlink(s) as relative:" in result.stdout
    assert os.readlink(run.spin_up / "input" / "abs_file") == "../output/a.nc"
    refresh.refresh.assert_called_once()


def test_the_hint_names_the_directory_as_it_was_typed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify the hint is for the directory the user typed, whose name is not the
    run-id the typed text slugifies to.
    """
    (tmp_path / "Spin Up").mkdir()
    monkeypatch.chdir(tmp_path)

    result = invoke("Spin Up", record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    text = flatten_cli_output(result.stderr)
    assert "To check the directory ./Spin Up, pass it as a path." in text
    assert "./spin-up" not in text


def attached_record(built: BuiltRun, access: Access | None = None) -> WorkplanRun:
    """The tracking record `cstar run attach` makes of a built run kept elsewhere."""
    metadata = {KEY_ATTACHED: "1", KEY_RUN_UUID: built.uuid}
    if access is not None:
        metadata[KEY_ACCESS] = access.value
    return WorkplanRun(
        workplan_path=built.root / f"{built.run_id}_transformed.yaml",
        trx_workplan_path=built.root / f"{built.run_id}_transformed.yaml",
        output_path=built.root,
        run_id=built.run_id,
        metadata=metadata,
    )


@pytest.fixture
def colleague_run(tmp_path: Path, build_run: t.Callable[..., BuiltRun]) -> BuiltRun:
    """A run copied from elsewhere, kept outside the data home and the project area."""
    return build_run(tmp_path / "colleague" / "their-run", "their-run")


@pytest.mark.parametrize("access", [None, Access.owned])
def test_an_attached_run_is_checked_as_its_directory_and_not_refreshed(
    colleague_run: BuiltRun, refresh: Refresh, access: Access | None
) -> None:
    """Verify a run attached from elsewhere has no sentinels to refresh from: it
    is read as found, and `--repair` may rewrite its links when it is owned.

    Parameters
    ----------
    access : Access | None
        The access the record holds; `None` for a record that says nothing.
    """
    absolute = add_absolute_link(colleague_run)
    record = attached_record(colleague_run, access)

    checked = invoke(colleague_run.run_id, record=record)
    repaired = invoke(colleague_run.run_id, ARG_REPAIR, record=record)

    assert checked.exit_code == EXIT_FINDINGS, checked.output
    assert "Symlinks with an absolute target inside the run" in checked.stdout
    assert repaired.exit_code == EXIT_FINDINGS, repaired.output
    assert "Rewrote 1 symlink(s) as relative:" in repaired.stdout
    assert os.readlink(absolute) == "../output/a.nc"
    refresh.status.assert_not_called()
    refresh.refresh.assert_not_called()


def test_an_attached_run_by_uuid_is_checked_as_its_directory(
    colleague_run: BuiltRun, refresh: Refresh
) -> None:
    """Verify `run://<uuid>` finds the attached run wherever it is kept."""
    record = attached_record(colleague_run)
    with mock.patch(
        "cstar.orchestration.index.TrackingRepository.list_latest_runs_sync",
        mock.Mock(return_value=[record]),
    ):
        result = invoke(run_uri(colleague_run.uuid), record=None)

    assert result.exit_code == EXIT_FINDINGS, result.output
    assert f"problem(s) found in {colleague_run.root}" in result.stdout
    refresh.refresh.assert_not_called()


def test_repair_refuses_a_run_attached_read_only(
    colleague_run: BuiltRun,
    refresh: Refresh,
    squash_cli_output: t.Callable[[str], str],
) -> None:
    """Verify `--repair` writes nothing into a run attached read-only, and the
    message says how to take ownership.
    """
    absolute = add_absolute_link(colleague_run)
    target = os.readlink(absolute)
    before = colleague_run.manifest_bytes

    result = invoke(
        colleague_run.run_id,
        ARG_REPAIR,
        record=attached_record(colleague_run, Access.read_only),
    )

    assert result.exit_code == EXIT_BAD_ARGS, result.output
    text = squash_cli_output(result.stderr)
    assert squash_cli_output("is attached read-only") in text
    assert squash_cli_output(f"cstar run attach {colleague_run.root} --own") in text
    assert result.stdout == ""
    assert os.readlink(absolute) == target
    assert colleague_run.manifest_bytes == before
    assert not (colleague_run.root / "gathered_output").exists()
    refresh.refresh.assert_not_called()


def test_a_run_attached_read_only_is_still_checked(colleague_run: BuiltRun) -> None:
    """Verify reading a read-only run is allowed: only `--repair` is refused."""
    add_absolute_link(colleague_run)

    result = invoke(
        colleague_run.run_id,
        record=attached_record(colleague_run, Access.read_only),
    )

    assert result.exit_code == EXIT_FINDINGS, result.output


def test_the_report_is_formatted_and_summarized_for_other_commands(
    run: FakeRun,
) -> None:
    """Verify the formatter and the summary are public, for the commands that
    report on a run directory as `check` does.
    """
    report = check_run(run.root, workplan=None, run_id=None, all_files=False)

    lines = list(format_findings(report))

    assert any(line.startswith("Symlinks with an absolute target") for line in lines)
    assert summarize(report).startswith(f"{len(report.problems)} problem(s) found in")
    assert str(run.root) in summarize(report)
