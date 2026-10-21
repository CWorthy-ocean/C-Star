"""Tests for the run-directory checks in `cstar.orchestration.run_check`."""

import hashlib
import logging
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.manifest import (
    ATTEMPTS_DIR_NAME,
    Attempt,
    Product,
    RunManifest,
    StepEntry,
    StepRecord,
    carried_digest,
)
from cstar.orchestration.models import BlueprintIdentity, Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.run_check import (
    Finding,
    FindingKind,
    Orphans,
    RunCheckReport,
    check_run,
    find_absolute_paths,
    find_bad_symlinks,
    find_input_findings,
    find_missing_step_dirs,
    find_orphans,
    find_product_findings,
    gathered_output_dir,
    repair_symlinks,
    report_orphans,
    steps_inside_run,
)
from cstar.orchestration.state import StateRepository
from cstar.tests.unit_tests.orchestration.run_tree import (
    RUN_ID,
    FakeRun,
    build_fake_run,
    link,
    write,
)

ABSOLUTE = FindingKind.ABSOLUTE_PATH

PRODUCT_KINDS = {
    FindingKind.PRODUCT_MISSING,
    FindingKind.PRODUCT_SIZE,
    FindingKind.HASH_MISMATCH,
    FindingKind.INPUT_MISMATCH,
    FindingKind.RECORD_MISSING,
    FindingKind.ATTEMPT_OPEN,
    FindingKind.INPUT_MISSING,
}
"""The kinds that come from the run manifest, which `run` has none of."""

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


@pytest.fixture
def run(tmp_path: Path, hello_world_bp_path: Path) -> FakeRun:
    """The fake run tree with one of everything."""
    return build_fake_run(hello_world_bp_path, tmp_path)


def located(run: FakeRun, findings: list[Finding]) -> set[tuple[str, int | None]]:
    """The place of each finding as run-relative file and line."""
    return {(run.rel(f.file), f.line) for f in findings}


def kinds(run: FakeRun, findings: list[Finding]) -> set[tuple[FindingKind, str]]:
    """The kind and run-relative file of each finding."""
    return {(f.kind, run.rel(f.file)) for f in findings}


def test_absolute_paths_are_found_in_the_reference_files_only(run: FakeRun) -> None:
    """Verify the emitted files that refer to other files are scanned, down to
    nested steps, and nothing else: not a launch artifact, not a stray file.
    """
    findings = find_absolute_paths(run.root, all_files=False)

    step = "tasks/spin-up"
    assert {x for x in located(run, findings) if x[0] != run.rel(run.trx_path)} == {
        (f"{step}/blueprints/B_spin-up.yaml", 2),
        (f"{step}/output/result.yaml", 2),
        (f"{step}/output/result.yaml", 3),
        (f"{step}/tasks/sub/output/n.yaml", 2),
        (f"{step}/work/cstar_generated_roms.nml", 2),
    }
    assert all(f.kind is ABSOLUTE for f in findings)


def test_absolute_paths_name_the_offending_path(run: FakeRun) -> None:
    """Verify each finding carries the whole path, wherever it starts on the line."""
    findings = find_absolute_paths(run.root, all_files=False)
    result = run.spin_up / "output" / "result.yaml"

    assert {f.path for f in findings if f.file == result} == {
        f"{run.spin_up}/output/a.nc",
        f"{DirectoryManager.data_home()}/workplan_runs/other-run/tasks/x/output/b.nc",
    }
    comment = [f for f in findings if f.file == run.trx_path][-1]
    assert comment.path == f"{run.root}/tasks"
    assert comment.line == len(run.trx_path.read_text().splitlines())


def test_all_files_adds_the_launch_artifacts(run: FakeRun) -> None:
    """Verify `all_files` scans what launchers regenerate, and only then."""
    default = located(run, find_absolute_paths(run.root, all_files=False))
    everything = located(run, find_absolute_paths(run.root, all_files=True))

    step = "tasks/spin-up"
    assert everything - default == {
        (f"{step}/work/directives.yaml", 2),
        (f"{step}/work/spin-up.ovrd.yaml", 2),
        (f"{step}/script.sh", 2),
        (f"{step}/input/runtime_code/namelist.nml", 2),
    }
    assert default <= everything


def test_a_path_must_start_at_a_boundary(
    run: FakeRun, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify a sibling directory with a longer name, and a path that merely ends
    in the run's path, are not the run's paths. The data home is moved aside so
    that only the run directory itself is matched.
    """
    monkeypatch.setattr(
        DirectoryManager, "data_home", classmethod(lambda cls: tmp_path / "unrelated")
    )
    write(
        run.spin_up / "output" / "bounded.yaml",
        f"""\
        a: {run.root}/y
        b: {run.root}0/y
        c: /prefix{run.root}/y
        d: "{run.root}/quoted", [{run.root}/listed]
        e: ./{run.root.name}/y
        f: PATH=/bin:{run.root}/bin
        """,
    )

    findings = find_absolute_paths(run.root, all_files=False)

    bounded = [f for f in findings if f.file.name == "bounded.yaml"]
    assert [(f.line, f.path) for f in bounded] == [
        (1, f"{run.root}/y"),
        (4, f"{run.root}/quoted"),
        (4, f"{run.root}/listed"),
        (6, f"{run.root}/bin"),
    ]


def test_the_run_is_found_through_a_symlinked_parent(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a path spelled through the run's symlinked alias is the run's path
    too, as on data homes placed on a scratch file system.
    """
    alias = tmp_path / "alias"
    alias.symlink_to(run.root)
    write(run.spin_up / "output" / "aliased.yaml", f"where: {alias}/tasks/x\n")

    findings = find_absolute_paths(alias, all_files=False)

    aliased = [f for f in findings if f.file.name == "aliased.yaml"]
    assert [f.path for f in aliased] == [f"{alias}/tasks/x"]
    assert not [
        f for f in find_absolute_paths(run.root, all_files=False) if f in aliased
    ]


def test_symlinks_are_reported_once_each(run: FakeRun) -> None:
    """Verify every link that breaks on a move is found, by kind, and one that
    keeps working is not.
    """
    findings = find_bad_symlinks(run.root)

    inputs = "tasks/spin-up/input"
    assert kinds(run, findings) == {
        (FindingKind.ABSOLUTE_LINK, f"{inputs}/abs_file"),
        (FindingKind.ABSOLUTE_LINK, f"{inputs}/abs_dir"),
        (FindingKind.ABSOLUTE_LINK, "gathered_output/a.nc"),
        (FindingKind.DANGLING_LINK, f"{inputs}/abs_dangling"),
        (FindingKind.DANGLING_LINK, f"{inputs}/rel_dangling"),
        (FindingKind.EXTERNAL_LINK, f"{inputs}/abs_outside"),
    }
    assert len(findings) == 6


def test_a_relative_link_that_climbs_out_of_the_run_is_external(
    tmp_path: Path,
) -> None:
    """Verify a relative link whose target is outside the run is reported, as the
    informational kind, while one that stays inside the run is not.
    """
    root = tmp_path / "run"
    inputs = root / "tasks" / "a" / "input"
    (root / "tasks" / "b").mkdir(parents=True)
    (root / "tasks" / "b" / "x.nc").touch()
    outside = tmp_path / "other" / "data.nc"
    outside.parent.mkdir()
    outside.touch()
    link(inputs / "stays.nc", "../../b/x.nc")
    link(inputs / "escapes.nc", "../../../../other/data.nc")

    findings = find_bad_symlinks(root)

    assert [(f.kind, f.file.name, f.path) for f in findings] == [
        (FindingKind.EXTERNAL_LINK, "escapes.nc", "../../../../other/data.nc")
    ]


def test_symlink_findings_carry_the_link_text(run: FakeRun) -> None:
    """Verify the link text is reported as written."""
    findings = {f.file.name: f.path for f in find_bad_symlinks(run.root)}

    assert findings["abs_dangling"] == f"{run.root}/tasks/gone"
    assert findings["rel_dangling"] == "../nothing"
    assert findings["abs_outside"] == str(run.elsewhere)


def test_only_a_link_out_or_a_gap_in_the_record_is_informational() -> None:
    """Verify the kinds that cannot fail the check are the link that leaves the
    run, a done step without a record, an attempt that has not finished and
    carried inputs that are gone.
    """
    assert {k for k in FindingKind if k.informational} == {
        FindingKind.EXTERNAL_LINK,
        FindingKind.RECORD_MISSING,
        FindingKind.ATTEMPT_OPEN,
        FindingKind.INPUT_MISSING,
    }


def test_a_link_back_into_the_run_is_not_followed(run: FakeRun) -> None:
    """Verify a symlinked directory that loops to an ancestor ends the walk."""
    link(run.spin_up / "tasks" / "loop", run.root)

    findings = find_bad_symlinks(run.root)

    assert (FindingKind.ABSOLUTE_LINK, "tasks/spin-up/tasks/loop") in kinds(
        run, findings
    )
    assert len(findings) == 7


def test_missing_step_dirs_follow_the_slug_not_the_recorded_directory(
    run: FakeRun,
) -> None:
    """Verify the step without a directory is found at `tasks/<slug>` below the
    run, wherever the workplan says it ran.
    """
    findings = find_missing_step_dirs(run.root, run.workplan)

    assert [(f.kind, run.rel(f.file)) for f in findings] == [
        (FindingKind.MISSING_STEP_DIR, "tasks/run")
    ]


def test_missing_step_dirs_in_a_moved_run(run: FakeRun, tmp_path: Path) -> None:
    """Verify a copy of the run is judged by its own tree, not by the old paths
    the transformed workplan holds.
    """
    moved = tmp_path / "archive" / "my-run"
    (moved / "tasks" / "spin-up").mkdir(parents=True)

    findings = find_missing_step_dirs(moved, run.workplan)

    assert [f.file for f in findings] == [(moved.resolve() / "tasks" / "run")]


def test_orphans_are_step_dirs_and_sentinels_no_step_accounts_for(run: FakeRun) -> None:
    """Verify a leftover task directory and a leftover sentinel are found, and
    the sentinel of a step and a file that is no sentinel are not.
    """
    orphans = find_orphans(run.root, RUN_ID, run.workplan)

    state = StateDirectoryManager.run_state_dir(run_id=RUN_ID)
    assert orphans == Orphans(
        step_dirs=(run.root / "tasks" / "stale",),
        sentinels=(state / "retired.sentinel.yaml",),
    )
    assert orphans


def test_a_run_without_leftovers_has_no_orphans(run: FakeRun) -> None:
    """Verify a run that has not started anything has none, and is falsy."""
    (run.root / "tasks" / "stale").rmdir()
    for sentinel in StateDirectoryManager.run_state_dir(run_id=RUN_ID).glob("ret*"):
        sentinel.unlink()

    orphans = find_orphans(run.root, RUN_ID, run.workplan)

    assert orphans == Orphans()
    assert not orphans


def test_repair_makes_the_links_inside_the_run_relative(run: FakeRun) -> None:
    """Verify each absolute link inside the run now has relative text and still
    reaches what it reached, and the links repair cannot fix are as they were.
    """
    inputs = run.spin_up / "input"
    before = {
        p: os.readlink(p) for p in (inputs / "abs_dangling", inputs / "abs_outside")
    }
    targets = {
        p: p.resolve()
        for p in (
            inputs / "abs_file",
            inputs / "abs_dir",
            run.root / "gathered_output" / "a.nc",
        )
    }

    repaired = repair_symlinks(run.root)

    assert sorted(repaired) == sorted(targets)
    assert os.readlink(inputs / "abs_file") == "../output/a.nc"
    assert os.readlink(inputs / "abs_dir") == "../output"
    assert os.readlink(run.root / "gathered_output" / "a.nc") == (
        "../tasks/spin-up/output/a.nc"
    )
    assert {p: p.resolve() for p in targets} == targets
    assert {p: os.readlink(p) for p in before} == before
    assert not list(run.root.rglob("*.repair"))


def test_repair_leaves_nothing_to_do_the_second_time(run: FakeRun) -> None:
    """Verify repairing is idempotent and the fixed links stop being reported."""
    repair_symlinks(run.root)

    assert repair_symlinks(run.root) == []
    assert {f.kind for f in find_bad_symlinks(run.root)} == {
        FindingKind.DANGLING_LINK,
        FindingKind.EXTERNAL_LINK,
    }


def test_repair_through_a_symlinked_parent_stays_inside_the_run(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a link spelled through the alias of the run is repaired to text that
    does not climb out of the run and back in.
    """
    alias = tmp_path / "alias"
    alias.symlink_to(run.root)
    aliased = link(
        run.spin_up / "input" / "via_alias", alias / "tasks/spin-up/output/a.nc"
    )

    repaired = repair_symlinks(alias)

    assert aliased in repaired
    assert os.readlink(aliased) == "../output/a.nc"


def test_a_run_report_knows_the_spelling_it_was_given(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a report for a run reached through a symlinked alias still
    recognizes paths and links spelled through that alias, which a report for the
    resolved directory cannot.
    """
    alias = tmp_path / "alias"
    alias.symlink_to(run.root)
    via_alias = link(
        run.spin_up / "input" / "via_alias", alias / "tasks/spin-up/output/a.nc"
    )
    aliased = write(run.spin_up / "output" / "aliased.yaml", f"where: {alias}/x\n")

    def problem_files(root: Path) -> set[Path]:
        report = check_run(root, workplan=None, run_id=None, all_files=False)
        return {f.file for f in report.problems} & {via_alias, aliased}

    assert problem_files(alias) == {via_alias, aliased}
    assert problem_files(run.root) == set()


def test_a_run_report_orders_findings_by_kind(run: FakeRun) -> None:
    """Verify the report holds every check, grouped by kind, and fails the run."""
    report = check_run(run.root, workplan=run.workplan, run_id=RUN_ID, all_files=False)

    assert report.run_root == run.root
    assert [f.kind for f in report.findings] == sorted(
        (f.kind for f in report.findings), key=list(FindingKind).index
    )
    assert {f.kind for f in report.findings} == set(FindingKind) - PRODUCT_KINDS
    assert all(f.kind is not FindingKind.EXTERNAL_LINK for f in report.problems)
    assert not report.clean
    assert report.orphans
    assert report.skipped == ("products and carried inputs: no run manifest",)


def test_a_run_report_counts_all_files_on_request(run: FakeRun) -> None:
    """Verify `all_files` reaches the absolute-path scan."""
    default = check_run(run.root, workplan=run.workplan, run_id=RUN_ID, all_files=False)
    everything = check_run(
        run.root, workplan=run.workplan, run_id=RUN_ID, all_files=True
    )

    assert len(everything.findings) == len(default.findings) + 4


def test_checks_that_need_the_workplan_are_skipped_without_one(run: FakeRun) -> None:
    """Verify no workplan means no step or orphan checks, and says so."""
    report = check_run(run.root, workplan=None, run_id=RUN_ID, all_files=False)

    assert FindingKind.MISSING_STEP_DIR not in {f.kind for f in report.findings}
    assert not report.orphans
    assert [s.split(":")[0] for s in report.skipped] == [
        "step directories and orphans",
        "products and carried inputs",
    ]


def test_sentinels_are_only_checked_for_a_tracked_run(run: FakeRun) -> None:
    """Verify a run given as a bare directory keeps its task-directory check and
    skips the sentinels, which are found by run-id.
    """
    report = check_run(run.root, workplan=run.workplan, run_id=None, all_files=False)

    assert report.orphans == Orphans(step_dirs=(run.root / "tasks" / "stale",))
    assert [s.split(":")[0] for s in report.skipped] == [
        "orphaned sentinels",
        "products and carried inputs",
    ]


def test_a_clean_run_passes(run: FakeRun, tmp_path: Path) -> None:
    """Verify a run whose files and links are all relative and whose steps have
    directories has no finding, though a link to the outside is still listed.
    """
    clean = tmp_path / "clean"
    for name in ("spin-up", "run"):
        (clean / "tasks" / name / "output").mkdir(parents=True)
    write(clean / "tasks" / "run" / "output" / "x.yaml", "where: ../spin-up\n")
    link(clean / "tasks" / "run" / "input" / "rel", "../output/x.yaml")
    link(clean / "tasks" / "run" / "input" / "outside", run.elsewhere)
    steps = [
        LiveStep.from_step(s, update={"working_dir": clean / "tasks" / s.safe_name})
        for s in run.workplan.steps
    ]
    workplan = LiveWorkplan(**run.workplan.model_dump(exclude={"steps"}), steps=steps)

    report = check_run(clean, workplan=workplan, run_id=None, all_files=True)

    assert report.clean
    assert [f.kind for f in report.findings] == [FindingKind.EXTERNAL_LINK]
    assert not report.orphans


def test_steps_inside_run_compares_with_the_recorded_directories(
    run: FakeRun, tmp_path: Path
) -> None:
    """Verify a workplan describes a run only when its steps live inside it."""
    assert steps_inside_run(run.root, run.workplan)
    assert not steps_inside_run(tmp_path / "archive", run.workplan)


def test_gathered_output_lives_in_the_run(run: FakeRun) -> None:
    """Verify the directory `gather` builds is the one the tree holds."""
    assert gathered_output_dir(run.root).is_dir()
    assert gathered_output_dir(run.root) == run.root / "gathered_output"


# --- orphan semantics shared with the re-entry report (PR #767) ---

ORPHAN_RUN_ID = "orphan-check"


@pytest.fixture
def run_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The root of a run whose data and state homes are in `tmp_path`."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path / "data"))
    monkeypatch.setenv(ENV_CSTAR_STATE_HOME, str(tmp_path / "state"))
    return StateDirectoryManager.data_dir(ORPHAN_RUN_ID)


def make_workplan(
    blueprint: Path, run_root: Path, *names: str, parent: LiveStep | None = None
) -> LiveWorkplan:
    """A prepared workplan whose steps work in `<run_root>/tasks/<slug>`."""
    tasks_dir = JobFileSystemManager(run_root).tasks_dir
    steps = [
        LiveStep.from_step(
            Step(name=name, application="hello_world", blueprint=blueprint),
            update={"parent": parent}
            if parent
            else {"working_dir": tasks_dir / slugify(name)},
        )
        for name in names
    ]
    return LiveWorkplan(name=ORPHAN_RUN_ID, description="a run to check", steps=steps)


def make_step_dirs(run_root: Path, *names: str) -> None:
    """Create the task directories a run leaves behind."""
    for name in names:
        (JobFileSystemManager(run_root).tasks_dir / name).mkdir(parents=True)


def make_sentinels(*names: str) -> list[Path]:
    """Create the sentinel files a run leaves behind."""
    paths = [
        StateRepository.sentinel_path(name, run_id=ORPHAN_RUN_ID) for name in names
    ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    return paths


def test_find_orphans_accounts_for_every_step(
    run_root: Path, fake_blueprint_path: Path
) -> None:
    """Verify a run whose directories and sentinels all belong to steps has no orphans."""
    wp = make_workplan(fake_blueprint_path, run_root, "Make Inputs", "simulate")
    make_step_dirs(run_root, "make-inputs", "simulate")
    make_sentinels("make-inputs", "simulate")

    orphans = find_orphans(run_root, ORPHAN_RUN_ID, wp)

    assert not orphans
    assert orphans.step_dirs == ()
    assert orphans.sentinels == ()


def test_find_orphans_reports_what_no_step_claims(
    run_root: Path, fake_blueprint_path: Path
) -> None:
    """Verify the directories and sentinels of a removed step are reported, and
    that only directories under `tasks` count as step directories.
    """
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    make_step_dirs(run_root, "simulate", "old-step")
    (JobFileSystemManager(run_root).tasks_dir / "notes.txt").touch()
    kept, stale = make_sentinels("simulate", "old-step")
    unrelated = StateRepository.sentinel_path(
        "simulate", run_id=ORPHAN_RUN_ID
    ).with_name("readme.txt")
    unrelated.touch()

    orphans = find_orphans(run_root, ORPHAN_RUN_ID, wp)

    assert orphans.step_dirs == (JobFileSystemManager(run_root).tasks_dir / "old-step",)
    assert orphans.sentinels == (stale,)
    assert kept not in orphans.sentinels


def test_find_orphans_accounts_for_the_parent_of_a_split_step(
    run_root: Path, fake_blueprint_path: Path
) -> None:
    """Verify the directory of a step that was split is not reported: its
    sub-steps work beneath it and only they remain in the workplan.
    """
    parent_wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    wp = make_workplan(
        fake_blueprint_path, run_root, "part-1", "part-2", parent=parent_wp.steps[0]
    )
    parent_dir = JobFileSystemManager(run_root).tasks_dir / "simulate"
    assert wp.steps[0].working_dir == parent_dir / "tasks" / "part-1"
    (parent_dir / "tasks" / "part-1").mkdir(parents=True)

    assert not find_orphans(run_root, ORPHAN_RUN_ID, wp)


def test_find_orphans_without_a_run_tree(
    run_root: Path, fake_blueprint_path: Path
) -> None:
    """Verify a run that has written nothing yet has no orphans."""
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")

    assert not run_root.exists()
    assert not find_orphans(run_root, ORPHAN_RUN_ID, wp)


def test_find_orphans_compares_resolved_paths(
    run_root: Path, tmp_path: Path, fake_blueprint_path: Path
) -> None:
    """Verify a run root reached through a symlink does not make every step
    directory look like an orphan.
    """
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    make_step_dirs(run_root, "simulate")
    link = tmp_path / "scratch-link"
    link.symlink_to(run_root, target_is_directory=True)

    assert not find_orphans(link, ORPHAN_RUN_ID, wp)


def test_report_orphans_logs_one_warning(
    run_root: Path,
    fake_blueprint_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify orphans are reported in a single warning that lists them, says they
    were left in place, and that nothing is deleted.
    """
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    make_step_dirs(run_root, "simulate", "old-step", "older-step")
    (stale,) = make_sentinels("old-step")

    with caplog.at_level(logging.WARNING):
        orphans = report_orphans(run_root, ORPHAN_RUN_ID, wp)

    (record,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    tasks_dir = JobFileSystemManager(run_root).tasks_dir
    message = record.getMessage()
    assert f"{tasks_dir / 'old-step'}, {tasks_dir / 'older-step'}" in message
    assert str(stale) in message
    assert "left in place" in message
    assert "remove them by hand" in message
    assert "cstar run check" not in message
    assert len(orphans.step_dirs) == 2
    assert all(d.is_dir() for d in orphans.step_dirs)
    assert stale.exists()


def test_report_orphans_is_quiet_without_orphans(
    run_root: Path,
    fake_blueprint_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify nothing is logged for a run with no orphans."""
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    make_step_dirs(run_root, "simulate")

    with caplog.at_level(logging.WARNING):
        report_orphans(run_root, ORPHAN_RUN_ID, wp)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def product(path: str, size: int = 0, sha256: str = "") -> Product:
    """A product record of the given kind-less file."""
    return Product(kind="other", path=path, size_bytes=size, sha256=sha256)


def manifest_of(
    *products: Product, status: str = "Done", finished: bool = True
) -> RunManifest:
    """A manifest of one step, `tasks/one`, whose record lists `products`."""
    record = StepRecord(
        step="one",
        application="hello_world",
        blueprint=BlueprintIdentity(
            kind="Blueprint", application="hello_world", name="bp", content_hash="abc"
        ),
        outcome="completed",
        finished_at=NOW,
        products=list(products),
    )
    return RunManifest(
        run_id="run",
        name="run",
        kind="workplan",
        created_at=NOW,
        attempts=[Attempt(start_at=NOW, finished_at=NOW if finished else None)],
        steps=[
            StepEntry(
                name="one",
                application="hello_world",
                working_dir="tasks/one",
                status=status,
                record=record,
            )
        ],
    )


@pytest.fixture
def recorded(tmp_path: Path) -> Path:
    """A run root whose step `one` holds three files, 4 bytes each."""
    for name in ("a.nc", "b.nc", "c.nc"):
        write(tmp_path / "tasks" / "one" / "output" / name, "data")
    return tmp_path


def located_products(findings: list[Finding]) -> set[tuple[FindingKind, str]]:
    """The kind and file name of each finding."""
    return {(f.kind, f.file.name) for f in findings}


def test_a_deleted_and_a_truncated_product_are_found(recorded: Path) -> None:
    """Verify a missing file and a file of another size are named, the intact
    one is silent, and the size finding carries both sizes.
    """
    manifest = manifest_of(
        product("output/a.nc", 4), product("output/b.nc", 4), product("output/c.nc", 4)
    )
    (recorded / "tasks/one/output/a.nc").unlink()
    (recorded / "tasks/one/output/b.nc").write_text("da")

    findings = find_product_findings(recorded, manifest, verify=False)

    assert located_products(findings) == {
        (FindingKind.PRODUCT_MISSING, "a.nc"),
        (FindingKind.PRODUCT_SIZE, "b.nc"),
    }
    size = next(f for f in findings if f.kind is FindingKind.PRODUCT_SIZE)
    assert size.path == "2 bytes, recorded 4"
    assert size.file == recorded / "tasks/one/output/b.nc"


def test_a_log_that_grew_is_not_a_finding_but_one_that_shrank_is(
    recorded: Path,
) -> None:
    """Verify a log is compared for loss only: the step's own output is still
    being written when the record is taken.
    """
    for name in ("grew.out", "shrank.out"):
        write(recorded / "tasks" / "one" / "logs" / name, "data")
    (recorded / "tasks/one/logs/grew.out").write_text("data and more")
    (recorded / "tasks/one/logs/shrank.out").write_text("da")
    manifest = manifest_of(
        Product(kind="log", path="logs/grew.out", size_bytes=4),
        Product(kind="log", path="logs/shrank.out", size_bytes=4),
    )

    findings = find_product_findings(recorded, manifest, verify=False)

    assert located_products(findings) == {(FindingKind.PRODUCT_SIZE, "shrank.out")}


def test_a_product_not_measured_is_never_a_size_finding(recorded: Path) -> None:
    """Verify a recorded size of zero means not measured, whatever the file holds."""
    manifest = manifest_of(product("output/a.nc", 0))

    assert find_product_findings(recorded, manifest, verify=False) == []


def test_a_step_without_products_has_no_finding(recorded: Path) -> None:
    """Verify an empty product list yields nothing."""
    assert find_product_findings(recorded, manifest_of(), verify=True) == []


def test_a_done_step_without_a_record_is_informational(recorded: Path) -> None:
    """Verify a step the manifest says is done, with no record, is reported
    against the record's expected file, and does not fail the check.
    """
    manifest = manifest_of()
    manifest.steps[0].record = None

    findings = find_product_findings(recorded, manifest, verify=False)

    assert findings == [
        Finding(FindingKind.RECORD_MISSING, recorded / "tasks/one/cstar-step.yaml")
    ]
    assert RunCheckReport(recorded, tuple(findings)).clean


def test_a_step_still_running_needs_no_record(recorded: Path) -> None:
    """Verify only a done step is expected to have a record."""
    manifest = manifest_of(status="Running")
    manifest.steps[0].record = None

    assert find_product_findings(recorded, manifest, verify=False) == []


def test_an_unfinished_attempt_is_informational(recorded: Path) -> None:
    """Verify the latest attempt not having finished is reported against the
    manifest, and does not fail the check.
    """
    findings = find_product_findings(
        recorded, manifest_of(finished=False), verify=False
    )

    assert findings == [Finding(FindingKind.ATTEMPT_OPEN, recorded / "cstar-run.yaml")]
    assert RunCheckReport(recorded, tuple(findings)).clean


def test_only_the_latest_attempt_is_considered(recorded: Path) -> None:
    """Verify an earlier attempt that never finished does not count once a
    later one did.
    """
    manifest = manifest_of()
    manifest.attempts.insert(0, Attempt(start_at=NOW))

    assert find_product_findings(recorded, manifest, verify=False) == []


def test_verify_compares_the_recorded_hash(recorded: Path) -> None:
    """Verify a differing hash is a mismatch carrying the digest found, a
    matching one is silent, and nothing is hashed without `verify`.
    """
    good = hashlib.sha256(b"data").hexdigest()
    manifest = manifest_of(
        product("output/a.nc", 4, "0" * 64), product("output/b.nc", 4, good)
    )

    verified = find_product_findings(recorded, manifest, verify=True)
    unverified = find_product_findings(recorded, manifest, verify=False)

    assert verified == [
        Finding(
            FindingKind.HASH_MISMATCH, recorded / "tasks/one/output/a.nc", path=good
        )
    ]
    assert unverified == []
    assert not RunCheckReport(recorded, tuple(verified)).clean


def test_verify_skips_a_product_recorded_without_a_hash(recorded: Path) -> None:
    """Verify a product with no sha256 is not compared, and a directory is not hashed."""
    (recorded / "tasks/one/output/dir").mkdir()
    manifest = manifest_of(
        product("output/a.nc", 4), product("output/dir", 0, "0" * 64)
    )

    assert find_product_findings(recorded, manifest, verify=True) == []


def test_a_size_finding_is_not_also_a_hash_finding(recorded: Path) -> None:
    """Verify a product that failed the cheaper check is reported once."""
    manifest = manifest_of(product("output/a.nc", 9, "0" * 64))

    findings = find_product_findings(recorded, manifest, verify=True)

    assert [f.kind for f in findings] == [FindingKind.PRODUCT_SIZE]


def test_a_run_without_a_manifest_skips_the_product_checks(recorded: Path) -> None:
    """Verify no manifest is named as a skipped check, not reported as a finding."""
    report = check_run(
        recorded, workplan=None, run_id=None, all_files=False, manifest=None
    )

    assert "products and carried inputs: no run manifest" in report.skipped
    assert not PRODUCT_KINDS & {f.kind for f in report.findings}


def test_a_run_report_holds_the_product_findings(recorded: Path) -> None:
    """Verify `check_run` folds the product findings in, ordered by kind, and
    that the failing ones fail the run.
    """
    manifest = manifest_of(product("output/a.nc", 4), product("output/gone.nc", 4))
    manifest.attempts[-1].finished_at = None

    report = check_run(
        recorded, workplan=None, run_id=None, all_files=False, manifest=manifest
    )

    assert [f.kind for f in report.findings] == [
        FindingKind.PRODUCT_MISSING,
        FindingKind.ATTEMPT_OPEN,
    ]
    assert [f.kind for f in report.problems] == [FindingKind.PRODUCT_MISSING]
    assert not any("products" in s for s in report.skipped)


def test_verify_without_any_recorded_hash_says_so(recorded: Path) -> None:
    """Verify `verify` against records that hold no sha256 names the skipped check."""
    manifest = manifest_of(product("output/a.nc", 4))

    def check(*, verify: bool) -> RunCheckReport:
        return check_run(
            recorded,
            workplan=None,
            run_id=None,
            all_files=False,
            manifest=manifest,
            verify=verify,
        )

    verified = check(verify=True)
    plain = check(verify=False)

    assert "hash verification: no product records a hash" in verified.skipped
    assert not any("hash" in s for s in plain.skipped)


def test_verify_with_a_recorded_hash_has_nothing_to_skip(recorded: Path) -> None:
    """Verify the hash skip is said only when no product records a hash."""
    manifest = manifest_of(
        product("output/a.nc", 4, hashlib.sha256(b"data").hexdigest())
    )

    report = check_run(
        recorded,
        workplan=None,
        run_id=None,
        all_files=False,
        manifest=manifest,
        verify=True,
    )

    assert not any("hash" in s for s in report.skipped)
    assert report.clean


ATTEMPT_DIR = f"{ATTEMPTS_DIR_NAME}/20261019T120000"
"""The directory, in the run root, of the attempt that carried inputs."""
OLDER_ATTEMPT_DIR = f"{ATTEMPTS_DIR_NAME}/20261018T090000"
"""The directory of an attempt before it."""


def carry(root: Path, directory: str, files: dict[str, str]) -> Attempt:
    """An attempt that carried `files`, by relative path, into `directory`."""
    carried = {
        relative: carried_digest(write(root / directory / relative, text))
        for relative, text in files.items()
    }
    return Attempt(start_at=NOW, finished_at=NOW, directory=directory, carried=carried)


def manifest_with(*attempts: Attempt) -> RunManifest:
    """A manifest of the given attempts and no steps."""
    return RunManifest(
        run_id="run",
        name="run",
        kind="workplan",
        created_at=NOW,
        attempts=list(attempts),
        steps=[],
    )


@pytest.fixture
def carried(tmp_path: Path) -> RunManifest:
    """The manifest of a run whose one attempt carried a workplan and a blueprint."""
    return manifest_with(
        carry(
            tmp_path,
            ATTEMPT_DIR,
            {"workplan.yaml": "name: wp\n", "blueprints/step-a/bp.yaml": "a: 1\n"},
        )
    )


def test_carried_inputs_that_are_as_recorded_have_no_finding(
    tmp_path: Path, carried: RunManifest
) -> None:
    """Verify an attempt whose carried files match the manifest is silent, and
    a file in its directory that the manifest does not list is never looked at.
    """
    write(tmp_path / ATTEMPT_DIR / "unlisted.txt", "not carried")

    assert find_input_findings(tmp_path, carried) == []


def test_an_edited_carried_blueprint_is_a_mismatch_carrying_the_digest(
    tmp_path: Path, carried: RunManifest
) -> None:
    """Verify a carried file whose content changed is named with the digest now
    found, the untouched one is silent, and the finding fails the check.
    """
    edited = write(tmp_path / ATTEMPT_DIR / "blueprints/step-a/bp.yaml", "a: 2\n")

    findings = find_input_findings(tmp_path, carried)

    assert findings == [
        Finding(FindingKind.INPUT_MISMATCH, edited, path=carried_digest(edited))
    ]
    assert findings[0].path != carried.attempts[0].carried["blueprints/step-a/bp.yaml"]
    assert not RunCheckReport(tmp_path, tuple(findings)).clean


def test_a_deleted_attempt_directory_is_one_informational_finding(
    tmp_path: Path,
) -> None:
    """Verify an older attempt deleted by hand is reported once, however many
    files it carried, never fails the check, and leaves a newer attempt's
    inputs checked.
    """
    manifest = manifest_with(
        carry(
            tmp_path,
            OLDER_ATTEMPT_DIR,
            {"workplan.yaml": "name: wp\n", "blueprints/step-a/bp.yaml": "a: 1\n"},
        ),
        carry(tmp_path, ATTEMPT_DIR, {"workplan.yaml": "name: wp\n"}),
    )
    shutil.rmtree(tmp_path / OLDER_ATTEMPT_DIR)

    findings = find_input_findings(tmp_path, manifest)

    assert findings == [
        Finding(FindingKind.INPUT_MISSING, tmp_path / OLDER_ATTEMPT_DIR)
    ]
    assert RunCheckReport(tmp_path, tuple(findings)).clean

    (tmp_path / ATTEMPT_DIR / "workplan.yaml").write_text("name: edited\n")

    assert [f.kind for f in find_input_findings(tmp_path, manifest)] == [
        FindingKind.INPUT_MISSING,
        FindingKind.INPUT_MISMATCH,
    ]


def test_a_deleted_carried_file_is_informational(
    tmp_path: Path, carried: RunManifest
) -> None:
    """Verify one carried file gone from an attempt that is still there is named
    on its own, and does not fail the check.
    """
    gone = tmp_path / ATTEMPT_DIR / "blueprints/step-a/bp.yaml"
    gone.unlink()

    findings = find_input_findings(tmp_path, carried)

    assert findings == [Finding(FindingKind.INPUT_MISSING, gone)]
    assert RunCheckReport(tmp_path, tuple(findings)).clean


def test_an_attempt_that_carried_nothing_has_nothing_to_check(tmp_path: Path) -> None:
    """Verify an attempt without a directory, as a re-entry replaying the recorded
    plan has, produces no finding, and the run's `attempts` need not exist.
    """
    manifest = manifest_with(Attempt(start_at=NOW, finished_at=NOW))

    assert not (tmp_path / ATTEMPTS_DIR_NAME).exists()
    assert find_input_findings(tmp_path, manifest) == []


def test_a_run_report_holds_the_carried_input_findings(
    tmp_path: Path, carried: RunManifest
) -> None:
    """Verify `check_run` folds the carried-input findings in without `verify`,
    ordered by kind around the other manifest findings, and that only the
    mismatch fails the run.
    """
    older = carry(tmp_path, OLDER_ATTEMPT_DIR, {"workplan.yaml": "name: wp\n"})
    shutil.rmtree(tmp_path / OLDER_ATTEMPT_DIR)
    carried.attempts.insert(0, older)
    carried.attempts[-1].finished_at = None
    (tmp_path / ATTEMPT_DIR / "workplan.yaml").write_text("name: edited\n")

    report = check_run(
        tmp_path, workplan=None, run_id=None, all_files=False, manifest=carried
    )

    assert [f.kind for f in report.findings] == [
        FindingKind.INPUT_MISMATCH,
        FindingKind.ATTEMPT_OPEN,
        FindingKind.INPUT_MISSING,
    ]
    assert [f.kind for f in report.problems] == [FindingKind.INPUT_MISMATCH]
    assert not report.clean


def test_a_run_report_with_intact_carried_inputs_is_clean(
    tmp_path: Path, carried: RunManifest
) -> None:
    """Verify a manifest whose carried inputs are all as recorded adds nothing."""
    report = check_run(
        tmp_path, workplan=None, run_id=None, all_files=False, manifest=carried
    )

    assert report.findings == ()
    assert report.clean
