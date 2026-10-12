"""Tests for the run-directory checks in `cstar.orchestration.run_check`."""

import logging
import os
from pathlib import Path

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME
from cstar.base.utils import slugify
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.run_check import (
    Finding,
    FindingKind,
    Orphans,
    check_run,
    find_absolute_paths,
    find_bad_symlinks,
    find_missing_step_dirs,
    find_orphans,
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


def test_symlink_findings_carry_the_link_text(run: FakeRun) -> None:
    """Verify the link text is reported as written."""
    findings = {f.file.name: f.path for f in find_bad_symlinks(run.root)}

    assert findings["abs_dangling"] == f"{run.root}/tasks/gone"
    assert findings["rel_dangling"] == "../nothing"
    assert findings["abs_outside"] == str(run.elsewhere)


def test_only_an_external_link_is_informational() -> None:
    """Verify a link that leaves the run is the one kind that cannot fail the check."""
    assert {k for k in FindingKind if k.informational} == {FindingKind.EXTERNAL_LINK}


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
    assert {f.kind for f in report.findings} == set(FindingKind)
    assert all(f.kind is not FindingKind.EXTERNAL_LINK for f in report.problems)
    assert not report.clean
    assert report.orphans
    assert report.skipped == ()


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
    assert [s.split(":")[0] for s in report.skipped] == ["step directories and orphans"]


def test_sentinels_are_only_checked_for_a_tracked_run(run: FakeRun) -> None:
    """Verify a run given as a bare directory keeps its task-directory check and
    skips the sentinels, which are found by run-id.
    """
    report = check_run(run.root, workplan=run.workplan, run_id=None, all_files=False)

    assert report.orphans == Orphans(step_dirs=(run.root / "tasks" / "stale",))
    assert [s.split(":")[0] for s in report.skipped] == ["orphaned sentinels"]


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
