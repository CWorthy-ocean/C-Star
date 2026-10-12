import logging
from pathlib import Path

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME
from cstar.base.utils import slugify
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.models import Step
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.run_check import find_orphans, report_orphans
from cstar.orchestration.state import StateRepository

RUN_ID = "run-check"


@pytest.fixture
def run_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The root of a run whose data and state homes are in `tmp_path`."""
    monkeypatch.setenv(ENV_CSTAR_DATA_HOME, str(tmp_path / "data"))
    monkeypatch.setenv(ENV_CSTAR_STATE_HOME, str(tmp_path / "state"))
    return StateDirectoryManager.data_dir(RUN_ID)


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
    return LiveWorkplan(name=RUN_ID, description="a run to check", steps=steps)


def make_step_dirs(run_root: Path, *names: str) -> None:
    """Create the task directories a run leaves behind."""
    for name in names:
        (JobFileSystemManager(run_root).tasks_dir / name).mkdir(parents=True)


def make_sentinels(*names: str) -> list[Path]:
    """Create the sentinel files a run leaves behind."""
    paths = [StateRepository.sentinel_path(name, run_id=RUN_ID) for name in names]
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

    orphans = find_orphans(run_root, RUN_ID, wp)

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
    unrelated = StateRepository.sentinel_path("simulate", run_id=RUN_ID).with_name(
        "readme.txt"
    )
    unrelated.touch()

    orphans = find_orphans(run_root, RUN_ID, wp)

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

    assert not find_orphans(run_root, RUN_ID, wp)


def test_find_orphans_without_a_run_tree(
    run_root: Path, fake_blueprint_path: Path
) -> None:
    """Verify a run that has written nothing yet has no orphans."""
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")

    assert not run_root.exists()
    assert not find_orphans(run_root, RUN_ID, wp)


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

    assert not find_orphans(link, RUN_ID, wp)


def test_report_orphans_logs_one_warning(
    run_root: Path,
    fake_blueprint_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify orphans are reported in a single warning that names them and the
    command to inspect them, and that nothing is deleted.
    """
    wp = make_workplan(fake_blueprint_path, run_root, "simulate")
    make_step_dirs(run_root, "simulate", "old-step", "older-step")
    (stale,) = make_sentinels("old-step")

    with caplog.at_level(logging.WARNING):
        orphans = report_orphans(run_root, RUN_ID, wp)

    (record,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert "old-step, older-step" in record.getMessage()
    assert stale.name in record.getMessage()
    assert f"cstar run check {RUN_ID}" in record.getMessage()
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
        report_orphans(run_root, RUN_ID, wp)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
