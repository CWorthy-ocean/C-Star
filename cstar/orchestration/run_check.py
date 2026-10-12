"""Find what a run directory holds that its workplan no longer accounts for.

Renaming a step between attempts leaves the old step's directory and sentinel
behind. Nothing here deletes them: they are reported so the user can decide.
"""

import typing as t
from dataclasses import dataclass
from pathlib import Path

from cstar.base.log import get_logger
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.state import StateRepository

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import LiveWorkplan

log = get_logger(__name__)


@dataclass(frozen=True)
class Orphans:
    """Step directories and sentinel files no step of a workplan accounts for."""

    step_dirs: tuple[Path, ...]
    """Directories directly under the run's `tasks` directory."""

    sentinels: tuple[Path, ...]
    """Sentinel files in the run's state directory."""

    def __bool__(self) -> bool:
        return bool(self.step_dirs or self.sentinels)


def find_orphans(run_root: Path, run_id: str, workplan: "LiveWorkplan") -> Orphans:
    """Locate the step directories and sentinels that `workplan` does not account for.

    A step directory is accounted for when a step works in it or in a
    directory nested beneath it, as the sub-steps of a split step do.

    Parameters
    ----------
    run_root : Path
        The run's directory, whose `tasks` directory holds the step directories.
    run_id : str
        The run-id, which locates the run's sentinels.
    workplan : LiveWorkplan
        The prepared workplan the run executes.

    Returns
    -------
    Orphans
    """
    tasks_dir = JobFileSystemManager(run_root).tasks_dir
    claimed = {_task_dir_name(step.working_dir, tasks_dir) for step in workplan.steps}
    step_dirs = (
        tuple(
            sorted(
                d for d in tasks_dir.iterdir() if d.is_dir() and d.name not in claimed
            )
        )
        if tasks_dir.is_dir()
        else ()
    )

    expected = {
        StateRepository.sentinel_path(step.safe_name, run_id=run_id)
        for step in workplan.steps
    }
    state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
    found = state_dir.rglob(StateRepository.sentinel_name("*"))

    return Orphans(step_dirs, tuple(sorted(p for p in found if p not in expected)))


def report_orphans(run_root: Path, run_id: str, workplan: "LiveWorkplan") -> Orphans:
    """Log one warning listing the orphans of a run that is being re-entered.

    Parameters
    ----------
    run_root : Path
        The run's directory.
    run_id : str
        The run-id.
    workplan : LiveWorkplan
        The prepared workplan the run executes.

    Returns
    -------
    Orphans
        What was found, which is empty when nothing was reported.
    """
    orphans = find_orphans(run_root, run_id, workplan)
    if orphans:
        listed = [
            f"{label}: {', '.join(p.name for p in paths)}"
            for label, paths in (
                ("step directories", orphans.step_dirs),
                ("state files", orphans.sentinels),
            )
            if paths
        ]
        msg = (
            f"Run {run_id!r} holds items no step of its workplan accounts for "
            f"({'; '.join(listed)}). Nothing was deleted; run "
            f"`cstar run check {run_id}` to inspect them."
        )
        log.warning(msg)

    return orphans


def _task_dir_name(working_dir: Path, tasks_dir: Path) -> str | None:
    """Return the name of the `tasks_dir` entry that holds `working_dir`.

    Parameters
    ----------
    working_dir : Path
        The directory a step works in; symlinks are resolved first.
    tasks_dir : Path
        An already-resolved `tasks` directory.

    Returns
    -------
    str | None
        `None` when the step works outside `tasks_dir`.
    """
    resolved = working_dir.resolve()
    if resolved == tasks_dir or not resolved.is_relative_to(tasks_dir):
        return None

    return resolved.relative_to(tasks_dir).parts[0]
