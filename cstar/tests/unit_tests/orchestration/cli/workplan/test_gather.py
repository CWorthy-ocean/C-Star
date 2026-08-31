from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.gather import app
from cstar.execution.file_system import (
    RomsFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.orchestration import LiveStep


def _joined_output_dir(step: Step) -> Path:
    """Return the `joined_output` directory for a step's working directory,
    computed the same way production code does via `LiveStep`.
    """
    working_dir = LiveStep.from_step(step).working_dir
    return RomsFileSystemManager(working_dir).joined_output_dir


def _run_root() -> Path:
    return StateDirectoryManager.data_dir("fake-run-id")


def _dest_dir() -> Path:
    return RomsFileSystemManager(_run_root()).joined_output_dir


def test_cli_workplan_gather_happy_path(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a unique file per step is linked into the consolidated
    `joined_output` directory as a relative symlink pointing at the source.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    sources: dict[str, Path] = {}
    for i, step in enumerate(wp.steps):
        joined = _joined_output_dir(step)
        joined.mkdir(parents=True)
        name = f"out_{i}.nc"
        src = joined / name
        src.write_text("data")
        sources[name] = src

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)

    assert result.exit_code == 0

    dest = _dest_dir()
    for name, src in sources.items():
        link = dest / name
        assert link.is_symlink()
        assert not Path(link.readlink()).is_absolute()
        assert link.resolve() == src.resolve()


def test_cli_workplan_gather_is_rerunnable(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that re-running gather picks up new files and drops stale links
    for sources that no longer exist.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    step = wp.steps[0]
    joined = _joined_output_dir(step)
    joined.mkdir(parents=True)
    stale_src = joined / "stale.nc"
    stale_src.write_text("data")

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)
    assert result.exit_code == 0

    dest = _dest_dir()
    assert (dest / "stale.nc").is_symlink()

    stale_src.unlink()
    new_src = joined / "fresh.nc"
    new_src.write_text("data")

    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)
    assert result.exit_code == 0

    assert not (dest / "stale.nc").exists()
    assert not (dest / "stale.nc").is_symlink()
    assert (dest / "fresh.nc").is_symlink()
    assert (dest / "fresh.nc").resolve() == new_src.resolve()


def test_cli_workplan_gather_conflict_aborts(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a filename collision across steps aborts with an error and
    modifies nothing on disk, including a pre-existing consolidated directory.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    if len(wp.steps) < 2:
        pytest.skip("Conflict detection requires a workplan with at least 2 steps")

    runner = CliRunner()

    # Seed and gather one distinct file first, to prove the pre-existing
    # consolidated directory is left untouched by the failed gather below.
    first_step = wp.steps[0]
    first_joined = _joined_output_dir(first_step)
    first_joined.mkdir(parents=True)
    (first_joined / "untouched.nc").write_text("data")

    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)
    assert result.exit_code == 0

    dest = _dest_dir()
    assert (dest / "untouched.nc").is_symlink()
    pre_existing_target = (dest / "untouched.nc").resolve()

    # An orphaned symlink from an earlier gather (its source is gone). A
    # correct gather aborts on conflict before touching the consolidated
    # directory, so it must survive; a relink-then-check ordering would
    # remove it.
    orphan = dest / "orphan.nc"
    orphan.symlink_to("no-longer-exists.nc")

    # Now introduce a collision between the first two steps.
    second_step = wp.steps[1]
    second_joined = _joined_output_dir(second_step)
    second_joined.mkdir(parents=True)

    colliding_a = first_joined / "colliding.nc"
    colliding_a.write_text("data-a")
    colliding_b = second_joined / "colliding.nc"
    colliding_b.write_text("data-b")

    result = runner.invoke(app, [fake_run_id], color=False)

    assert result.exit_code == 1
    assert "colliding.nc" in result.stdout
    assert str(colliding_a) in result.stdout
    assert str(colliding_b) in result.stdout

    # Pre-existing consolidated content is untouched.
    assert (dest / "untouched.nc").is_symlink()
    assert (dest / "untouched.nc").resolve() == pre_existing_target
    assert orphan.is_symlink()


def test_cli_workplan_gather_skips_steps_without_joined_output(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that steps with no `joined_output` directory are skipped quietly.

    When no step has produced any joined output, an empty consolidated
    directory is still created along with a friendly informational message.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    *_, fake_run_id = executed_workplan

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)

    assert result.exit_code == 0
    assert "No joined output was found" in result.stdout

    dest = _dest_dir()
    assert dest.exists()
    assert list(dest.iterdir()) == []


def test_cli_workplan_gather_skips_one_populated_step(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that only steps with an existing `joined_output` directory
    contribute links, while other steps are skipped without error.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    step = wp.steps[0]
    joined = _joined_output_dir(step)
    joined.mkdir(parents=True)
    src = joined / "only.nc"
    src.write_text("data")

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)

    assert result.exit_code == 0

    dest = _dest_dir()
    assert (dest / "only.nc").is_symlink()
    assert (dest / "only.nc").resolve() == src.resolve()
    assert len(list(dest.iterdir())) == 1


def test_cli_workplan_gather_refuses_to_delete_real_files(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that gather refuses to wipe non-symlink entries from an
    existing consolidated `joined_output` directory.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    *_, fake_run_id = executed_workplan

    dest = _dest_dir()
    dest.mkdir(parents=True)
    real_file = dest / "real_file.nc"
    real_file.write_text("do not delete me")

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False)

    assert result.exit_code == 1
    assert "real_file.nc" in result.stdout
    assert real_file.exists()
    assert real_file.read_text() == "do not delete me"


@pytest.mark.parametrize("dest_kind", ["regular_file", "dangling_symlink"])
def test_cli_workplan_gather_dest_is_not_a_directory(
    executed_workplan: tuple[Path, Workplan, str],
    dest_kind: str,
) -> None:
    """Verify that gather refuses to proceed when the consolidated
    `joined_output` path exists but is not a directory.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    dest_kind : str
        The kind of non-directory entry occupying the destination path.
    """
    *_, fake_run_id = executed_workplan

    dest = _dest_dir()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest_kind == "regular_file":
        dest.write_text("not a directory")
    else:
        dest.symlink_to("no-longer-exists")

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False)

    assert result.exit_code == 1
    assert "not a directory" in result.stdout
    if dest_kind == "regular_file":
        assert dest.is_file()
        assert dest.read_text() == "not a directory"
    else:
        assert dest.is_symlink()


def test_cli_workplan_gather_unknown_run_id() -> None:
    """Verify that an unknown run-id produces an error mentioning it cannot
    be located.
    """
    runner = CliRunner()
    unknown_run_id = "run-id-dne"

    result = runner.invoke(app, [unknown_run_id], color=False)

    assert result.exit_code != 0
    assert "unable to locate" in result.stderr.lower()
    assert unknown_run_id in result.stderr
