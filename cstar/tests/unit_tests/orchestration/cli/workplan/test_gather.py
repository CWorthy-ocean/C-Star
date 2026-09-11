from pathlib import Path

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.gather import GATHERED_OUTPUT_NAME, app, collect_links
from cstar.execution.file_system import JobFileSystemManager, StateDirectoryManager
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan


def _make_two_step_workplan(tmp_path: Path) -> tuple[LiveStep, LiveStep, LiveWorkplan]:
    """Build a two-step `LiveWorkplan` directly, without going through a
    template file or the `TrackingRepository`, for `collect_links` unit tests
    that need more than one step. Each step's `working_dir` is a distinct
    directory under `tmp_path`; the blueprint file just needs to exist on
    disk (its content is never read by `collect_links`).

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to root the steps' working directories and the
        shared placeholder blueprint file in.

    Returns
    -------
    tuple[LiveStep, LiveStep, LiveWorkplan]
        The two steps and the workplan containing them.
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    step_a = LiveStep(
        name="step-a",
        application="roms_marbl",
        blueprint=bp_path,
        working_dir=tmp_path / "step-a",
    )
    step_b = LiveStep(
        name="step-b",
        application="roms_marbl",
        blueprint=bp_path,
        working_dir=tmp_path / "step-b",
    )
    workplan = LiveWorkplan(
        name="test workplan",
        description="test workplan",
        steps=[step_a, step_b],
    )
    return step_a, step_b, workplan


def _output_dir(step: Step) -> Path:
    """Return the `output` directory for a step's working directory,
    computed the same way production code does via `LiveStep`.
    """
    working_dir = LiveStep.from_step(step).working_dir
    return JobFileSystemManager(working_dir).output_dir


def _run_root() -> Path:
    return StateDirectoryManager.data_dir("fake-run-id")


def _dest_dir() -> Path:
    return _run_root() / GATHERED_OUTPUT_NAME


def test_cli_workplan_gather_happy_path(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that a unique file per step is linked into the consolidated
    `gathered_output` directory as a relative symlink pointing at the source.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    sources: dict[str, Path] = {}
    for i, step in enumerate(wp.steps):
        output = _output_dir(step)
        output.mkdir(parents=True)
        name = f"out_{i}.nc"
        src = output / name
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
    output = _output_dir(step)
    output.mkdir(parents=True)
    stale_src = output / "stale.nc"
    stale_src.write_text("data")

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)
    assert result.exit_code == 0

    dest = _dest_dir()
    assert (dest / "stale.nc").is_symlink()

    stale_src.unlink()
    new_src = output / "fresh.nc"
    new_src.write_text("data")

    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)
    assert result.exit_code == 0

    assert not (dest / "stale.nc").exists()
    assert not (dest / "stale.nc").is_symlink()
    assert (dest / "fresh.nc").is_symlink()
    assert (dest / "fresh.nc").resolve() == new_src.resolve()


def test_collect_links_mangles_names_on_collision(tmp_path: Path) -> None:
    """Verify that a filename produced by more than one step is linked once
    per producing step under a step-name-mangled name, while a filename
    produced by only one step keeps its own name.

    Built directly against `collect_links` with a hand-built two-step
    `LiveWorkplan` (rather than through the CLI and the single-step
    `executed_workplan` fixture) so the multi-step scenario always runs.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to root the fake steps' working directories in.
    """
    first_step, second_step, workplan = _make_two_step_workplan(tmp_path)

    first_output = _output_dir(first_step)
    first_output.mkdir(parents=True)
    second_output = _output_dir(second_step)
    second_output.mkdir(parents=True)

    colliding_name = "output_rst.20120201000000.nc"
    colliding_a = first_output / colliding_name
    colliding_a.write_text("data-a")
    colliding_b = second_output / colliding_name
    colliding_b.write_text("data-b")

    unique_src = first_output / "unique.nc"
    unique_src.write_text("data-unique")

    links, conflicts, mangled_count = collect_links(workplan)

    assert conflicts == {}
    assert mangled_count == 2

    mangled_a_name = f"{first_step.safe_name}__{colliding_name}"
    mangled_b_name = f"{second_step.safe_name}__{colliding_name}"
    assert links[mangled_a_name] == colliding_a
    assert links[mangled_b_name] == colliding_b
    assert colliding_name not in links

    assert links["unique.nc"] == unique_src


def test_collect_links_residual_collision_reported(tmp_path: Path) -> None:
    """Verify that a mangled name which still collides with another link
    name is reported as a residual conflict rather than silently linked.

    Built directly against `collect_links` with a hand-built two-step
    `LiveWorkplan` for the same reason as the mangling test above.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory to root the fake steps' working directories in.
    """
    first_step, second_step, workplan = _make_two_step_workplan(tmp_path)

    first_output = _output_dir(first_step)
    first_output.mkdir(parents=True)
    second_output = _output_dir(second_step)
    second_output.mkdir(parents=True)

    # Both steps produce the same basename (which would mangle to
    # `<step>__colliding.nc` each), *and* one step also directly produces a
    # file whose plain name equals the other step's mangled name, so the
    # residual collision cannot be resolved by mangling alone.
    colliding_name = "colliding.nc"
    colliding_a = first_output / colliding_name
    colliding_a.write_text("data-a")
    (second_output / colliding_name).write_text("data-b")

    residual_name = f"{first_step.safe_name}__{colliding_name}"
    residual_src = second_output / residual_name
    residual_src.write_text("data-c")

    links, conflicts, mangled_count = collect_links(workplan)

    assert residual_name not in links
    assert residual_name in conflicts
    assert set(conflicts[residual_name]) == {colliding_a, residual_src}
    assert mangled_count == 2


def test_cli_workplan_gather_skips_steps_without_output(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that steps with no `output` directory are skipped quietly.

    When no step has produced any output, an empty consolidated directory is
    still created along with a friendly informational message.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    *_, fake_run_id = executed_workplan

    runner = CliRunner()
    result = runner.invoke(app, [fake_run_id], color=False, catch_exceptions=False)

    assert result.exit_code == 0
    assert "No output was found" in result.stdout

    dest = _dest_dir()
    assert dest.exists()
    assert list(dest.iterdir()) == []


def test_cli_workplan_gather_skips_one_populated_step(
    executed_workplan: tuple[Path, Workplan, str],
) -> None:
    """Verify that only steps with an existing `output` directory contribute
    links, while other steps are skipped without error.

    Parameters
    ----------
    executed_workplan : tuple[Path, Workplan, str]
        The path to a workplan YAML file, the workplan instance, and a run-id.
    """
    _, wp, fake_run_id = executed_workplan

    step = wp.steps[0]
    output = _output_dir(step)
    output.mkdir(parents=True)
    src = output / "only.nc"
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
    existing consolidated `gathered_output` directory.

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
    `gathered_output` path exists but is not a directory.

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
