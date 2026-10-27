"""Tests for `cstar run promote`: copying, linking or moving a run into the project's runs area.

The runs are built elsewhere than the data home, as a run that is kept on another
disk would be, with a finished attempt and a completed step record that lists a
product.
"""

import logging
import shutil
import typing as t
from dataclasses import replace
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.exceptions import CstarError
from cstar.cli.run import app as app_run
from cstar.orchestration.index import run_uri
from cstar.orchestration.manifest import (
    STEP_RECORD_NAME,
    AttemptStatus,
    Product,
    StepRecord,
    read_run_manifest,
    read_step_record,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.orchestration import ProcessHandle, Status
from cstar.orchestration.promotion import staging_dir
from cstar.orchestration.run_check import Finding, FindingKind, RunCheckReport
from cstar.orchestration.serialization import serialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.tracking import Access, TrackingRepository, WorkplanRun
from cstar.tests.unit_tests.orchestration.manifest_runs import STARTED, BuiltRun

EXIT_BAD_ARGS: t.Final[int] = 2
STEP: t.Final[str] = "Spin Up"
PRODUCT: t.Final[str] = "output/only.dat"
CONTENT: t.Final[str] = "data"


def invoke(*args: str) -> Result:
    """Run a `cstar run` command against the tracking of the test's data home."""
    return CliRunner().invoke(app_run, list(args), color=False)


def tracked(run_id: str) -> WorkplanRun | None:
    """The tracking record of a run-id, as the repository finds it."""
    return TrackingRepository().get_workplan_run_sync(run_id)


def text(result: Result, flatten: t.Callable[[str], str]) -> str:
    """Everything a command printed, with whitespace collapsed."""
    return flatten(result.output + result.stderr)


@pytest.fixture
def elsewhere(
    tmp_path: Path, build_run: t.Callable[..., BuiltRun]
) -> t.Callable[..., BuiltRun]:
    """Build a finished run that lives in a directory of its own."""

    def build(
        run_id: str = "arrived", *, finished: bool = True, sha256: str = ""
    ) -> BuiltRun:
        built = build_run(tmp_path / "origin" / run_id, run_id, steps=(STEP,))
        step = built.step_dir(STEP)
        (step / PRODUCT).parent.mkdir()
        (step / PRODUCT).write_text(CONTENT)
        write_step_record(
            step,
            StepRecord(
                step=STEP,
                application="hello_world",
                blueprint=BlueprintIdentity(
                    kind="Blueprint",
                    application="hello_world",
                    name="bp",
                    content_hash="abc",
                ),
                outcome="completed",
                finished_at=STARTED,
                products=[
                    Product(
                        kind="output",
                        path=PRODUCT,
                        size_bytes=len(CONTENT),
                        sha256=sha256,
                    )
                ],
            ),
        )
        manifest = read_run_manifest(built.root)
        assert manifest is not None
        if finished:
            manifest.attempts[-1].finished_at = STARTED
            manifest.attempts[-1].status = AttemptStatus.done
        write_run_manifest(built.root, manifest)

        # moved, so that the files that name the run's directory name the old one
        arrived = tmp_path / "elsewhere" / run_id
        shutil.copytree(built.root, arrived, symlinks=True)
        shutil.rmtree(built.root)
        return replace(built, root=arrived)

    return build


def test_the_default_destination_needs_a_project_home(
    elsewhere: t.Callable[..., BuiltRun],
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify with no project home and no `--to` the command says what to set."""
    built = elsewhere()
    monkeypatch.delenv(ENV_CSTAR_PROJECT_HOME, raising=False)
    monkeypatch.delenv("PROJECT", raising=False)

    result = invoke("promote", str(built.root))

    assert result.exit_code == EXIT_BAD_ARGS
    message = text(result, flatten_cli_output)
    assert ENV_CSTAR_PROJECT_HOME in message
    assert "--to" in message


def test_dry_run_prints_the_plan_and_writes_nothing(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify `--dry-run` lists the groups and the total, and leaves no destination."""
    built = elsewhere()

    result = invoke("promote", str(built.root), "--dry-run")

    assert result.exit_code == 0, result.output
    output = flatten_cli_output(result.output)
    assert "artifacts:" in output
    assert "products of Spin Up: 1 file(s), 4 B" in output
    assert "Total:" in output
    assert (
        f"Would copy run {built.run_id!r} ({run_uri(built.uuid)}) to "
        f"{project_runs / built.run_id}"
    ) in output
    assert not (project_runs / built.run_id).exists()
    assert not project_runs.exists()


def test_a_copy_lands_in_the_project_runs_area(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a copy holds what the run is made of, leaves the source alone, and is refused twice."""
    built = elsewhere()
    before = sorted(built.root.rglob("*"))
    manifest = built.manifest_bytes
    destination = project_runs / built.run_id

    result = invoke("promote", str(built.root))

    assert result.exit_code == 0, result.output + result.stderr
    assert f"Promoted run {built.run_id!r}" in flatten_cli_output(result.output)
    copied = read_run_manifest(destination)
    assert copied is not None
    assert copied.id == built.uuid
    assert (destination / f"{built.run_id}_transformed.yaml").is_file()
    step = destination / "tasks" / "spin-up"
    assert (step / STEP_RECORD_NAME).is_file()
    assert (step / PRODUCT).read_text() == CONTENT
    assert (step / "work").is_dir() or step.is_dir()
    assert sorted(built.root.rglob("*")) == before
    assert built.manifest_bytes == manifest

    again = invoke("promote", str(built.root))

    assert again.exit_code == EXIT_BAD_ARGS
    assert "already promoted" in text(again, flatten_cli_output)


def test_to_puts_the_copy_under_the_directory_named(
    elsewhere: t.Callable[..., BuiltRun], tmp_path: Path, project_runs: Path
) -> None:
    """Verify `--to DIR` puts the copy at `DIR/<run-id>`, not in the project's runs area."""
    built = elsewhere()
    target = tmp_path / "keep"

    result = invoke("promote", str(built.root), "--to", str(target))

    assert result.exit_code == 0, result.output + result.stderr
    copied = read_run_manifest(target / built.run_id)
    assert copied is not None
    assert copied.id == built.uuid
    assert not project_runs.exists()


def test_a_destination_holding_another_run_is_refused(
    elsewhere: t.Callable[..., BuiltRun],
    build_run: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify another run at the destination is refused, naming both uuids."""
    built = elsewhere()
    other = build_run(project_runs / built.run_id, built.run_id)

    result = invoke("promote", str(built.root))

    assert result.exit_code == EXIT_BAD_ARGS
    message = text(result, flatten_cli_output)
    assert run_uri(built.uuid) in message
    assert run_uri(other.uuid) in message
    assert not (other.step_dir(STEP) / PRODUCT).exists()


def test_a_destination_that_is_not_a_run_is_refused(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a directory that holds no run is never copied over."""
    built = elsewhere()
    (project_runs / built.run_id).mkdir(parents=True)

    result = invoke("promote", str(built.root))

    assert result.exit_code == EXIT_BAD_ARGS
    assert "is not a run" in text(result, flatten_cli_output)
    assert not any((project_runs / built.run_id).iterdir())


def test_a_missing_product_stops_the_copy(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a recorded product that is gone from the source copies nothing, exit 1."""
    built = elsewhere()
    (built.step_dir(STEP) / PRODUCT).unlink()

    result = invoke("promote", str(built.root))

    assert result.exit_code == 1
    message = text(result, flatten_cli_output)
    assert "cstar run check" in message
    assert "only.dat" in message
    assert not project_runs.exists()


def test_link_makes_a_symlink_and_materialize_replaces_it(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a link resolves to the source, is refused twice, and `materialize` makes it a copy."""
    built = elsewhere()
    destination = project_runs / built.run_id

    linked = invoke("promote", str(built.root), "--mode", "link")

    assert linked.exit_code == 0, linked.output + linked.stderr
    assert destination.is_symlink()
    assert destination.resolve() == built.root.resolve()

    for mode in ("link", "copy"):
        again = invoke("promote", str(built.root), "--mode", mode)

        assert again.exit_code == EXIT_BAD_ARGS
        assert "--mode materialize" in text(again, flatten_cli_output)

    result = invoke("promote", str(built.root), "--mode", "materialize")

    assert result.exit_code == 0, result.output + result.stderr
    assert "Materialized run" in flatten_cli_output(result.output)
    assert not destination.is_symlink()
    copied = read_run_manifest(destination)
    assert copied is not None
    assert copied.id == built.uuid
    assert (built.root / "cstar-run.yaml").is_file()


def test_materialize_needs_a_link(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify `--mode materialize` of a free destination is refused."""
    built = elsewhere()

    result = invoke("promote", str(built.root), "--mode", "materialize")

    assert result.exit_code == EXIT_BAD_ARGS
    assert "not one" in text(result, flatten_cli_output)
    assert not (project_runs / built.run_id).exists()


def test_a_run_in_the_project_runs_area_is_not_linked(
    project_run: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify linking a run that is in the project's runs area already is refused."""
    built = project_run("inplace")

    result = invoke("promote", str(built.root), "--mode", "link")

    assert result.exit_code == EXIT_BAD_ARGS
    assert "already in the project runs area" in text(result, flatten_cli_output)


def test_move_of_a_directory_removes_the_source(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify a run named by its directory, which tracking does not know, is moved whole."""
    built = elsewhere()

    result = invoke("promote", str(built.root), "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert not built.root.exists()
    destination = project_runs / built.run_id
    assert (destination / "tasks" / "spin-up" / PRODUCT).read_text() == CONTENT
    assert read_run_manifest(destination) is not None
    assert tracked(built.run_id) is None


def test_move_copies_the_whole_directory_whatever_include_says(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify a move loses nothing: a file no product names arrives in the copy."""
    built = elsewhere()
    stray = built.step_dir(STEP) / "output" / "extra.txt"
    stray.write_text("stray")

    result = invoke("promote", str(built.root), "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert not built.root.exists()
    copied = project_runs / built.run_id / "tasks" / "spin-up" / "output" / "extra.txt"
    assert copied.read_text() == "stray"


def test_a_copy_still_leaves_a_stray_file_behind(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify only a move widens the plan: a copy takes what `--include` says."""
    built = elsewhere()
    (built.step_dir(STEP) / "output" / "extra.txt").write_text("stray")

    result = invoke("promote", str(built.root))

    assert result.exit_code == 0, result.output + result.stderr
    assert not (
        project_runs / built.run_id / "tasks" / "spin-up" / "output" / "extra.txt"
    ).exists()


def test_a_second_transformed_workplan_is_refused_before_the_copy(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify two transformed workplans stop a tracked run as they stop an untracked one."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    (built.root / "stale_transformed.yaml").write_text("stale: true\n")

    result = invoke("promote", built.run_id)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "more than one transformed workplan" in text(result, flatten_cli_output)
    assert not (project_runs / built.run_id).exists()
    assert not project_runs.exists()


def test_a_move_tracks_the_copy_before_it_removes_the_source(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a source that cannot be removed leaves the record already on the copy."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0

    def refuse(root: Path) -> t.NoReturn:
        raise OSError("device busy")

    monkeypatch.setattr("cstar.cli.run.promote.remove_run", refuse)

    result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == 1
    message = text(result, flatten_cli_output)
    assert "and tracked it, but the source at" in message
    assert "was not removed: device busy" in message
    assert built.root.is_dir()
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == project_runs / built.run_id


def test_a_move_that_cannot_track_keeps_the_source(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a record that cannot be written stops the move before anything is removed."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0

    def refuse(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise CstarError("record locked")

    monkeypatch.setattr("cstar.cli.run.promote.write_attached_record", refuse)

    result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == 1
    assert "the move did not finish: record locked" in text(result, flatten_cli_output)
    assert (built.root / "cstar-run.yaml").is_file()
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == built.root


def test_move_needs_the_directory_of_a_run_found_by_its_manifest(
    local_run: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run found only by its manifest is not removed by its run-id."""
    built = local_run("indexed")

    result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == EXIT_BAD_ARGS
    assert "pass its directory" in text(result, flatten_cli_output)
    assert built.root.is_dir()
    assert not project_runs.exists()


@pytest.mark.parametrize("where", ["project", "elsewhere"])
def test_move_of_a_tracked_run_updates_its_record(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    tmp_path: Path,
    where: str,
) -> None:
    """Verify the record follows the run, attached, read-only in the project's area and owned outside."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    args = ["--to", str(tmp_path / "keep")] if where == "elsewhere" else []
    destination = (
        project_runs if where == "project" else tmp_path / "keep"
    ) / built.run_id

    result = invoke("promote", built.run_id, "--mode", "move", *args)

    assert result.exit_code == 0, result.output + result.stderr
    assert not built.root.exists()
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == destination
    assert record.trx_workplan_path == destination / f"{built.run_id}_transformed.yaml"
    assert record.sentinels == set()
    assert record.attached
    expected = Access.read_only if where == "project" else Access.owned
    assert record.access is expected
    assert ("--own" in result.output) is (where == "project")


def test_move_of_a_run_tracked_live_drops_its_sentinels(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify moving a run this machine tracked as live removes its sentinel files
    and leaves an attached record that names none.
    """
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    sentinels = {
        StateRepository.sentinel_path(name, run_id=built.run_id)
        for name in (STEP, "run")
    }
    for sentinel in sentinels:
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        handle = ProcessHandle(
            pid="1", name=sentinel.stem, run_id=built.run_id, status=Status.Done
        )
        assert serialize(sentinel, handle)

    def go_live(run: WorkplanRun) -> None:
        run.attached = False
        run.sentinels = set(sentinels)

    TrackingRepository().update_workplan_run_sync(built.run_id, go_live)
    live = tracked(built.run_id)
    assert live is not None
    assert live.live
    assert live.sentinels == sentinels

    result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert not any(sentinel.exists() for sentinel in sentinels)
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == project_runs / built.run_id
    assert record.sentinels == set()
    assert record.attached is True
    assert not record.live


def test_move_of_a_run_with_a_step_still_in_flight_keeps_that_sentinel(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify moving a run tracked as live removes the sentinels of finished steps,
    leaves, with a warning, the one of a step that has not reached a terminal
    status, and still attaches the record with no sentinels.

    Parameters
    ----------
    elsewhere : Callable[..., BuiltRun]
        Factory building a finished run in a directory of its own.
    project_runs : Path
        The project's runs area.
    caplog : pytest.LogCaptureFixture
        Fixture used to capture log output.
    """
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0
    statuses = {STEP: Status.Running, "run": Status.Done}
    sentinels = {
        name: StateRepository.sentinel_path(name, run_id=built.run_id)
        for name in statuses
    }
    for name, sentinel in sentinels.items():
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        handle = ProcessHandle(
            pid="1", name=name, run_id=built.run_id, status=statuses[name]
        )
        assert serialize(sentinel, handle)
    state_dir = sentinels[STEP].parent

    def go_live(run: WorkplanRun) -> None:
        run.attached = False
        run.sentinels = set(sentinels.values())

    TrackingRepository().update_workplan_run_sync(built.run_id, go_live)

    with caplog.at_level(logging.WARNING):
        result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert sentinels[STEP].exists()
    assert not sentinels["run"].exists()
    assert state_dir.is_dir()
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    [kept] = [w for w in warnings if "Left the sentinel" in w]
    assert str(sentinels[STEP]) in kept
    assert STEP in kept
    assert "Running" in kept
    assert str(sentinels["run"]) not in kept
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == project_runs / built.run_id
    assert record.attached is True
    assert record.sentinels == set()


def test_move_of_a_tracked_run_named_by_its_directory_follows_the_record(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify naming a tracked run by its directory still moves its record along."""
    built = elsewhere()
    assert invoke("attach", str(built.root)).exit_code == 0

    result = invoke("promote", str(built.root), "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert not built.root.exists()
    record = tracked(built.run_id)
    assert record is not None
    assert record.output_path == project_runs / built.run_id
    assert record.access is Access.read_only


def test_move_of_a_read_only_run_named_by_its_directory_is_refused(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify the directory spelling does not get around a read-only record."""
    built = elsewhere()
    assert invoke("attach", str(built.root), "--read-only").exit_code == 0

    result = invoke("promote", str(built.root), "--mode", "move")

    assert result.exit_code == EXIT_BAD_ARGS, result.output
    assert built.root.is_dir()


def test_move_of_a_read_only_run_is_refused(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a run attached read-only is not removed."""
    built = elsewhere()
    assert invoke("attach", str(built.root), "--read-only").exit_code == 0

    result = invoke("promote", built.run_id, "--mode", "move")

    assert result.exit_code == EXIT_BAD_ARGS
    assert "read-only" in text(result, flatten_cli_output)
    assert built.root.is_dir()
    assert not project_runs.exists()


def test_an_unfinished_run_is_copied_with_a_warning_and_never_moved(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify an open attempt is a warning for a copy and a refusal for a move."""
    built = elsewhere(finished=False)

    moved = invoke("promote", str(built.root), "--mode", "move")

    assert moved.exit_code == EXIT_BAD_ARGS
    assert "has not finished" in text(moved, flatten_cli_output)
    assert built.root.is_dir()
    assert not project_runs.exists()

    copied = invoke("promote", str(built.root))

    assert copied.exit_code == 0, copied.output + copied.stderr
    assert "has not finished" in flatten_cli_output(copied.stderr)
    assert (project_runs / built.run_id).is_dir()


def test_verify_records_digests_in_the_copy_only(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify `--verify` records a digest of each product in the copy and leaves the source's record."""
    built = elsewhere()

    result = invoke("promote", str(built.root), "--verify")

    assert result.exit_code == 0, result.output + result.stderr
    assert "Recorded 1 digest(s)" in result.output
    copied = read_step_record(project_runs / built.run_id / "tasks" / "spin-up")
    assert copied is not None
    assert copied.products[0].digest.startswith("sha256:")
    source = read_step_record(built.step_dir(STEP))
    assert source is not None
    assert source.products[0].digest == ""


@pytest.mark.parametrize("mode", ["copy", "move"])
def test_a_copy_with_problems_exits_1_and_keeps_the_source(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
    mode: str,
) -> None:
    """Verify a problem of the copy's own is exit 1, and a move then removes nothing."""
    built = elsewhere()

    def broken(run_root: Path, **_: t.Any) -> RunCheckReport:
        finding = Finding(FindingKind.PRODUCT_SIZE, run_root / PRODUCT, path="1 != 4")
        return RunCheckReport(run_root=run_root, findings=(finding,))

    monkeypatch.setattr("cstar.cli.run.promote.check_run", broken)

    result = invoke("promote", str(built.root), "--mode", mode)

    assert result.exit_code == 1
    assert "Promoted with problems" in text(result, flatten_cli_output)
    assert built.root.is_dir()
    assert (project_runs / built.run_id).is_dir()


def test_absolute_paths_are_not_problems_of_the_copy(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify the source's absolute paths, found in the copy, do not make a promotion fail."""
    built = elsewhere()

    def inherited(run_root: Path, **_: t.Any) -> RunCheckReport:
        finding = Finding(
            FindingKind.ABSOLUTE_PATH, run_root / "x.yaml", line=1, path="/old/root"
        )
        return RunCheckReport(run_root=run_root, findings=(finding,))

    monkeypatch.setattr("cstar.cli.run.promote.check_run", inherited)

    result = invoke("promote", str(built.root), "--mode", "move")

    assert result.exit_code == 0, result.output + result.stderr
    assert "/old/root" in flatten_cli_output(result.output)
    assert not built.root.exists()


def test_gathered_output_is_rebuilt_in_the_copy(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify a run that has a `gathered_output` has one in the copy, linking the copied product."""
    built = elsewhere()
    gathered = built.root / "gathered_output"
    gathered.mkdir()
    (gathered / "only.dat").symlink_to(built.step_dir(STEP) / PRODUCT)

    result = invoke("promote", str(built.root))

    assert result.exit_code == 0, result.output + result.stderr
    destination = project_runs / built.run_id
    link = destination / "gathered_output" / "only.dat"
    assert link.is_symlink()
    assert link.resolve() == (destination / "tasks" / "spin-up" / PRODUCT).resolve()


def test_a_failed_copy_exits_1_and_leaves_nothing(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a file system error while copying is one line and exit 1, not a traceback."""
    built = elsewhere()
    destination = project_runs / built.run_id

    def refuse(source: Path, target: Path) -> t.NoReturn:
        raise OSError("no space left on device")

    monkeypatch.setattr("cstar.orchestration.promotion._copy_file", refuse)

    result = invoke("promote", str(built.root))

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    message = result.stderr.strip()
    assert message == "Error: no space left on device"
    assert not destination.exists()
    assert not staging_dir(destination).exists()


@pytest.mark.parametrize("extra", [[], ["--dry-run"]])
def test_a_step_directory_outside_the_run_exits_1(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    flatten_cli_output: t.Callable[[str], str],
    extra: list[str],
) -> None:
    """Verify a manifest that records an absolute step directory is one line and exit 1."""
    built = elsewhere()
    manifest = read_run_manifest(built.root)
    assert manifest is not None
    manifest.steps[0].working_dir = str(built.step_dir(STEP))
    write_run_manifest(built.root, manifest)

    result = invoke("promote", str(built.root), *extra)

    assert result.exit_code == 1, result.output + result.stderr
    assert isinstance(result.exception, SystemExit)
    message = flatten_cli_output(result.stderr)
    assert message.startswith("Error: The path ")
    assert "leaves the run directory" in message
    assert "\n" not in result.stderr.strip()
    assert not project_runs.exists()


def test_a_failure_after_the_copy_says_where_the_copy_is(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify an error after the copy was renamed into place names the copy, exit 1."""
    built = elsewhere()
    (built.root / "gathered_output").mkdir()
    destination = project_runs / built.run_id

    def refuse(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise OSError("permission denied")

    monkeypatch.setattr("cstar.cli.run.promote._regather", refuse)

    result = invoke("promote", str(built.root))

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    message = flatten_cli_output(result.stderr)
    assert message == (
        f"Copied to {destination}, but the promotion did not finish: permission denied"
    )
    assert destination.is_dir()
    assert built.root.is_dir()


def test_a_link_that_cannot_be_made_exits_1(
    elsewhere: t.Callable[..., BuiltRun],
    project_runs: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a file system error while linking is one line and exit 1, not a traceback."""
    built = elsewhere()

    def refuse(*args: t.Any, **kwargs: t.Any) -> t.NoReturn:
        raise PermissionError("read-only file system")

    monkeypatch.setattr("cstar.cli.run.promote.link_run", refuse)

    result = invoke("promote", str(built.root), "--mode", "link")

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert result.stderr.strip() == "Error: read-only file system"


def test_an_unknown_include_is_refused(
    elsewhere: t.Callable[..., BuiltRun], project_runs: Path
) -> None:
    """Verify `--include` takes only what the library knows."""
    built = elsewhere()

    result = invoke("promote", str(built.root), "--include", "bogus")

    assert result.exit_code == EXIT_BAD_ARGS
    assert not project_runs.exists()


def test_a_directory_that_is_not_a_run_is_refused(
    tmp_path: Path, project_runs: Path
) -> None:
    """Verify a directory without a manifest is refused as a bad argument."""
    (tmp_path / "plain").mkdir()

    result = invoke("promote", str(tmp_path / "plain"))

    assert result.exit_code == EXIT_BAD_ARGS
