"""Tests for the `cstar run show` command."""

import json
import typing as t
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.cli.run import app as app_run
from cstar.cli.run.show import COMMAND_SHOW
from cstar.entrypoint.utils import ARG_FORMAT, ARG_NO_REFRESH
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    StepRecord,
    ensure_run_manifest,
    read_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.run_tree import (
    RUN_ID,
    FakeRun,
    build_fake_run,
)

LOOKUP: t.Final[str] = "cstar.cli.workplan.shared.TrackingRepository.get_workplan_run"
"""The tracking lookup, replaced so that a run-id resolves to a chosen record."""

EXIT_BAD_ARGS: t.Final[int] = 2
STEP: t.Final[str] = "Spin Up"
"""The fake run's step that has a directory."""


@pytest.fixture
def run(tmp_path: Path, hello_world_bp_path: Path) -> FakeRun:
    """The fake run tree, with a manifest naming its steps."""
    fake = build_fake_run(hello_world_bp_path, tmp_path)
    ensure_run_manifest(
        fake.root,
        run_id=RUN_ID,
        name="fake",
        kind="workplan",
        workplan=None,
        attempt=Attempt(start_at=datetime(2026, 10, 15, 12, 0, tzinfo=UTC)),
        steps={
            step.name: (step.application, fake.rel(step.working_dir), None)
            for step in fake.workplan.steps
        },
        runs={},
    )
    return fake


def invoke(*args: str, record: WorkplanRun | None = None) -> Result:
    """Run `cstar run show` against a repository that holds only `record`."""
    with mock.patch(LOOKUP, mock.AsyncMock(return_value=record)):
        return CliRunner().invoke(app_run, [COMMAND_SHOW, *args], color=False)


def write_record(run: FakeRun) -> None:
    """Record a finished `Spin Up` step in its directory."""
    write_step_record(
        run.spin_up,
        StepRecord(
            step=STEP,
            application="hello_world",
            blueprint=BlueprintIdentity(
                kind="Blueprint", application="hello_world", name="bp"
            ),
            outcome="completed",
            finished_at=datetime(2026, 10, 15, 13, 0, tzinfo=UTC),
        ),
    )


def test_a_run_id_shows_the_manifest(run: FakeRun) -> None:
    """Verify a tracked run is shown by its manifest, as recorded under `--no-refresh`."""
    result = invoke(RUN_ID, ARG_NO_REFRESH, record=run.record)

    assert result.exit_code == 0, result.output
    assert STEP in result.stdout
    assert "Attempts" in result.stdout
    assert "Steps" in result.stdout


def test_a_directory_shows_the_manifest_and_folds_records_in_memory(
    run: FakeRun,
) -> None:
    """Verify a directory is read as it is: a record written beside it shows,
    and nothing is written back.
    """
    write_record(run)

    result = invoke(run.root.as_posix())

    assert result.exit_code == 0, result.output
    assert STEP in result.stdout
    assert "completed" in result.stdout
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    entry = manifest.step(STEP)
    assert entry is not None
    assert entry.record is None
    assert manifest.observed_at is None


def test_a_run_id_refreshes_the_manifest(run: FakeRun) -> None:
    """Verify showing a tracked run observes it, and records what it saw."""
    write_record(run)

    with (
        mock.patch("cstar.cli.run.show.get_launcher"),
        mock.patch(
            "cstar.cli.run.show.load_run_state",
            mock.AsyncMock(return_value=DagStatus({STEP: Status.Running})),
        ),
    ):
        result = invoke(RUN_ID, record=run.record)

    assert result.exit_code == 0, result.output
    assert Status.Running.name in result.stdout
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    assert manifest.observed_at is not None
    entry = manifest.step(STEP)
    assert entry is not None
    assert entry.status == Status.Running.name
    assert entry.record is not None


def test_no_refresh_leaves_the_manifest_alone(run: FakeRun) -> None:
    """Verify `--no-refresh` neither polls the scheduler nor writes."""
    write_record(run)

    with mock.patch("cstar.cli.run.show.load_run_state") as mock_load:
        result = invoke(RUN_ID, ARG_NO_REFRESH, record=run.record)

    assert result.exit_code == 0, result.output
    mock_load.assert_not_called()
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    assert manifest.observed_at is None


def test_json_parses_back(run: FakeRun) -> None:
    """Verify `--format json` is the whole manifest and nothing else."""
    result = invoke(RUN_ID, ARG_NO_REFRESH, ARG_FORMAT, "json", record=run.record)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["run_id"] == RUN_ID


def test_yaml_names_the_run(run: FakeRun) -> None:
    """Verify `--format yaml` prints the manifest document."""
    result = invoke(RUN_ID, ARG_NO_REFRESH, ARG_FORMAT, "yaml", record=run.record)

    assert result.exit_code == 0, result.output
    assert "run_id:" in result.stdout


def test_a_bad_format_is_a_usage_error(run: FakeRun) -> None:
    """Verify an unknown format is refused before anything is read."""
    result = invoke(RUN_ID, ARG_FORMAT, "xml", record=run.record)

    assert result.exit_code == EXIT_BAD_ARGS


def test_a_directory_without_a_manifest(tmp_path: Path) -> None:
    """Verify a run directory that predates the manifest says how to get one."""
    bare = tmp_path / "bare"
    (bare / "tasks").mkdir(parents=True)

    result = invoke(bare.as_posix())

    assert result.exit_code == EXIT_BAD_ARGS
    output = " ".join(result.output.replace("│", " ").split())
    assert RUN_MANIFEST_NAME in output
    assert "predates the manifest" in output
    assert "cstar workplan run --run-id" in output


def test_a_directory_that_is_not_one(tmp_path: Path) -> None:
    """Verify a path that is no directory is a usage error."""
    result = invoke((tmp_path / "missing").as_posix())

    assert result.exit_code == EXIT_BAD_ARGS
    assert "is not a directory" in " ".join(result.output.split())


def test_a_workplan_file_is_refused(tmp_path: Path) -> None:
    """Verify a workplan file is pointed at `cstar workplan check`."""
    path = tmp_path / "workplan.yaml"
    path.write_text("name: x\n")

    result = invoke(path.as_posix())

    assert result.exit_code == EXIT_BAD_ARGS
    assert "cstar workplan check" in " ".join(result.output.replace("│", " ").split())


def test_an_unknown_run_id() -> None:
    """Verify an untracked run-id is a usage error."""
    result = invoke("no-such-run", record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "unknown run-id" in result.stderr


def test_a_tracked_run_without_a_manifest(run: FakeRun) -> None:
    """Verify a run that predates the manifest is reported, and not polled."""
    (run.root / RUN_MANIFEST_NAME).unlink()

    with mock.patch("cstar.cli.run.show.load_run_state") as mock_load:
        result = invoke(RUN_ID, record=run.record)

    assert result.exit_code == 1
    mock_load.assert_not_called()
    assert "has no manifest" in " ".join(result.stderr.split())


@pytest.mark.parametrize("text", ["{not: [valid", "schema_version: 99.0.0\n"])
def test_an_unreadable_manifest_is_reported(run: FakeRun, text: str) -> None:
    """Verify a corrupt or too new manifest ends the command with its message."""
    (run.root / RUN_MANIFEST_NAME).write_text(text)

    result = invoke(run.root.as_posix())

    assert result.exit_code == 1
    assert RUN_MANIFEST_NAME in result.stderr or "newer" in result.stderr
