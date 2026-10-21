"""Tests for the `cstar run show` command."""

import json
import typing as t
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
from click.testing import Result
from rich.console import Console
from typer.testing import CliRunner

from cstar.cli.run import app as app_run
from cstar.cli.run.show import COMMAND_SHOW, _duration
from cstar.entrypoint.utils import ARG_FORMAT, ARG_NO_REFRESH
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    StepRecord,
    ensure_run_manifest,
    read_run_manifest,
    write_run_manifest,
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


def _flat(text: str) -> str:
    """Join a CLI output into one line without rich's box borders.

    Rich wraps an error message at the terminal width and draws a border down
    each side, so a phrase can land across a `│`; the width differs between
    machines (80 columns on CI).
    """
    return " ".join(text.replace("│", " ").split())


def invoke(*args: str, record: WorkplanRun | None = None) -> Result:
    """Run `cstar run show` against a repository that holds only `record`."""
    with mock.patch(LOOKUP, mock.AsyncMock(return_value=record)):
        return CliRunner().invoke(app_run, [COMMAND_SHOW, *args], color=False)


def write_record(
    run: FakeRun,
    *,
    start_at: datetime | None = None,
    finished_at: datetime = datetime(2026, 10, 15, 13, 0, tzinfo=UTC),
) -> None:
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
            start_at=start_at,
            finished_at=finished_at,
        ),
    )


def submit(run: FakeRun, submitted_at: datetime | None) -> None:
    """Record in the manifest that `Spin Up` was submitted to SLURM as job 4242."""
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    entry = manifest.step(STEP)
    assert entry is not None
    entry.launcher = "slurm"
    entry.job_id = "4242"
    entry.submitted_at = submitted_at
    write_run_manifest(run.root, manifest)


def jobs_row(run: FakeRun, step: str = STEP) -> dict[str, str]:
    """Show the run's directory and read `step`'s row of the jobs table by column."""
    result = invoke(run.root.as_posix())

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "Jobs")
    table = lines[start : next(i for i in range(start, len(lines)) if "└" in lines[i])]
    header = next(line for line in table if "┃" in line)
    columns = [cell.strip() for cell in header.strip("┃ ").split("┃")]
    rows = [
        [cell.strip() for cell in line.strip("│ ").split("│")]
        for line in table
        if line.startswith("│")
    ]
    (cells,) = (row for row in rows if row[0] == step)
    return dict(zip(columns, cells, strict=True))


def test_a_run_id_shows_the_manifest(run: FakeRun) -> None:
    """Verify a tracked run is shown by its manifest, as recorded under `--no-refresh`."""
    result = invoke(RUN_ID, ARG_NO_REFRESH, record=run.record)

    assert result.exit_code == 0, result.output
    assert STEP in result.stdout
    assert "Attempts" in result.stdout
    assert "Steps" in result.stdout


def test_the_attempts_show_the_directory_each_carried_its_inputs_in(
    run: FakeRun,
) -> None:
    """Verify the last column of the attempts table names the attempt's directory
    under `attempts/`, and is a dash for an attempt that carried none.
    """
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    started = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
    manifest.attempts = [
        Attempt(start_at=started, directory="attempts/20261019T120000"),
        Attempt(start_at=started),
    ]
    write_run_manifest(run.root, manifest)

    with mock.patch("cstar.cli.run.show.console", Console(width=200)):
        result = invoke(run.root.as_posix())

    assert result.exit_code == 0, result.output
    rows = [line for line in result.stdout.splitlines() if "scheduled" in line]
    assert [row.strip("│ ").split("│")[-1].strip() for row in rows] == [
        "20261019T120000",
        "-",
    ]
    header = next(line for line in result.stdout.splitlines() if "Started" in line)
    assert header.strip("┃ ").split("┃")[-1].strip() == "Inputs"


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
            mock.AsyncMock(return_value=DagStatus({STEP: Status.Done})),
        ),
    ):
        result = invoke(RUN_ID, record=run.record)

    assert result.exit_code == 0, result.output
    assert Status.Done.name in result.stdout
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    assert manifest.observed_at is not None
    entry = manifest.step(STEP)
    assert entry is not None
    assert entry.status == Status.Done.name
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
    output = _flat(result.output)
    assert RUN_MANIFEST_NAME in output
    assert "predates the manifest" in output
    assert "cstar workplan run --run-id" in output


def test_a_directory_that_is_not_one(tmp_path: Path) -> None:
    """Verify a path that is no directory is a usage error."""
    result = invoke((tmp_path / "missing").as_posix())

    assert result.exit_code == EXIT_BAD_ARGS
    assert "is not a directory" in _flat(result.output)


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


SUBMITTED = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def test_the_jobs_table_shows_the_job_and_how_long_it_queued_and_ran(
    run: FakeRun,
) -> None:
    """Verify a step with a submission and a record shows its job, the time from
    the submission to the step starting, and the time it ran.
    """
    submit(run, SUBMITTED)
    write_record(
        run,
        start_at=SUBMITTED + timedelta(minutes=5, seconds=7),
        finished_at=SUBMITTED + timedelta(hours=1, minutes=5, seconds=7),
    )

    row = jobs_row(run)

    assert row["Job"] == "slurm 4242"
    assert row["Queued"] == "0:05:07"
    assert row["Wall"] == "1:00:00"


def test_a_step_that_ran_over_a_day_shows_its_hours(run: FakeRun) -> None:
    """Verify durations over 24 hours are hours, never days."""
    submit(run, SUBMITTED)
    start = SUBMITTED + timedelta(hours=30)
    write_record(run, start_at=start, finished_at=start + timedelta(hours=30))

    row = jobs_row(run)

    assert row["Queued"] == "30:00:00"
    assert row["Wall"] == "30:00:00"


def test_a_step_without_a_submission_or_a_record_shows_dashes(run: FakeRun) -> None:
    """Verify a step never submitted by an orchestrator, and one with no record,
    show no job and no durations.
    """
    write_record(run, start_at=SUBMITTED, finished_at=SUBMITTED + timedelta(hours=1))

    row = jobs_row(run)

    assert (row["Job"], row["Queued"], row["Wall"]) == ("-", "-", "1:00:00")
    unrecorded = jobs_row(run, step="run")
    assert (unrecorded["Job"], unrecorded["Queued"], unrecorded["Wall"]) == ("-",) * 3


def test_a_step_that_never_started_shows_no_durations(run: FakeRun) -> None:
    """Verify a record without a start leaves both durations as dashes."""
    submit(run, SUBMITTED)
    write_record(run, start_at=None)

    row = jobs_row(run)

    assert row["Job"] == "slurm 4242"
    assert (row["Queued"], row["Wall"]) == ("-", "-")


def test_a_record_that_started_before_the_submission_shows_no_queue_time(
    run: FakeRun,
) -> None:
    """Verify a record from an earlier run of the step, which started before the
    submission on the entry, has no queue time; its wall time still shows.
    """
    submit(run, SUBMITTED)
    start = SUBMITTED - timedelta(hours=2)
    write_record(run, start_at=start, finished_at=start + timedelta(minutes=30))

    row = jobs_row(run)

    assert (row["Queued"], row["Wall"]) == ("-", "0:30:00")


def test_a_submission_without_a_launcher_shows_the_bare_job_id(run: FakeRun) -> None:
    """Verify an entry that records no launcher shows only its job id."""
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    entry = manifest.step(STEP)
    assert entry is not None
    entry.job_id = "4242"
    write_run_manifest(run.root, manifest)

    assert jobs_row(run)["Job"] == "4242"


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0:00:00"),
        (59, "0:00:59"),
        (61, "0:01:01"),
        (3600, "1:00:00"),
        (86399, "23:59:59"),
        (30 * 3600, "30:00:00"),
        (100 * 3600 + 61, "100:01:01"),
    ],
)
def test_duration_formats_hours_minutes_and_seconds(
    seconds: int, expected: str
) -> None:
    """Verify a duration is `H:MM:SS` with unbounded hours."""
    assert _duration(SUBMITTED, SUBMITTED + timedelta(seconds=seconds)) == expected


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, SUBMITTED),
        (SUBMITTED, None),
        (None, None),
        (SUBMITTED, SUBMITTED - timedelta(seconds=1)),
    ],
    ids=["no-start", "no-end", "neither", "negative"],
)
def test_duration_is_a_dash_for_a_missing_moment_or_a_negative_span(
    start: datetime | None, end: datetime | None
) -> None:
    """Verify a missing moment, or an end before the start, is a dash."""
    assert _duration(start, end) == "-"
