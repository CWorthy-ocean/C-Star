"""Tests for the `cstar run show` command."""

import json
import typing as t
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import yaml
from click.testing import Result
from rich.console import Console
from typer.testing import CliRunner

from cstar.cli.run import app as app_run
from cstar.cli.run.show import COMMAND_SHOW, _duration
from cstar.entrypoint.utils import ARG_FORMAT, ARG_NO_REFRESH
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.index import run_uri
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    ExecutionFacts,
    ExternalRun,
    StepRecord,
    ensure_run_manifest,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.manifest_runs import BuiltRun
from cstar.tests.unit_tests.orchestration.run_tree import (
    RUN_ID,
    FakeRun,
    build_fake_run,
)

LOOKUP: t.Final[str] = (
    "cstar.orchestration.index.TrackingRepository.get_workplan_run_sync"
)
"""The tracking lookup the run resolver makes, replaced so that a run-id resolves
to a chosen record."""

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
    with mock.patch(LOOKUP, mock.Mock(return_value=record)):
        return CliRunner().invoke(app_run, [COMMAND_SHOW, *args], color=False)


def write_record(
    run: FakeRun,
    *,
    start_at: datetime | None = None,
    finished_at: datetime = datetime(2026, 10, 15, 13, 0, tzinfo=UTC),
    job_id: str = "",
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
            execution=ExecutionFacts(
                scheduler="slurm" if job_id else "local", job_id=job_id
            ),
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


def test_the_external_runs_show_the_uuid_each_resolved_to(run: FakeRun) -> None:
    """Verify the external runs table names each referenced run by its uuid."""
    uuid = "0b2f5c9e-1d4a-4e8b-9f3c-7a6d5e4c3b2a"
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    manifest.external.runs = [ExternalRun(alias="base", run_id="spinup", id=uuid)]
    write_run_manifest(run.root, manifest)

    with mock.patch("cstar.cli.run.show.console", Console(width=200)):
        result = invoke(run.root.as_posix())

    assert result.exit_code == 0, result.output
    (row,) = (line for line in result.stdout.splitlines() if "spinup" in line)
    assert [cell.strip() for cell in row.strip("│ ").split("│")[:3]] == [
        "base",
        "spinup",
        uuid,
    ]


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


@pytest.mark.parametrize("format_", ["table", "json", "yaml"])
def test_a_manifest_with_keys_a_newer_release_wrote_is_shown_in_every_format(
    run: FakeRun, format_: str
) -> None:
    """Verify a newer-minor manifest holding a key this build does not know is
    shown with the key ignored, in each format, and its file is left as it was.
    """
    path = run.root / RUN_MANIFEST_NAME
    document = yaml.safe_load(path.read_text())
    major, minor, _ = document["schema_version"].split(".")
    document["schema_version"] = f"{major}.{int(minor) + 1}.0"
    document["future_field"] = True
    path.write_text(yaml.safe_dump(document))
    before = path.read_bytes()

    result = invoke(RUN_ID, ARG_NO_REFRESH, ARG_FORMAT, format_, record=run.record)

    assert result.exit_code == 0, result.output
    assert "future_field" not in result.stdout
    assert path.read_bytes() == before


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
    assert "cstar workplan submit --run-id" in output


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


def test_an_unknown_run_id(flatten_cli_output: t.Callable[[str], str]) -> None:
    """Verify a run-id that is neither tracked nor in a manifest is a usage error."""
    result = invoke("no-such-run", record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "No run found for run-id no-such-run" in flatten_cli_output(result.stderr)


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


def test_the_jobs_table_shows_the_job_and_how_long_it_waited_and_ran(
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
    assert row["Waited"] == "0:05:07"
    assert row["Wall"] == "1:00:00"


def test_a_step_that_ran_over_a_day_shows_its_hours(run: FakeRun) -> None:
    """Verify durations over 24 hours are hours, never days."""
    submit(run, SUBMITTED)
    start = SUBMITTED + timedelta(hours=30)
    write_record(run, start_at=start, finished_at=start + timedelta(hours=30))

    row = jobs_row(run)

    assert row["Waited"] == "30:00:00"
    assert row["Wall"] == "30:00:00"


def test_a_step_without_a_submission_or_a_record_shows_dashes(run: FakeRun) -> None:
    """Verify a step never submitted by an orchestrator, and one with no record,
    show no job and no durations.
    """
    write_record(run, start_at=SUBMITTED, finished_at=SUBMITTED + timedelta(hours=1))

    row = jobs_row(run)

    assert (row["Job"], row["Waited"], row["Wall"]) == ("-", "-", "1:00:00")
    unrecorded = jobs_row(run, step="run")
    assert (unrecorded["Job"], unrecorded["Waited"], unrecorded["Wall"]) == ("-",) * 3


def test_a_step_that_never_started_shows_no_durations(run: FakeRun) -> None:
    """Verify a record without a start leaves both durations as dashes."""
    submit(run, SUBMITTED)
    write_record(run, start_at=None)

    row = jobs_row(run)

    assert row["Job"] == "slurm 4242"
    assert (row["Waited"], row["Wall"]) == ("-", "-")


def test_a_record_that_started_before_the_submission_shows_no_times(
    run: FakeRun,
) -> None:
    """Verify a record from an earlier run of the step, which started before the
    submission on the entry, shows neither time beside the new job.
    """
    submit(run, SUBMITTED)
    start = SUBMITTED - timedelta(hours=2)
    write_record(run, start_at=start, finished_at=start + timedelta(minutes=30))

    row = jobs_row(run)

    assert (row["Job"], row["Waited"], row["Wall"]) == ("slurm 4242", "-", "-")


def test_a_record_of_another_slurm_job_shows_no_times(run: FakeRun) -> None:
    """Verify a record that names another SLURM job than the latest submission, as
    one left by an earlier run when the new job died before writing its own,
    shows neither time.
    """
    submit(run, SUBMITTED)
    write_record(
        run,
        start_at=SUBMITTED + timedelta(minutes=5),
        finished_at=SUBMITTED + timedelta(hours=1),
        job_id="4100",
    )

    row = jobs_row(run)

    assert (row["Waited"], row["Wall"]) == ("-", "-")


def test_a_record_of_the_submitted_slurm_job_shows_its_times(run: FakeRun) -> None:
    """Verify a record that names the submitted SLURM job shows both times."""
    submit(run, SUBMITTED)
    write_record(
        run,
        start_at=SUBMITTED + timedelta(minutes=5),
        finished_at=SUBMITTED + timedelta(hours=1),
        job_id="4242",
    )

    row = jobs_row(run)

    assert (row["Waited"], row["Wall"]) == ("0:05:00", "0:55:00")


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


class Observed(t.NamedTuple):
    """The calls showing a tracked run makes to bring its manifest up to date."""

    status: mock.AsyncMock
    refresh: mock.Mock


@pytest.fixture
def observed() -> t.Iterator[Observed]:
    """Replace what refreshing the manifest of a tracked run reaches for."""
    status = mock.AsyncMock(return_value=DagStatus({}))
    with (
        mock.patch("cstar.cli.run.show.get_launcher"),
        mock.patch("cstar.cli.run.show.load_run_state", status),
        mock.patch("cstar.cli.run.show.refresh_run_manifest") as refresh_manifest,
    ):
        yield Observed(status, refresh_manifest)


@pytest.mark.parametrize("handle", ["run-id", "uuid"])
def test_an_untracked_local_run_is_read_as_a_directory(
    local_run: t.Callable[..., BuiltRun], observed: Observed, handle: str
) -> None:
    """Verify a run only its manifest names is shown by its run-id or its uuid as
    its directory would be: records beside it show, nothing is refreshed or
    written.
    """
    built = local_run("lonely")
    write_step_record(
        built.step_dir(STEP),
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
    before = built.manifest_bytes

    result = invoke(
        built.run_id if handle == "run-id" else run_uri(built.uuid), record=None
    )

    assert result.exit_code == 0, result.output
    assert "lonely" in result.stdout
    assert "completed" in result.stdout
    observed.status.assert_not_called()
    observed.refresh.assert_not_called()
    assert built.manifest_bytes == before


def test_a_run_in_the_project_area_is_shown_by_its_uuid(
    project_run: t.Callable[..., BuiltRun], observed: Observed
) -> None:
    """Verify a run kept in the project's runs area is found by its uuid."""
    built = project_run("shared-run")
    before = built.manifest_bytes

    result = invoke(run_uri(built.uuid), ARG_FORMAT, "json", record=None)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["id"] == built.uuid
    observed.refresh.assert_not_called()
    assert built.manifest_bytes == before


def test_the_uuid_of_a_tracked_run_refreshes_it(run: FakeRun) -> None:
    """Verify a run tracked on this machine is refreshed however it is named."""
    manifest = read_run_manifest(run.root)
    assert manifest is not None
    write_record(run)

    with (
        mock.patch("cstar.cli.run.show.get_launcher"),
        mock.patch(
            "cstar.cli.run.show.load_run_state",
            mock.AsyncMock(return_value=DagStatus({STEP: Status.Done})),
        ),
    ):
        result = invoke(run_uri(manifest.id), record=run.record)

    assert result.exit_code == 0, result.output
    assert Status.Done.name in result.stdout
    refreshed = read_run_manifest(run.root)
    assert refreshed is not None
    assert refreshed.observed_at is not None


def test_an_unknown_uuid(flatten_cli_output: t.Callable[[str], str]) -> None:
    """Verify a uuid no manifest holds is a usage error that names it."""
    wanted = "6f1c3a58-1b2e-4c6d-8e0f-0a1b2c3d4e5f"

    result = invoke(run_uri(wanted), record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert f"No run has the uuid {wanted}" in flatten_cli_output(result.stderr)


@pytest.mark.parametrize("text", ["run://", "run://not-a-uuid"])
def test_a_malformed_uuid_handle(
    text: str, flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a `run://` handle without a uuid is a usage error naming the form."""
    result = invoke(text, record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "run://<uuid>" in flatten_cli_output(result.stderr)


def test_a_run_id_that_names_a_local_directory_suggests_the_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flatten_cli_output: t.Callable[[str], str],
) -> None:
    """Verify a bare name that is a directory here, and no run, is not silently
    taken for either.
    """
    (tmp_path / "somewhere").mkdir()
    monkeypatch.chdir(tmp_path)

    result = invoke("somewhere", record=None)

    assert result.exit_code == EXIT_BAD_ARGS
    assert "To show the directory ./somewhere, pass it as a path" in (
        flatten_cli_output(result.stderr)
    )


def test_a_run_id_is_slugified_before_it_is_looked_up(
    local_run: t.Callable[..., BuiltRun],
) -> None:
    """Verify a run-id is normalized as every run-id is, uuids and paths excepted."""
    local_run("lonely-run")

    result = invoke("Lonely Run", record=None)

    assert result.exit_code == 0, result.output
    assert "lonely-run" in result.stdout
