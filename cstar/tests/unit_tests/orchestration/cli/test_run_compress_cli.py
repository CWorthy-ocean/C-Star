"""Tests for `cstar run compress` on a run tracked on this machine.

A run submitted with `cstar workplan submit` leaves its manifest showing a step as
submitted and its attempt open until a command reads the statuses, so compress
refreshes the manifest of a run it tracks before it asks whether the run finished.
"""

import contextlib
import typing as t
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.run import app as app_run
from cstar.cli.run.compress import COMMAND_COMPRESS
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.manifest import (
    ExecutionFacts,
    StepRecord,
    read_run_manifest,
    write_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import BlueprintIdentity
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.tests.unit_tests.orchestration.manifest_runs import BuiltRun

EXIT_BAD_ARGS: t.Final[int] = 2
STEP: t.Final[str] = "Spin Up"
JOB_ID: t.Final[str] = "123"
FINISHED: t.Final[datetime] = datetime(2026, 10, 15, 13, 0, tzinfo=UTC)


def invoke(*args: str) -> Result:
    """Run a `cstar run` command against the tracking of the test's data home."""
    return CliRunner().invoke(app_run, list(args), color=False)


def compress(target: str) -> Result:
    """Plan a compression of `target` without writing any compressed file."""
    return invoke(COMMAND_COMPRESS, target, "--yes", "--now", "--dry-run")


@pytest.fixture
def submitted(
    tmp_path: Path, build_run: t.Callable[..., BuiltRun]
) -> t.Callable[..., BuiltRun]:
    """Build a run tracked live here whose manifest shows its step submitted.

    The step has finished and left its record, but nothing has folded that into
    the manifest, whose latest attempt is open.
    """

    def build(*, read_only: bool = False) -> BuiltRun:
        built = build_run(tmp_path / "run", "r1", steps=(STEP,))
        write_step_record(
            built.step_dir(STEP),
            StepRecord(
                step=STEP,
                application="hello_world",
                blueprint=BlueprintIdentity(
                    kind="Blueprint", application="hello_world", name="bp"
                ),
                outcome="completed",
                finished_at=FINISHED,
                execution=ExecutionFacts(scheduler="slurm", job_id=JOB_ID),
            ),
        )
        manifest = read_run_manifest(built.root)
        assert manifest is not None
        (entry,) = manifest.steps
        entry.status = Status.Submitted.name
        entry.job_id = JOB_ID
        write_run_manifest(built.root, manifest)

        attach = ["attach", str(built.root)]
        assert invoke(*attach, *(["--read-only"] if read_only else [])).exit_code == 0
        if not read_only:
            TrackingRepository().update_workplan_run_sync(
                built.run_id, lambda run: setattr(run, "attached", False)
            )
            record = TrackingRepository().get_workplan_run_sync(built.run_id)
            assert isinstance(record, WorkplanRun)
            assert record.live
        return built

    return build


@contextlib.contextmanager
def launcher_says(status: Status) -> t.Iterator[None]:
    """Make the launcher report the step as `status`, leaving the refresh real.

    The launcher would refuse a launch, as a system does while its SLURM account
    is unset, which reading a status must not ask about.
    """
    unset = CstarExpectationFailed(
        "Unable to load environment variables required by the system: "
        "CSTAR_SLURM_ACCOUNT"
    )
    with (
        mock.patch.object(LocalLauncher, "check_preconditions", side_effect=unset),
        mock.patch(
            "cstar.cli.run.shared.load_run_state",
            mock.AsyncMock(return_value=DagStatus({STEP: status})),
        ),
    ):
        yield


def finished_attempt(root: Path) -> tuple[str, datetime | None]:
    """The status of the step in the manifest on disk, and when its attempt ended."""
    manifest = read_run_manifest(root)
    assert manifest is not None
    (entry,) = manifest.steps
    return entry.status, manifest.attempts[-1].finished_at


@pytest.mark.parametrize("handle", ["run-id", "directory"])
def test_a_finished_run_is_refreshed_before_compress_asks_if_it_finished(
    submitted: t.Callable[..., BuiltRun], handle: str
) -> None:
    """Verify a run the launcher reports done is accepted, however it is named,
    and its manifest records the end.
    """
    built = submitted()
    assert finished_attempt(built.root) == (Status.Submitted.name, None)

    with launcher_says(Status.Done):
        result = compress(built.run_id if handle == "run-id" else str(built.root))

    assert result.exit_code == 0, result.output + result.stderr
    assert "has not finished" not in result.output + result.stderr
    status, finished_at = finished_attempt(built.root)
    assert status == Status.Done.name
    assert finished_at is not None


def test_a_run_still_running_is_refused_naming_the_step(
    submitted: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a step the launcher reports running keeps the attempt open, and the
    refusal names the step and its status.
    """
    built = submitted()

    with launcher_says(Status.Running):
        result = compress(built.run_id)

    assert result.exit_code != 0
    assert f"has not finished: step '{STEP}' is Running" in flatten_cli_output(
        result.output + result.stderr
    )
    assert not (built.root / "compress").exists()


def test_a_run_attached_read_only_is_refused_by_its_directory(
    submitted: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify naming a read-only run by its directory does not get around the
    guard against writing into it.
    """
    built = submitted(read_only=True)

    result = compress(str(built.root))

    assert result.exit_code == EXIT_BAD_ARGS, result.output
    assert "read-only" in flatten_cli_output(result.output + result.stderr)
    assert not (built.root / "compress").exists()


def test_a_scheduler_that_cannot_be_queried_leaves_the_run_as_recorded(
    submitted: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a refresh that fails, such as `sacct` erroring, is a warning, and
    compress goes on with the manifest as last recorded instead of a traceback.
    """
    built = submitted()

    with mock.patch(
        "cstar.cli.run.shared.load_run_state",
        mock.AsyncMock(side_effect=RuntimeError("sacct: error: no route to host")),
    ):
        result = compress(built.run_id)

    output = flatten_cli_output(result.output + result.stderr)
    assert result.exit_code == EXIT_BAD_ARGS, output
    assert isinstance(result.exception, SystemExit)
    assert "the manifest was not refreshed: sacct: error: no route to host" in output
    assert f"has not finished: step '{STEP}' is Submitted" in output


def test_a_run_whose_tracking_record_cannot_be_read_is_refused_by_its_directory(
    submitted: t.Callable[..., BuiltRun], flatten_cli_output: t.Callable[[str], str]
) -> None:
    """Verify a run named by its directory whose record cannot be read is refused
    in one line: whether the run may be written is not known.
    """
    built = submitted()

    with mock.patch.object(
        TrackingRepository,
        "get_workplan_run_sync",
        side_effect=ValueError("not a valid run record"),
    ):
        result = compress(str(built.root))

    output = flatten_cli_output(result.output + result.stderr)
    assert result.exit_code == EXIT_BAD_ARGS, output
    assert f"The tracking record of run '{built.run_id}' cannot be read" in output
    assert not (built.root / "compress").exists()
