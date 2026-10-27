import errno
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
import yaml
from click.testing import Result
from typer.testing import CliRunner

from cstar.cli.workplan.status import app
from cstar.entrypoint.utils import ARG_NO_REFRESH
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.manifest import (
    RUN_MANIFEST_NAME,
    Attempt,
    StepRecord,
    ensure_run_manifest,
    read_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import KEY_PRE_RUN, BlueprintIdentity, Step, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import WorkplanRun
from cstar.system.migration import CstarSchemaTooNewError
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)

LOOKUP = "cstar.cli.workplan.shared.TrackingRepository.get_workplan_run"
"""The run-record lookup performed by the shared run-id argument."""


@pytest.mark.parametrize("pre_run", [True, False])
@pytest.mark.usefixtures("read_yaml_intercept")
def test_workplan_status_launcher_follows_pre_run(
    tmp_path: Path,
    wp_templates_dir: Path,
    mock_run_id: str,
    pre_run: bool,
) -> None:
    """Verify status loads a pre-run's sentinels with the local launcher even
    when the system has a scheduler: a SLURM query for a local pid would
    report (and persist) a bogus status over the pre-run's outcome.
    """
    wp_path = wp_templates_dir / "workplan.yaml"
    wp = deserialize(wp_path, Workplan)
    overrides = {KEY_PRE_RUN: True} if pre_run else {}
    live_steps = [
        LiveStep.from_step(step, update={"workflow_overrides": overrides})
        for step in wp.steps
    ]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / "live_transformed.yaml"
    assert serialize(trx_path, lwp)

    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )

    with (
        mock.patch(
            LOOKUP,
            mock.AsyncMock(return_value=run),
        ),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ) as mock_get_launcher,
        mock.patch(
            "cstar.cli.workplan.status.load_run_state",
            mock.AsyncMock(return_value=DagStatus({})),
        ),
        mock.patch("cstar.cli.workplan.status.display_summary"),
    ):
        result = CliRunner().invoke(app, [mock_run_id], color=False)

    assert result.exit_code == 0, result.output
    mock_get_launcher.assert_called_once()
    (passed_wp,) = mock_get_launcher.call_args.args
    assert passed_wp.compute_environment == lwp.compute_environment
    assert mock_get_launcher.call_args.kwargs == {"force_local": pre_run}


def test_workplan_status_reports_external_dependency(
    tmp_path: Path,
    mock_run_id: str,
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify status refreshes the external steps a run depends on and reports
    them as satisfied dependencies.
    """
    live_steps = [LiveStep.from_step(step) for step in external_workplan.steps]
    lwp = LiveWorkplan(
        **external_workplan.model_dump(exclude={"steps"}), steps=live_steps
    )
    trx_path = tmp_path / "consumer_transformed.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )
    external = mock.Mock()
    external.tasks.return_value = {EXTERNAL_TOKEN: external_task(Status.Done)}
    external.statuses.return_value = {EXTERNAL_TOKEN: Status.Done}
    external.errors.return_value = {}

    with (
        mock.patch(
            LOOKUP,
            mock.AsyncMock(return_value=run),
        ),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch(
            "cstar.cli.workplan.status.load_external_runs",
            mock.AsyncMock(return_value=external),
        ),
        mock.patch(
            "cstar.cli.workplan.status.load_run_state",
            mock.AsyncMock(return_value=DagStatus({"first": Status.Done})),
        ),
        mock.patch("cstar.cli.workplan.status.display_summary") as display,
    ):
        result = CliRunner().invoke(app, [mock_run_id], color=False)

    assert result.exit_code == 0, result.output
    lookup = display.call_args.args[1]
    assert lookup["first"].satisfied == [EXTERNAL_TOKEN]
    assert lookup["second"].satisfied == ["first", EXTERNAL_TOKEN]


def test_workplan_status_warns_about_unresolvable_external_dependency(
    tmp_path: Path,
    mock_run_id: str,
    external_workplan: Workplan,
) -> None:
    """Verify status still shows the run when an external step can no longer be
    resolved, warning about it instead of failing.
    """
    live_steps = [LiveStep.from_step(step) for step in external_workplan.steps]
    lwp = LiveWorkplan(
        **external_workplan.model_dump(exclude={"steps"}), steps=live_steps
    )
    trx_path = tmp_path / "consumer_transformed.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )

    # the external run has no record: its step cannot be resolved
    with (
        mock.patch(
            LOOKUP,
            mock.AsyncMock(return_value=run),
        ),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch(
            "cstar.cli.workplan.status.load_run_state",
            mock.AsyncMock(return_value=DagStatus({"first": Status.Done})),
        ),
        mock.patch("cstar.cli.workplan.status.display_summary") as display,
    ):
        result = CliRunner().invoke(app, [mock_run_id], color=False)

    assert result.exit_code == 0, result.output
    output = " ".join(result.output.split())
    assert f"Warning: external dependency {EXTERNAL_TOKEN} cannot be resolved" in output
    lookup = display.call_args.args[1]
    assert lookup["first"].awaiting == [EXTERNAL_TOKEN]
    assert lookup["second"].step.depends_on == ["first", EXTERNAL_TOKEN]


def test_workplan_status_unknown_run() -> None:
    """Verify an unrecorded run-id is a usage error, not a silent success."""
    with mock.patch(LOOKUP, mock.AsyncMock(return_value=None)):
        result = CliRunner().invoke(app, ["no-such-run"], color=False)

    assert result.exit_code == 2
    assert "unknown run-id" in result.stderr


def test_workplan_status_of_a_newer_record() -> None:
    """Verify a run whose record a newer release wrote is a usage error naming
    the record and the upgrade, not a traceback.
    """
    refused = CstarSchemaTooNewError(
        "run record /state/latest/newer.yaml", "2.0.0", "1.1.0"
    )
    with mock.patch(LOOKUP, mock.AsyncMock(side_effect=refused)):
        result = CliRunner().invoke(app, ["newer"], color=False)

    assert result.exit_code == 2
    message = " ".join(result.stderr.replace("│", " ").split())
    assert "run record /state/latest/newer.yaml schema 2.0.0 is newer" in message
    assert "Upgrade cstar-ocean" in message


@pytest.mark.parametrize("run_id", ["", "  ", "!!!"])
def test_workplan_status_invalid_run_id(run_id: str) -> None:
    """Verify a run-id that is empty once slugified is rejected, not looked up."""
    result = CliRunner().invoke(app, [run_id], color=False)

    assert result.exit_code == 2
    assert "run-id" in result.stderr


@pytest.mark.parametrize("no_refresh", [True, False])
def test_workplan_status_refreshes_the_manifest(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
    no_refresh: bool,
) -> None:
    """Verify status stamps the run's manifest with what it observed, and that
    `--no-refresh` leaves the manifest as it was recorded and writes no
    sentinels.
    """
    wp = Workplan(
        name="refreshed",
        description="A workplan whose manifest status refreshes.",
        steps=[
            Step(
                name="first",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
        ],
    )
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / "live-refreshed_transformed.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
    )
    step = live_steps[0]
    ensure_run_manifest(
        tmp_path,
        run_id=mock_run_id,
        name=wp.name,
        kind="workplan",
        workplan=None,
        attempt=Attempt(start_at=datetime(2026, 10, 15, tzinfo=UTC)),
        steps={step.name: (step.application, "tasks/first", None)},
        runs={},
    )
    args = [mock_run_id, ARG_NO_REFRESH] if no_refresh else [mock_run_id]

    with (
        mock.patch(LOOKUP, mock.AsyncMock(return_value=run)),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch(
            "cstar.cli.workplan.status.load_run_state",
            mock.AsyncMock(return_value=DagStatus({step.name: Status.Running})),
        ) as load_state,
        mock.patch("cstar.cli.workplan.status.display_summary"),
    ):
        result = CliRunner().invoke(app, args, color=False)

    assert result.exit_code == 0, result.output
    # `--no-refresh` reads the sentinels without writing them back
    assert load_state.await_args is not None
    assert load_state.await_args.kwargs == {"persist": not no_refresh}
    manifest = read_run_manifest(tmp_path)
    assert manifest is not None
    entry = manifest.step(step.name)
    assert entry is not None
    if no_refresh:
        assert manifest.observed_at is None
        assert entry.status == ""
    else:
        assert manifest.observed_at is not None
        assert entry.status == Status.Running.name


def _tracked_run(
    tmp_path: Path, hello_world_bp_path: Path, run_id: str
) -> tuple[WorkplanRun, Path]:
    """Lay out a run with one step: its transformed workplan and manifest.

    Returns
    -------
    tuple[WorkplanRun, Path]
        The run's record and the path of its transformed workplan.
    """
    wp = Workplan(
        name="refreshed",
        description="A workplan whose manifest status refreshes.",
        steps=[
            Step(
                name="first",
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
        ],
    )
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / "live-refreshed_transformed.yaml"
    assert serialize(trx_path, lwp)
    ensure_run_manifest(
        tmp_path,
        run_id=run_id,
        name=wp.name,
        kind="workplan",
        workplan=None,
        attempt=Attempt(start_at=datetime(2026, 10, 15, tzinfo=UTC)),
        steps={"first": ("hello_world", "tasks/first", None)},
        runs={},
    )
    return WorkplanRun(output_path=tmp_path, run_id=run_id), trx_path


def _status(run: WorkplanRun, observed: Status) -> tuple[Result, mock.MagicMock]:
    """Run `cstar run status` on a run whose only step is observed as `observed`.

    Returns
    -------
    tuple[Result, MagicMock]
        The command's result and the mock standing in for the summary display.
    """
    with (
        mock.patch(LOOKUP, mock.AsyncMock(return_value=run)),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch(
            "cstar.cli.workplan.status.load_run_state",
            mock.AsyncMock(return_value=DagStatus({"first": observed})),
        ),
        mock.patch("cstar.cli.workplan.status.display_summary") as display,
    ):
        result = CliRunner().invoke(app, [run.run_id], color=False)
    return result, display


def test_workplan_status_of_a_run_missing_a_file_is_one_line(
    tmp_path: Path, hello_world_bp_path: Path, mock_run_id: str
) -> None:
    """Verify a file of the run that cannot be found is reported in one line that
    names it, instead of a traceback.
    """
    run, trx_path = _tracked_run(tmp_path, hello_world_bp_path, mock_run_id)
    missing = FileNotFoundError(
        errno.ENOENT, f"Unable to read raw model from path: {trx_path}", trx_path
    )

    with (
        mock.patch(LOOKUP, mock.AsyncMock(return_value=run)),
        mock.patch("cstar.cli.workplan.status.LiveWorkplan.load", side_effect=missing),
    ):
        result = CliRunner().invoke(app, [run.run_id], color=False)

    assert result.exit_code == 0, result.output
    assert result.stderr.count(f"Unable to show the status of run {mock_run_id!r}") == 1
    assert str(trx_path) in result.stderr
    assert "Traceback" not in result.output


def test_workplan_status_leaves_an_unchanged_manifest_byte_for_byte(
    tmp_path: Path, hello_world_bp_path: Path, mock_run_id: str
) -> None:
    """Verify observing a run again, with nothing changed besides the time it was
    observed, does not rewrite the manifest.
    """
    run, _ = _tracked_run(tmp_path, hello_world_bp_path, mock_run_id)
    manifest_path = tmp_path / RUN_MANIFEST_NAME
    first, _ = _status(run, Status.Running)
    assert first.exit_code == 0, first.output
    observed = manifest_path.read_bytes()
    mtime = manifest_path.stat().st_mtime_ns

    second, _ = _status(run, Status.Running)

    assert second.exit_code == 0, second.output
    assert manifest_path.read_bytes() == observed
    assert manifest_path.stat().st_mtime_ns == mtime


def test_workplan_status_shows_a_run_a_newer_release_wrote_without_writing_it(
    tmp_path: Path, hello_world_bp_path: Path, mock_run_id: str
) -> None:
    """Verify status shows a run whose transformed workplan and manifest hold keys
    a newer minor release wrote, and leaves the manifest as it found it.
    """
    run, trx_path = _tracked_run(tmp_path, hello_world_bp_path, mock_run_id)
    manifest_path = tmp_path / RUN_MANIFEST_NAME
    for path, key in ((trx_path, "future_field"), (manifest_path, "other_field")):
        document = yaml.safe_load(path.read_text())
        major, minor, _ = document["schema_version"].split(".")
        document["schema_version"] = f"{major}.{int(minor) + 1}.0"
        document[key] = True
        path.write_text(yaml.safe_dump(document))
    before = manifest_path.read_bytes()

    result, display = _status(run, Status.Running)

    assert result.exit_code == 0, result.output
    display.assert_called_once()
    assert display.call_args.args[1]["first"].status == Status.Running
    assert manifest_path.read_bytes() == before


def test_workplan_status_of_an_attached_run_is_what_its_records_say(
    tmp_path: Path,
    hello_world_bp_path: Path,
    mock_run_id: str,
) -> None:
    """Verify an attached run has no sentinels to load and no manifest to
    refresh: the statuses are its step records', and the files are untouched.
    """
    wp = Workplan(
        name="attached",
        description="A workplan attached from elsewhere.",
        steps=[
            Step(
                name=name,
                application="hello_world",
                blueprint=hello_world_bp_path.as_posix(),
            )
            for name in ("first", "second", "third")
        ],
    )
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / "live-attached_transformed.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        output_path=tmp_path,
        run_id=mock_run_id,
        attached=True,
    )
    ensure_run_manifest(
        tmp_path,
        run_id=mock_run_id,
        name=wp.name,
        kind="workplan",
        workplan=None,
        attempt=Attempt(start_at=datetime(2026, 10, 15, tzinfo=UTC)),
        steps={
            step.name: (step.application, f"tasks/{step.name}", None)
            for step in live_steps
        },
        runs={},
    )
    for name, outcome in (("first", "completed"), ("second", "failed")):
        write_step_record(
            tmp_path / "tasks" / name,
            StepRecord(
                step=name,
                application="hello_world",
                blueprint=BlueprintIdentity(
                    kind="Blueprint",
                    application="hello_world",
                    name="bp",
                    content_hash="abc",
                ),
                outcome=outcome,
                finished_at=datetime(2026, 10, 15, tzinfo=UTC),
            ),
        )
    manifest_before = (tmp_path / "cstar-run.yaml").read_bytes()

    with (
        mock.patch(LOOKUP, mock.AsyncMock(return_value=run)),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch("cstar.cli.workplan.status.load_run_state") as load_state,
        mock.patch("cstar.cli.workplan.status.refresh_run_manifest") as refresh,
        mock.patch("cstar.cli.workplan.status.display_summary") as display,
    ):
        result = CliRunner().invoke(app, [mock_run_id], color=False)

    assert result.exit_code == 0, result.output
    load_state.assert_not_called()
    refresh.assert_not_called()
    lookup = display.call_args.args[1]
    assert lookup["first"].status == Status.Done
    assert lookup["second"].status == Status.Failed
    assert lookup["third"].status == Status.Unsubmitted
    assert (tmp_path / "cstar-run.yaml").read_bytes() == manifest_before
