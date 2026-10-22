from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.status import app
from cstar.entrypoint.utils import ARG_NO_REFRESH
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.manifest import (
    Attempt,
    StepRecord,
    ensure_run_manifest,
    read_run_manifest,
    write_step_record,
)
from cstar.orchestration.models import KEY_PRE_RUN, BlueprintIdentity, Step, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import KEY_ATTACHED, WorkplanRun
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
    trx_path = tmp_path / f"live-{wp_path.name}"
    assert serialize(trx_path, lwp)

    run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=trx_path,
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
    trx_path = tmp_path / "consumer-trx.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        workplan_path=trx_path,
        trx_workplan_path=trx_path,
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
    trx_path = tmp_path / "consumer-trx.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        workplan_path=trx_path,
        trx_workplan_path=trx_path,
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
    `--no-refresh` leaves the manifest as it was recorded.
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
    wp_path = tmp_path / "refreshed.yaml"
    live_steps = [LiveStep.from_step(step) for step in wp.steps]
    lwp = LiveWorkplan(**wp.model_dump(exclude={"steps"}), steps=live_steps)
    trx_path = tmp_path / "live-refreshed.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=trx_path,
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
        ),
        mock.patch("cstar.cli.workplan.status.display_summary"),
    ):
        result = CliRunner().invoke(app, args, color=False)

    assert result.exit_code == 0, result.output
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
    trx_path = tmp_path / "live-attached.yaml"
    assert serialize(trx_path, lwp)
    run = WorkplanRun(
        workplan_path=tmp_path / "attached.yaml",
        trx_workplan_path=trx_path,
        output_path=tmp_path,
        run_id=mock_run_id,
        metadata={KEY_ATTACHED: "1"},
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
