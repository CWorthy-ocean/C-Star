from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.status import app
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import KEY_PRE_RUN, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan, Status
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import WorkplanRun
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)


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
            "cstar.cli.workplan.status.TrackingRepository.get_workplan_run",
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
    mock_get_launcher.assert_called_once_with(force_local=pre_run)


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
            "cstar.cli.workplan.status.TrackingRepository.get_workplan_run",
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


def test_workplan_status_reports_unresolvable_external_dependency(
    tmp_path: Path,
    mock_run_id: str,
    external_workplan: Workplan,
) -> None:
    """Verify status names an external step that can no longer be resolved and
    exits with an error instead of raising from the planner.
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
            "cstar.cli.workplan.status.TrackingRepository.get_workplan_run",
            mock.AsyncMock(return_value=run),
        ),
        mock.patch(
            "cstar.cli.workplan.status.get_launcher", return_value=LocalLauncher()
        ),
        mock.patch("cstar.cli.workplan.status.display_summary") as display,
    ):
        result = CliRunner().invoke(app, [mock_run_id], color=False)

    assert result.exit_code == 1, result.output
    assert f"External dependency {EXTERNAL_TOKEN} cannot be resolved" in " ".join(
        result.output.split()
    )
    assert result.exception is None or isinstance(result.exception, SystemExit)
    display.assert_not_called()
