from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from cstar.cli.workplan.status import app
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import KEY_PRE_RUN, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.tracking import WorkplanRun


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
