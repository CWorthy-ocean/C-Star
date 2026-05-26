import os
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_DATA_HOME, ENV_CSTAR_STATE_HOME
from cstar.execution.file_system import DirectoryManager, JobFileSystemManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun


@pytest.fixture
def mock_state_dir(
    tmp_path: Path,
) -> Generator[Path, None, None]:
    """Verify that CLI plan action generates an output image from a workplan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs; used to create a temporary location
        for writing state directory content.

    """
    mock_state_dir = tmp_path / "mock-state-dir"
    mock_state_dir.mkdir(exist_ok=True)

    with mock.patch.dict(os.environ, {ENV_CSTAR_STATE_HOME: mock_state_dir.as_posix()}):
        yield mock_state_dir


@pytest.fixture
def mock_data_dir(
    tmp_path: Path,
) -> Generator[Path, None, None]:
    """Verify that CLI plan action generates an output image from a workplan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs; used to create a temporary location
        for writing data directory content.

    """
    mock_data_dir = tmp_path / "mock-data-dir"
    mock_data_dir.mkdir(exist_ok=True)

    with mock.patch.dict(os.environ, {ENV_CSTAR_DATA_HOME: mock_data_dir.as_posix()}):
        yield mock_data_dir


@pytest.fixture(params=["fanout", "linear", "parallel", "single_step"])
def prepared_workplan(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    wp_templates_dir: Path,
    default_blueprint_path: str,
    bp_templates_dir: Path,
) -> tuple[Path, Workplan]:
    """Verify that CLI plan action generates an output image from a workplan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan fixture to use for workplan creation
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    # mock_state_dir: Path
    #     Path to a temporary state directory for the test.
    """
    workplan_name = request.param
    template_file = f"{workplan_name}.yaml"  # "single_step.yaml"
    template_path = wp_templates_dir / template_file

    bp_template_path = bp_templates_dir / "blueprint.yaml"
    bp_content = bp_template_path.read_text()

    test_bp_path = tmp_path / "blueprint.yaml"
    test_bp_path.write_text(bp_content)

    content = template_path.read_text()
    content = content.replace(default_blueprint_path, test_bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(content)

    wp = deserialize(wp_path, Workplan)

    return wp_path, wp


@pytest.fixture
async def executed_workplan(
    tmp_path: Path,
    prepared_workplan: tuple[Path, Workplan],
    mock_state_dir: Path,  # noqa: ARG001
) -> tuple[Path, Workplan, str]:
    """Create a WorkplanRun record for the prepared workplan."""
    wp_path, wp = prepared_workplan
    fake_run_id = "fake-run-id"

    repo = TrackingRepository()
    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=wp_path,
        output_path=tmp_path / "mock-output",
        run_id=fake_run_id,
    )
    await repo.put_workplan_run(wp_run)

    mock_get_wp = mock.Mock(return_value=wp_run)

    with (
        mock.patch(
            "cstar.orchestration.tracking.TrackingRepository.get_workplan_run",
            mock_get_wp,
        ),
    ):
        return wp_path, wp, fake_run_id


@pytest.fixture
async def executed_workplan_with_sideeffects(
    executed_workplan: tuple[Path, Workplan, str],
    mock_data_dir: Path,  # noqa: ARG001
) -> tuple[Path, Workplan, str]:
    """Create a WorkplanRun record for the prepared workplan and populate
    the run directories with logs.
    """
    wp_path, wp, fake_run_id = executed_workplan
    root_fsm = JobFileSystemManager(DirectoryManager.data_home() / fake_run_id)

    for i, step in enumerate(wp.steps):
        step_fsm = root_fsm.get_subtask_manager(step.safe_name)
        step_fsm.prepare()
        log_path = step_fsm.logs_dir / f"{step.safe_name}.out"
        log_path.write_text(f"{step.name} message {i}")

    return wp_path, wp, fake_run_id
