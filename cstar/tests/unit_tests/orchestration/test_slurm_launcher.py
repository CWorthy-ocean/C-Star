"""Tests for the sentinel-based resubmission guard in `SlurmLauncher.launch`.

Re-running a workplan with the same run-id must not resubmit SLURM jobs that
already succeeded or are still in flight; failed steps must be resubmitted
after clearing their prior outputs. (This behavior was previously provided by
a Prefect result cache and had no direct coverage.)
"""

from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.models import Application
from cstar.orchestration.orchestration import LiveStep, Status

STEP_NAME = "test step"


@pytest.fixture
def step(tmp_path: Path) -> LiveStep:
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    return LiveStep(
        name=STEP_NAME,
        application=Application.HELLO_WORLD,
        blueprint=bp_path,
        working_dir=tmp_path / "work",
    )


def make_handle(pid: str = "1234") -> SlurmHandle:
    return SlurmHandle(pid=pid, name=STEP_NAME, run_id="20260806_000000")


async def test_launch_submits_when_no_prior_sentinel(step: LiveStep) -> None:
    """Without a sentinel for the step, a fresh job is submitted."""
    fresh = make_handle("5678")

    with (
        mock.patch(
            "cstar.orchestration.launch.slurm.StateRepository.get_sentinel",
            mock.AsyncMock(return_value=None),
        ),
        mock.patch.object(
            SlurmLauncher, "_submit", mock.AsyncMock(return_value=fresh)
        ) as submit,
        mock.patch.object(SlurmLauncher, "update_status", mock.AsyncMock()),
    ):
        task = await SlurmLauncher.launch(step, dependencies=[])

    submit.assert_awaited_once()
    assert task.handle is fresh


async def test_launch_reuses_prior_job_when_not_failed(step: LiveStep) -> None:
    """A prior submission that did not fail is re-used, not resubmitted."""
    prior = make_handle()

    with (
        mock.patch(
            "cstar.orchestration.launch.slurm.StateRepository.get_sentinel",
            mock.AsyncMock(return_value=prior),
        ),
        mock.patch.object(
            SlurmLauncher, "query_status", mock.AsyncMock(return_value=Status.Done)
        ),
        mock.patch.object(SlurmLauncher, "_submit", mock.AsyncMock()) as submit,
        mock.patch.object(SlurmLauncher, "update_status", mock.AsyncMock()),
    ):
        task = await SlurmLauncher.launch(step, dependencies=[])

    submit.assert_not_awaited()
    assert task.handle is prior
    assert task.step is step


@pytest.mark.parametrize("status", [Status.Running, Status.Submitted])
async def test_launch_reuses_in_flight_job(step: LiveStep, status: Status) -> None:
    """Jobs that are still pending or running are also re-used."""
    prior = make_handle()

    with (
        mock.patch(
            "cstar.orchestration.launch.slurm.StateRepository.get_sentinel",
            mock.AsyncMock(return_value=prior),
        ),
        mock.patch.object(
            SlurmLauncher, "query_status", mock.AsyncMock(return_value=status)
        ),
        mock.patch.object(SlurmLauncher, "_submit", mock.AsyncMock()) as submit,
        mock.patch.object(SlurmLauncher, "update_status", mock.AsyncMock()),
    ):
        task = await SlurmLauncher.launch(step, dependencies=[])

    submit.assert_not_awaited()
    assert task.handle is prior


async def test_launch_resubmits_after_failure(step: LiveStep) -> None:
    """A failed prior submission is cleared and resubmitted."""
    prior = make_handle()
    fresh = make_handle("5678")

    with (
        mock.patch(
            "cstar.orchestration.launch.slurm.StateRepository.get_sentinel",
            mock.AsyncMock(return_value=prior),
        ),
        mock.patch.object(
            SlurmLauncher, "query_status", mock.AsyncMock(return_value=Status.Failed)
        ),
        mock.patch.object(
            SlurmLauncher, "_locate_priors", mock.AsyncMock(return_value={})
        ),
        mock.patch(
            "cstar.orchestration.launch.slurm.get_slurm_batches",
            mock.AsyncMock(return_value={}),
        ),
        mock.patch.object(
            SlurmLauncher, "_submit", mock.AsyncMock(return_value=fresh)
        ) as submit,
        mock.patch.object(SlurmLauncher, "update_status", mock.AsyncMock()),
        mock.patch(
            "cstar.execution.file_system.JobFileSystemManager.clear_prior"
        ) as clear_prior,
    ):
        task = await SlurmLauncher.launch(step, dependencies=[])

    submit.assert_awaited_once()
    clear_prior.assert_called_once()
    assert task.handle is fresh
