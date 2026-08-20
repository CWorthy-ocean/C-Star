import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.orchestration import LiveStep, Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.state import StateRepository
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from cstar.system.scheduler import SlurmPartition, SlurmScheduler


def fake_get_queue(name: str = "fake-queue") -> t.Any:
    return SlurmPartition(name, "cannot-query", lambda x: "48:00:00")


@pytest.mark.usefixtures("read_yaml_intercept")
def test_proxiedrunrequestformatter_wp_intercept(wp_templates_dir: Path) -> None:
    """Verify that a formatter is initialized correctly when only a step is supplied."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    assert live_step


@pytest.mark.usefixtures("read_yaml_intercept")
def test_slurmlauncher_adapt_step_no_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
) -> None:
    """Verify that the `SlurmLauncher` correctly converts a step into a `SchedulerJob`."""
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
    )
    minimum_spec = SlurmLauncher._get_default_compute_spec(live_step)  # type: ignore

    mock_mgr = mock.Mock()
    mock_mgr.environment.package_root = tmp_path
    mock_mgr.scheduler = SlurmScheduler(
        queues=[fake_get_queue("default-q"), fake_get_queue("alt-q")],
        primary_queue_name="default-q",
        other_scheduler_directives={},
        requires_task_distribution=False,
        documentation="fake slurm scheduduler",
        max_cpus_per_node=128,
    )
    mock_getsysmgr = mock.Mock(return_value=mock_mgr)

    with (
        mock.patch("cstar.execution.scheduler_job.get_sysmgr", mock_getsysmgr),
        mock.patch.object(mock_mgr.scheduler, "get_queue", fake_get_queue),
    ):
        job = SlurmLauncher.adapt_step(live_step, [])

    # confirm that no job attributes have been overridden from the minimal spec
    assert minimum_spec.account_name == job.account_key
    assert minimum_spec.num_cpus == job.cpus
    assert minimum_spec.queue_name == job.queue_name
    assert minimum_spec.max_walltime == job.walltime

    # confirm default behaviors of `create_scheduler_job` for unspecified spec attributes
    assert minimum_spec.num_nodes == job.nodes
    assert minimum_spec.cpus_per_node == job.cpus_per_node


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.parametrize(
    (
        "overrides",
        "exp_account",
        "exp_queue",
        "exp_walltime",
        "exp_ncpus",
        "exp_nnodes",
        "exp_cpus_per_node",
    ),
    [
        pytest.param(
            {"slurm": {"queue_name": "alt-q"}},
            "default-account",
            "alt-q",
            "42:00:00",
            128,
            None,
            None,
            id="queue-name",
        ),
        pytest.param(
            {"slurm": {"num_cpus": "42"}},
            "default-account",
            "default-q",
            "42:00:00",
            42,
            None,
            None,
            id="num-cpus",
        ),
        pytest.param(
            {"slurm": {"num_nodes": "99"}},
            "default-account",
            "default-q",
            "42:00:00",
            128,
            99,
            None,
            id="num-nodes",
        ),
        pytest.param(
            {"slurm": {"max_walltime": "01:00:00"}},
            "default-account",
            "default-q",
            "01:00:00",
            128,
            None,
            None,
            id="walltime",
        ),
        pytest.param(
            {"slurm": {"cpus_per_node": "32"}},
            "default-account",
            "default-q",
            "42:00:00",
            128,
            None,
            32,
            id="cpus-per-node",
        ),
        pytest.param(
            {"slurm": {"account_name": "alt-account"}},
            "alt-account",
            "default-q",
            "42:00:00",
            128,
            None,
            None,
            id="account-name",
        ),
        pytest.param(
            {
                "slurm": {
                    "account_name": "alt-account",
                    "queue_name": "alt-q",
                    "num_cpus": 10,
                    "num_nodes": 20,
                    "max_walltime": "02:00:00",
                    "cpus_per_node": 30,
                }
            },
            "alt-account",
            "alt-q",
            "02:00:00",
            10,
            20,
            30,
            id="all",
        ),
    ],
)
def test_slurmlauncher_adapt_step_with_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
    overrides: dict[str, dict[str, str]],
    exp_account: str,
    exp_queue: str,
    exp_walltime: str,
    exp_ncpus: int,
    exp_nnodes: int | None,
    exp_cpus_per_node: int | None,
) -> None:
    """Verify that the `SlurmLauncher` correctly converts a step into a `SchedulerJob`
    that includes any provided per-step compute overrides.

    NOTE: num_cpus is populated from the template bp, resulting in 128: `return xi * eta`
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(
        workplan.steps[0],
        update={"compute_overrides": overrides},
    )

    mock_mgr = mock.Mock()
    mock_mgr.environment.package_root = tmp_path
    mock_mgr.scheduler = SlurmScheduler(
        queues=[fake_get_queue("default-q"), fake_get_queue("alt-q")],
        primary_queue_name="default-q",
        other_scheduler_directives={},
        requires_task_distribution=False,
        documentation="fake slurm scheduduler",
        max_cpus_per_node=128,
    )
    mock_getsysmgr = mock.Mock(return_value=mock_mgr)

    with (
        mock.patch.dict(
            os.environ,
            {
                ENV_CSTAR_SLURM_ACCOUNT: "default-account",
                ENV_CSTAR_SLURM_MAX_WALLTIME: "42:00:00",
                ENV_CSTAR_SLURM_QUEUE: "default-q",
            },
        ),
        mock.patch("cstar.execution.scheduler_job.get_sysmgr", mock_getsysmgr),
        mock.patch.object(mock_mgr.scheduler, "get_queue", fake_get_queue),
    ):
        minimum_spec = SlurmLauncher._get_default_compute_spec(live_step)  # type: ignore
        job = SlurmLauncher.adapt_step(live_step, [])

    # confirm defaults as baseline
    assert minimum_spec.account_name == "default-account"
    assert minimum_spec.cpus_per_node is None
    assert minimum_spec.max_walltime == "42:00:00"
    assert minimum_spec.num_cpus == 128
    assert minimum_spec.num_nodes is None
    assert minimum_spec.queue_name == "default-q"

    # confirm the job varies from defaults as expected
    assert job.account_key == exp_account
    assert job.cpus == exp_ncpus
    assert job.walltime == exp_walltime
    assert job.cpus_per_node == exp_cpus_per_node
    assert job.nodes == exp_nnodes
    assert job.queue.name == exp_queue

    # confirm slurm-specific env vars are captured in the command
    if exp_queue != minimum_spec.queue_name:
        assert f"{ENV_CSTAR_SLURM_QUEUE}={exp_queue!r}" in job.commands

    if exp_account != minimum_spec.account_name:
        assert f"{ENV_CSTAR_SLURM_ACCOUNT}={exp_account!r}" in job.commands

    if exp_walltime != minimum_spec.max_walltime:
        assert f"{ENV_CSTAR_SLURM_MAX_WALLTIME}={exp_walltime!r}" in job.commands


async def test_prune_completed_dependencies_drops_completed_jobs() -> None:
    """Verify dependencies on already-completed SLURM jobs are removed."""
    completed_dep = SlurmHandle(pid="111", name="step_1", run_id="test-run")
    running_dep = SlurmHandle(pid="222", name="step_0", run_id="test-run")

    batch_map = {
        "111": mock.Mock(status=ExecutionStatus.COMPLETED),
        "222": mock.Mock(status=ExecutionStatus.RUNNING),
    }
    get_batches_mock = mock.AsyncMock(return_value=batch_map)

    with mock.patch(
        "cstar.orchestration.launch.slurm.get_slurm_batches", get_batches_mock
    ):
        pruned = await SlurmLauncher._prune_completed_dependencies(
            [completed_dep, running_dep]
        )

    get_batches_mock.assert_awaited_once_with(["111", "222"])
    assert [d.pid for d in pruned] == ["222"]


async def test_prune_completed_dependencies_keeps_unfinished_jobs() -> None:
    """Verify dependencies are unchanged when no dependency job has completed."""
    pending_dep = SlurmHandle(pid="111", name="step_1", run_id="test-run")

    batch_map = {"111": mock.Mock(status=ExecutionStatus.PENDING)}

    with mock.patch(
        "cstar.orchestration.launch.slurm.get_slurm_batches",
        mock.AsyncMock(return_value=batch_map),
    ):
        pruned = await SlurmLauncher._prune_completed_dependencies([pending_dep])

    assert pruned == [pending_dep]


async def test_prune_completed_dependencies_no_deps_skips_query() -> None:
    """Verify an empty dependency list does not trigger a SLURM query."""
    get_batches_mock = mock.AsyncMock()

    with mock.patch(
        "cstar.orchestration.launch.slurm.get_slurm_batches", get_batches_mock
    ):
        pruned = await SlurmLauncher._prune_completed_dependencies([])

    get_batches_mock.assert_not_awaited()
    assert pruned == []


@pytest.mark.usefixtures("read_yaml_intercept")
async def test_launch_prunes_stale_deps_without_prior_sentinel(
    wp_templates_dir: Path,
) -> None:
    """Verify a never-before-submitted step does not submit with a SLURM
    dependency on a job that already completed (e.g. a step satisfied by a
    prior run and skipped on re-run).

    Regression test: previously the pruning only ran when the step itself had
    a failed prior sentinel, so re-running a workplan where step_1 was already
    Done caused step_2 to be submitted with `--dependency=afterok:<old job>`,
    which SLURM rejects or kills once the old job is gone.
    """
    wp_path = wp_templates_dir / "single_step.yaml"
    workplan = deserialize(wp_path, Workplan)
    live_step = LiveStep.from_step(workplan.steps[0])

    completed_dep = SlurmHandle(pid="111", name="step_1", run_id="test-run")
    running_dep = SlurmHandle(pid="222", name="step_0", run_id="test-run")

    new_handle = SlurmHandle(pid="333", name=live_step.name, run_id="test-run")
    submit_mock = mock.AsyncMock(return_value=new_handle)

    batch_map = {
        "111": mock.Mock(status=ExecutionStatus.COMPLETED),
        "222": mock.Mock(status=ExecutionStatus.RUNNING),
    }

    with (
        mock.patch.object(
            StateRepository, "get_sentinel", mock.AsyncMock(return_value=None)
        ),
        mock.patch.object(SlurmLauncher, "_submit", submit_mock),
        mock.patch.object(
            SlurmLauncher,
            "update_status",
            mock.AsyncMock(return_value=(False, new_handle)),
        ),
        mock.patch(
            "cstar.orchestration.launch.slurm.get_slurm_batches",
            mock.AsyncMock(return_value=batch_map),
        ),
    ):
        task = await SlurmLauncher.launch(live_step, [completed_dep, running_dep])

    submit_mock.assert_awaited_once()
    assert submit_mock.await_args is not None
    _, submitted_deps = submit_mock.await_args.args
    assert [d.pid for d in submitted_deps] == ["222"]
    assert task.handle.pid == "333"
