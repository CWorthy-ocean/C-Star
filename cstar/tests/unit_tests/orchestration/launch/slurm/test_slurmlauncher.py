import os
import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.orchestration import LiveStep, Workplan
from cstar.orchestration.serialization import deserialize
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
        queues=[fake_get_queue("a"), fake_get_queue("b")],
        primary_queue_name="a",
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
    ("overrides",),
    [
        pytest.param({"slurm": {"queue_name": "b"}}, id="queue-name"),
        pytest.param({"slurm": {"num_cpus": "42"}}, id="num-cpus"),
        pytest.param({"slurm": {"num_nodes": "99"}}, id="num-nodes"),
        pytest.param({"slurm": {"max_walltime": "01:00:00"}}, id="walltime"),
        pytest.param({"slurm": {"cpus_per_node": "32"}}, id="cpus-per-node"),
        pytest.param({"slurm": {"account_name": "alt-acct"}}, id="account-name"),
    ],
)
def test_slurmlauncher_adapt_step_with_overrides(
    tmp_path: Path,
    wp_templates_dir: Path,
    overrides: dict[str, dict[str, str]],
) -> None:
    """Verify that the `SlurmLauncher` correctly converts a step into a `SchedulerJob`
    that includes any provided per-step compute overrides.
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
        queues=[fake_get_queue("a"), fake_get_queue("b")],
        primary_queue_name="a",
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
                ENV_CSTAR_SLURM_ACCOUNT: "og-account",
                ENV_CSTAR_SLURM_MAX_WALLTIME: "00:42:00",
                ENV_CSTAR_SLURM_QUEUE: "a",
            },
        ),
        mock.patch("cstar.execution.scheduler_job.get_sysmgr", mock_getsysmgr),
        mock.patch.object(mock_mgr.scheduler, "get_queue", fake_get_queue),
    ):
        minimum_spec = SlurmLauncher._get_default_compute_spec(live_step)  # type: ignore
        job = SlurmLauncher.adapt_step(live_step, [])

    # confirm that no job attributes have been overridden from the minimal spec
    if value := overrides["slurm"].get("account_name", ""):
        assert job.account_key == value
    else:
        assert job.account_key == minimum_spec.account_name

    if value := overrides["slurm"].get("num_cpus", ""):
        assert job.cpus == int(value)
    else:
        assert job.cpus == minimum_spec.num_cpus

    if value := overrides["slurm"].get("queue_name", ""):
        assert job.queue_name == value
    else:
        assert job.queue_name == minimum_spec.queue_name

    if value := overrides["slurm"].get("max_walltime", ""):
        assert job.walltime == value
    else:
        assert job.walltime == minimum_spec.max_walltime
