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


@pytest.fixture
def deferred_live_step(tmp_path: Path) -> LiveStep:
    """Create a LiveStep whose blueprint is deferred to an upstream step.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs

    Returns
    -------
    LiveStep
    """
    return LiveStep.model_validate(
        {
            "name": "consumer",
            "application": "hello_world",
            "blueprint": {"from_step": "producer"},
            "depends_on": ["producer"],
            "working_dir": tmp_path / "consumer",
        },
    )


def test_default_compute_spec_deferred_defaults_to_one_cpu(
    deferred_live_step: LiveStep,
) -> None:
    """Verify a deferred step with no declaration defaults to a single cpu.

    Parameters
    ----------
    deferred_live_step : LiveStep
        A step whose blueprint is deferred to an upstream step.
    """
    assert deferred_live_step.blueprint is None

    spec = SlurmLauncher._get_default_compute_spec(deferred_live_step)  # type: ignore

    assert spec.num_cpus == 1


def test_compute_spec_deferred_with_overrides(deferred_live_step: LiveStep) -> None:
    """Verify compute_overrides predict the cpu needs of a deferred step.

    Parameters
    ----------
    deferred_live_step : LiveStep
        A step whose blueprint is deferred to an upstream step.
    """
    step = LiveStep.from_step(
        deferred_live_step,
        update={"compute_overrides": {"slurm": {"num_cpus": 8}}},
    )

    spec = SlurmLauncher._get_compute_spec(step)  # type: ignore

    assert spec.num_cpus == 8
