from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_ALLOW_LOGIN_NODE_RUN
from cstar.base.feature import FLAG_ON
from cstar.system.manager import (
    CStarSystemManager,
    OutsideSchedulerJob,
    outside_scheduler_job,
)


def _unrecognised_cluster(family: str) -> mock.Mock:
    """A system manager with no scheduler on a machine that shows `family`."""
    sysmgr = mock.Mock(spec=CStarSystemManager)
    sysmgr.scheduler = None
    sysmgr.selection.unsupported_scheduler.return_value = family
    sysmgr.selection.describe.return_value = "generic"
    return sysmgr


@pytest.mark.usefixtures("login_node")
def test_login_node_is_certain() -> None:
    """A system with a scheduler, outside a job, is certainly on a login node."""
    sysmgr = mock.Mock(scheduler=mock.Mock())
    sysmgr.name = "fake_hpc"
    where = outside_scheduler_job(sysmgr)

    assert where == OutsideSchedulerJob(system="fake_hpc")
    assert where.certain


@pytest.mark.usefixtures("login_node")
@pytest.mark.parametrize("job_var", ["SLURM_JOB_ID", "PBS_JOBID"])
def test_inside_a_job_is_none(monkeypatch: pytest.MonkeyPatch, job_var: str) -> None:
    """A scheduler job's environment means the process is where it belongs."""
    monkeypatch.setenv(job_var, "123")

    assert outside_scheduler_job(mock.Mock(scheduler=mock.Mock())) is None


@pytest.mark.usefixtures("login_node")
def test_allow_flag_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """`CSTAR_ALLOW_LOGIN_NODE_RUN=1` turns the answer off."""
    monkeypatch.setenv(ENV_CSTAR_ALLOW_LOGIN_NODE_RUN, FLAG_ON)

    assert outside_scheduler_job(mock.Mock(scheduler=mock.Mock())) is None


@pytest.mark.usefixtures("login_node")
def test_unrecognised_cluster_is_uncertain() -> None:
    """A scheduler the context cannot submit to makes the answer a guess."""
    where = outside_scheduler_job(_unrecognised_cluster("SLURM"))

    assert where is not None
    assert where.unsupported_scheduler == "SLURM"
    assert where.system == "generic"
    assert where.certain is False


@pytest.mark.usefixtures("login_node")
def test_machine_without_scheduler_is_none() -> None:
    """No scheduler anywhere means there is no job to be outside of."""
    assert outside_scheduler_job(_unrecognised_cluster("")) is None
