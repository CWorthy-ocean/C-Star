import pytest

from cstar.system.manager import (
    _DerechoSystemContext,
    _ExpanseSystemContext,
    _LinuxSystemContext,
    _MacOSSystemContext,
    _PerlmutterSystemContext,
    _SystemContext,
)
from cstar.system.scheduler import PBSScheduler, Scheduler, SlurmScheduler

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@pytest.mark.parametrize(
    ("cls_", "exp_sched_type", "exp_queue_names"),
    [
        (_PerlmutterSystemContext, SlurmScheduler, {"regular", "shared", "debug"}),
        (_MacOSSystemContext, None, None),
        (_DerechoSystemContext, PBSScheduler, {"main", "preempt", "develop"}),
        (_ExpanseSystemContext, SlurmScheduler, {"compute", "debug"}),
        (_LinuxSystemContext, None, None),
    ],
)
def test_context_registry(
    cls_: type[_SystemContext],
    exp_sched_type: type[Scheduler],
    exp_queue_names: set[str] | None,
) -> None:
    """Verify that the type of scheduler created by each of the known system contexts
    matches expectations."""
    scheduler = cls_.create_scheduler()

    if exp_sched_type is not None:
        assert type(scheduler) is exp_sched_type
        queues = getattr(scheduler, "queues")  # noqa: B009
        assert {q.name for q in queues} == exp_queue_names
    else:
        assert exp_sched_type is None
