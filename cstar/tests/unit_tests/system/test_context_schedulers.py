import pytest

from cstar.system.manager import (
    AnvilSystemContext,
    DerechoSystemContext,
    ExpanseSystemContext,
    LinuxSystemContext,
    MacOSSystemContext,
    PerlmutterSystemContext,
    SystemContext,
)
from cstar.system.scheduler import PBSScheduler, Scheduler, SlurmScheduler

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@pytest.mark.parametrize(
    ("wrapped_class", "exp_sched_type", "exp_queue_names"),
    [
        (AnvilSystemContext, SlurmScheduler, {"wholenode", "shared", "debug"}),
        (PerlmutterSystemContext, SlurmScheduler, {"regular", "shared", "debug"}),
        (MacOSSystemContext, None, None),
        (DerechoSystemContext, PBSScheduler, {"main", "preempt", "develop"}),
        (ExpanseSystemContext, SlurmScheduler, {"compute", "debug"}),
        (LinuxSystemContext, None, None),
    ],
)
def test_context_registry(
    wrapped_class: type[SystemContext],
    exp_sched_type: type[Scheduler],
    exp_queue_names: set[str] | None,
) -> None:
    """Verify that the type of scheduler created by each of the known system contexts
    matches expectations.
    """
    scheduler = wrapped_class.create_scheduler()

    if exp_sched_type is not None:
        assert type(scheduler) is exp_sched_type
        queues = getattr(scheduler, "queues")  # noqa: B009
        assert {q.name for q in queues} == exp_queue_names
        if exp_sched_type == PerlmutterSystemContext:
            assert scheduler.global_max_cpus_per_node == 128
    else:
        assert scheduler is None
