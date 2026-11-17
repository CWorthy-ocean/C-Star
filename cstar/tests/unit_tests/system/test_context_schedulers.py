import pytest

from cstar.system.manager import registered_context
from cstar.system.scheduler import PBSScheduler, Scheduler, SlurmScheduler

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


@pytest.mark.parametrize(
    ("system_name", "exp_sched_type", "exp_queue_names"),
    [
        (
            "anvil",
            SlurmScheduler,
            {"wholenode", "shared", "debug", "gpu", "gpu-debug"},
        ),
        ("perlmutter", SlurmScheduler, {"regular", "shared", "debug"}),
        ("darwin_arm64", None, None),
        ("derecho", PBSScheduler, {"main", "preempt", "develop"}),
        ("expanse", SlurmScheduler, {"compute", "debug"}),
        ("linux_x86_64", None, None),
    ],
)
def test_context_registry(
    system_name: str,
    exp_sched_type: type[Scheduler] | None,
    exp_queue_names: set[str] | None,
) -> None:
    """Verify that the type of scheduler created by each of the known system contexts
    matches expectations.
    """
    scheduler = registered_context(system_name).create_scheduler()

    if exp_sched_type is not None:
        assert scheduler is not None
        assert type(scheduler) is exp_sched_type
        queues = getattr(scheduler, "queues")
        assert {q.name for q in queues} == exp_queue_names
        if system_name == "perlmutter":
            assert scheduler.global_max_cpus_per_node == 128
    else:
        assert scheduler is None
