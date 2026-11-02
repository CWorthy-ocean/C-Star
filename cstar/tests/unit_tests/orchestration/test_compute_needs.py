import pytest
from pydantic import ValidationError

from cstar.orchestration.models import ComputeNeeds


def test_defaults() -> None:
    """A bare `ComputeNeeds` is one CPU task on any number of nodes."""
    needs = ComputeNeeds()

    assert (needs.tasks, needs.gpu_tasks, needs.single_node) == (1, 0, False)


def test_rejects_more_gpu_tasks_than_tasks() -> None:
    """A run cannot have more GPU tasks than tasks."""
    with pytest.raises(ValidationError, match="cannot exceed tasks"):
        ComputeNeeds(tasks=2, gpu_tasks=3)


@pytest.mark.parametrize(
    ("needs", "expected"),
    [
        pytest.param(ComputeNeeds(tasks=8), {"num_cpus": 8}, id="plain"),
        pytest.param(
            ComputeNeeds(tasks=8, single_node=True),
            {"num_cpus": 8, "single_node": True},
            id="single-node",
        ),
        pytest.param(
            ComputeNeeds(tasks=4, gpu_tasks=4),
            {"num_cpus": 4, "num_gpus": 4},
            id="gpu",
        ),
        pytest.param(
            ComputeNeeds(tasks=4, gpu_tasks=2, single_node=True),
            {"num_cpus": 4, "single_node": True, "num_gpus": 2},
            id="gpu-single-node",
        ),
    ],
)
def test_slurm_overrides(needs: ComputeNeeds, expected: dict[str, int | bool]) -> None:
    """Only the needs a run actually has appear in the slurm overrides."""
    assert needs.slurm_overrides() == expected


def test_is_frozen() -> None:
    """`ComputeNeeds` cannot change after construction."""
    needs = ComputeNeeds(tasks=2)

    with pytest.raises(ValidationError):
        needs.tasks = 3  # type: ignore[misc]
