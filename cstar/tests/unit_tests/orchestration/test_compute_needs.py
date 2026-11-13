import pytest
from pydantic import ValidationError

from cstar.orchestration.models import ComputeNeeds


def test_defaults() -> None:
    """A bare `ComputeNeeds` is one CPU task on any number of nodes."""
    needs = ComputeNeeds()

    assert (needs.tasks, needs.gpu_tasks, needs.single_node) == (1, 0, False)
    assert needs.nodes is None


def test_rejects_more_gpu_tasks_than_tasks() -> None:
    """A run cannot have more GPU tasks than tasks."""
    with pytest.raises(ValidationError, match="cannot exceed tasks"):
        ComputeNeeds(tasks=2, gpu_tasks=3)


@pytest.mark.parametrize("nodes", [0, -1])
def test_rejects_non_positive_nodes(nodes: int) -> None:
    """A run spans at least one node."""
    with pytest.raises(ValidationError, match="nodes"):
        ComputeNeeds(tasks=4, nodes=nodes)


@pytest.mark.parametrize("nodes", [None, 1])
def test_single_node_accepts_one_node(nodes: int | None) -> None:
    """A single-node run may leave the node count unset or name the one node."""
    assert ComputeNeeds(tasks=4, single_node=True, nodes=nodes).nodes == nodes


def test_single_node_rejects_more_nodes() -> None:
    """A run confined to one node cannot also span two."""
    with pytest.raises(ValidationError, match=r"single_node.*nodes=2"):
        ComputeNeeds(tasks=4, single_node=True, nodes=2)


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
        pytest.param(
            ComputeNeeds(tasks=10, gpu_tasks=8, nodes=2),
            {"num_cpus": 10, "num_gpus": 8, "num_nodes": 2},
            id="gpu-nodes",
        ),
        pytest.param(
            ComputeNeeds(tasks=8, gpu_tasks=8, nodes=2),
            {"num_cpus": 8, "num_gpus": 8, "num_nodes": 2},
            id="gpu-tasks-all-gpu",
        ),
        pytest.param(
            ComputeNeeds(tasks=8, single_node=True, nodes=1),
            {"num_cpus": 8, "single_node": True, "num_nodes": 1},
            id="single-node-one-node",
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
