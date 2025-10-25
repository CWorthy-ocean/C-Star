import typing as t
from enum import IntEnum

import networkx as nx
import pytest


class TaskStatus(IntEnum):
    Unknown = 0
    Inactive = 1
    Active = 100
    Done = 200
    Failed = 300


class GraphPlanner:
    """Mock a graph planner with some parallelism."""

    _mock_plan: tuple[tuple[int, ...], ...]
    graph: nx.DiGraph

    def __init__(self) -> None:
        self._mock_plan = ((0,), (1,), (2, 3), (4,), (5,), (6,))

        edges = [(0, 1), (1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (5, 6)]

        self.graph = nx.DiGraph(edges)

    def plan(self) -> tuple[tuple[int, ...], ...]:
        return self._mock_plan


status_history = [
    {0: TaskStatus.Active},
    {0: TaskStatus.Active},
    {0: TaskStatus.Active},
    {0: TaskStatus.Active},
    {0: TaskStatus.Done},
    {0: TaskStatus.Done},
    {1: TaskStatus.Active},
    {1: TaskStatus.Active},
    {1: TaskStatus.Active},
    {1: TaskStatus.Active},
    {1: TaskStatus.Active},
    {1: TaskStatus.Active},
    {1: TaskStatus.Done},
    {1: TaskStatus.Done},
    {2: TaskStatus.Active},
    {2: TaskStatus.Active},
    {2: TaskStatus.Active, 3: TaskStatus.Active},
    {2: TaskStatus.Active, 3: TaskStatus.Active},
    {2: TaskStatus.Active, 3: TaskStatus.Active},
    {2: TaskStatus.Active, 3: TaskStatus.Active},
    {2: TaskStatus.Done, 3: TaskStatus.Active},
    {2: TaskStatus.Done, 3: TaskStatus.Active},
    {3: TaskStatus.Active},
    {3: TaskStatus.Active},
    {3: TaskStatus.Active},
    {3: TaskStatus.Active},
    {3: TaskStatus.Done},
    {3: TaskStatus.Done},
    {4: TaskStatus.Active},
    {4: TaskStatus.Active},
    {4: TaskStatus.Done},
    {4: TaskStatus.Done},
    {5: TaskStatus.Active},
    {5: TaskStatus.Active},
    {5: TaskStatus.Active},
    {5: TaskStatus.Active},
    {5: TaskStatus.Active},
    {5: TaskStatus.Done},
    {6: TaskStatus.Active},
    {6: TaskStatus.Done},
    {6: TaskStatus.Done},
]

status_iter = iter(status_history)


async def status_fn(keys: t.Iterable[int]) -> dict[int, TaskStatus]:
    """Mock the dynamic retrieval of a new state dictionary on each call."""
    if status_update := next(status_iter):
        # if keys:
        #     discards = [k for k in status_update.keys() if k not in keys]
        #     for discard in discards:
        #         status_update.pop(discard)

        return status_update

    raise StopAsyncIteration


_TKey = t.TypeVar("_TKey", bound=int)
_TValue = t.TypeVar("_TValue", bound=int)


class PlanTracker(t.Generic[_TKey, _TValue]):
    """Minimal planner class.

    Demonstrate iteration over plan with parallel task capability.

    Implements `self-as-iterable`.
    """

    STATUS_ATTR: t.Literal["status"] = "status"

    planner: GraphPlanner
    graph: nx.DiGraph
    history: list[nx.DiGraph]

    def __init__(
        self,
        planner: GraphPlanner,
        status_callback: t.Callable[
            [t.Iterable[_TKey]], t.Awaitable[dict[_TKey, _TValue]]
        ],
    ) -> None:
        """Initialize something to iterate over."""
        # self.stuff = [[1], [2, 3], [4], [5]]
        self.planner = planner
        self.graph = planner.graph.copy()
        self.status_callback = status_callback
        self.history = []

        # default_status = {n: TaskStatus.Unknown for n in self.graph.nodes}
        # nx.set_node_attributes(self.graph, default_status, PlanTracker.STATUS_ATTR)

    def __len__(self) -> int:
        """Get the number of items in the container."""
        return len(self.planner.plan())

    # def __iter__(self) -> t.Iterator:
    #     ...

    def __aiter__(
        self,
    ) -> t.AsyncIterator[dict[_TKey, _TValue]]:  # [dict[int, TaskStatus]]:
        """Return an iterator for the collection."""
        return self

    async def _update_tracking(self) -> None:
        """Retrieve status for incomplete nodes using the status callback."""
        node_keys = [n for n in self.graph.nodes]
        # .get(PlanTracker.STATUS_ATTR, TaskStatus.Inactive) != TaskStatus.Active]
        if not node_keys:
            return

        status_updates = await self.status_callback(node_keys)
        dirty = False

        snapshot = self.graph.copy()

        for task_name, latest_status in status_updates.items():
            if task_name not in snapshot.nodes:
                # node may already be removed. ignore status update
                print(f"WARNING - received an unknown task_name: `{task_name}`")
                continue

            prior_status = snapshot.nodes[task_name].get(
                PlanTracker.STATUS_ATTR, TaskStatus.Unknown
            )

            if latest_status > prior_status:
                # change in state
                dirty = True

                snapshot.nodes[task_name][PlanTracker.STATUS_ATTR] = latest_status
                print(
                    f"Node `{task_name}` state changed from `{prior_status}` to `{latest_status}`"
                )

                if latest_status > TaskStatus.Active:
                    snapshot.remove_node(task_name)
                    print(
                        f"Node `{task_name}` state changed from `{prior_status}` to completed state `{latest_status}`"
                    )

        if dirty:
            self.history.append(self.graph)
            self.graph = snapshot
            print("Snapshot of graph history stored")

    @property
    def ready_nodes(self) -> t.Iterable[_TKey]:
        """Find all nodes that are ready to execute.

        A ready node is any node with no incoming edges; this indicates that all
        dependencies have been satisfied.
        """
        open_nodes = tuple(n for n in self.graph.nodes if self.graph.in_degree(n) == 0)
        print(f"Current open list: {open_nodes}")
        return open_nodes

    def _node_status(self, key: _TKey) -> _TValue:
        """Retrieve the stored status for a given node key."""
        return self.graph.nodes[key].get(PlanTracker.STATUS_ATTR, TaskStatus.Unknown)

    async def __anext__(self) -> dict[_TKey, _TValue]:
        """Return the next available item in the collection.

        NOTE: since `__iter__` returns `self`, position must be maintained here.
        """
        if self.graph.nodes:
            # as long as nodes exist, continue iteration.
            await self._update_tracking()

            if open_nodes := self.ready_nodes:
                return {key: self._node_status(key) for key in open_nodes}

            return {}

        raise StopAsyncIteration


@pytest.mark.asyncio
async def test_iter() -> None:
    """Verify that the StopIteration error is not propagated."""
    planner = GraphPlanner()
    lp = PlanTracker(planner, status_fn)

    async for item in lp:
        print(item)

    assert not lp.graph.nodes
