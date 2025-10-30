import asyncio
import functools
import random
import typing as t
from enum import IntEnum, StrEnum, auto

import networkx as nx
from pydantic import BaseModel, Field


class CstarDependencyError(Exception):
    """Raise this error when a critical task in a workplan fails."""

    ...


class ProcessHandle:
    """Contract used to identify processes created by any launcher."""

    pid: str
    """The process identifier."""

    def __init__(self, pid: str) -> None:
        """Initialize the handle."""
        self.pid = pid


class Status(IntEnum):
    """The state of a running task."""

    Unsubmitted = auto()
    """A task that has not been submitted by a launcher."""
    Running = auto()
    """A task that was submitted and has not terminated."""
    Ending = auto()
    """A task that has reported itself as nearing completion."""
    Done = auto()
    """A task that has terminated without error."""
    Cancelled = auto()
    """A task terminated due to cancellation (by the user or system)."""
    Failed = auto()
    """A task that terminated due to some failure in the task."""

    @classmethod
    def is_terminal(cls, status) -> bool:
        """Return `True` if a status is in the set of terminal statuses."""
        return status in {Status.Done, Status.Cancelled, Status.Failed}

    @classmethod
    def is_running(cls, status) -> bool:
        """Return `True` if a status is in the set of in-progress statuses."""
        return status in {Status.Running, Status.Ending}


class CStep(BaseModel):
    """User-defined configuration for execution of an application within a workplan."""

    name: str
    """The user-friendly name of the step."""

    application: str = Field(default="sleep")
    """The target application to execute."""

    critical: bool = Field(default=False)
    """(experimental) Mark steps that must be retried on failure."""

    depends_on: list[str] = Field(default_factory=list)
    """List containing the names of steps that must complete to start this step."""


class CWorkplan(BaseModel):
    name: str
    """The user-friendly workplan name."""

    # steps: t.Iterable[CStep] = Field(default_factory=list)
    steps: list[CStep]
    """The list of steps contained in the workplan."""


class Task:
    """A task represents a live-execution of a step."""

    status: Status
    """Current task status."""

    step: CStep
    """The step containing task configuration."""

    handle: ProcessHandle
    """The unique process identifier for the task."""

    def __init__(
        self,
        step: CStep,
        handle: ProcessHandle,
        status: Status = Status.Unsubmitted,
    ):
        self.status = status
        self.step = step
        self.handle = handle


_TValue = t.TypeVar("_TValue")


class Planner:
    """Identifies depdendencies of a workplan to produce an execution plan."""

    workplan: CWorkplan
    """The workplan to plan."""

    graph: nx.DiGraph = Field(init=False)
    """The graph used for task planning."""

    class Keys(StrEnum):
        """Identifies the attributes stored in the planning graph."""

        Step = auto()
        """Key for the step associated with a task."""
        Status = auto()
        """Key for the status associated with a step."""

    def __init__(
        self,
        workplan: CWorkplan,
    ) -> None:
        """Initialize the planner and build an execution graph.

        Parameters
        ----------
        workplan: Workplan
            The workplan to be planned.
        """
        self.workplan = workplan
        self.graph = Planner._workplan_to_graph(workplan)

    @classmethod
    def _workplan_to_graph(cls, workplan: CWorkplan) -> nx.DiGraph:
        """Convert a workplan into a graph for planning.

        Parameters
        ----------
        workplan: Workplan
            The workplan to be converted.

        Returns
        -------
        nx.DiGraph
            A graph of the execution plan.
        """
        data: t.Mapping[str, list[str]] = {s.name: [] for s in workplan.steps}
        for step in workplan.steps:
            for prereq in step.depends_on:
                data[prereq].append(step.name)

        g = nx.DiGraph(data)
        defaults = {
            n.name: {Planner.Keys.Status: Status.Unsubmitted, Planner.Keys.Step: n}
            for n in workplan.steps
        }
        nx.set_node_attributes(g, values=defaults)
        return g

    def flatten(self) -> t.Iterable[CStep]:
        """Return the planned steps in execution order.

        Returns
        -------
        Iterable[Step]
            A traversal of the execution plan honoring all dependencies.
        """

        def f(step: CStep) -> bool:
            return step is not None

        keys = nx.topological_sort(self.graph)
        steps = self.retrieve_all(Planner.Keys.Step, filter_fn=f)
        return [steps[k] for k in keys]

    def store(self, n: str, key: str, value: object) -> None:
        """Store a user-defined value with the plan.

        Parameters
        ----------
        n : str
            The node identifier.
        key : str
            The key to be written to on the node.
        """
        if key in Planner.Keys.__dict__:
            msg = f"WARNING: Writing to reserved key `{key}` on node `{n}`"
            print(msg)

        node = self.graph.nodes[n]
        node[key] = value

    def retrieve(
        self,
        n: str,
        key: str,
        default: _TValue | None = None,
    ) -> _TValue | None:
        """Retrieve a user-defined value from the plan.

        Parameters
        ----------
        n : str
            The node identifier.
        key : str
            The key to be retrieved from the node.
        default : _TValue | None
            The default value to retrieve if one is not found.

        Returns
        -------
        _TValue
            The value stored on the node retrieved using the key.
        """
        node = self.graph.nodes[n]
        return node.get(key, default)

    def retrieve_all(
        self,
        key: str,
        default: _TValue | None = None,
        filter_fn: t.Callable[[_TValue], bool] | None = None,
    ) -> t.Mapping[str, _TValue]:
        """Retrieve a user-defined value for every node in the plan.

        Parameters
        ----------
        key : str
            The key to be retrieved from the node.
        default : _TValue | None
            The default value to retrieve if one is not found.
        filter_fn : Callable[[_TValue], bool] | None
            A filter function to execute against the returned values.

        Returns
        -------
        Mapping[str, _TValue]
            Mapping of node name to value retrieved for the key.
        """
        values = {
            n: t.cast(_TValue, self.graph.nodes[n].get(key, default))
            for n in self.graph.nodes
        }
        if filter_fn:
            values = {k: v for k, v in values.items() if filter_fn(v)}
        return values


class Launcher(t.Protocol):
    """Contract required to implement a task launcher."""

    @classmethod
    async def launch(cls, step: CStep) -> Task:
        """Launch a process for a step.

        Parameters
        ----------
        step : Step
            The step to launch

        Returns
        -------
        Task
            The newly launched task.
        """
        ...

    @classmethod
    async def query_status(cls, item: Task | ProcessHandle) -> Status:
        """Retrieve the current status for a running task.

        Parameters
        ----------
        item : Task or ProcessHandle
            A task or process handle to query for status updates.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...

    @classmethod
    async def cancel(cls, item: Task) -> Task:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...


class Orchestrator:
    """Manage the execution of a `Workplan`."""

    class Keys(StrEnum):
        """Identifies the storage location for attributes used by a tracker."""

        Task = auto()
        """Key for the task associated with a step."""

    def __init__(self, planner: Planner, launcher: Launcher) -> None:
        """Initialize the orchestrator.

        Parameters
        ----------
        planner : Planner
            The planner containing an execution plan for a workplan.
        launcher : Launcher
            A launcher to manage processes for tasks.
        """
        self.planner = planner
        self.launcher = launcher

    def get_open_nodes(self) -> set[str] | None:
        """Retrieve the set of task nodes with a non-terminal state.

        Returns
        -------
        set[str] | None
            - Set of nodes to launch or retrieve status updates.
            - Empty set indicates no actions are currently possible.
            - Null indicates all nodes are closed (traversal is complete).
        """
        g = self.planner.graph

        open_nodes: list[str] = []
        closed_set = self.get_closed_nodes()
        if self.planner.workplan:
            nodes = {s.name for s in self.planner.workplan.steps}
        else:
            nodes = {n for n in self.planner.graph}

        working_list = set(nodes).difference(closed_set)

        for n in working_list:
            in_edges = list(g.in_edges(n))
            in_degree = g.in_degree(n)

            satisfied = all(
                g.nodes[u][Planner.Keys.Status] == Status.Done for (u, _) in in_edges
            )

            if in_degree == 0 or satisfied:
                open_nodes.append(n)

        if working_list:
            # working list has options. if none are ready, return empty set.
            return set(open_nodes)

        return None

    @staticmethod
    def _status_filter(status: Status, target: Status) -> bool:
        """Filter function for status attribute stored by the planner.

        Parameters
        ----------
        status : Status
            The first status to compare.
        other : Status
            The second status to compare.

        Returns
        -------
        bool
            `True` if the statuses are equal, `False` otherwise.
        """
        return status == target

    def _get_nodes_by_status(self, status: Status) -> set[str]:
        """Retrieve all nodes with a specific status.

        Parameters
        ----------
        status : Status
            The status to find.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with the status.
        """
        filter_fn = functools.partial(Orchestrator._status_filter, target=status)
        if matches := self.planner.retrieve_all(
            Planner.Keys.Status,
            filter_fn=filter_fn,
        ):
            return set(matches.keys())
        return set()

    def get_ready_nodes(self) -> set[str]:
        """Retrieve the set of task nodes that have not been launched.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with the Unsubmitted.
        """
        return self._get_nodes_by_status(Status.Unsubmitted)

    def get_closed_nodes(self) -> set[str]:
        """Retrieve the set of task nodes with a terminal state.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with a Done status.
        """
        return self._get_nodes_by_status(Status.Done)

    def get_wip_nodes(self) -> set[str]:
        """Retrieve the set of task nodes that are executing.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with the Running status.
        """
        return self._get_nodes_by_status(Status.Running)

    async def execute_node(self, node: str) -> Task | None:
        """Execute a 'simulated task'."""
        step = self.planner.retrieve(node, Planner.Keys.Step)
        if not step:
            msg = f"Invalid node identifier supplied: {node}"
            raise ValueError(msg)
        task = self.planner.retrieve(node, Orchestrator.Keys.Task)

        if not task:
            # remove `True or ` to simulate stochastic start up speed...
            if True or random.randint(1, 100) > 35:
                task = await self.launcher.launch(step)

                self.planner.store(node, Planner.Keys.Status, task.status)
                self.planner.store(node, Orchestrator.Keys.Task, task)

                print(f"Launched step: {step.name}")
                return task

            print(f"\t\tTask {node} did not start yet...")
        else:
            task = await self.update_status(task)

        return task

    async def postprocess_node(self, n: str, task: Task | None) -> None:
        """Perform post-processing after starting a task or fetching it's update.

        Parameters
        ----------
        n : str
            Node name
        task : Task | None
            The newly started task (or None if task fails to start).
        """
        if task is None:
            return

        # n = task.step.name  # todo: synchronize graph task names & workplan steps
        step = self.planner.retrieve(n, Planner.Keys.Step)

        if task.status == Status.Done:
            print(f"\t\tClosed node: {n}")
            self.planner.store(n, Planner.Keys.Status, Status.Done)
        elif task.status == Status.Failed:
            print(f"\t\tFailed node: {n}")
            # TODO: on failure, cancel all jobs if anything depends on it
            # - NOTE: this may occur naturally with SLURM but not local launch
            self.planner.store(n, Planner.Keys.Status, Status.Failed)
            if step and step.critical:
                raise CstarDependencyError(f"Node {n} task failed.")

    async def update_status(self, task: Task) -> Task:
        status = await self.launcher.query_status(task)
        # TODO: consider pushing update to the handle and using...
        # await task.query_status()
        task.status = status
        return task

    async def run(self) -> t.Mapping[str, Status]:
        """Execute tasks that are ready and query status on running tasks.

        Returns
        -------
        Mapping[Status]
            Mapping of node names to their current status.
        """
        open_set = self.get_open_nodes()

        if open_set is None:
            # no open nodes were found, return all current statuses
            return self.planner.retrieve_all(
                Planner.Keys.Status,
                default=Status.Unsubmitted,
            )
            # return {}

        exec_tasks = [asyncio.Task(self.execute_node(n)) for n in open_set]
        exec_results = await asyncio.gather(*exec_tasks)

        # TODO: confirm iter order of set. may be zipping incorrect n/result
        # NOTE - it won't matter once step name is sync'd and i can pass 1 arg

        kvp = dict(zip(open_set, exec_results))
        postproc_tasks = [
            asyncio.Task(self.postprocess_node(n, t)) for n, t in kvp.items()
        ]
        cancellations: tuple[Task, ...] = tuple()

        try:
            await asyncio.gather(*postproc_tasks)
        except CstarDependencyError:
            # cancel all running tasks except the known failure
            todo = {k: v for k, v in kvp.items() if v and Status.is_running(v.status)}
            cancellations = tuple(todo.values())

        await self._cancel(cancellations)

        return {k: v.status if v else Status.Unsubmitted for k, v in kvp.items()}

    async def _cancel(self, cancellations: t.Iterable[Task]) -> None:
        """Request the cancellation of running tasks.

        Parameters
        ----------
        cancellations : Iterable[Task]
            The tasks to be cancelled.
        """
        if cancellations:
            cancel_tasks = [
                asyncio.Task(self.launcher.cancel(t)) for t in cancellations
            ]
            cancel_results = await asyncio.gather(
                *cancel_tasks, return_exceptions=False
            )
            for t in cancel_results:
                self.planner.store(t.step.name, Planner.Keys.Status, t.status)
