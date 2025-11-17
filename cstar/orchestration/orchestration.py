import asyncio
import functools
import typing as t
from enum import IntEnum, StrEnum, auto

import networkx as nx
from pydantic import BaseModel, Field

from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.models import Workplan


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
    Submitted = auto()
    """A task that has been submitted by a launcher and awaits a status update."""
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
    def is_failure(cls, status) -> bool:
        """Return `True` if a status is in the set of terminal statuses."""
        return status in {Status.Cancelled, Status.Failed}

    @classmethod
    def is_running(cls, status) -> bool:
        """Return `True` if a status is in the set of in-progress statuses."""
        return status in {Status.Submitted, Status.Running, Status.Ending}


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

    blueprint: str = ""  # todo: unify with "real step" and kill CStep
    """The path to a blueprint file."""


_THandle = t.TypeVar("_THandle", bound=ProcessHandle)


class Task(t.Generic[_THandle]):
    """A task represents a live-execution of a step."""

    status: Status
    """Current task status."""

    step: CStep
    """The step containing task configuration."""

    handle: _THandle
    """The unique process identifier for the task."""

    def __init__(
        self,
        step: CStep,
        handle: _THandle,
        status: Status = Status.Unsubmitted,
    ) -> None:
        """Initialize the Task record.

        Parameters
        ----------
        status : Status
            The current status of the task
        step : Step
            The workplan `Step` that triggered the task to run
        handle : _THandle
            A handle that used to identify the running task.
        """
        self.status = status
        self.step = step
        self.handle = handle


_TValue = t.TypeVar("_TValue")


class Planner:
    """Identifies depdendencies of a workplan to produce an execution plan."""

    workplan: Workplan
    """The workplan to plan."""

    graph: nx.DiGraph = Field(init=False)
    """The graph used for task planning."""

    class Keys(StrEnum):
        """Keys used to store attributes on the planning graph."""

        Step = auto()
        """Key for the step associated with a task."""
        Status = auto()
        """Key for the status associated with a step."""

    def __init__(
        self,
        workplan: Workplan,
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
    def _workplan_to_graph(cls, workplan: Workplan) -> nx.DiGraph:
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


class Launcher(t.Protocol, t.Generic[_THandle]):
    """Contract required to implement a task launcher."""

    @classmethod
    async def launch(cls, step: CStep, dependencies: list[_THandle]) -> Task[_THandle]:
        """Launch a process for a step.

        Parameters
        ----------
        step : Step
            The step to launch

        Returns
        -------
        Task[_THandle]
            The newly launched task.
        """
        ...

    @classmethod
    async def query_status(cls, step: CStep, item: Task[_THandle] | _THandle) -> Status:
        """Retrieve the current status for a running task.

        Parameters
        ----------
        item : Task[_THandle] or ProcessHandle
            A task or process handle to query for status updates.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...

    @classmethod
    async def cancel(cls, item: Task[_THandle]) -> Task[_THandle]:
        """Cancel a task, if possible.

        Parameters
        ----------
        item : Task[_THandle] or ProcessHandle
            A task or process handle to cancel.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...


class Orchestrator:
    """Manage the execution of a `Workplan`."""

    class RunMode(StrEnum):
        """Specify the blocking behavior during plan execution."""

        Monitor = auto()
        """Block until tasks complete."""

        Schedule = auto()
        """Block until tasks are scheduled."""

    class Keys(StrEnum):
        """Keys used to store tracker attributes on the planning graph."""

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

    def get_open_nodes(self, *, mode: RunMode) -> set[str] | None:
        """Retrieve the set of task nodes with a non-terminal state that are
        executing or ready to execute.

        Returns
        -------
        set[str] | None
            - Set of open nodes ready for some processing actions.
            - An empty set indicates no actions are currently possible.
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

        if any(Status.is_failure(g.nodes[u][Planner.Keys.Status]) for u in closed_set):
            print("Exiting due to execution failures")
            return None

        for n in working_list:
            in_edges = list(g.in_edges(n))
            in_degree = g.in_degree(n)

            satisfied = all(
                Status.is_running(g.nodes[u][Planner.Keys.Status])
                or Status.is_terminal(g.nodes[u][Planner.Keys.Status])
                if mode == Orchestrator.RunMode.Schedule
                else Status.is_terminal(g.nodes[u][Planner.Keys.Status])
                for (u, _) in in_edges
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
        target: Status
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
        done = self._get_nodes_by_status(Status.Done)
        cancelled = self._get_nodes_by_status(Status.Cancelled)
        failed = self._get_nodes_by_status(Status.Failed)
        return {*cancelled, *failed, *done}

    def get_wip_nodes(self) -> set[str]:
        """Retrieve the set of task nodes that are executing.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with the Running status.
        """
        return self._get_nodes_by_status(Status.Running)

    def _locate_depedendencies(self, step: CStep) -> list[ProcessHandle] | None:
        """Look for the dependencies of the step.

        Returns
        -------
        list[ProcessHandle] | None
            The handles identifying the dependencies if the jobs are scheduled,
            otherwise None.

            An empty list indicates no dependencies.
        """
        dependencies: list[ProcessHandle] = []

        # TODO: replace this with proactively configuring the keys?
        # - e.g. reverse lookup...
        if step.depends_on:
            dep_tasks = [
                t.cast(
                    Task | None,
                    self.planner.retrieve(dnode, Orchestrator.Keys.Task, None),
                )
                for dnode in step.depends_on
            ]

            running_deps = [x for x in dep_tasks if x]

            if len(running_deps) != len(step.depends_on):
                # the dependencies have not been started. abort launch...
                return None

            dependencies = [d.handle for d in running_deps]

        return dependencies

    async def process_node(self, node: str) -> Task | None:
        """Execute a task.

        Parameters
        ----------
        node : str
            The name of the node to process.
        """
        step = t.cast(
            CStep | None, self.planner.retrieve(node, Planner.Keys.Step, None)
        )
        if step is None:
            msg = f"Unable to process. Invalid node identifier supplied: {node}"
            raise ValueError(msg)

        dependencies = self._locate_depedendencies(step)
        if dependencies is None:
            # prerequisite tasks weren't all started, yet.
            return None

        if task := self.planner.retrieve(node, Orchestrator.Keys.Task):
            status = await self.launcher.query_status(task.step, task)
            task.status = status
        else:
            task = await self.launcher.launch(step, dependencies)
            self.planner.store(node, Orchestrator.Keys.Task, task)
            print(f"Launched step: {step.name}")

        self.planner.store(node, Planner.Keys.Status, task.status)
        return task

    async def update_planner_state(self, n: str, task: Task | None) -> None:
        """Update tracking information for the plan after starting a task or
        fetching an update.

        Parameters
        ----------
        n : str
            Node name
        task : Task | None
            The newly started task (or None if task fails to start).
        """
        if task is None:
            return

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
                raise CstarExpectationFailed(f"Node {n} task failed.")

    async def run(self, mode: RunMode) -> t.Mapping[str, Status]:
        """Execute tasks that are ready and query status on running tasks.

        Parameters
        ----------
        mode : RunMode
            The operation mode. Passing `schedule` allows the orchestrator to
            submit tasks without waiting for their completion. Passing `monitor`
            causes the scheduler to track status for the tasks.

        Returns
        -------
        Mapping[Status]
            Mapping of node names to their current status.
        """
        open_set = self.get_open_nodes(mode=mode)

        if open_set is None:
            # no open nodes were found, return all current statuses
            return self.planner.retrieve_all(
                Planner.Keys.Status,
                default=Status.Unsubmitted,
            )

        exec_tasks = [asyncio.Task(self.process_node(n)) for n in open_set]
        exec_results = await asyncio.gather(*exec_tasks)

        # TODO: confirm iter order of set. may be zipping incorrect n/result
        # NOTE - it won't matter once step name is sync'd and i can pass 1 arg

        kvp = dict(zip(open_set, exec_results))
        postproc_tasks = [
            asyncio.Task(self.update_planner_state(n, t)) for n, t in kvp.items()
        ]
        cancellations: tuple[Task, ...] = tuple()

        try:
            await asyncio.gather(*postproc_tasks)
        except CstarExpectationFailed:
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
