import asyncio
import os
import typing as t
from enum import IntEnum, StrEnum, auto
from pathlib import Path

import networkx as nx

from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import LoggingMixin
from cstar.base.utils import ENV_CSTAR_OUTDIR, slugify
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.utils import (
    ENV_CSTAR_ORCH_RUNID,
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_QUEUE,
)

KEY_STATUS: t.Literal["status"] = "status"
KEY_STEP: t.Literal["step"] = "step"
KEY_TASK: t.Literal["task"] = "task"


class RunMode(StrEnum):
    """Specify the blocking behavior during plan execution."""

    Monitor = auto()
    """Block until tasks complete."""

    Schedule = auto()
    """Block until tasks are scheduled."""


class ProcessHandle:
    """Contract used to identify processes created by any launcher."""

    pid: str
    """The process identifier."""

    def __init__(self, pid: str) -> None:
        """Initialize the handle.

        Parameters
        ----------
        pid : str
            A unique ID identifying a running process.
        """
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
    def is_terminal(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of terminal statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Done, Status.Cancelled, Status.Failed}

    @classmethod
    def is_failure(cls, status) -> bool:
        """Return `True` if a status is in the set of terminal statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Cancelled, Status.Failed}

    @classmethod
    def is_running(cls, status) -> bool:
        """Return `True` if a status is in the set of in-progress statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in {Status.Submitted, Status.Running, Status.Ending}


_THandle = t.TypeVar("_THandle", bound=ProcessHandle)


class Task(t.Generic[_THandle]):
    """A task represents a live-execution of a step."""

    status: Status
    """Current task status."""

    step: Step
    """The step containing task configuration."""

    handle: _THandle
    """The unique process identifier for the task."""

    def __init__(
        self,
        step: Step,
        handle: _THandle,
        status: Status = Status.Unsubmitted,
    ) -> None:
        """Initialize the Task record.

        Parameters
        ----------
        step : Step
            The workplan `Step` that triggered the task to run
        handle : _THandle
            A handle that used to identify the running task.
        status : Status
            The current status of the task
        """
        self.status = status
        self.step = step
        self.handle = handle


class Planner(LoggingMixin):
    """Identifies depdendencies of a workplan to produce an execution plan."""

    workplan: Workplan
    """The workplan to plan."""

    graph: nx.DiGraph
    """The graph used for task planning."""

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
            n.name: {KEY_STATUS: Status.Unsubmitted, KEY_STEP: n, KEY_TASK: None}
            for n in workplan.steps
        }
        nx.set_node_attributes(g, values=defaults)
        return g

    def flatten(self) -> t.Iterable[Step]:
        """Return the planned steps in execution order.

        Returns
        -------
        Iterable[Step]
            A traversal of the execution plan honoring all dependencies.
        """

        def f(step: Step) -> bool:
            """Filter steps that are non-null."""
            return step is not None

        keys = nx.topological_sort(self.graph)
        steps = self.retrieve_all(KEY_STEP, filter_fn=f)
        return [steps[k] for k in keys]

    @t.overload
    def store(self, n: str, key: t.Literal["status"], value: Status) -> None: ...

    @t.overload
    def store(self, n: str, key: t.Literal["step"], value: Step) -> None: ...

    @t.overload
    def store(self, n: str, key: t.Literal["task"], value: Task) -> None: ...

    def store(self, n: str, key: str, value: object) -> None:
        """Store an arbitrary attribute on a node in the plan.

        Parameters
        ----------
        n : str
            The node identifier.
        key : str
            The key to be written to on the node.
        value : object
            The value to be stored.
        """
        stored = self.graph.nodes[n].get(key, "")
        if key in {KEY_STATUS, KEY_STEP, KEY_TASK} and stored != value:
            msg = f"Updating reserved key `{key}` on node `{n}` with value `{value}`"
            self.log.debug(msg)

        self.graph.nodes[n][key] = value

    @t.overload
    def retrieve(
        self,
        n: str,
        key: t.Literal["status"],
        default: Status | None = None,
    ) -> Status | None: ...

    @t.overload
    def retrieve(
        self,
        n: str,
        key: t.Literal["step"],
        default: Step | None = None,
    ) -> Step | None: ...

    @t.overload
    def retrieve(
        self,
        n: str,
        key: t.Literal["task"],
        default: Task | None = None,
    ) -> Task | None: ...

    def retrieve(
        self,
        n: str,
        key: str,
        default: t.Any | None = None,
    ) -> t.Any | None:
        """Retrieve an attribute from a node in the plan.

        Parameters
        ----------
        n : str
            The node identifier.
        key : str
            The key to be retrieved from the node.
        default : t.Any | None
            The default value to retrieve if one is not found.

        Returns
        -------
        t.Any
            The value stored on the node retrieved using the key.
        """
        node = self.graph.nodes[n]
        return node.get(key, default)

    @t.overload
    def retrieve_all(
        self,
        key: t.Literal["status"],
        default: Status | None = None,
        filter_fn: t.Callable[[Status], bool] | None = None,
    ) -> t.Mapping[str, Status]: ...

    @t.overload
    def retrieve_all(
        self,
        key: t.Literal["step"],
        default: Step | None = None,
        filter_fn: t.Callable[[Step], bool] | None = None,
    ) -> t.Mapping[str, Step]: ...

    @t.overload
    def retrieve_all(
        self,
        key: t.Literal["task"],
        default: Task | None = None,
        filter_fn: t.Callable[[Task], bool] | None = None,
    ) -> t.Mapping[str, Task]: ...

    def retrieve_all(
        self,
        key: str,
        default: t.Any | None = None,
        filter_fn: t.Callable[[t.Any], bool] | None = None,
    ) -> t.Mapping[str, t.Any]:
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
        values = {n: self.graph.nodes[n].get(key, default) for n in self.graph.nodes}
        if filter_fn:
            values = {k: v for k, v in values.items() if filter_fn(v)}
        return values


class Launcher(t.Protocol, t.Generic[_THandle]):
    """Contract required to implement a task launcher."""

    @classmethod
    async def launch(cls, step: Step, dependencies: list[_THandle]) -> Task[_THandle]:
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
    async def query_status(cls, step: Step, item: Task[_THandle] | _THandle) -> Status:
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


class Orchestrator(LoggingMixin):
    """Manage the execution of a `Workplan`."""

    planner: Planner
    """The planner used by the orchestrator to prioritize tasks."""

    launcher: Launcher
    """The launcher used by the orchestrator to manage task execution."""

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
        closed_set = self.get_closed_nodes(mode=mode)

        if self.planner.workplan:
            nodes = {s.name for s in self.planner.workplan.steps}
        else:
            nodes = {n for n in self.planner.graph}

        working_list = set(nodes).difference(closed_set)

        if failures := {
            u: g.nodes[u][KEY_STATUS]
            for u in closed_set
            if Status.is_failure(g.nodes[u][KEY_STATUS])
        }:
            self.log.error(f"Exiting due to task failures: {failures}")
            return None

        for n in working_list:
            in_edges = list(g.in_edges(n))
            in_degree = g.in_degree(n)

            satisfied = all(
                Status.is_running(g.nodes[u][KEY_STATUS])
                or Status.is_terminal(g.nodes[u][KEY_STATUS])
                if mode == RunMode.Schedule
                else Status.is_terminal(g.nodes[u][KEY_STATUS])
                for (u, _) in in_edges
            )

            if in_degree == 0 or satisfied:
                open_nodes.append(n)

        if working_list:
            # working list has options. if none are ready, return empty set.
            return set(open_nodes)

        return None

    def get_closed_nodes(self, *, mode: RunMode) -> set[str]:
        """Retrieve the set of task nodes with a terminal state.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with a Done status.
        """
        targets = {Status.Done, Status.Cancelled, Status.Failed}

        if mode == RunMode.Schedule:
            targets.update({Status.Submitted, Status.Running})

        return set(
            self.planner.retrieve_all(KEY_STATUS, filter_fn=lambda x: x in targets)
        )

    def _locate_dependencies(self, step: Step) -> list[ProcessHandle] | None:
        """Look for the dependencies of the step.

        Returns
        -------
        list[ProcessHandle] | None
            The handles identifying the dependencies if the jobs are scheduled,
            otherwise None.

            An empty list indicates no dependencies.
        """
        if not step.depends_on:
            return []

        # TODO: replace this with proactively configuring the keys?
        # - e.g. reverse lookup...
        dep_tasks = [
            self.planner.retrieve(dnode, KEY_TASK) for dnode in step.depends_on
        ]

        running_deps = [x for x in dep_tasks if x]

        if len(running_deps) != len(step.depends_on):
            # the dependencies have not been started. abort launch...
            return None

        return [d.handle for d in running_deps]

    async def process_node(self, node: str) -> Task | None:
        """Execute a task.

        Parameters
        ----------
        node : str
            The name of the node to process.

        Returns
        -------
        Task | None
            The created task, if successfully processed.
        """
        step = self.planner.retrieve(node, KEY_STEP, None)
        if step is None:
            msg = f"Unable to process. Invalid node identifier supplied: {node}"
            raise ValueError(msg)

        dependencies = self._locate_dependencies(step)
        if dependencies is None:
            # prerequisite tasks weren't all started, yet.
            return None

        if task := self.planner.retrieve(node, KEY_TASK):
            status = await self.launcher.query_status(task.step, task)
            task.status = status
        else:
            task = await self.launcher.launch(step, dependencies)
            self.planner.store(node, KEY_TASK, task)
            self.log.info(f"Launched step: {step.name}")

        self.planner.store(node, KEY_STATUS, task.status)
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

        if task.status == Status.Done:
            self.log.info(f"Closed node: {n}")
            self.planner.store(n, KEY_STATUS, Status.Done)
        elif task.status == Status.Failed:
            self.log.warning(f"Failed node: {n}")
            # TODO: on failure, cancel all jobs if anything depends on it
            # - NOTE: this may occur naturally with SLURM but not local launch
            self.planner.store(n, KEY_STATUS, Status.Failed)
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
        Mapping[str, Status]
            Mapping of node names to their current status.
        """
        open_set = self.get_open_nodes(mode=mode)

        if open_set is None:
            # no open nodes were found, return all current statuses
            return self.planner.retrieve_all(
                KEY_STATUS,
                default=Status.Unsubmitted,
            )

        # Ensure task/result pairing is consistent with a list
        open_nodes = list(open_set)
        exec_tasks = [asyncio.Task(self.process_node(n)) for n in open_nodes]
        exec_results = await asyncio.gather(*exec_tasks)

        kvp = dict(zip(open_nodes, exec_results))
        postproc_tasks = [
            asyncio.Task(self.update_planner_state(n, t)) for n, t in kvp.items()
        ]
        cancellations: set[Task] = set()

        try:
            await asyncio.gather(*postproc_tasks)
        except CstarExpectationFailed:
            cancellations = {
                v for v in kvp.values() if v and Status.is_running(v.status)
            }

        await self._cancel(cancellations)

        return {k: v.status if v else Status.Unsubmitted for k, v in kvp.items()}

    async def _cancel(self, cancellations: t.Iterable[Task]) -> None:
        """Request the cancellation of running tasks.

        Parameters
        ----------
        cancellations : Iterable[Task]
            The tasks to be cancelled.
        """
        tasks = [asyncio.Task(self.launcher.cancel(task)) for task in cancellations]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for task in results:
            self.planner.store(task.step.name, KEY_STATUS, task.status)


def check_environment() -> None:
    """Verify the environment is configured correctly.

    Raises
    ------
    ValueError
        If required environment variables are missing or empty.
    """
    required_vars = (
        ENV_CSTAR_SLURM_ACCOUNT,
        ENV_CSTAR_SLURM_QUEUE,
        ENV_CSTAR_ORCH_RUNID,
    )

    for key in required_vars:
        if not os.getenv(key, ""):
            raise ValueError(
                f"Unable to run workplan. `{key}` not found in environment."
            )


def configure_environment(
    output_dir: Path | None = None, run_id: str | None = None
) -> None:
    """Configure environment variables required by the runner.

    Parameters
    ----------
    output_dir : Path | None
        The directory where outputs will be written.
    run_id : str | None
        The unique identifier for an execution of the workplan.
    """
    if output_dir:
        os.environ[ENV_CSTAR_OUTDIR] = output_dir.expanduser().resolve().as_posix()

    if run_id:
        os.environ[ENV_CSTAR_ORCH_RUNID] = slugify(run_id)

    os.environ["CSTAR_FF_POSTRUN_ANALYSIS"] = "1"
