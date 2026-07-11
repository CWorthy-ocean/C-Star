import asyncio
import os
import typing as t
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from enum import IntEnum, StrEnum, auto
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from cstar.applications.core import ApplicationDefinition, get_application
from cstar.base.env import (
    ENV_CSTAR_DATA_HOME,
    ENV_CSTAR_RUNID,
)
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import LoggingMixin
from cstar.base.utils import lazy_import, slugify
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
)
from cstar.orchestration.converter.converter import (
    convert_step_to_blueprint_run_command,
)
from cstar.orchestration.models import Blueprint, Step, Workplan
from cstar.orchestration.serialization import (
    deserialize,
    intenum_representer,
    register_representer,
)
from cstar.system.environment import get_envfield_alias
from cstar.system.manager import cstar_sysmgr

nx = lazy_import("networkx")

KEY_STATUS: t.Literal["status"] = "status"
KEY_STEP: t.Literal["step"] = "step"
KEY_TASK: t.Literal["task"] = "task"

if t.TYPE_CHECKING:
    from networkx import DiGraph

    from cstar.entrypoint.runner import BlueprintRunner


class RunMode(StrEnum):
    """Specify the blocking behavior during plan execution."""

    Monitor = auto()
    """Block until tasks complete."""

    Schedule = auto()
    """Block until tasks are scheduled."""


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
    def terminal_states(cls) -> set["Status"]:
        """Return the set of terminal statuses.

        Returns
        -------
        set["Status"]
        """
        return {Status.Done, Status.Cancelled, Status.Failed}

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
        return status in cls.terminal_states()

    @classmethod
    def failure_states(cls) -> set["Status"]:
        """Return the set of failure statuses.

        Returns
        -------
        set["Status"]
        """
        return {Status.Cancelled, Status.Failed}

    @classmethod
    def is_failure(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of terminal statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in cls.failure_states()

    @classmethod
    def ready_states(cls) -> set["Status"]:
        """Return the set of ready statuses.

        Returns
        -------
        set["Status"]
        """
        return {Status.Unsubmitted, Status.Submitted}

    @classmethod
    def is_ready(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of ready statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in cls.ready_states()

    @classmethod
    def running_states(cls) -> set["Status"]:
        """Return the set of running statuses.

        Returns
        -------
        set["Status"]
        """
        return {Status.Running, Status.Ending}

    @classmethod
    def is_running(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of running statuses.

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in cls.running_states()

    @classmethod
    def in_progress_states(cls) -> set["Status"]:
        """Return the set of in-progress statuses.

        Returns
        -------
        set["Status"]
        """
        return {Status.Submitted, Status.Running, Status.Ending}

    @classmethod
    def is_in_progress(cls, status: "Status") -> bool:
        """Return `True` if a status is in the set of in-progress statuses (any non-terminal status).

        Paramters
        ---------
        status : "Status"
            The status to evaluate.

        Returns
        -------
        bool
        """
        return status in cls.in_progress_states()


class ProcessHandle(BaseModel):
    """Contract used to identify processes created by any launcher."""

    pid: str
    """The process identifier."""
    name: str
    """The name of the process."""
    run_id: str
    """The run-id the process was run under."""
    launcher_name: str = ""
    """The launcher used to launch the process."""
    status: Status = Status.Unsubmitted
    """The current status of the task."""

    @property
    def safe_name(self) -> str:
        """Return a path-safe version of the name."""
        return slugify(self.name)

    model_config: t.ClassVar[ConfigDict] = ConfigDict(extra="allow")


_THandle = t.TypeVar("_THandle", bound=ProcessHandle)


class LiveStep(Step):
    """A Step enriched with runtime metadata."""

    parent: t.Self | None = Field(default=None, exclude=True, frozen=True)
    """The step for which this step is a sub-task."""
    working_dir: Path | None = Field(default=None, frozen=True)
    """The root directory where this step can write outputs."""
    kind: str = Field(default="", frozen=True, init=False)
    """The instance discriminator."""

    @staticmethod
    def get_root_fsm() -> JobFileSystemManager:
        """Return the default, run-aware job file-system manager.

        Returns
        -------
        JobFileSystemManager
        """
        root_dir = DirectoryManager.data_home()
        run_dir = root_dir
        if run_id := os.environ.get(ENV_CSTAR_RUNID, ""):
            run_dir = root_dir / run_id
        return JobFileSystemManager(run_dir)

    @property
    def fsm(self) -> JobFileSystemManager:
        """Retrieve a file system manager rooted on the step working directory.

        Returns
        -------
        JobFileSystemManager
        """
        if not self.working_dir:
            raise RuntimeError
        return JobFileSystemManager(self.working_dir)

    @property
    def blueprint(self) -> Blueprint:
        """Load and return the blueprint associated with this step."""
        base_bp = deserialize(self.blueprint_path, Blueprint)
        app: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
            get_application(base_bp.application)
        )
        bp_type = app.blueprint
        return bp_type(**base_bp.model_dump())

    @property
    def command(self) -> str:
        """Generate the shell command that will execute the underlying application.

        Returns
        -------
        str
        """
        return convert_step_to_blueprint_run_command(self)

    @property
    def script_path(self) -> Path:
        """Return the path to the the log file written by this task."""
        return self.fsm.root_dir / "script.sh"

    @property
    def log_path(self) -> Path:
        """Return the path to the the log file written by this task."""
        return self.fsm.logs_dir / f"{self.safe_name}.out"

    @classmethod
    def from_step(
        cls,
        step: Step,
        update: Mapping[str, t.Any] | None = None,
    ) -> "LiveStep":
        """Convert a step from orchestration planning into a LiveStep.

        Parameters
        ----------
        step : Step
            The step to convert
        parent : LiveStep | Step | None (default: None)
            The parent of the converted step
        update : Mapping[str, t.Any]
            A mapping of updates to apply to the step
        """
        attributes = step.model_dump(by_alias=True)
        update = update or {}

        if "name" in update:
            attributes.pop("working_dir", None)

        attributes.update(update)
        return LiveStep(**attributes)

    @model_validator(mode="before")
    @classmethod
    def _model_validator_pre(cls, data: t.Any) -> "t.Any":
        """Ensure the working directory and fsm are computed.

        Returns
        -------
        LiveStep
        """
        name = str(data.get("name", ""))
        wd = t.cast("Path | str | None", data.get("working_dir", None))
        parent = t.cast("Step | None", data.get("parent", None))
        data["kind"] = "live"

        if parent and not isinstance(parent, LiveStep):
            parent = LiveStep(**parent.model_dump(by_alias=True))

        if parent:
            fsm = parent.fsm.get_subtask_manager(name)
            wd = fsm.root_dir
            data["parent"] = parent

        data["blueprint"] = data.get("blueprint")

        if wd:
            data["working_dir"] = wd
            fsm = JobFileSystemManager(Path(wd))
        else:
            fsm = LiveStep.get_root_fsm().get_subtask_manager(name)
            data["working_dir"] = fsm.root_dir

        return data

    @model_validator(mode="after")
    def _model_validator_post(self) -> "LiveStep":
        """Ensure string paths are converted to Path.

        Returns
        -------
        LiveStep
        """
        object.__setattr__(
            self, "working_dir", Path(self.working_dir) if self.working_dir else None
        )
        object.__setattr__(self, "blueprint_path", Path(self.blueprint_path))
        return self

    @t.override
    @classmethod
    def discriminator(cls) -> str:
        """Return the sub-type discriminator."""
        return "live"


class Task(BaseModel, t.Generic[_THandle]):
    """A task represents a live-execution of a step."""

    step: LiveStep
    """The step containing task configuration."""

    handle: _THandle
    """The unique process identifier for the task."""

    @property
    def status(self) -> Status:
        """Return the current status of the task."""
        return self.handle.status

    @status.setter
    def status(self, value: Status) -> None:
        """Update the status of the task."""
        self.handle.status = value


class Planner(LoggingMixin):
    """Identifies depdendencies of a workplan to produce an execution plan."""

    workplan: Workplan
    """The workplan to plan."""

    graph: "DiGraph[str]"
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
    def _workplan_to_graph(cls, workplan: Workplan) -> "DiGraph[str]":
        """Convert a workplan into a graph for planning.

        Parameters
        ----------
        workplan: Workplan
            The workplan to be converted.

        Returns
        -------
        DiGraph
            A graph of the execution plan.
        """
        data: Mapping[str, list[str]] = {s.name: [] for s in workplan.steps}
        for step in workplan.steps:
            for prereq in step.depends_on:
                data[prereq].append(step.name)

        g = nx.DiGraph(data)
        defaults: dict[str, dict[str, Status | Step | None]] = {
            str(n.name): {
                KEY_STATUS: Status.Unsubmitted,
                KEY_STEP: LiveStep.from_step(n),
                KEY_TASK: None,
            }
            for n in workplan.steps
        }
        nx.set_node_attributes(g, values=defaults)
        return g

    def flatten(self) -> Sequence[Step]:
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
        return tuple(steps[k] for k in keys)

    @t.overload
    def store(self, n: str, key: t.Literal["status"], value: Status) -> None: ...

    @t.overload
    def store(self, n: str, key: t.Literal["step"], value: LiveStep) -> None: ...

    @t.overload
    def store(
        self, n: str, key: t.Literal["task"], value: Task[ProcessHandle]
    ) -> None: ...

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
            self.log.trace(msg)

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
        default: LiveStep | None = None,
    ) -> LiveStep | None: ...

    @t.overload
    def retrieve(
        self,
        n: str,
        key: t.Literal["task"],
        default: Task[ProcessHandle] | None = None,
    ) -> Task[ProcessHandle] | None: ...

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
        filter_fn: Callable[[Status], bool] | None = None,
    ) -> Mapping[str, Status]: ...

    @t.overload
    def retrieve_all(
        self,
        key: t.Literal["step"],
        default: Step | None = None,
        filter_fn: Callable[[Step], bool] | None = None,
    ) -> Mapping[str, Step]: ...

    @t.overload
    def retrieve_all(
        self,
        key: t.Literal["task"],
        default: Task[ProcessHandle] | None = None,
        filter_fn: Callable[[Task[ProcessHandle]], bool] | None = None,
    ) -> Mapping[str, Task[ProcessHandle]]: ...

    def retrieve_all(
        self,
        key: str,
        default: t.Any | None = None,
        filter_fn: Callable[[t.Any], bool] | None = None,
    ) -> Mapping[str, t.Any]:
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
    def check_preconditions(cls) -> None:
        """Perform launcher-specific startup validation.

        Raises
        ------
        CstarExpectationFailed
            If an environment variable required by the launcher cannot be found.
        """
        if klass := cstar_sysmgr.environment.settings_klass:
            try:
                _ = klass()
            except ValidationError as ex:
                prefixed = ", ".join(
                    get_envfield_alias(klass, str(x["loc"][0])) for x in ex.errors()
                )
                msg = f"Unable to load environment variables required by the system: {prefixed}"
                raise CstarExpectationFailed(msg) from ex

    @classmethod
    async def launch(
        cls,
        step: LiveStep,
        dependencies: list[_THandle],
    ) -> Task[_THandle]:
        """Launch a process for a step.

        Parameters
        ----------
        step : LiveStep
            The step to launch

        Returns
        -------
        Task[_THandle]
            The newly launched task.
        """
        ...

    @classmethod
    async def query_status(
        cls,
        item: Task[_THandle] | _THandle,
    ) -> Status:
        """Retrieve the current status for a running task.

        Parameters
        ----------
        item : Task[_THandle] or _THandle
            A task or process handle to query for status updates.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...

    @classmethod
    async def update_status(
        cls,
        item: Task[_THandle] | _THandle,
    ) -> tuple[bool, _THandle]:
        """Query and update the status for a running task.

        Parameters
        ----------
        item : Task[_THandle] or _THandle
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
        item : Task[_THandle]
            A task to cancel.

        Returns
        -------
        Status
            The current status of the item.
        """
        ...

    @classmethod
    def handle_klass(cls) -> type[_THandle]:
        """Return the type used by the launcher instance for managing tasks."""
        ...


class Orchestrator(LoggingMixin):
    """Manage the execution of a `Workplan`."""

    planner: Planner
    """The planner used by the orchestrator to prioritize tasks."""

    launcher: Launcher[t.Any]
    """The launcher used by the orchestrator to manage task execution."""

    _on_status_changed: Callable[[ProcessHandle], Awaitable[None]] | None = None
    """A callback to be executed when the orchestrator detects a status change."""

    _on_launched: Callable[[ProcessHandle], Awaitable[None]] | None = None
    """A callback to be executed when the orchestrator launches a task."""

    def __init__(self, planner: Planner, launcher: Launcher[t.Any]) -> None:
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

    def get_open_nodes(self, *, mode: RunMode) -> Mapping[str, Status] | None:
        """Retrieve the set of task nodes with a non-terminal state that are
        executing or ready to execute.

        Returns
        -------
        dict[str] | None
            - Set of open nodes ready for some processing actions.
            - An empty set indicates no actions are currently possible.
            - Null indicates all nodes are closed (traversal is complete).
        """
        g = self.planner.graph
        open_nodes: dict[str, Status] = {}
        closed_set = self.get_closed_nodes(mode=mode)

        if self.planner.workplan:
            nodes = {s.name for s in self.planner.workplan.steps}
        else:
            nodes = {n for n in self.planner.graph}

        working_list = set(nodes).difference(closed_set.keys())

        if failures := {u: s for u, s in closed_set.items() if Status.is_failure(s)}:
            self.log.error(f"Exiting due to task failures: {failures}")
            return None

        for n in working_list:
            in_edges = list(g.in_edges(n))
            in_degree = g.in_degree(n)

            satisfied = all(
                (
                    Status.is_in_progress(g.nodes[u][KEY_STATUS])
                    or Status.is_terminal(g.nodes[u][KEY_STATUS])
                    if mode == RunMode.Schedule
                    else Status.is_terminal(g.nodes[u][KEY_STATUS])
                )
                for (u, _) in in_edges
            )

            if in_degree == 0:
                open_nodes[n] = Status.Unsubmitted
            elif satisfied:
                open_nodes[n] = g.nodes[n][KEY_STATUS]

        if working_list:
            # working list has options. if none are ready, return empty set.
            return open_nodes

        return None

    def get_closed_nodes(self, *, mode: RunMode) -> Mapping[str, Status]:
        """Retrieve the set of task nodes with a terminal state.

        Returns
        -------
        set of str
            A set of node IDs identifying nodes with a Done status.
        """
        targets = Status.terminal_states()

        if mode == RunMode.Schedule:
            # anything previously scheduled is "closed" when scheduling
            targets.update({Status.Submitted, Status.Running, Status.Ending})

        return self.planner.retrieve_all(KEY_STATUS, filter_fn=lambda x: x in targets)

    def _locate_dependencies(self, step: LiveStep) -> list[ProcessHandle] | None:
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

    async def process_node(self, node: str) -> Task[ProcessHandle] | None:
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
            old_status = task.status
            new_status = await self.launcher.query_status(task)
            if old_status != new_status:
                task.status = new_status

                if self._on_status_changed:
                    await self._on_status_changed(task.handle)
        else:
            task = await self.launcher.launch(step, dependencies)
            self.planner.store(node, KEY_TASK, task)
            self.log.info(f"Launched step: {step.name}")

            if self._on_launched:
                await self._on_launched(task.handle)

        self.planner.store(node, KEY_STATUS, task.status)
        return task

    async def update_planner_state(
        self, n: str, task: Task[ProcessHandle] | None
    ) -> None:
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

        self.planner.store(n, KEY_STATUS, Status.Done)

        if task.status == Status.Done:
            self.log.info(f"Closed node: {n}")
        elif Status.is_failure(task.status):
            self.log.warning(f"Failed node: {n!r}, status: {task.status.name!r}")
            raise CstarExpectationFailed(f"Node {n} task failed.")

    async def run(self, mode: RunMode) -> Mapping[str, Status]:
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
        exec_tasks = [asyncio.Task(self.process_node(n)) for n in open_set.keys()]
        exec_results = await asyncio.gather(*exec_tasks)

        kvp = dict(zip(open_set.keys(), exec_results))
        postproc_tasks = [
            asyncio.Task(self.update_planner_state(n, t)) for n, t in kvp.items()
        ]
        cancellations: list[Task[ProcessHandle]] = list()

        try:
            await asyncio.gather(*postproc_tasks)
        except CstarExpectationFailed:
            cancellations = [
                v for v in kvp.values() if v and Status.is_in_progress(v.status)
            ]
            self.log.exception("A task has failed unexpectedly")

        await self._cancel(cancellations)

        return {k: v.status if v else Status.Unsubmitted for k, v in kvp.items()}

    async def _cancel(self, cancellations: Iterable[Task[ProcessHandle]]) -> None:
        """Request the cancellation of running tasks.

        Parameters
        ----------
        cancellations : Iterable[Task]
            The tasks to be cancelled.
        """
        if not cancellations:
            return

        cancel_coros = [self.launcher.cancel(task) for task in cancellations]
        results = await asyncio.gather(*cancel_coros, return_exceptions=True)

        for task, coro_result in zip(cancellations, results, strict=True):
            if isinstance(coro_result, BaseException):
                msg = f"An error occurred while cancelling `{task.step.name}`: {coro_result}"
                self.log.error(msg)
            else:
                msg = f"The orchestrator requested cancellation of: {task.step.name}"
                self.log.warning(msg)
                self.planner.store(task.step.name, KEY_STATUS, task.status)

    def set_callback(
        self,
        event: t.Literal["status_changed", "launched"],
        func: Callable[[_THandle], Awaitable[None]],
    ) -> None:
        match event:
            case "status_changed":
                attr_name = "_on_status_changed"
            case "launched":
                attr_name = "_on_launched"
            case _:
                msg = f"Invalid launcher callback event specified: {event}"
                raise ValueError(msg)

        setattr(self, attr_name, func)


def check_environment() -> None:
    """Verify the environment is configured correctly.

    Raises
    ------
    ValueError
        If required environment variables are missing or empty.
    """
    required_vars: set[str] = {ENV_CSTAR_RUNID}

    for key in required_vars:
        if not os.getenv(key, ""):
            msg = f"Missing required environment variable: {key}"
            raise ValueError(msg)


def configure_environment(
    output_dir: Path | None = None,
    run_id: str | None = None,
    updates: Mapping[str, str] | None = None,
) -> None:
    """Configure environment variables required by the runner.

    Parameters
    ----------
    output_dir : Path | None
        The directory where outputs will be written.
    run_id : str | None
        The unique identifier for an execution of the workplan.
    updates : Mapping[str, str] | None
        Additional environment variable updates.
    """
    if output_dir:
        os.environ[ENV_CSTAR_DATA_HOME] = output_dir.expanduser().resolve().as_posix()

    if run_id:
        os.environ[ENV_CSTAR_RUNID] = slugify(run_id)

    if updates:
        os.environ.update(updates)


register_representer(Status, intenum_representer)
