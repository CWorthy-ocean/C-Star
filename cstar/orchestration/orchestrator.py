import re
import time
import typing as t
from collections import defaultdict
from enum import IntEnum
from functools import singledispatchmethod
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
from pydantic import BaseModel, Field

from cstar.orchestration.models import Step, Workplan

# def slugify(value: str) -> str:
#     """Collapse and replace whitespace characters."""
#     stripped = value.strip()
#     return re.sub(r"\s+", "-", value.strip())


_T = t.TypeVar("_T")


class Slugged(t.Generic[_T]):
    value: _T
    """The wrapped entity."""

    attribute: str | None
    """The attribute name to convert to a slug.

    If no attribute is passed, the original item is used to generate the slug.
    """

    def __init__(self, other: _T, attribute: str | None = None) -> None:
        """Initialize the instance."""
        self.value = other
        self.attribute = attribute

    @property
    def slug(self) -> str:
        """Return a slug-ified version of the item."""
        original = getattr(self.value, self.attribute) if self.attribute else self.value
        return re.sub(r"\s+", "-", str(original).lower().strip())


class TaskStatus(IntEnum):
    """Execution statuses for a task."""

    Waiting = 0
    Ready = 100
    Active = 200
    Done = 300
    Failed = 400
    Unknown = 500


class Task(BaseModel):
    """Track execution status of a `Workplan` step."""

    step: Step
    status: TaskStatus = Field(default=TaskStatus.Waiting, init=False)

    @property
    def name(self) -> str:
        """Wrap the step name to simplify task management for an orchestrator.

        NOTE: why bother with a `Named` protocol now...
        """
        return self.step.name


# @dataclass
# class StatusCheck:
#     status: TaskStatus
#     task_id:


class Launcher:
    tasks: dict[str, Task]

    def __init__(self) -> None:
        """Initialize the launcher instance."""
        self.tasks = {}
        self.check_counts: dict[str, int] = defaultdict(lambda: 0)

    def launch(self, steps: list[Step]) -> list[Task]:
        tasks: list[Task] = []

        for step in steps:
            task = Task(step=step)
            tasks.append(task)

            # TODO: fake starting the task...
            task.status = t.cast("TaskStatus", TaskStatus.Active)
            self.tasks[task.step.name] = task

        return tasks

    def report(self, item: Step | Task) -> TaskStatus:
        """Report the current status of a step (or task).

        Parameters
        ----------
        item : Step or Task
            The `Step` or `Task` instance to report on.
        """
        # if task := self.tasks.get(item.name, None):
        #     return task.status

        # return TaskStatus.Unknown
        statuses = self.report_all([item])
        return statuses[0]

    def report_all(self, items: list[Step | Task]) -> list[TaskStatus]:
        """Report the current status of a step (or task).

        Parameters
        ----------
        item : Step or Task
            The `Step` or `Task` instance to report on.
        """
        # names = {t.name for t in items}
        statuses: list[TaskStatus] = [
            (
                (
                    self.tasks[task.name].status
                    if self.check_counts[task.name] < 4
                    else TaskStatus.Done
                )
                if task.name in self.tasks
                else TaskStatus.Unknown
            )
            for task in items
        ]

        # force everything to change state...
        for k in self.tasks:
            self.check_counts[k] += 1

        return statuses

    def update(self) -> None:
        """Retrieve the current status for all managed tasks and update the
        task instance where necessary.
        """
        check_on = [
            task
            for name, task in self.tasks.items()
            if self.tasks[name].status < TaskStatus.Done
        ]
        self._query_status(check_on)

    def _query_status(self, tasks: list[Task]) -> None:
        """Must override method for launcher implementations..."""
        # slurm might...
        # sacct -j job_id
        # statuses = self.parse(t.name) for t in tasks
        # map(t.update, statuses)


# class SlurmLauncher(Launcher):
#     """Manage tasks with the SLURM workload manager."""

#     def _query_status(self, tasks: list[Task]) -> None:
#         """Must override method for launcher implementations..."""
#         # slurm might...
#         # sacct -j job_id
#         # statuses = self.parse(t.name) for t in tasks
#         # map(t.update, statuses)


# class LocalLauncher(Launcher):
#     """Manage tasks running on local processes."""

#     def _query_status(self, tasks: list[Task]) -> None:
#         """Must override method for launcher implementations..."""
#         # slurm might...
#         # sacct -j job_id
#         # statuses = self.parse(t.name) for t in tasks
#         # map(t.update, statuses)


# class SuperfacLauncher(Launcher):
#     """Manage tasks with the Superfacility API."""

#     def _query_status(self, tasks: list[Task]) -> None:
#         """Must override method for launcher implementations..."""
#         # slurm might...
#         # sacct -j job_id
#         # statuses = self.parse(t.name) for t in tasks
#         # map(t.update, statuses)


class Planner(t.Protocol):
    workplan: Workplan
    """The workplan used to create the tasks."""

    def __init__(self, plan: Workplan) -> None:
        """Initialize the planner."""
        self.workplan = plan

    def __next__(self) -> Step | None:
        steps: tuple[Step, ...] = ()

        if steps:
            return steps[0]

        raise StopIteration

    def __iter__(self) -> t.Iterator[Step]:
        return iter(self.workplan.steps)

    # def active(self) -> tuple[Step, ...]:
    #     """Return all steps marked as active.

    #     WARNING: probably should be on the orchestrator...
    #     """

    def remove(self, step: Step) -> None:
        """Remove the step from the plan."""


class GraphPlanner(Planner):
    """Convert workplans into task graphs and manage their execution."""

    # workplan: Workplan
    # """The workplan used to create the tasks."""

    graph: nx.DiGraph
    """The task graph to be executed."""

    step_map: dict[str, Step]
    """Maps the step slugs to step information."""

    # note: mapping to slugs is... a tangent. i didn't want to use non-url-safe
    # names (in case of output paths, URIs, or something else - but I don't have
    # a concrete need at this moment. consider getting rid of the slugs and maps.
    dep_map: dict[str, t.Iterable[str]]
    """Maps the step slugs to the dependency slugs."""

    name_map: dict[str, str]
    """Maps the step slugs to original names."""

    START_NODE: t.ClassVar[t.Literal["_cstar_start_node"]] = "_cstar_start_node"
    """Fixed name for task graph entrypoint."""

    def __init__(
        self, workplan: Workplan | None = None, graph: nx.DiGraph | None = None
    ) -> None:
        if workplan:
            super().__init__(workplan)

        if workplan and not graph:
            # slugged = [Slugged(step, "name") for step in workplan.steps]
            # self.step_map = {step.slug: step.value for step in slugged}
            # step_names = {step.name for step in workplan.steps}
            # for step in workplan.steps:
            #     step_names.add()

            self.step_map = {step.name: step for step in workplan.steps}
            self.dep_map = {
                # step.slug: [Slugged(dep).slug for dep in step.value.depends_on]
                # for step in slugged
                step.name: step.depends_on
                for step in workplan.steps
            }
            # self.name_map = {step.slug: step.value.name for step in slugged}
            self.name_map = {step.name: step.name for step in workplan.steps}

            self.graph = self._plan_to_graph()

        # note: the graph argument and `if graph` branch are a byproduct of
        #  testing and are not 100% desired. we may need it for loading from
        #  a serialized graph, though (e.g. if state is stored in the graph).
        if graph:
            self.step_map = {n: n for n in graph.nodes()}
            self.dep_map = {n: graph.out_edges(n) for n in graph.nodes()}
            self.name_map = {n: n for n in graph.nodes()}
            self.name_map[self.START_NODE] = "start"

            self._og = graph
            self.graph = GraphPlanner._add_start_node(graph)

    def __next__(self) -> Step | None:
        return None

    #     """Return an iterator that locates any `Step`'s ready for execution."""
    #     G = nx.DiGraph(self.graph)

    #     yield G.nodes[GraphPlanner.START_NODE]

    @classmethod
    def _add_start_node(cls, graph: nx.DiGraph) -> nx.DiGraph:
        """Add a single node to serve as the entrypoint to the task graph with edges
        to any nodes that have no dependencies.

        Parameters
        ----------
        graph : nx.DiGraph
            The source graph.

        Returns
        -------
        nx.DiGraph
            A copy of the original graph with the entrypoint node inserted
        """
        # find all steps with no dependencies, allowing immediate start
        graph = t.cast("nx.DiGraph", graph.copy())

        edges = [
            (cls.START_NODE, slug)
            for slug in graph.nodes()
            if graph.in_degree(slug) == 0
        ]

        # Add the start node with edges to all independent steps
        graph.add_edges_from(edges)

        return graph

    def _plan_to_graph(self) -> nx.DiGraph:
        # any node without a dependency can run immediately
        # start_edges = [
        #     (Orchestrator.START_NODE, slug) for slug, deps in self.dep_map if not deps
        # ]

        graph: nx.DiGraph = nx.DiGraph(self.dep_map, name=self.workplan.name)

        # find all steps with no dependencies, allowing immediate start
        start_edges = [
            (GraphPlanner.START_NODE, slug)
            for slug in graph.nodes()
            if graph.in_degree(slug) == 0
        ]

        # Add the start node with edges to all independent steps
        graph.add_edges_from(start_edges)

        return graph

    def _color_nodes(
        self,
        start_color: str = "#00ff00",
        term_color: str = "#ff0000",
        default_color: str = "#1f78b4",
    ) -> None:
        """Set an attribute on graph nodes to specify their color when rendered."""
        if "color" in self.graph.nodes[GraphPlanner.START_NODE]:
            return

        terminal_nodes = [n for n in self.graph if self.graph.out_degree(n) == 0]
        for n in terminal_nodes:
            self.graph.nodes[n]["color"] = term_color

        uncolored = (
            value for _, value in self.graph.nodes.data() if "color" not in value
        )
        for node_data in uncolored:
            node_data["color"] = default_color

        self.graph.nodes[GraphPlanner.START_NODE]["color"] = start_color

    def render(self, image_directory: Path) -> Path:
        """Render the graph to a file."""
        plt.cla()
        plt.clf()

        # WARNING: bfs_layout appears to require nx >= 3.5
        pos = nx.bfs_layout(self.graph, self.START_NODE)

        # Review available graph layout styles
        # pos = nx.kamada_kawai_layout(self.graph)  # pretty good
        # pos = nx.spring_layout(self.graph) # barf? adjust weight?
        # pos = nx.shell_layout(self.graph) # not bad
        # pos = nx.circular_layout(self.graph) # not bad
        # pos = nx.planar_layout(self.graph, scale=-1)  # meh
        # pos = nx.spiral_layout(self.graph) # weird
        # pos = nx.random_layout(self.graph)  # trash
        # pos = nx.spectral_layout(self.graph) # bad
        # pos = nx.multipartite_layout(self.graph, "color") # breaks
        # pos = nx.fruchterman_reingold_layout(self.graph) # nope
        # consider moving graph render into another componentdl
        self._color_nodes()

        node_colors = [
            node_data["color"] for _, node_data in self.graph.nodes(data=True)
        ]

        nx.draw(
            self.graph,
            node_color=node_colors,
            pos=pos,
            with_labels=False,
            node_size=2000,
            edgecolors="#000000",
        )
        nx.draw_networkx_labels(
            self.graph,
            pos,
            self.name_map,
            clip_on=False,
            font_size=8,
        )

        write_to = image_directory / f"{Slugged(self.workplan.name).slug}.png"
        plt.savefig(write_to, bbox_inches="tight", dpi=300)

        return write_to

    def remove(self, step: Step) -> None:
        # slug = Slugged(step).slug
        # self.graph.remove_node(slug)
        self.graph.remove_node(step.name)


class SerialPlanner(Planner):
    """Plan a serialized path through the tasks in a Workplan."""

    plan: list[Step]

    def __init__(
        self,
        workplan: Workplan,
        graph: nx.DiGraph | None = None,
        artifact_dir: Path | None = None,
    ) -> None:
        """Prepare the execution plan"""
        super().__init__(workplan)
        self.plan = SerialPlanner._create_plan(
            workplan,
            graph,
            artifact_dir=artifact_dir,
        )

    @classmethod
    def _create_plan(
        cls,
        plan: Workplan,
        graph: nx.DiGraph | None = None,
        artifact_dir: Path | None = None,  # TODO: debug only? remove
    ) -> list[Step]:
        """Build a task graph and flatten using a breadth-first traversal
        to ensure dependency order.
        """
        planner = GraphPlanner(plan, graph=graph)
        planner.render(artifact_dir or Path())
        edges = list(
            nx.bfs_edges(
                planner.graph,
                GraphPlanner.START_NODE,
                sort_neighbors=sorted,
            ),
        )

        # note: if i convert this node-name list to a new graph with edges, i can
        # render the serial plan as a graph just like the graph planner...
        return [planner.step_map[edge[1]] for edge in edges]

    def remove(self, step: Step) -> None:
        index = self.plan.index(step)

        if index == -1:
            # TODO: consider just warning with logger?
            msg = "Step not found in planner"
            raise ValueError(msg)

        if -1 < index < len(self.plan):
            # TODO: log this as a warning if out of range.
            self.plan.pop(index)

    def __next__(self) -> Step | None:
        """Return the next available step.

        If steps are blocked due to serial plan execution or dependencies, the
        currently executing step will be returned.
        """
        if self.plan:
            return self.plan[0]

        return None

    def __iter__(self) -> t.Iterator[Step]:
        """Return an iterator over the planner's steps."""
        return iter(self.plan)


class Orchestrator:
    """Manage the execution of a planned set of tasks."""

    SLEEP_DURATION: t.ClassVar[float] = 10

    planner: Planner
    """The planner determining the order of step execution."""

    launcher: Launcher
    """The launcher used to interact with executing tasks."""

    task_lookup: dict[str, Task]
    """Map for direct task retrieval"""

    task_archive: dict[str, Task]
    """Map for direct task retrieval of completed tasks."""

    def __init__(self, planner: Planner, launcher: Launcher) -> None:
        """Prepare the orchestrator to execute a plan."""
        self.planner = planner
        self.launcher = launcher
        self.task_lookup = {}
        self.task_archive = {}

    def _start(self, step: Step) -> Task:
        task, *_ = self.launcher.launch([step])

        if not task:
            msg = "Unable to launch task"
            raise RuntimeError(msg)

        self.task_lookup[step.name] = task
        # self.planner.remove(step)

        return task

    @singledispatchmethod
    def run_step(self, step: Step) -> TaskStatus:
        """Trigger execution of a step with the launcher."""
        if step.name not in self.task_lookup:
            # status = self.launcher.report(step)
            # if status == TaskStatus.Unknown:
            # launcher has seen this step before.
            task = self._start(step)
            # continue
            return task.status

        task = self.task_lookup[step.name]
        current_status = self.launcher.report(step)

        if task.status != current_status:
            print(f"Task status changed from {task.status} to {current_status}")
            # TODO (ankona, http://noop): publish an on-status-changed event...
            self.task_lookup[step.name].status = current_status

        if current_status > TaskStatus.Active:
            self.planner.remove(step)
            self.task_archive[task.step.name] = self.task_lookup.pop(task.step.name)

        return current_status

    @run_step.register(int)
    def _run_step_int(self, index: int) -> TaskStatus:
        steps = self.planner.workplan.steps

        if index > len(steps):
            msg = f"Step index is out of range. Index must be less than {len(steps)}"
            raise IndexError(msg)

        step = self.planner.workplan.steps[index]
        return self.run_step(step)

    def run(self) -> None:
        """Trigger execution of all steps in the plan."""
        while step := next(self.planner):
            self.run_step(step)
            time.sleep(Orchestrator.SLEEP_DURATION)
