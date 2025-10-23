import typing as t
from pathlib import Path

import networkx as nx

from cstar.orchestration.models import Step, Workplan


class Planner(t.Protocol):
    workplan: Workplan
    """The workplan used to plan tasks."""

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

    def plan(self) -> list[Step]:
        """Return the complete plan."""
        ...


class GraphPlanner(Planner):
    """Convert workplans into task graphs and manage their execution."""

    graph: nx.DiGraph
    """The task graph to be executed."""

    step_map: dict[str, Step]
    """Maps the step slugs to step information."""

    dep_map: dict[str, t.Iterable[str]]
    """Maps the step slugs to the dependency slugs."""

    name_map: dict[str, str]
    """Maps the step slugs to original names."""

    START_NODE: t.ClassVar[t.Literal["_cstar_start_node"]] = "_cstar_start_node"
    """Fixed name for task graph entrypoint."""

    TERMINAL_NODE: t.ClassVar[t.Literal["_cstar_term_node"]] = "_cstar_term_node"
    """Fixed name for task graph termination."""

    control_nodes: t.ClassVar[set[str]] = {START_NODE, TERMINAL_NODE}
    """Return a set containing the control node names."""

    NODE_ACTION_KEY: t.ClassVar[t.Literal["action"]] = "action"
    """Fixed name for node attribute containing node behavior."""

    NODE_BEHAVIOR_TASK: t.ClassVar[t.Literal["task"]] = "task"
    NODE_BEHAVIOR_START: t.ClassVar[t.Literal["start"]] = "start"
    NODE_BEHAVIOR_TERM: t.ClassVar[t.Literal["term"]] = "term"

    def __init__(
        self,
        workplan: Workplan | None = None,
        graph: nx.DiGraph | None = None,
    ) -> None:
        if workplan:
            super().__init__(workplan)

        if workplan and not graph:
            self._initialize_from_plan(workplan)

        # note: the graph argument and `if graph` branch are a byproduct of
        #  testing and are not 100% desired. we may need it for loading from
        #  a serialized graph, though (e.g. if state is stored in the graph).
        if graph:
            self._initialize_from_graph(graph)

        self.color_map = GraphPlanner._create_color_map()

    def _initialize_from_plan(self, workplan: Workplan) -> None:
        """Prepare instance from the supplied workplan."""
        self.step_map = {step.name: step for step in workplan.steps}
        self.dep_map = {
            step.name: [s.name for s in workplan.steps if step.name in s.depends_on]
            for step in workplan.steps
        }
        self.name_map = {step.name: step.name for step in workplan.steps}
        self.name_map.update({self.START_NODE: "start", self.TERMINAL_NODE: "end"})

        self.graph = self._workplan_to_graph()

    def _initialize_from_graph(self, graph: nx.DiGraph) -> None:
        """Prepare instance from the supplied graph."""
        # note: the graph argument and `if graph` branch are a byproduct of
        #  testing and are not 100% desired. we may need it for loading from
        #  a serialized graph, though (e.g. if state is stored in the graph).
        p = Path("step")
        # TODO: ensure the task is serialized onto the nodes to use in place of this hack.
        p.touch()

        self.step_map = {
            n: Step(name=n, application="no-app", blueprint=p) for n in graph.nodes()
        }
        self.dep_map = {n: graph.out_edges(n) for n in graph.nodes()}
        self.name_map = {n: n for n in graph.nodes()}
        self.name_map.update({self.START_NODE: "start", self.TERMINAL_NODE: "end"})

        self.graph = GraphPlanner._add_marker_nodes(graph)

    @classmethod
    def _create_color_map(
        cls,
        start_color: str = "#00ff00a1",
        term_color: str = "#ff7300a1",
        task_color: str = "#377aaaa1",
    ) -> dict[str, str]:
        """Return a dictionary mapping node types to color.

        Parameters
        ----------
        start_color : str
            Color of the start node
        term_color : str
            Color of the terminal node
        task_color : str
            Color of the task nodes

        Returns
        -------
        dict[str, str]
        """
        return {
            GraphPlanner.NODE_BEHAVIOR_START: start_color,
            GraphPlanner.NODE_BEHAVIOR_TERM: term_color,
            GraphPlanner.NODE_BEHAVIOR_TASK: task_color,
        }

    # def __next__(self) -> Step | None:
    #     """Return the next available step.
    #
    #     If steps are blocked due to serial plan execution or dependencies, the
    #     currently executing step will be returned.
    #     """
    #     if self.workplan and self.workplan.steps:
    #         # TODO: this must return something other than the first node.
    #         return self.workplan.steps[0]
    #
    #     return None

    def __iter__(self) -> t.Iterator[Step]:
        """Return an iterator over the ordered steps."""
        plan = self.plan()
        return iter(plan)

    @classmethod
    def _add_marker_nodes(cls, graph: nx.DiGraph) -> nx.DiGraph:
        """Add node to serve as the entrypoint and exit point of the task graph.

        Parameters
        ----------
        graph : nx.DiGraph
            The source graph.

        Returns
        -------
        nx.DiGraph
            A copy of the original graph with the entrypoint node inserted
        """
        graph = t.cast("nx.DiGraph", graph.copy())

        if cls.START_NODE not in graph.nodes:
            graph.add_node(
                cls.START_NODE,
                **{cls.NODE_ACTION_KEY: cls.NODE_BEHAVIOR_START},
            )
        else:
            graph.nodes[cls.START_NODE][cls.NODE_ACTION_KEY] = cls.NODE_BEHAVIOR_START

        if cls.TERMINAL_NODE not in graph.nodes:
            graph.add_node(
                cls.TERMINAL_NODE,
                **{cls.NODE_ACTION_KEY: cls.NODE_BEHAVIOR_TERM},
            )
        else:
            graph.nodes[cls.TERMINAL_NODE][cls.NODE_ACTION_KEY] = cls.NODE_BEHAVIOR_TERM

        # find steps with no dependencies, allowing immediate start
        no_dep_edges = [
            (cls.START_NODE, node)
            for node in graph.nodes()
            if graph.in_degree(node) == 0 and node not in cls.control_nodes
        ]

        # Add edges from the  start node to all independent steps
        graph.add_edges_from(no_dep_edges)

        # find steps that have no tasks after them
        terminal_edges = [
            (node, cls.TERMINAL_NODE)
            for node in graph.nodes()
            if graph.out_degree(node) == 0 and node != cls.TERMINAL_NODE
        ]

        # Add edges from leaf nodes to the terminal node
        graph.add_edges_from(terminal_edges)

        return graph

    def _workplan_to_graph(self) -> nx.DiGraph:
        """Create a graph for the workplan with start and terminal nodes.

        Returns
        -------
        nx.DiGraph
            The newly created graph.
        """
        graph: nx.DiGraph = nx.DiGraph(self.dep_map, name=self.workplan.name)

        # label nodes created from the workplan steps as "tasks"
        nx.set_node_attributes(
            graph, GraphPlanner.NODE_BEHAVIOR_TASK, GraphPlanner.NODE_ACTION_KEY
        )
        return GraphPlanner._add_marker_nodes(graph)

    @classmethod
    def _get_color_map(
        cls,
        group_map: dict[str, str],
        graph: nx.DiGraph,
    ) -> tuple[str, ...]:
        """Return a pyplot-usable iterable containing per-node coloring.

        Paramters
        ---------
        group_map : dict[str, str]
            A mapping of node behavior names to colors
        graph : nx.DiGraph
            The graph to create a color-map for

        Returns
        -------
        tuple[str, ...]
            tuple containing color strings
        """
        return tuple(
            group_map[node_data[cls.NODE_ACTION_KEY]]
            for node, node_data in graph.nodes(data=True)
        )

    def plan(
        self,
        artifact_dir: Path | None = None,  # TODO: debug only? remove
    ) -> list[Step]:
        """Create a plan specifying the desired execution order of all tasks.

        Builds a task graph and flatten using a breadth-first traversal
        to ensure dependency order.

        Parameters
        ----------
        artifact_dir: Path | None
            Directory where planner outputs may be written
        """
        if not self.graph and GraphPlanner.START_NODE not in self.graph.nodes:
            return []

        sorted_nodes: list[str] = list(nx.topological_sort(self.graph))

        g_plan = nx.DiGraph(
            [
                (sorted_nodes[n], sorted_nodes[n + 1])
                for n in range(len(sorted_nodes) - 1)
            ],
        )
        nx.set_node_attributes(
            g_plan.subgraph(
                n for n in g_plan.nodes if n not in GraphPlanner.control_nodes
            ),
            GraphPlanner.NODE_BEHAVIOR_TASK,
            GraphPlanner.NODE_ACTION_KEY,
        )
        g_plan = self._add_marker_nodes(g_plan)
        print(f"Ordered plan: {sorted_nodes}")
        return [
            self.step_map[node]
            for node in sorted_nodes
            if node not in GraphPlanner.control_nodes
        ]


class MonitoredPlanner(GraphPlanner):
    """A planner that injects additional steps for monitoring asynchronous tasks."""

    original: nx.DiGraph
    """The original graph specifying the user-defined tasks."""

    NODE_BEHAVIOR_MONITOR: t.ClassVar[t.Literal["monitor"]] = "monitor"
    NAME_PREFIX: t.ClassVar[t.Literal["m-"]] = "m-"

    def __init__(
        self, workplan: Workplan | None = None, graph: nx.DiGraph | None = None
    ) -> None:
        """Initialize the planner.

        Parameters
        ----------
        workplan : Workplan
            The workplan to create a plan for
        graph : nx.DiGraph | None
            A pre-initialized graph to re-use in place of a fresh plan
        artifact_dir: Path | None
            Directory where planner outputs may be written
        """
        super().__init__(workplan, graph)

        graph = self.graph.copy()
        self.original = graph.copy()
        monitored_graph = self._augment(graph)
        self._initialize_from_graph(monitored_graph)
        self.color_map.update({MonitoredPlanner.NODE_BEHAVIOR_MONITOR: "#d0a04ea1"})

    @classmethod
    def derive_name(cls, node: str) -> str:
        """Create a unique node name based on the original node name.

        Parameters
        ----------
        node : str
            The name of the source/related node

        Returns
        -------
        str
        """
        return f"{cls.NAME_PREFIX}{node}"

    @classmethod
    def _augment(cls, og: nx.DiGraph) -> nx.DiGraph:
        """Modify the original graph to have a monitoring layer.

        Parameters
        ----------
        og : nx.DiGraph
            The original, un-augmented graph to use for generating a new graph

        Returns
        -------
        nx.DiGraph
            The augmented graph
        """
        tasks = og.copy()

        monitor_labels = {
            node: cls.derive_name(node)
            for node in og.nodes
            if node not in {GraphPlanner.START_NODE, GraphPlanner.TERMINAL_NODE}
        }

        monitors = nx.relabel_nodes(
            og.subgraph(monitor_labels), monitor_labels, copy=True
        )
        nx.set_node_attributes(
            monitors, cls.NODE_BEHAVIOR_MONITOR, GraphPlanner.NODE_ACTION_KEY
        )

        combo = nx.union(tasks, monitors)

        # require the job to be scheduled before the monitor can start
        monitor_edges = list(monitor_labels.items())
        combo.add_edges_from(monitor_edges)

        return combo
