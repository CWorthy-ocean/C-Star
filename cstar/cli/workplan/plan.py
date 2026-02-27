import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.base.utils import lazy_import, slugify
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import Planner
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    WorkplanTransformer,
)

nx = lazy_import("networkx")
plt = lazy_import("matplotlib.pyplot")

app = typer.Typer()

START_NODE: t.Literal["_cs_start_"] = "_cs_start_"
TERMINAL_NODE: t.Literal["_cs_term_"] = "_cs_term_"

if t.TYPE_CHECKING:
    from networkx import DiGraph

    from cstar.orchestration.models import Step


def _add_marker_nodes(graph: "DiGraph") -> "DiGraph":
    """Add node to serve as the entrypoint and exit point of the task graph.
    Parameters
    ----------
    graph : DiGraph
        The source graph.
    Returns
    -------
    DiGraph
        A copy of the original graph with the entrypoint node inserted
    """
    graph = t.cast("DiGraph", graph.copy())

    if START_NODE not in graph.nodes:
        graph.add_node(
            START_NODE,
            **{"action": "start"},
        )
    else:
        graph.nodes[START_NODE]["action"] = "start"

    if TERMINAL_NODE not in graph.nodes:
        graph.add_node(
            TERMINAL_NODE,
            **{"action": "term"},
        )
    else:
        graph.nodes[TERMINAL_NODE]["action"] = "term"

    # find steps with no dependencies, allowing immediate start
    no_dep_edges = [
        (START_NODE, node)
        for node in graph.nodes()
        if graph.in_degree(node) == 0 and node not in [START_NODE, TERMINAL_NODE]
    ]

    # Add edges from the  start node to all independent steps
    graph.add_edges_from(no_dep_edges)

    # find steps that have no tasks after them
    terminal_edges = [
        (node, TERMINAL_NODE)
        for node in graph.nodes()
        if graph.out_degree(node) == 0 and node != TERMINAL_NODE
    ]

    # Add edges from leaf nodes to the terminal node
    graph.add_edges_from(terminal_edges)

    return graph


def _initialize_from_graph(
    workplan: "Workplan", graph: "DiGraph"
) -> tuple["DiGraph", dict[str, "Step"], dict[str, list[str]], dict[str, str]]:
    """Prepare instance from the supplied graph."""
    step_map = {step.name: step for step in workplan.steps}
    dep_map = {
        step.name: [s.name for s in workplan.steps if step.name in s.depends_on]
        for step in workplan.steps
    }
    name_map = {step.name: step.name for step in workplan.steps}
    name_map.update({START_NODE: "start", TERMINAL_NODE: "end"})

    nx.set_node_attributes(graph, "task", "action")
    graph = _add_marker_nodes(graph)
    return graph, step_map, dep_map, name_map


def _create_color_map(
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
        "start": start_color,
        "term": term_color,
        "task": task_color,
    }


def _get_color_map(
    group_map: dict[str, str],
    graph: "DiGraph",
) -> tuple[str, ...]:
    """Return a pyplot-usable iterable containing per-node coloring.
    Paramters
    ---------
    group_map : dict[str, str]
        A mapping of node behavior names to colors
    graph : DiGraph
        The graph to create a color-map for
    Returns
    -------
    tuple[str, ...]
        tuple containing color strings
    """
    return tuple(
        group_map[node_data["action"]] for node, node_data in graph.nodes(data=True)
    )


async def render(
    planner: "Planner",
    image_directory: Path,
    layout: str = "circular",
    cmap: str = "",
) -> Path:
    """Render the graph to a file.

    Parameters
    ----------
    image_directory : Path
        The directory to render the file to
    layout : str
        The graph layout to apply
    cmap : str
        A color map to apply to nodes

    Returns
    -------
    Path
        The path to the output file
    """
    plt.figure(figsize=(11, 8))
    plt.cla()
    plt.clf()

    graph, *_, name_map = _initialize_from_graph(planner.workplan, planner.graph)
    color_map = _create_color_map()

    if START_NODE not in graph:
        msg = "Start node was not found. Graph will not be rendered."
        print(msg)
        return Path("not-found")

    if layout == "spring":
        pos = nx.spring_layout(graph)
    elif layout == "circular":
        pos = nx.circular_layout(graph)
    elif layout == "kamada":
        pos = nx.kamada_kawai_layout(graph)
    elif layout == "shell":
        pos = nx.shell_layout(graph)
    elif layout == "spiral":
        pos = nx.spiral_layout(graph)
    elif layout == "planar":
        pos = nx.planar_layout(graph)
    elif layout == "fruchterman":
        pos = nx.fruchterman_reingold_layout(graph)
    else:
        # WARNING: bfs_layout appears to require nx >= 3.5
        pos = nx.bfs_layout(graph, START_NODE)

    node_colors = _get_color_map(color_map, graph)
    nx.draw_networkx(
        graph,
        pos,
        with_labels=True,
        labels=name_map,
        node_size=2000,
        node_color=range(len(graph.nodes)) if cmap else node_colors,
        font_weight="bold",
        cmap=cmap if cmap else None,
    )

    plt.tight_layout(pad=2.0)

    write_to = image_directory / f"{slugify(planner.workplan.name).lower()}.png"
    plt.savefig(write_to, bbox_inches="tight", dpi=500)  # was "tight"

    return write_to


@app.command()
def plan(
    path: t.Annotated[
        Path,
        typer.Argument(help="Path to a blueprint file."),
    ],
    output_dir: t.Annotated[
        Path,
        typer.Argument(help="Path to a directory where the plan will be stored."),
    ],
    transform: t.Annotated[
        bool,
        typer.Option(
            help="Apply runtime transformations to the workplan before rendering."
        ),
    ] = False,
) -> None:
    """Review the execution plan generated by a workplan."""
    plan_path: Path | None = None

    try:
        workplan = deserialize(path, Workplan)
    except (FileNotFoundError, ValueError):
        print(f"The workplan at `{path}` could not be loaded")
        return

    try:
        if transform:
            transformer = WorkplanTransformer(workplan, RomsMarblTimeSplitter())
            workplan = transformer.apply()

        planner = Planner(workplan)
        plan_path = asyncio.run(render(planner, output_dir))

        if plan_path is None:
            raise ValueError("Unable to generate plan")

        print(f"The plan has been generated and stored at: {plan_path}")
    except ValueError as ex:
        print(f"An error occurred while generating the plan: {ex}")
