import argparse
import typing as t
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx

from cstar.cli.core import PathConverterAction, RegistryResult, cli_activity
from cstar.orchestration.models import Workplan
from cstar.orchestration.planning import GraphPlanner
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import slugify


def render(
    planner: GraphPlanner,
    image_directory: Path,
    layout: str = "circular",
    cmap: str = "",
) -> Path:
    """Render the graph to a file.

    Parameters
    ----------
    graph : nx.DiGraph
        The graph to render
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

    graph = planner.graph

    if GraphPlanner.START_NODE not in graph:
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
        pos = nx.bfs_layout(graph, GraphPlanner.START_NODE)

    node_colors = GraphPlanner._get_color_map(planner.color_map, graph)
    nx.draw_networkx(
        graph,
        pos,
        with_labels=True,
        labels=planner.name_map,
        node_size=2000,
        node_color=range(len(graph.nodes)) if cmap else node_colors,
        font_weight="bold",
        cmap=cmap if cmap else None,
    )

    plt.tight_layout(pad=2.0)

    write_to = image_directory / f"{slugify(planner.workplan.name).lower()}.png"
    plt.savefig(write_to, bbox_inches="tight", dpi=500)  # was "tight"

    return write_to


def handle(ns: argparse.Namespace) -> None:
    """Generate the execution plan for a workplan.

    Parameters
    ----------
    ns : argparse.Namespace
        User inputs parsed by the CLI
    """
    plan_path: Path | None = None

    try:
        if workplan := deserialize(ns.path, Workplan):
            planner = GraphPlanner(workplan)
            plan_path = render(
                planner,
                ns.output_dir,
            )
        else:
            print(f"The workplan at `{ns.path}` could not be loaded")

    except ValueError as ex:
        print(f"Error occurred: {ex}")

    if plan_path is None:
        raise ValueError("Unable to generate plan")

    print(f"The plan has been generated and stored at: {plan_path}")


@cli_activity
def create_action() -> RegistryResult:
    """Integrate the workplan-plan command into the CLI."""
    command: t.Literal["workplan"] = "workplan"
    action: t.Literal["plan"] = "plan"

    def _fn(sp: argparse._SubParsersAction) -> argparse._SubParsersAction:
        """Add a parser for the command: `cstar workplan plan -o ouput/directory`"""
        parser = sp.add_parser(
            action,
            help="Review the execution plan for the Workplan",
            description="Path to the workplan (YAML)",
        )
        parser.add_argument(
            dest="path",
            help="Path to the workplan (YAML)",
            action=PathConverterAction,
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            help="Directory to write plan outputs to.",
            default=Path.cwd(),
            dest="output_dir",
            action=PathConverterAction,
        )
        parser.set_defaults(handler=handle)
        return parser

    return (command, action), _fn
