import typing as t
from pathlib import Path

import networkx as nx
import pytest

from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.orchestration import Orchestrator, Planner, Status


@pytest.fixture
def diamond_graph(tmp_path: Path) -> nx.DiGraph:
    """Generate a prototype graph with a fan-out, fan-in pattern."""
    data: dict[str, t.Iterable[str]] = {"0": ["1", "2"], "1": ["3"], "2": ["3"]}
    g: nx.DiGraph = nx.DiGraph(data)
    bp_path = tmp_path / "blueprint.yaml"
    initial_stats = {
        key: {
            Planner.Keys.Step: Step(
                name=f"s-{i:02d}", application="sleep", blueprint=bp_path.as_posix()
            ),
            Planner.Keys.Status: Status.Unsubmitted,
        }
        for i, key in enumerate(g.nodes)
    }
    nx.set_node_attributes(g, initial_stats)
    return g


@pytest.fixture
def tree_graph(tmp_path: Path) -> nx.DiGraph:
    """Generate a prototype graph of a 3-layer, binary tree."""
    data: dict[str, t.Iterable[str]] = {
        "0": ["1", "2"],
        "1": ["3", "4"],
        "2": ["5", "6"],
    }
    bp_path = tmp_path / "blueprint.yaml"
    g: nx.DiGraph = nx.DiGraph(data)
    initial_stats = {
        key: {
            Planner.Keys.Step: Step(
                name=key, application="sleep", blueprint=bp_path.as_posix()
            ),
            Planner.Keys.Status: Status.Unsubmitted,
        }
        for key in g.nodes
    }
    nx.set_node_attributes(g, initial_stats)
    return g


@pytest.fixture
def diamond_workplan(tmp_path: Path) -> Workplan:
    """Generate a workplan."""
    bp_tpl_path = (
        Path(__file__).parent.parent.parent.parent
        / "additional_files/templates/bp"
        / "blueprint.yaml"
    )
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    return Workplan(
        name="diamond",
        description="A workplan with steps arranged in a diamond-shaped dependency graph.",
        steps=[
            Step(name="s-00", application="sleep", blueprint=bp_path.as_posix()),
            Step(
                name="s-01",
                depends_on=["s-00"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="s-02",
                depends_on=["s-00"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="s-03",
                depends_on=["s-01", "s-02"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "graph_shape",
    [
        "diamond_workplan",
    ],
)
async def test_query_using_attrs(
    request: pytest.FixtureRequest, graph_shape: str
) -> None:
    wp: Workplan = request.getfixturevalue(graph_shape)
    if wp is None:
        assert False, "Workplan fixture failed to load."

    mode = Orchestrator.RunMode.Schedule
    orchestrator = Orchestrator(Planner(workplan=wp), LocalLauncher())
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)

    while open_set is not None:
        print(f"[on-enter] Open nodes: {open_set}, Closed: {closed_set}")

        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

        print(f"[on-exit] Open nodes: {open_set}, Closed: {closed_set}")
