import typing as t
from pathlib import Path

import networkx as nx
import pytest

from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.orchestration import (
    KEY_STATUS,
    KEY_STEP,
    Orchestrator,
    Planner,
    RunMode,
    Status,
)


@pytest.fixture
def diamond_graph(tmp_path: Path) -> nx.DiGraph:
    """Generate a prototype graph with a fan-out, fan-in pattern."""
    data: dict[str, t.Iterable[str]] = {"0": ["1", "2"], "1": ["3"], "2": ["3"]}
    g: nx.DiGraph = nx.DiGraph(data)
    bp_path = tmp_path / "blueprint.yaml"
    initial_stats = {
        key: {
            KEY_STEP: Step(
                name=f"s-{i:02d}", application="sleep", blueprint=bp_path.as_posix()
            ),
            KEY_STATUS: Status.Unsubmitted,
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
            KEY_STEP: Step(name=key, application="sleep", blueprint=bp_path.as_posix()),
            KEY_STATUS: Status.Unsubmitted,
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
    "mode",
    [
        RunMode.Schedule,
        RunMode.Monitor,
    ],
)
async def test_orchestrator_open_closed_lists(
    mode: RunMode, diamond_workplan: Workplan
) -> None:
    """Verify the orchestrator / dag runner loop over open/closed ends as-expected.

    The loop should move every item that is open into a closed state after running it.
    """
    orchestrator = Orchestrator(Planner(workplan=diamond_workplan), LocalLauncher())
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)

    assert open_set, "Orchestrator didn't identify any open nodes"
    encountered = set(open_set or [])

    while open_set is not None:
        print(f"[on-enter] Open nodes: {open_set}, Closed: {closed_set}")

        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

        print(f"[on-exit] Open nodes: {open_set}, Closed: {closed_set}")
        if open_set:
            encountered.update(open_set)

    assert closed_set, "The orchestrator failed to close tasks."
    assert encountered == set(closed_set), "The orchestrator didn't close all tasks"


def test_dep_keys(tmp_path: Path) -> None:
    """Verify the orchestrator fails gracefully when dependencies are
    mismatched to step names.
    """
    bp_path = tmp_path / "blueprint.yaml"
    bp_path.touch()

    with pytest.raises(ValueError) as ex:
        _ = Workplan(
            name="Invalid Dependency Key Example",
            description="Workplan with a dependency that doesn't match a step",
            steps=[
                Step(
                    name="Good Step",
                    application="sleep",
                    blueprint=bp_path.as_posix(),
                ),
                Step(
                    name="Bad Step",
                    application="sleep",
                    blueprint=bp_path.as_posix(),
                    depends_on=["Non-existent Step"],
                ),
            ],
        )

    assert "unknown dep" in str(ex).lower()
