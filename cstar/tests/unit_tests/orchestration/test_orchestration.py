import os
import typing as t
import uuid
from datetime import datetime
from pathlib import Path
from unittest import mock

import networkx as nx
import pytest

from cstar.base.env import FLAG_ON
from cstar.base.feature import (
    ENV_FF_ORCH_TRX_OVERRIDE,
    ENV_FF_ORCH_TRX_TIMESPLIT,
)
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import Application, RomsMarblBlueprint, Step, Workplan
from cstar.orchestration.orchestration import (
    KEY_STATUS,
    KEY_STEP,
    Orchestrator,
    Planner,
    RunMode,
    Status,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    WorkplanTransformer,
    get_time_slices,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_RUNID


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
    """Generate a prototype graph of a 3-layer, binary tree.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs

    Returns
    -------
    nx.DiGraph
    """
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
def diamond_workplan(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> Workplan:
    """Generate a workplan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files

    Returns
    -------
    Workplan
    """
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    default_output_dir = "output_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        default_output_dir, f"output_dir: {tmp_path.as_posix()}"
    )
    bp_path.write_text(bp_content)

    return Workplan(
        name="diamond",
        description="A workplan with steps arranged in a diamond-shaped dependency graph.",
        steps=[
            Step(
                name="d-00",
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-01",
                depends_on=["d-00"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-02",
                depends_on=["d-00"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-03",
                depends_on=["d-01", "d-02"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
        ],
    )


@pytest.fixture
def multi_entrypoint_workplan(
    tmp_path: Path,
    bp_templates_dir: Path,
) -> Workplan:
    """Generate a workplan with multiple tasks available to execute immediately.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    bp_templates_dir : Path
        Fixture returning the path to the directory containing blueprint template files

    Returns
    -------
    Workplan
    """
    bp_tpl_path = bp_templates_dir / "blueprint.yaml"
    default_output_dir = "output_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(
        default_output_dir, f"output_dir: {tmp_path.as_posix()}"
    )
    bp_path.write_text(bp_content)

    return Workplan(
        name="diamond",
        description="A workplan with two nodes immediately executable, followed by.",
        steps=[
            Step(
                name="d-00-a",
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-00-b",
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-01",
                depends_on=["d-00-a"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-02",
                depends_on=["d-00-a"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-03",
                depends_on=["d-00-b"],
                application="sleep",
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-04",
                depends_on=["d-01", "d-02", "d-00-b"],
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
        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

        if open_set:
            encountered.update(open_set)

    assert closed_set, "The orchestrator failed to close tasks."
    assert encountered == set(closed_set), "The orchestrator didn't close all tasks"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        RunMode.Schedule,
        RunMode.Monitor,
    ],
)
async def test_orchestrator_multi_entrypoint_open_closed_lists(
    mode: RunMode, multi_entrypoint_workplan: Workplan
) -> None:
    """Verify the orchestrator / dag runner loop over open/closed ends as-expected.

    This test uses a multi-entrypoint workplan to verify that the graph is traversed.
    """
    orchestrator = Orchestrator(
        Planner(workplan=multi_entrypoint_workplan), LocalLauncher()
    )
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)

    assert open_set, "Orchestrator didn't identify any open nodes"
    encountered = set(open_set or [])

    while open_set is not None:
        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

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


def test_workplan_transformation(diamond_workplan: Workplan) -> None:
    """Verify that the workplan transformation applies appropriate transforms when enabled."""
    for step in diamond_workplan.steps:
        step.application = Application.ROMS_MARBL.value

    transformer = WorkplanTransformer(diamond_workplan, RomsMarblTimeSplitter())
    with (
        mock.patch.dict(
            os.environ,
            {
                ENV_CSTAR_ORCH_RUNID: str(uuid.uuid4()),
                ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON,
                ENV_FF_ORCH_TRX_OVERRIDE: FLAG_ON,
            },
        ),
    ):
        transformed = transformer.apply()

    # start & end date in the blueprint.yaml file
    sd, ed = datetime(2020, 1, 1), datetime(2021, 1, 1)

    # start/end date cover 12 months. expect 4 steps per month.
    n_expected_steps = 4 * 12
    assert len(transformed.steps) == n_expected_steps, (
        f"Expected {n_expected_steps} steps, got {len(transformed.steps)}"
    )

    for step in transformed.steps:
        assert step.blueprint_overrides is not None
        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)

        # overrides will be transferred to the model after call to .apply
        assert "runtime_params" not in step.blueprint_overrides

        step_sd = blueprint.runtime_params.start_date
        step_ed = blueprint.runtime_params.end_date

        assert ((step_sd, step_ed)) in get_time_slices(sd, ed)
