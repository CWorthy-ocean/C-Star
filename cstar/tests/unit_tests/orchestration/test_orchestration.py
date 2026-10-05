import os
import typing as t
import uuid
from datetime import datetime
from pathlib import Path
from unittest import mock

import networkx as nx
import pytest

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.base.env import ENV_CSTAR_RUNID, FLAG_ON
from cstar.base.feature import ENV_FF_ORCH_TRX_TIMESPLIT
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import Application, Step, Workplan
from cstar.orchestration.orchestration import (
    KEY_STATUS,
    KEY_STEP,
    KEY_TASK,
    Orchestrator,
    Planner,
    ProcessHandle,
    RunMode,
    Status,
    Task,
)
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.transforms import (
    SplitFrequency,
    WorkplanTransformer,
    get_time_slices,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_TRX_FREQ
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)

if t.TYPE_CHECKING:
    from collections.abc import Iterable

APP_NAME: t.Final[str] = "roms_marbl"


@pytest.fixture
def diamond_graph(tmp_path: Path) -> nx.DiGraph:
    """Generate a prototype graph with a fan-out, fan-in pattern."""
    data: dict[str, Iterable[str]] = {"0": ["1", "2"], "1": ["3"], "2": ["3"]}
    g: nx.DiGraph = nx.DiGraph(data)
    bp_path = tmp_path / "blueprint.yaml"
    initial_stats = {
        key: {
            KEY_STEP: Step(
                name=f"s-{i:02d}",
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
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
    data: dict[str, Iterable[str]] = {
        "0": ["1", "2"],
        "1": ["3", "4"],
        "2": ["5", "6"],
    }
    bp_path = tmp_path / "blueprint.yaml"
    g = nx.DiGraph(data)
    initial_stats: dict[str, dict[str, Step | Status]] = {
        key: {
            KEY_STEP: Step(
                name=key,
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
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
    default_working_dir = "working_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(default_working_dir, f"working_dir: {tmp_path}")
    bp_path.write_text(bp_content)

    return Workplan(
        name="diamond",
        description="A workplan with steps arranged in a diamond-shaped dependency graph.",
        steps=[
            Step(
                name="d-00",
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-01",
                depends_on=["d-00"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-02",
                depends_on=["d-00"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-03",
                depends_on=["d-01", "d-02"],
                application=APP_NAME,
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
    default_working_dir = "working_dir: ."

    bp_path = tmp_path / "blueprint.yaml"
    bp_content = bp_tpl_path.read_text()
    bp_content = bp_content.replace(default_working_dir, f"working_dir: {tmp_path}")
    bp_path.write_text(bp_content)

    return Workplan(
        name="diamond",
        description="A workplan with two nodes immediately executable, followed by.",
        steps=[
            Step(
                name="d-00-a",
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-00-b",
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-01",
                depends_on=["d-00-a"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-02",
                depends_on=["d-00-a"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-03",
                depends_on=["d-00-b"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
            Step(
                name="d-04",
                depends_on=["d-01", "d-02", "d-00-b"],
                application=APP_NAME,
                blueprint=bp_path.as_posix(),
            ),
        ],
    )


@pytest.mark.skip(reason="TODO: fix underlying issue with delay")
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


@pytest.mark.skip(reason="TODO: fix underlying issue with delay")
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

    with pytest.raises(ValueError, match=r".*Unknown dep.*"):
        _ = Workplan(
            name="Invalid Dependency Key Example",
            description="Workplan with a dependency that doesn't match a step",
            steps=[
                Step(
                    name="Good Step",
                    application=APP_NAME,
                    blueprint=bp_path.as_posix(),
                ),
                Step(
                    name="Bad Step",
                    application=APP_NAME,
                    blueprint=bp_path.as_posix(),
                    depends_on=["Non-existent Step"],
                ),
            ],
        )


def test_workplan_transformation(diamond_workplan: Workplan) -> None:
    """Verify that the workplan transformation applies appropriate transforms when enabled."""
    for step in diamond_workplan.steps:
        step.application = Application.ROMS_MARBL.value

    diamond_steps = {str(s.name) for s in diamond_workplan.steps}

    transformer = WorkplanTransformer(diamond_workplan)
    with (
        mock.patch.dict(
            os.environ,
            {
                ENV_CSTAR_RUNID: str(uuid.uuid4()),
                ENV_FF_ORCH_TRX_TIMESPLIT: FLAG_ON,
                ENV_CSTAR_ORCH_TRX_FREQ: SplitFrequency.Monthly.value,
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
        # confirm that the original dependencies were updated
        assert not diamond_steps.intersection(step.depends_on)

        assert step.blueprint_overrides is not None
        blueprint = deserialize(step.blueprint_path, RomsMarblBlueprint)

        # overrides will be transferred to the model after call to .apply
        assert "runtime_params" not in step.blueprint_overrides

        step_sd = blueprint.runtime_params.start_date
        step_ed = blueprint.runtime_params.end_date

        assert ((step_sd, step_ed)) in get_time_slices(sd, ed)


def test_planner_creates_external_nodes(
    external_workplan: Workplan, external_task: ExternalTaskFactory
) -> None:
    """Verify an external dependency becomes a step-less node carrying its task."""
    task = external_task(Status.Running)

    planner = Planner(external_workplan, {EXTERNAL_TOKEN: task})

    assert set(planner.graph.nodes) == {"first", "second", EXTERNAL_TOKEN}
    assert set(planner.graph.successors(EXTERNAL_TOKEN)) == {"first", "second"}
    assert planner.retrieve(EXTERNAL_TOKEN, KEY_STEP) is None
    assert planner.retrieve(EXTERNAL_TOKEN, KEY_TASK) is task
    assert planner.retrieve(EXTERNAL_TOKEN, KEY_STATUS) == Status.Running


def test_planner_flatten_excludes_external_nodes(
    external_workplan: Workplan, external_task: ExternalTaskFactory
) -> None:
    """Verify the planned steps are those of this workplan, in dependency order."""
    task = external_task(Status.Done)

    planner = Planner(external_workplan, {EXTERNAL_TOKEN: task})

    assert [s.name for s in planner.flatten()] == ["first", "second"]


def test_planner_tolerates_unresolved_external_step(
    external_workplan: Workplan,
) -> None:
    """Verify a dependency on an external step with no task becomes an
    Unsubmitted node, so its dependents are planned but never launched.
    """
    planner = Planner(external_workplan)
    orchestrator = Orchestrator(planner, LocalLauncher())
    first = planner.retrieve("first", KEY_STEP)
    assert first is not None

    assert planner.retrieve(EXTERNAL_TOKEN, KEY_STATUS) == Status.Unsubmitted
    assert planner.retrieve(EXTERNAL_TOKEN, KEY_TASK) is None
    assert [s.name for s in planner.flatten()] == ["first", "second"]
    assert orchestrator._locate_dependencies(first) is None


def test_planner_ignores_unused_external_tasks(
    diamond_workplan: Workplan, external_task: ExternalTaskFactory
) -> None:
    """Verify a task no step depends on does not become a node."""
    task = external_task(Status.Done)

    planner = Planner(diamond_workplan, {EXTERNAL_TOKEN: task})

    assert EXTERNAL_TOKEN not in planner.graph


@pytest.mark.parametrize(
    ("status", "included"),
    [
        pytest.param(Status.Done, False, id="done is omitted"),
        pytest.param(Status.Running, True, id="running is passed"),
        pytest.param(Status.Submitted, True, id="submitted is passed"),
    ],
)
def test_locate_dependencies_external(
    status: Status,
    included: bool,
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify only a finished external dependency is withheld from the launcher,
    while local dependency handles are passed unchanged.
    """
    task = external_task(status)
    planner = Planner(external_workplan, {EXTERNAL_TOKEN: task})
    orchestrator = Orchestrator(planner, LocalLauncher())
    first = planner.retrieve("first", KEY_STEP)
    second = planner.retrieve("second", KEY_STEP)
    assert first is not None
    assert second is not None

    assert orchestrator._locate_dependencies(first) == (
        [task.handle] if included else []
    )

    # a local dependency that has not started blocks the launch
    assert orchestrator._locate_dependencies(second) is None

    local: Task[ProcessHandle] = Task(
        step=first,
        handle=ProcessHandle(pid="1", name="first", run_id="r", status=Status.Running),
    )
    planner.store("first", KEY_TASK, local)
    expected = [local.handle, *([task.handle] if included else [])]

    assert orchestrator._locate_dependencies(second) == expected


@pytest.mark.parametrize(
    ("status", "mode", "opens"),
    [
        pytest.param(Status.Done, RunMode.Schedule, True, id="done, schedule"),
        pytest.param(Status.Running, RunMode.Schedule, True, id="running, schedule"),
        pytest.param(Status.Done, RunMode.Monitor, True, id="done, monitor"),
        pytest.param(Status.Running, RunMode.Monitor, False, id="running, monitor"),
    ],
)
def test_open_nodes_with_external_dependency(
    status: Status,
    mode: RunMode,
    opens: bool,
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify a step becomes open when its external dependency is terminal, or
    (when scheduling, where SLURM enforces ordering) in progress.
    """
    task = external_task(status)
    planner = Planner(external_workplan, {EXTERNAL_TOKEN: task})
    orchestrator = Orchestrator(planner, LocalLauncher())

    open_nodes = orchestrator.get_open_nodes(mode=mode)

    assert open_nodes is not None
    assert ("first" in open_nodes) is opens
    assert EXTERNAL_TOKEN not in open_nodes
