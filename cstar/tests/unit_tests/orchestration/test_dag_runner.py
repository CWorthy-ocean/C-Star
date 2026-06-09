import os
import random
from collections.abc import AsyncGenerator
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from cstar.applications.hello_world_app import HelloWorldBlueprint
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.execution.file_system import DirectoryManager, JobFileSystemManager
from cstar.orchestration.dag_runner import get_status_detail_map, load_run_state
from cstar.orchestration.launch.local import LocalHandle, LocalLauncher
from cstar.orchestration.models import BlueprintState, Step, Workplan, WorkplanState
from cstar.orchestration.orchestration import Planner, Status
from cstar.orchestration.serialization import serialize
from cstar.orchestration.state import put_sentinel
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun


def draw_graph(planner: Planner) -> None:
    import matplotlib.pyplot as plt
    import networkx as nx

    plt.cla()
    plt.clf()
    pos = nx.circular_layout(planner.graph)
    nx.draw_networkx(planner.graph, pos, with_labels=True)
    plt.savefig("g.png", bbox_inches="tight", dpi=500)


@pytest.fixture
async def layered_workplan(
    tmp_path: Path,
    mock_data_dir: Path,
    mock_state_dir: Path,
) -> AsyncGenerator[tuple[Workplan, dict[str, LocalHandle]]]:
    """Create a layered workplan with the structure:
    0
    | \
    1  2
       | \
       3  4
           \
            5
    """
    fake_run_id = "fake-run-id"
    app_name = "hello_world"
    schema = "1.0.0"
    steps: list[Step] = []
    last_parent: str | None = None
    asset_path = tmp_path / "assets"
    handles: dict[str, LocalHandle] = {}

    with mock.patch.dict(os.environ, {ENV_CSTAR_RUNID: fake_run_id}):
        fsm_map = {"": JobFileSystemManager(mock_data_dir)}

        for idx in range(6):
            depends_on = []
            wd_path = asset_path / f"wd{idx}"
            wd_path.mkdir(parents=True)
            bp_path = wd_path / f"bp{idx}.yaml"
            bp_name = f"BP {idx}"
            step_name = f"Step {idx}"
            target = f"@{idx}"
            fsm_map[step_name] = JobFileSystemManager(
                DirectoryManager.data_home() / fake_run_id
            )

            bp = HelloWorldBlueprint(
                name=bp_name,
                description=bp_name,
                application=app_name,
                state=BlueprintState.Draft,
                schema_version=schema,
                working_dir=wd_path,
                target=target,
            )
            serialize(bp_path, bp)

            depends_on = [last_parent] if last_parent else []

            if idx % 2 == 0:
                last_parent = step_name

            step = Step(
                name=step_name,
                application=app_name,
                depends_on=depends_on,
                blueprint=bp_path,
            )

            parent_fsm = fsm_map[last_parent] if last_parent else fsm_map[""]
            step_fsm = parent_fsm.get_subtask_manager(step_name)
            step_fsm.prepare()
            log_path = step_fsm.logs_dir / f"{step.safe_name}.out"
            log_path.write_text(f"{step.name} message {idx}")

            handle = LocalHandle(
                pid=f"100{idx}",
                name=step.name,
                start_at=datetime.now(),
            )
            handles[step.name] = handle
            await put_sentinel(handle)

            steps.append(step)

        workplan = Workplan(
            name="test-wp-with-dependencies",
            description="A workplan with nested dependencies demonstrating dependency-based status",
            steps=steps,
            state=WorkplanState.Draft,
        )
        wp_path = asset_path / "workplan.yaml"
        serialize(wp_path, workplan)

        repo = TrackingRepository()
        wp_run = WorkplanRun(
            workplan_path=wp_path,
            trx_workplan_path=wp_path,
            output_path=tmp_path / "mock-output",
            run_id=fake_run_id,
        )
        await repo.put_workplan_run(wp_run)

        yield workplan, handles


@pytest.mark.parametrize(
    ("closed_indices", "open_indices"),
    [
        pytest.param([], ["all"], id="all cancelled"),
        pytest.param([], ["all"], id="all done"),
        pytest.param(["all"], [], id="all ending"),
        pytest.param([], ["all"], id="all failed"),
        pytest.param(["all"], [], id="all running"),
        pytest.param(["all"], [], id="all submitted"),
        pytest.param(["all"], [], id="all unsubmitted"),
        pytest.param([0, 2, 4], [1, 3, 5], id="critical path closed"),
        pytest.param([0], [1, 2, 3, 4, 5], id="one closed task"),
        pytest.param([0, 1], [2, 3, 4, 5], id="two closed tasks (0,1)"),
        pytest.param([0, 2], [1, 3, 4, 5], id="two closed tasks (0,2)"),
        pytest.param([0, 1, 2], [3, 4, 5], id="three closed tasks (0,1,2)"),
        pytest.param([0, 2, 3], [1, 4, 5], id="three closed tasks (0,2,3)"),
        pytest.param([0, 2, 4], [1, 3, 5], id="three closed tasks (0,2,4)"),
        pytest.param([0, 1, 2, 3], [4, 5], id="four closed tasks (0,1,2,3)"),
        pytest.param([0, 1, 2, 4], [3, 5], id="four closed tasks (0,1,2,4)"),
        pytest.param([0, 2, 3, 4], [1, 5], id="four closed tasks (0,2,3,4)"),
        pytest.param([0, 2, 4, 5], [1, 3], id="four closed tasks (0,2,4,5)"),
        pytest.param([0, 1, 2, 3, 4], [5], id="five closed tasks (0,1,2,3,4)"),
        pytest.param([0, 1, 2, 4, 5], [3], id="five closed tasks (0,1,2,4,5)"),
        pytest.param([0, 2, 3, 4, 5], [1], id="five closed tasks (0,1,2,3,4)"),
    ],
)
@pytest.mark.asyncio
async def test_dag_runner_load_run_state(
    open_indices: list[str],
    closed_indices: list[str],
    layered_workplan: tuple[Workplan, dict[str, LocalHandle]],
    mock_state_dir: Path,
) -> None:
    """Verify the status output matches expectations when all states are a single value."""
    workplan, handles = layered_workplan

    open_names = [f"Step {idx}" for idx in open_indices]
    closed_names = [f"Step {idx}" for idx in closed_indices]

    if "all" in open_indices:
        open_names = [f"Step {i}" for i in range(len(workplan.steps))]

    if "all" in closed_indices:
        closed_names = [f"Step {i}" for i in range(len(workplan.steps))]

    for handle in handles.values():
        if handle.name in open_names:
            handle.status = Status.Submitted
        if handle.name in closed_names:
            handle.status = Status.Done
        await put_sentinel(handle)

    launcher = LocalLauncher()
    run_id = os.getenv(ENV_CSTAR_RUNID) or ""

    dag_status = await load_run_state(run_id, launcher)

    # verify that state is loaded for every step
    open_items = list(dag_status.open_items)
    closed_items = list(dag_status.closed_items)

    exp_open = len(workplan.steps) if "all" in open_names else len(open_names)
    exp_closed = len(workplan.steps) if "all" in closed_names else len(closed_names)

    assert len(open_items) == exp_open
    assert len(closed_items) == exp_closed


@pytest.mark.parametrize(
    ("closed_indices", "open_indices"),
    [
        pytest.param([], ["all"], id="all cancelled"),
        pytest.param([], ["all"], id="all done"),
        pytest.param(["all"], [], id="all ending"),
        pytest.param([], ["all"], id="all failed"),
        pytest.param(["all"], [], id="all running"),
        pytest.param(["all"], [], id="all submitted"),
        pytest.param(["all"], [], id="all unsubmitted"),
        pytest.param([0, 2, 4], [1, 3, 5], id="critical path closed"),
        pytest.param([0], [1, 2, 3, 4, 5], id="one closed task"),
        pytest.param([0, 1], [2, 3, 4, 5], id="two closed tasks (0,1)"),
        pytest.param([0, 2], [1, 3, 4, 5], id="two closed tasks (0,2)"),
        pytest.param([0, 1, 2], [3, 4, 5], id="three closed tasks (0,1,2)"),
        pytest.param([0, 2, 3], [1, 4, 5], id="three closed tasks (0,2,3)"),
        pytest.param([0, 2, 4], [1, 3, 5], id="three closed tasks (0,2,4)"),
        pytest.param([0, 1, 2, 3], [4, 5], id="four closed tasks (0,1,2,3)"),
        pytest.param([0, 1, 2, 4], [3, 5], id="four closed tasks (0,1,2,4)"),
        pytest.param([0, 2, 3, 4], [1, 5], id="four closed tasks (0,2,3,4)"),
        pytest.param([0, 2, 4, 5], [1, 3], id="four closed tasks (0,2,4,5)"),
        pytest.param([0, 1, 2, 3, 4], [5], id="five closed tasks (0,1,2,3,4)"),
        pytest.param([0, 1, 2, 4, 5], [3], id="five closed tasks (0,1,2,4,5)"),
        pytest.param([0, 2, 3, 4, 5], [1], id="five closed tasks (0,1,2,3,4)"),
    ],
)
@pytest.mark.asyncio
async def test_dag_runner_get_status_detail_map(
    open_indices: list[str],
    closed_indices: list[str],
    layered_workplan: tuple[Workplan, dict[str, LocalHandle]],
    mock_state_dir: Path,
) -> None:
    """Verify the status output matches expectations when all states are a single value."""
    workplan, handles = layered_workplan
    deps = {step.name: step.depends_on for step in workplan.steps}

    open_names = [f"Step {idx}" for idx in open_indices]
    closed_names = [f"Step {idx}" for idx in closed_indices]

    if "all" in open_indices:
        open_names = [f"Step {i}" for i in range(len(workplan.steps))]

    if "all" in closed_indices:
        closed_names = [f"Step {i}" for i in range(len(workplan.steps))]

    for handle in handles.values():
        if handle.name in open_names:
            handle.status = Status.Submitted
        if handle.name in closed_names:
            handle.status = Status.Done
        await put_sentinel(handle)

    launcher = LocalLauncher()
    run_id = os.getenv(ENV_CSTAR_RUNID) or ""

    planner = Planner(workplan)
    # draw_graph(planner). # leave for quicker debugging

    dag_status = await load_run_state(run_id, launcher)
    detail_map = get_status_detail_map(planner, dag_status)
    ordered_names = list(detail_map.keys())

    # shuffle steps to ensure workplan order does not matter
    steps = list(workplan.steps)
    random.shuffle(steps)

    for step in steps:
        # slug = slugify(step.name)
        detail = detail_map[step.name]

        if step.name == "Step 0":
            # by the graph structure, we know the first step has no dependdencies
            continue

        # get the position of the step in the output
        slug_idx = ordered_names.index(step.name)
        # ... and the position of all tasks it depends on to start
        dep_indices = [ordered_names.index(d) for d in deps[step.name]]
        # then, confirm the step comes afer all of it's dependencies
        assert slug_idx > max(dep_indices)

        # confirm the step indicates it's waiting on a dependency if any are open
        if set(step.depends_on).intersection(open_names) and step.name in open_names:
            assert not detail.ready
        elif step.name in open_names:
            # it has no dependencies in the open set
            assert detail.ready
