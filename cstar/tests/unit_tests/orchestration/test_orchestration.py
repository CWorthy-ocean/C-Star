import os
import typing as t
import uuid
from datetime import datetime
from pathlib import Path
from unittest import mock

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

APP_NAME: t.Final[str] = "roms_marbl"


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
    ("status", "opens"),
    [
        pytest.param(Status.Done, True, id="done"),
        pytest.param(Status.Running, True, id="running"),
        pytest.param(Status.Unsubmitted, False, id="unsubmitted"),
    ],
)
def test_open_nodes_with_external_dependency(
    status: Status,
    opens: bool,
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify a step becomes open when its external dependency is terminal or
    in progress (the launcher enforces the ordering), but not before it is
    submitted.
    """
    task = external_task(status)
    planner = Planner(external_workplan, {EXTERNAL_TOKEN: task})
    orchestrator = Orchestrator(planner, LocalLauncher())

    open_nodes = orchestrator.get_open_nodes()

    assert open_nodes is not None
    assert ("first" in open_nodes) is opens
    assert EXTERNAL_TOKEN not in open_nodes


async def test_process_node_refuses_relaunch(
    external_workplan: Workplan,
    external_task: ExternalTaskFactory,
) -> None:
    """Verify a step that already holds a task is never launched again."""
    planner = Planner(external_workplan, {EXTERNAL_TOKEN: external_task(Status.Done)})
    orchestrator = Orchestrator(planner, LocalLauncher())
    first = planner.retrieve("first", KEY_STEP)
    assert first is not None

    prior: Task[ProcessHandle] = Task(
        step=first,
        handle=ProcessHandle(pid="1", name="first", run_id="r", status=Status.Failed),
    )
    planner.store("first", KEY_TASK, prior)

    with (
        mock.patch.object(LocalLauncher, "launch", mock.AsyncMock()) as launch,
        pytest.raises(RuntimeError, match="already launched"),
    ):
        await orchestrator.process_node("first")

    launch.assert_not_awaited()
