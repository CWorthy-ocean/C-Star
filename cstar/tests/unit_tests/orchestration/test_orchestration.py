import typing as t
from collections.abc import Mapping
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.models import Application, Step, Workplan
from cstar.orchestration.orchestration import (
    KEY_STATUS,
    KEY_STEP,
    KEY_TASK,
    LiveStep,
    Orchestrator,
    Planner,
    ProcessHandle,
    Status,
    Task,
)
from cstar.tests.unit_tests.orchestration.conftest import (
    EXTERNAL_TOKEN,
    ExternalTaskFactory,
)

if t.TYPE_CHECKING:
    from cstar.applications.ucla_roms.models import UclaRomsBlueprint

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
            name="invalid-dependency-key-example",
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
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
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
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
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
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")

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
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
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


def _scripted_launcher(
    outcomes: Mapping[str, Status],
) -> tuple[mock.AsyncMock, mock.AsyncMock]:
    """Create `launch` and `cancel` doubles for a launcher.

    `launch` returns a task whose status is the outcome scripted for the step
    (Running by default); `cancel` marks the task it is given as Cancelled, as
    a launcher does when the cancellation is accepted.

    Parameters
    ----------
    outcomes : Mapping[str, Status]
        The status each named step reports when it is launched.
    """

    async def launch(
        step: LiveStep, dependencies: list[ProcessHandle], *, run_uuid: str
    ) -> Task[ProcessHandle]:
        handle = ProcessHandle(
            pid=f"job-{step.name}",
            name=step.name,
            run_id="r",
            status=outcomes.get(step.name, Status.Running),
        )
        return Task(step=step, handle=handle)

    async def cancel(item: Task[ProcessHandle]) -> Task[ProcessHandle]:
        item.status = Status.Cancelled
        return item

    return mock.AsyncMock(side_effect=launch), mock.AsyncMock(side_effect=cancel)


def _launched(launch: mock.AsyncMock) -> list[str]:
    """Return the names of the steps passed to a `launch` double, in call order."""
    return [c.args[0].name for c in launch.await_args_list]


async def test_run_cancels_in_progress_siblings_of_a_failed_step(
    diamond_workplan: Workplan,
) -> None:
    """Verify a step that fails at launch stops the plan: the steps launched
    beside it that are still in progress are cancelled and recorded as such,
    and nothing downstream is launched on later passes.
    """
    planner = Planner(diamond_workplan)
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
    launch, cancel = _scripted_launcher({"d-01": Status.Failed})

    with (
        mock.patch.object(LocalLauncher, "launch", launch),
        mock.patch.object(LocalLauncher, "cancel", cancel),
    ):
        first_pass = await orchestrator.run()
        second_pass = await orchestrator.run()

        assert first_pass == {"d-00": Status.Running}
        assert second_pass == {"d-01": Status.Failed, "d-02": Status.Cancelled}

        cancel.assert_awaited_once()
        assert cancel.await_args is not None
        assert cancel.await_args.args[0].step.name == "d-02"
        assert planner.retrieve("d-02", KEY_STATUS) == Status.Cancelled

        # the cancelled sibling ends the traversal
        assert orchestrator.get_open_nodes() is None
        await orchestrator.run()

    assert sorted(_launched(launch)) == ["d-00", "d-01", "d-02"]
    assert planner.retrieve("d-03", KEY_TASK) is None


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "A step that fails at launch with no in-progress sibling is stored as "
        "Done (the deliberate unconditional Done in update_planner_state), so "
        "nothing records the failure and the next pass launches its dependents."
    ),
)
async def test_run_stops_after_a_lone_failed_step(diamond_workplan: Workplan) -> None:
    """Verify a step that fails at launch stops the plan even when nothing
    else is running beside it to cancel.
    """
    planner = Planner(diamond_workplan)
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
    launch, cancel = _scripted_launcher({"d-00": Status.Failed})

    with (
        mock.patch.object(LocalLauncher, "launch", launch),
        mock.patch.object(LocalLauncher, "cancel", cancel),
    ):
        assert await orchestrator.run() == {"d-00": Status.Failed}
        await orchestrator.run()

    assert _launched(launch) == ["d-00"]
    cancel.assert_not_awaited()


async def test_run_waits_for_every_upstream_step(diamond_workplan: Workplan) -> None:
    """Verify a step with two upstream steps is not open while one of them is
    still unsubmitted, even though the other is running.
    """
    planner = Planner(diamond_workplan)
    orchestrator = Orchestrator(planner, LocalLauncher(), run_uuid="")
    for name, status in (("d-00", Status.Done), ("d-01", Status.Running)):
        step = planner.retrieve(name, KEY_STEP)
        assert step is not None
        handle = ProcessHandle(pid=f"job-{name}", name=name, run_id="r", status=status)
        planner.store(name, KEY_TASK, Task(step=step, handle=handle))
        planner.store(name, KEY_STATUS, status)

    assert orchestrator.get_open_nodes() == {"d-02": Status.Unsubmitted}

    launch, _ = _scripted_launcher({})
    with mock.patch.object(LocalLauncher, "launch", launch):
        assert await orchestrator.run() == {"d-02": Status.Running}

    assert _launched(launch) == ["d-02"]
    assert [d.pid for d in launch.await_args_list[0].args[1]] == ["job-d-00"]


def test_live_step_blueprint_anchors_relative_locations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bp_templates_dir: Path
) -> None:
    """Verify a step's blueprint resolves relative locations against the blueprint
    file's directory, so transforms that re-write it elsewhere keep them valid.
    """
    bp_dir = tmp_path / "blueprints"
    bp_dir.mkdir()
    bp_path = bp_dir / "bp.yaml"
    bp_path.write_text(
        (bp_templates_dir / "blueprint.yaml")
        .read_text()
        .replace("http://mockdoc.com/partitioning1.nc", "../data/boundary.nc")
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    step = LiveStep(
        name="step",
        application=Application.UCLA_ROMS.value,
        blueprint=bp_path,
        working_dir=tmp_path / "work",
    )

    blueprint = t.cast("UclaRomsBlueprint", step.blueprint)

    assert str(blueprint.forcing.boundary.data[0].location) == str(
        tmp_path / "data" / "boundary.nc"
    )
