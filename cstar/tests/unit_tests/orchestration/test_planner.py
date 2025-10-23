# ruff: noqa: S101

import typing as t
from pathlib import Path

import pytest

from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.planning import GraphPlanner, MonitoredPlanner, Planner


@pytest.fixture
def the_workplan(
    tmp_path: Path,
    load_workplan: t.Callable[[Path], Workplan],
    fill_workplan_template: t.Callable[[dict[str, t.Any]], str],
    complete_workplan_template_input: dict[str, t.Any],
) -> Workplan:
    """Create a valid workplan.

    Parameters
    ----------
    tmp_path : Path
    A temporary path to store test outputs
    """
    data = complete_workplan_template_input
    # var0, var1, var2 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())

    for i, step in enumerate(data.get("steps", [])):
        empty_bp_path = tmp_path / f"blueprint-{i:00}.yaml"
        empty_bp_path.touch()
        step["blueprint"] = empty_bp_path.as_posix()

    wp_yaml = fill_workplan_template(data)
    yaml_path = tmp_path / "workplan.yaml"
    yaml_path.write_text(wp_yaml)

    return load_workplan(yaml_path)


@pytest.mark.parametrize(
    "planner_type",
    [
        GraphPlanner,
        MonitoredPlanner,
    ],
)
def test_planner_no_tasks(
    planner_type: type[Planner],
    the_workplan: Workplan,
) -> None:
    """Verify that planners do not blow up when supplied with an empty plan.

    Use object.__setattr__ to simulate an unexpected change to the model validation.
    - purposefully modifying the value of a frozen attribute
    """
    object.__setattr__(the_workplan, "steps", [])
    planner = planner_type(the_workplan)

    plan = list(planner)
    assert len(plan) == 0


def get_graph_plan_size(plan: Workplan) -> int:
    """Compute the number of nodes expected in a graph planner."""
    return len(plan.steps)  # the planner should skips the start step


def get_serial_plan_size(plan: Workplan) -> int:
    """Compute the number of nodes expected in a serial planner."""
    return len(plan.steps)


def get_monitored_graph_plan_size(plan: Workplan) -> int:
    """Compute the number of nodes expected in a monitored graph planner."""
    return 2 * len(plan.steps)


@pytest.mark.parametrize(
    ("planner_type", "num_steps", "node_fn"),
    [
        (GraphPlanner, 1, get_graph_plan_size),
        (MonitoredPlanner, 1, get_monitored_graph_plan_size),
        (GraphPlanner, 2, get_graph_plan_size),
        (MonitoredPlanner, 2, get_monitored_graph_plan_size),
        (GraphPlanner, 3, get_graph_plan_size),
        (MonitoredPlanner, 3, get_monitored_graph_plan_size),
        (GraphPlanner, 5, get_graph_plan_size),
        (MonitoredPlanner, 5, get_monitored_graph_plan_size),
    ],
)
def test_planner_with_tasks(
    planner_type: type[Planner],
    num_steps: int,
    node_fn: t.Callable[[Workplan], int],
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
    # the_workplan: Workplan,
) -> None:
    """Verify that the planner produces a plan with the expected number of steps.

    Parameters
    ----------
    planner_type : type[Planner]
        The type of planner to test
    num_steps : int
        The number of steps to add to the workplan
    node_fn : Callable[[Workplan], int]
        A function that takes a workplan as input and returns the size of the plan
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    steps = list(gen_fake_steps(num_steps))
    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
    )

    planner = planner_type(plan)
    proposed_plan = planner.plan()
    assert len(proposed_plan) == node_fn(plan)


@pytest.mark.parametrize(
    ("num_steps", "deps"),
    [
        (2, [(0, 1)]),
        (3, [(0, 1), (0, 2)]),
        (4, [(0, 1), (0, 2), (2, 3)]),
    ],
)
def test_planner_monitored_deps(
    tmp_path: Path,
    num_steps: int,
    deps: list[tuple[int, int]],
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that a dependency between two steps is carried to the monitors.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store test outputs
    num_steps : int
        The number of steps to add to the workplan
    deps : list[tuple[int, int]]
        Tuples containing indices of tasks to create dependencies between
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    assert num_steps >= 2, "Test assumes at least two tasks"  # noqa: PLR2004

    steps = list(gen_fake_steps(num_steps))

    for source, target in deps:
        steps[source].depends_on.append(steps[target].name)

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
    )

    planner = MonitoredPlanner(plan)
    proposed_plan = planner.plan(artifact_dir=tmp_path)

    edges = planner.graph.edges

    to_check: list[tuple[str, str]] = []

    # confirm the graph first...
    for source, target in deps:
        source_name = f"step-{source + 1:03d}"
        target_name = f"step-{target + 1:03d}"

        monitor_name = MonitoredPlanner.derive_name(source_name)

        # verify there is a dependency from the task to the monitor
        dep_task_to_monitor = (source_name, monitor_name)
        assert dep_task_to_monitor in edges

        # confirm there is a dependency between monitors when the tasks have one
        target_monitor_name = MonitoredPlanner.derive_name(target_name)
        dep_monitor_to_monitor = (monitor_name, target_monitor_name)
        assert dep_monitor_to_monitor in edges

        to_check.append(dep_task_to_monitor)
        to_check.append(dep_monitor_to_monitor)

    # Confirm that the serialized version of the plan honors all the dependencies
    step_names = [t.name for t in proposed_plan]
    for n_from, n_to in to_check:
        from_idx = step_names.index(n_from)
        to_idx = step_names.index(n_to)

        assert from_idx < to_idx, (
            f"Dependency between {n_from} and {n_to} was not honored"
        )


@pytest.mark.parametrize(
    ("num_steps", "deps"),
    [
        pytest.param(
            10,
            [
                (0, 1),
                (1, 2),
                (2, 3),
                (3, 5),
                (0, 4),
                (3, 4),
                (4, 5),
                (5, 6),
                (6, 7),
                (7, 8),
                (8, 9),
            ],
            id="BFS fail",
        ),
        pytest.param(
            6,
            [(0, 4), (1, 4), (2, 4), (3, 4), (4, 5)],
            id="fan out",
        ),
    ],
)
def test_planner_bfs_breaker(
    tmp_path: Path,
    num_steps: int,
    deps: list[tuple[int, int]],
    gen_fake_steps: t.Callable[[int], t.Generator[Step, None, None]],
) -> None:
    """Verify that a dependency that breaks BFS ordering is honored.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store test outputs
    num_steps : int
        The number of steps to add to the workplan
    deps : list[tuple[int, int]]
        Tuples containing indices of tasks to create dependencies between
    gen_fake_steps : t.Callable[[int], t.Generator[Step, None, None]]
        A generator function to produce minimally valid test steps

    """
    # Ensure the direct link from task 01 to task 04 is not evaluated until
    # after 03 is complete (4 requires 0 and 3 to be complete)
    #   ________ O4
    #  [          \
    # O0--O1--O2--O3--05-->End
    assert num_steps >= 2, "Test assumes at least two tasks"  # noqa: PLR2004

    steps = list(gen_fake_steps(num_steps))

    for source, target in deps:
        steps[source].depends_on.append(steps[target].name)

    plan = Workplan(
        name="test-plan",
        description="test-description",
        steps=steps,
    )

    planner = GraphPlanner(plan)
    proposed_plan = planner.plan(artifact_dir=tmp_path)

    edges = planner.graph.edges

    to_check: list[tuple[str, str]] = []

    # confirm the graph contains an edge for all the dependencies that were added
    for source, target in deps:
        source_name = f"step-{source + 1:03d}"
        target_name = f"step-{target + 1:03d}"

        # verify there is a dependency in the graph
        edge = (source_name, target_name)
        assert edge in edges

        to_check.append(edge)

    # Confirm that the serialized version of the plan honors all the dependencies
    step_names = [t.name for t in proposed_plan]
    for n_from, n_to in to_check:
        from_idx = step_names.index(n_from)
        to_idx = step_names.index(n_to)

        assert from_idx < to_idx, (
            f"Dependency between {n_from} and {n_to} was not honored"
        )
