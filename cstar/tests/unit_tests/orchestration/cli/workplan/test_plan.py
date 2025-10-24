from pathlib import Path

import pytest

from cstar.cli.workplan.actions.plan import render
from cstar.orchestration.planning import GraphPlanner


@pytest.mark.parametrize(
    "workplan_name",
    ["mvp_workplan", "fanout_workplan", "linear_workplan"],
)
def test_cli_plan_action(
    request: pytest.FixtureRequest, tmp_path: Path, workplan_name: str
) -> None:
    """Verify that CLI plan action generates an output image from a workplan.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request used to load fixtures by name
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan fixture to use for workplan creation
    """
    workplan = request.getfixturevalue(workplan_name)
    planner = GraphPlanner(workplan)

    plan_path = render(planner, tmp_path)

    assert plan_path, "The render method failed to return a path"
    assert plan_path.exists(), "The render method failed to create the file"
    print(plan_path)
