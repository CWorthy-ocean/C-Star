from pathlib import Path

import pytest

from cstar.cli.workplan.plan import render
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import Planner
from cstar.orchestration.serialization import deserialize


@pytest.mark.usefixtures("read_yaml_intercept")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_cli_plan_action(
    tmp_path: Path,
    workplan_name: str,
    wp_templates_dir: Path,
) -> None:
    """Verify that CLI plan action generates an output image from a workplan.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for test outputs
    workplan_name : str
        The name of a workplan fixture to use for workplan creation
    wp_templates_dir: Path
        Fixture returning the path to the directory containing workplan template files
    """
    template_file = f"{workplan_name}.yaml"
    wp_path = wp_templates_dir / template_file

    wp = deserialize(wp_path, Workplan)
    planner = Planner(wp)

    plan_path = await render(planner, tmp_path)

    assert plan_path, "The render method failed to return a path"
    assert plan_path.exists(), "The render method failed to create the file"
    print(plan_path)
