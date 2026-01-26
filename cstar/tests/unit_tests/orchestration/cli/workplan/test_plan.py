from pathlib import Path

import pytest

from cstar.cli.workplan.plan import render
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import Planner
from cstar.orchestration.serialization import deserialize


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_cli_plan_action(
    tmp_path: Path,
    workplan_name: str,
    wp_templates_dir: Path,
    default_blueprint_path: str,
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
    default_blueprint_path : str
        Fixture returning the default blueprint path contained in template workplans
    """
    template_file = f"{workplan_name}.yaml"
    template_path = wp_templates_dir / template_file

    empty_bp_path = tmp_path / "blueprint.yaml"
    empty_bp_path.touch()

    content = template_path.read_text()
    content = content.replace(default_blueprint_path, empty_bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(content)

    wp = deserialize(wp_path, Workplan)
    planner = Planner(wp)

    plan_path = await render(planner, tmp_path)

    assert plan_path, "The render method failed to return a path"
    assert plan_path.exists(), "The render method failed to create the file"
    print(plan_path)
