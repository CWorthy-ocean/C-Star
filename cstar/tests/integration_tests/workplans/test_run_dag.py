import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from cstar.orchestration.dag_runner import build_and_run_dag
from cstar.orchestration.models import Step, Workplan


def slurm() -> bool:
    """Check if srun exists."""
    process = subprocess.run(["which", "srun"], capture_output=True, text=True)

    if "not found" in process.stdout:
        return False

    if "no srun in" in process.stdout:
        return False

    return True


@pytest.mark.skip(reason="reminder/placeholder.")
def test_dep_fail() -> None:
    """Verify the orchestrator fails gracefully when a structurally invalid
    dependency structure is encountered.
    """
    # TODO: e.g. linear plan with circular dep

    # TODO: Consider validating in planner instead of runtime failure.
    # 1. all nodes are reachable
    # 2. no cycles
    ...


@pytest.mark.skip(reason="reminder/placeholder.")
def test_cancellation() -> None:
    """Verify that a cancellation of a step that has a dependent step
    causes the entire workplan to fail.
    """
    # TODO: wp with step a, step b
    # - schedule step A, schedule step B
    # - manually cancel step A immediately
    # - ...
    # - profit
    ...


@pytest.mark.skip(reason="reminder/placeholder.")
def test_dep_keys(tmp_path: Path) -> None:
    """Verify the orchestrator fails gracefully when dependencies are
    mismatched to step names.
    """
    # TODO: serialize the "bad workplan" and run the dag runner with the path.
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


@pytest.mark.skip(reason="Fix LocalLauncher infinite loop bug in schedule mode")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_build_and_run_local(tmp_path: Path, workplan_name: str) -> None:
    """Test the dag runner with a local launcher."""
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    # avoid running sims during tests
    os.environ["CSTAR_CMD_CONVERTER_OVERRIDE"] = "sleep"

    cstar_dir = Path(__file__).parent.parent.parent.parent
    template_file = f"{workplan_name}.yaml"
    templates_dir = cstar_dir / "additional_files/templates"
    template_path = templates_dir / "wp" / template_file

    bp_default = "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
    bp_path = tmp_path / "blueprint.yaml"
    bp_tpl_path = templates_dir / "bp" / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    wp_content = template_path.read_text()
    wp_content = wp_content.replace(bp_default, bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(wp_content)

    # create unique run name only once per hour, cache otherwise.
    now = datetime.now()
    yyyymmdd = now.strftime("%Y-%m-%d %H")

    my_run_name = f"{yyyymmdd}_{workplan_name}"
    os.environ["CSTAR_RUNID"] = my_run_name
    await build_and_run_dag(wp_path)  # , LocalLauncher())


# @pytest.mark.skipif(not slurm())
@pytest.mark.skip(reason="tests are very slow when scheduling many tasks with slurm")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_build_and_run(tmp_path: Path, workplan_name: str) -> None:
    """Test the dag runner with a SlurmLauncher."""
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    # avoid running sims during tests
    os.environ["CSTAR_CMD_CONVERTER_OVERRIDE"] = "sleep"

    cstar_dir = Path(__file__).parent.parent.parent.parent
    template_file = f"{workplan_name}.yaml"
    templates_dir = cstar_dir / "additional_files/templates"
    template_path = templates_dir / "wp" / template_file

    bp_default = "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
    bp_path = tmp_path / "blueprint.yaml"
    bp_tpl_path = templates_dir / "bp" / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    wp_content = template_path.read_text()
    wp_content = wp_content.replace(bp_default, bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(wp_content)

    # create unique run name only once per hour, cache otherwise.
    now = datetime.now()
    yyyymmdd = now.strftime("%Y-%m-%d %H")

    my_run_name = f"{yyyymmdd}_{workplan_name}"
    os.environ["CSTAR_RUNID"] = my_run_name
    await build_and_run_dag(wp_path)
