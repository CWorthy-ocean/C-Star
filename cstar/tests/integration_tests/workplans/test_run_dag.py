import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from cstar.orchestration.dag_runner import build_and_run_dag


def slurm() -> bool:
    """Check if srun exists."""
    process = subprocess.run(["which", "srun"], capture_output=True, text=True)

    if "not found" in process.stdout:
        return False

    if "no srun in" in process.stdout:
        return False

    return True


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

    for template in ["fanout", "linear", "parallel", "single_step"]:
        my_run_name = f"{yyyymmdd}_{template}"
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

    for template in ["fanout", "linear", "parallel", "single_step"]:
        my_run_name = f"{yyyymmdd}_{template}"
        os.environ["CSTAR_RUNID"] = my_run_name
        await build_and_run_dag(wp_path)
