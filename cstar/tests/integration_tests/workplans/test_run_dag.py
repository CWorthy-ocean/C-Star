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


@pytest.mark.skipif(not slurm())
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workplan_name",
    ["fanout", "linear", "parallel", "single_step"],
)
async def test_build_and_run(tmp_path: Path, workplan_name: str) -> None:
    """Temporary unit test to trigger workflow execution."""
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    cstar_dir = Path(__file__).parent.parent.parent.parent
    template_file = f"{workplan_name}.yaml"
    templates_dir = cstar_dir / "additional_files/templates"
    template_path = templates_dir / "wp" / template_file

    bp_tpl_path = templates_dir / "bp" / "blueprint.yaml"

    bp_path = tmp_path / "blueprint.yaml"
    bp_path.write_text(bp_tpl_path.read_text())

    bp_default_path = "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
    content = template_path.read_text()
    content = content.replace(bp_default_path, bp_path.as_posix())

    wp_path = tmp_path / template_file
    wp_path.write_text(content)

    # create unique run name only once per hour, cache otherwise.
    now = datetime.now()
    yyyymmdd = now.strftime("%Y-%m-%d %H")

    for template in ["fanout", "linear", "parallel", "single_step"]:
        my_run_name = f"{yyyymmdd}_{template}"
        os.environ["CSTAR_RUNID"] = my_run_name
        await build_and_run_dag(wp_path)
