import asyncio
import os
import sys
import typing as t
from itertools import cycle
from pathlib import Path
from tempfile import TemporaryDirectory

from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import (
    Launcher,
    Orchestrator,
    Planner,
)
from cstar.orchestration.serialization import deserialize


def incremental_delays() -> t.Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    float
    """
    # TODO: load delays from config to enable dynamic changes for tests.
    delays = [2, 2, 5, 5, 15, 15]
    delay_cycle = cycle(delays)
    yield from delay_cycle


async def process_plan(orchestrator: Orchestrator, mode: Orchestrator.RunMode) -> None:
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)
    delay_iter = iter(incremental_delays())

    while open_set is not None:
        print(f"[on-enter::{mode}] Open nodes: {open_set}, Closed: {closed_set}")
        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)
        print(f"[on-exit::{mode}] Open nodes: {open_set}, Closed: {closed_set}")

        sleep_duration = next(delay_iter)
        print(f"Sleeping for {sleep_duration} seconds before next {mode}.")
        await asyncio.sleep(sleep_duration)

    print(f"Workplan {mode} is complete.")


# @flow(log_prints=True)
async def build_and_run_dag(path: Path) -> None:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    """
    wp = deserialize(path, Workplan)
    print(f"Executing workplan: {wp.name}")

    planner = Planner(workplan=wp)
    # from cstar.orchestration.launch.local import LocalLauncher
    # launcher: Launcher = LocalLauncher()
    launcher: Launcher = SlurmLauncher()
    orchestrator = Orchestrator(planner, launcher)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, Orchestrator.RunMode.Schedule)

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, Orchestrator.RunMode.Monitor)


if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    # wp_path = Path("/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml")
    # wp_path = Path("/home/x-seilerman/wp_testing/workplan.yaml")
    # wp_path = Path("/anvil/projects/x-ees250129/x-cmcbride/workplans/01.simple.yaml")

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        for template in ["fanout", "linear", "parallel", "single_step"]:
            cstar_dir = Path(__file__).parent.parent
            template_file = f"{template}.yaml"
            templates_dir = cstar_dir / "additional_files/templates/wp"
            template_path = templates_dir / template_file

            empty_bp_path = tmp_path / "blueprint.yaml"
            empty_bp_path.touch()

            bp_default_path = (
                "~/code/cstar/cstar/additional_files/templates/blueprint.yaml"
            )
            content = template_path.read_text()
            content = content.replace(bp_default_path, empty_bp_path.as_posix())

            wp_path = tmp_path / template_file
            wp_path.write_text(content)

            my_run_name = f"{sys.argv[1]}_{template}"
            os.environ["CSTAR_RUNID"] = my_run_name
            asyncio.run(build_and_run_dag(wp_path))
