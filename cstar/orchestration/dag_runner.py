import asyncio
import os
import sys
import typing as t
from contextlib import contextmanager
from itertools import cycle
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest  # todo: remove after moving test to unit-tests

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

    closed_set = orchestrator.get_closed_nodes()
    open_set = orchestrator.get_open_nodes()
    delay_iter = iter(incremental_delays())

    while open_set is not None:
        print(f"[on-enter] Open nodes: {open_set}, Closed: {closed_set}")

        await orchestrator.run()

        closed_set = orchestrator.get_closed_nodes()
        open_set = orchestrator.get_open_nodes()

        print(f"[on-exit] Open nodes: {open_set}, Closed: {closed_set}")

        sleep_duration = next(delay_iter)
        print(f"Sleeping for {sleep_duration} seconds before next check.")
        await asyncio.sleep(sleep_duration)

    print(f"Workplan `{wp}` execution is complete.")


@contextmanager
def templated_plan(plan_name: str) -> t.Generator[Path, None, None]:
    wp_template_path = (
        Path(__file__).parent.parent / f"additional_files/templates/wp/{plan_name}.yaml"
    )
    bp_template_path = (
        Path(__file__).parent.parent / "additional_files/templates/bp/blueprint.yaml"
    )

    wp_tpl = wp_template_path.read_text()
    bp_tpl = bp_template_path.read_text()

    with (
        NamedTemporaryFile("w", delete_on_close=False) as wp,
        NamedTemporaryFile("w", delete_on_close=False) as bp,
    ):
        wp_path = Path(wp.name)
        bp_path = Path(bp.name)

        wp_tpl = wp_tpl.replace("{application}", "sleep")
        wp_tpl = wp_tpl.replace("{blueprint_path}", bp_path.as_posix())

        wp.write(wp_tpl)
        bp.write(bp_tpl)
        wp.close()
        bp.close()

        try:
            print(
                f"Temporary workplan located at {wp_path} with blueprint at {bp_path}"
            )
            yield wp_path
        finally:
            print(f"populated workplan template:\n{'#' * 80}\n{wp_tpl}\n{'#' * 80}")


@pytest.mark.asyncio
async def test_build_and_run() -> None:
    """Temporary unit test to trigger workflow execution."""
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "shared"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    for template in ["single_step", "fanout", "parallel"]:
        with templated_plan(template) as wp_path:
            my_run_name = f"{sys.argv[1]}_{template}"
            os.environ["CSTAR_RUNID"] = my_run_name
            asyncio.run(build_and_run_dag(wp_path))


if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    wp_path = Path("/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml")
    wp_path = Path("/home/x-seilerman/wp_testing/workplan.yaml")

    my_run_name = sys.argv[1]
    os.environ["CSTAR_RUNID"] = my_run_name
    asyncio.run(build_and_run_dag(wp_path))
