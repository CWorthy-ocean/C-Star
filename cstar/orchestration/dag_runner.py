import asyncio
import os
import typing as t
from dataclasses import dataclass
from itertools import cycle
from pathlib import Path

from cstar.base.log import get_logger
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import (
    Orchestrator,
    Planner,
    RunMode,
    check_environment,
    configure_environment,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    WorkplanTransformer,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_DELAYS, ENV_CSTAR_ORCH_REQD_ENV
from cstar.system.manager import cstar_sysmgr

if t.TYPE_CHECKING:
    from cstar.orchestration.orchestration import Launcher

log = get_logger(__name__)


@dataclass
class DagStatus:
    """The current status of a workflow."""

    open_items: t.Iterable[str]
    closed_items: t.Iterable[str]


def incremental_delays() -> t.Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    Generator[float]
    """
    delays = [0.1, 1, 2, 5, 15, 30, 60]

    if custom_delays := os.getenv(ENV_CSTAR_ORCH_DELAYS, ""):
        try:
            delays = [float(d) for d in custom_delays.split(",")]
        except ValueError:
            log.warning(f"Malformed delay provided: {custom_delays}. Using defaults.")

    delay_cycle = cycle(delays)
    yield from delay_cycle


async def retrieve_run_progress(orchestrator: Orchestrator) -> DagStatus:
    """Load the run state.

    Parameters
    ----------
    orchestrator : Orchestrator
        The orchestrator to be used for processing a plan.
    mode : RunMode
        The execution mode during processing.

        - RunMode.Schedule submits all processes in the plan in a non-blocking manner.
        - RunMode.Monitor waits for all processes in the plan to complete.

    Returns
    -------
    DagStatus
    """
    mode = RunMode.Monitor
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)

    # Run through all the tasks until we're caught up
    while open_set is not None:
        await orchestrator.run(mode=mode)

        closed_set = orchestrator.get_closed_nodes(mode=mode)
        open_set = orchestrator.get_open_nodes(mode=mode)

    return DagStatus(open_set or [], closed_set)


async def load_dag_status(path: Path, run_id: str) -> DagStatus:
    """Determine the current status of a workplan run.

    Parameters
    ----------
    path : Path
        The path to the blueprint being executed.
    run_id : str
        The unique run id to query status for.

    Returns
    -------
    DagStatus
    """
    wp = deserialize(path, Workplan)
    log.info(f"Loading status of workplan: {wp.name}")

    configure_environment(run_id=run_id)

    planner = Planner(workplan=wp)
    launcher = SlurmLauncher()
    orchestrator = Orchestrator(planner, launcher)

    return await retrieve_run_progress(orchestrator)


async def process_plan(orchestrator: Orchestrator, mode: RunMode) -> None:
    """Execute a plan from start to finish.

    Parameters
    ----------
    orchestrator : Orchestrator
        The orchestrator to be used for processing a plan.
    mode : RunMode
        The execution mode during processing.

        - RunMode.Schedule submits all processes in the plan in a non-blocking manner.
        - RunMode.Monitor waits for all processes in the plan to complete.
    """
    closed_set = orchestrator.get_closed_nodes(mode=mode)
    open_set = orchestrator.get_open_nodes(mode=mode)
    delay_iter = iter(incremental_delays())

    while open_set is not None:
        await orchestrator.run(mode=mode)

        curr_closed = orchestrator.get_closed_nodes(mode=mode)
        curr_open = orchestrator.get_open_nodes(mode=mode)

        if curr_closed != closed_set or curr_open != curr_open:
            # reset to initial delay when a task is found or completed
            delay_iter = iter(incremental_delays())

        open_set = curr_open
        closed_set = curr_closed

        sleep_duration = next(delay_iter)
        await asyncio.sleep(sleep_duration)

    log.info(f"Workplan {mode} is complete.")


async def prepare_workplan(
    wp_path: Path,
    output_dir: Path,
    run_id: str,
) -> tuple[Workplan, Path]:
    """Load the workplan and apply any applicable transforms.

    Parameters
    ----------
    wp_path : Path
        The path to the workplan to load.
    output_dir : Path
        The directory where workplan outputs will be written.
    run_id : str
        The unique ID for the current run.

    Returns
    -------
    Workplan
    """
    wp_orig = await asyncio.to_thread(deserialize, wp_path, Workplan)
    run_root_dir = output_dir / run_id

    transformer = WorkplanTransformer(wp_orig, RomsMarblTimeSplitter())
    wp = transformer.apply()

    if transformer.is_modified:
        log.info("A time-split workplan will be executed.")

    # make a copy of the original and modified blueprint in the output directory
    persist_orig = WorkplanTransformer.derived_path(
        wp_path, run_root_dir, "_original", ".bak"
    )
    persist_as = WorkplanTransformer.derived_path(wp_path, run_root_dir, "_transformed")

    _ = await asyncio.gather(
        asyncio.to_thread(serialize, persist_orig, wp_orig),
        asyncio.to_thread(serialize, persist_as, wp),
    )

    return wp, persist_as


# @flow(log_prints=True)
async def build_and_run_dag(
    wp_path: Path, run_id: str = "", output_dir: Path | None = None
) -> Path:
    """Execute the steps in the workplan.

    Parameters
    ----------
    path : Path
        The path to the blueprint to execute
    run_id : str | None
        The run-id to be used by the orchestrator.
    output_dir : Path | None
        The path to the output directory.

    Returns
    -------
    Path
        The path to the workplan that was executed after any tranformations
        were applied.
    """
    configure_environment(output_dir, run_id)
    output_dir = DirectoryManager.data_home()

    launcher: Launcher | None = None
    if cstar_sysmgr.scheduler:
        launcher = SlurmLauncher()
    else:
        launcher = LocalLauncher()
        os.environ[ENV_CSTAR_ORCH_REQD_ENV] = ""

    check_environment()
    wp, wp_path = await prepare_workplan(wp_path, output_dir, run_id)

    planner = Planner(workplan=wp)

    orchestrator = Orchestrator(planner, launcher)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, RunMode.Monitor)

    return wp_path
