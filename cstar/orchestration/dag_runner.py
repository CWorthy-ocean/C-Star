import asyncio
import os
import typing as t
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path

from prefect import flow

from cstar.base.env import ENV_CSTAR_RUNID, capture_environment
from cstar.base.log import get_logger
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmHandle, SlurmLauncher
from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import (
    Launcher,
    Orchestrator,
    Planner,
    RunMode,
    Status,
    check_environment,
    configure_environment,
)
from cstar.orchestration.serialization import deserialize, serialize
from cstar.orchestration.state import load_sentinels
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import (
    RomsMarblTimeSplitter,
    WorkplanTransformer,
)
from cstar.orchestration.utils import ENV_CSTAR_ORCH_DELAYS
from cstar.system.manager import cstar_sysmgr

log = get_logger(__name__)
repo = TrackingRepository()


@dataclass
class DagStatus:
    """The current status of a workflow."""

    details: t.Annotated[
        dict[str, Status],
        field(default_factory=dict, init=True, repr=True),
    ]

    @property
    def open_items(self) -> t.Iterable[str]:
        """Return the name of all items that have not completed."""
        return (k for k, v in self.details.items() if not Status.is_terminal(v))

    @property
    def closed_items(self) -> t.Iterable[str]:
        """Return the name of all items that have completed."""
        return (k for k, v in self.details.items() if Status.is_terminal(v))


def get_launcher() -> "Launcher":
    """Get the appropriate launcher for the current environment."""
    launcher = SlurmLauncher() if cstar_sysmgr.scheduler else LocalLauncher()
    launcher.check_preconditions()
    return launcher


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


async def attach_to_run(orchestrator: Orchestrator) -> DagStatus:
    """Load the run state and monitor until it completes.

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

    if open_set is None:
        open_set = {}

    return DagStatus({**open_set, **closed_set})


async def load_run_state(run_id: str, launcher: Launcher) -> DagStatus:
    """Load the run state.

    Parameters
    ----------
    run_id : str
        The run-id to load status for

    Returns
    -------
    DagStatus
    """
    os.environ[ENV_CSTAR_RUNID] = run_id
    sentinels = await load_sentinels(SlurmHandle)

    open_set: dict[str, Status] = {}
    closed_set: dict[str, Status] = {}

    # ensure most recent status is retrieved in case of crash or system failure
    updates = [launcher.update_status(sentinel) for sentinel in sentinels]
    await asyncio.gather(*updates)

    for sentinel in sentinels:
        if Status.is_terminal(sentinel.status):
            closed_set[sentinel.name] = sentinel.status
        else:
            open_set[sentinel.name] = sentinel.status

    return DagStatus({**open_set, **closed_set})


async def reload_dag_status(path: Path, run_id: str) -> DagStatus:
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
    launcher = get_launcher()
    orchestrator = Orchestrator(planner, launcher)

    return await attach_to_run(orchestrator)


async def process_plan(orchestrator: Orchestrator, mode: RunMode) -> DagStatus:
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

    if open_set is None:
        open_set = {}

    return DagStatus({**open_set, **closed_set})


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


@flow(log_prints=True)
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

    launcher = get_launcher()

    check_environment()
    wp, prepared_wp_path = await prepare_workplan(wp_path, output_dir, run_id)

    planner = Planner(workplan=wp)

    orchestrator = Orchestrator(planner, launcher)

    await repo.put_workplan_run(
        WorkplanRun(
            workplan_path=wp_path,
            trx_workplan_path=prepared_wp_path,
            output_path=output_dir,
            run_id=run_id,
            environment=capture_environment(),
        ),
    )

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, RunMode.Monitor)

    return prepared_wp_path
