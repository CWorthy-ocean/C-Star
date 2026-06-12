import asyncio
import os
import textwrap
import typing as t
from collections.abc import Awaitable, Generator, Iterable, Mapping
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path

from prefect import flow
from pydantic import BaseModel

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, capture_environment
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.execution.file_system import DirectoryManager, StateDirectoryManager
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import UserDefinedVariables, Workplan
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    Orchestrator,
    Planner,
    ProcessHandle,
    RunMode,
    Status,
    check_environment,
    configure_environment,
)
from cstar.orchestration.serialization import deserialize, serialize, try_deserialize
from cstar.orchestration.state import StateRepository, load_sentinels
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.orchestration.transforms import (
    TemplateFillTransform,
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
    def open_items(self) -> Iterable[str]:
        """Return the name of all items that have not completed."""
        return (k for k, v in self.details.items() if not Status.is_terminal(v))

    @property
    def closed_items(self) -> Iterable[str]:
        """Return the name of all items that have completed."""
        return (k for k, v in self.details.items() if Status.is_terminal(v))


def get_launcher() -> Launcher[t.Any]:
    """Get the appropriate launcher for the current environment."""
    launcher = SlurmLauncher() if cstar_sysmgr.scheduler else LocalLauncher()
    launcher.check_preconditions()
    return launcher


def incremental_delays() -> Generator[float, None, None]:
    """Return a value from an infinite cycle of incremental delays.

    Returns
    -------
    Generator[float]
    """
    delays: list[float] = [0.1, 1, 2, 5, 15, 30, 60]

    if custom_delays := os.getenv(ENV_CSTAR_ORCH_DELAYS, ""):
        try:
            delays = [float(d) for d in custom_delays.split(",")]
        except ValueError:
            log.warning(f"Malformed delay provided: {custom_delays}. Using defaults.")

    delay_cycle = cycle(delays)
    yield from delay_cycle


async def load_run_state(
    run_id: str,
    launcher: Launcher[t.Any],
) -> DagStatus:
    """Load the run state.

    Parameters
    ----------
    run_id : str
        The run-id to load status for

    Returns
    -------
    DagStatus
    """
    configure_environment(run_id=run_id)
    sentinels = await load_sentinels(launcher.handle_klass())

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


async def reload_dag(wp_run: WorkplanRun) -> DagStatus:
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
    wp = deserialize(wp_run.trx_workplan_path, Workplan)
    msg = f"Reloading workplan run: {wp.name}"
    log.debug(msg)

    configure_environment(wp_run.output_path, wp_run.run_id, wp_run.environment)

    planner = Planner(workplan=wp)
    launcher = get_launcher()
    orchestrator = Orchestrator(planner, launcher)

    return await process_plan(orchestrator, RunMode.Monitor)


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

        if curr_closed != closed_set or open_set != curr_open:
            # reset to initial delay when a task is found or completed
            delay_iter = iter(incremental_delays())

        open_set = curr_open
        closed_set = curr_closed

        sleep_duration = next(delay_iter)
        await asyncio.sleep(sleep_duration)

    msg = f"Workplan {str(mode)!r} is complete."
    log.info(msg)

    if open_set is None:
        open_set = {}

    return DagStatus({**open_set, **closed_set})


async def prepare_workplan(
    wp_path: Path,
    output_dir: Path,
    run_id: str,
    user_variables: Mapping[str, str] | None = None,
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
    user_variables : t.Mapping | None
        User-defined variables specified at runtime

    Returns
    -------
    tuple[Workplan, Path]
        Tuple containing the resulting workplan and workplan file path

    Raises
    ------
    ValueError
        If the expected and provided user variables are not in agreement.
    """
    wp_orig = await asyncio.to_thread(deserialize, wp_path, Workplan)
    run_root_dir = output_dir / run_id

    fill_transform: TemplateFillTransform | None = None
    file_io_extras: list[Awaitable[int]] = []

    if user_variables is not None:
        named_config = UserDefinedVariables(
            keys=set(wp_orig.runtime_vars), mapping=user_variables
        )
        if named_config.error:
            raise ValueError(named_config.error)

        fill_transform = TemplateFillTransform(
            variable_resolver=lambda name: named_config.mapping[name]
        )

        persist_vars = WorkplanTransformer.derived_path(
            wp_path, run_root_dir, suffix="", extension=".vars"
        )
        file_io_extras.append(asyncio.to_thread(serialize, persist_vars, named_config))

    transformer = WorkplanTransformer(
        wp_orig,
        fill_transform,
    )
    wp = transformer.apply()

    # make a copy of the original and modified blueprint in the output directory
    persist_orig = WorkplanTransformer.derived_path(
        wp_path, run_root_dir, "_original", ".bak"
    )
    persist_as = WorkplanTransformer.derived_path(wp_path, run_root_dir, "_transformed")

    file_io: list[Awaitable[int]] = [
        asyncio.to_thread(serialize, persist_orig, wp_orig),
        asyncio.to_thread(serialize, persist_as, wp),
        *file_io_extras,
    ]

    _ = await asyncio.gather(*file_io)

    return wp, persist_as


class ExecutiveStepSummary(BaseModel):
    """Aggregates and display metadata about the inputs and outputs of a step."""

    name: str
    """The step/process name."""
    log_path: str
    """The path to the logfile produced by the step."""
    script_path: str
    """The path to the script file used by the step."""
    working_dir: str
    """The path to the step's working directory."""
    blueprint_path: str
    """The path to the blueprint used to execute the step."""
    launcher: str
    """The name of the launcher used to launch the step."""
    task_id: str
    """The value of the `ProcessHandle.pid` for the underlying task."""
    sentinel_path: Path
    """The path to a sentinel file containing step state information."""

    def __str__(self) -> str:
        """Generate an _Executive Summary_ for a step from a workplan run."""
        underline_char = "-"
        step_header = f"{self.name!r}"
        step_underline = underline_char * len(step_header)
        assets_header = "Assets"
        assets_underline = underline_char * len(assets_header)
        job_header = "Job"
        job_underline = underline_char * len(job_header)

        handle = try_deserialize(self.sentinel_path, ProcessHandle)

        task_prompt = "Process ID"
        if handle is None:
            pid = "N/A"
            status = Status.Unsubmitted.name
        else:
            pid = handle.pid if handle else "N/A"
            status = (
                handle.status.name
                if hasattr(handle, "status")
                else Status.Unsubmitted.name
            )
            if handle.launcher_name == "slurm":
                task_prompt = "SLURM Job ID"

        summary = textwrap.dedent(f"""\
          {self.name!r}
          {step_underline}
           {assets_header}
           {assets_underline}
           - Log file: {self.log_path}
           - Configured blueprint: {self.blueprint_path}
           - Working directory: {self.working_dir}
           - Script path: {self.script_path}
           {job_header}
           {job_underline}
           - {task_prompt}: {pid}
           - Status: {status}
         """)

        return summary


class ExecutiveRunSummary(BaseModel):
    """Aggregate and display metadata about the inputs and outputs
    of a run.
    """

    run_id: str
    """The current run-id."""
    state_dir: str
    """The directory where c-star state information will be stored."""
    steps: list[ExecutiveStepSummary]
    """An executive summary for each step in the run."""
    dry_run: bool = False
    """Flag indicating a planning-only run was requested."""

    def __str__(self) -> str:
        """Generate an _Executive Summary_ for a workplan run."""
        prefix = "# "
        content_delimiter = "#" * 78
        step_summaries = [
            f"{prefix}{line}" for s in self.steps for line in str(s).split("\n")
        ]
        section_del = "-"
        steps_section = "\n".join(str(summary) for summary in step_summaries)

        header = "Workplan Execution Plan"
        if self.dry_run:
            header = f"{header} - DRY-RUN ONLY"
        section_del_wp = section_del * len(header)
        section_header_steps = "Task Details"
        section_del_steps = section_del * len(section_header_steps)

        summary = textwrap.dedent(f"""\
                {prefix}{content_delimiter}
                {prefix}{header}
                {prefix}{section_del_wp}
                {prefix}- Run ID: {self.run_id}
                {prefix}- State directory: {self.state_dir}
                {prefix}
                {prefix}{section_header_steps}
                {prefix}{section_del_steps}
                {prefix}
                <steps>
                {prefix}{content_delimiter}
                """)

        value_map = {"<steps>": steps_section}
        for tpl, value in value_map.items():
            summary = summary.replace(tpl, value)
        return summary


async def get_executive_summary(
    run_id: str,
    *,
    run: WorkplanRun | None = None,
) -> ExecutiveRunSummary:
    repo = TrackingRepository()
    run = run or await repo.get_workplan_run(run_id)
    if not run:
        msg = f"No run found with the run-id: {run_id}"
        raise CstarExpectationFailed(msg)

    workplan = deserialize(run.trx_workplan_path, Workplan)

    steps = t.cast("list[LiveStep]", workplan.steps)
    step_summaries: list[ExecutiveStepSummary] = []

    sentinels = await asyncio.gather(
        *[
            asyncio.to_thread(
                try_deserialize, StateRepository.sentinel_path(s), ProcessHandle
            )
            for s in steps
        ]
    )
    sentinel_map = {s.name: h for s, h in zip(steps, sentinels)}

    for step in steps:
        blueprint_path = Path(step.blueprint_path)
        live_step = LiveStep.from_step(step)
        handle = sentinel_map[step.name]

        summary = ExecutiveStepSummary(
            name=step.name,
            log_path=str(live_step.log_path),
            script_path=str(live_step.script_path),
            working_dir=str(live_step.working_dir) if live_step.working_dir else "",
            blueprint_path=str(blueprint_path),
            launcher=handle.launcher_name if handle else "",
            task_id=handle.pid if handle else "",
            sentinel_path=StateRepository.sentinel_path(live_step),
        )
        step_summaries.append(summary)

    return ExecutiveRunSummary(
        run_id=run_id,
        state_dir=StateDirectoryManager.run_state_dir().as_posix(),
        steps=step_summaries,
        dry_run=is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN),
    )


async def on_status_changed(handle: ProcessHandle) -> None:
    """Persist updates to process handles."""
    state_repo = StateRepository()
    run_repo = TrackingRepository()

    if path := await state_repo.put_sentinel(handle):
        if run := await run_repo.get_workplan_run(handle.run_id):
            run.sentinels.add(path)
            await run_repo.put_workplan_run(run)


@flow(log_prints=True)
async def build_and_run_dag(
    wp_path: Path,
    run_id: str = "",
    output_dir: Path | None = None,
    user_variables: Mapping[str, str] | None = None,
    dry_run: bool = False,
) -> Path:
    """Execute the steps in the workplan.

    Parameters
    ----------
    wp_path : Path
        The path to the blueprint to execute
    run_id : str | None
        The run-id to be used by the orchestrator.
    output_dir : Path | None
        The path to the output directory.
    user_variables : NamedConfiguration | None
        User-provided key-value pairs for use during templating.
    dry_run : bool
        If set to `true`, the execution plan will be built and persisted to disk
        but not executed.

    Returns
    -------
    Path
        The path to the workplan that was executed after any tranformations
        were applied.
    """
    output_dir = (output_dir or DirectoryManager.data_home()).expanduser().resolve()
    configure_environment(output_dir, run_id)

    launcher = get_launcher()

    check_environment()
    wp, prepared_wp_path = await prepare_workplan(
        wp_path, output_dir, run_id, user_variables
    )

    planner = Planner(workplan=wp)
    steps = t.cast("list[LiveStep]", planner.flatten())

    wp_run = WorkplanRun(
        workplan_path=wp_path,
        trx_workplan_path=prepared_wp_path,
        output_path=output_dir,
        run_id=run_id,
        environment=capture_environment(),
        user_variables=user_variables or {},
        sentinels={StateRepository.sentinel_path(s) for s in steps},
    )

    orchestrator = Orchestrator(planner, launcher)
    orchestrator.set_callback("status_changed", on_status_changed)
    orchestrator.set_callback("launched", on_status_changed)

    if dry_run:
        msg = f"Dry run complete. Prepared workplan location: {prepared_wp_path}"
        log.debug(msg)

        summary = await get_executive_summary(run_id, run=wp_run)
        log.info("\n" + str(summary))

        return prepared_wp_path

    run_repo = TrackingRepository()
    await run_repo.put_workplan_run(wp_run)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)
    summary = await get_executive_summary(run_id)
    log.info("\n" + str(summary))

    # monitor the scheduled tasks until they complete
    await process_plan(orchestrator, RunMode.Monitor)

    return prepared_wp_path
