import asyncio
import os
import typing as t
from collections import OrderedDict
from collections.abc import Awaitable, Generator, Iterable, Mapping
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path

from prefect import flow
from pydantic import BaseModel, Field, computed_field

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, capture_environment
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.execution.file_system import StateDirectoryManager
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.models import Step, UserDefinedVariables, Workplan
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
from cstar.system.manager import get_sysmgr

log = get_logger(__name__)


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

    def __getitem__(self, key: str) -> Status:
        """Access a status record using the step safe-name."""
        if status := self.details.get(key, None):
            return status
        return Status.Unsubmitted


class DagDetailRecord(t.NamedTuple):
    """Detail record used for displaying dependency-related information in table."""

    step: "Step"
    """The step."""
    ref_id: int
    """Numeric identifier used to refer dependencies among tasks in a summary."""
    status: Status
    """The status of the step."""
    awaiting: list[str]
    """A list of tasks this step is waiting on."""
    satisfied: list[str]
    """A list of tasks this step depends on that have successfully completed."""
    blocking: list[str]
    """A list of tasks this step depends on that have failed and block it from starting."""

    @property
    def ready(self) -> bool:
        """Flag indicating if all dependencies are complete."""
        return (
            self.status == Status.Submitted and not self.awaiting and not self.blocking
        )

    @property
    def waiting(self) -> bool:
        """Flag indicating if all dependencies are complete."""
        return (
            self.status == Status.Submitted
            and bool(self.awaiting)
            and not self.blocking
        )

    @property
    def running(self) -> bool:
        """Flag indicating if a task is currently executing."""
        return Status.is_running(self.status)

    @property
    def done(self) -> bool:
        """Flag indicating if a task successfully completed."""
        return self.status == Status.Done

    @property
    def failed(self) -> bool:
        """Flag indicating if a task failed."""
        return self.status == Status.Failed

    @property
    def cancelled(self) -> bool:
        """Flag indicating if a task was cancelled."""
        return self.status == Status.Cancelled

    @property
    def blocked(self) -> bool:
        return self.status == Status.Submitted and bool(self.blocking)

    @classmethod
    def get_ref_map(cls, d: OrderedDict[str, "DagDetailRecord"]) -> dict[str, int]:
        return {name: value.ref_id for name, value in d.items()}


def get_status_detail_map(
    planner: Planner,
    dag_status: DagStatus,
) -> OrderedDict[str, DagDetailRecord]:
    """Return a mapping from step names to their detailed status record.

    Parameters
    ----------
    planner : Planner
        The planner used to generate an execution graph for the workplan
    dag_status : DagStatus
        The overall completion status of the workplan.

    Returns
    -------
    OrderedDict[str, DagDetailRecord]
    """
    return OrderedDict(
        {
            step.name: DagDetailRecord(
                step=step,
                ref_id=i,
                status=dag_status[step.name],
                awaiting=[
                    s for s in step.depends_on if not Status.is_terminal(dag_status[s])
                ],
                satisfied=[
                    s
                    for s in step.depends_on
                    if (
                        Status.is_terminal(dag_status[s])
                        and not Status.is_failure(dag_status[s])
                    )
                ],
                blocking=[
                    s for s in step.depends_on if Status.is_failure(dag_status[s])
                ],
            )
            for i, step in enumerate(planner.flatten(), start=1)
        }
    )


def get_launcher() -> Launcher[t.Any]:
    """Get the appropriate launcher for the current environment.

    See: `cstar.system.manager.CStarSystemManager` for more information.

    Returns
    -------
    Launcher[t.Any]
    """
    launcher = SlurmLauncher() if get_sysmgr().scheduler else LocalLauncher()
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
    launcher: Launcher[ProcessHandle],
) -> DagStatus:
    """Load the run state.

    Parameters
    ----------
    run_id : str
        The run-id to load status for.
    launcher : Launcher[t.Any]
        The launcher used to execute the workplan.

    Returns
    -------
    DagStatus
    """
    configure_environment(run_id=run_id)
    sentinels = await load_sentinels(launcher.handle_klass())

    # ensure most recent status is retrieved in case of crash or system failure
    updates = await asyncio.gather(*map(launcher.update_status, sentinels))
    changes = [h for (is_updated, h) in updates if is_updated]
    await asyncio.gather(*map(on_status_changed, changes))

    closed_set = {s.name: s.status for s in sentinels if Status.is_terminal(s.status)}
    open_set = {s.name: s.status for s in sentinels if s.name not in closed_set}

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
    user_variables : Mapping | None
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
            variable_resolver=lambda name: named_config[name]
        )

        persist_vars = WorkplanTransformer.derived_path(
            wp_path, run_root_dir, suffix="", extension=".vars"
        )
        file_io_extras.append(asyncio.to_thread(serialize, persist_vars, named_config))
    else:
        fill_transform = TemplateFillTransform(variable_resolver=None)

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

    name: str = Field(description="The step/process name.", title="Step name")
    """The step/process name."""
    log_path: str = Field(
        description="The path to the logfile produced by the step.",
        title="Logfile path",
    )
    """The path to the logfile produced by the step."""
    script_path: str = Field(
        description="The path to the script file used by the step.",
        title="Script path",
    )
    """The path to the script file used by the step."""
    working_dir: str = Field(
        description="The path to the step's working directory.",
        title="Working directory",
    )
    """The path to the step's working directory."""
    blueprint_path: str = Field(
        description="The path to the blueprint used to execute the step.",
        title="Configured blueprint",
    )
    """The path to the blueprint used to execute the step."""
    launcher: str = Field(
        description="The name of the launcher used to launch the step.",
        title="Launcher name",
    )
    """The name of the launcher used to launch the step."""
    task_id: str = Field(
        description="The value of the `ProcessHandle.pid` for the underlying task.",
        title="Process ID",
    )
    """The value of the `ProcessHandle.pid` for the underlying task."""
    sentinel_path: Path = Field(
        description="The path to a sentinel file containing step state information.",
        title="State-file path",
    )
    """The path to a sentinel file containing step state information."""

    @computed_field(title="Status")
    def status(self) -> str:
        if not hasattr(self, "_handle"):
            self._handle = try_deserialize(self.sentinel_path, ProcessHandle)
        if self._handle is None:
            return Status.Unsubmitted.name
        return self._handle.status.name


class ExecutiveRunSummary(BaseModel):
    """Aggregate and display metadata about the inputs and outputs of a `Workplan` run."""

    run_id: str = Field(
        description="The run-id associated with the run being summarized",
        title="Run ID",
    )
    """The run-id associated with the run being summarized."""
    workplan_name: str = Field(
        description="The name of the workplan executed by the run.",
        title="Workplan name",
    )
    """The run-id associated with the run being summarized."""
    source_workplan: str = Field(
        description="The path to the original, unmodified workplan.",
        title="Original workplan",
    )
    """The path to the original, unmodified workplan."""
    final_workplan: str = Field(
        description="The path to the transformed, ready-to-run workplan.",
        title="Runnable workplan",
    )
    """The path to the transformed, ready-to-run workplan."""
    steps: list[ExecutiveStepSummary] = Field(
        default_factory=list[ExecutiveStepSummary],
        description="An executive summary for each step in the run.",
        title="Step details",
    )
    """An executive summary for each step in the run."""
    dry_run: bool = Field(
        default=False,
        description="Flag indicating a planning-only run was requested.",
        title="Dry-run only",
    )
    """Flag indicating a planning-only run was requested."""

    @computed_field(
        description="The directory where c-star state information will be stored.",
        title="State directory",
    )
    def state_dir(self) -> str:
        """The directory where c-star state information will be stored."""
        return str(StateDirectoryManager.run_state_dir(run_id=self.run_id))

    @classmethod
    async def from_run(
        cls,
        run: WorkplanRun,
    ) -> "ExecutiveRunSummary":
        workplan = deserialize(run.trx_workplan_path, Workplan)
        steps = [LiveStep.from_step(s) for s in workplan.steps]
        step_summaries: list[ExecutiveStepSummary] = []

        sentinels = await asyncio.gather(
            *[
                asyncio.to_thread(try_deserialize, path, ProcessHandle)
                for path in run.sentinels
            ]
        )

        for step, sentinel_path, handle in zip(
            steps,
            run.sentinels,
            sentinels,
        ):
            summary = ExecutiveStepSummary(
                name=step.name,
                log_path=str(step.log_path),
                script_path=str(step.script_path),
                working_dir=str(step.working_dir) if step.working_dir else "",
                blueprint_path=str(step.blueprint_path),
                launcher=handle.launcher_name if handle else "",
                task_id=handle.pid if handle else "",
                sentinel_path=sentinel_path,
            )
            step_summaries.append(summary)

        return ExecutiveRunSummary(
            run_id=run.run_id,
            workplan_name=workplan.name,
            source_workplan=str(run.workplan_path),
            final_workplan=str(run.trx_workplan_path),
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
) -> ExecutiveRunSummary:
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
    default_output_dir = StateDirectoryManager.data_dir()
    output_dir = (output_dir or default_output_dir).expanduser().resolve()
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
        return await ExecutiveRunSummary.from_run(wp_run)

    run_repo = TrackingRepository()
    await run_repo.put_workplan_run(wp_run)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator, RunMode.Schedule)
    return await ExecutiveRunSummary.from_run(wp_run)
