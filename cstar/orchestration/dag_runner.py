import asyncio
import functools
import shutil
import typing as t
from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, computed_field

from cstar.applications.core import get_application
from cstar.base.digest import digest_scheme
from cstar.base.env import capture_environment
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.base.utils import MAX_CONCURRENCY, slugify, utc_now
from cstar.entrypoint.utils import ARG_CLOBBER, ARG_PRE_RUN, ARG_RUN_ID
from cstar.execution.file_system import (
    StateDirectoryManager,
    is_remote_resource,
    local_copy,
    write_text_atomic,
)
from cstar.orchestration.compute_environment import (
    ComputeEnvironment,
    resolve_compute_environment,
)
from cstar.orchestration.launch.local import LocalLauncher
from cstar.orchestration.launch.slurm import SlurmLauncher
from cstar.orchestration.manifest import (
    ATTEMPT_BLUEPRINTS_DIR_NAME,
    ATTEMPT_TRANSFORMED_NAME,
    ATTEMPT_VARIABLES_NAME,
    ATTEMPT_WORKPLAN_NAME,
    RUN_MANIFEST_NAME,
    TRANSFORMED_WORKPLAN_GLOB,
    CarriedInputs,
    RunManifest,
    StepFacts,
    WorkplanIdentity,
    carried_digest,
    create_attempt_dir,
    describe_steps,
    ensure_run_manifest,
    fold_step_records,
    latest_attempt_dir,
    manifest_lock,
    new_attempt,
    read_run_manifest,
    record_submission,
    transformed_workplan_path,
    workplan_content_hash,
    write_run_manifest,
)
from cstar.orchestration.models import (
    KEY_CLOBBER,
    KEY_PRE_RUN,
    KEY_RESUME,
    Step,
    UserDefinedVariables,
    Workplan,
)
from cstar.orchestration.orchestration import (
    Launcher,
    LiveStep,
    LiveWorkplan,
    Orchestrator,
    Planner,
    ProcessHandle,
    Status,
    Task,
    check_environment,
    configure_environment,
)
from cstar.orchestration.run_check import report_orphans
from cstar.orchestration.serialization import (
    deserialize,
    model_to_yaml,
    serialize,
    try_deserialize,
)
from cstar.orchestration.state import StateRepository, load_sentinels
from cstar.orchestration.tracking import (
    KEY_RUN_UUID,
    TrackingRepository,
    WorkplanRun,
    measure_step_sizes,
)
from cstar.orchestration.transforms import (
    ExternalRuns,
    TemplateFillTransform,
    WorkplanTransformer,
    allowed_directives,
    external_dependencies,
    materialize_inline_blueprints,
    resolve_external_runs,
)
from cstar.system.manager import get_sysmgr
from cstar.system.migration import CstarSchemaTooNewError, version_key

if t.TYPE_CHECKING:
    from datetime import datetime

log = get_logger(__name__)

POLL_DELAYS: t.Final[tuple[float, ...]] = (0.1, 1, 2, 5, 15, 30, 60)
"""Seconds between orchestrator polls, cycled; restarts whenever a step changes state."""


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


def get_launcher(
    workplan: Workplan | None = None, *, force_local: bool = False
) -> Launcher[t.Any]:
    """Get the appropriate launcher for the workplan and current environment.

    A workplan's `compute_environment.launcher` selects the launcher; when
    unset, the launcher matching the current system is used (SLURM when the
    system has a scheduler, otherwise local).
    See: `cstar.system.manager.CStarSystemManager` for more information.

    Parameters
    ----------
    workplan : Workplan, optional
        The workplan whose `compute_environment` is honoured.
    force_local : bool
        Use the local launcher even when the system has a scheduler, as
        required by the sentinels of a pre-run (which are local processes).

    Returns
    -------
    Launcher[t.Any]

    Raises
    ------
    CstarExpectationFailed
        If the workplan requests the SLURM launcher on a system with no scheduler.
    CstarAdaptationError
        If the workplan's `compute_environment` is invalid.
    """
    env = (
        resolve_compute_environment(workplan.compute_environment)
        if workplan
        else ComputeEnvironment()
    )
    sysmgr = get_sysmgr()
    has_scheduler = bool(sysmgr.scheduler)

    if env.system and env.system.casefold() != sysmgr.name.casefold():
        log.warning(
            f"Workplan was written for system {env.system!r} but is running on "
            f"{sysmgr.name!r}"
        )

    launcher: Launcher[t.Any]
    if force_local or env.launcher == LocalLauncher.name:
        if has_scheduler and not force_local:
            log.warning(
                f"Workplan requests the local launcher on {sysmgr.name!r}, "
                "which has a scheduler"
            )
        launcher = LocalLauncher()
    elif env.launcher == SlurmLauncher.name or (not env.launcher and has_scheduler):
        if not has_scheduler:
            msg = (
                "Workplan requests the slurm launcher but system "
                f"{sysmgr.name!r} has no scheduler"
            )
            raise CstarExpectationFailed(msg)
        launcher = SlurmLauncher()
    else:
        launcher = LocalLauncher()

    launcher.check_preconditions(workplan)
    return launcher


def get_orchestrator(planner: Planner, *, root: Path, run_uuid: str) -> Orchestrator:
    """Create the orchestrator that launches the steps of a plan.

    Each launch persists the step's handle and records the submission in the
    run's manifest (see `on_launched`).

    Parameters
    ----------
    planner : Planner
        The planner holding the plan to execute.
    root : Path
        The run root, which holds the manifest the submissions are recorded in.
    run_uuid : str
        The uuid of the run's manifest; empty when it could not be written.

    Returns
    -------
    Orchestrator
    """
    launcher = get_launcher(planner.workplan, force_local=planner.workplan.pre_run)

    orchestrator = Orchestrator(planner, launcher, run_uuid=run_uuid)
    orchestrator.set_callback("launched", functools.partial(on_launched, root=root))

    return orchestrator


async def load_run_state(
    run_id: str,
    launcher: Launcher[ProcessHandle],
    *,
    persist: bool = True,
) -> DagStatus:
    """Load the run state.

    Parameters
    ----------
    run_id : str
        The run-id to load status for.
    launcher : Launcher[t.Any]
        The launcher used to execute the workplan.
    persist : bool
        Whether to write the statuses found to be stale back to the run's
        records. When `False` the statuses are computed and returned only.

    Returns
    -------
    DagStatus
    """
    configure_environment(run_id=run_id)
    sentinels = await load_sentinels(launcher.handle_klass())

    # ensure most recent status is retrieved in case of crash or system failure
    updates = await asyncio.gather(*map(launcher.update_status, sentinels))
    changes = [h for (is_updated, h) in updates if is_updated]

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _bounded_status_changed(handle: ProcessHandle) -> None:
        async with sem:
            await on_status_changed(handle)

    if persist:
        await asyncio.gather(*map(_bounded_status_changed, changes))

    closed_set = {s.name: s.status for s in sentinels if Status.is_terminal(s.status)}
    open_set = {s.name: s.status for s in sentinels if s.name not in closed_set}

    return DagStatus({**open_set, **closed_set})


def refresh_run_manifest(
    root: Path, run_id: str, status: DagStatus
) -> RunManifest | None:
    """Fold the observed step statuses and the step records into the run's manifest.

    The manifest holds a snapshot: the sentinels stay the live truth, and this
    is the one place that copies their statuses into it. The manifest is written
    only when something besides `observed_at` changed, and never when it or a
    record folded into it was written by a newer cstar-ocean with keys this build
    does not know: those are read with the keys ignored, so the refresh is kept
    in memory. A failure to refresh it is a warning: `--resume` refreshes it
    before scheduling, and keeping the record of a run never stops the run.

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run-id, which names the lock serializing the manifest's writers.
    status : DagStatus
        The state just loaded for the run.

    Returns
    -------
    RunManifest | None
        The refreshed manifest, or `None` when the run has no readable manifest
        (a run that predates it, or a corrupt or too new file) or it could not be
        refreshed (its lock stayed busy, its file could not be written).
    """
    statuses = {name: state.name for name, state in status.details.items()}
    try:
        with manifest_lock(run_id):
            manifest = read_run_manifest(root, tolerant=True)
            if manifest is None:
                log.debug(f"Run {run_id!r} has no manifest to refresh.")
                return None
            if fold_step_records(manifest, root, statuses, tolerant=True):
                try:
                    write_run_manifest(root, manifest)
                except CstarSchemaTooNewError as ex:
                    # read with keys a newer cstar-ocean wrote ignored, which
                    # the reads warned about: the refresh stays in memory
                    log.debug(f"Not writing the manifest of run {run_id!r}: {ex}")
    except Exception as ex:
        log.warning(f"Could not refresh the manifest of run {run_id!r}: {ex}")
        return None
    return manifest


async def load_external_runs(wp: Workplan, launcher: Launcher[t.Any]) -> ExternalRuns:
    """Refresh the external steps of a prepared workplan.

    A prepared workplan has its `runs` pinned, so the steps are the ones the
    run was scheduled against.

    Parameters
    ----------
    wp : Workplan
        The prepared (transformed) workplan.
    launcher : Launcher[t.Any]
        The launcher used to query the status of external steps.

    Returns
    -------
    ExternalRuns
        The registry, refreshed for every external step in `depends_on`.
    """
    external = ExternalRuns(wp.runs)
    await external.refresh(external_dependencies(wp), launcher)
    return external


async def process_plan(orchestrator: Orchestrator) -> DagStatus:
    """Schedule every step of a plan without waiting for completion.

    A step is launched once its upstream steps are scheduled, so several
    passes may be needed before every step is scheduled.

    Parameters
    ----------
    orchestrator : Orchestrator
        The orchestrator to be used for processing a plan.
    """
    closed_set = orchestrator.get_closed_nodes()
    open_set = orchestrator.get_open_nodes()
    delay_iter = cycle(POLL_DELAYS)

    while open_set is not None:
        await orchestrator.run()

        curr_closed = orchestrator.get_closed_nodes()
        curr_open = orchestrator.get_open_nodes()

        if curr_closed != closed_set or open_set != curr_open:
            # reset to initial delay when a task is found or completed
            delay_iter = cycle(POLL_DELAYS)

        open_set = curr_open
        closed_set = curr_closed

        sleep_duration = next(delay_iter)
        await asyncio.sleep(sleep_duration)

    log.info("Workplan scheduling is complete.")

    if open_set is None:
        open_set = {}

    return DagStatus({**open_set, **closed_set})


def original_workplan_backup(wp_path: Path, output_dir: Path) -> Path:
    """The legacy location of the untouched copy of a run's source workplan.

    A run scheduled before carried inputs kept the copy here, in the run root;
    now an attempt carries the authored workplan in its own directory (see
    `recorded_workplan`). Readers fall back to this path only for a run that has
    no such attempt.

    Parameters
    ----------
    wp_path : Path
        The path to the source workplan.
    output_dir : Path
        The run-specific directory where workplan artifacts are written.

    Returns
    -------
    Path
    """
    return WorkplanTransformer.derived_path(wp_path, output_dir, "_original", ".bak")


def recorded_workplan(wp_run: WorkplanRun) -> Path | None:
    """Locate the workplan file a recorded run was started from.

    The latest attempt that carried inputs holds the authored workplan; when its
    copy was deleted there is no record, and the legacy backup is not consulted
    because it would be older than that attempt. A run with no such attempt (it
    was scheduled before carried inputs, or has no manifest) has the legacy
    backup, when that exists.

    Parameters
    ----------
    wp_run : WorkplanRun
        The recorded run.

    Returns
    -------
    Path | None
        The recorded workplan, or `None` when none is on disk.

    Raises
    ------
    CstarSchemaTooNewError
        If the run's manifest is newer than this build reads (see `VersionedDocument`).
    CstarError
        If the run's manifest cannot be read.
    """
    root = wp_run.output_path
    manifest = read_run_manifest(root)

    attempt_dir = latest_attempt_dir(manifest, root) if manifest else None
    if attempt_dir is not None:
        carried = attempt_dir / ATTEMPT_WORKPLAN_NAME
        return carried if carried.exists() else None

    backup = original_workplan_backup(wp_run.workplan_path, root)
    return backup if backup.exists() else None


def _carry_blueprint(step: Step, directory: Path) -> Path | None:
    """Copy the blueprint file a step names into a directory.

    Parameters
    ----------
    step : Step
        A step of the transformed workplan, whose blueprint path has had its
        placeholders filled and relative path anchored.
    directory : Path
        Where to copy the blueprint to.

    Returns
    -------
    Path | None
        The copy, or `None` for a sentinel URI, which names no file yet.
    """
    text = str(step.blueprint_path).strip()

    if is_remote_resource(text):
        with local_copy(text) as source:
            name = Path(urlsplit(text).path).name or source.name
            return _copy_into(source, directory, name)

    if urlsplit(text).scheme:
        return None

    source = Path(text).expanduser()
    return _copy_into(source, directory, source.name)


def _copy_into(source: Path, directory: Path, name: str) -> Path:
    """Copy a file's content to `directory / name`, creating the directory.

    Parameters
    ----------
    source : Path
        The file to copy; a symlink is followed.
    directory : Path
        The destination directory.
    name : str
        The name of the copy.

    Returns
    -------
    Path
        The copy.
    """
    directory.mkdir(parents=True, exist_ok=True)
    return Path(shutil.copyfile(source, directory / name))


def _carry_inputs(
    output_dir: Path,
    started_at: "datetime",
    authored: bytes,
    transformed: str,
    steps: Sequence[Step],
    variables: UserDefinedVariables | None,
) -> CarriedInputs:
    """Copy what an attempt was asked into its own directory in the run root.

    The directory holds the authored workplan as it was read, the runtime
    variables when any were given, a copy of the transformed workplan and each
    step's blueprint file. A deferred step has no blueprint file yet and an
    inline step's blueprint is built from the workplan, so neither is copied.
    When anything cannot be carried the directory is removed, so a refused
    attempt leaves nothing for `check` or a reader to find.

    Parameters
    ----------
    output_dir : Path
        The run root.
    started_at : datetime
        When the attempt was scheduled; it names the attempt's directory.
    authored : bytes
        The content of the authored workplan file.
    transformed : str
        The rendered transformed workplan.
    steps : Sequence[Step]
        The steps of the transformed workplan, before inline blueprints are
        materialized.
    variables : UserDefinedVariables | None
        The runtime variables given for the attempt.

    Returns
    -------
    CarriedInputs
        Where the inputs were carried to and the digest of each file.

    Raises
    ------
    FileNotFoundError
        If a blueprint file cannot be read.
    """
    attempt_dir = create_attempt_dir(output_dir, started_at)
    try:
        workplan_copy = attempt_dir / ATTEMPT_WORKPLAN_NAME
        workplan_copy.write_bytes(authored)
        transformed_copy = attempt_dir / ATTEMPT_TRANSFORMED_NAME
        transformed_copy.write_text(transformed, encoding="utf-8")
        files = [workplan_copy, transformed_copy]
        if variables is not None:
            variables_copy = attempt_dir / ATTEMPT_VARIABLES_NAME
            serialize(variables_copy, variables)
            files.append(variables_copy)

        carried_blueprints: dict[str, str] = {}
        for step in steps:
            if step.is_deferred or step.is_inline:
                continue

            directory = attempt_dir / ATTEMPT_BLUEPRINTS_DIR_NAME / step.safe_name
            if copied := _carry_blueprint(step, directory):
                files.append(copied)
                carried_blueprints[step.name] = copied.relative_to(
                    output_dir
                ).as_posix()

        digests = {
            file.relative_to(attempt_dir).as_posix(): carried_digest(file)
            for file in files
        }
    except BaseException:
        shutil.rmtree(attempt_dir, ignore_errors=True)
        raise

    return CarriedInputs(
        start_at=started_at,
        directory=attempt_dir.relative_to(output_dir).as_posix(),
        carried=digests,
        carried_blueprints=carried_blueprints,
    )


async def prepare_workplan(
    wp_path: Path,
    output_dir: Path,
    user_variables: Mapping[str, str] | None = None,
    clobber_steps: "Sequence[str] | None" = None,
    pre_run: bool = False,
    blueprint_sources: "Mapping[str, Path] | None" = None,
) -> tuple[Workplan, Path, ExternalRuns, CarriedInputs]:
    """Load the workplan and apply any applicable transforms.

    Parameters
    ----------
    wp_path : Path
        The path to the workplan to load.
    output_dir : Path
        The run-specific directory where workplan artifacts will be written.
    user_variables : Mapping | None
        User-defined variables specified at runtime
    clobber_steps : Sequence[str] | None
        Names or safe_names of steps whose prior state should be cleared and
        re-executed, or `all` to target every step.
    pre_run : bool
        Mark every step to perform its pre-launch stages and stop. Unlike
        clobber, this is persisted with the prepared workplan.
    blueprint_sources : Mapping[str, Path] | None
        The migrated copy of a step's blueprint, by step name, that the step is
        prepared from in place of the file it names. The authored workplan is
        carried as it is.

    Returns
    -------
    tuple[Workplan, Path, ExternalRuns, CarriedInputs]
        Tuple containing the resulting workplan, the workplan file path, the
        registry of external runs the workplan depends on, as refreshed while
        preparing it, and what the attempt carried into its directory under
        the run root

    Raises
    ------
    ValueError
        If the expected and provided user variables are not in agreement, or
        a step requests pre-run on an application that does not support it.
    """
    started_at = utc_now()
    # read beside the parse, so an edit made while the attempt prepares is not carried
    authored = await asyncio.to_thread(wp_path.read_bytes)
    wp_orig = await asyncio.to_thread(deserialize, wp_path, Workplan)
    for step in wp_orig.steps:
        if source := (blueprint_sources or {}).get(step.name):
            step.blueprint_path = source

    fill_transform: TemplateFillTransform | None = None
    variables: UserDefinedVariables | None = None

    if user_variables is not None:
        named_config = UserDefinedVariables(
            keys=set(wp_orig.runtime_vars), mapping=user_variables
        )
        if named_config.error:
            raise ValueError(named_config.error)

        variables = named_config
        fill_transform = TemplateFillTransform(
            variable_resolver=lambda name: named_config[name]
        )
    else:
        fill_transform = TemplateFillTransform(variable_resolver=None)

    external = await resolve_external_runs(
        wp_orig,
        fill_transform,
        lambda: get_launcher(wp_orig, force_local=pre_run),
        base_dir=wp_path.resolve().parent,
    )

    transformer = WorkplanTransformer(
        wp_orig,
        fill_transform,
        external,
        wp_path=wp_path,
    )
    wp = transformer.apply()

    apply_clobber_overrides(wp, clobber_steps)
    check_pre_run_support(wp)
    if pre_run:
        apply_pre_run_overrides(wp)

    # an inline step's blueprint is carried by the workplan, not as a file
    transformed_steps = wp.steps

    # inline blueprints are written to disk only for a real run, not for `check`
    materialized = await asyncio.to_thread(materialize_inline_blueprints, wp.steps)
    wp = LiveWorkplan.model_validate(wp, from_attributes=True).model_copy(
        update={"steps": materialized, "root": output_dir.resolve()}
    )

    transformed = model_to_yaml(wp)
    carried = await asyncio.to_thread(
        _carry_inputs,
        output_dir,
        started_at,
        authored,
        transformed,
        transformed_steps,
        variables,
    )

    # replaced only once the inputs are carried: a refused attempt leaves the
    # plan the run executes as the last attempt left it. The plan is named
    # `.yaml` whatever the authored suffix
    persist_as = transformed_workplan_path(output_dir, wp_path)
    await asyncio.to_thread(write_text_atomic, persist_as, transformed)

    return wp, persist_as, external, carried


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
        """Build a run summary from a persisted run record.

        Parameters
        ----------
        run : WorkplanRun
            The run record to summarize.

        Returns
        -------
        ExecutiveRunSummary
            A summary of the run and the status of each of its steps.
        """
        workplan = LiveWorkplan.load(run.trx_workplan_path)
        steps = [LiveStep.from_step(s) for s in workplan.steps]
        step_summaries: list[ExecutiveStepSummary] = []

        # `run.sentinels` is an unordered set, so pair each step with its own
        # sentinel by file name rather than by position. Steps that were never
        # launched have no recorded sentinel and fall back to the canonical path.
        recorded = {path.name: path for path in run.sentinels}
        sentinel_paths = [
            recorded.get(
                StateRepository.sentinel_name(step.safe_name),
                StateRepository.sentinel_path(step.safe_name, run_id=run.run_id),
            )
            for step in steps
        ]

        sentinels = await asyncio.gather(
            *[
                asyncio.to_thread(try_deserialize, path, ProcessHandle)
                for path in sentinel_paths
            ]
        )

        for step, sentinel_path, handle in zip(steps, sentinel_paths, sentinels):
            summary = ExecutiveStepSummary(
                name=step.name,
                log_path=str(step.log_path),
                script_path=str(step.script_path),
                working_dir=str(step.working_dir),
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
        )


def check_clobber_targets(wp: Workplan, clobber_steps: "Sequence[str]") -> list[str]:
    """Return the clobber selections that match no step in the workplan.

    Parameters
    ----------
    wp : Workplan
        The workplan the selections are resolved against.
    clobber_steps : Sequence[str]
        Step names or safe_names selected for clobber.

    Returns
    -------
    list[str]
        The selections (sorted) that match neither a step's `name` nor its
        `safe_name`; empty when every selection is valid.
    """
    known = {key for step in wp.steps for key in (step.name, step.safe_name)}
    return sorted({token for token in clobber_steps if token not in known})


def check_clobber_dependents(wp: Workplan, clobber_steps: "Sequence[str]") -> list[str]:
    """Return the untargeted steps that depend on a step selected for clobber.

    Such steps will not be re-run and may consume stale outputs derived from
    their re-executed dependencies.

    Parameters
    ----------
    wp : Workplan
        The workplan the selections are resolved against.
    clobber_steps : Sequence[str]
        Step names or safe_names selected for clobber.

    Returns
    -------
    list[str]
        The names (sorted) of steps at risk of consuming stale outputs; empty
        when every dependent is itself selected.
    """
    by_token = {key: step for step in wp.steps for key in (step.name, step.safe_name)}
    targeted = {by_token[token].name for token in clobber_steps if token in by_token}
    if not targeted:
        return []
    return sorted(
        step.name
        for step in wp.steps
        if step.name not in targeted and targeted.intersection(step.depends_on)
    )


def apply_clobber_overrides(
    wp: Workplan, clobber_steps: "Sequence[str] | None"
) -> None:
    """Validate the clobber selection and record it on the workplan.

    Resolves each selection against `wp.steps` (matching on either `name` or
    `safe_name`), then mutates the matched steps' `workflow_overrides` in
    place so the selection is persisted with the workplan. Emits a single
    warning listing any untargeted steps that depend on a targeted one, since
    they will not be re-run and may consume stale outputs.

    Selections must be concrete step identifiers; the CLI-only token `all` is
    expanded to every step's safe_name before it reaches this layer.

    Parameters
    ----------
    wp : Workplan
        The workplan whose steps should be marked for per-step clobber.
    clobber_steps : Sequence[str] | None
        The step names or safe_names selected for clobber.

    Raises
    ------
    ValueError
        If any selection does not match a step's `name` or `safe_name`.
    """
    tokens = [x for token in clobber_steps or [] if (x := token.strip())]
    if not tokens:
        return

    if unknown := check_clobber_targets(wp, tokens):
        valid_names = sorted(step.name for step in wp.steps)
        msg = f"Unknown clobber step selection(s) {unknown}. Valid steps: {valid_names}"
        raise ValueError(msg)

    by_token = {key: step for step in wp.steps for key in (step.name, step.safe_name)}
    for token in tokens:
        by_token[token].workflow_overrides[KEY_CLOBBER] = True

    if stale := check_clobber_dependents(wp, tokens):
        msg = (
            "Some steps depend on clobbered steps but were not selected for "
            "clobber themselves; they will not be re-run and may consume "
            f"stale outputs. Review the following steps: {', '.join(stale)}"
        )
        log.warning(msg)


class NoPreparableStepsError(Exception):
    """Raised when pre-run mode leaves no step in the workplan to prepare."""


def check_pre_run_support(wp: Workplan) -> None:
    """Fail fast when a step requests pre-run on an unsupporting application.

    Only the `pre_run` override as written on the steps is checked, so run this
    before `apply_pre_run_overrides`.

    Parameters
    ----------
    wp : Workplan
        The workplan to check.

    A pre-run is a property of the whole run -- its steps are local processes
    whose sentinels no scheduler can refresh -- so the override must be set on
    every step or on none.

    Raises
    ------
    ValueError
        If any step requests pre-run on an application that does not declare
        `ApplicationDefinition.pre_runnable`, or only some steps request it;
        every offending step is named.
    """
    problems: list[str] = []
    unsupported = [
        f"{step.name!r} ({step.application})"
        for step in wp.steps
        if step.pre_run and not get_application(step.application).pre_runnable
    ]
    if unsupported:
        problems.append(
            f"step(s) {', '.join(unsupported)} request {KEY_PRE_RUN!r} but their "
            "application does not support it"
        )
    marked = [step.name for step in wp.steps if step.pre_run]
    if marked and len(marked) != len(wp.steps):
        problems.append(
            f"{KEY_PRE_RUN!r} is set on step(s) {', '.join(map(repr, marked))} "
            f"but not on every step; a pre-run covers the whole workplan "
            f"(use {ARG_PRE_RUN})"
        )
    if problems:
        raise ValueError("; ".join(problems))


def apply_pre_run_overrides(wp: Workplan) -> None:
    """Mark every step in the workplan to perform its pre-launch stages only.

    Like `apply_clobber_overrides`, this mutates the steps' `workflow_overrides`
    in place; unlike it, the caller persists the result, so the prepared
    workplan records that the run was a pre-run.

    Parameters
    ----------
    wp : Workplan
        The workplan whose steps should be marked for pre-run.
    """
    for step in wp.steps:
        step.workflow_overrides[KEY_PRE_RUN] = True


def _unpreparable_reason(step: Step) -> str:
    """Return why a pre-run cannot prepare a step, or an empty string if it can."""
    if not get_application(step.application).pre_runnable:
        return f"application does not support {ARG_PRE_RUN}"
    if step.is_deferred:
        return "blueprint is produced by another step"

    directives = allowed_directives(step.application)
    if any(
        directives[key].referenced_steps(config)
        for key, config in step.directives.items()
        if key in directives and isinstance(config, Mapping)
    ):
        return "directive references another step's output"
    return ""


def prune_unpreparable_steps(wp: Workplan) -> tuple[Workplan, dict[str, list[str]]]:
    """Drop the steps a pre-run cannot prepare ahead of the rest of the plan.

    A step is unpreparable when its application does not support pre-run, its
    blueprint is deferred to another step, or one of its directives consumes
    another step's output; none of those exist before the upstream steps run.
    Every step downstream of a dropped step is dropped too: a `depends_on`
    edge may carry data the planner cannot see, so a kept step never runs
    ahead of a skipped upstream.

    The pruned workplan is for planning only and is never persisted. It may have
    no steps, which `Workplan` would otherwise reject: callers must check.

    Parameters
    ----------
    wp : Workplan
        The workplan to prune.

    Returns
    -------
    tuple[Workplan, dict[str, list[str]]]
        The workplan without the unpreparable steps, and the names of the
        dropped steps keyed by the reason they were dropped.
    """
    reasons: dict[str, list[str]] = {}
    for step in wp.steps:
        if reason := _unpreparable_reason(step):
            reasons.setdefault(reason, []).append(step.name)

    dropped = {name for names in reasons.values() for name in names}
    if not dropped:
        return wp, reasons

    # steps are not guaranteed to be in dependency order: iterate to a fixpoint
    dependents: list[str] = []
    while True:
        newly = [
            step.name
            for step in wp.steps
            if step.name not in dropped and any(d in dropped for d in step.depends_on)
        ]
        if not newly:
            break
        dependents.extend(newly)
        dropped.update(newly)
    if dependents:
        reasons["depends on a skipped step"] = dependents

    kept = [step for step in wp.steps if step.name not in dropped]
    return wp.model_copy(update={"steps": kept}), reasons


def build_planner(
    wp: Workplan, external: Mapping[str, Task[ProcessHandle]] | None = None
) -> Planner:
    """Plan a workplan, dropping the steps a pre-run cannot prepare.

    Parameters
    ----------
    wp : Workplan
        The workplan to plan.
    external : Mapping[str, Task[ProcessHandle]] | None
        The refreshed external steps the workplan depends on, keyed by their
        `<step>@<alias>` token.

    Returns
    -------
    Planner

    Raises
    ------
    NoPreparableStepsError
        If the workplan is a pre-run and none of its steps can be prepared.
    """
    if wp.pre_run:
        wp, reasons = prune_unpreparable_steps(wp)
        for reason, names in reasons.items():
            msg = (
                f"Skipping {len(names)} step(s) in pre-run mode ({reason}): "
                f"{', '.join(names)}"
            )
            log.info(msg)

        if not wp.steps:
            msg = "No step can be prepared in pre-run mode; nothing was scheduled."
            raise NoPreparableStepsError(msg)

    return Planner(workplan=wp, external=external)


async def apply_resume_overrides(wp: Workplan, status: DagStatus) -> list[str]:
    """Mark every resumable, failed step of a prior run for resume.

    For each step whose status in `status` is a failure, marks it in
    `wp.steps` for resume when its application declares itself resumable
    (`ApplicationDefinition.resumable`); a failed step whose application does
    not support resume is left untouched and reported instead, so it re-runs
    from scratch on this reload.

    Like `apply_clobber_overrides`, this mutates the in-memory workplan only:
    the caller must not persist `resume: true` on the step, or a later plain
    reload (without `--resume`) would resume it instead of clobbering it.

    Parameters
    ----------
    wp : Workplan
        The workplan whose failed steps should be marked for resume.
    status : DagStatus
        The state of the prior run being re-entered, as loaded by the caller.

    Returns
    -------
    list[str]
        The names of the steps marked for resume.
    """
    by_name = {step.name: step for step in wp.steps}

    resumed: list[str] = []
    unsupported: list[str] = []
    for name, step_status in status.details.items():
        if not Status.is_failure(step_status) or name not in by_name:
            continue

        step = by_name[name]
        if get_application(step.application).resumable:
            step.workflow_overrides[KEY_RESUME] = True
            resumed.append(name)
        else:
            unsupported.append(f"{name!r} ({step.application})")

    if unsupported:
        msg = (
            f"Failed step(s) {', '.join(unsupported)} do not support resume "
            "and will be re-run from scratch."
        )
        log.warning(msg)

    if resumed:
        msg = f"Resuming failed step(s): {', '.join(resumed)}"
        log.info(msg)
    elif not unsupported:
        log.info("No failed steps to resume.")

    return resumed


async def on_status_changed(handle: ProcessHandle) -> None:
    """Persist updates to process handles.

    On a terminal status transition, the step's own disk usage is measured
    and recorded on the run's `step_sizes` before the run is persisted. A run
    record that cannot be read or written is a warning: keeping the record of
    a run never stops the run, and the sentinel is written first.
    """
    state_repo = StateRepository()
    run_repo = TrackingRepository()

    path = await state_repo.put_sentinel(handle)
    try:
        run = await run_repo.get_workplan_run(handle.run_id)
    except Exception as ex:
        log.warning(f"Could not read the record of run {handle.run_id!r}: {ex}")
        return

    if not path or run is None:
        return

    sentinel: Path = path
    sizes: dict[str, int] = {}

    if Status.is_terminal(handle.status):
        wp_path = run.try_trx_workplan_path()
        wp = LiveWorkplan.try_load(wp_path) if wp_path else None
        step = (
            next((s for s in wp.steps if s.name == handle.name), None) if wp else None
        )
        if step is not None:
            sizes = await measure_step_sizes(
                run, {step.name: step.working_dir}, asyncio.Semaphore(1)
            )
        else:
            msg = (
                f"Could not measure disk usage for step {handle.name!r} in "
                f"run {handle.run_id!r}: step not found in workplan"
            )
            log.debug(msg)

    def _apply(latest: WorkplanRun) -> None:
        """Merge this event into the freshly loaded record under the run lock."""
        latest.sentinels.add(sentinel)
        if sizes:
            latest.record_step_sizes(sizes)

    # measured outside the lock, applied atomically: concurrent step events for
    # the same run must not overwrite each other's sentinels or sizes
    try:
        await run_repo.update_workplan_run(handle.run_id, _apply)
    except Exception as ex:
        log.warning(
            f"Could not record step {handle.name!r} in the record of run "
            f"{handle.run_id!r}: {ex}"
        )


async def on_launched(handle: ProcessHandle, *, root: Path) -> None:
    """Persist a launched process's handle and record its submission in the manifest.

    A handle adopted from a prior attempt carries the submission it was created
    with, which `record_submission` finds already recorded. A failure to write
    the manifest does not stop scheduling: the sentinel does not depend on it.

    Parameters
    ----------
    handle : ProcessHandle
        The handle of the launched process.
    root : Path
        The run root, which holds the manifest.
    """
    await on_status_changed(handle)

    if handle.submitted_at is None:
        return

    try:
        await asyncio.to_thread(
            record_submission,
            root,
            handle.run_id,
            handle.name,
            launcher=handle.launcher_name,
            job_id=handle.pid,
            submitted_at=handle.submitted_at,
        )
    except Exception as ex:
        log.warning(
            f"Could not record the submission of step {handle.name!r} in "
            f"{root / RUN_MANIFEST_NAME}: {ex}"
        )


def _major_moved(recorded: str, now: str) -> bool:
    """Whether a blueprint's schema major differs from the one recorded for its step.

    Parameters
    ----------
    recorded : str
        The schema version the step's manifest entry records; empty when unknown.
    now : str
        The schema version of the blueprint content loaded now; empty when none.

    Returns
    -------
    bool
        False when either version is unknown.
    """
    return bool(recorded and now) and version_key(recorded)[0] != version_key(now)[0]


def refuse_edited_done_steps(
    wp: Workplan,
    manifest: RunManifest,
    status: DagStatus,
    facts: Mapping[str, StepFacts],
    clobber_steps: "Sequence[str]",
) -> None:
    """Refuse to re-enter a run in which a `Done` step was edited and not clobbered.

    A step with no recorded definition is not checked. Nor is a `Done` step whose
    recorded definition hash has another scheme than its current one: another
    release computed it (see `DEFINITION_HASH_SCHEME`), so the two cannot be
    compared. Nor is one whose definition changed while its blueprint moved to
    another schema major: an automatic migration changes the content it hashes.
    Unless the re-entry is refused, such steps are named in one warning; the
    attempt that follows records their current definitions, so the next
    re-entry checks them.

    Parameters
    ----------
    wp : Workplan
        The authored workplan being re-entered.
    manifest : RunManifest
        The run's manifest, holding each step's recorded definition.
    status : DagStatus
        The run's step statuses, as loaded from its state.
    facts : Mapping[str, StepFacts]
        The facts known about each authored step (see `describe_steps`).
    clobber_steps : Sequence[str]
        Names or safe_names of the steps the user chose to re-run.

    Raises
    ------
    CstarExpectationFailed
        If a step that reached `Done` in this run now has a different definition
        than the one recorded in the manifest and is not selected for clobber, by
        `clobber_steps` or by its own `workflow_overrides`.
    """
    tokens = {x for token in clobber_steps if (x := token.strip())}
    edited: list[str] = []
    unchecked: list[str] = []
    for step in wp.steps:
        entry = manifest.step(step.name)
        if entry is None:
            continue
        if not entry.definition_hash:
            log.debug(f"Step {step.name!r} has no recorded definition; not checked")
            continue
        if status[step.name] is not Status.Done:
            continue
        current = facts.get(step.name)
        if current is None or step.clobber or {step.name, step.safe_name} & tokens:
            continue
        recorded, now = entry.definition_hash, current.definition_hash
        if digest_scheme(recorded) != digest_scheme(now):
            unchecked.append(step.name)
        elif now != recorded:
            migrated = _major_moved(
                entry.blueprint_schema_version, current.blueprint_schema_version
            )
            (unchecked if migrated else edited).append(step.name)

    if edited:
        names = ", ".join(repr(name) for name in edited)
        rerun = " ".join(f"{ARG_CLOBBER} {name}" for name in edited)
        raise CstarExpectationFailed(
            f"Steps {names} reached Done in run {manifest.run_id!r} under a "
            f"different definition; pass {rerun} to re-run them, or "
            f"{ARG_RUN_ID} <other-id> to start a new run"
        )

    if unchecked:
        names = ", ".join(repr(name) for name in unchecked)
        log.warning(
            f"Steps {names} reached Done in run {manifest.run_id!r} under "
            "definitions recorded by another C-Star release, which this one "
            "cannot compare (a changed hash recipe, or a blueprint migrated to a "
            "newer schema), so an edit made to them since is not detected; pass "
            f"{ARG_CLOBBER} <step-name> to re-run a step you changed. This attempt "
            "records their definitions again."
        )


async def refuse_stale_run_state(run_id: str, root: Path) -> None:
    """Refuse to start a run over the step state of an earlier run whose directory is gone.

    Sentinels are kept by run-id, outside the run directory, so they outlive a
    run directory that was purged from scratch or removed by hand. A launcher
    adopts a sentinel that reads Done, so the new run would report those steps
    done without running them. A run root that still holds its transformed
    workplan is the earlier run itself (its manifest could not be written, or
    predates manifests), and is re-entered as before.

    Parameters
    ----------
    run_id : str
        The run-id.
    root : Path
        The run root, which holds no manifest.

    Raises
    ------
    CstarExpectationFailed
        If the run root holds no transformed workplan and the run-id has sentinels.
    """
    if any(root.glob(TRANSFORMED_WORKPLAN_GLOB)):
        return
    if not await StateRepository().has_sentinels(run_id=run_id):
        return

    state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
    escape = f"pass {ARG_RUN_ID} <other-id> to start a new run"
    record = await TrackingRepository().get_workplan_run(run_id)
    elsewhere = record.output_path if record else None
    if elsewhere and elsewhere.is_dir() and elsewhere.resolve() != root.resolve():
        # the run lives on (under another data home): `admin clean` would remove it
        msg = (
            f"Run {run_id!r} is tracked in {elsewhere}, not {root}, and has step "
            f"state in {state_dir}; this run would report its steps done without "
            f"running them. Re-enter it under the data home it was started in, "
            f"or {escape}"
        )
    elif root.is_dir() and any(root.iterdir()):
        # something of the run is left: `admin clean` would remove it too
        msg = (
            f"Run {run_id!r} has step state in {state_dir}, but its directory "
            f"{root} holds neither its manifest nor its transformed workplan; "
            f"this run would report those steps done without running them. "
            f"Remove {state_dir} to run them again, or {escape}"
        )
    else:
        msg = (
            f"Run {run_id!r} has step state in {state_dir} left by an earlier run "
            f"whose directory {root} is gone; this run would report those steps "
            f"done without running them. Clear it with `cstar admin clean "
            f"{ARG_RUN_ID} {run_id}`, or {escape}"
        )
    raise CstarExpectationFailed(msg)


async def build_dag(
    wp_path: Path,
    run_id: str = "",
    user_variables: Mapping[str, str] | None = None,
    clobber_steps: "Sequence[str] | None" = None,
    pre_run: bool = False,
    blueprint_sources: "Mapping[str, Path] | None" = None,
) -> tuple[Planner, Path, dict[str, StepFacts], CarriedInputs]:
    """Execute the steps in the workplan.

    Parameters
    ----------
    wp_path : Path
        The path to the blueprint to execute
    run_id : str | None
        The run-id to be used by the orchestrator.
    user_variables : NamedConfiguration | None
        User-provided key-value pairs for use during templating.
    clobber_steps : Sequence[str] | None
        Names or safe_names of steps whose prior state should be cleared and
        re-executed, or `all` to target every step, as supplied via
        `--clobber`.
    pre_run : bool
        Prepare every step without launching its model, as supplied via
        `--pre-run`; steps that cannot be prepared are skipped.
    blueprint_sources : Mapping[str, Path] | None
        The migrated copy of a step's blueprint, by step name, read and prepared
        in place of the file the step names (see `describe_steps`).

    Returns
    -------
    tuple[Planner, Path, dict[str, StepFacts], CarriedInputs]
        The planner built from the prepared workplan, the path to the
        workplan after any transformations were applied, the facts known
        about each authored step, and what the attempt carried into its
        directory under the run root.

    Raises
    ------
    NoPreparableStepsError
        If `pre_run` leaves no step to prepare.
    CstarExpectationFailed
        If the run's manifest cannot be read, a step that reached Done was
        edited and not clobbered, or the run-id has step state left by a run
        whose directory is gone.
    """
    if run_id:
        run_id = slugify(run_id)

    configure_environment(run_id=run_id)
    output_dir = StateDirectoryManager.data_dir(run_id)

    check_environment()
    wp_orig = await asyncio.to_thread(deserialize, wp_path, Workplan)
    facts = await asyncio.to_thread(
        describe_steps, wp_orig, wp_path.parent, blueprint_sources
    )

    # the guard runs before `prepare_workplan` writes anything
    escape = f"pass {ARG_RUN_ID} <other-id> to start a new run"
    try:
        manifest = read_run_manifest(output_dir)
    except CstarSchemaTooNewError as ex:
        msg = f"Run {run_id!r} cannot be re-entered: {ex} Or {escape}"
        raise CstarExpectationFailed(msg) from ex
    except CstarError as ex:
        msg = f"Run {run_id!r} cannot be re-entered: {ex}; fix or remove the file, or {escape}"
        raise CstarExpectationFailed(msg) from ex

    if manifest is None:
        await refuse_stale_run_state(run_id, output_dir)
        log.debug(f"Run {run_id!r} has no manifest; no step definition is checked")
    else:
        status = await load_run_state(
            run_id, get_launcher(wp_orig, force_local=pre_run)
        )
        refuse_edited_done_steps(wp_orig, manifest, status, facts, clobber_steps or [])

    wp, prepared_wp_path, external, carried = await prepare_workplan(
        wp_path, output_dir, user_variables, clobber_steps, pre_run, blueprint_sources
    )
    # the steps are live once prepared; report before a pre-run prunes any of them
    report_orphans(output_dir, run_id, t.cast("LiveWorkplan", wp))
    planner = build_planner(wp, external.tasks())

    return planner, prepared_wp_path, facts, carried


def _uuid_of_sentinels(root: Path, run_id: str) -> str:
    """Return the manifest uuid the sentinels of a run without a manifest record.

    A run whose manifest was removed from its directory keeps the identity
    its sentinels record, so a launcher still adopts them (`build_dag` has
    refused a run whose directory is gone).

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run-id.

    Returns
    -------
    str
        The one uuid the run's sentinels record, or an empty string when the
        run has a manifest, or its sentinels record none or several.
    """
    if (root / RUN_MANIFEST_NAME).exists():
        return ""

    state_dir = StateDirectoryManager.run_state_dir(run_id=run_id)
    uuids = {
        handle.run_uuid
        for path in state_dir.glob(StateRepository.sentinel_name("*"))
        if (handle := try_deserialize(path, ProcessHandle)) and handle.run_uuid
    }
    if len(uuids) != 1:
        if uuids:
            log.debug(f"The sentinels of run {run_id!r} record several uuids: {uuids}")
        return ""
    return uuids.pop()


def _record_attempt(
    root: Path,
    run_id: str,
    wp_path: Path,
    workplan: Workplan,
    steps: "Sequence[LiveStep]",
    step_facts: Mapping[str, StepFacts],
    carried: CarriedInputs | None,
) -> str:
    """Record the scheduling of the workplan in the run's manifest.

    A failure to write the manifest does not stop scheduling: the tracking
    record and the sentinels do not depend on it.

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run-id.
    wp_path : Path
        The path to the original workplan.
    workplan : Workplan
        The workplan being scheduled, after any pre-run pruning.
    steps : Sequence[LiveStep]
        The steps being scheduled.
    step_facts : Mapping[str, StepFacts]
        The facts known about each authored step; empty when re-entering a run.
    carried : CarriedInputs | None
        What the attempt carried into the run directory; `None` when
        re-entering a run.

    Returns
    -------
    str
        The uuid of the run's manifest, or an empty string when it could not
        be written.
    """
    outside: list[str] = []

    def _relative_dir(step: LiveStep) -> str:
        if step.working_dir.is_relative_to(root):
            return step.working_dir.relative_to(root).as_posix()
        resolved = step.working_dir.resolve()  # the root is resolved
        if resolved.is_relative_to(root):
            return resolved.relative_to(root).as_posix()
        outside.append(step.name)
        return step.working_dir.as_posix()

    entries = {
        step.name: (
            step.application,
            _relative_dir(step),
            step_facts.get(step.name),
        )
        for step in steps
    }
    if outside:
        log.debug(f"Steps with a working directory outside the run root: {outside}")

    content_hash = workplan_content_hash(workplan)
    try:
        with manifest_lock(run_id):
            manifest = ensure_run_manifest(
                root,
                run_id=run_id,
                name=workplan.name,
                kind="workplan",
                workplan=WorkplanIdentity(
                    name=workplan.name, content_hash=content_hash, path=str(wp_path)
                ),
                attempt=new_attempt(
                    workplan, workplan_hash=content_hash, carried=carried
                ),
                steps=entries,
                runs=workplan.runs,
                carried_blueprints=carried.carried_blueprints if carried else None,
                minted_id=_uuid_of_sentinels(root, run_id),
            )
    except Exception as ex:
        log.warning(f"Could not write the manifest of run {run_id!r}: {ex}")
        return ""
    return manifest.id


async def run_dag(
    wp_path: Path,
    trx_wp_path: Path,
    run_id: str,
    planner: Planner,
    user_variables: Mapping[str, str] | None = None,
    step_facts: Mapping[str, StepFacts] | None = None,
    carried: CarriedInputs | None = None,
) -> WorkplanRun:
    """Execute the steps in the workplan.

    Parameters
    ----------
    wp_path : Path
        The path to the original workplan, recorded on the run for provenance.
    trx_wp_path : Path
        The path to the prepared (transformed) workplan that is executed; the
        directory holding it is the run's root.
    run_id : str
        The run-id to be used by the orchestrator.
    planner : Planner
        The planner to execute
    user_variables : NamedConfiguration | None
        User-provided key-value pairs for use during templating.
    step_facts : Mapping[str, StepFacts] | None
        The facts known about each authored step, from `build_dag`. `None`
        when re-entering a run, whose manifest already holds them.
    carried : CarriedInputs | None
        What the attempt carried into its directory, from `build_dag`. `None`
        when re-entering a run, which replays the recorded plan and carries
        nothing.

    Returns
    -------
    WorkplanRun
        The persisted record describing the run.
    """
    steps = t.cast("list[LiveStep]", planner.flatten())
    # the run root is where `prepare_workplan` wrote the transformed workplan,
    # so a reloaded run keeps the root it is running under
    root = Path(trx_wp_path).resolve().parent

    uuid = await asyncio.to_thread(
        _record_attempt,
        root,
        run_id,
        wp_path,
        planner.workplan,
        steps,
        step_facts or {},  # the one place a reload's absent facts are read
        carried,
    )

    metadata = {KEY_RUN_UUID: uuid} if uuid else {}

    wp_run = WorkplanRun(
        output_path=root,
        run_id=run_id,
        environment=capture_environment(),
        user_variables=user_variables or {},
        sentinels={StateRepository.sentinel_path(s) for s in steps},
        metadata=metadata,
    )

    _ = await TrackingRepository().put_workplan_run(wp_run)

    orchestrator = get_orchestrator(planner, root=root, run_uuid=uuid)

    # schedule the tasks without waiting for completion
    await process_plan(orchestrator)
    return wp_run


async def build_and_run_dag(
    wp_path: Path,
    run_id: str = "",
    user_variables: Mapping[str, str] | None = None,
    clobber_steps: "Sequence[str] | None" = None,
    pre_run: bool = False,
    blueprint_sources: "Mapping[str, Path] | None" = None,
) -> WorkplanRun:
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
    clobber_steps : Sequence[str] | None
        Names or safe_names of steps whose prior state should be cleared and
        re-executed, or `all` to target every step, as supplied via
        `--clobber`.
    pre_run : bool
        Prepare every step without launching its model, as supplied via
        `--pre-run`.
    blueprint_sources : Mapping[str, Path] | None
        The migrated copy of a step's blueprint, by step name, run in place of
        the file the step names.

    Returns
    -------
    WorkplanRun
        The persisted record describing the run.

    Raises
    ------
    NoPreparableStepsError
        If `pre_run` leaves no step to prepare.
    CstarExpectationFailed
        If the run's manifest cannot be read, a step that reached Done was
        edited and not clobbered, or the run-id has step state left by a run
        whose directory is gone.
    """
    planner, prepared_wp_path, facts, carried = await build_dag(
        wp_path,
        run_id,
        user_variables=user_variables,
        clobber_steps=clobber_steps,
        pre_run=pre_run,
        blueprint_sources=blueprint_sources,
    )
    return await run_dag(
        wp_path,
        prepared_wp_path,
        run_id,
        planner,
        user_variables=user_variables,
        step_facts=facts,
        carried=carried,
    )
