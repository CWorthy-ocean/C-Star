import asyncio
import functools
import typing as t
from collections import Counter, OrderedDict
from collections.abc import Mapping, Sequence
from pathlib import Path

import typer
from rich.table import Column, Table

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerRequest,
    get_application,
)
from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.base.utils import MAX_CONCURRENCY
from cstar.cli.common import (
    cb_pipeline,
    checkmark,
    colored,
    console,
    get_from_ctxmap,
    id_label,
    normalize_runid,
    refuse_workplan_file,
    set_ctxmap,
    set_env,
)
from cstar.entrypoint.config import get_service_config
from cstar.entrypoint.runner import BlueprintRunner
from cstar.entrypoint.utils import ARG_SIZE
from cstar.execution.file_system import (
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.dag_runner import DagDetailRecord
from cstar.orchestration.index import (
    HandleKind,
    ManifestRecord,
    RunHandle,
    parse_run_handle,
    resolve_run,
)
from cstar.orchestration.models import Blueprint, Workplan
from cstar.orchestration.orchestration import LiveStep, LiveWorkplan
from cstar.orchestration.tracking import (
    TrackingRepository,
    WorkplanRun,
    measure_step_sizes,
)

log = get_logger(__name__)

KEY_RUN: t.Final[str] = "run"
"""Context-map key under which the resolved run record is stored."""

HELP_RUN_HANDLE: t.Final[str] = (
    "A run-id, `run://<uuid>`, or the path of a run directory."
)


if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from cstar.entrypoint.config import ServiceConfiguration
    from cstar.orchestration.index import RunRecord
    from cstar.orchestration.models import StepRef
    from cstar.orchestration.transforms import ExternalRuns


def list_runs(incomplete: str = "") -> list[tuple[str, str]]:
    """Retrieve a list of all recorded run-ids.

    Parameters
    ----------
    incomplete : str
        Any value from the user is provided to autocompletion.

    Returns
    -------
    list[tuple[str, str]]
        A tuple for each run-id discovered containing [run-id, workplan-path]
    """
    incomplete = incomplete.lower()

    async def _bounded() -> Sequence[WorkplanRun]:
        """Retrieve the run list while limiting concurrent reads."""
        async with TrackingRepository.bound(MAX_CONCURRENCY) as repo:
            return await repo.list_latest_runs(incomplete)

    run_list = asyncio.run(_bounded())

    if not run_list:
        if incomplete:
            return [(incomplete, "no results found")]

        return [("run-id", "no results found")]

    def _hint(run: WorkplanRun) -> str:
        """Describe a run by its workplan, or by nothing when none is known."""
        try:
            return f"Workplan path: {run.workplan_path}"
        except CstarError as ex:
            log.debug(str(ex))
            return ""

    return [(r.run_id, _hint(r)) for r in run_list]


async def list_steps(run_id: str, incomplete: str) -> list[str]:
    """Return step names for the already-typed run_id, for shell autocompletion.

    Applies the `incomplete` parameter as a filter, when supplied.

    Parameters
    ----------
    run_id : str
        The run-id for which steps will be retrieved.
    incomplete : str
        A string used to filter results by matching to the start of the
        discovered step names

    Returns
    -------
    list[str]
        The matching step names, from the recorded workplan when available
        and otherwise from a scan of the tasks directory.
    """
    if not run_id:
        return []

    incomplete = incomplete.lower()
    repo = TrackingRepository()

    if wp_run := await repo.get_workplan_run(run_id, tolerant=True):
        try:
            wp_path = wp_run.trx_workplan_path
            wp = LiveWorkplan.load(wp_path, tolerant=True)
            step_names = [str(s.name) for s in wp.steps]

            if incomplete:
                step_names = [s for s in step_names if s.lower().startswith(incomplete)]

            return step_names
        except CstarError as ex:
            log.debug(f"Workplan run has no transformed workplan: {ex}")
        except FileNotFoundError:
            msg = f"Workplan run contains a dead path: {wp_path} was not found"
            log.debug(msg)
    else:
        log.debug(f"No run for run-id {run_id!r} could be found.")

    # run state may be cleaned up. fallback to directory search
    run_dir = StateDirectoryManager.data_dir(run_id)
    tasks_dir = JobFileSystemManager(run_dir).tasks_dir

    if not tasks_dir.exists():
        msg = f"tasks_dir {str(tasks_dir)!r} was not found"
        log.debug(msg)
        return []

    return [
        d.name
        for d in sorted(tasks_dir.iterdir())
        if d.is_dir() and d.name.lower().startswith(incomplete)
    ]


def autocomplete_step_list(ctx: typer.Context, incomplete: str) -> list[str]:
    """Return an auto-completed list of step names.

    Use the parameters on `ctx` to locate the run-id supplied by the user.

    Parameters
    ----------
    ctx : typer.Context
        The typer context
    incomplete : str
        A string used to filter the step list to step names that start with the value.

    Returns
    -------
    list[str]
        The matching step names, or an empty list when no run-id is available
        (e.g. click stored None after the run-id callback failed during
        resilient completion parsing). Completion functions must not raise.
    """
    run_id: str | None = ctx.params.get("run_id")

    if not run_id:
        return []

    try:
        return asyncio.run(list_steps(run_id, incomplete))
    except Exception:
        log.warning(
            f"Step listing failed; autocomplete will be empty for run {run_id!r}"
        )

    return []


def exit_on_external_problems(
    external: "ExternalRuns", refs: "Iterable[StepRef]"
) -> None:
    """Report the external steps that cannot be depended upon and exit.

    Parameters
    ----------
    external : ExternalRuns
        The registry after a refresh.
    refs : Iterable[StepRef]
        The external steps the workplan depends on.

    Raises
    ------
    typer.Exit
        With code 1 if any external step cannot be depended upon.
    """
    problems = {str(ref): problem for ref in refs if (problem := external.problem(ref))}
    for token, problem in problems.items():
        console.print(f"External dependency {token}: {problem}", soft_wrap=True)
    if problems:
        raise typer.Exit(1)


def ref_label(record: DagDetailRecord, ref_map: dict[str, int]) -> str:
    """Label the dependencies of a step by their row number in the summary.

    A dependency without a row (a step of another run) is labelled by its
    `<step>@<alias>` token.
    """
    items: list[str] = []
    for d in record.step.depends_on:
        color = "white"
        if d in record.awaiting:
            color = "yellow"
        elif d in record.satisfied:
            color = "green"
        elif d in record.blocking:
            color = "red"

        items.append(colored(str(ref_map.get(d, d)), color))
    if items:
        return ", ".join(str(x) for x in items)
    return ""


def display_summary(
    run: WorkplanRun,
    lookup: OrderedDict[str, DagDetailRecord],
) -> None:
    """Display a summary describing the current state of
    a DAG executed by the orchestrator.

    Parameters
    ----------
    run : WorkplanRun
        The run record that will be summarized.
    lookup : OrderedDict[str, DagDetailRecord]
        The status object produced by the DAG runner containing task status details.
    """
    # don't pad the top and bottom but give some horizontal space
    padding = (0, 1)

    table = Table(
        Column(header="Step", justify="right"),
        Column(header="Submitted", justify="center"),
        Column(header="In Queue", justify="center"),
        Column(header="Running", justify="center"),
        Column(header="Done", justify="center"),
        Column(header="Failed", justify="center"),
        Column(header="Cancelled", justify="center"),
        Column(header="Dependencies", justify="center"),
        title=f"Run {colored(run.run_id, 'yellow')} Results",
        show_lines=True,
        padding=padding,
        pad_edge=False,
    )

    refs_map = DagDetailRecord.get_ref_map(lookup)

    for x in lookup.values():
        table.add_row(
            id_label(refs_map[x.step.name], x.step.safe_name, x.step.application),
            (checkmark("gray") if x.waiting else ""),
            (checkmark("white") if x.ready else ""),
            checkmark("cyan") if x.running else "",
            checkmark("green") if x.done else "",
            checkmark("red") if x.failed else "",
            checkmark("yellow") if x.cancelled else "",
            ref_label(x, refs_map),
        )

    if run.size_mb >= 0:
        caption = f"{run.size_mb}MB of step output"
        if run.size_measured_at:
            caption += f" as of {run.size_measured_at.astimezone():%Y-%m-%d %H:%M}"
    else:
        caption = (
            f"Disk usage not measured. Run: cstar run status {run.run_id} {ARG_SIZE}"
        )

    table.caption = caption

    console.print(table)


def check_and_capture_kvp(entry: str) -> tuple[str, str]:
    """Perform validation on user-supplied configuration value supplied
    as a key-value pair with the expected format `<key>=<value>`.

    Parameters
    ----------
    entry : str
        A string containing a key-value pair to be parsed.

    Returns
    -------
    tuple[str, str]
        The whitespace-stripped key-value pair

    Raises
    ------
    typer.BadParameter
        - If key and value are missing (e.g. `entry=="="`)
        - If no key is found (e.g. `entry=="=value"`)
        - If no value is found (e.g. `entry=="key="`)
    """
    splits = entry.split("=", 1)
    kvp_size: t.Final[int] = 2

    if len(splits) != kvp_size:
        msg = f"Variable `{entry}` not in expected format `<key>=<value>`"
        raise typer.BadParameter(msg)

    k, v = splits[0].strip(), splits[1].strip()

    if not k and not v:
        msg = "Found incomplete variable missing key and value"
        raise typer.BadParameter(msg)

    if not k:
        msg = f"Found orphaned variable value without key: {entry}"
        raise typer.BadParameter(msg)

    if not v:
        msg = f"Found variable with empty value for key: {entry}"
        raise typer.BadParameter(msg)

    return k, v


def check_and_capture_kvps(entries: list[str]) -> Mapping[str, str] | None:
    """Capture all unique keyj-value pairs from user-supplied configuration
    supplied as a list of key-value pairs in the format ["key1=value", "key2=value"]

    Parameters
    ----------
    entries : list[str]
        A list of strings, each containing a key-value pair to be parsed.

    Returns
    -------
    Mapping[str, str]
        The key-value pairs from the list converted into a mapping containing
        all unique key-value pairs

    Raises
    ------
    typer.BadParameter
        - If a <key>=<value> entry is malformed
        - If a key is provided more than once
    """
    if not entries:
        return {}

    captured_kvps = [check_and_capture_kvp(entry) for entry in entries]

    variables = dict(captured_kvps)

    if len(variables) < len(captured_kvps):
        counter = Counter(k for k, _ in captured_kvps)
        k, _ = counter.most_common(1)[0]
        msg = f"Found variable with multiple values: {k}"
        raise typer.BadParameter(msg)

    return variables


def preprocess_vars(
    ctx: typer.Context,
    user_variables: list[str],
) -> list[str]:
    """Perform validation and formatting on user-supplied variables.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    user_variables : list[str]
        A list of key-value pairs supplied by a user.

    Returns
    -------
    list[str]
        The original input with leading/trailing whitespace stripped from the keys
        and values; empty when no variables were supplied.

    Raises
    ------
    typer.BadParameter
        - If the key-value pair does not meet `key=value` convention
    """
    if user_variables:
        ctx.obj = check_and_capture_kvps(user_variables)

    return user_variables


def preprocess_varfile(
    ctx: typer.Context,
    user_varfile_path: Path | None,
) -> Path | None:
    """Perform validation and formatting on user-supplied variables
    supplied through a path to a variables file.

    Places the processed variables into the user data slot of the typer context object.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app.
    user_varfile_path : Path | None
        A path to a file containing the variable configuration.

    Returns
    -------
    Path | None

    Raises
    ------
    typer.BadParameter
        - If the source file does not exist
        - If any individual key-value pair is malformed
    """
    if user_varfile_path is None:
        return None

    with user_varfile_path.open("r") as fp:
        lines = [x.strip() for x in fp.readlines() if x.strip()]

    ctx.obj = check_and_capture_kvps(lines)

    return user_varfile_path


def create_xrunner(
    request: RunnerRequest[Blueprint],
    service_cfg: "ServiceConfiguration | None" = None,
    log_level: int | str = "INFO",
) -> BlueprintRunner[Blueprint]:
    """Dynamically create a runner using the application to look up the
    registered handler.

    Parameters
    ----------
    service_cfg : ServiceConfiguration
        Configuration applied to the service.
    request : RunnerRequest
        A request specifying the blueprint to be executed.
    """
    if service_cfg is None:
        service_cfg = get_service_config(
            log_level,
            name=f"{request.application}_runner",
        )

    app: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = get_application(
        request.application
    )
    klass = app.runner
    return klass(request, service_cfg)


def parse_handle(context: typer.Context, target: str) -> RunHandle:
    """Read what the user typed to name a run.

    A run-id is normalized as `normalize_runid` does, before anything looks for
    it; a uuid and a path never are.

    Parameters
    ----------
    context : typer.Context
        The typer context.
    target : str
        The user-supplied run-id, `run://<uuid>`, or run directory.

    Returns
    -------
    RunHandle

    Raises
    ------
    typer.BadParameter
        Raised when the target is not a run handle.
    """
    try:
        handle = parse_run_handle(target)
    except ValueError as ex:
        raise typer.BadParameter(str(ex)) from ex

    if handle.kind is not HandleKind.run_id:
        return handle

    try:
        return RunHandle(handle.kind, normalize_runid(context, handle.value))
    except ValueError as ex:
        msg = f"{handle.value!r} is not a valid run-id: {ex}"
        raise typer.BadParameter(msg) from ex


def lookup_run(handle: RunHandle, *, hint: str = "") -> "RunRecord":
    """Resolve a handle to the run it names.

    Parameters
    ----------
    handle : RunHandle
        What the user named.
    hint : str
        Text appended to the message when no run is found.

    Returns
    -------
    RunRecord

    Raises
    ------
    typer.BadParameter
        Raised when the handle does not identify a run.
    """
    try:
        return resolve_run(handle)
    except CstarError as ex:
        raise typer.BadParameter(f"{ex} {hint}".rstrip()) from ex


def preload_run(context: typer.Context, target: str) -> str:
    """Resolve a run handle then load the record of the run it names.

    Stores the record in the context map as "run". The record is the tracking
    record when the run is tracked on this machine, else the run's manifest.

    Parameters
    ----------
    context : typer.Context
        The typer context.
    target : str
        The user-supplied run-id, `run://<uuid>`, or run directory.

    Returns
    -------
    str
        The run-id of the run found, which is not the target when that names
        the run by its uuid or its directory.

    Raises
    ------
    typer.BadParameter
        Raised when the target is not a run handle or names no run.
    """
    record = lookup_run(parse_handle(context, target))
    set_ctxmap(context, KEY_RUN, record)

    return record.run_id


def get_run_record(context: typer.Context) -> "RunRecord":
    """Retrieve the run record that `preload_run` or `preload_run_record` stored.

    Parameters
    ----------
    context : typer.Context
        The typer context.

    Returns
    -------
    RunRecord
    """
    run = get_from_ctxmap(context, KEY_RUN, object)
    if not isinstance(run, WorkplanRun | ManifestRecord):
        log.warning(
            "Context map contains a run of an unexpected type: "
            f"{run.__class__.__name__!r}"
        )
        raise typer.Exit(102)

    return run


def require_run_id(context: typer.Context, target: str) -> str:
    """Refuse a run handle that is not a run-id.

    For the commands that act on a run tracked on this machine: they read its
    live state, or write into its directory.

    Parameters
    ----------
    context : typer.Context
        The typer context.
    target : str
        The user-supplied target.

    Returns
    -------
    str
        The target, unchanged.

    Raises
    ------
    typer.BadParameter
        Raised when the target is not a run-id.
    """
    if parse_handle(context, target).kind is not HandleKind.run_id:
        msg = (
            f"{target!r} is not a run-id: `cstar run {context.info_name}` acts on a "
            "run tracked on this machine, named by its run-id; use "
            f"`cstar run show {target}` to read that run."
        )
        raise typer.BadParameter(msg)

    return target


def preload_run_record(context: typer.Context, run_id: str) -> str:
    """Verify a run-id is valid then load the `WorkplanRun` record.

    Looks in tracking only. Stores the record in the context map as "run".

    Parameters
    ----------
    context : typer.Context
        The typer context.
    run_id : str
        The user-supplied run-id.

    Returns
    -------
    str
        The run-id for the loaded run

    Raises
    ------
    typer.BadParameter
        - Raised when the run-id is invalid, no `WorkplanRun` exists for it, or
          its record cannot be read (one a newer cstar-ocean wrote that this
          build cannot read)
    """
    repo = TrackingRepository()
    try:
        run = asyncio.run(repo.get_workplan_run(run_id, tolerant=True))
    except (ValueError, CstarError) as ex:
        raise typer.BadParameter(str(ex)) from ex

    if not run:
        msg = f"Unable to locate run with unknown run-id: {run_id}"
        raise typer.BadParameter(msg)

    set_ctxmap(context, KEY_RUN, run)

    return run_id


def preload_workplan(context: typer.Context, run_id: str) -> str:
    """Load the transformed `Workplan` of a run whose record is already loaded.

    NOTE: Requires the run record to be in the context map as "run"; see
    `preload_run`. Stores the workplan in the context map as "workplan".

    Parameters
    ----------
    context : typer.Context
        The typer context.
    run_id : str
        The run-id of the loaded run.

    Returns
    -------
    str
        The run-id for the loaded run

    Raises
    ------
    typer.BadParameter
        - Raised when the run has no transformed workplan, as a standalone
          blueprint run does
        - Raised when the transformed workplan cannot be deserialized
    """
    run = get_run_record(context)
    try:
        wp_path = run.trx_workplan_path
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex

    wp = LiveWorkplan.try_load(wp_path)
    if not wp:
        msg = f"Unable to deserialize workplan for run {run_id!r} from {str(wp_path)!r}"
        raise typer.BadParameter(msg)
    set_ctxmap(context, "workplan", wp)

    return run_id


def preload_step(context: typer.Context, step_name: str) -> str:
    """Given a step-name, ensure is a valid name in a preloaded workplan

    NOTE: Requires the workplan to be loaded into context dict as "workplan", e.g.
    `wp = cstar.cli.common.get_from_ctxmap(context, "workplan", Workplan)`

    See also: `cstar.cli.common.set_ctxmap`

    Parameters
    ----------
    context : typer.Context
        The typer context.
    step_name : str
        The user-supplied step-name.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        - Raised when the step-name cannot be found in the target workplan.
    """
    run_id = str(context.params.get("run_id", ""))
    if not run_id:
        msg = "A run-id is required to retrieve steps"
        raise typer.BadParameter(msg, param_hint="run_id")

    wp = get_from_ctxmap(context, "workplan", Workplan)
    step = next((x for x in wp.steps if step_name in {x.name, x.safe_name}), None)

    if step is None:
        valid_steps = ", ".join(f"{s.name!r}" for s in wp.steps)
        msg = (
            f"Unable to locate unknown step: {step_name!r}. Valid values: {valid_steps}"
        )
        raise typer.BadParameter(msg)

    set_ctxmap(context, "live_step", LiveStep.from_step(step))

    return step_name


_HELP_RUN_ID: t.Final[str] = "The unique identifier of a specific workplan execution."

RunIdArgument = t.Annotated[
    str,
    typer.Argument(
        help=HELP_RUN_HANDLE,
        autocompletion=list_runs,
        callback=cb_pipeline(
            refuse_workplan_file,
            preload_run,
            set_env(ENV_CSTAR_RUNID),
            preload_workplan,
        ),
    ),
]
"""Shared run argument: resolve the run handle, export the run-id of the run
found to the environment, and preload its record and transformed workplan into
the context map."""

RunHandleArgument = t.Annotated[
    str,
    typer.Argument(
        help=HELP_RUN_HANDLE,
        autocompletion=list_runs,
        callback=cb_pipeline(refuse_workplan_file, preload_run),
    ),
]
"""Run argument that only preloads the run record into the context map."""

RunRecordArgument = t.Annotated[
    str,
    typer.Argument(
        help=_HELP_RUN_ID,
        autocompletion=list_runs,
        callback=cb_pipeline(require_run_id, preload_run_record),
    ),
]
"""Run-id argument for the commands that read the live state of a tracked run;
only preloads the `WorkplanRun` record into the context map."""

TrackedRunIdArgument = t.Annotated[
    str,
    typer.Argument(
        help=_HELP_RUN_ID,
        autocompletion=list_runs,
        callback=cb_pipeline(
            refuse_workplan_file,
            require_run_id,
            normalize_runid,
            set_env(ENV_CSTAR_RUNID),
            preload_run_record,
            preload_workplan,
        ),
    ),
]
"""Run-id argument for the commands that write into a tracked run's directory:
normalize the run-id, export it to the environment, and preload the `WorkplanRun`
and transformed workplan into the context map. A run reached by its uuid or its
directory may not be the user's to write into."""


async def load_workplans(
    paths: Sequence[Path], cache: dict[Path, LiveWorkplan]
) -> None:
    """Load workplans from disk and add them to the in-memory cache.

    Parameters
    ----------
    paths : Sequence[Path]
        The paths to serialized, transformed workplans.
    cache : dict[Path, LiveWorkplan]
        The cached copy of loaded workplans, keyed on their file path;
        updated in place with the successfully loaded workplans.
    """
    workplans = await LiveWorkplan.load_all(paths, limit=MAX_CONCURRENCY)

    failed: list[Path] = []
    for path, wp in zip(paths, workplans):
        if wp:
            cache[path] = wp
        else:
            failed.append(path)

    if failed:
        log.debug(
            f"Workplan(s) failed deserialization and were skipped: "
            f"{', '.join(str(p) for p in failed)}"
        )


async def refresh_disk_usage(
    runs: Sequence[WorkplanRun], plans: Mapping[Path, LiveWorkplan]
) -> None:
    """Re-measure and persist per-step disk usage for a collection of runs.

    A run whose transformed workplan is not present in `plans` falls back to
    the subdirectories of its tasks directory; a run with neither is skipped,
    and so is a run whose record holds keys a newer cstar-ocean wrote, which
    this build does not write.

    Parameters
    ----------
    runs : Sequence[WorkplanRun]
        The runs whose step sizes will be measured and persisted.
    plans : Mapping[Path, LiveWorkplan]
        Loaded workplans, keyed on their transformed workplan path.
    """
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    by_id = {run.run_id: run for run in runs}
    step_dirs_by_run: dict[str, Mapping[str, Path]] = {}
    fallback: list[str] = []
    skipped: list[str] = []
    newer: list[str] = []

    for run in runs:
        if run.unknown_keys:
            newer.append(run.run_id)
            continue
        path = run.try_trx_workplan_path()
        plan = plans.get(path) if path is not None else None
        if plan is not None:
            step_dirs_by_run[run.run_id] = {s.name: s.working_dir for s in plan.steps}
            continue

        # no loadable workplan: take the step directories from the run's tasks dir
        tasks_dir = JobFileSystemManager(run.output_path).tasks_dir
        dirs = (
            {d.name: d for d in sorted(tasks_dir.iterdir()) if d.is_dir()}
            if tasks_dir.is_dir()
            else {}
        )
        if dirs:
            step_dirs_by_run[run.run_id] = dirs
            fallback.append(run.run_id)
        else:
            skipped.append(run.run_id)

    if fallback:
        log.debug(
            "Step directories were taken from the tasks directory for run(s) "
            f"without a loadable workplan: {', '.join(fallback)}"
        )
    if skipped:
        log.warning(
            "Disk usage was not refreshed for run(s) without a loadable workplan "
            f"or tasks directory: {', '.join(skipped)}"
        )
    if newer:
        log.warning(
            "Disk usage was not refreshed for run(s) whose record a newer "
            f"cstar-ocean wrote; upgrade cstar-ocean to refresh it: {', '.join(newer)}"
        )

    if not step_dirs_by_run:
        return

    results = await asyncio.gather(
        *(
            measure_step_sizes(by_id[rid], dirs, sem)
            for rid, dirs in step_dirs_by_run.items()
        )
    )
    measured = dict(zip(step_dirs_by_run, results))

    # apply under each run's lock so a concurrently running orchestrator's
    # sentinel/size updates are not overwritten
    async with TrackingRepository.bound(MAX_CONCURRENCY) as repo:
        await asyncio.gather(
            *(
                repo.update_workplan_run(
                    rid, functools.partial(WorkplanRun.record_step_sizes, sizes=sizes)
                )
                for rid, sizes in measured.items()
            )
        )
