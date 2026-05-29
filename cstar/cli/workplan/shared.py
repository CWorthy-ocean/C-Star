import asyncio
import typing as t
from collections import Counter
from collections.abc import Mapping

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.applications.core import (
    ApplicationDefinition,
    RunnerRequest,
    get_application,
)
from cstar.base.log import get_logger
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.entrypoint.runner import BlueprintRunner
from cstar.execution.file_system import DirectoryManager, JobFileSystemManager
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.models import Blueprint, Workplan
from cstar.orchestration.orchestration import Status
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository

console = Console()
log = get_logger(__name__)

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration
    from cstar.orchestration.tracking import WorkplanRun


def list_runs(incomplete: str) -> list[tuple[str, str]]:
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
    repo = TrackingRepository()
    run_list = asyncio.run(repo.list_latest_runs(incomplete))

    if not run_list:
        if incomplete:
            return [(incomplete, "no results found")]

        return [("run-id", "no results found")]

    return [(r.run_id, f"Workplan path: {r.workplan_path}") for r in run_list if r]


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
    """
    if not run_id:
        return []

    incomplete = incomplete.lower()
    wp_run: WorkplanRun | None = None
    try:
        repo = TrackingRepository()

        if wp_run := await repo.get_workplan_run(run_id):
            wp = deserialize(wp_run.trx_workplan_path, Workplan)
            step_names = [str(s.name) for s in wp.steps]

            if incomplete:
                step_names = [s for s in step_names if s.lower().startswith(incomplete)]

            return step_names
    except FileNotFoundError:
        if wp_run:
            msg = f"Workplan run contains a dead path: {wp_run.trx_workplan_path} was not found"
            log.debug(msg)

    # run state may be cleaned up. fallback to directory search
    run_dir = DirectoryManager.data_home() / run_id
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
    """
    run_id: str = ctx.params.get("run_id", "")

    if not run_id:
        msg = "run-id is required to autocomplete steps"
        raise typer.BadParameter(msg)

    return asyncio.run(list_steps(run_id, incomplete))


def checkmark(color: str) -> str:
    return f"[{color}]:heavy_check_mark:"


def display_summary(
    run_id: str,
    dag_status: DagStatus,
    step_order: list[str] | None = None,
    step_deps: dict[str, list[str]] | None = None,
) -> None:
    """Display a summary describing the current state of
    a DAG executed by the orchestrator.

    Parameters
    ----------
    run_id : str
        The run-id to retrieve the status for.
    dag_status : DagStatus
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
        title=f"Run [yellow]{run_id}[/yellow] Results",
        show_lines=True,
        padding=padding,
        pad_edge=False,
    )

    if step_order is not None:
        ordered = [
            (n, dag_status.details[n]) for n in step_order if n in dag_status.details
        ]
        seen = set(step_order)
        ordered += [
            (n, s) for n, s in sorted(dag_status.details.items()) if n not in seen
        ]
    else:
        ordered = sorted(dag_status.details.items())

    for task_name, status in ordered:
        deps_done = step_deps is None or all(
            dag_status.details.get(dep) == Status.Done
            for dep in step_deps.get(task_name, [])
        )
        table.add_row(
            task_name,
            checkmark("gray") if status == Status.Submitted and not deps_done else "",
            checkmark("green") if status == Status.Submitted and deps_done else "",
            checkmark("cyan") if Status.is_running(status) else "",
            checkmark("green") if status == Status.Done else "",
            checkmark("red") if status == Status.Failed else "",
            checkmark("yellow") if status == Status.Cancelled else "",
        )

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
    t.Mapping[str, str]
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


def create_xrunner(
    request: RunnerRequest[Blueprint],
    service_cfg: "ServiceConfiguration | None" = None,
    job_cfg: "JobConfig | None" = None,
    log_level: int | str = "INFO",
) -> BlueprintRunner[Blueprint]:
    """Dynamically create a runner using the application to look up the
    registered handler.

    Parameters
    ----------
    job_cfg : JobConfig
        Configuration applied to the scheduler.
    service_cfg : ServiceConfiguration
        Configuration applied to the service.
    request : RunnerRequest
        A request specifying the blueprint to be executed.
    """
    if job_cfg is None:
        job_cfg = get_job_config()
    if service_cfg is None:
        service_cfg = get_service_config(
            log_level,
            name=f"{request.application}_runner",
        )

    app: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = get_application(
        request.application
    )
    klass = app.runner
    return klass(request, service_cfg, job_cfg)
