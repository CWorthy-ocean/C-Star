import asyncio
import typing as t
from collections import Counter
from collections.abc import Mapping

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.applications.utils import get_application
from cstar.base.log import get_logger
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.entrypoint.xrunner import XBlueprintRunner, XRunnerRequest
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.models import Blueprint
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import TrackingRepository

console = Console()
log = get_logger(__name__)

if t.TYPE_CHECKING:
    from cstar.entrypoint.config import JobConfig, ServiceConfiguration


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
    repo = TrackingRepository()
    run_list = asyncio.run(repo.list_latest_runs(incomplete))

    if not run_list:
        if incomplete:
            return [(incomplete, "no results found")]

        return [("run-id", "no results found")]

    return [(r.run_id, f"Workplan path: {r.workplan_path}") for r in run_list if r]


def checkmark(color: str) -> str:
    return f"[{color}]:heavy_check_mark:"


def display_summary(
    run_id: str,
    dag_status: DagStatus,
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
        Column(header="Ready", justify="center"),
        Column(header="Running", justify="center"),
        Column(header="Done", justify="center"),
        Column(header="Failed", justify="center"),
        Column(header="Cancelled", justify="center"),
        title=f"Run [yellow]{run_id}[/yellow] Results",
        show_lines=True,
        padding=padding,
        pad_edge=False,
    )

    for task_name, status in sorted(dag_status.details.items()):
        table.add_row(
            task_name,
            checkmark("green") if Status.is_ready(status) else "",
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
    request: XRunnerRequest[Blueprint],
    service_cfg: "ServiceConfiguration | None" = None,
    job_cfg: "JobConfig | None" = None,
    log_level: int | str = "INFO",
) -> XBlueprintRunner[Blueprint]:
    """Dynamically create a runner using the application to look up the
    registered handler.

    Parameters
    ----------
    job_cfg : JobConfig
        Configuration applied to the scheduler.
    service_cfg : ServiceConfiguration
        Configuration applied to the service.
    request : XRunnerRequest
        A request specifying the blueprint to be executed.
    """
    if job_cfg is None:
        job_cfg = get_job_config()
    if service_cfg is None:
        service_cfg = get_service_config(
            log_level,
            name=f"{request.application}_runner",
        )

    klass = get_registered_runner(request.application)
    return klass(request, job_cfg, service_cfg)


def get_registered_bp(application: str) -> type[Blueprint]:
    """Retrieve the Blueprint type registered for the given application.

    Parameters
    ----------
    application
        The application name

    Returns
    -------
    type[Blueprint]
    """
    return get_application(application).blueprint


def get_registered_runner(application: str) -> type[XBlueprintRunner[Blueprint]]:
    """Retrieve the Runner type registered for the given application.

    Parameters
    ----------
    application
        The application name

    Returns
    -------
    type[XBlueprintRunner[Blueprint]]
    """
    return get_application(application).runner
