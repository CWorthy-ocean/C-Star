import asyncio
import typing as t

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.base.log import get_logger
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import TrackingRepository

console = Console()
log = get_logger(__name__)


def list_runs(incomplete: str) -> list[tuple[str, str]]:
    """Retrieve a list of all recorded run-ids.

    Parameters
    ----------
    incomplete : str
        Any value from the user is provided to autocompletion.

    Returns
    -------
    t.Iterable[str]
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
    open_set : Iterable[str] | None
        The names of jobs that are unstarted or incomplete.
    open_set : Iterable[str] | None
        The names of jobs that have completed.
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

    Returns
    -------
    tuple[str, str]
        The whitespace-stripped key-value pair

    Raises
    ------
    ValueError
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


def check_and_capture_kvps(var: list[str]) -> t.Mapping[str, str] | None:
    """Capture all unique keyj-value pairs from user-supplied configuration
    supplied as a list of key-value pairs in the format ["key1=value", "key2=value"]

    Returns
    -------
    t.Mapping[str, str]
        The key-value pairs from the list converted into a mapping containing
        all unique key-value pairs

    Raises
    ------
    ValueError
        - If any <key>=<value> entry is invalid
        - If any key is provided with multiple values
    """
    if not var:
        return {}

    captured_kvps = [check_and_capture_kvp(entry) for entry in var]

    variables = dict(captured_kvps)

    if len(variables) < len(captured_kvps):
        counter = t.Counter(k for k, _ in captured_kvps)
        k, _ = counter.most_common(1)[0]
        msg = f"Found variable with multiple values: {k}"
        raise typer.BadParameter(msg)

    return variables


_TCommandContext = t.TypeVar("_TCommandContext")


def upsert_command_context(
    ctx: typer.Context,
    klass: type[_TCommandContext],
    **kwargs,  # noqa: ANN003
) -> _TCommandContext:
    """Instantiate a `_TCommandContext` and store it on the typer context if one
    does not exist.

    Parameters
    ----------
    ctx : typer.Context
        A context object containing state for the typer app
    klass : type
        The type of context object to be stored in the typer app

    Returns
    -------
    RunContext
    """
    if ctx.obj is None:
        ctx.obj = klass(**kwargs)
    else:
        for k, v in kwargs.items():
            setattr(ctx.obj, k, v)
    return ctx.obj
