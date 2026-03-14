import asyncio
import os
import typing as t
from itertools import zip_longest

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cstar.base.log import get_logger
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
console = Console()


def display_summary(
    run_id: str,
    open_set: t.Iterable[str] | None,
    closed_set: t.Iterable[str] | None,
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
    open_closed = zip_longest(open_set or ["N/A"], closed_set or ["N/A"])
    table = Table("Incomplete", "Complete")

    console.print(Panel.fit(f"Run `{run_id}` Current Status"))

    for open_item, closed_item in open_closed:
        table.add_row(open_item, closed_item)

    console.print(table)


def list_runs(incomplete: str) -> list[str]:
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
    run_list = asyncio.run(repo.list_latest_runs())
    if not run_list:
        return ["run-id"]

    run_ids = [r.run_id for r in run_list if r]
    return run_ids


@app.command()
def status(
    run_id: t.Annotated[
        str,
        typer.Option(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ] = "...",
) -> None:
    """Retrieve the current status of a workplan."""
    repo = TrackingRepository()
    workplan_run = asyncio.run(repo.get_workplan_run(run_id))

    if workplan_run is None:
        print("An unknown run-id was supplied.")
        return

    for k, v in workplan_run.environment.items():
        os.environ[k] = v

    path = workplan_run.trx_workplan_path
    if not path.exists():
        console.print(f"The workplan could not be found at `{path}`")
        raise typer.Exit(code=1)

    log.debug(f"Checking status on workplan in: {path}")

    from cstar.orchestration.dag_runner import load_dag_status

    try:
        status = asyncio.run(load_dag_status(path, run_id))
        display_summary(run_id, status.open_items, status.closed_items)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
