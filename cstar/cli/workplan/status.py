import asyncio
import os
import typing as t

import typer
from rich.console import Console
from rich.table import Column, Table

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import list_runs
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
    lookup = {k: 1 for k in closed_set or []}
    lookup.update({k: 0 for k in open_set or []})

    table = Table(
        Column(header="Step", justify="right"),
        Column(header="Incomplete", justify="center"),
        Column(header="Complete", justify="center"),
        title=f"Run [yellow]{run_id}[/yellow] Results",
        show_lines=True,
        padding=(0, 1),  # 0 pad T/B, 1 pad L/R
        pad_edge=False,
    )

    for task_name, is_complete in sorted(lookup.items()):
        RED_CHECK = "[red]:heavy_check_mark:"
        GREEN_CHECK = "[green]:heavy_check_mark:"

        table.add_row(
            task_name,
            RED_CHECK if not is_complete else "",
            GREEN_CHECK if is_complete else "",
        )

    num_closed = sum(lookup.values())
    num_open = len(lookup) - num_closed

    table.add_row("[magenta]Total", f"[red]{num_open}", f"[green]{num_closed}")

    console.print(table)


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
