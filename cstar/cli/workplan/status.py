import asyncio
import typing as t
from itertools import zip_longest
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

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


@app.command()
def status(
    path: t.Annotated[Path, typer.Argument(help="Path to a workplan file.")],
    run_id: t.Annotated[
        str,
        typer.Option(help="The unique identifier of a specific workplan execution."),
    ] = "...",
) -> None:
    """Retrieve the current status of a workplan."""
    if not path.exists():
        console.print(f"The workplan could not be found at `{path}`")
        raise typer.Exit(code=1)

    from cstar.orchestration.dag_runner import load_dag_status

    try:
        status = asyncio.run(load_dag_status(path, run_id))
        display_summary(run_id, status.open_items, status.closed_items)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
