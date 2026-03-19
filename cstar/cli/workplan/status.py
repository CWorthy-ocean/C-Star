import asyncio
import typing as t

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import display_summary, list_runs
from cstar.orchestration.dag_runner import load_run_state
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
console = Console()


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

    try:
        status = asyncio.run(load_run_state(run_id))
        display_summary(run_id, status.open_items, status.closed_items)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
