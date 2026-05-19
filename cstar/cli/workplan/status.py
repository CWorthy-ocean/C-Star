import asyncio
import typing as t

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import display_summary, list_runs
from cstar.orchestration.dag_runner import get_launcher, load_run_state
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
console = Console()


@app.command(name="status", help="Retrieve the current status of a workplan.")
def status(
    run_id: t.Annotated[
        str | None,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ] = None,
    run_id_flag: t.Annotated[
        str | None,
        typer.Option(
            "--run-id",
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ] = None,
) -> None:
    """Retrieve the current status of a workplan."""
    effective_run_id = run_id or run_id_flag

    if effective_run_id is None:
        print("A run-id must be provided.")
        raise typer.Exit(1)

    repo = TrackingRepository()
    workplan_run = asyncio.run(repo.get_workplan_run(effective_run_id))

    if workplan_run is None:
        print("An unknown run-id was supplied.")
        return

    launcher = get_launcher()

    try:
        status = asyncio.run(load_run_state(effective_run_id, launcher))
        display_summary(effective_run_id, status)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
