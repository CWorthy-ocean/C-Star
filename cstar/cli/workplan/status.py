import asyncio
import os
import typing as t

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import display_summary, list_runs
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
