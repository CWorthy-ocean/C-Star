import asyncio
import typing as t

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import (
    display_summary,
    list_runs,
)
from cstar.orchestration.dag_runner import (
    get_launcher,
    get_status_detail_map,
    load_run_state,
)
from cstar.orchestration.orchestration import LiveWorkplan, Planner
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
console = Console()


@app.command(name="status", help="Retrieve the current status of a workplan.")
def status(
    run_id: t.Annotated[
        str,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ],
) -> None:
    """Retrieve the current status of a workplan."""
    repo = TrackingRepository()
    run = asyncio.run(repo.get_workplan_run(run_id))

    if run is None:
        print("An unknown run-id was supplied.")
        return

    launcher = get_launcher()
    wp_path = run.trx_workplan_path
    if not wp_path:
        msg = f"No live workplan for run-id `{run.run_id}` could be found."
        raise RuntimeError(msg)

    try:
        workplan = deserialize(wp_path, LiveWorkplan)

        planner = Planner(workplan)
        status = asyncio.run(load_run_state(run_id, launcher))
        lookup = get_status_detail_map(planner, status)

        display_summary(run_id, lookup)
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
