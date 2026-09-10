import asyncio
import typing as t
from collections.abc import Sequence

import typer
from rich.table import Column, Table

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import console
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

log = get_logger(__name__)
app = typer.Typer()

HELP_SHORT = "List runs started by a user."
ALL_COLUMNS: t.TypeAlias = t.Literal[
    "run-id",
    "name",
    "time",
]


def display_runs(runs: Sequence[WorkplanRun]) -> None:
    """Display a table containing all known run IDs for a user."""
    table = Table(
        Column("run-id", justify="right", style="yellow"),
        Column("workplan", justify="left", style="white"),
        Column("start time", justify="center", style="cyan"),
        row_styles=["", "dim"],
    )

    for run in runs:
        wp_path = run.workplan_path or ""
        wp = deserialize(wp_path, LiveWorkplan)
        table.add_row(run.run_id, wp.name, run.start_at.strftime("%Y-%m-%d %H:%M"))

    console.print(table)


@app.command(name="ls", help=HELP_SHORT)
def ls_runs(
    context: typer.Context,
    sort: t.Annotated[
        ALL_COLUMNS,
        typer.Option("-s", "--sort", help="Pass the column used to sort runs"),
    ],
    desc: t.Annotated[
        bool,
        typer.Option(
            "-d",
            "--descending",
            help="Set flag to reverse the sorting order to descending order.",
        ),
    ] = True,
) -> None:
    """List all runs started by a user."""
    tracking = TrackingRepository()
    runs = asyncio.run(tracking.list_latest_runs())

    if sort == "run-id":
        runs = sorted(runs, key=lambda x: x.run_id, reverse=desc)
    elif sort == "workplan":
        runs = sorted(runs, key=lambda x: x.workplan_path, reverse=desc)
    elif sort == "name":
        runs = sorted(runs, key=lambda x: x.start_at, reverse=desc)
    else:
        # default to sorting by run-id
        runs = sorted(runs, key=lambda x: x.start_at, reverse=desc)

    display_runs(runs)


if __name__ == "__main__":
    typer.run(ls_runs)
