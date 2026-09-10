import asyncio
import typing as t
from collections.abc import Sequence
from pathlib import Path

import typer
from rich.table import Column, Table

from cstar.base.log import get_logger
from cstar.base.utils import _run_cmd
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
    "size",
    "time",
]


def display_runs(runs: Sequence[WorkplanRun]) -> None:
    """Display a table containing all known run IDs for a user."""
    table = Table(
        Column("run-id", justify="right", style="yellow"),
        Column("workplan", justify="left", style="white"),
        Column("disk space", justify="left", style="magenta"),
        Column("start time", justify="center", style="cyan"),
        row_styles=["", "dim"],
    )

    for run in runs:
        wp_path = run.workplan_path or ""
        wp = deserialize(wp_path, LiveWorkplan)
        table.add_row(
            run.run_id,
            wp.name,
            run.metadata["size"],
            run.start_at.strftime("%Y-%m-%d %H:%M"),
        )

    console.print(table)


async def disk_usage(path: Path) -> str:
    """Return the size of all assets stored in a directory."""
    result = await asyncio.to_thread(_run_cmd, f"du -sm {str(path)}")
    return result.split()[0]


async def load_disk_usage(runs: Sequence[WorkplanRun]) -> None:
    disk_space = await asyncio.gather(*[disk_usage(run.output_path) for run in runs])
    sizes = [f"{size}MB" for size in disk_space]
    for i, run in enumerate(runs):
        run.metadata["size"] = sizes[i]


@app.command(name="ls", help=HELP_SHORT)
def ls_runs(
    context: typer.Context,
    sort: t.Annotated[
        ALL_COLUMNS,
        typer.Option("--sort", help="Pass the column used for sorting"),
    ] = "name",
    desc: t.Annotated[
        bool,
        typer.Option(
            "--desc",
            help="Set flag to sort in descending order.",
        ),
    ] = True,
) -> None:
    """List all runs started by a user."""
    tracking = TrackingRepository()

    runs = asyncio.run(tracking.list_latest_runs())
    asyncio.run(load_disk_usage(runs))

    if sort == "name":
        runs = sorted(runs, key=lambda x: x.start_at, reverse=desc)
    elif sort == "run-id":
        runs = sorted(runs, key=lambda x: x.run_id, reverse=desc)
    elif sort == "size":
        runs = sorted(runs, key=lambda x: x.metadata["size"], reverse=desc)
    elif sort == "time":
        runs = sorted(runs, key=lambda x: x.workplan_path, reverse=desc)

    display_runs(runs)


if __name__ == "__main__":
    typer.run(ls_runs)
