import asyncio
import csv
import datetime
import io
import json
import typing as t
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

import typer
from pydantic import BaseModel, computed_field
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
FIELD_NAMES: t.TypeAlias = t.Literal[
    "run-id",
    "name",
    "size",
    "time",
]
"""Fields available for use in listing, filtering, and sorting operations."""
FORMATS: t.TypeAlias = t.Literal["table", "csv", "json"]
"""Output format options."""
EXCLUSIONS: set[str] = {"raw_size", "raw_start"}
"""View fields excluded from rendered outputs."""
INCLUSIONS: set[str] = {"run_id", "name", "size", "start"}
"""View fields included in the rendered outputs."""


class ItemView(BaseModel):
    run_id: str
    name: str
    raw_size: int
    raw_start: datetime.datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def size(self) -> str:
        """The size as a string for display (includes units)."""
        return f"{self.raw_size}MB"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def start(self) -> str:
        """The start time as a string for display."""
        return self.raw_start.strftime("%Y-%m-%d %H:%M")


def adapt_runs_to_views(
    runs: Sequence[WorkplanRun],
    plan_cache: dict[Path, LiveWorkplan],
) -> Iterable[ItemView]:
    """Build the raw dataset that will be rendered in the view."""
    for run in runs:
        wp_path = run.workplan_path
        if not wp_path.exists():
            console.print(f"Workplan not found at {str(wp_path)!r}. Skipping")
            continue

        wp = plan_cache.get(wp_path, deserialize(wp_path, LiveWorkplan))
        yield ItemView(
            run_id=run.run_id,
            name=wp.name,
            raw_size=int(run.metadata["size"]),
            raw_start=run.start_at,
        )


def table_formatter(data: Iterable[ItemView]) -> None:
    """Display run information as a table."""
    table = Table(
        Column("run-id", justify="right", style="yellow"),
        Column("workplan", justify="left", style="white"),
        Column("disk space", justify="left", style="magenta"),
        Column("start time", justify="center", style="cyan"),
        row_styles=["", "dim"],
    )

    for datum in data:
        table.add_row(
            datum.run_id,
            datum.name,
            datum.size,
            datum.start,
        )
    console.print(table)


def csv_formatter(data: Iterable[ItemView]) -> None:
    """Display run information as CSV."""
    output = io.StringIO()

    renderables = (x.model_dump(exclude=EXCLUSIONS) for x in data)
    writer = csv.DictWriter(output, fieldnames=INCLUSIONS, quoting=csv.QUOTE_STRINGS)

    writer.writerows(renderables)
    document = output.getvalue()

    console.print(document)


def json_formatter(data: Iterable[ItemView]) -> None:
    """Display run information as JSON."""
    container = {"data": [x.model_dump(include=INCLUSIONS) for x in data]}
    document = json.dumps(container)

    console.print(document)


async def disk_usage(path: Path) -> str:
    """Return the size of all assets stored in a directory."""
    result = await asyncio.to_thread(_run_cmd, f"du -sm {str(path)}")
    return result.split()[0]


async def get_run_disk_usage(runs: Sequence[WorkplanRun]) -> None:
    disk_space = await asyncio.gather(*[disk_usage(run.output_path) for run in runs])
    sizes = [size for size in disk_space]
    for i, run in enumerate(runs):
        run.metadata["size"] = sizes[i]


def get_sorters() -> dict[
    FIELD_NAMES, Callable[[Iterable[WorkplanRun], bool], list[WorkplanRun]]
]:
    sorters: dict[
        FIELD_NAMES, Callable[[Iterable[WorkplanRun], bool], list[WorkplanRun]]
    ] = {
        "name": lambda runs, desc: sorted(runs, key=lambda x: x.start_at, reverse=desc),
        "run-id": lambda runs, desc: sorted(runs, key=lambda x: x.run_id, reverse=desc),
        "size": lambda runs, desc: sorted(
            runs, key=lambda x: x.metadata["size"], reverse=desc
        ),
        "time": lambda runs, desc: sorted(runs, key=lambda x: x.start_at, reverse=desc),
    }
    return sorters


def get_renderers() -> dict[FORMATS, Callable[[Iterable[ItemView]], None]]:
    return {
        "json": json_formatter,
        "csv": csv_formatter,
        "table": table_formatter,
    }


@app.command(name="ls", help=HELP_SHORT)
def ls_runs(
    context: typer.Context,
    sort: t.Annotated[
        FIELD_NAMES,
        typer.Option("--sort", help="Pass the column used for sorting"),
    ] = "name",
    desc: t.Annotated[
        bool,
        typer.Option(
            "--desc",
            help="Set flag to sort in descending order.",
        ),
    ] = True,
    format: t.Annotated[
        FORMATS,
        typer.Option("--format", help="Pass the desired output format."),
    ] = "table",
) -> None:
    """List all runs started by a user."""
    plan_cache: dict[Path, LiveWorkplan] = {}
    tracking = TrackingRepository()
    sorters = get_sorters()
    renderers = get_renderers()
    renderer = renderers[format]

    runs = asyncio.run(tracking.list_latest_runs())
    asyncio.run(get_run_disk_usage(runs))

    runs = sorters[sort](runs, desc)
    views = adapt_runs_to_views(runs, plan_cache)
    renderer(views)


if __name__ == "__main__":
    typer.run(ls_runs)
