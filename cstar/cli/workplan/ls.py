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
from rich.console import ConsoleRenderable
from rich.table import Column, Table
from rich.text import Text

from cstar.base.env import max_concurrency
from cstar.base.log import get_logger
from cstar.cli.workplan.shared import KEY_RUN_SIZE, attach_disk_usage, console
from cstar.entrypoint.utils import ARG_SIZE, ARG_SIZE_HELP
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import deserialize_all
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
EXCLUSIONS: set[str] = {"raw_size", "raw_start", "format"}
"""View fields excluded from rendered outputs."""
INCLUSIONS: set[str] = {"run_id", "name", "size", "start"}
"""View fields included in the rendered outputs."""


class ItemView(BaseModel):
    run_id: str
    name: str
    raw_size: int
    raw_start: datetime.datetime
    format: FORMATS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def size(self) -> str:
        """The size as a string for display (includes units)."""
        if self.raw_size == -1 and self.format == "table":
            return f'Run "cstar workplan status {self.run_id} {ARG_SIZE}"'

        return f"{self.raw_size}MB"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def start(self) -> str:
        """The start time as a string for display."""
        if self.format == "table":
            return self.raw_start.astimezone().strftime("%Y-%m-%d %H:%M")
        return self.raw_start.isoformat()


async def adapt_runs_to_views(
    runs: Sequence[WorkplanRun],
    plan_cache: dict[Path, LiveWorkplan],
    format: FORMATS,
) -> Sequence[ItemView]:
    """Build the raw dataset that will be rendered in the view."""

    async def _populate_cache(
        paths: list[Path], cache: dict[Path, LiveWorkplan]
    ) -> None:
        """Load workplans from disk and add them to the in-memory cache.

        Parameters
        ----------
        paths : list[Path]
            The paths to serialized workplans.
        cache : dict[Path, LiveWorkplan]
            The cached copy of loaded workplans, keyed on their file path.
        """
        limit = max_concurrency()
        workplans = await deserialize_all(paths, LiveWorkplan, limit=limit)
        for path, wp in zip(paths, workplans):
            if wp:
                cache[path] = wp
            else:
                log.debug(
                    f"Workplan at {str(path)!r} failed deserialization. Skipping cache"
                )

    missing = {
        r.trx_workplan_path for r in runs if r.trx_workplan_path not in plan_cache
    }
    await _populate_cache(list(sorted(missing)), plan_cache)

    views: list[ItemView] = []

    for run in runs:
        wp_path = run.trx_workplan_path
        name = "unknown"

        try:
            name = plan_cache[wp_path].name
        except KeyError:
            log.warning(f"The workplan path {str(wp_path)!r} was not loaded into cache")

        views.append(
            ItemView(
                run_id=run.run_id,
                name=name,
                raw_size=int(run.metadata.get(KEY_RUN_SIZE, "-1")),
                raw_start=run.start_at,
                format=format,
            )
        )
    return views


def table_formatter(data: Iterable[ItemView]) -> ConsoleRenderable:
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
    return table


def csv_formatter(data: Iterable[ItemView]) -> ConsoleRenderable:
    """Display run information as CSV."""
    output = io.StringIO()

    renderables = (x.model_dump(exclude=EXCLUSIONS) for x in data)
    writer = csv.DictWriter(
        output,
        fieldnames=sorted(INCLUSIONS),
        quoting=csv.QUOTE_STRINGS,
    )

    writer.writeheader()
    writer.writerows(renderables)
    document = output.getvalue()

    return Text(document)


def json_formatter(data: Iterable[ItemView]) -> ConsoleRenderable:
    """Display run information as JSON."""
    container = {"data": [x.model_dump(include=INCLUSIONS) for x in data]}
    document = json.dumps(container)

    return Text(document)


def filter_size(
    runs: Sequence[WorkplanRun],
    lt_filter: int | None = None,
    gt_filter: int | None = None,
) -> list[WorkplanRun]:
    if lt_filter is None and gt_filter is None:
        return list(runs)

    results: list[WorkplanRun] = []
    warn_unsized: list[str] = []

    for run in runs:
        size = int(run.metadata[KEY_RUN_SIZE])
        if size == -1:
            # never filter items with uncomputed sizes
            warn_unsized.append(run.run_id)
            results.append(run)
            continue

        if lt_filter is not None and size > lt_filter:
            continue
        if gt_filter is not None and size < gt_filter:
            continue

        results.append(run)

    if warn_unsized:
        log.warning(
            f"Runs without disk consumption calculations were not filtered: {','.join(warn_unsized)}"
        )

    return results


def filter_time(
    runs: Sequence[WorkplanRun],
    lt_filter: datetime.datetime | None = None,
    gt_filter: datetime.datetime | None = None,
) -> list[WorkplanRun]:
    if not lt_filter and not gt_filter:
        return list(runs)

    results: list[WorkplanRun] = []
    for run in runs:
        if lt_filter and run.start_at.astimezone(datetime.UTC) > lt_filter.astimezone(
            datetime.UTC
        ):
            continue
        if gt_filter and run.start_at.astimezone(datetime.UTC) < gt_filter.astimezone(
            datetime.UTC
        ):
            continue
        results.append(run)

    return results


def format_runid_filter(value: str) -> str:
    """Ensure the run-id filter has been converted to lower-case for comparisons."""
    return value.casefold()


sorters: dict[FIELD_NAMES, Callable[[Iterable[ItemView], bool], list[ItemView]]] = {
    "name": lambda runs, desc: sorted(runs, key=lambda x: x.name, reverse=desc),
    "run-id": lambda runs, desc: sorted(runs, key=lambda x: x.run_id, reverse=desc),
    "size": lambda runs, desc: sorted(runs, key=lambda x: x.raw_size, reverse=desc),
    "time": lambda runs, desc: sorted(runs, key=lambda x: x.raw_start, reverse=desc),
}


formatters = {
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
    ] = "time",
    reverse: t.Annotated[
        bool,
        typer.Option(
            "--reverse",
            help="Set flag to reverse the sort order.",
        ),
    ] = False,
    format: t.Annotated[
        FORMATS,
        typer.Option("--format", help="Pass the desired output format."),
    ] = "table",
    runid_filter: t.Annotated[
        str,
        typer.Option(
            "--run-filter",
            help="Pass a search term to match run-id",
            callback=format_runid_filter,
        ),
    ] = "",
    size_gt_filter: t.Annotated[
        int | None,
        typer.Option(
            "--min-size",
            help="Pass the minimum disk size (in MB) to include in results",
        ),
    ] = None,
    size_lt_filter: t.Annotated[
        int | None,
        typer.Option(
            "--max-size",
            help="Pass the maximum disk size (in MB) to include in results",
        ),
    ] = None,
    time_gt_filter: t.Annotated[
        datetime.datetime | None,
        typer.Option("--min-time", help="Pass the earliest date allowed in results"),
    ] = None,
    time_lt_filter: t.Annotated[
        datetime.datetime | None,
        typer.Option("--max-time", help="Pass the latest date allowed in results"),
    ] = None,
    refresh_usage: t.Annotated[
        bool,
        typer.Option(
            ARG_SIZE,
            help=(
                f"{ARG_SIZE_HELP} This operation may be slow; "
                "combine with `--run-filter` and `--[min|max]-time` for best performance."
            ),
        ),
    ] = False,
) -> None:
    """List all runs started by a user."""
    plan_cache: dict[Path, LiveWorkplan] = {}

    async def _list_runs() -> Sequence[WorkplanRun | None]:
        """Perform a max-concurrency bounded retrieval of the run list."""
        async with TrackingRepository.bound(max_concurrency()) as repo:
            return await repo.list_latest_runs(runid_filter)

    raw_runs = asyncio.run(_list_runs())
    runs = [r for r in raw_runs if r is not None]
    if dropped := len(raw_runs) - len(runs):
        log.warning(f"{dropped} run record(s) could not be read and were omitted")

    runs = filter_time(runs, time_lt_filter, time_gt_filter)

    asyncio.run(attach_disk_usage(runs, refresh=refresh_usage))
    runs = filter_size(runs, size_lt_filter, size_gt_filter)

    views = asyncio.run(adapt_runs_to_views(runs, plan_cache, format))

    views = sorters[sort](views, reverse)
    content = formatters[format](views)

    console.print(content)


if __name__ == "__main__":
    typer.run(ls_runs)
