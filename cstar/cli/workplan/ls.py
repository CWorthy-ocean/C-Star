import asyncio
import csv
import datetime
import io
import json
import typing as t
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

import typer
from pydantic import BaseModel, ConfigDict, computed_field
from rich.console import ConsoleRenderable
from rich.table import Column, Table
from rich.text import Text

from cstar.base.env import max_concurrency
from cstar.base.log import get_logger
from cstar.cli.workplan.shared import console, load_workplans, refresh_disk_usage
from cstar.entrypoint.utils import ARG_SIZE, ARG_SIZE_HELP
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.tracking import (
    KEY_RUN_NAME,
    UNKNOWN_SIZE,
    TrackingRepository,
    WorkplanRun,
)

log = get_logger(__name__)
app = typer.Typer()

ALIAS = "list"
"""Alternate name under which the `ls` command is also registered."""
HELP_SHORT = "List runs started by a user."
HELP_ALIASED = f"{HELP_SHORT} (alias: {ALIAS})"
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
UNKNOWN_NAME: t.Final[str] = "unknown"
"""Constant value used by the system when a name cannot be retrieved."""


class ItemView(BaseModel):
    """A single run prepared for rendering in one of the output formats."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        use_attribute_docstrings=True,
    )

    run_id: str
    """The unique identifier of the run."""

    name: str
    """The resolved workplan name, or the "unknown" sentinel."""

    raw_size: int
    """The disk usage in MB, or the `UNKNOWN_SIZE` sentinel."""

    raw_start: datetime.datetime
    """The time the run started."""

    format: FORMATS
    """The output format this view will be rendered with."""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def size(self) -> str:
        """The size as a string for display (includes units)."""
        if self.raw_size == UNKNOWN_SIZE and self.format == "table":
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
    """Build the raw dataset that will be rendered in the view.

    A run's name is taken from its recorded metadata when present; otherwise
    its transformed workplan is loaded to resolve the name, and a run whose
    workplan cannot be loaded is shown as "unknown".

    Parameters
    ----------
    runs : Sequence[WorkplanRun]
        The runs to adapt for display.
    plan_cache : dict[Path, LiveWorkplan]
        Previously loaded workplans, keyed on their file path; missing
        entries are loaded from disk and added.
    format : FORMATS
        The output format the views will be rendered with.

    Returns
    -------
    Sequence[ItemView]
        One view per run, in input order.
    """
    missing = {
        r.trx_workplan_path
        for r in runs
        if KEY_RUN_NAME not in r.metadata and r.trx_workplan_path not in plan_cache
    }
    if missing:
        await load_workplans(sorted(missing), plan_cache)

    views: list[ItemView] = []
    unresolved: list[str] = []

    for run in runs:
        name = run.metadata.get(KEY_RUN_NAME)

        if not name:
            plan = plan_cache.get(run.trx_workplan_path)
            name = plan.name if plan else UNKNOWN_NAME

        if name == UNKNOWN_NAME:
            unresolved.append(run.run_id)

        views.append(
            ItemView(
                run_id=run.run_id,
                name=name,
                raw_size=run.size_mb,
                raw_start=run.start_at,
                format=format,
            )
        )

    if unresolved:
        log.debug(
            f"{len(unresolved)} run(s) shown as {UNKNOWN_NAME!r}: transformed "
            f"workplan not readable: {', '.join(unresolved)}"
        )

    return views


def table_formatter(data: Iterable[ItemView]) -> ConsoleRenderable:
    """Display run information as a table.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.

    Returns
    -------
    ConsoleRenderable
        A rich table with one row per run.
    """
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
    """Display run information as CSV.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.

    Returns
    -------
    ConsoleRenderable
        A CSV document with a header row followed by one row per run.
    """
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
    """Display run information as JSON.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.

    Returns
    -------
    ConsoleRenderable
        A JSON document with the runs under a top-level "data" key.
    """
    container = {"data": [x.model_dump(include=INCLUSIONS) for x in data]}
    document = json.dumps(container)

    return Text(document)


def filter_size(
    runs: Sequence[WorkplanRun],
    lt_filter: int | None = None,
    gt_filter: int | None = None,
) -> list[WorkplanRun]:
    """Filter runs by their recorded disk usage.

    Bounds are inclusive. Runs whose usage has not been computed (the -1
    sentinel) are never filtered out; a warning names them when any exist.

    Parameters
    ----------
    runs : Sequence[WorkplanRun]
        The runs to filter; size metadata must already be attached.
    lt_filter : int | None
        The maximum disk usage (in MB) to include in results.
    gt_filter : int | None
        The minimum disk usage (in MB) to include in results.

    Returns
    -------
    list[WorkplanRun]
        The runs within the requested bounds, in input order.
    """
    if lt_filter is None and gt_filter is None:
        return list(runs)

    results: list[WorkplanRun] = []
    warn_unsized: list[str] = []

    for run in runs:
        size = run.size_mb

        if size == UNKNOWN_SIZE:
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
    """Filter runs by their start time.

    Bounds are inclusive and are normalized to UTC before comparison; a
    naive bound is interpreted in the system local timezone.

    Parameters
    ----------
    runs : Sequence[WorkplanRun]
        The runs to filter.
    lt_filter : datetime.datetime | None
        The latest start time to include in results.
    gt_filter : datetime.datetime | None
        The earliest start time to include in results.

    Returns
    -------
    list[WorkplanRun]
        The runs within the requested bounds, in input order.
    """
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
    """Ensure the run-id filter has been converted to lower-case for comparisons.

    Parameters
    ----------
    value : str
        The raw filter value supplied by the user.

    Returns
    -------
    str
        The case-folded filter value.
    """
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


@app.command(name="ls", help=HELP_ALIASED)
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

    async def _pipeline() -> Sequence[ItemView]:
        """List, filter, optionally re-measure, and adapt runs for display."""
        async with TrackingRepository.bound(max_concurrency()) as repo:
            runs = await repo.list_latest_runs(runid_filter)

        filtered = filter_time(list(runs), time_lt_filter, time_gt_filter)

        if refresh_usage:
            paths = {r.trx_workplan_path for r in filtered}
            await load_workplans(sorted(paths), plan_cache)
            await refresh_disk_usage(filtered, plan_cache)

        filtered = filter_size(filtered, size_lt_filter, size_gt_filter)

        return await adapt_runs_to_views(filtered, plan_cache, format)

    views = asyncio.run(_pipeline())

    sorted_views = sorters[sort](views, reverse)
    content = formatters[format](sorted_views)

    console.print(content)


# `list` is an alias for `ls`: the same command under a second name, hidden
# from `--help` so the listing shows a single entry.
app.command(name=ALIAS, hidden=True, help=HELP_SHORT)(ls_runs)


if __name__ == "__main__":
    typer.run(ls_runs)
