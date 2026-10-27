import asyncio
import csv
import datetime
import io
import json
import typing as t
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

import typer
from pydantic import BaseModel, ConfigDict, Field, computed_field
from rich.console import ConsoleRenderable
from rich.table import Column, Table
from rich.text import Text

from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.log import get_logger
from cstar.base.utils import MAX_CONCURRENCY
from cstar.cli.workplan.shared import console, load_workplans, refresh_disk_usage
from cstar.entrypoint.utils import (
    ARG_ALL,
    ARG_ALL_RUNS_HELP,
    ARG_FORMAT,
    ARG_FORMAT_HELP,
    ARG_SHARED,
    ARG_SHARED_HELP,
    ARG_SIZE,
    ARG_SIZE_HELP,
)
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.index import (
    ManifestRecord,
    Origin,
    default_index,
    untracked_locations,
    where_kept,
    where_tracked,
)
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.tracking import (
    UNKNOWN_SIZE,
    TrackingRepository,
    WorkplanRun,
)

if t.TYPE_CHECKING:
    from cstar.orchestration.index import RunRecord

log = get_logger(__name__)
app = typer.Typer()

_TRecord = t.TypeVar("_TRecord", bound="RunRecord")

ALIAS = "list"
"""Alternate name under which the `ls` command is also registered."""
HELP_SHORT = "List runs started by a user."
HELP_ALIASED = f"{HELP_SHORT} (alias: {ALIAS})"
HELP_LONG = f"""\
{HELP_ALIASED}

Lists the runs started on this machine. {ARG_ALL} also lists the runs that only
a manifest names, in the data home and in the project's runs area, and
{ARG_SHARED} lists the project's runs area alone; both scan the run directories
and add a `where` column: `local` for a run started here, `local, untracked`
for one in the data home that was not, `project` for a run kept in the project
area and `project, linked` for a link there to a run kept elsewhere. A run
attached from elsewhere is listed with the runs started here, as `attached`, or
`attached, read-only` when nothing may write into it. The disk space of a run
that was not started here is not known.
"""
FIELD_NAMES: t.TypeAlias = t.Literal[
    "run-id",
    "name",
    "size",
    "time",
]
"""Fields available for use in listing, filtering, and sorting operations."""
FORMATS: t.TypeAlias = t.Literal["table", "csv", "json"]
"""Output format options."""
UNWRAPPED_FORMATS: t.Final[frozenset[FORMATS]] = frozenset({"csv", "json"})
"""Machine-readable formats, printed without wrapping lines at the console width."""
NOT_MEASURED: t.Final[str] = "not measured"
"""Table text for the size of a run whose disk usage has not been measured."""
INCLUSIONS: set[str] = {"run_id", "name", "size", "start"}
"""View fields included in the rendered outputs."""
WHERE_INCLUSIONS: set[str] = INCLUSIONS | {"where"}
"""View fields included when the listing says where each run is kept."""
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

    where: str = Field(default="")
    """Where the run is kept; listed only when the command looks beyond the runs started here."""

    tracked: bool = Field(default=True)
    """Whether this machine tracks the run: only then can its disk usage be measured."""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def size(self) -> str | None:
        """The size as a string for display (includes units).

        `None` in machine formats when the disk usage has not been measured.
        """
        if self.raw_size == UNKNOWN_SIZE:
            if self.format != "table":
                return None
            if not self.tracked:
                return NOT_MEASURED
            return f"{NOT_MEASURED} (cstar run status {self.run_id} {ARG_SIZE})"

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

    A run's name is the one its manifest holds; for a run whose name is not
    known that way, its transformed workplan is loaded to resolve the name, and
    a run whose workplan cannot be found or loaded is shown as "unknown".

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
    missing: set[Path] = set()
    trx_paths: dict[str, Path] = {}
    for r in runs:
        if r.name or (path := r.try_trx_workplan_path()) is None:
            continue
        trx_paths[r.run_id] = path
        if path not in plan_cache:
            missing.add(path)
    if missing:
        await load_workplans(sorted(missing), plan_cache)

    views: list[ItemView] = []
    unresolved: list[str] = []

    for run in runs:
        name = run.name

        if not name:
            plan = (
                plan_cache.get(trx_paths[run.run_id])
                if run.run_id in trx_paths
                else None
            )
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
                where=where_tracked(run),
            )
        )

    if unresolved:
        log.debug(
            f"{len(unresolved)} run(s) shown as {UNKNOWN_NAME!r}: transformed "
            f"workplan not readable: {', '.join(unresolved)}"
        )

    return views


def find_other_runs(
    runid_filter: str, tracked: Sequence[WorkplanRun], *, shared_only: bool
) -> list[ManifestRecord]:
    """Find the runs to list that this machine does not track.

    Scans the run directories, once per process.

    Parameters
    ----------
    runid_filter : str
        The case-folded run-id prefix to match.
    tracked : Sequence[WorkplanRun]
        Every run tracked on this machine that the listing is of, before any
        filter but the run-id prefix drops one.
    shared_only : bool
        Whether to list the project's runs area alone, with no regard to what
        is tracked.

    Returns
    -------
    list[ManifestRecord]
        The runs in the index, the user's own before the project's.
    """
    locations = [
        loc
        for loc in default_index().locations()
        if loc.run_id.casefold().startswith(runid_filter)
    ]
    if shared_only:
        locations = [loc for loc in locations if loc.origin is Origin.project]
    else:
        locations = untracked_locations(locations, tracked)
    return [ManifestRecord(loc) for loc in locations]


def adapt_locations_to_views(
    records: Sequence[ManifestRecord], format: FORMATS
) -> list[ItemView]:
    """Build the views of runs that this machine does not track.

    Parameters
    ----------
    records : Sequence[ManifestRecord]
        The runs to adapt for display.
    format : FORMATS
        The output format the views will be rendered with.

    Returns
    -------
    list[ItemView]
        One view per run, in input order. A run's disk usage is not known.
    """
    return [
        ItemView(
            run_id=record.run_id,
            name=record.location.name or UNKNOWN_NAME,
            raw_size=UNKNOWN_SIZE,
            raw_start=record.start_at,
            format=format,
            where=where_kept(record.location),
            tracked=False,
        )
        for record in records
    ]


def table_formatter(
    data: Iterable[ItemView], *, with_where: bool = False
) -> ConsoleRenderable:
    """Display run information as a table.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.
    with_where : bool
        Whether to add a column saying where each run is kept.

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
    if with_where:
        table.add_column("where", justify="left", style="green")

    for datum in data:
        cells = [datum.run_id, datum.name, datum.size, datum.start]
        table.add_row(*cells, *([datum.where] if with_where else []))
    return table


def csv_formatter(
    data: Iterable[ItemView], *, with_where: bool = False
) -> ConsoleRenderable:
    """Display run information as CSV.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.
    with_where : bool
        Whether to add a column saying where each run is kept.

    Returns
    -------
    ConsoleRenderable
        A CSV document with a header row followed by one row per run.
    """
    output = io.StringIO()

    fields = WHERE_INCLUSIONS if with_where else INCLUSIONS
    renderables = (x.model_dump(include=fields) for x in data)
    writer = csv.DictWriter(
        output,
        fieldnames=sorted(fields),
        quoting=csv.QUOTE_STRINGS,
    )

    writer.writeheader()
    writer.writerows(renderables)
    document = output.getvalue()

    return Text(document)


def json_formatter(
    data: Iterable[ItemView], *, with_where: bool = False
) -> ConsoleRenderable:
    """Display run information as JSON.

    Parameters
    ----------
    data : Iterable[ItemView]
        The run views to render.
    with_where : bool
        Whether to add a field saying where each run is kept.

    Returns
    -------
    ConsoleRenderable
        A JSON document with the runs under a top-level "data" key.
    """
    fields = WHERE_INCLUSIONS if with_where else INCLUSIONS
    container = {"data": [x.model_dump(include=fields) for x in data]}
    document = json.dumps(container)

    return Text(document)


def filter_size(
    runs: Sequence[WorkplanRun],
    lt_filter: int | None = None,
    gt_filter: int | None = None,
    *,
    unmeasured: Sequence[str] = (),
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
    unmeasured : Sequence[str]
        The run-ids of listed runs that are not in `runs` and whose usage is
        not known, which are never filtered out either; named in the warning.

    Returns
    -------
    list[WorkplanRun]
        The runs within the requested bounds, in input order.
    """
    if lt_filter is None and gt_filter is None:
        return list(runs)

    results: list[WorkplanRun] = []
    warn_unsized: list[str] = list(unmeasured)

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
    runs: Sequence[_TRecord],
    lt_filter: datetime.datetime | None = None,
    gt_filter: datetime.datetime | None = None,
) -> list[_TRecord]:
    """Filter runs by their start time.

    Bounds are inclusive and are normalized to UTC before comparison; a
    naive bound is interpreted in the system local timezone.

    Parameters
    ----------
    runs : Sequence[RunRecord]
        The runs to filter.
    lt_filter : datetime.datetime | None
        The latest start time to include in results.
    gt_filter : datetime.datetime | None
        The earliest start time to include in results.

    Returns
    -------
    list[RunRecord]
        The runs within the requested bounds, in input order.
    """
    if not lt_filter and not gt_filter:
        return list(runs)

    results: list[_TRecord] = []
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


@app.command(name="ls", help=HELP_LONG, short_help=HELP_ALIASED)
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
        typer.Option(ARG_FORMAT, help=ARG_FORMAT_HELP),
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
    shared: t.Annotated[
        bool,
        typer.Option(ARG_SHARED, help=ARG_SHARED_HELP),
    ] = False,
    all_runs: t.Annotated[
        bool,
        typer.Option(ARG_ALL, help=ARG_ALL_RUNS_HELP),
    ] = False,
) -> None:
    """List all runs started by a user."""
    plan_cache: dict[Path, LiveWorkplan] = {}

    # both flags: every run there is
    only_shared = shared and not all_runs
    if only_shared and DirectoryManager.project_runs_home() is None:
        msg = (
            "There is no project's runs area to list: set "
            f"{ENV_CSTAR_PROJECT_HOME} (or $PROJECT), whose <project>/cstar/runs "
            "directory holds the shared runs."
        )
        raise typer.BadParameter(msg, param_hint=ARG_SHARED)

    scan = shared or all_runs

    async def _pipeline() -> Sequence[ItemView]:
        """List, filter, optionally re-measure, and adapt runs for display."""
        tracked: list[WorkplanRun] = []
        if not only_shared:
            async with TrackingRepository.bound(MAX_CONCURRENCY) as repo:
                tracked = list(await repo.list_latest_runs(runid_filter))

        others = (
            filter_time(
                find_other_runs(runid_filter, tracked, shared_only=only_shared),
                time_lt_filter,
                time_gt_filter,
            )
            if scan
            else []
        )

        filtered = filter_time(tracked, time_lt_filter, time_gt_filter)

        if refresh_usage:
            paths = {
                path
                for r in filtered
                if (path := r.try_trx_workplan_path()) is not None
            }
            await load_workplans(sorted(paths), plan_cache)
            await refresh_disk_usage(filtered, plan_cache)

        filtered = filter_size(
            filtered,
            size_lt_filter,
            size_gt_filter,
            unmeasured=[record.run_id for record in others],
        )

        return [
            *await adapt_runs_to_views(filtered, plan_cache, format),
            *adapt_locations_to_views(others, format),
        ]

    views = asyncio.run(_pipeline())

    sorted_views = sorters[sort](views, reverse)
    content = formatters[format](sorted_views, with_where=scan)

    console.print(content, soft_wrap=format in UNWRAPPED_FORMATS)


# `list` is an alias for `ls`: the same command under a second name, hidden
# from `--help` so the listing shows a single entry.
app.command(name=ALIAS, hidden=True, help=HELP_SHORT)(ls_runs)


if __name__ == "__main__":
    typer.run(ls_runs)
