import typing as t
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait

import typer
from rich.live import Live
from rich.prompt import Confirm
from rich.table import Table

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_YES,
    console,
)
from cstar.cli.common import (
    set_flag,
)
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import (
    ArtifactCache,
    UsageReport,
)

log = get_logger(__name__)
app = typer.Typer()


ARG_GC: t.Final[str] = "--garbage"
gc_help: t.Final[str] = (
    "Pass the number of days to be used as a stale indicator. All resources unused in that period are removed."
)

command_help: t.Final[str] = "Manually remove artifacts from the cache."
yes_help: t.Final[str] = "Perform user-level deletions without confirmation."


class CleanupStatusDict(t.TypedDict):
    status: str
    report: UsageReport


statuses: dict[str, CleanupStatusDict] = {}
live: Live | None = None


def run_garbage_collector(cache: ArtifactCache, report: UsageReport) -> UsageReport:
    """Delete the artifact identified in the usage report."""
    cache.gc([report])
    # import random
    # import time
    # time.sleep(random.randint(1, 4))
    return report


def generate_gc_status_table(
    reports: Sequence[UsageReport],
    statuses: dict[str, CleanupStatusDict],
) -> Table:

    table = Table()
    table.add_column("Artifact")
    table.add_column("Size (MB)")
    table.add_column("Status")

    for report in reports:
        table.add_row(
            report.name, str(report.size_bytes), statuses[report.name]["status"]
        )

    return table


def generate_gc_summary(reports: list[UsageReport], action: str) -> str:
    """Generate a summary of the garbage collection operation for display to user.

    Parameters
    ----------
    reports : list[UsageReport]
        The reports to summarize
    action : str
        Description of current action

    Returns
    -------
    str
    """
    size_mb = sum(r.size_bytes for r in reports) / 1024
    num_reclaimed = len(reports)

    return f"{size_mb}MB {action} {num_reclaimed} artifacts"


def generate_gc_prompt(report: UsageReport) -> str:
    """Generate a summary of the garbage collection operation for display to user.

    Parameters
    ----------
    report : UsageReport
        The report to display an action prompt for

    Returns
    -------
    str
    """
    size_mb = report.size_bytes / 1024
    return f"{report.name!r} ({size_mb}MB) last used on {report.last_used_at}. Delete?"


def on_deleting(report: UsageReport) -> UsageReport:
    global statuses
    global live
    statuses[report.name]["status"] = "in progresss"

    wip = [x["report"] for x in statuses.values() if x["status"]]
    if live:
        live.update(generate_gc_status_table(wip, statuses))
    return report


def run_gc_with_hooks(
    func: Callable[[ArtifactCache, UsageReport], UsageReport],
    cache: ArtifactCache,
    report: UsageReport,
) -> UsageReport:
    """Provide an on-start hook for action processed by threadpool."""
    on_deleting(report)
    return func(cache, report)


def on_deleted(future: Future[UsageReport]) -> object:
    global statuses
    global live

    report = future.result()
    statuses[report.name]["status"] = "deleted"

    wip = [x["report"] for x in statuses.values() if x["status"]]
    if live:
        live.update(generate_gc_status_table(wip, statuses))
    return report


def collect_garbage(age_limit: int, confirm_all: bool, cache: ArtifactCache) -> None:
    """Perform garbage collection on unused items stored in the cache.

    Parameters
    ----------
    age_limit : int
        The number of unused days indicating a stale item.
    confirm_all : bool
        Flag indicating that no confirmation dialogs should be displayed.
    cache : ArtifactCache
        The artifact cache
    """
    reports = cache.gc_candidates(age_limit)
    # reports = [
    #     UsageReport(
    #         name=f"item {i}",
    #         size_bytes=100,
    #         last_used_at="2026-08-20",
    #         idle_days=99,
    #         reference_total=1,
    #         recent_runs=[],
    #         promoted_from_run_id=None,
    #     )
    #     for i in range(15)
    # ]

    if not reports:
        console.print(f"No artifacts found unused for {age_limit} or more days")
        raise typer.Exit(0)

    console.print(generate_gc_summary(reports, "consumed by"))

    global statuses

    for report in reports:
        statuses[report.name] = {"status": "", "report": report}

    if not confirm_all:
        prompt = f"Delete {len(reports)} stale artifacts without reviewing?"
        confirm_all = Confirm.ask(prompt)

    executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="artifact-gc")
    pending: set[Future[UsageReport]] = set()

    for report in reports:
        if confirm_all or Confirm.ask(generate_gc_prompt(report), default=False):
            statuses[report.name] = {"status": "ready", "report": report}
            future = executor.submit(
                run_gc_with_hooks,
                run_garbage_collector,
                cache,
                report,
            )
            future.add_done_callback(on_deleted)
            pending.add(future)

    global live
    live = Live()

    with live:
        wip = [x["report"] for x in statuses.values() if x["status"]]
        live.update(generate_gc_status_table(wip, statuses))

        for report in reports:
            wip = [x["report"] for x in statuses.values() if x["status"]]
            t = generate_gc_status_table(wip, statuses)
            live.update(t)

        _, pending = wait(pending, timeout=0.25)
        while pending:
            wip = [x["report"] for x in statuses.values() if x["status"]]
            t = generate_gc_status_table(wip, statuses)
            live.update(t)
            _, pending = wait(pending, timeout=0.25)

        executor.shutdown(wait=True)

    completed = [x["report"] for x in statuses.values() if x["status"]]
    summary = generate_gc_summary(list(completed), "reclaimed from")
    console.print(summary)

    raise typer.Exit(0)


def check_age_limit(ctx: typer.Context, value: int) -> int:
    """Verify a non-negative integer is received."""
    if value < 1:
        msg = "A positive age limit is required"
        typer.BadParameter(msg, param_hint="age_limit")
    return value


@app.command(
    name="gc",
    help=command_help,
)
def collect(
    age_limit: t.Annotated[
        int,
        typer.Option(
            ARG_GC,
            help=gc_help,
            is_eager=True,
            callback=check_age_limit,
        ),
    ] = 90,
    confirm_all: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help=ARG_VERBOSE_HELP,
            callback=set_flag(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Manually remove artifacts from the cache."""
    cache = get_artifact_cache()
    collect_garbage(age_limit, confirm_all, cache)
    raise typer.Exit(0)


if __name__ == "__main__":
    app()
