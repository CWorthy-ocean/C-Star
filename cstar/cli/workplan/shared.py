import asyncio
import typing as t

from rich.console import Console
from rich.table import Column, Table

from cstar.orchestration.tracking import TrackingRepository

console = Console()


def list_runs(incomplete: str) -> list[tuple[str, str]]:
    """Retrieve a list of all recorded run-ids.

    Parameters
    ----------
    incomplete : str
        Any value from the user is provided to autocompletion.

    Returns
    -------
    t.Iterable[str]
    """
    repo = TrackingRepository()
    run_list = asyncio.run(repo.list_latest_runs(incomplete))

    if not run_list and incomplete:
        return [(incomplete, "no results found")]
    elif not run_list:
        return [("run-id", "no results found")]

    return [(r.run_id, f"Workplan path: {r.workplan_path}") for r in run_list if r]


def display_summary(
    run_id: str,
    open_set: t.Iterable[str] | None,
    closed_set: t.Iterable[str] | None,
) -> None:
    """Display a summary describing the current state of
    a DAG executed by the orchestrator.

    Parameters
    ----------
    open_set : Iterable[str] | None
        The names of jobs that are unstarted or incomplete.
    open_set : Iterable[str] | None
        The names of jobs that have completed.
    """
    lookup = {k: 1 for k in closed_set or []}
    lookup.update({k: 0 for k in open_set or []})

    table = Table(
        Column(header="Step", justify="right"),
        Column(header="Incomplete", justify="center"),
        Column(header="Complete", justify="center"),
        title=f"Run [yellow]{run_id}[/yellow] Results",
        show_lines=True,
        padding=(0, 1),  # 0 pad T/B, 1 pad L/R
        pad_edge=False,
    )

    for task_name, is_complete in sorted(lookup.items()):
        RED_CHECK = "[red]:heavy_check_mark:"
        GREEN_CHECK = "[green]:heavy_check_mark:"

        table.add_row(
            task_name,
            RED_CHECK if not is_complete else "",
            GREEN_CHECK if is_complete else "",
        )

    num_closed = sum(lookup.values())
    num_open = len(lookup) - num_closed

    table.add_row("[magenta]Total", f"[red]{num_open}", f"[green]{num_closed}")

    console.print(table)
