import asyncio

import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
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
