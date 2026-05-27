import asyncio
import typing as t

import networkx as nx
import typer
from rich.console import Console

from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.cli.workplan.shared import display_summary, list_runs
from cstar.orchestration.dag_runner import get_launcher, load_run_state
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository

log = get_logger(__name__)
app = typer.Typer()
console = Console()


@app.command(name="status", help="Retrieve the current status of a workplan.")
def status(
    run_id: t.Annotated[
        str | None,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ] = None,
    run_id_flag: t.Annotated[
        str | None,
        typer.Option(
            "--run-id",
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ] = None,
) -> None:
    """Retrieve the current status of a workplan."""
    effective_run_id = run_id or run_id_flag

    if effective_run_id is None:
        print("A run-id must be provided.")
        raise typer.Exit(1)

    repo = TrackingRepository()
    workplan_run = asyncio.run(repo.get_workplan_run(effective_run_id))

    if workplan_run is None:
        print("An unknown run-id was supplied.")
        return

    step_order: list[str] | None = None
    step_deps: dict[str, list[str]] | None = None
    step_apps: dict[str, str] | None = None
    try:
        workplan = deserialize(workplan_run.trx_workplan_path, Workplan)
        step_deps = {
            slugify(s.name): [slugify(p) for p in s.depends_on] for s in workplan.steps
        }
        step_apps = {slugify(s.name): s.application for s in workplan.steps}
        successors: dict[str, list[str]] = {slugify(s.name): [] for s in workplan.steps}
        for step in workplan.steps:
            for prereq in step.depends_on:
                successors[slugify(prereq)].append(slugify(step.name))
        step_order = list(nx.topological_sort(nx.DiGraph(successors)))
    except Exception:
        pass

    launcher = get_launcher()

    try:
        status = asyncio.run(load_run_state(effective_run_id, launcher))
        display_summary(
            effective_run_id,
            status,
            step_order=step_order,
            step_deps=step_deps,
            step_apps=step_apps,
        )
    except FileNotFoundError:  # blueprint not found.
        console.print_exception()


if __name__ == "__main__":
    typer.run(status)
