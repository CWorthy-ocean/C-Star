import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.orchestration.dag_runner import load_dag_status

app = typer.Typer()


def display_summary(
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
    print("The remaining steps in the plan are: ")
    if open_set:
        for node in open_set:
            print(f"\t- {node}")
    else:
        print("\t[N/A]")

    print("The completed steps in the plan are: ")
    if closed_set:
        for node in closed_set:
            print(f"\t- {node}")
    else:
        print("\t[N/A]")


@app.command()
def status(
    path: t.Annotated[Path, typer.Argument(help="Path to a workplan file.")],
    run_id: t.Annotated[
        str,
        typer.Option(help="The unique identifier for an execution of the workplan."),
    ] = "...",
) -> None:
    """Retrieve the current status of a workplan."""
    os.environ["CSTAR_RUNID"] = run_id
    status = asyncio.run(load_dag_status(path))

    display_summary(status.open_items, status.closed_items)
