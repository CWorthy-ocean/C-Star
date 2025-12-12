import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.orchestration.dag_runner import load_dag_status

app = typer.Typer()


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
    asyncio.run(load_dag_status(path))
