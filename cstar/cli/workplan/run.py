import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.orchestration.dag_runner import build_and_run_dag

app = typer.Typer()


@app.command()
def run(
    path: t.Annotated[Path, typer.Argument(help="Path to a workplan file.")],
    output_dir: t.Annotated[
        Path, typer.Argument(help="Path to a directory where outputs will be written.")
    ],
    run_id: t.Annotated[
        str,
        typer.Option(help="The unique identifier for an execution of the workplan."),
    ] = "...",
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    os.environ["CSTAR_RUNID"] = run_id

    try:
        asyncio.run(build_and_run_dag(path, output_dir))
        print("Workplan run has completed.")
    except Exception as ex:
        print(f"Workplan run has completed unsuccessfully: {ex}")
