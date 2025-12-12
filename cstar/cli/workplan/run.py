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
        asyncio.run(build_and_run_dag(path))
        print("Workplan run has completed.")
    except Exception as ex:
        print(f"Workplan run has completed unsuccessfully: {ex}")


def main() -> None:
    """Entrypoint for the run-workplan command."""
    app()


if __name__ == "__main__":
    main()
