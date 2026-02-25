import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.cli.workplan.check import check
from cstar.orchestration.dag_runner import build_and_run_dag

app = typer.Typer()


@app.command()
def run(
    path: t.Annotated[Path, typer.Argument(help="Path to a workplan file.")],
    run_id: t.Annotated[
        str,
        typer.Option(help="The unique identifier for an execution of the workplan."),
    ],
    output_dir: t.Annotated[
        str,
        typer.Option(
            help="Override the output directory specified in the environment with this path."
        ),
    ] = "",
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    if not check(path):
        return

    try:
        output_path = Path(output_dir) if output_dir else None
        asyncio.run(build_and_run_dag(path, run_id, output_path))
        print("Workplan run has completed.")
    except Exception as ex:
        print(f"Workplan run has completed unsuccessfully: {ex!r}")


if __name__ == "__main__":
    typer.run(run)
