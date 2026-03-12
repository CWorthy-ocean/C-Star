import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.base.log import get_logger
from cstar.cli.workplan.check import check
from cstar.execution.file_system import local_copy
from cstar.orchestration.dag_runner import build_and_run_dag

app = typer.Typer()
log = get_logger(__name__)


@app.command()
def run(
    path: t.Annotated[str, typer.Argument(help="Path to a workplan file.")],
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

    output_path = Path(output_dir) if output_dir else None

    try:
        with local_copy(path) as wp_path:
            asyncio.run(build_and_run_dag(wp_path, run_id, output_path))
        log.info(f"Workplan run `{run_id}` has completed")
    except Exception:
        log.exception(f"Workplan run `{run_id}` has completed unsuccessfully")


if __name__ == "__main__":
    typer.run(run)
