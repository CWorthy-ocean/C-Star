import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.base.log import get_logger
from cstar.cli.workplan.check import check
from cstar.cli.workplan.shared import list_runs
from cstar.execution.file_system import is_remote_resource, local_copy
from cstar.orchestration.dag_runner import build_and_run_dag
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

app = typer.Typer()
log = get_logger(__name__)


@app.command()
def run(
    run_id: t.Annotated[
        str,
        typer.Option(
            help="The unique identifier for an execution of the workplan.",
            autocompletion=list_runs,
        ),
    ],
    path: t.Annotated[str, typer.Argument(help="Path to a workplan file.")] = "",
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
    if not run_id and not path:
        log.error("A run-id or workplan path is required.")
        return

    repo = TrackingRepository()
    if not run_id:
        if is_remote_resource(path):
            with local_copy(path) as local_path:
                run_id = WorkplanRun.get_default_run_id(local_path)
        else:
            run_id = WorkplanRun.get_default_run_id(path)
    elif not path:
        workplan_run = asyncio.run(repo.get_workplan_run(run_id))
        if workplan_run is None:
            log.error(f"No runs with the id `{run_id}` could be found.")
            return

        for k, v in workplan_run.environment.items():
            os.environ[k] = v

        path = str(workplan_run.workplan_path)

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
