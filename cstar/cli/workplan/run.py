import asyncio
import os
import typing as t
from pathlib import Path

import typer

from cstar.base.log import get_logger
from cstar.cli.workplan.shared import DelimitedKeyValuePair, list_runs
from cstar.execution.file_system import local_copy
from cstar.orchestration.dag_runner import build_and_run_dag
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import validate_serialized_entity
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
    var: t.Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Specify runtime variables as key-value pairs in the form `<key>=<value>`."
                "\n\nThese values will be used to dynamically populate simulation "
                "configuration at runtime."
            ),
        ),
    ] = None,
) -> None:
    """Execute a workplan.

    Specify a previously used run_id option to re-start a prior run.
    """
    if not run_id and not path:
        log.error("A run-id or workplan path is required.")
        return

    if not run_id:
        run_id = WorkplanRun.get_default_run_id(path)

        log.debug(f"Using default run-id: {run_id}")

    runtime_vars = DelimitedKeyValuePair.to_map(var or [])

    if not path:
        repo = TrackingRepository()
        wp_run = asyncio.run(repo.get_workplan_run(run_id))
        if wp_run is None:
            log.error(f"No runs with the id `{run_id}` could be found.")
            return

        runtime_vars = wp_run.runtime_vars
        # ensure the environment matches the prior run
        os.environ.update(wp_run.environment)

        path = str(wp_run.workplan_path)
        log.debug(f"Starting run-id `{run_id}` with workplan at `{path}`")

    validation_result = validate_serialized_entity(path, Workplan)
    if not validation_result.is_valid:
        print(validation_result.error_msg)
        return

    output_path = Path(output_dir) if output_dir else None

    try:
        with local_copy(path) as wp_path:
            asyncio.run(build_and_run_dag(wp_path, run_id, output_path, runtime_vars))
    except Exception:
        log.exception(f"Workplan run `{run_id}` has completed unsuccessfully")


if __name__ == "__main__":
    typer.run(run)
