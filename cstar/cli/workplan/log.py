import typing as t

import typer

from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.cli.workplan.shared import list_runs
from cstar.execution.file_system import DirectoryManager, JobFileSystemManager

log = get_logger(__name__)
app = typer.Typer()


@app.command(name="log", help="Print the log for a workplan step.")
def workplan_log(
    run_id: t.Annotated[
        str,
        typer.Argument(
            help="The unique identifier of a specific workplan execution.",
            autocompletion=list_runs,
        ),
    ],
    step_name: t.Annotated[
        str,
        typer.Argument(
            help="The name of the step whose log should be printed.",
        ),
    ],
) -> None:
    """Print the log for a workplan step."""
    root_fsm = JobFileSystemManager(DirectoryManager.data_home() / run_id)
    step_fsm = root_fsm.get_subtask_manager(step_name)
    log_path = step_fsm.logs_dir / f"{slugify(step_name)}.log"

    if not log_path.exists():
        print(f"No log file found for step '{step_name}' in run '{run_id}'.")
        print(f"Expected path: {log_path}")
        raise typer.Exit(1)

    print(log_path.read_text(), end="")


if __name__ == "__main__":
    typer.run(workplan_log)
