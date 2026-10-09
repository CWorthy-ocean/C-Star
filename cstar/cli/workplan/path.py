import shlex
import typing as t

import typer

from cstar.cli.common import get_from_ctxmap
from cstar.cli.workplan.shared import (
    RunRecordArgument,
    autocomplete_step_list,
    preload_step,
    preload_workplan,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.tracking import WorkplanRun

if t.TYPE_CHECKING:
    from pathlib import Path

app = typer.Typer()

COMMAND_PATH: t.Final[str] = "path"
"""Name of the command that prints the directory of a run or step."""
COMMAND_CD: t.Final[str] = "cd"
"""Name of the command a shell function intercepts to change directory."""

HELP_PATH_SHORT = "Print the directory of a workplan run or step."
HELP_PATH_LONG = f"""\
{HELP_PATH_SHORT}

The path is the only output on stdout, so it composes with other commands,
e.g. `cd "$(cstar workplan path <run-id>)"`. Without a `step_name`, the
directory is the run's output directory. Failures are reported on stderr with
a non-zero exit code.
"""

HELP_CD_SHORT = "Change the shell directory to a workplan run or step."
HELP_CD_LONG = f"""\
{HELP_CD_SHORT}

Requires the shell function written by `cstar env shell-init`: a process
cannot change the directory of the shell that started it. Without the
function, this command prints how to set it up and exits with a non-zero code.
"""

CD_GUIDANCE: t.Final[str] = """\
cstar cannot change your shell's directory by itself. To go there now, run:
    cd {directory}
To make `cstar workplan cd` work, save the shell function once and load it:
    cstar env shell-init zsh > ~/.cstar-shell.zsh
    source ~/.cstar-shell.zsh
then add the `source` line to ~/.zshrc. For bash, use `shell-init bash` and ~/.bashrc."""
"""Message printed to stderr when `cd` runs without the shell function installed."""


def preload_optional_step(context: typer.Context, step_name: str) -> str:
    """Resolve the step when one is supplied; an empty value selects the run itself.

    Parameters
    ----------
    context : typer.Context
        The typer context.
    step_name : str
        The user-supplied step-name or an empty string.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        - Raised when the transformed workplan cannot be deserialized
        - Raised when the step-name cannot be found in the target workplan.
    """
    if not step_name:
        return step_name

    preload_workplan(context, str(context.params.get("run_id", "")))
    return preload_step(context, step_name)


OptionalStepArgument = t.Annotated[
    str,
    typer.Argument(
        help="The name of a step; omit to select the run's own directory.",
        autocompletion=autocomplete_step_list,
        callback=preload_optional_step,
    ),
]
"""Optional step argument: preloads the transformed workplan and step when supplied."""


def _target_directory(context: typer.Context, step_name: str) -> "Path":
    """Locate the directory of the selected step, or of the run when no step is selected.

    The run record owns the run directory and the transformed workplan owns each
    step's directory, so neither is recomputed here.

    Parameters
    ----------
    context : typer.Context
        The typer context with the run and, when `step_name` is set, the step preloaded.
    step_name : str
        The user-supplied step-name or an empty string.

    Returns
    -------
    Path
        An existing directory.

    Raises
    ------
    typer.Exit
        With code 1 if the directory does not exist.
    """
    run = get_from_ctxmap(context, "run", WorkplanRun)

    if step_name:
        step = get_from_ctxmap(context, "live_step", LiveStep)
        directory, subject = (
            step.working_dir,
            f"step {step_name!r} of run {run.run_id!r}",
        )
    else:
        directory, subject = run.output_path, f"run {run.run_id!r}"

    if not directory.is_dir():
        typer.echo(f"No directory exists for {subject}: {directory}", err=True)
        raise typer.Exit(1)

    return directory


@app.command(name=COMMAND_PATH, help=HELP_PATH_LONG, short_help=HELP_PATH_SHORT)
def workplan_path(
    context: typer.Context,
    run_id: RunRecordArgument,
    step_name: OptionalStepArgument = "",
) -> None:
    """Print the directory of a workplan run or step."""
    typer.echo(str(_target_directory(context, step_name)))


@app.command(name=COMMAND_CD, help=HELP_CD_LONG, short_help=HELP_CD_SHORT)
def workplan_cd(
    context: typer.Context,
    run_id: RunRecordArgument,
    step_name: OptionalStepArgument = "",
) -> None:
    """Explain how to change the shell directory to a workplan run or step."""
    directory = _target_directory(context, step_name)
    guidance = CD_GUIDANCE.format(directory=shlex.quote(str(directory)))
    typer.echo(guidance, err=True)
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
