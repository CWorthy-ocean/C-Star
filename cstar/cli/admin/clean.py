import asyncio
import itertools
import shutil
import typing as t
from collections.abc import Sequence
from pathlib import Path

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.prompt import Prompt

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.common import cb_pipeline, get_from_ctxmap, set_ctxmap, set_flag
from cstar.cli.workplan.shared import colored, list_runs, preload_run
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

log = get_logger(__name__)
app = typer.Typer()
console = Console()

short_help: t.Final[str] = "Clean up leftover data and files written to disk."
help: t.Final[str] = (
    f"{short_help}. WARNING: This will execute a destructive wipe "
    "of C-Star directories. Data will be unrecoverable"
)
yes_help: t.Final[str] = (
    "Perform clean operations for all stored assets without user interaction."
)
run_help: t.Final[str] = "Clean up stored assets for a specific run"


ARG_YES: t.Final[str] = "--yes"


class CleanupAction(BaseModel):
    """A resource managed by C-Star that can be cleaned up."""

    name: str
    """The user-friendly name of the resource."""
    description: str | None = None
    """An extended description of the resource to display to a user."""
    asset_paths: list[Path]
    """The paths containing the resources to be cleaned up."""

    def mitigated(self) -> bool:
        """Return `True` when all underlying assets are not found."""
        return all(not p.exists() for p in self.asset_paths)


def get_prefect_storage_path() -> Path:
    """Return the path to the directory containing cached assets in prefect.

    Returns
    -------
    Path
    """
    return Path("~/.prefect/storage").expanduser().resolve()


def get_default_cleanup_actions() -> list[CleanupAction]:
    """Return a list of all available clean-up actions.

    Performing cleanup on these folders is _the nuclear option_ and will
    wipe out all stored state.

    Returns
    -------
    list[CleanupAction]
    """
    return list(
        sorted(
            [
                CleanupAction(
                    name="C-Star package cache",
                    description="Cached copies of previously retrieved github repositories",
                    asset_paths=[DirectoryManager.cache_home()],
                ),
                CleanupAction(
                    name="C-Star state files",
                    description="Internal C-Star state information related to run history.",
                    asset_paths=[
                        DirectoryManager.state_home(),
                        get_prefect_storage_path(),
                    ],
                ),
                CleanupAction(
                    name="C-Star Data",
                    description="All datasets and assets created during a run.",
                    asset_paths=[DirectoryManager.data_home()],
                ),
                CleanupAction(
                    name="C-Star configuration",
                    description="Local user-specific C-Star configuration files",
                    asset_paths=[DirectoryManager.config_home()],
                ),
            ],
            key=lambda x: x.name,
        )
    )


def remove_asset(action: CleanupAction) -> list[Path]:
    """Clean up the files and directories for the action.

    Returns
    -------
    list[Path]
        A list of all paths that were removed.
    """
    removed: list[Path] = []

    try:
        for path in action.asset_paths:
            if path.exists() and path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.exists() and path.is_file():
                path.unlink()

            removed.append(path)
    except Exception:
        log.exception(f"Unable to remove assets for {action.name!r}")

    return removed


async def perform_actions(actions: Sequence[CleanupAction]) -> int:
    """Perform all specified clean-up actions.

    Parameters
    ----------
    actions : Sequence[CleanupAction]
        The cleanup actions that will be processed.

    Returns
    -------
    int
        The total number of actions taken
    """
    COLOR_WHITE: t.Final[str] = "white"
    COLOR_RED: t.Final[str] = "red"
    dry_run = is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN)
    msg, color = "Removed", COLOR_RED

    if dry_run:
        msg = "Will remove"
        color = COLOR_WHITE
        removal_results = [a.asset_paths for a in actions]
    else:
        removal_coros = [
            asyncio.to_thread(remove_asset, a) for a in actions if a.asset_paths
        ]
        removal_results = await asyncio.gather(*removal_coros)

    for i, dirs in enumerate(removal_results):
        descriptor = colored(msg, color)

        if len(dirs) == 1:
            path_csv = ", ".join(str(d) for d in dirs)
            console.print(f"{descriptor} {actions[i].name!r}: {path_csv}")
        else:
            console.print(f"{descriptor} {actions[i].name!r}:")
            for d in dirs:
                console.print(f"* {d}")

    return len(list(itertools.chain.from_iterable(removal_results)))


def actions_filter_prompt(
    available: list[CleanupAction],
    yes: bool,
) -> list[CleanupAction]:
    """Perform interactive action filtering with the user.

    Parameters
    ----------
    available : list[CleanupAction]
        List of the available cleanup actions the user can select.
    yes : bool
        Pass `True` to auto-accept available actions without user interaction.

    Returns
    -------
    list[CleanupAction]
    """
    CHOICE_YES: t.Final[str] = "y"
    CHOICE_NO: t.Final[str] = "n"

    choices: t.Final[list[str]] = [CHOICE_YES, CHOICE_NO]
    selected: list[CleanupAction] = []

    for action in available:
        prompt = f"Delete {action.name!r}"
        if action.description:
            prompt = f"{prompt} ({action.description})"

        if yes:
            answer = CHOICE_YES
        else:
            answer = Prompt.ask(
                prompt,
                default=CHOICE_NO,
                choices=choices,
                case_sensitive=False,
            )

        if answer == CHOICE_NO:
            console.print(f"\t[yellow]Skipping[/yellow] {action.name!r} deletion")
            continue

        selected.append(action)

    return selected


def get_run_action(context: typer.Context, run_id: str) -> str:
    """Load run information into the typer context, if necessary.

    Parameters
    ----------
    context : typer.Context
        The typer context
    run_id : str
        The run-id value received from the user

    Returns
    -------
    str
    """
    run_id = run_id.strip()

    if not run_id:
        log.debug("Skipping run loading; run-id was not specified")
        return run_id

    preload_run(context, run_id)

    run = get_from_ctxmap(context, "run", WorkplanRun)

    tracking_repo = TrackingRepository()
    run_paths = tracking_repo.list_run_paths(run_id)

    action = CleanupAction(
        name=f"Run {run_id!r} Assets",
        description="All state information, datasets, and code assets used in the run(s)",
        asset_paths=[
            run.state_dir,
            *run_paths,
        ],
    )
    set_ctxmap(context, "action", action)

    return run_id


@app.command(
    name="clean",
    help=help,
    short_help=short_help,
)
def clean(
    context: typer.Context,
    yes: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
        ),
    ] = False,
    run_id: t.Annotated[
        str,
        typer.Option(
            "--run-id",
            help=run_help,
            callback=get_run_action,
            min=1,
            autocompletion=list_runs,
        ),
    ] = "",
    dry_run: t.Annotated[
        bool,
        typer.Option(
            ARG_DRY_RUN,
            help="Enumerate the assets that will be removed without performing any deletions",
            callback=cb_pipeline(
                set_flag(ENV_CSTAR_CLI_DRY_RUN),
            ),
            envvar=ENV_CSTAR_CLI_DRY_RUN,
        ),
    ] = False,
) -> None:
    """Clean up leftover data and files written to disk.

    Returns
    -------
    None
    """
    if not run_id:
        all_actions = get_default_cleanup_actions()
        todo = actions_filter_prompt(all_actions, yes)
    else:
        todo = actions_filter_prompt(
            [get_from_ctxmap(context, "action", CleanupAction)], yes
        )

    asyncio.run(perform_actions(todo))
