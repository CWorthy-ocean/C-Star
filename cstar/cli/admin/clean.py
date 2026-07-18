import abc
import asyncio
import shutil
import typing as t
from abc import ABC
from collections.abc import Sequence
from enum import IntEnum, auto
from pathlib import Path

import typer
from pydantic import BaseModel, Field, PrivateAttr
from rich.console import Console
from rich.prompt import Prompt

from cstar.base.env import ENV_CSTAR_CLI_DRY_RUN, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.common import (
    cb_pipeline,
    set_env,
    set_flag,
)
from cstar.cli.workplan.shared import colored, list_runs
from cstar.entrypoint.utils import ARG_DRY_RUN
from cstar.execution.file_system import DirectoryManager, StateDirectoryManager
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.tracking import TrackingRepository

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


class CleanupStatus(IntEnum):
    """Describes the status of a cleanup action."""

    READY = auto()
    """The cleanup action has not been evaluated."""
    DONE = auto()
    """The cleanup was successfully completed."""
    SKIPPED = auto()
    """The cleanup action was determined to be unnecessary."""
    INCOMPLETE = auto()
    """The cleanup action was not completed successfully."""


class CleanupResult(BaseModel):
    """Describes the result of executing a cleanup action."""

    name: str
    """The name of the action producing the result."""

    ledger: dict[str, tuple[CleanupStatus, str]] = Field(
        default_factory=dict, init=False
    )
    """Mapping from action identifier to a tuple containing the status and,
    if necessary, an error description.
    """

    def record(self, key: str, status: CleanupStatus, error: str | None = None) -> None:
        """Record a cleanup task into the ledger."""
        self.ledger[key] = status, error or ""

    @property
    def status(self) -> CleanupStatus:
        if not self.ledger:
            return CleanupStatus.READY

        statuses = {x[0] for x in self.ledger.values()}
        if {CleanupStatus.INCOMPLETE, CleanupStatus.READY}.intersection(statuses):
            return CleanupStatus.INCOMPLETE

        return CleanupStatus.DONE

    def display(self) -> str:
        details: list[str] = []

        for k, (status, error) in self.ledger.items():
            view = f"{status.name}: {k}"
            if error:
                view = f"{status.name}: {k}\n{error}"
            details.append(view)

        csv = "\n".join(f"* {item}" for item in details)
        return f"{self.name}\n{csv}"


class CleanupAction(BaseModel, ABC):
    """A resource managed by C-Star that can be cleaned up."""

    name: str
    """The user-friendly name of the resource."""
    description: str | None = None
    """An extended description of the resource to display to a user."""
    _results: CleanupResult | None = PrivateAttr(default=None)
    """The results of executing the action."""

    @abc.abstractmethod
    def execute(self) -> CleanupResult:
        """Perform the cleanup behavior for an associated resource.

        Returns
        -------
        `True` if cleanup was completed (or unnecessary), `False` otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def mitigated(self) -> bool:
        """Return `True` when all underlying assets are not found."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def state(self) -> CleanupResult:
        """Return the results of executing the action.

        When the action has not been executed, all tasks are considered READY.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def display(self) -> str: ...


class FileSystemCleanupAction(CleanupAction):
    """A file-system resource managed by C-Star that can be cleaned up."""

    asset_paths: list[Path] = Field(default_factory=list[Path], min_length=1)
    """The paths containing the resources to be cleaned up."""

    def execute(self) -> CleanupResult:
        """Perform the cleanup behavior for an associated resource.

        Returns
        -------
        `True` if cleanup was completed (or unnecessary), `False` otherwise.
        """
        dry_run = is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN)
        results = CleanupResult(name=self.name)
        self._results = results

        asset_iter = iter(self.asset_paths)
        while asset := next(asset_iter, None):
            asset_key = str(asset)
            state = asset_key, CleanupStatus.READY, ""
            if not asset.exists():
                results.record(asset_key, CleanupStatus.SKIPPED, "")
                continue

            asset_type = "file" if asset.is_file() else "directory"

            try:
                if not dry_run:
                    if asset_type == "directory":
                        shutil.rmtree(asset)
                    elif asset_type == "file":
                        asset.unlink()
                    state = asset_key, CleanupStatus.DONE, ""
            except Exception:
                msg = f"An error occurred cleaning up {asset_type}: {asset}"
                state = asset_key, CleanupStatus.INCOMPLETE, msg
                log.exception(msg)
            finally:
                results.record(*state)

        self._results = results
        return results

    def mitigated(self) -> bool:
        """Return `True` when all underlying assets are not found."""
        return all(not p.exists() for p in self.asset_paths)

    @property
    def state(self) -> CleanupResult:
        if self._results is None:
            result = CleanupResult(name=self.name)
            for asset in self.asset_paths:
                result.record(str(asset), CleanupStatus.READY, None)
            return result
        return self._results

    def display(self) -> str:
        if self.description:
            header = f"{self.name} ({self.description})"
        else:
            header = f"{self.name}"

        if len(self.asset_paths) > 1:
            tasks = "\n".join(f"* {p}" for p in self.asset_paths)
            return f"{header}\n{tasks}"

        if self.asset_paths:
            return f"{header}\n{self.asset_paths[0]}"

        return header


def get_prefect_storage_path() -> Path:
    """Return the path to the directory containing cached assets in prefect.

    Returns
    -------
    Path
    """
    return Path("~/.prefect/storage").expanduser().resolve()


def get_default_cleanup_actions() -> list[CleanupAction]:
    """Return a list of all available clean-up actions.

    Performing cleanup on these folders is the **nuclear option** and will
    wipe out all stored state.

    Returns
    -------
    list[CleanupAction]
    """
    return list(
        sorted(
            [
                FileSystemCleanupAction(
                    name="C-Star package cache",
                    description="Cached copies of previously retrieved github repositories",
                    asset_paths=[DirectoryManager.cache_home()],
                ),
                FileSystemCleanupAction(
                    name="C-Star state files",
                    description="Internal C-Star state information related to run history.",
                    asset_paths=[
                        DirectoryManager.state_home(),
                        get_prefect_storage_path(),
                    ],
                ),
                FileSystemCleanupAction(
                    name="C-Star Data",
                    description="All datasets and assets created during a run.",
                    asset_paths=[DirectoryManager.data_home()],
                ),
                FileSystemCleanupAction(
                    name="C-Star configuration",
                    description="Local user-specific C-Star configuration files",
                    asset_paths=[DirectoryManager.config_home()],
                ),
            ],
            key=lambda x: x.name,
        )
    )


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
    COLOR_READY: t.Final[str] = "white"
    COLOR_EXECUTED: t.Final[str] = "red"
    dry_run = is_flag_enabled(ENV_CSTAR_CLI_DRY_RUN)
    msg, color = "Will remove", COLOR_READY

    if not dry_run:
        log.debug(f"Performing {len(actions)} cleanup actions")
        msg, color = "Removed", COLOR_EXECUTED
        pending = {asyncio.Task(asyncio.to_thread(a.execute)) for a in actions}

        while pending:
            done, pending = await asyncio.wait(pending, timeout=0.1)

            displays = (task.result().display().split("\n") for task in done)

            for group in displays:
                group[0] = f"{colored(msg, color)}: {group[0]}"
                for item in group:
                    console.print(item)
    else:
        displays = (a.display().split("\n") for a in actions)

        for group in displays:
            group[0] = f"{colored(msg, color)}: {group[0]}"
            for item in group:
                console.print(item)

    return sum(len(a.state.ledger) for a in actions)


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
    if not available:
        console.print("No cleanup actions to perform")
        raise typer.Exit(99)

    CHOICE_YES: t.Final[str] = "y"
    CHOICE_NO: t.Final[str] = "n"
    CHOICE_DETAILS: t.Final[str] = "d"

    choices: t.Final[list[str]] = [CHOICE_YES, CHOICE_NO, CHOICE_DETAILS]
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

        if answer == CHOICE_DETAILS:
            console.print(action.display())
            continue
        elif answer == CHOICE_NO:
            console.print(f"\t[yellow]Skipping[/yellow] {action.name!r} deletion")
            continue

        selected.append(action)

    return selected


def runid_callback(_context: typer.Context, run_id: str) -> str:
    """Clean and validate the run-id format.

    Parameters
    ----------
    run_id : str
        The run-id value received from the user

    Returns
    -------
    str
    """
    if run_id and not run_id.strip():
        log.debug("Skipping run loading; run-id was not specified")
        raise typer.Exit(1)

    return run_id.strip()


def get_run_actions(run_id: str) -> list[CleanupAction]:
    """Create a list of cleanup actions to be performed for a run.

    Parameters
    ----------
    run_id : str
        The run-id value received from the user

    Returns
    -------
    list[CleanupAction]
    """
    cache_paths: list[Path] = []
    runstate_paths: list[Path] = []
    rundata_paths: list[Path] = []

    run_repo = TrackingRepository()
    workplan: Workplan | None = None

    if run := asyncio.run(run_repo.get_workplan_run(run_id)):
        workplan = deserialize(run.trx_workplan_path, Workplan)
        rundata_paths.append(run.output_path)
    else:
        # if we can't load a run, check the default data path.
        data_dir = StateDirectoryManager.data_dir(run_id=run_id)
        rundata_paths.append(data_dir)
        log.debug(f"Run {run_id!r} not found")

    storage_root: t.Final[Path] = get_prefect_storage_path()
    if workplan:
        fn_name: t.Final[str] = "_submit"

        for step in workplan.steps:
            cache_key = f"{run_id}_{step.name}_{fn_name}"
            cache_paths.append(storage_root / cache_key)
    else:
        cache_paths.extend(
            p
            for p in storage_root.iterdir()
            if p.is_dir() and p.name.startswith(run_id)
        )

    if run_paths := run_repo.list_runtracking_paths(run_id, all_history=True):
        runstate_paths.extend(run_paths)

    actions: list[CleanupAction] = []

    if cache_paths:
        actions.append(
            FileSystemCleanupAction(
                name=f"{run_id!r} Cached Results",
                description="Cached outputs from successful steps",
                asset_paths=cache_paths,
            ),
        )
    if runstate_paths:
        actions.append(
            FileSystemCleanupAction(
                name=f"{run_id!r} State",
                description="Internal C-Star state information related to run history.",
                asset_paths=runstate_paths,
            ),
        )
    if rundata_paths:
        actions.append(
            FileSystemCleanupAction(
                name=f"{run_id!r} Data",
                description="All datasets and assets created during a run.",
                asset_paths=rundata_paths,
            ),
        )
    return actions


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
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
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
        run_actions = get_run_actions(run_id)
        todo = actions_filter_prompt(run_actions, yes)

    asyncio.run(perform_actions(todo))


if __name__ == "__main__":
    app()
