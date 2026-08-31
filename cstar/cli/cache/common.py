import typing as t
from collections.abc import Sequence

import typer
from rich.console import Console

from cstar.io.utils import get_artifact_cache

key_help: t.Final[str] = "A key that will identifies a cached resource."
move_help: t.Final[str] = (
    "Move the specified resource to the cache instead of making a copy."
)
path_help: t.Final[str] = "Path to the resource to be stored in the cache."
run_id_help: t.Final[str] = "A run-id for resources stored in user cache."
yes_help: t.Final[str] = (
    "Automatically overwrite existing cache data without confirmation."
)
artifact_exists: t.Final[str] = "An artifact with the specified key already exists"


ARG_KEY: t.Final[str] = "--key"
ARG_MOVE: t.Final[str] = "--move"
ARG_PATH: t.Final[str] = "--path"
ARG_RUNID: t.Final[str] = "--run-id"
ARG_YES: t.Final[str] = "--yes"
ARG_GC: t.Final[str] = "--garbage"


CHOICE_YES: t.Final[str] = "y"
CHOICE_NO: t.Final[str] = "n"
CHOICE_ALL: t.Final[str] = "all"
choices: t.Final[list[str]] = [CHOICE_YES, CHOICE_NO]

call_to_action: t.Final[str] = f"Press {CHOICE_YES!r} to allow"
cancellation_msg: t.Final[str] = "Action cancelled"

console = Console()


def print_not_found(run_id: str, key: str) -> None:
    """Print an informational message to the console for an unknown resource."""
    if run_id and key:
        console.print(f"No cached artifact {key!r} found for run {run_id!r}")
    elif not run_id and key:
        console.print(f"No cached artifact {key!r} found in shared cache")


def runid_callback(context: typer.Context, value: str) -> str:
    """Clean and validate the run-id format.

    Run-id is optional when performing actions on the shared cache.

    Parameters
    ----------
    run_id : str
        The run_id value received from the user.

    Returns
    -------
    str
    """
    if value:
        value = value.strip()

        if not value:
            console.print("A non-empty run-id must be specified.")
            raise typer.Exit(1)

    return value


def key_callback(context: typer.Context, value: str) -> str:
    """Clean and validate the key format.

    Parameters
    ----------
    value : str
        The key value received from the user.

    Returns
    -------
    str
    """
    if value and not value.strip():
        console.print("A non-empty key must be specified.")
        raise typer.Exit(1)

    return value.strip()


def list_runs_with_cache() -> list[str]:
    """List the runs that have cached entries.

    Returns
    -------
    list[str]
    """
    cache = get_artifact_cache()
    return cache.list_runs()


class Prompter:
    """Manages the conditional delivery of confirmation prompts to a user."""

    interactive: bool = True
    mode: t.Literal["single", "double"] = "single"
    primary: str = call_to_action
    secondary: str = f"Are you sure? {call_to_action}"
    _responses: list[bool]

    def __init__(
        self,
        *,
        interactive: bool | None = None,
        mode: t.Literal["single", "double"] = "single",
        primary: str | None = None,
        secondary: str | None = None,
    ) -> None:
        if interactive is not None:
            self.interactive = interactive
        self.mode = mode
        if primary is not None:
            self.primary = primary
        if secondary is not None:
            self.secondary = secondary
        self._responses = []

    def confirm(
        self,
        primary: str | None = None,
        secondary: str | None = None,
        mode: t.Literal["single", "double"] | None = None,
    ) -> bool:
        if not self.interactive:
            return True

        confirmation = typer.confirm(primary or self.primary)
        self._responses.append(confirmation)

        if (mode or self.mode) == "double":
            confirmation = typer.confirm(secondary or self.secondary)
            self._responses.append(confirmation)

        return confirmation

    @property
    def responses(self) -> Sequence[bool]:
        """Return the results of any user confirmations that have been completed."""
        return tuple(self._responses)
