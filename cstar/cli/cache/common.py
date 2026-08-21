import typing as t

import typer
from rich.console import Console
from rich.prompt import Prompt

from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import Location, Tier

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


console = Console()


def print_not_found(run_id: str, key: str) -> None:
    """Print an informational message to the console for an unknown resource."""
    if run_id and key:
        console.print(f"No cached artifact {key!r} found for run {run_id!r}")
    elif not run_id and key:
        console.print(f"No cached artifact {key!r} found in shared cache")


def runid_callback(context: typer.Context, value: str) -> str:
    """Clean and validate the run-id format.

    Parameters
    ----------
    run_id : str
        The run_id value received from the user.

    Returns
    -------
    str
    """
    if value and not value.strip():
        console.print("A non-empty run-id must be specified.")
        raise typer.Exit(1)
    return value.strip()


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


def confirm_overwrite(
    force_overwrite: bool = False,
    location: Location | None = None,
) -> bool:
    if location is None:
        console.print("An artifact with the specified key already exists")
    else:
        console.print(f"The artifact at {location.path} already exists for that key.")

    if force_overwrite:
        answer = CHOICE_YES
    else:
        prompt = f"Press {CHOICE_YES!r} to overwrite (any other key to skip)."

        answer = Prompt.ask(
            prompt,
            default=CHOICE_NO,
            choices=choices,
            case_sensitive=False,
        )

    return answer.lower() == CHOICE_YES


def confirm_remove_run(run_id: str, force_remove: bool = False) -> bool:
    """Ask whether every artifact cached for a run should be removed.

    Separate from :func:`confirm_remove`, which describes one artifact and
    treats a missing location as "nothing to remove". A whole-run deletion has
    no single location to name, so passing ``None`` there both prints the wrong
    message and reports a refusal the caller may then ignore.

    Parameters
    ----------
    run_id : str
        Run whose artifacts would be removed.
    force_remove : bool, optional
        Skip the prompt, for unattended use.

    Returns
    -------
    bool
        Whether the removal should proceed.
    """
    console.print(f"Every artifact cached for run {run_id!r} will be removed.")

    if force_remove:
        return True

    answer = Prompt.ask(
        f"Press {CHOICE_YES!r} to delete (any other key to skip)",
        default=CHOICE_NO,
        choices=choices,
        case_sensitive=False,
    )
    return answer.lower() == CHOICE_YES


def confirm_remove(
    force_remove: bool = False,
    location: Location | None = None,
) -> bool:
    if location is None:
        console.print("An artifact with the specified key does not exist")
        return False
    else:
        console.print(f"The artifact at {location.path} will be removed.")

    if force_remove:
        answer = CHOICE_YES
    else:
        prompt1 = f"Press {CHOICE_YES!r} to delete (any other key to skip)"
        prompt2 = f"Confirm with {CHOICE_YES!r} again to remove this shared artifact"

        answer = Prompt.ask(
            prompt1,
            default=CHOICE_NO,
            choices=choices,
            case_sensitive=False,
        )

        if location.tier == Tier.SHARED and answer.lower() == CHOICE_YES:
            # Both answers must be yes. Reassigning here would discard the
            # first refusal, so answering "n" then "y" would delete a shared
            # artifact the user had already declined to remove.
            answer = Prompt.ask(
                prompt2,
                default=CHOICE_NO,
                choices=choices,
                case_sensitive=False,
            )

    return answer.lower() == CHOICE_YES
