import typing as t

import typer
from rich.console import Console
from rich.prompt import Prompt

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.log import get_logger
from cstar.cli.common import (
    cb_pipeline,
    set_env,
)
from cstar.cli.workplan.shared import list_runs
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import (
    ArtifactExistsError,
    ArtifactNotFoundError,
    Location,
    OnConflict,
)

log = get_logger(__name__)
app = typer.Typer()
console = Console()


run_id_help: t.Final[str] = "A run-id containing the cached data to be promoted."
key_help: t.Final[str] = "A cache key specifying data to be promoted."
yes_help: t.Final[str] = (
    "Automatically overwrite existing cache data without confirmation."
)


ARG_RUNID: t.Final[str] = "--run-id"
ARG_KEY: t.Final[str] = "--key"
ARG_YES: t.Final[str] = "--yes"
HELP: t.Final[str] = (
    "Promote an item stored in user-level cache to the shared, group-level cache."
)


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
        print("A key must be specified.")
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
        print("A key must be specified.")
        raise typer.Exit(1)

    return value.strip()


def confirm_overwrite(force_overwrite: bool = False) -> bool:
    CHOICE_YES: t.Final[str] = "y"
    CHOICE_NO: t.Final[str] = "n"
    choices: t.Final[list[str]] = [CHOICE_YES, CHOICE_NO]

    print("This artifact is already promoted to the shared cache.")

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


@app.command(
    name="promote",
    help=HELP,
)
def promote(
    context: typer.Context,
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
            min=1,
            autocompletion=list_runs,
        ),
    ],
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
            # autocompletion=list_runs,
        ),
    ],
    overwrite: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
        ),
    ] = False,
) -> None:
    """Promote a user-level cache entry into the shared, group cache."""
    cache = get_artifact_cache()
    location: Location | None = None

    try:
        on_conflict = OnConflict.ERROR if not overwrite else OnConflict.OVERWRITE
        location = cache.promote(key, run_id, on_conflict=on_conflict)
    except ArtifactNotFoundError:
        print(f"No cache entry found for run-id {run_id!r} with key {key!r}")
    except ArtifactExistsError:
        if confirm_overwrite(force_overwrite=overwrite):
            location = cache.promote(key, run_id, on_conflict=OnConflict.OVERWRITE)

    if location:
        print(f"Cache entry {key!r} is promoted to the group cache at {location!r}")


if __name__ == "__main__":
    app()
