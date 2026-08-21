import typing as t

import typer

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.cli.cache.common import (
    ARG_KEY,
    ARG_RUNID,
    ARG_YES,
    Prompter,
    call_to_action,
    console,
    key_callback,
    key_help,
    list_runs_with_cache,
    run_id_help,
    runid_callback,
    yes_help,
)
from cstar.cli.common import (
    cb_pipeline,
    set_env,
    set_flag,
)
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import (
    ArtifactExistsError,
    ArtifactNotFoundError,
    Location,
    OnConflict,
)

log = get_logger(__name__)
app = typer.Typer()


command_help: t.Final[str] = (
    "Promote an item stored in user-level cache to the shared, group-level cache."
)


@app.command(
    name="promote",
    help=command_help,
)
def promote(
    run_id: t.Annotated[
        str,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=cb_pipeline(runid_callback, set_env(ENV_CSTAR_RUNID)),
            min=1,
            autocompletion=list_runs_with_cache,
        ),
    ],
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
        ),
    ],
    overwrite: t.Annotated[
        bool,
        typer.Option(
            ARG_YES,
            help=yes_help,
        ),
    ] = False,
    verbose: t.Annotated[
        bool,
        typer.Option(
            ARG_VERBOSE,
            help=ARG_VERBOSE_HELP,
            callback=set_flag(ENV_CSTAR_CLI_VERBOSE),
            envvar=ENV_CSTAR_CLI_VERBOSE,
        ),
    ] = False,
) -> None:
    """Promote a user-level cached artifact into the shared, group cache."""
    cache = get_artifact_cache()
    location: Location | None = None

    try:
        on_conflict = OnConflict.ERROR if not overwrite else OnConflict.OVERWRITE
        location = cache.promote(key, run_id, on_conflict=on_conflict)
    except ArtifactNotFoundError:
        console.print(f"No cache artifact found for run-id {run_id!r} with key {key!r}")
    except ArtifactExistsError:
        prompt = f"An existing asset will be overwritten. {call_to_action}"

        if not overwrite and not Prompter(primary=prompt, mode="double").confirm():
            msg = "Overwrite permission denied by user for shared asset. Aborting."
            log.info(msg)
            raise typer.Exit()
        else:
            location = cache.promote(key, run_id, on_conflict=OnConflict.OVERWRITE)

    if location:
        msg = f"Cached artifact {key!r} promoted to the group cache"
        if is_flag_enabled(ENV_CSTAR_CLI_VERBOSE):
            msg = f"{msg} at {location!r}"
        console.print(msg)


if __name__ == "__main__":
    app()
