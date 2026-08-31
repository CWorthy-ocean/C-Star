import typing as t

import typer

from cstar.base.env import ENV_CSTAR_CLI_VERBOSE
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
    run_id_help,
    runid_callback,
)
from cstar.cli.common import (
    set_flag,
)
from cstar.entrypoint.utils import ARG_VERBOSE, ARG_VERBOSE_HELP
from cstar.io.utils import get_artifact_cache
from cstar.orchestration.artifact_cache import (
    ArtifactCache,
    ArtifactNotFoundError,
    Location,
    Tier,
    UnsafePathError,
)

log = get_logger(__name__)
app = typer.Typer()


command_help: t.Final[str] = "Manually remove assets from the cache."
yes_help: t.Final[str] = "Perform user-level deletions without confirmation."


def handle_shared_deletion(
    cache: ArtifactCache,
    prompter: Prompter,
    location: Location,
) -> bool:
    """Perform the deletion of a shared artifact."""
    if confirmation := prompter.confirm(
        primary=f"Delete the shared asset in {location.path}? {call_to_action}",
        secondary=f"Are you sure you want to delete a shared asset? {call_to_action}",
        mode="double",
    ):
        if removed := cache.delete_shared(location.name, confirmation):
            console.print(f"Shared asset {location.name!r} has been deleted")
        else:
            console.print("Shared asset was not removed")
        return removed

    console.print(f"Confirmation required to remove shared asset: {location.name}")
    raise typer.Exit(1)


def handle_user_deletion(
    cache: ArtifactCache,
    prompter: Prompter,
    location: Location,
) -> bool:
    """Perform the deletion of a user-level artifact."""
    if not location.run_id:
        console.print("The asset location is missing a run-id. Unable to continue")
        raise typer.Exit(1)

    if prompter.confirm(
        primary=f"Delete your artifact in {location.path}? {call_to_action}",
        mode="single",
    ):
        if removed := cache.delete_user(location.name, location.run_id):
            console.print(f"User asset {location.name!r} deleted")
            return removed

    console.print("Confirmation required to remove user asset")
    raise typer.Exit(1)


def handle_run_deletion(
    cache: ArtifactCache,
    prompter: Prompter,
    run_id: str,
) -> bool:
    """Perform the deletion of all artifacts for a run."""
    if prompter.confirm(
        primary=f"Delete all assets for the run? {call_to_action}",
        secondary=f"Are you sure you want to delete all assets for the run? {call_to_action}",
        mode="double",
    ):
        if removed := cache.delete_user_run(run_id, missing_ok=True):
            console.print(f"All assets for run {run_id!r} have been deleted")
        return removed

    console.print(f"Confirmation required to remove run assets for {run_id!r}")
    raise typer.Exit(1)


@app.command(
    name="clean",
    help=command_help,
)
def clean_cache(
    key: t.Annotated[
        str,
        typer.Option(
            ARG_KEY,
            help=key_help,
            callback=key_callback,
            min=1,
        ),
    ] = "",
    run_id: t.Annotated[
        str | None,
        typer.Option(
            ARG_RUNID,
            help=run_id_help,
            callback=runid_callback,
            min=1,
        ),
    ] = None,
    noninteractive: t.Annotated[
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
    """Manually remove artifacts from the cache."""
    cache = get_artifact_cache()
    prompter = Prompter(interactive=not noninteractive)

    try:
        match (key, run_id):
            case (k, r) if k and not r:
                # no run-id targets a shared artifact (or is a mistake)
                if location := cache.resolve(k, r):
                    handle_shared_deletion(cache, prompter, location)
                else:
                    console.print(
                        f"No shared asset found for key: {k}. Did you forget a run-id?"
                    )
            case (k, r) if k and r:
                # key and run-id may find a user-level or shared artifact
                if location := cache.resolve(k, r, prefer_local=True):
                    if location.tier == Tier.USER:
                        handle_user_deletion(cache, prompter, location)
                    else:
                        msg = "Do not pass a run-id to delete a shared asset."
                        raise typer.BadParameter(msg, param_hint="run_id")
                else:
                    console.print(
                        f"No cached asset found for key {k!r} and run-id {r!r}?"
                    )
            case (k, r) if not k and r:
                # no key indicates either a mistake or a delete-full-run attempt
                try:
                    handle_run_deletion(cache, prompter, r)
                except ArtifactNotFoundError:
                    console.print(f"No assets found for run-id {r!r}?")
                except UnsafePathError:
                    console.print("The asset is outside the cache. Permission denied")
            case _:
                console.print("A key or run-id is required")
                raise typer.Exit()
    except ArtifactNotFoundError:
        msg = "Unknown asset could not be removed"
        log.error(msg)
        console.print(msg)
        raise typer.Exit(1)
    except UnsafePathError:
        msg = "Asset could not be removed"
        log.error(msg)
        console.print(msg)


if __name__ == "__main__":
    app()
