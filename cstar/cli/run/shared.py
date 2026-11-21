import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.base.adapter import CstarAdaptationError
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.cli.common import set_ctxmap
from cstar.cli.workplan.shared import KEY_RUN, lookup_run, parse_handle
from cstar.orchestration.dag_runner import (
    get_launcher,
    load_run_state,
    refresh_run_manifest,
)
from cstar.orchestration.index import HandleKind
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun

if t.TYPE_CHECKING:
    from collections.abc import Callable

    from cstar.orchestration.index import RunLocation
    from cstar.orchestration.manifest import RunManifest
    from cstar.orchestration.orchestration import LiveWorkplan

_T = t.TypeVar("_T")


def resolve_by_handle(
    context: typer.Context,
    target: str,
    *,
    verb: str,
    directory: "Callable[[str], _T]",
    tracked: "Callable[[WorkplanRun], _T]",
    indexed: "Callable[[str], _T] | None" = None,
) -> _T:
    """Resolve a run handle for a command that reads a run directory.

    The one place that decides whether a run is tracked on this machine, and so
    refreshed from its sentinels, or only read as it is on disk. A directory the
    user named, a run known only by its manifest, and a run attached from
    elsewhere (which has no sentinels here) are read as they are; a run
    scheduled here is refreshed first. A command that must tell a run found
    only by its manifest from a directory the user named, because it writes
    into the run, builds the first with `indexed`. Whether a command may write
    into an attached run is its own question, asked through `require_writable`.

    Parameters
    ----------
    context : typer.Context
        The typer context; receives the run record in the context map as "run"
        when the run is tracked.
    target : str
        The user-supplied run-id, `run://<uuid>`, or run directory.
    verb : str
        What the command does with the run, to suggest its directory by.
    directory : Callable[[str], _T]
        Builds the command's target from the path of a run directory.
    tracked : Callable[[WorkplanRun], _T]
        Builds the command's target from the tracking record of a run scheduled
        here.
    indexed : Callable[[str], _T] | None
        Builds the command's target from the path of the directory of a run
        that is not tracked here and was found by its manifest; `directory` by
        default.

    Returns
    -------
    _T
        What `directory`, `tracked` or `indexed` built.

    Raises
    ------
    typer.BadParameter
        Raised when the target is no run handle or names no run, and by the
        builders.
    """
    handle = parse_handle(context, target)
    if handle.kind is HandleKind.path:
        return directory(handle.value)

    # the text as typed: the run-id was slugified, the directory was not
    typed = target.strip()
    hint = ""
    if handle.kind is HandleKind.run_id and Path(typed).is_dir():
        hint = f"To {verb} the directory ./{typed}, pass it as a path."

    record = lookup_run(handle, hint=hint)
    if isinstance(record, WorkplanRun):
        set_ctxmap(context, KEY_RUN, record)
        if record.live:
            return tracked(record)
        return directory(str(record.output_path))

    return (indexed or directory)(str(record.output_path))


def tracked_record_of(root: Path, location: "RunLocation") -> WorkplanRun | None:
    """The tracking record of the run in a directory the user named.

    The directory of a run tracked here is still that run, whose record a command
    that writes into it must follow. A record of the same run-id that tracks
    another run (another uuid) or another directory is not this run's.

    Parameters
    ----------
    root : Path
        The run directory the user named.
    location : RunLocation
        The run its manifest describes (`read_run_location`).

    Returns
    -------
    WorkplanRun | None
        The record, or `None` when this machine does not track the run there.
    """
    tracked = TrackingRepository().get_workplan_run_sync(location.run_id)
    if tracked is None or tracked.uuid != location.uuid:
        return None
    if tracked.output_path.resolve() != root.resolve():
        return None
    return tracked


def refresh_tracked_manifest(
    root: Path, run_id: str, workplan: "LiveWorkplan"
) -> "RunManifest | None":
    """Refresh the manifest of a run scheduled here from its sentinels.

    This is what `cstar run status` does: each step's status is read from the
    launcher and folded, with the step records, into the manifest, which closes
    an attempt whose steps have all ended. The launcher is only read, so nothing
    a launch needs (such as the account setting the system requires) is asked
    for. A refresh that cannot run (the workplan names a scheduler this machine
    lacks) is a warning, and the caller reads the manifest as last recorded.

    Parameters
    ----------
    root : Path
        The run root.
    run_id : str
        The run-id of the tracked run.
    workplan : LiveWorkplan
        The run's transformed workplan, which selects its launcher.

    Returns
    -------
    RunManifest | None
        The refreshed manifest, or `None` when it was not refreshed (see also
        `refresh_run_manifest`).
    """
    try:
        launcher = get_launcher(
            workplan, force_local=workplan.pre_run, for_launch=False
        )
        status = asyncio.run(load_run_state(run_id, launcher))
    except (CstarError, CstarExpectationFailed, CstarAdaptationError, OSError) as ex:
        typer.echo(f"Warning: the manifest was not refreshed: {ex}", err=True)
        return None
    return refresh_run_manifest(root, run_id, status)
