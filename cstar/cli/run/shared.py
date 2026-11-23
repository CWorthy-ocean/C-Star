import asyncio
import typing as t
from pathlib import Path

import typer

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
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

log = get_logger(__name__)


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

    Decides, for a run-id or `run://` handle, whether the run is tracked on this
    machine, and so refreshed from its sentinels (`refresh_tracked_manifest`),
    or only read as it is on disk. A run known only by its manifest and a run
    attached from elsewhere (which has no sentinels here) are read as they are;
    a run scheduled here is refreshed first. A directory the user named is read
    as it is, except by a command that writes into the run: its `directory`
    builder finds the run's record with `tracked_record_of`, and it builds a run
    found only by its manifest with `indexed`, to tell the two apart. Whether a
    command may write into an attached run is its own question, asked through
    `require_writable`.

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

    Raises
    ------
    typer.BadParameter
        Raised when a record of the run-id exists and cannot be read, so whether
        the run may be written is not known.
    """
    try:
        tracked = TrackingRepository().get_workplan_run_sync(location.run_id)
    except (CstarError, ValueError, OSError) as ex:
        msg = f"The tracking record of run {location.run_id!r} cannot be read: {ex}"
        raise typer.BadParameter(msg) from ex
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
    lacks, the scheduler cannot be queried) is a warning, and the caller reads
    the manifest as last recorded: keeping the record of a run never stops a
    command.

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
    except Exception as ex:  # bookkeeping: the manifest is read as last recorded
        log.debug("The manifest was not refreshed", exc_info=True)
        typer.echo(f"Warning: the manifest was not refreshed: {ex}", err=True)
        return None
    return refresh_run_manifest(root, run_id, status)
