import typing as t
from pathlib import Path

import typer

from cstar.cli.common import set_ctxmap
from cstar.cli.workplan.shared import KEY_RUN, lookup_run, parse_handle
from cstar.orchestration.index import HandleKind
from cstar.orchestration.tracking import WorkplanRun

if t.TYPE_CHECKING:
    from collections.abc import Callable

_T = t.TypeVar("_T")


def resolve_by_handle(
    context: typer.Context,
    target: str,
    *,
    verb: str,
    directory: "Callable[[str], _T]",
    tracked: "Callable[[WorkplanRun], _T]",
) -> _T:
    """Resolve a run handle for a command that reads a run directory.

    The one place that decides whether a run is tracked on this machine, and so
    refreshed from its sentinels, or only read as it is on disk. A directory the
    user named, and a run known only by its manifest, are read as they are; a
    run tracked here is refreshed first.

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
        Builds the command's target from the tracking record of a run.

    Returns
    -------
    _T
        What `directory` or `tracked` built.

    Raises
    ------
    typer.BadParameter
        Raised when the target is no run handle or names no run, and by the
        builders.
    """
    handle = parse_handle(context, target)
    if handle.kind is HandleKind.path:
        return directory(handle.value)

    hint = ""
    if handle.kind is HandleKind.run_id and Path(handle.value).is_dir():
        hint = f"To {verb} the directory ./{handle.value}, pass it as a path."

    record = lookup_run(handle, hint=hint)
    if isinstance(record, WorkplanRun):
        set_ctxmap(context, KEY_RUN, record)
        return tracked(record)

    return directory(str(record.output_path))
