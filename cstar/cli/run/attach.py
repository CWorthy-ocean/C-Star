import os
import typing as t
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import typer

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.base.utils import slugify
from cstar.cli.common import (
    cb_pipeline,
    get_from_ctxmap,
    normalize_runid,
    refuse_workplan_file,
    set_ctxmap,
)
from cstar.cli.run.check import format_findings, summarize
from cstar.cli.workplan.shared import parse_handle
from cstar.entrypoint.utils import (
    ARG_AS,
    ARG_AS_HELP,
    ARG_DRY_RUN,
    ARG_DRY_RUN_HELP,
    ARG_OWN,
    ARG_OWN_HELP,
    ARG_READ_ONLY,
    ARG_READ_ONLY_HELP,
    ARG_REPAIR,
    ARG_VERIFY,
    ARG_VERIFY_HELP,
)
from cstar.execution.file_system import DirectoryManager, StateDirectoryManager
from cstar.orchestration.index import (
    HandleKind,
    RunLocation,
    read_run_location,
    run_uri,
)
from cstar.orchestration.manifest import (
    RunManifest,
    fold_step_records,
    read_run_manifest,
)
from cstar.orchestration.orchestration import LiveWorkplan, ProcessHandle, Status
from cstar.orchestration.run_check import (
    TRANSFORMED_WORKPLAN_GLOB,
    FindingKind,
    check_run,
)
from cstar.orchestration.serialization import try_deserialize
from cstar.orchestration.tracking import (
    KEY_RUN_UUID,
    Access,
    TrackingRepository,
    WorkplanRun,
)

log = get_logger(__name__)
app = typer.Typer()

COMMAND_ATTACH: t.Final[str] = "attach"
"""Name of the command that attaches a run directory to this machine."""

KEY_TARGET: t.Final[str] = "attach_target"
"""Context-map key under which the resolved target is stored."""

INCOMPLETE_KINDS: t.Final[frozenset[FindingKind]] = frozenset(
    {FindingKind.PRODUCT_MISSING, FindingKind.PRODUCT_SIZE}
)
"""The findings that show a transfer is incomplete: the run is not attached."""

HELP_SHORT = "Attach a run that arrived by copy to this machine's tracking."
HELP_LONG = f"""\
{HELP_SHORT}

The target is the directory of a run that was copied, rsynced or transferred by
Globus from another machine, or whose tracking record here was lost. Once it is
attached, `cstar run status`, `show`, `gather` and `ls`, a bare run-id and
`runs:` aliases know it. The run is read, never written: attach writes one
tracking record in this machine's state home, and nothing into the run
directory.

The run's products are checked first, as `cstar run check` does. A product the
step records list that is missing, or whose size differs, shows that the
transfer is incomplete: the run is not attached and the command exits with
code 1. Any other problem is listed and the run is attached all the same, with
exit code 1: {ARG_VERIFY} also compares the content of each product that
records a digest, says in one line how many matched, and a mismatch is such a
problem. Nothing is repaired or
recorded: `cstar run check <run-id> {ARG_REPAIR}` and `--record-digests` do
that.

The run is attached under the run-id its manifest names. A run-id that is
already tracked here for the same run (same uuid) in another directory is
re-attached: the record now names this directory. A run-id tracked here for
another run is refused, and {ARG_AS} <run-id> attaches the run under a free
one; `run://<uuid>` finds it either way. Attaching a run that is already
attached here changes nothing.

An attached run is read-only when it is in the project's runs area or belongs
to another user, since the verbs that write into a run (`cstar run gather`,
`cstar run check {ARG_REPAIR}` and `--record-digests`, and
`cstar admin clean` of its directory) would write into a shared or foreign
directory; it is owned otherwise. {ARG_OWN} and {ARG_READ_ONLY} decide it
instead, and on a run that is already attached they change its mode alone.

An attached run is never relaunched: `cstar workplan submit --run-id` and
`--resume` refuse it, as it has no sentinels here. To continue from it, start a
new run that references it (see "Extending a finished run").

{ARG_DRY_RUN} does everything but write the record, and says what it would have
done.

Exit code 0: attached, or already attached. 1: attached with problems found, or
not attached because the transfer is incomplete. 2: bad arguments, a run-id
that is taken, or a directory that is not a workplan run.
"""

_NOT_A_DIRECTORY_HELP: t.Final[str] = (
    "attach takes the directory of a run; a run reachable by its run-id or "
    "run://<uuid> is already known here: `cstar run show <handle>` reads it."
)


@dataclass(frozen=True)
class AttachTarget:
    """What the `target` argument resolved to."""

    root: Path
    """The run directory, absolute and not resolved: symlinks stay."""
    location: RunLocation
    """The run found in it."""


class Plan(StrEnum):
    """What attaching the run does to the tracking of this machine."""

    new = "new record"
    """No record holds the run-id: one is written."""

    reattach = "re-attach"
    """The record of this run names another directory: it is updated."""

    mode = "change mode"
    """The run is attached here as given, but its mode is not the one asked for."""

    already = "already attached"
    """The run is attached here as given: nothing is written."""


def resolve_attach_target(context: typer.Context, target: str) -> str:
    """Resolve the target to a workplan run directory and store it in the context map.

    Parameters
    ----------
    context : typer.Context
        The typer context; receives the resolved target.
    target : str
        The user-supplied path of a run directory.

    Returns
    -------
    str
        The target, unchanged.

    Raises
    ------
    typer.BadParameter
        - Raised when the target is a run-id or `run://<uuid>`: the run is known.
        - Raised when the path is not a directory holding a run manifest.
        - Raised when the run is a standalone blueprint run, which is not tracked.
    """
    handle = parse_handle(context, target)
    if handle.kind is not HandleKind.path:
        raise typer.BadParameter(_NOT_A_DIRECTORY_HELP)

    root = Path(os.path.abspath(Path(handle.value).expanduser()))
    try:
        location = read_run_location(root)
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex

    if location.kind == "blueprint":
        msg = (
            f"{str(root)!r} is a standalone blueprint run; such runs are not "
            f"tracked. `cstar run show {root}` reads it."
        )
        raise typer.BadParameter(msg)

    set_ctxmap(context, KEY_TARGET, AttachTarget(root, location))
    return target


def default_access(root: Path) -> Access:
    """Decide whether verbs may write into an attached run's directory.

    The run is read-only when it is in the project's runs area, which is
    shared, or when another user owns its directory; it is owned otherwise.

    Parameters
    ----------
    root : Path
        The run directory.

    Returns
    -------
    Access
    """
    project = DirectoryManager.project_runs_home()
    if project is not None and root.parent.resolve() == project.resolve():
        log.debug(f"{root} is in the project's runs area: read-only")
        return Access.read_only

    if (uid := root.stat().st_uid) != os.geteuid():
        log.debug(f"{root} belongs to user {uid}, not to this one: read-only")
        return Access.read_only

    log.debug(f"{root} is outside the project's runs area and this user's: owned")
    return Access.owned


def _read_run(root: Path) -> tuple[RunManifest, Path]:
    """Read the manifest of the run, with its step records folded in, and its transformed workplan.

    Parameters
    ----------
    root : Path
        The run directory.

    Returns
    -------
    tuple[RunManifest, Path]
        The manifest, changed in memory only, and the path of the transformed
        workplan.

    Raises
    ------
    typer.BadParameter
        Raised when the manifest cannot be read, or the directory does not hold
        exactly one transformed workplan.
    """
    try:
        manifest = read_run_manifest(root, tolerant=True)
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex
    if manifest is None:
        msg = f"{str(root)!r} no longer holds a run manifest."
        raise typer.BadParameter(msg)

    fold_step_records(manifest, root, tolerant=True)

    found = sorted(root.glob(TRANSFORMED_WORKPLAN_GLOB))
    if len(found) == 1:
        return manifest, found[0]
    if not found:
        msg = f"Run {manifest.run_id!r} at {root} holds no transformed workplan"
    else:
        names = ", ".join(path.name for path in found)
        msg = (
            f"Run {manifest.run_id!r} at {root} holds more than one transformed "
            f"workplan ({names}); remove the stale ones."
        )
    raise typer.BadParameter(msg)


def _tracked_as(tracked: WorkplanRun) -> str:
    """Name the run a tracking record holds, by its uuid when it is known."""
    return run_uri(tracked.uuid) if tracked.uuid else "a run of unknown uuid"


def _plan(
    tracked: WorkplanRun | None,
    manifest: RunManifest,
    root: Path,
    *,
    mode: Access,
    explicit: bool,
) -> Plan:
    """Decide what attaching the run changes, refusing a run-id taken by another run.

    Parameters
    ----------
    tracked : WorkplanRun | None
        The record of the run-id the run is attached under, if any.
    manifest : RunManifest
        The manifest of the run.
    root : Path
        The run directory.
    mode : Access
        The access the run is attached with.
    explicit : bool
        Whether the user asked for `mode`, rather than it being the default.

    Returns
    -------
    Plan

    Raises
    ------
    typer.BadParameter
        Raised when the run-id is tracked here for another run.
    """
    if tracked is None:
        return Plan.new

    if not tracked.uuid or tracked.uuid != manifest.id:
        run_id = tracked.run_id
        msg = (
            f"Run-id {run_id!r} is tracked here as {_tracked_as(tracked)} at "
            f"{tracked.output_path}; the run at {root} is {run_uri(manifest.id)}. "
            f"Pass {ARG_AS} <free-run-id> to attach it under another run-id."
        )
        raise typer.BadParameter(msg)

    if tracked.output_path.resolve() != root.resolve():
        return Plan.reattach
    if explicit and tracked.access is not mode:
        return Plan.mode
    return Plan.already


def build_attached_record(
    manifest: RunManifest,
    location: RunLocation,
    *,
    run_id: str,
    mode: Access,
) -> WorkplanRun:
    """Build the record that attaches the run.

    Parameters
    ----------
    manifest : RunManifest
        The manifest of the run.
    location : RunLocation
        The run as found in its directory.
    run_id : str
        The run-id to attach the run under.
    mode : Access
        The access the run is attached with.

    Returns
    -------
    WorkplanRun
        A record of the run directory, the uuid and the access. It holds no
        workplan path, name or start time: those are read from the run's
        manifest.
    """
    return WorkplanRun(
        output_path=location.root,
        run_id=run_id,
        sentinels=set(),
        attached=True,
        access=mode,
        metadata={KEY_RUN_UUID: manifest.id},
    )


def _drop_sentinels(run: WorkplanRun) -> None:
    """Remove the sentinels a record names, and the run's state directory when empty.

    An attached run has no sentinels on this machine; those a re-attached
    run's earlier record named would otherwise be left behind, nothing
    tracking them. A sentinel whose step has not reached a terminal status is
    the state of a step that may still be running here, and is left with a
    warning. Record-keeping never stops the command: a path that cannot be
    removed is warned about and left.

    Parameters
    ----------
    run : WorkplanRun
        The record being replaced, whose `sentinels` are removed.
    """
    for sentinel in run.sentinels:
        handle = try_deserialize(sentinel, ProcessHandle)
        if handle is not None and not Status.is_terminal(handle.status):
            log.warning(
                f"Left the sentinel {sentinel}: step {handle.name!r} is "
                f"{handle.status.name}, not finished."
            )
            continue
        try:
            sentinel.unlink(missing_ok=True)
        except OSError as ex:
            log.warning(f"Unable to remove the sentinel {sentinel}: {ex}")

    state_dir = StateDirectoryManager.run_state_dir(run_id=run.run_id)
    try:
        state_dir.rmdir()
    except FileNotFoundError:
        pass
    except OSError as ex:
        # not empty (the manifest lock may remain) or unreadable: leave it
        log.debug(f"Left the state directory {state_dir}: {ex}")


def write_attached_record(plan: Plan, fresh: WorkplanRun, mode: Access) -> None:
    """Write the one tracking record the command writes.

    Parameters
    ----------
    plan : Plan
        What attaching changes.
    fresh : WorkplanRun
        The record that attaches the run.
    mode : Access
        The access the run is attached with.

    Raises
    ------
    CstarError
        If the record of the run vanished before it was updated.
    """
    repository = TrackingRepository()
    if plan is Plan.new:
        repository.put_workplan_run_sync(fresh)
        return

    def mutate(run: WorkplanRun) -> None:
        if plan is Plan.reattach:
            _drop_sentinels(run)
            run.output_path = fresh.output_path
            run.sentinels = set()
            run.attached = True
            run.metadata[KEY_RUN_UUID] = fresh.metadata[KEY_RUN_UUID]
        run.access = mode

    if repository.update_workplan_run_sync(fresh.run_id, mutate) is None:
        msg = f"The record of run {fresh.run_id!r} vanished before it was updated."
        raise CstarError(msg)


@app.command(name=COMMAND_ATTACH, help=HELP_LONG, short_help=HELP_SHORT)
def attach(
    context: typer.Context,
    target: t.Annotated[
        str,
        typer.Argument(
            help="The directory of a run that was copied here.",
            callback=cb_pipeline(refuse_workplan_file, resolve_attach_target),
        ),
    ],
    verify: t.Annotated[bool, typer.Option(ARG_VERIFY, help=ARG_VERIFY_HELP)] = False,
    as_run_id: t.Annotated[
        str,
        typer.Option(ARG_AS, help=ARG_AS_HELP, callback=normalize_runid),
    ] = "",
    own: t.Annotated[bool, typer.Option(ARG_OWN, help=ARG_OWN_HELP)] = False,
    read_only: t.Annotated[
        bool, typer.Option(ARG_READ_ONLY, help=ARG_READ_ONLY_HELP)
    ] = False,
    dry_run: t.Annotated[
        bool, typer.Option(ARG_DRY_RUN, help=ARG_DRY_RUN_HELP)
    ] = False,
) -> None:
    """Attach a run that arrived by copy to this machine's tracking."""
    if own and read_only:
        msg = f"{ARG_OWN} and {ARG_READ_ONLY} contradict each other: pass one."
        raise typer.BadParameter(msg, param_hint=ARG_READ_ONLY)

    resolved = get_from_ctxmap(context, KEY_TARGET, AttachTarget)
    root = resolved.root
    manifest, trx = _read_run(root)

    # decided here, once, and recorded: no verb derives it again
    if own:
        mode = Access.owned
    elif read_only:
        mode = Access.read_only
    else:
        mode = default_access(root)
    run_id = slugify(as_run_id) if as_run_id else manifest.run_id

    # refuse a collision before the check reads the run
    tracked = TrackingRepository().get_workplan_run_sync(run_id)
    plan = _plan(tracked, manifest, root, mode=mode, explicit=own or read_only)

    workplan = try_deserialize(trx, LiveWorkplan)
    if workplan is None:
        typer.echo(f"Warning: unable to load the transformed workplan {trx}", err=True)

    report = check_run(
        root,
        workplan=workplan,
        run_id=None,
        all_files=False,
        manifest=manifest,
        verify=verify,
    )
    for line in format_findings(report):
        typer.echo(line)
    typer.echo(summarize(report))

    if any(problem.kind in INCOMPLETE_KINDS for problem in report.problems):
        msg = (
            f"Not attached: the transfer of {root} is incomplete "
            "(products missing or truncated)."
        )
        typer.echo(msg, err=True)
        raise typer.Exit(1)

    was = f" from {tracked.output_path}" if tracked and plan is Plan.reattach else ""
    if dry_run:
        typer.echo(
            f"Would attach run {run_id!r} ({manifest.id}) at {root} as "
            f"{mode.value} [{plan.value}{was}]"
        )
    elif plan is Plan.already:
        typer.echo(f"Run {run_id!r} is already attached here at {root}.")
    else:
        write_attached_record(
            plan,
            build_attached_record(
                manifest, resolved.location, run_id=run_id, mode=mode
            ),
            mode,
        )
        if plan is Plan.reattach and tracked is not None:
            typer.echo(
                f"Re-attached run {run_id!r}: its directory is now {root} "
                f"(was {tracked.output_path})."
            )
        elif plan is Plan.mode:
            typer.echo(f"Run {run_id!r} at {root} is now {mode.value}.")
        else:
            typer.echo(
                f"Attached run {run_id!r} ({run_uri(manifest.id)}) at {root} "
                f"as {mode.value}."
            )
        if mode is Access.read_only:
            typer.echo(f"Nothing will be written into it; {ARG_OWN} takes ownership.")

    if problems := report.problems:
        if any(p.kind is FindingKind.ABSOLUTE_LINK for p in problems):
            first = (
                f"`cstar run attach {root} {ARG_OWN}`, then "
                if (mode is Access.read_only)
                else ""
            )
            typer.echo(
                f"{first}`cstar run check {run_id} {ARG_REPAIR}` rewrites absolute symlinks."
            )
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
