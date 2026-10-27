import asyncio
import os
import typing as t
from dataclasses import dataclass, replace
from pathlib import Path

import typer

from cstar.base.env import ENV_CSTAR_PROJECT_HOME
from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.cli.common import (
    cb_pipeline,
    exit_on_failure,
    get_from_ctxmap,
    refuse_workplan_file,
    set_ctxmap,
    try_get_from_ctxmap,
)
from cstar.cli.run.attach import (
    Plan,
    build_attached_record,
    default_access,
    write_attached_record,
)
from cstar.cli.run.check import format_findings, summarize
from cstar.cli.run.shared import resolve_by_handle
from cstar.cli.workplan.gather import collect_links, relink, report_conflicts
from cstar.cli.workplan.shared import HELP_RUN_HANDLE, KEY_RUN, list_runs
from cstar.entrypoint.utils import (
    ARG_DRY_RUN,
    ARG_DRY_RUN_HELP,
    ARG_INCLUDE,
    ARG_INCLUDE_HELP,
    ARG_MODE,
    ARG_MODE_HELP,
    ARG_OWN,
    ARG_TO,
    ARG_TO_HELP,
    ARG_VERIFY,
    ARG_VERIFY_HELP,
)
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.dag_runner import (
    get_launcher,
    load_run_state,
    refresh_run_manifest,
)
from cstar.orchestration.index import RunLocation, read_run_location, run_uri
from cstar.orchestration.manifest import (
    RunManifest,
    compute_product_digests,
    fold_step_records,
    read_run_manifest,
    record_product_digests,
)
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.promotion import (
    CopyPlan,
    DestinationState,
    Include,
    Mode,
    destination_state,
    execute_plan,
    link_run,
    plan_promotion,
    remove_run,
)
from cstar.orchestration.run_check import (
    TRANSFORMED_WORKPLAN_GLOB,
    Finding,
    FindingKind,
    RunCheckReport,
    check_run,
    gathered_output_dir,
    steps_inside_run,
)
from cstar.orchestration.tracking import (
    Access,
    TrackingRepository,
    WorkplanRun,
    require_writable,
)

log = get_logger(__name__)
app = typer.Typer()

COMMAND_PROMOTE: t.Final[str] = "promote"
"""Name of the command that promotes a run to the project's runs area."""

KEY_SOURCE: t.Final[str] = "promote_source"
"""Context-map key under which the resolved source is stored."""

INHERITED_KINDS: t.Final[frozenset[FindingKind]] = frozenset(
    {FindingKind.ABSOLUTE_PATH}
)
"""The findings that say nothing about the copy.

Absolute paths inside the copied files are the source's (the transformed
workplan records its root, which `LiveWorkplan.load` rebases): they are reported
in place too, and do not stop a move.
"""

HELP_SHORT = "Copy, link or move a run into the project's runs area."
HELP_LONG = f"""\
{HELP_SHORT}

The target is a run-id, `run://<uuid>`, or the path of a run directory. By
default promote copies the run's manifest, its transformed workplan, its
attempts, the step records and logs, the small files in each step's `work/`
directory, and every product the step records list; a step's compile-time code,
runtime code, cloned codebases and per-rank output pieces are left out.
{ARG_INCLUDE} adds them: `builds` takes each ROMS step's compile-time code,
runtime code and cloned codebases, `temp-output` the per-rank output pieces, and
`all` the whole directory.

The copy goes to `<project runs area>/<run-id>`, the project's runs area being
`<project>/cstar/runs` under `{ENV_CSTAR_PROJECT_HOME}` (default `$PROJECT`);
{ARG_TO} <dir> puts it at `<dir>/<run-id>` instead. `cstar run ls --shared` lists
the project's runs area.

{ARG_MODE} decides how the run gets there. `copy` (the default) copies it and
leaves the source in place. `link` makes a symbolic link at the destination and
copies nothing. `move` copies the whole directory, whatever {ARG_INCLUDE} says,
so nothing is lost, checks the copy, removes the source only
after that check passed, and then updates this machine's tracking record of the
run: it is attached, which means it can be inspected and referenced but never
relaunched, and is read-only when the destination is in the project's runs area
(`cstar run attach <dir> {ARG_OWN}` takes ownership). `materialize` replaces a
link made by `link` with a copy of the run it points at; the run the link
pointed at is left in place.

Nothing is ever overwritten: a destination that holds another run, or that
holds this run already, is refused, and so is one that is not a run. Only
`materialize` replaces a link. Everything is refused before the first byte is
copied, and a product the step records list that is missing from the source
stops the command with exit code 1: `cstar run check <run-id>` it first.

The copy is checked as `cstar run check` does, and its findings are listed.
{ARG_VERIFY} also compares each product of the copy with its recorded digest,
computing and recording a digest in the copy first for each product that has
none; the source is not written. Absolute paths found inside the copied files are
the source's own and do not count as problems of the copy. A copy with problems
exits with code 1, and `move` then keeps the source and its tracking record.

{ARG_DRY_RUN} runs every refusal and prints what would be copied and where, then
stops: nothing is written.

Exit code 0: promoted. 1: promoted with problems found in the copy, nothing
copied because the source is missing products, the copy failed (nothing is left
at the destination), or the work after the copy failed (the message says where
the copy is). 2: bad arguments, no project's runs area and no {ARG_TO}, or a
destination that is refused.
"""

_NO_PROJECT_HELP: t.Final[str] = (
    f"No project runs area: set {ENV_CSTAR_PROJECT_HOME} (or $PROJECT) or pass "
    f"{ARG_TO} <dir>."
)


@dataclass(frozen=True)
class PromoteSource:
    """What the `target` argument resolved to."""

    root: Path
    """The run directory, absolute and not resolved: symlinks stay."""
    record: WorkplanRun | None
    """The tracking record of the run, when it is tracked on this machine."""
    named_by_path: bool
    """Whether the user passed the run's directory."""
    indexed: bool
    """Whether the run was found only by its manifest."""


def _directory_location(target: str) -> tuple[Path, RunLocation]:
    """Read the run in a directory the user named.

    Parameters
    ----------
    target : str
        The user-supplied path.

    Returns
    -------
    tuple[Path, RunLocation]
        The directory, absolute and not resolved, and the run it holds.

    Raises
    ------
    typer.BadParameter
        Raised when the path is not a directory holding a run manifest.
    """
    root = Path(os.path.abspath(Path(target).expanduser()))
    try:
        return root, read_run_location(root)
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex


def _directory_source(target: str) -> PromoteSource:
    """Resolve a target spelled as the path of a run directory.

    Parameters
    ----------
    target : str
        The user-supplied path.

    Returns
    -------
    PromoteSource

    Raises
    ------
    typer.BadParameter
        Raised when the path is not a directory holding a run manifest.
    """
    root, location = _directory_location(target)

    # the directory of a run tracked here is still that run: a move must follow it
    tracked = TrackingRepository().get_workplan_run_sync(location.run_id)
    if tracked is not None and (
        tracked.uuid != location.uuid or tracked.output_path.resolve() != root.resolve()
    ):
        tracked = None
    return PromoteSource(root, tracked, named_by_path=True, indexed=False)


def _tracked_source(run: WorkplanRun) -> PromoteSource:
    """Resolve a run scheduled on this machine, which is refreshed before it is read.

    Parameters
    ----------
    run : WorkplanRun
        The tracking record of the run.

    Returns
    -------
    PromoteSource

    Raises
    ------
    typer.BadParameter
        Raised when the run's recorded directory does not exist.
    """
    if not run.output_path.is_dir():
        msg = (
            f"The directory recorded for run {run.run_id!r} does not exist: "
            f"{run.output_path}. To promote a moved run, pass its directory as a path."
        )
        raise typer.BadParameter(msg)

    return PromoteSource(run.output_path, run, named_by_path=False, indexed=False)


def _indexed_source(target: str) -> PromoteSource:
    """Resolve a run that is not tracked here and was found by its manifest.

    Parameters
    ----------
    target : str
        The path of the run's directory.

    Returns
    -------
    PromoteSource

    Raises
    ------
    typer.BadParameter
        Raised when the directory is not a run, as `_directory_source` does.
    """
    root, _ = _directory_location(target)
    return PromoteSource(root, None, named_by_path=False, indexed=True)


def resolve_source(context: typer.Context, target: str) -> str:
    """Resolve the target to a run directory and store it in the context map.

    Parameters
    ----------
    context : typer.Context
        The typer context; receives the resolved source.
    target : str
        The user-supplied run-id, `run://<uuid>`, or directory path.

    Returns
    -------
    str
        The target, unchanged.

    Raises
    ------
    typer.BadParameter
        Raised when the target does not identify a run directory.
    """
    resolved = resolve_by_handle(
        context,
        target,
        verb=COMMAND_PROMOTE,
        directory=_directory_source,
        tracked=_tracked_source,
        indexed=_indexed_source,
    )
    # an attached record is read as its directory: the record is still the run's
    record = try_get_from_ctxmap(context, KEY_RUN, WorkplanRun) or resolved.record
    set_ctxmap(context, KEY_SOURCE, replace(resolved, record=record))
    return target


def _human_size(nbytes: int) -> str:
    """Format a number of bytes with a decimal unit, one decimal past the bytes."""
    size = float(nbytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1000:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1000
    return f"{size:.1f} TB"


def _transformed_workplan(root: Path) -> Path | None:
    """Find the one transformed workplan in a run directory.

    Parameters
    ----------
    root : Path
        The run directory.

    Returns
    -------
    Path | None
        The workplan, or `None` for a standalone blueprint run, which has none.

    Raises
    ------
    typer.BadParameter
        Raised when the directory holds more than one.
    """
    found = sorted(root.glob(TRANSFORMED_WORKPLAN_GLOB))
    if len(found) > 1:
        names = ", ".join(path.name for path in found)
        msg = (
            f"{str(root)!r} holds more than one transformed workplan ({names}); "
            "remove the stale ones."
        )
        raise typer.BadParameter(msg)
    return next(iter(found), None)


def _load_workplan(path: Path | None) -> LiveWorkplan | None:
    """Load a transformed workplan, rebased onto its directory, warning when it cannot be loaded."""
    if path is None:
        return None
    workplan = LiveWorkplan.try_load(path)
    if workplan is None:
        typer.echo(f"Warning: unable to load the transformed workplan {path}", err=True)
    return workplan


def _read_run(source: PromoteSource, workplan: LiveWorkplan | None) -> RunManifest:
    """Read the manifest of the run, refreshed first when the run is scheduled here.

    A tracked run's manifest is refreshed as `cstar run show` does, which needs
    its transformed workplan; a refresh that cannot run is a warning, and the
    manifest is read as last recorded. The manifest of a run that was not
    refreshed has the step records folded in in memory only.

    Parameters
    ----------
    source : PromoteSource
        The run to read.
    workplan : LiveWorkplan | None
        The run's transformed workplan, when it could be loaded.

    Returns
    -------
    RunManifest

    Raises
    ------
    typer.BadParameter
        Raised when the run's manifest cannot be read.
    """
    root = source.root
    refreshed = False
    if source.record is not None and source.record.live and workplan is not None:
        try:
            launcher = get_launcher(workplan, force_local=workplan.pre_run)
            status = asyncio.run(load_run_state(source.record.run_id, launcher))
            refresh_run_manifest(root, source.record.run_id, status)
            refreshed = True
        except (CstarError, CstarExpectationFailed, OSError) as ex:
            typer.echo(f"Warning: the manifest was not refreshed: {ex}", err=True)

    manifest = _read_manifest(root)
    if not refreshed:
        fold_step_records(manifest, root)
    return manifest


def _read_manifest(root: Path) -> RunManifest:
    """Read the manifest of a run directory, as it is on disk.

    Raises
    ------
    typer.BadParameter
        Raised when the manifest is missing or cannot be read.
    """
    try:
        manifest = read_run_manifest(root)
    except CstarError as ex:
        raise typer.BadParameter(str(ex)) from ex
    if manifest is None:
        msg = f"{str(root)!r} no longer holds a run manifest."
        raise typer.BadParameter(msg)
    return manifest


def _destination(to: str, run_id: str) -> Path:
    """Decide where the run goes.

    Parameters
    ----------
    to : str
        The directory `--to` named, or empty.
    run_id : str
        The run-id the manifest names, which is the name of the run's directory.

    Returns
    -------
    Path

    Raises
    ------
    typer.BadParameter
        Raised when no directory was named and there is no project's runs area.
    """
    if to:
        return Path(to).expanduser().absolute() / run_id
    if (project := DirectoryManager.project_runs_home()) is None:
        raise typer.BadParameter(_NO_PROJECT_HELP)
    return project / run_id


def _refuse_destination(
    mode: Mode, state: DestinationState, destination: Path, manifest: RunManifest
) -> None:
    """Refuse a destination that the mode may not take.

    Parameters
    ----------
    mode : Mode
        How the run is promoted.
    state : DestinationState
        What the destination holds.
    destination : Path
        Where the run would go.
    manifest : RunManifest
        The manifest of the run.

    Raises
    ------
    typer.BadParameter
        Raised when the destination is refused, saying what is there.
    """
    if mode is Mode.materialize:
        if state is DestinationState.link_to_run:
            return
        msg = (
            f"{ARG_MODE} {mode.value} replaces a link to this run with a copy, "
            f"and {destination} is not one: {_holds(state, destination, manifest)}."
        )
        raise typer.BadParameter(msg)

    match state:
        case DestinationState.free:
            return
        case DestinationState.link_to_run:
            msg = (
                f"{destination} is a link to this run already; "
                f"{ARG_MODE} {Mode.materialize.value} replaces it with a copy."
            )
        case DestinationState.promoted:
            msg = f"Run {manifest.run_id!r} is already promoted there: {destination}."
        case DestinationState.other_run | DestinationState.occupied:
            msg = (
                f"Not overwriting {destination}: "
                f"{_holds(state, destination, manifest)}."
            )
    raise typer.BadParameter(msg)


def _holds(state: DestinationState, destination: Path, manifest: RunManifest) -> str:
    """Say what a destination holds, for a refusal."""
    match state:
        case DestinationState.free:
            return "nothing is there"
        case DestinationState.link_to_run:
            return "it is a link to this run"
        case DestinationState.promoted:
            return f"it holds {run_uri(manifest.id)} already"
        case DestinationState.other_run:
            held = _read_manifest(destination)
            return (
                f"it holds another run, {run_uri(held.id)} ({held.run_id!r}), "
                f"and this one is {run_uri(manifest.id)}"
            )
        case DestinationState.occupied:
            return "it exists and is not a run"


def _refuse_move(source: PromoteSource, manifest: RunManifest) -> None:
    """Refuse to remove a run that may not be written, or that has not finished.

    Parameters
    ----------
    source : PromoteSource
        The run to move.
    manifest : RunManifest
        The manifest of the run, with the step records folded in.

    Raises
    ------
    typer.BadParameter
        Raised when the run is attached read-only, was found only by its
        manifest and not named by its directory, or has an attempt still open.
    """
    action = f"promote {ARG_MODE} {Mode.move.value}"
    if source.record is not None:
        try:
            require_writable(source.record, action=action)
        except CstarError as ex:
            raise typer.BadParameter(str(ex)) from ex
    elif source.indexed and not source.named_by_path:
        msg = (
            f"The run at {source.root} was found by its manifest and is not tracked "
            f"here, and {action} removes it. To move it deliberately, pass its "
            f"directory: `cstar run {COMMAND_PROMOTE} {source.root} "
            f"{ARG_MODE} {Mode.move.value}`."
        )
        raise typer.BadParameter(msg)

    if _open_attempt(manifest):
        msg = (
            f"Not moving {source.root}: the latest attempt has not finished; wait "
            f"for it, or copy it instead ({ARG_MODE} {Mode.copy.value})."
        )
        raise typer.BadParameter(msg)


def _open_attempt(manifest: RunManifest) -> bool:
    """Whether the latest attempt of the run has not finished."""
    return bool(manifest.attempts) and manifest.attempts[-1].finished_at is None


def _echo_plan(plan: CopyPlan) -> None:
    """Print what the plan copies: one line per group, then the total."""
    for group, entries in plan.groups.items():
        files = sum(1 for entry in entries if entry.kind == "file")
        size = _human_size(sum(entry.size_bytes for entry in entries))
        typer.echo(f"  {group}: {files} file(s), {size}")
    typer.echo(f"Total: {plan.file_count} file(s), {_human_size(plan.total_bytes)}")


def _regather(destination: Path, workplan: LiveWorkplan | None) -> None:
    """Rebuild the copy's `gathered_output` directory.

    The rebuild is skipped, with a message, when the copy's transformed workplan
    could not be loaded or does not point at the copy.

    Parameters
    ----------
    destination : Path
        The copy.
    workplan : LiveWorkplan | None
        The copy's transformed workplan, when it could be loaded.

    Raises
    ------
    typer.Exit
        With code 1 when a link name collides.
    """
    # gather links to resolved step directories, so the links are relative to the
    # resolved directory too, whichever way the run was reached
    dest = gathered_output_dir(destination.resolve())
    if workplan is None:
        reason = "no transformed workplan could be loaded"
    elif not steps_inside_run(destination, workplan):
        reason = "the transformed workplan describes a different directory"
    else:
        links, conflicts, _ = collect_links(workplan)
        if conflicts:
            report_conflicts(conflicts)
            raise typer.Exit(1)

        relink(dest, links)
        typer.echo(f"Rebuilt {dest} with {len(links)} link(s).")
        return

    typer.echo(f"Did not rebuild {dest}: {reason}.")


def _track_move(
    source: PromoteSource,
    manifest: RunManifest,
    destination: Path,
    workplan_path: Path | None,
) -> None:
    """Point this machine's tracking record of the run at its new directory.

    Parameters
    ----------
    source : PromoteSource
        The run that was moved; holds the record.
    manifest : RunManifest
        The manifest of the copy.
    destination : Path
        The copy's directory.
    workplan_path : Path | None
        The copy's transformed workplan.

    Raises
    ------
    CstarError
        Raised when the record cannot be written, or the copy has no workplan.
    """
    record = source.record
    if record is None:
        return
    if workplan_path is None:
        msg = f"the copy at {destination} holds no transformed workplan to track"
        raise CstarError(msg)

    mode = default_access(destination)
    fresh = build_attached_record(
        manifest,
        read_run_location(destination),
        run_id=record.run_id,
        mode=mode,
    )
    write_attached_record(Plan.reattach, fresh, mode)
    typer.echo(
        f"Moved run {record.run_id!r} ({run_uri(manifest.id)}) to {destination}; "
        f"it is tracked here as attached ({mode.value})."
    )
    if mode is Access.read_only:
        typer.echo(
            f"Nothing will be written into it; `cstar run attach {destination} "
            f"{ARG_OWN}` takes ownership."
        )


@dataclass(frozen=True)
class CopyCheck:
    """What checking a finished copy found."""

    report: RunCheckReport
    """The check's report, findings included."""
    own_problems: tuple[Finding, ...]
    """The problems of the copy itself: those the source's own paths are not."""
    manifest: RunManifest
    """The copy's manifest, with the step records folded in."""
    workplan: LiveWorkplan | None
    """The copy's transformed workplan, when it could be loaded."""
    workplan_path: Path | None
    """Where the copy's transformed workplan is."""


def _check_copy(destination: Path, *, verify: bool) -> CopyCheck:
    """Check a finished copy as `cstar run check` does, printing what it finds.

    Parameters
    ----------
    destination : Path
        The copy.
    verify : bool
        Whether to compare each product with its recorded digest.

    Returns
    -------
    CopyCheck
    """
    workplan_path = _transformed_workplan(destination)
    workplan = _load_workplan(workplan_path)
    manifest = _read_manifest(destination)
    fold_step_records(manifest, destination)
    report = check_run(
        destination,
        workplan=workplan,
        run_id=None,
        all_files=False,
        manifest=manifest,
        verify=verify,
    )
    for line in format_findings(report):
        typer.echo(line)
    typer.echo(summarize(report))

    own = tuple(p for p in report.problems if p.kind not in INHERITED_KINDS)
    return CopyCheck(report, own, manifest, workplan, workplan_path)


def _finish_move(source: PromoteSource, copy: CopyCheck, destination: Path) -> None:
    """Track the moved run, then remove the source: the record is never left behind the removal.

    Parameters
    ----------
    source : PromoteSource
        The run that was moved.
    copy : CopyCheck
        The checked copy.
    destination : Path
        The copy's directory.

    Raises
    ------
    typer.Exit
        With code 1 when the record cannot be written (the source is then still
        in place) or the source cannot be removed (the copy is then tracked).
    """
    with exit_on_failure("Promoted, but the move did not finish"):
        _track_move(source, copy.manifest, destination, copy.workplan_path)

    try:
        remove_run(source.root)
    except (CstarError, OSError) as ex:
        tracked = " and tracked it" if source.record is not None else ""
        typer.echo(
            f"Moved the copy to {destination}{tracked}, but the source at "
            f"{source.root} was not removed: {ex}",
            err=True,
        )
        raise typer.Exit(1) from ex


@app.command(name=COMMAND_PROMOTE, help=HELP_LONG, short_help=HELP_SHORT)
def promote(
    context: typer.Context,
    target: t.Annotated[
        str,
        typer.Argument(
            help=HELP_RUN_HANDLE,
            autocompletion=list_runs,
            callback=cb_pipeline(refuse_workplan_file, resolve_source),
        ),
    ],
    to: t.Annotated[str, typer.Option(ARG_TO, help=ARG_TO_HELP)] = "",
    mode: t.Annotated[Mode, typer.Option(ARG_MODE, help=ARG_MODE_HELP)] = Mode.copy,
    include: t.Annotated[
        list[Include], typer.Option(ARG_INCLUDE, help=ARG_INCLUDE_HELP)
    ] = [],
    dry_run: t.Annotated[
        bool, typer.Option(ARG_DRY_RUN, help=ARG_DRY_RUN_HELP)
    ] = False,
    verify: t.Annotated[bool, typer.Option(ARG_VERIFY, help=ARG_VERIFY_HELP)] = False,
) -> None:
    """Copy, link or move a run into the project's runs area."""
    source = get_from_ctxmap(context, KEY_SOURCE, PromoteSource)
    root = source.root

    project = DirectoryManager.project_runs_home()
    if (
        mode is Mode.link
        and project is not None
        and root.parent.resolve() == project.resolve()
    ):
        msg = f"The run at {root} is already in the project runs area: {project}."
        raise typer.BadParameter(msg)

    # refused up front for every source: the copy would carry both
    found = _transformed_workplan(root)
    workplan_path = source.record.trx_workplan_path if source.record else found
    manifest = _read_run(source, _load_workplan(workplan_path))

    destination = _destination(to, manifest.run_id)
    state = destination_state(destination, uuid=manifest.id)
    _refuse_destination(mode, state, destination, manifest)
    if mode is Mode.move:
        _refuse_move(source, manifest)
    elif _open_attempt(manifest):
        typer.echo(
            f"Warning: the latest attempt of {root} has not finished; "
            "the copy may be incomplete.",
            err=True,
        )

    handle = f"run {manifest.run_id!r} ({run_uri(manifest.id)})"
    if mode is Mode.link:
        if dry_run:
            typer.echo(f"Would link {handle} as {destination} -> {root}")
            return
        with exit_on_failure():
            try:
                link_run(destination, root)
            except CstarError as ex:
                raise typer.BadParameter(str(ex)) from ex
        typer.echo(f"Linked {handle} as {destination} -> {root}")
        return

    # a move removes the source, so it copies all of it
    extras = {Include.all} if mode is Mode.move else set(include)
    with exit_on_failure():
        plan = plan_promotion(root, manifest, include=extras)
        if plan.missing_products:
            typer.echo(
                f"Products the step records list that are missing from the source "
                f"({len(plan.missing_products)}):"
            )
            for missing in plan.missing_products:
                typer.echo(f"  {root / missing}")
            typer.echo(
                f"Not promoted: {len(plan.missing_products)} product(s) the step "
                f"records list are missing from the source; `cstar run check "
                f"{target}` it first.",
                err=True,
            )
            raise typer.Exit(1)

        _echo_plan(plan)
        if dry_run:
            typer.echo(f"Would {mode.value} {handle} to {destination}")
            return

        # before the copy: the digests are those of the source
        digests = compute_product_digests(root, manifest) if verify else {}
        try:
            execute_plan(plan, destination, replace_link=mode is Mode.materialize)
        except CstarError as ex:
            raise typer.BadParameter(str(ex)) from ex

    # the copy is complete: a failure from here on says where it is
    with exit_on_failure(f"Copied to {destination}, but the promotion did not finish"):
        if verify:
            recorded = record_product_digests(destination, digests)
            typer.echo(f"Recorded {recorded} digest(s) in the copy.")

        copy = _check_copy(destination, verify=verify)
        if copy.own_problems:
            kept = f"; the source was kept at {root}" if mode is Mode.move else ""
            typer.echo(
                f"Promoted with problems: {len(copy.own_problems)} problem(s) in the "
                f"copy at {destination}{kept}.",
                err=True,
            )
            raise typer.Exit(1)

        if gathered_output_dir(root).is_dir():
            _regather(destination, copy.workplan)

    if mode is Mode.move:
        _finish_move(source, copy, destination)
        if source.record is None:
            typer.echo(f"Moved {handle} to {destination}.")
    elif mode is Mode.materialize:
        typer.echo(
            f"Materialized {handle} at {destination} (the link is replaced; the "
            f"run it pointed at is left in place)."
        )
    else:
        typer.echo(f"Promoted {handle} to {destination}.")


if __name__ == "__main__":
    app()
