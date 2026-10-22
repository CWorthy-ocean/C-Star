"""Find a run by its run-id, its uuid or its directory.

The index is a scan of run manifests (`cstar-run.yaml`) under the directories
that hold runs: the user's data home and the project's runs area. It is built
lazily, at most once per process, and never persisted: no index file is written,
so there is nothing to go stale or to repair. Resolving a run-id opens only the
directories of that name, and never scans.

`resolve_run` is the one resolver. It answers with a `RunRecord`, whether the
run is tracked on this machine (`WorkplanRun`, with its live status) or known
only by its manifest (`ManifestRecord`), so consumers never ask which they hold.
"""

import functools
import os
import typing as t
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.execution.file_system import (
    DirectoryManager,
    JobFileSystemManager,
    StateDirectoryManager,
)
from cstar.orchestration.manifest import (
    DOCUMENT_READ_ERRORS,
    RunManifest,
    fold_step_records,
    read_run_manifest,
    status_of_record,
)
from cstar.orchestration.run_check import TRANSFORMED_WORKPLAN_GLOB
from cstar.orchestration.tracking import TrackingRepository, WorkplanRun
from cstar.system.migration import CstarMigrationError

if t.TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

log = get_logger(__name__)

RUN_URI_SCHEME: t.Final[str] = "run://"
"""The scheme of a handle that names a run by its uuid: `run://<uuid>`."""


class HandleKind(StrEnum):
    """What a run handle names."""

    run_id = "run_id"
    uuid = "uuid"
    path = "path"


@dataclass(frozen=True)
class RunHandle:
    """How a user names a run: by its run-id, its uuid or its directory."""

    kind: HandleKind
    """What `value` names."""

    value: str
    """The run-id as given, the canonical lowercase uuid, or the path as given."""

    def __str__(self) -> str:
        """The handle as it is authored: `run://<uuid>` for a uuid."""
        return run_uri(self.value) if self.kind is HandleKind.uuid else self.value


def is_path_like(text: str) -> bool:
    """Whether a text is spelled as a path rather than as a run-id.

    A run-id is a slug, so it never holds a path separator or starts with a dot
    or a tilde.

    Parameters
    ----------
    text : str
        The user-supplied text.

    Returns
    -------
    bool
    """
    return os.sep in text or text.startswith((".", "~"))


def run_uri(uuid: str) -> str:
    """Spell the handle that names a run by its uuid.

    Parameters
    ----------
    uuid : str
        The run's uuid.

    Returns
    -------
    str
        `run://<uuid>`.
    """
    return f"{RUN_URI_SCHEME}{uuid}"


def parse_run_handle(text: str) -> RunHandle:
    """Read what a user typed to name a run.

    The scheme is matched without regard to case and the text may be surrounded
    by whitespace. The run-id is not normalized: callers that slugify run-ids do
    so themselves. A uuid is canonicalized to lowercase.

    Parameters
    ----------
    text : str
        A run-id, a directory path, or `run://<uuid>`.

    Returns
    -------
    RunHandle

    Raises
    ------
    ValueError
        If the text is empty, or has the `run://` scheme but no valid uuid.
    """
    text = text.strip()
    if not text:
        msg = "A run handle is empty: give a run-id, a run directory or run://<uuid>."
        raise ValueError(msg)

    if text[: len(RUN_URI_SCHEME)].casefold() == RUN_URI_SCHEME:
        payload = text[len(RUN_URI_SCHEME) :].strip()
        try:
            return RunHandle(HandleKind.uuid, str(UUID(payload)))
        except ValueError as ex:
            msg = f"{text!r} is not a run handle: expected {RUN_URI_SCHEME}<uuid>."
            raise ValueError(msg) from ex

    kind = HandleKind.path if is_path_like(text) else HandleKind.run_id
    return RunHandle(kind, text)


class Origin(StrEnum):
    """Where a run's directory is kept."""

    local = "local"
    """The user's data home."""

    project = "project"
    """The project's runs area."""


class Durability(StrEnum):
    """How a run's directory survives a purge of the user's data home."""

    linked = "linked"
    """A symlink in the project area: as durable as its target."""

    materialized = "materialized"
    """A real directory in the project area."""

    none = "none"
    """A run in the user's data home, which nothing keeps."""


@dataclass(frozen=True)
class RunLocation:
    """A run directory found by the index, with the manifest read from it."""

    root: Path
    """The run directory as found; symlinks are not resolved, and the paths in the
    manifest are relative, so either spelling works."""

    manifest: RunManifest
    """The manifest in the run directory."""

    origin: Origin
    """Where the run directory is kept."""

    durability: Durability
    """How the run directory survives a purge of the user's data home."""

    @property
    def uuid(self) -> str:
        """The uuid the run's manifest holds."""
        return self.manifest.id

    @property
    def run_id(self) -> str:
        """The run-id the run's manifest holds."""
        return self.manifest.run_id


@dataclass(frozen=True)
class IndexRoot:
    """A directory that holds run directories."""

    path: Path
    """The directory."""

    origin: Origin
    """Where the runs in it are kept."""

    depth: int
    """How many levels down the run directories are: 1 for `<path>/<run>`, 2 for
    `<path>/<application>/<run>`."""


def scan_roots() -> list[IndexRoot]:
    """The directories that hold runs, the user's own before the project's.

    Returns
    -------
    list[IndexRoot]
        The workplan and blueprint runs of the data home and, when there is a
        project home, its runs area. Not all of them exist.
    """
    roots = [
        IndexRoot(StateDirectoryManager.workplan_runs_dir(), Origin.local, 1),
        IndexRoot(StateDirectoryManager.blueprint_runs_dir(), Origin.local, 2),
    ]
    if project := DirectoryManager.project_runs_home():
        roots.append(IndexRoot(project, Origin.project, 1))
    return roots


def _durability(root: Path, origin: Origin) -> Durability:
    """How a run directory found under a root of `origin` survives a purge."""
    if origin is Origin.local:
        return Durability.none
    return Durability.linked if root.is_symlink() else Durability.materialized


def _is_dir(path: Path) -> bool:
    """Whether `path` is a directory, `False` when it cannot be inspected."""
    try:
        return path.is_dir()
    except OSError as ex:
        log.debug(f"Unable to inspect {path}: {ex}")
        return False


def _children(parent: Path) -> list[Path]:
    """The directories directly in `parent`; none when it cannot be listed."""
    try:
        return sorted(child for child in parent.iterdir() if _is_dir(child))
    except OSError as ex:
        log.debug(f"Unable to list {parent}: {ex}")
        return []


def _parents(root: IndexRoot) -> list[Path]:
    """The directories whose children are the run directories of a root."""
    level = [root.path]
    for _ in range(root.depth - 1):
        level = [child for parent in level for child in _children(parent)]
    return level


def _open(
    directory: Path, origin: Origin, unreadable: list[tuple[Path, Exception]]
) -> RunLocation | None:
    """Read the manifest of a run directory.

    Parameters
    ----------
    directory : Path
        The run directory.
    origin : Origin
        Where the directory is kept.
    unreadable : list[tuple[Path, Exception]]
        Collects the directories whose manifest could not be read, with why.

    Returns
    -------
    RunLocation | None
        The run, or `None` when the directory holds no manifest (a run that
        predates it) or one that could not be read.
    """
    try:
        manifest = read_run_manifest(directory)
    except DOCUMENT_READ_ERRORS as ex:
        unreadable.append((directory, ex))
        return None
    if manifest is None:
        return None
    return RunLocation(directory, manifest, origin, _durability(directory, origin))


def _warn_unreadable(unreadable: "Sequence[tuple[Path, Exception]]") -> None:
    """Report the manifests that could not be read in one message."""
    if not unreadable:
        return
    paths = ", ".join(str(path) for path, _ in unreadable)
    log.warning(
        f"Skipped {len(unreadable)} run manifest(s) that could not be read: "
        f"{paths}. The first: {unreadable[0][1]}"
    )


class RunIndex:
    """The runs found under the directories that hold them."""

    def __init__(self, roots: "Sequence[IndexRoot] | None" = None) -> None:
        """Create an index that has not looked at anything yet.

        Parameters
        ----------
        roots : Sequence[IndexRoot] | None
            The directories to look in. `None` takes `scan_roots()` at first
            use, so the environment is read when the index is.
        """
        self._given = tuple(roots) if roots is not None else None
        self._roots: tuple[IndexRoot, ...] | None = None
        self._locations: list[RunLocation] | None = None

    @property
    def roots(self) -> tuple[IndexRoot, ...]:
        """The directories the index looks in."""
        if self._roots is None:
            self._roots = (
                self._given if self._given is not None else tuple(scan_roots())
            )
        return self._roots

    @property
    def scanned(self) -> bool:
        """Whether the index has scanned every run directory."""
        return self._locations is not None

    def locations(self) -> list[RunLocation]:
        """Every run the index finds.

        The first call scans the roots and keeps the result. A directory with no
        manifest is a run that predates it and is skipped silently; one whose
        manifest cannot be read is skipped with one warning for all of them.

        Returns
        -------
        list[RunLocation]
            The user's own runs before the project's.
        """
        if self._locations is None:
            unreadable: list[tuple[Path, Exception]] = []
            found = [
                location
                for root in self.roots
                for parent in _parents(root)
                for directory in _children(parent)
                if (location := _open(directory, root.origin, unreadable))
            ]
            _warn_unreadable(unreadable)
            self._locations = found
        return list(self._locations)

    def by_id(self, uuid: str) -> list[RunLocation]:
        """The runs whose manifest holds a uuid; this scans.

        Parameters
        ----------
        uuid : str
            The canonical lowercase uuid.

        Returns
        -------
        list[RunLocation]
        """
        return [loc for loc in self.locations() if loc.uuid == uuid]

    def by_run_id(self, run_id: str) -> list[RunLocation]:
        """The runs whose manifest holds a run-id.

        Once the index has scanned, this filters the result. Until then it
        opens only the directories named `run_id` and does not scan, so a
        manifest it has no reason to read is never touched.

        Parameters
        ----------
        run_id : str
            The run-id.

        Returns
        -------
        list[RunLocation]
        """
        if self._locations is not None:
            return [loc for loc in self._locations if loc.run_id == run_id]
        if not run_id or is_path_like(run_id):
            return []

        unreadable: list[tuple[Path, Exception]] = []
        found = [
            location
            for root in self.roots
            for parent in _parents(root)
            if _is_dir(parent / run_id)
            and (location := _open(parent / run_id, root.origin, unreadable))
            and location.run_id == run_id
        ]
        _warn_unreadable(unreadable)
        return found


@functools.cache
def default_index() -> RunIndex:
    """The index shared by this process.

    Built on first use and kept: a process scans the run directories at most
    once, however many runs it resolves.

    Returns
    -------
    RunIndex
    """
    return RunIndex()


class RunRecord(t.Protocol):
    """What a resolved run offers, whether it is tracked on this machine
    (`WorkplanRun`) or known only by its manifest (`ManifestRecord`).
    """

    @property
    def run_id(self) -> str:
        """The run's id."""
        ...

    @property
    def uuid(self) -> str:
        """The run's uuid; empty when it cannot be told."""
        ...

    @property
    def start_at(self) -> "datetime":
        """When the run was started."""
        ...

    @property
    def output_path(self) -> Path:
        """The run directory."""
        ...

    @property
    def trx_workplan_path(self) -> Path:
        """The transformed workplan in the run directory.

        Raises
        ------
        CstarError
            If the run has none.
        """
        ...

    def step_status(self, name: str) -> str:
        """The orchestration status name of a step; empty when unknown."""
        ...


@dataclass(frozen=True)
class ManifestRecord:
    """A run known only by its manifest: not tracked on this machine.

    It reads the run directory and writes nothing.
    """

    location: RunLocation
    """Where the run was found."""

    @property
    def run_id(self) -> str:
        """The run's id."""
        return self.location.run_id

    @property
    def uuid(self) -> str:
        """The run's uuid."""
        return self.location.uuid

    @property
    def output_path(self) -> Path:
        """The run directory."""
        return self.location.root

    @property
    def start_at(self) -> "datetime":
        """When the latest attempt was scheduled, else when the manifest was written."""
        manifest = self.location.manifest
        return (
            manifest.attempts[-1].start_at if manifest.attempts else manifest.created_at
        )

    @property
    def trx_workplan_path(self) -> Path:
        """The transformed workplan in the run directory.

        Raises
        ------
        CstarError
            If the run is a standalone blueprint run, or the directory holds
            no transformed workplan or several.
        """
        root = self.output_path
        if self.location.manifest.kind == "blueprint":
            msg = (
                f"Run {self.run_id!r} at {root} is a standalone blueprint run; "
                "it has no workplan"
            )
            raise CstarError(msg)

        found = sorted(root.glob(TRANSFORMED_WORKPLAN_GLOB))
        if len(found) == 1:
            return found[0]
        if not found:
            msg = f"Run {self.run_id!r} at {root} holds no transformed workplan"
        else:
            names = ", ".join(path.name for path in found)
            msg = (
                f"Run {self.run_id!r} at {root} holds more than one transformed "
                f"workplan ({names})"
            )
        raise CstarError(msg)

    @functools.cached_property
    def _folded(self) -> RunManifest:
        """A copy of the manifest with the step records on disk folded in.

        Read on first use and kept, so a record holds the state of the run's
        directory as of the first question about a step. Only the copy changes;
        nothing is written.
        """
        folded = self.location.manifest.model_copy(deep=True)
        try:
            fold_step_records(folded, self.output_path)
        except DOCUMENT_READ_ERRORS as ex:
            log.warning(f"Using the recorded status of run {self.run_id!r}: {ex}")
        return folded

    def step_status(self, name: str) -> str:
        """The orchestration status name of a step.

        The status its step record stands for, else the one the manifest last
        recorded. A run that is not tracked here is `Done` only when its step
        records say it completed.

        Parameters
        ----------
        name : str
            The name of the step.

        Returns
        -------
        str
            The status name, or an empty string when the manifest has no such
            step or the step has no status.
        """
        entry = self._folded.step(name)
        if entry is None:
            return ""
        return status_of_record(entry.record) if entry.record else entry.status


def _tracked_for(location: RunLocation, *, same_root: bool) -> WorkplanRun | None:
    """The tracking record of the run at a location, when it is that run.

    Parameters
    ----------
    location : RunLocation
        The run found.
    same_root : bool
        Whether the record's run directory must be the location's own: the
        user named that directory.

    Returns
    -------
    WorkplanRun | None
        The record, when tracking holds one for the run-id under the run's uuid
        and its run directory exists; otherwise `None`.
    """
    if location.manifest.kind != "workplan":
        return None
    try:
        tracked = TrackingRepository().get_workplan_run_sync(location.run_id)
        if tracked is None or not tracked.output_path.is_dir():
            return None
        if tracked.uuid != location.uuid:
            return None
        if same_root and tracked.output_path.resolve() != location.root.resolve():
            return None
    except (ValueError, OSError) as ex:
        log.debug(f"Unable to use the tracking record of {location.run_id!r}: {ex}")
        return None
    return tracked


def _prefer_tracked(
    locations: "Sequence[RunLocation]", *, same_root: bool
) -> RunRecord:
    """The tracking record of the first location it exists for, else the first's manifest.

    Parameters
    ----------
    locations : Sequence[RunLocation]
        The places one run was found; not empty.
    same_root : bool
        Whether a tracking record must be of the location's own directory.

    Returns
    -------
    RunRecord
    """
    for location in locations:
        if (tracked := _tracked_for(location, same_root=same_root)) is not None:
            return tracked
    return ManifestRecord(locations[0])


def _read_directory(root: Path) -> RunLocation:
    """Read the run in a directory the user named.

    Raises
    ------
    CstarError
        If it is not a directory, holds no manifest, or holds one that cannot
        be read.
    """
    if not root.is_dir():
        msg = f"{str(root)!r} is not a directory."
        raise CstarError(msg)

    try:
        manifest = read_run_manifest(root)
    except CstarMigrationError as ex:
        raise CstarError(str(ex)) from ex
    if manifest is None:
        msg = f"{str(root)!r} holds no run manifest."
        if JobFileSystemManager(root).tasks_dir.is_dir():
            msg += (
                " The run predates the manifest: re-entering it with "
                "`cstar workplan run --run-id <run-id>` records one."
            )
        raise CstarError(msg)

    project = DirectoryManager.project_runs_home()
    origin = (
        Origin.project
        if project is not None and root.parent.resolve() == project
        else Origin.local
    )
    return RunLocation(root, manifest, origin, _durability(root, origin))


def _resolve_path(path: str, uuid: str) -> RunRecord:
    """Resolve a directory the user named."""
    root = Path(os.path.abspath(Path(path).expanduser()))
    location = _read_directory(root)
    if uuid and location.uuid != uuid:
        msg = (
            f"The run at {root} is {run_uri(location.uuid)}, not {run_uri(uuid)}: "
            "it is not the run that was asked for."
        )
        raise CstarError(msg)
    return _prefer_tracked([location], same_root=True)


def _resolve_uuid(wanted: str, index: RunIndex) -> RunRecord:
    """Resolve the run a manifest names by its uuid."""
    found = index.by_id(wanted)
    if not found:
        roots = ", ".join(str(root.path) for root in index.roots)
        msg = f"No run has the uuid {wanted}: no manifest under {roots} names it."
        raise CstarError(msg)
    return _prefer_tracked(found, same_root=False)


def _warn_if_shadowed(tracked: WorkplanRun, run_id: str) -> None:
    """Warn when the project's runs area holds another run of the tracked run-id.

    Costs one failed `open` when the project area holds no such directory.
    """
    runs = DirectoryManager.project_runs_home()
    if runs is None:
        return
    try:
        other = read_run_manifest(runs / run_id)
    except DOCUMENT_READ_ERRORS as ex:
        log.debug(f"Unable to read a run of {run_id!r} in {runs}: {ex}")
        return

    if other is None:
        return
    # read only now: an unstamped record costs a parse of its run's manifest
    mine = tracked.uuid
    if not mine or other.id == mine:
        return
    log.warning(
        f"Run {run_id!r} is tracked here as {run_uri(mine)} at {tracked.output_path}, "
        f"which is the one used. Another run with that run-id is in the project area: "
        f"{run_uri(other.id)} at {runs / run_id}. Pass {run_uri(other.id)} or its "
        "directory to use that one."
    )


def _resolve_run_id(
    run_id: str, uuid: str, start_at: "datetime | None", index: RunIndex
) -> RunRecord:
    """Resolve a run-id: tracking first, then the manifests of that name."""
    try:
        tracked = TrackingRepository().get_workplan_run_sync(run_id, run_date=start_at)
    except ValueError as ex:
        msg = f"Unable to look up run {run_id!r}: {ex}"
        raise CstarError(msg) from ex

    if tracked is not None and (not uuid or tracked.uuid in ("", uuid)):
        if tracked.output_path.is_dir():
            if not uuid:
                _warn_if_shadowed(tracked, run_id)
            return tracked

        # the scratch was purged, or the run moved: a copy of the same run may
        # be kept elsewhere
        wanted = uuid or tracked.uuid
        if wanted and (
            copies := [loc for loc in index.by_run_id(run_id) if loc.uuid == wanted]
        ):
            log.info(
                f"The directory recorded for run {run_id!r}, {tracked.output_path}, "
                f"is gone; using the copy at {copies[0].root}"
            )
            return ManifestRecord(copies[0])
        return tracked

    candidates = index.by_run_id(run_id)
    if uuid:
        candidates = [loc for loc in candidates if loc.uuid == uuid] or [
            loc for loc in index.by_id(uuid) if loc.run_id == run_id
        ]

    uuids = list(dict.fromkeys(loc.uuid for loc in candidates))
    if not uuids:
        roots = ", ".join(str(root.path) for root in index.roots)
        msg = (
            f"No run found for run-id {run_id}: it is not tracked here and no "
            f"manifest under {roots} names it. `cstar run ls --all` lists the "
            "runs there are."
        )
        raise CstarError(msg)
    if len(uuids) > 1:
        listed = "\n".join(
            f"  {run_uri(loc.uuid)} at {loc.root} ({loc.origin})" for loc in candidates
        )
        msg = (
            f"More than one run has the run-id {run_id}:\n{listed}\n"
            "Pass run://<uuid> or the run's directory to say which."
        )
        raise CstarError(msg)
    return _prefer_tracked(candidates, same_root=False)


def resolve_run(
    handle: RunHandle,
    *,
    uuid: str = "",
    start_at: "datetime | None" = None,
    index: RunIndex | None = None,
) -> RunRecord:
    """Resolve a handle to the run it names.

    The one resolver. A run tracked on this machine answers with its tracking
    record, which has the live status; any other run answers with its manifest.

    - A directory handle reads the manifest in that directory.
    - A uuid handle finds the manifests holding that uuid, preferring one that
      tracking also holds.
    - A run-id handle goes to tracking first, and a tracked run wins outright:
      when the project area holds another run of that run-id, a warning says so
      and names how to pick it. If the tracked run's directory is gone, a copy of
      the same run in the index is used. Otherwise the manifests of that run-id
      are looked for, and more than one run under it is an error that lists
      them, never settled by recency.

    Parameters
    ----------
    handle : RunHandle
        What the user named.
    uuid : str
        The canonical uuid of the run the caller means, when it knows it, such
        as a scheduled workplan that recorded the run it follows; empty to
        take whatever the handle names.
    start_at : datetime | None
        The start of the run to look up in tracking, when the caller knows it.
    index : RunIndex | None
        The index to look in; the process's shared one by default.

    Returns
    -------
    RunRecord

    Raises
    ------
    CstarError
        If no run is found, a directory is no run, the run found is not the one
        in `uuid`, or a run-id names several runs.
    """
    if index is None:
        index = default_index()
    match handle.kind:
        case HandleKind.path:
            return _resolve_path(handle.value, uuid)
        case HandleKind.uuid:
            if uuid and uuid != handle.value:
                msg = f"{handle} was given with another uuid, {run_uri(uuid)}."
                raise CstarError(msg)
            return _resolve_uuid(handle.value, index)
        case HandleKind.run_id:
            return _resolve_run_id(handle.value, uuid, start_at, index)
