"""Copy a run elsewhere as a plan derived from its manifest: see `cstar run promote`.

A run directory holds a few small files that describe it, the products its steps
recorded, and a good deal that only mattered while it ran: compiled code, cloned
codebases, per-rank output pieces. `plan_promotion` lists what a copy should
hold, from the manifest alone: it walks only the small directories (`attempts/`,
each step's `logs/`) and, when asked, the large extras, and never reads a file's
contents. `execute_plan` makes the copy beside its destination and renames it
into place, so a failed copy leaves nothing at the destination.
"""

import os
import shutil
import stat
import typing as t
from dataclasses import dataclass
from enum import StrEnum
from itertools import chain
from pathlib import Path

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.execution.file_system import DirectoryManager, symlink_relative
from cstar.orchestration.manifest import (
    ATTEMPTS_DIR_NAME,
    RUN_MANIFEST_NAME,
    STEP_RECORD_NAME,
    read_run_manifest,
)
from cstar.orchestration.run_check import TRANSFORMED_WORKPLAN_GLOB
from cstar.roms.file_system import RomsFileSystemManager

if t.TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator, Mapping

    from cstar.orchestration.manifest import RunManifest

log = get_logger(__name__)

GROUP_ARTIFACTS: t.Final[str] = "artifacts"
"""The group of the files that describe the run: manifest, workplans, attempts, step records, logs."""

GROUP_EXTRAS: t.Final[str] = "extras"
"""The group of the large extras a plan includes when asked."""

_LEGACY_GLOBS: t.Final[tuple[str, ...]] = (
    f"{TRANSFORMED_WORKPLAN_GLOB}.*",
    "*_original.bak*",
    "*.vars*",
)
"""Globs, relative to a run directory, of the copies of the workplan that runs
scheduled before carried inputs left there, including their rotated `.1`, `.2` forms.
"""

_WORK_GLOBS: t.Final[tuple[str, ...]] = ("*.yaml", "*.yml")
"""Globs, relative to a step's work directory, of the files that hold its blueprint and directives."""

_COPY_BLOCK_BYTES: t.Final[int] = 64 * 2**20
"""The bytes `_copy_file` reads and writes at a time. Parallel file systems
report blocks of megabytes (GPFS's `st_blksize`), and a copy in blocks this
large runs several times faster there than the kernel's `sendfile` or
`copy_file_range`.
"""


def product_group(step: str) -> str:
    """Name the group of the products one step recorded.

    Parameters
    ----------
    step : str
        The step's name.

    Returns
    -------
    str
    """
    return f"products of {step}"


class Mode(StrEnum):
    """How a run is promoted."""

    copy = "copy"
    """A copy of the plan's files."""
    link = "link"
    """A symlink to the run in its place."""
    move = "move"
    """A copy, after which the source is removed."""
    materialize = "materialize"
    """A copy replacing an earlier link."""


class Include(StrEnum):
    """The large extras a plan may include."""

    builds = "builds"
    """Each ROMS step's compile-time code, runtime code and cloned codebases."""
    temp_output = "temp-output"
    """Each step's `temp_output/`, the per-rank output pieces."""
    all = "all"
    """The whole run directory."""


class DestinationState(StrEnum):
    """What a promotion's destination holds."""

    free = "free"
    """Nothing exists there."""
    link_to_run = "link-to-run"
    """A symlink to a directory that holds this run."""
    promoted = "promoted"
    """A directory that holds this run."""
    other_run = "other-run"
    """A directory that holds a different run."""
    occupied = "occupied"
    """Something else exists there: no run manifest can be read."""


@dataclass(frozen=True)
class Entry:
    """One path a copy plan holds."""

    relative: Path
    """The path, relative to the run root."""
    size_bytes: int
    """The file's size; 0 for a directory or a symlink."""
    kind: t.Literal["file", "link", "dir"]
    """A regular file, a symlink (never followed), or a directory."""


@dataclass(frozen=True)
class CopyPlan:
    """The paths of a run a copy holds, by group."""

    source: Path
    """The run root, absolute."""
    groups: "Mapping[str, tuple[Entry, ...]]"
    """The entries by group: `GROUP_ARTIFACTS`, then `product_group(step)` for
    each step with a record in manifest order, then `GROUP_EXTRAS` when non-empty.
    A path is in the first group that planned it.
    """
    missing_products: tuple[Path, ...]
    """The recorded products that are not on disk, relative to the run root."""
    directories: tuple[Path, ...]
    """The directories to create even when empty, relative to the run root: each
    step's working directory.
    """

    @property
    def entries(self) -> list[Entry]:
        """Every entry of every group, in order."""
        return list(chain.from_iterable(self.groups.values()))

    @property
    def total_bytes(self) -> int:
        """The size of the plan's files."""
        return sum(e.size_bytes for e in chain.from_iterable(self.groups.values()))

    @property
    def file_count(self) -> int:
        """The number of regular files in the plan."""
        entries = chain.from_iterable(self.groups.values())
        return sum(1 for entry in entries if entry.kind == "file")


def _entry(root: Path, relative: Path) -> Entry | None:
    """Describe one path of a run without following it.

    Parameters
    ----------
    root : Path
        The run root.
    relative : Path
        The path, relative to the run root.

    Returns
    -------
    Entry | None
        The entry, or `None` when the path does not exist or is neither a regular
        file, a symlink nor a directory (a fifo or socket cannot be copied).
    """
    try:
        info = (root / relative).lstat()
    except (FileNotFoundError, NotADirectoryError):
        return None

    if stat.S_ISLNK(info.st_mode):
        return Entry(relative, 0, "link")
    if stat.S_ISDIR(info.st_mode):
        return Entry(relative, 0, "dir")
    if stat.S_ISREG(info.st_mode):
        return Entry(relative, info.st_size, "file")

    log.debug("Skipping %s: not a regular file", root / relative)
    return None


def _tree(root: Path, relative: Path) -> "Iterator[Entry]":
    """List a path of a run and everything under it, never following a symlink.

    Parameters
    ----------
    root : Path
        The run root.
    relative : Path
        The path to list, relative to the run root; `.` for the whole run (which
        is not listed itself).

    Yields
    ------
    Entry
        The path (unless it is the run root) and each descendant, in a stable order.
    """
    top = _entry(root, relative)
    if relative == Path() and root.is_dir():
        # the root is given by the user, and may be a symlink to the run
        top = Entry(relative, 0, "dir")
    if top is None:
        return
    if relative != Path():
        yield top
    if top.kind != "dir":
        return

    for dirpath, dirnames, filenames in os.walk(root / relative, followlinks=False):
        here = Path(dirpath).relative_to(root)
        dirnames.sort()
        filenames.sort()
        for name in list(dirnames):
            child = _entry(root, here / name)
            if child is None:
                dirnames.remove(name)
                continue
            if child.kind == "link":
                dirnames.remove(name)
            yield child
        for name in filenames:
            if (child := _entry(root, here / name)) is not None:
                yield child


def _take(seen: set[Path], entries: "Iterable[Entry]") -> tuple[Entry, ...]:
    """Keep the entries no earlier group planned, and mark them planned.

    Parameters
    ----------
    seen : set[Path]
        The paths already planned; updated.
    entries : Iterable[Entry]
        The candidate entries.

    Returns
    -------
    tuple[Entry, ...]
    """
    taken: list[Entry] = []
    for entry in entries:
        if entry.relative not in seen:
            seen.add(entry.relative)
            taken.append(entry)
    return tuple(taken)


def _within(root: Path, name: str, *, base: Path = Path()) -> Path:
    """Resolve a relative path the manifest or a step record holds, lexically.

    Parameters
    ----------
    root : Path
        The run root, for the message.
    name : str
        The path as recorded.
    base : Path
        The directory it is relative to, relative to the run root.

    Returns
    -------
    Path
        The path relative to the run root, with `.` and `..` folded.

    Raises
    ------
    CstarError
        If the path is absolute or leaves the run directory.
    """
    path = Path(os.path.normpath(base / name))
    if path.is_absolute() or path.parts[:1] == ("..",):
        msg = f"The path {name!r} the run records leaves the run directory {root}."
        raise CstarError(msg)
    return path


def _artifacts(root: Path, manifest: "RunManifest") -> "Iterator[Entry]":
    """List the small files that describe a run.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The run's manifest.

    Yields
    ------
    Entry
    """
    names = [RUN_MANIFEST_NAME]
    names += sorted(p.name for p in root.glob(TRANSFORMED_WORKPLAN_GLOB))
    for pattern in _LEGACY_GLOBS:
        names += sorted(p.name for p in root.glob(pattern))
    for relative in (Path(name) for name in names):
        if (entry := _entry(root, relative)) is not None:
            yield entry
    yield from _tree(root, Path(ATTEMPTS_DIR_NAME))

    for step in manifest.steps:
        working = _within(root, step.working_dir)
        fs = RomsFileSystemManager(root / working)
        if (entry := _entry(root, working / STEP_RECORD_NAME)) is not None:
            yield entry
        yield from _tree(root, working / fs.logs_dir.relative_to(fs.root_dir))
        work = working / fs.run_dir.relative_to(fs.root_dir)
        for pattern in _WORK_GLOBS:
            for path in sorted((root / work).glob(pattern)):
                if (entry := _entry(root, work / path.name)) is not None:
                    yield entry


def _products(
    root: Path, working: Path, paths: "Iterable[str]", missing: list[Path]
) -> "Iterator[Entry]":
    """List the products a step recorded, as the files they are.

    Parameters
    ----------
    root : Path
        The run root.
    working : Path
        The step's working directory, relative to the run root.
    paths : Iterable[str]
        The recorded product paths, relative to the step's working directory.
    missing : list[Path]
        Collects the products that are not on disk; updated.

    Yields
    ------
    Entry
        One entry of kind `file` for each product; a symlinked product has the
        size of its target, since the copy follows it.
    """
    for name in paths:
        relative = _within(root, name, base=working)
        found = _entry(root, relative)
        if found is not None and found.kind == "link":
            try:
                info = (root / relative).stat()
            except OSError:
                found = None
            else:
                found = Entry(relative, info.st_size, "file")
                if stat.S_ISDIR(info.st_mode):
                    found = Entry(relative, 0, "dir")
        if found is None:
            missing.append(relative)
        elif found.kind == "dir":
            log.debug("Skipping product %s: a directory", root / relative)
        else:
            yield found


def _extras(
    root: Path, manifest: "RunManifest", include: "Collection[Include]"
) -> "Iterator[Entry]":
    """List the large extras a plan includes.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The run's manifest.
    include : Collection[Include]
        The extras asked for.

    Yields
    ------
    Entry
    """
    if Include.all in include:
        yield from _tree(root, Path())
        return

    for step in manifest.steps:
        working = _within(root, step.working_dir)
        fs = RomsFileSystemManager(root / working)
        wanted: list[Path] = []
        if Include.builds in include:
            wanted += [fs.compile_time_code_dir, fs.runtime_code_dir, fs.codebases_dir]
        if Include.temp_output in include:
            wanted.append(fs.temp_output_dir)
        for directory in wanted:
            yield from _tree(root, working / directory.relative_to(fs.root_dir))


def plan_promotion(
    root: Path, manifest: "RunManifest", *, include: "Collection[Include]" = ()
) -> CopyPlan:
    """Plan the copy of a run: its small artifacts, its recorded products and the extras asked for.

    Nothing is written, no file's contents are read, and each planned path is
    stat'ed once. Only `attempts/`, each step's `logs/` and the extras asked for
    are walked.

    Parameters
    ----------
    root : Path
        The run root.
    manifest : RunManifest
        The run's manifest, with its step records folded in (`fold_step_records`).
    include : Collection[Include]
        The large extras to include; `Include.all` makes the others redundant.

    Returns
    -------
    CopyPlan

    Raises
    ------
    CstarError
        If the manifest names a step directory or a product outside the run.
    """
    root = root.absolute()
    seen: set[Path] = set()
    groups: dict[str, tuple[Entry, ...]] = {
        GROUP_ARTIFACTS: _take(seen, _artifacts(root, manifest))
    }
    missing: list[Path] = []
    directories: dict[Path, None] = {}

    for step in manifest.steps:
        working = _within(root, step.working_dir)
        if working != Path():
            directories[working] = None
        if step.record is not None:
            paths = (product.path for product in step.record.products)
            groups[product_group(step.name)] = _take(
                seen, _products(root, working, paths, missing)
            )

    if extras := _take(seen, _extras(root, manifest, include)):
        groups[GROUP_EXTRAS] = extras

    plan = CopyPlan(root, groups, tuple(missing), tuple(directories))
    log.debug(
        "Planned %d files (%d bytes) of %s; %d recorded products missing",
        plan.file_count,
        plan.total_bytes,
        root,
        len(missing),
    )
    return plan


def destination_state(destination: Path, *, uuid: str) -> DestinationState:
    """Find out what a promotion's destination holds.

    Parameters
    ----------
    destination : Path
        The path the run would be promoted to.
    uuid : str
        The uuid of the run's manifest.

    Returns
    -------
    DestinationState
    """
    if not os.path.lexists(destination):
        return DestinationState.free

    try:
        found = read_run_manifest(destination)
    except (CstarError, OSError):
        found = None
    if found is None:
        return DestinationState.occupied
    if found.id != uuid:
        return DestinationState.other_run
    if destination.is_symlink():
        return DestinationState.link_to_run
    return DestinationState.promoted


def staging_dir(destination: Path) -> Path:
    """Name the directory a promotion copies into before renaming it to its destination.

    Parameters
    ----------
    destination : Path
        The path the run is promoted to.

    Returns
    -------
    Path
        A sibling of `destination`, so the rename never crosses a file system.
    """
    return destination.parent / f".{destination.name}.promoting"


def _link_text(plan: CopyPlan, entry: Entry) -> str:
    """Give the text a copied symlink should hold.

    A link into the source, absolute or relative, ends up relative to the same
    path in the copy, so the copy stays self-contained once the source is gone.
    A relative link that climbs out of the source becomes absolute, naming the
    file it reached from the source (the copy sits elsewhere, so the text would
    resolve to something else); any other absolute link keeps its text.

    Parameters
    ----------
    plan : CopyPlan
        The plan being executed.
    entry : Entry
        The symlink's entry.

    Returns
    -------
    str
    """
    text = os.readlink(plan.source / entry.relative)
    prefixes = {os.path.normpath(plan.source), os.path.normpath(plan.source.resolve())}

    if not os.path.isabs(text):
        here = os.path.dirname(plan.source / entry.relative)
        target = os.path.normpath(os.path.join(here, text))
        if any(_is_under(target, prefix) for prefix in prefixes):
            return text
        return target

    target = os.path.normpath(text)
    for prefix in prefixes:
        if _is_under(target, prefix):
            below = os.path.relpath(target, prefix)
            return os.path.relpath(below, entry.relative.parent)
    return text


def _is_under(path: str, prefix: str) -> bool:
    """Whether a normalized path is a directory or a path inside it."""
    return path == prefix or path.startswith(prefix + os.sep)


def _copy_into(plan: CopyPlan, staging: Path) -> int:
    """Copy a plan's entries under a directory.

    Parameters
    ----------
    plan : CopyPlan
        The plan to copy.
    staging : Path
        The directory to copy into; it exists.

    Returns
    -------
    int
        The bytes of the files copied.
    """
    for directory in plan.directories:
        (staging / directory).mkdir(parents=True, exist_ok=True)

    copied = 0
    for entry in plan.entries:
        source, target = plan.source / entry.relative, staging / entry.relative
        if entry.kind == "dir":
            target.mkdir(parents=True, exist_ok=True)
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        if entry.kind == "link":
            target.symlink_to(_link_text(plan, entry))
        else:
            _copy_file(source, target)
            copied += entry.size_bytes
    return copied


def _copy_file(source: Path, target: Path) -> None:
    """Copy a file's content in large blocks, then its metadata, as `shutil.copy2` does.

    `shutil.copy2` goes through `sendfile` on Linux, which GPFS serves at a
    fraction of the speed of buffered reads and writes of `_COPY_BLOCK_BYTES`.

    Parameters
    ----------
    source : Path
        The file to copy; a symlink is followed.
    target : Path
        The path of the copy; it is overwritten.
    """
    with source.open("rb") as src, target.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=_COPY_BLOCK_BYTES)
    shutil.copystat(source, target)


def execute_plan(
    plan: CopyPlan, destination: Path, *, replace_link: bool = False
) -> int:
    """Copy a plan to its destination, never overwriting.

    The copy is made in `staging_dir(destination)` and renamed into place, so an
    interrupted or failed copy leaves nothing at the destination, and removes
    its staging directory.

    Parameters
    ----------
    plan : CopyPlan
        The plan to copy.
    destination : Path
        The path of the copy, which must not exist.
    replace_link : bool
        Whether `destination` is a symlink the copy replaces; it is unlinked
        only after the copy succeeded.

    Returns
    -------
    int
        The bytes of the files copied.

    Raises
    ------
    CstarError
        If `destination` exists (and is not a symlink being replaced), its
        staging directory is left over from an earlier promotion, or the file
        system of `destination` has less free space than the plan's files.
    """
    if replace_link:
        if not destination.is_symlink():
            msg = f"Cannot replace {destination}: it is not a symlink."
            raise CstarError(msg)
    elif os.path.lexists(destination):
        msg = f"{destination} already exists; promote refuses to overwrite it."
        raise CstarError(msg)

    staging = staging_dir(destination)
    if os.path.lexists(staging):
        msg = (
            f"{staging} is left over from an earlier promotion of {destination}. "
            "Remove it and promote again."
        )
        raise CstarError(msg)

    destination.parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(destination.parent).free
    if free < plan.total_bytes:
        msg = (
            f"The copy needs {plan.total_bytes} bytes and {destination.parent} "
            f"has {free} free: short by {plan.total_bytes - free} bytes."
        )
        raise CstarError(msg)
    staging.mkdir()
    try:
        copied = _copy_into(plan, staging)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    previous = os.readlink(destination) if replace_link else None
    try:
        if previous is not None:
            destination.unlink()
        os.rename(staging, destination)
    except BaseException:
        if previous is not None and not os.path.lexists(destination):
            destination.symlink_to(previous)
        shutil.rmtree(staging, ignore_errors=True)
        raise

    log.debug(
        "Copied %d files (%d bytes) and %d other entries of %s to %s",
        plan.file_count,
        copied,
        len(plan.entries) - plan.file_count,
        plan.source,
        destination,
    )
    return copied


def link_run(destination: Path, root: Path) -> None:
    """Make a symlink at `destination` to a run, relative when both lie in the project home.

    Parameters
    ----------
    destination : Path
        The path of the link, which must not exist.
    root : Path
        The run root.

    Raises
    ------
    CstarError
        If `destination` exists.
    """
    if os.path.lexists(destination):
        msg = f"{destination} already exists; promote refuses to overwrite it."
        raise CstarError(msg)
    symlink_relative(destination, root, within=DirectoryManager.project_home())
    log.debug("Linked %s to %s", destination, root)


def remove_run(root: Path) -> None:
    """Remove a run directory.

    Parameters
    ----------
    root : Path
        The run root: a directory, not a symlink (the caller unlinks a linked run).

    Raises
    ------
    CstarError
        If `root` is not a directory, or is a symlink.
    """
    if root.is_symlink() or not root.is_dir():
        msg = f"Refusing to remove {root}: it is not a run directory."
        raise CstarError(msg)
    shutil.rmtree(root)
    log.debug("Removed %s", root)
