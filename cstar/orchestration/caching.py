"""Decorator applying the artifact cache to a function that produces a file.

The check / retrieve / cache flow is mechanical: derive a key from the declared
inputs, ask the cache, return the hit, and on a miss run the work and write the
result through. :func:`cached` performs it around an ordinary function, so the
function itself stays a plain producer with no knowledge of the cache.

What makes that possible without annotations is that the inputs which determine
a key are already distinguishable by type. A
:class:`~cstar.orchestration.models.Resource` declares *what* data is wanted and
a registered *companion* — the geometry it is split across, say — declares
*how it is laid out*; between them they are the key. The decorator binds the
call, finds them among the arguments, and derives the key before the function
runs. Which types can pair is asked of the key registry rather than named
here, so an application registers its own without this module learning it
exists.

Examples
--------
>>> @cached(cache_factory=get_cache)
... def fetch_boundary(
...     resource: VersionedResource, run_id: str, destination: Path
... ) -> Path:
...     download(resource.location, destination)
...     return destination

The function is unchanged by the decoration and still callable directly in a
test, and the caller always receives the path it asked for — never a path into
the cache. On a miss the producer writes there; on a hit the cached artifact is
copied there. A cache path handed to a caller is a cache path something will
eventually write through, and one client editing an artifact in place corrupts
it for every other run reading that key.

Notes
-----
This is deliberately small. It covers a producer taking one resource and
returning one path, which is the shape most preprocessing steps have. A step
consuming several resources, or one whose output is not determined by its
declared inputs alone, should call the cache directly rather than be forced
through a decorator that cannot express it.

Choosing a key
--------------
Because the shared tier is addressed by name alone, the name has to determine
the content. Everything below is a way of arriving at a name; they differ in
how much they can promise, and the cost of the weaker ones is paid by whoever
is confused six months from now, not by the person writing the call.

Take the first one that fits.

**1. A declared resource —** :func:`cached`, or
:func:`~cstar.orchestration.cache_keys.resource_key` directly.

    Use when the output is determined by a blueprint declaration. The key is
    *input-addressed*: derived from the declaration before the work runs,
    which is what lets a lookup skip the work rather than confirm it after the
    fact. Strongest guarantee available, and the only option that costs
    nothing to check.

**2. A type of your own —** :func:`~cstar.orchestration.cache_keys.identity_for`
on an identity function.

    Use when the output is determined by something that is not a blueprint
    resource — a solver configuration, a derived request object. Same
    guarantee as above, because it is the same mechanism; you are supplying
    the part that says which fields matter. The judgement is entirely in that
    function: a field that changes the output and is omitted puts two
    different artifacts under one key, while a field that cannot change the
    output costs a needless recompute every time it is edited. Omitting is
    the dangerous direction — too much in the key wastes time, too little
    returns the wrong file.

**3. Files you already have —** :func:`fileset_for` with :func:`cache_fileset`.

    Use when there is no declaration at all: a directory exists and should be
    kept. Keyed on the members' absolute paths, which is sound on a shared
    filesystem and costs a stat rather than a pass over the data — but it
    cannot see an in-place edit, so a changed file under a stable path serves
    the old contents. Reach for this when the files are produced once and not
    revised; reach for 2 instead when they are.

**4. A name you chose —** :meth:`~cstar.orchestration.artifact_cache.ArtifactCache.ingest`
and its set-shaped sibling ``ingest_aggregate``.

    The escape hatch. It is not without protection: names are validated as
    single path components, writes land in the user tier only, and promotion
    refuses a shared name already holding different bytes. What no mechanism
    can supply is the thing a derived key gives you for free — a hand-written
    name carries no record of what produced it, so nothing catches two
    configurations that share a name today and diverge after a code change.
    Worth using when you know the naming discipline holds; worth documenting
    at the call site when you do.

One rule cuts across all four: a key must be a pure function of what actually
determines the output, and of nothing else. A stale key looks exactly like a
cold cache — the work simply runs again, silently, and nobody files a bug — so
key scope is not a detail that surfaces on its own.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import os
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from cstar.base.env import ENV_CSTAR_ARTIFACT_CACHE_ENABLED, ENV_CSTAR_RUNID
from cstar.base.feature import is_flag_enabled
from cstar.base.log import get_logger
from cstar.orchestration.artifact_cache import (
    SET_MANIFEST_NAME,
    ArtifactCache,
    Location,
    OnConflict,
    SetManifest,
)
from cstar.orchestration.cache_keys import (
    AGGREGATE_SUFFIX,
    CacheKeyError,
    aggregate_key,
    generator_for,
    identity_for,
    is_registered,
    resource_key,
)
from cstar.orchestration.models import Resource

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

__all__ = [
    "ArtifactProducer",
    "CachedCallError",
    "FileSet",
    "cache_fileset",
    "cached",
    "fileset_for",
    "fileset_identity",
    "fileset_key",
    "to_cached_artifact",
    "to_cached_fileset",
]

Params = ParamSpec("Params")
"""The wrapped producer's own parameters, preserved through decoration."""

log = get_logger(__name__)


class ArtifactProducer(Protocol[Params]):
    """A function that writes one artifact to the path it is given.

    The contract :func:`cached` wraps. Two halves of it are enforced in
    different places, because Python's type system can express one and not the
    other.

    **Its return value is ignored.** A producer may return whatever suits its
    other callers; the decorated function returns the destination regardless.
    The type is ``object`` rather than ``Any`` so that the ignored value stays
    inert: every type is assignable *to* ``object``, so any producer is
    accepted, but nothing flows back *out* of it without an explicit narrowing
    check. ``Any`` would accept the same producers while silently satisfying
    any annotation the value reached and disabling checks on everything
    derived from it.

    Note what is being ignored. On a hit the producer is never called, so a
    return value is not merely discarded but unobtainable — a producer whose
    result carries information beyond the artifact on disk cannot be cached
    correctly, because that information has nowhere to come from.

    Whether the write succeeded is reported the way Python reports it — by
    raising — which also carries *why*, as a status value cannot.

    **Writes to its destination argument.** No type system can require a
    parameter of a particular name, so this half is checked at call time:
    :func:`cached` refuses a producer with no destination argument, and the
    commit refuses one that left the destination empty. A producer that writes
    somewhere else is therefore caught, just not statically.

    Examples
    --------
    >>> def fetch_boundary(
    ...     resource: VersionedResource, run_id: str, destination: Path
    ... ) -> None:
    ...     download(resource.location, destination)
    """

    def __call__(self, *args: Params.args, **kwargs: Params.kwargs) -> object:
        """Write the artifact to the destination among the arguments.

        Parameters
        ----------
        *args : Params.args
            The producer's own positional arguments.
        **kwargs : Params.kwargs
            The producer's own keyword arguments.

        Returns
        -------
        object
            Ignored. Producers commonly return ``None``.
        """
        ...  # pragma: no cover - a structural protocol has no implementation


class CachedCallError(Exception):
    """Raised when a decorated call cannot be cached as declared."""


@dataclass(frozen=True)
class FileSet:
    """A specific collection of files, identified by where they live.

    The escape hatch for "just make sure these files are cached". Everything
    else here is *input-addressed* — keyed on a declaration, before the work
    runs — because that is what lets a lookup skip the work. A file set has no
    declaration to key on: the caller has files already and wants them kept.
    It is keyed on its members' **absolute paths** instead.

    That works because these caches live on a shared filesystem, where
    ``/scratch/project/forcing/a.nc`` names the same bytes for every user on
    the allocation. Including the containing directory is what keeps two
    unrelated directories that happen to share filenames from serving each
    other's data.

    Warnings
    --------
    A path is not an identity. **Editing a file in place is invisible here**:
    same paths, same key, and the cache serves what it stored before the edit.
    Deriving the key from the members' contents instead would catch that, at
    the cost of reading every byte before any lookup can answer. This type
    takes the cheap side of that trade deliberately; anything whose contents
    change under a stable path should be keyed on a declaration through
    :func:`resource_key` or a registered type of its own, not stored here.

    Build one with :func:`fileset_for` rather than by hand.

    Attributes
    ----------
    root : Path
        Directory the members are relative to. Part of the identity, unlike
        elsewhere in this module.
    members : tuple of str
        Container-relative POSIX paths, sorted.
    """

    root: Path
    members: tuple[str, ...]

    @property
    def path_digest(self) -> str:
        """str: Digest over the members' absolute paths.

        Notes
        -----
        Absolute rather than relative, so the containing directory
        participates. Folded into one digest rather than listed field by field
        so that a set of ten thousand members still produces a bounded
        identity.
        """
        rolling = hashlib.sha256()
        for member in self.members:
            rolling.update(str(self.root / member).encode())
            rolling.update(b"\0")
        return rolling.hexdigest()


@identity_for(FileSet, "fileset")
def fileset_identity(fileset: FileSet) -> dict[str, str]:
    """Return the fields identifying a file set's contents.

    The wildcard that selected the members is deliberately absent. Two
    different patterns that select the same files produce the same artifact,
    and keying on the pattern would split the cache on a difference that
    cannot change which files were chosen.

    Parameters
    ----------
    fileset : FileSet
        Value being keyed.

    Returns
    -------
    dict of str to str
        Identifying fields.
    """
    return {
        "fileset.paths": fileset.path_digest,
        "fileset.count": str(len(fileset.members)),
    }


def fileset_for(path: Path | str, wildcard: str | None = None) -> FileSet:
    """Discover the files under a directory and describe them as a set.

    Parameters
    ----------
    path : Path or str
        Directory to search.
    wildcard : str or None, optional
        Pattern passed to :meth:`pathlib.Path.rglob`. Defaults to every file.
        Only what the pattern selects is described, and therefore only what it
        selects is ever cached.

    Returns
    -------
    FileSet
        The discovered files.

    Notes
    -----
    Discovery stats the tree and reads nothing, so this stays cheap on a set
    whose members are large. That is the direct consequence of keying on paths
    rather than contents; see :class:`FileSet`.

    Raises
    ------
    FileNotFoundError
        If ``path`` is not a directory.
    ValueError
        If the pattern selects nothing. An empty set would key identically to
        every other empty set and cache nothing useful, so it is a mistake
        worth reporting rather than a no-op.

    Examples
    --------
    >>> fileset_for(work_dir, "*.nc")
    FileSet(root=..., members=('rank000.nc', ...))
    """
    root = Path(path).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"not a directory: {root}")

    found = sorted(entry for entry in root.rglob(wildcard or "*") if entry.is_file())
    if not found:
        pattern = wildcard or "*"
        raise ValueError(f"no files matched {pattern!r} beneath {root}")

    members = tuple(str(PurePosixPath(entry.relative_to(root))) for entry in found)
    return FileSet(root=root, members=members)


def fileset_key(fileset: FileSet, *, name: str | None = None) -> str:
    """Derive the cache key naming a file set.

    Parameters
    ----------
    fileset : FileSet
        Set being named.
    name : str or None, optional
        Readable stem for the key. Defaults to the root directory's name.

    Returns
    -------
    str
        Key carrying :data:`~cstar.orchestration.cache_keys.AGGREGATE_SUFFIX`,
        since a file set is a collection rather than a file.
    """
    stem = name or fileset.root.name or "fileset"
    return generator_for(FileSet).key_for(fileset, stem, suffix=AGGREGATE_SUFFIX)


def cache_fileset(
    cache: ArtifactCache,
    fileset: FileSet,
    run_id: str,
    *,
    name: str | None = None,
    promote: bool = False,
    on_conflict: OnConflict = OnConflict.SKIP,
) -> Location:
    """Ensure a file set is in the cache, and return where it lives.

    Idempotent: a set already present is returned untouched, since its key is
    derived from its members' paths and the same paths are the same artifact.
    Note what that means when a member has been edited since — see
    :class:`FileSet`.

    Only the set's own members are stored. Anything else under
    :attr:`FileSet.root` is excluded explicitly rather than by omission — a
    directory holding ``a.txt`` and ``b.csv``, described with ``*.txt``, yields
    a container holding ``a.txt`` alone.

    Parameters
    ----------
    cache : ArtifactCache
        Cache to write into.
    fileset : FileSet
        Set to store.
    run_id : str
        Run storing it.
    name : str or None, optional
        See :func:`fileset_key`.
    promote : bool, optional
        Whether to publish to the shared tier. Off by default, as elsewhere:
        publishing is a separate decision from producing.
    on_conflict : OnConflict, optional
        How promotion resolves a shared name already holding different bytes.

    Returns
    -------
    Location
        The expanded container in the user tier.
    """
    key = fileset_key(fileset, name=name)
    found = cache.materialize(key, run_id, record_use=True)
    if found is None:
        found = cache.ingest_aggregate(
            fileset.root, key, run_id, members=fileset.members
        )
    if promote:
        cache.promote(key, run_id, on_conflict=on_conflict)
    return found


def cached(
    *,
    cache: ArtifactCache | None = None,
    cache_factory: Callable[[], ArtifactCache] | None = None,
    run_id_argument: str = "run_id",
    destination_argument: str = "destination",
    context: Mapping[str, str] | None = None,
    promote: bool = False,
    on_conflict: OnConflict = OnConflict.SKIP,
) -> Callable[[ArtifactProducer[Params]], Callable[Params, Path]]:
    """Wrap a file-producing function in the check / retrieve / cache flow.

    The wrapped function is called only on a miss. Its return value is taken to
    be a path in the caller's workspace, which is then written through the
    cache; the path handed back to the caller is the cached one.

    Parameters
    ----------
    cache : ArtifactCache or None, optional
        Cache to use. Mutually exclusive with ``cache_factory``.
    cache_factory : Callable returning ArtifactCache or None, optional
        Called on each invocation to obtain the cache. Prefer this in
        production, where the roots come from configuration that is not
        available at import time.
    run_id_argument : str, optional
        Name of the wrapped function's parameter carrying the run identifier.
    destination_argument : str, optional
        Name of the wrapped function's parameter carrying the path it writes
        to. The producer must accept one: on a miss it writes there and the
        result is taken from it, and on a hit the cached artifact is copied
        there. That symmetry is what lets the caller always receive the path
        it asked for rather than a path into the cache.
    context : Mapping of str to str or None, optional
        Extra inputs folded into the key — a code revision, a solver version.
        Anything that changes the output and is omitted here will make two
        different artifacts share a key. Values are strings so that their
        spelling is a decision rather than an accident of ``repr``.
    promote : bool, optional
        Whether a freshly produced artifact is published to the shared tier.
        Off by default: publishing is a separate decision about what belongs in
        a space everyone shares, and a producer is the wrong place to make it.
        Promote out of band once the artifact is known to be worth keeping.
    on_conflict : OnConflict, optional
        How promotion resolves a shared name already holding different bytes.
        Defaults to :attr:`OnConflict.SKIP`, since a re-derivation differing
        only in a header timestamp should not fail a long run.

    Returns
    -------
    Callable
        Decorator preserving the wrapped function's parameters while
        correcting its return type: the producer returns nothing, the
        decorated function returns the destination.

    Raises
    ------
    ValueError
        If neither or both of ``cache`` and ``cache_factory`` are given.

    Notes
    -----
    Artifact shape follows the arguments. A registered companion among them
    means the result is a set of files in a directory; without one it is a
    single file. Which key function derives the name depends on
    whether the resource declares itself already partitioned: if it does, the
    geometry describes the source and
    :func:`~cstar.orchestration.cache_keys.resource_key` handles it; if it does
    not, the geometry is being *produced* and
    :func:`~cstar.orchestration.cache_keys.aggregate_key` names the derived set
    in its own key space.
    """
    if (cache is None) == (cache_factory is None):
        raise ValueError("pass exactly one of cache or cache_factory")

    def decorate(
        function: ArtifactProducer[Params],
    ) -> Callable[Params, Path]:
        signature = inspect.signature(function)

        @functools.wraps(function)
        def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Path:
            active = cache if cache is not None else cache_factory()  # type: ignore[misc]
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()

            resource = _single_of_type(bound.arguments, Resource, function)
            companion = _companion_for(resource, bound.arguments, function)
            run_id = _run_id(bound.arguments, run_id_argument, function)
            destination = _destination(bound.arguments, destination_argument, function)
            key = _key_for(resource, companion, context)

            found = (
                active.materialize(key, run_id, record_use=True)
                if companion is not None
                else active.resolve(key, run_id, record_use=True)
            )
            if found is not None:
                _deliver(found, destination)
                return destination

            function(*args, **kwargs)
            _write_through(active, destination, key, run_id, companion)
            if promote:
                active.promote(key, run_id, on_conflict=on_conflict)
            return destination

        return wrapper

    return decorate


def _deliver(found: Location, destination: Path) -> None:
    """Copy a cached artifact to where the caller asked for it.

    The cache's own path is never handed out. A caller given one has nothing
    stopping it opening that path for writing, and a client that edits a
    shared artifact in place corrupts it for every other run on the
    allocation — silently, until someone checks a digest.

    An existing destination is replaced. The caller named this path for this
    artifact, and a partial file left by a killed run is exactly what should be
    overwritten; refusing would turn re-running a step into an error rather
    than the no-op it ought to be.

    Parameters
    ----------
    found : Location
        The cached artifact, in either tier.
    destination : Path
        Where the caller asked for it.

    Raises
    ------
    CachedCallError
        If the destination cannot be replaced by an artifact of that shape.
    """
    if found.path == destination:
        return

    destination.parent.mkdir(parents=True, exist_ok=True)

    if found.is_container:
        if destination.exists() and not destination.is_dir():
            raise CachedCallError(
                f"{str(destination)!r} is a file, but {found.name!r} is a set "
                "and needs a directory"
            )
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(found.path, destination)
        return

    if destination.is_dir():
        raise CachedCallError(
            f"{str(destination)!r} is a directory, but {found.name!r} is a single file"
        )
    shutil.copy2(found.path, destination)


def _destination(
    arguments: Mapping[str, Any], name: str, function: Callable[..., Any]
) -> Path:
    """Return the path the wrapped function was told to write to.

    Parameters
    ----------
    arguments : Mapping of str to Any
        Bound arguments of the call.
    name : str
        Parameter name carrying the destination.
    function : Callable
        Wrapped function, named in errors.

    Returns
    -------
    Path
        Where the artifact should end up.

    Raises
    ------
    CachedCallError
        If the parameter is absent or is not a path. Without it a hit has
        nowhere to put the artifact, since the function that would have chosen
        a path is precisely the one being skipped.
    """
    value = arguments.get(name)
    if not isinstance(value, (str, Path)) or not str(value):
        raise CachedCallError(
            f"{function.__qualname__} must take a {name!r} argument naming the "
            "path to write to; on a hit the function is not called, so there "
            "is nothing else to say where the artifact belongs"
        )
    return Path(value)


def _companion_for(
    resource: Resource, arguments: Mapping[str, Any], function: Callable[..., Any]
) -> Any | None:
    """Return the argument that pairs with the resource to identify the result.

    Found by asking the key registry which of the bound arguments has a
    registered pairing with this resource, rather than by naming a type. That
    is what keeps this module free of the application types it caches: an
    application registers its own pairing, and this discovers it.

    Parameters
    ----------
    resource : Resource
        Declared input found among the arguments.
    arguments : Mapping of str to Any
        Bound arguments of the call.
    function : Callable
        Wrapped function, named in errors.

    Returns
    -------
    Any or None
        The single companion, or ``None`` when the call has none.

    Raises
    ------
    CachedCallError
        If more than one argument pairs with the resource, since the key would
        be ambiguous.
    """
    found = [
        value
        for name, value in arguments.items()
        if value is not resource
        and not isinstance(value, (str, bytes, int, float, bool, Path, type(None)))
        and is_registered((type(resource), type(value)))
    ]
    if not found:
        return None
    if len(found) > 1:
        raise CachedCallError(
            f"{function.__qualname__} takes {len(found)} arguments that pair "
            "with its resource; the key would be ambiguous"
        )
    return found[0]


def _key_for(
    resource: Resource,
    companion: Any | None,
    context: Mapping[str, str] | None,
) -> str:
    """Derive the key naming what this call will produce.

    Parameters
    ----------
    resource : Resource
        Declared input found among the arguments.
    companion : Any or None
        Second identifying value found among the arguments, if any.
    context : Mapping of str to str or None
        Extra inputs folded into the key.

    Returns
    -------
    str
        Cache key.

    Raises
    ------
    CachedCallError
        If the arguments cannot produce a well-defined key.
    """
    already_split = bool(getattr(resource, "partitioned", False))
    try:
        if companion is None or already_split:
            return resource_key(resource, companion=companion, context=context)
        return aggregate_key(resource, companion, context=context)
    except CacheKeyError as error:
        raise CachedCallError(str(error)) from error


def _write_through(
    cache: ArtifactCache,
    produced: Path,
    key: str,
    run_id: str,
    companion: Any | None,
) -> Location:
    """Copy a freshly produced result into the user tier.

    Parameters
    ----------
    cache : ArtifactCache
        Cache to write into.
    produced : Path
        What the wrapped function returned.
    key : str
        Cache key naming the artifact.
    run_id : str
        Run identifier.
    companion : Any or None
        Present when the result is expected to be a set.

    Returns
    -------
    Location
        Committed user-tier artifact.

    Raises
    ------
    CachedCallError
        If what was produced does not match the shape the key promised. The
        key already encodes whether this is a set, so a mismatch would publish
        a directory under a name every reader treats as a file.
    """
    if not produced.exists():
        raise CachedCallError(f"produced path does not exist: {produced}")

    wants_set = companion is not None
    if wants_set and not produced.is_dir():
        raise CachedCallError(
            f"a companion value was supplied, so {key!r} names a set, but a "
            f"single file was produced: {produced}"
        )
    if not wants_set and produced.is_dir():
        raise CachedCallError(
            f"{key!r} names a single file, but a directory was produced: "
            f"{produced}. Accept a registered companion value to cache a set."
        )

    if wants_set:
        return cache.ingest_aggregate(produced, key, run_id)
    return cache.ingest(produced, key, run_id)


def _single_of_type(
    arguments: Mapping[str, Any], wanted: type, function: Callable[..., Any]
) -> Any:
    """Return the sole bound argument of a given type.

    Parameters
    ----------
    arguments : Mapping of str to Any
        Bound arguments of the call.
    wanted : type
        Type to look for.
    function : Callable
        Wrapped function, named in errors.

    Returns
    -------
    Any
        The single matching argument.

    Raises
    ------
    CachedCallError
        If there is not exactly one. Two resources would leave the key
        ambiguous, and guessing which one identifies the output is how a cache
        starts returning the wrong artifact.
    """
    found = [value for value in arguments.values() if isinstance(value, wanted)]
    if len(found) == 1:
        return found[0]
    detail = "none" if not found else f"{len(found)}"
    raise CachedCallError(
        f"{function.__qualname__} must take exactly one {wanted.__name__} "
        f"argument to be cached; found {detail}"
    )


def _run_id(
    arguments: Mapping[str, Any], name: str, function: Callable[..., Any]
) -> str:
    """Return the run identifier from the bound arguments.

    Parameters
    ----------
    arguments : Mapping of str to Any
        Bound arguments of the call.
    name : str
        Parameter name carrying the run identifier.
    function : Callable
        Wrapped function, named in errors.

    Returns
    -------
    str
        Run identifier.

    Raises
    ------
    CachedCallError
        If the parameter is absent or not a string. The user tier is addressed
        by run, so there is nowhere to put the result without it.
    """
    value = arguments.get(name)
    if not isinstance(value, str) or not value:
        raise CachedCallError(
            f"{function.__qualname__} must take a non-empty string {name!r} "
            "argument to be cached; the user tier is addressed by run"
        )
    return value


P = ParamSpec("P")
T = TypeVar("T")


def locate_entity_by_name(
    entity_type: type[T], key_source: str, bound_args: inspect.BoundArguments
) -> T | None:
    # use key source to find the entity in arguments to func or instance members
    arguments = bound_args.arguments
    self = cast("T | None", arguments.get("self", None))
    entity: T | None = None

    if self and hasattr(self, key_source):
        # func is a method on a class and that class contains the key source
        entity = getattr(self, key_source)
    elif key_source in arguments:
        # use bound_args to search both args and kwargs
        candidate = arguments[key_source]

        if not isinstance(candidate, entity_type):
            msg = f"The specified source key is invalid. {entity_type.__name__!r} expected, {type(candidate).__name__!r} received"
            raise TypeError(msg)
        entity = candidate
    else:
        msg = f"The specified key source is invalid. {key_source!r} not found."
        raise CachedCallError(msg)

    return entity


def locate_entity_by_type(
    entity_type: type[T], bound_args: inspect.BoundArguments
) -> T | None:
    arguments = bound_args.arguments
    self = cast("T | None", arguments.get("self", None))
    entity: T | None = None

    if self:
        # look for an instance member matching the target type.
        if candidate := next(
            (
                v
                for _, v in inspect.getmembers(
                    self, predicate=lambda x: isinstance(x, entity_type)
                )
            ),
            None,
        ):
            entity = cast("T", candidate)
    elif candidate := next(
        (x for x in arguments.values() if isinstance(x, entity_type)),
        None,
    ):
        # found entity of correct type in function arguments
        entity = candidate
    else:
        msg = (
            f"Unable to locate entity type {entity_type.__name__!r} for key generation"
        )
        raise CachedCallError(msg)

    return entity


def to_cached_artifact(
    cache_factory: Callable[[], ArtifactCache],
    entity_type: type[T],
    *,
    key_source: str = "",
    value_source: str = "",
    cache_key_function: Callable[[T, Mapping[str, str] | None], str] | None = None,
    on_hit: Literal["copy", "link"] = "copy",
    on_miss: Literal["copy", "write_through"] = "copy",
    context: Mapping[str, str] | None = None,
) -> Callable[[Callable[P, Path]], Callable[P, Path]]:
    """Decorator factory applied to functions that take in the entity to be saved
    and returns the path to the persisted output.

    Parameters
    ----------
    cache_factory : Callable[[], ArtifactCache]
        Factory function that returns the cache

    entity_type : type[T]
        The object type; used by the system to locate the key-generation function
        or serialization payload when `key_source` is not provided.

    key_source : str
        The name of a method parameter or instance member that identifies the
        serialization payload.

    cache_key_function : Callable[[T], str]
        Function to be used by the caching system to generate keys. Must be passed when an
        identity function is not pre-registered with `@identify_for(T)`

    on_hit: Literal["copy", "link"] (default: copy)
        Used by the system to determine how the cached item should be included in the
        working directory.
        - Use "copy" if the data may be modified.
        - Use "link" when the client use of the data is read-only to avoid writing
          an extra copy of the data.

    on_miss: Literal["copy", "write_through"] (default: copy)
        Used by the system to determine how the cached item should be included placed
        into the cache and what value will be returned.
        - "copy" creates a copy of the original data and returns the original target path.
        - "write_through" moves the serialized entity into the cache and replaces the original
          output path with the location in the cache. It can be used to pre-populate the cache
          and to avoid creating an extra copy of the data.

    Returns
    -------
    Callable[[Callable[P, Path]], Callable[P, Path]]
    """

    def _deco(func: Callable[P, Path]) -> Callable[P, Path]:
        """Decorator for handling the automatic storage or retrieval from a cache for a
        serialized entity with a key function registered for the entity type.
        """
        sig = inspect.signature(func)

        @functools.wraps(func)
        def _inner(*args: P.args, **kwargs: P.kwargs) -> Path:
            """Perform the cache lookup and return the result when found. Otherwise,
            execute the wrapped function and place the result into the cache.
            """
            run_id = os.getenv(ENV_CSTAR_RUNID, "")
            entity: T | None = None

            if not is_flag_enabled(ENV_CSTAR_ARTIFACT_CACHE_ENABLED):
                msg = f"Caching flag {ENV_CSTAR_ARTIFACT_CACHE_ENABLED!r} is disabled."
                log.debug(msg)
                return func(*args, **kwargs)

            if not run_id:
                msg = "Caching requires an active run-id. Cache disabled."
                log.warning(msg)
                return func(*args, **kwargs)

            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            if value_source:
                target_path = locate_entity_by_name(Path, value_source, bound_args)
            else:
                target_path = locate_entity_by_type(Path, bound_args)

            if not target_path:
                msg = f"Unable to locate target_path for {func.__name__!r} for value caching"
                raise CachedCallError(msg)

            cache = cache_factory()

            if key_source:
                entity = locate_entity_by_name(entity_type, key_source, bound_args)
            else:
                entity = locate_entity_by_type(entity_type, bound_args)

            if not entity:
                msg = f"Unable to locate entity of type {entity_type.__name__!r} for cache key generation"
                raise CachedCallError(msg)

            if cache_key_function:
                key = cache_key_function(entity, context)
            else:
                key = generator_for(entity_type).key_for(
                    entity,
                    target_path,
                    context=context,
                )

            if location := cache.resolve(key, run_id, record_use=True):
                cache_path = location.path.resolve()
                if on_hit == "copy":
                    shutil.copy2(src=cache_path, dst=target_path)
                    log.debug(
                        f"{key!r} copied from cache location {str(location.path)!r} into {(target_path)!r}"
                    )
                elif on_hit == "link":
                    target_path.symlink_to(cache_path)
                    log.debug(
                        f"{key!r} symlinked from cache location {str(location.path)!r} into {(target_path)!r}"
                    )
                return target_path
            else:
                log.debug(f"Cache miss on {key!r}")

            result = func(*args, **kwargs)

            location = cache.ingest(target_path, key, run_id)
            if on_miss == "write_through":
                result.unlink()
                result = location.path
                log.debug("Cache source removed; returning substituted cache path")

            return result

        return _inner

    return _deco


def to_cached_fileset(
    cache_factory: Callable[[], ArtifactCache],
    entity_type: type[T],
    *,
    key_source: str = "",
    member_source: str = "",
    root_source: str = "",
) -> Callable[
    [
        Callable[P, Sequence[Path]],
    ],
    Callable[P, Sequence[Path]],
]:
    """Decorator factory applied to functions that take in a list of files that must
    be saved as an atomic unit and returns the same collection.

    Applicable to any callable that has the following parameters:
    - p: Path - the target path (where to persist)
    - s: Sequence[Path] - the collection of file paths
    """

    def _deco(
        func: Callable[P, Sequence[Path]],
    ) -> Callable[P, Sequence[Path]]:
        """Decorator handling automatic storage and retrieval of a FileSet from the cache."""
        sig = inspect.signature(func)

        @functools.wraps(func)
        def _inner(*args: P.args, **kwargs: P.kwargs) -> Sequence[Path]:
            if not is_flag_enabled(ENV_CSTAR_ARTIFACT_CACHE_ENABLED):
                msg = f"Caching flag {ENV_CSTAR_ARTIFACT_CACHE_ENABLED!r} is disabled."
                log.debug(msg)
                return func(*args, **kwargs)

            run_id = os.getenv(ENV_CSTAR_RUNID, "")

            if not run_id:
                msg = "Caching requires an active run-id. Cache disabled."
                log.warning(msg)
                return func(*args, **kwargs)

            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            if key_source:
                entity = locate_entity_by_name(entity_type, key_source, bound_args)
                log.info(f"entity by name: {key_source=}, {entity=}")
            else:
                entity = locate_entity_by_type(entity_type, bound_args)
                log.info(f"entity by type: {entity=}")

            if not entity:
                msg = "Unable to determine entity for FileSet caching"
                raise CachedCallError(msg)

            if member_source:
                member_paths = locate_entity_by_name(
                    list[Path], member_source, bound_args
                )
                log.info(f"member paths by name: {member_source=}, {member_paths=}")
            else:
                member_paths = cast(
                    "list[Path]", locate_entity_by_type(list, bound_args)
                )
                log.info(f"member paths by type: {member_paths=}")

            if not member_paths:
                msg = "Unable to identify members for FileSet caching"
                raise CachedCallError(msg)

            root_path: Path | None = None
            if root_source:
                root_path = locate_entity_by_name(Path, root_source, bound_args)
                log.info(f"root path by name: {root_source=}, {root_path=}")
            else:
                root_path = cast("Path", locate_entity_by_type(Path, bound_args))
                log.info(f"root path by type: {member_paths=}")

            if not root_path:
                msg = "Unable to determine root path for FileSet caching"
                raise CachedCallError(msg)

            members = tuple(str(p) for p in member_paths)

            if root_path.is_file():
                log.debug(
                    f"Using parent directory of {str(root_path)!r} as FileSet.root"
                )
                stem = root_path.stem
                members = tuple(f"{stem}{member}" for member in members)
                root_path = root_path.parent

            cache = cache_factory()
            fileset = FileSet(root=root_path, members=members)
            datasource_key = generator_for(type(entity)).key_for(entity, root_path)
            key = fileset_key(fileset, name=datasource_key)

            if found := cache.materialize(
                key, run_id, prefer_local=True, record_use=True
            ):
                location = found
                manifest = SetManifest.model_validate_json(
                    (location.path / SET_MANIFEST_NAME).read_text()
                )
                return [location.path / member.path for member in manifest.members]

            file_paths = func(*args, **kwargs)
            filenames = tuple(p.name for p in file_paths)

            if set(filenames).union(members) != set(filenames):
                msg = (
                    f"Unable to cache FileSet. Members of {datasource_key} did not match the "
                    f"results of {func.__name__}. Expected: {filenames}, Actual: {members}"
                )
                log.warning(msg)
                return file_paths

            location = cache_fileset(
                cache,
                fileset,
                run_id,
            )

            return file_paths

        return _inner

    return _deco
