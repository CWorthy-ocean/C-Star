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
... def fetch_boundary(resource: VersionedResource, run_id: str) -> Path:
...     path = workspace / "boundary.nc"
...     download(resource.location, path)
...     return path

The function is unchanged by the decoration and still callable directly in a
test. What it returns changes, though: on a hit the caller receives the *cached*
path rather than the path the function would have written to, which is the
point — the work is skipped.

Notes
-----
This is deliberately small. It covers a producer taking one resource and
returning one path, which is the shape most preprocessing steps have. A step
consuming several resources, or one whose output is not determined by its
declared inputs alone, should call the cache directly rather than be forced
through a decorator that cannot express it.
"""

from __future__ import annotations

import functools
import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from cstar.orchestration.artifact_cache import (
    ArtifactCache,
    Location,
    OnConflict,
    Tier,
)
from cstar.orchestration.cache_keys import (
    CacheKeyError,
    aggregate_key,
    is_registered,
    resource_key,
)
from cstar.orchestration.models import Resource

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = ["CachedCallError", "cached"]

F = TypeVar("F", bound="Callable[..., Path]")
"""A producer: any callable returning the path it wrote its result to."""


class CachedCallError(Exception):
    """Raised when a decorated call cannot be cached as declared."""


def cached(
    *,
    cache: ArtifactCache | None = None,
    cache_factory: Callable[[], ArtifactCache] | None = None,
    run_id_argument: str = "run_id",
    context: Mapping[str, str] | None = None,
    promote: bool = False,
    on_conflict: OnConflict = OnConflict.SKIP,
    localize: bool = True,
) -> Callable[[F], F]:
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
    localize : bool, optional
        Whether a shared hit is copied into the run's own workspace before its
        path is returned. On by default: the caller is handed a path and
        nothing stops it writing there, and a client that edits an artifact in
        place would corrupt it for every other run on the allocation. Turn it
        off only where the consumer is known to be read-only and the copy is
        worth avoiding. Set artifacts are copied regardless, since expansion
        already targets the user tier.

    Returns
    -------
    Callable
        Decorator preserving the wrapped function's signature.

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

    def decorate(function: F) -> F:
        signature = inspect.signature(function)

        @functools.wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Path:
            active = cache if cache is not None else cache_factory()  # type: ignore[misc]
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()

            resource = _single_of_type(bound.arguments, Resource, function)
            companion = _companion_for(resource, bound.arguments, function)
            run_id = _run_id(bound.arguments, run_id_argument, function)
            key = _key_for(resource, companion, context)

            found = (
                active.materialize(key, run_id, record_use=True)
                if companion is not None
                else active.resolve(key, run_id, record_use=True)
            )
            if found is not None:
                return _localized(active, found, key, run_id, localize).path

            produced = Path(function(*args, **kwargs))
            location = _write_through(active, produced, key, run_id, companion)
            if promote:
                active.promote(key, run_id, on_conflict=on_conflict)
            return location.path

        return wrapper  # type: ignore[return-value]

    return decorate


def _localized(
    cache: ArtifactCache,
    found: Location,
    key: str,
    run_id: str,
    localize: bool,
) -> Location:
    """Return a hit, copied into the run's workspace when asked.

    A shared artifact is shared: the caller receives a path, and nothing in
    the type system stops it opening that path for writing. One client that
    edits in place corrupts the artifact for every other run on the
    allocation, and the damage is silent until a digest is checked. Copying is
    minutes against the days the artifact cost to produce.

    Parameters
    ----------
    cache : ArtifactCache
        Cache holding the artifact.
    found : Location
        The hit, in either tier.
    key : str
        Cache key naming the artifact.
    run_id : str
        Run identifier.
    localize : bool
        Whether to copy a shared file into the user tier.

    Returns
    -------
    Location
        Where the caller should read from.
    """
    if not localize or found.tier is not Tier.SHARED or found.is_container:
        return found
    return cache.ingest(found.path, key, run_id)


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
