"""Decorator applying the artifact cache to a function that produces a file.

The check / retrieve / cache flow is mechanical: derive a key from the declared
inputs, ask the cache, return the hit, and on a miss run the work and write the
result through. :func:`cached` performs it around an ordinary function, so the
function itself stays a plain producer with no knowledge of the cache.

What makes that possible without annotations is that the inputs which determine
a key are already distinguishable by type. A
:class:`~cstar.orchestration.models.Resource` declares *what* data is wanted and
a :class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet`
declares *how it is laid out*; between them they are the key. The decorator
binds the call, finds them among the arguments, and derives the key before the
function runs.

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

from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration.artifact_cache import (
    ArtifactCache,
    Location,
    OnConflict,
    Tier,
)
from cstar.orchestration.cache_keys import (
    CacheKeyError,
    ExpandAggregateKeyGenerator,
    generator_for,
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
    context: Mapping[str, Any] | None = None,
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
    context : Mapping of str to Any or None, optional
        Extra inputs folded into the key — a code revision, a solver version.
        Anything that changes the output and is omitted here will make two
        different artifacts share a key.
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
    Artifact shape follows the arguments. A
    :class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet`
    among them means the result is a set of files in a directory; without one
    it is a single file. Which key generator derives the name depends on
    whether the resource declares itself already partitioned: if it does, the
    geometry describes the source and the ordinary strategy handles it; if it
    does not, the geometry is being *produced* and
    :class:`~cstar.orchestration.cache_keys.ExpandAggregateKeyGenerator` names
    the derived set in its own key space.
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
            geometry = _optional_of_type(
                bound.arguments, PartitioningParameterSet, function
            )
            run_id = _run_id(bound.arguments, run_id_argument, function)
            key = _key_for(resource, geometry, context)

            found = (
                active.materialize(key, run_id, record_use=True)
                if geometry is not None
                else active.resolve(key, run_id, record_use=True)
            )
            if found is not None:
                return _localized(active, found, key, run_id, localize).path

            produced = Path(function(*args, **kwargs))
            location = _write_through(active, produced, key, run_id, geometry)
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


def _key_for(
    resource: Resource,
    geometry: PartitioningParameterSet | None,
    context: Mapping[str, Any] | None,
) -> str:
    """Derive the key naming what this call will produce.

    Parameters
    ----------
    resource : Resource
        Declared input found among the arguments.
    geometry : PartitioningParameterSet or None
        Partition geometry found among the arguments, if any.
    context : Mapping of str to Any or None
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
    generator = (
        generator_for(resource)
        if geometry is None or already_split
        else ExpandAggregateKeyGenerator()
    )
    try:
        return generator.key_for(resource, partitioning=geometry, context=context)
    except CacheKeyError as error:
        raise CachedCallError(str(error)) from error


def _write_through(
    cache: ArtifactCache,
    produced: Path,
    key: str,
    run_id: str,
    geometry: PartitioningParameterSet | None,
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
    geometry : PartitioningParameterSet or None
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

    wants_set = geometry is not None
    if wants_set and not produced.is_dir():
        raise CachedCallError(
            f"a PartitioningParameterSet was supplied, so {key!r} names a set, "
            f"but a single file was produced: {produced}"
        )
    if not wants_set and produced.is_dir():
        raise CachedCallError(
            f"{key!r} names a single file, but a directory was produced: "
            f"{produced}. Accept a PartitioningParameterSet to cache a set."
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


def _optional_of_type(
    arguments: Mapping[str, Any], wanted: type, function: Callable[..., Any]
) -> Any | None:
    """Return the sole bound argument of a given type, or ``None``.

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
    Any or None
        The single matching argument, or ``None`` when there is none.

    Raises
    ------
    CachedCallError
        If more than one is present.
    """
    found = [value for value in arguments.values() if isinstance(value, wanted)]
    if not found:
        return None
    if len(found) > 1:
        raise CachedCallError(
            f"{function.__qualname__} takes {len(found)} {wanted.__name__} "
            "arguments; the key would be ambiguous"
        )
    return found[0]


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
