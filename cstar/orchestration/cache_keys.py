"""Input-addressed cache keys derived from blueprint resources.

A cache key names an artifact in :class:`~cstar.orchestration.artifact_cache.
ArtifactCache`. Because the shared tier is addressed by name alone, the name
must determine the content: two runs that would produce interchangeable results
must generate the same key, and two runs that would not must generate
different ones.

Keys here are *input-addressed* — computed from the declaration of an input,
before that input is fetched or processed — rather than content-addressed. That
is what makes them usable as a lookup: a run can ask "has anyone already
produced this?" without first doing the work.

One mechanism, :class:`DynamicCacheKeyGenerator`, does the assembly: a scheme
naming the derivation, a scheme version, a readable stem, and a truncated
digest over a sorted payload. What identifies a particular subject is supplied
as a function, so nothing about any one type is baked into the key machinery.

Two identity functions cover blueprint resources:

:func:`hash_identity`
    Keys on the declared ``hash``. The hash identifies content exactly, so the
    key survives the file moving between mirrors and changes when the upstream
    data changes. Preferred whenever a hash is declared.
:func:`location_identity`
    Keys on the declared ``location``, for resources with no hash. Weaker: a
    URL can serve different bytes over time and the key cannot notice, so a
    stale artifact may be reused after the upstream file changes.

Either composes with :func:`partition_identity` via :func:`with_partitioning`
when a subject is a resource *and* the geometry it is split across. The
geometry is taken from the
:class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet` rather
than from the ``partitioned`` flag — by its declared ``hash`` where it has one,
otherwise by its parameters. The flag records only *that* a resource is split,
not *how*, so keying on it would give two runs that split one resource across
different process grids the same key for different data. A resource declared
``partitioned`` therefore cannot be keyed without its parameter set, and asking
for one raises rather than silently producing a colliding key.

:func:`generator_for` resolves a subject shape to the generator that keys it,
so a caller names the types it holds rather than choosing a strategy.
:func:`register_identity` adds shapes the orchestration layer does not know
about, which is how a type outside this package gets keyed without this module
learning it exists.
"""

from __future__ import annotations

import hashlib
import json
import re
import typing as t
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar
from urllib.parse import urlsplit, urlunsplit

from cstar.applications.roms_marbl.models import PartitioningParameterSet
from cstar.orchestration.models import Resource, VersionedResource

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping
    from pathlib import Path

    from cstar.orchestration.models import DataResource

__all__ = [
    "AGGREGATE_SUFFIX",
    "DIGEST_LENGTH",
    "KEY_SCHEME_VERSION",
    "CacheKeyError",
    "DynamicCacheKeyGenerator",
    "IdentityFunction",
    "Subject",
    "aggregate_key",
    "generator_for",
    "hash_identity",
    "location_identity",
    "normalise_location",
    "partition_identity",
    "readable_parts",
    "register_identity",
    "resource_key",
    "subject_for",
    "with_partitioning",
]

TDatum = TypeVar("TDatum")
"""Any value a caller wants to key an artifact on."""

Subject: t.TypeAlias = type | tuple[type, ...]
"""The shape of what is being keyed: one type, or several taken together."""

IdentityFunction: t.TypeAlias = "Callable[[Any], Mapping[str, str]]"
"""Returns the fields that identify a subject's content."""

KEY_SCHEME_VERSION: Final[int] = 3
"""Version of the key derivation, folded into every digest.

Bumping this invalidates every key at once, which is the intended way to change
what a key means without silently reusing artifacts computed under the old
rules.
"""

DIGEST_LENGTH: Final[int] = 16
"""Hex characters of SHA-256 retained in a key (64 bits)."""

_MAX_STEM: Final[int] = 40
"""Characters of the human-readable prefix retained in a key."""

_UNSAFE: Final[re.Pattern[str]] = re.compile(r"[^A-Za-z0-9._-]+")
"""Characters replaced when building the human-readable prefix."""

_DEFAULT_PORTS: Final[dict[str, str]] = {"http": "80", "https": "443", "ftp": "21"}
"""Ports dropped during URL normalisation."""

AGGREGATE_SUFFIX: Final[str] = ".set"
"""Suffix carried by a key naming a set rather than a single file.

An aggregate is not a NetCDF file, so inheriting the source's ``.nc`` would
mislead every tool that sniffs by extension. The shared archive and the
expanded directory carry this same suffix, since a key names one artifact
regardless of which tier is holding it.
"""

_PARAMETER_METADATA: Final[frozenset[str]] = frozenset({"documentation", "locked"})
"""Parameter-set fields excluded from a key.

These describe governance rather than the data a parameter set produces:
``documentation`` is a provenance URL and ``locked`` is a mutability flag.
Folding either in would split the cache on edits that cannot change a single
byte of output.

``hash`` is excluded from this set because it is not metadata but an identity;
see :meth:`CacheKeyGenerator._partition_identity`.
"""


def readable_parts(location: str) -> tuple[str, str]:
    """Return a filesystem-safe stem and suffix taken from a location.

    The digest alone would be a correct key; the stem exists so a human
    listing a cache directory can tell what they are looking at. Accepts a URL
    or a filesystem path — the path component is taken in either case.

    Parameters
    ----------
    location : str
        URL or path the artifact derives its readable name from.

    Returns
    -------
    tuple of (str, str)
        Sanitised stem, and suffix including its leading dot when present.
        The stem falls back to ``"artifact"`` when nothing usable remains.

    Examples
    --------
    >>> readable_parts("https://example.org/data/boundary-2010.nc")
    ('boundary-2010', '.nc')
    """
    path = PurePosixPath(urlsplit(location).path or location)
    stem = _UNSAFE.sub("-", path.stem).strip("-.")[:_MAX_STEM]
    suffix = _UNSAFE.sub("", path.suffix)[:16]
    return (stem or "artifact", suffix)


class CacheKeyError(Exception):
    """Raised when a subject cannot be keyed as asked."""


class DynamicCacheKeyGenerator(Generic[TDatum]):
    """Strategy deriving a cache key from any value plus an injected identity.

    :class:`CacheKeyGenerator` is bound to
    :class:`~cstar.orchestration.models.DataResource`: it reads ``location``,
    ``hash`` and ``partitioned`` off the value, and its partition handling is
    specific to one application's geometry. That is right where a blueprint
    resource is what is being keyed, and useless everywhere else.

    This keeps the *shape* of that design — a scheme naming the derivation, a
    scheme version, a readable stem, a truncated digest over a sorted payload —
    and moves the one part that cannot generalise behind a function the caller
    supplies. Anything that distinguishes one result from another, partition
    geometry included, goes in whatever ``identity_fn`` returns.

    Parameters
    ----------
    scheme : str
        Short tag naming the derivation, folded into every digest. Required
        rather than defaulted, because it *is* the key space: two identity
        functions over one type that shared a scheme would silently collide,
        and no default can know they differ. Deriving it from the function's
        name was rejected — renaming a function would invalidate every key it
        ever produced, with no error to say so.
    identity_fn : Callable
        Returns the fields identifying a value's content. Values are strings
        so that normalisation — rounding, case, ordering — is decided by the
        caller who understands the type, rather than by :func:`json.dumps`
        guessing at it.

    Attributes
    ----------
    scheme : str
        Short tag naming the derivation.
    identity_fn : Callable
        Injected identity function.

    Examples
    --------
    >>> def grid_identity(grid: Grid) -> dict[str, str]:
    ...     return {"nx": str(grid.nx), "ny": str(grid.ny), "crs": grid.crs}
    >>> generator = DynamicCacheKeyGenerator("grid", grid_identity)
    >>> generator.key_for(grid, "/data/domain.nc")
    'domain-4f8b1c02de75a396.nc'

    See Also
    --------
    CacheKeyGenerator : Blueprint-resource strategies, kept for that use.
    """

    def __init__(
        self,
        scheme: str,
        identity_fn: Callable[[TDatum], Mapping[str, str]],
    ) -> None:
        if not scheme or _UNSAFE.search(scheme):
            raise CacheKeyError(
                f"scheme must be a non-empty filesystem-safe tag, got {scheme!r}"
            )
        self.scheme = scheme
        self.identity_fn = identity_fn

    def identity(self, value: TDatum) -> dict[str, str]:
        """Return the fields identifying this value's content.

        Parameters
        ----------
        value : TDatum
            Value being keyed.

        Returns
        -------
        dict of str to str
            Whatever ``identity_fn`` returned, validated.

        Raises
        ------
        CacheKeyError
            If the mapping is empty, or holds a non-string key or value. An
            empty mapping would leave the key a function of the filename
            alone, so two unrelated values sharing a name would share an
            artifact — a silent wrong answer rather than a miss.
        """
        produced = self.identity_fn(value)
        if not produced:
            raise CacheKeyError(
                f"identity function for scheme {self.scheme!r} returned nothing; "
                "a key with no identity is the filename alone, which two "
                "unrelated values can share"
            )
        for field, entry in produced.items():
            if not isinstance(field, str) or not isinstance(entry, str):
                raise CacheKeyError(
                    f"identity function for scheme {self.scheme!r} must return "
                    f"str to str; got {type(field).__name__} to "
                    f"{type(entry).__name__}"
                )
        return dict(produced)

    def key_for(
        self,
        value: TDatum,
        path: Path | str,
        *,
        context: Mapping[str, Any] | None = None,
        suffix: str | None = None,
    ) -> str:
        """Derive the cache key naming this value's artifact.

        Parameters
        ----------
        value : TDatum
            Value being keyed.
        path : Path or str
            Location the readable stem and extension are taken from. Only its
            filename participates; the directory it sits in does not, so the
            same artifact keyed from two workspaces agrees.
        context : Mapping of str to Any or None, optional
            Further inputs that affect the result but are not part of the
            value — a code revision, a solver version. Anything omitted here
            that changes the output will make two genuinely different
            artifacts share a key.
        suffix : str or None, optional
            Extension to use in place of the one on ``path``. Pass
            :data:`AGGREGATE_SUFFIX` where the artifact is a set, so the key
            does not claim to be a file of the source's type.

        Returns
        -------
        str
            A key of the form ``<stem>-<digest><suffix>``. The filename is
            folded into the digest as well as prefixed, so the key stays a
            pure function of its inputs; the consequence is that one value
            keyed under two filenames caches twice.

        Raises
        ------
        CacheKeyError
            If ``identity_fn`` returns an unusable mapping.
        """
        stem, native = readable_parts(str(path))
        extension = native if suffix is None else suffix
        payload = {
            "scheme": self.scheme,
            "version": KEY_SCHEME_VERSION,
            "identity": self.identity(value),
            "context": dict(context) if context else {},
            "filename": f"{stem}{extension}",
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()[:DIGEST_LENGTH]
        return f"{stem}-{digest}{extension}"

    def __repr__(self) -> str:
        """Return a debugging representation naming the scheme.

        Returns
        -------
        str
            Representation of this generator.
        """
        return f"{type(self).__name__}(scheme={self.scheme!r})"


# ---------------------------------------------------------------------------
# Identity functions for blueprint resources
# ---------------------------------------------------------------------------


def normalise_location(location: str) -> str:
    """Canonicalise a location so equivalent spellings agree.

    Scheme and host are lowercased, default ports are dropped, and fragments
    are discarded. Query strings are kept, since they often select which
    content is served.

    Parameters
    ----------
    location : str
        Raw location from the blueprint.

    Returns
    -------
    str
        Normalised location. Non-URL values, such as filesystem paths, are
        returned with surrounding whitespace stripped and no other change.
    """
    parts = urlsplit(location.strip())
    if not parts.scheme or not parts.netloc:
        return location.strip()

    host = parts.hostname or ""
    port = parts.port
    if port is not None and str(port) != _DEFAULT_PORTS.get(parts.scheme.lower()):
        host = f"{host}:{port}"
    if parts.username:
        credentials = parts.username
        if parts.password:
            credentials = f"{credentials}:{parts.password}"
        host = f"{credentials}@{host}"

    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), host, path, parts.query, ""))


def location_identity(resource: DataResource) -> dict[str, str]:
    """Identify a resource by where it is served from.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.

    Returns
    -------
    dict of str to str
        Mapping of the normalised location, under ``resource.location``. The
        namespace is the subject rather than the strategy: it says the field
        describes a resource, while the scheme already records that the
        resource was identified by location rather than by hash.

    Raises
    ------
    CacheKeyError
        If the resource declares no location.

    Warnings
    --------
    A location is not an identity. The same URL can serve different bytes over
    time and this cannot detect it, so a cached artifact may be reused after
    its upstream source changes. For local filesystem paths the key is also
    machine-specific, which makes it unsuitable for the shared tier. Declare a
    hash wherever the data matters.
    """
    location = getattr(resource, "location", None)
    if not location:
        raise CacheKeyError(f"{type(resource).__name__} declares no location")
    return {"resource.location": normalise_location(str(location))}


def hash_identity(resource: DataResource) -> dict[str, str]:
    """Identify a resource by its declared content hash.

    The location is excluded deliberately: the same content behind two mirrors
    should share one cached artifact.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint. Must declare a hash.

    Returns
    -------
    dict of str to str
        Mapping of the declared digest, under ``resource.hash``. Qualified
        because a bare ``hash`` is a name several identity functions could
        plausibly want — :func:`partition_identity` is one — and a flat merge
        would let one silently overwrite another.

    Raises
    ------
    CacheKeyError
        If the resource declares no hash, which means this cannot identify its
        content.
    """
    digest = getattr(resource, "hash", None)
    if not digest:
        raise CacheKeyError(
            f"{type(resource).__name__} declares no hash; key on the location "
            "instead, or add a hash to the blueprint"
        )
    return {"resource.hash": str(digest)}


def partition_identity(partitioning: PartitioningParameterSet) -> dict[str, str]:
    """Identify the geometry a resource is split across.

    Parameters
    ----------
    partitioning : PartitioningParameterSet
        Geometry the resource is split across.

    Returns
    -------
    dict of str to str
        The parameter set's ``hash`` when it declares one, since that hash
        identifies the whole set including any dynamically added parameters.
        Otherwise the substantive parameters themselves. Fields are namespaced
        under ``partition.``: a parameter set's ``hash`` is not a resource's
        ``hash``, and merging the two flat would let the geometry silently
        overwrite the content digest — two different artifacts under one key.
        Qualifying here rather than at the merge site means every composer is
        safe, not just the one shipped below.

    Warnings
    --------
    A declared ``hash`` is trusted, not verified. Nothing here recomputes it,
    so a hash left stale after an edit to the parameters will key two different
    geometries alike — the one way this function can produce a false cache hit.
    Blueprints that do not maintain the hash should leave it unset, which falls
    back to the parameters themselves.
    """
    declared = getattr(partitioning, "hash", None)
    if declared:
        return {"partition.hash": str(declared)}
    return {
        f"partition.{field}": str(value)
        for field, value in partitioning.model_dump(mode="json").items()
        if field not in _PARAMETER_METADATA
    }


def with_partitioning(base: IdentityFunction) -> IdentityFunction:
    """Compose a resource identity with the geometry it is split across.

    Parameters
    ----------
    base : IdentityFunction
        Identity function for the resource alone.

    Returns
    -------
    IdentityFunction
        Identity function over a ``(resource, partitioning)`` pair.

    Notes
    -----
    A plain merge is safe because each identity function qualifies its own
    field names; see :func:`partition_identity`.
    """

    def identity(
        subject: tuple[DataResource, PartitioningParameterSet],
    ) -> dict[str, str]:
        """Return the resource's identity with the geometry folded in.

        Parameters
        ----------
        subject : tuple
            Resource and the geometry it is split across.

        Returns
        -------
        dict of str to str
            Identifying fields.
        """
        resource, partitioning = subject
        return {**base(resource), **partition_identity(partitioning)}

    return identity


# ---------------------------------------------------------------------------
# Subject registry
# ---------------------------------------------------------------------------

_REGISTRY: Final[dict[tuple[type, ...], tuple[str, IdentityFunction]]] = {}
"""Subject shape to the scheme and identity function that key it."""


def register_identity(
    subject: Subject, scheme: str, identity_fn: IdentityFunction
) -> None:
    """Register how a subject shape is identified.

    This is how a type outside this package becomes keyable without this module
    learning it exists: the layer that owns the type registers it.

    Parameters
    ----------
    subject : type or tuple of type
        Shape being registered. A tuple registers several values taken
        together, which is what lets a key be composed from more than one
        thing — a resource and the geometry it is split across, say.
    scheme : str
        Short tag naming the derivation, folded into every digest.
    identity_fn : IdentityFunction
        Returns the fields identifying a value of this shape. For a tuple
        subject it receives the values as a tuple, in the registered order.
        Field names should be unique to what the function identifies — a
        namespace prefix where the bare name is one another function could
        plausibly use — so that composing two identities cannot silently drop
        a field. :func:`partition_identity` is the worked example.

    Raises
    ------
    CacheKeyError
        If the shape is already registered. Silently replacing it would change
        the meaning of every key derived through it, with nothing to say so.
    """
    shape = _as_shape(subject)
    if shape in _REGISTRY:
        existing, _ = _REGISTRY[shape]
        raise CacheKeyError(
            f"{_describe(shape)} is already registered under scheme "
            f"{existing!r}; re-registering would change what every key derived "
            "through it means"
        )
    _REGISTRY[shape] = (scheme, identity_fn)


def generator_for(subject: Subject) -> DynamicCacheKeyGenerator[Any]:
    """Return the generator that keys a subject shape.

    Parameters
    ----------
    subject : type or tuple of type
        Shape being keyed. Pass a tuple where the key is composed from several
        values, in the order the identity function expects them.

    Returns
    -------
    DynamicCacheKeyGenerator
        Generator bound to the registered scheme and identity function.

    Raises
    ------
    CacheKeyError
        If nothing is registered for the shape, including for any base class of
        it.

    Notes
    -----
    Resolution prefers an exact match and otherwise walks each type's method
    resolution order, so a subclass is keyed like its base unless it registers
    something of its own. That is what lets
    :class:`~cstar.orchestration.models.VersionedResource` be keyed on its hash
    while every other :class:`~cstar.orchestration.models.Resource` falls back
    to its location.

    Examples
    --------
    >>> generator_for(VersionedResource).key_for(resource, resource.location)
    'boundary-2010-3f7a1c9e2b5d8046.nc'
    """
    scheme, identity_fn = _REGISTRY[_resolve(_as_shape(subject))]
    return DynamicCacheKeyGenerator(scheme, identity_fn)


def _as_shape(subject: Subject) -> tuple[type, ...]:
    """Normalise a subject to a tuple of types.

    Parameters
    ----------
    subject : type or tuple of type
        Shape being keyed.

    Returns
    -------
    tuple of type
        The shape as a tuple.

    Raises
    ------
    CacheKeyError
        If the subject is not a type or a tuple of types. A value passed where
        a type belongs would otherwise register under something no lookup can
        reproduce.
    """
    shape = subject if isinstance(subject, tuple) else (subject,)
    if not shape or not all(isinstance(entry, type) for entry in shape):
        raise CacheKeyError(
            f"subject must be a type or a tuple of types, got {subject!r}"
        )
    return shape


def _resolutions(shape: tuple[type, ...]) -> Generator[tuple[type, ...], None, None]:
    """Yield candidate shapes, most specific first.

    Parameters
    ----------
    shape : tuple of type
        Shape being resolved.

    Yields
    ------
    tuple of type
        Candidate registry keys, walking each position's method resolution
        order left to right.
    """
    yield shape
    for index, entry in enumerate(shape):
        for base in entry.__mro__[1:]:
            yield shape[:index] + (base,) + shape[index + 1 :]


def _describe(shape: tuple[type, ...]) -> str:
    """Name a subject shape for an error message.

    Parameters
    ----------
    shape : tuple of type
        Shape being described.

    Returns
    -------
    str
        Readable description.
    """
    names = ", ".join(entry.__name__ for entry in shape)
    return names if len(shape) == 1 else f"({names})"


register_identity(VersionedResource, "hash", hash_identity)
register_identity(Resource, "location", location_identity)
register_identity(
    (VersionedResource, PartitioningParameterSet),
    "hash",
    with_partitioning(hash_identity),
)
register_identity(
    (Resource, PartitioningParameterSet),
    "location",
    with_partitioning(location_identity),
)


def subject_for(
    resource: DataResource, partitioning: PartitioningParameterSet | None = None
) -> tuple[Subject, Any]:
    """Return the shape and value that key a resource.

    A convenience for the common case, so callers do not repeat the pairing
    rule at every site.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.
    partitioning : PartitioningParameterSet or None, optional
        Geometry the resource is split across. Required when the resource
        declares ``partitioned``; ignored otherwise, so a caller looping over a
        blueprint may pass it for every resource.

    Returns
    -------
    tuple of (Subject, Any)
        Shape to look up, and the value to key.

    Raises
    ------
    CacheKeyError
        If the resource declares ``partitioned`` but no parameter set was
        supplied. Keying on the flag alone would give two different process
        grids the same key for different data.
    """
    if not bool(getattr(resource, "partitioned", False)):
        return (type(resource), resource)
    if partitioning is None:
        raise CacheKeyError(
            f"{type(resource).__name__} is declared partitioned, so its "
            "PartitioningParameterSet is required: the geometry determines "
            "the content and cannot be inferred from the flag"
        )
    return ((type(resource), type(partitioning)), (resource, partitioning))


def resource_key(
    resource: DataResource,
    *,
    partitioning: PartitioningParameterSet | None = None,
    context: Mapping[str, Any] | None = None,
    suffix: str | None = None,
) -> str:
    """Derive the cache key naming a blueprint resource's artifact.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.
    partitioning : PartitioningParameterSet or None, optional
        See :func:`subject_for`.
    context : Mapping of str to Any or None, optional
        Further inputs that affect the result but are not fields of the
        resource, such as a solver version or code revision.
    suffix : str or None, optional
        Extension to use in place of the resource's own. Pass
        :data:`AGGREGATE_SUFFIX` where the artifact is a set.

    Returns
    -------
    str
        Cache key.

    Raises
    ------
    CacheKeyError
        If the resource cannot be keyed as declared.

    Examples
    --------
    >>> resource_key(resource)
    'boundary-2010-3f7a1c9e2b5d8046.nc'
    """
    shape, value = subject_for(resource, partitioning)
    return generator_for(shape).key_for(
        value, str(resource.location), context=context, suffix=suffix
    )


def aggregate_key(
    resource: DataResource,
    partitioning: PartitioningParameterSet,
    *,
    context: Mapping[str, Any] | None = None,
) -> str:
    """Derive the cache key naming the set produced by splitting a resource.

    Two things separate this from :func:`resource_key`. The scheme puts the
    derived set in its own key space, so a partition and the file it came from
    cannot land on one shared name — they are different shapes on disk, so a
    collision corrupts rather than wastes. And the geometry is mandatory and
    always identifying, because here it is what is being *produced*; there is
    no case in which omitting it is correct.

    Parameters
    ----------
    resource : DataResource
        Resource the set is derived from.
    partitioning : PartitioningParameterSet
        Geometry to split across.
    context : Mapping of str to Any or None, optional
        Further inputs that affect the result.

    Returns
    -------
    str
        Cache key carrying :data:`AGGREGATE_SUFFIX`.

    Raises
    ------
    CacheKeyError
        If the resource is itself declared ``partitioned``. Repartitioning from
        one grid to another is identified by both geometries together, which
        this cannot express, so it is refused rather than keyed on half its
        inputs.
    """
    if bool(getattr(resource, "partitioned", False)):
        raise CacheKeyError(
            f"{type(resource).__name__} is already partitioned; repartitioning "
            "is identified by the source and target geometries together, which "
            "this key cannot express"
        )
    shape = (type(resource), type(partitioning))
    scheme, identity_fn = _REGISTRY[_resolve(shape)]
    generator: DynamicCacheKeyGenerator[Any] = DynamicCacheKeyGenerator(
        f"aggregate-expand-{scheme}", identity_fn
    )
    return generator.key_for(
        (resource, partitioning),
        str(resource.location),
        context=context,
        suffix=AGGREGATE_SUFFIX,
    )


def _resolve(shape: tuple[type, ...]) -> tuple[type, ...]:
    """Return the registered shape a subject resolves to.

    Parameters
    ----------
    shape : tuple of type
        Shape being resolved.

    Returns
    -------
    tuple of type
        The registry key that matched.

    Raises
    ------
    CacheKeyError
        If nothing is registered for the shape.
    """
    for candidate in _resolutions(shape):
        if candidate in _REGISTRY:
            return candidate
    raise CacheKeyError(
        f"nothing registered to key {_describe(shape)}; call "
        "register_identity to say how it is identified"
    )
