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

Either composes with a *companion* — a second value that also determines the
result, such as the geometry a resource is split across. Companion pairings are
registered by whoever owns the companion type;
:mod:`cstar.applications.roms_marbl.cache` registers the ROMS/MARBL partition
geometry, which is why nothing here imports it. A resource declaring
``partitioned`` cannot be keyed without its companion, and asking for one raises
rather than silently producing a colliding key: the flag records only *that* a
resource is split, not *how*, so keying on it alone would give two runs that
split one resource across different process grids the same key for different
data.

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
    "checked_fields",
    "generator_for",
    "hash_identity",
    "identity_for",
    "is_registered",
    "location_identity",
    "normalise_location",
    "readable_parts",
    "register_identity",
    "resource_key",
    "subject_for",
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


def checked_fields(fields: Mapping[str, str], role: str) -> dict[str, str]:
    """Validate that a mapping is safe to fold into a digest.

    Every value reaching a key must already be a string. The digest is taken
    over ``json.dumps(..., default=str)``, and ``default=str`` renders an
    arbitrary object by its ``repr`` — which for a set depends on
    ``PYTHONHASHSEED`` and therefore differs between processes. A key that
    changes per process never hits, and nothing reports it: an unreachable
    cache is indistinguishable from a cold one. Requiring strings puts
    normalisation with whoever understands the values.

    Parameters
    ----------
    fields : Mapping of str to str
        Candidate fields.
    role : str
        What produced them, named in the error.

    Returns
    -------
    dict of str to str
        The fields, as a plain dict.

    Raises
    ------
    CacheKeyError
        If any key or value is not a string.
    """
    for field, value in fields.items():
        if not isinstance(field, str) or not isinstance(value, str):
            raise CacheKeyError(
                f"{role} must supply str to str; got "
                f"{type(field).__name__} to {type(value).__name__}. Format the "
                "value yourself so its spelling is a decision rather than an "
                "accident of repr()"
            )
    return dict(fields)


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
        return checked_fields(produced, f"identity function for scheme {self.scheme!r}")

    def key_for(
        self,
        value: TDatum,
        path: Path | str,
        *,
        context: Mapping[str, str] | None = None,
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
        context : Mapping of str to str or None, optional
            Further inputs that affect the result but are not part of the
            value — a code revision, a solver version. This is the escape
            hatch for inputs that have no natural type to register: the
            registry declares what a *type* means, once, while context carries
            what varies at the call site. Anything omitted here that changes
            the output will make two genuinely different artifacts share a
            key. Values are strings for the reason given in
            :func:`checked_fields`.
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
            "context": checked_fields(context, "context") if context else {},
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


def identity_for(
    subject: Subject, scheme: str, *, base: IdentityFunction | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register an identity function at its definition.

    The declarative form of :func:`register_identity`. Registration sits on the
    function it registers, so a reader sees what a type is keyed on without
    hunting for a call somewhere below.

    The decorated function is returned **unchanged**, which is what makes it
    still directly callable and testable, and what lets several applications of
    this decorator stack on one factory.

    Parameters
    ----------
    subject : type or tuple of type
        Shape being registered.
    scheme : str
        Short tag naming the derivation, folded into every digest.
    base : IdentityFunction or None, optional
        Present when the decorated function is a *factory* rather than an
        identity function — it is called as ``factory(base)`` and the result is
        registered. This is how a composed identity, such as a resource taken
        together with the geometry it is split across, is registered without
        naming the composition twice.

    Returns
    -------
    Callable
        Decorator returning its argument unchanged.

    Raises
    ------
    CacheKeyError
        If the shape is already registered, or if a factory does not return a
        callable.

    Examples
    --------
    A plain identity function::

        @identity_for(Foo, "foo")
        def foo_identity(foo: Foo) -> dict[str, str]:
            return {"foo.name": foo.name}

    A factory, registered once per base it composes with::

        @identity_for(
            (VersionedResource, Geometry), "hash", base=hash_identity
        )
        @identity_for(
            (Resource, Geometry), "location", base=location_identity
        )
        def with_geometry(base: IdentityFunction) -> IdentityFunction:
            ...
    """

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        """Register the function and hand it back untouched.

        Parameters
        ----------
        function : Callable
            Identity function, or a factory when ``base`` is given.

        Returns
        -------
        Callable
            ``function``, unchanged.
        """
        produced = function if base is None else function(base)
        if not callable(produced):
            raise CacheKeyError(
                f"{getattr(function, '__qualname__', function)!r} was given a "
                "base, so it is treated as a factory, but calling it returned "
                f"{type(produced).__name__} rather than an identity function"
            )
        register_identity(subject, scheme, produced)
        return function

    return decorate


def is_registered(subject: Subject) -> bool:
    """Report whether a subject shape can be keyed.

    Lets a caller discover which of the values it is holding pair with each
    other, without naming any particular type. That is what keeps this package
    free of the application types it keys — the pairing is declared by whoever
    owns the companion, and found here by asking.

    Parameters
    ----------
    subject : type or tuple of type
        Shape to check.

    Returns
    -------
    bool
        Whether the shape resolves, including through a base class.
    """
    try:
        _resolve(_as_shape(subject))
    except CacheKeyError:
        return False
    return True


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


def subject_for(
    resource: DataResource, companion: Any | None = None
) -> tuple[Subject, Any]:
    """Return the shape and value that key a resource.

    A convenience for the common case, so callers do not repeat the pairing
    rule at every site.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.
    companion : Any or None, optional
        Second value that also determines the result — the geometry a resource
        is split across, say. Required when the resource declares
        ``partitioned``; ignored otherwise, so a caller looping over a
        blueprint may pass it for every resource. Its type must have a
        registered pairing; see :func:`register_identity`.

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
    if companion is None:
        raise CacheKeyError(
            f"{type(resource).__name__} is declared partitioned, so the value "
            "describing how it is split is required: the geometry determines "
            "the content and cannot be inferred from the flag"
        )
    return ((type(resource), type(companion)), (resource, companion))


def resource_key(
    resource: DataResource,
    *,
    companion: Any | None = None,
    context: Mapping[str, str] | None = None,
    suffix: str | None = None,
) -> str:
    """Derive the cache key naming a blueprint resource's artifact.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.
    companion : Any or None, optional
        See :func:`subject_for`.
    context : Mapping of str to str or None, optional
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
    shape, value = subject_for(resource, companion)
    return generator_for(shape).key_for(
        value, str(resource.location), context=context, suffix=suffix
    )


def aggregate_key(
    resource: DataResource,
    companion: Any,
    *,
    context: Mapping[str, str] | None = None,
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
    companion : Any
        Value describing how the set is produced — the geometry to split
        across. Its pairing with the resource must be registered.
    context : Mapping of str to str or None, optional
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
    shape = (type(resource), type(companion))
    scheme, identity_fn = _REGISTRY[_resolve(shape)]
    generator: DynamicCacheKeyGenerator[Any] = DynamicCacheKeyGenerator(
        f"aggregate-expand-{scheme}", identity_fn
    )
    return generator.key_for(
        (resource, companion),
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
