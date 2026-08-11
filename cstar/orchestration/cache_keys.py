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

Two strategies, differing in what identifies the input:

:class:`HashKeyGenerator`
    Keys on the declared ``hash``. The hash identifies content exactly, so the
    key survives the file moving between mirrors and changes when the upstream
    data changes. Preferred whenever a hash is declared.
:class:`LocationKeyGenerator`
    Keys on the declared ``location``, for resources with no hash. Weaker: a
    URL can serve different bytes over time and the key cannot notice, so a
    stale artifact may be reused after the upstream file changes.

Both fold in the partition geometry, taken from the
:class:`~cstar.applications.roms_marbl.models.PartitioningParameterSet` rather
than from the ``partitioned`` flag — by its declared ``hash`` where it has one,
otherwise by its parameters. The flag records only *that* a resource is
split, not *how*, so keying on it would give two runs that split one resource
across different process grids the same key for different data. A resource
declared ``partitioned`` therefore cannot be keyed without its parameter set,
and asking for one raises rather than silently producing a colliding key.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, TypeVar
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from cstar.applications.roms_marbl.models import PartitioningParameterSet
    from cstar.orchestration.models import DataResource

__all__ = [
    "AGGREGATE_SUFFIX",
    "DIGEST_LENGTH",
    "KEY_SCHEME_VERSION",
    "CacheKeyError",
    "CacheKeyGenerator",
    "DerivedKeyGenerator",
    "DynamicCacheKeyGenerator",
    "ExpandAggregateKeyGenerator",
    "HashKeyGenerator",
    "LocationKeyGenerator",
    "generator_for",
    "readable_parts",
]

TDatum = TypeVar("TDatum")
"""Any value a caller wants to key an artifact on."""

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
    """Raised when a resource does not carry the fields a generator needs."""


class CacheKeyGenerator(ABC):
    """Strategy deriving a cache key from a resource declaration.

    Implementations are stateless and safe to share across threads. Subclass
    this to key on different fields; the cache depends only on the returned
    string.

    Attributes
    ----------
    scheme : str
        Short tag naming the derivation, folded into the digest so two schemes
        can never produce the same key for the same inputs.
    """

    scheme: ClassVar[str]

    @abstractmethod
    def identity(self, resource: DataResource) -> dict[str, Any]:
        """Return the fields that identify this resource's content.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint.

        Returns
        -------
        dict of str to Any
            Mapping folded into the digest. Must contain everything that
            distinguishes one result from another, and nothing that varies
            between equivalent runs.

        Raises
        ------
        CacheKeyError
            If the resource lacks a field this strategy requires.
        """

    def key_for(
        self,
        resource: DataResource,
        *,
        partitioning: PartitioningParameterSet | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> str:
        """Derive the cache key naming this resource's artifact.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint.
        partitioning : PartitioningParameterSet or None, optional
            Geometry the resource is split across. Required when the resource
            declares ``partitioned``; ignored otherwise, so a caller looping
            over a blueprint may pass it for every resource. Its declared
            ``hash`` identifies it when present, falling back to the
            parameters themselves — see
            :meth:`CacheKeyGenerator._partition_identity`.
        context : Mapping of str to Any or None, optional
            Further inputs that affect the result but are not fields of the
            resource, such as a solver version or code revision. Anything
            omitted here that changes the output will cause two genuinely
            different artifacts to share a key.

        Both optional arguments are keyword-only: a mapping passed positionally
        would otherwise bind to ``partitioning`` and be silently discarded for
        an unpartitioned resource, producing a key that quietly omits inputs.

        Returns
        -------
        str
            A filesystem-safe key of the form ``<stem>-<digest><suffix>``,
            carrying the source filename for readability and its extension so
            downstream tools still see a recognisable file.

            The filename is folded into the digest as well as prefixed, so the
            key stays a pure function of its inputs. The consequence is that
            the same content declared under two different filenames yields two
            keys, and therefore two cached copies.

        Raises
        ------
        CacheKeyError
            If the resource lacks a field this strategy requires, or declares
            ``partitioned`` without a partitioning parameter set.

        Examples
        --------
        >>> HashKeyGenerator().key_for(resource)
        'partitioning1-3f7a1c9e2b5d8046.nc'
        """
        stem, suffix = self._readable_parts(resource)
        payload = {
            "scheme": self.scheme,
            "version": KEY_SCHEME_VERSION,
            "identity": self.identity(resource),
            "partitioning": self._partition_identity(resource, partitioning),
            "context": dict(context) if context else {},
            "filename": f"{stem}{suffix}",
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()[:DIGEST_LENGTH]
        return f"{stem}-{digest}{suffix}"

    def _readable_parts(self, resource: DataResource) -> tuple[str, str]:
        """Return a safe filename stem and suffix taken from the location.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint.

        Returns
        -------
        tuple of (str, str)
            Sanitised stem, and suffix including its leading dot when present.
        """
        return readable_parts(str(getattr(resource, "location", "") or ""))

    def _partition_identity(
        self,
        resource: DataResource,
        partitioning: PartitioningParameterSet | None,
    ) -> dict[str, Any] | None:
        """Return the partition geometry contributing to a key.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint.
        partitioning : PartitioningParameterSet or None
            Geometry the resource is split across.

        Returns
        -------
        dict of str to Any or None
            The parameter set's ``hash`` when it declares one, since that hash
            identifies the whole set including any dynamically added
            parameters. Otherwise the substantive parameters themselves.
            ``None`` for an unsplit resource; a supplied parameter set is then
            ignored, since geometry cannot affect data that was never split.

        Raises
        ------
        CacheKeyError
            If the resource declares ``partitioned`` but no parameter set was
            supplied. Keying on the flag alone would give two different process
            grids the same key for different data.

        Warnings
        --------
        A declared ``hash`` is trusted, not verified. Nothing here recomputes
        it, so a hash left stale after an edit to the parameters will key two
        different geometries alike — the one way this function can produce a
        false cache hit. Blueprints that do not maintain the hash should leave
        it unset, which falls back to the parameters themselves.
        """
        if not bool(getattr(resource, "partitioned", False)):
            return None
        if partitioning is None:
            raise CacheKeyError(
                f"{type(resource).__name__} is declared partitioned, so its "
                "PartitioningParameterSet is required: the geometry determines "
                "the content and cannot be inferred from the flag"
            )
        declared = getattr(partitioning, "hash", None)
        if declared:
            return {"hash": str(declared)}
        return {
            field: value
            for field, value in partitioning.model_dump(mode="json").items()
            if field not in _PARAMETER_METADATA
        }

    def __repr__(self) -> str:
        """Return a debugging representation naming the strategy.

        Returns
        -------
        str
            Representation of this generator.
        """
        return f"{type(self).__name__}()"


class HashKeyGenerator(CacheKeyGenerator):
    """Strategy keying on a resource's declared hash and partition geometry.

    The hash identifies the upstream bytes, so the key changes when the
    upstream data changes and is unaffected by which host or path serves it.
    The location is excluded from identity for that reason: the same content
    behind two mirrors should share one cached artifact.

    The filename is the exception. It is folded into the key so the cache
    directory stays readable, which means the same content published under two
    different names caches twice. That is a deliberate trade of a little
    duplication for legible listings; key on the digest alone if storage
    matters more.
    """

    scheme: ClassVar[str] = "hash"

    def identity(self, resource: DataResource) -> dict[str, Any]:
        """Return the hash and partition flag identifying this resource.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint. Must declare a hash.

        Returns
        -------
        dict of str to Any
            Mapping of ``hash``. Partition geometry is contributed separately
            by :meth:`CacheKeyGenerator.key_for`.

        Raises
        ------
        CacheKeyError
            If the resource declares no hash, which means this strategy cannot
            identify its content.
        """
        digest = getattr(resource, "hash", None)
        if not digest:
            raise CacheKeyError(
                f"{type(resource).__name__} declares no hash; use "
                "LocationKeyGenerator or add a hash to the blueprint"
            )
        return {"hash": str(digest)}


class LocationKeyGenerator(CacheKeyGenerator):
    """Strategy keying on a resource's declared location and partition geometry.

    For resources with no hash. The location is normalised so trivially
    different spellings of one URL agree: scheme and host are lowercased,
    default ports are dropped, and fragments are discarded. Query strings are
    kept, since they often select which content is served.

    Warning
    -------
    A location is not an identity. The same URL can serve different bytes over
    time, and this key cannot detect that, so a cached artifact may be reused
    after its upstream source changes. For local filesystem paths the key is
    also machine-specific, which makes it unsuitable for the shared tier.
    Declare a hash wherever the data matters.
    """

    scheme: ClassVar[str] = "location"

    def identity(self, resource: DataResource) -> dict[str, Any]:
        """Return the normalised location and partition flag.

        Parameters
        ----------
        resource : DataResource
            Resource declaration from a blueprint.

        Returns
        -------
        dict of str to Any
            Mapping of ``location``. Partition geometry is contributed
            separately by :meth:`CacheKeyGenerator.key_for`.

        Raises
        ------
        CacheKeyError
            If the resource declares no location.
        """
        location = getattr(resource, "location", None)
        if not location:
            raise CacheKeyError(f"{type(resource).__name__} declares no location")
        return {"location": self._normalise(str(location))}

    @staticmethod
    def _normalise(location: str) -> str:
        """Canonicalise a location so equivalent spellings agree.

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


class DerivedKeyGenerator(CacheKeyGenerator, ABC):
    """Base for keys naming an artifact derived from another artifact.

    A derivation is keyed by *what it was derived from* plus *how*, so the
    identity of the source is delegated to whichever ordinary strategy suits
    the source resource, and this class contributes only the derivation. The
    consequence that matters is that a derived artifact occupies a separate
    key space: :attr:`~CacheKeyGenerator.scheme` is folded into every digest,
    so a partition of a file and the file itself can never collide under one
    shared name — which they otherwise would, one an archive and one a file.

    Parameters
    ----------
    source : CacheKeyGenerator or None, optional
        Strategy identifying the source resource. Defaults to
        :func:`generator_for`, chosen per resource at call time, so a resource
        that declares a hash is keyed on it and one that does not falls back.

    Attributes
    ----------
    scheme : str
        Short tag naming the derivation, folded into the digest.
    """

    def __init__(self, source: CacheKeyGenerator | None = None) -> None:
        self._source = source

    def identity(self, resource: DataResource) -> dict[str, Any]:
        """Return the source resource's identity, unchanged.

        Parameters
        ----------
        resource : DataResource
            Resource the artifact is derived from.

        Returns
        -------
        dict of str to Any
            Whatever the delegate strategy considers identifying.

        Raises
        ------
        CacheKeyError
            If the delegate cannot identify the resource.
        """
        delegate = self._source if self._source is not None else generator_for(resource)
        return delegate.identity(resource)

    def __repr__(self) -> str:
        """Return a debugging representation naming the strategy and delegate.

        Returns
        -------
        str
            Representation of this generator.
        """
        inner = "" if self._source is None else repr(self._source)
        return f"{type(self).__name__}({inner})"


class ExpandAggregateKeyGenerator(DerivedKeyGenerator):
    """Strategy keying the set of files produced by splitting one resource.

    The ranks of a partition are one artifact rather than many: they are only
    useful together, so the cache stores them as a single container and this
    names it. Two properties distinguish it from an ordinary resource key.

    The scheme separates the key space, so the partition and the file it came
    from cannot land on one shared name. The geometry is *mandatory* and always
    part of the identity, which is the difference from
    :meth:`CacheKeyGenerator._partition_identity`: there, a parameter set
    describes a resource that arrives already split and is ignored for one that
    does not, because geometry cannot affect data that was never split. Here
    the geometry is what is being *produced*, so it always determines the
    content and there is no case in which omitting it is correct.

    Examples
    --------
    >>> ExpandAggregateKeyGenerator().key_for(resource, partitioning=geometry)
    'bgc-boundary-2010-1d0e4a2f7c93b856.set'
    """

    scheme: ClassVar[str] = "aggregate-expand"

    def _readable_parts(self, resource: DataResource) -> tuple[str, str]:
        """Return the source stem with the aggregate suffix in place of its own.

        Parameters
        ----------
        resource : DataResource
            Resource the set is derived from.

        Returns
        -------
        tuple of (str, str)
            Sanitised stem from the source location, and
            :data:`AGGREGATE_SUFFIX`.
        """
        stem, _ = super()._readable_parts(resource)
        return (stem, AGGREGATE_SUFFIX)

    def _partition_identity(
        self,
        resource: DataResource,
        partitioning: PartitioningParameterSet | None,
    ) -> dict[str, Any] | None:
        """Return the geometry the set is produced across.

        Parameters
        ----------
        resource : DataResource
            Resource the set is derived from.
        partitioning : PartitioningParameterSet or None
            Geometry to split across.

        Returns
        -------
        dict of str to Any
            The parameter set's ``hash`` when it declares one, otherwise its
            substantive parameters. Never ``None``: a set that is not
            identified by its geometry is not identified.

        Raises
        ------
        CacheKeyError
            If no parameter set was supplied, or if the source resource is
            itself declared ``partitioned``. Repartitioning from one grid to
            another is identified by *both* geometries, and this signature
            carries only one, so it is refused rather than keyed on half the
            inputs.
        """
        if bool(getattr(resource, "partitioned", False)):
            raise CacheKeyError(
                f"{type(resource).__name__} is already partitioned; "
                "repartitioning is identified by the source and target "
                "geometries together, which this key cannot express"
            )
        if partitioning is None:
            raise CacheKeyError(
                "ExpandAggregateKeyGenerator requires a "
                "PartitioningParameterSet: the geometry determines the "
                "contents of the set being produced"
            )
        declared = getattr(partitioning, "hash", None)
        if declared:
            return {"hash": str(declared)}
        return {
            field: value
            for field, value in partitioning.model_dump(mode="json").items()
            if field not in _PARAMETER_METADATA
        }


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


def generator_for(resource: DataResource) -> CacheKeyGenerator:
    """Return the strongest strategy a resource supports.

    Parameters
    ----------
    resource : DataResource
        Resource declaration from a blueprint.

    Returns
    -------
    CacheKeyGenerator
        :class:`HashKeyGenerator` when the resource declares a hash, otherwise
        :class:`LocationKeyGenerator`.

    Notes
    -----
    Selection is by declared fields rather than by class, so a
    :class:`~cstar.orchestration.models.VersionedResource` whose hash is unset
    falls back rather than raising.
    """
    if getattr(resource, "hash", None):
        return HashKeyGenerator()
    return LocationKeyGenerator()
