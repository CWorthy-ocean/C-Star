"""Two-layer artifact cache with per-tier addressing.

This module provides :class:`ArtifactCache`, a storage component that manages
expensive derived data files (for example NetCDF products) across two tiers
that are addressed differently because they answer different questions:

``USER``
    A workspace. Laid out as ``<root>/<run_id>/<name>``, because the question
    a user asks of their own scratch is "what did my run produce?". Fast,
    subject to automatic purge policies, and freely deletable by its owner.

``SHARED``
    A library. Laid out as ``<root>/<name>``, because the question a consumer
    asks of shared storage is "does this artifact exist?" — answerable without
    knowing which run promoted it. Populated by *promotion*, which copies
    rather than moves, so the user tier copy remains valid.

Consequences of the asymmetry
-----------------------------
``run_id`` identifies a location only in the user tier. In
:meth:`ArtifactCache.promote` it names the *source*, never the destination, so
a later run can find promoted data by name alone.

Because the shared path no longer records provenance, the shared record
carries it explicitly: :attr:`ArtifactRecord.promoted_from_run_id`,
:attr:`ArtifactRecord.promoted_by`, and :attr:`ArtifactRecord.promoted_at`.

Because ``name`` is the sole identity of shared content, names must determine
content. Re-promoting a name that already exists compares fingerprints: byte-
identical content is an idempotent no-op, and genuine divergence raises.

Design notes
------------
The component performs **no caching of its own lookups**. Every resolution
stats the filesystem, so a file deleted by a user or reclaimed by a scratch
purge degrades to a cache miss rather than a stale hit.

The module has no workflow-engine dependencies. Orchestration concerns belong
in thin wrappers around this class, which keeps the storage logic unit-testable
without standing up a server.

All writes are atomic: content is staged to a temporary sibling and committed
with :func:`os.replace`, so readers never observe a partially written file.

Shared records live in per-artifact sidecars under a ``.manifests`` directory
rather than one tier-wide file, so concurrent promotions by different users do
not contend on a single lock — which matters on network filesystems where
advisory locking is expensive and less reliable.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from cstar.base.env import ENV_CSTAR_ARTIFACT_CACHE_BYPASS
from cstar.base.feature import is_flag_enabled
from cstar.orchestration.fingerprinting import (
    ChecksumMode,
    Fingerprinter,
    NullFingerprinter,
    fingerprinter_for,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence

__all__ = [
    "ArtifactCache",
    "ArtifactCacheError",
    "ArtifactExistsError",
    "ArtifactNotFoundError",
    "ArtifactRecord",
    "CacheModel",
    "ChecksumMode",
    "Fingerprinter",
    "Location",
    "Manifest",
    "Reference",
    "SharedRecord",
    "Tier",
    "UnsafePathError",
    "UsageReport",
]

MANIFEST_NAME: Final[str] = "manifest.json"
"""Filename of the sidecar manifest written into every user-tier run directory."""

SHARED_RECORD_DIR: Final[str] = ".manifests"
"""Directory under the shared root holding one JSON sidecar per artifact."""

MANIFEST_VERSION: Final[int] = 2
"""Schema version stamped into each manifest, for future migrations."""

MAX_REFERENCES: Final[int] = 64
"""Most recent reference entries retained per shared artifact.

The log is capped so a widely used artifact's sidecar stays small; the entries
dropped are the least recently used, and their existence survives in
:attr:`SharedRecord.reference_total`.
"""

DEFAULT_USE_INTERVAL_SECONDS: Final[float] = 3600.0
"""Minimum gap between recorded touches of one artifact by one run."""

_LOCK_SUFFIX: Final[str] = ".lock"
_TMP_SUFFIX: Final[str] = ".tmp"
_RECORD_SUFFIX: Final[str] = ".json"


class ArtifactCacheError(Exception):
    """Base class for all errors raised by :mod:`artifact_cache`."""


class ArtifactNotFoundError(ArtifactCacheError):
    """Raised when a requested artifact exists in neither tier."""


class ArtifactExistsError(ArtifactCacheError):
    """Raised when a write would overwrite an artifact and ``overwrite`` is False."""


class UnsafePathError(ArtifactCacheError):
    """Raised when a resolved path escapes the roots managed by the cache."""


class Tier(StrEnum):
    """Storage tier within the two-layer cache.

    Attributes
    ----------
    USER : str
        Per-user transient storage, addressed by ``(run_id, name)``.
    SHARED : str
        Durable shared storage, addressed by ``name`` alone.
    """

    USER = "user"
    SHARED = "shared"


class CacheModel(BaseModel):
    """Base model configuring shared validation behaviour for cache models.

    Notes
    -----
    Models are frozen because locations and records are value objects
    describing committed state; updates use
    :meth:`~pydantic.BaseModel.model_copy`.

    Unknown fields are ignored rather than forbidden, so a sidecar written by a
    newer version of this module still loads.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        frozen=True,
        extra="ignore",
        validate_assignment=True,
    )
    """Configures the behavior of the pydantic model."""


class Location(CacheModel):
    """Resolved filesystem position of an artifact.

    Parameters
    ----------
    path : Path
        Absolute path to the artifact on disk. Not guaranteed to exist.
    tier : Tier
        Tier this location belongs to.
    name : str
        Artifact filename, unique within its run directory (user tier) or
        within the whole tier (shared tier).
    run_id : str or None
        Run that produced the artifact. Always ``None`` for shared locations,
        whose paths deliberately carry no run identity.
    """

    path: Path
    tier: Tier
    name: str
    run_id: str | None = None

    @property
    def uri(self) -> str:
        """str: ``file://`` URI for this location.

        Suitable for use as a workflow-engine asset key. Derived from
        :attr:`path`, so the asset identifier cannot drift from the bytes
        actually written.
        """
        return self.path.as_uri()

    @property
    def exists(self) -> bool:
        """bool: Whether a regular file is present at :attr:`path` right now.

        Notes
        -----
        Performs a live ``stat`` on every access and is never memoized.
        """
        return self.path.is_file()


class ArtifactRecord(CacheModel):
    """Record describing a single committed artifact.

    Parameters
    ----------
    name : str
        Artifact filename.
    size_bytes : int
        Size of the file at the time it was committed.
    created_at : str
        UTC ISO-8601 timestamp of commit.
    created_by : str
        Operating-system username of the writer.
    checksum : str or None, optional
        Hex-encoded digest, or ``None`` when fingerprinting was skipped. Only
        meaningful alongside :attr:`checksum_mode`.
    checksum_mode : ChecksumMode or None, optional
        Strategy that produced :attr:`checksum`. A record carrying a checksum
        but no mode predates quick signatures and is read as
        :attr:`ChecksumMode.FULL`.
    source : str or None, optional
        Free-form provenance string, such as the input dataset path.
    asset_uri : str or None, optional
        Workflow-engine asset key emitted for this artifact.
    run_id : str or None, optional
        Run that produced the artifact, for user-tier records.
    promoted_from_run_id : str or None, optional
        Run whose copy was promoted, for shared-tier records. The shared path
        carries no run identity, so this is the only trace back to the
        producer.
    promoted_by : str or None, optional
        Username that performed the promotion.
    promoted_at : str or None, optional
        UTC ISO-8601 timestamp of promotion.
    metadata : dict of str to Any, optional
        Caller-supplied descriptive metadata.
    """

    name: str
    size_bytes: int
    created_at: str
    created_by: str
    checksum: str | None = None
    checksum_mode: ChecksumMode | None = None
    source: str | None = None
    asset_uri: str | None = None
    run_id: str | None = None
    promoted_from_run_id: str | None = None
    promoted_by: str | None = None
    promoted_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _infer_legacy_checksum_mode(cls, payload: Any) -> Any:
        """Interpret a mode-less checksum as a full digest.

        Parameters
        ----------
        payload : Any
            Raw input to model validation.

        Returns
        -------
        Any
            The payload, with ``checksum_mode`` filled in where inferable.
        """
        if not isinstance(payload, dict):
            return payload
        if payload.get("checksum") and payload.get("checksum_mode") is None:
            payload = {**payload, "checksum_mode": ChecksumMode.FULL}
        return payload

    @field_validator("metadata", mode="before")
    @classmethod
    def _coerce_null_metadata(cls, value: Any) -> Any:
        """Treat an explicit JSON null as an empty mapping.

        Parameters
        ----------
        value : Any
            Raw value supplied for the ``metadata`` field.

        Returns
        -------
        Any
            An empty dictionary when ``value`` is ``None``, otherwise ``value``.
        """
        return {} if value is None else value

    def to_dict(self) -> dict[str, Any]:
        """Serialise this record to a JSON-compatible mapping.

        Returns
        -------
        dict of str to Any
            Plain dictionary suitable for :func:`json.dump`.
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ArtifactRecord:
        """Reconstruct a record from its serialised form.

        Parameters
        ----------
        payload : Mapping of str to Any
            Mapping previously produced by :meth:`to_dict`.

        Returns
        -------
        ArtifactRecord
            Reconstructed record. Unknown keys are ignored.

        Raises
        ------
        pydantic.ValidationError
            If a required field is missing or a value cannot be coerced.
        """
        return cls.model_validate(payload)


class Reference(CacheModel):
    """A run's recorded use of a shared artifact.

    Notes
    -----
    This is a *lease*, not a reference count. Acquisition is voluntary — a
    consumer that opens :attr:`Location.path` directly is invisible to the
    cache — and release never happens reliably, because runs are killed by
    schedulers and crash without unwinding. A timestamp degrades safely under
    both: a vanished run's entry simply ages out, while a live consumer keeps
    refreshing its own.

    Parameters
    ----------
    run_id : str
        Run that used the artifact.
    used_by : str
        Operating-system username of the consumer.
    last_used_at : str
        UTC ISO-8601 timestamp of the most recent recorded use.
    """

    run_id: str
    used_by: str
    last_used_at: str


class SharedRecord(ArtifactRecord):
    """Record for a shared-tier artifact, extended with a reference log.

    Subclasses :class:`ArtifactRecord` rather than reusing :class:`Manifest`,
    because the shared tier has no directory to collect: it keeps one sidecar
    per artifact. Every inherited field applies, and only the shared-specific
    lifecycle information is added.

    Parameters
    ----------
    references : list of Reference, optional
        Recent consumers, most-recently-used last. Capped at
        :data:`MAX_REFERENCES`.
    first_referenced_at : str or None, optional
        UTC ISO-8601 timestamp of the earliest recorded use.
    reference_total : int, optional
        Count of distinct runs that have ever recorded a use, including any
        dropped from :attr:`references` by the cap.
    """

    references: list[Reference] = Field(default_factory=list)
    first_referenced_at: str | None = None
    reference_total: int = 0

    @field_validator("references", mode="before")
    @classmethod
    def _coerce_null_references(cls, value: Any) -> Any:
        """Treat an explicit JSON null as an empty reference log.

        Parameters
        ----------
        value : Any
            Raw value supplied for the ``references`` field.

        Returns
        -------
        Any
            An empty list when ``value`` is ``None``, otherwise ``value``.
        """
        return [] if value is None else value

    @property
    def last_used_at(self) -> str | None:
        """Most recent recorded use, or the promotion time when never used.

        Falls back to :attr:`ArtifactRecord.promoted_at` so an artifact nobody
        has read still has a meaningful age, rather than looking infinitely
        stale.

        Returns
        -------
        str or None
            UTC ISO-8601 timestamp, or ``None`` when neither exists.
        """
        if self.references:
            return max(ref.last_used_at for ref in self.references)
        return self.promoted_at


class UsageReport(CacheModel):
    """Summary of one shared artifact's size, age, and recent consumers.

    Returned by :meth:`ArtifactCache.gc_candidates`, which reports rather than
    deletes: liveness here is inferred from voluntary registration, and acting
    on it automatically would eventually delete data somebody still needs.

    Parameters
    ----------
    name : str
        Artifact filename.
    size_bytes : int
        Size recorded at commit time.
    last_used_at : str or None
        Most recent recorded use, or promotion time when never used.
    idle_days : float or None
        Days since :attr:`last_used_at`, or ``None`` when no timestamp exists.
    reference_total : int
        Distinct runs that have ever recorded a use.
    recent_runs : list of str
        Run identifiers retained in the reference log, most recent last.
    promoted_from_run_id : str or None
        Run whose copy was promoted.
    """

    name: str
    size_bytes: int
    last_used_at: str | None
    idle_days: float | None
    reference_total: int
    recent_runs: list[str]
    promoted_from_run_id: str | None


class Manifest(CacheModel):
    """Sidecar index describing the contents of one user-tier run directory.

    Only the user tier uses a manifest. Shared artifacts each carry their own
    sidecar, so promoting users never contend on a single tier-wide file.

    Parameters
    ----------
    run_id : str
        Identifier of the run this manifest describes.
    tier : Tier
        Tier the manifest lives in. Always :attr:`Tier.USER`.
    artifacts : dict of str to ArtifactRecord
        Records keyed by artifact name.
    version : int, optional
        Manifest schema version.
    """

    run_id: str
    tier: Tier = Tier.USER
    artifacts: dict[str, ArtifactRecord] = Field(default_factory=dict)
    version: int = MANIFEST_VERSION

    @field_validator("artifacts", mode="before")
    @classmethod
    def _coerce_null_artifacts(cls, value: Any) -> Any:
        """Treat an explicit JSON null as an empty artifact mapping.

        Parameters
        ----------
        value : Any
            Raw value supplied for the ``artifacts`` field.

        Returns
        -------
        Any
            An empty dictionary when ``value`` is ``None``, otherwise ``value``.
        """
        return {} if value is None else value

    def to_dict(self) -> dict[str, Any]:
        """Serialise this manifest to a JSON-compatible mapping.

        Returns
        -------
        dict of str to Any
            Plain dictionary suitable for :func:`json.dump`.
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Manifest:
        """Reconstruct a manifest from its serialised form.

        Parameters
        ----------
        payload : Mapping of str to Any
            Mapping previously produced by :meth:`to_dict`.

        Returns
        -------
        Manifest
            Reconstructed manifest.

        Raises
        ------
        pydantic.ValidationError
            If a required field is missing or a value cannot be coerced.
        """
        return cls.model_validate(payload)


class ArtifactCache:
    """Two-layer artifact cache with atomic writes and per-tier addressing.

    User-tier artifacts live at ``<user_root>/<run_id>/<name>``; shared-tier
    artifacts live at ``<shared_root>/<name>``. Lookups check the shared tier
    first and fall back to the user tier, so promoted data survives deletion of
    a user's local copy and is reachable by name alone.

    Parameters
    ----------
    user_root : Path or str
        Root of the per-user cache, for example ``~/.cache/app``.
    shared_root : Path or str
        Root of the shared cache, for example ``/scratch/app``.
    view_root : Path or str or None, optional
        Root beneath which symlink views are materialised.
    create_roots : bool, optional
        Whether to create the roots on construction. Default ``True``.
    fingerprinter : Fingerprinter or None, optional
        Default strategy for fingerprinting committed artifacts. Defaults to
        :class:`~cstar.orchestration.fingerprinting.NullFingerprinter`.
    bypass : bool or None, optional
        Ignore existing entries, so lookups report a miss and callers recreate
        their artifacts. Defaults to the
        :data:`~cstar.base.env.ENV_CSTAR_ARTIFACT_CACHE_BYPASS` environment
        flag. Reads only: writes still happen and overwrite, so the cache
        repopulates rather than being disabled.

    Attributes
    ----------
    user_root : Path
        Resolved user-tier root.
    shared_root : Path
        Resolved shared-tier root.
    view_root : Path or None
        Resolved view root, if configured.
    fingerprinter : Fingerprinter
        Strategy used when a write does not supply its own.
    bypass : bool
        Whether lookups report a miss regardless of what is on disk.

    Notes
    -----
    Instances hold no mutable lookup state and are safe to share across
    threads. Cross-process consistency of record updates relies on
    :func:`fcntl.flock`, which is reliable on local filesystems and NFSv4 but
    may be unreliable on some older or object-store-backed mounts.

    Examples
    --------
    >>> cache = ArtifactCache("~/.cache/app", "/scratch/app")
    >>> with cache.stage("filtered.nc", run_id="abc-123") as tmp:
    ...     tmp.write_bytes(b"...")
    >>> cache.promote("filtered.nc", run_id="abc-123")
    >>> cache.resolve("filtered.nc").tier          # no run_id needed
    <Tier.SHARED: 'shared'>
    """

    def __init__(
        self,
        user_root: Path | str,
        shared_root: Path | str,
        view_root: Path | str | None = None,
        create_roots: bool = True,
        fingerprinter: Fingerprinter | None = None,
        bypass: bool | None = None,
    ) -> None:
        self.user_root: Path = Path(user_root).expanduser().resolve()
        self.shared_root: Path = Path(shared_root).expanduser().resolve()
        self.view_root: Path | None = (
            Path(view_root).expanduser().resolve() if view_root is not None else None
        )
        self.fingerprinter: Fingerprinter = fingerprinter or NullFingerprinter()
        self.bypass: bool = (
            is_flag_enabled(ENV_CSTAR_ARTIFACT_CACHE_BYPASS)
            if bypass is None
            else bypass
        )
        if self.user_root == self.shared_root:
            raise ValueError("user_root and shared_root must differ")
        if create_roots:
            self.user_root.mkdir(parents=True, exist_ok=True)
            self.shared_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Location
    # ------------------------------------------------------------------

    def root_for(self, tier: Tier) -> Path:
        """Return the filesystem root backing a tier.

        Parameters
        ----------
        tier : Tier
            Tier to look up.

        Returns
        -------
        Path
            Root directory of ``tier``.
        """
        return self.user_root if tier is Tier.USER else self.shared_root

    def locate(self, name: str, tier: Tier, run_id: str | None = None) -> Location:
        """Compute the canonical location of an artifact in one tier.

        The single source of truth for artifact placement: both the filesystem
        path and the asset URI derive from its result, so the two cannot
        disagree.

        Parameters
        ----------
        name : str
            Artifact filename. Must not contain path separators.
        tier : Tier
            Tier to compute the location within.
        run_id : str or None, optional
            Run identifier. Required for :attr:`Tier.USER`. Rejected for
            :attr:`Tier.SHARED`, whose paths carry no run identity.

        Returns
        -------
        Location
            Canonical location. The file may or may not exist.

        Raises
        ------
        ValueError
            If ``run_id`` is missing for the user tier, or supplied for the
            shared tier.
        UnsafePathError
            If ``name`` or ``run_id`` contain separators or traversal
            components, or if the result escapes the tier root.
        """
        self._validate_component(name, "name")
        root = self.root_for(tier)

        if tier is Tier.SHARED:
            if run_id is not None:
                raise ValueError(
                    "shared-tier locations are addressed by name alone; "
                    f"remove run_id={run_id!r}"
                )
            path = root / name
            self._assert_contained(path, root)
            return Location(path=path, tier=tier, name=name, run_id=None)

        if run_id is None:
            raise ValueError("run_id is required for user-tier locations")
        self._validate_component(run_id, "run_id")
        path = root / run_id / name
        self._assert_contained(path, root)
        return Location(path=path, tier=tier, name=name, run_id=run_id)

    def candidates(self, name: str, run_id: str | None = None) -> tuple[Location, ...]:
        """Return the locations consulted by :meth:`resolve`, in order.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            Run whose user-tier copy should be considered. When ``None``, only
            the shared tier is a candidate.

        Returns
        -------
        tuple of Location
            ``(shared,)`` when ``run_id`` is ``None``, otherwise
            ``(shared, user)``.
        """
        shared = self.locate(name, Tier.SHARED)
        if run_id is None:
            return (shared,)
        return (shared, self.locate(name, Tier.USER, run_id))

    def resolve(
        self,
        name: str,
        run_id: str | None = None,
        prefer_local: bool = False,
        record_use: bool = False,
    ) -> Location | None:
        """Find an artifact, checking the shared tier first.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            Run whose user-tier copy may be used as a fallback. Omit it to ask
            only "does the shared tier have this?", which is the question a
            consuming run asks without knowing who promoted it.
        prefer_local : bool, optional
            Reverse the precedence and check the user tier first. Useful when
            iterating locally on a product that also exists in the shared tier.
            Ignored when ``run_id`` is ``None``.
        record_use : bool, optional
            Record this run's use when the artifact resolves to the shared
            tier, via :meth:`record_use`. Putting the touch on the read path is
            what keeps the reference log honest: a consumer that resolves and
            then opens the path directly would otherwise be invisible. Writes
            are debounced, so this costs at most one small write per artifact
            per run per :data:`DEFAULT_USE_INTERVAL_SECONDS`.

        Returns
        -------
        Location or None
            Location of the first tier in which the file is present, or
            ``None`` if absent.

        Notes
        -----
        Existence is tested live on every call and is never memoized.
        """
        if self.bypass:
            return None
        return self._lookup(
            name, run_id, prefer_local=prefer_local, record_use=record_use
        )

    def _lookup(
        self,
        name: str,
        run_id: str | None = None,
        prefer_local: bool = False,
        record_use: bool = False,
    ) -> Location | None:
        """Find an artifact on disk, ignoring :attr:`bypass`.

        Bypass models "pretend nothing is cached so the caller recreates it".
        That belongs to reuse decisions only. Operations on artifacts already
        known to exist — verifying integrity, or linking a view to files this
        run just wrote — must still see the filesystem, or bypass would make
        :meth:`verify` unusable and :meth:`refresh_view` silently empty.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            See :meth:`resolve`.
        prefer_local : bool, optional
            See :meth:`resolve`.
        record_use : bool, optional
            See :meth:`resolve`.

        Returns
        -------
        Location or None
            Location of the first tier in which the file is present.
        """
        ordered = self.candidates(name, run_id)
        if prefer_local and len(ordered) > 1:
            ordered = tuple(reversed(ordered))
        location = next((x for x in ordered if x.exists), None)
        if (
            location is not None
            and record_use
            and location.tier is Tier.SHARED
            and run_id is not None
        ):
            self.record_use(name, run_id)
        return location

    def require(
        self,
        name: str,
        run_id: str | None = None,
        prefer_local: bool = False,
    ) -> Location:
        """Resolve an artifact or raise if it is missing.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            See :meth:`resolve`.
        prefer_local : bool, optional
            See :meth:`resolve`.

        Returns
        -------
        Location
            Location of the existing artifact.

        Raises
        ------
        ArtifactNotFoundError
            If the artifact is absent from every candidate tier.
        """
        location = self.resolve(name, run_id, prefer_local=prefer_local)
        if location is None:
            if self.bypass:
                raise ArtifactNotFoundError(
                    f"artifact {name!r} reported missing because the cache is "
                    f"bypassed ({ENV_CSTAR_ARTIFACT_CACHE_BYPASS}); it may exist "
                    "on disk"
                )
            scope = (
                "shared tier" if run_id is None else f"shared tier or run {run_id!r}"
            )
            raise ArtifactNotFoundError(f"artifact {name!r} not found in {scope}")
        return location

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    @contextmanager
    def stage(
        self,
        name: str,
        run_id: str | None = None,
        tier: Tier = Tier.USER,
        source: str | None = None,
        asset_uri: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        fingerprinter: Fingerprinter | None = None,
        overwrite: bool = True,
        provenance: Mapping[str, Any] | None = None,
    ) -> Generator[Path]:
        """Stage an artifact for atomic creation.

        Yields a temporary path to write to. On clean exit the file is
        validated, committed with :func:`os.replace`, and recorded. If the body
        raises, the temporary file is removed and no partial artifact remains.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            Run identifier. Required for the user tier.
        tier : Tier, optional
            Tier to write into. Default :attr:`Tier.USER`; writing directly to
            the shared tier bypasses promotion and is rarely correct.
        source : str or None, optional
            Provenance string recorded with the artifact.
        asset_uri : str or None, optional
            Asset key recorded with the artifact. Defaults to the committed
            location's :attr:`Location.uri`.
        metadata : Mapping of str to Any or None, optional
            Descriptive metadata recorded with the artifact.
        fingerprinter : Fingerprinter or None, optional
            Strategy applied on commit, overriding
            :attr:`ArtifactCache.fingerprinter` for this write.
        overwrite : bool, optional
            Whether committing may replace an existing artifact. Forced to
            ``True`` when :attr:`bypass` is set, since the caller was told the
            artifact was missing and must be able to write it.
        provenance : Mapping of str to Any or None, optional
            Extra record fields, used by :meth:`promote` to stamp
            ``promoted_from_run_id``, ``promoted_by`` and ``promoted_at``.

        Yields
        ------
        Path
            Temporary path the caller must write its content to.

        Raises
        ------
        ArtifactExistsError
            If the artifact exists and ``overwrite`` is ``False``.
        ArtifactCacheError
            If the body completes without producing a non-empty file.

        Examples
        --------
        >>> with cache.stage("filtered.nc", run_id="abc-123") as tmp:
        ...     dataset.to_netcdf(tmp)
        """
        location = self.locate(name, tier, run_id)
        if self.bypass:
            overwrite = True
        if not overwrite and location.exists:
            raise ArtifactExistsError(f"{location.path} already exists")

        location.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = location.path.with_name(
            f"{location.path.name}.{os.getpid()}{_TMP_SUFFIX}"
        )
        try:
            yield tmp
            if not tmp.is_file():
                raise ArtifactCacheError(
                    f"staged artifact was never written: expected {tmp}"
                )
            if tmp.stat().st_size == 0:
                raise ArtifactCacheError(f"staged artifact is empty: {tmp}")
            strategy = fingerprinter or self.fingerprinter
            digest = strategy.digest(tmp)
            size = tmp.stat().st_size
            os.replace(tmp, location.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

        record_cls = SharedRecord if tier is Tier.SHARED else ArtifactRecord
        record = record_cls(
            name=name,
            size_bytes=size,
            created_at=self._utcnow(),
            created_by=self._username(),
            checksum=digest,
            checksum_mode=strategy.mode if digest is not None else None,
            source=source,
            asset_uri=asset_uri or location.uri,
            run_id=run_id,
            metadata=dict(metadata or {}),
            **dict(provenance or {}),
        )
        self._write_record(location, record)

    def ingest(
        self,
        source_path: Path | str,
        name: str,
        run_id: str,
        move: bool = False,
        metadata: Mapping[str, Any] | None = None,
        fingerprinter: Fingerprinter | None = None,
        overwrite: bool = True,
    ) -> Location:
        """Copy an externally produced file into the user tier.

        Parameters
        ----------
        source_path : Path or str
            Existing file in a transient, user-defined location.
        name : str
            Artifact filename to store it under.
        run_id : str
            Run identifier. Ingestion always targets the user tier, so this is
            required.
        move : bool, optional
            Remove ``source_path`` after a successful copy.
        metadata : Mapping of str to Any or None, optional
            Descriptive metadata recorded with the artifact.
        fingerprinter : Fingerprinter or None, optional
            See :meth:`stage`.
        overwrite : bool, optional
            See :meth:`stage`.

        Returns
        -------
        Location
            Committed user-tier location.

        Raises
        ------
        FileNotFoundError
            If ``source_path`` does not exist or is not a regular file.
        """
        src = Path(source_path).expanduser()
        if not src.is_file():
            raise FileNotFoundError(f"source artifact not found: {src}")

        with self.stage(
            name,
            run_id,
            tier=Tier.USER,
            source=str(src),
            metadata=metadata,
            fingerprinter=fingerprinter,
            overwrite=overwrite,
        ) as tmp:
            shutil.copy2(src, tmp)

        if move:
            src.unlink(missing_ok=True)
        return self.locate(name, Tier.USER, run_id)

    def promote(
        self,
        name: str,
        run_id: str,
        overwrite: bool = False,
        fingerprinter: Fingerprinter | None = None,
    ) -> Location:
        """Copy a user-tier artifact into the shared tier, addressed by name.

        ``run_id`` names the *source* only. The destination is
        ``<shared_root>/<name>``, so a later run can find the artifact without
        knowing which run promoted it.

        Promotion copies rather than moves, so the user's local copy remains
        valid. Provenance that the path no longer carries is recorded on the
        shared sidecar.

        Re-promoting a name that already exists compares fingerprints:
        byte-identical content is an idempotent no-op, and content that differs
        raises, because ``name`` is the sole identity of shared artifacts.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run whose user-tier copy to promote.
        overwrite : bool, optional
            Replace an existing shared artifact even when content differs.
            Default ``False``, since promoted data is treated as immutable.
        fingerprinter : Fingerprinter or None, optional
            Strategy used to fingerprint the promoted copy. Defaults to the
            cache's own.

        Returns
        -------
        Location
            Shared-tier location of the promoted artifact.

        Raises
        ------
        ArtifactNotFoundError
            If the artifact is absent from the named run's user tier.
        ArtifactExistsError
            If a different artifact already occupies that shared name and
            ``overwrite`` is ``False``.
        """
        user = self.locate(name, Tier.USER, run_id)
        if not user.exists:
            raise ArtifactNotFoundError(
                f"cannot promote {name!r}: absent from run {run_id!r} at {user.path}"
            )
        shared = self.locate(name, Tier.SHARED)

        if shared.exists and not overwrite:
            if self._same_content(user, shared):
                return shared
            raise ArtifactExistsError(
                f"{shared.path} already holds different content for {name!r}; "
                "shared artifacts are addressed by name alone, so either choose a "
                "content-identifying name or pass overwrite=True"
            )

        existing = self.record_for(user)
        with self.stage(
            name,
            tier=Tier.SHARED,
            source=str(user.path),
            asset_uri=shared.uri,
            metadata=dict(existing.metadata) if existing else None,
            fingerprinter=fingerprinter,
            overwrite=True,
            provenance={
                "promoted_from_run_id": run_id,
                "promoted_by": self._username(),
                "promoted_at": self._utcnow(),
            },
        ) as tmp:
            shutil.copy2(user.path, tmp)

        return shared

    def _same_content(self, user: Location, shared: Location) -> bool:
        """Report whether two committed artifacts hold identical bytes.

        Checks identity before content: two paths that resolve to the same
        inode hold the same bytes by definition, and answering that with two
        ``stat`` calls avoids re-reading a multi-gigabyte artifact to discover
        it is itself. Only distinct files fall through to the digest, which is
        compared using the strategy that produced the recorded value so the two
        values are comparable.

        Parameters
        ----------
        user : Location
            User-tier copy, assumed to exist.
        shared : Location
            Shared-tier copy, assumed to exist.

        Returns
        -------
        bool
            ``True`` when the two paths are the same file, or when a recorded
            digest exists and matches. Absent both there is no evidence of
            sameness, which is reported as ``False`` rather than assumed.
        """
        if self._is_same_file(user.path, shared.path):
            return True
        record = self.record_for(shared)
        if record is None or record.checksum is None or record.checksum_mode is None:
            return False
        strategy = fingerprinter_for(record.checksum_mode)
        return strategy.matches(user.path, record.checksum)

    @staticmethod
    def _is_same_file(left: Path, right: Path) -> bool:
        """Report whether two paths refer to one file on disk.

        Compares device and inode, so symlinks, hardlinks, and any two routes
        to the same file all answer ``True``.

        Parameters
        ----------
        left : Path
            First path.
        right : Path
            Second path.

        Returns
        -------
        bool
            ``True`` when both exist and share an inode. A missing path is
            reported as ``False`` rather than raising, since callers use this
            as a fast path and fall through to a content comparison.
        """
        try:
            return os.path.samefile(left, right)
        except OSError:
            return False

    def verify(
        self,
        name: str,
        run_id: str | None = None,
        prefer_local: bool = False,
    ) -> bool | None:
        """Re-fingerprint an artifact and compare against its record.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str or None, optional
            See :meth:`resolve`.
        prefer_local : bool, optional
            See :meth:`resolve`.

        Returns
        -------
        bool or None
            Whether the artifact still matches its recorded digest, or ``None``
            when no digest was taken and there is nothing to check against.

        Raises
        ------
        ArtifactNotFoundError
            If the artifact is absent from every candidate tier.
        """
        location = self._lookup(name, run_id, prefer_local=prefer_local)
        if location is None:
            scope = (
                "shared tier" if run_id is None else f"shared tier or run {run_id!r}"
            )
            raise ArtifactNotFoundError(f"artifact {name!r} not found in {scope}")
        record = self.record_for(location)
        if record is None or record.checksum is None or record.checksum_mode is None:
            return None
        strategy = fingerprinter_for(record.checksum_mode)
        return strategy.matches(location.path, record.checksum)

    def record_use(
        self,
        name: str,
        run_id: str,
        min_interval_seconds: float = DEFAULT_USE_INTERVAL_SECONDS,
    ) -> bool:
        """Record that a run used a shared artifact, debounced.

        Upserts the run's entry in the artifact's reference log. A run that
        reads the same artifact repeatedly refreshes its own timestamp at most
        once per ``min_interval_seconds``, so a hot artifact does not generate
        a write per read.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run recording the use.
        min_interval_seconds : float, optional
            Minimum gap between recorded touches by the same run. Pass ``0`` to
            force a write.

        Returns
        -------
        bool
            Whether the sidecar was updated. ``False`` means either the touch
            was debounced, or there is no shared artifact and record to attach
            it to.
        """
        location = self.locate(name, Tier.SHARED)
        if not location.exists:
            return False

        with self._lock(self.shared_record_path(name)):
            record = self.read_shared_record(name)
            if record is None:
                return False

            now = self._utcnow()
            others = [ref for ref in record.references if ref.run_id != run_id]
            mine = next(
                (ref for ref in record.references if ref.run_id == run_id), None
            )
            if mine is not None and self._age_seconds(mine.last_used_at, now) < (
                min_interval_seconds
            ):
                return False

            entry = Reference(run_id=run_id, used_by=self._username(), last_used_at=now)
            references = [*others, entry][-MAX_REFERENCES:]
            self.write_shared_record(
                record.model_copy(
                    update={
                        "references": references,
                        "first_referenced_at": record.first_referenced_at or now,
                        "reference_total": record.reference_total
                        + (1 if mine is None else 0),
                    }
                )
            )
        return True

    def references_for(self, name: str) -> list[Reference]:
        """Return the retained reference log for a shared artifact.

        Parameters
        ----------
        name : str
            Artifact filename.

        Returns
        -------
        list of Reference
            Recent consumers, most-recently-used last. Empty when the artifact
            has no record or no recorded uses.
        """
        record = self.read_shared_record(name)
        return list(record.references) if record else []

    def gc_candidates(self, idle_days: float = 180.0) -> list[UsageReport]:
        """Report shared artifacts that nothing has touched recently.

        Reports only; nothing is deleted. Liveness here is *inferred* from
        voluntary registration, so a quiet artifact may still have readers that
        never went through :meth:`record_use`. Deletion stays a human decision
        via :meth:`delete_shared`.

        Parameters
        ----------
        idle_days : float, optional
            Minimum idle period for an artifact to be reported.

        Returns
        -------
        list of UsageReport
            Candidates, most idle first. Artifacts with no timestamp at all —
            neither a use nor a promotion — are included, since nothing
            suggests they are live.
        """
        now = self._utcnow()
        reports: list[UsageReport] = []
        for location in self.list_shared_artifacts():
            record = self.read_shared_record(location.name)
            if record is None:
                continue
            last_used = record.last_used_at
            age = (
                self._age_seconds(last_used, now) / 86400.0
                if last_used is not None
                else None
            )
            if age is not None and age < idle_days:
                continue
            reports.append(
                UsageReport(
                    name=record.name,
                    size_bytes=record.size_bytes,
                    last_used_at=last_used,
                    idle_days=round(age, 2) if age is not None else None,
                    reference_total=record.reference_total,
                    recent_runs=[ref.run_id for ref in record.references],
                    promoted_from_run_id=record.promoted_from_run_id,
                )
            )
        reports.sort(key=lambda r: (r.idle_days is None, -(r.idle_days or 0.0)))
        return reports

    @staticmethod
    def _age_seconds(earlier: str, later: str) -> float:
        """Return the gap in seconds between two ISO-8601 timestamps.

        Parameters
        ----------
        earlier : str
            Earlier timestamp.
        later : str
            Later timestamp.

        Returns
        -------
        float
            Seconds between them, or ``0.0`` if either cannot be parsed, which
            makes an unparseable timestamp look recent rather than infinitely
            stale.
        """
        try:
            return (
                datetime.fromisoformat(later) - datetime.fromisoformat(earlier)
            ).total_seconds()
        except ValueError:
            return 0.0

    # ------------------------------------------------------------------
    # Records
    # ------------------------------------------------------------------

    def manifest_path(self, run_id: str) -> Path:
        """Return the manifest path for a user-tier run directory.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Path
            Path to the sidecar manifest, which may not exist.
        """
        self._validate_component(run_id, "run_id")
        path = self.user_root / run_id / MANIFEST_NAME
        self._assert_contained(path, self.user_root)
        return path

    def shared_record_path(self, name: str) -> Path:
        """Return the sidecar path for a shared-tier artifact.

        Parameters
        ----------
        name : str
            Artifact filename.

        Returns
        -------
        Path
            Path to the per-artifact sidecar under :data:`SHARED_RECORD_DIR`,
            which may not exist.
        """
        self._validate_component(name, "name")
        path = self.shared_root / SHARED_RECORD_DIR / f"{name}{_RECORD_SUFFIX}"
        self._assert_contained(path, self.shared_root)
        return path

    def read_manifest(self, run_id: str) -> Manifest:
        """Read a user-tier run manifest, returning an empty one if absent.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Manifest
            Parsed manifest, or an empty manifest when the sidecar is missing
            or unreadable.
        """
        payload = self._read_json(self.manifest_path(run_id))
        if payload is None:
            return Manifest(run_id=run_id)
        try:
            return Manifest.from_dict(payload)
        except ValidationError:
            return Manifest(run_id=run_id)

    def write_manifest(self, manifest: Manifest) -> Path:
        """Atomically write a user-tier manifest to its run directory.

        Parameters
        ----------
        manifest : Manifest
            Manifest to persist.

        Returns
        -------
        Path
            Path the manifest was written to.
        """
        return self._write_json(self.manifest_path(manifest.run_id), manifest.to_dict())

    def read_shared_record(self, name: str) -> SharedRecord | None:
        """Read the sidecar record for a shared-tier artifact.

        Parameters
        ----------
        name : str
            Artifact filename.

        Returns
        -------
        SharedRecord or None
            Parsed record, or ``None`` when the sidecar is missing or
            unreadable.
        """
        payload = self._read_json(self.shared_record_path(name))
        if payload is None:
            return None
        try:
            return SharedRecord.model_validate(payload)
        except ValidationError:
            return None

    def write_shared_record(self, record: SharedRecord) -> Path:
        """Atomically write a shared-tier sidecar record.

        Parameters
        ----------
        record : SharedRecord
            Record to persist.

        Returns
        -------
        Path
            Path the sidecar was written to.
        """
        return self._write_json(self.shared_record_path(record.name), record.to_dict())

    def record_for(self, location: Location) -> ArtifactRecord | None:
        """Return the record describing a location, from whichever store holds it.

        Parameters
        ----------
        location : Location
            Location to describe.

        Returns
        -------
        ArtifactRecord or None
            The record, or ``None`` when none has been written.
        """
        if location.tier is Tier.SHARED:
            return self.read_shared_record(location.name)
        if location.run_id is None:
            return None
        return self.read_manifest(location.run_id).artifacts.get(location.name)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def list_runs(self) -> list[str]:
        """List run identifiers present in the user tier.

        Returns
        -------
        list of str
            Sorted run identifiers, derived from directories on disk rather
            than from any index. The shared tier has no run directories, so
            this question applies only to the user tier.
        """
        if not self.user_root.is_dir():
            return []
        return sorted(
            p.name
            for p in self.user_root.iterdir()
            if p.is_dir() and not p.is_symlink() and not p.name.startswith(".")
        )

    def list_user_artifacts(self, run_id: str) -> list[Location]:
        """List artifacts physically present in a user-tier run directory.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        list of Location
            Sorted locations of regular files, excluding bookkeeping files.
        """
        self._validate_component(run_id, "run_id")
        directory = self.user_root / run_id
        if not directory.is_dir():
            return []
        return [
            self.locate(p.name, Tier.USER, run_id)
            for p in sorted(directory.iterdir())
            if p.is_file() and not self._is_reserved(p.name)
        ]

    def list_shared_artifacts(self) -> list[Location]:
        """List artifacts physically present in the shared tier.

        Returns
        -------
        list of Location
            Sorted locations of regular files at the shared root, excluding
            bookkeeping files. Answers "what is available?" without reference
            to any run.
        """
        if not self.shared_root.is_dir():
            return []
        return [
            self.locate(p.name, Tier.SHARED)
            for p in sorted(self.shared_root.iterdir())
            if p.is_file() and not self._is_reserved(p.name)
        ]

    def describe(self, run_id: str) -> dict[str, ArtifactRecord]:
        """Return user-tier records for artifacts that still exist on disk.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        dict of str to ArtifactRecord
            Records keyed by artifact name, filtered to present files.
        """
        present = {loc.name for loc in self.list_user_artifacts(run_id)}
        return {
            name: record
            for name, record in self.read_manifest(run_id).artifacts.items()
            if name in present
        }

    def describe_shared(self) -> dict[str, ArtifactRecord]:
        """Return shared-tier records for artifacts that still exist on disk.

        Returns
        -------
        dict of str to ArtifactRecord
            Records keyed by artifact name. Names present on disk without a
            readable sidecar are omitted.
        """
        described: dict[str, ArtifactRecord] = {}
        for location in self.list_shared_artifacts():
            record = self.read_shared_record(location.name)
            if record is not None:
                described[location.name] = record
        return described

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def refresh_view(
        self,
        run_id: str,
        view_dir: Path | str | None = None,
        names: Sequence[str] | None = None,
        prefer_local: bool = False,
    ) -> dict[str, Path]:
        """Rebuild a directory of symlinks pointing at a run's artifacts.

        Regenerated from current filesystem truth rather than maintained
        incrementally, so it self-heals after promotion, scratch purges, and
        user deletions. Links resolve through :meth:`resolve`, preferring the
        durable shared copy.

        Parameters
        ----------
        run_id : str
            Run whose artifacts to link.
        view_dir : Path or str or None, optional
            Directory to build. Defaults to ``view_root / run_id``.
        names : Sequence of str or None, optional
            Additional artifact names to include. The shared tier is flat and
            may hold far more than one run needs, so it is not swept wholesale;
            name the shared-only artifacts this run consumes.
        prefer_local : bool, optional
            See :meth:`resolve`.

        Returns
        -------
        dict of str to Path
            Mapping of artifact name to the link target it now points at.

        Raises
        ------
        ValueError
            If neither ``view_dir`` nor ``view_root`` was provided.

        Notes
        -----
        Links are replaced atomically. A symlink into a scratch filesystem is
        unusable from any container or batch job that does not bind-mount that
        filesystem at the same path.
        """
        if view_dir is None:
            if self.view_root is None:
                raise ValueError("no view_dir given and view_root is not configured")
            self._validate_component(run_id, "run_id")
            view_dir = self.view_root / run_id
        target_dir = Path(view_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)

        for stale in target_dir.iterdir():
            if stale.is_symlink():
                stale.unlink()

        wanted = {loc.name for loc in self.list_user_artifacts(run_id)}
        wanted |= set(names or ())

        linked: dict[str, Path] = {}
        for name in sorted(wanted):
            resolved = self._lookup(name, run_id, prefer_local=prefer_local)
            if resolved is None:
                continue
            link = target_dir / name
            tmp_link = target_dir / f".{name}.{os.getpid()}{_TMP_SUFFIX}"
            tmp_link.unlink(missing_ok=True)
            tmp_link.symlink_to(resolved.path)
            os.replace(tmp_link, link)
            linked[name] = resolved.path
        return linked

    # ------------------------------------------------------------------
    # Deletion
    # ------------------------------------------------------------------

    def delete_user_run(self, run_id: str, missing_ok: bool = True) -> bool:
        """Delete a run directory from the user tier.

        Parameters
        ----------
        run_id : str
            Run identifier.
        missing_ok : bool, optional
            Return ``False`` instead of raising when the run is absent.

        Returns
        -------
        bool
            Whether a directory was removed.

        Raises
        ------
        UnsafePathError
            If the target is a symlink or is not a directory.
        ArtifactNotFoundError
            If the run is absent and ``missing_ok`` is ``False``.
        """
        self._validate_component(run_id, "run_id")
        directory = self.user_root / run_id
        if directory.is_symlink():
            raise UnsafePathError(f"refusing to delete symlinked run dir: {directory}")
        if not directory.exists():
            if missing_ok:
                return False
            raise ArtifactNotFoundError(f"run {run_id!r} not present in user tier")
        if not directory.is_dir():
            raise UnsafePathError(f"not a directory: {directory}")
        self._assert_contained(directory, self.user_root)
        if directory.resolve() == self.user_root:
            raise UnsafePathError(f"refusing to delete tier root: {self.user_root}")
        shutil.rmtree(directory)
        return True

    def delete_shared(
        self,
        name: str,
        confirm: bool = False,
        missing_ok: bool = True,
    ) -> bool:
        """Delete a single artifact, and its sidecar, from the shared tier.

        Deliberately per-artifact rather than per-run: the shared tier has no
        run directories, and deletion there may affect other users, so it is
        guarded by an explicit flag.

        Parameters
        ----------
        name : str
            Artifact filename.
        confirm : bool, optional
            Must be ``True`` for the deletion to proceed.
        missing_ok : bool, optional
            Return ``False`` instead of raising when the artifact is absent.

        Returns
        -------
        bool
            Whether an artifact was removed.

        Raises
        ------
        PermissionError
            If ``confirm`` is not ``True``.
        UnsafePathError
            If the target is a symlink rather than a regular file.
        ArtifactNotFoundError
            If absent and ``missing_ok`` is ``False``.
        """
        if not confirm:
            raise PermissionError(
                f"refusing to delete shared artifact {name!r}: pass confirm=True"
            )
        location = self.locate(name, Tier.SHARED)
        if location.path.is_symlink():
            raise UnsafePathError(f"refusing to delete symlink: {location.path}")
        if not location.path.exists():
            if missing_ok:
                return False
            raise ArtifactNotFoundError(f"{name!r} not present in shared tier")
        location.path.unlink()
        self.shared_record_path(name).unlink(missing_ok=True)
        return True

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _write_record(self, location: Location, record: ArtifactRecord) -> None:
        """Persist a record to whichever store backs its tier.

        User-tier records are merged into the run manifest under an exclusive
        lock. Shared-tier records are written to their own sidecar, so
        concurrent promotions of different artifacts never contend.

        Parameters
        ----------
        location : Location
            Committed location the record describes.
        record : ArtifactRecord
            Record to persist.
        """
        if location.tier is Tier.SHARED:
            with self._lock(self.shared_record_path(location.name)):
                shared = (
                    record
                    if isinstance(record, SharedRecord)
                    else SharedRecord.model_validate(record.to_dict())
                )
                previous = self.read_shared_record(location.name)
                if previous is not None:
                    shared = shared.model_copy(
                        update={
                            "references": list(previous.references),
                            "first_referenced_at": previous.first_referenced_at,
                            "reference_total": previous.reference_total,
                        }
                    )
                self.write_shared_record(shared)
            return

        assert location.run_id is not None
        with self._lock(self.manifest_path(location.run_id)):
            manifest = self.read_manifest(location.run_id)
            artifacts = dict(manifest.artifacts)
            artifacts[record.name] = record
            self.write_manifest(manifest.model_copy(update={"artifacts": artifacts}))

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        """Read a JSON object from disk, tolerating absence and corruption.

        Parameters
        ----------
        path : Path
            File to read.

        Returns
        -------
        dict of str to Any or None
            Parsed object, or ``None`` when missing or unparseable. A
            truncated sidecar degrades to "no record" rather than breaking
            listing for everyone.
        """
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
        """Atomically write a JSON object to disk.

        Parameters
        ----------
        path : Path
            Destination file.
        payload : Mapping of str to Any
            Object to serialise.

        Returns
        -------
        Path
            The destination path.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{os.getpid()}{_TMP_SUFFIX}")
        tmp.write_text(
            json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(tmp, path)
        return path

    @contextmanager
    def _lock(self, record_path: Path) -> Generator[None]:
        """Hold an exclusive advisory lock beside a record file.

        Parameters
        ----------
        record_path : Path
            Record whose updates are being serialised. The lock file is a
            sibling, so shared-tier locks are per-artifact.

        Yields
        ------
        None
            Control is yielded with the lock held.
        """
        lock_path = record_path.with_name(f"{record_path.name}{_LOCK_SUFFIX}")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o664)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @staticmethod
    def _is_reserved(filename: str) -> bool:
        """Report whether a filename is cache bookkeeping rather than an artifact.

        Parameters
        ----------
        filename : str
            Basename to classify.

        Returns
        -------
        bool
            ``True`` for the run manifest, lock files, in-flight temporaries,
            and dotfiles. Shared sidecars live in a dot-directory, so they are
            never mistaken for artifacts even when an artifact ends in
            ``.json``.
        """
        return (
            filename == MANIFEST_NAME
            or filename.startswith(".")
            or filename.endswith((_TMP_SUFFIX, _LOCK_SUFFIX))
        )

    @staticmethod
    def _validate_component(value: str, label: str) -> None:
        """Reject path components that could escape managed storage.

        Parameters
        ----------
        value : str
            Candidate component.
        label : str
            Parameter name, used in the error message.

        Raises
        ------
        UnsafePathError
            If the value is empty, contains a separator, or is a traversal
            component.
        """
        if not value or value in {".", ".."} or os.sep in value or "/" in value:
            raise UnsafePathError(f"invalid {label}: {value!r}")

    @staticmethod
    def _assert_contained(path: Path, root: Path) -> None:
        """Assert that ``path`` lies beneath ``root``.

        Parameters
        ----------
        path : Path
            Path to check. Need not exist.
        root : Path
            Root it must be contained by.

        Raises
        ------
        UnsafePathError
            If ``path`` is outside ``root``.
        """
        try:
            path.expanduser().absolute().relative_to(root)
        except ValueError as exc:
            raise UnsafePathError(f"{path} escapes managed root {root}") from exc

    @staticmethod
    def _utcnow() -> str:
        """Return the current UTC time as an ISO-8601 string.

        Returns
        -------
        str
            Timestamp with second resolution.
        """
        return datetime.now(UTC).isoformat(timespec="seconds")

    @staticmethod
    def _username() -> str:
        """Return the current operating-system username.

        Returns
        -------
        str
            Username, or ``"unknown"`` if it cannot be determined.
        """
        try:
            import getpass

            return getpass.getuser()
        except (OSError, KeyError):
            return "unknown"

    def __repr__(self) -> str:
        """Return a debugging representation of the cache.

        Returns
        -------
        str
            Representation including both tier roots.
        """
        return (
            f"{type(self).__name__}(user_root={str(self.user_root)!r}, "
            f"shared_root={str(self.shared_root)!r}, bypass={self.bypass})"
        )
