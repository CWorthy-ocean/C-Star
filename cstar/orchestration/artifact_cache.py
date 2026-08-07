"""Two-layer artifact cache with sidecar manifests.

This module provides :class:`ArtifactCache`, a storage component that manages
expensive derived data files (for example NetCDF products) across two tiers:

``USER``
    A fast, transient per-user location such as HPC scratch. Cheap to write,
    subject to automatic purge policies, and freely deletable by its owner.

``SHARED``
    A durable location visible to all users. Populated by *promotion*, which
    copies rather than moves, so the user tier copy remains valid.

Design notes
------------
The component performs **no caching of its own lookups**. Every resolution
stats the filesystem, so a file deleted by a user or reclaimed by a scratch
purge degrades to a cache miss rather than a stale hit.

The module deliberately has no workflow-engine dependencies. Orchestration
concerns (retries, task boundaries, lineage events) belong in thin wrappers
around this class, which keeps the storage logic unit-testable without
standing up a server.

All writes are atomic: content is staged to a temporary sibling and committed
with :func:`os.replace`, so readers never observe a partially written file.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

__all__ = [
    "ArtifactCache",
    "ArtifactCacheError",
    "ArtifactExistsError",
    "ArtifactNotFoundError",
    "ArtifactRecord",
    "Location",
    "Manifest",
    "Tier",
    "UnsafePathError",
]

MANIFEST_NAME: Final[str] = "manifest.json"
"""Filename of the sidecar manifest written into every run directory."""

MANIFEST_VERSION: Final[int] = 1
"""Schema version stamped into each manifest, for future migrations."""

_LOCK_SUFFIX: Final[str] = ".lock"
_TMP_SUFFIX: Final[str] = ".tmp"
_CHECKSUM_CHUNK: Final[int] = 1024 * 1024


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
        Per-user transient storage. Fast, purge-eligible, owner-deletable.
    SHARED : str
        Durable shared storage, populated by :meth:`ArtifactCache.promote`.
    """

    USER = "user"
    SHARED = "shared"


@dataclass(frozen=True, slots=True)
class Location:
    """Resolved filesystem position of an artifact.

    Parameters
    ----------
    path : Path
        Absolute path to the artifact on disk. Not guaranteed to exist; use
        :attr:`exists` or :meth:`ArtifactCache.resolve` to test.
    tier : Tier
        Tier this location belongs to.
    name : str
        Artifact filename, unique within its run directory.
    run_id : str
        Identifier of the run that produced the artifact.
    """

    path: Path
    tier: Tier
    name: str
    run_id: str

    @property
    def uri(self) -> str:
        """str: ``file://`` URI for this location.

        Suitable for use as a workflow-engine asset key. Because the URI is
        derived from :attr:`path` rather than typed independently, the asset
        identifier cannot drift from the bytes actually written.
        """
        return self.path.as_uri()

    @property
    def exists(self) -> bool:
        """bool: Whether a regular file is present at :attr:`path` right now.

        Notes
        -----
        This performs a live ``stat`` on every access and is never memoized.
        """
        return self.path.is_file()


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    """Manifest entry describing a single artifact.

    Parameters
    ----------
    name : str
        Artifact filename within the run directory.
    size_bytes : int
        Size of the file at the time it was committed.
    created_at : str
        UTC ISO-8601 timestamp of commit.
    created_by : str
        Operating-system username of the writer.
    checksum : str or None, optional
        Hex-encoded SHA-256 digest, or ``None`` when checksumming was skipped.
    source : str or None, optional
        Free-form provenance string, such as the input dataset path.
    asset_uri : str or None, optional
        Workflow-engine asset key emitted for this artifact, enabling reverse
        lookup from disk contents back to lineage records.
    metadata : dict of str to Any, optional
        Caller-supplied descriptive metadata.
    """

    name: str
    size_bytes: int
    created_at: str
    created_by: str
    checksum: str | None = None
    source: str | None = None
    asset_uri: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise this record to a JSON-compatible mapping.

        Returns
        -------
        dict of str to Any
            Plain dictionary suitable for :func:`json.dump`.
        """
        return {
            "name": self.name,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "checksum": self.checksum,
            "source": self.source,
            "asset_uri": self.asset_uri,
            "metadata": dict(self.metadata),
        }

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
        """
        return cls(
            name=str(payload["name"]),
            size_bytes=int(payload["size_bytes"]),
            created_at=str(payload["created_at"]),
            created_by=str(payload["created_by"]),
            checksum=payload.get("checksum"),
            source=payload.get("source"),
            asset_uri=payload.get("asset_uri"),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class Manifest:
    """Sidecar index describing the contents of one run directory.

    One manifest is written per ``(tier, run_id)`` directory, making each
    directory self-describing. Promotion copies the manifest alongside the
    data so the shared tier never contains undocumented artifacts.

    Parameters
    ----------
    run_id : str
        Identifier of the run this manifest describes.
    tier : Tier
        Tier the manifest lives in.
    artifacts : dict of str to ArtifactRecord
        Records keyed by artifact name.
    version : int, optional
        Manifest schema version.
    promoted_at : str or None, optional
        UTC ISO-8601 timestamp at which this run was promoted, set only on
        shared-tier manifests.
    """

    run_id: str
    tier: Tier
    artifacts: dict[str, ArtifactRecord] = field(default_factory=dict)
    version: int = MANIFEST_VERSION
    promoted_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise this manifest to a JSON-compatible mapping.

        Returns
        -------
        dict of str to Any
            Plain dictionary suitable for :func:`json.dump`.
        """
        return {
            "version": self.version,
            "run_id": self.run_id,
            "tier": self.tier.value,
            "promoted_at": self.promoted_at,
            "artifacts": {n: r.to_dict() for n, r in self.artifacts.items()},
        }

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
        """
        return cls(
            run_id=str(payload["run_id"]),
            tier=Tier(payload["tier"]),
            artifacts={
                str(n): ArtifactRecord.from_dict(r)
                for n, r in (payload.get("artifacts") or {}).items()
            },
            version=int(payload.get("version", MANIFEST_VERSION)),
            promoted_at=payload.get("promoted_at"),
        )


class ArtifactCache:
    """Two-layer artifact cache with atomic writes and sidecar manifests.

    Artifacts are addressed by ``(run_id, name)`` and laid out as
    ``<root>/<run_id>/<name>``, with a :data:`MANIFEST_NAME` sidecar in each
    run directory. Lookups check the shared tier first and fall back to the
    user tier, so promoted data survives deletion of a user's local copy.

    Parameters
    ----------
    user_root : Path or str
        Root of the per-user cache, for example ``~/.cache/app``. ``~`` is
        expanded.
    shared_root : Path or str
        Root of the shared cache, for example ``/scratch/app``.
    view_root : Path or str or None, optional
        Root beneath which symlink views are materialised by
        :meth:`refresh_view`. Required only if views are used.
    create_roots : bool, optional
        Whether to create the roots on construction. Default ``True``.

    Attributes
    ----------
    user_root : Path
        Resolved user-tier root.
    shared_root : Path
        Resolved shared-tier root.
    view_root : Path or None
        Resolved view root, if configured.

    Notes
    -----
    Instances hold no mutable lookup state and are safe to share across
    threads. Cross-process consistency of manifest updates relies on
    :func:`fcntl.flock`, which is reliable on local filesystems and NFSv4 but
    may be unreliable on some older or object-store-backed mounts.

    Examples
    --------
    >>> cache = ArtifactCache("~/.cache/app", "/scratch/app")
    >>> with cache.stage("filtered.nc", run_id="abc-123") as tmp:
    ...     tmp.write_bytes(b"...")
    >>> cache.resolve("filtered.nc", run_id="abc-123").tier
    <Tier.USER: 'user'>
    """

    def __init__(
        self,
        user_root: Path | str,
        shared_root: Path | str,
        view_root: Path | str | None = None,
        create_roots: bool = True,
    ) -> None:
        self.user_root: Path = Path(user_root).expanduser().resolve()
        self.shared_root: Path = Path(shared_root).expanduser().resolve()
        self.view_root: Path | None = (
            Path(view_root).expanduser().resolve() if view_root is not None else None
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

    def locate(self, name: str, run_id: str, tier: Tier) -> Location:
        """Compute the canonical location of an artifact in one tier.

        This is the single source of truth for artifact placement: both the
        filesystem path and the asset URI derive from its result, so the two
        cannot disagree.

        Parameters
        ----------
        name : str
            Artifact filename. Must not contain path separators.
        run_id : str
            Run identifier. Must not contain path separators.
        tier : Tier
            Tier to compute the location within.

        Returns
        -------
        Location
            Canonical location. The file may or may not exist.

        Raises
        ------
        UnsafePathError
            If ``name`` or ``run_id`` contain separators or traversal
            components, or if the result escapes the tier root.
        """
        self._validate_component(name, "name")
        self._validate_component(run_id, "run_id")
        root = self.root_for(tier)
        path = root / run_id / name
        self._assert_contained(path, root)
        return Location(path=path, tier=tier, name=name, run_id=run_id)

    def candidates(self, name: str, run_id: str) -> tuple[Location, Location]:
        """Return both possible locations, in resolution order.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run identifier.

        Returns
        -------
        tuple of Location
            ``(shared, user)`` locations, matching the precedence used by
            :meth:`resolve`.
        """
        return (
            self.locate(name, run_id, Tier.SHARED),
            self.locate(name, run_id, Tier.USER),
        )

    def resolve(
        self,
        name: str,
        run_id: str,
        prefer_local: bool = False,
    ) -> Location | None:
        """Find an artifact, checking the shared tier first.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run identifier.
        prefer_local : bool, optional
            Reverse the precedence and check the user tier first. Useful when
            iterating locally on a product that also exists in the shared
            tier. Default ``False``.

        Returns
        -------
        Location or None
            Location of the first tier in which the file is present, or
            ``None`` if absent from both.

        Notes
        -----
        Existence is tested live on every call and is never memoized, so a
        purged or user-deleted file yields ``None`` rather than a stale hit.
        """
        shared, user = self.candidates(name, run_id)
        ordered = (user, shared) if prefer_local else (shared, user)
        for location in ordered:
            if location.exists:
                return location
        return None

    def require(
        self,
        name: str,
        run_id: str,
        prefer_local: bool = False,
    ) -> Location:
        """Resolve an artifact or raise if it is missing.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run identifier.
        prefer_local : bool, optional
            See :meth:`resolve`.

        Returns
        -------
        Location
            Location of the existing artifact.

        Raises
        ------
        ArtifactNotFoundError
            If the artifact exists in neither tier.
        """
        location = self.resolve(name, run_id, prefer_local=prefer_local)
        if location is None:
            raise ArtifactNotFoundError(
                f"artifact {name!r} for run {run_id!r} not found in either tier"
            )
        return location

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    @contextmanager
    def stage(
        self,
        name: str,
        run_id: str,
        tier: Tier = Tier.USER,
        source: str | None = None,
        asset_uri: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        compute_checksum: bool = False,
        overwrite: bool = True,
    ) -> Iterator[Path]:
        """Stage an artifact for atomic creation.

        Yields a temporary path to write to. On clean exit the file is
        validated, committed with :func:`os.replace`, and recorded in the run
        manifest. If the body raises, the temporary file is removed and no
        partial artifact is left behind.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run identifier.
        tier : Tier, optional
            Tier to write into. Default :attr:`Tier.USER`; writing directly to
            the shared tier bypasses promotion and is rarely correct.
        source : str or None, optional
            Provenance string recorded in the manifest.
        asset_uri : str or None, optional
            Asset key recorded in the manifest. Defaults to the committed
            location's :attr:`Location.uri`.
        metadata : Mapping of str to Any or None, optional
            Descriptive metadata recorded in the manifest.
        compute_checksum : bool, optional
            Whether to compute a SHA-256 digest on commit. Default ``False``,
            since digesting multi-gigabyte files is expensive; enable it where
            silent corruption matters more than write throughput.
        overwrite : bool, optional
            Whether committing may replace an existing artifact. Default
            ``True``.

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
        location = self.locate(name, run_id, tier)
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
            checksum = self._sha256(tmp) if compute_checksum else None
            size = tmp.stat().st_size
            os.replace(tmp, location.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

        record = ArtifactRecord(
            name=name,
            size_bytes=size,
            created_at=self._utcnow(),
            created_by=self._username(),
            checksum=checksum,
            source=source,
            asset_uri=asset_uri or location.uri,
            metadata=dict(metadata or {}),
        )
        self._record(location, record)

    def ingest(
        self,
        source_path: Path | str,
        name: str,
        run_id: str,
        move: bool = False,
        metadata: Mapping[str, Any] | None = None,
        compute_checksum: bool = False,
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
            Run identifier.
        move : bool, optional
            Remove ``source_path`` after a successful copy. Default ``False``.
        metadata : Mapping of str to Any or None, optional
            Descriptive metadata recorded in the manifest.
        compute_checksum : bool, optional
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
            compute_checksum=compute_checksum,
            overwrite=overwrite,
        ) as tmp:
            shutil.copy2(src, tmp)

        if move:
            src.unlink(missing_ok=True)
        return self.locate(name, run_id, Tier.USER)

    def promote(
        self,
        name: str,
        run_id: str,
        overwrite: bool = False,
    ) -> Location:
        """Copy a user-tier artifact into the shared tier.

        Promotion copies rather than moves, so the user's local copy remains
        valid and independently deletable. The manifest record is carried
        across and stamped with a promotion timestamp.

        Parameters
        ----------
        name : str
            Artifact filename.
        run_id : str
            Run identifier.
        overwrite : bool, optional
            Whether to replace an existing shared artifact. Default ``False``,
            since promoted data is treated as immutable.

        Returns
        -------
        Location
            Shared-tier location of the promoted artifact.

        Raises
        ------
        ArtifactNotFoundError
            If the artifact is absent from the user tier.
        ArtifactExistsError
            If it already exists in the shared tier and ``overwrite`` is
            ``False``.
        """
        user = self.locate(name, run_id, Tier.USER)
        if not user.exists:
            raise ArtifactNotFoundError(
                f"cannot promote {name!r}: absent from user tier at {user.path}"
            )
        shared = self.locate(name, run_id, Tier.SHARED)
        if shared.exists and not overwrite:
            raise ArtifactExistsError(
                f"{shared.path} already exists; pass overwrite=True to replace"
            )

        existing = self.read_manifest(run_id, Tier.USER).artifacts.get(name)
        with self.stage(
            name,
            run_id,
            tier=Tier.SHARED,
            source=str(user.path),
            asset_uri=shared.uri,
            metadata=dict(existing.metadata) if existing else None,
            overwrite=True,
        ) as tmp:
            shutil.copy2(user.path, tmp)

        self._stamp_promoted(run_id)
        return shared

    # ------------------------------------------------------------------
    # Manifests
    # ------------------------------------------------------------------

    def manifest_path(self, run_id: str, tier: Tier) -> Path:
        """Return the manifest path for a run directory.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier to look up.

        Returns
        -------
        Path
            Path to the sidecar manifest, which may not exist.
        """
        self._validate_component(run_id, "run_id")
        root = self.root_for(tier)
        path = root / run_id / MANIFEST_NAME
        self._assert_contained(path, root)
        return path

    def read_manifest(self, run_id: str, tier: Tier) -> Manifest:
        """Read a run manifest, returning an empty one if absent.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier to read from.

        Returns
        -------
        Manifest
            Parsed manifest, or an empty manifest when the sidecar is missing
            or unreadable.
        """
        path = self.manifest_path(run_id, tier)
        if not path.is_file():
            return Manifest(run_id=run_id, tier=tier)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return Manifest(run_id=run_id, tier=tier)
        return Manifest.from_dict(payload)

    def write_manifest(self, manifest: Manifest) -> Path:
        """Atomically write a manifest to its run directory.

        Parameters
        ----------
        manifest : Manifest
            Manifest to persist.

        Returns
        -------
        Path
            Path the manifest was written to.
        """
        path = self.manifest_path(manifest.run_id, manifest.tier)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{os.getpid()}{_TMP_SUFFIX}")
        tmp.write_text(
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp, path)
        return path

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def list_runs(self, tier: Tier) -> list[str]:
        """List run identifiers present in a tier.

        Parameters
        ----------
        tier : Tier
            Tier to enumerate.

        Returns
        -------
        list of str
            Sorted run identifiers, derived from directories on disk rather
            than from any index, so the result cannot drift from reality.
        """
        root = self.root_for(tier)
        if not root.is_dir():
            return []
        return sorted(
            p.name for p in root.iterdir() if p.is_dir() and not p.is_symlink()
        )

    def list_artifacts(self, run_id: str, tier: Tier) -> list[Location]:
        """List artifacts physically present in a run directory.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier to enumerate.

        Returns
        -------
        list of Location
            Sorted locations of regular files, excluding the manifest and any
            in-flight temporary files.
        """
        self._validate_component(run_id, "run_id")
        directory = self.root_for(tier) / run_id
        if not directory.is_dir():
            return []
        return [
            self.locate(p.name, run_id, tier)
            for p in sorted(directory.iterdir())
            if p.is_file() and not self._is_reserved(p.name)
        ]

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
            ``True`` for the sidecar manifest, its lock file, in-flight
            temporary files, and dotfiles.
        """
        return (
            filename == MANIFEST_NAME
            or filename.startswith(".")
            or filename.endswith((_TMP_SUFFIX, _LOCK_SUFFIX))
        )

    def describe(self, run_id: str, tier: Tier) -> dict[str, ArtifactRecord]:
        """Return manifest records for artifacts that still exist on disk.

        Reconciles the manifest against the filesystem, dropping records whose
        files have been purged or deleted.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier to inspect.

        Returns
        -------
        dict of str to ArtifactRecord
            Records keyed by artifact name, filtered to present files.
        """
        present = {loc.name for loc in self.list_artifacts(run_id, tier)}
        manifest = self.read_manifest(run_id, tier)
        return {n: r for n, r in manifest.artifacts.items() if n in present}

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def refresh_view(
        self,
        run_id: str,
        view_dir: Path | str | None = None,
        prefer_local: bool = False,
    ) -> dict[str, Path]:
        """Rebuild a directory of symlinks pointing at a run's artifacts.

        The view is regenerated from current filesystem truth rather than
        maintained incrementally, so it self-heals after promotion, scratch
        purges, and user deletions. Links resolve through :meth:`resolve`,
        preferring the durable shared copy.

        Parameters
        ----------
        run_id : str
            Run identifier.
        view_dir : Path or str or None, optional
            Directory to build. Defaults to ``view_root / run_id``.
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

        names = {loc.name for loc in self.list_artifacts(run_id, Tier.SHARED)}
        names |= {loc.name for loc in self.list_artifacts(run_id, Tier.USER)}

        linked: dict[str, Path] = {}
        for name in sorted(names):
            resolved = self.resolve(name, run_id, prefer_local=prefer_local)
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
            Default ``True``.

        Returns
        -------
        bool
            Whether a directory was removed.

        Raises
        ------
        ArtifactNotFoundError
            If the run is absent and ``missing_ok`` is ``False``.
        """
        return self._delete_run(run_id, Tier.USER, missing_ok=missing_ok)

    def delete_shared_run(
        self,
        run_id: str,
        confirm: bool = False,
        missing_ok: bool = True,
    ) -> bool:
        """Delete a run directory from the shared tier.

        Deliberately a separate method from :meth:`delete_user_run`, and
        guarded by an explicit flag, because shared data may be relied upon by
        other users.

        Parameters
        ----------
        run_id : str
            Run identifier.
        confirm : bool, optional
            Must be ``True`` for the deletion to proceed. Default ``False``.
        missing_ok : bool, optional
            See :meth:`delete_user_run`.

        Returns
        -------
        bool
            Whether a directory was removed.

        Raises
        ------
        PermissionError
            If ``confirm`` is not ``True``.
        ArtifactNotFoundError
            If the run is absent and ``missing_ok`` is ``False``.
        """
        if not confirm:
            raise PermissionError(
                f"refusing to delete shared run {run_id!r}: pass confirm=True"
            )
        return self._delete_run(run_id, Tier.SHARED, missing_ok=missing_ok)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _delete_run(self, run_id: str, tier: Tier, missing_ok: bool) -> bool:
        """Remove a run directory after verifying it is safe to do so.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier to delete from.
        missing_ok : bool
            Whether absence is tolerated.

        Returns
        -------
        bool
            Whether a directory was removed.

        Raises
        ------
        UnsafePathError
            If the target is a symlink or escapes the tier root, which would
            allow a recursive delete to walk outside managed storage.
        ArtifactNotFoundError
            If absent and ``missing_ok`` is ``False``.
        """
        self._validate_component(run_id, "run_id")
        root = self.root_for(tier)
        directory = root / run_id
        if directory.is_symlink():
            raise UnsafePathError(f"refusing to delete symlinked run dir: {directory}")
        if not directory.exists():
            if missing_ok:
                return False
            raise ArtifactNotFoundError(
                f"run {run_id!r} not present in {tier.value} tier"
            )
        if not directory.is_dir():
            raise UnsafePathError(f"not a directory: {directory}")
        self._assert_contained(directory, root)
        if directory.resolve() == root:
            raise UnsafePathError(f"refusing to delete tier root: {root}")
        shutil.rmtree(directory)
        return True

    def _record(self, location: Location, record: ArtifactRecord) -> None:
        """Insert a record into a run manifest under an exclusive lock.

        Parameters
        ----------
        location : Location
            Committed location the record describes.
        record : ArtifactRecord
            Record to insert.
        """
        with self._manifest_lock(location.run_id, location.tier):
            manifest = self.read_manifest(location.run_id, location.tier)
            artifacts = dict(manifest.artifacts)
            artifacts[record.name] = record
            self.write_manifest(replace(manifest, artifacts=artifacts))

    def _stamp_promoted(self, run_id: str) -> None:
        """Set ``promoted_at`` on the shared manifest for a run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        """
        with self._manifest_lock(run_id, Tier.SHARED):
            manifest = self.read_manifest(run_id, Tier.SHARED)
            self.write_manifest(replace(manifest, promoted_at=self._utcnow()))

    @contextmanager
    def _manifest_lock(self, run_id: str, tier: Tier) -> Iterator[None]:
        """Hold an exclusive advisory lock over a run's manifest.

        Parameters
        ----------
        run_id : str
            Run identifier.
        tier : Tier
            Tier whose manifest is being modified.

        Yields
        ------
        None
            Control is yielded with the lock held.
        """
        lock_path = self.manifest_path(run_id, tier).with_suffix(_LOCK_SUFFIX)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o664)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

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
    def _sha256(path: Path) -> str:
        """Compute the SHA-256 digest of a file.

        Parameters
        ----------
        path : Path
            File to digest.

        Returns
        -------
        str
            Hex-encoded digest.
        """
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_CHECKSUM_CHUNK), b""):
                digest.update(chunk)
        return digest.hexdigest()

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
            f"shared_root={str(self.shared_root)!r})"
        )
