"""On-disk cache stores and the tiered cache manager.

Layout of a store root::

    <root>/
      entries/
        <function-slug>/
          <key>/
            manifest.yaml
            payload/
              <files...>
      staging/
        <key>.<uuid>/          # same filesystem as entries/ => atomic rename

Writers populate a unique staging directory and commit it with an atomic
`Path.replace` into the entry location. When two processes compute the same
key concurrently, the first rename wins; the loser discards its staging
directory and adopts the winner's entry. Entries are validated on every read
(manifest deserializes, every recorded file exists at its recorded size), so
partially deleted or corrupt entries degrade to cache misses rather than
serving bad data.
"""

import getpass
import shutil
import typing as t
import uuid
from collections.abc import Iterator
from pathlib import Path

from cstar.base.log import LoggingMixin
from cstar.base.utils import slugify, utc_now
from cstar.caching.config import group_cache_root, personal_cache_root
from cstar.caching.models import (
    MANIFEST_FILENAME,
    PAYLOAD_DIRNAME,
    CacheEntry,
    CacheManifest,
    CacheTier,
)
from cstar.orchestration.serialization import serialize, try_deserialize

MIN_KEY_PREFIX_LENGTH: t.Final[int] = 6
"""Minimum number of key characters accepted when resolving entries by prefix."""


class CacheError(Exception):
    """Base class for artifact-cache errors."""


class CacheConfigurationError(CacheError):
    """Raised when a cache operation requires configuration that is absent."""


class CacheCommitError(CacheError):
    """Raised when a staged cache entry cannot be committed."""


class CacheEntryNotFoundError(CacheError):
    """Raised when no cache entry matches a user-supplied reference."""


class AmbiguousCacheKeyError(CacheError):
    """Raised when a user-supplied reference matches multiple cache entries."""


def function_slug(function_identity: str) -> str:
    """Return the directory-safe slug for a module-qualified function name."""
    return slugify(function_identity)


class CacheStore(LoggingMixin):
    """A single cache tier rooted at a directory."""

    _ENTRIES_DIRNAME: t.Final[str] = "entries"
    _STAGING_DIRNAME: t.Final[str] = "staging"

    def __init__(self, root: Path, tier: CacheTier) -> None:
        """Initialize the store.

        Parameters
        ----------
        root : Path
            The root directory of this cache tier.
        tier : CacheTier
            The tier this store represents.
        """
        self.root = root.expanduser().resolve()
        self.tier = tier

    @property
    def entries_dir(self) -> Path:
        """The directory containing committed cache entries."""
        return self.root / self._ENTRIES_DIRNAME

    @property
    def staging_dir(self) -> Path:
        """The directory containing in-progress staging directories."""
        return self.root / self._STAGING_DIRNAME

    def entry_dir(self, slug: str, key: str) -> Path:
        """The directory a committed entry for (function, key) occupies."""
        return self.entries_dir / slug / key

    def find(self, slug: str, key: str) -> CacheEntry | None:
        """Return the validated entry for (function, key), or `None`.

        Parameters
        ----------
        slug : str
            The function slug (see `function_slug`).
        key : str
            The full cache key.

        Returns
        -------
        CacheEntry | None
        """
        return self._load_entry(self.entry_dir(slug, key))

    def begin_staging(self, key: str) -> Path:
        """Create and return a unique staging directory for a pending entry.

        The staging directory contains an empty `payload/` subdirectory that
        the producing function writes into.

        Returns
        -------
        Path
        """
        staging = self.staging_dir / f"{key}.{uuid.uuid4().hex}"
        (staging / PAYLOAD_DIRNAME).mkdir(parents=True)
        return staging

    def commit(self, staging: Path, manifest: CacheManifest) -> CacheEntry:
        """Atomically publish a staged payload as a committed entry.

        Writes the manifest into the staging directory, then renames the
        staging directory into its entry location. If a concurrent writer
        already committed a valid entry for the same key, the staged copy is
        discarded and the existing entry is returned.

        Parameters
        ----------
        staging : Path
            A directory produced by `begin_staging`, with a populated payload.
        manifest : CacheManifest
            The manifest describing the staged payload.

        Returns
        -------
        CacheEntry

        Raises
        ------
        CacheCommitError
            If the entry cannot be committed and no valid entry exists.
        """
        serialize(staging / MANIFEST_FILENAME, manifest)

        target = self.entry_dir(function_slug(manifest.function), manifest.key)
        target.parent.mkdir(parents=True, exist_ok=True)

        try:
            staging.replace(target)
        except OSError:
            if existing := self._load_entry(target):
                msg = (
                    f"A concurrent writer committed cache entry {manifest.key[:12]!r} "
                    f"for {manifest.function!r} first; discarding staged copy."
                )
                self.log.info(msg)
                shutil.rmtree(staging, ignore_errors=True)
                return existing

            # The slot is occupied by an invalid/partial entry: replace it.
            msg = f"Replacing invalid cache entry at {target}"
            self.log.warning(msg)
            shutil.rmtree(target, ignore_errors=True)
            try:
                staging.replace(target)
            except OSError as ex:
                # a concurrent writer may have landed a valid entry between
                # the rmtree and this retry: adopt it rather than failing a
                # call whose function already ran to completion
                shutil.rmtree(staging, ignore_errors=True)
                if existing := self._load_entry(target):
                    return existing
                msg = (
                    f"Unable to commit cache entry for key {manifest.key!r} at {target}"
                )
                raise CacheCommitError(msg) from ex

        entry = self._load_entry(target)
        if entry is None:
            msg = f"Cache entry at {target} failed validation immediately after commit"
            raise CacheCommitError(msg)

        return entry

    def iter_entries(self) -> Iterator[CacheEntry]:
        """Yield every valid entry in this store."""
        if not self.entries_dir.is_dir():
            return

        for manifest_path in sorted(
            self.entries_dir.glob(f"*/*/{MANIFEST_FILENAME}"),
        ):
            if entry := self._load_entry(manifest_path.parent):
                yield entry

    def remove(self, entry: CacheEntry) -> None:
        """Delete a committed entry from this store."""
        if entry.tier != self.tier:
            msg = f"Refusing to remove {entry.tier} entry via the {self.tier} store"
            raise CacheError(msg)
        shutil.rmtree(entry.entry_dir)

    def purge(self) -> None:
        """Delete all entries and staging leftovers in this store."""
        for directory in (self.entries_dir, self.staging_dir):
            if directory.is_dir():
                shutil.rmtree(directory)

    def _load_entry(self, entry_dir: Path) -> CacheEntry | None:
        """Load and validate the entry at a directory, or return `None`.

        An entry is valid when its manifest deserializes and every recorded
        file exists in the payload at its recorded size.
        """
        manifest_path = entry_dir / MANIFEST_FILENAME
        if not manifest_path.is_file():
            return None

        manifest = try_deserialize(manifest_path, CacheManifest)
        if manifest is None:
            msg = f"Ignoring cache entry with unreadable manifest: {entry_dir}"
            self.log.warning(msg)
            return None

        payload_dir = entry_dir / PAYLOAD_DIRNAME
        for record in manifest.files:
            file_path = payload_dir / record.relpath
            if not file_path.is_file():
                msg = (
                    f"Ignoring cache entry missing file {record.relpath!r}: {entry_dir}"
                )
                self.log.warning(msg)
                return None
            if file_path.stat().st_size != record.size_bytes:
                msg = (
                    f"Ignoring cache entry with size mismatch for {record.relpath!r} "
                    f"(expected {record.size_bytes}): {entry_dir}"
                )
                self.log.warning(msg)
                return None

        return CacheEntry(tier=self.tier, entry_dir=entry_dir, manifest=manifest)


class CacheManager(LoggingMixin):
    """Tiered lookup and lifecycle management across cache stores."""

    def __init__(self, personal: CacheStore, group: CacheStore | None = None) -> None:
        """Initialize the manager.

        Parameters
        ----------
        personal : CacheStore
            The per-user ephemeral store; the write target for new entries.
        group : CacheStore | None
            The shared durable store, when configured.
        """
        self.personal = personal
        self.group = group

    @classmethod
    def from_env(cls) -> "CacheManager":
        """Build a manager from the environment-resolved cache roots.

        Returns
        -------
        CacheManager
        """
        personal = CacheStore(personal_cache_root(), CacheTier.personal)
        group_root = group_cache_root()
        group = CacheStore(group_root, CacheTier.group) if group_root else None
        return cls(personal, group)

    def lookup(self, slug: str, key: str) -> CacheEntry | None:
        """Find an entry for (function, key): group tier first, then personal.

        Group-tier failures (e.g. permission or filesystem errors) degrade to
        a personal-tier lookup with a warning rather than failing the call.

        Returns
        -------
        CacheEntry | None
        """
        if self.group is not None:
            try:
                if entry := self.group.find(slug, key):
                    return entry
            except OSError as ex:
                msg = (
                    f"Group cache lookup failed ({ex}); falling back to personal cache"
                )
                self.log.warning(msg)

        return self.personal.find(slug, key)

    def iter_all(self) -> Iterator[CacheEntry]:
        """Yield every valid entry: group tier first, then personal."""
        if self.group is not None:
            yield from self.group.iter_entries()
        yield from self.personal.iter_entries()

    def resolve(self, reference: str, tier: CacheTier | None = None) -> CacheEntry:
        """Resolve a user-supplied reference to a single entry.

        A reference matches an entry when it is a prefix (at least
        `MIN_KEY_PREFIX_LENGTH` characters) of the entry key, or exactly
        equals the entry label. When both tiers hold the same key, the
        group-tier entry is preferred unless a tier filter is supplied.

        Parameters
        ----------
        reference : str
            A key prefix or exact label.
        tier : CacheTier | None
            Restrict matching to a single tier.

        Returns
        -------
        CacheEntry

        Raises
        ------
        CacheEntryNotFoundError
            When nothing matches.
        AmbiguousCacheKeyError
            When the reference matches more than one distinct key.
        """
        if not reference:
            msg = "An empty cache entry reference cannot be resolved"
            raise CacheEntryNotFoundError(msg)

        matches: dict[str, CacheEntry] = {}
        for entry in self.iter_all():
            if tier is not None and entry.tier != tier:
                continue

            key_match = len(
                reference
            ) >= MIN_KEY_PREFIX_LENGTH and entry.manifest.key.startswith(reference)
            label_match = (
                bool(entry.manifest.label) and entry.manifest.label == reference
            )
            if not (key_match or label_match):
                continue

            # iter_all yields group entries first; keep the first per key
            matches.setdefault(entry.manifest.key, entry)

        if not matches:
            msg = f"No cache entry matches {reference!r}"
            raise CacheEntryNotFoundError(msg)

        if len(matches) > 1:
            candidates = ", ".join(
                f"{entry.manifest.key[:12]} ({entry.manifest.function})"
                for entry in matches.values()
            )
            msg = f"Reference {reference!r} is ambiguous; candidates: {candidates}"
            raise AmbiguousCacheKeyError(msg)

        return next(iter(matches.values()))

    def promote(self, entry: CacheEntry, *, delete_source: bool = False) -> CacheEntry:
        """Publish a personal-tier entry into the group tier.

        Copies the payload and manifest (stamped with promotion provenance)
        into group staging and commits atomically. Promotion is idempotent:
        when the group tier already holds a valid entry for the key, that
        entry is returned unchanged.

        Parameters
        ----------
        entry : CacheEntry
            The entry to promote.
        delete_source : bool
            Remove the personal-tier copy after a successful promotion.
            Defaults to `False` because deleting the source breaks symlinks
            placed into output directories by prior runs.

        Returns
        -------
        CacheEntry
            The group-tier entry.

        Raises
        ------
        CacheConfigurationError
            When no group cache is configured.
        """
        if self.group is None:
            msg = (
                "No group cache is configured; set CSTAR_CACHE_GROUP_ROOT to "
                "enable the group tier."
            )
            raise CacheConfigurationError(msg)

        if entry.tier == CacheTier.group:
            msg = f"Entry {entry.manifest.key[:12]!r} is already in the group cache"
            self.log.info(msg)
            return entry

        slug = function_slug(entry.manifest.function)
        if existing := self.group.find(slug, entry.manifest.key):
            msg = (
                f"Group cache already holds entry {entry.manifest.key[:12]!r}; "
                "promotion is a no-op."
            )
            self.log.info(msg)
            promoted = existing
        else:
            staging = self.group.begin_staging(entry.manifest.key)
            try:
                shutil.copytree(
                    entry.payload_dir,
                    staging / PAYLOAD_DIRNAME,
                    dirs_exist_ok=True,
                )
                manifest = entry.manifest.model_copy(deep=True)
                manifest.provenance.promoted_at = utc_now()
                manifest.provenance.promoted_by = getpass.getuser()
                promoted = self.group.commit(staging, manifest)
            except Exception:
                shutil.rmtree(staging, ignore_errors=True)
                raise

        if delete_source and entry.tier == CacheTier.personal:
            self.personal.remove(entry)

        return promoted


def place_symlinks(entry: CacheEntry, output_dir: Path) -> list[Path]:
    """Symlink every file of an entry into a user-facing output directory.

    Real data stays in cache storage; the output directory receives absolute
    symlinks mirroring the payload's relative structure. Existing files or
    stale symlinks at the link locations are replaced.

    Parameters
    ----------
    entry : CacheEntry
        The cache entry to expose.
    output_dir : Path
        The directory in which to place the symlinks.

    Returns
    -------
    list[Path]
        The symlink paths, in manifest order.

    Raises
    ------
    CacheError
        If the output directory lies inside the cache entry itself, or a
        link location is occupied by something a symlink cannot replace.
    """
    output_dir = output_dir.expanduser().resolve()

    # placing links inside the entry would unlink the real payload files and
    # replace them with self-referencing symlinks, destroying the entry
    entry_root = entry.entry_dir.resolve()
    if output_dir == entry_root or output_dir.is_relative_to(entry_root):
        msg = (
            f"Refusing to place cache symlinks inside the cache entry itself: "
            f"{output_dir}"
        )
        raise CacheError(msg)

    output_dir.mkdir(parents=True, exist_ok=True)

    links: list[Path] = []
    for record in entry.manifest.files:
        target = (entry.payload_dir / record.relpath).resolve()
        link = output_dir / record.relpath
        try:
            link.parent.mkdir(parents=True, exist_ok=True)
        except (FileExistsError, NotADirectoryError) as ex:
            msg = (
                f"Cannot create directory for cache symlink {link}: a file "
                "occupies part of the path."
            )
            raise CacheError(msg) from ex

        # `is_symlink` catches broken symlinks that `exists` reports as absent
        if link.is_symlink() or link.exists():
            if link.is_dir() and not link.is_symlink():
                msg = f"Cannot place cache symlink over existing directory: {link}"
                raise CacheError(msg)
            link.unlink()

        link.symlink_to(target)
        links.append(link)

    return links
