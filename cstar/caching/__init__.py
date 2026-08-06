"""Artifact caching for expensive file-producing operations.

Provides a two-tier cache (personal/ephemeral and group/durable) with a
decorator-based opt-in API. See `cstar.caching.decorator.cached_artifact`
for the developer-facing entry point.
"""

from cstar.caching.config import (
    caching_disabled,
    group_cache_root,
    personal_cache_root,
)
from cstar.caching.decorator import cached_artifact
from cstar.caching.keys import CacheKeyError, compute_key, file_fingerprint
from cstar.caching.models import (
    CacheEntry,
    CacheFileRecord,
    CacheHandle,
    CacheManifest,
    CacheProvenance,
    CacheTier,
    ReturnKind,
    ReturnSpec,
)
from cstar.caching.store import (
    AmbiguousCacheKeyError,
    CacheCommitError,
    CacheConfigurationError,
    CacheEntryNotFoundError,
    CacheError,
    CacheManager,
    CacheStore,
    place_symlinks,
)

__all__ = [
    "AmbiguousCacheKeyError",
    "CacheCommitError",
    "CacheConfigurationError",
    "CacheEntry",
    "CacheEntryNotFoundError",
    "CacheError",
    "CacheFileRecord",
    "CacheHandle",
    "CacheKeyError",
    "CacheManager",
    "CacheManifest",
    "CacheProvenance",
    "CacheStore",
    "CacheTier",
    "ReturnKind",
    "ReturnSpec",
    "cached_artifact",
    "caching_disabled",
    "compute_key",
    "file_fingerprint",
    "group_cache_root",
    "personal_cache_root",
    "place_symlinks",
]
