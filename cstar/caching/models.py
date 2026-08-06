"""Pydantic models for cache manifests, entries, and handles.

Every cache entry persists a ``manifest.yaml`` beside its payload files. The
manifest is the durable record of what was cached: the key, the key material
it was computed from, the produced files, and provenance. Entries are
therefore identifiable and inspectable after the fact without recomputing
keys or rerunning functions (see caching.md sections 3 and 7).
"""

import enum
import getpass
import importlib.metadata
import os
import socket
import typing as t
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cstar.base.env import ENV_CSTAR_RUNID
from cstar.base.utils import utc_now

MANIFEST_FILENAME: t.Final[str] = "manifest.yaml"
"""File name of the manifest within a cache entry directory."""

PAYLOAD_DIRNAME: t.Final[str] = "payload"
"""Directory name holding the cached files within a cache entry directory."""

MANIFEST_SCHEMA_VERSION: t.Final[str] = "1.0.0"
"""Schema version written into new manifests."""


def _cstar_version() -> str:
    """Return the installed cstar version, or empty-string when unavailable."""
    try:
        return importlib.metadata.version("cstar-ocean")
    except importlib.metadata.PackageNotFoundError:
        return ""


class CacheTier(enum.StrEnum):
    """The storage tiers of the artifact cache."""

    personal = enum.auto()
    """Per-user, ephemeral storage (SCRATCH on HPC systems)."""
    group = enum.auto()
    """Shared, durable storage holding approved/promoted results."""


class ReturnKind(enum.StrEnum):
    """Classification of a cached function's return value for restoration."""

    none = enum.auto()
    """Not restorable; cached executions return `None`."""
    path = enum.auto()
    """A single `Path` under the payload directory."""
    path_list = enum.auto()
    """A sequence of `Path` objects under the payload directory."""
    path_map = enum.auto()
    """A mapping of string keys to `Path` objects under the payload directory."""
    json_value = enum.auto()
    """A JSON-serializable value stored verbatim."""


class ReturnSpec(BaseModel):
    """Persisted description of how to reconstruct a function's return value."""

    # store the enum by value so manifests round-trip through yaml.safe_load
    model_config = ConfigDict(use_enum_values=True)

    kind: ReturnKind = ReturnKind.none
    """The restoration strategy."""
    relpaths: list[str] = Field(default_factory=list)
    """Payload-relative paths, for the path-based kinds."""
    map_keys: list[str] = Field(default_factory=list)
    """Mapping keys parallel to `relpaths`, for the `path_map` kind."""
    value: t.Any = None
    """The stored value, for the `json_value` kind."""


class CacheFileRecord(BaseModel):
    """A single file captured in a cache entry."""

    relpath: str
    """Path relative to the entry's payload directory."""
    size_bytes: int
    """Size of the file in bytes at capture time."""
    sha256: str = ""
    """Content digest. Unpopulated in the prototype; reserved for hardening."""


class CacheProvenance(BaseModel):
    """Provenance recorded when a cache entry is created or promoted.

    Creation fields are required (no defaults) so they always serialize:
    manifests are persisted with ``exclude_defaults=True``, and a defaulted
    field would be silently re-fabricated from the *reader's* environment on
    deserialization, corrupting provenance for shared entries.
    """

    created_at: datetime
    """When the entry was created."""
    created_by: str
    """The user that created the entry."""
    hostname: str
    """The host on which the entry was created."""
    cstar_version: str = ""
    """The installed C-Star version at creation time."""
    run_id: str = ""
    """The orchestrator run identifier, when created inside a run."""
    promoted_at: datetime | None = None
    """When the entry was promoted to the group tier, if ever."""
    promoted_by: str = ""
    """The user that promoted the entry."""

    @classmethod
    def capture(cls) -> "CacheProvenance":
        """Capture provenance from the current process environment.

        Returns
        -------
        CacheProvenance
        """
        return cls(
            created_at=utc_now(),
            created_by=getpass.getuser(),
            hostname=socket.gethostname(),
            cstar_version=_cstar_version(),
            run_id=os.getenv(ENV_CSTAR_RUNID, ""),
        )


class CacheManifest(BaseModel):
    """The durable, human-readable record persisted with every cache entry."""

    model_config = ConfigDict(extra="allow")

    # `schema_version`, `function_version`, and `provenance` are required
    # (not defaulted) so they always survive the `exclude_defaults=True`
    # manifest serialization; see the CacheProvenance docstring.
    schema_version: str
    """Version of the manifest schema (see `MANIFEST_SCHEMA_VERSION`)."""
    key: str
    """The full sha256 cache key."""
    function: str
    """Module-qualified name of the producing function."""
    function_version: str
    """The developer-managed function version used in the key."""
    label: str = ""
    """Optional human-friendly label supplied by the decorator."""
    key_material: dict[str, t.Any] = Field(default_factory=dict)
    """The tokenized inputs the key was computed from."""
    files: list[CacheFileRecord] = Field(default_factory=list)
    """The files captured in the payload directory."""
    return_spec: ReturnSpec = Field(default_factory=ReturnSpec)
    """How to reconstruct the function's return value."""
    provenance: CacheProvenance
    """Creation and promotion provenance."""


class CacheEntry(BaseModel):
    """A located, validated on-disk cache entry."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    tier: CacheTier
    """The tier the entry was found in."""
    entry_dir: Path
    """The directory containing the manifest and payload."""
    manifest: CacheManifest
    """The deserialized manifest."""

    @property
    def payload_dir(self) -> Path:
        """The directory containing the cached files."""
        return self.entry_dir / PAYLOAD_DIRNAME

    @property
    def payload_paths(self) -> list[Path]:
        """Absolute paths of the cached files, in manifest order."""
        return [self.payload_dir / record.relpath for record in self.manifest.files]

    @property
    def total_size_bytes(self) -> int:
        """Total recorded payload size in bytes."""
        return sum(record.size_bytes for record in self.manifest.files)


class CacheHandle(BaseModel):
    """The return value of a cached function call.

    Analogous to the orchestration `ProcessHandle`: a lightweight, serializable
    record exposing where outputs live and how they were obtained.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    key: str
    """The full sha256 cache key of the call."""
    function: str
    """Module-qualified name of the producing function."""
    hit: bool
    """`True` when outputs were reused from a cache entry."""
    tier: CacheTier | None = None
    """The tier that served the call; `None` when caching was bypassed."""
    paths: list[Path] = Field(default_factory=list)
    """Output files in the caller's requested output directory (symlinks when cached)."""
    payload_paths: list[Path] = Field(default_factory=list)
    """The real files inside cache storage backing `paths`."""
    created_at: datetime = Field(default_factory=utc_now)
    """When the backing entry was created (or now, when caching was bypassed)."""
    provenance: CacheProvenance | None = None
    """Provenance of the backing cache entry, when one exists."""
    result: t.Any = None
    """The restored (or fresh) return value of the wrapped function."""
