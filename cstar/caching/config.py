"""Resolution of artifact-cache storage roots and global switches.

The personal tier deliberately does **not** default to
`DirectoryManager.cache_home()`: that resolves to XDG `~/.cache`, which on HPC
systems lives on the quota-limited home filesystem. Large generated artifacts
belong on SCRATCH, so scratch detection (via `CSTAR_SCRATCH_DIRS`) takes
precedence, with the XDG cache home as the laptop fallback. The
`artifact-cache` subdirectory keeps this cache disjoint from the git
repository cache used by `CachedRemoteRepositoryStager`.
"""

import os
from pathlib import Path

from cstar.base.env import (
    ENV_CSTAR_CACHE_DISABLE,
    ENV_CSTAR_CACHE_GROUP_ROOT,
    ENV_CSTAR_CACHE_PERSONAL_ROOT,
    hpc_data_directory,
)
from cstar.base.feature import is_flag_enabled
from cstar.execution.file_system import DirectoryManager

ARTIFACT_CACHE_DIRNAME = "artifact-cache"
"""Subdirectory name for the artifact cache under a storage root."""


def personal_cache_root() -> Path:
    """Resolve the root directory of the personal (ephemeral) cache tier.

    Precedence: explicit `CSTAR_CACHE_PERSONAL_ROOT` override, then
    `<scratch>/cstar/artifact-cache` when a scratch filesystem is detected,
    then `<xdg-cache-home>/cstar/artifact-cache`.

    Returns
    -------
    Path
    """
    if override := os.getenv(ENV_CSTAR_CACHE_PERSONAL_ROOT, ""):
        return Path(override).expanduser().resolve()

    if scratch := hpc_data_directory():
        return (Path(scratch) / "cstar" / ARTIFACT_CACHE_DIRNAME).resolve()

    return DirectoryManager.cache_home() / ARTIFACT_CACHE_DIRNAME


def group_cache_root() -> Path | None:
    """Resolve the root directory of the group (durable, shared) cache tier.

    The `CSTAR_CACHE_GROUP_ROOT` value is used verbatim; group roots are
    provisioned explicitly (typically on PROJECT-class storage with group
    read/write permissions). When unset, the group tier is disabled.

    Returns
    -------
    Path | None
    """
    if configured := os.getenv(ENV_CSTAR_CACHE_GROUP_ROOT, ""):
        return Path(configured).expanduser().resolve()

    return None


def caching_disabled() -> bool:
    """Return `True` when the artifact cache is bypassed (`--no-cache`).

    Returns
    -------
    bool
    """
    return is_flag_enabled(ENV_CSTAR_CACHE_DISABLE)
