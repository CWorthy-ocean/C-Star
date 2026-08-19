import typing as t

from cstar.base.log import get_logger
from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.artifact_cache import ArtifactCache

log = get_logger(__name__)
"""Module logger."""

_cache: ArtifactCache | None = None


CACHE_DIR: t.Final[str] = "artifacts"
SHARED_DIR: t.Final[str] = "shared-artifacts"


def get_artifact_cache() -> ArtifactCache:
    global _cache

    if _cache is not None:
        return _cache

    user_root = DirectoryManager().cache_home()
    shared_root = DirectoryManager().shared_cache_home()

    _cache = ArtifactCache(user_root, shared_root, create_roots=True)
    return _cache
