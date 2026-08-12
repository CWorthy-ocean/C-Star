import os
import typing as t
from pathlib import Path

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

    project_dir: t.Final[str] = os.getenv("PROJECT", "")
    scratch_dir: t.Final[str] = os.getenv("SCRATCH", "")

    default_data_dir = DirectoryManager().data_home()
    log.debug(f"{default_data_dir=}")
    default_group_dir = default_data_dir.parent / "shared-artifacts"
    log.debug(f"{default_group_dir=}")

    user_cache_dir = Path(scratch_dir or default_data_dir) / CACHE_DIR
    log.debug(f"{user_cache_dir=}")
    group_cache_dir = Path(project_dir or default_group_dir) / SHARED_DIR
    log.debug(f"{group_cache_dir=}")

    _cache = ArtifactCache(user_cache_dir, group_cache_dir)
    return _cache
