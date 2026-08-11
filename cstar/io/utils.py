import getpass
import os
import typing as t
from pathlib import Path

from cstar.execution.file_system import DirectoryManager
from cstar.orchestration.artifact_cache import ArtifactCache


def get_artifact_cache() -> ArtifactCache:
    CSTAR_DIR: t.Final[str] = "cstar"
    CACHE_DIR: t.Final[str] = "artifacts"
    username: t.Final[str] = getpass.getuser()

    # TODO: use cstar.system.manager.XxxEnvSettings?
    project_dir: t.Final[str] = os.getenv("PROJECT", "")
    scratch_dir: t.Final[str] = os.getenv("SCRATCH", "")

    default_data_dir = DirectoryManager().data_home() / CSTAR_DIR
    default_group_dir = default_data_dir / CSTAR_DIR / "share"

    user_cache_dir = Path(scratch_dir or default_data_dir) / username / CACHE_DIR
    group_cache_dir = Path(project_dir or default_group_dir) / CSTAR_DIR / CACHE_DIR

    return ArtifactCache(user_cache_dir, group_cache_dir)
