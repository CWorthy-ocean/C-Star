import os
import typing as t
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest

from cstar.orchestration.artifact_cache import ArtifactCache


@pytest.fixture
def mock_artifact_cache_env(tmp_path: Path) -> Generator[dict[str, str]]:
    mock_env = {
        "USER": "mockuid",
        "PROJECT": str(tmp_path / "project123"),
        "SCRATCH": str(tmp_path / "scratch"),
    }

    with mock.patch.dict(os.environ, mock_env):
        yield mock_env


@pytest.fixture
def cache(tmp_path: Path) -> Generator[ArtifactCache]:
    CACHE_DIR: t.Final[str] = "test-artifacts"
    SHARED_DIR: t.Final[str] = "test-shared-artifacts"

    user_dir = tmp_path / CACHE_DIR
    group_dir = tmp_path / SHARED_DIR

    c = ArtifactCache(user_dir, group_dir)
    with mock.patch("cstar.io.utils._cache", c):
        yield c
