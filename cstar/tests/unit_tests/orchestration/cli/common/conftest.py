import os
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest


@pytest.fixture
def mock_artifact_cache_env(tmp_path: Path) -> Generator[dict[str, str]]:
    mock_env = {
        "USER": "mockuid",
        "PROJECT": str(tmp_path / "project123"),
        "SCRATCH": str(tmp_path / "scratch"),
    }

    with mock.patch.dict(os.environ, mock_env):
        yield mock_env
