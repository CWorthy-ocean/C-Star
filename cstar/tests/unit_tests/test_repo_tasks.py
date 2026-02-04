import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.io.source_data import SourceData


@pytest.fixture
def mock_home_dir(tmp_path: Path) -> t.Generator[t.Callable[[], Path], None, None]:
    home_dir = tmp_path / "test-asset-cache"
    home_dir.mkdir(parents=True, exist_ok=True)

    with mock.patch(
        "cstar.base.utils.get_home_dir", return_value=home_dir
    ) as mock_get_home_dir:
        yield mock_get_home_dir


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager(tmp_path: Path) -> None:
    source_data = SourceData("https://github.com/CWorthy-ocean/ucla-roms")
    stage_path = tmp_path / "my-roms"

    staged_data = source_data.stage(stage_path)
    assert staged_data is not None
