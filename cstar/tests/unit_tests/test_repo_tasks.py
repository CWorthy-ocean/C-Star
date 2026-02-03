from pathlib import Path

import pytest

from cstar.io.source_data import SourceData


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager(tmp_path: Path) -> None:
    source_data = SourceData("https://github.com/CWorthy-ocean/ucla-roms")
    stage_path = tmp_path / "my-roms"

    staged_data = source_data.stage(stage_path)
    assert staged_data is not None
