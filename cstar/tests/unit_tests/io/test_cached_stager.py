import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.io.source_data import SourceData
from cstar.io.stager import CachedRemoteRepositoryStager as CRRS


@pytest.fixture
def mock_home_dir(tmp_path: Path) -> t.Generator[t.Callable[[], Path], None, None]:
    """Fixture that replaces the default home directory with a temporary
    home directory.
    """
    home_dir = tmp_path / "test-asset-cache"
    home_dir.mkdir(parents=True, exist_ok=True)

    with mock.patch(
        "cstar.base.utils.get_home_dir", return_value=home_dir
    ) as mock_get_home_dir:
        yield mock_get_home_dir


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager(tmp_path: Path) -> None:
    """Verify that the cached stager retrieves and caches the remote repo.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    """
    source_data = SourceData("https://github.com/CWorthy-ocean/c-star")
    stage_path = tmp_path / "my-roms"

    staged_data = source_data.stage(stage_path)
    stager: CRRS = t.cast(CRRS, source_data.stager)
    cached_data_path = stager._get_cache_path()

    # confirm the repository is retrieved.
    files_1 = {x.resolve().as_posix() for x in staged_data.path.iterdir()}
    files_2 = {x.resolve().as_posix() for x in cached_data_path.iterdir()}

    # confirm cache & target dir both have files
    assert files_1
    assert files_2

    # confirm the cache and target
    assert not files_1.intersection(files_2)


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager_reuse(tmp_path: Path) -> None:
    """Verify that the cached stager performs a single clone and
    re-uses the cached copy on repeat requests for the repository.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    """
    repo_uri = "https://github.com/CWorthy-ocean/c-star"

    source_data_1 = SourceData(repo_uri)
    stage_path_1 = tmp_path / "my-roms-1"
    staged_data_1 = source_data_1.stage(stage_path_1)

    source_data_2 = SourceData(repo_uri)
    stage_path_2 = tmp_path / "my-roms-2"

    fake_pull = mock.MagicMock()
    fake_clone = mock.MagicMock()

    with (
        mock.patch("cstar.io.retriever._pull", new=fake_pull) as mock_pull,
        mock.patch("cstar.io.retriever._clone", new=fake_clone) as mock_clone,
    ):
        staged_data_2 = source_data_2.stage(stage_path_2)

    # confirm the local copy is refreshed before use without cloning
    mock_clone.assert_not_called()

    # confirm the cached copy is updated before use
    mock_pull.assert_called_once()

    # confirm both targets contain the same files.
    files_1 = {
        x.resolve().relative_to(staged_data_1.path.resolve())
        for x in staged_data_1.path.iterdir()
    }
    files_2 = {
        x.resolve().relative_to(staged_data_2.path.resolve())
        for x in staged_data_2.path.iterdir()
    }

    assert files_1 == files_2
