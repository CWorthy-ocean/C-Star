import typing as t
from pathlib import Path
from unittest import mock

import pytest

from cstar.io.source_data import SourceData

if t.TYPE_CHECKING:
    from cstar.io.stager import CachedRemoteRepositoryStager as CRRS


@pytest.fixture
def mock_home_dir(tmp_path: Path) -> t.Generator[t.Callable[[], Path], None, None]:
    """Fixture that replaces the default home directory with a temporary
    home directory.
    """
    state_dir = tmp_path / "test-asset-cache"
    state_dir.mkdir(parents=True, exist_ok=True)

    with mock.patch(
        "cstar.execution.file_system.DirectoryManager.state_home",
        return_value=state_dir,
    ) as mock_state_home:
        yield mock_state_home


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
    stager: CRRS = t.cast("CRRS", source_data.stager)
    cached_data_path = stager._get_cache_path()

    files_1 = {x.resolve().as_posix() for x in staged_data.path.iterdir()}
    files_2 = {x.resolve().as_posix() for x in cached_data_path.iterdir()}

    # confirm the repository is retrieved.
    assert files_1
    assert files_2

    # confirm the cache and target files do not resolve and overlap
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

    source_data_2 = SourceData(repo_uri, identifier="main")
    stage_path_2 = tmp_path / "my-roms-2"

    fake_pull = mock.MagicMock()
    fake_clone = mock.MagicMock()
    fake_checkout = mock.MagicMock()

    with (
        mock.patch("cstar.io.retriever._pull", new=fake_pull) as mock_pull,
        mock.patch("cstar.io.retriever._clone", new=fake_clone) as mock_clone,
        mock.patch("cstar.io.retriever._checkout", new=fake_checkout) as mock_checkout,
    ):
        staged_data_2 = source_data_2.stage(stage_path_2)

    # confirm the remote is not cloned
    mock_clone.assert_not_called()

    # confirm the cached copy is updated
    mock_pull.assert_called_once()

    # confirm that checkout is called
    mock_checkout.assert_called_once()

    # confirm both targets contain files.
    files_1 = {x.resolve().as_posix() for x in staged_data_1.path.iterdir()}
    files_2 = {x.resolve().as_posix() for x in staged_data_2.path.iterdir()}

    assert files_1
    assert files_2

    # confirm the cache and target files do not resolve and overlap
    assert not files_1.intersection(files_2)


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager_refresh_failure(tmp_path: Path) -> None:
    """Verify that the cached stager re-clones a repository
    if an attempt to refresh fails.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    """
    repo_uri = "https://github.com/CWorthy-ocean/c-star"

    cache_path = tmp_path / ".cache" / "https-github-com-cworthy-ocean-cstar"
    cached_junk = cache_path / ".git"
    cached_junk.mkdir(parents=True)
    (cached_junk / "garbage").touch()

    source_data_1 = SourceData(repo_uri)
    stage_path_1 = tmp_path / "my-roms-1"

    def fail_pull(*args, **kwargs) -> None:
        msg = "Simulating a failure to pull repo updates"
        raise RuntimeError(msg)

    with (
        mock.patch("cstar.io.retriever._pull", side_effect=fail_pull) as mock_refresh,
        mock.patch(
            "cstar.io.retriever.RemoteRepositoryRetriever.save", return_value=cache_path
        ) as mock_clone,
        mock.patch(
            "cstar.io.retriever.RemoteRepositoryRetriever.checkout"
        ) as mock_checkout,
        mock.patch("cstar.io.stager._run_cmd") as mock_copy_cache,
        mock.patch("cstar.io.staged_data._run_cmd"),
        mock.patch("shutil.rmtree") as mock_rmdir,
        mock.patch.object(
            source_data_1.stager, "_get_cache_path", return_value=cache_path
        ),
    ):
        _ = source_data_1.stage(stage_path_1)

    # confirm the refresh was attempted
    mock_refresh.assert_called_once()

    # confirm the cache is cleared and repo is re-cloned after refresh returns `False`
    mock_rmdir.assert_called_once_with(cache_path)
    mock_clone.assert_called_once()

    # confirm the re-cloned repo is copied from cache to target dir
    assert mock_copy_cache.call_count == 1  # cp -ar

    # confirm that checkout was called after the cache was copied
    mock_checkout.assert_called_once()
