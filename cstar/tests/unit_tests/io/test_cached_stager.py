import typing as t
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

import cstar
from cstar.io.source_data import SourceData
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
    stager: CRRS = t.cast(CRRS, source_data.stager)
    cached_data_path = stager._get_cache_path()

    files_1 = {x.resolve().as_posix() for x in staged_data.path.iterdir()}
    files_2 = {x.resolve().as_posix() for x in cached_data_path.iterdir()}

    # confirm the repository is retrieved.
    assert files_1
    assert files_2

    # confirm the cache and target files do not resolve and overlap
    assert not files_1.intersection(files_2)


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager_reuse(tmp_path: Path, mocker: MockerFixture) -> None:
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

    # set up spies on key functions to be called
    spy_saver: MagicMock = mocker.spy(source_data_2.stager.source.retriever, "save")
    spy_update_check: MagicMock = mocker.spy(
        cstar.io.staged_data, "_check_local_repo_changed_from_remote"
    )

    # call the operation in question
    staged_data_2 = source_data_2.stage(stage_path_2)

    # check that the repository hasn't changed, meaning we can rely on cache
    spy_update_check.assert_called_once()
    assert spy_update_check.spy_return_list[0] is False

    # check that .save() was not called again from the second sourcedata
    spy_saver.assert_not_called()

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


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager_restage(tmp_path: Path, mocker: MockerFixture) -> None:
    """Verify that the cached stager performs a fresh stage operation
    if the local cache is stale compared to remote.

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

    # set up spies on key functions to be called
    mock_saver: MagicMock = MagicMock(source_data_2.stager.source.retriever.save)
    spy_saver: MagicMock = mocker.spy(source_data_2.stager.source.retriever, "save")

    mock_update_check: MagicMock = MagicMock(
        cstar.io.staged_data._check_local_repo_changed_from_remote, return_value=True
    )

    with mock.patch(
        "cstar.io.staged_data._check_local_repo_changed_from_remote", mock_update_check
    ):
        # mock.patch("cstar.io.retriever.RemoteRepositoryRetriever._save", mock_saver)):
        # call the operation in question
        staged_data_2 = source_data_2.stage(stage_path_2)

        # check that mock claims the repo was changed
        mock_update_check.assert_called_once()

        spy_saver.assert_called_once()
        # mock_saver.assert_called_once()
