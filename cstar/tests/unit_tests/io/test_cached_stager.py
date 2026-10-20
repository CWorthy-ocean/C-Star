import fcntl
import subprocess
import threading
import time
import typing as t
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import pytest

from cstar.io import retriever
from cstar.io.source_data import SourceData

if t.TYPE_CHECKING:
    from cstar.io.stager import CachedRemoteRepositoryStager as CRRS


@pytest.fixture
def mock_home_dir(tmp_path: Path) -> Generator[Callable[[], Path], None, None]:
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


@pytest.fixture
def cache_home(tmp_path: Path) -> Generator[Path, None, None]:
    """Point the repository cache at an empty temporary directory."""
    cache_dir = tmp_path / "cache-home"
    with mock.patch(
        "cstar.execution.file_system.DirectoryManager.cache_home",
        return_value=cache_dir,
    ):
        yield cache_dir


@pytest.fixture
def remote_repo_uri(tmp_path: Path) -> str:
    """Return a remote-style (`file://localhost/...`) URI to a local git repository.

    It classifies as a remote repository, so the cached stager runs real git
    commands against it without reaching the network.
    """
    repo = tmp_path / "upstream" / "c-star"
    repo.mkdir(parents=True)
    (repo / "README.md").write_text("upstream\n")
    for cmd in (
        ["init", "-q", "-b", "main"],
        ["add", "README.md"],
        [
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "init",
        ],
    ):
        subprocess.run(["git", "-C", str(repo), *cmd], check=True)
    return f"file://localhost{repo.as_posix()}"


@pytest.mark.usefixtures("mock_home_dir")
def test_cached_stager(tmp_path: Path, remote_repo_uri: str) -> None:
    """Verify that the cached stager retrieves and caches the remote repo.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    remote_repo_uri : str
        URI of the repository to stage
    """
    source_data = SourceData(remote_repo_uri)
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
def test_cached_stager_reuse(tmp_path: Path, remote_repo_uri: str) -> None:
    """Verify that the cached stager performs a single clone and
    re-uses the cached copy on repeat requests for the repository.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    remote_repo_uri : str
        URI of the repository to stage
    """
    source_data_1 = SourceData(remote_repo_uri)
    stage_path_1 = tmp_path / "my-roms-1"
    staged_data_1 = source_data_1.stage(stage_path_1)

    source_data_2 = SourceData(remote_repo_uri, identifier="main")
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
def test_cached_stager_refresh_failure(tmp_path: Path, remote_repo_uri: str) -> None:
    """Verify that the cached stager re-clones a repository
    if an attempt to refresh fails.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to store repository clones
    remote_repo_uri : str
        URI of the repository to stage
    """
    cache_path = tmp_path / ".cache" / "cached-c-star"
    cached_junk = cache_path / ".git"
    cached_junk.mkdir(parents=True)
    (cached_junk / "garbage").touch()

    source_data_1 = SourceData(remote_repo_uri)
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


def test_cached_stager_concurrent_stages_clone_once(
    tmp_path: Path, remote_repo_uri: str, cache_home: Path
) -> None:
    """Verify that two steps staging one repository at once take turns on the cache.

    The clone is slowed down so that, without the cache lock, the second stage would
    find the half-cloned cache and pull or remove it under the first.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to stage the repository into
    remote_repo_uri : str
        URI of the repository to stage
    cache_home : Path
        The temporary cache directory
    """
    real_clone = retriever._clone

    def slow_clone(*args: t.Any, **kwargs: t.Any) -> None:
        time.sleep(0.3)
        real_clone(*args, **kwargs)

    sources = [SourceData(remote_repo_uri), SourceData(remote_repo_uri)]
    targets = [tmp_path / "step-1", tmp_path / "step-2"]
    barrier = threading.Barrier(len(sources))

    def stage(source: SourceData, target: Path) -> Path:
        barrier.wait()
        return source.stage(target).path

    with (
        mock.patch("cstar.io.retriever._clone", side_effect=slow_clone) as spy_clone,
        ThreadPoolExecutor(max_workers=len(sources)) as pool,
    ):
        futures = [pool.submit(stage, s, d) for s, d in zip(sources, targets)]
        staged_paths = [f.result() for f in futures]

    spy_clone.assert_called_once()
    assert staged_paths == targets
    for target in targets:
        assert (target / "README.md").read_text() == "upstream\n"


def test_cached_stager_locks_beside_the_cache(
    tmp_path: Path, remote_repo_uri: str, cache_home: Path
) -> None:
    """Verify that the cache lock file sits beside the cache and is held for the copy.

    Parameters
    ----------
    tmp_path : Path
        A temporary path to stage the repository into
    remote_repo_uri : str
        URI of the repository to stage
    cache_home : Path
        The temporary cache directory
    """
    source_data = SourceData(remote_repo_uri)
    stager: CRRS = t.cast("CRRS", source_data.stager)
    cache_path = stager._get_cache_path()
    lock_path = cache_path.parent / f"{cache_path.name}.lock"
    held_during_copy: list[bool] = []

    def probe_then_copy(cmd: str) -> str:
        with lock_path.open("a") as probe:
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                held_during_copy.append(True)
            else:
                fcntl.flock(probe, fcntl.LOCK_UN)
                held_during_copy.append(False)
        subprocess.run(cmd, shell=True, check=True)
        return ""

    with mock.patch("cstar.io.stager._run_cmd", side_effect=probe_then_copy):
        source_data.stage(tmp_path / "my-roms")

    assert held_during_copy == [True]
    assert lock_path.is_file()
    assert lock_path.parent == cache_home
    assert not (cache_path / lock_path.name).exists()
    assert (tmp_path / "my-roms" / "README.md").is_file()
