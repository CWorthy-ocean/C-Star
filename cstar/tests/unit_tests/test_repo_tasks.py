import datetime
import pytest
from cstar.base.utils import slugify
from cstar.tasks.repo import get_repo, get_cache_dir
from shutil import rmtree

from pathlib import Path
from unittest import mock
import typing as t
import os
import subprocess as sp

# PREFECT_UI_PORT: t.Final[int] = 8968
PREFECT_PORT: t.Final[int] = 8998
PREFECT_HOST: t.Final[str] = "0.0.0.0"

# os.environ["PREFECT_SERVER_API_HOST"] = PREFECT_HOST
# os.environ["PREFECT_SERVER_API_PORT"] = str(PREFECT_PORT)
# os.environ["PREFECT_UI_ENABLED"] = "1"


@pytest.fixture(scope="session")
def prefect_server() -> None:  # t.Generator[sp.Popen, None, None]:
    """Start a prefect server to process tasks."""
    sp.run(["rm", "-rf", Path("~/.prefect/storage").expanduser()])
    
    # cmd = f"prefect server start --ui --port {PREFECT_PORT} --background --host {PREFECT_HOST} --workers 1".split()
    # popen = sp.Popen(cmd)
    # yield popen
    # popen.kill()

    os.environ["PREFECT_SERVER_API_HOST"] = PREFECT_HOST
    os.environ["PREFECT_SERVER_API_PORT"] = str(PREFECT_PORT)
    os.environ["PREFECT_UI_ENABLED"] = "1"


@pytest.fixture
def mock_home_dir(tmp_path: Path) -> t.Generator[t.Callable[[], Path]]:
    

    home_dir = tmp_path / "test-asset-cache"
    home_dir.mkdir(parents=True, exist_ok=True)

    with mock.patch("cstar.base.utils.get_home_dir", return_value=home_dir) as mock_get_home_dir:
        yield mock_get_home_dir

    # rmtree(home_dir, ignore_errors=True)


# @pytest.mark.skip("Avoid true cloning in unit tests")
@pytest.mark.usefixtures("mock_home_dir", "prefect_server")
def test_materialized_clone(tmp_path: Path) -> None:
    """Confirm the caching and copying behavior of the `get_repo` method.
    
    `get_repo` should retrieve the repository to the cache, then copy the contents
    to the target directory.
    """
    source_loc = "https://github.com/ankona/c-star"

    target_location = tmp_path / "my-repo"
    source_slug = slugify(source_loc)
    cache_location = get_cache_dir() / source_slug

    repo_path = get_repo(source_loc, target_location)

    # confirm the get operation returns the expected path.
    assert repo_path == target_location 

    paths_orig = {x.resolve() for x in cache_location.glob("*")}
    paths_copy = {x.resolve() for x in target_location.glob("*")}

    # confirm the cache and the target have the same content
    files_orig = {x.as_posix().split(source_slug)[1] for x in paths_orig}
    files_copy = {x.as_posix().split("my-repo")[1] for x in paths_copy}
    assert files_copy == files_orig

    # confirm that the files are independent (not symlinks)
    intersection = {x.as_posix() for x in paths_orig} & {x.as_posix() for x in paths_copy}
    assert not intersection


# @pytest.mark.skip("Avoid true cloning in unit tests")
@pytest.mark.usefixtures("mock_home_dir", "prefect_server")
def test_materialized_clone_multi_repo(tmp_path: Path) -> None:
    """Confirm the caching behavior for different repositories.
    
    `get_repo` should not overwrite the content of one repository with another.
    """
    source_loc_1 = "https://github.com/cworthy-ocean/c-star"
    source_loc_2 = "https://github.com/CWorthy-ocean/roms-tools"

    target_location_1 = tmp_path / "cstar"
    target_location_2 = tmp_path / "romstools"

    repo_path_1 = get_repo(source_loc_1, target_location_1)
    repo_path_2 = get_repo(source_loc_2, target_location_2)

    # confirm that a unique path is returned from both calls (proving cache-key is unique)
    assert repo_path_1 != repo_path_2

    paths_repo_1 = {x.resolve() for x in repo_path_1.glob("*")}
    paths_repo_2 = {x.resolve() for x in repo_path_2.glob("*")}

    # confirm that no paths overlap (e.g. overwriting LICENSE.md would show in both)
    size_intersection = len({x.as_posix() for x in paths_repo_1} & {x.as_posix() for x in paths_repo_2})
    assert size_intersection == 0


# @pytest.mark.skip("Avoid true cloning in unit tests")
@pytest.mark.usefixtures("mock_home_dir", "prefect_server")
def test_materialized_clone_caching(tmp_path: Path) -> None:
    """Confirm the cached result is used on repeated accesses.
    
    `get_repo` should not make the same remote request twice.
    """
    source_loc = "https://github.com/CWorthy-ocean/ucla-roms"
    target_location_1 = tmp_path / "cstar"
    target_location_2 = tmp_path / "cstar-alt"

    rmtree(target_location_1, ignore_errors=True)
    rmtree(target_location_2, ignore_errors=True)

    t_start = datetime.datetime.now(tz=datetime.timezone.utc)
    repo_path_1 = get_repo(source_loc, target_location_1)
    t_end = datetime.datetime.now(tz=datetime.timezone.utc)

    print(f"First retrieval took {(t_end - t_start).total_seconds()} seconds")

    # confirm the directory isn't empty
    paths_repo_1 = {x.resolve() for x in repo_path_1.glob("*")}
    assert paths_repo_1

    def do_fail(*args, **kwargs) -> None:
        """Force a failure to occur if the mocked method is executed."""
        raise RuntimeError

    # confirm that cloning to a new target directory does not re-clone.
    with mock.patch("cstar.base.gitutils._clone", side_effect=do_fail):
        t_start = datetime.datetime.now(tz=datetime.timezone.utc)
        repo_path_2 = get_repo(source_loc, target_location_2)
        t_end = datetime.datetime.now(tz=datetime.timezone.utc)

        print(f"Second retrieval took {(t_end - t_start).total_seconds()} seconds")

    assert repo_path_2 != repo_path_1
