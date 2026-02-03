import typing as t
import shutil

from datetime import timedelta
from pathlib import Path
from prefect.assets import materialize
from prefect import flow, task
from prefect.tasks import exponential_backoff
from prefect.cache_policies import CachePolicy, TASK_SOURCE, INPUTS


from cstar.base.gitutils import _clone
from cstar.base.log import get_logger
from cstar.base.utils import get_cache_dir, slugify

log = get_logger(__name__)

REPO_CACHE_POLICY: t.Final[CachePolicy] = TASK_SOURCE + INPUTS
"""Cache policy for repositories retrieved in prefect tasks.

Uses the repository URL to uniquely identify the asset and invalidates the cache
whenever the source code of the task is updated.
"""

REPO_CACHE_DURATION: t.Final[timedelta] = timedelta(seconds=5 * 60)
"""Cache duration (in seconds) for repositories retrieved in prefect tasks."""


def cache_location(repo_uri: str) -> Path:
    """Generate a path where a sourcecode repository will be cached.
    
    Parameters
    ----------
    repo_uri : str
        The URI of the remote source code repository.
    
    Returns
    -------
    Path
    """
    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)

    return cache_dir / slugify(repo_uri)


# @materialize("file://repo", cache_policy=REPO_CACHE_POLICY, cache_expiration=REPO_CACHE_DURATION, cache_result_in_memory=True)
@task(cache_policy=REPO_CACHE_POLICY, cache_expiration=REPO_CACHE_DURATION, cache_result_in_memory=True)
def materialized_clone(repo_uri: str) -> Path:
    """Clone a remote source code repository into the local asset cache.
    
    Parameters
    ----------
    repo_uri : str
        The URI of the remote source code repository.

    Returns
    -------
    The path to the repository in the cache.
    """
    cache_to_path = cache_location(repo_uri)

    if cache_to_path.exists():
        return cache_to_path

    _clone(repo_uri, cache_to_path)

    return cache_to_path

@flow
def get_repo(repo_uri: str, target: Path) -> Path:
    """Clone a remote repository into a local directory.

    Parameters
    ----------
    repo_uri : str
        The URI of the remote source code repository.
    target : Path
        The path where the remote repository should be cloned.

    Returns
    -------
    Path

    Raises
    ------
    ValueError
        If the repository uri is empty.
    """
    asset_key = repo_uri.casefold().strip().casefold()

    if not asset_key:
        raise ValueError("An invalid repository URI was provided")

    # custom_clone_fn = materialized_clone.with_options(assets=[asset_key])
    # cached_clone_path = custom_clone_fn(asset_key)
    cached_clone_path = materialized_clone(asset_key)

    if not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copytree(cached_clone_path, target, symlinks=True)
    except FileNotFoundError:
        log.exception(f"Copy from `{cached_clone_path}` to `{target}` failed.")
        raise

    return target
