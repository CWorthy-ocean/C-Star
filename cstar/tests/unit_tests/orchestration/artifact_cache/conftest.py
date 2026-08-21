"""Shared fixtures for :mod:`cstar.orchestration.artifact_cache` unit tests."""

from pathlib import Path

import pytest

from cstar.orchestration.artifact_cache import ArtifactCache
from cstar.orchestration.fingerprinting import NullFingerprinter

RUN_ID = "run-abc-123"
"""Run identifier reused across the suite."""


@pytest.fixture
def user_root(tmp_path: Path) -> Path:
    """Return the user-tier root directory.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Path to use as the user tier root.
    """
    return tmp_path / "user"


@pytest.fixture
def shared_root(tmp_path: Path) -> Path:
    """Return the shared-tier root directory.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Path to use as the shared tier root.
    """
    return tmp_path / "shared"


@pytest.fixture
def view_root(tmp_path: Path) -> Path:
    """Return the symlink view root directory.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Path to use as the view root.
    """
    return tmp_path / "view"


@pytest.fixture
def cache(user_root: Path, shared_root: Path, view_root: Path) -> ArtifactCache:
    """Return a cache wired to isolated temporary roots.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.
    view_root : Path
        View root.

    Returns
    -------
    ArtifactCache
        Cache instance under test.
    """
    return ArtifactCache(user_root, shared_root, view_root)


@pytest.fixture
def null_cache(user_root: Path, shared_root: Path) -> ArtifactCache:
    """Return a cache that records no digests.

    Verification is on by default, so the no-digest behaviour has to be asked
    for explicitly — which is the point of the default and is what these tests
    exercise.

    Parameters
    ----------
    user_root : Path
        User tier root.
    shared_root : Path
        Shared tier root.

    Returns
    -------
    ArtifactCache
        Cache instance whose default strategy takes no digest.
    """
    return ArtifactCache(user_root, shared_root, fingerprinter=NullFingerprinter())


@pytest.fixture
def staged_artifact(cache: ArtifactCache) -> str:
    """Commit a single user-tier artifact and return its name.

    Parameters
    ----------
    cache : ArtifactCache
        Cache to write into.

    Returns
    -------
    str
        Name of the committed artifact.
    """
    name = "filtered.nc"
    with cache.stage(name, RUN_ID, source="raw.nc", metadata={"vars": ["x"]}) as tmp:
        tmp.write_bytes(b"netcdf-payload")
    return name
