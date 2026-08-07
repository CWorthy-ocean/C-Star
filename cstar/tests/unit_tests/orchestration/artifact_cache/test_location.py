"""Unit tests for :class:`cstar.orchestration.artifact_cache.Location`."""

import dataclasses
from pathlib import Path

import pytest

from cstar.orchestration.artifact_cache import Location, Tier


@pytest.fixture
def location(tmp_path: Path) -> Location:
    """Return a location pointing at a not-yet-created file.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Location
        Location under test.
    """
    return Location(
        path=tmp_path / "run-1" / "foo.nc",
        tier=Tier.USER,
        name="foo.nc",
        run_id="run-1",
    )


def test_uri_derives_from_path(location: Location) -> None:
    """The asset URI is computed from the path, so the two cannot drift."""
    assert location.uri == location.path.as_uri()
    assert location.uri.startswith("file:///")
    assert location.uri.endswith("/run-1/foo.nc")


def test_exists_is_false_before_creation(location: Location) -> None:
    """A location is valid to compute before the file is written."""
    assert location.exists is False


def test_exists_reflects_filesystem_live(location: Location) -> None:
    """Existence is re-checked on every access rather than memoized."""
    assert location.exists is False
    location.path.parent.mkdir(parents=True)
    location.path.write_bytes(b"data")
    assert location.exists is True
    location.path.unlink()
    assert location.exists is False


def test_exists_false_for_directory(tmp_path: Path) -> None:
    """A directory at the path does not count as an artifact."""
    directory = tmp_path / "not-a-file"
    directory.mkdir()
    location = Location(path=directory, tier=Tier.USER, name="x", run_id="r")
    assert location.exists is False


def test_is_frozen(location: Location) -> None:
    """Locations are immutable value objects."""
    with pytest.raises(dataclasses.FrozenInstanceError):
        location.path = Path("/elsewhere")  # type: ignore[misc]


def test_equality_is_structural(tmp_path: Path) -> None:
    """Two locations with identical fields compare equal."""
    kwargs = {
        "path": tmp_path / "a.nc",
        "tier": Tier.SHARED,
        "name": "a.nc",
        "run_id": "r",
    }
    assert Location(**kwargs) == Location(**kwargs)  # type: ignore[arg-type]
