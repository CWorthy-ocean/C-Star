"""Unit tests for the :mod:`cstar.orchestration.artifact_cache` exception hierarchy."""

import pytest

from cstar.orchestration.artifact_cache import (
    ArtifactCacheError,
    ArtifactExistsError,
    ArtifactNotFoundError,
    UnsafePathError,
)

SUBCLASSES = [ArtifactNotFoundError, ArtifactExistsError, UnsafePathError]


@pytest.mark.parametrize("exc", SUBCLASSES)
def test_all_errors_share_a_base(exc: type[ArtifactCacheError]) -> None:
    """Callers can catch every cache failure with a single except clause."""
    assert issubclass(exc, ArtifactCacheError)


def test_base_is_an_exception() -> None:
    """The base participates in the standard exception hierarchy."""
    assert issubclass(ArtifactCacheError, Exception)


@pytest.mark.parametrize("exc", SUBCLASSES)
def test_errors_are_distinct(exc: type[ArtifactCacheError]) -> None:
    """Sibling errors are not subclasses of one another."""
    siblings = [other for other in SUBCLASSES if other is not exc]
    assert all(not issubclass(exc, other) for other in siblings)


@pytest.mark.parametrize("exc", [ArtifactCacheError, *SUBCLASSES])
def test_message_is_preserved(exc: type[ArtifactCacheError]) -> None:
    """Errors carry their message through to the caller."""
    with pytest.raises(exc, match="boom"):
        raise exc("boom")
