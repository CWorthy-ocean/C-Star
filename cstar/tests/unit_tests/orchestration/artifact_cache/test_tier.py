"""Unit tests for :class:`cstar.orchestration.artifact_cache.Tier`."""

import pytest

from cstar.orchestration.artifact_cache import Tier


def test_members_have_expected_values() -> None:
    """Both tiers expose stable lowercase string values."""
    assert Tier.USER.value == "user"
    assert Tier.SHARED.value == "shared"


def test_is_a_str_subclass() -> None:
    """Members compare equal to their string values for ergonomic use in paths."""
    assert isinstance(Tier.USER, str)
    assert Tier.USER == "user"


def test_round_trips_through_value() -> None:
    """A serialised tier reconstructs to the same member."""
    for tier in Tier:
        assert Tier(tier.value) is tier


def test_unknown_value_rejected() -> None:
    """Constructing from an unrecognised value raises."""
    with pytest.raises(ValueError):
        Tier("archive")


def test_membership_is_exhaustive() -> None:
    """The enum defines exactly the two documented tiers."""
    assert {t.name for t in Tier} == {"USER", "SHARED"}
