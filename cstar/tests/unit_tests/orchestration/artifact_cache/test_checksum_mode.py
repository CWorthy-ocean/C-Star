"""Unit tests for :class:`cstar.orchestration.fingerprinting.ChecksumMode`."""

import pytest

from cstar.orchestration.fingerprinting import ChecksumMode


def test_members_have_expected_values() -> None:
    """The three strategies expose stable lowercase string values."""
    assert ChecksumMode.NONE.value == "none"
    assert ChecksumMode.QUICK.value == "quick"
    assert ChecksumMode.FULL.value == "full"


def test_round_trips_through_value() -> None:
    """A serialised mode reconstructs to the same member."""
    for mode in ChecksumMode:
        assert ChecksumMode(mode.value) is mode


def test_unknown_value_rejected() -> None:
    """Constructing from an unrecognised value raises."""
    with pytest.raises(ValueError):
        ChecksumMode("crc32")


def test_membership_is_exhaustive() -> None:
    """The enum defines exactly the three documented strategies."""
    assert {m.name for m in ChecksumMode} == {"NONE", "QUICK", "FULL"}
