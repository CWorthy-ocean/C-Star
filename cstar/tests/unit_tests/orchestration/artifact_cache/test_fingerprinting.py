"""Unit tests for :mod:`cstar.orchestration.fingerprinting` strategies."""

import hashlib
from pathlib import Path

import pytest

from cstar.orchestration.fingerprinting import (
    QUICK_PROBE_BYTES,
    ChecksumMode,
    Fingerprinter,
    FullFingerprinter,
    NullFingerprinter,
    QuickFingerprinter,
    fingerprinter_for,
)

PROBE = 4096
"""Small probe size so tests can build files that straddle the sampled blocks."""


@pytest.fixture
def big_file(tmp_path: Path) -> Path:
    """Return a file comfortably larger than twice the test probe size.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.

    Returns
    -------
    Path
        Path to the populated file.
    """
    path = tmp_path / "big.bin"
    path.write_bytes(bytes(range(256)) * (PROBE * 5 // 256))
    return path


@pytest.fixture
def quick() -> QuickFingerprinter:
    """Return a quick strategy using the small test probe.

    Returns
    -------
    QuickFingerprinter
        Strategy under test.
    """
    return QuickFingerprinter(probe=PROBE)


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------


def test_base_class_is_abstract() -> None:
    """The strategy interface cannot be instantiated directly."""
    with pytest.raises(TypeError):
        Fingerprinter()  # type: ignore[abstract]


@pytest.mark.parametrize(
    ("strategy", "mode"),
    [
        (NullFingerprinter(), ChecksumMode.NONE),
        (QuickFingerprinter(), ChecksumMode.QUICK),
        (FullFingerprinter(), ChecksumMode.FULL),
    ],
)
def test_each_strategy_tags_its_mode(
    strategy: Fingerprinter, mode: ChecksumMode
) -> None:
    """Every strategy advertises the mode recorded alongside its digests."""
    assert strategy.mode is mode


@pytest.mark.parametrize(
    "strategy", [NullFingerprinter(), QuickFingerprinter(), FullFingerprinter()]
)
def test_strategies_implement_the_interface(strategy: Fingerprinter) -> None:
    """Concrete strategies satisfy the abstract base."""
    assert isinstance(strategy, Fingerprinter)


def test_quick_repr_shows_probe() -> None:
    """The quick strategy's representation exposes its tuning."""
    assert repr(QuickFingerprinter(probe=64)) == "QuickFingerprinter(probe=64)"


def test_null_repr_names_the_strategy() -> None:
    """The default representation names the strategy."""
    assert repr(NullFingerprinter()) == "NullFingerprinter()"


def test_quick_rejects_non_positive_probe() -> None:
    """A probe of zero would sample nothing and is refused."""
    with pytest.raises(ValueError, match="probe must be positive"):
        QuickFingerprinter(probe=0)


def test_quick_probe_defaults_to_module_constant() -> None:
    """The default probe is the documented constant."""
    assert QuickFingerprinter().probe == QUICK_PROBE_BYTES


# ---------------------------------------------------------------------------
# Null strategy
# ---------------------------------------------------------------------------


def test_null_returns_no_digest(big_file: Path) -> None:
    """The default strategy produces no digest at all."""
    assert NullFingerprinter().digest(big_file) is None


def test_null_does_not_require_the_file_to_exist(tmp_path: Path) -> None:
    """Taking no digest reads nothing, so a missing file is not an error."""
    assert NullFingerprinter().digest(tmp_path / "absent.nc") is None


def test_null_never_matches(big_file: Path) -> None:
    """With no digest to compare, verification cannot succeed."""
    assert NullFingerprinter().matches(big_file, "deadbeef") is False


# ---------------------------------------------------------------------------
# Full strategy
# ---------------------------------------------------------------------------


def test_full_matches_hashlib(big_file: Path) -> None:
    """The streaming digest agrees with a one-shot hash of the whole file."""
    expected = hashlib.sha256(big_file.read_bytes()).hexdigest()
    assert FullFingerprinter().digest(big_file) == expected


def test_full_detects_middle_corruption(big_file: Path) -> None:
    """Hashing every byte catches damage the quick strategy misses."""
    before = FullFingerprinter().digest(big_file)
    data = bytearray(big_file.read_bytes())
    data[len(data) // 2] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert FullFingerprinter().digest(big_file) != before


def test_full_matches_recorded_digest(big_file: Path) -> None:
    """An unmodified file verifies against its own digest."""
    strategy = FullFingerprinter()
    assert strategy.matches(big_file, strategy.digest(big_file)) is True


# ---------------------------------------------------------------------------
# Quick strategy
# ---------------------------------------------------------------------------


def test_quick_reads_only_the_sampled_blocks(
    big_file: Path, quick: QuickFingerprinter
) -> None:
    """Only the size and the two probe blocks contribute to the digest.

    Reconstructing the expected value from those three inputs alone proves the
    strategy never touches the interior, which is what makes its cost
    independent of file size.
    """
    data = big_file.read_bytes()
    assert len(data) > 2 * PROBE

    expected = hashlib.sha256(f"{len(data)}:".encode())
    expected.update(data[:PROBE])
    expected.update(data[-PROBE:])

    assert quick.digest(big_file) == expected.hexdigest()


def test_quick_is_stable_for_identical_content(
    tmp_path: Path, big_file: Path, quick: QuickFingerprinter
) -> None:
    """Two files with identical bytes share a digest."""
    twin = tmp_path / "twin.bin"
    twin.write_bytes(big_file.read_bytes())
    assert quick.digest(twin) == quick.digest(big_file)


def test_quick_detects_truncation(big_file: Path, quick: QuickFingerprinter) -> None:
    """The interrupted-write failure mode changes the digest."""
    before = quick.digest(big_file)
    data = big_file.read_bytes()
    big_file.write_bytes(data[: len(data) // 2])
    assert quick.digest(big_file) != before


def test_quick_detects_head_corruption(
    big_file: Path, quick: QuickFingerprinter
) -> None:
    """A damaged leading block is caught because the head is sampled."""
    before = quick.digest(big_file)
    data = bytearray(big_file.read_bytes())
    data[0] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert quick.digest(big_file) != before


def test_quick_detects_tail_corruption(
    big_file: Path, quick: QuickFingerprinter
) -> None:
    """A damaged trailing block is caught because the tail is sampled."""
    before = quick.digest(big_file)
    data = bytearray(big_file.read_bytes())
    data[-1] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert quick.digest(big_file) != before


def test_quick_misses_middle_corruption(
    big_file: Path, quick: QuickFingerprinter
) -> None:
    """The documented blind spot: an unsampled middle byte is not detected.

    This is why the quick strategy is a smoke test rather than an integrity
    guarantee, and why the mode is recorded alongside the digest.
    """
    quick_before = quick.digest(big_file)
    full_before = FullFingerprinter().digest(big_file)
    data = bytearray(big_file.read_bytes())
    data[len(data) // 2] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert quick.digest(big_file) == quick_before
    assert FullFingerprinter().digest(big_file) != full_before


def test_quick_distinguishes_same_ends_different_size(
    tmp_path: Path, quick: QuickFingerprinter
) -> None:
    """Size participates in the digest, so padding alone changes it."""
    short = tmp_path / "short.bin"
    long = tmp_path / "long.bin"
    short.write_bytes(b"A" * (PROBE * 4))
    long.write_bytes(b"A" * (PROBE * 6))
    assert quick.digest(short) != quick.digest(long)


def test_quick_handles_file_smaller_than_probe(
    tmp_path: Path, quick: QuickFingerprinter
) -> None:
    """A file shorter than one probe is hashed once without seeking."""
    small = tmp_path / "small.bin"
    small.write_bytes(b"tiny")
    assert quick.digest(small) == hashlib.sha256(b"4:tiny").hexdigest()


def test_quick_and_full_are_not_interchangeable(
    big_file: Path, quick: QuickFingerprinter
) -> None:
    """The two strategies produce different values for the same file."""
    assert quick.digest(big_file) != FullFingerprinter().digest(big_file)


def test_probe_size_changes_the_digest(big_file: Path) -> None:
    """Tuning the probe changes what is sampled, and therefore the digest."""
    assert QuickFingerprinter(probe=PROBE).digest(big_file) != QuickFingerprinter(
        probe=PROBE * 2
    ).digest(big_file)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def test_matches_rejects_absent_expected(big_file: Path) -> None:
    """A missing recorded digest is reported as a failure, not a pass."""
    assert FullFingerprinter().matches(big_file, None) is False


def test_matches_rejects_a_different_digest(big_file: Path) -> None:
    """A digest from another file does not verify."""
    assert FullFingerprinter().matches(big_file, "0" * 64) is False


def test_matches_detects_modification(big_file: Path) -> None:
    """A file edited after commit no longer verifies."""
    strategy = FullFingerprinter()
    recorded = strategy.digest(big_file)
    big_file.write_bytes(b"replaced")
    assert strategy.matches(big_file, recorded) is False


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (ChecksumMode.NONE, NullFingerprinter),
        (ChecksumMode.QUICK, QuickFingerprinter),
        (ChecksumMode.FULL, FullFingerprinter),
    ],
)
def test_registry_maps_every_mode(
    mode: ChecksumMode, expected: type[Fingerprinter]
) -> None:
    """Every mode resolves to the strategy that produces it."""
    strategy = fingerprinter_for(mode)
    assert isinstance(strategy, expected)
    assert strategy.mode is mode


def test_registry_round_trips_a_digest(big_file: Path) -> None:
    """A digest verifies against a strategy rebuilt from its recorded mode."""
    original = QuickFingerprinter()
    digest = original.digest(big_file)
    assert fingerprinter_for(original.mode).matches(big_file, digest) is True
