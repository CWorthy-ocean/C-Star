"""Unit tests for :class:`cstar.orchestration.artifact_cache.ChecksumMode`."""

import hashlib
from pathlib import Path

import pytest

from cstar.orchestration.artifact_cache import ArtifactCache, ChecksumMode

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


def test_members_have_expected_values() -> None:
    """The three strategies expose stable lowercase string values."""
    assert ChecksumMode.NONE.value == "none"
    assert ChecksumMode.QUICK.value == "quick"
    assert ChecksumMode.FULL.value == "full"


def test_round_trips_through_value() -> None:
    """A serialised mode reconstructs to the same member."""
    for mode in ChecksumMode:
        assert ChecksumMode(mode.value) is mode


def test_digest_returns_none_for_no_checksum(big_file: Path) -> None:
    """The default strategy produces no digest at all."""
    assert ArtifactCache._digest(big_file, ChecksumMode.NONE) is None


def test_digest_dispatches_to_full(big_file: Path) -> None:
    """Full mode delegates to the streaming SHA-256 helper."""
    assert ArtifactCache._digest(big_file, ChecksumMode.FULL) == ArtifactCache._sha256(
        big_file
    )


def test_digest_dispatches_to_quick(big_file: Path) -> None:
    """Quick mode delegates to the sampling helper."""
    assert ArtifactCache._digest(
        big_file, ChecksumMode.QUICK
    ) == ArtifactCache._quick_signature(big_file)


def test_quick_and_full_are_not_interchangeable(big_file: Path) -> None:
    """The two strategies produce different values for the same file."""
    quick = ArtifactCache._quick_signature(big_file)
    assert quick != ArtifactCache._sha256(big_file)


def test_quick_is_stable_for_identical_content(tmp_path: Path, big_file: Path) -> None:
    """Two files with identical bytes share a quick signature."""
    twin = tmp_path / "twin.bin"
    twin.write_bytes(big_file.read_bytes())
    assert ArtifactCache._quick_signature(twin) == ArtifactCache._quick_signature(
        big_file
    )


def test_quick_detects_truncation(big_file: Path) -> None:
    """The interrupted-write failure mode changes the quick signature."""
    before = ArtifactCache._quick_signature(big_file)
    data = big_file.read_bytes()
    big_file.write_bytes(data[: len(data) // 2])
    assert ArtifactCache._quick_signature(big_file) != before


def test_quick_detects_tail_corruption(big_file: Path, tmp_path: Path) -> None:
    """A damaged trailing block is caught because the tail is sampled."""
    before = ArtifactCache._quick_signature(big_file, probe=PROBE)
    data = bytearray(big_file.read_bytes())
    data[-1] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert ArtifactCache._quick_signature(big_file, probe=PROBE) != before


def test_quick_detects_head_corruption(big_file: Path) -> None:
    """A damaged leading block is caught because the head is sampled."""
    before = ArtifactCache._quick_signature(big_file, probe=PROBE)
    data = bytearray(big_file.read_bytes())
    data[0] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert ArtifactCache._quick_signature(big_file, probe=PROBE) != before


def test_quick_misses_middle_corruption(big_file: Path) -> None:
    """The documented blind spot: an unsampled middle byte is not detected.

    This is the reason quick mode is a smoke test rather than an integrity
    guarantee, and the reason the mode is recorded alongside the digest.
    """
    quick_before = ArtifactCache._quick_signature(big_file, probe=PROBE)
    full_before = ArtifactCache._sha256(big_file)
    data = bytearray(big_file.read_bytes())
    data[len(data) // 2] ^= 0xFF
    big_file.write_bytes(bytes(data))
    assert ArtifactCache._quick_signature(big_file, probe=PROBE) == quick_before
    assert ArtifactCache._sha256(big_file) != full_before


def test_quick_distinguishes_same_ends_different_size(tmp_path: Path) -> None:
    """Size participates in the digest, so padding alone changes the signature."""
    short = tmp_path / "short.bin"
    long = tmp_path / "long.bin"
    short.write_bytes(b"A" * (PROBE * 4))
    long.write_bytes(b"A" * (PROBE * 6))
    assert ArtifactCache._quick_signature(
        short, probe=PROBE
    ) != ArtifactCache._quick_signature(long, probe=PROBE)


def test_quick_handles_file_smaller_than_probe(tmp_path: Path) -> None:
    """A file shorter than one probe is hashed once without seeking."""
    small = tmp_path / "small.bin"
    small.write_bytes(b"tiny")
    expected = hashlib.sha256(b"4:" + b"tiny").hexdigest()
    assert ArtifactCache._quick_signature(small, probe=PROBE) == expected


def test_quick_reads_bounded_bytes(
    big_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Quick mode reads at most two probes regardless of file size."""
    reads: list[int] = []
    real_open = Path.open

    def counting_open(self: Path, *args: object, **kwargs: object) -> object:
        handle = real_open(self, *args, **kwargs)  # type: ignore[arg-type]
        real_read = handle.read

        def tracked(n: int = -1) -> bytes:
            chunk = real_read(n)
            reads.append(len(chunk))
            return chunk  # type: ignore[no-any-return]

        handle.read = tracked  # type: ignore[method-assign,assignment]
        return handle

    monkeypatch.setattr(Path, "open", counting_open)
    ArtifactCache._quick_signature(big_file, probe=PROBE)
    assert sum(reads) <= 2 * PROBE
    assert big_file.stat().st_size > 2 * PROBE
