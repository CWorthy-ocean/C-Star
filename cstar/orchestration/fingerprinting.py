"""Content fingerprinting strategies for cached artifacts.

A :class:`Fingerprinter` turns a file on disk into a short string that changes
when the file's contents change. Strategies differ in what they read, and
therefore in what they cost and what they can detect:

:class:`NullFingerprinter`
    Reads nothing and returns no digest. The default, because digesting
    multi-gigabyte model output doubles the I/O of the write path.
:class:`QuickFingerprinter`
    Reads the file size and a block from each end. Cost is independent of file
    size. Detects truncation, extension, and wholesale replacement, which
    together cover the interrupted-write failure mode. Blind to corruption
    confined to the middle of a large file.
:class:`FullFingerprinter`
    Reads every byte. A true integrity check, at the cost of a complete
    additional read.

Digests from different strategies are not comparable, so every strategy
carries a :class:`ChecksumMode` tag that is recorded alongside the digest and
used to select a matching strategy when verifying later.

The abstraction exists so that callers can supply their own strategy — a
faster non-cryptographic hash, or one that delegates to a filesystem's native
checksums — without modifying the cache that uses it.
"""

from __future__ import annotations

import hashlib
import os
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar, Final

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "QUICK_PROBE_BYTES",
    "ChecksumMode",
    "Fingerprinter",
    "FullFingerprinter",
    "NullFingerprinter",
    "QuickFingerprinter",
    "fingerprinter_for",
]

QUICK_PROBE_BYTES: Final[int] = 1024 * 1024
"""Bytes sampled from each end of a file by :class:`QuickFingerprinter`."""


class ChecksumMode(StrEnum):
    """Tag identifying which strategy produced a digest.

    Persisted alongside the digest so a later reader can tell whether two
    values are comparable, and can reconstruct the strategy needed to verify.

    Attributes
    ----------
    NONE : str
        No digest was taken.
    QUICK : str
        Size plus leading and trailing blocks; see :class:`QuickFingerprinter`.
    FULL : str
        SHA-256 over every byte; see :class:`FullFingerprinter`.
    """

    NONE = "none"
    QUICK = "quick"
    FULL = "full"


class Fingerprinter(ABC):
    """Strategy that reduces a file's contents to a comparable digest.

    Implementations are stateless and safe to share across threads. Subclass
    this to plug in an alternative algorithm; the cache depends only on this
    interface.

    Attributes
    ----------
    mode : ChecksumMode
        Tag recorded alongside digests produced by this strategy.
    """

    mode: ClassVar[ChecksumMode]

    @abstractmethod
    def digest(self, path: Path) -> str | None:
        """Fingerprint a file.

        Parameters
        ----------
        path : Path
            Existing file to read.

        Returns
        -------
        str or None
            Hex-encoded digest, or ``None`` if this strategy takes no digest.
        """

    def matches(self, path: Path, expected: str | None) -> bool:
        """Report whether a file still fingerprints to a recorded digest.

        Parameters
        ----------
        path : Path
            Existing file to re-read.
        expected : str or None
            Previously recorded digest.

        Returns
        -------
        bool
            ``True`` when both values are present and equal. A ``None`` on
            either side means there is nothing to compare, which is reported as
            ``False`` rather than treated as success.
        """
        actual = self.digest(path)
        if actual is None or expected is None:
            return False
        return actual == expected

    def __repr__(self) -> str:
        """Return a debugging representation naming the strategy.

        Returns
        -------
        str
            Representation of this fingerprinter.
        """
        return f"{type(self).__name__}()"


class NullFingerprinter(Fingerprinter):
    """Strategy that takes no digest at all.

    The cache's default. Chosen so that writing a large artifact costs one
    pass over the data rather than two.
    """

    mode: ClassVar[ChecksumMode] = ChecksumMode.NONE

    def digest(self, path: Path) -> str | None:
        """Return no digest, without reading the file.

        Parameters
        ----------
        path : Path
            Ignored; this strategy performs no I/O.

        Returns
        -------
        str or None
            Always ``None``. The return type matches the base class so callers
            can treat every strategy uniformly.
        """
        return None


class QuickFingerprinter(Fingerprinter):
    """Strategy sampling a file's size and its leading and trailing blocks.

    Cost is independent of file size: a ``stat`` plus at most two reads of
    :attr:`probe` bytes. Including the size means padding alone changes the
    digest, and sampling the tail is what catches a job killed mid-write, which
    leaves an intact head and a truncated tail.

    Cannot detect corruption confined to the middle of a large file, so this is
    a smoke test rather than an integrity guarantee.

    Parameters
    ----------
    probe : int, optional
        Bytes read from each end. Defaults to :data:`QUICK_PROBE_BYTES`.

    Attributes
    ----------
    probe : int
        Bytes read from each end.
    """

    mode: ClassVar[ChecksumMode] = ChecksumMode.QUICK

    def __init__(self, probe: int = QUICK_PROBE_BYTES) -> None:
        if probe <= 0:
            raise ValueError(f"probe must be positive, got {probe}")
        self.probe = probe

    def digest(self, path: Path) -> str:
        """Fingerprint a file from its size and sampled blocks.

        Parameters
        ----------
        path : Path
            Existing file to read.

        Returns
        -------
        str
            Hex-encoded SHA-256 digest over the size and sampled blocks.
        """
        size = path.stat().st_size
        digest = hashlib.sha256(f"{size}:".encode())
        with path.open("rb") as handle:
            digest.update(handle.read(self.probe))
            if size > self.probe * 2:
                handle.seek(-self.probe, os.SEEK_END)
                digest.update(handle.read(self.probe))
        return digest.hexdigest()

    def __repr__(self) -> str:
        """Return a debugging representation including the probe size.

        Returns
        -------
        str
            Representation of this fingerprinter.
        """
        return f"{type(self).__name__}(probe={self.probe})"


class FullFingerprinter(Fingerprinter):
    """Strategy hashing every byte of a file with SHA-256.

    Delegates to :func:`hashlib.file_digest`, which streams the file in C
    without materialising it in memory. Memory use is bounded; time is not,
    since cost is proportional to file size.
    """

    mode: ClassVar[ChecksumMode] = ChecksumMode.FULL

    def digest(self, path: Path) -> str:
        """Fingerprint a file over its complete contents.

        Parameters
        ----------
        path : Path
            Existing file to read.

        Returns
        -------
        str
            Hex-encoded SHA-256 digest of every byte.
        """
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()


def fingerprinter_for(mode: ChecksumMode) -> Fingerprinter:
    """Return a default strategy instance for a recorded mode.

    Used when verifying an artifact, where the strategy must match whichever
    one produced the stored digest.

    Parameters
    ----------
    mode : ChecksumMode
        Mode recorded alongside a digest.

    Returns
    -------
    Fingerprinter
        Strategy tagged with ``mode``, configured with its defaults.

    Raises
    ------
    ValueError
        If ``mode`` has no registered strategy.
    """
    match mode:
        case ChecksumMode.NONE:
            return NullFingerprinter()
        case ChecksumMode.QUICK:
            return QuickFingerprinter()
        case ChecksumMode.FULL:
            return FullFingerprinter()
        case _:
            raise ValueError(f"no fingerprinter registered for mode {mode!r}")
