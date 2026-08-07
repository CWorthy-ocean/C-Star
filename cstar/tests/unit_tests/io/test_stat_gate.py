"""Unit tests for the ``stat`` gate recorded when data is staged.

:meth:`cstar.io.stager.Stager.stage` snapshots ``os.stat`` at staging time so
:attr:`cstar.io.staged_data.StagedFile.changed_from_source` can report a
modified file without hashing it. The gate is deliberately asymmetric: a
mismatch short-circuits to ``True``, while a match still falls through to the
checksum, because size and modification time do not prove matching content.
"""

import os
from pathlib import Path
from unittest import mock

from cstar.io import staged_data, stager
from cstar.io.staged_data import StagedFile

PAYLOAD = b"payload" * 1000
"""Body written to staged fixtures, long enough that truncation changes size."""


def _fake_source(
    tmp_path: Path,
    file_hash: str | None,
    create: bool = True,
) -> tuple[mock.Mock, Path]:
    """Build a stand-in ``SourceData`` whose retriever yields a known path.

    Parameters
    ----------
    tmp_path : Path
        Directory in which the staged file is created.
    file_hash : str or None
        Value returned by the source's ``file_hash`` property, standing in for
        the checksum declared in a blueprint.
    create : bool, optional
        Whether to write the file before staging. Pass ``False`` to exercise
        the "future file" path, where staging refers to data that does not
        exist yet. Default ``True``.

    Returns
    -------
    tuple of (unittest.mock.Mock, Path)
        The fake source and the path its retriever will return.
    """
    source = mock.Mock()
    source.file_hash = file_hash
    source.basename = "data.nc"

    retrieved = tmp_path / "staged" / "data.nc"
    retrieved.parent.mkdir(parents=True, exist_ok=True)
    if create:
        retrieved.write_bytes(PAYLOAD)

    source.retriever.save.return_value = retrieved
    return source, retrieved


def _fake_symlink_source(target: Path) -> mock.Mock:
    """Build a stand-in ``SourceData`` for the local-symlink staging path.

    Parameters
    ----------
    target : Path
        Location the staged symlink will point at. It need not exist.

    Returns
    -------
    unittest.mock.Mock
        The fake source.
    """
    source = mock.Mock()
    source.basename = "data.nc"
    source.file_hash = None
    source.location = target
    return source


# ---------------------------------------------------------------------------
# Recording the stat
# ---------------------------------------------------------------------------


def test_stage_records_stat(tmp_path: Path) -> None:
    """Staging snapshots the file's ``stat`` so the gate has a baseline.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, retrieved = _fake_source(tmp_path, file_hash="abc")

    staged = stager.Stager(source).stage(tmp_path / "staged")

    assert isinstance(staged, StagedFile)
    assert staged._stat is not None
    assert staged._stat.st_size == retrieved.stat().st_size


def test_symlink_stager_records_stat(tmp_path: Path) -> None:
    """The local-symlink path snapshots the link target's ``stat``.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    real = tmp_path / "real.nc"
    real.write_bytes(PAYLOAD)
    target_dir = tmp_path / "stagedir"
    target_dir.mkdir()

    staged = stager.LocalBinaryFileStager(_fake_symlink_source(real)).stage(target_dir)

    assert staged._stat is not None
    assert staged._stat.st_size == real.stat().st_size


# ---------------------------------------------------------------------------
# Gate behaviour
# ---------------------------------------------------------------------------


def test_modified_file_skips_the_hash(tmp_path: Path) -> None:
    """A ``stat`` mismatch is conclusive, so the checksum is never computed.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, retrieved = _fake_source(tmp_path, file_hash="abc")
    staged = stager.Stager(source).stage(tmp_path / "staged")

    retrieved.write_bytes(b"different length payload")

    with mock.patch.object(
        staged_data, "_get_sha256_hash", side_effect=AssertionError("hashed!")
    ) as hashed:
        assert staged.changed_from_source is True

    hashed.assert_not_called()


def test_unchanged_file_still_verifies_content(tmp_path: Path) -> None:
    """A ``stat`` match is not trusted on its own; the checksum still runs.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, _ = _fake_source(tmp_path, file_hash="declared-hash")
    staged = stager.Stager(source).stage(tmp_path / "staged")

    with mock.patch.object(
        staged_data, "_get_sha256_hash", return_value="declared-hash"
    ) as hashed:
        assert staged.changed_from_source is False

    hashed.assert_called_once()


def test_tamper_preserving_size_and_mtime_is_still_caught(tmp_path: Path) -> None:
    """Content edited without changing size or ``mtime`` is still detected.

    This is the case that motivates keeping the checksum on the matching path:
    on a network filesystem such as Lustre, a cached or skewed ``mtime`` can
    misreport an unchanged file.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, retrieved = _fake_source(tmp_path, file_hash="declared-hash")
    staged = stager.Stager(source).stage(tmp_path / "staged")

    original = retrieved.stat()
    data = bytearray(retrieved.read_bytes())
    data[len(data) // 2] ^= 0xFF
    retrieved.write_bytes(bytes(data))
    os.utime(retrieved, ns=(original.st_atime_ns, original.st_mtime_ns))

    assert retrieved.stat().st_size == original.st_size
    assert retrieved.stat().st_mtime_ns == original.st_mtime_ns

    with mock.patch.object(
        staged_data, "_get_sha256_hash", return_value="something-else"
    ):
        assert staged.changed_from_source is True


# ---------------------------------------------------------------------------
# Files that do not exist yet
# ---------------------------------------------------------------------------


def test_stage_tolerates_a_file_that_does_not_exist_yet(tmp_path: Path) -> None:
    """Staging a "future file" records no ``stat`` rather than raising.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, retrieved = _fake_source(tmp_path, file_hash=None, create=False)
    assert not retrieved.exists()

    staged = stager.Stager(source).stage(tmp_path / "staged")

    assert isinstance(staged, StagedFile)
    assert staged._stat is None


def test_future_file_reports_changed_until_it_appears(tmp_path: Path) -> None:
    """With no file and no ``stat``, the staged data reads as changed.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, _ = _fake_source(tmp_path, file_hash=None, create=False)
    staged = stager.Stager(source).stage(tmp_path / "staged")

    assert staged.changed_from_source is True


def test_future_file_records_stat_once_restaged(tmp_path: Path) -> None:
    """Once the data exists, staging again captures the baseline.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    source, retrieved = _fake_source(tmp_path, file_hash=None, create=False)
    staged = stager.Stager(source).stage(tmp_path / "staged")
    assert isinstance(staged, StagedFile)
    assert staged._stat is None

    retrieved.write_bytes(PAYLOAD)

    restaged = stager.Stager(source).stage(tmp_path / "staged")
    assert isinstance(restaged, StagedFile)
    assert restaged._stat is not None
    assert restaged._stat.st_size == len(PAYLOAD)


def test_symlink_stager_tolerates_dangling_link(tmp_path: Path) -> None:
    """A symlink to data that does not exist yet records no ``stat``.

    ``os.stat`` follows symlinks, so an unguarded call would raise
    :class:`FileNotFoundError` here rather than return a result.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory.
    """
    target_dir = tmp_path / "stagedir"
    target_dir.mkdir()
    source = _fake_symlink_source(tmp_path / "not_yet_there.nc")

    staged = stager.LocalBinaryFileStager(source).stage(target_dir)

    assert (target_dir / "data.nc").is_symlink()
    assert staged._stat is None
