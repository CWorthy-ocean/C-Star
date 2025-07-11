import logging
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest

from cstar.base.local_file_stats import FileInfo, LocalFileStatistics
from cstar.base.utils import _get_sha256_hash


@pytest.fixture
def make_tmp_files(tmp_path) -> Callable[[int], List[Path]]:
    def _make_tmp_files(num_files: int = 1) -> List[Path]:
        paths = []
        for i in range(num_files):
            p = tmp_path / f"file_{i}.txt"
            p.write_text(f"contents {i}")
            paths.append(p)
        return paths

    return _make_tmp_files


class TestFileInfoInit:
    """Tests for the FileInfo class."""

    def test_fileinfo_init_with_path(self, make_tmp_files):
        """Ensure a FileInfo instance with a single path creates a minimal instance with
        empty caches for stat and sha256."""
        (file1,) = make_tmp_files(num_files=1)

        file_info = FileInfo(path=file1)
        assert file_info.path == file1
        assert file_info._stat is None
        assert file_info._sha256 is None

    def test_fileinfo_init_with_stat(self, make_tmp_files):
        """Ensure a FileInfo instance with a path and stat creates a FileInfo instance
        with a correct cache value for stat."""

        (file1,) = make_tmp_files(num_files=1)
        stat1 = file1.stat()
        file_info = FileInfo(path=file1, stat=stat1)
        assert file_info.path == file1
        assert file_info._stat == stat1
        assert file_info._sha256 is None

    def test_fileinfo_init_with_hash(self, make_tmp_files):
        """Ensure a FileInfo instance with a path and hash creates a FileInfo instance
        with a correct cache value for hash."""

        (file1,) = make_tmp_files(num_files=1)
        hash1 = "my_hash"
        file_info = FileInfo(path=file1, sha256=hash1)
        assert file_info.path == file1
        assert file_info._stat is None
        assert file_info._sha256 == hash1


class TestFileInfoStatsAndHasesProperties:
    """Tests for the `stats` and `hashes` properties of LocalFileStatistics."""

    @patch("cstar.base.local_file_stats._get_sha256_hash")
    def test_sha256_is_computed_and_cached(self, mock_hash, make_tmp_files):
        """Test lazy hash computation and caching.

        Verifies that:
        - SHA-256 hashes are computed only when first accessed
        - The `_get_sha256_hash` function is called once per file
        - Cached results are reused on subsequent accesses

        Parameters
        ----------
        mock_hash : Mock
            Patches `_get_sha256_hash` to return dummy hashes.
        make_tmp_files : list[Path]
            Two temporary files for which hashes will be computed.

        Asserts
        -------
        - `_hash` is initially empty
        - Hashes are correctly assigned on first access
        - `_get_sha256_hash` is not called more than once per file
        """

        (file1,) = make_tmp_files(1)

        mock_hash.return_value = "fakehash123"

        file_info = FileInfo(path=file1)
        assert file_info._sha256 is None
        test_sha256 = file_info.sha256

        assert test_sha256 == "fakehash123"
        assert file_info._sha256 == "fakehash123"
        assert mock_hash.call_count == 1

        # Second access should not re-call the hash function
        _ = file_info.sha256
        assert mock_hash.call_count == 1  # still 1

    def test_stat_is_computed_and_cached(self, make_tmp_files):
        """Test lazy stat computation and caching.

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.
        """

        (file1,) = make_tmp_files(1)
        stat1 = file1.stat()

        file_info = FileInfo(path=file1)

        # Assert _stat cache is initially empty
        assert file_info._stat is None
        test_stat = file_info.stat

        # Assert calculated stat matches expected
        assert test_stat == stat1
        # Assert cached stat is updated
        assert file_info._stat == stat1


class TestFileInfoValidate:
    """Tests for the `validate()` method of FileInfo.

    These tests confirm that `validate()` correctly detects and raises errors when:
    - Files are missing
    - File metadata (size or modification time) has changed
    - SHA-256 hashes no longer match the cached values

    Tests
    -----
    test_validate_raises_if_stats_mismatch
        Simulates a change in file size or mtime and verifies ValueError is raised.
    test_validate_raises_if_hashes_mismatch
        Simulates a mismatch between computed and cached hashes and verifies error.
    """

    @patch("cstar.base.local_file_stats._get_sha256_hash", return_value="fakehash")
    def test_validate_completes_with_matching_stats_and_hashes(
        self, mock_hash, make_tmp_files
    ):
        """Confirms `validate()` completes without error if metadata and hashes match.

        Simulates a scenario where both `stats` and `hashes` are precomputed
        and match the current state of the files.

        Parameters
        ----------
        mock_hash : Mock
            Patches `_get_sha256_hash` to return the expected value.
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - No exception is raised when calling `validate()`
        """

        (file1,) = make_tmp_files(1)
        stat1, hash1 = file1.stat(), "fakehash"

        file_info = FileInfo(path=file1, stat=stat1, sha256=hash1)
        file_info.validate()

        assert True  # if validate does not raise, then pass

    @patch("pathlib.Path.exists", return_value=False)
    def test_validate_raises_if_file_missing(self, mock_exists, make_tmp_files):
        """Verifies that a FileNotFoundError is raised when a tracked file is missing.

        Patches `Path.exists()` to simulate that a file does not exist.

        Parameters
        ----------
        mock_exists : Mock
            Forces `Path.exists()` to return False.
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises FileNotFoundError with a meaningful message.
        """

        (file1,) = make_tmp_files(1)
        file_info = FileInfo(path=file1)
        with pytest.raises(FileNotFoundError, match="does not exist locally"):
            file_info.validate()

    def test_validate_raises_if_stats_mismatch(self, make_tmp_files):
        """Raise ValueError if file size or mtime no longer matches cache.

        Manually alters the modification time of one file using `os.utime()`.

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises ValueError indicating stat mismatch.
        """

        (file1,) = make_tmp_files(1)
        stat1 = file1.stat()

        file_info = FileInfo(path=file1, stat=stat1)

        # Set mtime to Jan 1, 2000
        old_time = 946684800  # epoch seconds
        os.utime(file1, (old_time, old_time))

        with pytest.raises(ValueError, match="do not match those in cache"):
            file_info.validate()

    def test_validate_raises_if_hashes_mismatch(self, make_tmp_files):
        """Tests for a ValueError raise when computed hash does not match cached value.

        Supplies intentionally incorrect hash values during initialization.

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises ValueError with the expected hash mismatch message.
        """

        (file1,) = make_tmp_files(1)
        file_info = FileInfo(path=file1, sha256="incorrect_hash1")

        with pytest.raises(ValueError, match="does not match value in cache"):
            file_info.validate()

    def test_validate_logs_if_no_cache(self, make_tmp_files, caplog):
        """Confirm that a DEBUG message is written to the logger with missing cache
        values."""

        (file1,) = make_tmp_files(1)
        file_info = FileInfo(path=file1)

        caplog.set_level(logging.DEBUG, logger=file_info.log.name)
        file_info.validate()
        assert any("has no cached stat" in message for message in caplog.messages)
        assert any("has no cached hash" in message for message in caplog.messages)


class TestLocalFileStatisticsInit:
    """Tests for the `__init__` method of `LocalFileStatistics`.

    Ensures that the class initializes correctly with valid inputs and raises
    appropriate exceptions for invalid ones.
    """

    def test_init_with_paths(self, make_tmp_files):
        """Confirms correct initialization when only file paths are provided, ensuring
        that caches are empty and paths are resolved."""

        file1, file2 = make_tmp_files(2)

        lfs = LocalFileStatistics(files=[file1, file2])

        # The `paths` attribute stores the absolute form of the input paths
        assert lfs.paths == [file1.resolve(), file2.resolve()]

        # The `parent_dir` is correctly inferred.
        assert lfs.parent_dir.resolve() == file1.parent

    def test_init_with_fileinfo(self, make_tmp_files):
        """Ensures __init__ accepts and caches a valid `FileInfo` instance."""

        file1, file2 = make_tmp_files(2)
        stat1 = file1.stat()
        stat2 = file2.stat()
        hash1 = _get_sha256_hash(file1)
        hash2 = _get_sha256_hash(file2)

        fileinfo1 = FileInfo(path=file1, stat=stat1, sha256=hash1)
        fileinfo2 = FileInfo(path=file2, stat=stat2, sha256=hash2)

        lfs = LocalFileStatistics(files=[fileinfo1, fileinfo2])

        expected_stats = {file1: stat1, file2: stat2}
        expected_hashes = {file1: hash1, file2: hash2}

        assert lfs.stats == expected_stats
        assert lfs.hashes == expected_hashes
        assert lfs.files == {file1: fileinfo1, file2: fileinfo2}

    def test_init_raises_if_files_missing(self):
        """Ensure a ValueError is raised if files is an empty list or falsy."""
        with pytest.raises(ValueError, match="At least one file must be provided"):
            _ = LocalFileStatistics(files=[])

    def test_init_raises_if_paths_not_colocated(self, tmp_path):
        """Raise ValueError if paths do not share a common parent directory.

        Fixtures
        --------
        tmp_path : pathlib.Path
            pytest-provided temporary root directory for the test.
        """

        # Create a pair of independent directories and files
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        file1 = dir1 / "a.txt"
        file2 = dir2 / "b.txt"
        file1.touch()
        file2.touch()

        # Assert error is correctly raised when files not colocated:
        with pytest.raises(ValueError, match="All files must be in the same directory"):
            LocalFileStatistics(files=[file1, file2])


class TestStrAndReprFunctions:
    """Tests for `__str__`, `__repr__`, and `as_table()` in LocalFileStatistics.

    Tests
    -----
    test_str
        Confirms that the human-readable summary includes parent directory info
        and a file list table.
    test_repr
        Verifies that the formal string representation includes all file paths
        and is constructor-style.
    test_as_table
        Validates the output table formatting for a small number of files.
    test_as_table_with_many_files
        Ensures truncation occurs when file count exceeds `max_rows`.
    """

    def test_str(self, make_tmp_files):
        """Test the `__str__` method for general format and content.

        Ensures that the string representation includes:
        - The class name and section dividers
        - The correct parent directory path
        - A table header labeled 'Files:'

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - The output contains key structural and descriptive components.
        """

        file1, file2 = make_tmp_files(2)

        lfs = LocalFileStatistics(files=[file1, file2])
        out = str(lfs)

        expected = f"""LocalFileStatistics
-------------------
Parent directory: {file1.parent}
Files:
"""

        assert expected.strip() in out.strip()

    def test_repr(self, make_tmp_files):
        """Test the `__repr__` method for structured, evaluable output.

        Verifies that:
        - The output starts with the class name and an opening parenthesis
        - All tracked file paths appear in the output
        - The string ends with a closing parenthesis

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - The output conforms to a constructor-style format
        - File paths are present and intact
        """
        file1, file2 = make_tmp_files(2)
        lfs = LocalFileStatistics(files=[file1, file2])
        rep = repr(lfs)

        # Basic structure checks
        assert rep.startswith("LocalFileStatistics(")
        assert str(file1) in rep
        assert str(file2) in rep
        assert rep.endswith(")")  # multiline repr still ends this way

    def test_as_table(self, make_tmp_files):
        """Test the tabular summary returned by `as_table()`.

        Populates each file with content to ensure hash and size display.
        Timestamps are extracted and matched exactly.

        Parameters
        ----------
        make_tmp_files : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - Output matches expected structure and content for both files
        - Hashes, sizes, and modified timestamps are displayed correctly
        """

        file1, file2 = make_tmp_files(2)
        file1.write_text("abc")
        file2.write_text("def")

        ts1 = datetime.fromtimestamp(file1.stat().st_mtime).isoformat(
            sep=" ", timespec="seconds"
        )
        ts2 = datetime.fromtimestamp(file2.stat().st_mtime).isoformat(
            sep=" ", timespec="seconds"
        )

        lfs = LocalFileStatistics(files=[file1, file2])

        tab = lfs.as_table()
        expected_tab = (
            f"""Name                                     Hash         Size (bytes) Modified
file_0.txt                               ba7816bf8f…  3            {ts1}
file_1.txt                               cb8379ac20…  3            {ts2}"""
        ).strip()

        assert expected_tab == tab

    def test_as_table_with_many_files(self, tmp_path):
        """Test table truncation behavior with more than `max_rows` files.

        Creates 20 empty files and limits output to 10 rows in `as_table()`.

        Parameters
        ----------
        tmp_path : pathlib.Path
            Root temporary directory for generated files.

        Asserts
        -------
        - Output includes a truncation notice
        - Only `max_rows` lines of file metadata are printed before the notice
        """

        paths = []
        for f in range(20):
            new_file = tmp_path / f"{f}.txt"
            new_file.touch()
            paths.append(new_file)

        lfs = LocalFileStatistics(files=paths)
        tab = lfs.as_table(max_rows=10)
        assert "... (10 more files)" in tab


def test_localfilestatistics_getitem(make_tmp_files):
    """Tests that the getitem method on LocalFileStatistics performs correct file
    lookup."""

    # Create files, stats, hashes,
    file1, file2, file3 = make_tmp_files(3)
    stat1, stat2, _ = file1.stat(), file2.stat(), file3.stat()
    hash1, hash2, _ = "fakehash1", "fakehash2", "fakehash3"

    # Create FileInfo Instances
    fileinfo1 = FileInfo(path=file1, stat=stat1, sha256=hash1)
    fileinfo2 = FileInfo(path=file2, stat=stat2, sha256=hash2)

    # Create LocalFileStatistics and attempt to retrieve FileInfo
    lfs = LocalFileStatistics(files=[fileinfo1, fileinfo2])
    assert lfs[file1] == fileinfo1
    assert lfs[file2] == fileinfo2

    # Test for raise with missing file:
    with pytest.raises(KeyError, match="not found"):
        lfs[file3]
