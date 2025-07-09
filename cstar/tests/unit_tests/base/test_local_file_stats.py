import os
from datetime import datetime
from unittest.mock import patch

import pytest

from cstar.base.local_file_stats import FileInfo, LocalFileStatistics
from cstar.base.utils import _get_sha256_hash


@pytest.fixture
def tmp_file_pair(tmp_path):
    """Create two temporary files in a shared temporary directory.

    Returns
    -------
    tuple of Path
        A tuple containing two Path objects: (a.txt, b.txt).
    """

    file1 = tmp_path / "a.txt"
    file2 = tmp_path / "b.txt"
    file1.touch()
    file2.touch()
    return file1, file2


class TestLocalFileStatisticsInit:
    """Tests for the `__init__` method of `LocalFileStatistics`.

    Ensures that the class initializes correctly with valid inputs and raises
    appropriate exceptions for invalid ones. Also verifies that optional
    `stats` and `hashes` arguments are correctly interpreted.

    Tests
    -----
    test_init_with_paths_only
        Confirms correct initialization when only file paths are provided.
        Ensures that caches are empty and paths are resolved.
    test_init_raises_if_paths_not_colocated
        Verifies that a ValueError is raised when files are not in the same directory.
    test_init_with_stats
        Checks that a valid precomputed `stats` dictionary is accepted and used.
    test_init_raises_if_stats_keys_mismatch
        Verifies that a mismatched `stats` dictionary raises a ValueError.
    test_init_with_hashes
        Checks that a valid precomputed `hashes` dictionary is accepted and used.
    test_init_raises_if_hash_keys_mismatch
        Verifies that a mismatched `hashes` dictionary raises a ValueError.
    """

    def test_init_with_paths_only(self, tmp_file_pair):
        """Initialize with only file paths; verify default cache behavior.

        Fixtures
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - The `paths` attribute stores the absolute form of the input paths.
        - The `parent_dir` is correctly inferred.
        - Both `_stat_cache` and `_hash_cache` are empty dictionaries.
        """
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(files=[file1, file2])

        assert lfs.paths == [file1.resolve(), file2.resolve()]
        assert lfs.parent_dir.resolve() == file1.parent

    def test_init_raises_if_paths_not_colocated(self, tmp_path):
        """Raise ValueError if paths do not share a common parent directory.

        Fixtures
        --------
        tmp_path : pathlib.Path
            pytest-provided temporary root directory for the test.

        Asserts
        -------
        - A ValueError is raised with an appropriate message.
        """

        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        file1 = dir1 / "a.txt"
        file2 = dir2 / "b.txt"
        file1.touch()
        file2.touch()

        with pytest.raises(ValueError, match="All files must be in the same directory"):
            LocalFileStatistics(files=[file1, file2])

    def test_init_with_fileinfo(self, tmp_file_pair):
        """Accept a valid `FileInfo` instance and cache it properly.

        Fixtures
        --------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `_stat_cache` matches the input `stat` instances.
        - `_hash_cache` matches the input `hash` instances.
        - The `stats` property returns the expected dict of stats
        - The `hashes` property returns the expected dict of hashes
        """

        file1, file2 = tmp_file_pair
        stat1 = file1.stat()
        stat2 = file2.stat()
        hash1 = _get_sha256_hash(file1)
        hash2 = _get_sha256_hash(file2)

        fileinfo1 = FileInfo(path=file1, stat=stat1, sha256=hash1)
        fileinfo2 = FileInfo(path=file2, stat=stat2, sha256=hash2)

        lfs = LocalFileStatistics(files=[fileinfo1, fileinfo2])

        expected_stats = [stat1, stat2]
        expected_hashes = [hash1, hash2]

        assert lfs.stats == expected_stats
        assert lfs.hashes == expected_hashes


class TestStatsAndHasesProperties:
    """Tests for the `stats` and `hashes` properties of LocalFileStatistics.

    Tests
    -----
    test_hashes_is_computed_and_cached
        Confirms that hash values are computed once and cached.
        Verifies that subsequent accesses do not re-trigger computation.
    test_stats_is_computed_and_cached
        Confirms that stat values are collected lazily and cached.
        Validates both content and identity of cached stat data.
    """

    @patch("cstar.base.local_file_stats._get_sha256_hash")
    def test_hashes_is_computed_and_cached(self, mock_hash, tmp_file_pair):
        """Test lazy hash computation and caching.

        Verifies that:
        - SHA-256 hashes are computed only when first accessed
        - The `_get_sha256_hash` function is called once per file
        - Cached results are reused on subsequent accesses

        Parameters
        ----------
        mock_hash : Mock
            Patches `_get_sha256_hash` to return dummy hashes.
        tmp_file_pair : list[Path]
            Two temporary files for which hashes will be computed.

        Asserts
        -------
        - `_hash_cache` is initially empty
        - Hashes are correctly assigned on first access
        - `_get_sha256_hash` is not called more than once per file
        """

        file1, file2 = tmp_file_pair
        mock_hash.side_effect = ["fakehash1", "fakehash2"]

        lfs = LocalFileStatistics(files=[file1, file2])

        hashes = lfs.hashes
        assert hashes[0] == "fakehash1"
        assert hashes[1] == "fakehash2"
        assert mock_hash.call_count == 2

        # Second access should not re-call the hash function
        _ = lfs.hashes
        assert mock_hash.call_count == 2  # still 2

    def test_stats_is_computed_and_cached(self, tmp_file_pair):
        """Test lazy stat computation and caching.

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `_stat_cache` is initially empty
        - Returned keys match the input file paths
        - All values are `os.stat_result` instances
        - The same dictionary object is returned on second access
        """

        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(files=[file1, file2])

        stats = lfs.stats

        # Check keys match
        assert set(lfs.paths) == {file1.absolute(), file2.absolute()}

        # Check values are stat_result instances
        for stat in stats:
            assert isinstance(stat, os.stat_result)

        # Check second access returns same object (i.e. no recomputation)
        assert lfs.stats == stats


class TestValidate:
    """Tests for the `validate()` method of LocalFileStatistics.

    These tests confirm that `validate()` correctly detects and raises errors when:
    - Files are missing
    - File metadata (size or modification time) has changed
    - SHA-256 hashes no longer match the cached values

    The method is also tested under ideal conditions to confirm it completes
    without raising when all files and metadata are consistent.

    Mocks
    -----
    - `_get_sha256_hash` is patched to simulate known hash values
    - `Path.exists` is patched to simulate missing files

    Tests
    -----
    test_validate_completes_with_matching_stats_and_hashes
        Ensures that `validate()` does not raise when all file metadata matches.
    test_validate_raises_if_file_missing
        Verifies that a missing file causes a FileNotFoundError.
    test_validate_raises_if_stats_mismatch
        Simulates a change in file size or mtime and verifies ValueError is raised.
    test_validate_raises_if_hashes_mismatch
        Simulates a mismatch between computed and cached hashes and verifies error.
    """

    @patch("cstar.base.local_file_stats._get_sha256_hash", return_value="fakehash")
    def test_validate_completes_with_matching_stats_and_hashes(
        self, mock_hash, tmp_file_pair
    ):
        """`validate()` completes without error if metadata and hashes match.

        Simulates a scenario where both `stats` and `hashes` are precomputed
        and match the current state of the files.

        Parameters
        ----------
        mock_hash : Mock
            Patches `_get_sha256_hash` to return the expected value.
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - No exception is raised when calling `validate()`
        """

        file1, file2 = tmp_file_pair
        stat1, hash1 = file1.stat(), "fakehash"
        stat2, hash2 = file2.stat(), "fakehash"

        lfs = LocalFileStatistics(
            files=[
                FileInfo(path=file1, stat=stat1, sha256=hash1),
                FileInfo(path=file2, stat=stat2, sha256=hash2),
            ]
        )
        lfs.validate()

        assert True  # if validate does not raise, then pass

    @patch("pathlib.Path.exists", return_value=False)
    def test_validate_raises_if_file_missing(self, mock_exists, tmp_file_pair):
        """Raise FileNotFoundError when a tracked file is missing.

        Patches `Path.exists()` to simulate that a file does not exist.

        Parameters
        ----------
        mock_exists : Mock
            Forces `Path.exists()` to return False.
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises FileNotFoundError with a meaningful message.
        """

        file1, file2 = tmp_file_pair
        lfs = LocalFileStatistics(files=[file1, file2])
        with pytest.raises(FileNotFoundError, match="does not exist locally"):
            lfs.validate()

    def test_validate_raises_if_stats_mismatch(self, tmp_file_pair):
        """Raise ValueError if file size or mtime no longer matches cache.

        Manually alters the modification time of one file using `os.utime()`.

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises ValueError indicating stat mismatch.
        """

        file1, file2 = tmp_file_pair
        stat1, stat2 = file1.stat(), file2.stat()

        lfs = LocalFileStatistics(
            files=[
                FileInfo(path=file1, stat=stat1, sha256="fakehash"),
                FileInfo(path=file2, stat=stat2, sha256="fakehash"),
            ]
        )

        # Set mtime to Jan 1, 2000
        old_time = 946684800  # epoch seconds
        os.utime(file1, (old_time, old_time))

        with pytest.raises(ValueError, match="do not match those in cache"):
            lfs.validate()

    def test_validate_raises_if_hashes_mismatch(self, tmp_file_pair):
        """Raise ValueError when computed hash does not match cached value.

        Supplies intentionally incorrect hash values during initialization.

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - `validate()` raises ValueError with the expected hash mismatch message.
        """

        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(
            [
                FileInfo(path=file1, stat=file1.stat(), sha256="incorrect_hash1"),
                FileInfo(path=file2, stat=file2.stat(), sha256="incorrect_hash2"),
            ]
        )

        with pytest.raises(ValueError, match="does not match value in cache"):
            lfs.validate()


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

    def test_str(self, tmp_file_pair):
        """Test the `__str__` method for general format and content.

        Ensures that the string representation includes:
        - The class name and section dividers
        - The correct parent directory path
        - A table header labeled 'Files:'

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - The output contains key structural and descriptive components.
        """

        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(files=[file1, file2])
        out = str(lfs)

        expected = f"""LocalFileStatistics
-------------------
Parent directory: {file1.parent}
Files:
"""

        assert expected.strip() in out.strip()

    def test_repr(self, tmp_file_pair):
        """Test the `__repr__` method for structured, evaluable output.

        Verifies that:
        - The output starts with the class name and an opening parenthesis
        - All tracked file paths appear in the output
        - The string ends with a closing parenthesis

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - The output conforms to a constructor-style format
        - File paths are present and intact
        """
        file1, file2 = tmp_file_pair
        lfs = LocalFileStatistics(files=[file1, file2])
        rep = repr(lfs)

        # Basic structure checks
        assert rep.startswith("LocalFileStatistics(")
        assert str(file1) in rep
        assert str(file2) in rep
        assert rep.endswith(")")  # multiline repr still ends this way

    def test_as_table(self, tmp_file_pair):
        """Test the tabular summary returned by `as_table()`.

        Populates each file with content to ensure hash and size display.
        Timestamps are extracted and matched exactly.

        Parameters
        ----------
        tmp_file_pair : list[Path]
            A pair of temporary file paths in the same directory.

        Asserts
        -------
        - Output matches expected structure and content for both files
        - Hashes, sizes, and modified timestamps are displayed correctly
        """

        file1, file2 = tmp_file_pair
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
a.txt                                    ba7816bf8f…  3            {ts1}
b.txt                                    cb8379ac20…  3            {ts2}"""
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
