import os
from datetime import datetime
from unittest.mock import patch

import pytest

from cstar.base.local_file_stats import LocalFileStatistics


@pytest.fixture
def tmp_file_pair(tmp_path):
    file1 = tmp_path / "a.txt"
    file2 = tmp_path / "b.txt"
    file1.touch()
    file2.touch()
    return file1, file2


class TestLocalFileStatisticsInit:
    def test_init_with_paths_only(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(paths=[file1, file2])

        assert lfs.paths == [file1.resolve(), file2.resolve()]
        assert lfs.parent_dir.resolve() == file1.parent
        assert lfs._stat_cache == {}
        assert lfs._hash_cache == {}

    def test_init_raises_if_paths_not_colocated(self, tmp_path):
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        file1 = dir1 / "a.txt"
        file2 = dir2 / "b.txt"
        file1.touch()
        file2.touch()

        with pytest.raises(ValueError, match="paths to exist in a common directory"):
            LocalFileStatistics(paths=[file1, file2])

    def test_init_with_stats(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        stats = {
            file1.absolute(): file1.stat(),
            file2.absolute(): file2.stat(),
        }

        lfs = LocalFileStatistics(paths=[file1, file2], stats=stats)

        assert lfs._stat_cache == stats
        assert lfs._hash_cache == {}
        assert lfs.stats == stats

    def test_init_raises_if_stats_keys_mismatch(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        stats = {file1.parent: file1.stat(), file2.resolve(): file2.stat()}

        with pytest.raises(ValueError, match="keys must match"):
            LocalFileStatistics(paths=[file1, file2], stats=stats)

    def test_init_with_hashes(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        dummy_hash = "a0b1c2d3" * 8

        hashes = {
            file1.absolute(): dummy_hash,
            file2.absolute(): dummy_hash,
        }

        lfs = LocalFileStatistics(paths=[file1, file2], hashes=hashes)

        assert lfs._hash_cache == hashes
        assert lfs._stat_cache == {}
        assert lfs.hashes == hashes

    def test_init_raises_if_hash_keys_mismatch(self, tmp_file_pair):
        file1, file2 = tmp_file_pair
        dummy_hash = "a0b1c2d3" * 8

        hashes = {
            file1.parent: dummy_hash,
            file2.absolute(): dummy_hash,
        }

        with pytest.raises(ValueError, match="keys must match"):
            LocalFileStatistics(paths=[file1, file2], hashes=hashes)


class TestStatsAndHasesProperties:
    @patch("cstar.base.local_file_stats._get_sha256_hash")
    def test_hashes_is_computed_and_cached(self, mock_hash, tmp_file_pair):
        file1, file2 = tmp_file_pair
        mock_hash.side_effect = ["fakehash1", "fakehash2"]

        lfs = LocalFileStatistics(paths=[file1, file2])
        assert lfs._hash_cache == {}

        hashes = lfs.hashes
        assert hashes[file1.resolve()] == "fakehash1"
        assert hashes[file2.resolve()] == "fakehash2"
        assert mock_hash.call_count == 2

        # Second access should not re-call the hash function
        _ = lfs.hashes
        assert mock_hash.call_count == 2  # still 2

    def test_stats_is_computed_and_cached(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(paths=[file1, file2])
        assert lfs._stat_cache == {}

        stats = lfs.stats

        # Check keys match
        assert set(stats.keys()) == {file1.absolute(), file2.absolute()}

        # Check values are stat_result instances
        for stat in stats.values():
            assert isinstance(stat, os.stat_result)

        # Check second access returns same object (i.e. no recomputation)
        assert lfs.stats is stats


class TestValidate:
    @patch("cstar.base.local_file_stats._get_sha256_hash", return_value="fakehash")
    def test_validate_completes_with_matching_stats_and_hashes(
        self, mock_hash, tmp_file_pair
    ):
        file1, file2 = tmp_file_pair
        stats = {file1: file1.stat(), file2: file2.stat()}
        hashes = {file1: "fakehash", file2: "fakehash"}

        lfs = LocalFileStatistics(paths=[file1, file2], stats=stats, hashes=hashes)
        lfs.validate()

        assert True  # if validate does not raise, then pass

    @patch("pathlib.Path.exists", return_value=False)
    def test_validate_raises_if_file_missing(self, mock_exists, tmp_file_pair):
        file1, file2 = tmp_file_pair
        lfs = LocalFileStatistics(paths=[file1, file2])
        with pytest.raises(FileNotFoundError, match="does not exist locally"):
            lfs.validate()

    def test_validate_raises_if_stats_mismatch(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(
            paths=[file1, file2], stats={file1: file1.stat(), file2: file2.stat()}
        )

        # Set mtime to Jan 1, 2000
        old_time = 946684800  # epoch seconds
        os.utime(file1, (old_time, old_time))

        with pytest.raises(ValueError, match="do not match those in cache"):
            lfs.validate()

    def test_validate_raises_if_hashes_mismatch(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(
            paths=[file1, file2],
            hashes={file1: "incorrect_hash1", file2: "incorrect_hash2"},
        )
        with pytest.raises(ValueError, match="does not match value in cache"):
            lfs.validate()


class TestStrAndReprFunctions:
    def test_str(self, tmp_file_pair):
        file1, file2 = tmp_file_pair

        lfs = LocalFileStatistics(paths=[file1, file2])
        out = str(lfs)

        expected = f"""LocalFileStatistics
-------------------
Parent directory: {file1.parent}
Files:
"""

        assert expected.strip() in out.strip()

    def test_repr(self, tmp_file_pair):
        file1, file2 = tmp_file_pair
        lfs = LocalFileStatistics(paths=[file1, file2])
        rep = repr(lfs)

        # Basic structure checks
        assert rep.startswith("LocalFileStatistics(")
        assert str(file1) in rep
        assert str(file2) in rep
        assert rep.endswith(")")  # multiline repr still ends this way

    def test_as_table(self, tmp_file_pair):
        file1, file2 = tmp_file_pair
        file1.write_text("abc")
        file2.write_text("def")

        ts1 = datetime.fromtimestamp(file1.stat().st_mtime).isoformat(
            sep=" ", timespec="seconds"
        )
        ts2 = datetime.fromtimestamp(file2.stat().st_mtime).isoformat(
            sep=" ", timespec="seconds"
        )

        lfs = LocalFileStatistics(paths=[file1, file2])

        tab = lfs.as_table()
        expected_tab = (
            f"""Name                                     Hash         Size (bytes) Modified
a.txt                                    ba7816bf8f…  3            {ts1}
b.txt                                    cb8379ac20…  3            {ts2}"""
        ).strip()

        assert expected_tab == tab

    def test_as_table_with_many_files(self, tmp_path):
        paths = []
        for f in range(20):
            new_file = tmp_path / f"{f}.txt"
            new_file.touch()
            paths.append(new_file)

        lfs = LocalFileStatistics(paths=paths)
        tab = lfs.as_table(max_rows=10)
        assert "... (10 more files)" in tab
