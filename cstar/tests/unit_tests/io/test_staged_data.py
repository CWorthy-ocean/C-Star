import os
import shutil
from unittest import mock

import pytest

import cstar.io.staged_data as staged_data


class TestStagedFile:
    def test_unstage_removes_file_and_clears_cache(self, tmp_path):
        file = tmp_path / "f.txt"
        file.write_text("hello")
        sf = staged_data.StagedFile(source=mock.Mock(), path=file)

        sf.unstage()
        assert not file.exists()
        # Private attrs cleared
        assert sf._stat is None
        assert sf._sha256 is None

    def test_reset_calls_stage_if_changed(self, tmp_path):
        file = tmp_path / "f.txt"
        file.write_text("hello")

        fake_source = mock.Mock()
        sf = staged_data.StagedFile(source=fake_source, path=file)
        # Force changed state
        with mock.patch.object(
            staged_data.StagedFile,
            "changed_from_source",
            new_callable=mock.PropertyMock,
            return_value=True,
        ):
            sf.reset()
        fake_source.stage.assert_called_once_with(target_dir=file)


class TestStagedFileChangedFromSource:
    def setup_file(self, tmp_path):
        file = tmp_path / "f.txt"
        file.write_text("hello")
        real_stat = os.stat(file)
        sf = staged_data.StagedFile(
            source=mock.Mock(),
            path=file,
            sha256=None,  # trigger real hash calc
            stat=real_stat,
        )
        return file, real_stat, sf

    def test_unchanged(self, tmp_path):
        file, _, sf = self.setup_file(tmp_path)
        assert sf.changed_from_source is False

    def test_detects_mtime_change(self, tmp_path):
        file, real_stat, sf = self.setup_file(tmp_path)

        fake_stat = mock.Mock()
        # Copy across fields that changed_from_source will check
        fake_stat.st_mtime = real_stat.st_mtime + 100
        fake_stat.st_size = real_stat.st_size
        # Make os.stat return this fake stat
        with mock.patch("os.stat", return_value=fake_stat):
            assert sf.changed_from_source is True

    def test_detects_size_change(self, tmp_path):
        file, real_stat, sf = self.setup_file(tmp_path)

        fake_stat = mock.Mock()
        # Keep mtime the same so the mtime check doesn't trip
        fake_stat.st_mtime = real_stat.st_mtime
        # Change size so only size check fails
        fake_stat.st_size = real_stat.st_size + 10

        with mock.patch("os.stat", return_value=fake_stat):
            assert sf.changed_from_source is True

    def test_detects_sha256_mismatch(self, tmp_path):
        file, _, sf = self.setup_file(tmp_path)
        with mock.patch("cstar.io.staged_data._get_sha256_hash", return_value="wrong"):
            assert sf.changed_from_source is True

    def test_detects_deleted_file(self, tmp_path):
        file, _, sf = self.setup_file(tmp_path)
        file.unlink()
        assert sf.changed_from_source is True


class TestStagedRepository:
    def test_init_runs_git_rev_parse(self, tmp_path):
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        fake_source = mock.Mock(location="http://example.com/repo.git")

        with mock.patch(
            "cstar.io.staged_data._run_cmd", return_value="abc123"
        ) as mock_run:
            sr = staged_data.StagedRepository(source=fake_source, path=repo_path)

        mock_run.assert_called_once_with(
            cmd="git rev-parse HEAD", cwd=repo_path, raise_on_error=True
        )
        assert sr._checkout_hash == "abc123"

    def test_changed_from_source_delegates(self, tmp_path):
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        fake_source = mock.Mock(location="http://example.com/repo.git")

        with (
            mock.patch("cstar.io.staged_data._run_cmd", return_value="hash"),
            mock.patch(
                "cstar.io.staged_data._check_local_repo_changed_from_remote",
                return_value=True,
            ) as mock_check,
        ):
            sr = staged_data.StagedRepository(source=fake_source, path=repo_path)
            assert sr.changed_from_source is True
        mock_check.assert_called_once()

    def test_unstage_removes_directory(self, tmp_path):
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        (repo_path / "file").write_text("data")
        fake_source = mock.Mock()

        with mock.patch("cstar.io.staged_data._run_cmd", return_value="hash"):
            sr = staged_data.StagedRepository(source=fake_source, path=repo_path)

        sr.unstage()
        assert not repo_path.exists()

    def test_reset_clones_if_missing_else_hard_reset(self, tmp_path):
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        fake_source = mock.Mock(
            location="http://example.com/repo.git", checkout_target="deadbeef"
        )

        with mock.patch(
            "cstar.io.staged_data._run_cmd", return_value="hash"
        ) as mock_run:
            sr = staged_data.StagedRepository(source=fake_source, path=repo_path)
            sr.reset()
        mock_run.assert_called_with(
            cmd="git reset --hard deadbeef", cwd=repo_path, raise_on_error=True
        )

        # Case: path missing -> source.stage called
        shutil.rmtree(repo_path)
        fake_source.stage.reset_mock()
        sr.reset()
        fake_source.stage.assert_called_once_with(target_dir=repo_path)


class TestStagedDataCollection:
    def test_len_getitem_iter_append_and_paths(self, tmp_path):
        file = tmp_path / "f.txt"
        file.write_text("abc")
        s1 = staged_data.StagedFile(source=mock.Mock(), path=file)

        coll = staged_data.StagedDataCollection([s1])
        assert len(coll) == 1
        assert coll[0] is s1
        assert list(iter(coll)) == [s1]
        assert coll.paths == [file]

        # append valid
        s2 = staged_data.StagedFile(source=mock.Mock(), path=file)
        coll.append(s2)
        assert len(coll) == 2

        # append invalid type raises
        with pytest.raises(TypeError):
            coll.append("not staged data")

    def test_changed_from_source_and_reset_unstage(self, tmp_path):
        file = tmp_path / "f.txt"
        file.write_text("abc")
        s1 = staged_data.StagedFile(source=mock.Mock(), path=file)
        coll = staged_data.StagedDataCollection([s1])

        with (
            mock.patch.object(
                staged_data.StagedFile,
                "changed_from_source",
                new_callable=mock.PropertyMock,
                return_value=True,
            ),
            mock.patch.object(s1, "reset") as mock_reset,
            mock.patch.object(s1, "unstage") as mock_unstage,
        ):
            assert coll.changed_from_source is True
            coll.reset()
            coll.unstage()
        mock_reset.assert_called_once()
        mock_unstage.assert_called_once()
