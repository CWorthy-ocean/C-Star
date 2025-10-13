#!/usr/bin/env python3
from pathlib import Path
from unittest import mock

import pytest

from cstar.io import stager
from cstar.io.constants import SourceClassification


class TestRegistry:
    def test_register_and_get_stager_isolated(self):
        with mock.patch.dict(stager._registry, {}, clear=True):

            class DummyStager(stager.Stager):
                _classification = SourceClassification.LOCAL_TEXT_FILE

            stager.register_stager(DummyStager)
            result = stager.get_stager(SourceClassification.LOCAL_TEXT_FILE)
            assert isinstance(result, DummyStager)

    def test_get_stager_not_registered(self):
        with pytest.raises(ValueError):
            stager.get_stager(SourceClassification.LOCAL_DIRECTORY)


class TestStagerABC:
    def test_retriever_property_calls_get_retriever(self):
        with mock.patch("cstar.io.stager.get_retriever") as mock_get:
            mock_retriever = mock.Mock()
            mock_get.return_value = mock_retriever

            class DummyStager(stager.Stager):
                _classification = SourceClassification.REMOTE_BINARY_FILE

            s = DummyStager()
            assert s.retriever is mock_retriever
            mock_get.assert_called_once_with(SourceClassification.REMOTE_BINARY_FILE)

    def test_stage_calls_retriever_and_returns_stagedfile(self):
        fake_source = mock.Mock(file_hash="abc123")
        fake_target = Path("/fake/path")

        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_target

        with (
            mock.patch.object(
                stager.Stager, "retriever", new_callable=mock.PropertyMock
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedFile") as mock_staged,
        ):
            mock_ret.return_value = fake_retriever
            s = stager.RemoteBinaryFileStager()

            result = s.stage(Path("/tmp"), fake_source)

        fake_retriever.save.assert_called_once_with(
            source=fake_source, target_dir=Path("/tmp")
        )
        mock_staged.assert_called_once_with(
            source=fake_source, path=fake_target, sha256="abc123", stat=None
        )
        assert result is mock_staged.return_value


class TestStagerSubclasses:
    def test_registry_contains_all_stagers(self):
        for cls in [
            stager.RemoteBinaryFileStager,
            stager.RemoteTextFileStager,
            stager.LocalBinaryFileStager,
            stager.LocalTextFileStager,
            stager.RemoteRepositoryStager,
        ]:
            inst = stager.get_stager(cls._classification)
            assert isinstance(inst, cls)

    def test_local_binary_file_stager_creates_symlink(self, tmp_path):
        source_dir = tmp_path / "src"
        source_dir.mkdir()
        src_file = source_dir / "source.bin"
        src_file.write_bytes(b"123")

        fake_source = mock.Mock(
            basename="source.bin", location=src_file, file_hash="deadtofu"
        )

        staging_dir = tmp_path / "stage"
        staging_dir.mkdir()

        s = stager.LocalBinaryFileStager()
        result = s.stage(staging_dir, fake_source)

        target = staging_dir / "source.bin"
        assert target.is_symlink()
        assert target.read_bytes() == b"123"
        assert result.source is fake_source
        assert result.path == target

    def test_remote_repository_stager_returns_stagedrepo(self, tmp_path):
        fake_source = mock.Mock()
        fake_path = tmp_path / "repo"

        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_path

        with (
            mock.patch.object(
                stager.RemoteRepositoryStager,
                "retriever",
                new_callable=mock.PropertyMock,
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedRepository") as mock_staged_repo,
        ):
            mock_ret.return_value = fake_retriever
            s = stager.RemoteRepositoryStager()
            result = s.stage(tmp_path, fake_source)

        fake_retriever.save.assert_called_once_with(
            source=fake_source, target_dir=tmp_path
        )
        mock_staged_repo.assert_called_once_with(source=fake_source, path=fake_path)
        assert result is mock_staged_repo.return_value
