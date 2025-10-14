#!/usr/bin/env python3
from pathlib import Path
from unittest import mock

import pytest

from cstar.io import stager
from cstar.io.constants import SourceClassification


class DummyStager(stager.Stager):
    """Stager example for testing"""

    _classification = SourceClassification.LOCAL_TEXT_FILE


class TestRegistry:
    def test_register_and_get_stager(self):
        """Tests that new Stagers can be added and gotten from the registry"""
        with mock.patch.dict(stager._registry, {}, clear=True):
            stager.register_stager(DummyStager)
            result = stager.get_stager(SourceClassification.LOCAL_TEXT_FILE)
            assert isinstance(result, DummyStager)

    def test_get_stager_not_registered(self):
        """Tests that get_stager raises if there is no registry item."""
        with pytest.raises(ValueError):
            stager.get_stager(SourceClassification.LOCAL_DIRECTORY)


class TestStagerABC:
    def test_retriever_property_calls_get_retriever(self):
        """Tests that Stager.retriever calls `get_retriever()`"""
        with mock.patch("cstar.io.stager.get_retriever") as mock_get:
            mock_retriever = mock.Mock()
            mock_get.return_value = mock_retriever
            s = DummyStager()
            assert s.retriever is mock_retriever
            mock_get.assert_called_once_with(SourceClassification.LOCAL_TEXT_FILE)

    def test_stage_calls_retriever_and_returns_stagedfile(self):
        """Tests that Stager.stage() calls Stager.retriever.save() and returns a StagedFile"""
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
        """Tests that all defined stagers are registered."""
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
        """Tests that LocalBinaryFileStager.stage() creates a symbolic link to the source file."""
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
        """Tests that RemoteRepositoryStager.stage calls .retriever.save returns a StagedRepository."""
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
