#!/usr/bin/env python3
from pathlib import Path
from typing import Protocol
from unittest import mock

import pytest

from cstar.io import stager
from cstar.io.constants import SourceClassification
from cstar.io.source_data import SourceData


class SourceDataFactory(Protocol):
    def __call__(
        self,
        classification: SourceClassification | None = None,
        location: str | Path | None = None,
        identifier: str | None = None,
    ) -> SourceData: ...


class DummyStager(stager.Stager):
    """Stager example for testing"""

    _classification = SourceClassification.LOCAL_TEXT_FILE


class TestRegistry:
    def test_register_and_get_stager(
        self, mocksourcedata_local_text_file: SourceDataFactory
    ) -> None:
        """Tests that new Stagers can be added and gotten from the registry"""
        with mock.patch.dict(stager._registry, {}, clear=True):
            stager.register_stager(DummyStager)
            result = stager.get_stager(mocksourcedata_local_text_file())
            assert isinstance(result, DummyStager)

    def test_get_stager_not_registered(
        self, mocksourcedata_factory: SourceDataFactory
    ) -> None:
        """Tests that get_stager raises if there is no registry item."""
        source = mocksourcedata_factory(
            location="somewhere", classification=SourceClassification.LOCAL_DIRECTORY
        )
        with pytest.raises(ValueError):
            stager.get_stager(source)

    @pytest.mark.parametrize(
        ("classification", "expected_stager_cls"),
        [
            (
                SourceClassification.REMOTE_REPOSITORY,
                stager.CachedRemoteRepositoryStager,
            ),
            (SourceClassification.REMOTE_BINARY_FILE, stager.RemoteBinaryFileStager),
            (SourceClassification.REMOTE_TEXT_FILE, stager.RemoteTextFileStager),
            (SourceClassification.LOCAL_BINARY_FILE, stager.LocalBinaryFileStager),
            (SourceClassification.LOCAL_TEXT_FILE, stager.LocalTextFileStager),
        ],
    )
    def test_get_stager_returns_expected_class(
        self,
        classification: SourceClassification,
        expected_stager_cls: type[stager.Stager],
        mocksourcedata_factory: SourceDataFactory,
    ) -> None:
        """Tests that the `get_stager` method looks up the expected stager for several classifications."""
        source = mocksourcedata_factory(
            location="somewhere", classification=classification
        )
        stgr = stager.get_stager(source)
        assert isinstance(stgr, expected_stager_cls)


class TestStagerABC:
    def test_stage_calls_retriever_and_returns_stagedfile(
        self, tmp_path: Path, mocksourcedata_remote_file: SourceDataFactory
    ) -> None:
        """Tests that Stager.stage() calls Stager.retriever.save() and returns a StagedFile"""
        source = mocksourcedata_remote_file()

        fake_target = Path("/fake/path")

        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_target

        with (
            mock.patch.object(
                type(source), "retriever", new_callable=mock.PropertyMock
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedFile") as mock_staged,
        ):
            mock_ret.return_value = fake_retriever
            s = stager.RemoteBinaryFileStager(source)

            result = s.stage(tmp_path)

        fake_retriever.save.assert_called_once_with(target_dir=tmp_path)
        mock_staged.assert_called_once_with(
            source=source, path=fake_target, sha256="abc123", stat=None
        )

        assert result is mock_staged.return_value


class TestStagerSubclasses:
    def test_registry_contains_all_stagers(
        self, mocksourcedata_factory: SourceDataFactory
    ) -> None:
        """Tests that all defined stagers are registered."""
        for cls in [
            stager.RemoteBinaryFileStager,
            stager.RemoteTextFileStager,
            stager.LocalBinaryFileStager,
            stager.LocalTextFileStager,
            stager.CachedRemoteRepositoryStager,
        ]:
            source = mocksourcedata_factory(
                location="somewhere", classification=cls._classification
            )
            inst = stager.get_stager(source)
            assert isinstance(inst, cls)

    def test_local_binary_file_stager_creates_symlink(
        self, tmp_path: Path, mocksourcedata_local_file: SourceDataFactory
    ) -> None:
        """Tests that LocalBinaryFileStager.stage() creates a symbolic link to the source file."""
        source_dir = tmp_path / "src"
        source_dir.mkdir()
        src_file = source_dir / "source.bin"
        src_file.write_bytes(b"123")
        source = mocksourcedata_local_file(location=src_file, identifier=None)

        staging_dir = tmp_path / "stage"
        staging_dir.mkdir()

        s = stager.LocalBinaryFileStager(source)
        result = s.stage(staging_dir)

        target = staging_dir / "source.bin"
        assert target.is_symlink()
        assert target.read_bytes() == b"123"
        assert result.source is source
        assert result.path == target

    def test_remote_repository_stager_returns_stagedrepo(
        self, tmp_path: Path, mocksourcedata_remote_repo: SourceDataFactory
    ) -> None:
        """Tests that RemoteRepositoryStager.stage calls .retriever.save returns a StagedRepository."""
        source = mocksourcedata_remote_repo()
        fake_path = tmp_path / "repo"

        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_path

        with (
            mock.patch.object(
                type(source),
                "retriever",
                new_callable=mock.PropertyMock,
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedRepository") as mock_staged_repo,
        ):
            mock_ret.return_value = fake_retriever
            s = stager.RemoteRepositoryStager(source)
            result = s.stage(tmp_path)

        fake_retriever.save.assert_called_once_with(target_dir=tmp_path)
        mock_staged_repo.assert_called_once_with(source=source, path=fake_path)
        assert result is mock_staged_repo.return_value

    def test_cached_remote_repository_stager_writes_to_cache(
        self,
        tmp_path: Path,
        mocksourcedata_remote_repo: SourceDataFactory,
    ) -> None:
        """Tests that CachedRemoteRepositoryStager.stage caches the retrieved repository
        in the correct path.
        """
        source = mocksourcedata_remote_repo()
        repo_name = "repo"
        fake_path = tmp_path / repo_name
        fake_cache_path = tmp_path / "cache" / repo_name
        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_cache_path

        with (
            mock.patch.object(
                type(source),
                "retriever",
                new_callable=mock.PropertyMock,
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedRepository") as mock_staged_repo,
            mock.patch("cstar.io.stager._run_cmd") as mock_run_cmd,
            mock.patch(
                "cstar.io.stager.CachedRemoteRepositoryStager._get_cache_path",
            ) as mock_get_cache_path,
        ):
            mock_get_cache_path.return_value = fake_cache_path
            mock_run_cmd.return_value = ""
            mock_ret.return_value = fake_retriever
            s = stager.CachedRemoteRepositoryStager(source)

            result = s.stage(fake_path)

        # confirm repo is put into cache
        fake_retriever.save.assert_called_once_with(target_dir=fake_cache_path)

        # confirm the content is copied from the cache to the target
        mock_run_cmd.assert_called_once_with(f"cp -av {fake_cache_path}/ {fake_path}")

        # confirm the result is a new path and the cached location is not leaked
        mock_staged_repo.assert_called_once_with(source=source, path=fake_path)
        assert result is mock_staged_repo.return_value

    def test_cached_remote_repository_stager_reads_from_cache(
        self,
        tmp_path: Path,
        mocksourcedata_remote_repo: SourceDataFactory,
    ) -> None:
        """Tests that CachedRemoteRepositoryStager.stage returns fresh content from
        the cache if the remote repository has been previously staged.
        """
        source = mocksourcedata_remote_repo()
        repo_name = "repo"
        fake_path = tmp_path / repo_name

        fake_cache_path = tmp_path / "cache" / repo_name
        fake_cache_path.mkdir(parents=True, exist_ok=False)
        fake_content_path = fake_cache_path / "foo.txt"
        fake_content_path.touch()  # trick stager into believing content is cached.

        fake_retriever = mock.Mock()
        fake_retriever.save.return_value = fake_cache_path

        with (
            mock.patch.object(
                type(source),
                "retriever",
                new_callable=mock.PropertyMock,
            ) as mock_ret,
            mock.patch("cstar.io.stager.StagedRepository"),  # avoid validation
            mock.patch("cstar.io.stager._run_cmd") as mock_run_cmd,
            mock.patch(
                "cstar.io.stager.CachedRemoteRepositoryStager._get_cache_path",
            ) as mock_get_cache_path,
        ):
            mock_get_cache_path.return_value = fake_cache_path
            mock_run_cmd.return_value = ""
            mock_ret.return_value = fake_retriever
            s = stager.CachedRemoteRepositoryStager(source)

            _ = s.stage(fake_path)

        # confirm repo is not retrieved
        fake_retriever.save.assert_not_called()

        # confirm cached repo is updated with latest changes
        fake_retriever.refresh.assert_called_once_with(target_dir=fake_cache_path)

        # confirm the correct hash/branch is checked out from the copy (not cache)
        fake_retriever.checkout.assert_called_once_with(target_dir=fake_path)
