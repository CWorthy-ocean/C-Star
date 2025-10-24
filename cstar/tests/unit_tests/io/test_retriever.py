from unittest import mock

import pytest

from cstar.base.utils import _get_sha256_hash
from cstar.io import retriever
from cstar.io.constants import SourceClassification


class DummyRetriever(retriever.Retriever):
    """Example Retriever for testing"""

    _classification = SourceClassification.LOCAL_TEXT_FILE

    def read(self):
        return b""

    def _save(self, target_dir):
        return target_dir / "out"


class TestRegistry:
    def test_register_and_get_retriever(self, mocksourcedata_local_text_file):
        """Tests that a new retriever can be registered and fetched from the register"""
        temp_registry = {}
        with mock.patch("cstar.io.retriever._registry", temp_registry):
            retriever.register_retriever(DummyRetriever)
            r = retriever.get_retriever(mocksourcedata_local_text_file())
            assert isinstance(r, DummyRetriever)

    def test_get_retriever_not_registered(self, mocksourcedata_factory):
        """Tests that getting an unregistered retriever raises a ValueError"""
        sourcedata = mocksourcedata_factory(
            classification=SourceClassification.LOCAL_DIRECTORY,
            location="somewhere",
        )
        with pytest.raises(ValueError):
            retriever.get_retriever(sourcedata)


class TestRetrieverABC:
    def test_save_creates_dir_and_calls_subclass_save(
        self, tmp_path, mocksourcedata_local_text_file
    ):
        """Tests that parent class save ensures validity before calling subclass save()"""
        r = DummyRetriever(mocksourcedata_local_text_file)
        result = r.save(tmp_path / "newdir")
        assert result == tmp_path / "newdir/out", (
            f"EXPECTED {tmp_path / 'newdir/out'} \nGOT {result}"
        )

    def test_save_raises_if_not_directory(
        self, tmp_path, mocksourcedata_local_text_file
    ):
        """Tests that parent class save raises if target_dir is not a dir"""
        file_path = tmp_path / "afile"
        file_path.write_text("x")

        r = retriever.LocalFileRetriever(mocksourcedata_local_text_file)
        with pytest.raises(ValueError):
            r.save(file_path)


class TestRemoteFileRetriever:
    def test_read_returns_content(self, mocksourcedata_remote_text_file):
        """Tests that RemoteFileRetriever.read returns expected bytes"""
        fake_response = mock.Mock()
        fake_response.content = b"abc"
        fake_response.raise_for_status = mock.Mock()
        with mock.patch("cstar.io.retriever.requests.get", return_value=fake_response):
            r = retriever.RemoteTextFileRetriever(mocksourcedata_remote_text_file())
            result = r.read()
        assert result == b"abc"
        fake_response.raise_for_status.assert_called_once()


class TestRemoteTextFileRetriever:
    def test_save_writes_file(self, tmp_path, mocksourcedata_remote_text_file):
        """Tests that RemoteTextFileRetriever.save takes `read` output and saves it to file"""
        fake_data = b"hello world"

        # Patch .read so we don’t hit the network
        with mock.patch.object(
            retriever.RemoteTextFileRetriever, "read", return_value=fake_data
        ) as mock_read:
            r = retriever.RemoteTextFileRetriever(
                source=mocksourcedata_remote_text_file()
            )
            result = r._save(tmp_path)

        # Ensure read was called with our source
        mock_read.assert_called_once()

        # File should exist with correct contents
        assert result == tmp_path / "remote_file.yaml"
        assert result.read_bytes() == fake_data


class TestRemoteBinaryFileRetriever:
    def test_save_writes_file(self, tmp_path, mocksourcedata_remote_file):
        """Tests that RemoteBinaryFileRetriever.save iterates over chunks and updates file correctly."""
        fake_chunk = b"abc"
        source = mocksourcedata_remote_file()
        source._identifier = None
        fake_response = mock.MagicMock()
        fake_response.__enter__.return_value = fake_response
        fake_response.iter_content.return_value = [fake_chunk]
        fake_response.raise_for_status = mock.Mock()

        with mock.patch("cstar.io.retriever.requests.get", return_value=fake_response):
            r = retriever.RemoteBinaryFileRetriever(source)
            path = r._save(tmp_path)

        assert path.exists()
        assert path.read_bytes() == fake_chunk

    def test_save_raises_on_hash_mismatch(self, tmp_path, mocksourcedata_remote_file):
        """Tests that RemoteBinaryFileRetriever.save raises if the final calculated hash does not match `identifier`"""
        source = mocksourcedata_remote_file()
        fake_chunk = b"abc"

        fake_response = mock.MagicMock()
        fake_response.__enter__.return_value = fake_response
        fake_response.iter_content.return_value = [fake_chunk]
        fake_response.raise_for_status = mock.Mock()

        with mock.patch("cstar.io.retriever.requests.get", return_value=fake_response):
            r = retriever.RemoteBinaryFileRetriever(source=source)
            with pytest.raises(ValueError, match="Hash mismatch"):
                r._save(tmp_path)


class TestLocalFileRetriever:
    def test_read_and_save(self, tmp_path, mocksourcedata_local_text_file):
        """Tests that LocalFileRetriever.read and .save read and copy files, respectively"""
        test_file = tmp_path / "f.txt"
        source = mocksourcedata_local_text_file(location=test_file)
        test_file.write_text("hello")

        r = retriever.LocalFileRetriever(source=source)
        assert r.read() == b"hello"

        newdir = tmp_path / "out"
        newdir.mkdir(parents=True)
        result = r._save(newdir)
        assert result.exists()
        assert _get_sha256_hash(result) == _get_sha256_hash(test_file)
        assert result.read_text() == "hello"


class TestRemoteRepositoryRetriever:
    def test_read_raises(self, mocksourcedata_remote_repo):
        """Tests that RemoteRepositoryRetriever.read raises a NotImplementedError"""
        source = mocksourcedata_remote_repo()
        r = retriever.RemoteRepositoryRetriever(source)
        with pytest.raises(NotImplementedError):
            r.read()

    def test_save_clones_and_checkouts(self, tmp_path, mocksourcedata_remote_repo):
        """Tests that RemoteRepositoryRetriever.save() clones the repo and checks out the target."""
        source = mocksourcedata_remote_repo()
        with (
            mock.patch("cstar.io.retriever._clone") as mock_clone,
            mock.patch("cstar.io.retriever._checkout") as mock_checkout,
        ):
            r = retriever.RemoteRepositoryRetriever(source)
            result = r._save(tmp_path)

        mock_clone.assert_called_once_with(
            source_repo=source.location, local_path=tmp_path
        )
        mock_checkout.assert_called_once_with(
            source_repo=source.location,
            local_path=tmp_path,
            checkout_target="test_target",
        )
        assert result == tmp_path

    def test_save_raises_if_dir_not_empty(self, tmp_path, mocksourcedata_remote_repo):
        """Tests that RemoteRepositoryRetriever.save raises a ValueError if target_dir is not empty."""
        source = mocksourcedata_remote_repo()
        (tmp_path / "afile").write_text("x")
        r = retriever.RemoteRepositoryRetriever(source)
        with pytest.raises(ValueError):
            r._save(tmp_path)
