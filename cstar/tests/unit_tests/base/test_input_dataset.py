import logging
import stat
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from cstar.tests.unit_tests.fake_abc_subclasses import FakeInputDataset


class TestInputDatasetInit:
    """Test class for the initialization of the InputDataset class.

    Tests
    -----
    test_local_init
       Test initialization of an InputDataset with a local source
    test_remote_init
       Test initialization of an InputDataset with a remote source.
    test_remote_requires_file_hash
       Test that a remote InputDataset raises an error when the file hash is missing
    """

    def test_local_init(self, fake_inputdataset_local):
        """Test initialization of an InputDataset with a local source.

        Fixtures
        --------
        fake_inputdataset_local: FakeInputDataset instance for local files.

        Asserts
        -------
        - The `location_type` is "path".
        - The `basename` is "local_file.nc".
        - The dataset is an instance of FakeInputDataset.
        """
        assert fake_inputdataset_local.source.location_type == "path", (
            "Expected location_type to be 'path'"
        )
        assert fake_inputdataset_local.source.basename == "local_file.nc", (
            "Expected basename to be 'local_file.nc'"
        )
        assert isinstance(fake_inputdataset_local, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )

    def test_remote_init(self, fake_inputdataset_remote):
        """Test initialization of an InputDataset with a remote source.

        Fixtures
        --------
        fake_inputdataset_remote: FakeInputDataset instance for remote files.

        Asserts
        -------
        - The `location_type` is "url".
        - The `basename` is "remote_file.nc".
        - The `file_hash` is set to "abc123".
        - The dataset is an instance of FakeInputDataset.
        """
        assert fake_inputdataset_remote.source.location_type == "url", (
            "Expected location_type to be 'url'"
        )
        assert fake_inputdataset_remote.source.basename == "remote_file.nc", (
            "Expected basename to be 'remote_file.nc'"
        )
        assert fake_inputdataset_remote.source.file_hash == "abc123", (
            "Expected file_hash to be 'abc123'"
        )
        assert isinstance(fake_inputdataset_remote, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )

    def test_remote_requires_file_hash(self, fake_inputdataset_remote):
        """Test that a remote InputDataset raises an error when the file hash is
        missing.

        This test confirms that a ValueError is raised if a remote dataset is created without a required file hash.

        Fixtures
        --------
        fake_inputdataset_remote: FakeInputDataset instance for remote files.

        Asserts
        -------
        - A ValueError is raised if the `file_hash` is missing for a remote dataset.
        - The exception message matches the expected error message.
        """
        with pytest.raises(ValueError) as exception_info:
            FakeInputDataset("http://example.com/remote_file.nc")

        expected_message = (
            "Cannot create InputDataset for \n http://example.com/remote_file.nc:\n "
            + "InputDataset.source.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
            + "A file hash is required to verify non-plaintext files downloaded from remote sources."
        )

        assert str(exception_info.value) == expected_message


class TestStrAndRepr:
    """Test class for the __str__ and __repr__ methods on an InputDataset.

    Tests
    -----
    test_local_str
       Test the string representation of an InputDataset with a local source
    test_local_repr
       Test the repr representation of an InputDataset with a local source
    test_remote_repr
       Test the repr representation of an InputDataset with a remote source
    test_remote_str
       Test the string representation of an InputDataset with a remote source
    test_str_with_working_path
       Test the string representation when the InputDataset.working_path attribute is defined
    test_repr_with_working_path
       Test the repr representation when the InputDataset.working_path attribute is defined
    """

    def test_local_str(self, fake_inputdataset_local):
        """Test the string representation of a local InputDataset."""
        expected_str = dedent(
            """\
    ----------------
    FakeInputDataset
    ----------------
    Source location: some/local/source/path/local_file.nc
    start_date: 2024-10-22 12:34:56
    end_date: 2024-12-31 23:59:59
    Working path: None ( does not yet exist. Call InputDataset.get() )"""
        )
        assert str(fake_inputdataset_local) == expected_str

    def test_local_repr(self, fake_inputdataset_local):
        """Test the repr representation of a local InputDataset."""
        expected_repr = dedent(
            """\
    FakeInputDataset(
    location = 'some/local/source/path/local_file.nc',
    file_hash = None,
    start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
    end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
    )"""
        )
        assert repr(fake_inputdataset_local) == expected_repr

    def test_remote_repr(self, fake_inputdataset_remote):
        """Test the repr representation of a remote InputDataset."""
        expected_repr = dedent(
            """\
    FakeInputDataset(
    location = 'http://example.com/remote_file.nc',
    file_hash = 'abc123',
    start_date = datetime.datetime(2024, 10, 22, 12, 34, 56),
    end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
    )"""
        )
        assert repr(fake_inputdataset_remote) == expected_repr

    def test_remote_str(self, fake_inputdataset_remote):
        """Test the string representation of a remote InputDataset."""
        expected_str = dedent(
            """\
    ----------------
    FakeInputDataset
    ----------------
    Source location: http://example.com/remote_file.nc
    Source file hash: abc123
    start_date: 2024-10-22 12:34:56
    end_date: 2024-12-31 23:59:59
    Working path: None ( does not yet exist. Call InputDataset.get() )"""
        )
        assert str(fake_inputdataset_remote) == expected_str

    @mock.patch.object(
        FakeInputDataset, "local_hash", new_callable=mock.PropertyMock
    )  # Mock local_hash
    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    def test_str_with_working_path(
        self, mock_exists_locally, mock_local_hash, fake_inputdataset_local
    ):
        """Test the string output when the working_path attribute is defined.

        This test verifies that the string output includes the correct working path
        and whether the path exists or not, mocking the `exists_locally` and `local_hash`
        properties to simulate both cases.

        Fixtures
        --------
        fake_inputdataset_local: FakeInputDataset instance for local files.

        Asserts
        -------
        - The string output includes the working path when it is set.
        - If the working path exists, the string includes "(exists)".
        - If the working path does not exist, the string includes a message indicating the path does not yet exist.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")

        # Mock local_hash to prevent triggering _get_sha256_hash
        mock_local_hash.return_value = {"mocked_path": "mocked_hash"}

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "Working path: /some/local/path" in str(fake_inputdataset_local)
        assert "(exists)" in str(fake_inputdataset_local)

        # Simulate exists_locally being False
        mock_exists_locally.return_value = False
        assert "Working path: /some/local/path" in str(fake_inputdataset_local)
        assert " ( does not yet exist. Call InputDataset.get() )" in str(
            fake_inputdataset_local
        )

    @mock.patch.object(
        FakeInputDataset, "local_hash", new_callable=mock.PropertyMock
    )  # Mock local_hash
    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    def test_repr_with_working_path(
        self, mock_exists_locally, mock_local_hash, fake_inputdataset_local
    ):
        """Test the repr output when the working_path attribute is defined.

        This test verifies that the repr output correctly includes the working path and indicates
        whether or not the path exists, mocking the `exists_locally` and `local_hash` properties
        to simulate both cases.

        Fixtures
        --------
        fake_inputdataset_local: FakeInputDataset instance for local files.

        Asserts
        -------
        - If the working path exists, the repr includes the path with no additional notes.
        - If the working path does not exist, the repr includes a note indicating the path does not exist.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")

        # Mock local_hash to prevent triggering _get_sha256_hash
        mock_local_hash.return_value = {"mocked_path": "mocked_hash"}

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert (
            "State: <working_path = /some/local/path, local_hash = {'mocked_path': 'mocked_hash'}>"
            in repr(fake_inputdataset_local)
        )

        # Simulate exists_locally being False
        mock_exists_locally.return_value = False
        mock_local_hash.return_value = None
        assert "State: <working_path = /some/local/path (does not exist)>" in repr(
            fake_inputdataset_local
        )


class TestExistsLocally:
    """Test class for the 'exists_locally' property.

    Tests
    -----
    test_no_working_path_or_stat_cache
       Test exists_locally when no working path or stat cache is defined
    test_file_does_not_exist
       Test exists_locally when the file does not exist
    test_no_cached_stats
       Test exists_locally when no cached stats are available
    test_size_mismatch
       Test exists_locally when the file size does not match the cached value
    test_modification_time_mismatch_with_hash_match
       Test exists_locally when the modification time does not match but the hash
    test_modification_time_and_hash_mismatch
       Test exists_locally when both modification time and hash do not match.
    test_all_checks_pass
       Test exists_locally when all checks pass
    """

    def test_no_working_path_or_stat_cache(self, fake_inputdataset_local):
        """Test exists_locally when no working path or stat cache is defined.

        Asserts:
        - exists_locally is False when `working_path` or `_local_file_stat_cache` is None.
        """
        fake_inputdataset_local.working_path = None
        fake_inputdataset_local._local_file_stat_cache = None
        assert not fake_inputdataset_local.exists_locally, (
            "Expected exists_locally to be False when working_path or stat cache is None"
        )

    def test_file_does_not_exist(self, fake_inputdataset_local):
        """Test exists_locally when the file does not exist.

        Asserts:
        - exists_locally is False when any file in `working_path` does not exist.
        """
        fake_inputdataset_local.working_path = Path("/some/nonexistent/path")
        fake_inputdataset_local._local_file_stat_cache = {
            Path("/some/nonexistent/path"): None
        }

        with mock.patch.object(Path, "exists", return_value=False):
            assert not fake_inputdataset_local.exists_locally, (
                "Expected exists_locally to be False when the file does not exist"
            )

    def test_no_cached_stats(self, fake_inputdataset_local):
        """Test exists_locally when no cached stats are available.

        Asserts:
        - exists_locally is False when no stats are cached for a file.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")
        fake_inputdataset_local._local_file_stat_cache = {}

        with mock.patch.object(Path, "exists", return_value=True):
            assert not fake_inputdataset_local.exists_locally, (
                "Expected exists_locally to be False when no cached stats are available"
            )

    def test_size_mismatch(self, fake_inputdataset_local):
        """Test exists_locally when the file size does not match the cached value.

        Asserts:
        - exists_locally is False when the file size does not match.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")
        fake_inputdataset_local._local_file_stat_cache = {
            Path("/some/local/path"): mock.Mock(st_size=100)
        }

        with mock.patch.object(Path, "exists", return_value=True):
            with mock.patch.object(Path, "stat", return_value=mock.Mock(st_size=200)):
                assert not fake_inputdataset_local.exists_locally, (
                    "Expected exists_locally to be False when file size does not match cached stats"
                )

    def test_modification_time_mismatch_with_hash_match(self, fake_inputdataset_local):
        """Test exists_locally when the modification time does not match but the hash
        matches.

        Asserts:
        - exists_locally is True when the modification time does not match but the hash matches.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")
        fake_inputdataset_local._local_file_stat_cache = {
            Path("/some/local/path"): mock.Mock(st_size=100, st_mtime=12345)
        }
        fake_inputdataset_local._local_file_hash_cache = {
            Path("/some/local/path"): "mocked_hash"
        }

        with mock.patch.object(Path, "exists", return_value=True):
            with mock.patch.object(
                Path,
                "stat",
                return_value=mock.Mock(
                    st_size=100, st_mtime=54321, st_mode=stat.S_IFREG
                ),
            ):
                with mock.patch(
                    "cstar.base.input_dataset._get_sha256_hash",
                    return_value="mocked_hash",
                ):
                    assert fake_inputdataset_local.exists_locally, (
                        "Expected exists_locally to be True when modification time mismatches but hash matches"
                    )

    def test_modification_time_and_hash_mismatch(self, fake_inputdataset_local):
        """Test exists_locally when both modification time and hash do not match.

        Asserts:
        - exists_locally is False when both modification time and hash do not match.
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")
        fake_inputdataset_local._local_file_stat_cache = {
            Path("/some/local/path"): mock.Mock(st_size=100, st_mtime=12345)
        }
        fake_inputdataset_local._local_file_hash_cache = {
            Path("/some/local/path"): "mocked_hash"
        }

        with mock.patch.object(Path, "exists", return_value=True):
            with mock.patch.object(
                Path,
                "stat",
                return_value=mock.Mock(
                    st_size=100, st_mtime=54321, st_mode=stat.S_IFREG
                ),
            ):
                with mock.patch(
                    "cstar.base.input_dataset._get_sha256_hash",
                    return_value="different_hash",
                ):
                    assert not fake_inputdataset_local.exists_locally, (
                        "Expected exists_locally to be False when both modification time and hash do not match"
                    )

    def test_all_checks_pass(self, fake_inputdataset_local):
        """Test exists_locally when all checks pass.

        Asserts:
        - exists_locally is True when the file exists, size matches, and hash matches (if needed).
        """
        fake_inputdataset_local.working_path = Path("/some/local/path")
        fake_inputdataset_local._local_file_stat_cache = {
            Path("/some/local/path"): mock.Mock(st_size=100, st_mtime=12345)
        }
        fake_inputdataset_local._local_file_hash_cache = {
            Path("/some/local/path"): "mocked_hash"
        }

        with mock.patch.object(Path, "exists", return_value=True):
            with mock.patch.object(
                Path, "stat", return_value=mock.Mock(st_size=100, st_mtime=12345)
            ):
                assert fake_inputdataset_local.exists_locally, (
                    "Expected exists_locally to be True when all checks pass"
                )


def test_to_dict(fake_inputdataset_remote):
    """Test the InputDataset.to_dict method, using a remote InputDataset as an example.

    Fixtures
    --------
    fake_inputdataset_remote: FakeInputDataset instance for remote files.

    Asserts
    -------
    - The dictionary returned matches a known expected dictionary
    """
    assert fake_inputdataset_remote.to_dict() == {
        "location": "http://example.com/remote_file.nc",
        "file_hash": "abc123",
        "start_date": "2024-10-22 12:34:56",
        "end_date": "2024-12-31 23:59:59",
    }


class TestInputDatasetGet:
    """Test class for the InputDataset.get method.

    This test class covers scenarios for both local and remote datasets and verifies the
    behavior of the InputDataset.get method, including handling of existing files,
    file downloading, and symbolic link creation.

    Attributes
    ----------
    - target_dir: Simulated directory for storing files.
    - target_filepath_local: Path for local files in the target directory.
    - target_filepath_remote: Path for remote files in the target directory.

    Tests
    -----
    - test_get_when_file_exists
    - test_get_with_local_source
    - test_get_local_wrong_hash
    - test_get_with_remote_source
    - test_get_remote_with_no_file_hash
    """

    # Common attributes
    target_dir = Path("/some/local/target/dir")
    target_filepath_local = target_dir / "local_file.nc"
    target_filepath_remote = target_dir / "remote_file.nc"

    def setup_method(self, fake_inputdataset_local):
        """Setup method to patch various file system operations used in the get method.

        This method mocks file system interactions to prevent actual disk operations during testing.

        Mocks
        -----
        - Path.mkdir: Mocks directory creation to avoid creating real directories.
        - Path.symlink_to: Mocks symbolic link creation to avoid modifying the file system.
        - Path.exists: Mocks file existence checks to simulate whether files or directories already exist.
        """
        # Patch Path.mkdir globally for all tests in this class to avoid file system interaction
        self.patch_mkdir = mock.patch.object(Path, "mkdir")
        self.mock_mkdir = self.patch_mkdir.start()

        # Patch Path.symlink_to globally for all tests
        self.patch_symlink_to = mock.patch.object(Path, "symlink_to")
        self.mock_symlink_to = self.patch_symlink_to.start()

        # Patch Path.exists globally for all tests but let each test set the return_value
        self.patcher_exists = mock.patch.object(Path, "exists")
        self.mock_exists = self.patcher_exists.start()

    def teardown_method(self):
        """Stops all patches started in setup_method."""
        mock.patch.stopall()

    @mock.patch("cstar.base.input_dataset._get_sha256_hash", return_value="mocked_hash")
    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )
    def test_get_when_file_exists(
        self,
        mock_exists_locally,
        mock_get_hash,
        fake_inputdataset_local,
        mock_path_resolve,
    ):
        """Test the InputDataset.get method when the target file already exists."""
        # Hardcode the resolved path for local_dir
        local_dir_resolved = Path("/resolved/local/dir")
        target_path = local_dir_resolved / "local_file.nc"

        # Mock `exists_locally` to return True
        mock_exists_locally.return_value = True

        # Set `working_path` to match `target_path`
        fake_inputdataset_local.working_path = target_path

        # Call the `get` method
        fake_inputdataset_local.get(local_dir_resolved)

        # Ensure `_get_sha256_hash` was not called
        mock_get_hash.assert_not_called()

        # Assert `working_path` remains unchanged
        assert fake_inputdataset_local.working_path == target_path, (
            f"Expected working_path to remain as {target_path}, "
            f"but got {fake_inputdataset_local.working_path}"
        )

        mock_path_resolve.assert_called()

    @mock.patch("cstar.base.input_dataset._get_sha256_hash", return_value="mocked_hash")
    def test_get_with_local_source(
        self, mock_get_hash, fake_inputdataset_local, mock_path_resolve
    ):
        """Test the InputDataset.get method with a local source file.

        This test verifies that when the source file is local, a symbolic link is
        created in the target directory and the working_path is updated accordingly.
        """
        # Define resolved paths for local_dir and source file
        source_filepath = Path(fake_inputdataset_local.source.location)

        # Mock Path.exists to simulate that the file doesn't exist yet in local_dir
        self.mock_exists.return_value = False

        # Mock Path.stat to simulate valid file stats for target_path
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        with mock.patch.object(Path, "stat", return_value=mock_stat_result):
            # Call the get method
            fake_inputdataset_local.get(self.target_dir)

            # Assert that a symbolic link was created with the resolved path
            self.mock_symlink_to.assert_called_once_with(source_filepath)

            # Assert that working_path is updated to the resolved target path
            expected_target_path = self.target_dir / "local_file.nc"

            assert fake_inputdataset_local.working_path == expected_target_path, (
                f"Expected working_path to be {expected_target_path}, but got {fake_inputdataset_local.working_path}"
            )

        mock_path_resolve.assert_called()

    @mock.patch("cstar.base.input_dataset._get_sha256_hash", return_value="mocked_hash")
    def test_get_local_wrong_hash(
        self, mock_get_hash, fake_inputdataset_local, mock_path_resolve
    ):
        """Test the `get` method with a bogus file_hash for local sources."""
        # Assign a bogus file hash
        fake_inputdataset_local.source._file_hash = "bogus_hash"

        source_filepath_local = Path(fake_inputdataset_local.source.location)

        # Mock Path.exists to simulate that the file doesn't yet exist
        self.mock_exists.return_value = False

        # Call `get` and assert it raises a ValueError
        with pytest.raises(
            ValueError, match="The provided file hash.*does not match.*"
        ):
            fake_inputdataset_local.get(self.target_dir)

        # Ensure `_get_sha256_hash` was called with the source path
        mock_get_hash.assert_called_once_with(source_filepath_local)

        mock_path_resolve.assert_called()

    @mock.patch("pooch.create")
    @mock.patch("pooch.HTTPDownloader")
    @mock.patch("cstar.base.input_dataset._get_sha256_hash", return_value="mocked_hash")
    def test_get_with_remote_source(
        self,
        mock_get_hash,
        mock_downloader,
        mock_pooch_create,
        fake_inputdataset_remote,
    ):
        """Test the InputDataset.get method with a remote source file.

        This test verifies that when the source file is remote, the file is downloaded
        correctly using pooch, and the working_path is updated to the downloaded file
        path.
        """
        # Define resolved paths
        target_filepath_remote = self.target_dir / "remote_file.nc"

        # Mock Path.stat to simulate file stats for target_path
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        with mock.patch.object(Path, "stat", return_value=mock_stat_result):
            # Mock Path.exists to simulate the target file does not yet exist
            self.mock_exists.return_value = False

            # Mock Path.resolve to return the correct target directory
            with mock.patch.object(Path, "resolve", return_value=self.target_dir):
                # Create a mock Pooch instance and mock the fetch method
                mock_pooch_instance = mock.Mock()
                mock_pooch_create.return_value = mock_pooch_instance
                mock_fetch = mock.Mock()
                mock_pooch_instance.fetch = mock_fetch

                # Call the get method
                fake_inputdataset_remote.get(self.target_dir)

                # Ensure pooch.create was called correctly
                mock_pooch_create.assert_called_once_with(
                    path=self.target_dir,
                    base_url="http://example.com/",
                    registry={"remote_file.nc": "abc123"},
                )

                # Ensure fetch was called with the mocked downloader
                mock_fetch.assert_called_once_with(
                    "remote_file.nc", downloader=mock_downloader.return_value
                )

                # Assert that working_path is updated to the expected target path
                assert (
                    fake_inputdataset_remote.working_path == target_filepath_remote
                ), (
                    f"Expected working_path to be {target_filepath_remote}, but got {fake_inputdataset_remote.working_path}"
                )

    def test_get_remote_with_no_file_hash(
        self, fake_inputdataset_remote, mock_path_resolve
    ):
        """Test the InputDataset.get method when no file_hash is provided for a remote
        source.

        This test verifies that the get method raises a ValueError when a remote source file is
        attempted to be fetched without a defined file_hash, as file verification is necessary.

        Fixtures
        --------
        fake_inputdataset_remote: FakeInputDataset instance for remote files.
        mock_exists: Mock for Path.exists to simulate that the target file does not yet exist.
        mock_path_resolve: Mock for Path.resolve to simulate resolving the target directory.

        Asserts
        -------
        - A ValueError is raised when no file_hash is provided for a remote file.
        - The error message matches the expected message regarding the missing file_hash.
        """
        fake_inputdataset_remote.source._file_hash = None
        self.mock_exists.return_value = False

        expected_message = (
            "Source type is URL but no file hash was not provided. Cannot proceed."
        )

        with pytest.raises(ValueError) as exception_info:
            fake_inputdataset_remote.get(self.target_dir)
        assert str(exception_info.value) == expected_message

        mock_path_resolve.assert_called()


class TestLocalHash:
    """Test class for the `local_hash` property.

    Mocks
    -----
    Path.resolve()
       Mocks calls to resolve any mocked paths with pathlib
    cstar.utils._get_sha256_hash
       Mocks calls to compute sha256 checksums using cstar.utils
    InputDataset.exists_locally
       Mocks calls to the boolean `exists_locally` property of InputDataset

    Tests
    -----
    test_local_hash_single_file
       Test `local_hash` calculation for a single file.
    test_local_hash_cached
       Test `local_hash` when the hash is cached
    test_local_hash_no_working_path
       Test `local_hash` when no working path is set.
    test_local_hash_multiple_files
       Test `local_hash` calculation for multiple files
    """

    def setup_method(self):
        """Set up common mocks for `local_hash` tests."""
        # Patch _get_sha256_hash
        self.patcher_get_hash = mock.patch("cstar.base.input_dataset._get_sha256_hash")
        self.mock_get_hash = self.patcher_get_hash.start()
        self.mock_get_hash.return_value = "mocked_hash"

        # Patch exists_locally
        self.patcher_exists_locally = mock.patch(
            "cstar.base.input_dataset.InputDataset.exists_locally",
            new_callable=mock.PropertyMock,
        )
        self.mock_exists_locally = self.patcher_exists_locally.start()
        self.mock_exists_locally.return_value = True

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_local_hash_single_file(self, fake_inputdataset_local, log: logging.Logger):
        """Test `local_hash` calculation for a single file."""
        fake_inputdataset_local._local_file_hash_cache = {}
        fake_inputdataset_local.working_path = Path("/some/local/path")

        result = fake_inputdataset_local.local_hash

        # Check that the result uses the resolved path
        assert result == {Path("/some/local/path"): "mocked_hash"}, (
            f"Expected calculated local_hash, but got {result}"
        )

        # Verify _get_sha256_hash was called with the resolved path
        self.mock_get_hash.assert_called_once_with(Path("/some/local/path"))

    def test_local_hash_cached(self, fake_inputdataset_local):
        """Test `local_hash` when the hash is cached."""
        cached_hash = {Path("/resolved/local/path"): "cached_hash"}
        fake_inputdataset_local._local_file_hash_cache = cached_hash

        result = fake_inputdataset_local.local_hash

        assert result == cached_hash, "Expected the cached hash to be returned."
        self.mock_get_hash.assert_not_called()

    def test_local_hash_no_working_path(self, fake_inputdataset_local):
        """Test `local_hash` when no working path is set."""
        fake_inputdataset_local.working_path = None

        result = fake_inputdataset_local.local_hash

        assert result == {}, (
            "Expected local_hash to be empty when working_path is not set."
        )
        self.mock_get_hash.assert_not_called()

    def test_local_hash_multiple_files(
        self, fake_inputdataset_local, mock_path_resolve
    ):
        """Test `local_hash` calculation for multiple files."""
        expected_path_1 = Path("/some/local/path1")
        expected_path_2 = Path("/some/local/path2")

        fake_inputdataset_local._local_file_hash_cache = {}
        fake_inputdataset_local.working_path = [
            expected_path_1,
            expected_path_2,
        ]
        result = fake_inputdataset_local.local_hash

        assert result == {
            expected_path_1: "mocked_hash",
            expected_path_2: "mocked_hash",
        }, f"Expected calculated local_hash for multiple files, but got {result}"

        self.mock_get_hash.assert_has_calls(
            [
                mock.call(expected_path_1),
                mock.call(expected_path_2),
            ],
            any_order=True,
        )

        mock_path_resolve.assert_called()
