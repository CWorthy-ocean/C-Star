from datetime import datetime
from pathlib import Path
from textwrap import dedent
from unittest import mock

from cstar.io.source_data import SourceData
from cstar.io.staged_data import StagedData
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
        ind = fake_inputdataset_local
        assert ind.source.basename == "local_file.nc", (
            "Expected basename to be 'local_file.nc'"
        )
        assert isinstance(ind, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )
        assert ind.start_date == datetime(2024, 10, 22, 12, 34, 56)
        assert ind.end_date == datetime(2024, 12, 31, 23, 59, 59)

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
        ind = fake_inputdataset_remote
        assert ind.source.basename == "remote_file.nc", (
            "Expected basename to be 'remote_file.nc'"
        )
        assert ind.source.file_hash == "abc123", "Expected file_hash to be 'abc123'"
        assert isinstance(ind, FakeInputDataset), (
            "Expected an instance of FakeInputDataset"
        )
        assert ind


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
    Source file hash: test_target
    start date: 2024-10-22 12:34:56
    end date: 2024-12-31 23:59:59
    Local copy: None"""
        )

        assert str(fake_inputdataset_local) == expected_str

    def test_local_repr(self, fake_inputdataset_local):
        """Test the repr representation of a local InputDataset."""
        expected_repr = dedent(
            """\
    FakeInputDataset(
    location = 'some/local/source/path/local_file.nc',
    file_hash = 'test_target',
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
    start date: 2024-10-22 12:34:56
    end date: 2024-12-31 23:59:59
    Local copy: None"""
        )
        assert str(fake_inputdataset_remote) == expected_str

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    @mock.patch.object(FakeInputDataset, "working_copy", new_callable=mock.PropertyMock)
    def test_str_with_working_copy(
        self, mock_working_copy, mock_exists_locally, fake_inputdataset_local
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
        mock_staged = mock.create_autospec(StagedData)
        mock_staged.path = "/some/local/path"
        mock_working_copy.return_value = mock_staged
        fake_inputdataset_local.working_copy.path = Path("/some/local/path")

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "Local copy: /some/local/path" in str(fake_inputdataset_local), (
            f"substring not in {str(fake_inputdataset_local)}"
        )

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )  # Mock exists_locally
    @mock.patch.object(FakeInputDataset, "working_copy", new_callable=mock.PropertyMock)
    def test_repr_with_working_copy(
        self, mock_working_copy, mock_exists_locally, fake_inputdataset_local
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
        mock_staged = mock.create_autospec(StagedData)
        mock_staged.path = "/some/local/path"
        mock_working_copy.return_value = mock_staged
        fake_inputdataset_local.working_copy.path = Path("/some/local/path")

        # Simulate exists_locally being True
        mock_exists_locally.return_value = True
        assert "State: <working_copy = /some/local/path>" in repr(
            fake_inputdataset_local
        ), f"substring not in {repr(fake_inputdataset_local)}"


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

    @mock.patch.object(
        FakeInputDataset, "exists_locally", new_callable=mock.PropertyMock
    )
    @mock.patch.object(SourceData, "stage")
    def test_get_when_file_exists(
        self,
        mock_stage,
        mock_exists_locally,
        fake_inputdataset_local,
        mock_path_resolve,
    ):
        """Test the InputDataset.get method when the target file already exists."""
        # Hardcode the resolved path for local_dir
        local_dir_resolved = Path("/resolved/local/dir")

        # Mock `exists_locally` to return True
        mock_exists_locally.return_value = True

        # Call the `get` method
        fake_inputdataset_local.get(local_dir_resolved)
        mock_stage.assert_not_called()
