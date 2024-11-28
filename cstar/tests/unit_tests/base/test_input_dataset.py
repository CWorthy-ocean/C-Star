import pytest
from unittest import mock
from pathlib import Path
from cstar.base import InputDataset
from cstar.base.datasource import DataSource


class MockInputDataset(InputDataset):
    """Mock subclass of the InputDataset abstract base class.

    Since InputDataset is an abstract base class, this mock class is needed to allow
    instantiation for testing purposes. It inherits from InputDataset without adding any
    new behavior, serving only to allow tests to create and manipulate instances.
    """

    pass


@pytest.fixture
def local_input_dataset():
    """Fixture to provide a mock local InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a local dataset,
    initializing it with relevant attributes like location, start date, and end date.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'path'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'local_file.nc'

    Yields
    ------
    MockInputDataset: Instance representing a local input dataset for testing.
    """
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        mock_location_type.return_value = "path"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "local_file.nc"

        dataset = MockInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def remote_input_dataset():
    """Fixture to provide a mock remote InputDataset instance.

    This fixture patches properties of the DataSource class to simulate a remote dataset,
    initializing it with attributes such as URL location, file hash, and date range.

    Mocks
    -----
    - Mocked DataSource.location_type property returning 'url'
    - Mocked DataSource.source_type property returning 'netcdf'
    - Mocked DataSource.basename property returning 'remote_file.nc'

    Yields
    ------
    MockInputDataset: Instance representing a remote input dataset for testing.
    """

    # Using context managers to patch properties on DataSource
    with (
        mock.patch.object(
            DataSource, "location_type", new_callable=mock.PropertyMock
        ) as mock_location_type,
        mock.patch.object(
            DataSource, "source_type", new_callable=mock.PropertyMock
        ) as mock_source_type,
        mock.patch.object(
            DataSource, "basename", new_callable=mock.PropertyMock
        ) as mock_basename,
    ):
        # Mock property return values for a remote file (URL)
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "netcdf"
        mock_basename.return_value = "remote_file.nc"

        # Create the InputDataset instance; it will use the mocked DataSource
        dataset = MockInputDataset(
            location="http://example.com/remote_file.nc",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        # Yield the dataset for use in the test
        yield dataset


def test_local_init(local_input_dataset):
    """Test initialization of a local InputDataset.

    Fixtures
    --------
    local_input_dataset: MockInputDataset instance for local files.

    Asserts
    -------
    - The `location_type` is "path".
    - The `basename` is "local_file.nc".
    - The dataset is an instance of MockInputDataset.
    """

    assert (
        local_input_dataset.source.location_type == "path"
    ), "Expected location_type to be 'path'"
    assert (
        local_input_dataset.source.basename == "local_file.nc"
    ), "Expected basename to be 'local_file.nc'"
    assert isinstance(
        local_input_dataset, MockInputDataset
    ), "Expected an instance of MockInputDataset"


def test_remote_init(remote_input_dataset):
    """Test initialization of a remote InputDataset.

    Fixtures
    --------
    remote_input_dataset: MockInputDataset instance for remote files.

    Asserts
    -------
    - The `location_type` is "url".
    - The `basename` is "remote_file.nc".
    - The `file_hash` is set to "abc123".
    - The dataset is an instance of MockInputDataset.
    """
    assert (
        remote_input_dataset.source.location_type == "url"
    ), "Expected location_type to be 'url'"
    assert (
        remote_input_dataset.source.basename == "remote_file.nc"
    ), "Expected basename to be 'remote_file.nc'"
    assert (
        remote_input_dataset.file_hash == "abc123"
    ), "Expected file_hash to be 'abc123'"
    assert isinstance(
        remote_input_dataset, MockInputDataset
    ), "Expected an instance of MockInputDataset"


def test_remote_requires_file_hash(remote_input_dataset):
    """Test that a remote InputDataset raises an error when the file hash is missing.

    This test confirms that a ValueError is raised if a remote dataset is created without a required file hash.

    Fixtures
    --------
    remote_input_dataset: MockInputDataset instance for remote files.

    Asserts
    -------
    - A ValueError is raised if the `file_hash` is missing for a remote dataset.
    - The exception message matches the expected error message.
    """
    with pytest.raises(ValueError) as exception_info:
        MockInputDataset("http://example.com/remote_file.nc")

    expected_message = (
        "Cannot create InputDataset for \n http://example.com/remote_file.nc:\n "
        + "InputDataset.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
        + "A file hash is required to verify files downloaded from remote sources."
    )

    assert str(exception_info.value) == expected_message


def test_local_str(local_input_dataset):
    """Test the string representation of a local InputDataset."""
    expected_str = """----------------
MockInputDataset
----------------
Source location: some/local/source/path/local_file.nc
start_date: 2024-10-22 12:34:56
end_date: 2024-12-31 23:59:59
Working path: None ( does not yet exist. Call InputDataset.get() )"""
    assert str(local_input_dataset) == expected_str


def test_local_repr(local_input_dataset):
    """Test the repr representation of a local InputDataset."""
    expected_repr = """MockInputDataset(
location = 'some/local/source/path/local_file.nc',
file_hash = None
start_date = datetime.datetime(2024, 10, 22, 12, 34, 56)
end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
)"""
    assert repr(local_input_dataset) == expected_repr


def test_remote_repr(remote_input_dataset):
    """Test the repr representation of a remote InputDataset."""
    expected_repr = """MockInputDataset(
location = 'http://example.com/remote_file.nc',
file_hash = abc123
start_date = datetime.datetime(2024, 10, 22, 12, 34, 56)
end_date = datetime.datetime(2024, 12, 31, 23, 59, 59)
)"""
    assert repr(remote_input_dataset) == expected_repr


def test_remote_str(remote_input_dataset):
    """Test the string representation of a remote InputDataset."""
    expected_str = """----------------
MockInputDataset
----------------
Source location: http://example.com/remote_file.nc
file_hash: abc123
start_date: 2024-10-22 12:34:56
end_date: 2024-12-31 23:59:59
Working path: None ( does not yet exist. Call InputDataset.get() )"""
    assert str(remote_input_dataset) == expected_str


def test_str_with_working_path(local_input_dataset):
    """Test the string output when the working_path attribute is defined.

    This test verifies that the string output includes the correct working path
    and whether the path exists or not, mocking Path.exists() to simulate both
    cases.


    Fixtures
    --------
    local_input_dataset: MockInputDataset instance for local files.

    Asserts
    -------
    - The string output includes the working path when it is set.
    - If the working path exists, the string includes "(exists)".
    - If the working path does not exist, the string includes a message indicating the path does not yet exist.
    """
    local_input_dataset.working_path = Path("/some/local/path")
    with mock.patch.object(Path, "exists", return_value=True):
        assert "Working path: /some/local/path" in str(local_input_dataset)
        assert "(exists)" in str(local_input_dataset)
    with mock.patch.object(Path, "exists", return_value=False):
        assert "Working path: /some/local/path" in str(local_input_dataset)
        assert " ( does not yet exist. Call InputDataset.get() )" in str(
            local_input_dataset
        )


def test_repr_with_working_path(local_input_dataset):
    """Test the repr output when the working_path attribute is defined.

    This test verifies that the repr output correctly includes the working path and indicates
    whether or not the path exists, mocking Path.exists() to simulate both cases.

    Fixtures
    --------
    local_input_dataset: MockInputDataset instance for local files.

    Asserts
    -------
    - If the working path exists, the repr includes the path with no additional notes.
    - If the working path does not exist, the repr includes a note indicating the path does not exist.
    """
    local_input_dataset.working_path = Path("/some/local/path")
    with mock.patch.object(Path, "exists", return_value=True):
        assert "State: <working_path = /some/local/path>" in repr(local_input_dataset)
    with mock.patch.object(Path, "exists", return_value=False):
        assert "State: <working_path = /some/local/path (does not exist)>" in repr(
            local_input_dataset
        )


def test_exists_locally_with_single_path(local_input_dataset):
    """Test the exists_locally property when working_path describes a single file.

    This test verifies InputDataset.exists_locally correctly reflects whether the
    specified working path exists or not, mocking Path.exists() in each case.

    Fixtures
    --------
    local_input_dataset: MockInputDataset instance for local files.

    Asserts
    -------
    - exists_locally is True when the working path exists.
    - exists_locally is False when the working path does not exist.
    """
    local_input_dataset.working_path = Path(
        "/some/path/to/file"
    )  # Simulating a single path

    # Mock Path.exists() to return True
    with mock.patch.object(Path, "exists", return_value=True):
        assert (
            local_input_dataset.exists_locally
        ), "Expected exists_locally to be True when the path exists"

    # Mock Path.exists() to return False
    with mock.patch.object(Path, "exists", return_value=False):
        assert (
            not local_input_dataset.exists_locally
        ), "Expected exists_locally to be False when the path does not exist"


def test_exists_locally_with_list_of_paths(local_input_dataset):
    """Test the InputDataset.exists_locally property when working_path describes
    multiple files.

    Tests that InputDataset.exists_locally returns the correct result when given a list of paths,
    mocking Path.exists() to verify exists_locally is True when all paths exist and False when
    at least one does not.

    Fixtures
    --------
    local_input_dataset: MockInputDataset instance for local files.

    Asserts
    -------
    - exists_locally is True when all paths exist.
    - exists_locally is False when any of the paths do not exist.
    """

    # Simulating a list of paths:
    local_input_dataset.working_path = [
        Path("/some/path/to/file1"),
        Path("/some/path/to/file2"),
    ]

    # Mock Path.exists() to return True for all paths
    with mock.patch.object(Path, "exists", return_value=True):
        assert (
            local_input_dataset.exists_locally
        ), "Expected exists_locally to be True when all paths exist"

    # Mock Path.exists() to return False for one of the paths
    with mock.patch.object(Path, "exists", side_effect=[True, False]):
        assert (
            not local_input_dataset.exists_locally
        ), "Expected exists_locally to be False when one path does not exist"


def test_to_dict(remote_input_dataset):
    """Test the InputDataset.to_dict method, using a remote InputDataset as an example.

    Fixtures
    --------
    remote_input_dataset: MockInputDataset instance for remote files.

    Asserts
    -------
    - The dictionary returned matches a known expected dictionary
    """
    assert remote_input_dataset.to_dict() == {
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
    - test_get_when_filename_exists
    - test_get_with_local_source
    - test_get_with_remote_source
    - test_get_remote_with_no_file_hash
    """

    # Common attributes
    target_dir = Path("/some/local/target/dir")
    target_filepath_local = target_dir / "local_file.nc"
    target_filepath_remote = target_dir / "remote_file.nc"

    def setup_method(self, local_input_dataset):
        """Setup method to patch various file system operations used in the get method.

        This method mocks file system interactions to prevent actual disk operations during testing.

        Mocks
        -----
        - Path.mkdir: Mocks directory creation to avoid creating real directories.
        - Path.symlink_to: Mocks symbolic link creation to avoid modifying the file system.
        - Path.resolve: Mocks path resolution, allowing the test to control what paths are "resolved" to.
        - Path.exists: Mocks file existence checks to simulate whether files or directories already exist.
        """
        # Patch Path.mkdir globally for all tests in this class to avoid file system interaction
        self.patch_mkdir = mock.patch.object(Path, "mkdir")
        self.mock_mkdir = self.patch_mkdir.start()

        # Patch Path.symlink_to globally for all tests
        self.patch_symlink_to = mock.patch.object(Path, "symlink_to")
        self.mock_symlink_to = self.patch_symlink_to.start()

        # Patch Path.resolve globally for all tests but let each test set the side_effect
        self.patcher_resolve = mock.patch.object(Path, "resolve")
        self.mock_resolve = self.patcher_resolve.start()

        # Patch Path.exists globally for all tests but let each test set the return_value
        self.patcher_exists = mock.patch.object(Path, "exists")
        self.mock_exists = self.patcher_exists.start()

    def teardown_method(self):
        """Stops all patches started in setup_method."""
        mock.patch.stopall()

    def test_get_when_filename_exists(self, capsys, local_input_dataset):
        """Test the InputDataset.get method when the target file already exists.

        This test verifies that when a file with the same name already exists in the target directory,
        an appropriate message is printed and the working_path is updated to the existing file.

        Fixtures
        --------
        capsys: Pytest fixture to capture output to stdout.
        local_input_dataset: MockInputDataset instance for local files.
        mock_resolve: Mock for Path.resolve to simulate the target directory.
        mock_exists: Mock for Path.exists to simulate the existence of the target file.

        Asserts
        -------
        - The printed message matches the expected output, indicating the file already exists.
        - The working_path is correctly set to the existing file in the target directory.
        """
        self.mock_resolve.return_value = self.target_dir
        self.mock_exists.return_value = True

        local_input_dataset.get(self.target_dir)

        expected_message = "A file by the name of local_file.nc already exists at /some/local/target/dir\n"
        captured = capsys.readouterr()
        assert captured.out == expected_message
        assert local_input_dataset.working_path == self.target_dir / "local_file.nc"

    def test_get_with_local_source(self, local_input_dataset):
        """Test the InputDataset.get method with a local source file.

        This test verifies that when the source file is local, a symbolic link is created
        in the target directory and the working_path is updated accordingly.

        Fixtures
        --------
        local_input_dataset: MockInputDataset instance for local files.
        mock_exists: Mock for Path.exists to simulate that the target file does not yet exist.
        mock_resolve: Mock for Path.resolve to simulate resolving the source and target paths.
        mock_symlink_to: Mock for Path.symlink_to to simulate creating a symbolic link.

        Asserts
        -------
        - A symbolic link is created pointing to the local source file.
        - The working_path is correctly updated to the target file path.
        """
        source_filepath_local = Path(
            local_input_dataset.source.location
        )  # Source file in the local system

        # Mock Path.exists to simulate that the file doesn't exist yet in local_dir
        self.mock_exists.return_value = False
        self.mock_resolve.side_effect = [self.target_dir, source_filepath_local]

        # Call the get method
        local_input_dataset.get(self.target_dir)

        # Assert that a symbolic link was created with the resolved path
        self.mock_symlink_to.assert_called_once_with(source_filepath_local)

        # Assert that working_filepath is updated to the resolved target path
        assert (
            local_input_dataset.working_path == self.target_filepath_local
        ), f"Expected working_filepath to be {self.target_filepath_local}, but got {local_input_dataset.working_path}"

    def test_get_with_remote_source(self, remote_input_dataset):
        """Test the InputDataset.get method with a remote source file.

        This test verifies that when the source file is remote, the file is downloaded
        correctly using pooch, and the working_path is updated to the downloaded file path.

        Fixtures
        --------
        remote_input_dataset: MockInputDataset instance for remote files.
        mock_exists: Mock for Path.exists to simulate that the target file does not yet exist.
        mock_resolve: Mock for Path.resolve to simulate resolving the target directory.
        mock_pooch_create: Mock for pooch.create to simulate the pooch setup for file downloading.
        mock_downloader: Mock for pooch.HTTPDownloader to simulate the file download process.
        mock_fetch: Mock for pooch.fetch to simulate fetching the remote file.

        Asserts
        -------
        - The pooch.create and fetch methods are called with the correct arguments for downloading the file.
        - The working_path is correctly updated to the path of the downloaded file.
        """

        # Mock Path.exists to simulate that the file doesn't exist yet in local_dir
        self.mock_exists.return_value = False

        # Mock resolve to simulate resolving the downloaded file path
        self.mock_resolve.return_value = self.target_dir

        with (
            mock.patch("pooch.create") as mock_pooch_create,
            mock.patch("pooch.HTTPDownloader") as mock_downloader,
        ):
            # Create a mock Pooch instance
            mock_pooch_instance = mock.MagicMock()
            # Set our mock pooch instance as the return of pooch.create()
            mock_pooch_create.return_value = mock_pooch_instance
            # Create a mock fetch method
            mock_fetch = mock.MagicMock()
            # Set the fetch method of our mock pooch instance to the mocked method
            mock_pooch_instance.fetch = mock_fetch

            # Call the get method to simulate downloading the file
            remote_input_dataset.get(self.target_dir)

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
            # Assert that working_filepath is updated to the resolved downloaded file path
            assert (
                remote_input_dataset.working_path == self.target_filepath_remote
            ), f"Expected working_path to be {self.target_filepath_remote}, but got {remote_input_dataset.working_path}"

    def test_get_remote_with_no_file_hash(self, remote_input_dataset):
        """Test the InputDataset.get method when no file_hash is provided for a remote
        source.

        This test verifies that the get method raises a ValueError when a remote source file is
        attempted to be fetched without a defined file_hash, as file verification is necessary.

        Fixtures
        --------
        remote_input_dataset: MockInputDataset instance for remote files.
        mock_exists: Mock for Path.exists to simulate that the target file does not yet exist.
        mock_resolve: Mock for Path.resolve to simulate resolving the target directory.

        Asserts
        -------
        - A ValueError is raised when no file_hash is provided for a remote file.
        - The error message matches the expected message regarding the missing file_hash.
        """
        remote_input_dataset.file_hash = None
        self.mock_exists.return_value = False
        self.mock_resolve.return_value = self.target_dir
        expected_message = (
            "InputDataset.source.source_type is 'url' "
            + "but no InputDataset.file_hash is not defined. "
            + "Cannot proceed."
        )

        with pytest.raises(ValueError) as exception_info:
            remote_input_dataset.get(self.target_dir)
        assert str(exception_info.value) == expected_message
