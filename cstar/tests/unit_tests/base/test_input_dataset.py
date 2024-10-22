import pytest
from unittest import mock
from pathlib import Path
from cstar.base import InputDataset
from cstar.base.datasource import DataSource

# TO TEST:
# - get (this will probably be a class)


class MockInputDataset(InputDataset):
    pass


@pytest.fixture
def local_input_dataset():
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

        dataset = MockInputDataset(location="some/local/source/path")

        yield dataset


@pytest.fixture
def remote_input_dataset():
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
            location="http://example.com/remote_file.nc", file_hash="abc123"
        )

        # Yield the dataset for use in the test
        yield dataset


def test_local_init(local_input_dataset):
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
    with pytest.raises(ValueError) as exception_info:
        MockInputDataset("http://example.com/remote_file.nc")

    expected_message = (
        "Cannot create InputDataset for \n http://example.com/remote_file.nc:\n "
        + "InputDataset.file_hash cannot be None if InputDataset.source.location_type is 'url'.\n"
        + "A file hash is required to verify files downloaded from remote sources."
    )

    assert str(exception_info.value) == expected_message


def test_local_str(local_input_dataset):
    expected_str = """----------------
MockInputDataset
----------------
Source location: some/local/source/path
Working path: None ( does not yet exist. Call InputDataset.get() )"""
    assert str(local_input_dataset) == expected_str


def test_local_repr(local_input_dataset):
    expected_repr = """MockInputDataset(
location = 'some/local/source/path',
file_hash = None
)"""
    assert repr(local_input_dataset) == expected_repr


def test_remote_repr(remote_input_dataset):
    expected_repr = """MockInputDataset(
location = 'http://example.com/remote_file.nc',
file_hash = abc123
)"""
    assert repr(remote_input_dataset) == expected_repr


def test_remote_str(remote_input_dataset):
    expected_str = """----------------
MockInputDataset
----------------
Source location: http://example.com/remote_file.nc
file_hash: abc123
Working path: None ( does not yet exist. Call InputDataset.get() )"""
    assert str(remote_input_dataset) == expected_str


def test_exists_locally_with_single_path(local_input_dataset):
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
    assert remote_input_dataset.to_dict() == {
        "location": "http://example.com/remote_file.nc",
        "file_hash": "abc123",
    }


class TestInputDatasetGet:
    def setup_method(self):
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
        mock.patch.stopall()

    def test_get_when_filename_exists(self, capsys, local_input_dataset):
        target_dir = Path("/some/local/target/dir")
        self.mock_resolve.return_value = target_dir
        self.mock_exists.return_value = True

        local_input_dataset.get(target_dir)

        expected_message = "A file by the name of local_file.nc already exists at /some/local/target/dir\n"
        captured = capsys.readouterr()
        assert captured.out == expected_message
        assert local_input_dataset.working_path == target_dir / "local_file.nc"

    def test_get_with_local_source(self, local_input_dataset):
        source_filepath = Path(
            local_input_dataset.source.location
        )  # Source file in the local system
        target_dir = Path("/some/local/target/dir")
        target_filepath = target_dir / "local_file.nc"  # Target path before resolving

        # Mock Path.exists to simulate that the file doesn't exist yet in local_dir
        self.mock_exists.return_value = False
        self.mock_resolve.side_effect = [target_dir, source_filepath]

        # Call the get method
        local_input_dataset.get(target_dir)

        # Assert that a symbolic link was created with the resolved path
        self.mock_symlink_to.assert_called_once_with(source_filepath)

        # Assert that working_filepath is updated to the resolved target path
        assert (
            local_input_dataset.working_path == target_filepath
        ), f"Expected working_filepath to be {target_filepath}, but got {local_input_dataset.working_path}"

    def test_get_with_remote_source(self, remote_input_dataset):
        target_dir = Path("/some/local/target/dir")
        target_filepath = target_dir / "remote_file.nc"

        # Mock Path.exists to simulate that the file doesn't exist yet in local_dir
        self.mock_exists.return_value = False

        # Mock resolve to simulate resolving the downloaded file path
        self.mock_resolve.side_effect = [target_dir]

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
            remote_input_dataset.get(target_dir)

            # Ensure pooch.create was called correctly
            mock_pooch_create.assert_called_once_with(
                path=target_dir,
                base_url="http://example.com/",
                registry={"remote_file.nc": "abc123"},
            )

            # Ensure fetch was called with the mocked downloader
            mock_fetch.assert_called_once_with(
                "remote_file.nc", downloader=mock_downloader.return_value
            )
            # Assert that working_filepath is updated to the resolved downloaded file path
            assert (
                remote_input_dataset.working_path == target_filepath
            ), f"Expected working_path to be {target_filepath}, but got {remote_input_dataset.working_path}"
