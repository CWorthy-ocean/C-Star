import pytest
from unittest import mock

from cstar.base import InputDataset
from cstar.base.datasource import DataSource

# TO TEST:
# - exists locally (somehow)
# - to_dict creates expected dict
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

        dataset = MockInputDataset(location="some/local/path")

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
Source location: some/local/path
Working path: None ( does not yet exist. Call InputDataset.get() )"""
    assert str(local_input_dataset) == expected_str


def test_local_repr(local_input_dataset):
    expected_repr = """MockInputDataset(
location = 'some/local/path',
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
