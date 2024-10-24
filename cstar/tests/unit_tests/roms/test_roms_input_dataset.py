import pytest
import datetime as dt
from unittest import mock
from pathlib import Path
from cstar.roms import ROMSInputDataset
from cstar.base.datasource import DataSource


class MockROMSInputDataset(ROMSInputDataset):
    pass


@pytest.fixture
def local_roms_netcdf_dataset():
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

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def local_roms_yaml_dataset():
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
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "local_file.yaml"

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def remote_roms_netcdf_dataset():
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
        dataset = MockROMSInputDataset(
            location="http://example.com/remote_file.nc",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        # Yield the dataset for use in the test
        yield dataset


@pytest.fixture
def remote_roms_yaml_dataset():
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
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "remote_file.yaml"

        # Create the InputDataset instance; it will use the mocked DataSource
        dataset = MockROMSInputDataset(
            location="http://example.com/remote_file.yaml",
            file_hash="abc123",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

    yield dataset


################################################################################


def test_str_with_partitioned_files(local_roms_netcdf_dataset):
    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    assert """Partitioned files: ['local_file.001.nc',
                    'local_file.002.nc']""" in str(local_roms_netcdf_dataset)


def test_repr_with_partitioned_files(local_roms_netcdf_dataset):
    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    assert """State: <partitioned_files = ['local_file.001.nc',
                             'local_file.002.nc']>""" in repr(local_roms_netcdf_dataset)


def test_repr_with_partitioned_files_and_working_path(local_roms_netcdf_dataset):
    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    local_roms_netcdf_dataset.working_path = "/some/path/local_file.nc"

    assert """State: <working_path = /some/path/local_file.nc (does not exist),
        partitioned_files = ['local_file.001.nc',
                             'local_file.002.nc']""" in repr(local_roms_netcdf_dataset)


class TestROMSInputDatasetGet:
    def setup_method(self):
        """Setup common patches for each test."""
        # Mocking InputDataset.get()
        self.patch_get = mock.patch(
            "cstar.roms.input_dataset.InputDataset.get", autospec=True
        )
        self.mock_get = self.patch_get.start()

        # Mocking Path methods
        self.patch_is_symlink = mock.patch("pathlib.Path.is_symlink", autospec=True)
        self.mock_is_symlink = self.patch_is_symlink.start()

        self.patch_resolve = mock.patch("pathlib.Path.resolve", autospec=True)
        self.mock_resolve = self.patch_resolve.start()

        self.patch_unlink = mock.patch("pathlib.Path.unlink", autospec=True)
        self.mock_unlink = self.patch_unlink.start()

        # Mock shutil.copy2
        self.patch_copy2 = mock.patch("shutil.copy2", autospec=True)
        self.mock_copy2 = self.patch_copy2.start()

        # Mock open for reading YAML
        self.patch_open = mock.patch(
            "builtins.open", mock.mock_open(read_data="---\nheader---\ndata")
        )
        self.mock_open = self.patch_open.start()

        # Mock yaml.safe_load
        self.patch_yaml_load = mock.patch("yaml.safe_load", autospec=True)
        self.mock_yaml_load = self.patch_yaml_load.start()

        # Mock yaml.dump for writing modified YAML
        self.patch_yaml_dump = mock.patch("yaml.dump", autospec=True)
        self.mock_yaml_dump = self.patch_yaml_dump.start()

        # Mock roms_tools.Grid
        self.patch_rt_grid = mock.patch(
            "cstar.roms.input_dataset.roms_tools.Grid", autospec=True
        )
        self.mock_rt_grid = self.patch_rt_grid.start()

        # Explicitly create a mock for the Grid instance
        self.mock_rt_grid_instance = mock.Mock()

        # Configure the save method to return a list of file paths
        self.mock_rt_grid_instance.save.return_value = ["some/local/dir/file.nc"]

        # Configure from_yaml to return this explicit mock instance
        self.mock_rt_grid.from_yaml.return_value = self.mock_rt_grid_instance

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_get_from_yaml_with_local_symlink(self, local_roms_yaml_dataset):
        """Test get_from_yaml when the local YAML file is a symlink."""
        # Set up the dataset object
        roms_dataset = local_roms_yaml_dataset

        # Mock the is_symlink method to return True
        self.mock_is_symlink.return_value = True

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.return_value = resolved_path

        # Mock yaml loading
        self.mock_yaml_load.return_value = {
            "Grid": {
                "start_time": None,
                "ini_time": None,
                "end_time": None,
            }
        }

        # Define start and end date
        start_date = dt.datetime(2022, 1, 1)
        end_date = dt.datetime(2022, 1, 31)

        # Call the method under test
        roms_dataset.get_from_yaml(
            local_dir="some/local/dir", start_date=start_date, end_date=end_date
        )
        assert self.mock_get.called, "The `get()` method was not called."
        print("DEBUG")
        print(self.mock_get.call_args_list)  # Debugging to see the actual calls

        # Assertions to ensure everything worked as expected
        self.mock_get.assert_called_once_with(roms_dataset, Path("some/local/dir"))

        # Assert that symlink handling code was triggered
        self.mock_is_symlink.assert_called_once()
        self.mock_resolve.assert_called_once()
        self.mock_unlink.assert_called_once()
        self.mock_copy2.assert_called_once_with(
            resolved_path, Path("some/local/dir/local_file.yaml")
        )

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that roms_tools.Grid.from_yaml or roms_tools.OtherClass.from_yaml was called
        self.mock_rt_grid.from_yaml.assert_called_once_with(resolved_path)

        # Finally, ensure the save method is called
        self.mock_rt_grid_instance.save.assert_called_once_with(
            Path("some/local/dir/local_file.nc")
        )
