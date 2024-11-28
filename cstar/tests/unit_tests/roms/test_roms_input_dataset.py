import pytest
import datetime as dt
from unittest import mock
from pathlib import Path
from cstar.roms import ROMSInputDataset
from cstar.base.datasource import DataSource


class MockROMSInputDataset(ROMSInputDataset):
    """A minimal example subclass of the ROMSInputDataset abstract base class."""

    pass


@pytest.fixture
def local_roms_netcdf_dataset():
    """Fixture to provide a ROMSInputDataset with a local NetCDF source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'netcdf'
    - DataSource.basename: Property mocked as 'local_file.nc'

    Yields:
    -------
        MockROMSInputDataset: A mock dataset pointing to a local NetCDF file.
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

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.nc",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


@pytest.fixture
def local_roms_yaml_dataset():
    """Fixture to provide a ROMSInputDataset with a local YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'path'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'local_file.yaml'

    Yields:
    -------
        MockROMSInputDataset: A mock dataset pointing to a local YAML file.
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
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "local_file.yaml"

        dataset = MockROMSInputDataset(
            location="some/local/source/path/local_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


################################################################################


def test_str_with_partitioned_files(local_roms_netcdf_dataset):
    """Test the ROMSInputDataset string representation hass correct substring for
    partitioned_files.

    Fixtures:
    ---------
    - local_roms_netcdf_dataset: Provides a ROMSInputDataset with a local NetCDF source.

    Asserts:
    --------
    - String representation of the dataset includes the list of
      partitioned files in the correct format.
    """

    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    assert """Partitioned files: ['local_file.001.nc',
                    'local_file.002.nc']""" in str(local_roms_netcdf_dataset)


def test_repr_with_partitioned_files(local_roms_netcdf_dataset):
    """Test the ROMSInputDataset repr has correct substring for partitioned_files.

    Fixtures:
    ---------
    - local_roms_netcdf_dataset: Provides a ROMSInputDataset with a local NetCDF source.

    Asserts:
    --------
    - ROMSInputDataset repr includes the list of partitioned files in the correct format.
    """
    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    assert """State: <partitioned_files = ['local_file.001.nc',
                             'local_file.002.nc']>""" in repr(local_roms_netcdf_dataset)


def test_repr_with_partitioned_files_and_working_path(local_roms_netcdf_dataset):
    """Test the ROMSInputDataset repr has correct substring for partitioned_files and
    working_path.

    Fixtures:
    ---------
    - local_roms_netcdf_dataset: Provides a ROMSInputDataset with a local NetCDF source.

    Asserts:
    --------
    - ROMSInputDataset repr includes both the working_path and the list of partitioned files in the correct format.
    """

    local_roms_netcdf_dataset.partitioned_files = [
        "local_file.001.nc",
        "local_file.002.nc",
    ]
    local_roms_netcdf_dataset.working_path = "/some/path/local_file.nc"

    assert """State: <working_path = /some/path/local_file.nc (does not exist),
        partitioned_files = ['local_file.001.nc',
                             'local_file.002.nc']""" in repr(local_roms_netcdf_dataset)


class TestROMSInputDatasetGetFromYAML:
    """Test class for ROMSInputDataset.get_from_yaml method."""

    def setup_method(self):
        """Set up common patches and mocks for each test.

        This method patches several methods and properties that are commonly used across the tests
        to avoid external dependencies and isolate the behavior of the `get_from_yaml` method.

        Mocks:
        ------
        - InputDataset.get: Mocks the parent class' get method to simulate dataset retrieval.
        - Path.is_symlink: Mocks the is_symlink method to control symlink behavior.
        - Path.resolve: Mocks the resolve method to simulate resolving symlinks.
        - Path.unlink: Mocks the unlink method to simulate file unlinking.
        - shutil.copy2: Mocks file copying to avoid file system modifications.
        - open (for reading): Mocks the open function to simulate reading YAML files.
        - yaml.safe_load: Mocks YAML loading to return a test-specific dictionary.
        - yaml.dump: Mocks YAML dumping to simulate saving modified YAML.
        - roms_tools.Grid: Mocks the Grid class from roms_tools:
            - mocks the class itself (to simulate Grid.from_yaml)
            - mocks a specific instance (to simulate grid_instance.save)
        - roms_tools.SurfaceForcing: represents all other classes in roms_tools except Grid
            - mocks the class itself (to simulate ROMSToolsClass.from_yaml)
            - mocks a specific instance (to simulate roms_tools_instance.save)

        Tests:
        ------
        - test_get_from_yaml_raises_when_not_yaml
        - test_get_grid_from_local_yaml_partitioned
        - test_get_surface_forcing_from_local_yaml_unpartitioned
        - test_get_from_yaml_raises_with_wrong_number_of_keys
        """

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
        # When we call 'read' in the tested method, we split header and data,
        # so reflect this here:
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

        # Configure from_yaml to return this explicit mock instance
        self.mock_rt_grid.from_yaml.return_value = self.mock_rt_grid_instance

        # Mock roms_tools.SurfaceForcing
        self.patch_rt_surface_forcing = mock.patch(
            "cstar.roms.input_dataset.roms_tools.SurfaceForcing", autospec=True
        )
        self.mock_rt_surface_forcing = self.patch_rt_surface_forcing.start()

        # Explicitly create a mock for the SurfaceForcing instance
        self.mock_rt_surface_forcing_instance = mock.Mock()

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_get_from_yaml_raises_when_not_yaml(self, local_roms_netcdf_dataset):
        """Test the get_from_yaml method raises a ValueError if source is not YAML."""
        with pytest.raises(ValueError) as exception_info:
            local_roms_netcdf_dataset.get_from_yaml(local_dir="some/local/dir")

        expected_message = (
            "Attempted to call `ROMSInputDataset.get_from_yaml() "
            + "but ROMSInputDataset.source.source_type is "
            + "netcdf, not 'yaml'"
        )
        assert str(exception_info.value) == expected_message

    def test_get_grid_from_local_yaml_partitioned(self, local_roms_yaml_dataset):
        """Test get_from_yaml for roms_tools.Grid with the saved file partitioned.

        This test simulates the process of creating a ROMS grid file
        from a local roms_tools yaml file.
        The method being tested involves:
        - calling 'get' on the file, which creates a local symlink to its path on the filesystem
        - replacing this symlink with a copy of the file
        - loading the YAML file and determining the roms-tools class it describes
        - creating the roms_tools.Grid object and saving it as a series of netCDF files
          corresponding to ROMS' domain partitioning for MPI runs

        Fixtures:
        ---------
        - local_roms_yaml_dataset: Provides a ROMSInputDataset with a local YAML source.
        - mock_is_symlink: Mocks Path.is_symlink to simulate a symlinked file.
        - mock_resolve: Mocks Path.resolve to simulate resolving a symlink to the actual file path.
        - mock_unlink: Mocks Path.unlink to simulate unlinking the symlink.
        - mock_copy2: Mocks shutil.copy2 to simulate copying the resolved file to the specified directory.
        - mock_yaml_load: Mocks yaml.safe_load to return a test dictionary for the Grid.
        - mock_rt_grid: Mocks the roms_tools.Grid class.
        - mock_rt_grid_instance: Mocks the instance of the roms_tools.Grid class and its save method.

        Asserts:
        --------
        - `mock_get` is called on the YAML file itself with the correct arguments.
        - `mock_is_symlink` is called once to check for a symlink.
        - `mock_resolve` is called once to resolve the symlink to its actual path.
        - `mock_unlink` is called once to unlink the symlink.
        - `mock_copy2` is called once to copy the resolved file to the symlink's former location
        - `mock_yaml_load` is called to parse the local copy of the YAML file.
        - `mock_rt_grid.from_yaml` is called with the resolved file path.
        - `mock_rt_grid_instance.save` is called with the correct partitioning parameters (np_xi, np_eta).
        - `ROMSInputDataset.partitioned_files` is updated to reflect the partitioned netCDF file
        """

        # Mock the is_symlink method to return True
        self.mock_is_symlink.return_value = True

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.return_value = resolved_path

        # Mock yaml loading
        self.mock_yaml_load.return_value = {
            "Grid": {"source": "ETOPO5", "fake": "entry"}
        }

        # Configure the save method to return a list of file paths
        self.mock_rt_grid_instance.save.return_value = ["some/local/dir/local_file.nc"]

        # Call the method under test
        local_roms_yaml_dataset.get_from_yaml(
            local_dir="some/local/dir", np_xi=3, np_eta=4
        )
        # Assert "get" was called on the yaml file itself
        self.mock_get.assert_called_once_with(
            local_roms_yaml_dataset, Path("some/local/dir")
        )

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
            Path("some/local/dir/PARTITIONED/local_file"), np_xi=3, np_eta=4
        )
        assert local_roms_yaml_dataset.partitioned_files == [
            "some/local/dir/local_file.nc"
        ]

    def test_get_surface_forcing_from_local_yaml_unpartitioned(
        self, local_roms_yaml_dataset
    ):
        """Test get_from_yaml for roms_tools.SurfaceForcing with the saved file
        partitioned.

        This test simulates the process of creating a ROMS surface forcing (or other) file
        from a local roms_tools yaml file.
        The method being tested involves:
        - calling 'get' on the file, which creates a local symlink to its path on the filesystem
        - replacing this symlink with a copy of the file
        - loading the modified YAML file and determining the roms-tools class it describes
        - modifying the YAML dictionary to set desired start and end dates
        - writing the modified YAML file, replacing the local copy
        - creating the roms_tools.SurfaceForcing object and saving as an unpartitioned netCDF file

        Fixtures:
        ---------
        - local_roms_yaml_dataset: Provides a ROMSInputDataset with a local YAML source.
        - mock_is_symlink: Mocks Path.is_symlink to simulate a symlinked file.
        - mock_resolve: Mocks Path.resolve to simulate resolving a symlink to the actual file path.
        - mock_unlink: Mocks Path.unlink to simulate unlinking the symlink.
        - mock_copy2: Mocks shutil.copy2 to simulate copying the resolved file to the specified directory.
        - mock_yaml_load: Mocks yaml.safe_load to return a test dictionary for Grid and SurfaceForcing.
        - mock_rt_surface_forcing: Mocks the roms_tools.SurfaceForcing class.
        - mock_rt_surface_forcing_instance: Mocks the instance of roms_tools.SurfaceForcing and its save method.

        Asserts:
        --------
        - `start_time` and `end_time` in the YAML dictionary are updated with the correct values.
        - `mock_get` is called on the YAML file itself with the correct arguments.
        - `mock_is_symlink` is called once to check for a symlink.
        - `mock_resolve` is called once to resolve the symlink to its actual path.
        - `mock_unlink` is called once to unlink the symlink.
        - `mock_copy2` is called once to copy the resolved file to the symlink's former location
        - `mock_yaml_load` is called to parse the local copy of the YAML file.
        - `mock_rt_surface_forcing.from_yaml` is called with the resolved file path and use_dask=True.
        - `mock_rt_surface_forcing_instance.save` is called to save the SurfaceForcing data.
        """

        # Mock the is_symlink method to return True
        self.mock_is_symlink.return_value = True

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.return_value = resolved_path

        # Mock yaml loading for a more complex YAML with both Grid and SurfaceForcing
        yaml_dict = {
            "Grid": {"fake": "entry", "topography_source": "ETOPO5"},
            "SurfaceForcing": {
                "fake": "entry",
                "model_reference_date": "2000-01-01T00:00:00",
                "start_time": "__START_TIME_PLACEHOLDER__",
                "end_time": "__END_TIME_PLACEHOLDER__",
            },
        }

        # Mock yaml.safe_load to return this dictionary
        self.mock_yaml_load.return_value = yaml_dict

        # Configure from_yaml mock to return the SurfaceForcing instance
        self.mock_rt_surface_forcing.from_yaml.return_value = (
            self.mock_rt_surface_forcing_instance
        )
        # Configure mock save method for SurfaceForcing to return a valid list
        self.mock_rt_surface_forcing_instance.save.return_value = [
            "some/local/dir/surface_forcing_file.nc"
        ]

        # Call the method under test
        local_roms_yaml_dataset.get_from_yaml(
            local_dir="some/local/dir", start_date="2022-01-01", end_date="2022-01-31"
        )

        assert (
            yaml_dict["SurfaceForcing"]["start_time"]
            == dt.datetime(2022, 1, 1).isoformat()
        )
        assert (
            yaml_dict["SurfaceForcing"]["end_time"]
            == dt.datetime(2022, 1, 31).isoformat()
        )

        # Assertions to ensure everything worked as expected
        self.mock_get.assert_called_once_with(
            local_roms_yaml_dataset, Path("some/local/dir")
        )

        # Assert that symlink handling code was triggered
        self.mock_is_symlink.assert_called_once()
        self.mock_resolve.assert_called_once()
        self.mock_unlink.assert_called_once()
        self.mock_copy2.assert_called_once_with(
            resolved_path, Path("some/local/dir/local_file.yaml")
        )

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that both roms_tools.SurfaceForcing was instantiated
        self.mock_rt_surface_forcing.from_yaml.assert_called_once_with(
            resolved_path, use_dask=True
        )

        # Ensure the save methods were called for both instances
        self.mock_rt_surface_forcing_instance.save.assert_called_once_with(
            Path("some/local/dir/local_file.nc")
        )

    def test_get_from_yaml_raises_with_wrong_number_of_keys(
        self, local_roms_yaml_dataset
    ):
        """Test that the get_from_yaml method raises a ValueError when the yaml file
        contains more than two sections.

        Fixtures:
        ---------
        - local_roms_yaml_dataset: Provides a ROMSInputDataset with a local YAML source.
        - mock_resolve: Mocks Path.resolve to simulate resolving a symlink to the actual file path.
        - mock_yaml_load: Mocks yaml.safe_load to return a dictionary with too many sections.

        Asserts:
        --------
        - Ensures `mock_resolve` is called once to resolve the symlink to the actual path.
        - Ensures `mock_yaml_load` is called to parse the YAML file.
        - Asserts that a ValueError is raised when the YAML file contains more than two sections (Grid and one other).
        """

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.return_value = resolved_path

        # Mock yaml loading for a more complex YAML with both Grid and SurfaceForcing
        self.mock_yaml_load.return_value = {
            "Grid": {"fake": "entry", "topography_source": "ETOPO5"},
            "SurfaceForcing": {
                "fake": "entry",
                "model_reference_date": "2000-01-01T00:00:00",
            },
            "AnotherSection": {"should": "fail", "with": 3, "sections": "in yaml"},
        }

        with pytest.raises(ValueError) as exception_info:
            local_roms_yaml_dataset.get_from_yaml(local_dir="some/local/dir")
        expected_message = (
            "roms tools yaml file has 3 sections. "
            + "Expected 'Grid' and one other class"
        )

        assert str(exception_info.value) == expected_message
