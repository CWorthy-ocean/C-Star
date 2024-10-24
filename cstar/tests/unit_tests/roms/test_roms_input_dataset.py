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
        self.mock_rt_grid_instance.save.return_value = ["some/local/dir/local_file.nc"]

        # Configure from_yaml to return this explicit mock instance
        self.mock_rt_grid.from_yaml.return_value = self.mock_rt_grid_instance

        # Mock roms_tools.SurfaceForcing
        self.patch_rt_surface_forcing = mock.patch(
            "cstar.roms.input_dataset.roms_tools.SurfaceForcing", autospec=True
        )
        self.mock_rt_surface_forcing = self.patch_rt_surface_forcing.start()

        # Explicitly create a mock for the SurfaceForcing instance
        self.mock_rt_surface_forcing_instance = mock.Mock()

        # Configure from_yaml to return the SurfaceForcing instance
        self.mock_rt_surface_forcing.from_yaml.return_value = (
            self.mock_rt_surface_forcing_instance
        )

        # Configure save method for SurfaceForcing to return a valid list
        self.mock_rt_surface_forcing_instance.save.return_value = [
            "some/local/dir/surface_forcing_file.nc"
        ]

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_get_from_yaml_raises_when_not_yaml(self, local_roms_netcdf_dataset):
        with pytest.raises(ValueError) as exception_info:
            local_roms_netcdf_dataset.get_from_yaml(local_dir="some/local/dir")

        expected_message = (
            "Attempted to call `ROMSInputDataset.get_from_yaml() "
            + "but ROMSInputDataset.source.source_type is "
            + "netcdf, not 'yaml'"
        )
        assert str(exception_info.value) == expected_message

    def test_get_grid_from_local_yaml_parallel(self, local_roms_yaml_dataset):
        """Test get_from_yaml when the local YAML file is a symlink."""

        # Mock the is_symlink method to return True
        self.mock_is_symlink.return_value = True

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.return_value = resolved_path

        # Mock yaml loading
        self.mock_yaml_load.return_value = {
            "Grid": {"source": "ETOPO5", "fake": "entry"}
        }

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

    def test_get_surface_forcing_from_local_yaml_serial(self, local_roms_yaml_dataset):
        """Test get_from_yaml when the YAML file contains multiple sections like Grid
        and SurfaceForcing."""

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
        """Test get_from_yaml when the YAML file contains multiple sections like Grid
        and SurfaceForcing."""

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
