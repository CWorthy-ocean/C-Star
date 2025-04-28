import logging
import pytest
import datetime as dt
from unittest import mock
from pathlib import Path
from textwrap import dedent
from cstar.roms import ROMSInputDataset, ROMSForcingCorrections
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


@pytest.fixture
def remote_roms_yaml_dataset():
    """Fixture to provide a ROMSInputDataset with a remote YAML source.

    Mocks:
    ------
    - DataSource.location_type: Property mocked as 'url'
    - DataSource.source_type: Property mocked as 'yaml'
    - DataSource.basename: Property mocked as 'remote_file.yaml'

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
        mock_location_type.return_value = "url"
        mock_source_type.return_value = "yaml"
        mock_basename.return_value = "remote_file.yaml"

        dataset = MockROMSInputDataset(
            location="https://dodgyfakeyamlfiles.ru/all/remote_file.yaml",
            start_date="2024-10-22 12:34:56",
            end_date="2024-12-31 23:59:59",
        )

        yield dataset


################################################################################


class TestStrAndRepr:
    """Test class for verifying the string and repr outputs of ROMSInputDataset.

    This class contains tests to validate the correct representation of the
    `ROMSInputDataset` object in its string and repr outputs, including support
    for additional attributes like `partitioned_files` and `working_path`.

    Tests:
    ------
    - `test_str_with_partitioned_files`: Validates the string output includes
      the `partitioned_files` attribute in the correct format.
    - `test_repr_with_partitioned_files`: Validates the repr output includes
      the `partitioned_files` attribute in the correct format.
    - `test_repr_with_partitioned_files_and_working_path`: Ensures the repr
      output includes both `working_path` and `partitioned_files` in the
      correct format.
    """

    def test_str_with_partitioned_files(self, local_roms_netcdf_dataset):
        """Test the ROMSInputDataset string representation has correct substring for
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
        expected_str = dedent(
            """\
            Partitioned files: ['local_file.001.nc',
                                'local_file.002.nc']
        """
        ).strip()
        actual_str = str(local_roms_netcdf_dataset).strip()
        assert (
            expected_str in actual_str
        ), f"Expected:\n{expected_str}\nBut got:\n{actual_str}"

    def test_repr_with_partitioned_files(self, local_roms_netcdf_dataset):
        """Test the ROMSInputDataset repr includes `partitioned_files`.

        This test ensures that the `repr` output of a `ROMSInputDataset` object
        contains the `partitioned_files` attribute formatted as expected.

        Fixtures:
        ---------
        - `local_roms_netcdf_dataset`: Provides a mock ROMSInputDataset object
          with a local NetCDF source.

        Asserts:
        --------
        - The `partitioned_files` attribute is included in the repr output.
        - The format of the `partitioned_files` list matches the expected string output.
        """

        local_roms_netcdf_dataset.partitioned_files = [
            "local_file.001.nc",
            "local_file.002.nc",
        ]
        expected_repr = dedent(
            """\
            State: <partitioned_files = ['local_file.001.nc',
                                         'local_file.002.nc']>
        """
        ).strip()
        actual_repr = repr(local_roms_netcdf_dataset)

        # Normalize whitespace for comparison
        expected_repr_normalized = " ".join(expected_repr.split())
        actual_repr_normalized = " ".join(actual_repr.split())

        assert (
            expected_repr_normalized in actual_repr_normalized
        ), f"Expected:\n{expected_repr}\nBut got:\n{actual_repr}"

    def test_repr_with_partitioned_files_and_working_path(
        self, local_roms_netcdf_dataset
    ):
        """Test the ROMSInputDataset repr includes `partitioned_files` and
        `working_path`.

        This test ensures that the `repr` output of a `ROMSInputDataset` object
        contains both the `partitioned_files` and `working_path` attributes formatted
        as expected.

        Fixtures:
        ---------
        - `local_roms_netcdf_dataset`: Provides a mock ROMSInputDataset object
          with a local NetCDF source.

        Asserts:
        --------
        - The `working_path` and `partitioned_files` attributes are included in the repr output.
        - The format of both attributes matches the expected string output.
        """

        local_roms_netcdf_dataset.partitioned_files = [
            "local_file.001.nc",
            "local_file.002.nc",
        ]
        local_roms_netcdf_dataset.working_path = "/some/path/local_file.nc"

        expected_repr = dedent(
            """\
            State: <working_path = /some/path/local_file.nc (does not exist),
                    partitioned_files = ['local_file.001.nc',
                                         'local_file.002.nc'] >
        """
        ).strip()
        actual_repr = repr(local_roms_netcdf_dataset)

        # Normalize whitespace for comparison
        expected_repr_normalized = " ".join(expected_repr.split())
        actual_repr_normalized = " ".join(actual_repr.split())

        assert (
            expected_repr_normalized in actual_repr_normalized
        ), f"Expected:\n{expected_repr_normalized}\n to be in \n{actual_repr_normalized}"


class TestROMSInputDatasetGet:
    """Test class for ROMSInputDataset.get() method.

    This class includes tests for the `get` method, ensuring correct handling
    of local and YAML-based ROMS input datasets. The tests cover cases for
    partitioned files, unpartitioned files, early exits, and error conditions.

    Tests:
    ------
    - `test_get_grid_from_local_yaml_partitioned`:
      Verifies the creation of partitioned ROMS grid files from a local YAML file.
    - `test_get_surface_forcing_from_local_yaml_unpartitioned`:
      Checks the handling of unpartitioned ROMS surface forcing files from a YAML file.
    - `test_get_raises_with_wrong_number_of_keys`:
      Asserts that a ValueError is raised for invalid YAML file structures.
    - `test_get_skips_if_working_path_in_same_parent_dir`:
      Ensures the method skips processing if the dataset already exists locally.
    - `test_get_skips_if_working_path_list_in_same_parent_dir`:
      Verifies skipping execution for a list of working paths in the same directory.
    - `test_get_exits_if_not_yaml`:
      Confirms that the method exits early for non-YAML input datasets.
    """

    def setup_method(self):
        """Set up common patches and mocks for each test in TestROMSInputDatasetGet.

        This method initializes patches and mocks for methods and classes commonly
        used across the tests to ensure consistent behavior and avoid dependencies
        on external systems or file operations.

        Mocks:
        ------
        - `InputDataset.get`: Mocks the parent class' `get` method to simulate dataset retrieval.
        - `builtins.open`: Mocks the `open` function to simulate reading YAML files.
        - `yaml.safe_load`: Mocks YAML parsing to return a test-specific dictionary.
        - `yaml.dump`: Mocks YAML dumping to simulate saving modified YAML files.
        - `roms_tools.Grid`: Mocks the `Grid` class from `roms_tools`:
            - Mocks the `from_yaml` method for creating Grid instances.
            - Mocks a specific Grid instance, including its `save` method.
        - `roms_tools.SurfaceForcing`: Represents all other `roms_tools` classes except Grid:
            - Mocks the `from_yaml` method for creating instances.
            - Mocks specific SurfaceForcing instances, including their `save` method.
        """

        # Mocking InputDataset.get()
        self.patch_get = mock.patch(
            "cstar.roms.input_dataset.InputDataset.get", autospec=True
        )
        self.mock_get = self.patch_get.start()

        # Mocking Path methods
        self.patch_resolve = mock.patch("pathlib.Path.resolve", autospec=True)
        self.mock_resolve = self.patch_resolve.start()

        self.patch_mkdir = mock.patch("pathlib.Path.mkdir", autospec=True)
        self.mock_mkdir = self.patch_mkdir.start()

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

    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    @mock.patch("pathlib.Path.stat", autospec=True)
    def test_get_grid_from_local_yaml_partitioned(
        self, mock_stat, mock_get_hash, local_roms_yaml_dataset
    ):
        """Test the `get` method for partitioned ROMS grid files from a local YAML
        source.

        This test ensures the `get` method correctly processes a `roms-tools` YAML file to
        create partitioned ROMS grid files. It covers opening and reading a YAML file,
        editing it in memory, creating a Grid object,
        and saving the Grid object with proper partitioning.

        Fixtures:
        ---------
        - `local_roms_yaml_dataset`: Provides a ROMSInputDataset instance with a local YAML source.

        Mocks:
        ------
        - `Path.stat`: Simulates retrieving file metadata for partitioned files.
        - `_get_sha256_hash`: Simulates computing the hash of each partitioned file.
        - `yaml.safe_load`: Simulates loading YAML content from a file.
        - `roms_tools.Grid.from_yaml`: Simulates creating a Grid object from the YAML file.
        - `roms_tools.Grid.save`: Simulates saving Grid data as partitioned NetCDF files.

        Asserts:
        --------
        - Confirms `resolve` is called for the directory, and partitioned files.
        - Ensures `yaml.safe_load` processes the YAML content as expected.
        - Validates `roms_tools.Grid.from_yaml` creates the Grid object from the YAML file.
        - Verifies `roms_tools.Grid.save` saves files with correct partitioning parameters.
        - Confirms the list of partitioned files is updated correctly in the dataset.
        - Ensures metadata and checksums for partitioned files are cached via `stat` and `_get_sha256_hash`.
        """

        # Mock the stat result
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        mock_stat.return_value = mock_stat_result

        # Mock resolve to return a resolved path
        self.mock_resolve.side_effect = [
            Path("some/local/dir"),  # First resolve: local_dir passed to 'get'
            *(
                Path(f"some/local/dir/PARTITIONED/local_file.{i:02d}.nc")
                for i in range(1, 13)
            ),  # Resolves for partitioned files
        ]

        # Mock yaml loading
        self.mock_yaml_load.return_value = {
            "Grid": {"source": "ETOPO5", "fake": "entry"}
        }

        # Configure the save method to return a list of partitioned file paths
        partitioned_paths = [
            Path(f"some/local/dir/PARTITIONED/local_file.{i:02d}.nc")
            for i in range(1, 13)
        ]
        self.mock_rt_grid_instance.save.return_value = partitioned_paths
        self.mock_yaml_dump.return_value = "mocked_yaml_content"

        # Call the method under test
        local_roms_yaml_dataset.get(local_dir=Path("some/local/dir"), np_xi=3, np_eta=4)

        # Assert resolve calls
        expected_resolve_calls = [
            mock.call(Path("some/local/dir")),
            # mock.call(Path("some/local/dir/local_file.yaml")),
            *(
                mock.call(Path(f"some/local/dir/PARTITIONED/local_file.{i:02d}.nc"))
                for i in range(1, 13)
            ),
        ]
        assert self.mock_resolve.call_args_list == expected_resolve_calls, (
            f"Expected resolve calls:\n{expected_resolve_calls}\n"
            f"But got:\n{self.mock_resolve.call_args_list}"
        )

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that roms_tools.Grid.from_yaml was called
        self.mock_rt_grid.from_yaml.assert_called_once()

        # Finally, ensure the save method is called
        self.mock_rt_grid_instance.save.assert_called_once_with(
            filepath=Path("some/local/dir/PARTITIONED/local_file"), np_xi=3, np_eta=4
        )

        # Assert partitioned files are updated correctly
        assert local_roms_yaml_dataset.partitioned_files == partitioned_paths

        # Ensure stat was called for each partitioned file
        assert mock_stat.call_count == len(partitioned_paths), (
            f"Expected stat to be called {len(partitioned_paths)} times, "
            f"but got {mock_stat.call_count} calls."
        )

    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    @mock.patch("pathlib.Path.stat", autospec=True)
    @mock.patch("requests.get", autospec=True)
    def test_get_grid_from_remote_yaml_partitioned(
        self, mock_request, mock_stat, mock_get_hash, remote_roms_yaml_dataset
    ):
        """Test the `get` method for unpartitioned ROMS grid files from a remote YAML
        source.

        This test ensures the `get` method correctly processes a `roms-tools` YAML file to
        create partitioned ROMS grid files. It covers requesting yaml data from a URL,
        editing it in memory, creating a Grid object,
        and saving the Grid object with proper partitioning.

        Fixtures:
        ---------
        - `remote_roms_yaml_dataset`: Provides a ROMSInputDataset instance with a remote YAML source.

        Mocks:
        ------
        - `Path.stat`: Simulates retrieving file metadata for partitioned files.
        - `_get_sha256_hash`: Simulates computing the hash of each partitioned file.
        - `yaml.safe_load`: Simulates loading YAML content from a file.
        - `roms_tools.Grid.from_yaml`: Simulates creating a Grid object from the YAML file.
        - `roms_tools.Grid.save`: Simulates saving Grid data as partitioned NetCDF files.

        Asserts:
        --------
        - Confirms `resolve` is called for the directory, and saved file.
        - Ensures `yaml.safe_load` processes the YAML content as expected.
        - Validates `roms_tools.Grid.from_yaml` creates the Grid object from the YAML file.
        - Verifies `roms_tools.Grid.save` saves files with correct partitioning parameters.
        - Ensures metadata and checksums for partitioned files are cached via `stat` and `_get_sha256_hash`.
        """

        # Mock the stat result
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        mock_stat.return_value = mock_stat_result

        # Mock the call to requests.get on the remote yaml file
        mock_request.return_value.text = "---\nheader---\ndata"

        self.mock_resolve.side_effect = [
            Path("some/local/dir"),  # First resolve: local_dir
            Path("some/local/dir/remote_file.nc"),  # Second resolve: during caching
        ]

        # Mock the list of paths returned by roms_tools.save
        self.mock_rt_grid_instance.save.return_value = [
            Path("some/local/dir/remote_file.nc"),
        ]

        # Mock yaml loading
        self.mock_yaml_load.return_value = {
            "Grid": {"source": "ETOPO5", "fake": "entry"}
        }
        self.mock_yaml_dump.return_value = "mocked_yaml_content"

        # Call the method under test
        remote_roms_yaml_dataset.get(local_dir=Path("some/local/dir"))

        # Assert resolve calls
        expected_resolve_calls = [
            mock.call(Path("some/local/dir")),
            mock.call(Path("some/local/dir/remote_file.nc")),
        ]
        assert self.mock_resolve.call_args_list == expected_resolve_calls, (
            f"Expected resolve calls:\n{expected_resolve_calls}\n"
            f"But got:\n{self.mock_resolve.call_args_list}"
        )

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that roms_tools.Grid.from_yaml was called
        self.mock_rt_grid.from_yaml.assert_called_once()

        # Finally, ensure the save method is called
        self.mock_rt_grid_instance.save.assert_called_once_with(
            filepath=Path("some/local/dir/remote_file.nc")
        )

        # Ensure stat was called for the saved file
        assert (
            mock_stat.call_count == 1
        ), f"Expected stat to be called 1 time, but got {mock_stat.call_count} calls."

    @mock.patch("pathlib.Path.stat", autospec=True)
    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    def test_get_surface_forcing_from_local_yaml_unpartitioned(
        self, mock_get_hash, mock_stat, local_roms_yaml_dataset
    ):
        """Test the `get` method for creating unpartitioned SurfaceForcing from a local
        YAML source.

        This test verifies that the `get` method processes a `roms-tools` YAML file correctly to
        create a SurfaceForcing dataset without partitioning. It ensures proper handling of YAML
        content and the creation of unpartitioned NetCDF files.

        Fixtures:
        ---------
        - `local_roms_yaml_dataset`: Provides a ROMSInputDataset instance with a local YAML source.

        Mocks:
        ------
        - `Path.stat`: Simulates retrieving file metadata for the generated file.
        - `_get_sha256_hash`: Simulates computing the hash of the saved file.
        - `yaml.safe_load`: Simulates loading YAML content from a file.
        - `roms_tools.SurfaceForcing.from_yaml`: Simulates creating a SurfaceForcing object from the YAML file.
        - `roms_tools.SurfaceForcing.save`: Simulates saving SurfaceForcing data as an unpartitioned NetCDF file.

        Asserts:
        --------
        - Ensures the start and end times in the YAML dictionary are updated correctly.
        - Validates that `get` is called on the YAML file with the correct arguments.
        - Verifies `yaml.safe_load` processes the YAML content correctly.
        - Confirms `roms_tools.SurfaceForcing.from_yaml` creates the SurfaceForcing object from the YAML file.
        - Ensures `roms_tools.SurfaceForcing.save` saves the file with the correct parameters.
        - Verifies file metadata and checksum caching via `stat` and `_get_sha256_hash`.
        """

        # Mock resolve to return a resolved path
        self.mock_resolve.side_effect = [
            Path("some/local/dir"),  # First resolve: local_dir
            Path("some/local/dir/local_file.nc"),  # Second resolve: during caching
        ]

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
        # Mock yaml.dump to return something 'write' can handle:
        self.mock_yaml_dump.return_value = "mocked_yaml_content"
        # Configure from_yaml mock to return the SurfaceForcing instance
        self.mock_rt_surface_forcing.from_yaml.return_value = (
            self.mock_rt_surface_forcing_instance
        )
        # Configure mock save method for SurfaceForcing to return a valid list
        self.mock_rt_surface_forcing_instance.save.return_value = [
            Path("some/local/dir/surface_forcing_file.nc")
        ]

        # Mock stat result for saved files
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        mock_stat.return_value = mock_stat_result

        # Call the method under test
        local_roms_yaml_dataset.get(local_dir="some/local/dir")

        # Assert that start_time and end_time are updated in the YAML dictionary

        assert (
            yaml_dict["SurfaceForcing"]["start_time"]
            == dt.datetime(2024, 10, 22, 12, 34, 56).isoformat()
        )
        assert (
            yaml_dict["SurfaceForcing"]["end_time"]
            == dt.datetime(2024, 12, 31, 23, 59, 59).isoformat()
        )

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that roms_tools.SurfaceForcing was instantiated
        self.mock_rt_surface_forcing.from_yaml.assert_called_once()

        # Ensure the save method was called for the SurfaceForcing instance
        self.mock_rt_surface_forcing_instance.save.assert_called_once_with(
            filepath=Path("some/local/dir/local_file.nc")
        )

        # Ensure stat was called for the saved file
        assert (
            mock_stat.call_count == 1
        ), f"Expected stat to be called 1 time, but got {mock_stat.call_count} calls."

    @mock.patch("pathlib.Path.stat", autospec=True)
    def test_get_raises_with_wrong_number_of_keys(
        self, mock_stat, local_roms_yaml_dataset
    ):
        """Test that the `get` method raises a ValueError when the YAML file contains
        more than two sections.

        This test ensures that the `get` method validates the structure of the YAML file
        and raises an error if the file contains more than two sections (e.g., `Grid` and one other).

        Fixtures:
        ---------
        - `local_roms_yaml_dataset`: Provides a ROMSInputDataset with a local YAML source.

        Mocks:
        ------
        - `Path.resolve`: Simulates resolving the path to the source yaml file
        - `yaml.safe_load`: Simulates loading YAML content from a file with too many sections.

        Asserts:
        --------
        - Ensures `resolve` is called to determine the actual path of the YAML file.
        - Ensures `yaml.safe_load` is invoked to parse the YAML content.
        - Confirms that a `ValueError` is raised when the YAML file contains more than two sections.
        - Validates that the exception message matches the expected error message.
        """

        # Mock resolve to return a resolved path
        resolved_path = Path("/resolved/path/to/local_file.yaml")
        self.mock_resolve.side_effect = [
            Path("some/local/dir"),  # First resolve: local_dir
            resolved_path,  # Second resolve: source location
        ]

        # Mock yaml loading for a YAML with too many sections
        self.mock_yaml_load.return_value = {
            "Grid": {"fake": "entry", "topography_source": "ETOPO5"},
            "SurfaceForcing": {
                "fake": "entry",
                "model_reference_date": "2000-01-01T00:00:00",
            },
            "AnotherSection": {"should": "fail", "with": 3, "sections": "in yaml"},
        }

        # Mock stat to prevent unintended file operations
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        mock_stat.return_value = mock_stat_result

        # Call the method under test and expect a ValueError
        with pytest.raises(ValueError) as exception_info:
            local_roms_yaml_dataset.get(local_dir="some/local/dir")

        # Define the expected error message
        expected_message = (
            "roms tools yaml file has 3 sections. "
            + "Expected 'Grid' and one other class"
        )

        # Assert the error message matches
        assert str(exception_info.value) == expected_message

        # Assertions to ensure everything worked as expected
        self.mock_resolve.assert_called()
        self.mock_yaml_load.assert_called_once()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_skips_if_working_path_in_same_parent_dir(
        self,
        mock_exists_locally,
        local_roms_yaml_dataset,
        caplog: pytest.LogCaptureFixture,
    ):
        """Test that the `get` method skips execution when `working_path` is set and
        points to the same parent directory as `local_dir`.

        This test verifies that if `working_path` exists and is in the same directory
        as the specified `local_dir`, the `get` method does not proceed with further
        file operations.

        Fixtures:
        ---------
        - `local_roms_yaml_dataset`: Provides a ROMSInputDataset with a mocked `source` attribute.
        - `caplog`: Captures log outputs to verify the correct skip message is displayed.

        Mocks:
        ------
        - `exists_locally`: Simulates the local existence check for `working_path`.
        - `Path.resolve`: Simulates resolving paths to their actual locations.

        Asserts:
        --------
        - Ensures the skip message is printed when `working_path` exists in `local_dir`.
        - Confirms that no further operations (e.g., copying, YAML parsing) are performed.
        - An information message is logged
        """

        caplog.set_level(logging.INFO, logger=local_roms_yaml_dataset.log.name)

        # Mock `working_path` to point to a file in `some/local/dir`
        local_roms_yaml_dataset.working_path = Path("some/local/dir/local_file.yaml")

        # Mock the `exists_locally` property to return True
        mock_exists_locally.return_value = True

        # Set the `mock_resolve` side effect to resolve `local_dir` correctly
        self.mock_resolve.return_value = Path("some/local/dir")

        local_roms_yaml_dataset.get(local_dir="some/local/dir")

        # Assert the skip message was printed
        captured = caplog.text
        assert "already exists, skipping." in captured

        # Ensure no further operations were performed
        self.mock_get.assert_not_called()
        self.mock_yaml_load.assert_not_called()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_skips_if_working_path_list_in_same_parent_dir(
        self,
        mock_exists_locally,
        local_roms_yaml_dataset,
        caplog: pytest.LogCaptureFixture,
    ):
        """Test that the `get` method skips execution when `working_path` is a list and
        its first element points to the same parent directory as `local_dir`.

        This test ensures that if `working_path` is a list of paths, and at least one of
        its elements exists in the same parent directory as `local_dir`, the `get` method
        does not proceed with further file operations.

        Fixtures:
        ---------
        - `local_roms_yaml_dataset`: Provides a ROMSInputDataset with a mocked `source` attribute.
        - `caplog`: Captures log outputs to verify the correct skip message is displayed.

        Mocks:
        ------
        - `exists_locally`: Simulates the local existence check for `working_path`.
        - `Path.resolve`: Simulates resolving paths to their actual locations.

        Asserts:
        --------
        - Ensures the skip message is printed when a `working_path` in the list exists in `local_dir`.
        - Confirms that no further operations (e.g., copying, YAML parsing) are performed.
        """

        caplog.set_level(logging.INFO, logger=local_roms_yaml_dataset.log.name)

        # Mock `working_path` to be a list pointing to files in `some/local/dir`
        local_roms_yaml_dataset.working_path = [
            Path("some/local/dir/local_file_1.yaml"),
            Path("some/local/dir/local_file_2.yaml"),
        ]

        # Mock the `exists_locally` property to return True
        mock_exists_locally.return_value = True

        # Set the `mock_resolve` side effect to resolve `local_dir` correctly
        self.mock_resolve.return_value = Path("some/local/dir")

        local_roms_yaml_dataset.get(local_dir="some/local/dir")

        # Assert the skip message was printed
        assert "already exists, skipping." in caplog.text

        # Ensure no further operations were performed
        self.mock_get.assert_not_called()
        self.mock_yaml_load.assert_not_called()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_exits_if_not_yaml(self, mock_exists_locally, local_roms_yaml_dataset):
        """Test that the `get` method exits early if `self.source.source_type` is not
        'yaml'.

        This test ensures that the `get` method performs no further operations if the input dataset's
        source type is not 'yaml', ensuring that early returns function correctly.

        Fixtures:
        ---------
        - local_roms_yaml_dataset: Provides a ROMSInputDataset with a mocked `source` attribute.
        - mock_exists_locally: Mocks the `exists_locally` property to simulate the file's existence.

        Asserts:
        --------
        - Ensures the parent class `get` method (`self.mock_get`) is called with the correct arguments.
        - Ensures no further actions (e.g., loading, modifying YAML, or saving files) occur.
        - Ensures no messages are printed during the method's execution.
        """
        # Mock the `source` attribute and its `source_type` property
        mock_source = mock.Mock()
        type(mock_source).source_type = mock.PropertyMock(
            return_value="netcdf"
        )  # Non-yaml type

        # Assign the mocked `source` to the dataset
        with mock.patch.object(local_roms_yaml_dataset, "source", mock_source):
            mock_exists_locally.return_value = (
                False  # Ensure the file does not exist locally
            )

            # Mock `resolve` to return the expected path
            self.mock_resolve.return_value = Path("some/local/dir")

            # Call the method under test
            local_roms_yaml_dataset.get(local_dir=Path("some/local/dir"))

            # Assert the parent `get` method was called with the correct arguments
            self.mock_get.assert_called_once_with(
                local_roms_yaml_dataset, local_dir=Path("some/local/dir")
            )

            # Ensure no further processing happened
            assert (
                not self.mock_yaml_load.called
            ), "Expected no calls to yaml.safe_load, but some occurred."


class TestROMSInputDatasetPartition:
    """Test class for the `ROMSInputDataset.partition` method.

    Tests:
    ------
    - test_partition_single_file:
        Ensures that a single NetCDF file is partitioned and relocated correctly.
    - test_partition_multiple_files:
        Verifies partitioning behavior when multiple files are provided.
    - test_partition_raises_when_not_local:
        Confirms an error is raised when `working_path` does not exist locally.
    - test_partition_raises_with_mismatched_directories:
        Validates that an error is raised if files span multiple directories.
    """

    def setup_method(self):
        self.patch_mkdir = mock.patch("pathlib.Path.mkdir", autospec=True)
        self.mock_mkdir = self.patch_mkdir.start()

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_single_file(
        self, mock_partition_netcdf, local_roms_netcdf_dataset
    ):
        """Ensures that a single NetCDF file is partitioned and relocated correctly.

        Mocks:
        ------
        - partition_netcdf: Simulates the behavior of the partitioning utility.
        - Path.rename: Mocks the moving of partitioned files to the PARTITIONED directory.

        Fixtures:
        ---------
        - local_roms_netcdf_dataset: Provides a dataset with a single NetCDF file.

        Asserts:
        --------
        - `partition_netcdf` is called with the correct arguments.
        - Files are renamed correctly to the PARTITIONED directory.
        - `partitioned_files` is updated with the expected file paths.
        """

        np_xi, np_eta = 2, 3
        num_partitions = np_xi * np_eta

        # Set up the working_path for the dataset
        local_roms_netcdf_dataset.working_path = Path(
            "some/local/source/path/local_file.nc"
        )

        # Mock the exists_locally property
        with mock.patch.object(
            type(local_roms_netcdf_dataset),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Mock the partitioning function to produce files
            mock_partition_netcdf.return_value = [
                Path(f"local_file.{i}.nc") for i in range(1, num_partitions + 1)
            ]

            # Mock the rename method
            with mock.patch.object(Path, "rename", autospec=True) as mock_rename:
                # Call the method under test
                local_roms_netcdf_dataset.partition(np_xi=np_xi, np_eta=np_eta)

                # Assert partition_netcdf is called with the correct arguments
                mock_partition_netcdf.assert_called_once_with(
                    local_roms_netcdf_dataset.working_path, np_xi=np_xi, np_eta=np_eta
                )

                # Assert rename is called for each file
                expected_calls = [
                    mock.call(
                        Path(f"local_file.{i}.nc"),
                        Path(f"some/local/source/path/PARTITIONED/local_file.{i}.nc"),
                    )
                    for i in range(1, num_partitions + 1)
                ]
                mock_rename.assert_has_calls(expected_calls, any_order=False)

                # Assert partitioned_files attribute is updated correctly
                expected_partitioned_files = [
                    Path(f"some/local/source/path/PARTITIONED/local_file.{i}.nc")
                    for i in range(1, num_partitions + 1)
                ]
                assert (
                    local_roms_netcdf_dataset.partitioned_files
                    == expected_partitioned_files
                )

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_multiple_files(
        self, mock_partition_netcdf, local_roms_netcdf_dataset
    ):
        """Verifies partitioning behavior when multiple files are provided.

        Mocks:
        ------
        - partition_netcdf: Simulates the behavior of the partitioning utility.
        - Path.rename: Mocks the renaming of partitioned files to the PARTITIONED directory.

        Fixtures:
        ---------
        - local_roms_netcdf_dataset: Provides a dataset with multiple files.

        Asserts:
        --------
        - `partition_netcdf` is called the correct number of times.
        - Files are moved correctly to the PARTITIONED directory.
        - `partitioned_files` is updated with the expected file paths.
        """

        np_xi, np_eta = 2, 2
        num_partitions = np_xi * np_eta

        # Set up the working_path for multiple files
        local_roms_netcdf_dataset.working_path = [
            Path("some/local/source/path/file1.nc"),
            Path("some/local/source/path/file2.nc"),
        ]

        # Mock the exists_locally property
        with mock.patch.object(
            type(local_roms_netcdf_dataset),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Mock the partitioning function to produce files for each input
            mock_partition_netcdf.side_effect = [
                [Path(f"file1.{i}.nc") for i in range(1, num_partitions + 1)],
                [Path(f"file2.{i}.nc") for i in range(1, num_partitions + 1)],
            ]

            # Mock the rename method
            with mock.patch.object(Path, "rename", autospec=True) as mock_rename:
                # Call the method under test
                local_roms_netcdf_dataset.partition(np_xi=np_xi, np_eta=np_eta)

                # Assert partition_netcdf is called for each file
                assert mock_partition_netcdf.call_count == len(
                    local_roms_netcdf_dataset.working_path
                )

                # Assert rename is called for each file
                expected_calls = [
                    mock.call(
                        Path(f"file1.{i}.nc"),
                        Path(f"some/local/source/path/PARTITIONED/file1.{i}.nc"),
                    )
                    for i in range(1, num_partitions + 1)
                ] + [
                    mock.call(
                        Path(f"file2.{i}.nc"),
                        Path(f"some/local/source/path/PARTITIONED/file2.{i}.nc"),
                    )
                    for i in range(1, num_partitions + 1)
                ]
                mock_rename.assert_has_calls(expected_calls, any_order=False)

                # Assert partitioned_files attribute is updated correctly
                expected_partitioned_files = [
                    Path(f"some/local/source/path/PARTITIONED/file1.{i}.nc")
                    for i in range(1, num_partitions + 1)
                ] + [
                    Path(f"some/local/source/path/PARTITIONED/file2.{i}.nc")
                    for i in range(1, num_partitions + 1)
                ]
                assert (
                    local_roms_netcdf_dataset.partitioned_files
                    == expected_partitioned_files
                )

    def test_partition_raises_when_not_local(self, local_roms_netcdf_dataset):
        """Confirms an error is raised when `working_path` does not exist locally.

        Mocks:
        ------
        - exists_locally: Ensures the dataset is treated as non-existent.

        Fixtures:
        ---------
        - local_roms_netcdf_dataset: Provides a dataset for testing.

        Asserts:
        --------
        - A `ValueError` is raised with the correct message.
        """

        # Simulate a dataset that does not exist locally
        with mock.patch.object(
            type(local_roms_netcdf_dataset),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = False

            with pytest.raises(ValueError) as exception_info:
                local_roms_netcdf_dataset.partition(np_xi=2, np_eta=3)

        expected_message = (
            f"working_path of InputDataset \n {local_roms_netcdf_dataset.working_path}, "
            + "refers to a non-existent file"
            + "\n call InputDataset.get() and try again."
        )
        assert str(exception_info.value) == expected_message

    def test_partition_raises_with_mismatched_directories(
        self, local_roms_netcdf_dataset
    ):
        """Validates that an error is raised if files span multiple directories.

        Mocks:
        ------
        - exists_locally: Ensures the dataset is treated as existing.

        Fixtures:
        ---------
        - local_roms_netcdf_dataset: Provides a dataset for testing.

        Asserts:
        --------
        - A `ValueError` is raised with the correct message.
        """

        # Set up the dataset with files in different directories
        local_roms_netcdf_dataset.working_path = [
            Path("some/local/source/path/file1.nc"),
            Path("some/other/source/path/file2.nc"),
        ]

        # Mock the exists_locally property to ensure it returns True
        with mock.patch.object(
            type(local_roms_netcdf_dataset),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Expect a ValueError due to mismatched directories
            with pytest.raises(ValueError) as exception_info:
                local_roms_netcdf_dataset.partition(np_xi=2, np_eta=3)

            expected_message = (
                f"A single input dataset exists in multiple directories: "
                f"{local_roms_netcdf_dataset.working_path}."
            )
            assert str(exception_info.value) == expected_message


def test_correction_cannot_be_yaml():
    """Checks that the `validate()` method correctly raises a TypeError if
    `ROMSForcingCorrections.source.source_type` is `yaml` (unsupported)"""

    with pytest.raises(TypeError) as exception_info:
        ROMSForcingCorrections(
            location="https://www.totallylegityamlfiles.pk/downloadme.yaml"
        )
        expected_msg = (
            "ROMSForcingCorrections cannot be initialized with a source YAML file."
        )
        assert expected_msg in str(exception_info.value)
