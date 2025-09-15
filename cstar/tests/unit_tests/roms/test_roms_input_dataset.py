import datetime as dt
import logging
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from cstar.roms import ROMSForcingCorrections, ROMSPartitioning


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

    def test_str_with_partitioned_files(self, fake_romsinputdataset_netcdf_local):
        """Test the ROMSInputDataset string representation has correct substring for
        partitioned_files.

        Fixtures
        --------
        - fake_romsinputdataset_netcdf_local: Provides a ROMSInputDataset with a local NetCDF source.

        Asserts
        -------
        - String representation of the dataset includes the list of
          partitioned files in the correct format.
        """
        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                "local_file.001.nc",
                "local_file.002.nc",
            ],
        )
        expected_str = (
            "Partitioning: ROMSPartitioning(np_xi=1, np_eta=2, files=['local_file.001.nc',\n"
            + " " * 43
            + "'local_file.002.nc'])"
        )

        actual_str = str(fake_romsinputdataset_netcdf_local).strip()
        assert expected_str in actual_str, (
            f"Expected:\n{expected_str}\nBut got:\n{actual_str}"
        )

    def test_repr_with_partitioned_files(self, fake_romsinputdataset_netcdf_local):
        """Test the ROMSInputDataset repr includes `partitioned_files`.

        This test ensures that the `repr` output of a `ROMSInputDataset` object
        contains the `partitioned_files` attribute formatted as expected.

        Fixtures
        --------
        - `fake_romsinputdataset_netcdf_local`: Provides a mock ROMSInputDataset object
          with a local NetCDF source.

        Asserts
        -------
        - The `partitioned_files` attribute is included in the repr output.
        - The format of the `partitioned_files` list matches the expected string output.
        """
        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                "local_file.001.nc",
                "local_file.002.nc",
            ],
        )
        expected_repr = dedent(
            """\
            State: <partitioning = ROMSPartitioning(np_xi=1, np_eta=2, files=['local_file.001.nc', 'local_file.002.nc'])>
        """
        ).strip()
        actual_repr = repr(fake_romsinputdataset_netcdf_local)

        # Normalize whitespace for comparison
        expected_repr_normalized = " ".join(expected_repr.split())
        actual_repr_normalized = " ".join(actual_repr.split())

        assert expected_repr_normalized in actual_repr_normalized, (
            f"Expected:\n{expected_repr}\nBut got:\n{actual_repr}"
        )

    def test_repr_with_partitioned_files_and_working_path(
        self, fake_romsinputdataset_netcdf_local
    ):
        """Test the ROMSInputDataset repr includes `partitioned_files` and
        `working_path`.

        This test ensures that the `repr` output of a `ROMSInputDataset` object
        contains both the `partitioned_files` and `working_path` attributes formatted
        as expected.

        Fixtures
        --------
        - `fake_romsinputdataset_netcdf_local`: Provides a mock ROMSInputDataset object
          with a local NetCDF source.

        Asserts
        -------
        - The `working_path` and `partitioned_files` attributes are included in the repr output.
        - The format of both attributes matches the expected string output.
        """
        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                "local_file.001.nc",
                "local_file.002.nc",
            ],
        )

        fake_romsinputdataset_netcdf_local.working_path = "/some/path/local_file.nc"

        expected_repr = dedent(
            """\
            State: <working_path = /some/path/local_file.nc (does not exist), partitioning = ROMSPartitioning(np_xi=1, np_eta=2, files=['local_file.001.nc', 'local_file.002.nc']) >
        """
        ).strip()
        actual_repr = repr(fake_romsinputdataset_netcdf_local)

        # Normalize whitespace for comparison
        expected_repr_normalized = " ".join(expected_repr.split())
        actual_repr_normalized = " ".join(actual_repr.split())

        assert expected_repr_normalized in actual_repr_normalized, (
            f"Expected:\n{expected_repr_normalized}\n to be in \n{actual_repr_normalized}"
        )


class TestROMSInputDatasetGet:
    """Test class for ROMSInputDataset.get() method.

    This class includes tests for the `get` method, ensuring correct handling
    of local and YAML-based ROMS input datasets. The tests cover cases for
    partitioned files, unpartitioned files, early exits, and error conditions.

    Tests:
    ------
    - `test_get_grid_from_remote_yaml`:
      Verifies the creation of a ROMS grid file from a remote YAML file.
    - `test_get_surface_forcing_from_local_yaml`:
      Checks the handling of ROMS surface forcing files from a YAML file.
    - `test_get_from_yaml_raises_if_not_yaml`:
      Checks that a ValueError is raised when _get_from_yaml is called on a non-yaml source
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

        Mocks
        -----
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
    @mock.patch("requests.get", autospec=True)
    def test_get_grid_from_remote_yaml(
        self,
        mock_request,
        mock_stat,
        mock_get_hash,
        fake_romsinputdataset_yaml_remote,
        mock_path_resolve,
    ):
        """Test the `get` method for ROMS grid files from a remote YAML source.

        This test ensures the `get` method correctly processes a `roms-tools` YAML file to
        create ROMS grid files. It covers requesting yaml data from a URL,
        editing it in memory, creating a Grid object,
        and saving the Grid object.

        Fixtures
        --------
        - `fake_romsinputdataset_yaml_remote`: Provides a ROMSInputDataset instance with a remote YAML source.

        Mocks
        -----
        - `Path.stat`: Simulates retrieving file metadata for file.
        - `_get_sha256_hash`: Simulates computing the hash of the file.
        - `yaml.safe_load`: Simulates loading YAML content from a file.
        - `roms_tools.Grid.from_yaml`: Simulates creating a Grid object from the YAML file.
        - `roms_tools.Grid.save`: Simulates saving Grid data as a NetCDF file.

        Asserts
        -------
        - Confirms `resolve` is called for the directory, and saved file.
        - Ensures `yaml.safe_load` processes the YAML content as expected.
        - Validates `roms_tools.Grid.from_yaml` creates the Grid object from the YAML file.
        - Verifies `roms_tools.Grid.save` saves files with correct parameters.
        - Ensures metadata and checksums for saved file is cached via `stat` and `_get_sha256_hash`.
        """
        # Mock the stat result
        mock_stat_result = mock.Mock(
            st_size=12345, st_mtime=1678901234, st_mode=0o100644
        )
        mock_stat.return_value = mock_stat_result

        # Mock the call to requests.get on the remote yaml file
        mock_request.return_value.text = "---\nheader---\ndata"

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
        fake_romsinputdataset_yaml_remote.get(local_dir=Path("some/local/dir"))

        # Check that the yaml.safe_load was called properly
        self.mock_yaml_load.assert_called_once()

        # Assert that roms_tools.Grid.from_yaml was called
        self.mock_rt_grid.from_yaml.assert_called_once()

        # Finally, ensure the save method is called
        self.mock_rt_grid_instance.save.assert_called_once_with(
            filepath=Path("some/local/dir/remote_file.nc")
        )

        # Ensure stat was called for the saved file
        assert mock_stat.call_count == 1, (
            f"Expected stat to be called 1 time, but got {mock_stat.call_count} calls."
        )

    @mock.patch("pathlib.Path.stat", autospec=True)
    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    def test_get_surface_forcing_from_local_yaml(
        self,
        mock_get_hash,
        mock_stat,
        fake_romsinputdataset_yaml_local,
        mock_path_resolve,
    ):
        """Test the `get` method for creating SurfaceForcing from a local YAML source.

        This test verifies that the `get` method processes a `roms-tools` YAML file correctly to
        create a SurfaceForcing dataset. It ensures proper handling of YAML
        content and the creation of associated NetCDF files.

        Fixtures
        --------
        - `fake_romsinputdataset_yaml_local`: Provides a ROMSInputDataset instance with a local YAML source.

        Mocks
        -----
        - `Path.stat`: Simulates retrieving file metadata for the generated file.
        - `_get_sha256_hash`: Simulates computing the hash of the saved file.
        - `yaml.safe_load`: Simulates loading YAML content from a file.
        - `roms_tools.SurfaceForcing.from_yaml`: Simulates creating a SurfaceForcing object from the YAML file.
        - `roms_tools.SurfaceForcing.save`: Simulates saving SurfaceForcing data as a NetCDF file.

        Asserts
        -------
        - Ensures the start and end times in the YAML dictionary are updated correctly.
        - Validates that `get` is called on the YAML file with the correct arguments.
        - Verifies `yaml.safe_load` processes the YAML content correctly.
        - Confirms `roms_tools.SurfaceForcing.from_yaml` creates the SurfaceForcing object from the YAML file.
        - Ensures `roms_tools.SurfaceForcing.save` saves the file with the correct parameters.
        - Verifies file metadata and checksum caching via `stat` and `_get_sha256_hash`.
        """
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
        fake_romsinputdataset_yaml_local.get(local_dir="some/local/dir")

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
        assert mock_stat.call_count == 1, (
            f"Expected stat to be called 1 time, but got {mock_stat.call_count} calls."
        )

    def test_get_from_yaml_raises_if_not_yaml(self, fake_romsinputdataset_netcdf_local):
        """Tests that the `ROMSInputDataset._get_from_yaml` method raises if called from
        a ROMSInputDataset whose source.source_type is not 'yaml'.

        Fixtures
        --------
        fake_romsinputdataset_netcdf_local (ROMSInputDataset)
            Example ROMSInputDataset whose source is a local netCDF file

        Asserts
        -------
        - A ValueError is raised when _get_from_yaml is called
        """
        with pytest.raises(
            ValueError,
            match="_get_from_yaml requires a ROMSInputDataset whose source_type is yaml",
        ):
            fake_romsinputdataset_netcdf_local._get_from_yaml(local_dir="/some/dir")

    @mock.patch("pathlib.Path.stat", autospec=True)
    def test_get_raises_with_wrong_number_of_keys(
        self, mock_stat, fake_romsinputdataset_yaml_local, mock_path_resolve
    ):
        """Test that the `get` method raises a ValueError when the YAML file contains
        more than two sections.

        This test ensures that the `get` method validates the structure of the YAML file
        and raises an error if the file contains more than two sections (e.g., `Grid` and one other).

        Fixtures
        --------
        - `fake_romsinputdataset_yaml_local`: Provides a ROMSInputDataset with a local YAML source.

        Mocks
        -----
        - `Path.resolve`: Simulates resolving the path to the source yaml file
        - `yaml.safe_load`: Simulates loading YAML content from a file with too many sections.

        Asserts
        -------
        - Ensures `yaml.safe_load` is invoked to parse the YAML content.
        - Confirms that a `ValueError` is raised when the YAML file contains more than two sections.
        - Validates that the exception message matches the expected error message.
        """
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
            fake_romsinputdataset_yaml_local.get(local_dir="some/local/dir")

        # Define the expected error message
        expected_message = (
            "roms tools yaml file has 3 sections. "
            + "Expected 'Grid' and one other class"
        )

        # Assert the error message matches
        assert str(exception_info.value) == expected_message

        # Assertions to ensure everything worked as expected
        self.mock_yaml_load.assert_called_once()

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_skips_if_working_path_in_same_parent_dir(
        self,
        mock_exists_locally,
        fake_romsinputdataset_yaml_local,
        caplog: pytest.LogCaptureFixture,
        mock_path_resolve,
    ):
        """Test that the `get` method skips execution when `working_path` is set and
        points to the same parent directory as `local_dir`.

        This test verifies that if `working_path` exists and is in the same directory
        as the specified `local_dir`, the `get` method does not proceed with further
        file operations.

        Fixtures
        --------
        - `fake_romsinputdataset_yaml_local`: Provides a ROMSInputDataset with a mocked `source` attribute.
        - `caplog`: Captures log outputs to verify the correct skip message is displayed.

        Mocks
        -----
        - `exists_locally`: Simulates the local existence check for `working_path`.
        - `Path.resolve`: Simulates resolving paths to their actual locations.

        Asserts
        -------
        - Ensures the skip message is printed when `working_path` exists in `local_dir`.
        - Confirms that no further operations (e.g., copying, YAML parsing) are performed.
        - An information message is logged
        """
        caplog.set_level(logging.INFO, logger=fake_romsinputdataset_yaml_local.log.name)

        # Mock `working_path` to point to a file in `some/local/dir`
        fake_romsinputdataset_yaml_local.working_path = Path(
            "some/local/dir/local_file.yaml"
        )

        # Mock the `exists_locally` property to return True
        mock_exists_locally.return_value = True

        fake_romsinputdataset_yaml_local.get(local_dir="some/local/dir")

        # Assert the skip message was printed
        captured = caplog.text
        assert "already exists, skipping." in captured

        # Ensure no further operations were performed
        self.mock_get.assert_not_called()
        self.mock_yaml_load.assert_not_called()

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_skips_if_working_path_list_in_same_parent_dir(
        self,
        mock_exists_locally,
        fake_romsinputdataset_yaml_local,
        caplog: pytest.LogCaptureFixture,
        mock_path_resolve,
    ):
        """Test that the `get` method skips execution when `working_path` is a list and
        its first element points to the same parent directory as `local_dir`.

        This test ensures that if `working_path` is a list of paths, and at least one of
        its elements exists in the same parent directory as `local_dir`, the `get` method
        does not proceed with further file operations.

        Fixtures
        --------
        - `fake_romsinputdataset_yaml_local`: Provides a ROMSInputDataset with a mocked `source` attribute.
        - `caplog`: Captures log outputs to verify the correct skip message is displayed.

        Mocks
        -----
        - `exists_locally`: Simulates the local existence check for `working_path`.
        - `Path.resolve`: Simulates resolving paths to their actual locations.

        Asserts
        -------
        - Ensures the skip message is printed when a `working_path` in the list exists in `local_dir`.
        - Confirms that no further operations (e.g., copying, YAML parsing) are performed.
        """
        caplog.set_level(logging.INFO, logger=fake_romsinputdataset_yaml_local.log.name)

        # Mock `working_path` to be a list pointing to files in `some/local/dir`
        fake_romsinputdataset_yaml_local.working_path = [
            Path("some/local/dir/local_file_1.yaml"),
            Path("some/local/dir/local_file_2.yaml"),
        ]

        # Mock the `exists_locally` property to return True
        mock_exists_locally.return_value = True

        fake_romsinputdataset_yaml_local.get(local_dir="some/local/dir")

        # Assert the skip message was printed
        assert "already exists, skipping." in caplog.text

        # Ensure no further operations were performed
        self.mock_get.assert_not_called()
        self.mock_yaml_load.assert_not_called()

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_exits_if_not_yaml(
        self, mock_exists_locally, fake_romsinputdataset_yaml_local, mock_path_resolve
    ):
        """Test that the `get` method exits early if `self.source.source_type` is not
        'yaml'.

        This test ensures that the `get` method performs no further operations if the input dataset's
        source type is not 'yaml', ensuring that early returns function correctly.

        Fixtures
        --------
        - fake_romsinputdataset_yaml_local: Provides a ROMSInputDataset with a mocked `source` attribute.
        - mock_exists_locally: Mocks the `exists_locally` property to simulate the file's existence.

        Asserts
        -------
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
        with mock.patch.object(fake_romsinputdataset_yaml_local, "source", mock_source):
            mock_exists_locally.return_value = (
                False  # Ensure the file does not exist locally
            )

            # Call the method under test
            fake_romsinputdataset_yaml_local.get(local_dir=Path("some/local/dir"))

            # Assert the parent `get` method was called with the correct arguments
            self.mock_get.assert_called_once_with(
                fake_romsinputdataset_yaml_local, local_dir=Path("some/local/dir")
            )

            # Ensure no further processing happened
            assert not self.mock_yaml_load.called, (
                "Expected no calls to yaml.safe_load, but some occurred."
            )

        mock_path_resolve.assert_called()

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.roms.input_dataset.ROMSInputDataset._get_from_partitioned_source",
        autospec=True,
    )
    def test_get_with_partitioned_source(
        self,
        mock_get_from_partitioned_source,
        fake_romsinputdataset_netcdf_local,
        mock_path_resolve,
    ):
        """Tests the 'get' method calls _get_from_partitioned_source when the
        ROMSInputDataset has a partitioned source.

        Mocks and Fixtures
        ------------------
        mock_get_from_partitioned_source (MagicMock)
            Mocks the `_get_from_partitioned_source` method to assert it was called
        fake_romsinputdataset_netcdf_local (ROMSInputDataset)
            Provides an example ROMSInputDataset on which to call get()

        Asserts
        -------
        - _get_from_partitioned_source is called once with the expected arguments
        """
        # Set source partitioning attributes
        fake_romsinputdataset_netcdf_local.source._location = (
            "some/local/source/path/local_file.00.nc"
        )
        fake_romsinputdataset_netcdf_local.source_np_xi = 4
        fake_romsinputdataset_netcdf_local.source_np_eta = 3

        fake_romsinputdataset_netcdf_local.get(local_dir="/some/dir")

        mock_get_from_partitioned_source.assert_called_once_with(
            fake_romsinputdataset_netcdf_local,
            local_dir=Path("/some/dir"),
            source_np_xi=4,
            source_np_eta=3,
        )

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.roms.input_dataset.ROMSInputDataset._symlink_or_download_from_source",
        autospec=False,
    )
    @mock.patch("cstar.roms.input_dataset._get_sha256_hash")
    @mock.patch("cstar.roms.input_dataset.Path.stat")
    def test_get_from_partitioned_source(
        self,
        mock_path_stat,
        mock_get_hash,
        mock_symlink_or_download,
        fake_romsinputdataset_netcdf_local,
    ):
        """Tests the _get_from_partitioned_source helper method.

        This test takes an example ROMSInputDataset whose source is partitioned into
        12 files and calls `_get_from_partitioned_source` on it, checking each file
        is fetched and the `ROMSInputDataset.partitioning` attribute is correctly set.

        Mocks & Fixtures
        ----------------
        mock_path_stat (MagicMock)
            mocks the Path.stat method to set the ROMSPartitioning._local_file_stat_cache attr
        mock_get_hash (MagicMock)
            mocks the _get_sha256_hash method to compute the shasum of the partitioned files
        mock_symlink_or_download (MagicMock)
            mocks the InputDataset._symlink_or_download_from_source method to fetch the partitions
        fake_romsinputdataset_netcdf_local (ROMSInputDataset)
            provides an example ROMSInputDataset with a local netcdf source

        Asserts
        -------
        - Asserts _symlink_or_download_from_source was called 12 times (once per partition)
        - Asserts the calls to _symlink_or_download_from_source have expected arguments
        - Asserts the `ROMSInputDataset.partitioning` attribute is set as expected
        """
        # Set source partitioning attributes
        fake_romsinputdataset_netcdf_local.source._location = (
            "some/local/source/path/local_file.00.nc"
        )
        fake_romsinputdataset_netcdf_local.source_np_xi = 4
        fake_romsinputdataset_netcdf_local.source_np_eta = 3

        fake_romsinputdataset_netcdf_local._get_from_partitioned_source(
            local_dir=Path("/some/dir"),
            source_np_xi=4,
            source_np_eta=3,
        )

        mock_get_hash.side_effect = [f"mock_hash_{i}" for i in range(12)]

        assert mock_symlink_or_download.call_count == 12
        expected_calls = [
            mock.call(
                source_location=f"some/local/source/path/local_file.{i:02d}.nc",
                location_type="path",
                expected_file_hash=None,
                target_path=mock.ANY,
                logger=mock.ANY,
            )
            for i in range(12)
        ]

        mock_symlink_or_download.assert_has_calls(expected_calls, any_order=False)

        expected_files = [Path(f"/some/dir/local_file.{i:02d}.nc") for i in range(12)]

        assert fake_romsinputdataset_netcdf_local.partitioning.np_xi == 4
        assert fake_romsinputdataset_netcdf_local.partitioning.np_eta == 3
        assert fake_romsinputdataset_netcdf_local.partitioning.files == expected_files


class TestROMSInputDatasetPartition:
    """Test class for the `ROMSInputDataset.partition` method.

    Tests:
    ------
    - test_source_partitioning:
        Tests the ROMSInputDataset.source_partitioning property
    - test_to_dict_with_source_partitioning
        Test the ROMSInputDataset.to_dict() method with a partitioned source file
    - test_partition_single_file:
        Ensures that a single NetCDF file is partitioned and relocated correctly.
    - test_partition_multiple_files:
        Verifies partitioning behavior when multiple files are provided.
    - test_partition_skips_if_already_partitioned:
        Tests that no action is taken if a ROMSInputDataset has already been
        partitioned.
    - test_partition_raises_if_already_partitioned_differently:
        Test that a FileExistsError is raised if this ROMSInputDataset has been
        partitioned in a different arrangement to that requested.
    - test_partition_restores_existing_files_if_repeat_partitioning_fails
        Test that existing partitioned files are restored if a repeat call fails
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

    def test_source_partitioning(self, fake_romsinputdataset_netcdf_local):
        """Test the ROMSInputDataset.source_partitioning property."""
        fake_romsinputdataset_netcdf_local.source_np_xi = 4
        fake_romsinputdataset_netcdf_local.source_np_eta = 3

        assert fake_romsinputdataset_netcdf_local.source_partitioning == (4, 3)

    def test_to_dict_with_source_partitioning(self, fake_romsinputdataset_netcdf_local):
        """Test the ROMSInputDataset.to_dict() method with a partitioned source file."""
        fake_romsinputdataset_netcdf_local.source_np_xi = 4
        fake_romsinputdataset_netcdf_local.source_np_eta = 3

        test_dict = fake_romsinputdataset_netcdf_local.to_dict()
        assert test_dict["source_np_xi"] == 4
        assert test_dict["source_np_eta"] == 3

    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    @mock.patch("pathlib.Path.stat", autospec=True)
    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_single_file(
        self,
        mock_partition_netcdf,
        mock_get_hash,
        mock_stat,
        fake_romsinputdataset_netcdf_local,
    ):
        """Ensures that a single NetCDF file is partitioned and tracked correctly.

        Mocks
        -----
        - partition_netcdf: Simulates the behavior of the partitioning utility.
        - Path.stat: Mocks computation of file statistics
        - _get_sha256_hash: Mocks file shasum calculation
        - Path.resolve: Mocks the resolution of the partitioned filepaths

        Fixtures
        --------
        - fake_romsinputdataset_netcdf_local: Provides a dataset with a single NetCDF file.

        Asserts
        -------
        - `partition_netcdf` is called with the correct arguments.
        - `ROMSInputDataset.partitioning.files` is updated with the expected file paths.
        - `Path.stat` is called once for each partitioned file
        """
        np_xi, np_eta = 2, 3
        num_partitions = np_xi * np_eta

        # Set up the working_path for the dataset
        fake_romsinputdataset_netcdf_local.working_path = Path(
            "some/local/source/path/local_file.nc"
        )

        # Mock the exists_locally property
        with mock.patch.object(
            type(fake_romsinputdataset_netcdf_local),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Mock the partitioning function to produce files
            mock_partition_netcdf.return_value = [
                Path(f"local_file.{i}.nc") for i in range(1, num_partitions + 1)
            ]

            # Test partitioned_files attribute is updated correctly
            expected_partitioned_files = [
                Path(f"some/local/source/path/local_file.{i}.nc")
                for i in range(1, num_partitions + 1)
            ]

            # # Mock the resolve method
            with mock.patch.object(
                Path,
                "resolve",
                autospec=True,
                side_effect=expected_partitioned_files * 2,
            ):
                # Call the method under test
                fake_romsinputdataset_netcdf_local.partition(np_xi=np_xi, np_eta=np_eta)

                # Assert partition_netcdf is called with the correct arguments
                mock_partition_netcdf.assert_called_once_with(
                    fake_romsinputdataset_netcdf_local.working_path,
                    np_xi=np_xi,
                    np_eta=np_eta,
                )

                assert (
                    fake_romsinputdataset_netcdf_local.partitioning.files
                    == expected_partitioned_files
                )

                # Ensure stat was called for each saved file
                assert mock_stat.call_count == 6, (
                    f"Expected stat to be called 6 times, but got {mock_stat.call_count} calls."
                )

    @mock.patch("cstar.roms.input_dataset._get_sha256_hash", return_value="mocked_hash")
    @mock.patch("pathlib.Path.stat", autospec=True)
    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_multiple_files(
        self,
        mock_partition_netcdf,
        mock_get_hash,
        mock_stat,
        fake_romsinputdataset_netcdf_local,
    ):
        """Verifies partitioning behavior when multiple files are provided.

        Mocks
        -----
        - partition_netcdf: Simulates the behavior of the partitioning utility.
        - Path.stat: Mocks computation of file statistics
        - _get_sha256_hash: Mocks file shasum calculation
        - Path.resolve: Mocks the resolution of the partitioned filepaths

        Fixtures
        ---------
        - fake_romsinputdataset_netcdf_local: Provides a dataset with multiple files.

        Asserts
        -------
        - `partition_netcdf` is called the correct number of times.
        - `ROMSInputDataset.partitioning.files` is updated with the expected file paths.
        - `Path.stat` is called once for each partitioned file
        """
        np_xi, np_eta = 2, 2
        num_partitions = np_xi * np_eta

        # Set up the working_path for multiple files
        fake_romsinputdataset_netcdf_local.working_path = [
            Path("some/local/source/path/file1.nc"),
            Path("some/local/source/path/file2.nc"),
        ]

        # Mock the exists_locally property
        with mock.patch.object(
            type(fake_romsinputdataset_netcdf_local),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Mock the partitioning function to produce files for each input
            mock_partition_netcdf.side_effect = [
                [Path(f"file1.{i}.nc") for i in range(1, num_partitions + 1)],
                [Path(f"file2.{i}.nc") for i in range(1, num_partitions + 1)],
            ]

            # Assert partitioned_files attribute is updated correctly
            expected_partitioned_files = [
                Path(f"some/local/source/path/file1.{i}.nc")
                for i in range(1, num_partitions + 1)
            ] + [
                Path(f"some/local/source/path/file2.{i}.nc")
                for i in range(1, num_partitions + 1)
            ]

            # Mock the resolve method
            with mock.patch.object(
                Path,
                "resolve",
                autospec=True,
                side_effect=expected_partitioned_files * 2,
            ):
                # Call the method under test
                fake_romsinputdataset_netcdf_local.partition(np_xi=np_xi, np_eta=np_eta)

                # Assert partition_netcdf is called for each file
                assert mock_partition_netcdf.call_count == len(
                    fake_romsinputdataset_netcdf_local.working_path
                )
                assert (
                    fake_romsinputdataset_netcdf_local.partitioning.files
                    == expected_partitioned_files
                )

                # Ensure stat was called for each saved file
                assert mock_stat.call_count == 8, (
                    f"Expected stat to be called 8 times, but got {mock_stat.call_count} calls."
                )

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_skips_if_already_partitioned(
        self,
        mock_partition_netcdf,
        fake_romsinputdataset_netcdf_local,
        caplog: pytest.LogCaptureFixture,
    ):
        """Tests that no action is taken if a ROMSInputDataset has already been
        partitioned.

        Mocks & Fixtures
        ----------------
        mock_partition_netcdf (MagicMock)
            Mocks the roms_tools.partition_netcdf method to check whether it is called
        fake_romsinputdataset_netcdf_local (ROMSInputDataset)
            Provides a dataset for testing
        caplog
            Builtin fixture to capture logging messages

        Asserts
        -------
        - Confirms that an appropriate message is logged
        - Confirms that roms_tools.partition_netcdf is not called
        """
        caplog.set_level(
            logging.INFO, logger=fake_romsinputdataset_netcdf_local.log.name
        )

        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                Path("/some/dir/local_file.0.nc"),
                Path("/some/dir/local_file.1.nc"),
            ],
        )
        fake_romsinputdataset_netcdf_local.partition(np_xi=1, np_eta=2)

        assert "FakeROMSInputDataset already partitioned, skipping" in caplog.text
        mock_partition_netcdf.assert_not_called

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_raises_if_already_partitioned_differently(
        self, mock_partition_netcdf, fake_romsinputdataset_netcdf_local
    ):
        """Test that a FileExistsError is raised if this ROMSInputDataset has been
        partitioned in a different arrangement to that requested.

        This test takes an example ROMSInputDataset with partitioning (1,2) and then
        calls `partition` requesting partitioning of (3,4)

        Mocks & Fixtures
        ----------------
        mock_partition_netcdf (MagicMock)
            Mocks the roms_tools.partition_netcdf method to confirm it was not called
        fake_romsinputdataset_netcdf_local (ROMSInputDataset)
            Provides an example ROMSInputDataset on which to call `partition`.

        Asserts
        -------
        - A FileExistsError is raised with an appropriate message
        - roms_tools.partition_netcdf is not called
        """
        with pytest.raises(
            FileExistsError,
            match="The file has already been partitioned into a different arrangement",
        ):
            fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
                np_xi=1,
                np_eta=2,
                files=["/some/dir/local_file.0.nc", "/some/dir/local_file.1.nc"],
            )
            fake_romsinputdataset_netcdf_local.partition(np_xi=3, np_eta=4)

        mock_partition_netcdf.assert_not_called

    @mock.patch("cstar.roms.input_dataset.shutil.move")
    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_restores_existing_files_if_repeat_partitioning_fails(
        self,
        mock_partition_netcdf,
        mock_exists,
        mock_move,
        fake_romsinputdataset_netcdf_local,
    ):
        """Test that existing partitioned files are restored if _repeat_ partitioning
        fails.

        This test simulates a scenario where `partition()` is called on an
        already partitioned ROMSInputDataset with `overwrite_existing_files=True`,
        but the actual partitioning operation raises an exception.
        The test ensures that any existing partitioned
        files are backed up before the attempt, and correctly restored after
        the failure.

        Mocks
        -----
        - `partition_netcdf`: Simulates failure by raising a RuntimeError.
        - `exists_locally`: Ensures the dataset is treated as locally available.
        - `shutil.move`: Tracks file move operations during backup and restore.
        - `Path.resolve`: Returns deterministic paths for assertions.

        Asserts:
        --------
        - `shutil.move` is called four times: twice to back up existing files,
          and twice to restore them after failure.
        - The first two calls move files into a temporary directory.
        - The last two calls restore files from the backup location.
        - The original exception (`RuntimeError`) is raised.
        """
        existing_files = [
            Path("/some/dir/local_file.0.nc"),
            Path("/some/dir/local_file.1.nc"),
        ]
        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1, np_eta=2, files=existing_files
        )
        fake_romsinputdataset_netcdf_local.working_path = "/some/dir/local_file.nc"
        mock_exists.return_value = True
        mock_partition_netcdf.side_effect = RuntimeError("simulated failure")
        with mock.patch.object(
            Path, "resolve", autospec=True, side_effect=existing_files * 2
        ):
            with pytest.raises(RuntimeError, match="simulated failure"):
                fake_romsinputdataset_netcdf_local.partition(
                    np_xi=3, np_eta=3, overwrite_existing_files=True
                )

        assert mock_move.call_count == 4

        backup_calls = mock_move.call_args_list[:2]
        for call, original in zip(backup_calls, existing_files):
            src, dst = call[0]
            assert src == original
            assert "tmp" in str(dst)  # destination should be in a temp dir

        restore_calls = mock_move.call_args_list[2:]
        for call, original in zip(restore_calls, existing_files):
            src, dst = call[0]
            assert dst == original
            assert "tmp" in str(src)

    def test_partition_raises_when_not_local(self, fake_romsinputdataset_netcdf_local):
        """Confirms an error is raised when `working_path` does not exist locally.

        Mocks
        -----
        - exists_locally: Ensures the dataset is treated as non-existent.

        Fixtures
        --------
        - fake_romsinputdataset_netcdf_local: Provides a dataset for testing.

        Asserts
        -------
        - A `ValueError` is raised with the correct message.
        """
        # Simulate a dataset that does not exist locally
        with mock.patch.object(
            type(fake_romsinputdataset_netcdf_local),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = False

            with pytest.raises(ValueError) as exception_info:
                fake_romsinputdataset_netcdf_local.partition(np_xi=2, np_eta=3)

        expected_message = (
            f"working_path of InputDataset \n {fake_romsinputdataset_netcdf_local.working_path}, "
            + "refers to a non-existent file"
            + "\n call InputDataset.get() and try again."
        )
        assert str(exception_info.value) == expected_message

    def test_partition_raises_with_mismatched_directories(
        self, fake_romsinputdataset_netcdf_local
    ):
        """Validates that an error is raised if files span multiple directories.

        Mocks
        -----
        - exists_locally: Ensures the dataset is treated as existing.

        Fixtures
        --------
        - fake_romsinputdataset_netcdf_local: Provides a dataset for testing.

        Asserts:
        --------
        - A `ValueError` is raised with the correct message.
        """
        # Set up the dataset with files in different directories
        fake_romsinputdataset_netcdf_local.working_path = [
            Path("some/local/source/path/file1.nc"),
            Path("some/other/source/path/file2.nc"),
        ]

        # Mock the exists_locally property to ensure it returns True
        with mock.patch.object(
            type(fake_romsinputdataset_netcdf_local),
            "exists_locally",
            new_callable=mock.PropertyMock,
        ) as mock_exists_locally:
            mock_exists_locally.return_value = True

            # Expect a ValueError due to mismatched directories
            with pytest.raises(ValueError) as exception_info:
                fake_romsinputdataset_netcdf_local.partition(np_xi=2, np_eta=3)

            expected_message = (
                f"A single input dataset exists in multiple directories: "
                f"{fake_romsinputdataset_netcdf_local.working_path}."
            )
            assert str(exception_info.value) == expected_message

    def test_path_for_roms(self, fake_romsinputdataset_netcdf_local):
        """Test the `path_for_roms` property."""
        existing_files = [
            Path("/some/dir/local_file.0.nc"),
            Path("/some/dir/local_file.1.nc"),
        ]
        fake_romsinputdataset_netcdf_local.partitioning = ROMSPartitioning(
            np_xi=1, np_eta=2, files=existing_files
        )
        assert fake_romsinputdataset_netcdf_local.path_for_roms == [
            Path("/some/dir/local_file.nc"),
        ]

    def test_path_for_roms_raises_if_no_partitioning(
        self, fake_romsinputdataset_netcdf_local
    ):
        with pytest.raises(
            FileNotFoundError, match="ROMS requires files to be partitioned for use"
        ):
            fake_romsinputdataset_netcdf_local.path_for_roms


def test_correction_cannot_be_yaml():
    """Checks that the `validate()` method correctly raises a TypeError if
    `ROMSForcingCorrections.source.source_type` is `yaml` (unsupported)
    """
    with pytest.raises(TypeError) as exception_info:
        ROMSForcingCorrections(
            location="https://www.totallylegityamlfiles.pk/downloadme.yaml"
        )
        expected_msg = (
            "ROMSForcingCorrections cannot be initialized with a source YAML file."
        )
        assert expected_msg in str(exception_info.value)
