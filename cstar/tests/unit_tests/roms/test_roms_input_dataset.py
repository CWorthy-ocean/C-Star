import logging
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from cstar.io.source_data import SourceDataCollection
from cstar.io.staged_data import StagedDataCollection
from cstar.roms import ROMSPartitioning
from cstar.tests.unit_tests.fake_abc_subclasses import FakeROMSInputDataset


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

    def test_str_with_partitioned_files(self, romsinputdataset_local_netcdf):
        """Test the ROMSInputDataset string representation has correct substring for
        partitioned_files.

        Fixtures
        --------
        - romsinputdataset_local_netcdf: Provides a ROMSInputDataset with a local NetCDF source.

        Asserts
        -------
        - String representation of the dataset includes the list of
          partitioned files in the correct format.
        """
        romsinputdataset_local_netcdf.partitioning = ROMSPartitioning(
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

        actual_str = str(romsinputdataset_local_netcdf).strip()
        assert expected_str in actual_str, (
            f"Expected:\n{expected_str}\nBut got:\n{actual_str}"
        )

    def test_repr_with_partitioned_files(self, romsinputdataset_local_netcdf):
        """Test the ROMSInputDataset repr includes `partitioned_files`.

        This test ensures that the `repr` output of a `ROMSInputDataset` object
        contains the `partitioned_files` attribute formatted as expected.

        Fixtures
        --------
        - `romsinputdataset_local_netcdf`: Provides a mock ROMSInputDataset object
          with a local NetCDF source.

        Asserts
        -------
        - The `partitioned_files` attribute is included in the repr output.
        - The format of the `partitioned_files` list matches the expected string output.
        """
        romsinputdataset_local_netcdf.partitioning = ROMSPartitioning(
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
        actual_repr = repr(romsinputdataset_local_netcdf)

        # Normalize whitespace for comparison
        expected_repr_normalized = " ".join(expected_repr.split())
        actual_repr_normalized = " ".join(actual_repr.split())

        assert expected_repr_normalized in actual_repr_normalized, (
            f"Expected:\n{expected_repr}\nBut got:\n{actual_repr}"
        )

    def test_repr_with_partitioned_files_and_working_copy(
        self,
        stagedfile_remote_source,
        romsinputdataset_remote_netcdf,
    ):
        """Test the ROMSInputDataset repr includes `partitioned_files` and
        `working_copy`.
        """
        romsinputdataset_remote_netcdf.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                "remote_file.001.nc",
                "remote_file.002.nc",
            ],
        )

        romsinputdataset_remote_netcdf._working_copy = stagedfile_remote_source()
        expected_repr = dedent(
            """\
            State: <working_copy = some/local/dir/remote_file.nc, partitioning = ROMSPartitioning(np_xi=1, np_eta=2, files=['remote_file.001.nc', 'remote_file.002.nc']) >
        """
        ).strip()
        actual_repr = repr(romsinputdataset_remote_netcdf)

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
    """

    def setup_method(self):
        """Set up common patches and mocks for each test in TestROMSInputDatasetGet."""
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

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_get_skips_if_files_exist_with_partitioned_source(
        self,
        mock_exists_locally,
        romsinputdataset_remote_partitioned_source,
        stageddatacollection_remote_files,
        caplog: pytest.LogCaptureFixture,
        mock_path_resolve,
    ):
        """Test that get() skips already staged files from a partitioned_source"""
        caplog.set_level(
            logging.INFO,
            logger=romsinputdataset_remote_partitioned_source.log.name,
        )

        # Mock the `exists_locally` property to return True
        mock_exists_locally.return_value = True

        # Mock `working_path` to be a list pointing to files in `some/local/dir`
        staged_path_parent = Path("/some/local/dir")
        staged_paths = [
            staged_path_parent / f.basename
            for f in romsinputdataset_remote_partitioned_source.partitioned_source.sources
        ]

        staged = stageddatacollection_remote_files(
            paths=staged_paths,
            sources=romsinputdataset_remote_partitioned_source.partitioned_source.sources,
        )

        with mock.patch.object(
            FakeROMSInputDataset,
            "working_copy",
            new_callable=mock.PropertyMock(return_value=staged),
        ):
            romsinputdataset_remote_partitioned_source.get(local_dir=staged_path_parent)

        # Assert the skip message was printed
        assert "already exists, skipping." in caplog.text

        # Ensure no further operations were performed
        self.mock_get.assert_not_called()
        self.mock_yaml_load.assert_not_called()

        mock_path_resolve.assert_called()

    @mock.patch(
        "cstar.roms.input_dataset.ROMSInputDataset._get_from_partitioned_source",
        autospec=True,
    )
    def test_get_with_partitioned_source(
        self,
        mock_get_from_partitioned_source,
        romsinputdataset_remote_partitioned_source,
        mock_path_resolve,
    ):
        """Tests the 'get' method calls _get_from_partitioned_source when the
        ROMSInputDataset has a partitioned source.
        """
        # Set source partitioning attributes

        romsinputdataset_remote_partitioned_source.get(local_dir=Path("some/local/dir"))
        mock_get_from_partitioned_source.assert_called_once_with(
            romsinputdataset_remote_partitioned_source,
            local_dir=Path("some/local/dir"),
        )

        mock_path_resolve.assert_called()

    def test_get_from_partitioned_source_calls_sourcedatacollection_stage(
        self,
        romsinputdataset_remote_partitioned_source,
    ):
        """Tests the _get_from_partitioned_source helper method.

        This test takes an example ROMSInputDataset whose source is partitioned into
        12 files and calls `_get_from_partitioned_source` on it, verifying that `stage`
        is called once on the corresponding SourceDataCollection
        """
        dataset = romsinputdataset_remote_partitioned_source
        with mock.patch.object(SourceDataCollection, "stage") as mock_stage:
            dataset._get_from_partitioned_source(local_dir=Path("some/local/dir"))
            mock_stage.assert_called_once()

    def test_get_from_partitioned_source_updates_working_copy(
        self, romsinputdataset_remote_partitioned_source, mock_path_resolve
    ):
        """Tests the `working_copy` attribute is updated by `get_from_partitioned_source`."""
        dataset = romsinputdataset_remote_partitioned_source  #
        dataset.get(local_dir=Path("some/local/dir"))
        assert isinstance(dataset.working_copy, StagedDataCollection)
        expected_paths = [
            Path("some/local/dir") / s.basename
            for s in dataset.partitioned_source.sources
        ]
        assert dataset.working_copy.paths == expected_paths


class TestROMSInputDatasetPartition:
    """Test class for the `ROMSInputDataset.partition` method."""

    def setup_method(self):
        self.patch_mkdir = mock.patch("pathlib.Path.mkdir", autospec=True)
        self.mock_mkdir = self.patch_mkdir.start()

    def teardown_method(self):
        """Stop all patches."""
        mock.patch.stopall()

    def test_source_partitioning(self, romsinputdataset_local_netcdf):
        """Test the ROMSInputDataset.source_partitioning property."""
        romsinputdataset_local_netcdf.source_np_xi = 4
        romsinputdataset_local_netcdf.source_np_eta = 3

        assert romsinputdataset_local_netcdf.source_partitioning == (
            4,
            3,
        )

    def test_to_dict_with_source_partitioning(self, romsinputdataset_local_netcdf):
        """Test the ROMSInputDataset.to_dict() method with a partitioned source file."""
        romsinputdataset_local_netcdf.source_np_xi = 4
        romsinputdataset_local_netcdf.source_np_eta = 3

        test_dict = romsinputdataset_local_netcdf.to_dict()
        assert test_dict["source_np_xi"] == 4
        assert test_dict["source_np_eta"] == 3

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_partition_single_file(
        self,
        mock_exists_locally,
        mock_partition_netcdf,
        romsinputdataset_remote_netcdf,
        stagedfile_remote_source,
    ):
        """Ensures that a single NetCDF file is partitioned and tracked correctly."""
        np_xi, np_eta = 2, 3
        num_partitions = np_xi * np_eta

        # Spoof existing local files:
        dataset = romsinputdataset_remote_netcdf
        dataset._working_copy = stagedfile_remote_source()
        # Mock the `exists_locally` property to return True
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
            dataset.partition(np_xi=np_xi, np_eta=np_eta)

            # Assert partition_netcdf is called with the correct arguments
            mock_partition_netcdf.assert_called_once_with(
                dataset.working_copy.path,
                np_xi=np_xi,
                np_eta=np_eta,
            )

            assert dataset.partitioning.files == expected_partitioned_files

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_partition_multiple_files(
        self,
        mock_exists_locally,
        mock_partition_netcdf,
        romsinputdataset_remote_netcdf,
        stageddatacollection_remote_files,
    ):
        """Tests partitioning behavior when multiple files are provided to partition_netcdf"""
        np_xi, np_eta = 2, 2
        num_partitions = np_xi * np_eta

        # Spoof existing local files:
        mock_exists_locally.return_value = True
        dataset = romsinputdataset_remote_netcdf
        dataset._working_copy = stageddatacollection_remote_files(
            paths=[
                Path("some/local/dir/file1.nc"),
                Path("some/local/dir/file2.nc"),
            ]
        )

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
            romsinputdataset_remote_netcdf.partition(np_xi=np_xi, np_eta=np_eta)

            # Assert partition_netcdf is called for each file
            assert mock_partition_netcdf.call_count == len(
                romsinputdataset_remote_netcdf.working_copy.paths
            )
            assert (
                romsinputdataset_remote_netcdf.partitioning.files
                == expected_partitioned_files
            )

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_skips_if_already_partitioned(
        self,
        mock_partition_netcdf,
        romsinputdataset_local_netcdf,
        caplog: pytest.LogCaptureFixture,
    ):
        """Tests that no action is taken if a ROMSInputDataset has already been
        partitioned.

        Mocks & Fixtures
        ----------------
        mock_partition_netcdf (MagicMock)
            Mocks the roms_tools.partition_netcdf method to check whether it is called
        romsinputdataset_local_netcdf (ROMSInputDataset)
            Provides a dataset for testing
        caplog
            Builtin fixture to capture logging messages

        Asserts
        -------
        - Confirms that an appropriate message is logged
        - Confirms that roms_tools.partition_netcdf is not called
        """
        caplog.set_level(logging.INFO, logger=romsinputdataset_local_netcdf.log.name)

        romsinputdataset_local_netcdf.partitioning = ROMSPartitioning(
            np_xi=1,
            np_eta=2,
            files=[
                Path("/some/dir/local_file.0.nc"),
                Path("/some/dir/local_file.1.nc"),
            ],
        )
        romsinputdataset_local_netcdf.partition(np_xi=1, np_eta=2)

        assert "FakeROMSInputDataset already partitioned, skipping" in caplog.text
        mock_partition_netcdf.assert_not_called

    @mock.patch("cstar.roms.input_dataset.roms_tools.partition_netcdf")
    def test_partition_raises_if_already_partitioned_differently(
        self, mock_partition_netcdf, romsinputdataset_local_netcdf
    ):
        """Test that a FileExistsError is raised if this ROMSInputDataset has been
        partitioned in a different arrangement to that requested.

        This test takes an example ROMSInputDataset with partitioning (1,2) and then
        calls `partition` requesting partitioning of (3,4)

        Mocks & Fixtures
        ----------------
        mock_partition_netcdf (MagicMock)
            Mocks the roms_tools.partition_netcdf method to confirm it was not called
        romsinputdataset_local_netcdf (ROMSInputDataset)
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
            romsinputdataset_local_netcdf.partitioning = ROMSPartitioning(
                np_xi=1,
                np_eta=2,
                files=["/some/dir/local_file.0.nc", "/some/dir/local_file.1.nc"],
            )
            romsinputdataset_local_netcdf.partition(np_xi=3, np_eta=4)

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
        romsinputdataset_remote_netcdf,
        stagedfile_remote_source,
    ):
        """Test that existing partitioned files are restored if _repeat_ partitioning
        fails.

        This test simulates a scenario where `partition()` is called on an
        already partitioned ROMSInputDataset with `overwrite_existing_files=True`,
        but the actual partitioning operation raises an exception.
        The test ensures that any existing partitioned
        files are backed up before the attempt, and correctly restored after
        the failure.
        """
        # Spoof existing local files:
        dataset = romsinputdataset_remote_netcdf
        dataset._working_copy = stagedfile_remote_source()

        existing_files = [
            Path("/some/dir/local_file.0.nc"),
            Path("/some/dir/local_file.1.nc"),
        ]
        dataset.partitioning = ROMSPartitioning(np_xi=1, np_eta=2, files=existing_files)
        dataset.working_path = "/some/dir/local_file.nc"
        mock_exists.return_value = True
        mock_partition_netcdf.side_effect = RuntimeError("simulated failure")

        with mock.patch.object(
            Path, "resolve", autospec=True, side_effect=existing_files * 2
        ):
            with pytest.raises(RuntimeError, match="simulated failure"):
                dataset.partition(np_xi=3, np_eta=3, overwrite_existing_files=True)

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

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_partition_raises_when_not_local(
        self,
        mock_exists_locally,
        romsinputdataset_local_netcdf,
    ):
        """Confirms an error is raised when `ROMSInputDataset.exists_locally` is False"""
        # Simulate a dataset that does not exist locally

        mock_exists_locally.return_value = False

        with pytest.raises(ValueError) as exception_info:
            romsinputdataset_local_netcdf.partition(np_xi=2, np_eta=3)

        expected_message = (
            f"local path(s) to InputDataset \n {romsinputdataset_local_netcdf._local}, "
            + "refers to a non-existent file(s)"
            + "\n call InputDataset.get() and try again."
        )
        assert str(exception_info.value) == expected_message

    @mock.patch(
        "cstar.base.input_dataset.InputDataset.exists_locally",
        new_callable=mock.PropertyMock,
    )
    def test_partition_raises_with_mismatched_directories(
        self,
        mock_exists_locally,
        romsinputdataset_remote_netcdf,
        stageddatacollection_remote_files,
    ):
        """Tests partition_netcdf raises if files provided span multiple directories."""
        # Set up the dataset with files in different directories
        dataset = romsinputdataset_remote_netcdf
        dataset._working_copy = stageddatacollection_remote_files(
            paths=[
                Path("some/local/source/path/file1.nc"),
                Path("some/other/source/path/file2.nc"),
            ]
        )
        mock_exists_locally.return_value = True

        # Expect a ValueError due to mismatched directories
        with pytest.raises(ValueError) as exception_info:
            romsinputdataset_remote_netcdf.partition(np_xi=2, np_eta=3)

        expected_message = (
            f"A single input dataset exists in multiple directories: "
            f"{romsinputdataset_remote_netcdf.working_copy.paths}."
        )
        assert str(exception_info.value) == expected_message

    def test_path_for_roms(self, romsinputdataset_local_netcdf):
        """Test the `path_for_roms` property."""
        existing_files = [
            Path("/some/dir/local_file.0.nc"),
            Path("/some/dir/local_file.1.nc"),
        ]
        romsinputdataset_local_netcdf.partitioning = ROMSPartitioning(
            np_xi=1, np_eta=2, files=existing_files
        )
        assert romsinputdataset_local_netcdf.path_for_roms == [
            Path("/some/dir/local_file.nc"),
        ]

    def test_path_for_roms_raises_if_no_partitioning(
        self, romsinputdataset_local_netcdf
    ):
        with pytest.raises(
            FileNotFoundError, match="ROMS requires files to be partitioned for use"
        ):
            romsinputdataset_local_netcdf.path_for_roms


def test_correction_cannot_be_yaml(
    mocksourcedata_remote_text_file, roms_forcing_corrections
):
    """Checks that the `validate()` method correctly raises a TypeError if
    `ROMSForcingCorrections.source.source_type` is `yaml` (unsupported)
    """
    location = "https://www.totallylegityamlfiles.pk/downloadme.yaml"
    source_data = mocksourcedata_remote_text_file(location=location)

    with pytest.raises(TypeError) as exception_info:
        roms_forcing_corrections(location=location, sourcedata=source_data)
    expected_msg = (
        "ROMSForcingCorrections cannot be initialized with a source YAML file."
    )
    assert expected_msg in str(exception_info.value)
