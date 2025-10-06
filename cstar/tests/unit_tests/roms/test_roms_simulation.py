import logging
import pickle
import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, PropertyMock, mock_open, patch

import pytest
import yaml

from cstar.base.additional_code import AdditionalCode
from cstar.base.external_codebase import ExternalCodeBase
from cstar.execution.handler import ExecutionStatus
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.roms import ROMSRuntimeSettings
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.input_dataset import (
    ROMSBoundaryForcing,
    ROMSCdrForcing,
    ROMSForcingCorrections,
    ROMSInitialConditions,
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSRiverForcing,
    ROMSSurfaceForcing,
    ROMSTidalForcing,
)
from cstar.roms.simulation import ROMSSimulation
from cstar.system.environment import CStarEnvironment
from cstar.system.manager import cstar_sysmgr


class TestROMSSimulationInitialization:
    """Unit tests for initializing a `ROMSSimulation` instance.

    This test class verifies that the `ROMSSimulation` object is correctly initialized
    with required and optional parameters, handles errors appropriately, and assigns
    default values where necessary.

    Tests
    -----
    - `test_init`: Ensures correct assignment of attributes when all required parameters
      are provided.
    - `test_init_raises_if_no_discretization`: Verifies that a `ValueError` is raised
      if `discretization` is not provided.
    - `test_init_raises_if_no_runtime_code`: Verifies that a `ValueError` is raised
      if `runtime_code` is not provided.
    - `test_init_raises_if_no_compile_time_code`: Ensures a `NotImplementedError`
      is raised when `compile_time_code` is missing.
    - `test_default_codebase_assignment`: Confirms that default external codebases
      are correctly assigned if not explicitly provided.
    - `test_check_forcing_collection`: Confirms that an error is raised if forcing
      attributes corresponding to lists are incorrectly typed
    - `test_codebases`: Ensures the `codebases` property correctly returns the list
      of external codebases associated with the simulation.
    - `test_input_datasets`: Ensures the `input_datasets` property correctly returns
      a list of `ROMSInputDataset` instances.
    - `test_check_inputdataset_dates_warns_and_sets_start_date`:
      Test that `_check_inputdataset_dates` warns and overrides mismatched `start_date`
    - `test_check_inputdataset_dates_warns_and_sets_end_date`:
      Test that `_check_inputdataset_dates` warns and overrides mismatched `end_date`
    """

    def test_init(self, fake_romssimulation):
        """Test correct initialization of a `ROMSSimulation` instance.

        This test ensures that a `ROMSSimulation` object is properly instantiated
        when provided with all required parameters.

        Assertions
        ----------
        - Verifies that attributes match expected values.
        - Checks that private attributes related to execution remain `None` initially.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance
          and its corresponding directory.
        """
        sim = fake_romssimulation

        assert sim.name == "ROMSTest"
        assert sim.directory.is_dir()
        assert sim.discretization.__dict__ == {
            "time_step": 60,
            "n_procs_x": 2,
            "n_procs_y": 3,
        }

        assert sim.codebase.source_repo == "http://my.code/repo.git"
        assert sim.codebase.checkout_target == "dev"
        assert sim.runtime_code.source.location == str(sim.directory.parent)
        assert sim.runtime_code.subdir == "subdir/"
        assert sim.runtime_code.checkout_target == "main"
        assert sim.runtime_code.files == [
            "file1",
            "file2.in",
            "marbl_in",
            "marbl_tracer_output_list",
            "marbl_diagnostic_output_list",
        ]
        assert sim.compile_time_code.source.location == str(sim.directory.parent)
        assert sim.compile_time_code.subdir == "subdir/"
        assert sim.compile_time_code.checkout_target == "main"
        assert sim.compile_time_code.files == ["file1.h", "file2.opt"]

        assert sim.marbl_codebase.source_repo == "http://marbl.com/repo.git"
        assert sim.marbl_codebase.checkout_target == "v1"

        assert sim.start_date == datetime(2025, 1, 1)
        assert sim.end_date == datetime(2025, 12, 31)
        assert sim.valid_start_date == datetime(2024, 1, 1)
        assert sim.valid_end_date == datetime(2026, 1, 1)

        assert isinstance(sim.model_grid, ROMSModelGrid)
        assert sim.model_grid.source.location == "http://my.files/grid.nc"
        assert sim.model_grid.source.file_hash == "123"

        assert isinstance(sim.initial_conditions, ROMSInitialConditions)
        assert sim.initial_conditions.source.location == "http://my.files/initial.nc"
        assert sim.initial_conditions.source.file_hash == "234"

        assert isinstance(sim.tidal_forcing, ROMSTidalForcing)
        assert sim.tidal_forcing.source.location == "http://my.files/tidal.nc"
        assert sim.tidal_forcing.source.file_hash == "345"

        assert isinstance(sim.cdr_forcing, ROMSCdrForcing)
        assert sim.cdr_forcing.source.location == "http://my.files/cdr.nc"
        assert sim.cdr_forcing.source.file_hash == "542"

        assert isinstance(sim.boundary_forcing, list)
        assert [isinstance(x, ROMSBoundaryForcing) for x in sim.boundary_forcing]
        assert sim.boundary_forcing[0].source.location == "http://my.files/boundary.nc"
        assert sim.boundary_forcing[0].source.file_hash == "456"

        assert isinstance(sim.surface_forcing, list)
        assert [isinstance(x, ROMSSurfaceForcing) for x in sim.surface_forcing]
        assert sim.surface_forcing[0].source.location == "http://my.files/surface.nc"
        assert sim.surface_forcing[0].source.file_hash == "567"

        # Uninitialized at outset/private
        assert sim.exe_path is None
        assert sim._exe_hash is None
        assert sim._execution_handler is None

    def test_init_raises_if_no_discretization(self, tmp_path):
        """Ensure that `ROMSSimulation` raises a `ValueError` if no discretization is
        provided.

        This test verifies that an attempt to instantiate `ROMSSimulation` without
        a required `discretization` parameter results in a `ValueError`.

        Assertions
        ----------
        - Ensures that a `ValueError` is raised when `discretization=None`.

        Mocks & Fixtures
        ----------------
        - `tmp_path` : A temporary directory fixture provided by `pytest` for
          safely testing file system interactions.
        """
        with pytest.raises(ValueError, match="could not find 'discretization' entry"):
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=None,
                runtime_code=AdditionalCode(location="some/dir"),
                compile_time_code=AdditionalCode(location="some/dir"),
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
            )

    def test_init_raises_if_no_runtime_code(self, tmp_path):
        """Ensure that `ROMSSimulation` raises a `ValueError` if no runtime code is
        provided.

        This test checks that instantiating `ROMSSimulation` without a `runtime_code`
        parameter results in a `ValueError` with an expected message substring.

        Assertions
        ----------
        - Ensures that a `ValueError` is raised when `runtime_code=None`.

        Mocks & Fixtures
        ----------------
        - `tmp_path` : A temporary directory fixture provided by `pytest` for
          safely testing file system interactions.
        """
        with pytest.raises(ValueError, match="could not find 'runtime_code' entry"):
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=ROMSDiscretization(time_step=60),
                runtime_code=None,
                compile_time_code=AdditionalCode(location="some/dir"),
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
            )

    def test_init_raises_if_no_compile_time_code(self, tmp_path):
        """Test `ROMSSimulation` raises a `NotImplementedError` if no compile-time code
        is provided.

        This test verifies that attempting to instantiate `ROMSSimulation` without a
        `compile_time_code` parameter results in a `NotImplementedError`. The error message
        should indicate that the `compile_time_code` entry is missing.

        Assertions
        ----------
        - Ensures that a `NotImplementedError` is raised when `compile_time_code=None`.

        Mocks & Fixtures
        ----------------
        - `tmp_path` : A temporary directory fixture provided by `pytest` for
          safely testing file system interactions.
        """
        with pytest.raises(
            NotImplementedError, match="could not find a 'compile_time_code' entry"
        ):
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=ROMSDiscretization(time_step=60),
                runtime_code=AdditionalCode(location="some/dir"),
                compile_time_code=None,
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
            )

    def test_default_codebase_assignment(self, tmp_path, caplog):
        """Ensure `ROMSSimulation` reverts to default codebases when not provided.

        This test verifies that if no `codebase` or `marbl_codebase` is explicitly
        passed to `ROMSSimulation`, the instance correctly assigns default values.

        Assertions
        ----------
        - An information message is logged
        - Ensures that `sim.codebase` is an instance of `ROMSExternalCodeBase`.
        - Ensures that `sim.codebase.source_repo` and `sim.codebase.checkout_target` match
          the default ROMS repository settings.
        - Ensures that `sim.marbl_codebase` is an instance of `MARBLExternalCodeBase`.
        - Ensures that `sim.marbl_codebase.source_repo` and `sim.marbl_codebase.checkout_target`
          match the default MARBL repository settings.
        - A `UserWarning` is issued when no explicit MARBL codebase is provided.

        Mocks & Fixtures
        ----------------
        - tmp_path (pathlib.Path)
            Builtin fixture providing a temporary filepath
        - caplog (pytest.LogCaptureFixture)
            Builtin fixture capturing log messages
        """
        sim = ROMSSimulation(
            name="test",
            directory=tmp_path,
            discretization=ROMSDiscretization(time_step=60),
            runtime_code=AdditionalCode(
                location="some/dir",
                files=[
                    "roms.in",
                ],
            ),
            compile_time_code=AdditionalCode(location="some/dir"),
            start_date="2012-01-01",
            end_date="2012-01-02",
            valid_start_date="2012-01-01",
            valid_end_date="2012-01-02",
        )
        caplog.set_level(logging.INFO, logger=sim.log.name)
        assert "default codebase will be used" in caplog.text

        assert isinstance(sim.codebase, ROMSExternalCodeBase)
        assert (
            sim.codebase.source_repo == "https://github.com/CWorthy-ocean/ucla-roms.git"
        )
        assert sim.codebase.checkout_target == "main"

        assert isinstance(sim.marbl_codebase, MARBLExternalCodeBase)
        assert (
            sim.marbl_codebase.source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )
        assert sim.marbl_codebase.checkout_target == "marbl0.45.0"

    def test_find_dotin_file(self, fake_romssimulation):
        """Test that the `_find_dotin_file` helper function correctly finds and sets the
        `_in_file` non-public attribute to the `.in` file.
        """
        sim = fake_romssimulation
        assert sim._in_file == "file2.in"

    def test_find_dotin_file_raises_if_no_dot_in_files(self, fake_romssimulation):
        """Test that the `_find_dotin_file` helper function correctly raises a
        ValueError if a single `.in` file is not found.
        """
        sim = fake_romssimulation
        sim.runtime_code = AdditionalCode(
            location="some/dir", files=["no", "dotin.files", "here"]
        )
        with pytest.raises(RuntimeError):
            sim._find_dotin_file()

    @pytest.mark.parametrize(
        "attrname, expected_type_name",
        [
            ("surface_forcing", "ROMSSurfaceForcing"),
            ("boundary_forcing", "ROMSBoundaryForcing"),
            ("forcing_corrections", "ROMSForcingCorrections"),
        ],
    )
    def test_check_forcing_collection(
        self, tmp_path: Path, attrname: str, expected_type_name: str
    ) -> None:
        """Ensure a TypeError is raised when attributes of ROMSSimulation that should be
        lists of ROMSInputDatasets (e.g., ROMSSurfaceForcing, ROMSBoundaryForcing,
        ROMSForcingCorrections) are not.

        Parameters
        ----------
        attrname : str
            The attribute being checked (e.g., "surface_forcing").
        expected_type_name : str
            The name of the expected input dataset class for elements of the attribute.

        Assertions
        ----------
        - A `TypeError` is raised when attributes that should be lists of ROMSInputDatasets
          are incorrectly set to other types.

        Mocks & Fixtures
        ----------------
        tmp_path : Path
            Temporary directory fixture provided by `pytest` for safely
            testing file system interactions.
        """
        expected_msg = f"must be a list of {expected_type_name} instances"
        test_args = cast(dict[str, Any], {attrname: ["this", "is", "not", "valid"]})

        with pytest.raises(TypeError) as exception_info:
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=ROMSDiscretization(time_step=60),
                codebase=ROMSExternalCodeBase(),
                runtime_code=AdditionalCode(location="some/dir"),
                compile_time_code=AdditionalCode(location="some/dir"),
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
                **test_args,
            )

            assert expected_msg in str(exception_info.value)

    def test_codebases(self, fake_romssimulation):
        """Test that the `codebases` property correctly lists the `ExternalCodeBase`
        instances.

        This test verifies that the `codebases` property returns a list containing
        both the ROMS and MARBL external codebases associated with the `ROMSSimulation`
        instance.

        Assertions
        ----------
        - `codebases` is a list.
        - The first element in the list is an instance of `ROMSExternalCodeBase`.
        - The second element in the list is an instance of `MARBLExternalCodeBase`.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : A fixture providing an initialized
          `ROMSSimulation` instance.
        """
        sim = fake_romssimulation
        assert isinstance(sim.codebases, list)
        assert isinstance(sim.codebases[0], ROMSExternalCodeBase)
        assert isinstance(sim.codebases[1], MARBLExternalCodeBase)
        assert sim.codebases[0] == sim.codebase
        assert sim.codebases[1] == sim.marbl_codebase

    @patch.object(ROMSInputDataset, "path_for_roms")
    def test_forcing_paths(self, mock_path_for_roms, fake_romssimulation):
        """Test that the `_forcing_paths` property correctly takes any forcing-related
        InputDatasets associated with the ROMSSimulation and returns a list of paths to
        the relevant forcing files.
        """
        sim = fake_romssimulation
        fake_paths = [
            Path("tidal.nc"),
            [Path("surface.nc"), Path("surface2.nc")],
            Path("boundary.nc"),
            Path("sw_corr.nc"),
        ]
        datasets = [
            sim.tidal_forcing,
            sim.surface_forcing,
            sim.boundary_forcing,
            sim.forcing_corrections,
        ]

        # Set at least one forcing type to None to check handling
        sim.river_forcing = None

        # Set working paths of forcing types to fake paths
        for ds, fake_path in zip(datasets, fake_paths):
            for d in ds if isinstance(ds, list) else [ds]:
                d.path_for_roms = fake_path

        # Flatten list of fake paths (contains a list as an entry)
        flat_paths = [
            i
            for item in fake_paths
            for i in (item if isinstance(item, list) else [item])
        ]
        assert sim._forcing_paths == flat_paths

    def test_forcing_paths_raises_if_path_missing(self, fake_romssimulation):
        sim = fake_romssimulation
        with pytest.raises(FileNotFoundError):
            sim._forcing_paths

    def test_n_time_steps(self, fake_romssimulation):
        sim = fake_romssimulation

        assert sim._n_time_steps == 524160
        pass

    @patch("cstar.roms.simulation.ROMSRuntimeSettings.from_file")
    @patch.object(ROMSSimulation, "_forcing_paths", new_callable=PropertyMock)
    @patch.object(ROMSInitialConditions, "path_for_roms", new_callable=PropertyMock)
    @patch.object(ROMSModelGrid, "path_for_roms", new_callable=PropertyMock)
    def test_roms_runtime_settings(
        self,
        mock_grid_path,
        mock_ini_path,
        mock_forcing_paths,
        mock_from_file,
        fake_romssimulation,
    ):
        """Test that the ROMSSimulation.runtime_settings property correctly returns a
        modified ROMSRuntimeSettings instance containing a combination of parameters
        (mock) read from a file and those overriden by the ROMSSimulation itself (e.g.
        number of timesteps).

        Assertions
        ----------
        - Asserts any parameters overriden by ROMSSimulation attributes are correctly set
        - Asserts that the `marbl_biogeochemistry` entry is None for an alternate
          ROMSSimulation instance where the `runtime_code` attribute corresponds to a single
          (`.in`) file

        Fixtures
        --------
        fake_romssimulation: ROMSSimulation
           A pytest fixture returning a common, example ROMSSimulation instance.

        Mocks
        -----
        mock_forcing_paths: PropertyMock
           Mocks the output of the `ROMSSimulation._forcing_paths` property to return a
           list of imitation forcing filepaths
        mock_from_file: unittest.mock.MagicMock
           Mock the output of ROMSRuntimeSettings.from_file to return a basic, mocked
           ROMSRuntimeSettings instance.
        """
        sim = fake_romssimulation

        # Stop complaints about missing local paths:
        sim.runtime_code.working_path = Path("my_code/")
        # sim.model_grid.working_path = Path("grid.nc")
        # sim.initial_conditions.working_path = Path("ini.nc")

        # mock_roms_path.side_effect = [[Path("grid.nc"),],[Path("ini.nc"),]]
        mock_grid_path.return_value = [
            Path("grid.nc"),
        ]
        mock_ini_path.return_value = [
            Path("fake_ini.nc"),
        ]

        mock_forcing_paths.return_value = [Path("forcing1.nc"), Path("forcing2.nc")]

        mock_settings = ROMSRuntimeSettings(
            title={"title": "test_settings"},
            time_stepping={"ntimes": 100, "dt": 0, "ndtfast": 1, "ninfo": 1},
            bottom_drag={"rdrg": 100, "rdrg2": 10, "zob": 1},
            initial={"nrrec": 2, "ininame": Path("fake_ini.nc")},
            forcing={"filenames": [Path("forcing1.nc"), Path("forcing2.nc")]},
            output_root_name={"output_root_name": "TEST"},
        )

        mock_from_file.return_value = mock_settings

        tested_settings = sim.roms_runtime_settings

        assert tested_settings.title.title == "test_settings"
        assert tested_settings.time_stepping.dt == sim.discretization.time_step
        assert tested_settings.time_stepping.ntimes == sim._n_time_steps
        assert tested_settings.grid.grid == sim.model_grid.path_for_roms[0]
        assert tested_settings.initial.nrrec == 2
        assert (
            tested_settings.initial.ininame == sim.initial_conditions.path_for_roms[0]
        )
        assert tested_settings.forcing.filenames == sim._forcing_paths
        assert (
            tested_settings.marbl_biogeochemistry.marbl_namelist_fname
            == sim.runtime_code.working_path / "marbl_in"
        )
        assert (
            tested_settings.marbl_biogeochemistry.marbl_tracer_list_fname
            == sim.runtime_code.working_path / "marbl_tracer_output_list"
        )
        assert (
            tested_settings.marbl_biogeochemistry.marbl_diag_list_fname
            == sim.runtime_code.working_path / "marbl_diagnostic_output_list"
        )

        # Test with no MARBL files:
        sim.runtime_code = AdditionalCode(location="nowhere", files="onefile.in")
        sim.runtime_code.working_path = Path("my_code/")
        assert sim.roms_runtime_settings.marbl_biogeochemistry is None

        # Test with no grid:
        sim.model_grid = None
        assert sim.roms_runtime_settings.grid is None

    def test_roms_runtime_settings_raises_if_no_runtime_code_working_path(
        self, fake_romssimulation
    ):
        """Test that the ROMSSimulation.runtime_settings property correctly raises a
        ValueError if there is no local `.in` file from which to create a base
        ROMSRuntimeSettings instance.

        Assertions
        ----------
        - A ValueError is raised if the `.in` file is missing

        Fixtures
        --------
        fake_romssimulation: ROMSSimulation
           A pytest fixture returning a common, example ROMSSimulation instance.
        """
        sim = fake_romssimulation
        with pytest.raises(
            ValueError, match="Cannot access runtime settings without local `.in` file."
        ):
            sim.roms_runtime_settings

    def test_input_datasets(self, fake_romssimulation):
        """Test that the `input_datasets` property returns the correct list of
        `ROMSInputDataset` instances.

        This test ensures that `input_datasets` correctly aggregates all input datasets
        associated with the simulation, including the model grid, initial conditions,
        tidal forcing, boundary forcing, and surface forcing.

        Assertions
        ----------
        - The `input_datasets` property returns a list containing:
          - `model_grid`
          - `initial_conditions`
          - `tidal_forcing`
          - All entries in `boundary_forcing`
          - All entries in `surface_forcing`
        - The order of elements in the list matches the expected order.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : A fixture providing an initialized
          `ROMSSimulation` instance.
        """
        sim = fake_romssimulation
        mg = sim.model_grid
        ic = sim.initial_conditions
        td = sim.tidal_forcing
        rf = sim.river_forcing
        bc = sim.boundary_forcing[0]
        sf = sim.surface_forcing[0]
        fc = sim.forcing_corrections[0]

        assert sim.input_datasets == [mg, ic, td, rf, bc, sf, fc]

    @pytest.mark.parametrize(
        "start_date, valid_start_date, end_date, valid_end_date, substring",
        [
            (
                "1992-02-11",
                "1993-05-15",
                "2000-01-01",
                "2000-01-01",
                "before the earliest",
            ),
            (
                "1992-02-11",
                "1992-02-11",
                "2001-01-01",
                "2000-01-01",
                "after the latest",
            ),
            ("1993-05-15", "1900-01-01", "1992-02-11", "2000-01-01", "after end_date"),
        ],
    )
    def test_validate_date_range(
        self,
        tmp_path,
        start_date,
        valid_start_date,
        end_date,
        valid_end_date,
        substring,
    ):
        """Test the _validate_date_range function.

        This test ensures the _validate_date_range function correctly raises
        a ValueError in each of the following cases:
        - The start date is before the valid_start_date
        - The end date is after the valid_end_date
        - The start date is after the end date


        Parameters
        ----------
        start_date : str
           The requested simulation start date.
        valid_start_date : str
           The earliest valid start date for the simulation.
        end_date : str
           The requested simulation end date.
        valid_end_date : str
           The latest valid end date for the simulation.
        substring : str
           A substring expected to be found in the raised error message.

        Mocks & Fixtures
        ----------------
        - `tmp_path` : A pytest fixture providing a temporary directory for filesystem-safe tests.

        Assertions
        ----------
        - Checks that `ROMSSimulation` raises a `ValueError` when the provided dates are invalid.
        - Asserts that the error message contains a specific substring identifying the issue.
        """
        with pytest.raises(ValueError) as exception_info:
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                runtime_code=[],
                compile_time_code=[],
                discretization=ROMSDiscretization(time_step=1),
                start_date=start_date,
                end_date=end_date,
                valid_start_date=valid_start_date,
                valid_end_date=valid_end_date,
            )

            assert substring in str(exception_info.value)

    def test_check_inputdataset_dates_warns_and_sets_start_date(
        self, fake_romssimulation, caplog
    ):
        """Test that `_check_inputdataset_dates` warns and overrides mismatched
        `start_date`.

        This test ensures that when an input dataset (with `source_type='yaml'`) defines a
        `start_date` that differs from the simulation's `start_date`, a `UserWarning` is issued,
        and the input dataset's `start_date` is overwritten.

        Mocks & Fixtures
        ----------------
        fake_romssimulation (cstar.roms.ROMSSimulation)
            Provides a `ROMSSimulation` instance.
        caplog (pytest.LogCaptureFixture)
            Builtin fixture to capture log messages

        Assertions
        ----------
        - A warning is logged
        - The input dataset's `start_date` is set to match the simulation's `start_date`.
        """
        sim = fake_romssimulation
        caplog.set_level(logging.INFO, logger=sim.log.name)

        sim.river_forcing = ROMSInitialConditions(
            location="http://dodgyyamls4u.ru/riv.yaml", start_date="1999-01-01"
        )

        sim._check_inputdataset_dates()

        assert sim.river_forcing.start_date == sim.start_date
        assert "does not match ROMSSimulation.start_date" in caplog.text

    def test_check_inputdataset_dates_warns_and_sets_end_date(
        self, fake_romssimulation, caplog
    ):
        """Test that `_check_inputdataset_dates` warns and overrides mismatched
        `end_date`.

        This test ensures that for non-initial-condition datasets with `source_type='yaml'`,
        a `UserWarning` is issued and the `end_date` is corrected if it differs from the
        simulation's `end_date`.

        Mocks & Fixtures
        ----------------
        fake_romssimulation (cstar.roms.ROMSSimulation)
            Provides a `ROMSSimulation` instance.
        caplog (pytest.LogCaptureFixture)
            Builtin fixture capturing output logs

        Assertions
        ----------
        - An appropriate warning message is logged
        - The input dataset's `end_date` is updated to match the simulation's `end_date`.
        """
        sim = fake_romssimulation
        caplog.set_level(logging.INFO, logger=sim.log.name)
        sim.river_forcing = ROMSRiverForcing(
            location="http://dodgyyamls4u.ru/riv.yaml", end_date="1999-12-31"
        )

        sim._check_inputdataset_dates()
        assert "does not match ROMSSimulation.end_date" in caplog.text
        assert sim.river_forcing.end_date == sim.end_date

    @patch(
        "cstar.roms.simulation.ROMSInputDataset.source_partitioning",
        new_callable=PropertyMock,
    )
    def test_check_inputdataset_partitioning(
        self, mock_source_partitioning, fake_romssimulation
    ):
        mock_source_partitioning.return_value = (120, 360)
        with pytest.raises(ValueError, match="Cannot instantiate ROMSSimulation"):
            sim = fake_romssimulation
            sim._check_inputdataset_partitioning()


class TestStrAndRepr:
    """Test class for the `__str__`, `__repr__`, and `tree` methods of `ROMSSimulation`.

    This test suite ensures that the string representations and tree structure output
    of `ROMSSimulation` instances are correctly formatted and contain the expected
    information.

    Tests
    -----
    - `test_str()`: Verifies that the `__str__` method outputs a well-formatted summary
      of the `ROMSSimulation` instance, including attributes such as name, directory,
      dates, discretization settings, codebases, input datasets, and setup status.
    - `test_repr()`: Ensures that the `__repr__` method provides a valid Python
      expression that reconstructs the `ROMSSimulation` instance with all its attributes.
    - `test_tree()`: Confirms that the `tree` method correctly formats a hierarchical
      directory structure representing runtime code, compile-time code, and input datasets.

    Mocks & Fixtures
    ----------------
    - `fake_romssimulation`: A fixture providing an initialized `ROMSSimulation`
      instance with a populated directory structure.
    """

    @patch.object(AdditionalCode, "exists_locally", new_callable=PropertyMock)
    @patch.object(ROMSSimulation, "roms_runtime_settings", new_callable=PropertyMock)
    def test_str(self, mock_runtime_settings, mock_exists_locally, fake_romssimulation):
        """Test the `__str__` method of `ROMSSimulation`.

        Ensures that calling `str()` on a `ROMSSimulation` instance produces a properly
        formatted string containing key attributes, including name, directory, dates,
        discretization settings, codebases, input datasets, and setup status.

        The expected output is compared against a predefined string representation.

        Mocks & Fixtures
        ----------------
        - `mock_runtime_settings` : mocked ROMRuntimeSettings instance
        - `mock_exists_locally` : mocks ROMSSimulation.runtime_code.exists_locally so
           ROMSRuntimeSettings are included in the __str__
        - `fake_romssimulation` : A fixture providing an initalized `ROMSSimulation`
           instance

        Assertions
        ----------
        - The string representation matches a predefined string.
        """
        sim = fake_romssimulation

        mock_settings = ROMSRuntimeSettings(
            title={"title": "test_settings"},
            time_stepping={"ntimes": 100, "dt": 0, "ndtfast": 1, "ninfo": 1},
            bottom_drag={"rdrg": 100, "rdrg2": 10, "zob": 1},
            initial={"nrrec": 2, "ininame": Path("fake_ini.nc")},
            forcing={"filenames": [Path("forcing1.nc"), Path("forcing2.nc")]},
            output_root_name={"output_root_name": "TEST"},
        )

        mock_runtime_settings.return_value = mock_settings

        expected_str = f"""\
ROMSSimulation
--------------
Name: ROMSTest
Directory: {sim.directory}
Start date: 2025-01-01 00:00:00
End date: 2025-12-31 00:00:00
Valid start date: 2024-01-01 00:00:00
Valid end date: 2026-01-01 00:00:00

Discretization: ROMSDiscretization(time_step = 60, n_procs_x = 2, n_procs_y = 3)

Code:
Codebase: ROMSExternalCodeBase instance (query using ROMSSimulation.codebase)
Runtime code: AdditionalCode instance with 5 files (query using ROMSSimulation.runtime_code)
Compile-time code: AdditionalCode instance with 2 files (query using ROMSSimulation.compile_time_code)
Runtime Settings: ROMSRuntimeSettings instance (query using ROMSSimulation.roms_runtime_settings)

MARBL Codebase: MARBLExternalCodeBase instance (query using ROMSSimulation.marbl_codebase)

Input Datasets:
Model grid: <ROMSModelGrid instance>
Initial conditions: <ROMSInitialConditions instance>
Tidal forcing: <ROMSTidalForcing instance>
River forcing: <ROMSRiverForcing instance>
Surface forcing: <list of 1 ROMSSurfaceForcing instances>
Boundary forcing: <list of 1 ROMSBoundaryForcing instances>
Forcing corrections: <list of 1 ROMSForcingCorrections instances>

Is setup: False"""

        assert sim.__str__() == expected_str

    def test_repr(self, fake_romssimulation):
        """Test the `__repr__` method of `ROMSSimulation`.

        Ensures that calling `repr()` on a `ROMSSimulation` instance returns a
        properly formatted string representation that includes all key attributes.
        This output should be a valid Python expression that can be used to
        reconstruct the instance.

        The expected output is compared against a predefined representation.

        Parameters
        ----------
        fake_romssimulation : fixture
            A fixture providing an initialized `ROMSSimulation` instance.

        Assertions
        ----------
        - The string representation matches a predefined string.
        """
        sim = fake_romssimulation
        expected_repr = f"""\
ROMSSimulation(
name = ROMSTest,
directory = {sim.directory},
start_date = 2025-01-01 00:00:00,
end_date = 2025-12-31 00:00:00,
valid_start_date = 2024-01-01 00:00:00,
valid_end_date = 2026-01-01 00:00:00,
discretization = ROMSDiscretization(time_step = 60, n_procs_x = 2, n_procs_y = 3),
codebase = <ROMSExternalCodeBase instance>,
runtime_code = <AdditionalCode instance>,
compile_time_code = <AdditionalCode instance>
model_grid = <ROMSModelGrid instance>,
initial_conditions = <ROMSInitialConditions instance>,
tidal_forcing = <ROMSTidalForcing instance>,
river_forcing = <ROMSRiverForcing instance>,
surface_forcing = <list of 1 ROMSSurfaceForcing instances>,
boundary_forcing = <list of 1 ROMSBoundaryForcing instances>,
forcing_corrections = <list of 1 ROMSForcingCorrections instances>
)"""
        assert expected_repr == sim.__repr__()

    def test_tree(self, fake_romssimulation, log: logging.Logger):
        """Test the `tree` method of `ROMSSimulation`.

        Ensures that calling `tree()` on a `ROMSSimulation` instance correctly
        returns a hierarchical string representation of the simulation's directory
        structure, including runtime code, compile-time code, and input datasets.

        Parameters
        ----------
        fake_romssimulation : fixture
            A fixture providing an initialized `ROMSSimulation` instance.

        Assertions
        ----------
        - The output of tree() matches a predefined string
        """
        sim = fake_romssimulation
        expected_tree = f"""\
{sim.directory}
└── ROMS
    ├── input_datasets
    │   ├── grid.nc
    │   ├── initial.nc
    │   ├── tidal.nc
    │   ├── river.nc
    │   ├── boundary.nc
    │   ├── surface.nc
    │   └── sw_corr.nc
    ├── runtime_code
    │   ├── file1
    │   ├── file2.in
    │   ├── marbl_in
    │   ├── marbl_tracer_output_list
    │   └── marbl_diagnostic_output_list
    └── compile_time_code
        ├── file1.h
        └── file2.opt
"""

        assert sim.tree() == expected_tree


class TestToAndFromDictAndBlueprint:
    """Tests for the `to_dict`, `from_dict`, `to_blueprint`, and `from_blueprint`
    methods of `ROMSSimulation`.

    This test suite ensures that `ROMSSimulation` instances can be correctly serialized
    to dictionaries and YAML blueprints, and that they can be reconstructed accurately
    from these representations.

    Tests
    -----
    - `test_to_dict`:
        Ensures that calling `to_dict()` produces a dictionary with the correct structure
        and expected key-value pairs.
    - `test_from_dict`:
        Verifies that `from_dict()` reconstructs a `ROMSSimulation` instance that matches
        the original.
    - `test_from_dict_with_single_forcing_entries`:
        Tests that `from_dict()` correctly handles boundary and surface forcing data
        when they are represented as dictionaries instead of lists.
    - `test_dict_roundtrip`:
        Ensures that a `ROMSSimulation` instance serialized with `to_dict()` and
        reconstructed with `from_dict()` retains all properties.
    - `test_to_blueprint`:
        Verifies that `to_blueprint()` correctly writes a YAML file with the expected
        simulation data.
    - `test_from_blueprint_valid_file`:
        Tests `from_blueprint()` with a valid local YAML file.
    - `test_from_blueprint_invalid_filetype`:
        Ensures `from_blueprint()` raises an error when given a non-YAML file.
    - `test_from_blueprint_url`:
        Verifies that `from_blueprint()` correctly loads a YAML blueprint from a URL.
    - `test_blueprint_roundtrip`:
        Ensures that a `ROMSSimulation` instance serialized with `to_blueprint()` and
        reconstructed with `from_blueprint()` retains all properties.
    """

    def setup_method(self):
        """Sets a common dictionary representation of the ROMSSimulation instance
        associated with the `fake_romssimulation` fixture to be used across tests in
        this class.
        """
        self.example_simulation_dict = {
            "name": "ROMSTest",
            "valid_start_date": datetime(2024, 1, 1, 0, 0),
            "valid_end_date": datetime(2026, 1, 1, 0, 0),
            "codebase": {
                "source_repo": "http://my.code/repo.git",
                "checkout_target": "dev",
            },
            "discretization": {"time_step": 60, "n_procs_x": 2, "n_procs_y": 3},
            "runtime_code": {
                "location": "some/dir",
                "subdir": "subdir/",
                "checkout_target": "main",
                "files": [
                    "file1",
                    "file2.in",
                    "marbl_in",
                    "marbl_tracer_output_list",
                    "marbl_diagnostic_output_list",
                ],
            },
            "compile_time_code": {
                "location": "some/dir",
                "subdir": "subdir/",
                "checkout_target": "main",
                "files": ["file1.h", "file2.opt"],
            },
            "marbl_codebase": {
                "source_repo": "http://marbl.com/repo.git",
                "checkout_target": "v1",
            },
            "model_grid": {"location": "http://my.files/grid.nc", "file_hash": "123"},
            "initial_conditions": {
                "location": "http://my.files/initial.nc",
                "file_hash": "234",
            },
            "tidal_forcing": {
                "location": "http://my.files/tidal.nc",
                "file_hash": "345",
            },
            "river_forcing": {
                "location": "http://my.files/river.nc",
                "file_hash": "543",
            },
            "cdr_forcing": {
                "location": "http://my.files/cdr.nc",
                "file_hash": "542",
            },
            "surface_forcing": [
                {"location": "http://my.files/surface.nc", "file_hash": "567"}
            ],
            "boundary_forcing": [
                {"location": "http://my.files/boundary.nc", "file_hash": "456"},
            ],
            "forcing_corrections": [
                {"location": "http://my.files/sw_corr.nc", "file_hash": "890"}
            ],
        }

    def test_to_dict(self, fake_romssimulation):
        """Tests that `to_dict()` correctly represents a `ROMSSimulation` instance in a
        dictionary.

        This test ensures that the dictionary returned by `to_dict()` contains all expected
        key-value pairs unique to `ROMSSimulation` (excluding those inherited from `Simulation`,
        which are tested separately).

        Assertions
        --------------
        - The dictionary contains the correct MARBL codebase information.
        - The model grid, initial conditions, tidal forcing, boundary forcing, and
          surface forcing data match the expected values.

        Mocks & Fixtures
        ---------------------
        - `fake_romssimulation`: A fixture providing a pre-configured `ROMSSimulation` instance.
        """
        sim = fake_romssimulation
        test_dict = sim.to_dict()

        assert (
            test_dict["marbl_codebase"]
            == self.example_simulation_dict["marbl_codebase"]
        )
        assert test_dict["model_grid"] == self.example_simulation_dict["model_grid"]
        assert (
            test_dict["initial_conditions"]
            == self.example_simulation_dict["initial_conditions"]
        )
        assert (
            test_dict["tidal_forcing"] == self.example_simulation_dict["tidal_forcing"]
        )
        assert (
            test_dict["boundary_forcing"]
            == self.example_simulation_dict["boundary_forcing"]
        )
        assert (
            test_dict["surface_forcing"]
            == self.example_simulation_dict["surface_forcing"]
        )

    def test_from_dict(self, fake_romssimulation):
        """Tests that `from_dict()` correctly reconstructs a `ROMSSimulation` instance.

        This test verifies that calling `from_dict()` with a valid simulation dictionary
        results in an instance matching the expected `ROMSSimulation`.

        Assertions
        --------------
        - The reconstructed instance matches the expected instance byte-for-byte.
        - The properties of the new instance are identical to the expected instance.

        Mocks & Fixtures
        ---------------------
        - `fake_romssimulation`: A fixture providing a pre-configured `ROMSSimulation` instance.
        """
        sim = fake_romssimulation
        sim_dict = self.example_simulation_dict
        sim_dict["runtime_code"]["location"] = sim.directory.parent
        sim_dict["compile_time_code"]["location"] = sim.directory.parent
        sim2 = ROMSSimulation.from_dict(
            sim_dict,
            directory=sim.directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )

        assert pickle.dumps(sim2) == pickle.dumps(sim), "Instances are not identical"

    def test_from_dict_with_single_forcing_entries(self, tmp_path):
        """Tests that `from_dict()` works with single surface and boundary forcing or
        forcing correction entries.

        This test ensures that when `surface_forcing` and `boundary_forcing` are provided
        as dictionaries (instead of lists), they are correctly converted into lists of
        `ROMSSurfaceForcing` and `ROMSBoundaryForcing` instances.

        Assertions
        ----------
        - The `boundary_forcing` attribute is a list.
        - The `surface_forcing` attribute is a list.
        - The `forcing_corrections` attribute is a list
        - Each item in `boundary_forcing` is an instance of `ROMSBoundaryForcing`.
        - Each item in `surface_forcing` is an instance of `ROMSSurfaceForcing`.
        - Each item in `forcing_corrections` is an instance of `ROMSForcingCorrections`.
        - The properties of the reconstructed instances match the input data.

        Mocks & Fixtures
        ----------------
        - `tmp_path`: A pytest fixture providing a temporary directory for testing.
        """
        sim_dict = self.example_simulation_dict.copy()
        sim_dict["surface_forcing"] = {
            "location": "http://my.files/surface.nc",
            "file_hash": "567",
        }
        sim_dict["boundary_forcing"] = {
            "location": "http://my.files/boundary.nc",
            "file_hash": "456",
        }
        sim_dict["forcing_corrections"] = {
            "location": "http://my.files/sw_corr.nc",
            "file_hash": "345",
        }

        sim = ROMSSimulation.from_dict(
            sim_dict, directory=tmp_path, start_date="2024-01-01", end_date="2024-01-02"
        )

        assert isinstance(sim.boundary_forcing, list)
        assert [isinstance(x, ROMSBoundaryForcing) for x in sim.boundary_forcing]
        assert sim.boundary_forcing[0].source.location == "http://my.files/boundary.nc"
        assert sim.boundary_forcing[0].source.file_hash == "456"

        assert isinstance(sim.surface_forcing, list)
        assert [isinstance(x, ROMSSurfaceForcing) for x in sim.surface_forcing]
        assert sim.surface_forcing[0].source.location == "http://my.files/surface.nc"
        assert sim.surface_forcing[0].source.file_hash == "567"

        assert isinstance(sim.forcing_corrections, list)
        assert [isinstance(x, ROMSForcingCorrections) for x in sim.forcing_corrections]
        assert (
            sim.forcing_corrections[0].source.location == "http://my.files/sw_corr.nc"
        )
        assert sim.forcing_corrections[0].source.file_hash == "345"

    def test_dict_roundtrip(self, fake_romssimulation):
        """Tests that `to_dict()` and `from_dict()` produce consistent results.

        This test ensures that converting a `ROMSSimulation` instance to a dictionary
        and then reconstructing it using `from_dict()` results in an equivalent object.

        Assertions
        ----------
        - The dictionary produced by `to_dict()` matches the dictionary of the
          reconstructed instance from `from_dict()`, ensuring data integrity.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation`: A fixture providing a pre-configured `ROMSSimulation` instance.
        """
        sim = fake_romssimulation
        sim_to_dict = sim.to_dict()
        sim_from_dict = sim.from_dict(
            simulation_dict=sim_to_dict,
            directory=sim.directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )

        assert sim_from_dict.to_dict() == sim_to_dict

    def test_to_blueprint(self, fake_romssimulation):
        """Tests that `to_blueprint()` writes a `ROMSSimulation` dictionary to a YAML
        file.

        This test verifies that the `to_blueprint()` method writes a YAML file with
        the expected dictionary representation of the `ROMSSimulation` instance.

        Assertions
        ----------
        - `open()` is called once with the correct filename and write mode.
        - `yaml.dump()` is called with the dictionary representation of the simulation

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation`: A fixture providing a pre-configured `ROMSSimulation` instance.
        - `mock_open`: A mock for Python's built-in `open()` function.
        - `mock_yaml_dump`: A mock for `yaml.dump()` to intercept and verify the YAML writing operation.
        """
        sim = fake_romssimulation
        mock_file_path = "mock_path.yaml"

        # Mock `open` and `yaml.dump`
        with (
            patch("builtins.open", mock_open()) as mock_file,
            patch("yaml.dump") as mock_yaml_dump,
        ):
            sim.to_blueprint(mock_file_path)

            mock_file.assert_called_once_with(mock_file_path, "w")

            mock_yaml_dump.assert_called_once_with(
                sim.to_dict(), mock_file(), default_flow_style=False, sort_keys=False
            )

    @patch("pathlib.Path.exists", return_value=True)
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="name: TestROMS\ndiscretization:\n  time_step: 60",
    )
    def test_from_blueprint_valid_file(
        self, mock_open_file, mock_path_exists, tmp_path
    ):
        """Tests that `from_blueprint()` correctly loads a `ROMSSimulation` from a valid
        YAML file.

        This test mocks the output of yaml.safe_load to return the expected dictionary
        and then verifies that this output is properly processed.

         Assertions
         ----------
         - The returned object is an instance of `ROMSSimulation`.
         - `open()` is called exactly once with the expected file path in read mode.

         Mocks & Fixtures
         ----------------
         - `mock_open_file`: Mocks the built-in `open()` function to simulate reading a YAML file.
         - `mock_path_exists`: Mocks `Path.exists()` to return `True`, ensuring the test bypasses file existence checks.
         - `tmp_path`: A temporary directory provided by `pytest` to simulate the blueprint file's location.
        """
        blueprint_path = tmp_path / "roms_blueprint.yaml"

        with patch("yaml.safe_load", return_value=self.example_simulation_dict):
            sim = ROMSSimulation.from_blueprint(
                blueprint=str(blueprint_path),
                directory=tmp_path,
                start_date="2024-01-01",
                end_date="2025-01-01",
            )

        # Assertions
        assert isinstance(sim, ROMSSimulation)
        mock_open_file.assert_called_once_with(str(blueprint_path))

    @patch("pathlib.Path.exists", return_value=True)
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="name: TestROMS\ndiscretization:\n  time_step: 60",
    )
    def test_from_blueprint_invalid_filetype(
        self, mock_open_file, mock_path_exists, tmp_path
    ):
        """Tests that `from_blueprint()` raises a `ValueError` when given a non-YAML
        file.

        This test ensures that `from_blueprint()` enforces the expected file format by
        raising an error if the blueprint file does not have a `.yaml` extension.

        Assertions
        ----------
        - A `ValueError` is raised with the expected message.

        Mocks & Fixtures
        ----------------
        - `mock_open_file`: Mocks the built-in `open()` function to simulate reading a non-YAML file.
        - `mock_path_exists`: Mocks `Path.exists()` to return `True`, bypassing file existence checks.
        - `tmp_path`: A temporary directory provided by `pytest` to simulate the blueprint's location.
        """
        blueprint_path = tmp_path / "roms_blueprint.nc"

        with patch("yaml.safe_load", return_value=self.example_simulation_dict):
            with pytest.raises(
                ValueError, match="C-Star expects blueprint in '.yaml' format"
            ):
                ROMSSimulation.from_blueprint(
                    blueprint=str(blueprint_path),
                    directory=tmp_path,
                    start_date="2024-01-01",
                    end_date="2025-01-01",
                )

    @patch("requests.get")
    @patch("pathlib.Path.exists", return_value=True)
    def test_from_blueprint_url(self, mock_path_exists, mock_requests_get, tmp_path):
        """Tests that `from_blueprint()` correctly loads a `ROMSSimulation` from a URL.

        This test ensures that when given a valid URL to a YAML blueprint file,
        `from_blueprint()` retrieves, parses, and constructs a `ROMSSimulation` instance.

        Assertions
        ----------
        - The returned object is an instance of `ROMSSimulation`.
        - The request to fetch the blueprint is made exactly once.

        Mocks & Fixtures
        ----------------
        - `mock_path_exists`: Mocks `Path.exists()` to return `True`, bypassing file existence checks.
        - `mock_requests_get`: Mocks `requests.get()` to return a simulated YAML blueprint response.
        - `tmp_path`: A temporary directory provided by `pytest` to simulate the simulation directory.
        """
        mock_response = MagicMock()
        mock_response.text = yaml.dump(self.example_simulation_dict)
        mock_requests_get.return_value = mock_response
        blueprint_path = "http://sketchyamlfiles4u.ru/roms_blueprint.yaml"

        sim = ROMSSimulation.from_blueprint(
            blueprint=blueprint_path,
            directory=tmp_path,
            start_date="2024-01-01",
            end_date="2025-01-01",
        )

        assert isinstance(sim, ROMSSimulation)
        mock_requests_get.assert_called_once()

    def test_blueprint_roundtrip(self, fake_romssimulation, tmp_path):
        """Tests that a `ROMSSimulation` can be serialized to a YAML blueprint and
        reconstructed correctly using `from_blueprint()`.

        This test verifies that after saving a simulation instance to a YAML blueprint
        and then reloading it, the reloaded instance matches the original.

        Assertions
        ----------
        - The dictionary representation of the reloaded instance matches the original.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation`: A fixture providing a sample `ROMSSimulation` instance.
        - `tmp_path`: A temporary directory provided by `pytest` to store the blueprint file.
        """
        sim = fake_romssimulation
        output_file = tmp_path / "test.yaml"
        sim.to_blueprint(filename=output_file)
        sim2 = ROMSSimulation.from_blueprint(
            blueprint=output_file,
            directory=tmp_path / "sim2",
            start_date=sim.start_date,
            end_date=sim.end_date,
        )
        assert sim.to_dict() == sim2.to_dict()


class TestProcessingAndExecution:
    """Tests processing steps and execution methods of `ROMSSimulation`.

    This test class covers functionality related to modifying runtime code,
    preparing input datasets, executing the simulation, and handling post-run
    processes.

    Tests
    -----
    - `test_setup`
        Ensures that `setup()` correctly configures external codebases, runtime
        code, and input datasets.
    - `test_is_setup_external_codebases`
        Validates that `is_setup` returns False if external codebases are not
        configured properly.
    - `test_is_setup_additional_code`
        Ensures that `is_setup` returns False when required runtime or compile-time
        code is missing.
    - `test_is_setup_input_datasets`
        Verifies that `is_setup` correctly considers input dataset availability and
        date ranges.
    - `test_build`
        Tests that `build()` correctly compiles the ROMS executable.
    - `test_build_no_rebuild`
        Ensures that `build()` does not recompile if the source code and executable
        remain unchanged.
    - `test_build_raises_if_make_clean_error`
        Checks that an error is raised when the `make compile_clean` step fails.
    - `test_build_raises_if_make_error`
        Ensures that a runtime error is raised when the `make` step fails during
        compilation.
    - `test_build_raises_if_no_build_dir`
        Verifies that `build()` raises an error when no compile-time code directory
        is set.
    - `test_pre_run`
        Ensures that `pre_run()` correctly partitions input datasets before execution.
    - `test_run_raises_if_no_executable`
        Checks that `run()` raises an error when no executable is found.
    - `test_run_raises_if_no_node_distribution`
        Verifies that `run()` raises an error when the node distribution is not set.
    - `test_run_raises_if_no_runtime_code_working_path`
        Verifies that `run()` raises an error when the runtime code is not fetched.
    - `test_run_local_execution`
        Ensures that `run()` correctly starts a local process when no scheduler is
        available.
    - `test_run_with_scheduler`
        Tests that `run()` submits a job with scheduler defaults when queue and
        walltime are not specified.
    - `test_run_with_scheduler_raises_if_no_account_key`
        Ensures that `run()` raises an error when executed with a scheduler but no
        account key is provided.
    - `test_post_run_raises_if_called_before_run`
        Checks that `post_run()` raises an error if called before `run()`.
    - `test_post_run_raises_if_still_running`
        Ensures that `post_run()` raises an error if execution is not yet completed.
    - `test_post_run_merges_netcdf_files`
        Tests that `post_run()` correctly merges NetCDF output files after execution.
    - `test_post_run_prints_message_if_no_files`
        Verifies that `post_run()` prints a message and exits when no output files
        are found.
    - `test_post_run_raises_error_if_ncjoin_fails`
        Ensures that `post_run()` raises an error if `ncjoin` fails during merging.
    """

    @patch.object(ROMSInputDataset, "get")
    @patch.object(AdditionalCode, "get")
    @patch.object(ExternalCodeBase, "handle_config_status")
    def test_setup(
        self,
        mock_handle_config_status,
        mock_additionalcode_get,
        mock_inputdataset_get,
        fake_romssimulation,
    ):
        """Tests that `setup` correctly fetches and organizes simulation components.

        This test verifies that `setup` correctly:
        - Configures external codebases.
        - Retrieves and organizes compile-time and runtime code.
        - Fetches input datasets.

        Mocks & Fixtures
        ---------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_handle_config_status` : Mocks `handle_config_status` for external codebases.
        - `mock_additionalcode_get` : Mocks `get()` for runtime and compile-time code.
        - `mock_inputdataset_get` : Mocks `get()` for input datasets.

        Assertions
        ----------
        - Ensures `handle_config_status` is called twice (ROMS & MARBL codebases).
        - Ensures `get()` is called twice for compile-time and runtime code.
        - Ensures `get()` is called once per input dataset.
        """
        sim = fake_romssimulation
        sim.setup()
        assert mock_handle_config_status.call_count == 2
        assert mock_additionalcode_get.call_count == 2
        assert mock_inputdataset_get.call_count == 7

    @pytest.mark.parametrize(
        "codebase_status, marbl_status, expected",
        [
            (0, 0, True),  # T Both correctly configured → should return True
            (1, 0, False),  # F Codebase not configured
            (0, 1, False),  # F MARBL codebase not configured
            (1, 1, False),  # F Both not configured
        ],
    )
    @patch.object(
        AdditionalCode, "exists_locally", new_callable=PropertyMock, return_value=True
    )
    def test_is_setup_external_codebases(
        self,
        mock_exists_locally,
        codebase_status,
        marbl_status,
        expected,
        fake_romssimulation,
    ):
        """Tests that `is_setup` correctly checks external codebase configuration.

        This test verifies that `is_setup` correctly evaluates the setup status based
        on the configuration of the ROMS and MARBL external codebases. It parametrizes
        the configuration status of each codebase and verifies different combinations.

        Mocks & Fixtures
        ---------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_exists_locally` : Mocks `exists_locally` for additional code components.
        - `codebase_status` : Parameterized mock return value for `ROMSExternalCodeBase.local_config_status`.
        - `marbl_status` : Parameterized mock return value for `MARBLExternalCodeBase.local_config_status`.

        Assertions
        ----------
        - Ensures that `is_setup` returns `True` only when both codebases are configured.
        - Ensures that `is_setup` returns `False` if either codebase is not configured.
        """
        sim = fake_romssimulation

        with (
            patch.object(
                ROMSExternalCodeBase, "local_config_status", new_callable=PropertyMock
            ) as mock_codebase_status,
            patch.object(
                MARBLExternalCodeBase, "local_config_status", new_callable=PropertyMock
            ) as mock_marbl_status,
            patch.object(
                type(sim), "input_datasets", new_callable=PropertyMock, return_value=[]
            ),
        ):
            mock_codebase_status.return_value = codebase_status
            mock_marbl_status.return_value = marbl_status

            assert sim.is_setup == expected

    @pytest.mark.parametrize(
        "runtime_exists, compile_exists, expected",
        [
            (True, True, True),  # T Both exist → is_setup should be True
            (False, True, False),  # F Runtime code missing
            (True, False, False),  # F Compile-time code missing
            (False, False, False),  # F Both missing
        ],
    )
    @patch.object(
        ExternalCodeBase,
        "local_config_status",
        new_callable=PropertyMock,
        return_value=0,
    )
    @patch.object(AdditionalCode, "exists_locally", new_callable=PropertyMock)
    def test_is_setup_additional_code(
        self,
        mock_additionalcode_exists,
        mock_local_config_status,
        runtime_exists,
        compile_exists,
        expected,
        fake_romssimulation,
    ):
        """Tests that `is_setup` correctly checks for the presence of runtime and
        compile-time code.

        This test ensures that `is_setup` evaluates the setup status correctly based on whether
        runtime and compile-time code components exist locally. It parametrizes the `exists_locally`
        property on different AdditionalCode instances and checks different combinations.

        Mocks & Fixtures
        ---------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_additionalcode_exists` : Mocks `exists_locally` for runtime and compile-time code.
        - `mock_local_config_status` : Mocks external codebase configuration status.
        - `runtime_exists` : Parameterized mock return value indicating if runtime code exists.
        - `compile_exists` : Parameterized mock return value indicating if compile-time code exists.

        Assertions
        ----------
        - Ensures `is_setup` returns `True` only when both runtime and compile-time code exist.
        - Ensures `is_setup` returns `False` when either component is missing.
        """
        sim = fake_romssimulation

        mock_additionalcode_exists.side_effect = [runtime_exists, compile_exists]

        with patch.object(
            ROMSSimulation, "input_datasets", new_callable=PropertyMock, return_value=[]
        ):
            assert sim.is_setup == expected

    @pytest.mark.parametrize(
        "dataset_exists, dataset_start, dataset_end, sim_start, sim_end, expected",
        [
            (True, None, None, None, None, True),
            (False, None, None, None, None, False),
            (False, datetime(2025, 1, 1), datetime(2025, 12, 31), None, None, False),
            (
                False,
                datetime(2025, 1, 1),
                datetime(2025, 12, 31),
                datetime(2025, 6, 1),
                datetime(2025, 6, 30),
                False,
            ),
            (
                False,
                datetime(2025, 1, 1),
                datetime(2025, 3, 31),
                datetime(2025, 6, 1),
                datetime(2025, 6, 30),
                True,
            ),
        ],
    )
    @patch.object(
        ExternalCodeBase,
        "local_config_status",
        new_callable=PropertyMock,
        return_value=0,
    )
    @patch.object(
        AdditionalCode, "exists_locally", new_callable=PropertyMock, return_value=True
    )
    def test_is_setup_input_datasets(
        self,
        mock_additionalcode_exists,
        mock_local_config_status,
        dataset_exists,
        dataset_start,
        dataset_end,
        sim_start,
        sim_end,
        expected,
        fake_romssimulation,
    ):
        """Test the `is_setup` property of `ROMSSimulation` with different
        configurations of input datasets, mocking any ExternalCodeBase and
        AdditionalCode instances as correctly setup.

        This test verifies whether a ROMS simulation is considered "set up" based on
        the presence of required input datasets and their date ranges.

        Parameters
        ----------
        dataset_exists : bool
            Whether the dataset is found locally.
        dataset_start : datetime or None
            The start date of the dataset, if defined.
        dataset_end : datetime or None
            The end date of the dataset, if defined.
        sim_start : datetime or None
            The start date of the simulation, if defined.
        sim_end : datetime or None
            The end date of the simulation, if defined.
        expected : bool
            The expected result of `sim.is_setup`.
        fake_romssimulation : fixture
            A fixture providing a pre-configured `ROMSSimulation` instance.

        Test Cases
        ----------
        |Case|dataset_exists|dataset_start|dataset_end|sim_start |sim_end   |Expected|
        |----|--------------|-------------|-----------|----------|----------|--------|
        | 1  |True          |None         |None       |None      |None      |False   |
        | 2  |False         |None         |None       |None      |None      |False   |
        | 3  |False         |2025-01-01   |2025-12-31 |None      |None      |False   |
        | 4  |False         |2025-01-01   |2025-12-31 |2025-06-01|2025-06-30|False   |
        | 5  |False         |2025-01-01   |2025-03-31 |2025-06-01|2025-06-30|True    |
        """
        sim = fake_romssimulation

        # Create a mock dataset
        with patch("cstar.roms.input_dataset.ROMSInputDataset") as MockDataset:
            # mock_dataset = patch("cstar.roms.input_dataset.ROMSInputDataset", autospec=True).start()
            mock_dataset = MockDataset()
            mock_dataset.exists_locally = dataset_exists
            mock_dataset.start_date = dataset_start
            mock_dataset.end_date = dataset_end

            # Patch input_datasets list
            with patch.object(
                ROMSSimulation,
                "input_datasets",
                new_callable=PropertyMock,
                return_value=[mock_dataset],
            ):
                sim.start_date = sim_start
                sim.end_date = sim_end
                assert sim.is_setup == expected

    @patch("cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash")
    @patch("subprocess.run")
    def test_build(self, mock_subprocess, mock_get_hash, fake_romssimulation):
        """Tests that `build` correctly compiles the ROMS executable.

        This test ensures that the `build` method performs the following steps:
        - Cleans the build directory if necessary.
        - Calls `make compile_clean` and `make` to compile the executable.
        - Stores the executable path and hash after a successful build.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_subprocess` : Mocks subprocess calls for compilation.
        - `mock_get_hash` : Mocks checksum retrieval for the compiled executable.

        Assertions
        ----------
        - Ensures `make compile_clean` and `make` commands are executed in sequence.
        - Ensures the executable path is correctly stored in `exe_path`.
        - Ensures the executable hash is stored in `_exe_hash` after successful compilation.
        """
        sim = fake_romssimulation
        build_dir = sim.directory / "ROMS/compile_time_code"
        (build_dir / "Compile").mkdir(exist_ok=True, parents=True)
        sim.compile_time_code.working_path = build_dir

        mock_subprocess.return_value = MagicMock(returncode=0, stderr="")
        mock_get_hash.return_value = "mockhash123"

        sim.build()
        assert mock_subprocess.call_count == 2
        mock_subprocess.assert_any_call(
            "make compile_clean",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )
        mock_subprocess.assert_any_call(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )

        assert sim.exe_path == build_dir / "roms"
        assert sim._exe_hash == "mockhash123"

    @patch(
        "cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash"
    )  # Mock hash function
    @patch("subprocess.run")  # Mock subprocess (should not be called)
    def test_build_no_rebuild(
        self,
        mock_subprocess,
        mock_get_hash,
        fake_romssimulation,
        caplog: pytest.LogCaptureFixture,
    ):
        """Tests that `build` does not recompile if the executable already exists and is
        unchanged.

        This test ensures that `build` exits early when:
        - The ROMS executable already exists.
        - The compile-time code exists locally.
        - The hash of the existing executable matches the stored hash.
        - `rebuild` is set to `False`.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_subprocess` : Mocks subprocess calls for compilation (should not be called).
        - `mock_get_hash` : Mocks checksum retrieval for the compiled executable.
        - `caplog` : Captures log outputs to verify early exit message.

        Assertions
        ----------
        - Ensures `build` exits early without calling `make compile_clean` or `make`.
        - Ensures an informational message is logged about skipping recompilation.
        """
        sim = fake_romssimulation
        caplog.set_level(logging.INFO, logger=sim.log.name)
        build_dir = sim.directory / "ROMS/compile_time_code"
        # Mock properties for early exit conditions
        with (
            patch.object(
                AdditionalCode,
                "exists_locally",
                new_callable=PropertyMock,
                return_value=True,
            ),
            patch.object(Path, "exists", return_value=True),
        ):
            # Pretend the executable exists
            sim._exe_hash = "dummy_hash"
            sim.compile_time_code.working_path = build_dir
            sim.build(rebuild=False)

            # Ensure early exit exception was triggered
            expected_msg = (
                f"ROMS has already been built at {build_dir / 'roms'}, and "
                "the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSSimulation.build(rebuild = True)"
            )
            captured = caplog.text
            assert expected_msg in captured

            # Ensure subprocess.run was *not* called
            mock_subprocess.assert_not_called()

    @patch("cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash")
    @patch("subprocess.run")
    def test_build_raises_if_make_clean_error(
        self, mock_subprocess, mock_get_hash, fake_romssimulation
    ):
        """Tests that `build` raises an error if `make compile_clean` fails.

        This test ensures that if `make compile_clean` returns a nonzero exit code,
        a `RuntimeError` is raised with the appropriate error message.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_subprocess` : Mocks subprocess calls for compilation.
        - `mock_get_hash` : Mocks checksum retrieval for the compiled executable.

        Assertions
        ----------
        - Ensures `make compile_clean` is called before compilation.
        - Verifies that a `RuntimeError` is raised when `make compile_clean` fails.
        """
        sim = fake_romssimulation
        build_dir = sim.directory / "ROMS/compile_time_code"
        (build_dir / "Compile").mkdir(exist_ok=True, parents=True)
        sim.compile_time_code.working_path = build_dir

        mock_subprocess.return_value = MagicMock(returncode=1, stderr="")
        mock_get_hash.return_value = "mockhash123"

        with pytest.raises(
            RuntimeError, match="Error when cleaning existing ROMS compilation."
        ):
            sim.build()
        assert mock_subprocess.call_count == 1
        mock_subprocess.assert_any_call(
            "make compile_clean",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )

    @patch("cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash")
    @patch("subprocess.run")
    def test_build_raises_if_make_error(
        self, mock_subprocess, mock_get_hash, fake_romssimulation
    ):
        """Tests that `build` raises an error if `make` fails during compilation.

        This test ensures that if `make` returns a nonzero exit code during the ROMS
        compilation process, a `RuntimeError` is raised with the appropriate error message.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_subprocess` : Mocks subprocess calls for compilation.
        - `mock_get_hash` : Mocks checksum retrieval for the compiled executable.

        Assertions
        ----------
        - Ensures `make` is called for ROMS compilation.
        - Verifies that a `RuntimeError` is raised when `make` fails.
        """
        sim = fake_romssimulation
        build_dir = sim.directory / "ROMS/compile_time_code"
        sim.compile_time_code.working_path = build_dir

        mock_subprocess.return_value = MagicMock(returncode=1, stderr="")
        mock_get_hash.return_value = "mockhash123"

        with pytest.raises(RuntimeError, match="Error when compiling ROMS"):
            sim.build()
        assert mock_subprocess.call_count == 1

        mock_subprocess.assert_any_call(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )

    def test_build_raises_if_no_build_dir(self, fake_romssimulation):
        """Tests that `build` raises an error if no build directory is set.

        This test verifies that calling `build` without a valid `compile_time_code.working_path`
        results in a `ValueError`, as the build process requires a designated directory.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.

        Assertions
        ----------
        - Ensures that calling `build` without a build directory raises a `ValueError`.
        """
        sim = fake_romssimulation
        with pytest.raises(ValueError, match="Unable to compile ROMSSimulation"):
            sim.build()

    @patch.object(ROMSInputDataset, "partition")  # Mock partition method
    def test_pre_run(self, mock_partition, fake_romssimulation):
        """Tests that `pre_run` partitions any locally available input datasets.

        This test verifies that `pre_run` correctly calls `partition()` on input datasets
        that exist locally, while skipping those that do not.

        Mocks & Fixtures
        ----------------
        - `mock_partition` : Mocks the `partition` method of `ROMSInputDataset`.
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.

        Assertions
        ----------
        - Ensures that `partition()` is called only on datasets that exist locally.
        - Ensures that datasets not found locally are not partitioned.
        """
        sim = fake_romssimulation

        # Mock some input datasets
        dataset_1 = MagicMock(spec=ROMSInputDataset, exists_locally=True)
        dataset_2 = MagicMock(
            spec=ROMSInputDataset, exists_locally=False
        )  # Should be ignored
        dataset_3 = MagicMock(spec=ROMSInputDataset, exists_locally=True)
        with patch.object(
            ROMSSimulation, "input_datasets", new_callable=PropertyMock
        ) as mock_input_datasets:
            mock_input_datasets.return_value = [dataset_1, dataset_2, dataset_3]

            # Call the method
            sim.pre_run()

            # Assert that partition() was called only on datasets that exist locally
            dataset_1.partition.assert_called_once_with(
                np_xi=2, np_eta=3, overwrite_existing_files=False
            )
            dataset_2.partition.assert_not_called()  # Does not exist → shouldn't be partitioned
            dataset_3.partition.assert_called_once_with(
                np_xi=2, np_eta=3, overwrite_existing_files=False
            )

    def test_run_raises_if_no_runtime_code_working_path(self, fake_romssimulation):
        """Confirm that ROMSSimulation.run() raises a FileNotFoundError if
        ROMSSimulation.runtime_code does not exist locally.
        """
        sim = fake_romssimulation
        sim.exe_path = Path("madeup.exe")
        with pytest.raises(
            FileNotFoundError,
            match="Local copy of ROMSSimulation.runtime_code does not exist.",
        ):
            sim.run()

    def test_run_raises_if_no_executable(self, fake_romssimulation):
        """Tests that `run` raises an error if no executable is found.

        This test ensures that calling `run` without a defined `exe_path` results in
        a `ValueError`, preventing execution when the ROMS executable is missing.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.

        Assertions
        ----------
        - Ensures `ValueError` is raised with the expected error message when `exe_path` is `None`.
        """
        sim = fake_romssimulation
        with pytest.raises(ValueError, match="unable to find ROMS executable"):
            sim.run()

    def test_run_raises_if_no_node_distribution(self, fake_romssimulation):
        """Tests that `run` raises an error if node distribution is not set.

        This test ensures that if `n_procs_tot` is `None`, calling `run` will
        raise a `ValueError`, preventing execution without a valid node distribution.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - ROMSDiscretization.n_procs_tot: Mocks `n_procs_tot` to return `None`.

        Assertions
        ----------
        - Ensures `ValueError` is raised with the expected error message when `n_procs_tot` is `None`.
        """
        sim = fake_romssimulation
        sim.exe_path = sim.directory / "ROMS/compile_time_code/roms"
        with patch(
            "cstar.roms.simulation.ROMSDiscretization.n_procs_tot",
            new_callable=PropertyMock,
            return_value=None,
        ):
            with pytest.raises(
                ValueError, match="Unable to calculate node distribution"
            ):
                sim.run()

    @patch("cstar.roms.ROMSSimulation.persist")
    @patch.object(ROMSSimulation, "roms_runtime_settings", new_callable=PropertyMock)
    def test_run_local_execution(
        self, mock_runtime_settings, mock_persist, fake_romssimulation
    ):
        """Tests that `run` correctly starts a local process when no scheduler is
        available.

        This test verifies that if a scheduler is not present, `run` initiates
        local execution using `LocalProcess`. The method should construct the appropriate
        command and set the execution handler correctly.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `patch("cstar.roms.simulation.LocalProcess")` : Mocks `LocalProcess` to track its instantiation.
        - CStarSystemManager.scheduler: Mocks `cstar_sysmgr.scheduler` to return `None`

        Assertions
        ----------
        - Ensures `LocalProcess` is instantiated with the correct command and run path.
        - Ensures `LocalProcess.start()` is called.
        - Ensures the returned execution handler matches the mocked `LocalProcess` instance.
        """
        sim = fake_romssimulation

        # Mock no scheduler
        with (
            patch("cstar.roms.simulation.LocalProcess") as mock_local_process,
            patch(
                "cstar.system.manager.CStarSystemManager.scheduler",
                new_callable=PropertyMock,
                return_value=None,
            ),
        ):
            sim.exe_path = sim.directory / "ROMS/compile_time_code/roms"
            sim.runtime_code.working_path = sim.directory / "ROMS/runtime_code/"
            mock_process_instance = MagicMock()
            mock_local_process.return_value = mock_process_instance

            execution_handler = sim.run()

            # Check LocalProcess was instantiated correctly
            mock_local_process.assert_called_once_with(
                commands=f"{cstar_sysmgr.environment.mpi_exec_prefix} -n {sim.discretization.n_procs_tot} {sim.exe_path} {sim.runtime_code.working_path}/ROMSTest.in",
                run_path=sim.directory / "output",
            )

            # Ensure process was started
            mock_process_instance.start.assert_called_once()

            # Ensure execution handler was set correctly
            assert execution_handler == mock_process_instance

            mock_persist.assert_called_once()

    @pytest.mark.parametrize(
        "mock_system_name,exp_mpi_prefix",
        [
            ["darwin_arm64", "mpirun"],
            ["derecho", "mpirun"],
            ["expanse", "srun --mpi=pmi2"],
            ["perlmutter", "srun"],
        ],
    )
    @patch("cstar.roms.ROMSSimulation.persist")
    @patch.object(ROMSSimulation, "roms_runtime_settings", new_callable=PropertyMock)
    def test_run_with_scheduler(
        self,
        mock_runtime_settings,
        mock_persist,
        fake_romssimulation,
        mock_system_name: str,
        exp_mpi_prefix: str,
    ):
        """Tests that `run` correctly submits a job to a scheduler when available.

        This test verifies that if a scheduler is present, `run` creates a scheduler job
        with the appropriate parameters and submits it.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `patch("cstar.roms.simulation.create_scheduler_job")` : Mocks `create_scheduler_job`
          to verify job creation.
        - `patch("cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock)` :
          Mocks `cstar_sysmgr.scheduler` to simulate a scheduler environment.

        Assertions
        ----------
        - Ensures `create_scheduler_job` is called with the correct arguments.
        - Ensures the scheduler job's `submit()` method is called.
        - Ensures the returned execution handler matches the created job instance.
        """
        sim = fake_romssimulation
        build_dir = sim.directory / "ROMS/compile_time_code"
        sim.runtime_code.working_path = sim.directory / "ROMS/runtime_code/"

        # Mock scheduler object
        mock_scheduler = MagicMock()
        mock_scheduler.primary_queue_name = "default_queue"
        mock_scheduler.get_queue.return_value.max_walltime = "12:00:00"

        with (
            patch("cstar.roms.simulation.create_scheduler_job") as mock_create_job,
            patch(
                "cstar.system.environment.CStarEnvironment.uses_lmod",
                new_callable=PropertyMock,
                return_value=False,
            ),
            patch(
                "cstar.system.manager.CStarSystemManager.name",
                new_callable=PropertyMock,
                return_value=mock_system_name,
            ),
            patch(
                "cstar.system.manager.CStarSystemManager.environment",
                CStarEnvironment(
                    system_name=mock_system_name,
                    mpi_exec_prefix=exp_mpi_prefix,
                    compiler="mock-compiler",
                ),
            ),
            patch(
                "cstar.system.manager.CStarSystemManager.scheduler",
                new_callable=PropertyMock,
                return_value=mock_scheduler,
            ),
        ):
            sim.exe_path = build_dir / "roms"
            mock_job_instance = MagicMock()
            mock_create_job.return_value = mock_job_instance

            # Call `run()` without explicitly passing `queue_name` and `walltime`
            execution_handler = sim.run(account_key="some_key")
            mock_create_job.assert_called_once_with(
                commands=f"{exp_mpi_prefix} -n 6 {build_dir / 'roms'} {sim.runtime_code.working_path}/ROMSTest.in",
                job_name=None,
                cpus=6,
                account_key="some_key",
                run_path=sim.directory / "output",
                queue_name="default_queue",
                walltime="12:00:00",
            )

            mock_job_instance.submit.assert_called_once()

            assert execution_handler == mock_job_instance

            mock_persist.assert_called_once()

    @patch.object(ROMSSimulation, "roms_runtime_settings", new_callable=PropertyMock)
    def test_run_with_scheduler_raises_if_no_account_key(
        self, mock_runtime_settings, fake_romssimulation
    ):
        """Tests that `run` raises a `ValueError` if no account key is provided when
        using a scheduler.

        This test ensures that if a scheduler is available but `account_key` is not provided,
        the method raises an appropriate error instead of submitting a job.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `patch("cstar.roms.simulation.create_scheduler_job")` : Mocks job creation to prevent real execution.
        - `patch("cstar.system.manager.CStarSystemManager.scheduler", new_callable=PropertyMock)` :
          Mocks `cstar_sysmgr.scheduler` to simulate a scheduler environment.

        Assertions
        ----------
        - Ensures `create_scheduler_job` is never called if `account_key` is missing.
        - Confirms that the expected `ValueError` is raised with an appropriate message.
        """
        sim = fake_romssimulation
        build_dir = sim.directory / "ROMS/compile_time_code"
        sim.runtime_code.working_path = sim.directory / "ROMS/runtime_code/"

        # Mock scheduler object
        mock_scheduler = MagicMock()
        mock_scheduler.primary_queue_name = "default_queue"
        mock_scheduler.get_queue.return_value.max_walltime = "12:00:00"

        with (
            patch("cstar.roms.simulation.create_scheduler_job") as mock_create_job,
            patch(
                "cstar.system.manager.CStarSystemManager.scheduler",
                new_callable=PropertyMock,
                return_value=mock_scheduler,
            ),
        ):
            sim.exe_path = build_dir / "roms"

            # Call `run()` without explicitly passing `queue_name` and `walltime`
            with pytest.raises(
                ValueError,
                match=re.escape(
                    "please call Simulation.run() with a value for account_key"
                ),
            ):
                sim.run()
            mock_create_job.assert_not_called()

    def test_post_run_raises_if_called_before_run(self, fake_romssimulation):
        """Tests that `post_run` raises a `RuntimeError` if called before `run`.

        This test ensures that attempting to execute `post_run` before the simulation has been run
        results in an appropriate error, preventing unexpected behavior.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.

        Assertions
        ----------
        - Confirms that calling `post_run` without a prior `run` invocation raises `RuntimeError`.
        - Checks the error message to verify it correctly informs the user of the issue.
        """
        sim = fake_romssimulation
        with pytest.raises(
            RuntimeError,
            match=re.escape(
                "Cannot call 'ROMSSimulation.post_run()' before calling 'ROMSSimulation.run()'"
            ),
        ):
            sim.post_run()

    def test_post_run_raises_if_still_running(self, fake_romssimulation):
        """Tests that `post_run` raises a `RuntimeError` if the simulation is still
        running.

        This test ensures that calling `post_run` while the execution status is not `COMPLETED`
        results in an appropriate error, preventing premature post-processing.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - Mocks `_execution_handler` to simulate an active (non-completed) execution state.

        Assertions
        ----------
        - Confirms that calling `post_run` while the execution handler's status is `RUNNING`
          raises `RuntimeError`.
        - Validates that the error message correctly informs the user of the issue.
        """
        sim = fake_romssimulation

        # Mock `_execution_handler` and set its `status` attribute to something *not* COMPLETED
        sim._execution_handler = MagicMock()
        sim._execution_handler.status = ExecutionStatus.RUNNING

        # Ensure RuntimeError is raised
        with pytest.raises(
            RuntimeError,
            match=re.escape(
                "Cannot call 'ROMSSimulation.post_run()' until the ROMS run is completed"
            ),
        ):
            sim.post_run()

    @patch("cstar.roms.ROMSSimulation.persist")
    @patch("subprocess.run")  # Mock ncjoin execution
    def test_post_run_merges_netcdf_files(
        self, mock_subprocess, mock_persist, fake_romssimulation
    ):
        """Tests that `post_run` correctly merges partitioned NetCDF output files.

        This test verifies that `post_run` identifies NetCDF output files, executes
        `ncjoin` to merge them, and moves the partitioned files to the `PARTITIONED`
        subdirectory. It creates mock netCDF files in a temporary directory (using
        touch) and then checks these are correctly handled.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_subprocess` : Mocks `subprocess.run` to simulate successful `ncjoin` execution.

        Assertions
        ----------
        - Ensures `ncjoin` is called correctly for each set of partitioned files.
        - Confirms that the partitioned files are moved to `PARTITIONED` after merging.
        - Validates that the `post_run` process completes without errors.
        """
        # Setup
        sim = fake_romssimulation
        output_dir = sim.directory / "output"
        output_dir.mkdir()

        # Create fake partitioned NetCDF files
        (output_dir / "ocean_his.20240101000000.001.nc").touch()
        (output_dir / "ocean_his.20240101000000.002.nc").touch()
        (output_dir / "ocean_rst.20240101000000.001.nc").touch()

        # Mock execution handler
        sim._execution_handler = MagicMock()
        sim._execution_handler.status = (
            ExecutionStatus.COMPLETED
        )  # Ensure run is complete

        mock_subprocess.return_value = MagicMock(returncode=0, stderr="")
        # Call post_run
        sim.post_run()

        # Check that ncjoin was called correctly
        mock_subprocess.assert_any_call(
            "ncjoin ocean_his.20240101000000.*.nc",
            cwd=output_dir,
            capture_output=True,
            text=True,
            shell=True,
        )
        mock_subprocess.assert_any_call(
            "ncjoin ocean_rst.20240101000000.*.nc",
            cwd=output_dir,
            capture_output=True,
            text=True,
            shell=True,
        )

        # Check that files were moved
        partitioned_dir = output_dir / "PARTITIONED"
        assert partitioned_dir.exists()
        assert (partitioned_dir / "ocean_his.20240101000000.001.nc").exists()
        assert (partitioned_dir / "ocean_his.20240101000000.002.nc").exists()
        assert (partitioned_dir / "ocean_rst.20240101000000.001.nc").exists()

        mock_persist.assert_called_once()

    @patch("cstar.roms.ROMSSimulation.persist")
    @patch.object(Path, "glob", return_value=[])  # Mock glob to return no files
    def test_post_run_prints_message_if_no_files(
        self,
        mock_glob,
        mock_persist,
        fake_romssimulation,
        caplog: pytest.LogCaptureFixture,
    ):
        """Tests that `post_run` prints a message and exits early if no output files are
        found.

        This test ensures that when `post_run` is called and no partitioned NetCDF files
        exist, a message is printed indicating that no suitable output was found.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_glob` : Mocks `Path.glob` to return an empty list, simulating no files.
        - `caplog` : Capture logging output to verify the correct message is displayed.

        Assertions
        ----------
        - Ensures `logger.info()` is called with the expected message.
        - Confirms that `Path.glob` is called once to check for output files.
        """
        # Setup
        sim = fake_romssimulation
        sim._execution_handler = MagicMock()
        sim._execution_handler.status = (
            ExecutionStatus.COMPLETED
        )  # Ensure simulation is complete
        caplog.set_level(logging.WARNING)

        # Call post_run
        sim.post_run()

        # Check that the expected messages were logged
        captured = caplog.text
        assert "No suitable output found" in captured

        # Ensure glob was called once
        mock_glob.assert_called_once()

        mock_persist.assert_called_once()

    @patch("subprocess.run")  # Mock subprocess.run to simulate a failure
    @patch.object(Path, "glob")  # Mock glob to return fake files
    def test_post_run_raises_error_if_ncjoin_fails(
        self, mock_glob, mock_subprocess, fake_romssimulation
    ):
        """Tests that `post_run` raises a `RuntimeError` if `ncjoin` fails during file
        merging.

        This test verifies that when `ncjoin` encounters an error while attempting to join
        partitioned NetCDF output files, a `RuntimeError` is raised, ensuring the failure
        is correctly handled.

        Mocks & Fixtures
        ----------------
        - `fake_romssimulation` : Provides a pre-configured `ROMSSimulation` instance.
        - `mock_glob` : Mocks `Path.glob` to return a list of fake NetCDF files.
        - `mock_subprocess` : Mocks `subprocess.run` to simulate a failed `ncjoin` execution.

        Assertions
        ----------
        - Ensures `ncjoin` is executed with the correct file pattern.
        - Confirms that a `RuntimeError` is raised when `ncjoin` returns a non-zero exit code.
        """
        # Setup
        sim = fake_romssimulation
        output_dir = sim.directory / "output"
        output_dir.mkdir(exist_ok=True, parents=True)

        # Fake file paths to match ncjoin pattern
        fake_files = [
            output_dir / "ocean_his.20240101000000.001.nc",
            output_dir / "ocean_his.20240101000000.002.nc",
        ]
        mock_glob.return_value = fake_files

        # Simulate ncjoin failure
        mock_subprocess.return_value.returncode = 1  # Non-zero exit code
        mock_subprocess.return_value.stderr = "ncjoin error message"

        # Mock execution handler
        sim._execution_handler = MagicMock()
        sim._execution_handler.status = (
            ExecutionStatus.COMPLETED
        )  # Ensure run is complete

        # Call post_run and expect error
        with pytest.raises(
            RuntimeError, match="Command `ncjoin ocean_his.20240101000000.*.nc` failed."
        ):
            sim.post_run()

        mock_subprocess.assert_called_once_with(
            "ncjoin ocean_his.20240101000000.*.nc",
            cwd=output_dir,
            capture_output=True,
            text=True,
            shell=True,
        )


class TestROMSSimulationRestart:
    """Tests for the `restart` method of `ROMSSimulation`.

    This test class verifies that the `restart` method correctly initializes a new
    `ROMSSimulation` instance from an existing simulation's restart file. It ensures
    that the new instance properly inherits the configuration and updates its
    initial conditions.

    Tests
    -----
    - `test_restart` : Verifies that `restart` creates a new `ROMSSimulation` instance
      and correctly sets the initial conditions from the restart file.
    - `test_restart_raises_if_no_restart_files` : Ensures `restart` raises a
      `FileNotFoundError` when no restart files matching the expected pattern are found.
    - `test_restart_raises_if_multiple_restarts_found` : Confirms `restart` raises a
      `ValueError` if multiple restart files are found, preventing ambiguity.
    """

    @patch.object(Path, "glob")  # Mock file search
    @patch.object(Path, "exists", return_value=True)
    def test_restart(self, mock_exists, mock_glob, fake_romssimulation):
        """Test that `restart` creates a new `ROMSSimulation` instance with updated
        initial conditions.

        This test ensures that when calling `restart` with a new end date, the method:
        - Creates a new `ROMSSimulation` instance.
        - Searches for the appropriate restart file in the output directory.
        - Assigns the found restart file as the new instance’s initial conditions.

        Mocks & Fixtures
        ----------------
        mock_exists : Mock
            Mocks `Path.exists` to return `True`, ensuring that the output directory is
            considered present.
        mock_glob : Mock
            Mocks `Path.glob` to return a list containing the expected restart file.
        fake_romssimulation : Fixture
            Provides an instance of `ROMSSimulation` and a temporary directory for testing.

        Assertions
        ----------
        - The method searches for a restart file matching the expected timestamp format.
        - A new `ROMSSimulation` instance is returned.
        - The new instance's `initial_conditions` attribute is correctly assigned the
          detected restart file.
        """
        # Setup mock simulation
        sim = fake_romssimulation
        new_end_date = datetime(2026, 6, 1)

        # Mock restart file found
        restart_file = sim.directory / "output/restart_rst.20251231000000.nc"
        mock_glob.return_value = [restart_file]

        # Call method
        new_sim = sim.restart(new_end_date=new_end_date)

        # Verify restart logic
        mock_glob.assert_called_once_with("*_rst.20251231000000.nc")
        assert isinstance(new_sim.initial_conditions, ROMSInitialConditions)
        assert new_sim.initial_conditions.source.location == str(restart_file.resolve())

    @patch.object(Path, "glob")  # Mock file search
    @patch.object(Path, "exists", return_value=True)
    def test_restart_raises_if_no_restart_files(
        self, mock_exists, mock_glob, fake_romssimulation
    ):
        """Test that `restart` raises a `FileNotFoundError` if no restart files are
        found.

        This test ensures that if the method is unable to locate a valid ROMS restart file,
        it raises a `FileNotFoundError` and does not proceed with simulation creation.

        Mocks & Fixtures
        ----------------
        mock_exists : Mock
            Mocks `Path.exists` to return `True`, ensuring the output directory exists.
        mock_glob : Mock
            Mocks `Path.glob` to return an empty list, simulating no restart files found.
        fake_romssimulation : Fixture
            Provides an instance of `ROMSSimulation` and a temporary directory for testing.

        Assertions
        ----------
        - The method searches for restart files with the expected filename pattern.
        - A `FileNotFoundError` is raised if no matching restart files are found.
        """
        # Setup mock simulation
        sim = fake_romssimulation
        new_end_date = datetime(2026, 6, 1)

        # Mock restart file found
        mock_glob.return_value = []

        # Call method
        with pytest.raises(
            FileNotFoundError, match=f"No files in {sim.directory / 'output'} match"
        ):
            sim.restart(new_end_date=new_end_date)

        mock_glob.assert_called_once_with("*_rst.20251231000000.nc")

    @patch.object(Path, "glob")
    def test_restart_raises_if_multiple_restarts_found(
        self, mock_glob, fake_romssimulation
    ):
        """Test that `restart` raises a `ValueError` if multiple restart files are
        found.

        This test ensures that when multiple distinct restart files are found matching
        the expected pattern, a `ValueError` is raised due to ambiguity.

        Mocks & Fixtures
        ----------------
        mock_glob : Mock
            Mocks `Path.glob` to return multiple restart files, simulating an ambiguous case.
        fake_romssimulation : Fixture
            Provides an instance of `ROMSSimulation` and a temporary directory for testing.

        Assertions
        ----------
        - The method searches for restart files with the expected filename pattern.
        - A `ValueError` is raised if multiple restart files are found.
        """
        sim = fake_romssimulation
        restart_dir = sim.directory / "output"
        new_end_date = datetime(2025, 6, 1)

        # Fake multiple unique restart files
        mock_glob.return_value = [
            restart_dir / "restart_rst.20250601000000.nc",
            restart_dir / "ocean_rst.20250601000000.nc",
        ]

        with pytest.raises(
            ValueError, match="Found multiple distinct restart files corresponding to"
        ):
            sim.restart(new_end_date=new_end_date)
