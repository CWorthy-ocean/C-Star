import re
import pytest
import yaml
import pickle
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, mock_open, MagicMock, PropertyMock
from cstar.base.external_codebase import ExternalCodeBase
from cstar.roms.simulation import ROMSSimulation
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.input_dataset import (
    ROMSInputDataset,
    ROMSModelGrid,
    ROMSInitialConditions,
    ROMSTidalForcing,
    ROMSBoundaryForcing,
    ROMSSurfaceForcing,
)
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.base.additional_code import AdditionalCode
from cstar.execution.handler import ExecutionStatus
from cstar.system.manager import cstar_sysmgr


@pytest.fixture
def example_roms_simulation(tmp_path):
    directory = tmp_path
    sim = ROMSSimulation(
        name="ROMSTest",
        directory=directory,
        discretization=ROMSDiscretization(time_step=60, n_procs_x=2, n_procs_y=3),
        codebase=ROMSExternalCodeBase(
            source_repo="http://my.code/repo.git", checkout_target="dev"
        ),
        runtime_code=AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=[
                "file1",
                "file2.in_TEMPLATE",
                "marbl_in",
                "marbl_tracer_output_list",
                "marbl_diagnostic_output_list",
            ],
        ),
        compile_time_code=AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1.h", "file2.opt"],
        ),
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
        marbl_codebase=MARBLExternalCodeBase(
            source_repo="http://marbl.com/repo.git", checkout_target="v1"
        ),
        model_grid=ROMSModelGrid(location="http://my.files/grid.nc", file_hash="123"),
        initial_conditions=ROMSInitialConditions(
            location="http://my.files/initial.nc", file_hash="234"
        ),
        tidal_forcing=ROMSTidalForcing(
            location="http://my.files/tidal.nc", file_hash="345"
        ),
        boundary_forcing=[
            ROMSBoundaryForcing(
                location="http://my.files/boundary.nc", file_hash="456"
            ),
        ],
        surface_forcing=[
            ROMSSurfaceForcing(location="http://my.files/surface.nc", file_hash="567"),
        ],
    )

    yield sim, directory  # Ensures pytest can handle resource cleanup if needed
    # sim.stop()  # Optional cleanup


class TestROMSSimulationInitialization:
    """Test the initialization of ROMSSimulation."""

    def test_init(self, example_roms_simulation):
        """Test that the ROMSSimulation initializes correctly with only required
        parameters."""

        sim, directory = example_roms_simulation

        assert sim.name == "ROMSTest"
        assert sim.directory == directory
        assert sim.discretization.__dict__ == {
            "time_step": 60,
            "n_procs_x": 2,
            "n_procs_y": 3,
        }

        assert sim.codebase.source_repo == "http://my.code/repo.git"
        assert sim.codebase.checkout_target == "dev"
        assert sim.runtime_code.source.location == str(directory.parent)
        assert sim.runtime_code.subdir == "subdir/"
        assert sim.runtime_code.checkout_target == "main"
        assert sim.runtime_code.files == [
            "file1",
            "file2.in_TEMPLATE",
            "marbl_in",
            "marbl_tracer_output_list",
            "marbl_diagnostic_output_list",
        ]
        assert sim.compile_time_code.source.location == str(directory.parent)
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
        assert sim.partitioned_files is None
        assert sim._execution_handler is None

    def test_init_raises_if_no_discretization(self, tmp_path):
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

    def test_default_codebase_assignment(self, tmp_path):
        """Ensure ROMSSimulation assigns default codebase when not provided."""
        with pytest.warns(UserWarning, match="default codebase will be used"):
            sim = ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=ROMSDiscretization(time_step=60),
                runtime_code=AdditionalCode(location="some/dir"),
                compile_time_code=AdditionalCode(location="some/dir"),
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
            )

        assert isinstance(sim.codebase, ROMSExternalCodeBase)
        assert sim.codebase.source_repo == "https://github.com/CESR-lab/ucla-roms.git"
        assert sim.codebase.checkout_target == "main"

        assert isinstance(sim.marbl_codebase, MARBLExternalCodeBase)
        assert (
            sim.marbl_codebase.source_repo
            == "https://github.com/marbl-ecosys/MARBL.git"
        )
        assert sim.marbl_codebase.checkout_target == "marbl0.45.0"

    def test_invalid_surface_forcing_type(self, tmp_path):
        """Ensure a TypeError is raised when surface_forcing is not a list of
        ROMSSurfaceForcing."""
        with pytest.raises(
            TypeError, match="must be a list of ROMSSurfaceForcing instances"
        ):
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
                surface_forcing=[
                    "list",
                    "of",
                    "strings",
                    "instead",
                    "of",
                    "ROMSSurfaceForcing",
                    "instances",
                ],
            )

    def test_invalid_boundary_forcing_type(self, tmp_path):
        """Ensure a TypeError is raised when boundary_forcing is not a list of
        ROMSBoundaryForcing."""
        with pytest.raises(
            TypeError, match="must be a list of ROMSBoundaryForcing instances"
        ):
            ROMSSimulation(
                name="test",
                directory=tmp_path,
                discretization=ROMSDiscretization(time_step=60),
                codebase=ROMSExternalCodeBase(),
                runtime_code=AdditionalCode("some/dir"),
                compile_time_code=AdditionalCode("some/dir"),
                start_date="2012-01-01",
                end_date="2012-01-02",
                valid_start_date="2012-01-01",
                valid_end_date="2012-01-02",
                boundary_forcing=[
                    "list",
                    "of",
                    "strings",
                    "instead",
                    "of",
                    "ROMSBoundaryForcing",
                    "instances",
                ],
            )

    def test_codebases(self, example_roms_simulation):
        """Test that the `codebases` property correctly lists the ExternalCodeBase
        instances."""
        sim, _ = example_roms_simulation
        assert isinstance(sim.codebases, list)
        assert isinstance(sim.codebases[0], ROMSExternalCodeBase)
        assert isinstance(sim.codebases[1], MARBLExternalCodeBase)
        assert sim.codebases[0] == sim.codebase
        assert sim.codebases[1] == sim.marbl_codebase

    def test_in_file_single_file(self, example_roms_simulation):
        """Test that the `in_file` property correctly retrieves a .in file from
        runtime_code."""
        sim, _ = example_roms_simulation
        assert sim.in_file == Path("file2.in")

    def test_in_file_no_file(self, tmp_path, example_roms_simulation):
        """Test that the `in_file` property raises an error if no suitable '.in' file is
        found."""
        sim, directory = example_roms_simulation
        sim.runtime_code = AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        )

        with pytest.raises(ValueError, match="No '.in' file found"):
            sim.in_file

    def test_in_file_multiple_files(self, tmp_path, example_roms_simulation):
        """Test that the `in_file` property raises an error if multiple '.in' files are
        found."""
        sim, directory = example_roms_simulation
        sim.runtime_code = AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1.in_TEMPLATE", "file2.in"],
        )

        with pytest.raises(ValueError, match="Multiple '.in' files found"):
            sim.in_file

    def test_in_file_no_runtime_code(self, tmp_path, example_roms_simulation):
        """Test that the `in_file` property raises an error if the runtime_code attr is
        unset."""
        sim, directory = example_roms_simulation
        sim.runtime_code = None

        with pytest.raises(ValueError, match="ROMS requires a runtime options file"):
            sim.in_file

    def test_in_file_working_path(self, tmp_path, example_roms_simulation):
        """Test that the `in_file` property provides a correct path for local runtime
        code."""
        sim, directory = example_roms_simulation
        wp = tmp_path
        sim.runtime_code.working_path = wp

        assert sim.in_file == wp / "file2.in"

    def test_input_datasets(self, example_roms_simulation):
        """Test that the input_datasets property returns the correct list of
        ROMSInputDataset instances."""
        sim, directory = example_roms_simulation
        mg = sim.model_grid
        ic = sim.initial_conditions
        td = sim.tidal_forcing
        bc = sim.boundary_forcing[0]
        sf = sim.surface_forcing[0]

        assert sim.input_datasets == [mg, ic, td, bc, sf]


class TestStrAndRepr:
    def test_str(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        expected_str = f"""\
ROMSSimulation
--------------
Name: ROMSTest
Directory: {directory}
Start date: 2025-01-01 00:00:00
End date: 2025-12-31 00:00:00
Valid start date: 2024-01-01 00:00:00
Valid end date: 2026-01-01 00:00:00

Discretization: ROMSDiscretization(time_step = 60, n_procs_x = 2, n_procs_y = 3)

Code:
Codebase: ROMSExternalCodeBase instance (query using ROMSSimulation.codebase)
Runtime code: AdditionalCode instance with 5 files (query using ROMSSimulation.runtime_code)
Compile-time code: AdditionalCode instance with 2 files (query using ROMSSimulation.compile_time_code)
MARBL Codebase: MARBLExternalCodeBase instance (query using ROMSSimulation.marbl_codebase)

Input Datasets:
Model grid: <ROMSModelGrid instance>
Initial conditions: <ROMSInitialConditions instance>
Tidal forcing: <ROMSTidalForcing instance>
Surface forcing: <list of 1 ROMSSurfaceForcing instances>
Boundary forcing: <list of 1 ROMSBoundaryForcing instances>

Is setup: False"""
        assert sim.__str__() == expected_str

    def test_repr(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        expected_repr = f"""\
ROMSSimulation(
name = ROMSTest,
directory = {directory},
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
surface_forcing = <list of 1 ROMSSurfaceForcing instances>,
boundary_forcing = <list of 1 ROMSBoundaryForcing instances>
)"""
        assert expected_repr == sim.__repr__()

    def test_tree(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        expected_tree = f"""\
{directory}
└── ROMS
    ├── input_datasets
    │   ├── grid.nc
    │   ├── initial.nc
    │   ├── tidal.nc
    │   ├── boundary.nc
    │   └── surface.nc
    ├── runtime_code
    │   ├── file1
    │   ├── file2.in_TEMPLATE
    │   ├── marbl_in
    │   ├── marbl_tracer_output_list
    │   └── marbl_diagnostic_output_list
    └── compile_time_code
        ├── file1.h
        └── file2.opt
"""

        # print(sim.tree())
        # print(expected_tree)
        assert sim.tree() == expected_tree


class TestToAndFromDictAndBlueprint:
    def setup_method(self):
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
                    "file2.in_TEMPLATE",
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
            "surface_forcing": [
                {"location": "http://my.files/surface.nc", "file_hash": "567"}
            ],
            "boundary_forcing": [
                {"location": "http://my.files/boundary.nc", "file_hash": "456"}
            ],
        }

    def test_to_dict(self, example_roms_simulation):
        """Test key/value pairs unique ROMSSimulation.to_dict (Simulation.to_dict tested
        elsewhere)"""
        sim, directory = example_roms_simulation
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

    def test_from_dict(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        sim_dict = self.example_simulation_dict
        sim_dict["runtime_code"]["location"] = directory.parent
        sim_dict["compile_time_code"]["location"] = directory.parent
        sim2 = ROMSSimulation.from_dict(
            sim_dict,
            directory=directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )

        assert pickle.dumps(sim2) == pickle.dumps(sim), "Instances are not identical"

    def test_from_dict_with_single_forcing_entries(self, tmp_path):
        sim_dict = self.example_simulation_dict.copy()
        sim_dict["surface_forcing"] = {
            "location": "http://my.files/surface.nc",
            "file_hash": "567",
        }
        sim_dict["boundary_forcing"] = {
            "location": "http://my.files/boundary.nc",
            "file_hash": "456",
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

    def test_dict_roundtrip(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        sim_to_dict = sim.to_dict()
        sim_from_dict = sim.from_dict(
            simulation_dict=sim_to_dict,
            directory=directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )

        assert sim_from_dict.to_dict() == sim_to_dict

    def test_to_blueprint(self, example_roms_simulation):
        sim, directory = example_roms_simulation
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
        """Ensure that from_blueprint correctly loads a ROMSSimulation from a valid YAML
        file."""
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
        mock_open_file.assert_called_once_with(str(blueprint_path), "r")

    @patch("pathlib.Path.exists", return_value=True)
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="name: TestROMS\ndiscretization:\n  time_step: 60",
    )
    def test_from_blueprint_invalid_filetype(
        self, mock_open_file, mock_path_exists, tmp_path
    ):
        """Ensure that from_blueprint correctly loads a ROMSSimulation from a valid YAML
        file."""
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
        """Ensure that from_blueprint correctly loads a ROMSSimulation from a valid YAML
        file."""
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

    def test_blueprint_roundtrip(self, example_roms_simulation, tmp_path):
        sim, _ = example_roms_simulation
        sim.directory = tmp_path / "simdir"
        output_file = tmp_path / "test.yaml"
        sim.to_blueprint(filename=output_file)
        sim2 = ROMSSimulation.from_blueprint(
            blueprint=output_file,
            directory=sim.directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )
        assert sim.to_dict() == sim2.to_dict()


class TestProcessingAndExecution:
    def test_runtime_code_modifications(self, example_roms_simulation):
        sim, directory = example_roms_simulation

        # Fake the runtime code
        rtc_dir = directory / "ROMS_runtime_code"
        sim.runtime_code.working_path = rtc_dir

        # Fake the partitioned files
        dataset_directory = directory / "ROMS/input_datasets"
        partitioned_directory = dataset_directory / "PARTITIONED"

        for ind in sim.input_datasets:
            ind.working_path = dataset_directory / ind.source.basename
            partitioned_path = partitioned_directory / ind.source.basename
            ind.partitioned_files = [
                partitioned_path.with_suffix("").with_suffix(f".{i}.nc")
                for i in range(sim.discretization.n_procs_tot)
            ]

        assert isinstance(sim._runtime_code_modifications, list)
        assert [isinstance(rcm, dict) for rcm in sim._runtime_code_modifications]
        assert [sim._runtime_code_modifications[i] == {} for i in [0, 2, 3, 4]]

        rtcm_dict = sim._runtime_code_modifications[1]

        assert rtcm_dict["__TIMESTEP_PLACEHOLDER__"] == sim.discretization.time_step
        assert rtcm_dict["__GRID_FILE_PLACEHOLDER__"] == str(
            partitioned_directory / "grid.nc"
        )
        assert rtcm_dict["__INITIAL_CONDITION_FILE_PLACEHOLDER__"] == str(
            partitioned_directory / "initial.nc"
        )
        assert rtcm_dict["__FORCING_FILES_PLACEHOLDER__"] == (
            f"{partitioned_directory/'surface.nc'}"
            + f"\n     {partitioned_directory/'boundary.nc'}"
            + f"\n     {partitioned_directory/'tidal.nc'}"
        )

        assert rtcm_dict["__MARBL_SETTINGS_FILE_PLACEHOLDER__"] == str(
            rtc_dir / "marbl_in"
        )
        assert rtcm_dict["__MARBL_TRACER_LIST_FILE_PLACEHOLDER__"] == str(
            rtc_dir / "marbl_tracer_output_list"
        )
        assert rtcm_dict["__MARBL_DIAG_LIST_FILE_PLACEHOLDER__"] == str(
            rtc_dir / "marbl_diagnostic_output_list"
        )

    def test_runtime_code_modifications_raises_if_no_working_path(
        self, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        with pytest.raises(
            ValueError, match="does not have a 'working_path' attribute."
        ):
            sim._runtime_code_modifications

    def test_runtime_code_modifications_raises_if_no_template(
        self, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        sim.runtime_code.working_path = directory / "ROMS/runtime_code"
        sim.runtime_code.files = [f.rstrip("_TEMPLATE") for f in sim.runtime_code.files]

        with pytest.raises(ValueError, match="could not find expected template"):
            sim._runtime_code_modifications

    @pytest.mark.parametrize(
        "missing_type", [ROMSModelGrid, ROMSInitialConditions, ROMSTidalForcing]
    )
    def test_runtime_code_modifications_raises_if_no_partitioned_files(
        self, example_roms_simulation, missing_type
    ):
        sim, directory = example_roms_simulation
        sim.runtime_code.working_path = directory / "ROMS/runtime_code"

        # Fake the partitioned files
        dataset_directory = directory / "ROMS/input_datasets"
        partitioned_directory = dataset_directory / "PARTITIONED"

        for ind in sim.input_datasets:
            if not isinstance(ind, missing_type):
                ind.working_path = dataset_directory / ind.source.basename
                partitioned_path = partitioned_directory / ind.source.basename
                ind.partitioned_files = [
                    partitioned_path.with_suffix("").with_suffix(f".{i}.nc")
                    for i in range(sim.discretization.n_procs_tot)
                ]
        with pytest.raises(
            ValueError, match="could not find a local path to a partitioned ROMS"
        ):
            sim._runtime_code_modifications

    @patch.object(
        ROMSSimulation, "_runtime_code_modifications", new_callable=PropertyMock
    )
    @patch("cstar.roms.simulation._replace_text_in_file")
    @patch("shutil.copy")
    def test_update_runtime_code(
        self, mock_copy, mock_replace_text, mock_rtcm, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        runtime_directory = directory / "ROMS/runtime_code"
        mock_rtcm.return_value = [{}, {"hello": "world"}, {}, {}, {}]

        sim.runtime_code.working_path = runtime_directory
        sim.runtime_code.modified_files = [None, "file2.in", None, None, None]

        sim.update_runtime_code()

        mock_copy.assert_called_once_with(
            runtime_directory / "file2.in_TEMPLATE", runtime_directory / "file2.in"
        )
        mock_replace_text.assert_called_once_with(
            runtime_directory / "file2.in", "hello", "world"
        )

    def test_update_runtime_code_warns_without_template(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        sim.runtime_code.working_path = directory / "ROMS/runtime_code"
        sim.runtime_code.files = ["file1.in", "file2", "marbl_in"]
        sim.runtime_code.modified_files = [None, None, None]

        with pytest.warns(UserWarning, match="No editable runtime code found"):
            sim.update_runtime_code()

    @patch.object(ROMSInputDataset, "get")
    @patch.object(AdditionalCode, "get")
    @patch.object(ExternalCodeBase, "handle_config_status")
    def test_setup(
        self,
        mock_handle_config_status,
        mock_additionalcode_get,
        mock_inputdataset_get,
        example_roms_simulation,
    ):
        sim, directory = example_roms_simulation
        sim.setup()
        assert mock_handle_config_status.call_count == 2
        assert mock_additionalcode_get.call_count == 2
        assert mock_inputdataset_get.call_count == 5

    @pytest.mark.parametrize(
        "codebase_status, marbl_status, expected",
        [
            (0, 0, True),  # ✅ Both correctly configured → should return True
            (1, 0, False),  # ❌ Codebase not configured
            (0, 1, False),  # ❌ MARBL codebase not configured
            (1, 1, False),  # ❌ Both not configured
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
        example_roms_simulation,
    ):
        sim, _ = example_roms_simulation

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
            (True, True, True),  # ✅ Both exist → is_setup should be True
            (False, True, False),  # ❌ Runtime code missing
            (True, False, False),  # ❌ Compile-time code missing
            (False, False, False),  # ❌ Both missing
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
        example_roms_simulation,
    ):
        sim, _ = example_roms_simulation

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
        example_roms_simulation,
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
        example_roms_simulation : fixture
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
        sim, _ = example_roms_simulation

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
    def test_build(self, mock_subprocess, mock_get_hash, example_roms_simulation):
        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"
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

    @patch("builtins.print")  # Mock print to check if the early exit message is printed
    @patch(
        "cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash"
    )  # Mock hash function
    @patch("subprocess.run")  # Mock subprocess (should not be called)
    def test_build_no_rebuild(
        self, mock_subprocess, mock_get_hash, mock_print, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"
        # Mock properties for early exit conditions
        with (
            patch.object(
                AdditionalCode,
                "exists_locally",
                new_callable=PropertyMock,
                return_value=True,
            ),
            patch.object(Path, "exists", return_value=True),
        ):  # Pretend the executable exists
            sim._exe_hash = "dummy_hash"
            sim.compile_time_code.working_path = build_dir
            sim.build(rebuild=False)

            # Ensure early exit message was printed
            mock_print.assert_any_call(
                f"ROMS has already been built at {build_dir/"roms"}, and "
                "the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSSimulation.build(rebuild = True)"
            )

            # Ensure subprocess.run was *not* called
            mock_subprocess.assert_not_called()

    @patch("cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash")
    @patch("subprocess.run")
    def test_build_raises_if_make_clean_error(
        self, mock_subprocess, mock_get_hash, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"
        (build_dir / "Compile").mkdir(exist_ok=True, parents=True)
        sim.compile_time_code.working_path = build_dir

        mock_subprocess.return_value = MagicMock(returncode=1, stderr="")
        mock_get_hash.return_value = "mockhash123"

        with pytest.raises(RuntimeError, match="Error 1 when compiling ROMS"):
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
        self, mock_subprocess, mock_get_hash, example_roms_simulation
    ):
        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"
        sim.compile_time_code.working_path = build_dir

        mock_subprocess.return_value = MagicMock(returncode=1, stderr="")
        mock_get_hash.return_value = "mockhash123"

        with pytest.raises(RuntimeError, match="Error 1 when compiling ROMS"):
            sim.build()
        assert mock_subprocess.call_count == 1

        mock_subprocess.assert_any_call(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )

    def test_build_raises_if_no_build_dir(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        with pytest.raises(ValueError, match="Unable to compile ROMSSimulation"):
            sim.build()

    @patch.object(ROMSInputDataset, "partition")  # Mock partition method
    def test_pre_run(self, mock_partition, example_roms_simulation):
        """Test that pre_run calls partition() only on datasets that exist locally."""

        sim, _ = example_roms_simulation

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
            dataset_1.partition.assert_called_once_with(np_xi=2, np_eta=3)
            dataset_2.partition.assert_not_called()  # Does not exist → shouldn't be partitioned
            dataset_3.partition.assert_called_once_with(np_xi=2, np_eta=3)

    def test_run_no_executable(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        with pytest.raises(ValueError, match="unable to find ROMS executable"):
            sim.run()

    def test_run_no_node_distribution(self, example_roms_simulation):
        sim, directory = example_roms_simulation
        sim.exe_path = directory / "ROMS/compile_time_code/roms"
        with patch(
            "cstar.roms.simulation.ROMSDiscretization.n_procs_tot",
            new_callable=PropertyMock,
            return_value=None,
        ):
            with pytest.raises(
                ValueError, match="Unable to calculate node distribution"
            ):
                sim.run()

    @patch("cstar.roms.simulation._replace_text_in_file")  # Mock text replacement
    @patch.object(ROMSSimulation, "update_runtime_code")  # Mock updating runtime code
    def test_run_local_execution(
        self, mock_update_runtime_code, mock_replace_text, example_roms_simulation
    ):
        """Test that run() correctly starts a local process when no scheduler is
        available."""

        sim, directory = example_roms_simulation

        # Mock no scheduler
        with (
            patch("cstar.roms.simulation.LocalProcess") as mock_local_process,
            patch(
                "cstar.system.manager.CStarSystemManager.scheduler",
                new_callable=PropertyMock,
                return_value=None,
            ),
        ):
            sim.exe_path = directory / "ROMS/compile_time_code/roms"
            mock_process_instance = MagicMock()
            mock_local_process.return_value = mock_process_instance

            execution_handler = sim.run()

            # Check LocalProcess was instantiated correctly
            mock_local_process.assert_called_once_with(
                commands=f"{cstar_sysmgr.environment.mpi_exec_prefix} -n {sim.discretization.n_procs_tot} {sim.exe_path} {sim.in_file}",
                run_path=sim.directory / "output",
            )

            # Ensure process was started
            mock_process_instance.start.assert_called_once()

            # Ensure execution handler was set correctly
            assert execution_handler == mock_process_instance

    @patch("cstar.roms.simulation._replace_text_in_file")  # Mock text replacement
    @patch.object(ROMSSimulation, "update_runtime_code")  # Mock updating runtime code
    def test_run_with_scheduler(
        self, mock_update_runtime_code, mock_replace_text, example_roms_simulation
    ):
        """Test that run() correctly uses scheduler defaults when queue_name and
        walltime are None."""

        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"

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
            mock_job_instance = MagicMock()
            mock_create_job.return_value = mock_job_instance

            # Call `run()` without explicitly passing `queue_name` and `walltime`
            execution_handler = sim.run(account_key="some_key")

            mock_create_job.assert_called_once_with(
                commands=f'mpirun -n 6 {build_dir/"roms"} file2.in',
                job_name=None,
                cpus=6,
                account_key="some_key",
                run_path=directory / "output",
                queue_name="default_queue",
                walltime="12:00:00",
            )

            mock_job_instance.submit.assert_called_once()

            assert execution_handler == mock_job_instance

    @patch("cstar.roms.simulation._replace_text_in_file")  # Mock text replacement
    @patch.object(ROMSSimulation, "update_runtime_code")  # Mock updating runtime code
    def test_run_with_scheduler_raises_if_no_account_key(
        self, mock_update_runtime_code, mock_replace_text, example_roms_simulation
    ):
        """Test that run() correctly uses scheduler defaults when queue_name and
        walltime are None."""

        sim, directory = example_roms_simulation
        build_dir = directory / "ROMS/compile_time_code"

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

    def test_post_run_raises_if_called_before_run(self, example_roms_simulation):
        sim, _ = example_roms_simulation
        with pytest.raises(
            RuntimeError,
            match=re.escape(
                "Cannot call 'ROMSSimulation.post_run()' before calling 'ROMSSimulation.run()'"
            ),
        ):
            sim.post_run()

    def test_post_run_raises_if_still_running(self, example_roms_simulation):
        sim, _ = example_roms_simulation

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

    @patch("subprocess.run")  # Mock ncjoin execution
    def test_post_run_merges_netcdf_files(
        self, mock_subprocess, example_roms_simulation
    ):
        """Test that post_run correctly identifies NetCDF output files and calls
        ncjoin."""

        # Setup
        sim, directory = example_roms_simulation
        output_dir = directory / "output"
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

    @patch("builtins.print")  # Mock print to check output
    @patch.object(Path, "glob", return_value=[])  # Mock glob to return no files
    def test_post_run_prints_message_if_no_files(
        self, mock_glob, mock_print, example_roms_simulation
    ):
        """Test that post_run prints a message and exits early if no suitable files are
        found."""

        # Setup
        sim, _ = example_roms_simulation
        sim._execution_handler = MagicMock()
        sim._execution_handler.status = (
            ExecutionStatus.COMPLETED
        )  # Ensure simulation is complete

        # Call post_run
        sim.post_run()

        # Check print was called with the expected message
        mock_print.assert_called_once_with("no suitable output found")

        # Ensure glob was called once
        mock_glob.assert_called_once()

    @patch("subprocess.run")  # Mock subprocess.run to simulate a failure
    @patch.object(Path, "glob")  # Mock glob to return fake files
    def test_post_run_raises_error_if_ncjoin_fails(
        self, mock_glob, mock_subprocess, example_roms_simulation
    ):
        """Test that post_run raises a RuntimeError if ncjoin fails."""

        # Setup
        sim, directory = example_roms_simulation
        output_dir = directory / "output"
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
        with pytest.raises(RuntimeError, match="Error 1 while joining ROMS output"):
            sim.post_run()

        mock_subprocess.assert_called_once_with(
            "ncjoin ocean_his.20240101000000.*.nc",
            cwd=output_dir,
            capture_output=True,
            text=True,
            shell=True,
        )


class TestROMSSimulationRestart:
    """Test class for the `restart()` method of `ROMSSimulation`."""

    @patch.object(Path, "glob")  # Mock file search
    @patch.object(Path, "exists", return_value=True)
    def test_restart(self, mock_exists, mock_glob, example_roms_simulation):
        """Test that `restart()` creates a new ROMSSimulation and sets
        initial_conditions correctly."""

        # Setup mock simulation
        sim, directory = example_roms_simulation
        new_end_date = datetime(2026, 6, 1)

        # Mock restart file found
        restart_file = directory / "output/restart_rst.20251231000000.nc"
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
        self, mock_exists, mock_glob, example_roms_simulation
    ):
        """Test that `restart()` creates a new ROMSSimulation and sets
        initial_conditions correctly."""

        # Setup mock simulation
        sim, directory = example_roms_simulation
        new_end_date = datetime(2026, 6, 1)

        # Mock restart file found
        mock_glob.return_value = []

        # Call method
        with pytest.raises(
            FileNotFoundError, match=f"No files in {directory/'output'} match"
        ):
            sim.restart(new_end_date=new_end_date)

        mock_glob.assert_called_once_with("*_rst.20251231000000.nc")

    @patch.object(Path, "glob")
    def test_restart_raises_if_multiple_restarts_found(
        self, mock_glob, example_roms_simulation
    ):
        """Test that restart() raises a ValueError if multiple unique restart files are
        found."""

        sim, directory = example_roms_simulation
        restart_dir = directory / "output"
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
