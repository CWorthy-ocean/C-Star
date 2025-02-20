import pytest
import pickle
from pathlib import Path
from datetime import datetime
from cstar.roms.simulation import ROMSSimulation
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.input_dataset import (
    ROMSModelGrid,
    ROMSInitialConditions,
    ROMSTidalForcing,
    ROMSBoundaryForcing,
    ROMSSurfaceForcing,
)
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.base.additional_code import AdditionalCode


@pytest.fixture
def example_roms_simulation(tmp_path):
    directory = tmp_path
    sim = ROMSSimulation(
        name="ROMSTest",
        directory=directory,
        discretization=ROMSDiscretization(time_step=60),
        codebase=ROMSExternalCodeBase(
            source_repo="http://my.code/repo.git", checkout_target="dev"
        ),
        runtime_code=AdditionalCode(
            location=directory.parent,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2.in_TEMPLATE"],
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
            "n_procs_x": 1,
            "n_procs_y": 1,
        }

        assert sim.codebase.source_repo == "http://my.code/repo.git"
        assert sim.codebase.checkout_target == "dev"
        assert sim.runtime_code.source.location == str(directory.parent)
        assert sim.runtime_code.subdir == "subdir/"
        assert sim.runtime_code.checkout_target == "main"
        assert sim.runtime_code.files == ["file1", "file2.in_TEMPLATE"]
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

Discretization: ROMSDiscretization(time_step = 60, n_procs_x = 1, n_procs_y = 1)

Code:
Codebase: ROMSExternalCodeBase instance (query using ROMSSimulation.codebase)
Runtime code: AdditionalCode instance with 2 files (query using ROMSSimulation.runtime_code)
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
discretization = ROMSDiscretization(time_step = 60, n_procs_x = 1, n_procs_y = 1),
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


class TestToAndFromDictAndBlueprint:
    def test_to_dict(self, example_roms_simulation):
        """Test key/value pairs unique ROMSSimulation.to_dict (Simulation.to_dict tested
        elsewhere)"""
        sim, directory = example_roms_simulation
        test_dict = sim.to_dict()
        assert (
            test_dict["marbl_codebase"]["source_repo"] == sim.marbl_codebase.source_repo
        )
        assert (
            test_dict["marbl_codebase"]["checkout_target"]
            == sim.marbl_codebase.checkout_target
        )
        assert test_dict["model_grid"] == {
            "location": "http://my.files/grid.nc",
            "file_hash": "123",
        }
        assert test_dict["initial_conditions"] == {
            "location": "http://my.files/initial.nc",
            "file_hash": "234",
        }
        assert test_dict["tidal_forcing"] == {
            "location": "http://my.files/tidal.nc",
            "file_hash": "345",
        }
        assert test_dict["boundary_forcing"] == [
            {"location": "http://my.files/boundary.nc", "file_hash": "456"},
        ]
        assert test_dict["surface_forcing"] == [
            {"location": "http://my.files/surface.nc", "file_hash": "567"},
        ]

    def test_from_dict(self, example_roms_simulation):
        sim, directory = example_roms_simulation

        sim_dict = {
            "name": "ROMSTest",
            "valid_start_date": datetime(2024, 1, 1, 0, 0),
            "valid_end_date": datetime(2026, 1, 1, 0, 0),
            "codebase": {
                "source_repo": "http://my.code/repo.git",
                "checkout_target": "dev",
            },
            "discretization": {"time_step": 60, "n_procs_x": 1, "n_procs_y": 1},
            "runtime_code": {
                "location": directory.parent,
                "subdir": "subdir/",
                "checkout_target": "main",
                "files": ["file1", "file2.in_TEMPLATE"],
            },
            "compile_time_code": {
                "location": directory.parent,
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

        sim2 = ROMSSimulation.from_dict(
            sim_dict,
            directory=directory,
            start_date=sim.start_date,
            end_date=sim.end_date,
        )

        assert pickle.dumps(sim2) == pickle.dumps(sim), "Instances are not identical"

    def test_from_dict_warns_if_default_codebase(self, tmp_path):
        sim_dict = {
            "name": "ROMSTest",
            "valid_start_date": datetime(2024, 1, 1, 0, 0),
            "valid_end_date": datetime(2026, 1, 1, 0, 0),
            "discretization": {"time_step": 60, "n_procs_x": 1, "n_procs_y": 1},
            "runtime_code": {"location": "some/dir"},
            "compile_time_code": {"location": "some/dir"},
        }

        with pytest.warns(UserWarning, match="default codebase will be used"):
            ROMSSimulation.from_dict(
                sim_dict,
                directory=tmp_path,
                start_date="2024-01-01",
                end_date="2025-01-01",
            )

    def test_from_dict_with_single_forcing_entries(self, tmp_path):
        sim_dict = {
            "name": "ROMSTest",
            "valid_start_date": datetime(2024, 1, 1, 0, 0),
            "valid_end_date": datetime(2026, 1, 1, 0, 0),
            "codebase": {
                "source_repo": "http://my.code/repo.git",
                "checkout_target": "dev",
            },
            "discretization": {"time_step": 60, "n_procs_x": 1, "n_procs_y": 1},
            "runtime_code": {"location": "some/dir"},
            "compile_time_code": {"location": "some/dir"},
            "marbl_codebase": {"source_repo": "http://marbl.com/repo.git"},
            "surface_forcing": {
                "location": "http://my.files/surface.nc",
                "file_hash": "567",
            },
            "boundary_forcing": {
                "location": "http://my.files/boundary.nc",
                "file_hash": "456",
            },
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

    """Tests for loading ROMSSimulation from a YAML blueprint."""

    # @patch("pathlib.Path.exists", return_value=True)
    # @patch("builtins.open", new_callable=mock_open, read_data="name: TestROMS\ndiscretization:\n  time_step: 60")
    # @patch("yaml.safe_load", return_value={"name": "TestROMS", "discretization": {"time_step": 60}})
    # def test_from_blueprint_valid_file(self, mock_yaml_load, mock_open_file, mock_path_exists, tmp_path):
    #     """Ensure that from_blueprint correctly loads a ROMSSimulation from a valid YAML file."""
    #     blueprint_path = tmp_path / "roms_blueprint.yaml"

    #     with patch("cstar.base.datasource.DataSource.source_type", new="yaml"):
    #         sim = ROMSSimulation.from_blueprint(blueprint=str(blueprint_path), directory=tmp_path)

    #     # Assertions
    #     assert sim.name == "TestROMS"
    #     assert sim.discretization.time_step == 60
    #     mock_path_exists.assert_called_once()
    #     mock_open_file.assert_called_once_with(str(blueprint_path), "r")
    #     mock_yaml_load.assert_called_once()

    # @patch("builtins.open", new_callable=mock_open, read_data="invalid_yaml")
    # @patch("yaml.safe_load", side_effect=yaml.YAMLError)
    # def test_from_blueprint_invalid_yaml_format(self, mock_yaml_load, mock_open_file, tmp_path):
    #     """Ensure an error is raised when trying to load an incorrectly formatted YAML file."""
    #     blueprint_path = tmp_path / "invalid_blueprint.yaml"

    #     with pytest.raises(yaml.YAMLError):
    #         ROMSSimulation.from_blueprint(blueprint=str(blueprint_path), directory=tmp_path)

    #     mock_open_file.assert_called_once_with(str(blueprint_path), "r")
    #     mock_yaml_load.assert_called_once()

    # @patch("requests.get")
    # @patch("yaml.safe_load", return_value={"name": "TestROMS", "discretization": {"time_step": 60}})
    # def test_from_blueprint_from_url(self, mock_yaml_load, mock_requests_get, tmp_path):
    #     """Ensure that from_blueprint correctly loads a blueprint from a URL."""
    #     mock_response = MagicMock()
    #     mock_response.text = "name: TestROMS\ndiscretization:\n  time_step: 60"
    #     mock_requests_get.return_value = mock_response

    #     url = "https://example.com/roms_blueprint.yaml"

    #     with patch("cstar.base.datasource.DataSource.source_type", new="yaml"):
    #         sim = ROMSSimulation.from_blueprint(blueprint=url, directory=tmp_path)

    #     # Assertions
    #     assert sim.name == "TestROMS"
    #     assert sim.discretization.time_step == 60
    #     mock_requests_get.assert_called_once_with(url)
    #     mock_yaml_load.assert_called_once()
