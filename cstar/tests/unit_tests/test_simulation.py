import pickle
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
from cstar.base import ExternalCodeBase, AdditionalCode, Discretization
from cstar.simulation import Simulation
from cstar.execution.handler import ExecutionStatus
from cstar.execution.local_process import LocalProcess


# Minimal concrete subclass for testing
class MockExternalCodeBase(ExternalCodeBase):
    @property
    def default_source_repo(self) -> str:
        return "https://github.com/test/repo.git"

    @property
    def default_checkout_target(self) -> str:
        return "test-tag"

    @property
    def expected_env_var(self) -> str:
        return "TEST_CODEBASE_ROOT"

    def get(self, target: str | Path) -> None:
        pass


class MockSimulation(Simulation):
    @property
    def default_codebase(self):
        return MockExternalCodeBase()

    @classmethod
    def from_dict(cls, simulation_dict, directory):
        return cls(**simulation_dict)

    @classmethod
    def from_blueprint(cls, blueprint, directory):
        pass

    def to_blueprint(self, filename):
        pass

    def setup(self):
        pass

    def build(self, rebuild=False):
        pass

    def pre_run(self):
        pass

    def run(self):
        pass

    def post_run(self):
        pass


class TestSimulationInitialization:
    def setup_method(self):
        self.simulation = MockSimulation(
            name="TestSim",
            directory="some/dir",
            discretization=Discretization(time_step=60),
            start_date="2025-01-01",
            end_date="2025-12-31",
            valid_start_date="2024-01-01",
            valid_end_date="2026-01-01",
        )

    """Tests Simulation initialization and its helper functions."""

    @pytest.mark.parametrize(
        "input_date,expected",
        [
            ("2025-01-01", datetime(2025, 1, 1)),
            (datetime(2025, 1, 1), datetime(2025, 1, 1)),
            (None, None),
        ],
    )
    def test_parse_date(self, input_date, expected):
        """Ensure `_parse_date()` correctly processes different date formats."""
        assert (
            self.simulation._parse_date(date=input_date, field_name="test_field")
            == expected
        )

    def test_get_date_or_fallback_valid(self):
        """Ensure `_get_date_or_fallback()` correctly selects provided date over
        fallback."""
        assert self.simulation._get_date_or_fallback(
            date="2025-01-01", fallback=datetime(2024, 1, 1), field_name="start_date"
        ) == datetime(2025, 1, 1)

    def test_get_date_or_fallback_fallback(self):
        """Ensure `_get_date_or_fallback()` correctly falls back to the default date."""
        with pytest.warns(UserWarning, match="start_date not provided"):
            assert self.simulation._get_date_or_fallback(
                date=None, fallback=datetime(2024, 1, 1), field_name="start_date"
            ) == datetime(2024, 1, 1)

    def test_get_date_or_fallback_raises_error(self):
        """Ensure `_get_date_or_fallback()` raises an error if no valid date is
        provided."""
        with pytest.raises(
            ValueError, match="Neither start_date nor a valid fallback was provided."
        ):
            self.simulation._get_date_or_fallback(
                date=None, fallback=None, field_name="start_date"
            )

    def test_validate_date_range_valid(self):
        """Ensure `_validate_date_range()` does not raise errors when dates are
        valid."""
        self.simulation._validate_date_range()  # Should not raise any error

    def test_validate_date_range_start_date_too_early(self, tmp_path):
        """Ensure `_validate_date_range()` raises an error if start_date is before
        valid_start_date."""
        with pytest.raises(
            ValueError, match="start_date .* is before the earliest valid start date"
        ):
            MockSimulation(
                name="InvalidSim",
                directory=tmp_path,
                discretization=Discretization(time_step=60),
                start_date="2023-12-31",  # Too early
                end_date="2025-12-31",
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

    def test_validate_date_range_end_date_too_late(self, tmp_path):
        """Ensure `_validate_date_range()` raises an error if end_date is after
        valid_end_date."""
        with pytest.raises(
            ValueError, match="end_date .* is after the latest valid end date"
        ):
            MockSimulation(
                name="InvalidSim",
                directory=tmp_path,
                discretization=Discretization(time_step=60),
                start_date="2025-01-01",
                end_date="2026-02-01",  # Too late
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

    def test_simulation_raises_error_if_start_date_after_end_date(self, tmp_path):
        """Ensure an error is raised if start_date is after end_date."""
        with pytest.raises(ValueError, match="start_date .* is after end_date"):
            MockSimulation(
                name="InvalidSim",
                directory=tmp_path,
                discretization=Discretization(time_step=60),
                start_date="2025-12-31",
                end_date="2025-01-01",
                valid_start_date="2025-12-01",
                valid_end_date="2026-01-01",
            )

    # Tests for _validate_simulation_directory
    def test_validate_simulation_directory_new_directory(self, tmp_path):
        """Ensure `_validate_simulation_directory()` correctly resolves a new
        directory."""
        new_dir = tmp_path / "new_simulation"
        assert (
            self.simulation._validate_simulation_directory(new_dir) == new_dir.resolve()
        )

    def test_validate_simulation_directory_existing_non_empty_directory(self, tmp_path):
        """Ensure `_validate_simulation_directory()` raises an error for a non-empty
        existing directory."""
        non_empty_dir = tmp_path / "existing_simulation"
        non_empty_dir.mkdir()
        (non_empty_dir / "file.txt").touch()

        with pytest.raises(
            FileExistsError, match="exists and is not an empty directory"
        ):
            self.simulation._validate_simulation_directory(non_empty_dir)

    # Test Initialisation directly:

    def test_simulation_initialization_valid(self):
        """Ensure simulation initializes correctly with valid values and calls helper
        methods."""
        with (
            patch.object(
                MockSimulation, "_validate_simulation_directory"
            ) as mock_validate_dir,
            patch.object(MockSimulation, "_validate_date_range") as mock_validate_dates,
        ):
            mock_validate_dir.return_value = Path("some/dir").resolve()
            sim = MockSimulation(
                name="TestSim",
                directory="some/dir",
                discretization=Discretization(time_step=60),
                start_date="2025-01-01",
                end_date="2025-12-31",
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

            mock_validate_dir.assert_called_once_with("some/dir")
            mock_validate_dates.assert_called_once()

            assert sim.directory == Path("some/dir").resolve()
            assert sim.start_date == datetime(2025, 1, 1)
            assert sim.end_date == datetime(2025, 12, 31)
            assert sim.valid_start_date == datetime(2024, 1, 1)
            assert sim.valid_end_date == datetime(2026, 1, 1)

    def test_simulation_uses_fallback_dates(self, tmp_path):
        """Ensure that missing start_date and end_date correctly default to
        valid_start_date and valid_end_date."""
        with pytest.warns(UserWarning, match="not provided. Defaulting to"):
            sim = MockSimulation(
                name="FallbackSim",
                directory=tmp_path,
                discretization=Discretization(time_step=60),
                valid_start_date="2025-01-01",
                valid_end_date="2025-12-31",
            )

        assert sim.start_date == datetime(2025, 1, 1)
        assert sim.end_date == datetime(2025, 12, 31)

    def test_simulation_warns_if_no_valid_dates(self, tmp_path):
        with pytest.warns(RuntimeWarning, match="Cannot enforce date range validation"):
            MockSimulation(
                name="FallbackSim",
                directory=tmp_path,
                discretization=Discretization(time_step=60),
                start_date="2025-01-01",
                end_date="2025-01-02",
            )


class TestStrAndRepr:
    def test_str(self, tmp_path):
        sim_dir = tmp_path
        sim = MockSimulation(
            name="TestSim",
            directory=sim_dir,
            discretization=Discretization(time_step=60),
            runtime_code=AdditionalCode(location=sim_dir, files=["file1", "file2"]),
            compile_time_code=AdditionalCode(
                location=sim_dir, files=["file1", "file2"]
            ),
            start_date="2025-01-01",
            end_date="2025-12-31",
            valid_start_date="2024-01-01",
            valid_end_date="2026-01-01",
        )
        sim.exe_path = sim_dir

        expected_str = f"""\
MockSimulation
--------------
Name: TestSim
Directory: {sim_dir.resolve()}
Start date: 2025-01-01 00:00:00
End date: 2025-12-31 00:00:00
Valid start date: 2024-01-01 00:00:00
Valid end date: 2026-01-01 00:00:00

Discretization: Discretization(time_step = 60)

Code:
Codebase: MockExternalCodeBase instance (query using MockSimulation.codebase)
Runtime code: AdditionalCode instance with 2 files (query using MockSimulation.runtime_code)
Compile-time code: AdditionalCode instance with 2 files (query using MockSimulation.compile_time_code)
Is compiled: True
Executable path: {sim_dir}"""

        assert sim.__str__() == expected_str

    def test_repr(self, tmp_path):
        sim_dir = tmp_path
        sim = MockSimulation(
            name="TestSim",
            directory=sim_dir,
            discretization=Discretization(time_step=60),
            runtime_code=AdditionalCode(location=sim_dir, files=["file1", "file2"]),
            compile_time_code=AdditionalCode(
                location=sim_dir, files=["file1", "file2"]
            ),
            start_date="2025-01-01",
            end_date="2025-12-31",
            valid_start_date="2024-01-01",
            valid_end_date="2026-01-01",
        )
        expected_repr = f"""\
MockSimulation(
name = TestSim,
directory = {sim_dir},
start_date = 2025-01-01 00:00:00,
end_date = 2025-12-31 00:00:00,
valid_start_date = 2024-01-01 00:00:00,
valid_end_date = 2026-01-01 00:00:00,
discretization = Discretization(time_step = 60),
codebase = <MockExternalCodeBase instance>,
runtime_code = <AdditionalCode instance>,
compile_time_code = <AdditionalCode instance>)"""

        assert sim.__repr__() == expected_repr


class TestSimulationPersistence:
    def setup_method(self):
        """Setup a mock simulation instance for testing."""
        self.simulation = MockSimulation(
            name="TestSim",
            directory="some/dir",
            discretization=Discretization(time_step=60),
            start_date="2025-01-01",
            end_date="2025-12-31",
            valid_start_date="2024-01-01",
            valid_end_date="2026-01-01",
        )

    def test_persist_creates_file(self, tmp_path):
        """Ensure `persist` creates the expected file."""
        self.simulation.directory = tmp_path
        self.simulation.persist()
        assert (
            self.simulation.directory / "simulation_state.pkl"
        ), "Persisted file was not created."

    def test_persist_and_restore(self, tmp_path):
        """Ensure `persist` and `restore` correctly save and load the simulation."""
        self.simulation.directory = tmp_path
        self.simulation.persist()
        restored_sim = MockSimulation.restore(self.simulation.directory)

        # Ensure the restored object has the same attributes
        assert (
            restored_sim.to_dict() == self.simulation.to_dict()
        ), "Restored simulation does not match the original"

        # Alternative: Compare serialized versions
        assert pickle.dumps(restored_sim) == pickle.dumps(
            self.simulation
        ), "Serialized data mismatch after restore"

    def test_restore_missing_file(self, tmp_path):
        """Ensure `restore` raises an error if the persisted file is missing."""
        with pytest.raises(FileNotFoundError):
            MockSimulation.restore(tmp_path)

    def test_persist_raises_error_if_simulation_is_running(self):
        """Ensure `persist` raises an error if the simulation is running."""
        mock_handler = MagicMock(spec=LocalProcess)
        mock_handler.status = ExecutionStatus.RUNNING

        # Assign our mock execution handler to the simulation
        self.simulation._execution_handler = mock_handler

        with pytest.raises(
            RuntimeError, match="at least one local process is currently running"
        ):
            self.simulation.persist()


class TestSimulationRestart:
    def setup_method(self, tmp_path):
        """Setup a mock simulation instance for testing."""
        self.simulation = MockSimulation(
            name="TestSim",
            directory="some/dir",
            discretization=Discretization(time_step=60),
            start_date="2025-01-01",
            end_date="2025-12-31",
            valid_start_date="2024-01-01",
            valid_end_date="2026-01-01",
        )

    def test_restart_creates_new_instance(self):
        """Ensure `restart` returns a new Simulation instance."""
        new_sim = self.simulation.restart(new_end_date="2026-06-30")

        assert isinstance(
            new_sim, MockSimulation
        ), "Restart did not return a new MockSimulation instance"
        assert (
            new_sim is not self.simulation
        ), "Restarted simulation should be a new object"

    def test_restart_updates_start_and_end_dates(self):
        """Ensure `restart` sets start_date to the original end_date and updates
        end_date correctly."""
        new_end_date = datetime(2026, 6, 30)
        new_sim = self.simulation.restart(new_end_date=new_end_date)
        assert (
            new_sim.start_date == self.simulation.end_date
        ), "Restarted simulation start_date is incorrect"
        assert (
            new_sim.end_date == new_end_date
        ), "Restarted simulation end_date does not match input"

    def test_restart_preserves_other_attributes(self):
        """Ensure attributes other than start_date, end_date, and directory remain
        unchanged."""
        new_sim = self.simulation.restart(new_end_date="2026-06-30")

        assert new_sim.name == self.simulation.name
        assert (
            new_sim.discretization.__dict__ == self.simulation.discretization.__dict__
        )
        assert new_sim.valid_start_date == self.simulation.valid_start_date
        assert new_sim.valid_end_date == self.simulation.valid_end_date

    def test_restart_updates_directory(self):
        """Ensure `restart` updates the directory to include the restart timestamp."""
        new_sim = self.simulation.restart(new_end_date="2026-06-30")

        expected_dir_suffix = (
            f"RESTART_{self.simulation.end_date.strftime('%Y%m%d_%H%M%S')}"
        )
        assert expected_dir_suffix in str(
            new_sim.directory
        ), "Restart directory does not include correct timestamp"

    def test_restart_raises_error_on_invalid_new_end_date(self):
        """Ensure `restart` raises an error if `new_end_date` is invalid."""
        with pytest.raises(
            ValueError, match="Expected str or datetime for `new_end_date`"
        ):
            self.simulation.restart(new_end_date=42)  # Invalid type

    def test_restart_with_string_end_date(self):
        """Ensure `restart` correctly parses string `new_end_date`."""
        new_sim = self.simulation.restart(new_end_date="2026-06-30")

        assert new_sim.end_date == datetime(
            2026, 6, 30
        ), "Restarted simulation did not correctly parse string end_date"


def test_to_dict(tmp_path):
    sim_dir = tmp_path
    sim = MockSimulation(
        name="TestSim",
        directory=sim_dir,
        discretization=Discretization(time_step=60),
        codebase=MockExternalCodeBase(),
        runtime_code=AdditionalCode(
            location=sim_dir,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        ),
        compile_time_code=AdditionalCode(
            location=sim_dir,
            subdir="subdir/",
            checkout_target="main",
            files=["file1", "file2"],
        ),
        start_date="2025-01-01",
        end_date="2025-12-31",
        valid_start_date="2024-01-01",
        valid_end_date="2026-01-01",
    )

    test_dict = sim.to_dict()

    assert test_dict["name"] == "TestSim"
    assert test_dict["discretization"] == {"time_step": 60}
    assert test_dict["codebase"]["source_repo"] == "https://github.com/test/repo.git"
    assert test_dict["codebase"]["checkout_target"] == "test-tag"
    assert test_dict["runtime_code"]["location"] == str(sim_dir)
    assert test_dict["runtime_code"]["files"] == ["file1", "file2"]
    assert test_dict["runtime_code"]["subdir"] == "subdir/"
    assert test_dict["runtime_code"]["checkout_target"] == "main"
    assert test_dict["compile_time_code"]["location"] == str(sim_dir)
    assert test_dict["compile_time_code"]["subdir"] == "subdir/"
    assert test_dict["compile_time_code"]["files"] == ["file1", "file2"]
    assert test_dict["compile_time_code"]["checkout_target"] == "main"
    assert test_dict["valid_start_date"] == datetime(2024, 1, 1, 0, 0)
    assert test_dict["valid_end_date"] == datetime(2026, 1, 1, 0, 0)
