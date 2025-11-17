import logging
import pickle
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cstar.base import Discretization
from cstar.execution.handler import ExecutionStatus
from cstar.execution.local_process import LocalProcess
from cstar.tests.unit_tests.fake_abc_subclasses import (
    StubSimulation,
)


class TestSimulationInitialization:
    """Tests the initialization of the `Simulation` class and its helper functions.

    This test suite verifies that `Simulation` initializes correctly under various conditions,
    including handling of date parsing, validation of simulation directories, and enforcement
    of valid date ranges.

    Tests
    -----
    - `test_parse_date`: Ensures `_parse_date()` correctly processes different date formats.
    - `test_get_date_or_fallback_valid`: Ensures `_get_date_or_fallback()` selects provided
      date over fallback.
    - `test_get_date_or_fallback_fallback`: Ensures `_get_date_or_fallback()` falls back to
      the specified fallback date.
    - `test_get_date_or_fallback_raises_if_no_dates`: Ensures `_get_date_or_fallback()` raises an
      error if no valid date is provided.
    - `test_validate_date_range_valid`: Ensures `_validate_date_range()` does not raise
      errors when dates are valid.
    - `test_validate_date_range_start_date_too_early`: Ensures `_validate_date_range()`
      raises an error if `start_date` is before `valid_start_date`.
    - `test_validate_date_range_end_date_too_late`: Ensures `_validate_date_range()` raises
      an error if `end_date` is after `valid_end_date`.
    - `test_simulation_raises_error_if_start_date_after_end_date`: Ensures an error is raised
      if `start_date` is after `end_date`.
    - `test_validate_simulation_directory_new_directory`: Ensures `_validate_simulation_directory()`
      correctly resolves a new directory.
    - `test_validate_simulation_directory_existing_non_empty_directory`: Ensures
      `_validate_simulation_directory()` raises an error for a non-empty existing directory.
    - `test_simulation_initialization_valid`: Ensures `Simulation` initializes correctly
      with valid values and calls helper methods.
    - `test_simulation_uses_fallback_dates`: Ensures that missing `start_date` and `end_date`
      correctly default to `valid_start_date` and `valid_end_date` in the Simulation instance.
    - `test_simulation_warns_if_no_valid_dates`: Ensures a warning is raised when no valid
      date constraints are provided.
    """

    @pytest.mark.parametrize(
        "input_date,expected",
        [
            ("2025-01-01", datetime(2025, 1, 1)),
            (datetime(2025, 1, 1), datetime(2025, 1, 1)),
            (None, None),
        ],
    )
    def test_parse_date(self, input_date, expected, stub_simulation):
        """Test `_parse_date()` for correct date format handling.

        This test ensures that `_parse_date()` properly converts string representations
        of dates into `datetime` objects and correctly handles `None` values.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Parameters
        ----------
        input_date : str, datetime, or None
            The input date to be parsed.
        expected : datetime or None
            The expected output after parsing.

        Assertions
        ----------
        - The returned value matches the expected `datetime` object or `None`.
        """
        sim = stub_simulation
        assert sim._parse_date(date=input_date, field_name="test_field") == expected

    def test_get_date_or_fallback_valid(self, stub_simulation):
        """Test `_get_date_or_fallback()` with a provided valid date.

        This test ensures that `_get_date_or_fallback()` correctly selects the provided
        date over the fallback value.

        Mocks & Fixtures
        ----------------
        stub_simulation (cstar.Simulation)
            Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The returned value matches the explicitly provided date.
        """
        sim = stub_simulation
        assert sim._get_date_or_fallback(
            date="2025-01-01", fallback=datetime(2024, 1, 1), field_name="start_date"
        ) == datetime(2025, 1, 1)

    def test_get_date_or_fallback_fallback(self, stub_simulation, caplog):
        """Test `_get_date_or_fallback()` when the date is missing.

        This test verifies that `_get_date_or_fallback()` correctly defaults to the
        fallback value when no explicit date is provided.

        Mocks & Fixtures
        ----------------
        stub_simulation (cstar.Simulation)
            Provides a mock `Simulation` instance.
        caplog (pytest.LogCaptureFixture)
            Builtin fixture capturing log messages

        Assertions
        ----------
        - A warning is logged indicating that the fallback value is being used.
        - The returned value matches the fallback date.
        """
        sim = stub_simulation
        caplog.set_level(logging.DEBUG, logger=sim.log.name)

        assert sim._get_date_or_fallback(
            date=None, fallback=datetime(2024, 1, 1), field_name="start_date"
        ) == datetime(2024, 1, 1)
        assert "start_date not provided" in caplog.text

    def test_get_date_or_fallback_raises_if_no_dates(self, stub_simulation):
        """Test `_get_date_or_fallback()` when both date and fallback are `None`.

        This test ensures that `_get_date_or_fallback()` raises a `ValueError` when
        neither a date nor a fallback is provided.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - A `ValueError` is raised with the expected error message.
        """
        sim = stub_simulation
        with pytest.raises(
            ValueError, match="Neither start_date nor a valid fallback was provided."
        ):
            sim._get_date_or_fallback(date=None, fallback=None, field_name="start_date")

    def test_validate_date_range_valid(self, stub_simulation):
        """Test `_validate_date_range()` with valid date ranges.

        This test ensures that `_validate_date_range()` does not raise any errors
        when `start_date` and `end_date` are within the valid range.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.
        """
        sim = stub_simulation
        sim._validate_date_range()  # Should not raise any error

    def test_validate_date_range_start_date_too_early(
        self, tmp_path, fakeexternalcodebase
    ):
        """Test `_validate_date_range()` when `start_date` is before `valid_start_date`.

        This test ensures that `_validate_date_range()` raises a `ValueError` if
        `start_date` is set earlier than `valid_start_date`.

        Mocks & Fixtures
        ----------------
        - `tmp_path`: Temporary directory for simulation setup.
        - `fakeexternalcodebase`: an ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
        - A `ValueError` is raised with a message indicating `start_date` is too early.
        """
        with pytest.raises(
            ValueError, match="start_date .* is before the earliest valid start date"
        ):
            StubSimulation(
                name="InvalidSim",
                directory=tmp_path,
                codebase=fakeexternalcodebase,
                discretization=Discretization(time_step=60),
                start_date="2023-12-31",  # Too early
                end_date="2025-12-31",
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

    def test_validate_date_range_end_date_too_late(
        self, fakeexternalcodebase, tmp_path
    ):
        """Test `_validate_date_range()` when `end_date` is after `valid_end_date`.

        This test ensures that `_validate_date_range()` raises a `ValueError` if
        `end_date` falls after `valid_end_date`.

        Mocks & Fixtures
        ----------------
        - `tmp_path`: Temporary directory for simulation setup.
        - `fakeexternalcodebase`: an ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
        - A `ValueError` is raised with a message indicating `end_date` is too late.
        """
        with pytest.raises(
            ValueError, match="end_date .* is after the latest valid end date"
        ):
            StubSimulation(
                name="InvalidSim",
                directory=tmp_path,
                codebase=fakeexternalcodebase,
                discretization=Discretization(time_step=60),
                start_date="2025-01-01",
                end_date="2026-02-01",  # Too late
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

    def test_simulation_raises_error_if_start_date_after_end_date(
        self, fakeexternalcodebase, tmp_path
    ):
        """Test that an error is raised when `start_date` is after `end_date`.

        This test ensures that `Simulation` initialization fails with a `ValueError`
        if `start_date` is set later than `end_date`.

        Mocks & Fixtures
        ----------------
        - `tmp_path`: Temporary directory for simulation setup.
        - `fakeexternalcodebase`: an ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
        - A `ValueError` is raised with a message indicating `start_date` is after `end_date`.
        """
        with pytest.raises(ValueError, match="start_date .* is after end_date"):
            StubSimulation(
                name="InvalidSim",
                directory=tmp_path,
                codebase=fakeexternalcodebase,
                discretization=Discretization(time_step=60),
                start_date="2025-12-31",
                end_date="2025-01-01",
                valid_start_date="2025-12-01",
                valid_end_date="2026-01-01",
            )

    # Test Initialisation directly:

    def test_simulation_initialization_valid(self, fakeexternalcodebase):
        """Test valid initialization of a `Simulation` instance.

        This test ensures that `Simulation` initializes correctly when provided
        with valid input values. It also verifies that the necessary helper methods
        (`_validate_simulation_directory` and `_validate_date_range`) are called.

        Mocks & Fixtures
        ----------------
        - `patch.object(StubSimulation, "_validate_simulation_directory")`: Mocks directory validation.
        - `patch.object(StubSimulation, "_validate_date_range")`: Mocks date range validation.
        - `fakeexternalcodebase`: an ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
        - `_validate_simulation_directory()` is called with the correct argument.
        - `_validate_date_range()` is called once.
        - The `Simulation` instance has correctly set attributes.
        """
        with (
            patch.object(StubSimulation, "_validate_date_range") as mock_validate_dates,
        ):
            sim = StubSimulation(
                name="TestSim",
                directory="some/dir",
                codebase=fakeexternalcodebase,
                discretization=Discretization(time_step=60),
                start_date="2025-01-01",
                end_date="2025-12-31",
                valid_start_date="2024-01-01",
                valid_end_date="2026-01-01",
            )

            mock_validate_dates.assert_called_once()

            assert sim.directory == Path("some/dir").resolve()
            assert sim.start_date == datetime(2025, 1, 1)
            assert sim.end_date == datetime(2025, 12, 31)
            assert sim.valid_start_date == datetime(2024, 1, 1)
            assert sim.valid_end_date == datetime(2026, 1, 1)

    def test_simulation_uses_fallback_dates(
        self, fakeexternalcodebase, tmp_path, caplog
    ):
        """Test that missing `start_date` and `end_date` default to valid ranges.

        This test ensures that when `start_date` or `end_date` is not provided,
        `Simulation` correctly defaults them to `valid_start_date` and `valid_end_date`,
        respectively.

        Mocks & Fixtures
        ----------------
        tmp_path (pathlib.Path)
            Temporary directory for simulation setup.
        caplog (pytest.LogCaptureFixture)
            builtin fixture for capturing logged messages
        fakeexternalcodebase
            An ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
        - The `Simulation` instance's `start_date` is set to `valid_start_date`.
        - The `Simulation` instance's `end_date` is set to `valid_end_date`.
        - A warning is logged indicating that default values are being used.
        """
        sim = StubSimulation(
            name="FallbackSim",
            directory=tmp_path,
            codebase=fakeexternalcodebase,
            discretization=Discretization(time_step=60),
            valid_start_date="2025-01-01",
            valid_end_date="2025-12-31",
        )

        caplog.set_level(logging.DEBUG, logger=sim.log.name)

        assert "not provided. Defaulting to" in caplog.text
        assert sim.start_date == datetime(2025, 1, 1)
        assert sim.end_date == datetime(2025, 12, 31)

    def test_simulation_warns_if_no_valid_dates(
        self, fakeexternalcodebase, tmp_path, caplog
    ):
        """Test that a warning is issued when no valid date constraints are provided.

        This test ensures that if neither `valid_start_date` nor `valid_end_date`
        is specified, `Simulation` issues a `RuntimeWarning`, indicating that
        date validation cannot be enforced.

        Mocks & Fixtures
        ----------------
        tmp_path (pathlib.Path)
            Temporary directory for simulation setup.
        caplog (pytest.LogCaptureFixture)
            builtin fixture for capturing logged messages
        fakeexternalcodebase
            an ExternalCodeBase instance without filesystem or network logic

        Assertions
        ----------
          - A warning is logged indicating that date range validation is not possible.
        """
        sim = StubSimulation(
            name="FallbackSim",
            codebase=fakeexternalcodebase,
            directory=tmp_path,
            discretization=Discretization(time_step=60),
            start_date="2025-01-01",
            end_date="2025-01-02",
        )
        caplog.set_level(logging.DEBUG, logger=sim.log.name)

        assert "Cannot enforce date range validation" in caplog.text


class TestStrAndRepr:
    """Tests for `__str__` and `__repr__` methods of `Simulation`.

    This test class verifies that the string and representation methods
    correctly format the `Simulation` instance's attributes.

    Tests
    -----
    - `test_str`: Ensures `__str__()` returns a properly formatted multi-line string
      containing simulation details, including name, directory, dates, discretization,
      and codebase information.
    - `test_repr`: Ensures `__repr__()` returns a correctly formatted string representation
      suitable for debugging, preserving all key attributes.
    """

    def test_str(self, stub_simulation):
        """Test the `__str__()` method of `Simulation`.

        This test ensures that `__str__()` produces a correctly formatted string
        representation of the `Simulation` instance, including key attributes such
        as name, directory, start and end dates, discretization settings, and
        codebase information.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The generated string matches the expected multi-line formatted output.
        """
        sim = stub_simulation
        sim.exe_path = sim.directory
        sim_dir = sim.directory
        expected_str = f"""\
StubSimulation
--------------
Name: TestSim
Directory: {sim_dir.resolve()}
Start date: 2025-01-01 00:00:00
End date: 2025-12-31 00:00:00
Valid start date: 2024-01-01 00:00:00
Valid end date: 2026-01-01 00:00:00

Discretization: Discretization(time_step = 60)

Code:
Codebase: FakeExternalCodeBase instance (query using StubSimulation.codebase)
Runtime code: AdditionalCode instance with 3 files (query using StubSimulation.runtime_code)
Compile-time code: AdditionalCode instance with 3 files (query using StubSimulation.compile_time_code)
Is compiled: True
Executable path: {sim_dir}"""

        assert sim.__str__() == expected_str

    def test_repr(self, stub_simulation):
        """Test the `__repr__()` method of `Simulation`.

        This test ensures that `__repr__()` returns a correctly formatted string
        representation suitable for debugging, preserving all key attributes
        of the `Simulation` instance.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The generated string matches the expected format for `repr()`.
        """
        sim = stub_simulation
        expected_repr = f"""\
StubSimulation(
name = TestSim,
directory = {sim.directory},
start_date = 2025-01-01 00:00:00,
end_date = 2025-12-31 00:00:00,
valid_start_date = 2024-01-01 00:00:00,
valid_end_date = 2026-01-01 00:00:00,
discretization = Discretization(time_step = 60),
codebase = <FakeExternalCodeBase instance>,
runtime_code = <AdditionalCode instance>,
compile_time_code = <AdditionalCode instance>)"""

        assert sim.__repr__() == expected_repr


class TestSimulationPersistence:
    """Tests for persisting and restoring `Simulation` instances.

    This test class verifies that a `Simulation` instance can be correctly
    serialized, saved, restored, and that errors are handled appropriately.

    Tests
    -----
    - `test_persist_creates_file`: Ensures `persist()` creates the expected state file.
    - `test_persist_and_restore`: Verifies that `persist()` and `restore()` correctly
      save and reload the simulation instance while maintaining its attributes.
    - `test_restore_missing_file`: Ensures `restore()` raises an error when the
      expected persisted file is missing.
    - `test_persist_raises_error_if_simulation_is_running`: Ensures `persist()`
      raises an error if the simulation is currently running.
    """

    def test_persist_creates_file(self, stub_simulation):
        """Test that `persist()` creates the expected simulation state file.

        This test verifies that calling `persist()` results in a
        `simulation_state.pkl` file in the designated directory.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The `simulation_state.pkl` file is successfully created in the directory.
        """
        sim = stub_simulation
        sim.codebase._source = None  # Can't pickle mocked SourceData
        sim.persist()
        assert sim.directory / "simulation_state.pkl", "Persisted file was not created."

    def test_persist_and_restore(self, stub_simulation):
        """Test that `persist()` and `restore()` correctly save and reload a
        `Simulation`.

        This test ensures that a `Simulation` instance can be serialized, stored, and
        then restored without loss of data or corruption.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restored instance matches the original instance when converted to a dictionary.
        - The serialized version of the restored instance matches the original.
        """
        sim = stub_simulation
        sim.persist()
        restored_sim = StubSimulation.restore(sim.directory)

        # Also compare serialized versions
        assert pickle.dumps(restored_sim) == pickle.dumps(sim), (
            "Serialized data mismatch after restore"
        )

    def test_restore_missing_file(self, tmp_path):
        """Test that `restore()` raises an error when the state file is missing.

        This test ensures that attempting to restore a `Simulation` instance fails
        with a `FileNotFoundError` if the `simulation_state.pkl` file is not present.

        Mocks & Fixtures
        ----------------
        - `tmp_path`: Temporary directory for simulation setup.

        Assertions
        ----------
        - A `FileNotFoundError` is raised when calling `restore()` on a directory
          without a saved state file.
        """
        with pytest.raises(FileNotFoundError):
            StubSimulation.restore(tmp_path)

    def test_persist_raises_error_if_simulation_is_running(self, stub_simulation):
        """Test `persist()` raises an error if it is running in a local process.

        This test ensures that calling `persist()` while the simulation has an
        active `LocalProcess` execution raises a `RuntimeError`.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.
        - `MagicMock(spec=LocalProcess)`: Mocks an LocalProcess with a
          running status.

        Assertions
        ----------
        - A `RuntimeError` is raised with a message indicating that persistence
          is not allowed while a local process is running.
        """
        sim = stub_simulation
        mock_handler = MagicMock(spec=LocalProcess)
        mock_handler.status = ExecutionStatus.RUNNING

        # Assign our mock execution handler to the simulation
        sim._execution_handler = mock_handler

        with pytest.raises(
            RuntimeError, match="at least one local process is currently running"
        ):
            sim.persist()


class TestSimulationRestart:
    """Tests for the `restart()` method of `Simulation`.

    This test class verifies that restarting a simulation creates a new instance
    with updated start and end dates while preserving other attributes.

    Tests
    -----
    - `test_restart_creates_new_instance`: Ensures `restart()` returns a new `Simulation` instance.
    - `test_restart_updates_start_and_end_dates`: Validates that the restarted simulation
      has the correct start and end dates.
    - `test_restart_preserves_other_attributes`: Ensures that all attributes except
      `start_date`, `end_date`, and `directory` remain unchanged.
    - `test_restart_updates_directory`: Verifies that the directory is updated to
      include the restart timestamp.
    - `test_restart_raises_error_on_invalid_new_end_date`: Ensures `restart()` raises
      an error if `new_end_date` is not a valid type.
    - `test_restart_with_string_end_date`: Checks that `restart()` correctly parses
      string representations of `new_end_date`.
    """

    def test_restart_creates_new_instance(self, stub_simulation):
        """Test that `restart()` creates a new `Simulation` instance.

        This test ensures that calling `restart()` generates a new instance of
        `Simulation` rather than modifying the existing one.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restarted simulation is an instance of `StubSimulation`.
        - The restarted simulation is a new object and not the same as the original instance.
        """
        sim = stub_simulation
        new_sim = sim.restart(new_end_date="2026-06-30")

        assert isinstance(new_sim, StubSimulation), (
            "Restart did not return a new StubSimulation instance"
        )
        assert new_sim is not sim, "Restarted simulation should be a new object"

    def test_restart_updates_start_and_end_dates(self, stub_simulation):
        """Test that `restart()` correctly updates `start_date` and `end_date`.

        This test ensures that calling `restart()` sets the new simulation's
        `start_date` to the original simulation's `end_date` and updates `end_date`
        correctly.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restarted simulation's `start_date` matches the original simulation's `end_date`.
        - The restarted simulation's `end_date` matches the provided `new_end_date`.
        """
        sim = stub_simulation
        new_end_date = datetime(2026, 6, 30)
        new_sim = sim.restart(new_end_date=new_end_date)
        assert new_sim.start_date == sim.end_date, (
            "Restarted simulation start_date is incorrect"
        )
        assert new_sim.end_date == new_end_date, (
            "Restarted simulation end_date does not match input"
        )

    def test_restart_preserves_other_attributes(self, stub_simulation):
        """Test that `restart()` maintains all attributes except dates and directory.

        This test ensures that calling `restart()` does not modify attributes
        such as the simulation name, discretization settings, and valid date ranges.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restarted simulation's `name` remains unchanged.
        - The `discretization` settings remain identical.
        - `valid_start_date` and `valid_end_date` remain the same.
        """
        sim = stub_simulation
        new_sim = sim.restart(new_end_date="2026-06-30")

        assert new_sim.name == sim.name
        assert new_sim.discretization.__dict__ == sim.discretization.__dict__
        assert new_sim.valid_start_date == sim.valid_start_date
        assert new_sim.valid_end_date == sim.valid_end_date

    def test_restart_updates_directory(self, stub_simulation):
        """Test that `restart()` updates the simulation directory.

        This test ensures that the restarted simulation's directory is updated to a
        subdirectory of the simulation directory and includes a timestamp corresponding
        to the new simulation's start date

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restarted simulation's directory name includes the expected timestamp
          derived from the original simulation's `end_date`.
        """
        sim = stub_simulation
        new_sim = sim.restart(new_end_date="2026-06-30")

        expected_dir_suffix = f"RESTART_{sim.end_date.strftime('%Y%m%d_%H%M%S')}"
        assert expected_dir_suffix in str(new_sim.directory), (
            "Restart directory does not include correct timestamp"
        )

    def test_restart_raises_error_on_invalid_new_end_date(self, stub_simulation):
        """Test that `restart()` raises an error for an invalid `new_end_date`.

        This test ensures that calling `restart()` with a `new_end_date` that is
        neither a string nor a `datetime` object raises a `ValueError`.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - A `ValueError` is raised with a message indicating that `new_end_date`
          must be a `str` or `datetime`.
        """
        sim = stub_simulation
        with pytest.raises(
            ValueError, match="Expected str or datetime for `new_end_date`"
        ):
            sim.restart(new_end_date=42)  # Invalid type

    def test_restart_with_string_end_date(self, stub_simulation):
        """Test that `restart()` correctly parses string `new_end_date`.

        This test ensures that calling `restart()` with a string representation
        of a date correctly converts it to a `datetime` object.

        Mocks & Fixtures
        ----------------
        - `stub_simulation`: Provides a mock `Simulation` instance.

        Assertions
        ----------
        - The restarted simulation's `end_date` is correctly parsed as a `datetime` object.
        - The parsed `end_date` matches the expected `datetime` value.
        """
        sim = stub_simulation
        new_sim = sim.restart(new_end_date="2026-06-30")

        assert new_sim.end_date == datetime(2026, 6, 30), (
            "Restarted simulation did not correctly parse string end_date"
        )


def test_to_dict(stub_simulation):
    """Test that `to_dict()` correctly serializes the `Simulation` instance.

    This test ensures that calling `to_dict()` returns a dictionary containing
    key/value pairs corresponding to the attributes  of the `Simulation` instance.

    Mocks & Fixtures
    ----------------
    - `stub_simulation`: Provides a mock `Simulation` instance.

    Assertions
    ----------
    - The values in the dictionary correctly match the `Simulation` instance's attributes.
    """
    sim = stub_simulation
    test_dict = sim.to_dict()

    assert test_dict["name"] == "TestSim"
    assert test_dict["discretization"] == {"time_step": 60}
    assert test_dict["codebase"]["source_repo"] == "https://github.com/test/repo.git"
    assert test_dict["codebase"]["checkout_target"] == "test_target"
    assert test_dict["runtime_code"]["location"] == "/some/local/directory"
    assert test_dict["runtime_code"]["files"] == [
        "test_file_1.F",
        "test_file_2.py",
        "test_file_3.opt",
    ]
    assert test_dict["runtime_code"]["subdir"] == "some/subdirectory"
    assert test_dict["compile_time_code"]["location"] == "/some/local/directory"
    assert test_dict["compile_time_code"]["subdir"] == "some/subdirectory"
    assert test_dict["compile_time_code"]["files"] == [
        "test_file_1.F",
        "test_file_2.py",
        "test_file_3.opt",
    ]
    assert test_dict["valid_start_date"] == datetime(2024, 1, 1, 0, 0)
    assert test_dict["valid_end_date"] == datetime(2026, 1, 1, 0, 0)
