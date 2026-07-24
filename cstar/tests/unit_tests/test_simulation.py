import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from cstar.base.discretization import Discretization
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
