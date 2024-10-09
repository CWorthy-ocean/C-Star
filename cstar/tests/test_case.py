import pytest
from unittest.mock import Mock
from datetime import datetime
from cstar import Case


class TestWarningsAndErrors:
    """Test whether warnings and errors are raised as expected."""

    @pytest.mark.parametrize(
        "case_params, warning_message",
        [
            ({"valid_end_date": "2023-01-31"}, "Valid start date not provided."),
            ({"valid_start_date": "2023-01-01"}, "Valid end date not provided."),
        ],
    )
    def test_case_init_valid_date_warnings(self, case_params, warning_message):
        """Test if warnings are raised when no valid start or end date is provided."""
        with pytest.warns(RuntimeWarning, match=warning_message):
            case = Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                start_date="2023-01-01",
                end_date="2023-01-31",
                **case_params,
            )
        assert (
            case.valid_start_date is None
            if "start date" in warning_message
            else case.valid_end_date is None
        )

    def test_case_init_start_date_warning(self):
        """Test if a warning is raised when no start_date is provided and defaults to
        valid_start_date."""

        valid_start_date = "2023-01-01"

        with pytest.warns(
            UserWarning,
            match=f"Defaulting to earliest valid start date: {valid_start_date}",
        ):
            case = Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                valid_start_date=valid_start_date,
                end_date="2023-01-31",
                valid_end_date="2023-01-31",
            )
        assert case.start_date == datetime.strptime(valid_start_date, "%Y-%m-%d")

    def test_case_init_end_date_warning(self):
        """Test if a warning is raised when no end_date is provided and defaults to
        valid_end_date."""

        valid_end_date = "2023-01-31"

        with pytest.warns(
            UserWarning, match=f"Defaulting to latest valid end date: {valid_end_date}"
        ):
            case = Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                start_date="2023-01-01",
                valid_start_date="2023-01-01",
                valid_end_date=valid_end_date,
            )
        assert case.end_date == datetime.strptime(valid_end_date, "%Y-%m-%d")

    @pytest.mark.parametrize(
        "case_params, error_message",
        [
            (
                {"end_date": "2023-01-31"},
                "Neither start_date nor valid_start_date provided.",
            ),
            (
                {"start_date": "2023-01-01"},
                "Neither end_date nor valid_end_date provided.",
            ),
        ],
    )
    def test_case_init_missing_dates(self, case_params, error_message):
        """Test if ValueErrors are raised when neither start_date nor end_date is
        provided."""
        with pytest.raises(ValueError, match=error_message):
            Case(
                components=[], name="Test Case", caseroot="test_caseroot", **case_params
            )

    def test_case_invalid_start_date_range(self):
        """Test if a ValueError is raised when start_date is before valid_start_date."""
        with pytest.raises(
            ValueError, match="start_date .* is before the earliest valid start date"
        ):
            Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                start_date="2022-01-01",
                valid_start_date="2023-01-01",
                valid_end_date="2023-01-31",
            )

    def test_case_invalid_end_date_range(self):
        """Test if a ValueError is raised when end_date is after valid_end_date."""
        with pytest.raises(
            ValueError, match="end_date .* is after the latest valid end date"
        ):
            Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                start_date="2023-01-01",
                end_date="2024-01-01",
                valid_end_date="2023-01-31",
            )

    def test_case_start_date_after_end_date(self):
        """Test if a ValueError is raised when start_date is after end_date."""
        with pytest.raises(ValueError, match="start_date .* is after end_date"):
            Case(
                components=[],
                name="Test Case",
                caseroot="test_caseroot",
                start_date="2023-12-31",
                end_date="2023-01-01",
            )


class TestStrRepr:
    """Test string and repr representations of the Case class."""

    @pytest.fixture
    def case_with_mock_component(self):
        """Fixture to create a Case object."""
        mock_component = Mock()
        mock_component.__class__.__name__ = "MockComponent"
        mock_component.__repr__ = lambda self: "MockComponent()"

        return Case(
            components=[mock_component],
            name="TestCase",
            caseroot="/path/to/case",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            valid_start_date=datetime(2023, 1, 1),
            valid_end_date=datetime(2025, 1, 1),
        )

    def test_case_str(self, case_with_mock_component):
        """Test the __str__ method of the Case class."""
        expected_substrings = [
            "C-Star Case",
            "Name: TestCase",
            "caseroot: /path/to/case",
            "start_date: 2024-01-01 00:00:00",
            "end_date: 2024-12-31 00:00:00",
            "Is setup: False",
            "valid_start_date: 2023-01-01 00:00:00",
            "valid_end_date: 2025-01-01 00:00:00",
            "<MockComponent instance>",
        ]

        case_str = str(case_with_mock_component)

        for substring in expected_substrings:
            assert substring in case_str

    def test_case_repr(self, case_with_mock_component):
        """Test the __repr__ method of the Case class."""
        expected_repr = (
            "Case(\n"
            "name = TestCase, \n"
            "caseroot = /path/to/case, \n"
            "start_date = 2024-01-01 00:00:00, \n"
            "end_date = 2024-12-31 00:00:00, \n"
            "valid_start_date = 2023-01-01 00:00:00, \n"
            "valid_end_date = 2025-01-01 00:00:00, \n"
            "components = [\nMockComponent()\n])"
        )

        case_repr = repr(case_with_mock_component)

        assert case_repr == expected_repr
