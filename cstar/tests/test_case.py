import pytest
import warnings
from pathlib import Path
from datetime import datetime
from cstar import Case

class TestWarningsAndErrors:
    """Test whether warnings and errors are raised as expected."""

    def test_case_init_valid_start_date_warning(self):
        """Test if a warning is raised when no valid_start_date is provided."""
        with pytest.warns(RuntimeWarning, match="Valid start date not provided."):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot",
                start_date="2023-01-01",
                end_date="2023-01-31",
                valid_end_date="2023-01-31"
            )
        assert case.valid_start_date is None
    
    def test_case_init_valid_end_date_warning(self):
        """Test if a warning is raised when no valid_end_date is provided."""
        with pytest.warns(RuntimeWarning, match="Valid end date not provided."):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2023-01-01",
                end_date="2023-01-31",
                valid_start_date="2023-01-01"
            )
        assert case.valid_end_date is None
    
    def test_case_init_start_date_warning(self):
        """Test if a warning is raised when no start_date is provided and defaults to valid_start_date."""

        valid_start_date="2023-01-01"

        with pytest.warns(UserWarning, match=f"Defaulting to earliest valid start date: {valid_start_date}"):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                valid_start_date=valid_start_date,
                end_date="2023-01-31",
                valid_end_date="2023-01-31"
            )
        assert case.start_date == datetime.strptime(valid_start_date, "%Y-%m-%d")
    
    def test_case_init_end_date_warning(self):
        """Test if a warning is raised when no end_date is provided and defaults to valid_end_date."""
        
        valid_end_date="2023-01-31"

        with pytest.warns(UserWarning, match=f"Defaulting to latest valid end date: {valid_end_date}"):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2023-01-01",
                valid_start_date="2023-01-01",
                valid_end_date=valid_end_date
            )
        assert case.end_date == datetime.strptime(valid_end_date, "%Y-%m-%d")
    
    def test_case_init_start_date_error(self):
        """Test if a ValueError is raised when neither start_date nor valid_start_date is provided."""
        with pytest.raises(ValueError, match="Neither start_date nor valid_start_date provided."):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot",
                end_date="2023-01-31"
            )
    
    def test_case_init_end_date_error(self):
        """Test if a ValueError is raised when neither end_date nor valid_end_date is provided."""
        with pytest.raises(ValueError, match="Neither end_date nor valid_end_date provided."):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2023-01-01"
            )
    
    def test_case_invalid_date_range_start(self):
        """Test if a ValueError is raised when start_date is before valid_start_date."""
        with pytest.raises(ValueError, match="start_date .* is before the earliest valid start date"):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2022-01-01", 
                valid_start_date="2023-01-01",
                valid_end_date="2023-01-31"
            )
    
    def test_case_invalid_date_range_end(self):
        """Test if a ValueError is raised when end_date is after valid_end_date."""
        with pytest.raises(ValueError, match="end_date .* is after the latest valid end date"):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2023-01-01", 
                end_date="2024-01-01", 
                valid_end_date="2023-01-31"
            )
    
    def test_case_start_date_after_end_date(self):
        """Test if a ValueError is raised when start_date is after end_date."""
        with pytest.raises(ValueError, match="start_date .* is after end_date"):
            case = Case(
                components=[], 
                name="Test Case", 
                caseroot="test_caseroot", 
                start_date="2023-12-31", 
                end_date="2023-01-01"
            )
    
        
