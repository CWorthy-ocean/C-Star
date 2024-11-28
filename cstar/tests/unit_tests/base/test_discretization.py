import pytest
from cstar.base.discretization import Discretization


@pytest.fixture
def discretization():
    """Create a Discretization instance with fixed parameters for testing."""
    return Discretization(time_step=3)


def test_init(discretization):
    """Test the attributes were set correctly."""
    assert discretization.time_step == 3
    print(discretization)


def test_str(discretization):
    """Test the string representation is correct."""
    expected_str = """Discretization
--------------
time_step: 3s"""
    assert str(discretization) == expected_str


def test_repr(discretization):
    """Test the repr representation is correct."""
    expected_repr = "Discretization(time_step = 3)"
    assert repr(discretization) == expected_repr
