import pytest
from cstar.roms.discretization import ROMSDiscretization


@pytest.fixture
def roms_discretization():
    """Create a ROMSDiscretization instance with fixed parameters for testing."""
    return ROMSDiscretization(time_step=3, n_procs_x=2, n_procs_y=123)


def test_init(roms_discretization):
    """Test the attributes were set correctly."""
    assert roms_discretization.time_step == 3
    assert roms_discretization.n_procs_x == 2
    assert roms_discretization.n_procs_y == 123


def test_defaults():
    """Test defaults are set correctly when not provided."""
    roms_discretization = ROMSDiscretization(time_step=3)
    assert roms_discretization.n_procs_x == 1
    assert roms_discretization.n_procs_y == 1


def test_n_procs_tot(roms_discretization):
    """Test the n_procs_tot property correctly multiplies n_procs_x and n_procs_y."""
    assert roms_discretization.n_procs_tot == 2 * 123


def test_repr(roms_discretization):
    """Test the repr representation is correct."""
    expected_repr = "ROMSDiscretization(time_step = 3, n_procs_x = 2, n_procs_y = 123)"
    assert repr(roms_discretization) == expected_repr


def test_str(roms_discretization):
    """Test the string representation is correct."""
    expected_str = """ROMSDiscretization
------------------
time_step: 3s
n_procs_x: 2 (Number of x-direction processors)
n_procs_y: 123 (Number of y-direction processors)"""

    assert expected_str == str(roms_discretization)
