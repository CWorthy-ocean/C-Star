import pytest

from cstar.roms.discretization import ROMSDiscretization


@pytest.fixture
def roms_discretization():
    """Create a ROMSDiscretization instance with fixed parameters for testing."""
    return ROMSDiscretization(n_procs_x=2, n_procs_y=123)


def test_init(roms_discretization):
    """Test the attributes were set correctly."""
    assert roms_discretization.n_procs_x == 2
    assert roms_discretization.n_procs_y == 123
    assert roms_discretization.n_cores is None


def test_defaults():
    """Test defaults are set correctly when not provided."""
    roms_discretization = ROMSDiscretization()
    assert roms_discretization.n_procs_x is None
    assert roms_discretization.n_procs_y is None
    assert roms_discretization.n_cores is None


class TestNProcsTot:
    """Tests for the `n_procs_tot` property."""

    def test_returns_n_cores_when_set(self):
        """`n_cores` takes precedence over n_procs_x * n_procs_y when set."""
        discretization = ROMSDiscretization(n_procs_x=2, n_procs_y=3, n_cores=100)
        assert discretization.n_procs_tot == 100

    def test_returns_product_when_procs_set(self, roms_discretization):
        """Falls back to n_procs_x * n_procs_y when n_cores is not set."""
        assert roms_discretization.n_procs_tot == 2 * 123

    @pytest.mark.parametrize(
        "n_procs_x, n_procs_y",
        [(None, None), (2, None), (None, 3)],
    )
    def test_returns_none_when_insufficient_info(self, n_procs_x, n_procs_y):
        """Returns None when neither n_cores nor both procs axes are set."""
        discretization = ROMSDiscretization(n_procs_x=n_procs_x, n_procs_y=n_procs_y)
        assert discretization.n_procs_tot is None


def test_str(roms_discretization):
    """Test the string representation only includes set attributes."""
    expected_str = """ROMSDiscretization
------------------
n_procs_x: 2
n_procs_y: 123"""
    assert str(roms_discretization) == expected_str


def test_repr(roms_discretization):
    """Test the repr representation only includes set attributes."""
    expected_repr = "ROMSDiscretization(n_procs_x = 2, n_procs_y = 123)"
    assert repr(roms_discretization) == expected_repr


def test_str_and_repr_with_n_cores():
    """n_cores appears in __str__/__repr__ when set."""
    discretization = ROMSDiscretization(n_cores=42)
    assert "n_cores: 42" in str(discretization)
    assert repr(discretization) == "ROMSDiscretization(n_cores = 42)"
