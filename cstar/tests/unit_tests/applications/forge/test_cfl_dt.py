"""The CFL time step the resolver derives when ``dt`` is not given."""

import math

import pytest
import yaml

from cstar.applications.forge.resolve import cfl_dt
from cstar.applications.forge.util import MAX_DERIVED_DT, compute_timestep_from_cfl
from cstar.base.env import BUNDLED_CATALOG_ROOT

_DOMAIN_SPECS = BUNDLED_CATALOG_ROOT / "specs" / "DomainSpec"


def _cfl_estimate(size_km: float, n: int) -> float:
    """``0.7 * dx / c`` with the helper's default baroclinic phase speed."""
    return 0.7 * size_km / n * 1000.0 / math.sqrt(0.04 * 2000.0)


def _derived(size_km: float, n: int) -> float:
    return compute_timestep_from_cfl(size_km, size_km, n, n, None)


@pytest.mark.parametrize(
    ("size_km", "n"),
    [(12.0, 1), (2688.0, 224), (1.0, 1), (0.01, 1), (8000.0, 20), (4380.0, 208)],
)
def test_never_above_the_estimate_and_divides_an_hour(size_km, n):
    dt = _derived(size_km, n)
    assert isinstance(dt, float)
    assert MAX_DERIVED_DT % dt == 0
    assert dt <= max(_cfl_estimate(size_km, n), 1.0)


def test_rounds_down_rather_than_to_the_nearest():
    """12 km gives an estimate of about 939 s; the old nearest-divisor-of-a-day
    snap returned 960, above it.
    """
    assert _derived(12.0, 1) == 900.0


def test_a_coarse_grid_is_capped_at_an_hour():
    """400 km gives about 31,300 s, which ucla-roms' MARBL step (at most
    10,800 s) cannot take.
    """
    assert _derived(8000.0, 20) == float(MAX_DERIVED_DT) == 3600.0


def test_a_sub_second_estimate_gives_one_second():
    assert _derived(0.01, 1) == 1.0


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("GoA-10th_deg", 450.0),
        ("NEP-4th_deg", 1200.0),
        ("PAC_2fth_deg", 1800.0),
        ("ccs-12km", 900.0),
        ("gulf-guinea-toy", 3600.0),
        ("hvalfjordur-0", 180.0),
        ("wio-toy", 3600.0),
    ],
)
def test_bundled_domains(spec, expected):
    grid_kwargs = yaml.safe_load((_DOMAIN_SPECS / f"{spec}.yaml").read_text())[
        "grid_kwargs"
    ]
    assert cfl_dt(grid_kwargs) == expected
