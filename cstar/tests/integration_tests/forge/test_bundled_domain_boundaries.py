"""Tier 1: no bundled DomainSpec closes an edge that is mostly ocean.

Open-boundary names follow the grid, not the compass: ``east`` is the end of the
xi axis, so on a grid rotated by -90 degrees it is the geographic south edge. Each
test builds a bundled DomainSpec's grid at reduced resolution (same extent, centre
and rotation, at most ``MAX_CELLS`` cells along the longer axis) against ETOPO5,
which roms-tools fetches into its own pooch cache, and fails when an edge the spec
closes is more than ``WET_LIMIT`` ocean.

The wizard derives open boundaries with roms-tools' ``check_and_set_boundaries``,
which opens any edge with at least one ocean cell. That rule does not survive the
reduced resolution (an all-land edge can gain a few wet cells), so this check reads
the same edge cells (roms-tools' ``get_boundary_coords``) and applies a wet-fraction
threshold instead.
"""

from pathlib import Path
from typing import Any

import pytest
import roms_tools as rt
import yaml
from roms_tools.setup.utils import get_boundary_coords

import cstar
from cstar.applications.forge.blueprint import (
    OpenBoundaries,
    TopographySource,
    grid_build_kwargs,
)

DOMAIN_SPEC_DIR = (
    Path(cstar.__file__).parent / "catalog" / "bundled" / "specs" / "DomainSpec"
)
DOMAIN_SPECS = sorted(DOMAIN_SPEC_DIR.glob("*.yaml"))
assert DOMAIN_SPECS, f"no DomainSpec files under {DOMAIN_SPEC_DIR}: the glob is stale"

MAX_CELLS = 48
"""Cells along the longer axis of the reduced grid; a smaller grid keeps its own."""
WET_LIMIT = 0.5
"""The largest ocean fraction a closed edge may have."""


def reduced_grid_kwargs(grid_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Return ``grid_kwargs`` with at most ``MAX_CELLS`` cells along the longer axis.

    The extent, centre, rotation and the ratio of ``nx`` to ``ny`` (so the cell
    shape) are kept. Forge's own keys (``metadata``, ``verbose``) are dropped, as
    Forge drops them (:func:`grid_build_kwargs`).
    """
    kwargs = grid_build_kwargs(grid_kwargs)
    factor = max(kwargs["nx"], kwargs["ny"]) / MAX_CELLS
    if factor > 1:
        kwargs["nx"] = max(2, round(kwargs["nx"] / factor))
        kwargs["ny"] = max(2, round(kwargs["ny"] / factor))
    return kwargs


def closed_wet_edges(spec: dict[str, Any]) -> dict[str, float]:
    """Return the ocean fraction of each edge ``spec`` closes that is over ``WET_LIMIT``."""
    grid = rt.Grid(**reduced_grid_kwargs(spec["grid_kwargs"]), verbose=False)
    edges = get_boundary_coords()["rho"]
    boundaries = OpenBoundaries.model_validate(spec["open_boundaries"]).model_dump()
    wet = {}
    for edge, is_open in boundaries.items():
        if is_open:
            continue
        fraction = float(grid.ds["mask_rho"].isel(edges[edge]).mean())
        if fraction > WET_LIMIT:
            wet[edge] = round(fraction, 3)
    return wet


@pytest.mark.parametrize("path", DOMAIN_SPECS, ids=lambda p: p.stem)
def test_no_closed_edge_is_mostly_ocean(path: Path) -> None:
    """A bundled DomainSpec opens every edge that is more than half ocean."""
    spec = yaml.safe_load(path.read_text())
    topography = spec.get("topography_source", TopographySource.ETOPO5)
    assert topography == TopographySource.ETOPO5 and not spec.get("topography_path"), (
        f"{path.stem} names topography {topography!r} "
        f"(path {spec.get('topography_path')!r}); this check builds grids against "
        "ETOPO5 only, so extend it before bundling such a spec"
    )
    wet = closed_wet_edges(spec)
    assert not wet, (
        f"{path.stem} closes edges that are mostly ocean (ocean fraction {wet}). "
        "Edge names follow the grid's xi/eta axes, not the compass: on a rotated "
        "grid, find the edges that face the sea with the wizard's 'Derive from "
        "grid' or the grid plot."
    )


def test_check_flags_a_closed_ocean_edge() -> None:
    """Closing PAC_2fth_deg's south edge, all open ocean near 5 N, fails."""
    spec = yaml.safe_load((DOMAIN_SPEC_DIR / "PAC_2fth_deg.yaml").read_text())
    spec["open_boundaries"]["south"] = False
    assert set(closed_wet_edges(spec)) == {"south"}
