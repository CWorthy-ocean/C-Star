"""Regenerate the three forge blueprints of the tiny nesting + upscaling example.

The committed ``forge_parent.yaml``, ``forge_child.yaml`` and ``forge_parent_up.yaml``
are the example's artifact; this script only records how they were made, so the numbers
below (grids, windows, release) can be changed and the files rewritten::

    python make_forge_blueprints.py

It resolves the bundled ModelSpec, ForcingSpec and OutputSpec into blueprints, exactly as
the wizard does. It generates no model input: grids, forcing and everything else are
built later by the forge steps of ``workplan.yaml``. Nothing is downloaded either.

Layout (all times are model-clock UTC, ``model_reference_date`` 2000-01-01):

    parent     24 x 24 cells, 400 km, centred at (-7.5, 60.0); runs 2012-01-01T12 -> 01-03T07
               and extracts hourly boundary data for the child (``grid_kwargs_child``)
    child      24 x 24 cells, 100 km, centred at (-6.5, 60.4), nested in the parent; runs
               2012-01-02T06 -> 01-03T06 with one ALK release near its east boundary
    parent_up  the parent grid again, no child; reruns the child's window (from the
               parent's restart at the child's start) with the child's boundary flux
               (upscaled CDR) as its CDR forcing
"""

from __future__ import annotations

import copy
import math
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cstar.applications.forge.blueprint import Composition, SpecRef
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import build_catalog_stack

HERE = Path(__file__).parent

MODEL = "roms-marbl-0.10-default"  # pins ucla-roms 0.10.1; ROMS_COMMIT replaces that
# ucla-roms main after #390 (CDR_LITE upscaling), which conserves the flux of the
# upscaled CDR depth profiles parent_up reads. Switch to 0.10.2 once it is released.
ROMS_COMMIT = "453189ed372d02c5515c882722c9d98e0bd8ac9d"
FORCING = "glorys-era5-unified"
OUTPUT = "daily-restarts"

# ---- source files ------------------------------------------------------------------
# The blueprints carry placeholder paths. A source with an explicit path is not staged by
# forge (no copernicusmarine / Google Drive download), and workplan.yaml replaces each
# placeholder with the real file through ``blueprint_overrides``.
DATA = "{{example_data}}"
GLORYS = f"{DATA}/GLORYS_NA_2012.nc"
ERA5 = f"{DATA}/ERA5_NA_2012.nc"
UNIFIED = f"{DATA}/coarsened_UNIFIED_bgc_dataset_v2_1.nc"

# ---- windows -----------------------------------------------------------------------
MODEL_REFERENCE_DATE = datetime(2000, 1, 1)  # noqa: DTZ001 (blueprint times are naive)
PARENT_START = datetime(2012, 1, 1, 12)  # noqa: DTZ001  # the first GLORYS record of the test data
CHILD_START = PARENT_START + timedelta(hours=18)  # a parent restart time
CHILD_END = CHILD_START + timedelta(hours=24)
PARENT_END = CHILD_END + timedelta(hours=1)  # the child needs boundary data to its end

RESTART_PERIOD = 6 * 3600.0  # s; CHILD_START falls exactly on a restart
EXTRACT_PERIOD = 3600.0  # s; the parent's boundary extraction for the child

# ---- grids -------------------------------------------------------------------------
N_LEVELS = 10
GRID_COMMON = {
    "N": N_LEVELS,
    "rot": 0.0,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 300.0,
    "hmin": 5.0,
}
PARENT_GRID = {
    "nx": 24,
    "ny": 24,
    "size_x": 400.0,
    "size_y": 400.0,
    "center_lon": -7.5,
    "center_lat": 60.0,
    **GRID_COMMON,
}
CHILD_GRID = {
    "nx": 24,
    "ny": 24,
    "size_x": 100.0,
    "size_y": 100.0,
    "center_lon": -6.5,
    "center_lat": 60.4,
    **GRID_COMMON,
}
OPEN_BOUNDARIES = {"north": True, "south": True, "east": True, "west": True}
PARTITIONING = {"n_procs_x": 2, "n_procs_y": 2}

# dt is chosen far under the CFL limit: 16.7 km / 4.2 km cells, depths to ~3 km
# (barotropic wave speed ~170 m/s with ndtfast 60 -> 1 s / 0.5 s fast steps).
DT_PARENT = 60.0
DT_CHILD = 30.0

# ---- the CDR release (child) -------------------------------------------------------
# One ALK release 2.5 child cells inside the east boundary, on the mean eastward flow
# (GLORYS surface u ~ 0.1-0.2 m/s here), so the added alkalinity reaches the boundary
# and the upscaled flux is non-zero. Released for the first 4 h of the child run.
RELEASE_CELLS_INSIDE = 2.5
RELEASE_HOURS = 4
RELEASE_ALK_FLUX = 2.0e6  # meq/s while releasing
RELEASE_DEPTH = 10.0  # m
RELEASE_HSC = 2500.0  # m, horizontal Gaussian scale
RELEASE_VSC = 15.0  # m, vertical Gaussian scale


def _east_release_position() -> tuple[float, float]:
    """The (lon, lat) of the release: 2.5 child cells inside the east boundary.

    Plain spherical-earth arithmetic; the child grid is only 100 km wide, so the
    distortion of roms-tools' projection is negligible at this precision.
    """
    lat = CHILD_GRID["center_lat"]
    km_per_deg_lon = 111.195 * math.cos(math.radians(lat))
    cell_km = CHILD_GRID["size_x"] / CHILD_GRID["nx"]
    km_inside = CHILD_GRID["size_x"] / 2 - RELEASE_CELLS_INSIDE * cell_km
    return round(CHILD_GRID["center_lon"] + km_inside / km_per_deg_lon, 3), lat


def _iso(when: datetime) -> str:
    return when.isoformat()


def _forcing(*, child: bool, catalog: Any) -> dict[str, Any]:
    """The bundled ``glorys-era5-unified`` ForcingSpec trimmed to this example.

    Kept: GLORYS (regional layout) initial conditions and boundaries, UNIFIED BGC
    initial conditions, ERA5 physics surface forcing and UNIFIED BGC surface forcing.
    Dropped: tides, rivers, the MBL_co2 surface source and WOA salinity restoring.
    Every source carries a placeholder path (see ``DATA``).

    A child domain takes its initial state and its boundaries from the parent run
    (``continue-from`` / ``nest-from``), so it gets neither here.
    """
    spec = copy.deepcopy(catalog.forcing_data(FORCING))
    forcing = spec["forcing"]
    forcing["surface"] = [
        item
        for item in forcing["surface"]
        if item["source"]["name"] in ("ERA5", "UNIFIED")
    ]
    forcing["tidal"] = []
    forcing["river"] = []

    def place(source: dict[str, Any], path: str) -> None:
        source["path"] = path

    for item in forcing["surface"]:
        place(item["source"], ERA5 if item["source"]["name"] == "ERA5" else UNIFIED)
    for section in (spec["initial_conditions"], forcing["boundary"]):
        place(section["source"], GLORYS)
        for bgc in section["bgc_sources"]:
            place(bgc["source"], UNIFIED)

    if child:
        spec.pop("initial_conditions")
        forcing["boundary"] = None
    return spec


def _composition(forcing_modified: bool = True) -> Composition:
    return Composition(
        model=SpecRef(name=MODEL, origin="catalog", modified=True),
        forcing=SpecRef(name=FORCING, origin="catalog", modified=forcing_modified),
        output=SpecRef(name=OUTPUT, origin="catalog", modified=True),
        overrides={"code": {"roms": {"commit": ROMS_COMMIT}}},
    )


def _release() -> dict[str, Any]:
    lon, lat = _east_release_position()
    times = [CHILD_START, CHILD_START + timedelta(hours=RELEASE_HOURS), CHILD_END]
    return {
        "start_time": _iso(CHILD_START),
        "end_time": _iso(CHILD_END),
        "model_reference_date": _iso(MODEL_REFERENCE_DATE),
        "releases": [
            {
                "name": "oae_east",
                "lat": lat,
                "lon": lon,
                "depth": RELEASE_DEPTH,
                "hsc": RELEASE_HSC,
                "vsc": RELEASE_VSC,
                "times": [_iso(t) for t in times],
                "time_interpolation": False,
                "release_type": "tracer_perturbation",
                "tracer_fluxes": {"ALK": [RELEASE_ALK_FLUX, 0.0, 0.0]},
            }
        ],
    }


# Run-time namelist overrides. Every enabled output stream's rollover (nrpf x period)
# must divide the restart period (ucla-roms precheck), hence 6 hourly records per file.
def _streams(
    *, extract: bool = False, cdr: bool = False, upscale: bool = False
) -> dict:
    overrides: dict[str, Any] = {
        "ocean_vars": {"output_period_rst": RESTART_PERIOD, "nrpf_rst": 1},
    }
    if extract:
        overrides["extract_data"] = {"nrpf": 6}
    if cdr:
        overrides["cdr_output"] = {
            "do_cdr_output": True,
            "output_period": 3600,
            "nrpf": 6,
        }
    if upscale:
        overrides["upscale_output"] = {
            "do_upscale": True,
            "output_period_uscl": 3600,
            "nrpf_uscl": 6,
        }
    return overrides


def main() -> None:
    """Write the three blueprints."""
    specs = {
        "forge_parent": {
            "name": "nest-tiny-parent",
            "description": "Tiny nesting example, parent: 24x24 cells over 400 km west of Scotland; extracts boundary data for the child",
            "grid_name": "tiny-parent",
            "grid_kwargs": PARENT_GRID,
            "grid_kwargs_child": CHILD_GRID,
            "metadata_child": {"period": EXTRACT_PERIOD},
            "start_date": PARENT_START,
            "end_date": PARENT_END,
            "dt": DT_PARENT,
            "cdr": {"mode": "none"},
            "run_time_overrides": _streams(extract=True),
        },
        "forge_child": {
            "name": "nest-tiny-child",
            "description": "Tiny nesting example, child: 24x24 cells over 100 km nested in the parent; one ALK release; writes upscale output",
            "grid_name": "tiny-child",
            "grid_kwargs": CHILD_GRID,
            "grid_kwargs_parent": PARENT_GRID,
            "start_date": CHILD_START,
            "end_date": CHILD_END,
            "dt": DT_CHILD,
            "cdr": {"mode": "yaml", "cdr_forcing": _release()},
            "run_time_overrides": _streams(cdr=True, upscale=True),
        },
        "forge_parent_up": {
            "name": "nest-tiny-parent-up",
            "description": "Tiny nesting example, parent rerun: the parent grid with the child's boundary flux as its CDR forcing (upscaled CDR)",
            "grid_name": "tiny-parent-up",
            "grid_kwargs": PARENT_GRID,
            # Forge window = the parent's, not the child's: forge builds an initial
            # condition for every non-nested domain, and roms-tools needs a GLORYS
            # record at (or within 24 h after) its start, which exists only at
            # PARENT_START. The run itself starts at the child's start, because
            # ``continue-from`` takes the start date from the restart file.
            "start_date": PARENT_START,
            "end_date": CHILD_END,
            "dt": DT_PARENT,
            "cdr": {"mode": "upscaled"},
            # the upscaled flux profiles have the child's vertical levels
            "run_time_overrides": {
                **_streams(cdr=True),
                "cdr_frc": {"nz_chd": N_LEVELS},
            },
        },
    }
    for stem, spec in specs.items():
        child = "grid_kwargs_parent" in spec
        with tempfile.TemporaryDirectory() as scratch:
            # an empty top layer over the bundled catalog: no user catalog is read
            catalog = build_catalog_stack([scratch])
            cfg = build_forge_blueprint(
                model_dir=catalog.model_dir(MODEL),
                open_boundaries=OPEN_BOUNDARIES,
                partitioning=PARTITIONING,
                model_reference_date=MODEL_REFERENCE_DATE,
                roms_ref=ROMS_COMMIT,
                forcing_inputs=_forcing(child=child, catalog=catalog),
                output_settings=catalog.output_data(OUTPUT),
                composition=_composition(),
                **spec,
            )
        print(f"wrote {cfg.to_yaml(HERE / f'{stem}.yaml')}")


if __name__ == "__main__":
    main()
