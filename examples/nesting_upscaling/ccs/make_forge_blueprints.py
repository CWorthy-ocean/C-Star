"""Write the three forge blueprints of the CCS nested + upscaled CDR example.

All three compose the bundled catalog (ModelSpec ``roms-marbl-0.10-default``,
ForcingSpec ``glorys-era5-glodap-woa``, OutputSpec ``daily-restarts``) over Ben's
California Current grid pair (a 168 x 336 x 20 parent with a 70 x 140 x 20 child,
4:1 refinement, both rotated 33.3 degrees), so the nesting and the upscaling can be
exercised with real forcing:

* ``forge_parent.yaml``    -- the parent. Writes the nesting extraction
  (``child_bry``) for the child grid. No CDR.
* ``forge_child.yaml``     -- the child, nested in the parent (boundaries from the
  parent's extraction, initial conditions from ``nest_ic``). One ALK release,
  ``upscale_output`` and ``cdr_output`` on.
* ``forge_parent_up.yaml`` -- the parent grid again, with CDR ``upscaled``: its CDR
  forcing is the child's upscaled tracer tendencies (the workplan supplies the file).

The windows written here are the SMOKE windows (parent 4 days, child and parent_up
the last 2). The workplan overrides them through its runtime variables, so the same
three files serve the smoke and the full run.

Run from a checkout of C-Star with its environment active:

    python make_forge_blueprints.py [--out-dir DIR] [--rivers {default,none}]
                                    [--river-climatology {if_any_missing,never,always}]
                                    [--pressure-fluxes {off,on}]

Two defaults depart from the bundled spec / Ben's file so the example runs (README,
"Known issues"): DAI rivers use ``convert_to_climatology: never`` (``if_any_missing``
makes roms-tools 5.1.1 write a river file ucla-roms cannot read), and the parent does not
extract pressure fluxes (``--pressure-fluxes on`` crashes ucla-roms 0.10.1 with PIO).
"""

import argparse
import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cstar.applications.forge.blueprint import Composition, SpecRef
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import default_catalog_stack

MODEL = "roms-marbl-0.10-default"  # ucla-roms 0.10.1, MARBL max-it-10, PIO 2.7.1-fork
FORCING = "glorys-era5-glodap-woa"  # GLORYS + ERA5 + GLODAP/WOA_BGC/UNIFIED BGC + ...
OUTPUT = "daily-restarts"
PARENT, CHILD, PARENT_UP = "ccs-parent", "ccs-child", "ccs-parent-up"

# One reference date for all three: ``nest_ic`` and ``continue-from`` read the parent
# restart's time axis, so the child and parent_up must count from the same origin.
REFERENCE_DATE = datetime(2000, 1, 1)
# Smoke windows (the workplan's runtime variables replace them for a full run). Any dates
# work with ``convert_to_climatology: never`` rivers; with ``if_any_missing`` (the bundled
# spec) the window would have to start on a 15th and still could not be run (README).
PARENT_START = datetime(2010, 1, 15)
PARENT_END = datetime(2010, 1, 19)
CHILD_START = datetime(2010, 1, 17)
CHILD_END = PARENT_END
PULSE_DAYS = 30  # the release runs this long, but never past the child window

# Ben's California Current parent (ccs-12km-esper-woa-5yr-nest4km): 16 km x 15.7 km.
PARENT_GRID: dict[str, Any] = dict(
    nx=168,
    ny=336,
    N=20,
    size_x=2688.0,
    size_y=5280.0,
    center_lon=-134.5,
    center_lat=39.6,
    rot=33.3,
    theta_s=6.0,
    theta_b=6.0,
    hc=250.0,
    close_narrow_channels=True,
)
# Ben's child (3.93 km x 4.0 km, 4:1). His file stores center_lon in the 0-360 form
# (237.494); it is the same point as -122.506. Normalised here to the convention the
# parent and the CDR release use (roms-tools wraps either form to [0, 360], so this is
# cosmetic, see README "Known issues").
CHILD_GRID: dict[str, Any] = dict(
    nx=70,
    ny=140,
    N=20,
    size_x=280.0,
    size_y=550.0,
    center_lon=round(237.494 - 360.0, 3),
    center_lat=36.629,
    rot=33.3,
    theta_s=6.0,
    theta_b=6.0,
    hc=250.0,
)
OPEN_BOUNDARIES = dict(north=True, south=True, east=False, west=True)
TOPOGRAPHY = "SRTM15"

# dt (s). Parent: Ben's value (the forge CFL rule would give ~1200). Child: forge's own
# CFL rule gives 300 s at 3.93 km; 240 s is used so that the barotropic Courant number
# (dt/ndtfast * sqrt(g h_max) * sqrt(1/dx^2 + 1/dy^2), ndtfast 60, h_max ~4500 m) is
# the parent's 0.28 instead of 0.35. All divide 3600 and 86400.
PARENT_DT, CHILD_DT = 900.0, 240.0
PARENT_V_SPONGE, CHILD_V_SPONGE = 1600.0, 400.0  # grid spacing (m) / 10
PARENT_PART = dict(n_procs_x=4, n_procs_y=4)  # Ben's 16 cores; set again at launch
CHILD_PART = dict(n_procs_x=2, n_procs_y=4)  # 70 and 140 must divide: 35 x 35 tiles
NESTING_PERIOD = 3600.0  # child boundary data every hour (Ben's metadata_child)

# CDR placeholder (see README): ALK tracer_perturbation near-shore Monterey Bay.
# roms-tools reads ALK ``tracer_fluxes`` in meq/s (not mol/s): 1.0e7 meq/s = 1e4 mol/s.
RELEASE_ALK_FLUX = 1.0e7  # meq/s while releasing (= 1e4 mol/s)
RELEASE: dict[str, Any] = dict(
    name="monterey_alk",
    lat=36.8,
    lon=-122.0,
    depth=2.0,
    hsc=4000.0,
    vsc=10.0,
    release_type="tracer_perturbation",
    tracer_fluxes={"ALK": [RELEASE_ALK_FLUX, 0.0]},  # meq/s on, then off (step-like)
)

catalog = default_catalog_stack()
out_dir = Path(__file__).parent
model_dir = catalog.model_dir(MODEL)
domain_name = "ccs-nest"  # recorded in composition; the grids are Ben's, not catalog's


RIVERS = "default"
RIVER_CLIMATOLOGY: str | None = "never"
PRESSURE_FLUXES = False


def forcing(*, child: bool) -> dict[str, Any]:
    """The bundled forcing; a child takes its state from the parent, so no IC/boundary.

    The resolver already skips ``boundary`` for a nested child; the initial
    conditions it would otherwise build (GLORYS, unused: ``nest_ic`` supplies them)
    are dropped here so forge_child stages no GLORYS at all.
    """
    data = copy.deepcopy(catalog.forcing_data(FORCING))
    if child:
        data.pop("initial_conditions", None)
        data["forcing"].pop("boundary", None)
    if RIVERS == "none":
        data["forcing"].pop("river", None)
    elif RIVER_CLIMATOLOGY is not None:
        for item in data["forcing"].get("river", []):
            item["convert_to_climatology"] = RIVER_CLIMATOLOGY
    return data


def forcing_overrides() -> dict[str, Any]:
    """The forcing edits made in :func:`forcing`, as a sparse record for ``composition``."""
    if RIVERS == "none":
        return {"river": "removed"}
    if RIVER_CLIMATOLOGY is not None:
        return {"river": {"convert_to_climatology": RIVER_CLIMATOLOGY}}
    return {}


def release_schedule() -> dict[str, Any]:
    """The roms-tools ``CDRForcing`` kwargs (``cdr.mode: yaml``): one ALK release."""
    pulse_end = min(CHILD_START + timedelta(days=PULSE_DAYS), CHILD_END)
    release = dict(RELEASE, times=[CHILD_START.isoformat(), pulse_end.isoformat()])
    return {
        "start_time": CHILD_START.isoformat(),
        "end_time": CHILD_END.isoformat(),
        "releases": [release],
    }


def build(
    name: str,
    description: str,
    *,
    grid: dict[str, Any],
    partitioning: dict[str, int],
    dt: float,
    v_sponge: float,
    start: datetime,
    end: datetime,
    child: bool = False,
    nests_child: bool = False,
    cdr: dict[str, Any] | None = None,
    output_overrides: dict[str, Any] | None = None,
    run_time_overrides: dict[str, Any] | None = None,
    cdr_note: str | None = None,
) -> Path:
    output = catalog.output_data(OUTPUT)
    for section, values in (output_overrides or {}).items():
        output[section] = {**output[section], **values}
    kwargs: dict[str, Any] = {}
    if child:
        kwargs["grid_kwargs_parent"] = dict(PARENT_GRID)
    if nests_child:
        kwargs["grid_kwargs_child"] = dict(CHILD_GRID)
        kwargs["metadata_child"] = {"period": NESTING_PERIOD}
        kwargs["nesting_include_pressure_fluxes"] = PRESSURE_FLUXES
    blueprint = build_forge_blueprint(
        model_dir=model_dir,
        name=name,
        description=description,
        grid_name=name,
        grid_kwargs=dict(grid),
        topography_source=TOPOGRAPHY,
        open_boundaries=OPEN_BOUNDARIES,
        partitioning=dict(partitioning),
        start_date=start,
        end_date=end,
        model_reference_date=REFERENCE_DATE,
        dt=dt,
        v_sponge=v_sponge,
        forcing_inputs=forcing(child=child),
        output_settings=output,
        cdr=cdr or {"mode": "none"},
        run_time_overrides=run_time_overrides,
        composition=Composition(
            model=SpecRef(name=MODEL, origin="catalog"),
            domain=SpecRef(name=domain_name, origin="custom"),
            forcing=SpecRef(
                name=FORCING,
                origin="catalog",
                modified=child or RIVERS == "none" or RIVER_CLIMATOLOGY is not None,
            ),
            cdr=SpecRef(name=cdr_note or "", origin="custom"),
            output=SpecRef(
                name=OUTPUT, origin="catalog", modified=bool(output_overrides)
            ),
            overrides={
                **(output_overrides or {}),
                **(run_time_overrides or {}),
                **forcing_overrides(),
            },
        ),
        **kwargs,
    )
    blueprint = blueprint.stamp_provenance(tool=Path(__file__).name)
    return blueprint.to_yaml(
        out_dir / f"forge_{name.removeprefix('ccs-').replace('-', '_')}.yaml"
    )


def main() -> None:
    global out_dir, RIVERS, RIVER_CLIMATOLOGY, PRESSURE_FLUXES
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out-dir", type=Path, default=out_dir)
    parser.add_argument("--rivers", choices=["default", "none"], default="default")
    parser.add_argument(
        "--river-climatology",
        choices=["if_any_missing", "never", "always"],
        default=RIVER_CLIMATOLOGY,
    )
    parser.add_argument("--pressure-fluxes", choices=["off", "on"], default="off")
    args = parser.parse_args()
    out_dir, RIVERS, RIVER_CLIMATOLOGY = (
        args.out_dir,
        args.rivers,
        args.river_climatology,
    )
    PRESSURE_FLUXES = args.pressure_fluxes == "on"
    out_dir.mkdir(parents=True, exist_ok=True)
    written = [
        build(
            PARENT,
            "CCS parent (168x336x20, ~16 km): writes the nesting extraction for the child",
            grid=PARENT_GRID,
            partitioning=PARENT_PART,
            dt=PARENT_DT,
            v_sponge=PARENT_V_SPONGE,
            start=PARENT_START,
            end=PARENT_END,
            nests_child=True,
        ),
        build(
            CHILD,
            "CCS child (70x140x20, ~4 km) nested in ccs-parent: one ALK release; "
            "writes the upscaling output",
            grid=CHILD_GRID,
            partitioning=CHILD_PART,
            dt=CHILD_DT,
            v_sponge=CHILD_V_SPONGE,
            start=CHILD_START,
            end=CHILD_END,
            child=True,
            cdr={"mode": "yaml", "cdr_forcing": release_schedule()},
            cdr_note="ccs-monterey-alk-placeholder",
            output_overrides={
                "upscale_output": {"do_upscale": True},
                "cdr_output": {"do_cdr_output": True},
            },
        ),
        build(
            PARENT_UP,
            "CCS parent re-run with the child's upscaled CDR tracers as forcing",
            grid=PARENT_GRID,
            partitioning=PARENT_PART,
            dt=PARENT_DT,
            v_sponge=PARENT_V_SPONGE,
            start=CHILD_START,
            end=CHILD_END,
            cdr={"mode": "upscaled"},
            output_overrides={"cdr_output": {"do_cdr_output": True}},
            # the upscaled depth profiles carry the child's levels (not the default 50),
            # and some of them land on parent land cells: the resolver's upscaled mode
            # sets relocate_to_wet_pts false, with which ucla-roms stops ("CDR release
            # N is on land", 51 of them in the smoke run); true is the original's value
            run_time_overrides={
                "cdr_frc": {"nz_chd": CHILD_GRID["N"], "relocate_to_wet_pts": True}
            },
        ),
    ]
    for path in written:
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
