"""Write the forge blueprint of the CCS parent Mac benchmark.

Ben's California Current parent grid (168 x 336 x 20, ~16 km, rotated 33.3 degrees) with
MARBL BGC and ``dt`` 900 s, split 2 x 2 for a laptop, no nesting, no CDR. The forcing is
what a laptop user can get without staging anything by hand: GLORYS regional (downloaded
by forge from Copernicus Marine, which needs ``copernicusmarine login`` once), ERA5
(streamed), UNIFIED BGC (staged by forge). It is the bundled ``glorys-era5-unified``
ForcingSpec without tides, rivers, MBL_co2 and salinity restoring.

Run from a checkout of C-Star with its environment active::

    python make_forge_blueprint.py

It writes ``forge_parent_mac.yaml`` next to this script and generates no model input.
"""

import copy
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from cstar.applications.forge.blueprint import Composition, SpecRef
from cstar.applications.forge.resolve import build_forge_blueprint
from cstar.catalog.domain_catalog import build_catalog_stack

MODEL = "roms-marbl-0.10-default"  # ucla-roms 0.10.1, MARBL max-it-10, PIO 2.7.1-fork
FORCING = "glorys-era5-unified"
OUTPUT = "daily-restarts"
NAME = "ccs-mac-parent"

# 10 simulated days. The window is also the one the workplan runs; to benchmark a different
# length edit both ``run`` dates here and rerun this script (forge downloads one GLORYS
# file per day, padded by a day each side).
START = datetime(2010, 1, 1)  # blueprint times are naive
END = datetime(2010, 1, 11)
REFERENCE_DATE = datetime(2000, 1, 1)

# Ben's California Current parent (ccs-12km-esper-woa-5yr-nest4km), the same kwargs as
# ../make_forge_blueprints.py.
GRID: dict[str, Any] = {
    "nx": 168,
    "ny": 336,
    "N": 20,
    "size_x": 2688.0,
    "size_y": 5280.0,
    "center_lon": -134.5,
    "center_lat": 39.6,
    "rot": 33.3,
    "theta_s": 6.0,
    "theta_b": 6.0,
    "hc": 250.0,
    "close_narrow_channels": True,
}
OPEN_BOUNDARIES = {"north": True, "south": True, "east": False, "west": True}
PARTITIONING = {"n_procs_x": 2, "n_procs_y": 2}  # 84 x 168 cells per rank, 4 ranks
DT = 900.0  # Ben's value
V_SPONGE = 1600.0  # grid spacing (m) / 10, as ../make_forge_blueprints.py


def forcing(catalog: Any) -> dict[str, Any]:
    """``glorys-era5-unified`` without tides, rivers, MBL_co2 and WOA restoring."""
    spec = copy.deepcopy(catalog.forcing_data(FORCING))
    forcing = spec["forcing"]
    forcing["surface"] = [
        item
        for item in forcing["surface"]
        if item["source"]["name"] in ("ERA5", "UNIFIED")
    ]
    forcing["tidal"] = []
    forcing["river"] = []
    return spec


def main() -> None:
    """Resolve the blueprint and write it."""
    with tempfile.TemporaryDirectory() as scratch:
        # an empty top layer over the bundled catalog: no user catalog is read
        catalog = build_catalog_stack([scratch])
        blueprint = build_forge_blueprint(
            model_dir=catalog.model_dir(MODEL),
            name=NAME,
            description=(
                "CCS parent (168x336x20, ~16 km) for the Mac benchmark: GLORYS regional, "
                "ERA5, UNIFIED BGC, no tides, rivers or nesting; 2x2 ranks"
            ),
            grid_name=NAME,
            grid_kwargs=dict(GRID),
            topography_source="ETOPO5",  # built into roms-tools; Ben's case uses SRTM15
            open_boundaries=OPEN_BOUNDARIES,
            partitioning=dict(PARTITIONING),
            start_date=START,
            end_date=END,
            model_reference_date=REFERENCE_DATE,
            dt=DT,
            v_sponge=V_SPONGE,
            forcing_inputs=forcing(catalog),
            output_settings=catalog.output_data(OUTPUT),
            cdr={"mode": "none"},
            composition=Composition(
                model=SpecRef(name=MODEL, origin="catalog"),
                domain=SpecRef(name="ccs-parent", origin="custom"),
                forcing=SpecRef(name=FORCING, origin="catalog", modified=True),
                output=SpecRef(name=OUTPUT, origin="catalog"),
            ),
        )
    path = blueprint.to_yaml(Path(__file__).parent / "forge_parent_mac.yaml")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
