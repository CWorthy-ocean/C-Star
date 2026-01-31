import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
from roms_tools import Grid, ROMSOutput

from cstar.base.log import get_logger

log = get_logger(__name__)


def perform_analysis(working_dir: Path, grid_path: Path, rst_path: Path) -> None:
    """Perform analysis of Simulation output data.

    Parameters
    ----------
    paths : list[Path]
        The list of all joined output file paths.
    """
    all_paths = ", ".join([working_dir.as_posix(), grid_path.as_posix(), rst_path.as_posix()])
    log.info("Performing analysis of data found at: %s", all_paths)

    ########### START SAM'S HACK BLOCK ##########
    if not grid_path.exists():
        msg = f"Grid file not found at {grid_path}"
        log.error(msg)
        raise RuntimeError(msg)

    # if not rst_path.parent.glob(rst_path.stem):
    #     msg = f"Reset file not found at {rst_path}"
    #     log.error(msg)
    #     raise RuntimeError(msg)

    pattern = r"\._rst.*\.nc"
    rst_wildcard = re.sub(pattern, "_rst.*", rst_path.as_posix())
    output_plot_path = working_dir

    log.info(f"Creating ROMS grid for analysis from: {rst_wildcard}")
    grid = Grid.from_file(grid_path)
    roms_output = ROMSOutput(
        grid=grid,
        path=rst_path,
        use_dask=True,
    )

    temp_ds = roms_output.ds["ALK"].isel({"s_rho": -1, "eta_rho": 11, "xi_rho": 11})
    temp_ds.plot(marker=".")

    alk_path = output_plot_path / "time_ALK.png"
    plt.savefig(str(alk_path))
    log.info(f"Alkalinity over time: {alk_path}")

    alk_surface_path = (output_plot_path / "surface_ALK.png")
    roms_output.plot(
        "ALK",
        time=1,
        s=-1,
        save_path=str(alk_surface_path),
    )
    log.info(f"Surface alkalinity: {alk_surface_path}")

    temp_path = output_plot_path / "surface_temp.png"
    roms_output.plot(
        "temp",
        time=1,
        s=-1,
        save_path=str(temp_path),
    )
    log.info(f"Surface temp: {temp_path}")
    ########### END SAM'S HACK BLOCK ##########


def main() -> None:
    """Entrypoint for executing basic analysis of a Simulation run."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--grid",
        "-g",
        type=str,
        required=True,
        help="Specify the path to simulation grid file",
    )
    parser.add_argument(
        "--reset",
        "-r",
        type=str,
        required=True,
        help="Specify the path to simulation reset files",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Specify the path where outputs will be written",
    )

    ns = parser.parse_args(sys.argv[1:])
    perform_analysis(Path(ns.output), Path(ns.grid), Path(ns.reset))


if __name__ == "__main__":
    main()
