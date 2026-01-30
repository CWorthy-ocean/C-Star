import argparse
from pathlib import Path

from roms_tools import Grid, ROMSOutput

from cstar.base.log import get_logger

log = get_logger(__name__)


def perform_analysis(working_dir: Path, paths: list[Path]) -> None:
    """Perform analysis of Simulation output data.

    Parameters
    ----------
    paths : list[Path]
        The list of all joined output file paths.
    """
    all_paths = ", ".join(p.as_posix() for p in paths)
    log.info("Performing analysis of data found at: %s", all_paths)

    ########### START SAM'S HACK BLOCK ##########
    grid_path = next((p for p in paths if "grid_" in p.name), None)
    rst_path = next((p for p in paths if "_rst" in p.name), None)

    if not grid_path:
        msg = f"No grid file found in {all_paths}"
        log.error(msg)
        raise RuntimeError(msg)

    if not rst_path:
        msg = f"No reset file found in {all_paths}"
        log.error(msg)
        raise RuntimeError(msg)

    if not grid_path.exists():
        msg = f"Grid file not found at {grid_path}"
        log.error(msg)
        raise RuntimeError(msg)

    if not rst_path.exists():
        msg = f"Grid file not found at {rst_path}"
        log.error(msg)
        raise RuntimeError(msg)

    output_plot_path = working_dir

    grid = Grid.from_file(grid_path)
    roms_output = ROMSOutput(
        grid=grid,
        path=rst_path,
        use_dask=True,
    )

    roms_output.plot(
        "ALK",
        time=1,
        s=-1,
        save_path=(output_plot_path / "surface_ALK.png").as_posix(),
    )
    roms_output.plot(
        "temp",
        time=1,
        s=-1,
        save_path=(output_plot_path / "surface_temp.png").as_posix(),
    )
    roms_output.plot(
        "ALK",
        time=5,
        lat=27,
        s=-1,
        save_path=(output_plot_path / "surf_lat_ALK.png").as_posix(),
    )
    roms_output.plot(
        "temp",
        time=5,
        lat=27,
        s=-1,
        save_path=(output_plot_path / "surf_lat_temp.png").as_posix(),
    )
    ########### END SAM'S HACK BLOCK ##########


def main() -> None:
    """Entrypoint for executing basic analysis of a Simulation run."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        "-p",
        type=str,
        required=True,
        action="append",
        dest="paths",
        help="Specify the path to simulation outputs",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Specify the path where outputs will be written",
    )

    ns = parser.parse_args()
    paths = [Path(p) for p in ns.paths]
    working_dir = Path(ns.output)

    perform_analysis(working_dir, paths)


if __name__ == "__main__":
    main()
