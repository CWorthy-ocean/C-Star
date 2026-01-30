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
    grid_path = next(p for p in paths if "_grid" in p.stem)
    rst_path = next(p for p in paths if "_reset" in p.stem)
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
        save_path=output_plot_path / "surface_ALK.png",
    )
    roms_output.plot(
        "temp",
        time=1,
        s=-1,
        save_path=output_plot_path / "surface_temp.png",
    )
    roms_output.plot("ALK", time=5, lat=27, s=-1)
    roms_output.plot("temp", time=5, lat=27, s=-1)
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
