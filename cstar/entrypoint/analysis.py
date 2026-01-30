import argparse
from pathlib import Path

from cstar.base.log import get_logger

log = get_logger(__name__)


def perform_analysis(paths: list[Path]) -> None:
    """Perform analysis of Simulation output data.

    Parameters
    ----------
    paths : list[Path]
        The list of all joined output file paths.
    """
    all_paths = ", ".join(p.as_posix() for p in paths)
    log.info("Performing analysis of data found at: %s", all_paths)


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
        help="Specify the path to simulation outputs.",
    )
    ns = parser.parse_args()
    paths = [Path(p) for p in ns.paths]

    perform_analysis(paths)


if __name__ == "__main__":
    main()
