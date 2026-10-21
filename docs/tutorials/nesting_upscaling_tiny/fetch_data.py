"""Fetch the pinned source data the tiny nesting + upscaling example reads.

Downloads (or finds in the cache) the three files the example's forge blueprints name,
through C-Star's integration-test data registry, which pins the roms-tools-test-data
commit and the file checksums. Prints the directory that holds them, so the workplan
can take it as a runtime variable::

    cstar workplan run docs/tutorials/nesting_upscaling_tiny/workplan.yaml --var example_data=$(python docs/tutorials/nesting_upscaling_tiny/fetch_data.py)

This script only downloads; it builds nothing. Progress and errors go to stderr so
stdout stays exactly the directory, safe to use in ``$(...)``.
"""

import sys

FILES = (
    "GLORYS_NA_2012.nc",
    "ERA5_NA_2012.nc",
    "coarsened_UNIFIED_bgc_dataset_v2_1.nc",
)
"""The registry files the example reads: ocean state (GLORYS), surface forcing (ERA5)
and the global 2 degree biogeochemical climatology (UNIFIED)."""


def main() -> int:
    """Fetch ``FILES`` and print the directory that holds them."""
    try:
        from cstar.tests.integration_tests.data_registry import fetch
    except ImportError as exc:
        print(
            "fetch_data.py needs C-Star's integration-test data registry "
            "(cstar.tests.integration_tests.data_registry). Run it with the Python "
            "of the activated environment that C-Star is installed into (see "
            f"'Before you start' in the tutorial). Import failed with: {exc}",
            file=sys.stderr,
        )
        return 1

    paths = [fetch(name) for name in FILES]
    folders = {path.parent for path in paths}
    if len(folders) != 1:  # pooch puts every registry file in one cache directory
        print(f"expected one data directory, found {sorted(folders)}", file=sys.stderr)
        return 1
    print(folders.pop())
    return 0


if __name__ == "__main__":
    sys.exit(main())
