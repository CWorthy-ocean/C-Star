"""Tier 2 negative control: ROMS with ParallelIO refuses a non-classic-format grid.

Forge runs in-process; the emitted ``roms_marbl`` blueprint is then edited to point at
a NETCDF4 copy of the grid and run with ``cstar blueprint run``. The check lives in
``pre_run``, after the compile, so this test also builds ROMS and takes minutes.
"""

import shutil
import subprocess
import typing as t
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.host import HostPaths
from cstar.tests.integration_tests.cli_harness import make_cli_env, run_cstar

if t.TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

GUARD_MESSAGE = "not a classic-format netCDF file"
RUN_TIMEOUT = 40 * 60
MISSING_TOOLS = [
    tool
    for tool, found in {
        "nccopy": shutil.which("nccopy"),
        "mpirun": shutil.which("mpirun"),
        "mpifort or gfortran": shutil.which("mpifort") or shutil.which("gfortran"),
    }.items()
    if not found
]

pytestmark = pytest.mark.skipif(
    bool(MISSING_TOOLS), reason=f"ROMS toolchain missing: {', '.join(MISSING_TOOLS)}"
)


def with_netcdf4_grid(forge_dir: Path, edited: Path, run_dir: Path) -> Path:
    """Write a copy of the emitted blueprint whose grid is a NETCDF4 file.

    Returns
    -------
    Path
        The edited blueprint's path.
    """
    import xarray as xr

    (emitted,) = (forge_dir / "blueprints").glob("B_*.yaml")
    blueprint = yaml.safe_load(emitted.read_text())

    grid_entry = blueprint["grid"]["data"][0]
    hdf_grid = edited.parent / "grid_netcdf4.nc"
    with xr.open_dataset(grid_entry["location"]) as ds:
        ds.to_netcdf(hdf_grid, format="NETCDF4")
    assert hdf_grid.read_bytes()[:4] == b"\x89HDF"

    grid_entry["location"] = str(hdf_grid)
    blueprint["working_dir"] = str(run_dir)
    edited.write_text(yaml.safe_dump(blueprint, sort_keys=False))
    return edited


@pytest.fixture(scope="module")
def guarded_run(
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
    cstar_shim: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> subprocess.CompletedProcess[str]:
    """Run ROMS on a NETCDF4 grid once for the module.

    Returns
    -------
    subprocess.CompletedProcess[str]
        The finished ``cstar blueprint run``.
    """
    root = tmp_path_factory.mktemp("pio_guard")
    forge_dir = root / "forge"
    cfg, _ = forge_blueprint_factory("unified", forge_dir)
    host = HostPaths(
        working_dir=forge_dir, source_data_cache=forge_dir / "cache", system="test"
    )
    process_forge_blueprint(cfg, host=host, use_dask=False)

    edited = with_netcdf4_grid(forge_dir, root / "edited.yaml", root / "roms_run")
    return run_cstar(
        make_cli_env(root, cstar_shim),
        "blueprint",
        "run",
        str(edited),
        timeout=RUN_TIMEOUT,
    )


def test_netcdf4_grid_fails_the_run(
    guarded_run: subprocess.CompletedProcess[str],
) -> None:
    """The run exits non-zero."""
    assert guarded_run.returncode != 0, guarded_run.stdout[-3000:]


def test_failure_names_the_format_problem(
    guarded_run: subprocess.CompletedProcess[str],
) -> None:
    """The output says the grid is not a classic-format netCDF file."""
    combined = f"{guarded_run.stdout}\n{guarded_run.stderr}"
    assert GUARD_MESSAGE in combined, combined[-3000:]
