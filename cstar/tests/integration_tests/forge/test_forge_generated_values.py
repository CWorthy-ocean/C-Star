"""Tier 1: the values generation derives reach the namelist and cppdefs, whatever the
stored ``model_settings`` snapshot says.

One parent of the ``unified`` case is processed with real roms-tools after three
edits a saved blueprint can carry: every surface item says ``coarse_grid_mode:
always``, the parent feeds a child whose ``grid_kwargs_child`` gives no ``N`` or
``hc`` (a parent written before the resolver required them), and the snapshot holds
stale values for the coarse-grid flags, the child's coordinates, a tile count and a
boundary switch.
"""

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import f90nml
import pytest

from cstar.applications.forge.blueprint import CoarseGridMode
from cstar.applications.forge.engine import process_forge_blueprint
from cstar.applications.forge.executor import ForgeExecutor
from cstar.applications.forge.host import HostPaths

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import ForgeBlueprint

pytestmark = pytest.mark.skipif(
    shutil.which("nccopy") is None,
    reason="nccopy is required to convert use_pio inputs to CDF-5",
)

# Inside na-test-8x8 (400 km square at 5W 60N, rotated 10 degrees). N and hc are
# left to roms-tools.
_CHILD_GRID_KWARGS = {
    "nx": 4,
    "ny": 4,
    "size_x": 100,
    "size_y": 100,
    "center_lon": -5,
    "center_lat": 60,
    "rot": 10,
    "theta_s": 5.0,
    "theta_b": 2.0,
}


@dataclass(frozen=True)
class Processed:
    """The namelist, cppdefs and executor of the processed parent."""

    namelist: f90nml.Namelist
    cppdefs: str
    executor: ForgeExecutor


def _stale_parent(cfg: "ForgeBlueprint") -> None:
    cfg.forcing.surface = [
        item.model_copy(update={"coarse_grid_mode": CoarseGridMode.ALWAYS})
        for item in cfg.forcing.surface
    ]
    cfg.domain.grid_kwargs_child = dict(_CHILD_GRID_KWARGS)
    # One extraction record per file, every 1800 s: the extract stream must divide
    # the test-minimal OutputSpec's 1800 s restart period.
    cfg.domain.metadata_child = {"period": 1800.0}
    settings: dict[str, Any] = cfg.model_settings
    settings["extract_data"].update(
        do_extract=True,
        extract_file="nesting.nc",
        extract_period=1800.0,
        nrpf=1,
        n_chd=90,
        theta_s_chd=5.0,
        theta_b_chd=2.0,
        hc_chd=250.0,
    )
    settings["blk_frc"]["interp_frc"] = False
    settings["bgc"]["interp_frc"] = False
    settings["param"]["np_xi"] = 1
    settings["cppdefs"]["obc_west"] = False


@pytest.fixture(scope="module")
def processed(
    tmp_path_factory: pytest.TempPathFactory,
    forge_blueprint_factory: Callable[..., tuple["ForgeBlueprint", Path]],
) -> Processed:
    working_dir = tmp_path_factory.mktemp("generated-values")
    cfg, _ = forge_blueprint_factory("unified", working_dir, name="it-generated")
    _stale_parent(cfg)
    host = HostPaths(
        working_dir=working_dir,
        source_data_cache=working_dir / "source_cache",
        system="test",
    )
    executor = process_forge_blueprint(cfg, host=host, use_dask=False)
    assert isinstance(executor, ForgeExecutor)
    (namelist_path,) = working_dir.rglob("namelist.nml")
    (cppdefs_path,) = working_dir.rglob("cppdefs.opt")
    return Processed(f90nml.read(namelist_path), cppdefs_path.read_text(), executor)


def test_coarse_surface_files_are_interpolated(processed: Processed) -> None:
    """``coarse_grid_mode: always`` writes the surface files on the coarse grid, so
    ROMS must interpolate them.
    """
    assert processed.namelist["surf_frc_settings"]["interp_bulk_frc"] is True
    assert processed.namelist["bgc_settings"]["interp_bgc_frc"] is True


def test_child_coordinates_match_the_child_grid(processed: Processed) -> None:
    grid_child = processed.executor.grid_child
    assert grid_child is not None
    extract = processed.namelist["extract_data_settings"]
    assert extract["n_chd"] == grid_child.N
    assert extract["hc_chd"] == grid_child.hc
    assert extract["theta_s_chd"] == grid_child.theta_s
    assert extract["theta_b_chd"] == grid_child.theta_b


def test_partitioning_and_boundaries_come_from_the_domain(
    processed: Processed,
) -> None:
    assert processed.namelist["param_settings"]["np_xi"] == 2
    assert "#define OBC_WEST" in processed.cppdefs
