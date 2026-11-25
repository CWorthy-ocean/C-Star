"""Tier 1: the values generation derives reach the namelist and cppdefs, whatever the
stored ``model_settings`` snapshot says.

One parent of the ``unified`` case is processed with real roms-tools after three
edits a saved blueprint can carry: every surface item says ``coarse_grid_mode:
always``, the parent feeds two children, the first of whose ``grid_kwargs`` give no
``N`` or ``hc`` (a parent written before the resolver required them), and the
snapshot holds stale values for the coarse-grid flags, the child's coordinates, a
tile count and a boundary switch.
"""

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import f90nml
import netCDF4
import pytest

from cstar.applications.forge.blueprint import CoarseGridMode, NestedChild
from cstar.applications.forge.engine import (
    known_derived_values,
    process_forge_blueprint,
)
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
_SECOND_CHILD_GRID_KWARGS = {
    "nx": 4,
    "ny": 4,
    "size_x": 80,
    "size_y": 80,
    "center_lon": -6.5,
    "center_lat": 60.6,
    "rot": 10,
    "N": 6,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 100.0,
}


@dataclass(frozen=True)
class Processed:
    """The namelist, cppdefs and executor of the processed parent, and the blueprint
    it was processed from.
    """

    cfg: "ForgeBlueprint"
    namelist: f90nml.Namelist
    cppdefs: str
    executor: ForgeExecutor


def _stale_parent(cfg: "ForgeBlueprint") -> None:
    cfg.forcing.surface = [
        item.model_copy(update={"coarse_grid_mode": CoarseGridMode.ALWAYS})
        for item in cfg.forcing.surface
    ]
    cfg.domain.children = {
        "child": NestedChild(grid_kwargs=dict(_CHILD_GRID_KWARGS)),
        "salish-sea": NestedChild(grid_kwargs=dict(_SECOND_CHILD_GRID_KWARGS)),
    }
    # One extraction record per file, every 1800 s: the extract stream must divide
    # the test-minimal OutputSpec's 1800 s restart period.
    cfg.domain.nest_period = 1800.0
    settings: dict[str, Any] = cfg.model_settings
    settings["extract_data"].update(
        do_extract=True,
        extract_file="it-generated_nesting_child.nc",
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
    return Processed(
        cfg, f90nml.read(namelist_path), cppdefs_path.read_text(), executor
    )


def test_coarse_surface_files_are_interpolated(processed: Processed) -> None:
    """``coarse_grid_mode: always`` writes the surface files on the coarse grid, so
    ROMS must interpolate them.
    """
    assert processed.namelist["surf_frc_settings"]["interp_bulk_frc"] is True
    assert processed.namelist["bgc_settings"]["interp_bgc_frc"] is True


def test_child_coordinates_match_the_child_grids(processed: Processed) -> None:
    extract = processed.namelist["extract_data_settings"]
    children = processed.executor.grid_children
    assert list(children) == ["child", "salish-sea"]
    assert extract["extract_root_name"] == list(children)
    for i, grid_child in enumerate(children.values()):
        assert extract["n_chd"][i] == grid_child.N
        assert extract["hc_chd"][i] == grid_child.hc
        assert extract["theta_s_chd"][i] == grid_child.theta_s
        assert extract["theta_b_chd"][i] == grid_child.theta_b


def test_each_child_gets_a_nesting_file_with_its_name(processed: Processed) -> None:
    input_dir = processed.executor.input_data_dir
    for name in processed.executor.grid_children:
        (path,) = input_dir.glob(f"*_nesting_{name}.nc")
        with netCDF4.Dataset(path) as ds:
            assert f"{name}_xi_rho" in ds.dimensions
        assert list(input_dir.glob(f"*_grid_child-{name}.nc"))


def test_partitioning_and_boundaries_come_from_the_domain(
    processed: Processed,
) -> None:
    assert processed.namelist["param_settings"]["np_xi"] == 2
    assert "#define OBC_WEST" in processed.cppdefs


def test_values_known_before_generation_match_what_generation_wrote(
    processed: Processed,
) -> None:
    """``known_derived_values`` restates input generation's rules so conflicts are
    refused before generation; it must agree with what the generator wrote.
    """
    written = {
        **processed.executor._settings_run_time,
        "cppdefs": processed.executor._settings_compile_time["cppdefs"],
    }
    known = known_derived_values(processed.cfg)
    assert known["blk_frc"] and known["extract_data"] and known["param"]
    for section, leaves in known.items():
        for leaf, value in leaves.items():
            assert written[section][leaf] == value, f"{section}.{leaf}"
