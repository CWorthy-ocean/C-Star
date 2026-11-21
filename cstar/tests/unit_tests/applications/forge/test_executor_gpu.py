"""The Forge executor emitting a ``ucla_roms_gpu`` blueprint (experimental).

The executor assembles the ``ucla_roms``-shaped blueprint through every stage and
converts it once, at emit (``emitted_blueprint_data``); ``configure_build`` renders
no ``cppdefs.opt`` and applies the GPU model's limits (Rules F, F2, G).
"""

from __future__ import annotations

import copy
import tempfile
import typing as t
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr
import yaml
from pydantic import ValidationError

import cstar.applications.ucla_roms.models as cstar_models
import cstar.catalog
from cstar.applications.forge.engine import (
    forge_blueprint_to_builder_kwargs,
    split_model_settings,
)
from cstar.applications.forge.executor import ForgeExecutor, emitted_blueprint_data
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.namelist_model import run_time_settings_for_ref
from cstar.applications.forge.resolve import GPU_FORCING_SPEC, build_forge_blueprint
from cstar.applications.ucla_roms_gpu.models import (
    APP_NAME as GPU_APP,
)
from cstar.applications.ucla_roms_gpu.models import (
    TilingParameterSet,
    UclaRomsGpuBlueprint,
)
from cstar.catalog.domain_catalog import default_catalog
from cstar.orchestration.models import Resource
from cstar.roms.precheck import NamelistConsistencyError

_SPECS = Path(cstar.catalog.__file__).parent / "bundled" / "specs" / "ModelSpec"
_GRID_KWARGS = {
    "nx": 8,
    "ny": 8,
    "size_x": 500,
    "size_y": 500,
    "center_lon": 0,
    "center_lat": 55,
    "rot": 10,
    "N": 3,
    "theta_s": 5.0,
    "theta_b": 2.0,
    "hc": 250.0,
}


def _grid_mock() -> MagicMock:
    grid = MagicMock()
    grid.size_x = grid.size_y = 100.0
    grid.nx = grid.ny = 100
    grid.ds.__contains__.return_value = True
    grid.ds.__getitem__.return_value.max.return_value.values = 1000.0
    return grid


@pytest.fixture(autouse=True)
def _mock_grid() -> t.Iterator[None]:
    with patch("cstar.applications.forge.executor.rt.Grid") as grid:
        grid.return_value = _grid_mock()
        yield


def _resolve(**partitioning: t.Any):
    return build_forge_blueprint(
        model_path=_SPECS / "roms-marbl-gpu-0.1.3-default.yaml",
        grid_name="test-tiny",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries={"south": False, "east": True, "north": True, "west": False},
        partitioning={"n_procs_x": 2, "n_procs_y": 2, **partitioning},
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        dt=7200,
        v_sponge=100.0,
        forcing_inputs=default_catalog.forcing_data(GPU_FORCING_SPEC),
        output_settings=default_catalog.output_data("standard"),
    )


def _host() -> HostPaths:
    tmp = Path(tempfile.mkdtemp(prefix="forge-test-gpu-"))
    return HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")


def _complete(builder: ForgeExecutor, tmp_path: Path) -> ForgeExecutor:
    """Give the placeholder blueprint real input files, as generate_inputs does."""
    file = tmp_path / "input.nc"
    xr.Dataset({"h": ("x", [1.0, 2.0])}).to_netcdf(file)
    ds = cstar_models.Dataset(data=[Resource(location=str(file), partitioned=False)])
    assert builder.ucla_roms_blueprint is not None
    builder.ucla_roms_blueprint = builder.ucla_roms_blueprint.model_copy(
        update={
            "grid": ds,
            "initial_conditions": ds,
            "forcing": cstar_models.ForcingConfiguration(boundary=ds, surface=ds),
        }
    )
    builder._inputs_generated = True
    # The sections generate_inputs fills in from the generated files.
    builder._settings_run_time.update(
        grid={"grid_file": str(file)},
        s_coord={"theta_s": 5.0, "theta_b": 2.0, "tcline": 250.0},
        initial={"initial_file": str(file)},
        forcing={"surface_forcing_path": str(file)},
    )
    return builder


@pytest.fixture
def cfg():
    return _resolve()


@pytest.fixture
def builder(cfg, tmp_path) -> ForgeExecutor:
    return _complete(ForgeExecutor.from_forge_blueprint(cfg, host=_host()), tmp_path)


def _configure(builder: ForgeExecutor, cfg, **kwargs: t.Any) -> MagicMock:
    run_ov, compile_ov = split_model_settings(cfg)
    run_ov = {**run_ov, **kwargs.pop("run_time", {})}
    with patch("cstar.applications.forge.executor.render_roms_settings") as render:
        render.return_value = {"location": "x", "filter": {"files": ["marbl_in"]}}
        builder.configure_build(
            compile_time_settings=compile_ov, run_time_settings=run_ov, **kwargs
        )
    return render


def _cpu_dump(builder: ForgeExecutor) -> dict[str, t.Any]:
    assert builder.ucla_roms_blueprint is not None
    return builder.ucla_roms_blueprint.model_dump(mode="json", exclude_none=True)


class TestEmittedBlueprintData:
    def test_cpu_is_unchanged(self, builder):
        data = _cpu_dump(builder)
        assert (
            emitted_blueprint_data(data, application="ucla_roms", tiling=None) is data
        )

    def test_gpu_conversion(self, builder):
        data = _cpu_dump(builder)
        before = copy.deepcopy(data)
        tiling = TilingParameterSet(n_procs_x=2, n_procs_y=2, nodes=1)

        gpu = emitted_blueprint_data(data, application=GPU_APP, tiling=tiling)

        assert data == before  # the CPU dump is not edited
        assert gpu["application"] == GPU_APP
        assert (
            gpu["schema_version"]
            == UclaRomsGpuBlueprint.model_fields["schema_version"].default
        )
        assert "partitioning" not in gpu
        assert set(gpu["tiling"]) >= {
            "n_procs_x",
            "n_procs_y",
            "nodes",
            "writers_per_node",
        }
        assert set(gpu["code"]) == {"roms", "run_time"}
        assert gpu["$schema"].endswith(f"{GPU_APP}_schema.{gpu['schema_version']}.json")

    def test_gpu_needs_a_tiling(self, builder):
        with pytest.raises(ValueError, match="needs a tiling"):
            emitted_blueprint_data(_cpu_dump(builder), application=GPU_APP, tiling=None)


class TestExecutorFields:
    def test_builder_kwargs_carry_tiling_and_a_cstar_partitioning(self, cfg):
        kwargs = forge_blueprint_to_builder_kwargs(cfg)

        assert kwargs["emitted_application"] == GPU_APP
        assert kwargs["tiling"] == cfg.tiling
        assert "nodes" not in kwargs["partitioning"]
        assert "writers_per_node" not in kwargs["partitioning"]
        cstar_models.PartitioningParameterSet(**kwargs["partitioning"])

    def test_kwargs_with_the_gpu_layout_keys_set(self):
        cfg = _resolve()
        cfg.domain.partitioning.writers_per_node = 2
        kwargs = forge_blueprint_to_builder_kwargs(cfg)

        assert kwargs["tiling"].writers_per_node == 2
        assert "writers_per_node" not in kwargs["partitioning"]
        ForgeExecutor.from_forge_blueprint(cfg, host=_host())

    def test_gpu_without_tiling_is_refused(self, cfg):
        kwargs = forge_blueprint_to_builder_kwargs(cfg) | {"tiling": None}
        with pytest.raises(ValidationError, match="tiling must be set"):
            ForgeExecutor(host=_host(), **kwargs)

    def test_cpu_with_tiling_is_refused(self, cfg):
        kwargs = forge_blueprint_to_builder_kwargs(cfg) | {
            "emitted_application": "ucla_roms"
        }
        with pytest.raises(ValidationError, match="tiling must be set"):
            ForgeExecutor(host=_host(), **kwargs)

    def test_tiling_dims_must_match_partitioning(self, cfg):
        kwargs = forge_blueprint_to_builder_kwargs(cfg) | {
            "tiling": TilingParameterSet(n_procs_x=4, n_procs_y=1)
        }
        with pytest.raises(ValidationError, match="must match partitioning"):
            ForgeExecutor(host=_host(), **kwargs)


class TestConfigureBuild:
    def test_emits_a_gpu_blueprint_without_cppdefs(self, builder, cfg):
        render = _configure(builder, cfg)

        # nothing was rendered from a compile-time template
        assert all(
            call.kwargs["template_files"] != ["cppdefs.opt.j2"]
            for call in render.call_args_list
        )
        assert not (builder.compile_time_code_dir / "cppdefs.opt").exists()
        emitted = yaml.safe_load(builder.path_ucla_roms_blueprint().read_text())
        assert emitted["application"] == GPU_APP
        assert "partitioning" not in emitted
        assert set(emitted["code"]) == {"roms", "run_time"}
        assert emitted["tiling"]["n_procs_x"] == 2
        loaded = UclaRomsGpuBlueprint.model_validate(emitted)
        assert loaded.tiling.compute_needs.gpu_tasks == 4

    def test_namelist_is_on_the_0_11_tier(self, builder, cfg):
        with patch(
            "cstar.applications.forge.executor.run_time_settings_for_ref",
            wraps=run_time_settings_for_ref,
        ) as tier:
            _configure(builder, cfg)

        assert tier.call_args.args == ("0.11.0",)
        assert (builder.run_time_code_dir / "namelist.nml").exists()

    def test_in_memory_blueprint_stays_cpu_shaped_and_reconfigures(self, builder, cfg):
        _configure(builder, cfg)

        assert isinstance(builder.ucla_roms_blueprint, cstar_models.UclaRomsBlueprint)
        in_memory = builder.ucla_roms_blueprint.model_dump()
        assert in_memory["partitioning"]["n_procs_x"] == 2
        assert "tiling" not in in_memory

        _configure(builder, cfg)

        emitted = yaml.safe_load(builder.path_ucla_roms_blueprint().read_text())
        assert emitted["application"] == GPU_APP

    def test_get_ds_reads_the_gpu_shaped_file(self, builder, cfg):
        _configure(builder, cfg)

        assert builder.get_ds("grid") is not None
        blueprint = builder.ucla_roms_blueprint_from_file
        assert blueprint is not None
        assert blueprint.grid is not None
        assert blueprint.forcing is not None

    def test_validation_failure_is_reported_for_the_gpu_model(self, builder, cfg):
        builder.ucla_roms_blueprint = builder.ucla_roms_blueprint.model_copy(
            update={"runtime_params": {"start_date": builder.start_date}}
        )
        with pytest.raises(ValueError, match="does not validate against the installed"):
            builder._validated_ucla_roms_blueprint()

    def test_rule_f_refuses_a_stop_switch(self, builder, cfg):
        with pytest.raises(NamelistConsistencyError, match="z-slice"):
            _configure(builder, cfg, run_time={"zslice": {"do_zslice": True}})
        assert not builder.path_ucla_roms_blueprint().exists()

    def test_rule_f2_refuses_a_co2_value_the_model_ignores(self, builder, cfg):
        with pytest.raises(NamelistConsistencyError, match="xco2air_default"):
            _configure(builder, cfg, run_time={"bgc": {"xco2air_default": 400.0}})

    def test_rule_g_refuses_too_many_forcing_files(self, builder, cfg):
        files = [f"f{i}.nc" for i in range(65)]
        with pytest.raises(NamelistConsistencyError, match="frcfiles"):
            _configure(
                builder, cfg, run_time={"forcing": {"surface_forcing_bgc_path": files}}
            )
