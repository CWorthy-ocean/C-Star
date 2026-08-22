"""
Tests for the ForgeExecutor (cstar_forge.forge.executor).

The executor is now config/authoring-free: it is constructed the canonical way via
``ForgeExecutor.from_forge_blueprint(cfg, host=host)`` where ``cfg`` is a resolved
``ForgeBlueprint`` (built by ``build_forge_blueprint``) and ``host`` is an injected
``HostPaths``. All produced-artifact paths route under ``host.working_dir``.

Tests cover:
- ForgeExecutor initialization and validation
- Properties (name, input_data_dir, roms_marbl_blueprint_dir, path_roms_marbl_blueprint, datasets)
- Model post-init behavior
- Blueprint persist / path_roms_marbl_blueprint
- get_ds method
- ensure_source_data
- generate_inputs
- configure_build / build
- deep-merge helper
"""

import asyncio
import copy
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

import cstar.applications.roms_marbl.models as cstar_models
import numpy as np
import pytest
import xarray as xr
import yaml
from cstar.applications.core import RunnerRequest
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.execution.handler import ExecutionStatus
from cstar.orchestration.models import Resource
from pydantic import ValidationError

import cstar_forge
from cstar_forge import models as forge_models
from cstar_forge import run as forge_run
from cstar_forge.domain_catalog import default_catalog as _CATALOG
from cstar_forge.forge.app import ForgeRunner
from cstar_forge.forge.executor import ForgeExecutor, _deep_merge_settings_dict
from cstar_forge.forge.forge_blueprint import ForgeBlueprint
from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint
from cstar_forge.forge.host import HostPaths
from cstar_forge.forge.input_data import CHILD_IC_PLACEHOLDER_LOCATION
from cstar_forge.forge_blueprint_resolve import build_forge_blueprint

requires_cstar_pio = pytest.mark.skipif(
    "pio" not in cstar_models.ROMSCompositeCodeRepository.model_fields,
    reason=(
        "installed C-Star predates ParallelIO support "
        "(code.pio / model_params.use_pio fields, cstar #594)"
    ),
)

_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
)
# ucla-roms >= 0.5.0 ModelSpec -- used by the versioned-namelist golden test below.
_MODEL_DIR_ROMS050 = (
    Path(cstar_forge.__file__).parent
    / "catalog"
    / "ModelSpec"
    / "roms-marbl-0.5-default"
)
# ModelSpec no longer embeds a default forcing/output selection -- these tests just
# need a valid, representative pair from the bundled catalog.
_FORCING_INPUTS = _CATALOG.forcing_data("glorys-era5-unified")
_OUTPUT_SETTINGS = _CATALOG.output_data("standard")


def _make_builder(args, **overrides):
    """Single construction point: build a resolved ForgeBlueprint from the bundled ModelSpec
    plus a temp host, then construct the executor the canonical way.
    """
    merged = {**args, **overrides}
    ob = merged["open_boundaries"]
    part = merged["partitioning"]
    cfg = build_forge_blueprint(
        model_dir=_MODEL_DIR,
        grid_name=merged["grid_name"],
        grid_kwargs=merged["grid_kwargs"],
        open_boundaries=ob.model_dump() if hasattr(ob, "model_dump") else ob,
        partitioning=part.model_dump() if hasattr(part, "model_dump") else part,
        start_date=merged["start_date"],
        end_date=merged["end_date"],
        description=merged.get("description", "Generated blueprint"),
        name=merged.get("name"),
        use_pio=merged.get("use_pio", False),
        dt=7200,
        forcing_inputs=merged.get("forcing_inputs", _FORCING_INPUTS),
        output_settings=merged.get("output_settings", _OUTPUT_SETTINGS),
    )
    tmp = Path(tempfile.mkdtemp(prefix="forge-test-core-"))
    host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
    return ForgeExecutor.from_forge_blueprint(cfg, host=host)


def _create_empty_dataset(tmp_path):
    """Helper to create an empty Dataset with a placeholder resource."""
    placeholder_file = tmp_path / "placeholder.nc"
    placeholder_file.touch()
    return cstar_models.Dataset(
        data=[Resource(location=str(placeholder_file), partitioned=False)]
    )


def _create_grid_mock():
    """Helper function to create a proper grid mock with required attributes."""
    mock_grid_instance = MagicMock()
    # Add grid dimensions and sizes (needed for CFL calculation)
    mock_grid_instance.size_x = 100.0  # km
    mock_grid_instance.size_y = 100.0  # km
    mock_grid_instance.nx = 100
    mock_grid_instance.ny = 100

    # Create a proper dataset mock for CFL calculation
    # The 'h' variable is bathymetry (depth) at RHO-points
    mock_h_array = MagicMock()
    mock_h_max_result = MagicMock()
    mock_h_max_result.values = 1000.0  # Max depth in meters
    mock_h_array.max.return_value = mock_h_max_result

    # Create a dataset mock that supports 'h' in ds and ds['h']
    class MockDataset:
        def __contains__(self, key):
            return key == "h"

        def __getitem__(self, key):
            if key == "h":
                return mock_h_array
            return MagicMock()

    mock_grid_instance.ds = MockDataset()

    return mock_grid_instance


@pytest.fixture(autouse=True)
def mock_grid():
    """Autouse: the executor still builds the grid in model_post_init via rt.Grid,
    so keep it mocked. Tests that need the mock can request it by name.
    """
    with patch("cstar_forge.forge.executor.rt.Grid") as mg:
        mg.return_value = _create_grid_mock()
        yield mg


@pytest.fixture
def sample_grid_kwargs():
    """Sample grid keyword arguments."""
    return {
        "nx": 3,
        "ny": 4,
        "size_x": 500,
        "size_y": 1000,
        "center_lon": 0,
        "center_lat": 55,
        "rot": 10,
        "N": 3,
        "theta_s": 5.0,
        "theta_b": 2.0,
        "hc": 250.0,
    }


@pytest.fixture
def sample_open_boundaries():
    """Sample open boundaries configuration."""
    return forge_models.OpenBoundaries(north=True, south=True, east=True, west=True)


@pytest.fixture
def sample_partitioning():
    """Sample partitioning parameters."""
    return cstar_models.PartitioningParameterSet(n_procs_x=2, n_procs_y=2)


@pytest.fixture
def sample_runtime_params():
    """Sample runtime parameters."""
    return cstar_models.RuntimeParameterSet(
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        checkpoint_frequency="1d",
    )


@pytest.fixture
def sample_model_params():
    """Sample model parameters."""
    return cstar_models.ModelParameterSet(time_step=60)


@pytest.fixture
def minimal_cstar_spec_builder_args(
    sample_grid_kwargs,
    sample_open_boundaries,
    sample_partitioning,
):
    """Minimal arguments for creating a ForgeExecutor."""
    return {
        "model_name": "cson_roms-marbl_v0.1",
        "grid_name": "test-grid",
        "grid_kwargs": sample_grid_kwargs,
        "open_boundaries": sample_open_boundaries,
        "partitioning": sample_partitioning,
        "start_date": datetime(2012, 1, 1),
        "end_date": datetime(2012, 1, 2),
    }


class TestForgeExecutorInitialization:
    """Tests for ForgeExecutor initialization and validation."""

    def test_initialization_minimal(self, minimal_cstar_spec_builder_args):
        """Test creating ForgeExecutor with minimal required fields."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder.name.startswith("cson_roms-marbl_v0.1_")
        assert builder.grid_name == "test-grid"
        assert builder.description == "Generated blueprint"  # Default value

    def test_initialization_with_description(self, minimal_cstar_spec_builder_args):
        """Test creating ForgeExecutor with custom description."""
        minimal_cstar_spec_builder_args["description"] = "Custom description"
        builder = _make_builder(minimal_cstar_spec_builder_args)
        assert builder.description == "Custom description"

    def test_initialization_prints_planned_netcdf_outputs(
        self,
        minimal_cstar_spec_builder_args,
        capsys,
    ):
        """Test initialization prints planned NetCDF output list (from the resolved
        forcing_override, not the catalog ModelSpec).
        """
        _make_builder(minimal_cstar_spec_builder_args)

        stdout = capsys.readouterr().out
        assert "ForgeExecutor: planned NetCDF outputs" in stdout
        assert "_grid.nc" in stdout
        assert "_initial_conditions.nc" in stdout
        # Forcing stems come from the resolved forcing categories/types. The type is the
        # enum *value* (e.g. "physics"/"bgc"), not the enum repr — the bridge dumps with
        # mode="json", so filenames no longer leak "SurfaceType.PHYSICS".
        assert "_surface-physics.nc" in stdout
        assert "_surface-bgc.nc" in stdout
        assert "_boundary-physics.nc" in stdout
        assert "_tidal.nc" in stdout
        planned_section = stdout.split("ForgeExecutor: planned NetCDF outputs", 1)[
            1
        ].split("ForgeExecutor: output locations", 1)[0]
        assert "v0.1" not in planned_section, (
            "Planned paths must match on-disk NetCDF naming (dots in model name → underscores)"
        )

    def test_validation_end_date_before_start_date(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that validation raises error when end_date is before start_date."""
        minimal_cstar_spec_builder_args["end_date"] = datetime(2012, 1, 1)
        minimal_cstar_spec_builder_args["start_date"] = datetime(2012, 1, 2)

        with pytest.raises(ValidationError) as exc_info:
            _make_builder(minimal_cstar_spec_builder_args)
        assert "start_date must precede end_date" in str(
            exc_info.value
        ) or "end_date must be after start_date" in str(exc_info.value)

    def test_validation_end_date_equals_start_date(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that validation raises error when end_date equals start_date."""
        minimal_cstar_spec_builder_args["end_date"] = datetime(2012, 1, 1)
        minimal_cstar_spec_builder_args["start_date"] = datetime(2012, 1, 1)

        with pytest.raises(ValidationError) as exc_info:
            _make_builder(minimal_cstar_spec_builder_args)
        assert "start_date must precede end_date" in str(
            exc_info.value
        ) or "end_date must be after start_date" in str(exc_info.value)


class TestVSpongeDefault:
    """Tests for default v_sponge (resolved by build_forge_blueprint from grid_kwargs)."""

    def test_v_sponge_default_from_grid_on_init(self, minimal_cstar_spec_builder_args):
        builder = _make_builder(minimal_cstar_spec_builder_args)

        # v_sponge default = (size_x / nx) * 1000 / 10 (grid spacing in m / 10).
        # size_x=500, nx=3 -> (500/3)*1000/10.
        expected = (500 / 3) * 1000.0 / 10.0
        assert builder._settings_run_time["v_sponge"]["v_sponge"] == pytest.approx(
            expected
        )

    def test_v_sponge_explicit_run_time_settings_override(
        self, minimal_cstar_spec_builder_args
    ):
        builder = _make_builder(minimal_cstar_spec_builder_args)

        builder._update_settings_run_time({"v_sponge": {"v_sponge": 42.0}})
        assert builder._settings_run_time["v_sponge"]["v_sponge"] == 42.0


class TestUpdateSettingsRunTime:
    """Edge cases for the run-time settings merge entry point."""

    def test_empty_update_is_a_noop(self, minimal_cstar_spec_builder_args):
        builder = _make_builder(minimal_cstar_spec_builder_args)
        before = copy.deepcopy(builder._settings_run_time)

        builder._update_settings_run_time({})
        assert builder._settings_run_time == before

    def test_unknown_top_level_key_raises(self, minimal_cstar_spec_builder_args):
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(ValueError, match="nothing-shared"):
            builder._update_settings_run_time({"nothing-shared": "foo"})


class TestForgeExecutorProperties:
    """Tests for ForgeExecutor properties."""

    def test_name_property(self, minimal_cstar_spec_builder_args):
        """Test the name property (now the stored ForgeBlueprint.name,
        which defaults to {model_name}_{grid_name}_{n_procs}procs).
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        n_procs = builder.partitioning.n_procs_x * builder.partitioning.n_procs_y
        expected_name = f"cson_roms-marbl_v0.1_{builder.grid_name}_{n_procs}procs"
        assert builder.name == expected_name

    def test_input_data_dir_property(self, minimal_cstar_spec_builder_args):
        """Test that the input data dir routes under host.working_dir."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        assert builder.input_data_dir == builder.host.working_dir / "input_data"

    def test_roms_marbl_blueprint_dir_property(self, minimal_cstar_spec_builder_args):
        """Test the roms_marbl_blueprint_dir property (under host.working_dir)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        assert (
            builder.roms_marbl_blueprint_dir == builder.host.working_dir / "blueprints"
        )

    def test_path_roms_marbl_blueprint_method(self, minimal_cstar_spec_builder_args):
        """Test the path_roms_marbl_blueprint method (host-based)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        expected_path = builder.roms_marbl_blueprint_dir / f"B_{builder.name}.yaml"
        assert builder.path_roms_marbl_blueprint() == expected_path

    def test_datasets_property_auto_populates(
        self, minimal_cstar_spec_builder_args, tmp_path
    ):
        """Test that datasets property auto-populates from blueprint."""
        # Create test files
        grid_file = tmp_path / "grid.nc"
        grid_file.touch()
        ic_file = tmp_path / "ic.nc"
        ic_file.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        # Set blueprint with data
        builder.roms_marbl_blueprint.grid = cstar_models.Dataset(
            data=[Resource(location=str(grid_file), partitioned=False)]
        )
        builder.roms_marbl_blueprint.initial_conditions = cstar_models.Dataset(
            data=[Resource(location=str(ic_file), partitioned=False)]
        )

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds

            result = builder.datasets

            assert isinstance(result, dict)
            assert "grid" in result
            assert "initial_conditions" in result


class TestForgeExecutorModelPostInit:
    """Tests for model_post_init behavior."""

    def test_model_post_init_initializes_roms_marbl_blueprint(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that model_post_init initializes the blueprint."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder.roms_marbl_blueprint is not None
        assert isinstance(builder.roms_marbl_blueprint, cstar_models.RomsMarblBlueprint)
        assert builder.roms_marbl_blueprint.name == builder.name

    def test_model_post_init_creates_grid(
        self, minimal_cstar_spec_builder_args, mock_grid
    ):
        """Test that model_post_init creates the grid via rt.Grid(**grid_kwargs)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        mock_grid.assert_called_once_with(
            **builder.grid_kwargs, verbose=builder.verbose
        )
        assert builder.grid == mock_grid.return_value

    def test_model_post_init_etopo5_leaves_grid_kwargs_untouched(
        self, minimal_cstar_spec_builder_args, mock_grid
    ):
        """The default ETOPO5 source injects nothing — roms-tools fetches ETOPO5 itself,
        so grid_kwargs must reach rt.Grid without a ``topography_source`` key.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder.topography_source == "ETOPO5"
        _, kwargs = mock_grid.call_args
        assert "topography_source" not in kwargs

    def test_model_post_init_srtm15_injects_topography_source(
        self, minimal_cstar_spec_builder_args, mock_grid
    ):
        """SRTM15 is staged and its {'name','path'} dict is injected into grid_kwargs
        BEFORE rt.Grid is called. This is the load-bearing wiring whose failure is silent
        (roms-tools would otherwise fall back to ETOPO5).
        """
        args = minimal_cstar_spec_builder_args
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name=args["grid_name"],
            grid_kwargs=args["grid_kwargs"],
            open_boundaries=args["open_boundaries"].model_dump(),
            partitioning=args["partitioning"].model_dump(),
            start_date=args["start_date"],
            end_date=args["end_date"],
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )
        # Drive an SRTM15 spec (the bundled ModelSpec defaults to ETOPO5).
        cfg = cfg.model_copy(
            update={
                "domain": cfg.domain.model_copy(update={"topography_source": "SRTM15"})
            }
        )
        tmp = Path(tempfile.mkdtemp(prefix="forge-test-srtm15-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        staged = tmp / "SRTM15" / "SRTM15_V2.7.nc"
        # Mock the staging download: prepare_all() is a no-op, path_for_source returns the path.
        with patch("cstar_forge.forge.executor.source_data.SourceData") as mock_sd:
            inst = mock_sd.return_value
            inst.prepare_all.return_value = inst
            inst.path_for_source.return_value = staged
            builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)

        assert builder.topography_source == "SRTM15"
        _, kwargs = mock_grid.call_args
        assert kwargs["topography_source"] == {"name": "SRTM15", "path": str(staged)}
        # name must be a plain str, never the TopographySource enum (so grid.to_yaml is safe).
        assert type(kwargs["topography_source"]["name"]) is str

    def test_model_post_init_custom_topography_path_used_verbatim(
        self, minimal_cstar_spec_builder_args, mock_grid
    ):
        """An explicit ``topography_path`` is injected verbatim with no staging call —
        it overrides the derive-from-SourceData behavior for any source name.
        """
        args = minimal_cstar_spec_builder_args
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name=args["grid_name"],
            grid_kwargs=args["grid_kwargs"],
            open_boundaries=args["open_boundaries"].model_dump(),
            partitioning=args["partitioning"].model_dump(),
            start_date=args["start_date"],
            end_date=args["end_date"],
            topography_path="/custom/my_topo.nc",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )
        cfg = cfg.model_copy(
            update={
                "domain": cfg.domain.model_copy(update={"topography_source": "SRTM15"})
            }
        )
        assert cfg.domain.topography_path == "/custom/my_topo.nc"
        tmp = Path(tempfile.mkdtemp(prefix="forge-test-topopath-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        # Staging must NOT be invoked when an explicit path is given.
        with patch("cstar_forge.forge.executor.source_data.SourceData") as mock_sd:
            builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
            mock_sd.assert_not_called()

        assert builder.topography_path == "/custom/my_topo.nc"
        _, kwargs = mock_grid.call_args
        assert kwargs["topography_source"] == {
            "name": "SRTM15",
            "path": "/custom/my_topo.nc",
        }

    def test_model_post_init_does_not_persist(self, minimal_cstar_spec_builder_args):
        """model_post_init only builds the in-memory blueprint; the executor now
        persists exactly once, at the end of configure_build(). Nothing should be
        on disk yet, so roms_marbl_blueprint_from_file finds nothing to load.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert not builder.path_roms_marbl_blueprint().exists()
        assert builder.roms_marbl_blueprint_from_file is None

    def test_configure_build_blueprint_loads_back_from_file(
        self, minimal_cstar_spec_builder_args
    ):
        """After configure_build() persists the blueprint, roms_marbl_blueprint_from_file
        loads it back from disk.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }
            builder.configure_build()

        assert builder.roms_marbl_blueprint_from_file is not None
        assert isinstance(
            builder.roms_marbl_blueprint_from_file, cstar_models.RomsMarblBlueprint
        )


class TestForgeExecutorModelPostInitGridFile:
    """Tests for the ``domain.grid_file`` pathway in ``model_post_init``: a
    user-supplied pre-made grid netCDF loaded via ``rt.Grid(filename=...)``
    instead of one built from ``grid_kwargs``.

    ``cfg`` is built normally (``_make_builder``, no ``grid_file``) and then
    ``model_copy``'d to inject ``domain.grid_file`` -- mirrors
    ``test_model_post_init_srtm15_injects_topography_source``'s pattern of
    exercising an exotic domain value without re-running validators. The
    resolver-level ``grid_file=...`` normalization/derivation itself is covered
    in ``tests/test_forge_blueprint.py``.
    """

    @staticmethod
    def _write_tiny_netcdf(tmp_path, name="grid.nc"):
        import numpy as np
        import xarray as xr

        ds = xr.Dataset(
            {"mask_rho": (("eta_rho", "xi_rho"), np.ones((4, 5)))},
            attrs={"title": "tiny user-supplied grid"},
        )
        path = tmp_path / name
        ds.to_netcdf(path)
        return path

    def _cfg_with_grid_file(self, minimal_cstar_spec_builder_args, tmp_path, gf):
        args = minimal_cstar_spec_builder_args
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name=args["grid_name"],
            grid_kwargs=args["grid_kwargs"],
            open_boundaries=args["open_boundaries"].model_dump(),
            partitioning=args["partitioning"].model_dump(),
            start_date=args["start_date"],
            end_date=args["end_date"],
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )
        return cfg.model_copy(
            update={
                "domain": cfg.domain.model_copy(
                    update={"grid_kwargs": {}, "grid_file": gf}
                )
            }
        )

    def test_grid_file_builds_via_filename(
        self, minimal_cstar_spec_builder_args, mock_grid, tmp_path
    ):
        from cstar_forge.forge.forge_blueprint import UserProvidedFile
        from cstar_forge.forge.user_files import hash_netcdf_contents

        grid_path = self._write_tiny_netcdf(tmp_path)
        gf = UserProvidedFile(
            location=str(grid_path), content_hash=hash_netcdf_contents(grid_path)
        )
        cfg = self._cfg_with_grid_file(minimal_cstar_spec_builder_args, tmp_path, gf)

        tmp = Path(tempfile.mkdtemp(prefix="forge-test-gridfile-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)

        mock_grid.assert_called_once_with(
            filename=str(grid_path.resolve()), verbose=builder.verbose
        )
        assert builder.grid == mock_grid.return_value

    def test_grid_file_missing_raises_file_not_found(
        self, minimal_cstar_spec_builder_args, tmp_path
    ):
        from cstar_forge.forge.forge_blueprint import UserProvidedFile

        missing = tmp_path / "does-not-exist.nc"
        gf = UserProvidedFile(location=str(missing), content_hash="x" * 64)
        cfg = self._cfg_with_grid_file(minimal_cstar_spec_builder_args, tmp_path, gf)

        tmp = Path(tempfile.mkdtemp(prefix="forge-test-gridfile-missing-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        with pytest.raises(FileNotFoundError, match="grid"):
            ForgeExecutor.from_forge_blueprint(cfg, host=host)

    def test_grid_file_hash_mismatch_warns(
        self, minimal_cstar_spec_builder_args, mock_grid, tmp_path
    ):
        from cstar_forge.forge.forge_blueprint import UserProvidedFile

        grid_path = self._write_tiny_netcdf(tmp_path)
        gf = UserProvidedFile(location=str(grid_path), content_hash="not-the-real-hash")
        cfg = self._cfg_with_grid_file(minimal_cstar_spec_builder_args, tmp_path, gf)

        tmp = Path(tempfile.mkdtemp(prefix="forge-test-gridfile-mismatch-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        with pytest.warns(UserWarning, match="grid"):
            ForgeExecutor.from_forge_blueprint(cfg, host=host)

    def test_grid_file_rejects_nesting_kwargs(
        self, minimal_cstar_spec_builder_args, tmp_path
    ):
        """Defensive check on the executor itself: even though the ForgeBlueprint
        schema already forbids ``grid_file`` + nesting (``Domain``), the executor
        (a separate Pydantic model, fed by ``forge_blueprint_to_builder_kwargs``)
        raises too rather than silently building a nested grid from a
        user-supplied file.
        """
        from cstar_forge.forge.forge_blueprint import UserProvidedFile

        grid_path = self._write_tiny_netcdf(tmp_path)
        gf = UserProvidedFile(location=str(grid_path), content_hash="x" * 64)
        cfg = self._cfg_with_grid_file(minimal_cstar_spec_builder_args, tmp_path, gf)
        cfg = cfg.model_copy(
            update={
                "domain": cfg.domain.model_copy(
                    update={"grid_kwargs_parent": {"nx": 10, "ny": 10}}
                )
            }
        )

        tmp = Path(tempfile.mkdtemp(prefix="forge-test-gridfile-nesting-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        with pytest.raises(ValueError, match="nesting"):
            ForgeExecutor.from_forge_blueprint(cfg, host=host)


class TestForgeExecutorGetDs:
    """Tests for the get_ds method."""

    def test_get_ds_grid_from_roms_marbl_blueprint(
        self,
        minimal_cstar_spec_builder_args,
        sample_runtime_params,
        sample_model_params,
        tmp_path,
    ):
        """Test getting grid dataset from blueprint."""
        # Create a mock dataset file
        test_file = tmp_path / "test_grid.nc"
        test_file.touch()

        ic_file = tmp_path / "ic.nc"
        ic_file.touch()
        boundary_file = tmp_path / "boundary.nc"
        boundary_file.touch()
        surface_file = tmp_path / "surface.nc"
        surface_file.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        grid_dataset = cstar_models.Dataset(
            data=[Resource(location=str(test_file), partitioned=False)]
        )
        roms_marbl_blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.roms_marbl_blueprint.code,
            grid=grid_dataset,
            initial_conditions=cstar_models.Dataset(
                data=[Resource(location=str(ic_file), partitioned=False)]
            ),
            forcing=cstar_models.ForcingConfiguration(
                boundary=cstar_models.Dataset(
                    data=[Resource(location=str(boundary_file), partitioned=False)]
                ),
                surface=cstar_models.Dataset(
                    data=[Resource(location=str(surface_file), partitioned=False)]
                ),
            ),
            partitioning=minimal_cstar_spec_builder_args["partitioning"],
            model_params=sample_model_params,
            runtime_params=sample_runtime_params,
        )
        builder.roms_marbl_blueprint = roms_marbl_blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds

            result = builder.get_ds("grid", from_file=False)

            # get_ds returns a list of datasets
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] == mock_ds
            mock_open.assert_called_once_with(str(test_file), decode_timedelta=False)

    def test_get_ds_returns_none_when_roms_marbl_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that get_ds returns None when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = None

        result = builder.get_ds("grid", from_file=False)
        assert result is None

    def test_get_ds_returns_none_when_field_not_found(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that get_ds returns None when field doesn't exist."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        result = builder.get_ds("nonexistent_field", from_file=False)
        assert result is None

    def test_get_ds_forcing_surface(
        self,
        minimal_cstar_spec_builder_args,
        sample_runtime_params,
        sample_model_params,
        tmp_path,
    ):
        """Test getting forcing.surface dataset."""
        test_file = tmp_path / "test_surface.nc"
        test_file.touch()
        grid_file = tmp_path / "grid.nc"
        grid_file.touch()
        ic_file = tmp_path / "ic.nc"
        ic_file.touch()
        boundary_file = tmp_path / "boundary.nc"
        boundary_file.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        surface_dataset = cstar_models.Dataset(
            data=[Resource(location=str(test_file), partitioned=False)]
        )
        roms_marbl_blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.roms_marbl_blueprint.code,
            grid=cstar_models.Dataset(
                data=[Resource(location=str(grid_file), partitioned=False)]
            ),
            initial_conditions=cstar_models.Dataset(
                data=[Resource(location=str(ic_file), partitioned=False)]
            ),
            forcing=cstar_models.ForcingConfiguration(
                boundary=cstar_models.Dataset(
                    data=[Resource(location=str(boundary_file), partitioned=False)]
                ),
                surface=surface_dataset,
            ),
            partitioning=minimal_cstar_spec_builder_args["partitioning"],
            model_params=sample_model_params,
            runtime_params=sample_runtime_params,
        )
        builder.roms_marbl_blueprint = roms_marbl_blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds

            result = builder.get_ds("forcing.surface", from_file=False)

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] == mock_ds
            mock_open.assert_called_once_with(str(test_file), decode_timedelta=False)


class TestForgeExecutorEnsureSourceData:
    """Tests for the ensure_source_data method."""

    def test_ensure_source_data_raises_when_grid_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that ensure_source_data raises when grid is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.grid = None

        with pytest.raises(RuntimeError) as exc_info:
            builder.ensure_source_data()
        assert "Grid must be created" in str(exc_info.value)

    def test_ensure_source_data_calls_source_data_prepare_all(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that ensure_source_data calls SourceData.prepare_all."""
        with patch(
            "cstar_forge.forge.executor.source_data.SourceData"
        ) as mock_source_data_class:
            mock_source_data_instance = MagicMock()
            mock_source_data_class.return_value = mock_source_data_instance
            mock_source_data_instance.prepare_all.return_value = (
                mock_source_data_instance
            )

            builder = _make_builder(minimal_cstar_spec_builder_args)
            builder.ensure_source_data()

            mock_source_data_class.assert_called_once()
            mock_source_data_instance.prepare_all.assert_called_once_with(
                include_streamable=False
            )


class TestForgeExecutorGenerateInputs:
    """Tests for the generate_inputs method."""

    def test_generate_inputs_raises_when_roms_marbl_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that generate_inputs raises when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = None

        with pytest.raises(RuntimeError) as exc_info:
            builder.generate_inputs()
        assert "Blueprint must be initialized" in str(exc_info.value)


class TestForgeExecutorBuildAndRun:
    """Tests for configure_build."""

    def test_build_updates_compile_time_location(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() updates compile_time.location in blueprint."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        expected_code_output_dir = builder.compile_time_code_dir
        expected_location = str(expected_code_output_dir.resolve())

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": expected_location,
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            assert builder.roms_marbl_blueprint is not None
            assert builder.roms_marbl_blueprint.code is not None
            assert builder.roms_marbl_blueprint.code.compile_time is not None
            assert (
                builder.roms_marbl_blueprint.code.compile_time.location
                == expected_location
            )

    def test_build_persists_roms_marbl_blueprint(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() persists the blueprint to file -- the only
        time the executor writes it to disk.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        # Nothing is persisted before configure_build() runs.
        assert not builder.path_roms_marbl_blueprint().exists()

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            expected_bp_path = builder.path_roms_marbl_blueprint()
            assert expected_bp_path.exists()

            with open(expected_bp_path) as f:
                roms_marbl_blueprint_data = yaml.safe_load(f)
                assert roms_marbl_blueprint_data is not None
                assert "code" in roms_marbl_blueprint_data
                assert "compile_time" in roms_marbl_blueprint_data["code"]
                assert "location" in roms_marbl_blueprint_data["code"]["compile_time"]

    def test_configure_build_writes_exactly_one_blueprint_and_sidecar(
        self, minimal_cstar_spec_builder_args
    ):
        """No preconfig/postconfig/run stage artifacts are ever written -- the
        stages concept is gone. `roms_marbl_blueprint_dir` holds exactly one
        `B_{name}.yaml` and one `settings_B_{name}.yaml` after configure_build().
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }
            builder.configure_build()

        bp_files = sorted(builder.roms_marbl_blueprint_dir.glob("B_*.yaml"))
        settings_files = sorted(
            builder.roms_marbl_blueprint_dir.glob("settings_B_*.yaml")
        )
        assert bp_files == [builder.path_roms_marbl_blueprint()]
        assert settings_files == [
            builder.path_roms_marbl_blueprint().parent
            / f"settings_{builder.path_roms_marbl_blueprint().name}"
        ]
        for suffix in ("_preconfig", "_postconfig", "_run"):
            assert suffix not in bp_files[0].name

    def test_build_stages_compile_time_templates(self, minimal_cstar_spec_builder_args):
        """configure_build() stages the compile-time templates (via C-Star
        AdditionalCode) and renders from that staged directory.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            assert mock_render.called
            compile_time_calls = [
                call
                for call in mock_render.call_args_list
                if "compile_time" in str(call.kwargs.get("template_dir", ""))
            ]
            assert len(compile_time_calls) > 0
            template_dir = compile_time_calls[0].kwargs.get("template_dir")
            assert template_dir is not None
            assert "templates" in str(template_dir)
            # Staging really ran (offline, from the working tree): the template file
            # was materialized into the staged directory.
            assert (Path(template_dir) / "cppdefs.opt.j2").exists()

    @requires_cstar_pio
    def test_build_with_use_pio_emits_code_pio_and_model_param(
        self, minimal_cstar_spec_builder_args
    ):
        """With use_pio, the emitted RomsMarblBlueprint carries code.pio and
        model_params.use_pio: true.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args, use_pio=True)

        assert builder._use_pio is True

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

        bp_path = builder.path_roms_marbl_blueprint()
        with open(bp_path) as f:
            data = yaml.safe_load(f)
        assert data["model_params"]["use_pio"] is True
        assert data["code"]["pio"]["location"] == (
            "https://github.com/CWorthy-ocean/ParallelIO.git"
        )
        assert data["code"]["pio"]["commit"] == "2.7.1-fork"

    def test_build_without_use_pio_omits_pio(self, minimal_cstar_spec_builder_args):
        """Without use_pio, model_params has no use_pio key and code.pio is unset
        (keeps non-PIO blueprints loadable by main-branch C-Star).
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder._use_pio is False

        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

        bp_path = builder.path_roms_marbl_blueprint()
        with open(bp_path) as f:
            data = yaml.safe_load(f)
        assert "use_pio" not in data["model_params"]
        assert data["code"].get("pio") is None

    def test_configure_build_does_not_clobber_generated_river_and_tidal_settings(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """Regression for the §3a bug (docs/dev-notes/forge-blueprint-parameter-audit.md): before
        the fix, ``configure_build``'s overlay applied the *entire* stored
        ``ForgeBlueprint.model_settings`` snapshot on top of whatever ``generate_inputs``
        had just derived from the real generated forcing objects — silently reverting a
        correctly-generated river configuration back to "disabled" and a real tidal
        constituent count back to the merely-declared one.

        Drives the exact real call chain ``process_forge_blueprint`` uses
        (``split_model_settings(cfg)`` -> ``ForgeExecutor.configure_build``), with the
        ``generate_inputs`` step simulated (as input_data.py's ``_generate_river_forcing``/
        ``_generate_tidal_forcing`` would) rather than actually downloading/generating
        real forcing data.
        """
        from cstar_forge.forge.forge_blueprint_engine import split_model_settings
        from cstar_forge.forge_blueprint_resolve import build_forge_blueprint

        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-grid",
            grid_kwargs=sample_grid_kwargs,
            open_boundaries=sample_open_boundaries.model_dump(),
            partitioning=sample_partitioning.model_dump(),
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )
        assert cfg.forcing.river, "fixture must have a configured river for this test"
        # The ForcingSpec declares ntides=15; simulate a real TPXO extraction that
        # actually yields a different constituent count, to prove the *real* value wins.
        assert cfg.model_settings["tides"]["ntides"] == 15

        tmp = Path(tempfile.mkdtemp(prefix="forge-test-core-clobber-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)

        # Before the fix, this pre-generation snapshot is exactly what configure_build's
        # overlay would clobber the post-generation values back to.
        assert builder._settings_run_time["river_frc"]["river_source"] is False
        assert builder._settings_run_time["river_frc"]["nriv"] == 0

        # Simulate generate_inputs having derived the *actual* generated values.
        builder._update_settings_run_time(
            {
                "river_frc": {
                    "river_source": True,
                    "analytical": False,
                    "nriv": 3,
                    "rvol_vname": "river_volume",
                    "rvol_tname": "river_time",
                    "rtrc_vname": "river_tracer",
                    "rtrc_tname": "river_time",
                },
                "tides": {
                    "ntides": 7,
                    "bry_tides": True,
                    "pot_tides": True,
                    "ana_tides": False,
                },
            },
            allow_new=True,
        )

        run_ov, compile_ov = split_model_settings(cfg)
        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }
            builder.configure_build(
                compile_time_settings=compile_ov, run_time_settings=run_ov
            )

        assert builder._settings_run_time["river_frc"]["river_source"] is True
        assert builder._settings_run_time["river_frc"]["nriv"] == 3
        assert builder._settings_run_time["tides"]["ntides"] == 7

    def _cdr_cfg_and_builder(
        self,
        sample_grid_kwargs,
        sample_open_boundaries,
        sample_partitioning,
        **build_over,
    ):
        """Resolver cfg + executor for the CDR configure_build tests (same shape as
        the clobber test above).
        """
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-grid",
            grid_kwargs=sample_grid_kwargs,
            open_boundaries=sample_open_boundaries.model_dump(),
            partitioning=sample_partitioning.model_dump(),
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
            **build_over,
        )
        tmp = Path(tempfile.mkdtemp(prefix="forge-test-core-cdr-"))
        host = HostPaths(working_dir=tmp, source_data_cache=tmp, system="test")
        return cfg, host

    def _run_configure_build(self, cfg, host):
        from cstar_forge.forge.forge_blueprint_engine import split_model_settings

        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        run_ov, compile_ov = split_model_settings(cfg)
        with (
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
            patch("cstar_forge.forge.executor.write_roms_namelist"),
        ):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }
            builder.configure_build(
                compile_time_settings=compile_ov, run_time_settings=run_ov
            )
        return builder

    def test_configure_build_user_cdr_output_without_forcing(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """Pathway 1 (CDR output, no CDR forcing), in the wizard-override shape: the
        wizard applies accordion overrides *after* the resolver, so a stored blueprint
        can carry ``cdr_output.do_cdr_output=True`` alongside a stale
        ``cppdefs.cdr_forcing=False`` and a diagnostics list without the CDR names.
        ``configure_build``'s consistency net must force the CPP flag and the
        required MARBL diagnostics, while ``cdr_frc.cdr_source`` stays False (no
        forcing file exists for ROMS to open).
        """
        from cstar_forge.forge.namelist_model import (
            CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS,
        )

        cfg, host = self._cdr_cfg_and_builder(
            sample_grid_kwargs, sample_open_boundaries, sample_partitioning
        )
        # Post-resolve wizard-style toggle: runtime switch on, cppdefs/diagnostics
        # untouched (the resolver's consistency block never saw it).
        cfg.model_settings["cdr_output"]["do_cdr_output"] = True
        assert cfg.model_settings["cppdefs"]["cdr_forcing"] is False
        assert not set(CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS) & set(
            cfg.model_settings["marbl_bgc"]["marbl_diagnostics_to_write"]
        )

        builder = self._run_configure_build(cfg, host)

        assert builder._settings_run_time["cdr_output"]["do_cdr_output"] is True
        assert builder._settings_compile_time["cppdefs"]["cdr_forcing"] is True
        assert builder._settings_run_time["cdr_frc"]["cdr_source"] is False
        diags = builder._settings_run_time["marbl_bgc"]["marbl_diagnostics_to_write"]
        for name in CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS:
            assert diags.count(name) == 1, name

    def test_configure_build_rejects_cdr_output_without_marbl(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """The resolver's CDR-requires-MARBL guard re-checked at configure_build: a
        stored/hand-edited blueprint reaches here without re-resolving, and
        CDR_FORCING without MARBL must not silently bake into cppdefs.opt.
        """
        cfg, host = self._cdr_cfg_and_builder(
            sample_grid_kwargs, sample_open_boundaries, sample_partitioning
        )
        cfg.model_settings["cdr_output"]["do_cdr_output"] = True
        cfg.model_settings["cppdefs"]["marbl"] = False

        with pytest.raises(ValueError, match="requires MARBL"):
            self._run_configure_build(cfg, host)

    def test_configure_build_rejects_non_divisible_rst_period(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """The resolver's restart-period guard re-checked at configure_build: a
        stored/hand-edited blueprint reaches here without re-resolving, so a
        hand-mutated output_period_rst that no longer divides evenly by dt must
        still be caught before any build side effects.
        """
        cfg, host = self._cdr_cfg_and_builder(
            sample_grid_kwargs, sample_open_boundaries, sample_partitioning
        )
        assert cfg.model_settings["time_stepping"]["dt"] == 7200
        # Reassign rather than mutate in place: ocean_vars isn't in the ModelSpec's
        # own model_settings, so the resolver's deep-merge aliases this dict
        # straight from the (test-module-shared) OutputSpec dict -- mutating it
        # in place would leak into every other test using the default fixture.
        cfg.model_settings["ocean_vars"] = {
            **cfg.model_settings["ocean_vars"],
            "output_period_rst": 10000.0,
        }

        with pytest.raises(ValueError, match="output_period_rst"):
            self._run_configure_build(cfg, host)

    def test_configure_build_generated_cdr_forcing_wins_over_stored_disabled(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """Pathway 2 back-compat: a pre-rename blueprint stores
        ``do_cdr_output=False`` (migrated from the always-False ``do_cdr``) yet a CDR
        forcing IS configured/generated. Now that the overlay carries the stored
        value (``do_cdr_output`` left ``GENERATION_DERIVED_LEAF_KEYS``), the
        consistency net must re-assert output=True for the run that generated
        forcing -- the guarantee the old leaf-exclusion used to provide.
        """
        cfg, host = self._cdr_cfg_and_builder(
            sample_grid_kwargs,
            sample_open_boundaries,
            sample_partitioning,
            cdr_forcing={"enabled": True},
        )
        assert cfg.model_settings["cdr_output"]["do_cdr_output"] is True
        # Simulate the pre-rename stored shape.
        cfg.model_settings["cdr_output"]["do_cdr_output"] = False

        builder = self._run_configure_build(cfg, host)

        assert builder.cdr_forcing == {"enabled": True}
        assert builder._settings_run_time["cdr_output"]["do_cdr_output"] is True
        assert builder._settings_compile_time["cppdefs"]["cdr_forcing"] is True

    @pytest.mark.real_template_staging
    def test_template_repo_args_map_from_code_spec(
        self, minimal_cstar_spec_builder_args
    ):
        """The (unpatched) cfg->AdditionalCode-args mapping forwards the git ref from
        code.templates_* verbatim — the Forge side of the fetch that CI can't run live.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)

        for stage in ("compile_time", "run_time"):
            repo = getattr(builder.code_spec, f"templates_{stage}")
            args = builder._template_repo_args(stage)
            assert args["location"] == str(repo.location)
            assert args["subdir"] == (repo.directory or "")
            assert args["checkout_target"] == (repo.commit or repo.branch or "")
            assert args["files"] == list(repo.files)
        # Resolver default: github repo pinned at the ModelSpec code.templates_commit,
        # repo-root-relative directory.
        pinned = yaml.safe_load((_MODEL_DIR / "model.yaml").read_text())["code"][
            "templates_commit"
        ]
        ct = builder._template_repo_args("compile_time")
        assert ct["location"].endswith("cstar-forge.git")
        assert ct["subdir"] == "templates/compile-time"
        assert ct["checkout_target"] == pinned
        assert ct["files"] == ["cppdefs.opt.j2"]


class TestForgeExecutorPathRomsMarblBlueprint:
    """Tests for path_roms_marbl_blueprint method."""

    def test_path_roms_marbl_blueprint(self, minimal_cstar_spec_builder_args):
        """path_roms_marbl_blueprint returns the single B_{name}.yaml path -- there
        is no stage suffix and no stage/run_params arguments to pass.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_roms_marbl_blueprint()

        assert path == builder.roms_marbl_blueprint_dir / f"B_{builder.name}.yaml"
        assert path.suffix == ".yaml"


class TestForgeExecutorPersist:
    """Tests for persist method."""

    def test_persist_writes_blueprint_and_sidecar(
        self, minimal_cstar_spec_builder_args
    ):
        """persist() writes the single B_{name}.yaml plus its settings sidecar."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        builder.persist()

        bp_path = builder.path_roms_marbl_blueprint()
        assert bp_path.exists()

        with bp_path.open("r") as f:
            first_line = f.readline()
            f.seek(0)
            data = yaml.safe_load(f)
            assert data is not None
            assert "name" in data

        # "$schema" must travel as the yaml-language-server comment (the
        # canonical C-Star format), never as a document key -- a key would be
        # rejected as an extra field by extra="forbid" C-Star deserializers
        # that don't strip it before validating.
        assert "$schema" not in data
        assert first_line.startswith("# yaml-language-server: $schema=")

        settings_path = bp_path.parent / f"settings_{bp_path.name}"
        assert settings_path.exists()

    def test_persist_raises_when_roms_marbl_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test persist raises error when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = None

        with pytest.raises(ValueError) as exc_info:
            builder.persist()
        assert "blueprint is not initialized" in str(exc_info.value)


class TestValidatedRomsMarblBlueprint:
    """Tests for the emit-time validation gate (_validated_roms_marbl_blueprint).

    The blueprint is assembled with model_construct (placeholder stages are
    deliberately partial), so an extra field smuggled onto a cstar model --
    they are extra="forbid" -- would otherwise only fail when C-Star loads the
    persisted file. configure_build re-validates the final blueprint whenever
    generate_inputs has filled in real data.
    """

    def _complete_blueprint(self, builder, tmp_path, runtime_params_extra=None):
        """Overlay real-looking data on the placeholder blueprint, mimicking
        what generate_inputs + configure_build assemble.
        """
        placeholder_file = tmp_path / "input.nc"
        placeholder_file.touch()
        ds = cstar_models.Dataset(
            data=[Resource(location=str(placeholder_file), partitioned=False)]
        )
        bp_dict = builder.roms_marbl_blueprint.model_dump()
        bp_dict.pop("$schema", None)
        bp_dict.update(
            grid=ds,
            initial_conditions=ds,
            forcing=cstar_models.ForcingConfiguration(boundary=ds, surface=ds),
            model_params={"time_step": 60},
            runtime_params={
                "start_date": builder.start_date,
                "end_date": builder.end_date,
                **(runtime_params_extra or {}),
            },
        )
        return cstar_models.RomsMarblBlueprint.model_construct(**bp_dict)

    def test_validated_blueprint_round_trips(
        self, minimal_cstar_spec_builder_args, tmp_path
    ):
        """A complete blueprint validates and comes back as a real (validated)
        RomsMarblBlueprint instance.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = self._complete_blueprint(builder, tmp_path)

        validated = builder._validated_roms_marbl_blueprint()

        assert isinstance(validated, cstar_models.RomsMarblBlueprint)
        assert validated.runtime_params.start_date == builder.start_date
        assert validated.model_params.time_step == 60

    def test_validated_blueprint_rejects_smuggled_extra_field(
        self, minimal_cstar_spec_builder_args, tmp_path
    ):
        """An undeclared key on a cstar sub-model (the old runtime_params.output_dir
        bug) fails at emit time with a clear error, not at C-Star load time.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = self._complete_blueprint(
            builder, tmp_path, runtime_params_extra={"output_dir": str(tmp_path)}
        )

        with pytest.raises(ValueError) as exc_info:
            builder._validated_roms_marbl_blueprint()
        assert "does not validate against the installed C-Star" in str(exc_info.value)
        assert "output_dir" in str(exc_info.value)


class TestForgeExecutorDefaultRuntimeParams:
    """Tests for default_runtime_params property."""

    def test_default_runtime_params(self, minimal_cstar_spec_builder_args):
        """Test default_runtime_params property (dates only; the run output
        location travels on the blueprint ``working_dir``, not runtime_params).
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        runtime_params = builder.default_runtime_params

        assert runtime_params.start_date == builder.start_date
        assert runtime_params.end_date == builder.end_date
        # output_dir is a pre-2.0.0 field: it must not be emitted (the cstar
        # models are extra="forbid").
        assert "output_dir" not in runtime_params.model_dump()


class TestForgeExecutorRomsBlueprintWorkingDir:
    """Tests for roms_blueprint_working_dir property."""

    def test_swaps_cstar_forge_run_segment(self, minimal_cstar_spec_builder_args):
        """When run_output_dir has the known cstar-forge-run root, the blueprint
        working dir is the sibling cstar-roms-run root, name preserved.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        run_dir = Path("/home/user/cstar-forge-run/my_run_name")
        builder.host = HostPaths(
            working_dir=run_dir,
            source_data_cache=builder.host.source_data_cache,
            system="test",
        )

        assert builder.roms_blueprint_working_dir == Path(
            "/home/user/cstar-roms-run/my_run_name"
        )

    def test_falls_back_to_subdir_when_unrecognized(
        self, minimal_cstar_spec_builder_args
    ):
        """When run_output_dir doesn't contain the known cstar-forge-run segment,
        fall back to a cstar-roms-run subdirectory under it.
        """
        builder = _make_builder(minimal_cstar_spec_builder_args)
        run_dir = Path("/custom/spot")
        builder.host = HostPaths(
            working_dir=run_dir,
            source_data_cache=builder.host.source_data_cache,
            system="test",
        )

        assert builder.roms_blueprint_working_dir == Path("/custom/spot/cstar-roms-run")

    def test_forge_and_roms_roots_are_siblings_with_matching_name(
        self, minimal_cstar_spec_builder_args
    ):
        """The forge run root and the emitted-blueprint run root should always be
        siblings sharing the run name, derived from DEFAULT_WORKING_ROOT and
        ROMS_RUN_SEGMENT -- this guards against those two constants drifting apart.
        """
        from cstar_forge import config as forge_config

        merged = minimal_cstar_spec_builder_args
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name=merged["grid_name"],
            grid_kwargs=merged["grid_kwargs"],
            open_boundaries=merged["open_boundaries"].model_dump(),
            partitioning=merged["partitioning"].model_dump(),
            start_date=merged["start_date"],
            end_date=merged["end_date"],
            name="my_sibling_run",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )

        host = forge_config.resolve_host(cfg.working_dir)
        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)

        # The two working dirs must never collapse into the same directory (the
        # failure mode the old fragile .replace() risked).
        assert builder.roms_blueprint_working_dir != builder.run_output_dir
        # ...but they are siblings, sharing the grandparent (host root) and name.
        assert (
            builder.roms_blueprint_working_dir.parent.parent
            == builder.run_output_dir.parent.parent
        )
        assert (
            builder.roms_blueprint_working_dir.name
            == builder.run_output_dir.name
            == "my_sibling_run"
        )


class TestCaptureOutput:
    """Tests for cstar_forge.run._capture_output (tees print + logging into
    <working_dir>/logs/forge_<timestamp>.log, in addition to the existing screen
    output).
    """

    def test_tees_print_and_logging_into_log_file(self, tmp_path, capsys):
        test_logger = logging.getLogger("cstar_forge.test_capture_output")
        with forge_run._capture_output(tmp_path, verbose=False) as log_path:
            print("hello from print")
            test_logger.info("hello from logging")

        assert log_path.parent == tmp_path / "logs"
        assert log_path.exists()
        content = log_path.read_text()
        assert "hello from print" in content
        assert "hello from logging" in content

        # Screen output is teed, not redirected -- print() still shows on screen.
        assert "hello from print" in capsys.readouterr().out

    def test_restores_streams_and_logger_levels_on_exit(self, tmp_path):
        old_out, old_err = sys.stdout, sys.stderr
        prev_level = logging.getLogger("cstar_forge").level
        root_handlers_before = list(logging.getLogger().handlers)
        try:
            with forge_run._capture_output(tmp_path):
                assert sys.stdout is not old_out
                assert sys.stderr is not old_err
        finally:
            logging.getLogger("cstar_forge").setLevel(prev_level)

        assert sys.stdout is old_out
        assert sys.stderr is old_err
        assert logging.getLogger("cstar_forge").level == prev_level
        # The file handler added for the run is removed again -- no leak onto root.
        assert logging.getLogger().handlers == root_handlers_before

    def test_lowers_info_level_so_app_path_logs_reach_the_file(self, tmp_path):
        """The C-Star app path never calls logging.basicConfig, so the whole
        cstar_forge.* hierarchy sits at NOTSET and inherits root's default
        (WARNING) -- _capture_output must lower the cstar_forge logger itself or
        an INFO message never reaches the file.
        """
        parent = logging.getLogger("cstar_forge")
        child = logging.getLogger("cstar_forge.test_capture_output")
        prev_parent_level, prev_child_level = parent.level, child.level
        parent.setLevel(logging.NOTSET)
        child.setLevel(logging.NOTSET)
        try:
            # Sanity: unconfigured, INFO wouldn't clear root's WARNING default.
            assert child.getEffectiveLevel() >= logging.WARNING
            with forge_run._capture_output(tmp_path, verbose=False) as log_path:
                child.info("info-level message")
        finally:
            parent.setLevel(prev_parent_level)
            child.setLevel(prev_child_level)

        assert "info-level message" in log_path.read_text()

    def test_late_write_through_tee_after_exit_does_not_raise(self, tmp_path, capsys):
        """Something (a logging handler, tqdm, a background thread) can grab
        sys.stdout/sys.stderr by reference while the block is open and keep writing
        through it afterwards. That write must not blow up on the now-closed log
        file -- it should just reach the real screen stream.
        """
        with forge_run._capture_output(tmp_path) as log_path:
            tee = sys.stdout

        tee.write("late write\n")
        tee.flush()

        assert "late write" in capsys.readouterr().out
        assert "late write" not in log_path.read_text()

    def test_escaping_exception_traceback_is_written_to_log(self, tmp_path):
        """An exception that escapes the block is pretty-printed by typer/rich only
        after the capture context has restored stderr and closed the file, so
        _capture_output itself must record the traceback before re-raising.
        """
        with pytest.raises(ValueError, match="boom"):
            with forge_run._capture_output(tmp_path) as log_path:
                raise ValueError("boom")

        content = log_path.read_text()
        assert "Traceback (most recent call last)" in content
        assert "ValueError: boom" in content

    def test_logging_handler_created_during_capture_survives_exit(
        self, tmp_path, capsys
    ):
        """Mimics cstar.base.log.get_logger, which lazily attaches a
        logging.StreamHandler(sys.stdout) to a logger the first time it's used.
        If that first use happens inside the capture block, the handler captures
        the tee and must not error (as '--- Logging error ---' on stderr) when the
        logger is used again after the block exits.
        """
        logger = logging.getLogger("cstar_forge.test_late_handler")
        handler = None
        try:
            with forge_run._capture_output(tmp_path):
                handler = logging.StreamHandler(sys.stdout)
                logger.addHandler(handler)
                logger.warning("during capture")

            logger.warning("after capture")

            assert "--- Logging error ---" not in capsys.readouterr().err
        finally:
            if handler is not None:
                logger.removeHandler(handler)


class TestProcessCapturesRunOutput:
    """Verifies cstar_forge.run.process wraps the engine call in _capture_output, so
    both entry points (CLI ``main()`` and the C-Star app's ``ForgeRunner`` -> process())
    get a per-run log under the resolved host's working_dir.
    """

    def test_process_writes_log_under_host_working_dir(
        self,
        tmp_path,
        sample_grid_kwargs,
        sample_open_boundaries,
        sample_partitioning,
    ):
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-grid",
            grid_kwargs=sample_grid_kwargs,
            open_boundaries=sample_open_boundaries.model_dump(),
            partitioning=sample_partitioning.model_dump(),
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            description="capture-output test",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )

        captured_host = {}

        def fake_process_forge_blueprint(spec, *, host=None, **kwargs):
            logging.getLogger("cstar_forge.fake_engine").info("engine ran")
            captured_host["host"] = host
            return object()

        with patch(
            "cstar_forge.run.process_forge_blueprint",
            side_effect=fake_process_forge_blueprint,
        ):
            forge_run.process(cfg, working_dir=str(tmp_path))

        host = captured_host["host"]
        log_files = list((Path(host.working_dir) / "logs").glob("forge_*.log"))
        assert len(log_files) == 1
        assert "engine ran" in log_files[0].read_text()


class TestForgeExecutorGenerateInputsComprehensive:
    """Comprehensive tests for generate_inputs method covering full workflow."""

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_creates_input_data_instance(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs creates RomsMarblInputData with correct parameters."""
        mock_input_data_instance = MagicMock()
        mock_roms_marbl_blueprint_elements = MagicMock()
        mock_roms_marbl_blueprint_elements.grid = MagicMock()
        mock_roms_marbl_blueprint_elements.initial_conditions = MagicMock()
        mock_roms_marbl_blueprint_elements.forcing = MagicMock()
        mock_roms_marbl_blueprint_elements.cdr_forcing = None
        mock_input_data_instance.generate_all.return_value = (
            mock_roms_marbl_blueprint_elements,
            {},
            {},
        )
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            builder = _make_builder(minimal_cstar_spec_builder_args)
            builder.generate_inputs(clobber=True, test=True)

            mock_input_data_class.assert_called_once()
            call_kwargs = mock_input_data_class.call_args[1]
            assert call_kwargs["domain_name"] == builder.name
            assert call_kwargs["start_date"] == builder.start_date
            assert call_kwargs["end_date"] == builder.end_date
            assert call_kwargs["use_pio"] is False

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    @requires_cstar_pio
    def test_generate_inputs_use_pio_forwards_use_pio(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """With use_pio, RomsMarblInputData is told to do its own CDF-5 conversion."""
        mock_input_data_instance = MagicMock()
        mock_roms_marbl_blueprint_elements = MagicMock()
        mock_roms_marbl_blueprint_elements.grid = MagicMock()
        mock_roms_marbl_blueprint_elements.initial_conditions = MagicMock()
        mock_roms_marbl_blueprint_elements.forcing = MagicMock()
        mock_roms_marbl_blueprint_elements.cdr_forcing = None
        mock_input_data_instance.generate_all.return_value = (
            mock_roms_marbl_blueprint_elements,
            {},
            {},
        )
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            builder = _make_builder(minimal_cstar_spec_builder_args, use_pio=True)
            builder.generate_inputs(clobber=True, test=True)

            call_kwargs = mock_input_data_class.call_args[1]
            assert call_kwargs["use_pio"] is True

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_does_not_persist(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """generate_inputs() only updates the in-memory blueprint -- it never persists
        (regardless of the `test` flag). The blueprint is written to disk exactly
        once, at the end of configure_build().
        """
        mock_input_data_instance = MagicMock()
        mock_roms_marbl_blueprint_elements = MagicMock()
        mock_roms_marbl_blueprint_elements.grid = MagicMock()
        mock_roms_marbl_blueprint_elements.initial_conditions = MagicMock()
        mock_roms_marbl_blueprint_elements.forcing = MagicMock()
        mock_roms_marbl_blueprint_elements.cdr_forcing = None
        mock_input_data_instance.generate_all.return_value = (
            mock_roms_marbl_blueprint_elements,
            {},
            {},
        )
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            builder = _make_builder(minimal_cstar_spec_builder_args)

            with patch(
                "cstar_forge.forge.executor.ForgeExecutor.persist"
            ) as mock_persist:
                builder.generate_inputs(clobber=True, test=True)

                mock_persist.assert_not_called()

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_raises_when_roms_marbl_blueprint_elements_none(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs raises RuntimeError when roms_marbl_blueprint_elements is None."""
        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (None, {}, {})
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            builder = _make_builder(minimal_cstar_spec_builder_args)

            with pytest.raises(RuntimeError) as exc_info:
                builder.generate_inputs(clobber=True)
            assert "_settings_compile_time" in str(
                exc_info.value
            ) or "Blueprint mismatch" in str(exc_info.value)

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_nesting_info_serialized_to_roms_marbl_blueprint_dict(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
        tmp_path,
    ):
        """Test that nesting_info from roms_marbl_blueprint_elements is written into the blueprint dict."""
        nesting_file = tmp_path / "nesting.nc"
        nesting_file.touch()
        nesting_dataset = cstar_models.Dataset(
            data=[Resource(location=str(nesting_file), partitioned=False)]
        )

        mock_roms_marbl_blueprint_elements = MagicMock()
        mock_roms_marbl_blueprint_elements.grid = MagicMock()
        mock_roms_marbl_blueprint_elements.grid.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.initial_conditions = MagicMock()
        mock_roms_marbl_blueprint_elements.initial_conditions.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.forcing = MagicMock()
        mock_roms_marbl_blueprint_elements.forcing.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.cdr_forcing = None
        mock_roms_marbl_blueprint_elements.nesting_info = nesting_dataset

        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (
            mock_roms_marbl_blueprint_elements,
            {},
            {},
        )
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            with patch("cstar_forge.forge.executor.ForgeExecutor.persist"):
                builder = _make_builder(minimal_cstar_spec_builder_args)
                # Manually set settings so the guard passes
                builder._settings_compile_time = {"cppdefs": {}}
                builder._settings_run_time = {"time_stepping": {}}

                builder.generate_inputs(clobber=True, test=False)

            # blueprint is built via model_construct (no validation), so nesting_info
            # is the raw dict from model_dump(), not a Dataset instance
            nesting_info = builder.roms_marbl_blueprint.nesting_info
            assert nesting_info is not None
            assert nesting_info["data"][0]["location"] == str(nesting_file)

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_nesting_info_none_in_roms_marbl_blueprint_dict(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
        tmp_path,
    ):
        """Test that nesting_info is None in blueprint when elements.nesting_info is None."""
        mock_roms_marbl_blueprint_elements = MagicMock()
        mock_roms_marbl_blueprint_elements.grid = MagicMock()
        mock_roms_marbl_blueprint_elements.grid.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.initial_conditions = MagicMock()
        mock_roms_marbl_blueprint_elements.initial_conditions.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.forcing = MagicMock()
        mock_roms_marbl_blueprint_elements.forcing.model_dump.return_value = {}
        mock_roms_marbl_blueprint_elements.cdr_forcing = None
        mock_roms_marbl_blueprint_elements.nesting_info = None

        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (
            mock_roms_marbl_blueprint_elements,
            {},
            {},
        )
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            with patch("cstar_forge.forge.executor.ForgeExecutor.persist"):
                builder = _make_builder(minimal_cstar_spec_builder_args)
                builder._settings_compile_time = {"cppdefs": {}}
                builder._settings_run_time = {"time_stepping": {}}

                builder.generate_inputs(clobber=True, test=False)

            assert builder.roms_marbl_blueprint.nesting_info is None


class TestForgeExecutorGetDsComprehensive:
    """Comprehensive tests for get_ds method."""

    def test_get_ds_returns_list(
        self, minimal_cstar_spec_builder_args, sample_model_params, tmp_path
    ):
        """Test get_ds returns list of datasets."""
        test_file1 = tmp_path / "test1.nc"
        test_file1.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        boundary_dataset = cstar_models.Dataset(
            data=[Resource(location=str(test_file1), partitioned=False)]
        )
        roms_marbl_blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.roms_marbl_blueprint.code,
            grid=_create_empty_dataset(tmp_path),
            initial_conditions=_create_empty_dataset(tmp_path),
            forcing=cstar_models.ForcingConfiguration(
                boundary=boundary_dataset,
                surface=_create_empty_dataset(tmp_path),
            ),
            partitioning=minimal_cstar_spec_builder_args["partitioning"],
            model_params=sample_model_params,
            runtime_params=cstar_models.RuntimeParameterSet(
                start_date=datetime(2012, 1, 1),
                end_date=datetime(2012, 1, 2),
                checkpoint_frequency="1d",
            ),
        )
        builder.roms_marbl_blueprint = roms_marbl_blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds1 = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds1

            result = builder.get_ds("forcing.boundary", from_file=False)

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] == mock_ds1
            assert mock_open.call_count == 1

    def test_get_ds_returns_none_when_no_locations(
        self, minimal_cstar_spec_builder_args, sample_model_params, tmp_path
    ):
        """Test get_ds propagates FileNotFoundError when file doesn't exist."""
        placeholder_file = tmp_path / "placeholder_grid.nc"
        placeholder_file.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        grid_dataset = cstar_models.Dataset(
            data=[Resource(location=str(placeholder_file), partitioned=False)]
        )
        roms_marbl_blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.roms_marbl_blueprint.code,
            grid=grid_dataset,
            initial_conditions=_create_empty_dataset(tmp_path),
            forcing=cstar_models.ForcingConfiguration(
                boundary=_create_empty_dataset(tmp_path),
                surface=_create_empty_dataset(tmp_path),
            ),
            partitioning=minimal_cstar_spec_builder_args["partitioning"],
            model_params=sample_model_params,
            runtime_params=cstar_models.RuntimeParameterSet(
                start_date=datetime(2012, 1, 1),
                end_date=datetime(2012, 1, 2),
                checkpoint_frequency="1d",
            ),
        )
        builder.roms_marbl_blueprint = roms_marbl_blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_open.side_effect = FileNotFoundError("File not found")
            with pytest.raises(FileNotFoundError):
                builder.get_ds("grid", from_file=False)

    def test_get_ds_filters_none_locations(
        self, minimal_cstar_spec_builder_args, sample_model_params, tmp_path
    ):
        """Test get_ds filters out resources with None location."""
        test_file = tmp_path / "test.nc"
        test_file.touch()

        builder = _make_builder(minimal_cstar_spec_builder_args)

        grid_dataset = cstar_models.Dataset(
            data=[Resource(location=str(test_file), partitioned=False)]
        )
        roms_marbl_blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.roms_marbl_blueprint.code,
            grid=grid_dataset,
            initial_conditions=_create_empty_dataset(tmp_path),
            forcing=cstar_models.ForcingConfiguration(
                boundary=_create_empty_dataset(tmp_path),
                surface=_create_empty_dataset(tmp_path),
            ),
            partitioning=minimal_cstar_spec_builder_args["partitioning"],
            model_params=sample_model_params,
            runtime_params=cstar_models.RuntimeParameterSet(
                start_date=datetime(2012, 1, 1),
                end_date=datetime(2012, 1, 2),
                checkpoint_frequency="1d",
            ),
        )
        builder.roms_marbl_blueprint = roms_marbl_blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds

            result = builder.get_ds("grid", from_file=False)

            assert len(result) == 1
            mock_open.assert_called_once_with(str(test_file), decode_timedelta=False)


class TestDeepMergeSettingsDict:
    """Regression tests for recursive run/compile settings merge."""

    def test_preserves_sibling_keys_under_time_stepping(self):
        """Verify that merging dictionaries does not remove/not copy any upstream dict entries"""
        target = {
            "time_stepping": {
                "ntimes": 100,
                "dt": 60,
                "ndtfast": 30,
                "ninfo": 1,
            },
        }
        update = {"time_stepping": {"dt": 1800}}
        _deep_merge_settings_dict(target, update)
        ts = target["time_stepping"]
        assert ts["dt"] == 1800
        assert ts["ntimes"] == 100
        assert ts["ndtfast"] == 30
        assert ts["ninfo"] == 1

    def test_preserves_sibling_keys_under_forcing(self):
        """Verify that merging dictionaries does not replace the shared ancestor"""
        target = {
            "forcing": {
                "surface_forcing_path": "/a",
                "boundary_forcing_path": "/b",
            },
        }
        update = {"forcing": {"surface_forcing_path": "/c"}}
        _deep_merge_settings_dict(target, update)
        f = target["forcing"]
        assert f["surface_forcing_path"] == "/c"
        assert f["boundary_forcing_path"] == "/b"

    def test_non_dict_replaces_existing(self):
        """Verify that a non-dict value replaces an existing dict value"""
        target = {"blk": {"nested": {"x": 1}}}
        _deep_merge_settings_dict(target, {"blk": {"nested": "scalar"}})
        assert target["blk"]["nested"] == "scalar"


class TestGoldenNamelist:
    """Byte-level golden test for the rendered ``namelist.nml``.

    This is the deterministic, mocked-forcing golden referenced in the Follow-ups
    section of ``docs/dev-notes/forge-blueprint-parameter-audit.md`` and in
    ``docs/architecture-details.md`` Sec 6 — it is NOT the real-generated-data integration
    test those docs separately name as still deferred (this one mocks every
    roms-tools construction class; a real run against GLORYS/ERA5/TPXO/DAI data is a
    different, heavier test that doesn't exist yet).

    It drives the real ``ForgeExecutor.generate_inputs()`` -> ``configure_build()``
    chain (real ``write_roms_namelist``), mocking out only the roms-tools
    construction classes (grid geometry + river/tidal/CDR derived counts are given
    concrete, non-placeholder values via the mocks). That means it exercises exactly
    the settings-merge path the §3a fix (``GENERATION_DERIVED_LEAF_KEYS`` /
    ``split_model_settings`` in ``forge_blueprint_engine.py``) protects: river/CDR/tides
    values must reach the namelist as their *generated* values, not the resolver's
    placeholders — so this test doubles as the byte-level proof of that fix.

    To regenerate the fixture after an intentional schema/default/template change,
    rerun with ``UPDATE_GOLDEN=1`` set, review the resulting diff, and commit it.
    """

    _GRID_KWARGS: ClassVar[dict] = dict(
        nx=6,
        ny=2,
        size_x=500,
        size_y=1000,
        center_lon=0,
        center_lat=55,
        rot=10,
        N=3,
        theta_s=5.0,
        theta_b=2.0,
        hc=250.0,
    )
    _BOUNDARIES: ClassVar[dict] = {
        "south": False,
        "east": True,
        "north": True,
        "west": False,
    }
    _PARTITIONING: ClassVar[dict] = {"n_procs_x": 1, "n_procs_y": 1}
    _CDR_FORCING: ClassVar[dict] = {"enabled": True}

    @staticmethod
    def _touch_save(path, **_kw):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).touch()
        return path

    @staticmethod
    def _touch_save_list(path, **_kw):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).touch()
        return [path]

    @staticmethod
    def _mock_source_data(tmp_path):
        """A SourceData stand-in covering every source name the glorys-era5-unified
        ForcingSpec references (GLORYS/UNIFIED/ERA5/TPXO/DAI/MBL_co2/WOA); mirrors
        ``sample_source_data`` in tests/test_input_data.py.
        """
        mock_sd = MagicMock()
        source_file = tmp_path / "source.nc"
        source_file.touch()

        def _dks(name, glorys_layout=None):
            if name == "GLORYS":
                return (
                    "GLORYS_GLOBAL" if glorys_layout == "global" else "GLORYS_REGIONAL"
                )
            return {
                "UNIFIED": "UNIFIED_BGC",
                "ERA5": "ERA5",
                "TPXO": "TPXO",
                "DAI": "DAI",
            }.get(name, name.upper())

        mock_sd.path_for_source = MagicMock(return_value=source_file)
        mock_sd.dataset_key_for_source = MagicMock(side_effect=_dks)
        mock_sd.streamable_for_source = MagicMock(
            side_effect=lambda name, glorys_layout=None: name.upper() in {"ERA5", "DAI"}
        )
        return mock_sd

    def _run_golden_namelist_case(
        self, mock_grid, tmp_path, model_dir, golden_filename
    ) -> str:
        """Shared body for the golden namelist tests: drives the real
        ``generate_inputs()`` -> ``configure_build()`` chain against ``model_dir``
        and diffs the rendered ``namelist.nml`` against
        ``tests/fixtures/<golden_filename>``.

        Factored out (rather than ``pytest.mark.parametrize``) so
        ``UPDATE_GOLDEN=1 pytest -k <test name>`` regenerates exactly one fixture
        at a time -- the two golden tests stay independently selectable and the
        pre-existing fixture is never at risk from a run targeting the other.

        Returns the normalized rendered namelist text (workdir paths replaced by
        ``<WORKDIR>``) so callers can layer additional assertions on top of the
        byte-for-byte golden comparison.
        """
        cfg = build_forge_blueprint(
            model_dir=model_dir,
            grid_name="test-tiny",
            grid_kwargs=self._GRID_KWARGS,
            open_boundaries=self._BOUNDARIES,
            partitioning=self._PARTITIONING,
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            description="Golden namelist test",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
            cdr_forcing=self._CDR_FORCING,
            # Explicit False (matches cson_roms-marbl_v0.1's own default) so both
            # golden cases stay decoupled from PIO: roms-marbl-0.5-default bakes in
            # use_pio: true, which would otherwise route grid generation through
            # the real ``nccopy`` CDF-5 conversion (_pio_finalize) -- unrelated to
            # the versioned-namelist behavior this test targets, and incompatible
            # with the mocked (empty-file) grid.save() used here.
            use_pio=False,
        )

        grid_mock = _create_grid_mock()
        grid_mock.nx = self._GRID_KWARGS["nx"]
        grid_mock.ny = self._GRID_KWARGS["ny"]
        grid_mock.N = self._GRID_KWARGS["N"]
        grid_mock.theta_s = self._GRID_KWARGS["theta_s"]
        grid_mock.theta_b = self._GRID_KWARGS["theta_b"]
        grid_mock.hc = self._GRID_KWARGS["hc"]
        grid_mock.save.side_effect = self._touch_save
        mock_grid.return_value = grid_mock

        run_dir = tmp_path / "run"
        host = HostPaths(
            working_dir=run_dir,
            source_data_cache=run_dir,
            system="test",
        )
        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        builder.src_data = self._mock_source_data(tmp_path)

        with (
            patch("cstar_forge.forge.input_data.rt.InitialConditions") as mock_ic,
            patch("cstar_forge.forge.input_data.rt.SurfaceForcing") as mock_surface,
            patch("cstar_forge.forge.input_data.rt.BoundaryForcing") as mock_boundary,
            patch("cstar_forge.forge.input_data.rt.TidalForcing") as mock_tidal,
            patch("cstar_forge.forge.input_data.rt.RiverForcing") as mock_river,
            patch("cstar_forge.forge.input_data.rt.CDRForcing") as mock_cdr,
            patch(
                "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES",
                {"ERA5"},
            ),
        ):
            mock_ic_instance = MagicMock()
            mock_ic_instance.save.side_effect = self._touch_save_list
            mock_ic.return_value = mock_ic_instance

            mock_surface_instance = MagicMock()
            mock_surface_instance.save.side_effect = self._touch_save
            mock_surface_instance.use_coarse_grid = False
            mock_surface.return_value = mock_surface_instance

            mock_boundary_instance = MagicMock()
            mock_boundary_instance.save.side_effect = self._touch_save
            mock_boundary.return_value = mock_boundary_instance

            mock_tidal_instance = MagicMock()
            mock_tidal_instance.save.side_effect = self._touch_save
            mock_tidal_instance.ntides = 15
            mock_tidal.return_value = mock_tidal_instance

            mock_river_instance = MagicMock()
            mock_river_instance.save.side_effect = self._touch_save
            mock_river_instance.ds = xr.Dataset(
                {
                    "river_volume": (["nriver", "time"], np.zeros((3, 2))),
                    "river_tracer": (
                        ["nriver", "time", "tracer"],
                        np.zeros((3, 2, 2)),
                    ),
                }
            )
            mock_river.return_value = mock_river_instance

            mock_cdr_instance = MagicMock()
            mock_cdr_instance.save.side_effect = self._touch_save
            mock_releases = MagicMock()
            mock_releases.__len__.return_value = 2
            mock_releases.release_type = "volume"
            mock_cdr_instance.releases = mock_releases
            mock_cdr.return_value = mock_cdr_instance

            builder.generate_inputs(clobber=True, use_dask=False, test=False)

        # The §3a fix's whole point: these generation-derived values must survive
        # configure_build's overlay, not get reverted to resolver-time placeholders.
        assert builder._settings_run_time["river_frc"]["nriv"] == 3
        assert builder._settings_run_time["tides"]["ntides"] == 15
        assert builder._settings_run_time["cdr_frc"]["ncdr_parm"] == 2
        assert builder._settings_run_time["cdr_output"]["do_cdr_output"] is True

        from cstar_forge.forge.forge_blueprint_engine import split_model_settings

        run_ov, compile_ov = split_model_settings(cfg)
        with patch("cstar_forge.forge.executor.render_roms_settings") as mock_render:
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["cppdefs.opt"]},
                "branch": "main",
            }
            builder.configure_build(
                compile_time_settings=compile_ov, run_time_settings=run_ov
            )

        assert builder._settings_run_time["river_frc"]["nriv"] == 3
        assert builder._settings_run_time["tides"]["ntides"] == 15
        assert builder._settings_run_time["cdr_frc"]["ncdr_parm"] == 2

        namelist_path = builder.run_time_code_dir / "namelist.nml"
        assert namelist_path.exists()
        raw = namelist_path.read_text()

        # Host-rooted absolute paths (grdname/inifile/frcfiles/output_root_name/...)
        # are the only non-deterministic content; normalize both the raw and the
        # OS-resolved (e.g. macOS /private-prefixed) forms of the working dir.
        normalized = raw.replace(str(run_dir.resolve()), "<WORKDIR>").replace(
            str(run_dir), "<WORKDIR>"
        )

        golden_path = (
            Path(cstar_forge.__file__).parents[1]
            / "tests"
            / "fixtures"
            / golden_filename
        )

        if os.environ.get("UPDATE_GOLDEN"):
            golden_path.write_text(normalized)
            pytest.fail(
                f"UPDATE_GOLDEN=1: wrote {golden_path}. Review the diff and commit "
                "it, then rerun without UPDATE_GOLDEN to confirm the test passes."
            )

        golden = golden_path.read_text()
        assert normalized == golden, (
            f"Rendered namelist.nml drifted from tests/fixtures/{golden_filename}. "
            "If this is an intentional schema/default/template change, regenerate "
            f"with UPDATE_GOLDEN=1 pytest tests/test_core.py -k <this test name>, "
            "review the diff, and commit the updated fixture; otherwise this is a "
            "regression."
        )
        return normalized

    def test_golden_namelist_test_tiny(self, mock_grid, tmp_path):
        self._run_golden_namelist_case(
            mock_grid, tmp_path, _MODEL_DIR, "golden_namelist_test-tiny.nml"
        )

    def test_golden_namelist_test_tiny_roms050(self, mock_grid, tmp_path):
        """Same test-tiny domain/forcing/output, but resolved against the
        ``roms-marbl-0.5-default`` ModelSpec (ucla-roms >= 0.5.0) -- proves the
        versioned namelist path end to end: ``configure_build`` threads
        ``code_spec.roms.commit`` ("0.5.0") through to ``write_roms_namelist``,
        which selects ``RunTimeSettingsV0_5_0``/``RomsNamelistV0_5_0``.

        The two schema-visible differences from the < 0.5.0 golden are: no
        ``nrpf_rst`` in ``&basic_output_settings``, and
        ``output_period_particles``/``nrpf_particles`` (not ``output_period``/
        ``nrpf``) in ``&particles_settings``.
        """
        normalized = self._run_golden_namelist_case(
            mock_grid,
            tmp_path,
            _MODEL_DIR_ROMS050,
            "golden_namelist_test-tiny-roms050.nml",
        )

        basic_output = normalized.split("&basic_output_settings")[1].split("/", 1)[0]
        assert "nrpf_rst" not in basic_output

        particles = normalized.split("&particles_settings")[1].split("/", 1)[0]
        assert "output_period_particles" in particles
        assert "nrpf_particles" in particles
        assert "output_period =" not in particles
        assert "nrpf =" not in particles


class TestChildDomainNoInitialConditionsValidatesAtEmit:
    """End-to-end proof that a child domain with no explicit initial conditions
    (``Forcing.initial_conditions=None``; see
    ``forge_blueprint_resolve._build_forcing``) survives the real
    ``generate_inputs()`` -> ``configure_build()`` chain (the same
    roms-tools-mocked chain ``TestGoldenNamelist`` drives) and emits a
    schema-valid roms_marbl blueprint plus a valid namelist.

    C-Star's ``RomsMarblBlueprint.initial_conditions`` requires a non-empty
    Dataset (min_length=1), and its orchestrator validates the emitted
    blueprint eagerly -- before the runtime 'nest-from' directive can replace
    it with the parent-derived initial state -- so ``RomsMarblInputData`` must
    emit a schema-valid PLACEHOLDER resource for a child with no generated IC
    (``CHILD_IC_PLACEHOLDER_LOCATION`` in input_data.py). This closes the gap
    ``_validated_roms_marbl_blueprint`` would otherwise hit at emit time.

    Separately, C-Star's namelist ``InitialConditions.inifile`` is also
    required non-None, and ``_generate_initial_conditions`` (the only code
    that normally populates the run-time "initial" settings section) never
    runs for this child -- so ``RomsMarblInputData.__init__`` seeds the same
    sentinel into ``self._settings_run_time["initial"]`` up front. The
    run-time namelist gets regenerated downstream from the blueprint once the
    'nest-from' directive supplies the real parent-derived IC.
    """

    _GRID_KWARGS: ClassVar[dict] = TestGoldenNamelist._GRID_KWARGS
    _BOUNDARIES: ClassVar[dict] = TestGoldenNamelist._BOUNDARIES
    _PARTITIONING: ClassVar[dict] = TestGoldenNamelist._PARTITIONING
    _PARENT_GRID_KWARGS: ClassVar[dict] = dict(
        nx=20,
        ny=20,
        size_x=2000,
        size_y=2000,
        center_lon=0,
        center_lat=55,
        rot=0,
        N=10,
        theta_s=6.0,
        theta_b=3.0,
        hc=250.0,
    )

    def test_configure_build_succeeds_with_ic_placeholder(self, mock_grid, tmp_path):
        import copy

        forcing_inputs = copy.deepcopy(_FORCING_INPUTS)
        assert forcing_inputs["initial_conditions"]["source"]  # sanity: fixture has IC
        del forcing_inputs["initial_conditions"]

        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-tiny",
            grid_kwargs=self._GRID_KWARGS,
            grid_kwargs_parent=self._PARENT_GRID_KWARGS,
            open_boundaries=self._BOUNDARIES,
            partitioning=self._PARTITIONING,
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            description="Child domain, no IC",
            dt=7200,
            forcing_inputs=forcing_inputs,
            output_settings=_OUTPUT_SETTINGS,
        )
        assert cfg.domain.is_child is True
        assert cfg.forcing.initial_conditions is None

        grid_mock = _create_grid_mock()
        grid_mock.nx = self._GRID_KWARGS["nx"]
        grid_mock.ny = self._GRID_KWARGS["ny"]
        grid_mock.N = self._GRID_KWARGS["N"]
        grid_mock.theta_s = self._GRID_KWARGS["theta_s"]
        grid_mock.theta_b = self._GRID_KWARGS["theta_b"]
        grid_mock.hc = self._GRID_KWARGS["hc"]
        grid_mock.save.side_effect = TestGoldenNamelist._touch_save
        mock_grid.return_value = grid_mock

        run_dir = tmp_path / "run"
        host = HostPaths(working_dir=run_dir, source_data_cache=run_dir, system="test")

        # A child (has a parent, but is not itself a parent) makes the executor
        # build grid_parent + align this grid to it -- align_grids is a real
        # roms-tools function that expects real xarray Grid internals, which the
        # autouse rt.Grid mock doesn't provide, so it's stubbed too.
        with patch("cstar_forge.forge.executor.rt.align_grids", return_value=grid_mock):
            builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        builder.src_data = TestGoldenNamelist._mock_source_data(tmp_path)

        with (
            patch("cstar_forge.forge.input_data.rt.InitialConditions") as mock_ic,
            patch("cstar_forge.forge.input_data.rt.SurfaceForcing") as mock_surface,
            patch("cstar_forge.forge.input_data.rt.BoundaryForcing") as mock_boundary,
            patch("cstar_forge.forge.input_data.rt.TidalForcing") as mock_tidal,
            patch("cstar_forge.forge.input_data.rt.RiverForcing") as mock_river,
            patch(
                "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES",
                {"ERA5"},
            ),
        ):
            mock_surface_instance = MagicMock()
            mock_surface_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_surface_instance.use_coarse_grid = False
            mock_surface.return_value = mock_surface_instance

            mock_tidal_instance = MagicMock()
            mock_tidal_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_tidal_instance.ntides = 15
            mock_tidal.return_value = mock_tidal_instance

            mock_river_instance = MagicMock()
            mock_river_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_river_instance.ds = xr.Dataset(
                {
                    "river_volume": (["nriver", "time"], np.zeros((3, 2))),
                    "river_tracer": (
                        ["nriver", "time", "tracer"],
                        np.zeros((3, 2, 2)),
                    ),
                }
            )
            mock_river.return_value = mock_river_instance

            builder.generate_inputs(clobber=True, use_dask=False, test=False)

        # Neither IC nor boundary generation is planned for this child: IC has no
        # explicit source, and boundary is skipped for a child by the resolver.
        mock_ic.assert_not_called()
        mock_boundary.assert_not_called()
        assert builder._inputs_generated is True

        # Right after generate_inputs the in-memory blueprint is a plain dict
        # (model_construct round-tripped via model_dump -- see generate_inputs);
        # the typed-model assertion comes after configure_build re-validates.
        ic_elem = builder.roms_marbl_blueprint.initial_conditions
        assert len(ic_elem["data"]) == 1
        assert ic_elem["data"][0]["location"] == CHILD_IC_PLACEHOLDER_LOCATION
        assert (
            builder._settings_run_time["initial"]["initial_file"]
            == CHILD_IC_PLACEHOLDER_LOCATION
        )

        from cstar_forge.forge.forge_blueprint_engine import split_model_settings

        run_ov, compile_ov = split_model_settings(cfg)
        with patch("cstar_forge.forge.executor.render_roms_settings") as mock_render:
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["cppdefs.opt"]},
                "branch": "main",
            }
            builder.configure_build(
                compile_time_settings=compile_ov, run_time_settings=run_ov
            )

        validated = builder.roms_marbl_blueprint
        assert isinstance(validated, cstar_models.RomsMarblBlueprint)
        assert len(validated.initial_conditions.data) == 1
        assert (
            validated.initial_conditions.data[0].location
            == CHILD_IC_PLACEHOLDER_LOCATION
        )

        namelist_path = builder.run_time_code_dir / "namelist.nml"
        assert namelist_path.exists()
        raw = namelist_path.read_text()
        assert CHILD_IC_PLACEHOLDER_LOCATION in raw


class TestForgeRunnerEndToEnd:
    """Proves forge is a real, C-Star-discoverable application (see
    ``cstar_forge.forge.app.ForgeRunner``): drives ``ForgeRunner`` -- C-Star's own
    ``BlueprintRunner``/``RunnerRequest`` machinery -- all the way down to real
    ``ForgeExecutor.generate_inputs()``/``configure_build()``, the same
    roms-tools-mocked chain ``TestGoldenNamelist`` exercises directly.

    ``ForgeRunner.run()`` delegates to ``cstar_forge.run.process`` (the disposable
    host-resolution glue), which this test intercepts to inject a fake ``HostPaths``
    and an ``executor_factory`` that stands in ``src_data`` (avoiding real dataset
    downloads) -- mirroring how ``TestGoldenNamelist`` swaps ``builder.src_data``
    after construction. Everything else (``ForgeExecutor``, ``process_forge_blueprint``,
    roms-tools construction classes) is real.
    """

    _GRID_KWARGS: ClassVar[dict] = TestGoldenNamelist._GRID_KWARGS
    _BOUNDARIES: ClassVar[dict] = TestGoldenNamelist._BOUNDARIES
    _PARTITIONING: ClassVar[dict] = TestGoldenNamelist._PARTITIONING

    def _make_blueprint_yaml(self, tmp_path) -> Path:
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-tiny",
            grid_kwargs=self._GRID_KWARGS,
            open_boundaries=self._BOUNDARIES,
            partitioning=self._PARTITIONING,
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            description="ForgeRunner e2e test",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
        )
        bp_path = tmp_path / "forge_blueprint.yaml"
        cfg.to_yaml(bp_path)
        return bp_path

    def test_forge_runner_generates_inputs_and_completes(self, mock_grid, tmp_path):
        grid_mock = _create_grid_mock()
        grid_mock.nx = self._GRID_KWARGS["nx"]
        grid_mock.ny = self._GRID_KWARGS["ny"]
        grid_mock.N = self._GRID_KWARGS["N"]
        grid_mock.theta_s = self._GRID_KWARGS["theta_s"]
        grid_mock.theta_b = self._GRID_KWARGS["theta_b"]
        grid_mock.hc = self._GRID_KWARGS["hc"]
        grid_mock.save.side_effect = TestGoldenNamelist._touch_save
        mock_grid.return_value = grid_mock

        bp_path = self._make_blueprint_yaml(tmp_path)
        run_dir = tmp_path / "run"
        fake_host = HostPaths(
            working_dir=run_dir,
            source_data_cache=run_dir,
            system="test",
        )

        def fake_process(spec, **_kwargs):
            def factory(cfg, host, verbose):
                builder = ForgeExecutor.from_forge_blueprint(
                    cfg, host=host, verbose=verbose
                )
                builder.src_data = TestGoldenNamelist._mock_source_data(tmp_path)
                return builder

            # ensure_data=False: skip real dataset staging (ForgeExecutor.ensure_source_data
            # is exercised elsewhere; this test's scope is the ForgeRunner -> engine wiring).
            return process_forge_blueprint(
                spec,
                host=fake_host,
                executor_factory=factory,
                use_dask=False,
                ensure_data=False,
            )

        with (
            patch("cstar_forge.run.process", side_effect=fake_process),
            patch("cstar_forge.forge.input_data.rt.InitialConditions") as mock_ic,
            patch("cstar_forge.forge.input_data.rt.SurfaceForcing") as mock_surface,
            patch("cstar_forge.forge.input_data.rt.BoundaryForcing") as mock_boundary,
            patch("cstar_forge.forge.input_data.rt.TidalForcing") as mock_tidal,
            patch("cstar_forge.forge.input_data.rt.RiverForcing") as mock_river,
            patch("cstar_forge.forge.input_data.rt.CDRForcing") as mock_cdr,
            patch(
                "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES",
                {"ERA5"},
            ),
            patch("cstar_forge.forge.executor.render_roms_settings") as mock_render,
        ):
            mock_ic_instance = MagicMock()
            mock_ic_instance.save.side_effect = TestGoldenNamelist._touch_save_list
            mock_ic.return_value = mock_ic_instance

            mock_surface_instance = MagicMock()
            mock_surface_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_surface_instance.use_coarse_grid = False
            mock_surface.return_value = mock_surface_instance

            mock_boundary_instance = MagicMock()
            mock_boundary_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_boundary.return_value = mock_boundary_instance

            mock_tidal_instance = MagicMock()
            mock_tidal_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_tidal_instance.ntides = 15
            mock_tidal.return_value = mock_tidal_instance

            mock_river_instance = MagicMock()
            mock_river_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_river_instance.ds = xr.Dataset(
                {
                    "river_volume": (["nriver", "time"], np.zeros((3, 2))),
                    "river_tracer": (
                        ["nriver", "time", "tracer"],
                        np.zeros((3, 2, 2)),
                    ),
                }
            )
            mock_river.return_value = mock_river_instance

            mock_cdr_instance = MagicMock()
            mock_cdr_instance.save.side_effect = TestGoldenNamelist._touch_save
            mock_releases = MagicMock()
            mock_releases.__len__.return_value = 2
            mock_releases.release_type = "volume"
            mock_cdr_instance.releases = mock_releases
            mock_cdr.return_value = mock_cdr_instance

            mock_render.return_value = {
                "location": str(run_dir),
                "filter": {"files": ["cppdefs.opt"]},
                "branch": "main",
            }

            job_cfg = get_job_config()
            service_cfg = get_service_config("INFO", name="ForgeRunnerTest")
            request = RunnerRequest(str(bp_path), ForgeBlueprint)
            runner = ForgeRunner(request, service_cfg, job_cfg)

            asyncio.run(runner.execute())

        assert runner.state.status == ExecutionStatus.COMPLETED, runner.result.errors
        assert not runner.result.errors

        namelist_path = run_dir / "builds" / "run-time" / "namelist.nml"
        assert namelist_path.exists()

        blueprint_yaml_paths = list((run_dir / "blueprints").glob("B_*.yaml"))
        assert blueprint_yaml_paths, "expected an emitted roms_marbl B_{name}.yaml"

        # ForgeRunner also publishes a copy to <working root>/output/ -- where
        # C-Star's deferred-blueprint resolution looks for a producer step's
        # artifact. Only the blueprint (no settings sidecar), so a filename-less
        # deferred reference resolves to a unique candidate.
        published = list((run_dir / "output").glob("*.yaml"))
        assert [p.name for p in published] == [blueprint_yaml_paths[0].name]
        assert published[0].read_bytes() == blueprint_yaml_paths[0].read_bytes()


class TestOnlyInputsReuseIsIdempotent:
    """Proves the `--only-inputs` design's load-bearing assumption: no state file
    is needed between a piecemeal run and a later full run, because settings and
    the downstream blueprint are fully re-derived from the grid object + whatever
    is already on disk -- not read back from anywhere in memory.

    Drives the real ``generate_inputs()`` -> ``configure_build()`` chain (as
    ``TestGoldenNamelist`` does) TWICE against the *same* working directory, each
    time against a brand-new ``ForgeExecutor`` (so nothing carries over in
    memory between passes -- only the files on disk do), and asserts pass 2
    (pure reuse) produces a byte-identical ``namelist.nml`` and ``B_{name}.yaml``
    to pass 1.

    Unlike ``TestGoldenNamelist``, the roms-tools mocks here write REAL sidecar
    files (YAML + NetCDF), not no-ops -- specifically so pass 2 takes each
    input's *cheapest* reuse branch (the one the real piecemeal-then-full
    workflow actually exercises, since a one-off run leaves its sidecars on
    disk), not just the "sidecar missing -> reconstruct" fallback:

    - tidal: ``to_yaml`` writes a real multi-doc YAML with ``TidalForcing.ntides``,
      so pass 2 reads ``ntides`` straight from it (``yaml.safe_load_all``), never
      touching ``rt.TidalForcing`` again.
    - river: ``save`` writes a real minimal NetCDF (river_volume/river_tracer over
      a 3-sized ``nriver`` dim), so pass 2 reads ``nriv`` straight from it via
      ``xr.open_dataset``, never touching ``rt.RiverForcing`` again.
    - boundary: ``to_yaml`` writes a real (empty) sidecar file, so pass 2 reuses
      the existing NetCDF with no reconstruction at all (boundary's cheap path
      needs no read-back).
    - surface: ``to_yaml`` writes a real (empty) sidecar file. Its cheap path
      (``_interp_frc_surface_reuse``) peeks at the real NetCDF for
      ``xi_coarse``/``eta_coarse`` dims and falls back to 0 on any read error;
      since the mocked ``.save()`` only touches an empty placeholder file, this
      peek harmlessly no-ops to the same ``interp_frc=0`` the mock's
      ``use_coarse_grid=False`` would have given a fresh construction -- so this
      exercises the "no reconstruction" code path, not the value derivation
      itself.
    - grid/initial_conditions/cdr_forcing: unconditionally re-derive settings
      from the live (grid/reconstructed) object on every run regardless of
      reuse -- there is no separate "cheap" branch to distinguish for these.
    """

    _GRID_KWARGS: ClassVar[dict] = dict(
        nx=6,
        ny=2,
        size_x=500,
        size_y=1000,
        center_lon=0,
        center_lat=55,
        rot=10,
        N=3,
        theta_s=5.0,
        theta_b=2.0,
        hc=250.0,
    )
    _BOUNDARIES: ClassVar[dict] = {
        "south": False,
        "east": True,
        "north": True,
        "west": False,
    }
    _PARTITIONING: ClassVar[dict] = {"n_procs_x": 1, "n_procs_y": 1}
    _CDR_FORCING: ClassVar[dict] = {"enabled": True}

    @staticmethod
    def _touch_save(path, **_kw):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).touch()
        return path

    @staticmethod
    def _touch_save_list(path, **_kw):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).touch()
        return [path]

    @staticmethod
    def _touch_yaml(path, **_kw):
        """A real (empty-content) sidecar write -- enough to satisfy the
        ``yaml_path.exists()`` gate that routes reuse to the cheap branch.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).touch()

    @staticmethod
    def _write_tidal_yaml(path, **_kw):
        """A real multi-doc sidecar matching what tidal's cheap-reuse parser
        (``yaml.safe_load_all`` looking for a ``TidalForcing`` doc) expects.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with Path(path).open("w") as f:
            yaml.safe_dump_all(
                [{"roms_tools_version": "test"}, {"TidalForcing": {"ntides": 15}}], f
            )

    @staticmethod
    def _river_dataset():
        return xr.Dataset(
            {
                "river_volume": (["nriver", "time"], np.zeros((3, 2))),
                "river_tracer": (
                    ["nriver", "time", "tracer"],
                    np.zeros((3, 2, 2)),
                ),
            }
        )

    @classmethod
    def _write_river_netcdf(cls, path, **_kw):
        """A real minimal NetCDF matching what river's cheap-reuse read
        (``xr.open_dataset`` -> ``ds.sizes["nriver"]``) expects.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        cls._river_dataset().to_netcdf(path)
        return path

    @staticmethod
    def _mock_source_data(tmp_path):
        mock_sd = MagicMock()
        source_file = tmp_path / "source.nc"
        source_file.touch()

        def _dks(name, glorys_layout=None):
            if name == "GLORYS":
                return (
                    "GLORYS_GLOBAL" if glorys_layout == "global" else "GLORYS_REGIONAL"
                )
            return {
                "UNIFIED": "UNIFIED_BGC",
                "ERA5": "ERA5",
                "TPXO": "TPXO",
                "DAI": "DAI",
            }.get(name, name.upper())

        mock_sd.path_for_source = MagicMock(return_value=source_file)
        mock_sd.dataset_key_for_source = MagicMock(side_effect=_dks)
        mock_sd.streamable_for_source = MagicMock(
            side_effect=lambda name, glorys_layout=None: name.upper() in {"ERA5", "DAI"}
        )
        return mock_sd

    def _run_one_pass(self, cfg, host, mock_grid, tmp_path):
        """One full generate_inputs() -> configure_build() pass against `host`'s
        working_dir. Returns (namelist_text, blueprint_text). Uses clobber=False
        both times -- pass 1 has nothing on disk yet (generates fresh); pass 2
        (a fresh executor against the same working_dir) finds pass 1's files
        already there and takes the reuse branches.
        """
        grid_mock = _create_grid_mock()
        grid_mock.nx = self._GRID_KWARGS["nx"]
        grid_mock.ny = self._GRID_KWARGS["ny"]
        grid_mock.N = self._GRID_KWARGS["N"]
        grid_mock.theta_s = self._GRID_KWARGS["theta_s"]
        grid_mock.theta_b = self._GRID_KWARGS["theta_b"]
        grid_mock.hc = self._GRID_KWARGS["hc"]
        grid_mock.save.side_effect = self._touch_save
        mock_grid.return_value = grid_mock

        builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
        builder.src_data = self._mock_source_data(tmp_path)

        with (
            patch("cstar_forge.forge.input_data.rt.InitialConditions") as mock_ic,
            patch("cstar_forge.forge.input_data.rt.SurfaceForcing") as mock_surface,
            patch("cstar_forge.forge.input_data.rt.BoundaryForcing") as mock_boundary,
            patch("cstar_forge.forge.input_data.rt.TidalForcing") as mock_tidal,
            patch("cstar_forge.forge.input_data.rt.RiverForcing") as mock_river,
            patch("cstar_forge.forge.input_data.rt.CDRForcing") as mock_cdr,
            patch(
                "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES",
                {"ERA5"},
            ),
        ):
            mock_ic_instance = MagicMock()
            mock_ic_instance.save.side_effect = self._touch_save_list
            mock_ic.return_value = mock_ic_instance

            mock_surface_instance = MagicMock()
            mock_surface_instance.save.side_effect = self._touch_save
            mock_surface_instance.to_yaml.side_effect = self._touch_yaml
            mock_surface_instance.use_coarse_grid = False
            mock_surface.return_value = mock_surface_instance

            mock_boundary_instance = MagicMock()
            mock_boundary_instance.save.side_effect = self._touch_save
            mock_boundary_instance.to_yaml.side_effect = self._touch_yaml
            mock_boundary.return_value = mock_boundary_instance

            mock_tidal_instance = MagicMock()
            mock_tidal_instance.save.side_effect = self._touch_save
            mock_tidal_instance.to_yaml.side_effect = self._write_tidal_yaml
            mock_tidal_instance.ntides = 15
            mock_tidal.return_value = mock_tidal_instance

            mock_river_instance = MagicMock()
            mock_river_instance.save.side_effect = self._write_river_netcdf
            mock_river_instance.to_yaml.side_effect = self._touch_yaml
            mock_river_instance.ds = self._river_dataset()
            mock_river.return_value = mock_river_instance

            mock_cdr_instance = MagicMock()
            mock_cdr_instance.save.side_effect = self._touch_save
            mock_releases = MagicMock()
            mock_releases.__len__.return_value = 2
            mock_releases.release_type = "volume"
            mock_cdr_instance.releases = mock_releases
            mock_cdr.return_value = mock_cdr_instance

            builder.generate_inputs(clobber=False, use_dask=False, test=False)

        from cstar_forge.forge.forge_blueprint_engine import split_model_settings

        run_ov, compile_ov = split_model_settings(cfg)
        with patch("cstar_forge.forge.executor.render_roms_settings") as mock_render:
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["cppdefs.opt"]},
                "branch": "main",
            }
            builder.configure_build(
                compile_time_settings=compile_ov, run_time_settings=run_ov
            )

        namelist_text = (builder.run_time_code_dir / "namelist.nml").read_text()
        blueprint_text = builder.path_roms_marbl_blueprint().read_text()
        return namelist_text, blueprint_text

    def test_second_pass_reuse_matches_first_pass_byte_for_byte(
        self, mock_grid, tmp_path
    ):
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
            grid_name="test-tiny",
            grid_kwargs=self._GRID_KWARGS,
            open_boundaries=self._BOUNDARIES,
            partitioning=self._PARTITIONING,
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            description="Only-inputs idempotence test",
            dt=7200,
            forcing_inputs=_FORCING_INPUTS,
            output_settings=_OUTPUT_SETTINGS,
            cdr_forcing=self._CDR_FORCING,
        )

        run_dir = tmp_path / "run"
        host = HostPaths(
            working_dir=run_dir,
            source_data_cache=run_dir,
            system="test",
        )

        namelist_1, blueprint_1 = self._run_one_pass(cfg, host, mock_grid, tmp_path)
        namelist_2, blueprint_2 = self._run_one_pass(cfg, host, mock_grid, tmp_path)

        assert namelist_2 == namelist_1, (
            "A second full run against a working_dir that already has all "
            "inputs must re-derive a byte-identical namelist.nml -- otherwise "
            "the piecemeal-then-full workflow (generate a subset, verify, then "
            "run the rest) cannot be trusted to produce a correct build."
        )
        assert blueprint_2 == blueprint_1, (
            "A second full run against a working_dir that already has all "
            "inputs must re-derive a byte-identical B_{name}.yaml."
        )
