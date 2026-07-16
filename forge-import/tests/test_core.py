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
- deep-merge helper and RomsMarblBlueprintStage
"""

import os
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
from cstar.orchestration.models import Resource
from pydantic import ValidationError

import cstar_forge
from cstar_forge import models as forge_models
from cstar_forge.domain_catalog import default_catalog as _CATALOG
from cstar_forge.forge.executor import ForgeExecutor, _deep_merge_settings_dict
from cstar_forge.forge.host import HostPaths
from cstar_forge.forge_blueprint_resolve import build_forge_blueprint

_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
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
        ensemble_id=merged.get("ensemble_id"),
        use_pio=merged.get("use_pio", False),
        dt=7200,
        forcing_inputs=merged.get("forcing_inputs", _FORCING_INPUTS),
        output_settings=merged.get("output_settings", _OUTPUT_SETTINGS),
    )
    tmp = Path(tempfile.mkdtemp(prefix="forge-test-core-"))
    host = HostPaths(
        working_dir=tmp, source_data_cache=tmp, system="test", machine_config=None
    )
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
        output_dir=Path(),
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

        assert builder.model_name == "cson_roms-marbl_v0.1"
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


class TestForgeExecutorProperties:
    """Tests for ForgeExecutor properties."""

    def test_name_property(self, minimal_cstar_spec_builder_args):
        """Test the name property."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        # Name includes n_procs suffix: {model_name}_{grid_name}_{n_procs}procs
        n_procs = builder.partitioning.n_procs_x * builder.partitioning.n_procs_y
        expected_name = f"{builder.model_name}_{builder.grid_name}_{n_procs}procs"
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
        expected_path = (
            builder.roms_marbl_blueprint_dir / f"B_{builder.name}_preconfig.yml"
        )
        assert builder.path_roms_marbl_blueprint(stage="preconfig") == expected_path

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

        mock_grid.assert_called_once_with(**builder.grid_kwargs)
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
        host = HostPaths(
            working_dir=tmp, source_data_cache=tmp, system="test", machine_config=None
        )
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
        host = HostPaths(
            working_dir=tmp, source_data_cache=tmp, system="test", machine_config=None
        )
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

    def test_model_post_init_loads_roms_marbl_blueprint_from_file_when_exists(
        self, minimal_cstar_spec_builder_args
    ):
        """model_post_init persists a PRECONFIG blueprint; roms_marbl_blueprint_from_file loads it."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        # The preconfig blueprint was persisted during initialization, so it loads back.
        assert builder.roms_marbl_blueprint_from_file is not None
        assert isinstance(
            builder.roms_marbl_blueprint_from_file, cstar_models.RomsMarblBlueprint
        )


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

    def test_build_sets_stage_to_build(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() sets _stage to BUILD."""
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

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

            assert builder._stage == RomsMarblBlueprintStage.BUILD

    def test_build_persists_roms_marbl_blueprint(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() persists blueprint to file."""
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

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

            expected_bp_path = builder.path_roms_marbl_blueprint(
                stage=RomsMarblBlueprintStage.BUILD
            )
            assert expected_bp_path.exists()

            with open(expected_bp_path) as f:
                roms_marbl_blueprint_data = yaml.safe_load(f)
                assert roms_marbl_blueprint_data is not None
                assert "code" in roms_marbl_blueprint_data
                assert "compile_time" in roms_marbl_blueprint_data["code"]
                assert "location" in roms_marbl_blueprint_data["code"]["compile_time"]

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

    def test_build_with_use_pio_emits_code_pio_and_model_param(
        self, minimal_cstar_spec_builder_args
    ):
        """With use_pio, the emitted RomsMarblBlueprint carries code.pio and
        model_params.use_pio: true.
        """
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

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

        bp_path = builder.path_roms_marbl_blueprint(stage=RomsMarblBlueprintStage.BUILD)
        with open(bp_path) as f:
            data = yaml.safe_load(f)
        assert data["model_params"]["use_pio"] is True
        assert data["code"]["pio"]["location"] == (
            "https://github.com/NCAR/ParallelIO.git"
        )
        assert data["code"]["pio"]["commit"] == "pio2_7_0"

    def test_build_without_use_pio_omits_pio(self, minimal_cstar_spec_builder_args):
        """Without use_pio, model_params has no use_pio key and code.pio is unset
        (keeps non-PIO blueprints loadable by main-branch C-Star).
        """
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

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

        bp_path = builder.path_roms_marbl_blueprint(stage=RomsMarblBlueprintStage.BUILD)
        with open(bp_path) as f:
            data = yaml.safe_load(f)
        assert "use_pio" not in data["model_params"]
        assert data["code"].get("pio") is None

    def test_configure_build_does_not_clobber_generated_river_and_tidal_settings(
        self, sample_grid_kwargs, sample_open_boundaries, sample_partitioning
    ):
        """Regression for the §3a bug (docs/forge-blueprint-parameter-audit.md): before
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
        host = HostPaths(
            working_dir=tmp, source_data_cache=tmp, system="test", machine_config=None
        )
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
        pinned = yaml.safe_load((_MODEL_DIR / "model.yml").read_text())["code"][
            "templates_commit"
        ]
        ct = builder._template_repo_args("compile_time")
        assert ct["location"].endswith("cstar-forge.git")
        assert ct["subdir"] == "templates/compile-time"
        assert ct["checkout_target"] == pinned
        assert ct["files"] == ["cppdefs.opt.j2"]


class TestForgeExecutorPathRomsMarblBlueprint:
    """Tests for path_roms_marbl_blueprint method."""

    def test_path_roms_marbl_blueprint_preconfig(self, minimal_cstar_spec_builder_args):
        """Test path_roms_marbl_blueprint for preconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_roms_marbl_blueprint(stage="preconfig")

        assert "preconfig" in str(path)
        assert builder.name in str(path)
        assert path.suffix == ".yml"

    def test_path_roms_marbl_blueprint_postconfig(
        self, minimal_cstar_spec_builder_args
    ):
        """Test path_roms_marbl_blueprint for postconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_roms_marbl_blueprint(stage="postconfig")

        assert "postconfig" in str(path)
        assert builder.name in str(path)

    def test_path_roms_marbl_blueprint_build(self, minimal_cstar_spec_builder_args):
        """Test path_roms_marbl_blueprint for build stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_roms_marbl_blueprint(stage="build")

        assert "build" in str(path)
        assert builder.name in str(path)
        assert path.name.endswith("_build.yml")

    def test_path_roms_marbl_blueprint_run_with_params(
        self, minimal_cstar_spec_builder_args, sample_runtime_params
    ):
        """Test path_roms_marbl_blueprint for run stage with runtime params."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_roms_marbl_blueprint(
            stage="run", run_params=sample_runtime_params
        )

        assert "run" in str(path)
        assert "20120101" in str(path)  # start_date
        assert "20120102" in str(path)  # end_date

    def test_path_roms_marbl_blueprint_run_without_params(
        self, minimal_cstar_spec_builder_args
    ):
        """Test path_roms_marbl_blueprint for run stage without params raises error."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(ValueError) as exc_info:
            builder.path_roms_marbl_blueprint(stage="run", run_params=None)
        assert "run_params is required" in str(exc_info.value)

    def test_path_roms_marbl_blueprint_invalid_stage(
        self, minimal_cstar_spec_builder_args
    ):
        """Test path_roms_marbl_blueprint with invalid stage raises error."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(ValueError) as exc_info:
            builder.path_roms_marbl_blueprint(stage="invalid_stage")
        assert "stage must be one of" in str(exc_info.value)

    def test_path_roms_marbl_blueprint_uses_roms_marbl_blueprint_state(
        self, minimal_cstar_spec_builder_args
    ):
        """Test path_roms_marbl_blueprint uses blueprint state when stage is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint.state = "postconfig"

        path = builder.path_roms_marbl_blueprint(stage=None)
        assert "postconfig" in str(path)


class TestForgeExecutorPersist:
    """Tests for persist method."""

    def test_persist_preconfig(self, minimal_cstar_spec_builder_args):
        """Test persist for preconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "preconfig"

        builder.persist()

        bp_path = builder.path_roms_marbl_blueprint(stage="preconfig")
        assert bp_path.exists()

        with bp_path.open("r") as f:
            data = yaml.safe_load(f)
            assert data is not None
            assert "name" in data

    def test_persist_postconfig(self, minimal_cstar_spec_builder_args):
        """Test persist for postconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "postconfig"

        builder.persist()

        bp_path = builder.path_roms_marbl_blueprint(stage="postconfig")
        assert bp_path.exists()

    def test_persist_run(self, minimal_cstar_spec_builder_args, sample_runtime_params):
        """Test persist for run stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "run"
        builder.roms_marbl_blueprint.runtime_params = sample_runtime_params

        builder.persist()

        bp_path = builder.path_roms_marbl_blueprint(
            stage="run", run_params=sample_runtime_params
        )
        assert bp_path.exists()

    def test_persist_raises_when_roms_marbl_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test persist raises error when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.roms_marbl_blueprint = None
        builder._stage = "preconfig"

        with pytest.raises(ValueError) as exc_info:
            builder.persist()
        assert "blueprint is not initialized" in str(exc_info.value)

    def test_persist_raises_when_stage_none(self, minimal_cstar_spec_builder_args):
        """Test persist raises error when _stage is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = None

        with pytest.raises(ValueError) as exc_info:
            builder.persist()
        assert "_stage is not set" in str(exc_info.value)

    def test_persist_raises_when_run_stage_no_runtime_params(
        self, minimal_cstar_spec_builder_args
    ):
        """Test persist raises error for run stage without runtime_params."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "run"
        builder.roms_marbl_blueprint.runtime_params = None

        with pytest.raises(ValueError) as exc_info:
            builder.persist()
        assert "runtime_params is not set" in str(exc_info.value)


class TestForgeExecutorDefaultRuntimeParams:
    """Tests for default_runtime_params property."""

    def test_default_runtime_params(self, minimal_cstar_spec_builder_args):
        """Test default_runtime_params property (output_dir routes under host)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        runtime_params = builder.default_runtime_params

        assert runtime_params.start_date == builder.start_date
        assert runtime_params.end_date == builder.end_date
        # run_output_dir is the injected host working_dir.
        assert runtime_params.output_dir == builder.run_output_dir
        assert runtime_params.output_dir == builder.host.working_dir


class TestForgeExecutorGenerateInputsComprehensive:
    """Comprehensive tests for generate_inputs method covering full workflow."""

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_with_partition_files_raises_error(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs raises NotImplementedError when partition_files=True."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(NotImplementedError) as exc_info:
            builder.generate_inputs(partition_files=True)
        assert "partitioning functionality" in str(exc_info.value).lower()

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
            assert call_kwargs["netcdf_format"] == "NETCDF4"

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_use_pio_sets_classic_netcdf_format(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """With use_pio, inputs are written classic-format (CDF-5) for PnetCDF."""
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
            assert call_kwargs["netcdf_format"] == "NETCDF3_64BIT_DATA"

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_test_mode_does_not_persist(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs in test mode does not persist blueprint."""
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
                output_dir=Path(),
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
                output_dir=Path(),
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
                output_dir=Path(),
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


class TestRomsMarblBlueprintStage:
    """Tests for RomsMarblBlueprintStage class."""

    def test_roms_marbl_blueprintstage_constants(self):
        """Test RomsMarblBlueprintStage constants."""
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

        assert RomsMarblBlueprintStage.PRECONFIG == "preconfig"
        assert RomsMarblBlueprintStage.POSTCONFIG == "postconfig"
        assert RomsMarblBlueprintStage.BUILD == "build"
        assert RomsMarblBlueprintStage.RUN == "run"

    def test_roms_marbl_blueprintstage_validate_stage_valid(self):
        """Test RomsMarblBlueprintStage.validate_stage with valid stage."""
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

        assert RomsMarblBlueprintStage.validate_stage("preconfig") == "preconfig"
        assert RomsMarblBlueprintStage.validate_stage("postconfig") == "postconfig"
        assert RomsMarblBlueprintStage.validate_stage("build") == "build"
        assert RomsMarblBlueprintStage.validate_stage("run") == "run"

    def test_roms_marbl_blueprintstage_validate_stage_invalid(self):
        """Test RomsMarblBlueprintStage.validate_stage with invalid stage."""
        from cstar_forge.forge.executor import RomsMarblBlueprintStage

        with pytest.raises(ValueError) as exc_info:
            RomsMarblBlueprintStage.validate_stage("invalid")
        assert "stage must be one of" in str(exc_info.value)


class TestGoldenNamelist:
    """Byte-level golden test for the rendered ``namelist.nml``.

    This is the deterministic, mocked-forcing golden referenced in the Follow-ups
    section of ``docs/forge-blueprint-parameter-audit.md`` and in
    ``docs/developer-guide.md`` Sec 6 — it is NOT the real-generated-data integration
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
        return mock_sd

    def test_golden_namelist_test_tiny(self, mock_grid, tmp_path):
        cfg = build_forge_blueprint(
            model_dir=_MODEL_DIR,
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
            machine_config=None,
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
        assert builder._settings_run_time["cdr_output"]["do_cdr"] is True

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
            / "golden_namelist_test-tiny.nml"
        )

        if os.environ.get("UPDATE_GOLDEN"):
            golden_path.write_text(normalized)
            pytest.fail(
                f"UPDATE_GOLDEN=1: wrote {golden_path}. Review the diff and commit "
                "it, then rerun without UPDATE_GOLDEN to confirm the test passes."
            )

        golden = golden_path.read_text()
        assert normalized == golden, (
            "Rendered namelist.nml drifted from "
            "tests/fixtures/golden_namelist_test-tiny.nml. If this is an intentional "
            "schema/default/template change, regenerate with "
            "UPDATE_GOLDEN=1 pytest tests/test_core.py -k golden_namelist_test_tiny, "
            "review the diff, and commit the updated fixture; otherwise this is a "
            "regression."
        )
