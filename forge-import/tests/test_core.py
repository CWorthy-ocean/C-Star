"""
Tests for the ForgeExecutor (cstar_forge.forge.executor).

The executor is now config/authoring-free: it is constructed the canonical way via
``ForgeExecutor.from_spec_config(cfg, host=host)`` where ``cfg`` is a resolved
``SpecConfig`` (built by ``build_spec_config``) and ``host`` is an injected
``HostPaths``. All produced-artifact paths route under ``host.working_dir``.

Tests cover:
- ForgeExecutor initialization and validation
- Properties (name, input_data_dir, blueprint_dir, path_blueprint, datasets)
- Model post-init behavior
- Blueprint persist / path_blueprint
- get_ds method
- ensure_source_data
- generate_inputs
- configure_build / build
- deep-merge helper and BlueprintStage
"""
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr
import yaml
from pydantic import ValidationError

import cstar.applications.roms_marbl.models as cstar_models
from cstar.orchestration.models import Resource

import cstar_forge
from cstar_forge import models as forge_models
from cstar_forge.forge.executor import ForgeExecutor, _deep_merge_settings_dict
from cstar_forge.forge.host import HostPaths
from cstar_forge.spec_config_resolve import build_spec_config


_MODEL_DIR = (
    Path(cstar_forge.__file__).parent / "catalog" / "ModelSpec" / "cson_roms-marbl_v0.1"
)


def _make_builder(args, **overrides):
    """Single construction point: build a resolved SpecConfig from the bundled ModelSpec
    plus a temp host, then construct the executor the canonical way."""
    merged = {**args, **overrides}
    ob = merged["open_boundaries"]
    part = merged["partitioning"]
    cfg = build_spec_config(
        model_dir=_MODEL_DIR,
        grid_name=merged["grid_name"],
        grid_kwargs=merged["grid_kwargs"],
        open_boundaries=ob.model_dump() if hasattr(ob, "model_dump") else ob,
        partitioning=part.model_dump() if hasattr(part, "model_dump") else part,
        start_date=merged["start_date"],
        end_date=merged["end_date"],
        description=merged.get("description", "Generated blueprint"),
        ensemble_id=merged.get("ensemble_id"),
        dt=7200,
    )
    tmp = Path(tempfile.mkdtemp(prefix="forge-test-core-"))
    host = HostPaths(
        working_dir=tmp, source_data_cache=tmp, system="test", machine_config=None
    )
    return ForgeExecutor.from_spec_config(cfg, host=host)


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
    so keep it mocked. Tests that need the mock can request it by name."""
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
        forcing_override, not the catalog ModelSpec)."""
        _make_builder(minimal_cstar_spec_builder_args)

        stdout = capsys.readouterr().out
        assert "ForgeExecutor: planned NetCDF outputs" in stdout
        assert "_grid.nc" in stdout
        assert "_initial_conditions.nc" in stdout
        # Forcing stems come from the resolved forcing categories/types (enum reprs,
        # dots normalized to underscores by netcdf_filename_component).
        assert "_surface-SurfaceType_PHYSICS.nc" in stdout
        assert "_surface-SurfaceType_BGC.nc" in stdout
        assert "_boundary-BoundaryType_PHYSICS.nc" in stdout
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
        assert (
            "start_date must precede end_date" in str(exc_info.value)
            or "end_date must be after start_date" in str(exc_info.value)
        )

    def test_validation_end_date_equals_start_date(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that validation raises error when end_date equals start_date."""
        minimal_cstar_spec_builder_args["end_date"] = datetime(2012, 1, 1)
        minimal_cstar_spec_builder_args["start_date"] = datetime(2012, 1, 1)

        with pytest.raises(ValidationError) as exc_info:
            _make_builder(minimal_cstar_spec_builder_args)
        assert (
            "start_date must precede end_date" in str(exc_info.value)
            or "end_date must be after start_date" in str(exc_info.value)
        )


class TestVSpongeDefault:
    """Tests for default v_sponge (resolved by build_spec_config from grid_kwargs)."""

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

    def test_blueprint_dir_property(self, minimal_cstar_spec_builder_args):
        """Test the blueprint_dir property (under host.working_dir)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        assert builder.blueprint_dir == builder.host.working_dir / "blueprints"

    def test_path_blueprint_method(self, minimal_cstar_spec_builder_args):
        """Test the path_blueprint method (host-based)."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        expected_path = builder.blueprint_dir / f"B_{builder.name}_preconfig.yml"
        assert builder.path_blueprint(stage="preconfig") == expected_path

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
        builder.blueprint.grid = cstar_models.Dataset(
            data=[Resource(location=str(grid_file), partitioned=False)]
        )
        builder.blueprint.initial_conditions = cstar_models.Dataset(
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

    def test_model_post_init_initializes_blueprint(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that model_post_init initializes the blueprint."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder.blueprint is not None
        assert isinstance(builder.blueprint, cstar_models.RomsMarblBlueprint)
        assert builder.blueprint.name == builder.name

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
        so grid_kwargs must reach rt.Grid without a ``topography_source`` key."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        assert builder.topography_source == "ETOPO5"
        _, kwargs = mock_grid.call_args
        assert "topography_source" not in kwargs

    def test_model_post_init_srtm15_injects_topography_source(
        self, minimal_cstar_spec_builder_args, mock_grid
    ):
        """SRTM15 is staged and its {'name','path'} dict is injected into grid_kwargs
        BEFORE rt.Grid is called. This is the load-bearing wiring whose failure is silent
        (roms-tools would otherwise fall back to ETOPO5)."""
        args = minimal_cstar_spec_builder_args
        cfg = build_spec_config(
            model_dir=_MODEL_DIR,
            grid_name=args["grid_name"],
            grid_kwargs=args["grid_kwargs"],
            open_boundaries=args["open_boundaries"].model_dump(),
            partitioning=args["partitioning"].model_dump(),
            start_date=args["start_date"],
            end_date=args["end_date"],
            dt=7200,
        )
        # Drive an SRTM15 spec (the bundled ModelSpec defaults to ETOPO5).
        cfg = cfg.model_copy(
            update={"domain": cfg.domain.model_copy(update={"topography_source": "SRTM15"})}
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
            builder = ForgeExecutor.from_spec_config(cfg, host=host)

        assert builder.topography_source == "SRTM15"
        _, kwargs = mock_grid.call_args
        assert kwargs["topography_source"] == {"name": "SRTM15", "path": str(staged)}
        # name must be a plain str, never the TopographySource enum (so grid.to_yaml is safe).
        assert type(kwargs["topography_source"]["name"]) is str

    def test_model_post_init_loads_blueprint_from_file_when_exists(
        self, minimal_cstar_spec_builder_args
    ):
        """model_post_init persists a PRECONFIG blueprint; blueprint_from_file loads it."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        # The preconfig blueprint was persisted during initialization, so it loads back.
        assert builder.blueprint_from_file is not None
        assert isinstance(
            builder.blueprint_from_file, cstar_models.RomsMarblBlueprint
        )


class TestForgeExecutorGetDs:
    """Tests for the get_ds method."""

    def test_get_ds_grid_from_blueprint(
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
        blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.blueprint.code,
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
        builder.blueprint = blueprint

        with patch("cstar_forge.forge.executor.xr.open_dataset") as mock_open:
            mock_ds = MagicMock(spec=xr.Dataset)
            mock_open.return_value = mock_ds

            result = builder.get_ds("grid", from_file=False)

            # get_ds returns a list of datasets
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] == mock_ds
            mock_open.assert_called_once_with(str(test_file), decode_timedelta=False)

    def test_get_ds_returns_none_when_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that get_ds returns None when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.blueprint = None

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
        blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.blueprint.code,
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
        builder.blueprint = blueprint

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

    def test_generate_inputs_raises_when_blueprint_none(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that generate_inputs raises when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.blueprint = None

        with pytest.raises(RuntimeError) as exc_info:
            builder.generate_inputs()
        assert "Blueprint must be initialized" in str(exc_info.value)


class TestForgeExecutorBuildAndRun:
    """Tests for configure_build."""

    def test_build_updates_compile_time_location(
        self, minimal_cstar_spec_builder_args
    ):
        """Test that configure_build() updates compile_time.location in blueprint."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        expected_code_output_dir = builder.compile_time_code_dir
        expected_location = str(expected_code_output_dir.resolve())

        with patch(
            "cstar_forge.forge.executor.render_roms_settings"
        ) as mock_render, patch("cstar_forge.forge.executor.write_roms_namelist"):
            mock_render.return_value = {
                "location": expected_location,
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            assert builder.blueprint is not None
            assert builder.blueprint.code is not None
            assert builder.blueprint.code.compile_time is not None
            assert builder.blueprint.code.compile_time.location == expected_location

    def test_build_sets_stage_to_build(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() sets _stage to BUILD."""
        from cstar_forge.forge.executor import BlueprintStage

        builder = _make_builder(minimal_cstar_spec_builder_args)

        with patch(
            "cstar_forge.forge.executor.render_roms_settings"
        ) as mock_render, patch("cstar_forge.forge.executor.write_roms_namelist"):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            assert builder._stage == BlueprintStage.BUILD

    def test_build_persists_blueprint(self, minimal_cstar_spec_builder_args):
        """Test that configure_build() persists blueprint to file."""
        from cstar_forge.forge.executor import BlueprintStage

        builder = _make_builder(minimal_cstar_spec_builder_args)

        with patch(
            "cstar_forge.forge.executor.render_roms_settings"
        ) as mock_render, patch("cstar_forge.forge.executor.write_roms_namelist"):
            mock_render.return_value = {
                "location": str(builder.compile_time_code_dir),
                "filter": {"files": ["test.opt"]},
                "branch": "main",
            }

            builder.configure_build()

            expected_bp_path = builder.path_blueprint(stage=BlueprintStage.BUILD)
            assert expected_bp_path.exists()

            with open(expected_bp_path) as f:
                blueprint_data = yaml.safe_load(f)
                assert blueprint_data is not None
                assert "code" in blueprint_data
                assert "compile_time" in blueprint_data["code"]
                assert "location" in blueprint_data["code"]["compile_time"]

    def test_build_stages_compile_time_templates(
        self, minimal_cstar_spec_builder_args
    ):
        """configure_build() stages the compile-time templates (via C-Star
        AdditionalCode) and renders from that staged directory."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with patch(
            "cstar_forge.forge.executor.render_roms_settings"
        ) as mock_render, patch("cstar_forge.forge.executor.write_roms_namelist"):
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

    @pytest.mark.real_template_staging
    def test_template_repo_args_map_from_code_spec(
        self, minimal_cstar_spec_builder_args
    ):
        """The (unpatched) cfg->AdditionalCode-args mapping forwards the git ref from
        code.templates_* verbatim — the Forge side of the fetch that CI can't run live."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        for stage in ("compile_time", "run_time"):
            repo = getattr(builder.code_spec, f"templates_{stage}")
            args = builder._template_repo_args(stage)
            assert args["location"] == str(repo.location)
            assert args["subdir"] == (repo.directory or "")
            assert args["checkout_target"] == (repo.commit or repo.branch or "")
            assert args["files"] == list(repo.files)
        # Resolver default: github repo + branch main, repo-root-relative directory.
        ct = builder._template_repo_args("compile_time")
        assert ct["location"].endswith("cstar-forge.git")
        assert ct["subdir"] == "templates/compile-time"
        assert ct["checkout_target"] == "main"
        assert ct["files"] == ["cppdefs.opt.j2"]


class TestForgeExecutorPathBlueprint:
    """Tests for path_blueprint method."""

    def test_path_blueprint_preconfig(self, minimal_cstar_spec_builder_args):
        """Test path_blueprint for preconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_blueprint(stage="preconfig")

        assert "preconfig" in str(path)
        assert builder.name in str(path)
        assert path.suffix == ".yml"

    def test_path_blueprint_postconfig(self, minimal_cstar_spec_builder_args):
        """Test path_blueprint for postconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_blueprint(stage="postconfig")

        assert "postconfig" in str(path)
        assert builder.name in str(path)

    def test_path_blueprint_build(self, minimal_cstar_spec_builder_args):
        """Test path_blueprint for build stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_blueprint(stage="build")

        assert "build" in str(path)
        assert builder.name in str(path)
        assert path.name.endswith("_build.yml")

    def test_path_blueprint_run_with_params(
        self, minimal_cstar_spec_builder_args, sample_runtime_params
    ):
        """Test path_blueprint for run stage with runtime params."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        path = builder.path_blueprint(stage="run", run_params=sample_runtime_params)

        assert "run" in str(path)
        assert "20120101" in str(path)  # start_date
        assert "20120102" in str(path)  # end_date

    def test_path_blueprint_run_without_params(self, minimal_cstar_spec_builder_args):
        """Test path_blueprint for run stage without params raises error."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(ValueError) as exc_info:
            builder.path_blueprint(stage="run", run_params=None)
        assert "run_params is required" in str(exc_info.value)

    def test_path_blueprint_invalid_stage(self, minimal_cstar_spec_builder_args):
        """Test path_blueprint with invalid stage raises error."""
        builder = _make_builder(minimal_cstar_spec_builder_args)

        with pytest.raises(ValueError) as exc_info:
            builder.path_blueprint(stage="invalid_stage")
        assert "stage must be one of" in str(exc_info.value)

    def test_path_blueprint_uses_blueprint_state(
        self, minimal_cstar_spec_builder_args
    ):
        """Test path_blueprint uses blueprint state when stage is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.blueprint.state = "postconfig"

        path = builder.path_blueprint(stage=None)
        assert "postconfig" in str(path)


class TestForgeExecutorPersist:
    """Tests for persist method."""

    def test_persist_preconfig(self, minimal_cstar_spec_builder_args):
        """Test persist for preconfig stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "preconfig"

        builder.persist()

        bp_path = builder.path_blueprint(stage="preconfig")
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

        bp_path = builder.path_blueprint(stage="postconfig")
        assert bp_path.exists()

    def test_persist_run(self, minimal_cstar_spec_builder_args, sample_runtime_params):
        """Test persist for run stage."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder._stage = "run"
        builder.blueprint.runtime_params = sample_runtime_params

        builder.persist()

        bp_path = builder.path_blueprint(stage="run", run_params=sample_runtime_params)
        assert bp_path.exists()

    def test_persist_raises_when_blueprint_none(self, minimal_cstar_spec_builder_args):
        """Test persist raises error when blueprint is None."""
        builder = _make_builder(minimal_cstar_spec_builder_args)
        builder.blueprint = None
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
        builder.blueprint.runtime_params = None

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
        mock_blueprint_elements = MagicMock()
        mock_blueprint_elements.grid = MagicMock()
        mock_blueprint_elements.initial_conditions = MagicMock()
        mock_blueprint_elements.forcing = MagicMock()
        mock_blueprint_elements.cdr_forcing = None
        mock_input_data_instance.generate_all.return_value = (
            mock_blueprint_elements,
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

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_test_mode_does_not_persist(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs in test mode does not persist blueprint."""
        mock_input_data_instance = MagicMock()
        mock_blueprint_elements = MagicMock()
        mock_blueprint_elements.grid = MagicMock()
        mock_blueprint_elements.initial_conditions = MagicMock()
        mock_blueprint_elements.forcing = MagicMock()
        mock_blueprint_elements.cdr_forcing = None
        mock_input_data_instance.generate_all.return_value = (
            mock_blueprint_elements,
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
    def test_generate_inputs_raises_when_blueprint_elements_none(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
    ):
        """Test generate_inputs raises RuntimeError when blueprint_elements is None."""
        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (None, {}, {})
        mock_input_data_class.return_value = mock_input_data_instance

        with patch.object(ForgeExecutor, "ensure_source_data"):
            builder = _make_builder(minimal_cstar_spec_builder_args)

            with pytest.raises(RuntimeError) as exc_info:
                builder.generate_inputs(clobber=True)
            assert (
                "_settings_compile_time" in str(exc_info.value)
                or "Blueprint mismatch" in str(exc_info.value)
            )

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_nesting_info_serialized_to_blueprint_dict(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
        tmp_path,
    ):
        """Test that nesting_info from blueprint_elements is written into the blueprint dict."""
        nesting_file = tmp_path / "nesting.nc"
        nesting_file.touch()
        nesting_dataset = cstar_models.Dataset(
            data=[Resource(location=str(nesting_file), partitioned=False)]
        )

        mock_blueprint_elements = MagicMock()
        mock_blueprint_elements.grid = MagicMock()
        mock_blueprint_elements.grid.model_dump.return_value = {}
        mock_blueprint_elements.initial_conditions = MagicMock()
        mock_blueprint_elements.initial_conditions.model_dump.return_value = {}
        mock_blueprint_elements.forcing = MagicMock()
        mock_blueprint_elements.forcing.model_dump.return_value = {}
        mock_blueprint_elements.cdr_forcing = None
        mock_blueprint_elements.nesting_info = nesting_dataset

        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (
            mock_blueprint_elements,
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
            nesting_info = builder.blueprint.nesting_info
            assert nesting_info is not None
            assert nesting_info["data"][0]["location"] == str(nesting_file)

    @patch("cstar_forge.forge.executor.input_data.RomsMarblInputData")
    def test_generate_inputs_nesting_info_none_in_blueprint_dict(
        self,
        mock_input_data_class,
        minimal_cstar_spec_builder_args,
        tmp_path,
    ):
        """Test that nesting_info is None in blueprint when elements.nesting_info is None."""
        mock_blueprint_elements = MagicMock()
        mock_blueprint_elements.grid = MagicMock()
        mock_blueprint_elements.grid.model_dump.return_value = {}
        mock_blueprint_elements.initial_conditions = MagicMock()
        mock_blueprint_elements.initial_conditions.model_dump.return_value = {}
        mock_blueprint_elements.forcing = MagicMock()
        mock_blueprint_elements.forcing.model_dump.return_value = {}
        mock_blueprint_elements.cdr_forcing = None
        mock_blueprint_elements.nesting_info = None

        mock_input_data_instance = MagicMock()
        mock_input_data_instance.generate_all.return_value = (
            mock_blueprint_elements,
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

            assert builder.blueprint.nesting_info is None


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
        blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.blueprint.code,
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
        builder.blueprint = blueprint

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
        blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.blueprint.code,
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
        builder.blueprint = blueprint

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
        blueprint = cstar_models.RomsMarblBlueprint(
            name="test",
            description="Test",
            valid_start_date=datetime(2012, 1, 1),
            valid_end_date=datetime(2012, 1, 2),
            code=builder.blueprint.code,
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
        builder.blueprint = blueprint

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


class TestBlueprintStage:
    """Tests for BlueprintStage class."""

    def test_blueprintstage_constants(self):
        """Test BlueprintStage constants."""
        from cstar_forge.forge.executor import BlueprintStage

        assert BlueprintStage.PRECONFIG == "preconfig"
        assert BlueprintStage.POSTCONFIG == "postconfig"
        assert BlueprintStage.BUILD == "build"
        assert BlueprintStage.RUN == "run"

    def test_blueprintstage_validate_stage_valid(self):
        """Test BlueprintStage.validate_stage with valid stage."""
        from cstar_forge.forge.executor import BlueprintStage

        assert BlueprintStage.validate_stage("preconfig") == "preconfig"
        assert BlueprintStage.validate_stage("postconfig") == "postconfig"
        assert BlueprintStage.validate_stage("build") == "build"
        assert BlueprintStage.validate_stage("run") == "run"

    def test_blueprintstage_validate_stage_invalid(self):
        """Test BlueprintStage.validate_stage with invalid stage."""
        from cstar_forge.forge.executor import BlueprintStage

        with pytest.raises(ValueError) as exc_info:
            BlueprintStage.validate_stage("invalid")
        assert "stage must be one of" in str(exc_info.value)
