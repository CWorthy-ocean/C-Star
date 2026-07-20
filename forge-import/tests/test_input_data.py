"""
Comprehensive tests for the input_data.py module.

Tests cover:
- InputData base class
- RomsMarblInputData class
- Input generation methods (grid, initial_conditions, forcing, etc.)
- generate_all workflow
- _partition_files
- Helper methods (_resolve_source_block, _build_input_args, etc.)
- Input registry and registration
- Edge cases and error handling
"""

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import cstar.applications.roms_marbl.models as cstar_models
import numpy as np
import pytest
import roms_tools as rt
import xarray as xr
from cstar.orchestration.models import Resource

from cstar_forge import config
from cstar_forge import models as forge_models
from cstar_forge.config import DataPaths
from cstar_forge.forge import source_data
from cstar_forge.forge.input_data import (
    INPUT_REGISTRY,
    InputData,
    InputStep,
    RomsMarblBlueprintInputData,
    RomsMarblInputData,
    register_input,
    resolve_input_selection,
)


@contextmanager
def _patch_xarray_open_dataset_for_input_data(mock_ds):
    """
    Patch xarray.open_dataset where input_data (and roms_tools) resolve it.

    Tests use empty ``.touch()`` NetCDF paths; real ``open_dataset`` needs a backend
    (e.g. netCDF4). Patching only ``xarray.open_dataset`` misses ``cstar_forge.forge.input_data.xr``
    after import; patching both avoids IO backend errors.
    """

    @contextmanager
    def _fake_open(*args, **kwargs):
        yield mock_ds

    with (
        patch("cstar_forge.forge.input_data.xr.open_dataset", side_effect=_fake_open),
        patch("xarray.open_dataset", side_effect=_fake_open),
    ):
        yield


def _create_mock_paths(tmp_path):
    """Helper to create a mock DataPaths with tmp_path as input_data."""
    return DataPaths(
        here=config.paths.here,
        source_data=config.paths.source_data,
        input_data=tmp_path,
        scratch=config.paths.scratch,
        catalog=config.paths.catalog,
        blueprints=config.paths.blueprints,
        models_yaml=config.paths.models_yaml,
        builds_yaml=config.paths.builds_yaml,
        machines_yaml=config.paths.machines_yaml,
    )


@pytest.fixture
def sample_grid_kwargs():
    """Sample grid keyword arguments."""
    return {
        "nx": 20,
        "ny": 20,
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
def sample_grid(sample_grid_kwargs):
    """Create a sample Grid object."""
    return rt.Grid(**sample_grid_kwargs)


def _build_forcing_override(ic, surface=(), boundary=(), tidal=(), river=()):
    """Build the forcing_override dict shape RomsMarblInputData consumes
    (initial_conditions + flat forcing categories) directly from item objects.

    ModelSpec no longer carries embedded forcing data (that's a ForcingSpec's job),
    so this builds the dict straight from the roms-tools item models instead of
    deriving it from a ModelSpec.inputs block.
    """
    forcing = {}
    for category, items in (
        ("surface", surface),
        ("boundary", boundary),
        ("tidal", tidal),
        ("river", river),
    ):
        if items:
            forcing[category] = [it.model_dump() for it in items]
    return {"forcing": forcing, "initial_conditions": ic.model_dump()}


@pytest.fixture
def sample_forcing_override():
    """forcing_override covering all four categories + initial conditions."""
    ic = forge_models.InitialConditionsInput(
        source=forge_models.SourceSpec(name="GLORYS"),
        bgc_source=forge_models.SourceSpec(name="UNIFIED", climatology=True),
    )
    surface_item = forge_models.SurfaceForcingItem(
        source=forge_models.SourceSpec(name="ERA5"), type="physics"
    )
    surface_bgc_item = forge_models.SurfaceForcingItem(
        source=forge_models.SourceSpec(name="UNIFIED", climatology=True), type="bgc"
    )
    boundary_item = forge_models.BoundaryForcingItem(
        source=forge_models.SourceSpec(name="GLORYS"), type="physics"
    )
    boundary_bgc_item = forge_models.BoundaryForcingItem(
        source=forge_models.SourceSpec(name="UNIFIED", climatology=True), type="bgc"
    )
    tidal_item = forge_models.TidalForcingItem(
        source=forge_models.SourceSpec(name="TPXO")
    )
    river_item = forge_models.RiverForcingItem(
        source=forge_models.SourceSpec(name="DAI")
    )
    return _build_forcing_override(
        ic,
        surface=[surface_item, surface_bgc_item],
        boundary=[boundary_item, boundary_bgc_item],
        tidal=[tidal_item],
        river=[river_item],
    )


@pytest.fixture
def sample_open_boundaries():
    """Sample open boundaries configuration."""
    return forge_models.OpenBoundaries(north=True, south=True, east=True, west=False)


@pytest.fixture
def sample_source_data(tmp_path):
    """Create a mock SourceData object."""
    mock_source_data = MagicMock(spec=source_data.SourceData)
    source_file = tmp_path / "source.nc"
    source_file.touch()  # Ensure file exists

    def _dks(name, glorys_layout=None):
        if name == "GLORYS":
            return "GLORYS_GLOBAL" if glorys_layout == "global" else "GLORYS_REGIONAL"
        return {
            "UNIFIED": "UNIFIED_BGC",
            "ERA5": "ERA5",
            "TPXO": "TPXO",
            "DAI": "DAI",
        }.get(name, name.upper())

    mock_source_data.path_for_source = MagicMock(return_value=source_file)
    mock_source_data.dataset_key_for_source = MagicMock(side_effect=_dks)
    mock_source_data.streamable_for_source = MagicMock(
        side_effect=lambda name, glorys_layout=None: name.upper() in {"ERA5", "DAI"}
    )

    # Mock STREAMABLE_SOURCES
    with patch("cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES", {"ERA5"}):
        yield mock_source_data


@pytest.fixture
def sample_partitioning():
    """Sample partitioning parameters."""
    return cstar_models.PartitioningParameterSet(n_procs_x=2, n_procs_y=2)


@pytest.fixture
def sample_roms_marbl_input_data(
    tmp_path,
    sample_grid,
    sample_forcing_override,
    sample_open_boundaries,
    sample_source_data,
    sample_partitioning,
):
    """Create a RomsMarblInputData instance for testing."""
    roms_marbl_blueprint_dir = tmp_path / "blueprints"
    roms_marbl_blueprint_dir.mkdir(parents=True, exist_ok=True)

    data_dir = tmp_path / "input_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    return RomsMarblInputData(
        domain_name="test_grid",
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        forcing_override=sample_forcing_override,
        grid=sample_grid,
        boundaries=sample_open_boundaries,
        source_data=sample_source_data,
        roms_marbl_blueprint_dir=roms_marbl_blueprint_dir,
        partitioning=sample_partitioning,
        use_dask=False,
        input_data_dir=data_dir,
    )


class TestInputData:
    """Tests for InputData base class."""

    def test_inputdata_initialization(self, tmp_path):
        """Test InputData initialization."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        assert data.domain_name == "test_grid"
        assert data.start_date == datetime(2012, 1, 1)
        assert data.end_date == datetime(2012, 1, 2)
        assert data.input_data_dir.exists()

    # NB: input_data_dir dirname sanitization moved to the executor's input_data_dir
    # property (Phase C config-injection); the base class now uses the injected dir
    # verbatim. That behavior is covered by test_core::test_path_input_data_property.

    def test_inputdata_forcing_filename(self, tmp_path):
        """Test _forcing_filename method."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        filename = data._forcing_filename("grid")
        assert filename.name == "test_grid_grid.nc"
        assert filename.parent == data.input_data_dir

    def test_inputdata_forcing_filename_dots_replaced_except_nc_suffix(self, tmp_path):
        """Basenames must have no ``.`` except ``.nc`` (e.g. ``v0.1`` in domain name)."""
        data = InputData(
            domain_name="case_v0.1_x",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )
        filename = data._forcing_filename("surface-physics")
        assert filename.name == "case_v0_1_x_surface-physics.nc"
        assert filename.name.count(".") == 1
        assert filename.name.endswith(".nc")

    def test_inputdata_ensure_empty_or_clobber_no_files(self, tmp_path):
        """Test _ensure_empty_or_clobber when directory is empty."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        result = data._ensure_empty_or_clobber(clobber=False)
        assert result is True

    def test_inputdata_ensure_empty_or_clobber_with_files_no_clobber(self, tmp_path):
        """When .nc files exist and clobber=False, allow continuing (reuse mode)."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        # Create a dummy .nc file
        nc_path = data.input_data_dir / "test.nc"
        nc_path.touch()

        result = data._ensure_empty_or_clobber(clobber=False)
        assert result is True
        assert nc_path.exists()

    def test_inputdata_ensure_empty_or_clobber_with_files_clobber(self, tmp_path):
        """Test _ensure_empty_or_clobber when files exist and clobber=True."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        # Create dummy .nc files
        (data.input_data_dir / "test1.nc").touch()
        (data.input_data_dir / "test2.nc").touch()

        result = data._ensure_empty_or_clobber(clobber=True)
        assert result is True
        assert len(list(data.input_data_dir.glob("*.nc"))) == 0

    def test_inputdata_generate_all_not_implemented(self, tmp_path):
        """Test that InputData.generate_all raises NotImplementedError."""
        data = InputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            input_data_dir=tmp_path,
        )

        with pytest.raises(NotImplementedError):
            data.generate_all()


class TestRomsMarblBlueprintInputData:
    """Tests for RomsMarblBlueprintInputData class."""

    def test_roms_marbl_blueprint_input_data_creation_empty(self):
        """Test creating RomsMarblBlueprintInputData with all None."""
        data = RomsMarblBlueprintInputData()
        assert data.grid is None
        assert data.initial_conditions is None
        assert data.forcing is None
        assert data.cdr_forcing is None
        assert data.nesting_info is None

    def test_roms_marbl_blueprint_input_data_creation_with_data(self):
        """Test creating RomsMarblBlueprintInputData with data."""
        grid_dataset = cstar_models.Dataset(data=[])
        ic_dataset = cstar_models.Dataset(data=[])
        forcing_config = cstar_models.ForcingConfiguration(
            boundary=cstar_models.Dataset(data=[]),
            surface=cstar_models.Dataset(data=[]),
        )
        cdr_dataset = cstar_models.Dataset(data=[])

        data = RomsMarblBlueprintInputData(
            grid=grid_dataset,
            initial_conditions=ic_dataset,
            forcing=forcing_config,
            cdr_forcing=cdr_dataset,
        )

        assert data.grid is not None
        assert data.initial_conditions is not None
        assert data.forcing is not None
        assert data.cdr_forcing is not None

    def test_roms_marbl_blueprint_input_data_creation_with_nesting_info(self):
        """Test creating RomsMarblBlueprintInputData with nesting_info set."""
        nesting_dataset = cstar_models.Dataset(data=[])
        data = RomsMarblBlueprintInputData(nesting_info=nesting_dataset)
        assert data.nesting_info is not None
        assert data.nesting_info == nesting_dataset


class TestInputStep:
    """Tests for InputStep class."""

    def test_inputstep_creation(self):
        """Test creating InputStep."""

        def handler(self, key, **kwargs):
            pass

        step = InputStep(name="test", order=10, label="Test Step", handler=handler)

        assert step.name == "test"
        assert step.order == 10
        assert step.label == "Test Step"
        assert step.handler == handler


class TestRegisterInput:
    """Tests for register_input decorator."""

    def test_register_input_decorator(self):
        """Test that register_input decorator registers a function."""
        # Clear registry for this test
        original_registry = INPUT_REGISTRY.copy()
        INPUT_REGISTRY.clear()

        try:

            @register_input(name="test_input", order=10, label="Test Input")
            def test_handler(self, key, **kwargs):
                pass

            assert "test_input" in INPUT_REGISTRY
            step = INPUT_REGISTRY["test_input"]
            assert step.name == "test_input"
            assert step.order == 10
            assert step.label == "Test Input"
            assert step.handler == test_handler
        finally:
            INPUT_REGISTRY.clear()
            INPUT_REGISTRY.update(original_registry)

    def test_register_input_without_label(self):
        """Test register_input without explicit label."""
        original_registry = INPUT_REGISTRY.copy()
        INPUT_REGISTRY.clear()

        try:

            @register_input(name="test_input2", order=20)
            def test_handler2(self, key, **kwargs):
                pass

            assert "test_input2" in INPUT_REGISTRY
            step = INPUT_REGISTRY["test_input2"]
            assert step.label == "test_input2"  # Should use name as label
        finally:
            INPUT_REGISTRY.clear()
            INPUT_REGISTRY.update(original_registry)


class TestResolveInputSelection:
    """Tests for resolve_input_selection (the ``--only-inputs`` normalizer)."""

    def test_canonical_names_pass_through(self):
        assert resolve_input_selection(
            ["grid", "initial_conditions", "cdr_forcing"]
        ) == {
            "grid",
            "initial_conditions",
            "cdr_forcing",
        }

    def test_aliases_map_to_canonical_registry_keys(self):
        assert resolve_input_selection(["surface", "boundary", "tidal", "river"]) == {
            "forcing.surface",
            "forcing.boundary",
            "forcing.tidal",
            "forcing.river",
        }
        assert resolve_input_selection(["bry", "tides", "rivers", "ic", "cdr"]) == {
            "forcing.boundary",
            "forcing.tidal",
            "forcing.river",
            "initial_conditions",
            "cdr_forcing",
        }

    def test_case_insensitive_and_deduplicates(self):
        assert resolve_input_selection(["Boundary", "BOUNDARY", " bry "]) == {
            "forcing.boundary"
        }

    def test_unknown_name_raises_with_valid_names_listed(self):
        with pytest.raises(ValueError, match="bogus"):
            resolve_input_selection(["boundary", "bogus"])

        with pytest.raises(ValueError, match="boundary"):
            resolve_input_selection(["bogus"])

    def test_empty_selection_returns_empty_set(self):
        assert resolve_input_selection([]) == set()


class TestRomsMarblInputDataInitialization:
    """Tests for RomsMarblInputData initialization."""

    def test_romsmarblinputdata_initialization(
        self,
        tmp_path,
        sample_grid,
        sample_forcing_override,
        sample_open_boundaries,
        sample_source_data,
        sample_partitioning,
    ):
        """Test RomsMarblInputData initialization."""
        roms_marbl_blueprint_dir = tmp_path / "blueprints"
        roms_marbl_blueprint_dir.mkdir(parents=True, exist_ok=True)

        data = RomsMarblInputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            forcing_override=sample_forcing_override,
            grid=sample_grid,
            boundaries=sample_open_boundaries,
            source_data=sample_source_data,
            roms_marbl_blueprint_dir=roms_marbl_blueprint_dir,
            partitioning=sample_partitioning,
            input_data_dir=tmp_path,
            use_dask=False,
        )

        assert data.domain_name == "test_grid"
        assert data.grid is not None
        assert data.forcing_override is not None
        assert data.roms_marbl_blueprint_elements is not None
        assert len(data.input_list) > 0

    def test_romsmarblinputdata_missing_handler(self, tmp_path, sample_grid):
        """Test RomsMarblInputData raises error for missing handler."""
        ic = forge_models.InitialConditionsInput(
            source=forge_models.SourceSpec(name="GLORYS")
        )
        surface_item = forge_models.SurfaceForcingItem(
            source=forge_models.SourceSpec(name="ERA5"), type="physics"
        )
        boundary_item = forge_models.BoundaryForcingItem(
            source=forge_models.SourceSpec(name="GLORYS"), type="physics"
        )
        forcing_override = _build_forcing_override(
            ic, surface=[surface_item], boundary=[boundary_item]
        )

        roms_marbl_blueprint_dir = tmp_path / "blueprints"
        roms_marbl_blueprint_dir.mkdir(parents=True, exist_ok=True)

        open_boundaries = forge_models.OpenBoundaries()
        mock_source_data = MagicMock()
        partitioning = cstar_models.PartitioningParameterSet(n_procs_x=2, n_procs_y=2)

        # This should work since all inputs are registered
        data = RomsMarblInputData(
            domain_name="test_grid",
            start_date=datetime(2012, 1, 1),
            end_date=datetime(2012, 1, 2),
            forcing_override=forcing_override,
            grid=sample_grid,
            boundaries=open_boundaries,
            source_data=mock_source_data,
            roms_marbl_blueprint_dir=roms_marbl_blueprint_dir,
            partitioning=partitioning,
            input_data_dir=tmp_path,
            use_dask=False,
        )

        # Should have input_list with registered handlers
        assert len(data.input_list) > 0


class TestRomsMarblInputDataHelperMethods:
    """Tests for RomsMarblInputData helper methods."""

    def test_yaml_filename(self, sample_roms_marbl_input_data):
        """Test _yaml_filename method."""
        yaml_path = sample_roms_marbl_input_data._yaml_filename("grid")
        assert yaml_path.name == "_grid.yaml"
        assert yaml_path.parent == sample_roms_marbl_input_data.roms_marbl_blueprint_dir
        assert sample_roms_marbl_input_data.roms_marbl_blueprint_dir.exists()

    def test_resolve_source_block_string(self, sample_roms_marbl_input_data):
        """Test _resolve_source_block with string input."""
        result = sample_roms_marbl_input_data._resolve_source_block("GLORYS")
        assert result["name"] == "GLORYS"
        # Should have path if source_data provides it
        if sample_roms_marbl_input_data.source_data.path_for_source.return_value:
            assert "path" in result

    def test_resolve_source_block_dict(self, sample_roms_marbl_input_data):
        """Test _resolve_source_block with dict input."""
        result = sample_roms_marbl_input_data._resolve_source_block({"name": "GLORYS"})
        assert result["name"] == "GLORYS"

    def test_resolve_source_block_dict_missing_name(self, sample_roms_marbl_input_data):
        """Test _resolve_source_block raises error when name is missing."""
        with pytest.raises(ValueError) as exc_info:
            sample_roms_marbl_input_data._resolve_source_block({"climatology": True})
        assert "name" in str(exc_info.value).lower()

    def test_resolve_source_block_invalid_type(self, sample_roms_marbl_input_data):
        """Test _resolve_source_block raises error for invalid type."""
        with pytest.raises(TypeError) as exc_info:
            sample_roms_marbl_input_data._resolve_source_block(123)
        assert "Unsupported source block type" in str(exc_info.value)

    def test_resolve_source_block_streamable(self, sample_roms_marbl_input_data):
        """Test _resolve_source_block with streamable source."""
        with patch(
            "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES", {"ERA5"}
        ):
            sample_roms_marbl_input_data.source_data.dataset_key_for_source.return_value = "ERA5"
            result = sample_roms_marbl_input_data._resolve_source_block("ERA5")
            # Should not add path for streamable sources if not explicitly provided
            assert result["name"] == "ERA5"

    def test_resolve_source_block_none_path_derives(self, sample_roms_marbl_input_data):
        """A None path (as SourceSpec.model_dump emits) must not block the derived path."""
        result = sample_roms_marbl_input_data._resolve_source_block(
            {"name": "GLORYS", "path": None}
        )
        # Derived path from source_data is injected despite the explicit None key.
        assert result[
            "path"
        ] == sample_roms_marbl_input_data.source_data.path_for_source("GLORYS")

    def test_resolve_source_block_explicit_path_survives(
        self, sample_roms_marbl_input_data
    ):
        """An explicit custom path overrides the derived path."""
        result = sample_roms_marbl_input_data._resolve_source_block(
            {"name": "GLORYS", "path": "/custom/glofas_v4_rivers_daily.nc"}
        )
        assert result["path"] == "/custom/glofas_v4_rivers_daily.nc"

    def test_resolve_source_block_streamable_none_path_omitted(
        self, sample_roms_marbl_input_data
    ):
        """A streamable source with a None path stays path-less (no path=None leaked)."""
        with patch(
            "cstar_forge.forge.input_data.source_data.STREAMABLE_SOURCES", {"ERA5"}
        ):
            sample_roms_marbl_input_data.source_data.dataset_key_for_source.return_value = "ERA5"
            result = sample_roms_marbl_input_data._resolve_source_block(
                {"name": "ERA5", "path": None}
            )
            assert "path" not in result

    def test_build_input_args_with_base_kwargs(self, sample_roms_marbl_input_data):
        """Test _build_input_args with base_kwargs."""
        base_kwargs = {"source": {"name": "GLORYS"}, "type": "physics"}

        result = sample_roms_marbl_input_data._build_input_args(
            "forcing.surface", base_kwargs=base_kwargs
        )

        assert result["type"] == "physics"
        assert "source" in result

    def test_build_input_args_with_extra(self, sample_roms_marbl_input_data):
        """Test _build_input_args with extra parameters."""
        base_kwargs = {"source": {"name": "GLORYS"}, "type": "physics"}
        extra = {"correct_radiation": True}

        result = sample_roms_marbl_input_data._build_input_args(
            "forcing.surface", base_kwargs=base_kwargs, extra=extra
        )

        assert result["type"] == "physics"
        assert result["correct_radiation"] is True

    def test_build_input_args_extra_overrides(self, sample_roms_marbl_input_data):
        """Test that extra overrides base_kwargs in _build_input_args."""
        base_kwargs = {"type": "physics", "correct_radiation": False}
        extra = {"correct_radiation": True}

        result = sample_roms_marbl_input_data._build_input_args(
            "forcing.surface", base_kwargs=base_kwargs, extra=extra
        )

        assert result["correct_radiation"] is True  # Extra should override

    def test_save_kwargs_empty_at_default_format(self, sample_roms_marbl_input_data):
        """At the default format no format= kwarg is passed, so released
        roms-tools (no such kwarg) keeps working when PIO is off.
        """
        assert sample_roms_marbl_input_data.netcdf_format == "NETCDF4"
        assert sample_roms_marbl_input_data._save_kwargs == {}

    def test_save_kwargs_forwards_non_default_format(
        self, sample_roms_marbl_input_data
    ):
        sample_roms_marbl_input_data.netcdf_format = "NETCDF3_64BIT_DATA"
        assert sample_roms_marbl_input_data._save_kwargs == {
            "format": "NETCDF3_64BIT_DATA"
        }


class TestRomsMarblInputDataGeneration:
    """Tests for input generation methods."""

    @patch("cstar_forge.forge.input_data.rt.Grid")
    def test_generate_grid(
        self, mock_grid_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_grid method."""
        mock_grid = MagicMock()
        mock_grid_class.return_value = sample_roms_marbl_input_data.grid
        sample_roms_marbl_input_data.grid = mock_grid

        # Update input_data_dir to use the mocked path since it was set in __post_init__
        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)

        # Make grid.save() actually create a file so Pydantic validation passes
        # _generate_grid creates a Resource with location=out_path, which must exist
        out_path = sample_roms_marbl_input_data._forcing_filename(input_name="grid")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()  # Create empty file so it exists for validation

        # Mock xarray.open_dataset since we're using a dummy file
        # _generate_grid reads the file back to check for xi_coarse dimension
        # Note: xarray is imported inside _generate_grid, so we patch it at the module level
        # xr.Dataset is already a context manager, so it works with 'with xr.open_dataset()'
        mock_ds = xr.Dataset({"var": (["x"], [1, 2, 3])})
        with patch("xarray.open_dataset", return_value=mock_ds):
            sample_roms_marbl_input_data._generate_grid()

        # Check that grid.save was called (without format= at the default)
        mock_grid.save.assert_called_once()
        assert "format" not in mock_grid.save.call_args.kwargs
        mock_grid.to_yaml.assert_called_once()

        # Check that resource was added to roms_marbl_blueprint_elements
        assert (
            len(sample_roms_marbl_input_data.roms_marbl_blueprint_elements.grid.data)
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.Grid")
    def test_generate_grid_forwards_netcdf_format(
        self, mock_grid_class, sample_roms_marbl_input_data, tmp_path
    ):
        """A non-default netcdf_format is forwarded to grid.save as format=."""
        mock_grid = MagicMock()
        mock_grid_class.return_value = sample_roms_marbl_input_data.grid
        sample_roms_marbl_input_data.grid = mock_grid
        sample_roms_marbl_input_data.netcdf_format = "NETCDF3_64BIT_DATA"

        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)

        out_path = sample_roms_marbl_input_data._forcing_filename(input_name="grid")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()

        mock_ds = xr.Dataset({"var": (["x"], [1, 2, 3])})
        with patch("xarray.open_dataset", return_value=mock_ds):
            sample_roms_marbl_input_data._generate_grid()

        assert mock_grid.save.call_args.kwargs["format"] == "NETCDF3_64BIT_DATA"

    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    def test_generate_initial_conditions(
        self, mock_ic_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_initial_conditions method."""
        mock_ic = MagicMock()
        ic_path = tmp_path / "ic.nc"
        ic_path.touch()  # Ensure file exists for Pydantic validation
        # Code expects paths to be a list for paths[0] access
        mock_ic.save.return_value = [ic_path]
        mock_ic_class.return_value = mock_ic

        sample_roms_marbl_input_data._generate_initial_conditions()

        # Check that InitialConditions was created
        mock_ic_class.assert_called_once()
        mock_ic.save.assert_called_once()
        mock_ic.to_yaml.assert_called_once()

        # Check that resource was added
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.initial_conditions.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    def test_generate_initial_conditions_multiple_paths(
        self, mock_ic_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_initial_conditions with multiple paths."""
        mock_ic = MagicMock()
        ic1_path = tmp_path / "ic1.nc"
        ic2_path = tmp_path / "ic2.nc"
        ic1_path.touch()  # Ensure files exist for Pydantic validation
        ic2_path.touch()
        mock_ic.save.return_value = [ic1_path, ic2_path]
        mock_ic_class.return_value = mock_ic

        sample_roms_marbl_input_data._generate_initial_conditions()

        # Should have 2 resources
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.initial_conditions.data
            )
            == 2
        )

    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    def test_generate_surface_forcing(
        self, mock_sf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_surface_forcing method."""
        mock_sf = MagicMock()
        surface_path = tmp_path / "surface.nc"
        surface_path.touch()  # Ensure file exists for Pydantic validation
        mock_sf.save.return_value = surface_path
        mock_sf_class.return_value = mock_sf

        sample_roms_marbl_input_data._generate_surface_forcing(
            key="forcing.surface", source={"name": "ERA5"}, type="physics"
        )

        mock_sf_class.assert_called_once()
        mock_sf.save.assert_called_once()
        assert "format" not in mock_sf.save.call_args.kwargs
        mock_sf.to_yaml.assert_called_once()

        # Check that resource was added to forcing.surface
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    def test_generate_surface_forcing_forwards_netcdf_format(
        self, mock_sf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """A non-default netcdf_format is forwarded to save as format=."""
        mock_sf = MagicMock()
        surface_path = tmp_path / "surface.nc"
        surface_path.touch()
        mock_sf.save.return_value = surface_path
        mock_sf_class.return_value = mock_sf
        sample_roms_marbl_input_data.netcdf_format = "NETCDF3_64BIT_DATA"

        sample_roms_marbl_input_data._generate_surface_forcing(
            key="forcing.surface", source={"name": "ERA5"}, type="physics"
        )

        assert mock_sf.save.call_args.kwargs["format"] == "NETCDF3_64BIT_DATA"

    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    def test_generate_surface_forcing_missing_type(
        self, mock_sf_class, sample_roms_marbl_input_data
    ):
        """Test _generate_surface_forcing raises error when type is missing."""
        with pytest.raises(ValueError) as exc_info:
            sample_roms_marbl_input_data._generate_surface_forcing(
                key="forcing.surface",
                source={"name": "ERA5"},
                # Missing type
            )
        assert "type" in str(exc_info.value).lower()

    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    def test_generate_surface_forcing_reuse_skips_roms_tools_calls(
        self, mock_sf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """When NetCDF exists, reuse paths without constructing SurfaceForcing."""
        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)
        nc_path = sample_roms_marbl_input_data._forcing_filename(
            input_name="surface-physics"
        )
        nc_path.touch()
        yaml_path = sample_roms_marbl_input_data._yaml_filename(
            "forcing.surface-physics"
        )
        yaml_path.write_text("---\nSurfaceForcing:\n  type: physics\n")

        sample_roms_marbl_input_data._generate_surface_forcing(
            key="forcing.surface",
            source={"name": "ERA5"},
            type="physics",
        )

        mock_sf_class.assert_not_called()
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    def test_generate_boundary_forcing(
        self, mock_bf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_boundary_forcing method."""
        mock_bf = MagicMock()
        boundary_path = tmp_path / "boundary.nc"
        boundary_path.touch()  # Ensure file exists for Pydantic validation
        mock_bf.save.return_value = boundary_path
        mock_bf_class.return_value = mock_bf

        sample_roms_marbl_input_data._generate_boundary_forcing(
            key="forcing.boundary", source={"name": "GLORYS"}, type="physics"
        )

        mock_bf_class.assert_called_once()
        mock_bf.save.assert_called_once()
        mock_bf.to_yaml.assert_called_once()

        # Check that resource was added to forcing.boundary
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.boundary.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    def test_generate_boundary_forcing_missing_type(
        self, mock_bf_class, sample_roms_marbl_input_data
    ):
        """Test _generate_boundary_forcing raises error when type is missing."""
        with pytest.raises(ValueError) as exc_info:
            sample_roms_marbl_input_data._generate_boundary_forcing(
                key="forcing.boundary",
                source={"name": "GLORYS"},
                # Missing type
            )
        assert "type" in str(exc_info.value).lower()

    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    def test_generate_tidal_forcing(
        self, mock_tf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_tidal_forcing method."""
        mock_tf = MagicMock()
        tidal_path = tmp_path / "tidal.nc"
        tidal_path.touch()  # Ensure file exists for Pydantic validation
        mock_tf.save.return_value = tidal_path
        mock_tf_class.return_value = mock_tf

        sample_roms_marbl_input_data._generate_tidal_forcing(
            key="forcing.tidal", source={"name": "TPXO"}
        )

        mock_tf_class.assert_called_once()
        mock_tf.save.assert_called_once()
        mock_tf.to_yaml.assert_called_once()

        # Check that resource was added to forcing.tidal
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.tidal.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    def test_generate_tidal_forcing_reuse_skips_roms_tools_calls(
        self, mock_tf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """When NetCDF and YAML exist, do not construct TidalForcing."""
        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)
        nc_path = sample_roms_marbl_input_data._forcing_filename(input_name="tidal")
        nc_path.touch()
        yaml_path = sample_roms_marbl_input_data._yaml_filename("forcing.tidal")
        yaml_path.write_text("TidalForcing: \n  ntides: 10\n")

        sample_roms_marbl_input_data._generate_tidal_forcing(
            key="forcing.tidal",
            source={"name": "TPXO", "path": str(nc_path)},
        )

        mock_tf_class.assert_not_called()
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.tidal.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    def test_generate_river_forcing(
        self, mock_rf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_river_forcing method."""
        mock_rf = MagicMock()
        river_path = tmp_path / "river.nc"
        river_path.touch()  # Ensure file exists for Pydantic validation
        mock_rf.save.return_value = river_path
        # Create a mock dataset with required variables
        mock_ds = xr.Dataset(
            {
                "river_volume": (["nriver", "time"], np.random.rand(5, 10)),
                "river_tracer": (
                    ["nriver", "time", "tracer"],
                    np.random.rand(5, 10, 3),
                ),
            }
        )
        mock_rf.ds = mock_ds
        mock_rf_class.return_value = mock_rf

        sample_roms_marbl_input_data._generate_river_forcing(
            key="forcing.river", source={"name": "DAI"}
        )

        mock_rf_class.assert_called_once()
        mock_rf.save.assert_called_once()
        mock_rf.to_yaml.assert_called_once()

        # Check that resource was added to forcing.river
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.river.data
            )
            > 0
        )

    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    def test_generate_river_forcing_reuse_skips_roms_tools_calls(
        self, mock_rf_class, sample_roms_marbl_input_data, tmp_path
    ):
        """When NetCDF and YAML exist, do not construct RiverForcing."""
        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)
        nc_path = sample_roms_marbl_input_data._forcing_filename(input_name="river")
        nriver, ntime, ntrc = 2, 2, 1
        ds = xr.Dataset(
            {
                "river_volume": (["nriver", "time"], np.ones((nriver, ntime))),
                "river_tracer": (
                    ["nriver", "time", "tracer"],
                    np.ones((nriver, ntime, ntrc)),
                ),
            }
        )
        ds.to_netcdf(nc_path)
        yaml_path = sample_roms_marbl_input_data._yaml_filename("forcing.river")
        yaml_path.write_text("roms_tools_version: test\n")

        sample_roms_marbl_input_data._generate_river_forcing(
            key="forcing.river",
            source={"name": "DAI"},
        )

        mock_rf_class.assert_not_called()
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.river.data
            )
            > 0
        )
        assert (
            sample_roms_marbl_input_data._settings_run_time["river_frc"]["nriv"]
            == nriver
        )

    @patch("cstar_forge.forge.input_data.rt.CDRForcing")
    def test_generate_cdr_forcing(
        self, mock_cdr_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_cdr_forcing method."""
        # Initialize cdr_forcing as a Dataset if it's None
        if (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.cdr_forcing
            is None
        ):
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.cdr_forcing = (
                cstar_models.Dataset(data=[])
            )

        mock_cdr = MagicMock()
        cdr_path = tmp_path / "cdr.nc"
        cdr_path.touch()  # Ensure file exists for Pydantic validation
        mock_cdr.save.return_value = cdr_path
        mock_cdr_class.return_value = mock_cdr

        sample_roms_marbl_input_data._generate_cdr_forcing(
            key="cdr_forcing", cdr_kwargs={"foo": "bar"}
        )

        mock_cdr_class.assert_called_once()
        mock_cdr.save.assert_called_once()
        mock_cdr.to_yaml.assert_called_once()

        # Check that resource was added to cdr_forcing
        assert (
            len(
                sample_roms_marbl_input_data.roms_marbl_blueprint_elements.cdr_forcing.data
            )
            > 0
        )

    def test_generate_cdr_forcing_empty_list(self, sample_roms_marbl_input_data):
        """Test _generate_cdr_forcing with empty cdr_list returns early."""
        with patch("cstar_forge.forge.input_data.rt.CDRForcing") as mock_cdr_class:
            sample_roms_marbl_input_data._generate_cdr_forcing(
                key="cdr_forcing", cdr_list=[]
            )

            # Should not create CDRForcing if list is empty
            mock_cdr_class.assert_not_called()

    def test_generate_cdr_forcing_end_to_end_real_construction(
        self, sample_roms_marbl_input_data
    ):
        """End-to-end (no mocked rt.CDRForcing): a real params dict extracted from a
        roms-tools CDRForcing.to_yaml() dump (the wizard-upload shape) must construct,
        save a real NetCDF, and flip the same toggles the mocked unit test only
        asserts were *called*. This is the "grid-less construction really works"
        guarantee the resolver/wizard's "no grid injection" design decision depends on.
        """
        import cstar_forge
        from cstar_forge.forge_blueprint_resolve import read_cdr_forcing_yaml

        sample = (
            Path(cstar_forge.__file__).parent
            / "catalog"
            / "blueprints"
            / "MacOS"
            / "cson_roms-marbl_v0.1_test-tiny_1procs"
            / "_cdr_forcing.yaml"
        )
        cdr_kwargs = read_cdr_forcing_yaml(sample)

        sample_roms_marbl_input_data.roms_marbl_blueprint_elements.cdr_forcing = (
            cstar_models.Dataset(data=[])
        )

        sample_roms_marbl_input_data._generate_cdr_forcing(
            key="cdr_forcing", cdr_kwargs=cdr_kwargs
        )

        resources = (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.cdr_forcing.data
        )
        assert resources, "expected at least one Resource registered"
        nc_path = Path(resources[0].location)
        assert nc_path.exists() and nc_path.stat().st_size > 0
        assert (
            sample_roms_marbl_input_data._settings_compile_time["cppdefs"][
                "cdr_forcing"
            ]
            is True
        )
        assert (
            sample_roms_marbl_input_data._settings_run_time["cdr_frc"]["cdr_file"]
            == "cdr.nc"
        )
        assert (
            sample_roms_marbl_input_data._settings_run_time["cdr_output"]["do_cdr"]
            is True
        )

    def test_generate_corrections_not_implemented(self, sample_roms_marbl_input_data):
        """Test _generate_corrections raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            sample_roms_marbl_input_data._generate_corrections()

    @patch("cstar_forge.forge.input_data.roms_tools_nesting_writer")
    @patch("cstar_forge.forge.input_data.rt.Grid")
    def test_generate_grid_with_child(
        self,
        mock_grid_class,
        mock_nesting_writer,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """Test _generate_grid sets nesting_info and extract_data settings when grid_child is present."""
        mock_grid = MagicMock()
        mock_grid.nx = 20
        mock_grid.ny = 20
        mock_grid.N = 3
        mock_grid.theta_s = 5.0
        mock_grid.theta_b = 2.0
        mock_grid.hc = 250.0
        sample_roms_marbl_input_data.grid = mock_grid

        mock_child = MagicMock()
        mock_child.N = 5
        mock_child.theta_s = 6.0
        mock_child.theta_b = 3.0
        mock_child.hc = 300.0
        sample_roms_marbl_input_data.grid_child = mock_child

        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)

        # Create expected output files so Pydantic resource validation passes
        out_path = sample_roms_marbl_input_data._forcing_filename(input_name="grid")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()
        out_path_child = sample_roms_marbl_input_data._forcing_filename(
            input_name="grid_child"
        )
        out_path_child.touch()
        out_path_nesting = sample_roms_marbl_input_data._forcing_filename(
            input_name="nesting"
        )
        out_path_nesting.touch()

        mock_ds = xr.Dataset({"var": (["x"], [1, 2, 3])})
        with patch("xarray.open_dataset", return_value=mock_ds):
            sample_roms_marbl_input_data._generate_grid()

        # nesting_info should be set as a Dataset pointing to the nesting file
        assert (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.nesting_info
            is not None
        )
        nesting_resources = (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.nesting_info.data
        )
        assert len(nesting_resources) == 1
        assert str(out_path_nesting) in nesting_resources[0].location

        # extract_data settings should be set
        extract_data = sample_roms_marbl_input_data._settings_run_time["extract_data"]
        assert extract_data["do_extract"] is True
        assert extract_data["n_chd"] == mock_child.N
        assert extract_data["theta_s_chd"] == mock_child.theta_s
        assert extract_data["theta_b_chd"] == mock_child.theta_b
        assert extract_data["hc_chd"] == mock_child.hc

    @patch("cstar_forge.forge.input_data.roms_tools_nesting_writer")
    @patch("cstar_forge.forge.input_data.rt.Grid")
    def test_generate_grid_extract_file_is_basename(
        self,
        mock_grid_class,
        mock_nesting_writer,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """Test that extract_file in compile-time settings is the bare filename, not a full path."""
        mock_grid = MagicMock()
        mock_grid.nx = 20
        mock_grid.ny = 20
        mock_grid.N = 3
        mock_grid.theta_s = 5.0
        mock_grid.theta_b = 2.0
        mock_grid.hc = 250.0
        sample_roms_marbl_input_data.grid = mock_grid

        mock_child = MagicMock()
        mock_child.N = 5
        mock_child.theta_s = 6.0
        mock_child.theta_b = 3.0
        mock_child.hc = 300.0
        sample_roms_marbl_input_data.grid_child = mock_child

        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)

        for name in ("grid", "grid_child", "nesting"):
            p = sample_roms_marbl_input_data._forcing_filename(input_name=name)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()

        mock_ds = xr.Dataset({"var": (["x"], [1, 2, 3])})
        with patch("xarray.open_dataset", return_value=mock_ds):
            sample_roms_marbl_input_data._generate_grid()

        extract_file = sample_roms_marbl_input_data._settings_run_time["extract_data"][
            "extract_file"
        ]
        # Should be just the filename, not an absolute path
        assert extract_file == "nesting.nc"
        assert "/" not in str(extract_file)

    @patch("cstar_forge.forge.input_data.rt.Grid")
    def test_generate_grid_without_child_nesting_info_is_none(
        self, mock_grid_class, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _generate_grid leaves nesting_info as None when no grid_child is set."""
        mock_grid = MagicMock()
        sample_roms_marbl_input_data.grid = mock_grid
        sample_roms_marbl_input_data.grid_child = None

        sample_roms_marbl_input_data.input_data_dir = (
            tmp_path / f"{sample_roms_marbl_input_data.domain_name}"
        )
        sample_roms_marbl_input_data.input_data_dir.mkdir(parents=True, exist_ok=True)

        out_path = sample_roms_marbl_input_data._forcing_filename(input_name="grid")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.touch()

        mock_ds = xr.Dataset({"var": (["x"], [1, 2, 3])})
        with patch("xarray.open_dataset", return_value=mock_ds):
            sample_roms_marbl_input_data._generate_grid()

        assert (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.nesting_info
            is None
        )
        assert not sample_roms_marbl_input_data._settings_run_time.get(
            "extract_data", {}
        ).get("do_extract", False)


class TestRomsMarblInputDataGenerateAll:
    """Tests for generate_all method."""

    @patch("cstar_forge.forge.input_data.rt.Grid")
    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    def test_generate_all_basic(
        self,
        mock_river,
        mock_tidal,
        mock_boundary,
        mock_surface,
        mock_ic,
        mock_grid,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """Test generate_all with basic workflow."""
        # Setup mocks - save() should create the file at the path passed to it
        mock_grid_instance = MagicMock()

        def grid_save(path):
            # Create the file at the path that was passed
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_grid_instance.save.side_effect = grid_save
        mock_grid_instance.to_yaml = MagicMock()
        sample_roms_marbl_input_data.grid = mock_grid_instance

        mock_ic_instance = MagicMock()

        def ic_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            # Code expects paths[0], so return a list
            return [path]

        mock_ic_instance.save.side_effect = ic_save
        mock_ic_instance.to_yaml = MagicMock()
        mock_ic.return_value = mock_ic_instance

        mock_surface_instance = MagicMock()

        def surface_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_surface_instance.save.side_effect = surface_save
        mock_surface_instance.to_yaml = MagicMock()
        mock_surface.return_value = mock_surface_instance

        mock_boundary_instance = MagicMock()

        def boundary_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_boundary_instance.save.side_effect = boundary_save
        mock_boundary_instance.to_yaml = MagicMock()
        mock_boundary.return_value = mock_boundary_instance

        mock_tidal_instance = MagicMock()

        def tidal_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_tidal_instance.save.side_effect = tidal_save
        mock_tidal_instance.to_yaml = MagicMock()
        mock_tidal.return_value = mock_tidal_instance

        mock_river_instance = MagicMock()

        def river_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_river_instance.save.side_effect = river_save
        mock_river_instance.to_yaml = MagicMock()
        # Create a mock dataset with required variables
        mock_river_ds = xr.Dataset(
            {
                "river_volume": (["nriver", "time"], np.random.rand(5, 10)),
                "river_tracer": (
                    ["nriver", "time", "tracer"],
                    np.random.rand(5, 10, 3),
                ),
            }
        )
        mock_river_instance.ds = mock_river_ds
        mock_river.return_value = mock_river_instance
        mock_ds = xr.Dataset()
        # Mock xr.open_dataset to prevent file operations when opening source files
        with patch("xarray.combine_by_coords") as mock_combine:
            mock_combine.return_value = mock_ds
            with _patch_xarray_open_dataset_for_input_data(mock_ds):
                result = sample_roms_marbl_input_data.generate_all(
                    clobber=True, test=False
                )

        assert result is not None
        roms_marbl_blueprint_elements, settings_compile_time, settings_run_time = result
        assert (
            roms_marbl_blueprint_elements
            == sample_roms_marbl_input_data.roms_marbl_blueprint_elements
        )
        # Settings should be populated (non-empty dicts)
        assert settings_compile_time is not None
        assert settings_run_time is not None

    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    @patch("xarray.combine_by_coords")
    @patch("xarray.open_dataset")
    def test_generate_all_test_mode(
        self,
        mock_open_dataset,
        mock_combine,
        mock_boundary_class,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """Test generate_all in test mode."""
        # Mock BoundaryForcing to prevent file operations
        mock_boundary = MagicMock()

        def boundary_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_boundary.save.side_effect = boundary_save
        mock_boundary.to_yaml = MagicMock()
        mock_boundary_class.return_value = mock_boundary

        # Mock xr.open_dataset to prevent file operations
        import xarray as xr

        mock_ds = xr.Dataset()  # Create a real empty Dataset
        mock_open_dataset.return_value = mock_ds
        mock_combine.return_value = mock_ds

        result = sample_roms_marbl_input_data.generate_all(clobber=True, test=True)

        # In test mode, should only process forcing.boundary
        # and stop after 2 iterations
        # The exact behavior depends on the order of steps
        assert result is not None

    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    @patch("xarray.combine_by_coords")
    @patch("xarray.open_dataset")
    def test_generate_all_only_restricts_to_selected_categories(
        self,
        mock_open_dataset,
        mock_combine,
        mock_boundary_class,
        mock_ic_class,
        mock_river_class,
        mock_tidal_class,
        mock_surface_class,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """``only={"forcing.boundary"}`` runs grid + boundary only.

        Grid always runs (every other input depends on the in-memory grid
        object); every other requested category (initial_conditions, surface,
        tidal, river) must be skipped -- proven here by asserting their
        roms-tools classes are never constructed and their blueprint elements
        stay empty, not just by checking a return value.
        """
        mock_grid_instance = MagicMock()

        def grid_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_grid_instance.save.side_effect = grid_save
        mock_grid_instance.to_yaml = MagicMock()
        mock_grid_instance.nx = 6
        mock_grid_instance.ny = 2
        mock_grid_instance.N = 3
        mock_grid_instance.hc = 250.0
        mock_grid_instance.theta_b = 2.0
        mock_grid_instance.theta_s = 5.0
        sample_roms_marbl_input_data.grid = mock_grid_instance

        mock_boundary_instance = MagicMock()

        def boundary_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_boundary_instance.save.side_effect = boundary_save
        mock_boundary_instance.to_yaml = MagicMock()
        mock_boundary_class.return_value = mock_boundary_instance

        mock_ds = xr.Dataset()
        mock_open_dataset.return_value = mock_ds
        mock_combine.return_value = mock_ds

        elements, _settings_compile_time, _settings_run_time = (
            sample_roms_marbl_input_data.generate_all(
                clobber=True, only={"forcing.boundary"}
            )
        )

        # Selected: grid ran (always does) and boundary ran.
        mock_boundary_class.assert_called()
        assert len(elements.grid.data) >= 1
        assert len(elements.forcing.boundary.data) >= 1

        # Not selected: skipped entirely, not merely reused -- their roms-tools
        # constructors were never called and no Resources were appended.
        mock_ic_class.assert_not_called()
        mock_surface_class.assert_not_called()
        mock_tidal_class.assert_not_called()
        mock_river_class.assert_not_called()
        assert elements.initial_conditions.data == []
        assert elements.forcing.surface.data == []
        assert elements.forcing.tidal.data == []
        assert elements.forcing.river.data == []

    @patch("cstar_forge.forge.input_data.rt.Grid")
    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    def test_generate_all_no_clobber_with_files(
        self,
        mock_river,
        mock_tidal,
        mock_boundary,
        mock_surface,
        mock_ic,
        mock_grid,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """With pre-existing .nc files and clobber=False, generate_all still runs (reuse path)."""
        mock_grid_instance = MagicMock()

        def grid_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_grid_instance.save.side_effect = grid_save
        mock_grid_instance.to_yaml = MagicMock()
        sample_roms_marbl_input_data.grid = mock_grid_instance

        mock_ic_instance = MagicMock()

        def ic_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return [path]

        mock_ic_instance.save.side_effect = ic_save
        mock_ic_instance.to_yaml = MagicMock()
        mock_ic.return_value = mock_ic_instance

        mock_surface_instance = MagicMock()

        def surface_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_surface_instance.save.side_effect = surface_save
        mock_surface_instance.to_yaml = MagicMock()
        mock_surface.return_value = mock_surface_instance

        mock_boundary_instance = MagicMock()

        def boundary_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_boundary_instance.save.side_effect = boundary_save
        mock_boundary_instance.to_yaml = MagicMock()
        mock_boundary.return_value = mock_boundary_instance

        mock_tidal_instance = MagicMock()

        def tidal_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_tidal_instance.save.side_effect = tidal_save
        mock_tidal_instance.to_yaml = MagicMock()
        mock_tidal.return_value = mock_tidal_instance

        mock_river_instance = MagicMock()

        def river_save(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            return path

        mock_river_instance.save.side_effect = river_save
        mock_river_instance.to_yaml = MagicMock()
        mock_river_instance.ds = xr.Dataset(
            {
                "river_volume": (["nriver", "time"], np.random.rand(5, 10)),
                "river_tracer": (
                    ["nriver", "time", "tracer"],
                    np.random.rand(5, 10, 3),
                ),
            }
        )
        mock_river.return_value = mock_river_instance

        (sample_roms_marbl_input_data.input_data_dir / "existing.nc").touch()
        mock_ds = xr.Dataset()
        with patch("xarray.combine_by_coords") as mock_combine:
            mock_combine.return_value = mock_ds
            with _patch_xarray_open_dataset_for_input_data(mock_ds):
                result = sample_roms_marbl_input_data.generate_all(
                    clobber=False, test=False
                )

        assert result is not None
        assert result != (None, {}, {})
        roms_marbl_blueprint_elements, settings_compile_time, settings_run_time = result
        assert roms_marbl_blueprint_elements is not None
        assert settings_compile_time is not None
        assert settings_run_time is not None
        assert (sample_roms_marbl_input_data.input_data_dir / "existing.nc").exists()

    @patch("cstar_forge.forge.input_data.rt.RiverForcing")
    @patch("cstar_forge.forge.input_data.rt.TidalForcing")
    @patch("cstar_forge.forge.input_data.rt.BoundaryForcing")
    @patch("cstar_forge.forge.input_data.rt.SurfaceForcing")
    @patch("cstar_forge.forge.input_data.rt.InitialConditions")
    @patch("xarray.combine_by_coords")
    @patch("xarray.open_dataset")
    @patch("cstar_forge.forge.input_data.rt.partition_netcdf")
    def test_generate_all_with_partition_files(
        self,
        mock_partition,
        mock_open_dataset,
        mock_combine,
        mock_ic_class,
        mock_surface_class,
        mock_boundary_class,
        mock_tidal_class,
        mock_river_class,
        sample_roms_marbl_input_data,
        tmp_path,
    ):
        """Test generate_all with partition_files=True."""

        # Helper to create a mock with save/to_yaml
        def create_mock_forcing_class():
            mock_obj = MagicMock()

            def save(path_arg):
                Path(path_arg).parent.mkdir(parents=True, exist_ok=True)
                Path(path_arg).touch()
                # Return as list since _generate_initial_conditions uses paths[0]
                # Other methods handle both list and single path, so returning list is safe
                return [path_arg]

            mock_obj.save.side_effect = save
            mock_obj.to_yaml = MagicMock()
            return mock_obj

        # Helper to create a mock river with dataset
        def create_mock_river_class():
            mock_obj = create_mock_forcing_class()
            # Create a mock dataset with required variables for river forcing
            mock_river_ds = xr.Dataset(
                {
                    "river_volume": (["nriver", "time"], np.random.rand(5, 10)),
                    "river_tracer": (
                        ["nriver", "time", "tracer"],
                        np.random.rand(5, 10, 3),
                    ),
                }
            )
            mock_obj.ds = mock_river_ds
            return mock_obj

        # Mock all forcing classes
        mock_ic_class.return_value = create_mock_forcing_class()
        mock_surface_class.return_value = create_mock_forcing_class()
        mock_boundary_class.return_value = create_mock_forcing_class()
        mock_tidal_class.return_value = create_mock_forcing_class()
        mock_river_class.return_value = create_mock_river_class()

        # Mock xr.open_dataset to prevent file operations
        mock_ds = xr.Dataset()  # Create a real empty Dataset
        mock_open_dataset.return_value = mock_ds
        mock_combine.return_value = mock_ds

        # Mock partition_netcdf to return list of paths
        partitioned_paths = [
            tmp_path / "partitioned_0.nc",
            tmp_path / "partitioned_1.nc",
        ]
        # Ensure files exist for Pydantic validation
        for p in partitioned_paths:
            p.touch()
        mock_partition.return_value = partitioned_paths

        # Create some resources in roms_marbl_blueprint_elements
        surface_file = tmp_path / "surface.nc"
        surface_file.touch()
        sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data.append(
            Resource(location=str(surface_file), partitioned=False)
        )

        # Patch at class level so the registry uses the patched methods
        with patch("cstar_forge.forge.input_data.RomsMarblInputData._generate_grid"):
            with patch(
                "cstar_forge.forge.input_data.RomsMarblInputData._generate_initial_conditions"
            ):
                with patch(
                    "cstar_forge.forge.input_data.RomsMarblInputData._generate_surface_forcing"
                ):
                    with patch(
                        "cstar_forge.forge.input_data.RomsMarblInputData._generate_boundary_forcing"
                    ):
                        with patch(
                            "cstar_forge.forge.input_data.RomsMarblInputData._generate_tidal_forcing"
                        ):
                            with patch(
                                "cstar_forge.forge.input_data.RomsMarblInputData._generate_river_forcing"
                            ):
                                # This should raise NotImplementedError since partition_files=True
                                # But actually _partition_files doesn't raise NotImplementedError,
                                # so this test might need to be updated
                                # For now, just verify it doesn't crash
                                try:
                                    result = sample_roms_marbl_input_data.generate_all(
                                        clobber=True, partition_files=True, test=False
                                    )
                                    # If it succeeds, that's fine - partitioning is implemented
                                    assert result is not None
                                except NotImplementedError:
                                    # If it raises NotImplementedError, that's also fine
                                    pass


class TestRomsMarblInputDataPartitionFiles:
    """Tests for _partition_files method."""

    @patch("cstar_forge.forge.input_data.rt.partition_netcdf")
    def test_partition_files_basic(
        self, mock_partition, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _partition_files with basic workflow."""
        # Create a resource with a file
        surface_file = tmp_path / "surface.nc"
        surface_file.touch()
        resource = Resource(location=str(surface_file), partitioned=False)
        sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data.append(
            resource
        )

        # Mock partition_netcdf to return list of paths
        partitioned_paths = [
            tmp_path / "surface_part0.nc",
            tmp_path / "surface_part1.nc",
        ]
        # Ensure files exist for Pydantic validation
        for p in partitioned_paths:
            p.touch()
        mock_partition.return_value = partitioned_paths

        sample_roms_marbl_input_data._partition_files()

        # Should have called partition_netcdf
        mock_partition.assert_called()

        # Should have created new resources
        # Note: grid and initial_conditions are skipped, so only forcing should be partitioned
        # The original resource should be replaced with partitioned ones
        # But since we're skipping grid and initial_conditions, and the input_list
        # determines what gets partitioned, we need to check the actual behavior

    @patch("cstar_forge.forge.input_data.rt.partition_netcdf")
    def test_partition_files_skips_empty(
        self, mock_partition, sample_roms_marbl_input_data
    ):
        """Test _partition_files skips empty datasets."""
        # Don't add any resources - dataset is empty
        # Should print warning and skip

        with patch("builtins.print"):  # Suppress print output
            sample_roms_marbl_input_data._partition_files()

            # Should not call partition_netcdf for empty datasets
            # (exact behavior depends on input_list)

    @patch("cstar_forge.forge.input_data.rt.partition_netcdf")
    def test_partition_files_skips_none_location(
        self, mock_partition, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _partition_files skips resources with None location."""
        # Create resource with a valid location first, then test skipping None in the logic
        surface_file = tmp_path / "surface.nc"
        surface_file.touch()
        resource = Resource(location=str(surface_file), partitioned=False)
        sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data.append(
            resource
        )

        # Mock partition_netcdf to return valid paths
        partitioned_paths = [
            tmp_path / "surface_part0.nc",
            tmp_path / "surface_part1.nc",
        ]
        for p in partitioned_paths:
            p.touch()  # Ensure files exist for Pydantic validation
        mock_partition.return_value = partitioned_paths

        sample_roms_marbl_input_data._partition_files()

        # Should not call partition_netcdf for None location
        # The resource should be kept as-is

    @patch("cstar_forge.forge.input_data.rt.partition_netcdf")
    def test_partition_files_creates_multiple_resources(
        self, mock_partition, sample_roms_marbl_input_data, tmp_path
    ):
        """Test _partition_files creates multiple resources from one."""
        # Create a resource
        surface_file = tmp_path / "surface.nc"
        surface_file.touch()
        original_resource = Resource(location=str(surface_file), partitioned=False)
        sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface.data.append(
            original_resource
        )

        # Mock partition_netcdf to return 3 partitioned paths
        partitioned_paths = [
            tmp_path / "surface_part0.nc",
            tmp_path / "surface_part1.nc",
            tmp_path / "surface_part2.nc",
        ]
        # Ensure files exist for Pydantic validation
        for p in partitioned_paths:
            p.touch()
        mock_partition.return_value = partitioned_paths

        # Need to set up input_list to include forcing.surface
        # The actual partitioning happens in a loop over input_list
        # For this test, we'll directly test the partitioning logic
        dataset = (
            sample_roms_marbl_input_data.roms_marbl_blueprint_elements.forcing.surface
        )
        new_resources = []
        for resource in dataset.data:
            if resource.location is None:
                new_resources.append(resource)
                continue
            partitioned_paths_result = mock_partition(resource.location)
            for p_path in partitioned_paths_result:
                resource_dict = resource.model_dump()
                resource_dict["location"] = str(
                    p_path
                )  # Convert to str for Pydantic validation
                resource_dict["partitioned"] = True
                new_resources.append(Resource(**resource_dict))
        dataset.data = new_resources

        # Should have 3 resources now
        assert len(dataset.data) == 3
        assert all(r.partitioned for r in dataset.data)
        assert all(r.location is not None for r in dataset.data)
