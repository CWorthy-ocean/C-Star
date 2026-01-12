"""
Tests for the models.py module.

Tests cover:
- SourceSpec instantiation and validation
- GridInput instantiation and validation
- InitialConditionsInput instantiation and validation
- Forcing item classes (Surface, Boundary, Tidal, River)
- ForcingInput container class
- ModelInputs top-level class
- Validation errors for invalid inputs
- Integration with YAML structure
"""
import pytest
from pydantic import ValidationError

from cson_forge.models import (
    SourceSpec,
    GridInput,
    InitialConditionsInput,
    SurfaceForcingItem,
    BoundaryForcingItem,
    TidalForcingItem,
    RiverForcingItem,
    ForcingInput,
    ModelInputs,
)


class TestSourceSpec:
    """Tests for SourceSpec class."""
    
    def test_sourcespec_creation_minimal(self):
        """Test creating SourceSpec with minimal required fields."""
        spec = SourceSpec(name="GLORYS")
        assert spec.name == "GLORYS"
        assert spec.climatology is False  # Default value
    
    def test_sourcespec_creation_with_climatology(self):
        """Test creating SourceSpec with climatology specified."""
        spec = SourceSpec(name="UNIFIED", climatology=True)
        assert spec.name == "UNIFIED"
        assert spec.climatology is True
    
    def test_sourcespec_validation_missing_name(self):
        """Test that SourceSpec raises error when name is missing."""
        with pytest.raises(ValidationError) as exc_info:
            SourceSpec()
        assert "name" in str(exc_info.value).lower()
    
    def test_sourcespec_validation_extra_fields(self):
        """Test that SourceSpec rejects extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            SourceSpec(name="GLORYS", extra_field="not allowed")
        assert "extra" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()


class TestGridInput:
    """Tests for GridInput class."""
    
    def test_gridinput_creation(self):
        """Test creating GridInput with required fields."""
        grid = GridInput(topography_source="ETOPO5")
        assert grid.topography_source == "ETOPO5"
    
    def test_gridinput_validation_missing_topography_source(self):
        """Test that GridInput raises error when topography_source is missing."""
        with pytest.raises(ValidationError) as exc_info:
            GridInput()
        assert "topography_source" in str(exc_info.value).lower()
    
    def test_gridinput_validation_extra_fields(self):
        """Test that GridInput rejects extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            GridInput(topography_source="ETOPO5", extra_field="not allowed")
        assert "extra" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()


class TestInitialConditionsInput:
    """Tests for InitialConditionsInput class."""
    
    def test_initialconditionsinput_creation_minimal(self):
        """Test creating InitialConditionsInput with minimal fields."""
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        assert ic.source.name == "GLORYS"
        assert ic.bgc_source is None  # Default value
    
    def test_initialconditionsinput_creation_with_bgc(self):
        """Test creating InitialConditionsInput with bgc_source."""
        source = SourceSpec(name="GLORYS")
        bgc_source = SourceSpec(name="UNIFIED", climatology=True)
        ic = InitialConditionsInput(source=source, bgc_source=bgc_source)
        assert ic.source.name == "GLORYS"
        assert ic.bgc_source.name == "UNIFIED"
        assert ic.bgc_source.climatology is True
    
    def test_initialconditionsinput_validation_missing_source(self):
        """Test that InitialConditionsInput raises error when source is missing."""
        with pytest.raises(ValidationError) as exc_info:
            InitialConditionsInput()
        assert "source" in str(exc_info.value).lower()


class TestSurfaceForcingItem:
    """Tests for SurfaceForcingItem class."""
    
    def test_surfaceforcingitem_creation_minimal(self):
        """Test creating SurfaceForcingItem with minimal fields."""
        source = SourceSpec(name="ERA5")
        item = SurfaceForcingItem(source=source, type="physics")
        assert item.source.name == "ERA5"
        assert item.type == "physics"
        assert item.correct_radiation is False  # Default value
    
    def test_surfaceforcingitem_creation_with_correct_radiation(self):
        """Test creating SurfaceForcingItem with correct_radiation."""
        source = SourceSpec(name="ERA5")
        item = SurfaceForcingItem(
            source=source,
            type="physics",
            correct_radiation=True
        )
        assert item.correct_radiation is True
    
    def test_surfaceforcingitem_creation_bgc_type(self):
        """Test creating SurfaceForcingItem with bgc type."""
        source = SourceSpec(name="UNIFIED", climatology=True)
        item = SurfaceForcingItem(source=source, type="bgc")
        assert item.type == "bgc"
    
    def test_surfaceforcingitem_validation_invalid_type(self):
        """Test that SurfaceForcingItem rejects invalid type."""
        source = SourceSpec(name="ERA5")
        with pytest.raises(ValidationError) as exc_info:
            SurfaceForcingItem(source=source, type="invalid")
        assert "type" in str(exc_info.value).lower() or "pattern" in str(exc_info.value).lower()
    
    def test_surfaceforcingitem_validation_missing_fields(self):
        """Test that SurfaceForcingItem raises error when required fields are missing."""
        with pytest.raises(ValidationError):
            SurfaceForcingItem()


class TestBoundaryForcingItem:
    """Tests for BoundaryForcingItem class."""
    
    def test_boundaryforcingitem_creation_physics(self):
        """Test creating BoundaryForcingItem with physics type."""
        source = SourceSpec(name="GLORYS")
        item = BoundaryForcingItem(source=source, type="physics")
        assert item.source.name == "GLORYS"
        assert item.type == "physics"
    
    def test_boundaryforcingitem_creation_bgc(self):
        """Test creating BoundaryForcingItem with bgc type."""
        source = SourceSpec(name="UNIFIED", climatology=True)
        item = BoundaryForcingItem(source=source, type="bgc")
        assert item.type == "bgc"
    
    def test_boundaryforcingitem_validation_invalid_type(self):
        """Test that BoundaryForcingItem rejects invalid type."""
        source = SourceSpec(name="GLORYS")
        with pytest.raises(ValidationError) as exc_info:
            BoundaryForcingItem(source=source, type="invalid")
        assert "type" in str(exc_info.value).lower() or "pattern" in str(exc_info.value).lower()


class TestTidalForcingItem:
    """Tests for TidalForcingItem class."""
    
    def test_tidalforcingitem_creation_minimal(self):
        """Test creating TidalForcingItem with minimal fields."""
        source = SourceSpec(name="TPXO")
        item = TidalForcingItem(source=source)
        assert item.source.name == "TPXO"
        assert item.ntides is None  # Default value
    
    def test_tidalforcingitem_creation_with_ntides(self):
        """Test creating TidalForcingItem with ntides specified."""
        source = SourceSpec(name="TPXO")
        item = TidalForcingItem(source=source, ntides=15)
        assert item.ntides == 15
    
    def test_tidalforcingitem_validation_missing_source(self):
        """Test that TidalForcingItem raises error when source is missing."""
        with pytest.raises(ValidationError) as exc_info:
            TidalForcingItem(ntides=15)
        assert "source" in str(exc_info.value).lower()


class TestRiverForcingItem:
    """Tests for RiverForcingItem class."""
    
    def test_riverforcingitem_creation_minimal(self):
        """Test creating RiverForcingItem with minimal fields."""
        source = SourceSpec(name="DAI", climatology=False)
        item = RiverForcingItem(source=source)
        assert item.source.name == "DAI"
        assert item.include_bgc is False  # Default value
    
    def test_riverforcingitem_creation_with_include_bgc(self):
        """Test creating RiverForcingItem with include_bgc."""
        source = SourceSpec(name="DAI", climatology=False)
        item = RiverForcingItem(source=source, include_bgc=True)
        assert item.include_bgc is True
    
    def test_riverforcingitem_validation_missing_source(self):
        """Test that RiverForcingItem raises error when source is missing."""
        with pytest.raises(ValidationError) as exc_info:
            RiverForcingItem(include_bgc=True)
        assert "source" in str(exc_info.value).lower()


class TestForcingInput:
    """Tests for ForcingInput class."""
    
    def test_forcinginput_creation_minimal(self):
        """Test creating ForcingInput with minimal required fields."""
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        assert len(forcing.surface) == 1
        assert len(forcing.boundary) == 1
        assert forcing.tidal is None
        assert forcing.river is None
    
    def test_forcinginput_creation_complete(self):
        """Test creating ForcingInput with all fields."""
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        tidal_item = TidalForcingItem(
            source=SourceSpec(name="TPXO"),
            ntides=15
        )
        river_item = RiverForcingItem(
            source=SourceSpec(name="DAI", climatology=False),
            include_bgc=True
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item],
            tidal=[tidal_item],
            river=[river_item]
        )
        assert len(forcing.surface) == 1
        assert len(forcing.boundary) == 1
        assert len(forcing.tidal) == 1
        assert len(forcing.river) == 1
    
    def test_forcinginput_creation_multiple_items(self):
        """Test creating ForcingInput with multiple items in each category."""
        surface_items = [
            SurfaceForcingItem(
                source=SourceSpec(name="ERA5"),
                type="physics",
                correct_radiation=True
            ),
            SurfaceForcingItem(
                source=SourceSpec(name="UNIFIED", climatology=True),
                type="bgc"
            )
        ]
        boundary_items = [
            BoundaryForcingItem(
                source=SourceSpec(name="GLORYS"),
                type="physics"
            ),
            BoundaryForcingItem(
                source=SourceSpec(name="UNIFIED", climatology=True),
                type="bgc"
            )
        ]
        forcing = ForcingInput(
            surface=surface_items,
            boundary=boundary_items
        )
        assert len(forcing.surface) == 2
        assert len(forcing.boundary) == 2
    
    def test_forcinginput_validation_missing_surface(self):
        """Test that ForcingInput raises error when surface is missing."""
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        with pytest.raises(ValidationError) as exc_info:
            ForcingInput(boundary=[boundary_item])
        assert "surface" in str(exc_info.value).lower()
    
    def test_forcinginput_validation_missing_boundary(self):
        """Test that ForcingInput raises error when boundary is missing."""
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        with pytest.raises(ValidationError) as exc_info:
            ForcingInput(surface=[surface_item])
        assert "boundary" in str(exc_info.value).lower()
    
    def test_forcinginput_validation_empty_lists(self):
        """Test that ForcingInput accepts empty lists for required fields."""
        forcing = ForcingInput(surface=[], boundary=[])
        assert len(forcing.surface) == 0
        assert len(forcing.boundary) == 0


class TestModelInputs:
    """Tests for ModelInputs top-level class."""
    
    def test_modelinputs_creation_minimal(self):
        """Test creating ModelInputs with minimal required fields."""
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        
        inputs = ModelInputs(
            grid=grid,
            initial_conditions=ic,
            forcing=forcing
        )
        assert inputs.grid.topography_source == "ETOPO5"
        assert inputs.initial_conditions.source.name == "GLORYS"
        assert len(inputs.forcing.surface) == 1
    
    def test_modelinputs_creation_complete(self):
        """Test creating ModelInputs with all fields populated."""
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        bgc_source = SourceSpec(name="UNIFIED", climatology=True)
        ic = InitialConditionsInput(source=source, bgc_source=bgc_source)
        
        surface_items = [
            SurfaceForcingItem(
                source=SourceSpec(name="ERA5"),
                type="physics",
                correct_radiation=True
            ),
            SurfaceForcingItem(
                source=SourceSpec(name="UNIFIED", climatology=True),
                type="bgc"
            )
        ]
        boundary_items = [
            BoundaryForcingItem(
                source=SourceSpec(name="GLORYS"),
                type="physics"
            ),
            BoundaryForcingItem(
                source=SourceSpec(name="UNIFIED", climatology=True),
                type="bgc"
            )
        ]
        tidal_items = [
            TidalForcingItem(
                source=SourceSpec(name="TPXO"),
                ntides=15
            )
        ]
        river_items = [
            RiverForcingItem(
                source=SourceSpec(name="DAI", climatology=False),
                include_bgc=True
            )
        ]
        forcing = ForcingInput(
            surface=surface_items,
            boundary=boundary_items,
            tidal=tidal_items,
            river=river_items
        )
        
        inputs = ModelInputs(
            grid=grid,
            initial_conditions=ic,
            forcing=forcing
        )
        assert inputs.grid.topography_source == "ETOPO5"
        assert inputs.initial_conditions.bgc_source is not None
        assert len(inputs.forcing.surface) == 2
        assert len(inputs.forcing.boundary) == 2
        assert len(inputs.forcing.tidal) == 1
        assert len(inputs.forcing.river) == 1
    
    def test_modelinputs_validation_missing_grid(self):
        """Test that ModelInputs raises error when grid is missing."""
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        with pytest.raises(ValidationError) as exc_info:
            ModelInputs(initial_conditions=ic, forcing=forcing)
        assert "grid" in str(exc_info.value).lower()
    
    def test_modelinputs_validation_missing_initial_conditions(self):
        """Test that ModelInputs raises error when initial_conditions is missing."""
        grid = GridInput(topography_source="ETOPO5")
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        with pytest.raises(ValidationError) as exc_info:
            ModelInputs(grid=grid, forcing=forcing)
        assert "initial_conditions" in str(exc_info.value).lower()
    
    def test_modelinputs_validation_missing_forcing(self):
        """Test that ModelInputs raises error when forcing is missing."""
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        with pytest.raises(ValidationError) as exc_info:
            ModelInputs(grid=grid, initial_conditions=ic)
        assert "forcing" in str(exc_info.value).lower()
    
    def test_modelinputs_validation_extra_fields(self):
        """Test that ModelInputs rejects extra fields."""
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        with pytest.raises(ValidationError) as exc_info:
            ModelInputs(
                grid=grid,
                initial_conditions=ic,
                forcing=forcing,
                extra_field="not allowed"
            )
        assert "extra" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()


class TestModelInputsFromDict:
    """Tests for creating ModelInputs from dictionary (YAML-like structure)."""
    
    def test_modelinputs_from_dict_minimal(self):
        """Test creating ModelInputs from a minimal dictionary."""
        data = {
            "grid": {"topography_source": "ETOPO5"},
            "initial_conditions": {
                "source": {"name": "GLORYS"}
            },
            "forcing": {
                "surface": [
                    {"source": {"name": "ERA5"}, "type": "physics"}
                ],
                "boundary": [
                    {"source": {"name": "GLORYS"}, "type": "physics"}
                ]
            }
        }
        inputs = ModelInputs(**data)
        assert inputs.grid.topography_source == "ETOPO5"
        assert inputs.initial_conditions.source.name == "GLORYS"
        assert len(inputs.forcing.surface) == 1
    
    def test_modelinputs_from_dict_complete(self):
        """Test creating ModelInputs from a complete dictionary matching models.yml."""
        data = {
            "grid": {"topography_source": "ETOPO5"},
            "initial_conditions": {
                "source": {"name": "GLORYS"},
                "bgc_source": {"name": "UNIFIED", "climatology": True}
            },
            "forcing": {
                "surface": [
                    {
                        "source": {"name": "ERA5"},
                        "type": "physics",
                        "correct_radiation": True
                    },
                    {
                        "source": {"name": "UNIFIED", "climatology": True},
                        "type": "bgc"
                    }
                ],
                "boundary": [
                    {"source": {"name": "GLORYS"}, "type": "physics"},
                    {
                        "source": {"name": "UNIFIED", "climatology": True},
                        "type": "bgc"
                    }
                ],
                "tidal": [
                    {"source": {"name": "TPXO"}, "ntides": 15}
                ],
                "river": [
                    {
                        "source": {"name": "DAI", "climatology": False},
                        "include_bgc": True
                    }
                ]
            }
        }
        inputs = ModelInputs(**data)
        assert inputs.grid.topography_source == "ETOPO5"
        assert inputs.initial_conditions.bgc_source.name == "UNIFIED"
        assert len(inputs.forcing.surface) == 2
        assert inputs.forcing.surface[0].correct_radiation is True
        assert inputs.forcing.surface[1].type == "bgc"
        assert len(inputs.forcing.boundary) == 2
        assert len(inputs.forcing.tidal) == 1
        assert inputs.forcing.tidal[0].ntides == 15
        assert len(inputs.forcing.river) == 1
        assert inputs.forcing.river[0].include_bgc is True


class TestModelInputsSerialization:
    """Tests for ModelInputs serialization methods."""
    
    def test_modelinputs_model_dump(self):
        """Test that ModelInputs can be serialized using model_dump()."""
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        inputs = ModelInputs(
            grid=grid,
            initial_conditions=ic,
            forcing=forcing
        )
        
        # Test model_dump()
        dumped = inputs.model_dump()
        assert isinstance(dumped, dict)
        assert dumped["grid"]["topography_source"] == "ETOPO5"
        assert dumped["initial_conditions"]["source"]["name"] == "GLORYS"
        assert len(dumped["forcing"]["surface"]) == 1
    
    def test_modelinputs_model_dump_json(self):
        """Test that ModelInputs can be serialized to JSON."""
        import json
        grid = GridInput(topography_source="ETOPO5")
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        surface_item = SurfaceForcingItem(
            source=SourceSpec(name="ERA5"),
            type="physics"
        )
        boundary_item = BoundaryForcingItem(
            source=SourceSpec(name="GLORYS"),
            type="physics"
        )
        forcing = ForcingInput(
            surface=[surface_item],
            boundary=[boundary_item]
        )
        inputs = ModelInputs(
            grid=grid,
            initial_conditions=ic,
            forcing=forcing
        )
        
        # Test model_dump_json()
        json_str = inputs.model_dump_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["grid"]["topography_source"] == "ETOPO5"

