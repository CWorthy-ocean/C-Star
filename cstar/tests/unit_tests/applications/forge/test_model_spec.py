"""
Tests for the models.py module.

Tests cover:
- SourceSpec instantiation and validation
- InitialConditionsInput instantiation and validation
- Forcing item classes (Surface, Boundary, Tidal, River)
- Validation errors for invalid inputs

(GridInput/ForcingInput/ModelInputs and the dataset-derivation helpers were removed
when ModelSpec was consolidated into a single YAML with no embedded default
forcing -- see test_models_comprehensive.py::TestModelSpec/TestLoadModelsYaml for
the current ModelSpec/ModelCode/ModelTemplates coverage.)
"""

import pytest
from pydantic import ValidationError

from cstar_forge.models import (
    BgcSourceItem,
    BoundaryForcing,
    InitialConditionsInput,
    RiverForcingItem,
    SourceSpec,
    SurfaceForcingItem,
    TidalForcingItem,
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
        assert (
            "extra" in str(exc_info.value).lower()
            or "forbidden" in str(exc_info.value).lower()
        )

    def test_sourcespec_glorys_layout(self):
        """GLORYS may set glorys_layout; default is None (regional when mapped)."""
        assert SourceSpec(name="GLORYS").glorys_layout is None
        assert (
            SourceSpec(name="GLORYS", glorys_layout="regional").glorys_layout
            == "regional"
        )
        assert (
            SourceSpec(name="GLORYS", glorys_layout="global").glorys_layout == "global"
        )

    def test_sourcespec_glorys_layout_only_for_glorys(self):
        """glorys_layout is invalid for non-GLORYS sources."""
        with pytest.raises(ValidationError):
            SourceSpec(name="ERA5", glorys_layout="regional")


class TestInitialConditionsInput:
    """Tests for InitialConditionsInput class."""

    def test_initialconditionsinput_creation_minimal(self):
        """Test creating InitialConditionsInput with minimal fields."""
        source = SourceSpec(name="GLORYS")
        ic = InitialConditionsInput(source=source)
        assert ic.source.name == "GLORYS"
        assert ic.bgc_sources == []  # Default value

    def test_initialconditionsinput_creation_with_bgc(self):
        """Test creating InitialConditionsInput with a single bgc source."""
        source = SourceSpec(name="GLORYS")
        bgc_source = SourceSpec(name="UNIFIED", climatology=True)
        ic = InitialConditionsInput(
            source=source,
            bgc_sources=[BgcSourceItem(source=bgc_source)],
        )
        assert ic.source.name == "GLORYS"
        assert ic.bgc_sources[0].source.name == "UNIFIED"
        assert ic.bgc_sources[0].source.climatology is True

    def test_initialconditionsinput_creation_with_multiple_bgc_sources(self):
        """Multiple bgc_sources, each with its own use_vars down-selection."""
        ic = InitialConditionsInput(
            source=SourceSpec(name="GLORYS"),
            bgc_sources=[
                BgcSourceItem(
                    source=SourceSpec(name="UNIFIED", climatology=True),
                    use_vars=["CHL", "PO4", "NO3", "SiO3", "O2"],
                ),
                BgcSourceItem(
                    source=SourceSpec(name="GLODAP"), use_vars=["ALK", "DIC"]
                ),
                BgcSourceItem(
                    source=SourceSpec(name="constants", constants={"Fe": 3.0e-3}),
                    use_vars=["Fe"],
                ),
            ],
        )
        assert [b.source.name for b in ic.bgc_sources] == [
            "UNIFIED",
            "GLODAP",
            "constants",
        ]
        assert ic.bgc_sources[0].use_vars == ["CHL", "PO4", "NO3", "SiO3", "O2"]
        assert ic.bgc_sources[2].source.constants == {"Fe": 3.0e-3}

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
        item = SurfaceForcingItem(source=source, type="physics", correct_radiation=True)
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
        assert (
            "type" in str(exc_info.value).lower()
            or "pattern" in str(exc_info.value).lower()
        )

    def test_surfaceforcingitem_validation_missing_fields(self):
        """Test that SurfaceForcingItem raises error when required fields are missing."""
        with pytest.raises(ValidationError):
            SurfaceForcingItem()


class TestBoundaryForcing:
    """Tests for BoundaryForcing class -- a structural mirror of
    InitialConditionsInput (see BgcSourceItem's docstring): a required physics
    `source` plus zero or more `bgc_sources`, no `type` discriminator.
    """

    def test_boundaryforcing_creation_physics_only(self):
        """Test creating BoundaryForcing with just a physics source."""
        source = SourceSpec(name="GLORYS")
        item = BoundaryForcing(source=source)
        assert item.source.name == "GLORYS"
        assert item.bgc_sources == []  # Default value

    def test_boundaryforcing_creation_with_bgc(self):
        """Test creating BoundaryForcing with a bgc source."""
        source = SourceSpec(name="GLORYS")
        bgc_source = SourceSpec(name="UNIFIED", climatology=True)
        item = BoundaryForcing(
            source=source,
            bgc_sources=[BgcSourceItem(source=bgc_source)],
        )
        assert item.source.name == "GLORYS"
        assert item.bgc_sources[0].source.name == "UNIFIED"
        assert item.bgc_sources[0].source.climatology is True

    def test_boundaryforcing_creation_with_multiple_bgc_sources(self):
        """Multiple bgc_sources, each optionally overriding bgc_interpolation_method."""
        item = BoundaryForcing(
            source=SourceSpec(name="GLORYS"),
            bgc_sources=[
                BgcSourceItem(
                    source=SourceSpec(name="UNIFIED", climatology=True),
                    use_vars=["CHL", "PO4", "NO3", "SiO3", "O2"],
                ),
                BgcSourceItem(
                    source=SourceSpec(name="GLODAP"),
                    use_vars=["ALK", "DIC"],
                    bgc_interpolation_method="density",
                ),
            ],
        )
        assert [b.source.name for b in item.bgc_sources] == ["UNIFIED", "GLODAP"]
        assert item.bgc_sources[0].bgc_interpolation_method is None
        assert item.bgc_sources[1].bgc_interpolation_method == "density"

    def test_boundaryforcing_validation_missing_source(self):
        """Test that BoundaryForcing raises error when source is missing."""
        with pytest.raises(ValidationError) as exc_info:
            BoundaryForcing()
        assert "source" in str(exc_info.value).lower()


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
