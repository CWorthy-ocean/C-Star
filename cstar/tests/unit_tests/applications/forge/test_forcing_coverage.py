"""``check_forcing_coverage``: MARBL compiled without MARBL code, open sides with no
boundary block, and MARBL source sets with no CHL or Fe provider (whose plankton,
iron and ligand tracers roms-tools would set to zero).
"""

import pytest

from cstar.applications.forge.blueprint import (
    BgcSourceItem,
    BoundaryForcing,
    Forcing,
    InitialConditions,
    SourceSpec,
)
from cstar.applications.forge.namelist_model import check_forcing_coverage
from cstar.catalog.domain_catalog import default_catalog

_GLORYS = SourceSpec(name="GLORYS", glorys_layout="regional")
_OPEN = {"south": False, "east": True, "north": False, "west": False}
_CLOSED = dict.fromkeys(("south", "east", "north", "west"), False)
_ESPER_VARS = ["ALK", "DIC", "NO3", "PO4", "SiO3", "O2"]
# what roms-tools derives from CHL (BGCMarbl._CHL_FACTORS)
_CHL_TRACERS = ("zooC", "spChl", "spC", "diatChl", "diazFe")


def _item(name: str, use_vars=None, **source) -> BgcSourceItem:
    return BgcSourceItem(source=SourceSpec(name=name, **source), use_vars=use_vars)


def _esper(use_vars=None) -> BgcSourceItem:
    return _item("ESPER", use_vars)


def _constants(values: dict[str, float], use_vars=None) -> BgcSourceItem:
    return _item("constants", use_vars, constants=values)


def _forcing(ic=None, boundary=None, *, has_boundary=True) -> Forcing:
    return Forcing(
        initial_conditions=(
            InitialConditions(source=_GLORYS, bgc_sources=ic)
            if ic is not None
            else None
        ),
        boundary=(
            BoundaryForcing(source=_GLORYS, bgc_sources=boundary or [])
            if has_boundary
            else None
        ),
    )


def _check(forcing, *, marbl=True, marbl_code=True, **over):
    kw = {"open_boundaries": _OPEN, "is_child": False, **over}
    check_forcing_coverage(
        forcing, cppdefs={"marbl": marbl}, marbl_code=marbl_code, **kw
    )


class TestMarblSeedFields:
    """Under MARBL, a source set must provide CHL (or the plankton tracers) and Fe."""

    def test_esper_only_initial_conditions_is_refused(self):
        with pytest.raises(ValueError) as exc:
            _check(_forcing(ic=[_esper()]))
        msg = str(exc.value)
        assert "initial_conditions.bgc_sources (ESPER)" in msg
        assert "no source provides CHL or Fe" in msg
        for tracer in ("Fe", "Lig", *_CHL_TRACERS):
            assert tracer in msg
        assert "UNIFIED" in msg and "constants" in msg  # names the fix

    def test_esper_only_boundary_is_refused(self):
        ic = [_item("UNIFIED")]
        with pytest.raises(ValueError, match=r"forcing\.boundary\.bgc_sources"):
            _check(_forcing(ic=ic, boundary=[_esper()]))

    @pytest.mark.parametrize("name", ["GLODAP", "WOA_BGC"])
    def test_a_dataset_without_chl_and_fe_is_refused(self, name):
        with pytest.raises(ValueError, match="no source provides CHL or Fe"):
            _check(_forcing(ic=[_item(name)]))

    def test_a_missing_fe_alone_names_fe_iron_and_ligand_only(self):
        ic = [_esper(_ESPER_VARS), _constants({"CHL": 0.3}, ["CHL"])]
        with pytest.raises(ValueError, match="no source provides Fe, so") as exc:
            _check(_forcing(ic=ic))
        assert "sets Fe, Lig to zero" in str(exc.value)

    def test_a_missing_chl_alone_names_the_plankton_only(self):
        with pytest.raises(ValueError, match="no source provides CHL, so") as exc:
            _check(_forcing(ic=[_constants({"Fe": 6e-4, "DIC": 2100.0})]))
        assert "Lig" not in str(exc.value)

    @pytest.mark.parametrize("name", ["UNIFIED", "CESM_REGRIDDED"])
    def test_a_complete_dataset_passes(self, name):
        # UNIFIED supplies CHL and Fe; CESM supplies the plankton tracers and Lig
        # directly, with no CHL.
        _check(_forcing(ic=[_item(name)], boundary=[_item(name)]))

    def test_esper_beside_unified_chl_and_fe_passes(self):
        ic = [_esper(_ESPER_VARS), _item("UNIFIED", ["CHL", "Fe", "NH4"])]
        _check(_forcing(ic=ic, boundary=ic))

    def test_a_constants_source_naming_chl_and_fe_passes(self):
        # roms-tools reads a constants source's CHL like any other source's:
        # BGCMarbl.known_vars() includes CHL, so the plankton tracers derive from it.
        _check(_forcing(ic=[_constants({"CHL": 0.3, "Fe": 6e-4, "DIC": 2100.0})]))

    @pytest.mark.parametrize("bundled", ["glorys-era5-esper", "glorys-era5-glodap-woa"])
    def test_the_bundled_multi_source_specs_pass(self, bundled):
        data = default_catalog.forcing_data(bundled)
        forcing = Forcing(
            initial_conditions=data["initial_conditions"],
            boundary=data["forcing"]["boundary"],
        )
        _check(forcing)

    def test_marbl_off_is_not_checked(self):
        _check(_forcing(ic=[_esper()], boundary=[_esper()]), marbl=False)

    def test_an_empty_source_list_is_left_to_check_marbl_bgc_inputs(self):
        _check(_forcing(ic=[], boundary=[]))

    def test_a_child_or_closed_boundary_is_not_checked(self):
        ic = [_item("UNIFIED")]
        _check(_forcing(ic=ic, boundary=[_esper()]), is_child=True)
        _check(_forcing(ic=ic, boundary=[_esper()]), open_boundaries=_CLOSED)

    def test_a_child_without_its_own_initial_conditions_passes(self):
        _check(_forcing(has_boundary=False), is_child=True)


class TestMarblCode:
    """``cppdefs.marbl`` on needs ``code.marbl`` (``bgc_mode: marbl``)."""

    def test_marbl_cppdef_without_marbl_code_is_refused(self):
        with pytest.raises(ValueError, match=r'bgc_mode "marbl" instead of overriding'):
            _check(_forcing(ic=[_item("UNIFIED")]), marbl_code=False)

    def test_marbl_code_or_unknown_passes(self):
        _check(_forcing(ic=[_item("UNIFIED")]), marbl_code=True)
        _check(_forcing(ic=[_item("UNIFIED")]), marbl_code=None)

    def test_marbl_off_without_code_passes(self):
        _check(_forcing(ic=[]), marbl=False, marbl_code=False)

    def test_checked_without_forcing(self):
        with pytest.raises(ValueError, match=r"cppdefs\.marbl is on"):
            check_forcing_coverage(
                None,
                cppdefs={"marbl": True},
                marbl_code=False,
                open_boundaries=_OPEN,
                is_child=False,
            )


class TestBoundaryBlock:
    """Open sides on a domain that is not a child need ``forcing.boundary``."""

    def test_open_sides_without_a_boundary_block_are_refused(self):
        with pytest.raises(ValueError) as exc:
            _check(_forcing(ic=[], has_boundary=False), marbl=False)
        msg = str(exc.value)
        assert "open sides (east)" in msg and "no boundary block" in msg
        assert "close those sides" in msg

    def test_a_child_takes_its_boundary_from_the_parent(self):
        _check(_forcing(has_boundary=False), marbl=False, is_child=True)

    def test_closed_sides_need_no_boundary_block(self):
        _check(_forcing(ic=[], has_boundary=False), open_boundaries=_CLOSED)

    def test_every_problem_is_reported_together(self):
        with pytest.raises(ValueError) as exc:
            _check(_forcing(ic=[_esper()], has_boundary=False), marbl_code=False)
        msg = str(exc.value)
        assert "cppdefs.marbl is on" in msg
        assert "no boundary block" in msg
        assert "initial_conditions.bgc_sources (ESPER)" in msg
