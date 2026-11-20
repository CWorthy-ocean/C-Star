"""The resolver's warnings: what a blueprint would run with that nothing refuses.

``resolver_warnings`` names two of these: MARBL forced with a constant atmospheric CO2
nobody chose (the ModelSpec's pre-industrial ``bgc.xco2air_default``, with no
``MBL_co2`` item), and MARBL without BGC sources on a ucla-roms release that starts the
run anyway with those tracers at zero. ``build_forge_blueprint`` logs each once.
"""

import copy
import logging
import typing as t
from datetime import datetime
from pathlib import Path

import pytest
import yaml

import cstar
import cstar.catalog
from cstar.applications.forge.blueprint import (
    BgcSourceItem,
    BoundaryForcing,
    Forcing,
    ForgeBlueprint,
    InitialConditions,
    SourceSpec,
)
from cstar.applications.forge.namelist_model import zero_bgc_inputs_warning
from cstar.applications.forge.resolve import build_forge_blueprint, resolver_warnings
from cstar.catalog.domain_catalog import DomainCatalog

_BUNDLED = Path(cstar.catalog.__file__).parent / "bundled"
_CATALOG = DomainCatalog(catalog_root=_BUNDLED)
_LOGGER = "cstar.applications.forge.resolve"
_GRID_KWARGS = {
    "nx": 6,
    "ny": 2,
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
_OPEN = {"south": False, "east": True, "north": True, "west": False}


def _resolve(
    model: str = "roms-marbl-0.15-default",
    forcing: dict[str, t.Any] | None = None,
    **kw: t.Any,
) -> ForgeBlueprint:
    """Resolve a tiny domain against the bundled catalog only."""
    return build_forge_blueprint(
        model_path=_CATALOG.model_path(model),
        grid_name="tiny",
        grid_kwargs=_GRID_KWARGS,
        open_boundaries=_OPEN,
        partitioning={"n_procs_x": 1, "n_procs_y": 1},
        start_date=datetime(2012, 1, 1),
        end_date=datetime(2012, 1, 2),
        dt=7200,
        forcing_inputs=(
            forcing if forcing is not None else _CATALOG.forcing_data("simple-bgc-demo")
        ),
        output_settings=_CATALOG.output_data("daily-restarts"),
        **kw,
    )


def _logged(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The messages the resolver logged at WARNING."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == _LOGGER and r.levelno == logging.WARNING
    ]


def _without_mbl_co2(forcing: dict[str, t.Any]) -> dict[str, t.Any]:
    forcing = copy.deepcopy(forcing)
    surface = forcing["forcing"]["surface"]
    forcing["forcing"]["surface"] = [
        item for item in surface if item["source"]["name"] != "MBL_co2"
    ]
    assert len(forcing["forcing"]["surface"]) == len(surface) - 1
    return forcing


def _without_bgc_sources(forcing: dict[str, t.Any]) -> dict[str, t.Any]:
    forcing = copy.deepcopy(forcing)
    forcing["initial_conditions"]["bgc_sources"] = []
    forcing["forcing"]["boundary"]["bgc_sources"] = []
    return forcing


class TestPreindustrialCo2Warning:
    """MARBL without an ``MBL_co2`` item and with the ModelSpec's ``xco2air_default``."""

    def test_the_demo_forcing_carries_co2_and_draws_no_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = _resolve()
        assert cfg.model_settings["cppdefs"]["co2_tvarying"] is True
        assert _logged(caplog) == []

    def test_resolving_wio_toy_simple_reproduces_it_without_a_warning(self, caplog):
        """The bundled blueprint resolves from its own composition to the same forcing,
        with CO2 forced from MBL_co2, and draws no warning.
        """
        stored = ForgeBlueprint.from_yaml(
            _BUNDLED / "blueprints" / "forge" / "wio-toy-simple.yaml"
        )
        comp = stored.composition
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = build_forge_blueprint(
                model_path=_CATALOG.model_path(comp.model.name),
                grid_name=stored.domain.grid_name,
                grid_kwargs=stored.domain.grid_kwargs,
                open_boundaries=stored.domain.open_boundaries.model_dump(),
                partitioning={
                    "n_procs_x": stored.domain.partitioning.n_procs_x,
                    "n_procs_y": stored.domain.partitioning.n_procs_y,
                },
                start_date=stored.run.start_date,
                end_date=stored.run.end_date,
                dt=stored.domain.dt,
                forcing_inputs=_CATALOG.forcing_data(comp.forcing.name),
                output_settings=_CATALOG.output_data(comp.output.name),
            )
        assert cfg.model_settings["cppdefs"]["co2_tvarying"] is True
        assert cfg.forcing == stored.forcing
        assert cfg.datasets == stored.datasets
        assert _logged(caplog) == []

    def test_marbl_without_mbl_co2_warns_once_naming_both_fixes(self, caplog):
        forcing = _without_mbl_co2(_CATALOG.forcing_data("simple-bgc-demo"))
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = _resolve(forcing=forcing)
        assert cfg.model_settings["cppdefs"]["co2_tvarying"] is False
        assert cfg.model_settings["bgc"]["xco2air_default"] == 284.7
        (message,) = _logged(caplog)
        assert "bgc.xco2air_default = 284.7 ppm" in message
        assert "no MBL_co2 surface item" in message
        assert "source MBL_co2" in message
        assert "set bgc.xco2air_default" in message

    @pytest.mark.parametrize("value", [284.7, 392.6])
    def test_a_value_the_user_set_draws_no_warning(self, caplog, value):
        """Any value the user sets is a choice, the pre-industrial one included."""
        forcing = _without_mbl_co2(_CATALOG.forcing_data("simple-bgc-demo"))
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = _resolve(
                forcing=forcing,
                run_time_overrides={"bgc": {"xco2air_default": value}},
            )
        assert cfg.model_settings["bgc"]["xco2air_default"] == value
        assert _logged(caplog) == []

    def test_an_override_of_another_bgc_setting_still_warns(self, caplog):
        forcing = _without_mbl_co2(_CATALOG.forcing_data("simple-bgc-demo"))
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            _resolve(forcing=forcing, run_time_overrides={"bgc": {"nbgc_flx": 2}})
        assert len(_logged(caplog)) == 1

    def test_no_warning_without_marbl(self, caplog):
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = _resolve(
                forcing=_CATALOG.forcing_data("glorys-era5-physics"), bgc_mode="none"
            )
        assert cfg.model_settings["cppdefs"]["marbl"] is False
        assert _logged(caplog) == []


class TestZeroBgcInputsWarning:
    """MARBL without BGC sources on a ucla-roms release below 0.14.0, which runs it."""

    def test_the_resolver_warns_once_before_0_14(self, caplog):
        forcing = _without_bgc_sources(_CATALOG.forcing_data("simple-bgc-demo"))
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            cfg = _resolve("roms-marbl-0.13-default", forcing)
        assert cfg.model_settings["cppdefs"]["marbl"] is True
        (message,) = _logged(caplog)
        assert message.startswith(
            "forcing.boundary and initial_conditions have no BGC sources"
        )
        assert "(before 0.14.0)" in message
        assert "the open boundaries feed them zero and they start from zero" in message
        assert "glorys-era5-unified" in message and 'bgc_mode "none"' in message

    def test_the_resolver_still_refuses_from_0_14(self):
        forcing = _without_bgc_sources(_CATALOG.forcing_data("simple-bgc-demo"))
        with pytest.raises(ValueError, match="ucla-roms >= 0.14.0 stops"):
            _resolve("roms-marbl-0.15-default", forcing)

    def _forcing(self, *, boundary_bgc: bool, ic_bgc: bool) -> Forcing:
        glorys = SourceSpec(name="GLORYS", glorys_layout="regional")
        bgc = [BgcSourceItem(source=SourceSpec(name="UNIFIED"))]
        return Forcing(
            boundary=BoundaryForcing(
                source=glorys, bgc_sources=bgc if boundary_bgc else []
            ),
            initial_conditions=InitialConditions(
                source=glorys, bgc_sources=bgc if ic_bgc else []
            ),
        )

    def _warning(self, forcing: Forcing, **over: t.Any) -> str | None:
        kw: dict[str, t.Any] = {
            "open_boundaries": _OPEN,
            "is_child": False,
            "marbl": True,
            "roms_ref": "0.13.1",
        }
        return zero_bgc_inputs_warning(forcing, **(kw | over))

    def test_names_only_the_initial_conditions(self):
        message = self._warning(self._forcing(boundary_bgc=True, ic_bgc=False))
        assert message is not None
        assert message.startswith("initial_conditions has no BGC sources")
        assert "they start from zero" in message
        assert "open boundaries" not in message

    def test_a_child_boundary_comes_from_its_parent(self):
        forcing = self._forcing(boundary_bgc=False, ic_bgc=True)
        assert self._warning(forcing) is not None
        assert self._warning(forcing, is_child=True) is None

    @pytest.mark.parametrize("roms_ref", ["0.14.0", "0.15.0", "main", None, "a" * 40])
    def test_only_a_release_below_the_minimum(self, roms_ref):
        """From 0.14.0, and on a branch, the check refuses instead; a commit hash's
        release is unknown here.
        """
        forcing = self._forcing(boundary_bgc=False, ic_bgc=False)
        assert self._warning(forcing, roms_ref=roms_ref) is None

    def test_nothing_without_marbl_or_with_sources(self):
        assert (
            self._warning(self._forcing(boundary_bgc=False, ic_bgc=False), marbl=False)
            is None
        )
        assert self._warning(self._forcing(boundary_bgc=True, ic_bgc=True)) is None


def _shipped_forge_blueprints() -> list[Path]:
    """The bundled forge blueprints and every forge blueprint in the docs."""
    docs = Path(cstar.__file__).parents[1] / "docs"
    paths = sorted((_BUNDLED / "blueprints" / "forge").glob("*.yaml"))
    for path in sorted(docs.rglob("*.yaml")) if docs.is_dir() else []:
        try:
            head = next(yaml.safe_load_all(path.read_text()), None)
        except yaml.YAMLError:
            continue
        if isinstance(head, dict) and head.get("application") == "forge":
            paths.append(path)
    return paths


@pytest.mark.parametrize("path", _shipped_forge_blueprints(), ids=lambda p: p.name)
def test_shipped_blueprints_draw_no_resolver_warning(path: Path) -> None:
    """No shipped blueprint runs MARBL at the ModelSpec's pre-industrial CO2 or without
    BGC inputs; a value a file sets on purpose is recorded in ``composition.overrides``.
    """
    blueprint = ForgeBlueprint.from_yaml(path)
    user_set = {
        (section, field)
        for section, fields in blueprint.composition.overrides.items()
        for field in (fields if isinstance(fields, dict) else (None,))
    }
    assert (
        resolver_warnings(
            blueprint, user_set=user_set, roms_ref=blueprint.code.roms.commit
        )
        == []
    )
