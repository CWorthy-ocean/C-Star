"""The resolver's warnings: what a blueprint would run with that nothing refuses.

``resolver_warnings`` names MARBL without BGC sources on a ucla-roms release that starts
the run anyway with those tracers at zero; ``build_forge_blueprint`` logs it once.
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


def _without_bgc_sources(forcing: dict[str, t.Any]) -> dict[str, t.Any]:
    forcing = copy.deepcopy(forcing)
    forcing["initial_conditions"]["bgc_sources"] = []
    forcing["forcing"]["boundary"]["bgc_sources"] = []
    return forcing


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
    """No shipped blueprint runs MARBL without BGC inputs on a release that allows it."""
    blueprint = ForgeBlueprint.from_yaml(path)
    assert resolver_warnings(blueprint, roms_ref=blueprint.code.roms.commit) == []
