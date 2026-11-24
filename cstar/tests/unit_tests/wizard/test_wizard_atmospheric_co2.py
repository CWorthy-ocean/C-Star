"""The Surface forcing pane's "Atmospheric CO2" block.

MARBL's air CO2 is either Static, the constant ``bgc.xco2air_default`` (284.7 ppm, the
pre-industrial average, in the bundled ModelSpecs), or Time-varying, an ``MBL_co2``
surface item. The block replaces both the BGC Advanced pane's ``xco2air_default`` box
and ``MBL_co2`` as a bgc row source.
"""

from datetime import date
from pathlib import Path
from typing import Any

import ipywidgets as W
import pytest

import cstar.catalog
from cstar.wizard.forcing_editor import (
    _CO2_STATIC,
    _CO2_TIME_VARYING,
    _ForcingEditor,
    _source_opts_for,
)
from cstar.wizard.settings_editor import _FORCING_CARD
from cstar.wizard.wizard import ForgeBlueprintWizard

_BUNDLED = Path(cstar.catalog.__file__).parent / "bundled"
_XCO2 = ("bgc", "xco2air_default")
_BGC_PANE = "Biogeochemistry (BGC / MARBL)"


def _wizard() -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    return wiz


def _editor(wiz: ForgeBlueprintWizard) -> _ForcingEditor:
    assert wiz._forcing_editor is not None
    return wiz._forcing_editor


def _surface(wiz: ForgeBlueprintWizard) -> list[str]:
    assert wiz.config is not None
    return [item.source.name for item in wiz.config.forcing.surface]


def _summary(wiz: ForgeBlueprintWizard) -> list[str]:
    """The Surface forcing pane header's pill texts."""
    return [pill.text for pill in wiz._forcing_summary("surface")]


def _shown(widget: Any) -> bool:
    return widget.layout.display != "none"


def test_a_spec_with_mbl_co2_loads_time_varying_in_its_own_place():
    wiz = _wizard()
    editor = _editor(wiz)
    assert wiz.forcing_dd.value == "glorys-era5-unified"
    assert editor.co2_mode.value == _CO2_TIME_VARYING
    assert [row["name"].value for row in editor._rows["surface"]] == [
        "ERA5",
        "UNIFIED",
        "WOA",
    ]
    assert _surface(wiz) == ["ERA5", "UNIFIED", "MBL_co2", "WOA"]
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True
    assert not wiz._forcing_modified()
    assert _summary(wiz) == ["ERA5", "UNIFIED", "WOA", "CO2 time-varying"]


def test_bgc_rows_no_longer_offer_mbl_co2():
    assert "MBL_co2" not in _source_opts_for("surface", "bgc")
    assert "UNIFIED" in _source_opts_for("surface", "bgc")


def test_a_spec_without_mbl_co2_loads_static_at_the_pre_industrial_default():
    wiz = _wizard()
    wiz.forcing_dd.value = "simple-bgc-demo"
    editor = _editor(wiz)
    assert editor.co2_mode.value == _CO2_STATIC
    assert _shown(editor._co2_static_box) and not _shown(editor._co2_varying_box)

    # The static value is a field row: glossary label, unit and the hint.
    widget = wiz.editor._widgets[_XCO2][0]
    row = wiz.editor.forcing_box.children[0]
    assert row.forge_key == "settings.bgc.xco2air_default"
    assert row.forge_widget is widget
    assert widget.description == ""
    assert "pre-industrial" in row.children[1].children[1].value
    assert widget.value == 284.7
    assert wiz.editor.forcing_box in wiz.co2_static_box.children
    assert wiz.editor._group_fields[_FORCING_CARD] == {"bgc": ["xco2air_default"]}
    assert "xco2air_default" not in wiz.editor._group_fields[_BGC_PANE].get("bgc", [])
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is False
    assert _summary(wiz)[-1] == "CO2 static"


def test_the_static_value_is_an_override_shown_as_a_forcing_change():
    wiz = _wizard()
    wiz.forcing_dd.value = "simple-bgc-demo"
    wiz.editor._widgets[_XCO2][0].value = 392.6
    assert wiz._overrides[_XCO2] == 392.6
    assert wiz.config is not None
    assert wiz.config.model_settings["bgc"]["xco2air_default"] == 392.6
    assert wiz.config.composition.overrides["bgc"] == {"xco2air_default": 392.6}
    assert "Modified" in wiz.card_chips["forcing"].value
    assert "All defaults" in wiz.card_chips["advanced"].value


def test_switching_to_time_varying_adds_mbl_co2_after_the_bgc_rows():
    wiz = _wizard()
    wiz.forcing_dd.value = "simple-bgc-demo"
    editor = _editor(wiz)
    editor.co2_mode.value = _CO2_TIME_VARYING
    assert _shown(editor._co2_varying_box) and not _shown(editor._co2_static_box)
    assert _surface(wiz) == ["ERA5", "UNIFIED", "MBL_co2"]
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True

    editor.co2_mode.value = _CO2_STATIC
    assert _surface(wiz) == ["ERA5", "UNIFIED"]
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is False


def test_the_time_varying_fields_round_trip_in_place():
    mbl = {
        "type": "bgc",
        "source": {"name": "MBL_co2", "path": "/data/mbl.txt"},
        "coarse_grid_mode": "never",
        "regrid_method": "xesmf",
        "options": {"bypass_validation": True},
    }
    era5 = {"type": "physics", "source": {"name": "ERA5"}}
    editor = _ForcingEditor(
        W, {"forcing": {"surface": [mbl, era5]}}, on_change=lambda: None
    )
    row = editor._co2_row
    for key in ("type", "name", "climatology"):
        assert not _shown(row[key])
    for key in ("path", "coarse_grid_mode", "regrid_method", "options"):
        assert _shown(row[key])
    # The custom file sits under the tile's disclosure and counts as an option.
    assert row["path"] in row["_options"].body.children
    assert row["_options"].get_title().startswith("Custom file and roms-tools options")
    # path, coarse grid, regrid method and options differ from their defaults
    assert "6 settings · 4 non-default" in row["_options"].get_title()
    tile = editor._co2_varying_box.children[0]
    assert "forge-frow-static" in tile.children[0]._dom_classes
    assert row["path"] not in tile.children

    surface = editor.gather()["forcing"]["surface"]
    assert [item["source"]["name"] for item in surface] == ["MBL_co2", "ERA5"]
    (gathered,) = (item for item in surface if item["source"]["name"] == "MBL_co2")
    assert gathered["source"] == {"name": "MBL_co2", "path": "/data/mbl.txt"}
    assert gathered["coarse_grid_mode"] == "never"
    assert gathered["regrid_method"] == "xesmf"
    assert gathered["options"] == {"bypass_validation": True}


def test_only_marbl_shows_the_block_and_writes_mbl_co2():
    """Outside MARBL the block hides and no MBL_co2 item reaches the resolver; the
    choice is kept for a switch back.
    """
    wiz = _wizard()
    wiz.forcing_dd.value = "glorys-era5-esper"  # not the mode's default: stays put
    editor = _editor(wiz)
    assert editor.co2_mode.value == _CO2_TIME_VARYING

    wiz.model_card.bgc_dd.value = "none"
    assert wiz._forcing_editor is editor
    assert not _shown(editor.co2_block)
    assert not any("CO2" in text for text in _summary(wiz))
    surface = wiz._gather()["forcing_inputs"]["forcing"]["surface"]
    assert "MBL_co2" not in [item["source"]["name"] for item in surface]
    assert editor.co2_mode.value == _CO2_TIME_VARYING

    wiz.model_card.bgc_dd.value = "marbl"
    assert _shown(editor.co2_block)
    assert "MBL_co2" in _surface(wiz)


def test_a_custom_co2_file_survives_a_save_and_reload(tmp_path: Path):
    """The time-varying CO2 custom file is saved and loads back into its box."""
    wiz = _wizard()
    wiz.load_path.value = str(
        _BUNDLED / "blueprints" / "forge" / "wio-toy-example.yaml"
    )
    wiz._on_load_path(None)
    _editor(wiz)._co2_row["path"].value = "/data/mbl.txt"
    assert wiz.config is not None
    saved = wiz._save_config(tmp_path / "bp.yaml")

    again = _wizard()
    again.load_path.value = str(saved)
    again._on_load_path(None)
    assert _editor(again)._co2_row["path"].value == "/data/mbl.txt"
    assert again.config is not None
    (mbl,) = (i for i in again.config.forcing.surface if i.source.name == "MBL_co2")
    assert str(mbl.source.path) == "/data/mbl.txt"


@pytest.mark.parametrize(
    ("name", "mode"),
    [("wio-toy-example", _CO2_TIME_VARYING), ("wio-toy-simple", _CO2_STATIC)],
)
def test_a_loaded_blueprint_sets_the_mode(name: str, mode: str):
    wiz = _wizard()
    wiz.load_path.value = str(_BUNDLED / "blueprints" / "forge" / f"{name}.yaml")
    wiz._on_load_path(None)
    assert _editor(wiz).co2_mode.value == mode
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is (
        mode == _CO2_TIME_VARYING
    )
