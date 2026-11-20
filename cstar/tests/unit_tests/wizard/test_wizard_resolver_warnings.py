"""The wizard shows the resolver's warnings in its validation banner.

The wizard applies the accordion overrides after resolving, so it checks the effective
settings itself: a pre-industrial CO2 warning shows when the forcing loses its
``MBL_co2`` item, and goes away once the user sets ``bgc.xco2air_default``.
"""

from datetime import date
from typing import Any

from cstar.wizard.wizard import ForgeBlueprintWizard


def _wizard() -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    return wiz


def _surface_row(wiz: ForgeBlueprintWizard, name: str) -> dict[str, Any]:
    editor = wiz._forcing_editor
    assert editor is not None
    (row,) = [w for w in editor._rows["surface"] if w["name"].value == name]
    return row


def test_the_default_forcing_shows_no_warning():
    wiz = _wizard()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is True
    assert "forge-banner warn" not in wiz.validation.value


def test_dropping_mbl_co2_warns_until_the_user_sets_xco2air_default():
    wiz = _wizard()
    _surface_row(wiz, "MBL_co2")["_remove_btn"].click()
    assert wiz.config is not None
    assert wiz.config.model_settings["cppdefs"]["co2_tvarying"] is False
    assert "forge-banner warn" in wiz.validation.value
    assert "no MBL_co2 surface item" in wiz.validation.value
    assert "Blueprint is valid." in wiz.validation.value

    wiz.editor._widgets[("bgc", "xco2air_default")][0].value = 392.6
    assert wiz._overrides[("bgc", "xco2air_default")] == 392.6
    assert wiz.config.model_settings["bgc"]["xco2air_default"] == 392.6
    assert "forge-banner warn" not in wiz.validation.value
