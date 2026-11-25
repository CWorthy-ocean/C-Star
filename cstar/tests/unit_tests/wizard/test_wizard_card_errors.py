"""An invalid blueprint is reported on the wizard card that owns the problem.

`_blueprint_problems` maps a failed build's exception to the card the user fixes it
in, in the wizard's own words; `_update_status` shows it there (a problem chip and
a banner) and leaves every other card's state alone; the BGC panes warn about
sources that do not partition the tracers before Forge refuses them.
"""

from datetime import date

import ipywidgets as W
import pytest
from pydantic import ValidationError
from pydantic_core import ValidationError as CoreValidationError

from cstar.applications.forge import blueprint as forge_blueprint
from cstar.applications.forge.blueprint import (
    BgcSourceItem,
    BoundaryForcing,
    Domain,
    Forcing,
    InitialConditions,
    RunWindow,
    _require_partitioned_bgc_use_vars,
)
from cstar.wizard.forcing_editor import _BOUNDARY_NONE, _ForcingEditor
from cstar.wizard.wizard import (
    _ERROR_CARDS,
    _ERROR_MODEL_PATHS,
    _STICKY_STEPS,
    ForgeBlueprintWizard,
    _blueprint_problems,
    _wizard_words,
)


def _raised(validate, data) -> ValidationError:
    with pytest.raises(ValidationError) as info:
        validate(data)
    return info.value


def _bgc_sources(*use_vars: str) -> list[dict]:
    """UNIFIED BGC sources, one per comma-separated ``use_vars`` text."""
    return [
        {
            "source": {"name": "UNIFIED"},
            **({"use_vars": text.split(",")} if text else {}),
        }
        for text in use_vars
    ]


def _wizard() -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    assert wiz.config is not None, wiz.review_card.derived.value
    return wiz


def _shown(widget) -> bool:
    return widget.layout.display != "none"


# --- mapping an error to its card -------------------------------------------------


def test_a_model_level_bgc_sources_error_goes_to_forcing_in_wizard_words():
    """The reported case: InitialConditions' own validator raises with no ``loc``,
    and the message names Forge's path and field, not the editor's row and box.
    """
    exc = _raised(
        InitialConditions.model_validate,
        {"source": {"name": "GLORYS"}, "bgc_sources": _bgc_sources("", "")},
    )
    assert exc.title == "InitialConditions"
    assert exc.errors()[0]["loc"] == ()

    (problem,) = _blueprint_problems(exc)
    assert problem.card == "forcing"
    assert problem.text.startswith(
        "BGC initial conditions, row 1 (UNIFIED) has no Variables to read."
    )
    assert "each one its Variables to read" in problem.text
    assert "bgc_sources" not in problem.text and "use_vars" not in problem.text


def test_overlapping_boundary_bgc_sources_name_both_rows():
    exc = _raised(
        BoundaryForcing.model_validate,
        {"source": {"name": "GLORYS"}, "bgc_sources": _bgc_sources("DIC", "DIC,ALK")},
    )
    (problem,) = _blueprint_problems(exc)
    assert problem.card == "forcing"
    assert problem.text == (
        "BGC boundary forcing, row 2 and row 1 both read DIC. "
        "List each variable under one source only."
    )


def test_a_field_error_names_the_forcing_row_and_box():
    exc = _raised(
        Forcing.model_validate,
        {
            "surface": [
                {
                    "type": "physics",
                    "source": {"name": "ERA5"},
                    "coarse_grid_mode": "bogus",
                }
            ]
        },
    )
    (problem,) = _blueprint_problems(exc)
    assert problem.card == "forcing"
    assert problem.text.startswith("Surface forcing, row 1, Coarse grid: ")


def test_a_section_built_on_its_own_is_placed_by_its_model_name():
    """Forge builds Domain before the blueprint, so ``loc`` is relative to Domain;
    its partitioning (the Run card's processor layout) is not a grid problem.
    """
    exc = _raised(
        Domain.model_validate, {"partitioning": {"n_procs_x": "x", "n_procs_y": 2}}
    )
    cards = {p.text.split(":")[0]: p.card for p in _blueprint_problems(exc)}
    assert cards["domain.partitioning.n_procs_x"] == "run"
    assert cards["domain.grid_name"] == "grid"


def test_run_window_errors_go_to_run():
    exc = _raised(
        RunWindow.model_validate, {"start_date": "x", "end_date": "2012-01-02"}
    )
    assert [p.card for p in _blueprint_problems(exc)] == ["run"]


@pytest.mark.parametrize(
    ("section", "field", "card"),
    [
        ("bgc", "wrt_his", "output"),
        ("bgc", "interp_frc", "advanced"),
        ("bgc", "xco2air_default", "forcing"),
        ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation", "model"),
    ],
)
def test_a_settings_error_goes_to_the_card_that_shows_the_setting(section, field, card):
    exc = CoreValidationError.from_exception_data(
        "ForgeBlueprint",
        [
            {
                "type": "value_error",
                "loc": ("model_settings", section, field),
                "input": 0,
                "ctx": {"error": ValueError("bad value")},
            }
        ],
    )
    (problem,) = _blueprint_problems(exc)
    assert problem.card == card
    assert problem.text.endswith(": bad value")  # no "Value error, " prefix


@pytest.mark.parametrize(
    ("message", "card"),
    [
        ("partitioning.auto_tiling requires use_pio=True", "run"),
        ("cdr.mode='netcdf' requires cdr_forcing_file to be set.", "run"),
        ("domain.grid_file is set (a user-supplied grid) but grid_kwargs ...", "grid"),
        ('bgc_mode="none" but the ForcingSpec requests BGC forcing:\n  - x', "model"),
        ("initial_conditions is required unless the domain has a parent", "forcing"),
        ("BGC initial conditions, row 2, Constant values: bad", "forcing"),
        ("Initial conditions, Additional options: not a mapping", "forcing"),
        ("carbonate sensitivities: attach a directory of ROMS _cdrgas", "model"),
        (
            "The blueprint cannot run as configured:\n"
            "  - initial_conditions.bgc_sources (UNIFIED, ESPER): no CHL\n"
            "  - A BGC source set needs a source of CHL and Fe",
            "forcing",
        ),
        ("The ModelSpec is missing a code.roms repository", "review"),
        ("runaway value", "review"),  # a key must end where the path does
    ],
)
def test_a_plain_error_is_placed_by_the_path_it_starts_with(message, card):
    (problem,) = _blueprint_problems(ValueError(message))
    assert problem.card == card


def test_every_mapped_model_is_a_forge_section_model():
    """A Forge model renamed under the table would quietly send its errors to Review."""
    for name in _ERROR_MODEL_PATHS:
        assert isinstance(getattr(forge_blueprint, name, None), type), name


def test_every_card_in_the_table_is_a_wizard_card():
    assert set(_ERROR_CARDS.values()) <= {key for key, _ in _STICKY_STEPS}


@pytest.mark.parametrize(
    ("text", "words"),
    [
        (
            "initial_conditions.bgc_sources[0].use_vars",
            "BGC initial conditions, row 1, Variables to read",
        ),
        ("forcing.boundary.bgc_sources[1]", "BGC boundary forcing, row 2"),
        (
            "initial_conditions.bgc_sources (UNIFIED)",
            "BGC initial conditions (UNIFIED)",
        ),
        (
            "river[0] (include_bgc=True)",
            "River forcing, row 1 (Include biogeochemistry=True)",
        ),
        ("the river is wide", "the river is wide"),  # a bare word may be prose
        ("forcing.surfaces[0]", "forcing.surfaces[0]"),  # not a row path
    ],
)
def test_wizard_words(text, words):
    assert _wizard_words(text) == words


# --- the cards ---------------------------------------------------------------------


def test_an_empty_second_bgc_source_flags_only_the_forcing_card():
    """The reported case, end to end: adding an empty BGC initial-conditions row
    leaves Model and Run "Complete", says so on the Forcing card in the wizard's
    words, and the sticky bar's Invalid chip links there.
    """
    wiz = _wizard()
    editor = wiz._forcing_editor
    editor._add("ic_bgc")

    assert wiz.config is None
    assert "1 problem" in wiz.card_chips["forcing"].value
    assert "Complete" in wiz.card_chips["model"].value
    assert "Complete" in wiz.card_chips["run"].value
    assert "Invalid" in wiz.card_chips["review"].value

    banner = wiz.card_errors["forcing"]
    assert _shown(banner)
    assert "BGC initial conditions, row 1" in banner.value
    assert "Variables to read" in banner.value
    assert [k for k, w in wiz.card_errors.items() if _shown(w)] == ["forcing"]
    assert "href='#forge-sec-forcing'" in wiz.sticky_bar.value
    assert "Invalid blueprint" in wiz.review_card.derived.value
    assert "href='#forge-sec-forcing'" in wiz.review_card.derived.value

    note = editor._bgc_notes["ic_bgc"]
    assert _shown(note)
    assert "Variables to read" in note.value
    assert all(
        "forge-need" in r["use_vars"]._dom_classes for r in editor._rows["ic_bgc"]
    )

    editor._remove("ic_bgc", editor._rows["ic_bgc"][1])
    assert wiz.config is not None, wiz.review_card.derived.value
    assert not _shown(banner)
    assert not _shown(note)
    assert "forge-need" not in editor._rows["ic_bgc"][0]["use_vars"]._dom_classes
    assert "● Valid" in wiz.sticky_bar.value


def test_a_failed_build_keeps_the_forcing_cards_modified_state():
    """Forcing's "Modified" comes from the editor, not from a built blueprint, so a
    problem elsewhere does not reset it to "At preset defaults".
    """
    wiz = _wizard()
    wiz._forcing_editor.ic_validate.value = False  # a forcing edit that stays valid
    assert "Modified" in wiz.card_chips["forcing"].value

    wiz.run_card.start.value = None
    assert wiz.config is None
    assert "Modified" in wiz.card_chips["forcing"].value
    assert "1 problem" in wiz.card_chips["run"].value
    assert "start and end dates" in wiz.card_errors["run"].value
    assert "Complete" in wiz.card_chips["model"].value
    assert "href='#forge-sec-run'" in wiz.sticky_bar.value


def test_an_unplaced_error_stays_on_review(monkeypatch):
    wiz = _wizard()
    import cstar.wizard.wizard as wizard_module

    def refuse(**_kw):
        raise RuntimeError("the ModelSpec is unreadable")

    monkeypatch.setattr(wizard_module, "build_forge_blueprint", refuse)
    wiz._rebuild()
    assert wiz.config is None
    assert "the ModelSpec is unreadable" in wiz.review_card.derived.value
    assert not any(_shown(w) for w in wiz.card_errors.values())
    assert "Complete" in wiz.card_chips["model"].value
    assert "Complete" in wiz.card_chips["run"].value
    assert "href='#forge-sec-review'" in wiz.sticky_bar.value


def test_a_card_banner_escapes_the_message(monkeypatch):
    wiz = _wizard()
    import cstar.wizard.wizard as wizard_module

    def refuse(**_kw):
        raise ValueError("cdr.mode <custom> & 'x'")

    monkeypatch.setattr(wizard_module, "build_forge_blueprint", refuse)
    wiz._rebuild()
    assert "cdr.mode &lt;custom&gt; &amp; &#x27;x&#x27;" in wiz.card_errors["run"].value


# --- the BGC panes' note mirrors Forge ----------------------------------------------


@pytest.mark.parametrize(
    "use_vars",
    [
        ("",),
        ("DIC",),
        ("", ""),
        ("DIC", ""),
        ("DIC", "ALK"),
        ("DIC", "DIC"),
        ("DIC,DIC", "ALK"),
        ("DIC,ALK", "NO3", "ALK"),
        ("DIC", " , "),
    ],
)
@pytest.mark.parametrize("cat", ["ic_bgc", "boundary_bgc"])
def test_the_bgc_note_shows_exactly_when_forge_refuses(cat, use_vars):
    sources = _bgc_sources(*use_vars)
    section = {"source": {"name": "GLORYS"}, "bgc_sources": sources}
    seed = (
        {"initial_conditions": section}
        if cat == "ic_bgc"
        else {"forcing": {"boundary": section}}
    )
    editor = _ForcingEditor(W, seed, on_change=lambda: None)
    assert len(editor._rows[cat]) == len(use_vars)
    editor.refresh_bgc_notes()

    gathered = editor.gather()
    block = (
        gathered["initial_conditions"]
        if cat == "ic_bgc"
        else gathered["forcing"]["boundary"]
    )
    items = [BgcSourceItem.model_validate(bs) for bs in block["bgc_sources"]]
    try:
        _require_partitioned_bgc_use_vars(items, "x")
        refused = False
    except ValueError:
        refused = True
    assert _shown(editor._bgc_notes[cat]) is refused
    flagged = any("forge-need" in r["use_vars"]._dom_classes for r in editor._rows[cat])
    assert flagged is refused


def test_the_bgc_note_is_quiet_for_a_section_the_blueprint_does_not_carry():
    editor = _ForcingEditor(
        W,
        {
            "forcing": {
                "boundary": {
                    "source": {"name": "GLORYS"},
                    "bgc_sources": _bgc_sources("", ""),
                }
            }
        },
        on_change=lambda: None,
    )
    editor.refresh_bgc_notes()
    assert _shown(editor._bgc_notes["boundary_bgc"])
    editor.boundary_name.value = _BOUNDARY_NONE
    editor.refresh_bgc_notes()
    assert not _shown(editor._bgc_notes["boundary_bgc"])
