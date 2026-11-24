"""The forcing editor pairs physics and BGC: five panes, with the Initial conditions
and Boundary forcing panes each holding a Physics and a Biogeochemistry subsection
and one options link for the settings both share.
"""

from datetime import date

import ipywidgets as W
import pytest

from cstar.wizard.wizard import (
    _IC_NONE,
    _ROW_OPTIONS_TITLE,
    ForgeBlueprintWizard,
    _ForcingEditor,
)

_TITLES = [
    "Initial conditions",
    "Surface forcing",
    "Boundary forcing",
    "Tidal forcing",
    "River forcing",
]


def _wizard() -> ForgeBlueprintWizard:
    wiz = ForgeBlueprintWizard()
    wiz.start.value = date(2012, 1, 1)
    wiz.end.value = date(2012, 1, 2)
    wiz._rebuild()
    return wiz


def _title(wiz, i: int):
    return wiz._forcing_editor._acc.panes[i].title


def _pills(wiz, i: int) -> list[tuple[str, str]]:
    return [(p.text, p.kind) for p in _title(wiz, i).pills]


def _keys(box) -> list[str]:
    """The ``forge_key`` of each field row at or below *box*, in order."""
    found: list[str] = []

    def walk(w) -> None:
        key = getattr(w, "forge_key", None)
        if key:
            found.append(key)
            return
        for c in getattr(w, "children", ()):
            walk(c)
        body = getattr(w, "body", None)
        if body is not None:
            walk(body)

    walk(box)
    return list(dict.fromkeys(found))  # a collapsible lists its body twice


def _shown(w) -> bool:
    return w.layout.display != "none"


def _ic_pane(editor):
    return editor.widget.panes[0].body


def _options_link(pane_body):
    return pane_body.children[2]


def test_five_panes_with_required_chips():
    wiz = _wizard()
    panes = wiz._forcing_editor._acc.panes
    assert len(panes) == 5
    assert [_title(wiz, i).title for i in range(5)] == _TITLES
    assert [_title(wiz, i).chip for i in range(5)] == [
        "Required",
        "Required",
        "Required",
        "Optional",
        "Optional",
    ]


def test_ic_pane_carries_the_source_and_bgc_pills_and_none_drops_the_bgc_one():
    wiz = _wizard()
    editor = wiz._forcing_editor
    texts = [t for t, _ in _pills(wiz, 0)]
    assert editor.ic_name.value in texts
    assert "BGC UNIFIED" in texts
    assert _shown(editor._bgc_sections["ic_bgc"])

    editor.ic_name.value = _IC_NONE
    wiz._update_status(wiz.config)
    assert "BGC UNIFIED" not in [t for t, _ in _pills(wiz, 0)]
    assert not _shown(editor._bgc_sections["ic_bgc"])

    editor.ic_name.value = "GLORYS"
    wiz._update_status(wiz.config)
    assert "BGC UNIFIED" in [t for t, _ in _pills(wiz, 0)]
    assert _shown(editor._bgc_sections["ic_bgc"])


def test_a_physics_only_model_hides_empty_bgc_sections():
    wiz = _wizard()
    editor = wiz._forcing_editor
    for cat in ("ic_bgc", "boundary_bgc"):
        for row in list(editor._rows[cat]):
            editor._remove(cat, row)
    wiz.bgc_dd.value = "none"
    wiz._update_status(wiz.config)
    for cat in ("ic_bgc", "boundary_bgc"):
        assert not _shown(editor._bgc_sections[cat])
    for i in (0, 2):
        assert not any(t.startswith(("BGC", "no BGC")) for t, _ in _pills(wiz, i))


def test_show_bgc_false_keeps_a_section_with_rows_and_removing_the_last_hides_it():
    wiz = _wizard()
    editor = wiz._forcing_editor
    assert editor._rows["ic_bgc"]
    editor.show_bgc(False)
    assert _shown(editor._bgc_sections["ic_bgc"])
    editor._remove("ic_bgc", editor._rows["ic_bgc"][0])
    assert not _shown(editor._bgc_sections["ic_bgc"])
    editor._add("ic_bgc")
    assert _shown(editor._bgc_sections["ic_bgc"])


@pytest.mark.parametrize("prefix", ["ic", "boundary"])
def test_without_marbl_a_bgc_setting_off_its_default_keeps_the_section_shown(prefix):
    """The blueprint still carries these settings, so they stay where they can be
    seen and reset.
    """
    cat = f"{prefix}_bgc"
    editor = _ForcingEditor(W, {}, on_change=lambda: None)
    editor.show_bgc(False)
    assert not editor._rows[cat]
    assert not _shown(editor._bgc_sections[cat])

    groom = getattr(editor, f"{prefix}_groom")
    groom.value = False
    assert _shown(editor._bgc_sections[cat])
    groom.value = True
    assert not _shown(editor._bgc_sections[cat])

    interp = getattr(editor, f"{prefix}_bgc_interp")
    interp.value = "density"
    assert _shown(editor._bgc_sections[cat])


def test_adding_a_bgc_source_on_a_physics_only_model_shows_it_in_the_header():
    wiz = _wizard()
    wiz.bgc_dd.value = "none"
    editor = wiz._forcing_editor
    assert not editor._rows["ic_bgc"]
    assert not _shown(editor._bgc_sections["ic_bgc"])

    editor._add("ic_bgc")  # its on_change rebuilds, which retitles the panes
    assert _shown(editor._bgc_sections["ic_bgc"])
    assert any(t.startswith("BGC") for t, _ in _pills(wiz, 0))


def test_a_bgc_source_that_does_not_partition_adds_a_warn_pill_until_fixed():
    wiz = _wizard()
    editor = wiz._forcing_editor
    editor._add("ic_bgc")
    wiz._update_status(wiz.config)
    assert editor.bgc_needs_fix("ic_bgc")
    assert ("check BGC sources", "warn") in _pills(wiz, 0)
    assert ("check BGC sources", "warn") not in _pills(wiz, 2)

    editor._remove("ic_bgc", editor._rows["ic_bgc"][1])
    wiz._update_status(wiz.config)
    assert not editor.bgc_needs_fix("ic_bgc")
    assert ("check BGC sources", "warn") not in _pills(wiz, 0)


@pytest.mark.parametrize(("prefix", "n"), [("ic", 7), ("boundary", 6)])
def test_options_link_counts_settings_and_non_default(prefix, n):
    editor = _ForcingEditor(W, {}, on_change=lambda: None)
    body = editor.widget.panes[0 if prefix == "ic" else 2].body
    link = _options_link(body)
    title = link.title
    assert title.title == _ROW_OPTIONS_TITLE
    assert [p.text for p in title.pills] == [f"{n} settings · defaults"]

    prefill = getattr(editor, f"{prefix}_prefill")
    prefill.value = next(o for o in prefill.options if o)
    assert [p.text for p in link.title.pills] == [f"{n} settings · 1 non-default"]


def test_options_link_hides_with_no_ic():
    editor = _ForcingEditor(W, {}, on_change=lambda: None)
    link = _options_link(_ic_pane(editor))
    assert _shown(link)
    editor.ic_name.value = _IC_NONE
    assert not _shown(link)
    editor.ic_name.value = "GLORYS"
    assert _shown(link)


@pytest.mark.parametrize("prefix", ["ic", "boundary"])
def test_pane_layout_physics_bgc_options(prefix):
    editor = _ForcingEditor(W, {}, on_change=lambda: None)
    body = editor.widget.panes[0 if prefix == "ic" else 2].body
    physics, bgc, options = body.children
    p = prefix
    assert _keys(physics) == [f"{p}.{p}_name", f"{p}.{p}_layout", f"{p}.{p}_path"]
    cat = f"{p}_bgc"
    assert bgc is editor._bgc_sections[cat]
    assert editor._containers[cat] in bgc.children
    assert _keys(bgc.children[1]) == [f"{p}.{p}_bgc_interp", f"{p}.{p}_groom"]
    suffixes = ["validate", "prefill", "regrid_method", "extrap_method"]
    suffixes += ["bottom_fill_blend_m", "options"]
    if prefix == "ic":
        suffixes.insert(0, "flex_time")
    assert _keys(options) == [f"{p}.{p}_{s}" for s in suffixes]
