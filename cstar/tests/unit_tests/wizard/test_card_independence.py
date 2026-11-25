"""Each wizard card stands on its own.

A card builds with fake callables and no wizard, and imports neither the wizard
nor another card. The wizard rebuilds on every widget a card lists in
``rebuild_on``, after that widget's other observers.
"""

import ast
from contextlib import contextmanager
from pathlib import Path

import ipywidgets as W
import pytest

import cstar.wizard
from cstar.catalog.domain_catalog import default_catalog
from cstar.wizard.domain_card import DomainCard
from cstar.wizard.model_card import ModelCard
from cstar.wizard.review_card import ReviewCard, SaveRefused
from cstar.wizard.run_card import RunCard
from cstar.wizard.wizard import ForgeBlueprintWizard

_CARD_STEMS = {"model_card", "run_card", "review_card", "domain_card"}


class FakeSuspend:
    """A reentrant suspend: ``__call__`` is a context manager, ``is_suspended``
    is true while any is open.
    """

    def __init__(self):
        self.depth = 0

    @contextmanager
    def __call__(self):
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1

    def is_suspended(self):
        return self.depth > 0


class FakeRebuild:
    """A rebuild callable that counts its calls."""

    def __init__(self):
        self.calls = 0

    def __call__(self, *_args):
        self.calls += 1


def _common():
    suspend = FakeSuspend()
    rebuild = FakeRebuild()
    return (
        rebuild,
        {
            "rebuild": rebuild,
            "suspend": suspend,
            "is_suspended": suspend.is_suspended,
        },
        suspend,
    )


def _check_layout(card):
    """Wire the card and check its rows and `rebuild_on` are well formed."""
    card.wire()
    assert all(isinstance(r, W.Widget) for r in card.rows())
    rebuild_on = card.rebuild_on
    assert all(isinstance(w, W.Widget) and "value" in w.traits() for w in rebuild_on)
    assert len({id(w) for w in rebuild_on}) == len(rebuild_on)


def test_review_card_stands_alone():
    """ReviewCard rebuilds on a name edit, but not under suspend."""
    rebuild, kw, suspend = _common()
    card = ReviewCard(
        W,
        default_catalog,
        **kw,
        config=lambda: None,
        save_blueprint=lambda p: p,
        boundaries_pending=lambda: False,
    )
    _check_layout(card)
    assert card.rebuild_on == ()

    card.name.value = "x"
    assert rebuild.calls == 1
    assert card._name_touched

    with suspend():
        card.name.value = "y"
    assert rebuild.calls == 1


def _review_card_saving_with(save_blueprint, tmp_path):
    """A ReviewCard whose wizard save is ``save_blueprint``, set to save to
    ``tmp_path / "bp.yaml"``.
    """
    _rebuild, kw, _suspend = _common()
    card = ReviewCard(
        W,
        default_catalog,
        **kw,
        config=lambda: None,
        save_blueprint=save_blueprint,
        boundaries_pending=lambda: False,
    )
    card.save_path.value = str(tmp_path / "bp.yaml")
    return card


def test_review_card_save_reports_the_file_written(tmp_path):
    """Save asks the wizard's save for the Save path and names the file written."""
    asked = []

    def _save(path):
        asked.append(path)
        return path

    card = _review_card_saving_with(_save, tmp_path)
    card._on_save(None)
    assert asked == [tmp_path / "bp.yaml"]
    assert f"Saved {tmp_path / 'bp.yaml'}" in card.save_status.value


@pytest.mark.parametrize(
    ("refused", "save_message", "run_message"),
    [
        (
            SaveRefused.BOUNDARIES,
            "Save aborted — open boundaries could not be derived from the grid",
            "Run aborted — open boundaries could not be derived from the grid",
        ),
        (
            SaveRefused.INVALID,
            "Nothing to save — config is invalid.",
            "Nothing to run — config is invalid.",
        ),
    ],
)
def test_review_card_reports_a_refused_save(
    refused, save_message, run_message, tmp_path
):
    """Save and Run Forge here say why the wizard's save refused, in the card's
    own words, and Run starts nothing.
    """
    asked = []

    def _save(path):
        asked.append(path)
        return refused

    card = _review_card_saving_with(_save, tmp_path)
    card._on_save(None)
    assert save_message in card.save_status.value
    card.run_output.append_stdout("previous run\n")
    card._on_run(None)
    assert run_message in card.run_status.value
    assert asked == [tmp_path / "bp.yaml"] * 2
    # a run that started would have cleared the previous run's log
    assert [o["text"] for o in card.run_output.outputs] == ["previous run\n"]


def test_review_card_reports_a_failed_save(tmp_path):
    """A save that raises is reported by Save and by Run, and Run starts nothing."""

    def _save(_path):
        raise OSError("disk full")

    card = _review_card_saving_with(_save, tmp_path)
    card._on_save(None)
    assert "OSError: disk full" in card.save_status.value
    card.run_output.append_stdout("previous run\n")
    card._on_run(None)
    assert "OSError: disk full" in card.run_status.value
    assert "while running" not in card.run_status.value
    assert [o["text"] for o in card.run_output.outputs] == ["previous run\n"]


def test_model_card_stands_alone():
    """ModelCard's BGC mode drives its properties and its visibility syncs."""
    _rebuild, kw, _suspend = _common()
    card = ModelCard(W, default_catalog, **kw, use_pio=lambda: False)
    _check_layout(card)

    card.bgc_dd.value = "cdr_lite"
    assert card.is_cdr_lite
    assert not card.uses_marbl
    assert card.cs_group.layout.display == ""

    card.bgc_dd.value = "marbl"
    assert card.uses_marbl
    assert not card.is_cdr_lite
    assert card.cs_group.layout.display == "none"


def test_run_card_stands_alone():
    """RunCard shows its CDR hint from the `cdr_lite` callable alone."""
    _rebuild, kw, _suspend = _common()
    state = {"cdr_lite": True}
    card = RunCard(
        W,
        default_catalog,
        **kw,
        cdr_lite=lambda: state["cdr_lite"],
        grid_center=lambda: (0.0, 0.0),
        grid_fingerprint=lambda: (),
        build_grid=lambda: None,
    )
    _check_layout(card)

    card.sync_cdr_lite_hint()
    assert card.cdr_lite_hint.layout.display == ""
    state["cdr_lite"] = False
    card.sync_cdr_lite_hint()
    assert card.cdr_lite_hint.layout.display == "none"


def test_domain_card_stands_alone():
    """DomainCard marks dt touched and rebuilds on an edit, but not under suspend."""
    rebuild, kw, suspend = _common()
    card = DomainCard(W, default_catalog, **kw, show_summary=lambda _msg: None)
    _check_layout(card)
    assert not card._dt_touched
    before = rebuild.calls

    with suspend():
        card.dt.value = card.dt.value + 1
    assert not card._dt_touched
    assert rebuild.calls == before

    card.dt.value = card.dt.value + 1
    assert card._dt_touched
    assert rebuild.calls == before + 1


def _imported_names(path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_cards_import_neither_wizard_nor_each_other():
    """No card module imports the wizard or another card, even lazily."""
    files = sorted(Path(cstar.wizard.__file__).parent.glob("*_card.py"))
    assert {f.stem for f in files} == _CARD_STEMS

    for path in files:
        forbidden = {"cstar.wizard.wizard"} | {
            f"cstar.wizard.{stem}" for stem in _CARD_STEMS - {path.stem}
        }
        bad = _imported_names(path) & forbidden
        assert not bad, f"{path.name} imports {sorted(bad)}"


def test_wizard_rebuilds_last_on_every_card_widget():
    """On each `rebuild_on` widget the wizard's `_rebuild` is the last observer."""
    wiz = ForgeBlueprintWizard()
    cards = (wiz.model_card, wiz.run_card, wiz.review_card, wiz.domain_card)
    total = 0
    for card in cards:
        for w in card.rebuild_on:
            total += 1
            last = w._trait_notifiers["value"]["change"][-1]
            assert last == wiz._rebuild, (type(card).__name__, w)
    # The 45 widgets of the wizard's old hard-coded list (25 entries, two of them
    # dicts of nested-grid widgets) and the Run card's 9 simple-CDR fields.
    assert total >= 54
