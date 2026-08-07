"""
Unit tests for ``drop_empty_sections`` in ``finalize_release_notes.py``.

This ``ci/`` script has no existing pytest CI wiring in this repo; run
directly with::

    pytest ci/test_finalize_release_notes.py

Covers the guarantees that make the drop safe: hand-written content is never
removed, and the RST stays well-formed no matter which categories disappear
(RST needs a blank line before every section title).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "finalize_release_notes",
    Path(__file__).resolve().parent / "finalize_release_notes.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
drop_empty_sections = _MODULE.drop_empty_sections
finalize_text = _MODULE.finalize_text
_is_placeholder = _MODULE._is_placeholder

_HEADER = ".. _unreleased:\n\nUnreleased\n----------\n\n"


def _section(title: str, body: str) -> str:
    return f"{title}\n{'~' * len(title)}\n\n{body}\n\n"


def test_empty_and_placeholder_categories_dropped():
    text = (
        _HEADER
        + _section("Breaking Changes", "- A real note")
        + _section("New Features", "- N/A")
        + _section("Bug Fixes", "- None")
    )
    out, dropped = drop_empty_sections(text)
    assert dropped == ["New Features", "Bug Fixes"]
    assert "New Features" not in out
    assert "Bug Fixes" not in out
    assert "- A real note\n" in out


def test_dropping_the_last_category_keeps_a_single_trailing_newline():
    text = (
        _HEADER
        + _section("Breaking Changes", "- A real note")
        + _section("Miscellaneous", "- N/A")
    )
    out, dropped = drop_empty_sections(text)
    assert dropped == ["Miscellaneous"]
    assert out.endswith("- A real note\n")
    assert not out.endswith("\n\n")


def test_dropping_a_leading_category_keeps_blank_line_before_next_title():
    text = (
        _HEADER
        + _section("Breaking Changes", "- N/A")
        + _section("Bug Fixes", "- A real note")
    )
    out, dropped = drop_empty_sections(text)
    assert dropped == ["Breaking Changes"]
    assert "----------\n\nBug Fixes\n~~~~~~~~~\n" in out


def test_hand_written_prose_is_not_dropped():
    text = _HEADER + _section("Improvements", "See the migration guide for details.")
    _, dropped = drop_empty_sections(text)
    assert dropped == []


def test_all_categories_empty_leaves_valid_rst():
    text = (
        _HEADER + _section("Breaking Changes", "- N/A") + _section("Bug Fixes", "- N/A")
    )
    out, dropped = drop_empty_sections(finalize_text(text, "1.0.0"))
    assert dropped == ["Breaking Changes", "Bug Fixes"]
    assert out == ".. _1.0.0:\n\n1.0.0\n----------\n"


def test_release_title_is_never_treated_as_a_category():
    # The title is underlined with "-", categories with "~"; only the latter
    # are candidates for removal.
    text = _HEADER + _section("Bug Fixes", "- A real note")
    out, dropped = drop_empty_sections(text)
    assert dropped == []
    assert out == text


def test_nothing_dropped_when_every_category_has_notes():
    text = _HEADER + _section("Bug Fixes", "- A real note")
    out, dropped = drop_empty_sections(text)
    assert dropped == []
    assert out == text


def test_placeholder_recognition():
    for value in ("N/A", "n/a", "None", "none.", "Nothing", "No changes"):
        assert _is_placeholder(value), value
    for value in (
        "Nonetheless, we fixed it",
        "No longer crashes",
        "N/A handling added",
    ):
        assert not _is_placeholder(value), value
