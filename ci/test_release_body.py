"""
Unit tests for ``release_body.py`` (RST -> Markdown release-notes conversion).

This ``ci/`` script has no existing pytest CI wiring in this repo; run
directly with::

    pytest ci/test_release_body.py

Covers the four transforms the converter promises, and — as a guard against
the converter silently degrading — runs it over every real
``docs/releases/*.rst`` file and asserts no RST link/anchor syntax survives.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "release_body",
    Path(__file__).resolve().parent / "release_body.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


_SAMPLE = (
    ".. _0.11.1:\n"
    "\n"
    "0.11.1\n"
    "----------\n"
    "\n"
    "\n"
    "Breaking Changes\n"
    "~~~~~~~~~~~~~~~~\n"
    "\n"
    "\n"
    "- CSTAR_CLOBBER_WORKING_DIR is gone. Use ``--clobber all`` instead. "
    "(`#635 <https://github.com/CWorthy-ocean/C-Star/pull/635>`_)\n"
    "\n"
    "New features\n"
    "~~~~~~~~~~~~\n"
    "\n"
    "\n"
    "- Add ``--clobber <step name>``. (`#635 <https://github.com/CWorthy-ocean/C-Star/pull/635>`_)\n"
)


def test_conversion_of_all_four_transforms():
    md = _MODULE.rst_to_markdown(_SAMPLE)
    # Anchor line dropped.
    assert "_0.11.1" not in md
    # Release title + underline dropped (GitHub shows the tag).
    assert "0.11.1" not in md
    assert "----" not in md and "~~~~" not in md
    # Category headings -> ### .
    assert "### Breaking Changes" in md
    assert "### New features" in md
    # `text <url>`_ -> [text](url)
    assert "[#635](https://github.com/CWorthy-ocean/C-Star/pull/635)" in md
    assert "<https://" not in md and "`_" not in md
    # ``code`` -> `code`
    assert "`--clobber all`" in md
    assert "``" not in md
    # Bullets pass through.
    assert md.count("- ") == 2


def test_first_heading_is_title_rest_are_categories():
    md = _MODULE.rst_to_markdown(_SAMPLE)
    # Exactly the two categories become headings; the title does not.
    assert md.count("### ") == 2


def test_inline_helpers():
    assert _MODULE._inline("see (`#1 <http://x/1>`_)", {}) == "see ([#1](http://x/1))"
    assert _MODULE._inline("run ``foo bar``", {}) == "run `foo bar`"


def test_named_reference_footer_is_resolved():
    # A "commit history" footer (named target definition + named reference)
    # becomes an inline Markdown link, with no raw RST left behind.
    rst = (
        ".. _0.2.0:\n"
        "\n"
        "0.2.0\n"
        "-----\n"
        "\n"
        "New features\n"
        "~~~~~~~~~~~~\n"
        "\n"
        "- Did a thing.\n"
        "\n"
        "---\n"
        "\n"
        "For more details, see the `0.2.0 commit history`_.\n"
        "\n"
        ".. _0.2.0 commit history: https://github.com/x/compare/0.1.0...0.2.0\n"
    )
    md = _MODULE.rst_to_markdown(rst)
    assert "[0.2.0 commit history](https://github.com/x/compare/0.1.0...0.2.0)" in md
    # No raw RST leaks: no target-definition line, no stray transition marker,
    # no unresolved `name`_ reference.
    assert ".. _" not in md
    assert not any(ln.strip() == "---" for ln in md.splitlines())
    assert not _MODULE._NAMED_REF_RE.search(md)


def test_unknown_named_reference_falls_back_to_text():
    md = _MODULE.rst_to_markdown("x\n-\n\nSee `nowhere`_ for details.\n")
    assert "See nowhere for details." in md
    assert not _MODULE._NAMED_REF_RE.search(md)


def test_is_underline():
    assert _MODULE._is_underline("~~~~")
    assert _MODULE._is_underline("----")
    assert not _MODULE._is_underline("- a bullet")
    assert not _MODULE._is_underline("Breaking Changes")
    # '..' (comment) and '::' (literal-block marker) are not underlines.
    assert not _MODULE._is_underline("..")
    assert not _MODULE._is_underline("::")


@pytest.mark.parametrize(
    "rst_file", sorted(_MODULE.RELEASES_DIR.glob("*.rst")), ids=lambda p: p.name
)
def test_no_rst_syntax_survives_on_real_files(rst_file):
    """Guard: converting any shipped release file leaves no raw RST syntax behind."""
    md = _MODULE.rst_to_markdown(rst_file.read_text())
    assert not _MODULE._LINK_RE.search(md)  # no `text <url>`_ inline links
    lines = md.splitlines()
    # Named references are searched per line: an inline literal like `_private`
    # produces a lone backtick+underscore that would spuriously bridge across
    # lines to a later one under a whole-document search.
    assert not any(_MODULE._NAMED_REF_RE.search(ln) for ln in lines)  # no `name`_ refs
    assert not any(_MODULE._ANCHOR_RE.match(ln) for ln in lines)  # no anchors
    assert not any(_MODULE._TARGET_RE.match(ln) for ln in lines)  # no target defs
    assert not any(ln.strip() == "---" for ln in lines)  # no stray transitions
    assert "``" not in md  # no unconverted inline literals
