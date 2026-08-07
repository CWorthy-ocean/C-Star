"""
Unit tests for ``_md_code_to_rst`` and ``parse_pr_body`` in
``update_release_notes.py``.

Not wired into the main ``cstar/tests/`` pytest suite (this ``ci/`` script has
no existing test coverage there); run directly with::

    pytest ci/test_update_release_notes.py

Covers the tricky real-world release-notes lines that motivated the
tokenize-protect-then-convert approach: RST roles, hyperlink references
(explicit-target and bare-named), adjacent code spans, and idempotency.

The ``parse_pr_body`` tests focus on the un-bulleted-prose fallback: authors
regularly delete the PR template's ``- `` markers and write plain sentences
under a category, and those notes used to be dropped silently.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "update_release_notes", Path(__file__).resolve().parent / "update_release_notes.py"
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_md_code_to_rst = _MODULE._md_code_to_rst
parse_pr_body = _MODULE.parse_pr_body


def test_plain_span_converted():
    assert _md_code_to_rst("Rename `caseroot` to `directory`") == (
        "Rename ``caseroot`` to ``directory``"
    )


def test_role_preserved():
    assert _md_code_to_rst(":term:`blueprint`") == ":term:`blueprint`"
    assert _md_code_to_rst(":ref:`v0.0.1-alpha` is deprecated") == (
        ":ref:`v0.0.1-alpha` is deprecated"
    )


def test_explicit_target_ref_preserved():
    text = "fixed a bug (`#519 <https://github.com/CWorthy-ocean/C-Star/pull/519>`_)"
    assert _md_code_to_rst(text) == text


def test_bare_named_ref_preserved():
    assert _md_code_to_rst("see the `commit history`_ for details") == (
        "see the `commit history`_ for details"
    )


def test_adjacent_code_spans_with_dunder_not_misread_as_ref():
    # Regression: a naive reference-matcher can mis-read a span's closing
    # backtick + prose + the next span's opening backtick + `__init__`'s
    # leading underscores as one bogus anonymous reference.
    text = (
        "The `WorkplanTransformer` no longer receives transforms to the "
        "`__init__` method"
    )
    assert _md_code_to_rst(text) == (
        "The ``WorkplanTransformer`` no longer receives transforms to the "
        "``__init__`` method"
    )


def test_existing_double_backticks_untouched():
    text = "already ``ROMSSimulation`` fine"
    assert _md_code_to_rst(text) == text


def test_idempotent():
    text = "Rename `caseroot` to `directory`, see `commit history`_"
    once = _md_code_to_rst(text)
    twice = _md_code_to_rst(once)
    assert once == twice


# ---------------------------------------------------------------------------
# parse_pr_body
# ---------------------------------------------------------------------------


def test_bulleted_sections_unchanged():
    body = (
        "# Summary\nDoes a thing.\n\n## Bug Fixes\n- Fixed one thing\n- Fixed another\n"
    )
    assert parse_pr_body(body) == {
        "Bug Fixes": [("Fixed one thing", []), ("Fixed another", [])]
    }


def test_unbulleted_prose_becomes_a_note():
    body = "## Bug Fixes\nPassive tracers were not written to the output files.\n"
    assert parse_pr_body(body) == {
        "Bug Fixes": [("Passive tracers were not written to the output files.", [])]
    }


def test_unbulleted_prose_gets_rst_code_conversion():
    body = "## Bug Fixes\nThe `extract_root_name` key was missing.\n"
    assert parse_pr_body(body) == {
        "Bug Fixes": [("The ``extract_root_name`` key was missing.", [])]
    }


def test_wrapped_prose_joins_into_one_note_per_paragraph():
    body = "## Improvements\nFirst paragraph line one\nline two.\n\nSecond paragraph.\n"
    assert parse_pr_body(body) == {
        "Improvements": [
            ("First paragraph line one line two.", []),
            ("Second paragraph.", []),
        ]
    }


def test_prose_ignored_when_the_section_also_has_bullets():
    # An introductory paragraph followed by bullets must still yield only the
    # bullets, exactly as before the fallback existed.
    body = (
        "## New Features\nThis PR adds the following:\n\n- Feature one\n- Feature two\n"
    )
    assert parse_pr_body(body) == {
        "New Features": [("Feature one", []), ("Feature two", [])]
    }


def test_placeholder_prose_and_bullets_are_dropped():
    body = "## Breaking Changes\nNone\n\n## New Features\n- N/A\n"
    assert parse_pr_body(body) == {}


def test_summary_and_checklist_prose_never_scraped():
    body = (
        "# Summary\n"
        "A long prose summary that must not become a release note.\n"
        "\n"
        "## Code Review Checklist\n"
        "Everything looks fine to me.\n"
    )
    assert parse_pr_body(body) == {}


def test_prose_under_an_invented_heading_is_ignored():
    # PR authors add narrative headings ("Testing", "Setup", …); only the
    # template's own categories are eligible for the prose fallback.
    body = "## Testing\nRan the full suite locally and it passed.\n"
    assert parse_pr_body(body) == {}


def test_fenced_code_and_markup_lines_excluded():
    body = (
        "## Miscellaneous\n"
        "Now supports the new flag.\n"
        "\n"
        "```bash\n"
        "run --with-flag\n"
        "```\n"
        "\n"
        '<img width="600" src="https://example.invalid/x.png" />\n'
        "\n"
        "| col | col |\n"
        "|-----|-----|\n"
    )
    assert parse_pr_body(body) == {
        "Miscellaneous": [("Now supports the new flag.", [])]
    }


def test_html_comments_do_not_become_notes():
    body = (
        "## Bug Fixes\n"
        "<!-- List any behavioral changes resulting from pre-existing code -->\n"
        "Fixed the thing.\n"
    )
    assert parse_pr_body(body) == {"Bug Fixes": [("Fixed the thing.", [])]}


def test_sub_bullets_still_attach_to_their_parent():
    body = "## Improvements\n- Parent note\n  - child one\n  - child two\n"
    assert parse_pr_body(body) == {
        "Improvements": [("Parent note", ["child one", "child two"])]
    }
