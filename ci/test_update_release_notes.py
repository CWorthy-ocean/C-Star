"""
Unit tests for ``_md_code_to_rst`` in ``update_release_notes.py``.

Not wired into the main ``cstar/tests/`` pytest suite (this ``ci/`` script has
no existing test coverage there); run directly with::

    pytest ci/test_update_release_notes.py

Covers the tricky real-world release-notes lines that motivated the
tokenize-protect-then-convert approach: RST roles, hyperlink references
(explicit-target and bare-named), adjacent code spans, and idempotency.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "update_release_notes", Path(__file__).resolve().parent / "update_release_notes.py"
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_md_code_to_rst = _MODULE._md_code_to_rst


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
