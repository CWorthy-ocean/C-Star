"""
Tests for ``add_release_include.py`` (listing a new release in the index).

Run with ``pytest ci/test_add_release_include.py``; like the other ``ci/``
tests, CI does not run it.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

_SPEC = importlib.util.spec_from_file_location(
    "add_release_include", REPO / "ci" / "add_release_include.py"
)
assert _SPEC is not None and _SPEC.loader is not None
ari = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ari)

INDEX = (
    "Release notes\n#############\n\n"
    ".. include:: releases/0.18.0.rst\n\n"
    ".. include:: releases/v0.7.0.rst\n"
)


@pytest.fixture
def docs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A scratch ``docs/`` with an index and two released files."""
    releases = tmp_path / "releases"
    releases.mkdir()
    (releases / "0.18.0.rst").write_text(".. _0.18.0:\n\n0.18.0\n------\n")
    (releases / "v0.7.0.rst").write_text(".. _v0.7.0:\n\nv0.7.0\n------\n")
    (tmp_path / "releases.rst").write_text(INDEX)
    monkeypatch.setattr(ari, "RELEASES_DIR", releases)
    monkeypatch.setattr(ari, "RELEASES_INDEX", tmp_path / "releases.rst")
    return tmp_path


def _main(monkeypatch: pytest.MonkeyPatch, tag: str) -> None:
    monkeypatch.setattr(sys, "argv", ["add_release_include.py", tag])
    ari.main()


def test_new_release_goes_above_the_other_includes():
    assert ari.add_include(INDEX, "0.19.0") == (
        "Release notes\n#############\n\n"
        ".. include:: releases/0.19.0.rst\n\n"
        ".. include:: releases/0.18.0.rst\n\n"
        ".. include:: releases/v0.7.0.rst\n"
    )


def test_real_index_takes_a_new_release():
    text = (REPO / "docs" / "releases.rst").read_text()
    new = ari.add_include(text, "9999.0.0")
    includes = [ln for ln in new.splitlines() if ln.startswith(".. include::")]
    assert includes[0] == ".. include:: releases/9999.0.0.rst"
    assert new.replace(".. include:: releases/9999.0.0.rst\n\n", "", 1) == text


@pytest.mark.parametrize("tag", ["0.18.0", "0.7.0"])
def test_release_already_in_the_index_is_refused(tag):
    with pytest.raises(SystemExit, match=f"already includes release '{tag}'"):
        ari.add_include(INDEX, tag)


def test_index_without_includes_is_refused():
    with pytest.raises(SystemExit, match="No '.. include::' entries"):
        ari.add_include("Release notes\n#############\n", "0.19.0")


def test_main_writes_the_index(docs, monkeypatch):
    _main(monkeypatch, "v0.19.0")
    assert (docs / "releases.rst").read_text().splitlines()[3] == (
        ".. include:: releases/0.19.0.rst"
    )


@pytest.mark.parametrize(
    ("tag", "name"), [("0.18.0", "0.18.0.rst"), ("v0.7.0", "v0.7.0.rst")]
)
def test_main_refuses_a_release_that_has_notes(docs, monkeypatch, tag, name):
    with pytest.raises(SystemExit, match=f"docs/releases/{name} already exists"):
        _main(monkeypatch, tag)
    assert (docs / "releases.rst").read_text() == INDEX


def test_main_refuses_a_notes_file_missing_from_the_index(docs, monkeypatch):
    # towncrier would overwrite it, though the index does not list it.
    (docs / "releases" / "0.19.0.rst").write_text("Hand-written.\n")
    with pytest.raises(SystemExit, match=r"docs/releases/0\.19\.0\.rst already"):
        _main(monkeypatch, "0.19.0")
