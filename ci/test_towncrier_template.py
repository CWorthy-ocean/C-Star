"""
Tests for the towncrier setup that turns ``changes/`` into ``docs/releases/``.

Run with ``pytest ci/test_towncrier_template.py``. Needs ``towncrier`` (in the
``dev`` extra) and is skipped without it; like the other ``ci/`` tests, CI does
not run it.

Each test builds a release from sample fragments with the repository's own
``[tool.towncrier]`` config and template, in a scratch git repository, exactly as
``finalize_release_notes.yaml`` does, then checks the result against the style
of the releases already in ``docs/releases/`` and reads it back with
``release_body.py``, as ``publish_release.yaml`` does.
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytest.importorskip("towncrier")

REPO = Path(__file__).resolve().parent.parent
URL = "https://github.com/CWorthy-ocean/C-Star/pull"
INDEX = "Release notes\n#############\n\n.. include:: releases/0.18.0.rst\n"
OLD_RELEASE = (
    ".. _0.18.0:\n\n0.18.0\n----------\n\n"
    f"Bug Fixes\n~~~~~~~~~\n\n- An old fix. (`#1 <{URL}/1>`_)\n"
)


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, REPO / "ci" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


release_body = _load("release_body")
add_release_include = _load("add_release_include")


def _run(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)


def _link(pr: int) -> str:
    return f"(`#{pr} <{URL}/{pr}>`_)"


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A git repository holding the real towncrier config and template."""
    shutil.copy(REPO / "pyproject.toml", tmp_path)
    (tmp_path / "ci").mkdir()
    shutil.copy(REPO / "ci" / "towncrier_template.rst", tmp_path / "ci")
    releases = tmp_path / "docs" / "releases"
    releases.mkdir(parents=True)
    (releases / "0.18.0.rst").write_text(OLD_RELEASE)
    (tmp_path / "docs" / "releases.rst").write_text(INDEX)
    (tmp_path / "changes").mkdir()
    shutil.copy(REPO / "changes" / "README.md", tmp_path / "changes")
    _run(tmp_path, "git", "init", "-q")
    _run(tmp_path, "git", "add", "-A")
    for module in (release_body, add_release_include):
        monkeypatch.setattr(module, "RELEASES_DIR", releases)
        monkeypatch.setattr(module, "RELEASES_INDEX", tmp_path / "docs" / "releases.rst")
    return tmp_path


def _add_fragments(repo: Path, fragments: dict[str, str]) -> None:
    for name, text in fragments.items():
        (repo / "changes" / name).write_text(text)
    _run(repo, "git", "add", "-A")


def _build(repo: Path, version: str) -> subprocess.CompletedProcess[str]:
    """List *version* in the index, then build its notes, as the workflow does."""
    index = repo / "docs" / "releases.rst"
    index.write_text(add_release_include.add_include(index.read_text(), version))
    return _run(
        repo, sys.executable, "-m", "towncrier", "build", "--yes", "--version", version
    )


def _notes(repo: Path, version: str) -> str:
    return (repo / "docs" / "releases" / f"{version}.rst").read_text()


def test_build_matches_release_notes_style(scratch):
    _add_fragments(
        scratch,
        {
            "12.breaking.rst": "Lead ``text``.\n- First detail.\n- Second detail.\n",
            "16.output.rst": "Different numbers.\n",
            "14.deprecation.rst": "Deprecated: ``a``, removed in 0.21.0; use ``b``.\n",
            "12.feature.rst": "A feature.\n",
            # Not yet numbered: renders without a link until the release.
            "+later.feature.rst": "An unnumbered feature.\n",
            "13.bugfix.rst": "A later fix.\n",
            "11.bugfix.rst": "An earlier fix.\n",
            "15.misc.1.rst": "One note.\n",
            "15.misc.2.rst": "Another note.\n",
        },
    )
    result = _build(scratch, "0.19.0")
    assert result.returncode == 0, result.stderr

    head = (
        ".. _0.19.0:\n\n0.19.0\n------\n\n"
        "Breaking Changes\n~~~~~~~~~~~~~~~~\n\n"
        f"- Lead ``text``. {_link(12)}\n\n"
        "  - First detail.\n"
        "  - Second detail.\n\n"
        "Changes to Output\n~~~~~~~~~~~~~~~~~\n\n"
        f"- Different numbers. {_link(16)}\n\n"
        "Deprecations\n~~~~~~~~~~~~\n\n"
        f"- Deprecated: ``a``, removed in 0.21.0; use ``b``. {_link(14)}\n\n"
        "New features\n~~~~~~~~~~~~\n\n"
        f"- A feature. {_link(12)}\n"
        "- An unnumbered feature.\n\n"
        "Bug Fixes\n~~~~~~~~~\n\n"
        f"- An earlier fix. {_link(11)}\n"
        f"- A later fix. {_link(13)}\n\n"
        "Miscellaneous\n~~~~~~~~~~~~~\n\n"
    )
    text = _notes(scratch, "0.19.0")
    assert text.startswith(head)
    # Two notes from one PR and type come out in directory-listing order.
    assert sorted(text[len(head) :].splitlines()) == [
        f"- Another note. {_link(15)}",
        f"- One note. {_link(15)}",
    ]
    assert [p.name for p in (scratch / "changes").iterdir()] == ["README.md"]
    # Earlier releases are left alone, and the new one is listed first.
    assert _notes(scratch, "0.18.0") == OLD_RELEASE
    assert (scratch / "docs" / "releases.rst").read_text() == INDEX.replace(
        ".. include::", ".. include:: releases/0.19.0.rst\n\n.. include::", 1
    )

    # publish_release.yaml: the tag is the newest release, and its body is
    # Markdown with the same sections.
    release_body.check_newest("0.19.0")
    body = release_body.section_body("0.19.0")
    assert body.startswith(
        "### Breaking Changes\n\n"
        f"- Lead `text`. ([#12]({URL}/12))\n\n"
        "  - First detail.\n"
        "  - Second detail.\n\n"
        "### Changes to Output\n\n"
    )
    assert "`_" not in body and ".. _" not in body


def test_fragment_written_as_a_bullet(scratch):
    _add_fragments(
        scratch,
        {
            "16.feature.rst": "- A feature.\n  - A detail.\n  - Another.\n",
            "17.feature.rst": "* A second feature\n  on two lines.\n",
            # Sub-bullets flush with the lead's own marker still nest under it.
            "18.feature.rst": "- A third feature.\n- A flush detail.\n",
        },
    )
    result = _build(scratch, "0.19.0")
    assert result.returncode == 0, result.stderr
    assert _notes(scratch, "0.19.0").endswith(
        "New features\n~~~~~~~~~~~~\n\n"
        f"- A feature. {_link(16)}\n\n"
        "  - A detail.\n"
        "  - Another.\n\n"
        "- A second feature\n"
        f"  on two lines. {_link(17)}\n"
        f"- A third feature. {_link(18)}\n\n"
        "  - A flush detail.\n"
    )


def test_link_stays_on_the_lead(scratch):
    _add_fragments(
        scratch,
        {
            # A blank line before the sub-bullets is fine too.
            "17.feature.rst": "A feature.\n\n- A detail.\n\n  More on it.\n- Last.\n",
            # A second paragraph, and a directive, follow the link.
            "18.bugfix.rst": "A fix.\n\nWhy it matters.\n\n.. code-block:: bash\n\n   x\n",
            # The link must not break the "::" that opens a literal block.
            "19.misc.rst": "Run it like this::\n\n    cstar run x\n",
        },
    )
    result = _build(scratch, "0.19.0")
    assert result.returncode == 0, result.stderr
    text = _notes(scratch, "0.19.0")
    assert (
        f"- A feature. {_link(17)}\n\n"
        "  - A detail.\n\n"
        "    More on it.\n"
        "  - Last.\n\n"
    ) in text
    assert (
        f"- A fix. {_link(18)}\n\n"
        "  Why it matters.\n\n"
        "  .. code-block:: bash\n\n"
        "     x\n\n"
    ) in text
    assert text.endswith(
        f"- Run it like this::\n\n      cstar run x\n\n  {_link(19)}\n"
    )


def test_built_notes_are_valid_rst(scratch):
    docutils_core = pytest.importorskip("docutils.core")
    from docutils.writers import pseudoxml

    _add_fragments(
        scratch,
        {
            "12.breaking.rst": "Lead ``text``.\n- First detail.\n- Second detail.\n",
            "13.breaking.rst": "A plain note after one with details.\n",
            "16.feature.rst": "- A feature.\n  - A detail.\n",
            "18.bugfix.rst": "A fix.\n\nWhy it matters.\n\n.. code-block:: bash\n\n   x\n",
            "19.misc.rst": "Run it like this::\n\n    cstar run x\n",
            "20.misc.rst": "A last note.\n",
        },
    )
    result = _build(scratch, "0.19.0")
    assert result.returncode == 0, result.stderr
    tree = docutils_core.publish_string(
        _notes(scratch, "0.19.0"),
        writer=pseudoxml.Writer(),
        # Keep warnings and errors (level 2 and up) in the tree; never raise.
        settings_overrides={"report_level": 2, "halt_level": 5},
    ).decode()
    assert "system_message" not in tree, tree
    lines = tree.splitlines()
    assert lines.count(" " * 8 + '<bullet_list bullet="-">') == 4
    assert lines.count(" " * 16 + '<bullet_list bullet="-">') == 2
    assert sum(ln.startswith(" " * 16 + "<literal_block") for ln in lines) == 2
    assert "block_quote" not in tree and "definition_list" not in tree


def test_build_without_fragments_is_not_empty(scratch):
    result = _build(scratch, "0.19.0")
    assert result.returncode == 0, result.stderr
    assert _notes(scratch, "0.19.0") == (
        ".. _0.19.0:\n\n0.19.0\n------\n\nNo significant changes.\n"
    )
    # release_body.py refuses an empty body, which would stop the release.
    assert release_body.section_body("0.19.0") == "No significant changes.\n"


@pytest.mark.parametrize("name", ["PR.misc.rst", "12.features.rst"])
def test_fragment_names_are_checked(scratch, name):
    _add_fragments(scratch, {name: "A note.\n"})
    result = _build(scratch, "0.19.0")
    assert result.returncode != 0
    assert not (scratch / "docs" / "releases" / "0.19.0.rst").exists()


def test_draft_previews_without_writing(scratch):
    _add_fragments(scratch, {"+note.misc.rst": "A note.\n"})
    result = _run(
        scratch,
        *(sys.executable, "-m", "towncrier", "build", "--draft"),
        *("--version", "Unreleased"),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.endswith(
        ".. _Unreleased:\n\nUnreleased\n----------\n\n"
        "Miscellaneous\n~~~~~~~~~~~~~\n\n- A note.\n\n"
    )
    assert (scratch / "changes" / "+note.misc.rst").exists()
    assert not (scratch / "docs" / "releases" / "Unreleased.rst").exists()
