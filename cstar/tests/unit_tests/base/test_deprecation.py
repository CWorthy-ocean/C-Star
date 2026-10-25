"""Tests for `cstar.base.deprecation` and the removal guard over its call sites."""

import ast
import logging
from pathlib import Path

import pytest
from packaging.version import InvalidVersion, Version

import cstar
from cstar.base.deprecation import CstarDeprecationWarning, deprecated

CSTAR_ROOT = Path(cstar.__file__).parent
"""The package source tree the guard reads."""
RELEASE_NOTES = CSTAR_ROOT.parent / "docs" / "releases"
"""One `<version>.rst` per release, written when the release is finalized."""


def _deprecated_calls() -> list[tuple[str, ast.expr | None]]:
    """Return `(file:line, removal argument)` for every `deprecated` call in `cstar/`."""
    calls: list[tuple[str, ast.expr | None]] = []
    for path in sorted(CSTAR_ROOT.rglob("*.py")):
        if "tests" in path.relative_to(CSTAR_ROOT).parts:
            continue
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
            if name != deprecated.__name__:
                continue
            removal = next((k.value for k in node.keywords if k.arg == "removal"), None)
            site = f"{path.relative_to(CSTAR_ROOT.parent)}:{node.lineno}"
            calls.append((site, removal))
    return calls


def _release_being_cut() -> Version:
    """Return the newest release this checkout is, or is finalizing.

    The finalize-release PR adds ``docs/releases/<version>.rst``, so that file
    is how a checkout knows the release before it is tagged; a tagged build
    also reports it as its version. A development version (and the ``9999``
    fallback when no package metadata exists) says nothing about the release.
    """
    versions = []
    for notes in RELEASE_NOTES.glob("*.rst"):
        try:
            versions.append(Version(notes.stem))
        except InvalidVersion:
            continue
    installed = Version(cstar.__version__)
    if not (installed.is_devrelease or installed.local or installed.major >= 9999):
        versions.append(installed)
    return max(versions)


DEPRECATED_CALLS = _deprecated_calls()


def test_deprecated_calls_are_found() -> None:
    """Verify the guard below sees the call sites, so it cannot pass vacuously."""
    assert DEPRECATED_CALLS


@pytest.mark.parametrize(
    ("site", "removal"), DEPRECATED_CALLS, ids=[site for site, _ in DEPRECATED_CALLS]
)
def test_deprecation_is_removed_by_its_release(site: str, removal: ast.expr) -> None:
    """Verify no deprecation outlives the release its warning promises to remove it in.

    Fails on the finalize PR of that release (or on its tag). The fix is the
    removal described in ``docs/contributing.rst``: drop the old form, and turn
    its detection into an error naming the migration, never into silence.
    """
    if not RELEASE_NOTES.is_dir():
        pytest.skip("release notes are not part of this installation")
    assert isinstance(removal, ast.Constant) and isinstance(removal.value, str), (
        f"{site}: pass `removal` to `deprecated` as a string literal"
    )
    due = Version(removal.value)
    assert due.release[2:] == (0,), f"{site}: removals happen in minor releases"
    cutting = _release_being_cut()
    assert due > cutting, (
        f"{site} promises removal in {due}, and {cutting} is being released: "
        "remove the deprecated form and make its detection an error that names "
        "the migration (docs/contributing.rst, Deprecations and compatibility)"
    )


def test_deprecated_warns_and_logs_once(caplog: pytest.LogCaptureFixture) -> None:
    """Verify one warning and one log line per message per process."""
    with (
        pytest.warns(CstarDeprecationWarning) as record,
        caplog.at_level(logging.WARNING),
    ):
        deprecated("The 'x' key", "'y'", removal="0.20.0")
        deprecated("The 'x' key", "'y'", removal="0.20.0")

    expected = (
        "The 'x' key is deprecated and stops working in cstar-ocean 0.20.0; "
        "use 'y' instead."
    )
    assert [str(w.message) for w in record] == [expected]
    assert [r.getMessage() for r in caplog.records] == [expected]
    assert record[0].filename == __file__


def test_deprecated_warns_per_message() -> None:
    """Verify a different deprecation is still announced."""
    with pytest.warns(CstarDeprecationWarning) as record:
        deprecated("The 'x' key", "'y'", removal="0.20.0")
        deprecated("The 'z' key", "'y'", removal="0.20.0")
    assert len(record) == 2
