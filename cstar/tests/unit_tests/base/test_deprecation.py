"""Tests for `cstar.base.deprecation` and the removal guard over its call sites."""

import ast
import importlib.util
import logging
import textwrap
import warnings
from pathlib import Path

import pytest
from packaging.version import InvalidVersion, Version

import cstar
from cstar.base.deprecation import CstarDeprecationWarning, deprecated

REPO_ROOT = Path(cstar.__file__).parent.parent
"""The checkout: the guard and the release notes are not part of the package."""
RELEASE_NOTES = REPO_ROOT / "docs" / "releases"
"""One `<version>.rst` per release, written when the release is finalized."""
CHECK_SCRIPT = REPO_ROOT / "ci" / "check_deprecations.py"
"""The guard the finalize-release workflow runs."""

if not CHECK_SCRIPT.is_file():
    pytest.skip("ci/ is not part of this installation", allow_module_level=True)

_SPEC = importlib.util.spec_from_file_location("check_deprecations", CHECK_SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
check_deprecations = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(check_deprecations)


def _release_being_cut() -> tuple[int, int, int]:
    """Return the newest release this checkout is, or is finalizing.

    The finalize-release PR adds ``docs/releases/<version>.rst`` and a tagged
    build reports the release as its version. A development version (and the
    ``9999`` fallback when no package metadata exists) says nothing about the
    release.
    """
    releases = []
    for notes in RELEASE_NOTES.glob("*.rst"):
        try:
            releases.append(check_deprecations.parse_release(notes.stem))
        except ValueError:
            continue
    try:
        installed: Version | None = Version(cstar.__version__)
    except InvalidVersion:
        installed = None
    if installed and not (
        installed.is_devrelease or installed.local or installed.major >= 9999
    ):
        releases.append(check_deprecations.parse_release(installed.base_version))
    return max(releases, default=(0, 0, 0))


DEPRECATION_CALLS = check_deprecations.deprecation_calls()


@pytest.mark.parametrize(("site", "removal"), DEPRECATION_CALLS)
def test_deprecation_is_removed_by_its_release(site: str, removal: ast.expr) -> None:
    """Verify no deprecation outlives the release it promises to remove it in.

    The finalize-release workflow refuses that release first
    (``ci/check_deprecations.py``); this fails `main` too once the release is
    finalized or tagged. The fix is the removal in ``docs/contributing.rst``:
    drop the old form, and turn its detection into an error naming the
    migration, never into silence.
    """
    release = _release_being_cut()
    assert not check_deprecations.removal_problem(site, removal, release)


def test_guard_reports_due_and_malformed_removals(tmp_path: Path) -> None:
    """Verify the guard finds the calls, skips tests, and names each problem."""
    pkg = tmp_path / "cstar"
    (pkg / "tests").mkdir(parents=True)
    (pkg / "mod.py").write_text(
        textwrap.dedent(
            """\
            deprecated("a", "b", removal="0.20.0")
            deprecated("a", "b", removal="0.21.0")
            deprecated("a", "b", removal=RELEASE)
            deprecated("a", "b", removal="0.20.1")
            deprecation_message("a", "b", removal="0.19.0")
            warnings.deprecated("not ours")
            """
        )
    )
    (pkg / "tests" / "test_mod.py").write_text(
        'deprecated("a", "b", removal="0.1.0")\n'
    )

    calls = check_deprecations.deprecation_calls(pkg)
    problems = [
        check_deprecations.removal_problem(site, removal, (0, 20, 0))
        for site, removal in calls
    ]

    assert [site for site, _ in calls] == [f"cstar/mod.py:{n}" for n in range(1, 6)]
    assert "due in this release" in problems[0]
    assert problems[1] == ""
    assert "string literal" in problems[2]
    assert "minor releases" in problems[3]
    assert "due in this release" in problems[4]


@pytest.mark.parametrize(
    ("tag", "expected"),
    [("0.20.0", (0, 20, 0)), ("v0.7.0", (0, 7, 0)), ("0.20.0rc1", (0, 20, 0))],
)
def test_guard_parses_release_tags(tag: str, expected: tuple[int, int, int]) -> None:
    """Verify the tags the finalize workflow accepts parse to their release."""
    assert check_deprecations.parse_release(tag) == expected


def test_guard_fails_only_the_release_a_deprecation_is_due_in(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify the script's exit code for a release before and at the removal."""
    pkg = tmp_path / "cstar"
    pkg.mkdir()
    (pkg / "mod.py").write_text('deprecated("a", "b", removal="0.20.0")\n')
    monkeypatch.setattr(check_deprecations, "CSTAR_ROOT", pkg)

    assert check_deprecations.main(["0.19.0"]) == 0
    assert check_deprecations.main(["0.20.0"]) == 1


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


def test_deprecated_logs_before_an_error_filter_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify the log line is written when warnings are turned into errors."""
    with (
        warnings.catch_warnings(),
        caplog.at_level(logging.WARNING),
        pytest.raises(CstarDeprecationWarning),
    ):
        warnings.simplefilter("error", CstarDeprecationWarning)
        deprecated("The 'x' key", "'y'", removal="0.20.0")

    assert len(caplog.records) == 1
