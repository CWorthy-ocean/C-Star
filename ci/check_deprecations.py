#!/usr/bin/env python3
"""
Refuse to finalize a release that a C-Star deprecation is due to be removed in.

Every call of ``cstar.base.deprecation.deprecated`` (or of
``deprecation_message``, for text C-Star prints itself) names the release that
removes the deprecated form as a string literal. This script reads those calls
from the source tree and fails if any is due at or before the given tag, so the
release stops until the removal lands (``docs/contributing.rst``,
"Deprecations and compatibility").

``finalize_release_notes.yaml`` runs it before it opens the finalize PR: that PR
is opened with the default token and so runs no tests. The unit test
``cstar/tests/unit_tests/base/test_deprecation.py`` reuses the scan.

Standard library only, so the workflow needs no install.

Usage:
    python ci/check_deprecations.py <tag>
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

CSTAR_ROOT = Path(__file__).resolve().parent.parent / "cstar"
"""The package source tree that is scanned; its ``tests`` are skipped."""
HELPERS = frozenset({"deprecated", "deprecation_message"})
"""The `cstar.base.deprecation` functions whose `removal` argument is checked."""
HELPERS_MODULE = Path("base") / "deprecation.py"
"""Where the helpers are defined, relative to the package; not a call site."""

_RELEASE_RE = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")


def parse_release(text: str) -> tuple[int, int, int]:
    """
    Return the ``(major, minor, patch)`` a release tag starts with.

    Args:
        text: A tag such as ``0.20.0``, ``v0.20.0`` or ``0.20.0rc1``.

    Raises:
        ValueError: If *text* does not start with ``X.Y.Z``.
    """
    m = _RELEASE_RE.match(text.strip())
    if not m:
        raise ValueError(f"not a release: {text!r}")
    major, minor, patch = (int(part) for part in m.groups())
    return major, minor, patch


def deprecation_calls(root: Path = CSTAR_ROOT) -> list[tuple[str, ast.expr | None]]:
    """
    Return ``(file:line, removal argument)`` for every helper call under *root*.

    Only plain-name calls (``deprecated(...)``) count, which is how C-Star calls
    them; the argument is ``None`` when ``removal`` is missing. The tests and the
    helpers' own module are skipped.
    """
    calls: list[tuple[str, ast.expr | None]] = []
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root)
        if "tests" in relative.parts or relative == HELPERS_MODULE:
            continue
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in HELPERS
            ):
                removal = next(
                    (k.value for k in node.keywords if k.arg == "removal"), None
                )
                site = f"{path.relative_to(root.parent)}:{node.lineno}"
                calls.append((site, removal))
    return calls


def removal_problem(
    site: str, removal: ast.expr | None, release: tuple[int, int, int]
) -> str:
    """
    Return why the call at *site* blocks *release*, or ``""`` when it does not.

    A call blocks every release when its ``removal`` is not a string literal
    naming a minor release (``X.Y.0``), and blocks *release* once ``removal``
    is at or before it.
    """
    if not (isinstance(removal, ast.Constant) and isinstance(removal.value, str)):
        return f"{site}: pass `removal` as a string literal"
    try:
        due = parse_release(removal.value)
    except ValueError:
        return f"{site}: `removal` {removal.value!r} is not a release"
    if due[2] != 0:
        return f"{site}: removals happen in minor releases, not {removal.value}"
    if due <= release:
        return (
            f"{site}: promises removal in {removal.value}, which is due in this "
            "release; remove the deprecated form and make its detection an "
            "error that names the migration"
        )
    return ""


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    release = parse_release(argv[0])
    problems = [
        problem
        for site, removal in deprecation_calls(CSTAR_ROOT)
        if (problem := removal_problem(site, removal, release))
    ]
    for problem in problems:
        print(f"::error::{problem}")
    if problems:
        print(
            f"{len(problems)} deprecation(s) block release {argv[0]}; see "
            "'Deprecations and compatibility' in docs/contributing.rst."
        )
        return 1
    print(f"No deprecation is due in {argv[0]}.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
