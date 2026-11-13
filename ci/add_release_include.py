#!/usr/bin/env python3
"""
List a new release's notes file first in the release-notes index.

Each release has its own file, ``docs/releases/<tag>.rst``, and
``docs/releases.rst`` includes them newest first. ``towncrier build`` writes
the file for a new release but does not know about the index, so
``finalize_release_notes.yaml`` runs this script first: it adds
``.. include:: releases/<tag>.rst`` above the other includes, which is where
``release_body.py --check-only`` expects the release being published.

It refuses a tag that already has a notes file or an include, because
``towncrier build`` would overwrite that file without asking.

Release tags are written without a leading ``v`` (``0.19.0``); one given with
a ``v`` is stripped.

Usage:
    python ci/add_release_include.py <tag>
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

RELEASES_DIR = Path(__file__).resolve().parent.parent / "docs" / "releases"
RELEASES_INDEX = Path(__file__).resolve().parent.parent / "docs" / "releases.rst"

_INCLUDE_RE = re.compile(r"^\.\.\s+include::\s*releases/(\S+?)\.rst\s*$", re.MULTILINE)


def normalize_tag(tag: str) -> str:
    """Strip a leading ``v``/``V`` from *tag* if present (``v0.7.0`` -> ``0.7.0``)."""
    return tag[1:] if re.match(r"^[vV]\d", tag) else tag


def add_include(index_text: str, tag: str) -> str:
    """
    Return *index_text* with the include for *tag* above the other includes.

    Raises ``SystemExit`` if the index has no include to put it above, or
    already includes *tag* (with or without a leading ``v``).

    Args:
        index_text: Full contents of ``docs/releases.rst``.
        tag: Bare version tag of the new release (e.g. ``"0.19.0"``).
    """
    first = _INCLUDE_RE.search(index_text)
    if first is None:
        sys.exit(f"No '.. include::' entries found in {RELEASES_INDEX.name}.")
    included = {normalize_tag(stem) for stem in _INCLUDE_RE.findall(index_text)}
    if tag in included:
        sys.exit(f"{RELEASES_INDEX.name} already includes release '{tag}'.")
    line = f".. include:: releases/{tag}.rst\n\n"
    return index_text[: first.start()] + line + index_text[first.start() :]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("tag", help="Release tag, e.g. 0.19.0")
    args = parser.parse_args()
    tag = normalize_tag(args.tag)

    for name in (f"{tag}.rst", f"v{tag}.rst"):
        if (RELEASES_DIR / name).exists():
            sys.exit(
                f"docs/releases/{name} already exists: release '{tag}' has its "
                "notes. Check the tag."
            )
    RELEASES_INDEX.write_text(add_include(RELEASES_INDEX.read_text(), tag))
    print(f"Added releases/{tag}.rst to {RELEASES_INDEX.name}.")


if __name__ == "__main__":
    main()
