#!/usr/bin/env python3
"""
Finalize the in-development release notes for a tagged release.

Locates the active "unreleased" release notes file in ``docs/releases/``
(identified by its ``.. _unreleased:`` anchor / ``Unreleased`` heading, not
its filename), then:

* renames it to ``docs/releases/<tag>.rst``,
* rewrites its anchor to ``.. _<tag>:`` and its heading to ``<tag>``,
* removes the "this release is currently in development" ``.. note::`` block,
* updates the matching ``.. include::`` line in ``docs/releases.rst``.

Release tags are written *without* a leading ``v`` (e.g. ``0.7.0``, not
``v0.7.0``) — this is the standardized convention going forward across all
CWorthy repos. If invoked with a ``v``-prefixed tag it is stripped
automatically.

Usage:
    python ci/finalize_release_notes.py <tag> [--dry-run]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

RELEASES_DIR = Path(__file__).resolve().parent.parent / "docs" / "releases"
RELEASES_INDEX = Path(__file__).resolve().parent.parent / "docs" / "releases.rst"

_ANCHOR_RE = re.compile(r"^\.\. _unreleased:[ \t]*$", re.IGNORECASE | re.MULTILINE)
_HEADING_RE = re.compile(r"^Unreleased(\n-{2,}\n)", re.MULTILINE)
_NOTE_BLOCK_RE = re.compile(r"^\.\. note::\n(?:[ \t]+\S.*\n)*", re.MULTILINE)


def normalize_tag(tag: str) -> str:
    """
    Strip a leading ``v``/``V`` from *tag*, if present.

    Args:
        tag: Raw tag as passed on the command line (e.g. ``"v0.7.0"`` or ``"0.7.0"``).
    """
    if re.match(r"^[vV]\d", tag):
        return tag[1:]
    return tag


def find_active_file() -> Path | None:
    """
    Return the ``.rst`` file in ``RELEASES_DIR`` that is still marked as
    unreleased (via its ``.. _unreleased:`` anchor or ``Unreleased`` heading),
    or ``None`` if no such file exists.
    """
    for f in sorted(RELEASES_DIR.glob("*.rst")):
        text = f.read_text()
        if _ANCHOR_RE.search(text) or _HEADING_RE.search(text):
            return f
    return None


def finalize_text(text: str, tag: str) -> str:
    """
    Return *text* with the unreleased anchor/heading rewritten to *tag* and
    the in-development ``.. note::`` block removed.

    Args:
        text: Full contents of the active release notes ``.rst`` file.
        tag: Bare version tag to finalize to (e.g. ``"0.7.0"``).
    """
    text = _ANCHOR_RE.sub(f".. _{tag}:", text)
    text = _HEADING_RE.sub(lambda m: f"{tag}{m.group(1)}", text)
    text = _NOTE_BLOCK_RE.sub("", text)
    return text


def update_index(index_text: str, old_stem: str, tag: str) -> tuple[str, bool]:
    """
    Return *(new_index_text, changed)* with the ``.. include::`` line for
    *old_stem* repointed to *tag*.

    Args:
        index_text: Full contents of ``docs/releases.rst``.
        old_stem: Filename stem of the release file before finalizing
            (e.g. ``"unreleased"``).
        tag: Bare version tag being finalized to (e.g. ``"0.7.0"``).
    """
    old_line = f".. include:: releases/{old_stem}.rst"
    new_line = f".. include:: releases/{tag}.rst"
    if old_line not in index_text:
        print(f"  WARNING: '{old_line}' not found in {RELEASES_INDEX.name} — skipping.")
        return index_text, False
    if old_line == new_line:
        return index_text, False
    return index_text.replace(old_line, new_line), True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Finalize the active unreleased release notes for a tagged release."
    )
    parser.add_argument("tag", help="Release tag to finalize to, e.g. 0.7.0")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without modifying any files.",
    )
    args = parser.parse_args()
    tag = normalize_tag(args.tag)

    active_file = find_active_file()
    if active_file is None:
        print("No unreleased release notes file found — nothing to finalize.")
        return

    print(f"Finalizing {active_file.relative_to(Path.cwd())} -> {tag}")
    old_stem = active_file.stem
    new_path = RELEASES_DIR / f"{tag}.rst"

    new_text = finalize_text(active_file.read_text(), tag)
    index_text, index_changed = update_index(RELEASES_INDEX.read_text(), old_stem, tag)

    if args.dry_run:
        print(f"--- dry-run: would write {new_path} ---")
        print(new_text)
        if index_changed:
            index_rel = RELEASES_INDEX.relative_to(Path.cwd())
            print(f"--- dry-run: would update {index_rel} ---")
        return

    new_path.write_text(new_text)
    if active_file != new_path:
        active_file.unlink()
        print(f"Renamed {old_stem}.rst -> {new_path.name}")

    if index_changed:
        RELEASES_INDEX.write_text(index_text)
        print(f"Updated include in {RELEASES_INDEX.relative_to(Path.cwd())}")

    print(f"\nFinalized release notes for {tag}.")


if __name__ == "__main__":
    main()
