#!/usr/bin/env python3
"""
Finalize the in-development release notes for a tagged release.

Locates the active "unreleased" release notes file in ``docs/releases/``
(identified by its ``.. _unreleased:`` anchor / ``Unreleased`` heading, not
its filename), then:

* renames it to ``docs/releases/<tag>.rst``,
* rewrites its anchor to ``.. _<tag>:`` and its heading to ``<tag>``,
* removes the "this release is currently in development" ``.. note::`` block,
* drops any category subsection that collected no notes this cycle (either
  empty or holding nothing but an ``N/A``-style placeholder),
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

# Category subsections are underlined with "~"; the release title itself uses
# "-", so this deliberately does not match it.
_SUBSECTION_UNDERLINE_RE = re.compile(r"^~{2,}\s*$")
_BULLET_RE = re.compile(r"^[*-]\s+(.+?)\s*$")

# Bullet texts that are placeholders rather than real release notes.  A
# category holding only these is treated as having collected nothing.
_PLACEHOLDER_TEXTS = frozenset(
    {"n/a", "na", "none", "nothing", "no", "no change", "no changes"}
)


def _is_placeholder(text: str) -> bool:
    """
    Return True if *text* is an "empty" placeholder rather than a real note.

    Args:
        text: A bullet's text, with the leading ``-``/``*`` marker removed.
    """
    return text.strip().rstrip(".").strip().lower() in _PLACEHOLDER_TEXTS


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


def _subsection_starts(lines: list[str]) -> list[tuple[int, str]]:
    """
    Return ``[(title_line_index, title), …]`` for every ``~``-underlined
    category subsection, in file order.

    Args:
        lines: Lines of a release notes ``.rst`` file.
    """
    found: list[tuple[int, str]] = []
    for i in range(len(lines) - 1):
        title = lines[i].rstrip()
        underline = lines[i + 1].rstrip()
        if (
            title
            and _SUBSECTION_UNDERLINE_RE.match(underline)
            and len(underline) >= len(title)
        ):
            found.append((i, title))
    return found


def _section_is_empty(content: list[str]) -> bool:
    """
    Return True if *content* holds no real notes.

    Blank lines are ignored and placeholder bullets (``- N/A`` and friends)
    do not count as content.  Any other non-blank line — a real bullet, but
    also hand-written prose or a directive — counts, so we never drop
    something a maintainer wrote by hand.

    Args:
        content: The lines belonging to one category, excluding its title
            and underline lines.
    """
    for line in content:
        stripped = line.strip()
        if not stripped:
            continue
        m = _BULLET_RE.match(stripped)
        if m and _is_placeholder(m.group(1)):
            continue
        return False
    return True


def _splice_out(lines: list[str], drop: set[int]) -> list[str]:
    """
    Return *lines* with the indices in *drop* removed, re-inserting the blank
    line RST requires wherever a deletion left two non-blank lines flush
    against each other, and trimming the result to a single trailing newline.

    Only the junctions a deletion actually created are repaired, so untouched
    parts of the file are reproduced byte for byte.

    Args:
        lines: Lines of the release notes ``.rst`` file.
        drop: Line indices to remove.
    """
    out: list[str] = []
    for i, line in enumerate(lines):
        if i in drop:
            continue
        if i - 1 in drop and out and out[-1].strip() and line.strip():
            out.append("\n")
        out.append(line)

    while len(out) > 1 and not out[-1].strip():
        out.pop()
    if out and not out[-1].endswith("\n"):
        out[-1] += "\n"
    return out


def drop_empty_sections(text: str) -> tuple[str, list[str]]:
    """
    Return *(new_text, dropped_titles)* with every category subsection that
    collected no notes removed.

    Args:
        text: Full contents of the release notes ``.rst`` file being finalized.
    """
    lines = text.splitlines(keepends=True)
    sections = _subsection_starts(lines)

    drop: set[int] = set()
    dropped: list[str] = []
    for n, (start, title) in enumerate(sections):
        end = sections[n + 1][0] if n + 1 < len(sections) else len(lines)
        # Content starts after the title's underline line
        if _section_is_empty(lines[start + 2 : end]):
            dropped.append(title)
            drop.update(range(start, end))

    if not drop:
        return text, []

    return "".join(_splice_out(lines, drop)), dropped


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
    new_text, dropped = drop_empty_sections(new_text)
    for title in dropped:
        print(f"  Dropping empty category: {title}")

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
