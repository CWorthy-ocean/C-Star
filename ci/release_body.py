#!/usr/bin/env python3
"""
Print a Markdown release-notes body for a finalized release *tag*.

Reads ``docs/releases/<tag>.rst`` — the per-version file produced by
``finalize_release_notes.py`` — and converts it to GitHub-flavored Markdown for
``gh release create --notes-file``. The conversion is deliberately narrow,
handling only what these release-note files actually contain:

* ``.. _anchor:`` target lines are dropped,
* the release title and its underline are dropped (GitHub shows the tag),
* every other underlined heading (category, e.g. "New features") becomes a
  ``### `` Markdown heading,
* RST inline links (the `text <url>`_ form) become [text](url),
* RST inline literals (the double-backtick form) become single-backtick spans.

Everything else (bullets, prose, blank lines) passes through unchanged. The
``.. note::`` in-development banner is already removed by ``finalize`` before we
run, so no directive handling is needed.

With ``--check-only`` nothing is printed; instead ``docs/releases/<tag>.rst`` is
required to exist and to be the newest ``.. include::`` in ``docs/releases.rst``,
exiting non-zero on mismatch. This guards the publish workflow against a
retitled PR or a finalize step that never ran.

Release tags are stored without a leading ``v`` (e.g. ``0.7.0``); a ``v``-prefix
on the argument is stripped before use.

Usage:
    python ci/release_body.py <tag> [--check-only]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

RELEASES_DIR = Path(__file__).resolve().parent.parent / "docs" / "releases"
RELEASES_INDEX = Path(__file__).resolve().parent.parent / "docs" / "releases.rst"

# RST section underlines are a run of one repeated punctuation char. Bullets
# ("- foo") contain a space and so never match. '.' and ':' are deliberately
# excluded so a bare ".." (comment) or "::" (literal-block marker) is not
# mistaken for an underline.
_UNDERLINE_CHARS = frozenset("-=~^\"+*#`'")
# Bare anchor with no target on the same line, e.g. ".. _0.7.0:" — dropped.
_ANCHOR_RE = re.compile(r"^\.\.\s+_\S+:\s*$")
# Named hyperlink-target definition, e.g. ".. _v0.2.0 commit history: https://..."
# (name may contain spaces). Captured into a name->url map, then the line dropped.
_TARGET_RE = re.compile(r"^\.\.\s+_(.+?):\s+(\S.*?)\s*$")
_INCLUDE_RE = re.compile(r"^\.\.\s+include::\s*releases/(\S+?\.rst)\s*$")
# `text <url>`_  ->  [text](url). The URL class excludes backticks and
# whitespace (a real RST link target has neither) so the match cannot bridge
# out of an inline literal that happens to contain a '<' — e.g. ``compilers<2``.
_LINK_RE = re.compile(r"`([^`<]+?)\s*<([^>`\s]+)>`_")
# `name`_  ->  named reference resolved against the target map (or bare text).
# The trailing `_` must sit at a word boundary (whitespace / sentence
# punctuation / end of line) — this is what distinguishes a real named
# reference from an inline literal that merely starts with an underscore
# (e.g. `_private`, whose backtick+underscore is the OPENING, not a closer).
_NAMED_REF_RE = re.compile(r"`([^`<]+?)`_(?=\s|[.,;:)\]]|$)")
# ``code``  ->  `code`
_CODE_RE = re.compile(r"``([^`]+?)``")


def _ref_key(name: str) -> str:
    """Normalize an RST reference name (case- and whitespace-insensitive)."""
    return " ".join(name.split()).lower()


def normalize_tag(tag: str) -> str:
    """Strip a leading ``v``/``V`` from *tag* if present (``v0.7.0`` -> ``0.7.0``)."""
    return tag[1:] if re.match(r"^[vV]\d", tag) else tag


def _is_underline(line: str) -> bool:
    """Return True if *line* is an RST section underline (all one punctuation char)."""
    s = line.strip()
    return len(s) >= 2 and all(c == s[0] for c in s) and s[0] in _UNDERLINE_CHARS


def _inline(text: str, targets: dict[str, str]) -> str:
    """
    Convert RST inline roles to their Markdown equivalents.

    Handles inline-URL links, named references (resolved against *targets*, or
    reduced to bare text if the name is unknown), and double-backtick literals.
    """
    text = _LINK_RE.sub(r"[\1](\2)", text)

    def _named(m: re.Match[str]) -> str:
        name = m.group(1)
        url = targets.get(_ref_key(name))
        return f"[{name}]({url})" if url else name

    text = _NAMED_REF_RE.sub(_named, text)
    text = _CODE_RE.sub(r"`\1`", text)
    return text


def _collect_targets(lines: list[str]) -> dict[str, str]:
    """Return a ``{normalized name: url}`` map of named hyperlink-target definitions."""
    targets: dict[str, str] = {}
    for line in lines:
        if _ANCHOR_RE.match(line):
            continue  # bare anchor, no URL
        m = _TARGET_RE.match(line)
        if m:
            targets[_ref_key(m.group(1))] = m.group(2)
    return targets


def rst_to_markdown(rst: str) -> str:
    """
    Convert a finalized per-version release-note RST document to Markdown.

    The first underlined heading is treated as the release title and dropped;
    every subsequent underlined heading becomes a ``### `` category heading.
    Anchor and target-definition lines are dropped (their URLs are inlined at
    the reference site), as are stray transition markers.
    """
    lines = rst.splitlines()
    targets = _collect_targets(lines)
    out: list[str] = []
    seen_title = False
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        # Drop anchors and named target definitions (folded into refs above).
        if _ANCHOR_RE.match(line) or _TARGET_RE.match(line):
            i += 1
            continue
        # A heading is a non-empty, non-underline line immediately followed by
        # an underline. The first one is the release title (dropped); the rest
        # are categories.
        if (
            line.strip()
            and not _is_underline(line)
            and i + 1 < n
            and _is_underline(lines[i + 1])
        ):
            if seen_title:
                out.append(f"### {_inline(line.strip(), targets)}")
            else:
                seen_title = True
            i += 2
            continue
        # An underline reaching here belongs to no heading — a stray transition
        # marker (e.g. a lone '---'); drop it so it does not render as an <hr>.
        if _is_underline(line):
            i += 1
            continue
        out.append(_inline(line, targets))
        i += 1

    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text).strip("\n")
    return text + "\n"


def _release_file(tag: str) -> Path | None:
    """
    Return the ``docs/releases/`` file for *tag*, or ``None`` if absent.

    The naming convention dropped the leading ``v`` at 0.8.0, but older files
    keep it (``v0.7.0.rst``), so both spellings are tried.
    """
    tag = normalize_tag(tag)
    for name in (f"{tag}.rst", f"v{tag}.rst"):
        path = RELEASES_DIR / name
        if path.exists():
            return path
    return None


def section_body(tag: str) -> str:
    """
    Return the Markdown body for the release file matching *tag*.

    Raises ``SystemExit`` if that file does not exist or converts to an empty
    body.
    """
    path = _release_file(tag)
    if path is None:
        sys.exit(f"Release notes file not found for tag '{normalize_tag(tag)}'.")
    body = rst_to_markdown(path.read_text())
    if not body.strip():
        sys.exit(f"Release notes file {path} converts to an empty body.")
    return body


def check_newest(tag: str) -> None:
    """
    Validate that *tag*'s file exists and is the newest include in the index.

    Raises ``SystemExit`` if the file is missing, the index has no includes, or
    the newest include is some other version. Include filenames are compared
    with a leading ``v`` stripped from both sides.
    """
    tag = normalize_tag(tag)
    if _release_file(tag) is None:
        sys.exit(f"Release notes file not found for tag '{tag}'.")
    includes: list[str] = []
    for ln in RELEASES_INDEX.read_text().splitlines():
        m = _INCLUDE_RE.match(ln)
        if m:
            includes.append(m.group(1))
    if not includes:
        sys.exit(f"No '.. include::' entries found in {RELEASES_INDEX}")
    newest = normalize_tag(Path(includes[0]).stem)
    if newest != tag:
        sys.exit(
            f"Tag '{tag}' is not the newest release in "
            f"{RELEASES_INDEX.name} (top include is '{includes[0]}')."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit release-notes body for a tag.")
    parser.add_argument("tag", help="Release tag, e.g. 0.7.0")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate the tag is the newest release; print nothing.",
    )
    args = parser.parse_args()

    if args.check_only:
        check_newest(args.tag)
        return
    sys.stdout.write(section_body(args.tag))


if __name__ == "__main__":
    main()
