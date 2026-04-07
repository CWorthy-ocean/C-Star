#!/usr/bin/env python3
"""
Update release notes from merged GitHub pull requests.

Fetches all PRs merged since the last tagged release on
https://github.com/CWorthy-ocean/C-Star and inserts their
categorised notes into the active development release .rst file.

Usage:
    python ci/update_release_notes.py [--dry-run]

Environment:
    GITHUB_TOKEN: Optional personal access token for higher API rate limits
                  (unauthenticated requests are limited to 60/hour).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_OWNER = "CWorthy-ocean"
REPO_NAME = "C-Star"
REPO = f"{REPO_OWNER}/{REPO_NAME}"
GITHUB_API = "https://api.github.com"
RELEASES_DIR = Path(__file__).resolve().parent.parent / "docs" / "releases"
RELEASES_INDEX = Path(__file__).resolve().parent.parent / "docs" / "releases.rst"
UNRELEASED_RST = RELEASES_DIR / "unreleased.rst"
PR_URL_BASE = f"https://github.com/{REPO}/pull"

# PR template section heading  →  RST section heading
SECTION_MAP: dict[str, str] = {
    "Breaking Changes": "Breaking Changes",
    "New Features": "New features",
    "Bug Fixes": "Bug Fixes",
    "Improvements": "Improvements",
    "Miscellaneous": "Miscellaneous",
    "Security Fixes": "Security Fixes",
}

# PR template sections to skip entirely.
# Exact names are matched case-insensitively; any section whose name contains
# "checklist" is also skipped, catching variants like "Review Checklist".
_SKIP_SECTION_EXACT = frozenset({"summary", "code review checklist"})
_SKIP_SECTION_SUBSTRINGS = ("checklist",)


def _should_skip_section(name: str) -> bool:
    lower = name.lower()
    return lower in _SKIP_SECTION_EXACT or any(s in lower for s in _SKIP_SECTION_SUBSTRINGS)

# RST underline characters that mark a section heading
_RST_UNDERLINE_RE = re.compile(r"^[~=\-`#^*+<>]{2,}$")

# Matches a PR link appended by this script: (`#NNN <url>`_)
_LINK_SUFFIX_RE = re.compile(
    r"\s*\(`#\d+\s+<https?://[^>]+>`_\)", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Version sorting
# ---------------------------------------------------------------------------


def _version_key(stem: str) -> tuple:
    """
    Sortable key for release file stems like ``v0.5.0`` or ``v0.0.1-alpha``.

    Pre-release suffixes (``-alpha``, ``-beta``, …) sort *before* the
    corresponding final release.
    """
    s = stem.lstrip("v")
    pre = ""
    if "-" in s:
        s, pre = s.split("-", 1)
    numeric = tuple(int(x) for x in s.split("."))
    numeric += (0,) * (3 - len(numeric))
    # (0, tag) < (1, "") ensures pre-release < release
    pre_key: tuple = (0, pre) if pre else (1, "")
    return (*numeric, *pre_key)


def get_latest_rst() -> tuple[str, Path]:
    """Return *(version_stem, path)* for the highest-versioned .rst file."""
    candidates: list[tuple] = []
    for f in RELEASES_DIR.glob("*.rst"):
        try:
            candidates.append((_version_key(f.stem), f.stem, f))
        except (ValueError, TypeError):
            continue
    if not candidates:
        raise RuntimeError(f"No versioned .rst files found in {RELEASES_DIR}")
    candidates.sort()
    _, stem, path = candidates[-1]
    return stem, path


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------


def make_session() -> requests.Session:
    """Build a ``requests.Session`` with GitHub API headers."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    else:
        print(
            "Warning: GITHUB_TOKEN not set — unauthenticated requests are "
            "limited to 60/hour.",
            file=sys.stderr,
        )
    return session


def get_last_tag_date(session: requests.Session) -> tuple[str, str]:
    """
    Return *(tag_name, ISO-8601 commit date)* for the highest-versioned tag.

    GitHub's tags API returns tags in commit-date order, which can surface
    old pre-release tags ahead of newer stable releases.  We fetch all tags,
    sort them by semantic version (reusing ``_version_key``), and pick the
    highest one instead.
    """
    versioned: list[tuple] = []
    page = 1
    while True:
        resp = session.get(
            f"{GITHUB_API}/repos/{REPO}/tags",
            params={"per_page": 100, "page": page},
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for tag in batch:
            try:
                # Skip pre-release tags (e.g. -alpha, -beta, -rc); only stable
                # releases mark the boundary for collecting PR notes.
                if "-" in tag["name"].lstrip("v"):
                    continue
                versioned.append((_version_key(tag["name"]), tag))
            except (ValueError, TypeError):
                pass  # skip non-semver tags
        if len(batch) < 100:
            break
        page += 1

    if not versioned:
        raise RuntimeError(f"No semver tags found in {REPO}")

    versioned.sort(key=lambda x: x[0])
    latest = versioned[-1][1]
    tag_name: str = latest["name"]

    # Resolve the tag to its commit to get the commit date
    sha = latest["commit"]["sha"]
    resp = session.get(f"{GITHUB_API}/repos/{REPO}/commits/{sha}")
    resp.raise_for_status()
    date: str = resp.json()["commit"]["committer"]["date"]
    return tag_name, date


def get_merged_prs_since(session: requests.Session, since: str) -> list[dict]:
    """
    Return all PRs merged into ``main`` after *since* (ISO-8601 string),
    sorted by PR number ascending.
    """
    results: list[dict] = []
    page = 1
    while True:
        resp = session.get(
            f"{GITHUB_API}/repos/{REPO}/pulls",
            params={
                "state": "closed",
                "sort": "updated",
                "direction": "desc",
                "base": "main",
                "per_page": 100,
                "page": page,
            },
        )
        resp.raise_for_status()
        batch: list[dict] = resp.json()
        if not batch:
            break

        any_newer = False
        for pr in batch:
            merged_at: str | None = pr.get("merged_at")
            if merged_at and merged_at > since:
                results.append(pr)
                any_newer = True

        # If the oldest updated_at on this page is before *since* and we
        # found nothing newer, no further pages will have relevant PRs.
        if not any_newer and batch[-1]["updated_at"] <= since:
            break

        if len(batch) < 100:
            break
        page += 1

    return sorted(results, key=lambda p: p["number"])


# ---------------------------------------------------------------------------
# PR body parsing
# ---------------------------------------------------------------------------


def parse_pr_body(
    body: str | None,
) -> dict[str, list[tuple[str, list[str]]]]:
    """
    Parse a PR description into ``{section_name: [(text, [sub_item, …]), …]}``.

    Each top-level bullet is stored as a ``(text, sub_items)`` tuple where
    *sub_items* contains any indented child bullets directly beneath it.

    - Recognises both ``#`` and ``##`` level Markdown headings as section
      boundaries so that non-standard headers (e.g. ``# Review Checklist``)
      are handled correctly.
    - Skips the Summary and any section whose name contains "checklist".
    - Drops bullets whose stripped text is exactly ``N/A`` (case-insensitive).
    - Drops checklist-style bullets (``- [ ]`` / ``- [x]``) even if they
      appear under a content section, as a belt-and-suspenders guard.
    - Strips inline HTML comments (``<!-- … -->``) before processing.
    """
    if not body:
        return {}

    result: dict[str, list[tuple[str, list[str]]]] = {}
    current: str | None = None
    # Each element: [top_level_text, [sub_item, …]] — mutable for sub-item appends
    items: list[list] = []

    def _flush() -> None:
        if current and not _should_skip_section(current):
            good = [
                (t, s) for t, s in items if t.strip().lower() != "n/a"
            ]
            if good:
                result[current] = [(t, s) for t, s in good]

    for raw in body.splitlines():
        # Measure indentation on the comment-stripped version *before* stripping
        cleaned = re.sub(r"<!--.*?-->", "", raw).rstrip()
        indent = len(cleaned) - len(cleaned.lstrip())
        line = cleaned.strip()

        # Match # or ## (and ###) level headings
        heading = re.match(r"^#{1,3}\s+(.+)$", line)
        if heading:
            _flush()
            current = heading.group(1).strip()
            items = []
            continue

        if current is not None and _should_skip_section(current):
            continue

        # Skip checklist-style bullets regardless of which section they appear in
        if re.match(r"^[-*]\s+\[[ xX]\]", line):
            continue

        bullet = re.match(r"^[-*]\s+(.+)$", line)
        if bullet and current is not None:
            text = bullet.group(1).strip()
            if not text:
                continue
            if indent == 0:
                items.append([text, []])
            elif items:
                # Attach as a sub-bullet of the most recent top-level item
                items[-1][1].append(text)

    _flush()
    return result


# ---------------------------------------------------------------------------
# RST helpers
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """
    Lowercase and collapse whitespace; also strips any appended PR link so
    that duplicate detection is link-agnostic.
    """
    stripped = _LINK_SUFFIX_RE.sub("", text).strip()
    return re.sub(r"\s+", " ", stripped).lower()


def get_known_pr_numbers(rst_text: str) -> set[int]:
    """PR numbers already linked anywhere in *rst_text*."""
    return {int(n) for n in re.findall(r"/pull/(\d+)>`_", rst_text)}


def _find_section_positions(lines: list[str]) -> dict[str, int]:
    """
    Return ``{section_title: title_line_index}`` for every RST section
    found via the ``title\\nunderline`` pattern.
    """
    positions: dict[str, int] = {}
    for i in range(len(lines) - 1):
        title = lines[i].rstrip()
        underline = lines[i + 1].rstrip()
        if (
            title
            and _RST_UNDERLINE_RE.match(underline)
            and len(underline) >= len(title)
            and title not in positions
        ):
            positions[title] = i
    return positions


def _section_bounds(
    positions: dict[str, int],
    title_idx: int,
    total_lines: int,
) -> tuple[int, int]:
    """
    Return *(content_start, content_end)* line indices for the section
    whose title is at *title_idx* (content starts after the underline line).
    """
    content_start = title_idx + 2
    content_end = total_lines
    for idx in positions.values():
        if title_idx < idx < content_end:
            content_end = idx
    return content_start, content_end


def get_section_bullets(lines: list[str], section_title: str) -> set[str]:
    """Normalised set of bullet texts already present in *section_title*."""
    positions = _find_section_positions(lines)
    if section_title not in positions:
        return set()
    start, end = _section_bounds(positions, positions[section_title], len(lines))
    bullets: set[str] = set()
    for line in lines[start:end]:
        m = re.match(r"^-\s+(.+)$", line.rstrip())
        if m:
            bullets.add(_normalize(m.group(1)))
    return bullets


def _bullet_block_to_lines(block: str) -> list[str]:
    """
    Convert a pre-formatted RST bullet block (from ``format_rst_bullet``) into
    a list of newline-terminated lines ready for splicing into the file.

    Simple bullets produce one line; bullets with sub-items produce several
    lines with a required blank line before the sub-list and after it.
    """
    parts = block.split("\n")
    result = [p + "\n" for p in parts]
    if len(parts) > 1:
        # RST requires a trailing blank line after a nested list so the next
        # top-level bullet is not swallowed into the sub-list.
        result.append("\n")
    return result


def insert_bullets_into_section(
    lines: list[str],
    section_title: str,
    new_bullets: list[str],
) -> list[str]:
    """
    Return a new line list with *new_bullets* appended to *section_title*.

    *new_bullets* are pre-formatted RST bullet blocks as returned by
    ``format_rst_bullet`` — they may be multi-line (when sub-items are present).

    If the section contains only a ``- N/A`` placeholder, it is removed first.
    """
    positions = _find_section_positions(lines)
    if section_title not in positions:
        print(f"  WARNING: section '{section_title}' not found in .rst — skipping.")
        return lines

    title_idx = positions[section_title]
    start, end = _section_bounds(positions, title_idx, len(lines))

    # Remove bare "- N/A" placeholders within this section
    cleaned: list[str] = []
    for i, line in enumerate(lines):
        if start <= i < end and re.match(r"^-\s+[Nn]/[Aa]\s*$", line):
            continue
        cleaned.append(line)

    # Recompute positions after potential removals
    positions = _find_section_positions(cleaned)
    if section_title not in positions:
        return cleaned  # Unexpected; bail out safely

    title_idx = positions[section_title]
    start, end = _section_bounds(positions, title_idx, len(cleaned))

    # Find the true end of the last bullet block (including any sub-items and
    # their trailing blank line) so we don't insert mid-block.
    insert_at = start
    i = start
    while i < end:
        if re.match(r"^-\s+", cleaned[i]):
            # Scan forward to find where this bullet block ends: the next
            # top-level bullet or the section boundary, whichever comes first.
            j = i + 1
            while j < end and not re.match(r"^-\s+", cleaned[j]):
                j += 1
            # Strip trailing blank lines from the block so insert_at points to
            # the last content line + 1, not into trailing whitespace.
            block_end = j
            while block_end > i + 1 and not cleaned[block_end - 1].strip():
                block_end -= 1
            insert_at = block_end
            i = j
        else:
            i += 1

    # If no bullets found at all, skip leading blank lines and insert there.
    if insert_at == start:
        while insert_at < end and not cleaned[insert_at].strip():
            insert_at += 1

    # If the line just before our insertion point is an indented sub-bullet,
    # RST needs a blank line to separate it from the new top-level bullet.
    needs_leading_blank = (
        insert_at > 0
        and cleaned[insert_at - 1].strip()
        and cleaned[insert_at - 1].startswith(" ")
    )

    new_lines: list[str] = []
    if needs_leading_blank:
        new_lines.append("\n")
    for block in new_bullets:
        new_lines.extend(_bullet_block_to_lines(block))

    return cleaned[:insert_at] + new_lines + cleaned[insert_at:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def format_rst_bullet(text: str, sub_items: list[str], pr_number: int) -> str:
    """
    Return the complete RST text for one bullet item (no trailing newline).

    The PR link is appended to the top-level text.  Sub-items are formatted as
    an RST nested list, indented by two spaces with a required blank line
    separating them from the parent line::

        - Parent text (`#NNN <url>`_)

          - sub item 1
          - sub item 2
    """
    link = f"`#{pr_number} <{PR_URL_BASE}/{pr_number}>`_"
    top = f"- {text} ({link})"
    if not sub_items:
        return top
    sub_block = "\n".join(f"  - {s}" for s in sub_items)
    return f"{top}\n\n{sub_block}"


_UNRELEASED_TEMPLATE = """\
.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~

- N/A

New features
~~~~~~~~~~~~

- N/A

Security Fixes
~~~~~~~~~~~~~~

- N/A

Bug Fixes
~~~~~~~~~

- N/A

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- N/A
"""


def create_unreleased_rst(dry_run: bool = False) -> Path:
    """
    Create ``docs/releases/unreleased.rst`` from the standard template and
    add a ``.. include::`` directive for it at the top of ``docs/releases.rst``.

    Returns the path to the (possibly just-created) file.
    """
    if not UNRELEASED_RST.exists():
        print(f"Creating {UNRELEASED_RST.relative_to(Path.cwd())}")
        if not dry_run:
            UNRELEASED_RST.write_text(_UNRELEASED_TEMPLATE)

    # Add the include to docs/releases.rst if not already present
    index_text = RELEASES_INDEX.read_text()
    include_line = ".. include:: releases/unreleased.rst\n"
    if include_line not in index_text:
        # Insert after the title block (first blank line following the heading)
        lines = index_text.splitlines(keepends=True)
        insert_at = 0
        for i, line in enumerate(lines):
            # Find the first ``.. include::`` directive and insert before it
            if line.startswith(".. include::"):
                insert_at = i
                break
        new_index = "".join(lines[:insert_at]) + include_line + "\n" + "".join(lines[insert_at:])
        print(f"Adding include to {RELEASES_INDEX.relative_to(Path.cwd())}")
        if not dry_run:
            RELEASES_INDEX.write_text(new_index)

    return UNRELEASED_RST


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update the active release .rst with notes from merged PRs."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without modifying any files.",
    )
    args = parser.parse_args()

    session = make_session()

    # Determine the cutoff: date of the most recent stable tag
    tag_name, tag_date = get_last_tag_date(session)
    print(f"Last tagged release : {tag_name}  (commit date {tag_date})")

    # Locate the active release .rst — the highest-versioned file that is
    # strictly newer than the last tag.  If none exists yet, use (or create)
    # unreleased.rst.
    version_stem, candidate_path = get_latest_rst()
    if _version_key(version_stem) > _version_key(tag_name):
        rst_path = candidate_path
    else:
        rst_path = create_unreleased_rst(dry_run=args.dry_run)

    print(f"Target release file : {rst_path.relative_to(Path.cwd())}")

    # Fetch all PRs merged after that date
    print("Fetching merged PRs…")
    prs = get_merged_prs_since(session, tag_date)
    print(f"Found {len(prs)} PR(s) merged since {tag_name}.\n")

    if not prs:
        print("Nothing to do.")
        return

    rst_text = rst_path.read_text() if rst_path.exists() else _UNRELEASED_TEMPLATE
    lines = rst_text.splitlines(keepends=True)
    known_prs = get_known_pr_numbers(rst_text)
    total_added = 0

    for pr in prs:
        pr_num: int = pr["number"]
        pr_title: str = pr["title"]

        if pr_num in known_prs:
            print(f"  #{pr_num:5d} already in release notes — skipping.")
            continue

        body_sections = parse_pr_body(pr.get("body"))
        if not body_sections:
            print(f"  #{pr_num:5d} '{pr_title}' — no categorised notes found.")
            continue

        pr_added = 0
        for pr_section, rst_section in SECTION_MAP.items():
            items = body_sections.get(pr_section, [])
            if not items:
                continue

            existing = get_section_bullets(lines, rst_section)
            to_add: list[str] = []
            for item_text, sub_items in items:
                if _normalize(item_text) in existing:
                    print(
                        f"  #{pr_num:5d} [{pr_section}] duplicate — '{item_text[:70]}'"
                    )
                    continue
                to_add.append(format_rst_bullet(item_text, sub_items, pr_num))

            if to_add:
                lines = insert_bullets_into_section(lines, rst_section, to_add)
                pr_added += len(to_add)
                for b in to_add:
                    print(f"  #{pr_num:5d} + [{rst_section}] {b[:90]}")

        if pr_added:
            total_added += pr_added
            known_prs.add(pr_num)
        elif not any(body_sections.get(s) for s in SECTION_MAP):
            print(f"  #{pr_num:5d} '{pr_title}' — no relevant section content.")

    if total_added == 0:
        print("\nNo new release notes to add.")
        return

    new_text = "".join(lines)
    if args.dry_run:
        print(f"\n--- dry-run: {total_added} bullet(s) would be added to {rst_path.name} ---")
        print(new_text)
    else:
        rst_path.write_text(new_text)
        print(f"\nAdded {total_added} bullet(s) to {rst_path}.")


if __name__ == "__main__":
    main()
