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
    """Return *(tag_name, ISO-8601 commit date)* for the most recent tag."""
    resp = session.get(
        f"{GITHUB_API}/repos/{REPO}/tags",
        params={"per_page": 10},
    )
    resp.raise_for_status()
    tags = resp.json()
    if not tags:
        raise RuntimeError(f"No tags found in {REPO}")

    latest = tags[0]
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


def parse_pr_body(body: str | None) -> dict[str, list[str]]:
    """
    Parse a PR description into ``{section_name: [bullet_text, …]}``.

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

    result: dict[str, list[str]] = {}
    current: str | None = None
    items: list[str] = []

    def _flush() -> None:
        if current and not _should_skip_section(current):
            good = [i for i in items if i.strip().lower() != "n/a"]
            if good:
                result[current] = good

    for raw in body.splitlines():
        line = re.sub(r"<!--.*?-->", "", raw).strip()

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
            if text:
                items.append(text)

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


def insert_bullets_into_section(
    lines: list[str],
    section_title: str,
    new_bullets: list[str],
) -> list[str]:
    """
    Return a new line list with *new_bullets* appended to *section_title*.

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

    # Insert after the last existing bullet; if none, after leading blank lines
    insert_at = start
    for i in range(start, end):
        if re.match(r"^-\s+", cleaned[i]):
            insert_at = i + 1
    if insert_at == start:
        while insert_at < end and not cleaned[insert_at].strip():
            insert_at += 1

    new_lines = [f"- {b}\n" for b in new_bullets]
    return cleaned[:insert_at] + new_lines + cleaned[insert_at:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_bullet(text: str, pr_number: int) -> str:
    """Append a RST inline hyperlink to *text* referencing *pr_number*."""
    link = f"`#{pr_number} <{PR_URL_BASE}/{pr_number}>`_"
    return f"{text} ({link})"


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

    # Locate the active (highest-version) release .rst
    version_stem, rst_path = get_latest_rst()
    print(f"Target release file : {rst_path.relative_to(Path.cwd())} ({version_stem})")

    session = make_session()

    # Determine the cutoff: date of the most recent git tag
    tag_name, tag_date = get_last_tag_date(session)
    print(f"Last tagged release : {tag_name}  (commit date {tag_date})")

    # Fetch all PRs merged after that date
    print("Fetching merged PRs…")
    prs = get_merged_prs_since(session, tag_date)
    print(f"Found {len(prs)} PR(s) merged since {tag_name}.\n")

    if not prs:
        print("Nothing to do.")
        return

    rst_text = rst_path.read_text()
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
            for item in items:
                if _normalize(item) in existing:
                    print(
                        f"  #{pr_num:5d} [{pr_section}] duplicate — '{item[:70]}'"
                    )
                    continue
                to_add.append(build_bullet(item, pr_num))

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
