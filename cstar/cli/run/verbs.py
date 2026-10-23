import typing as t

NAME: t.Final[str] = "run"
"""Name under which the run-level subcommand tree is attached."""

RUN_LEVEL_VERBS: t.Final[frozenset[str]] = frozenset(
    {"status", "log", "gather", "path", "cd", "ls", "list"}
)
"""The verbs that operate on an existing run.

They are listed under `cstar run`. Their earlier `cstar workplan` spellings stay
registered, hidden from `--help`, and run the same command. Kept apart from the group so that
other modules can name the set without importing the sub-apps `cstar run` attaches.
"""
