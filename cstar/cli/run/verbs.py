import typing as t

NAME: t.Final[str] = "run"
"""Name under which the run-level subcommand tree is attached."""

RUN_LEVEL_VERBS: t.Final[frozenset[str]] = frozenset(
    {"status", "log", "gather", "path", "cd", "ls", "list"}
)
"""The verbs that operate on an existing run.

They are attached under `cstar run` and, as before, under `cstar workplan`; the
two spellings share one sub-app per verb. Kept apart from the group so that
other modules can name the set without importing the sub-apps `cstar run` attaches.
"""
