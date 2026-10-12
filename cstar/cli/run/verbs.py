import typing as t

NAME: t.Final[str] = "run"
"""Name under which the run-level subcommand tree is attached."""

RUN_LEVEL_VERBS: t.Final[frozenset[str]] = frozenset(
    {"status", "log", "gather", "path", "cd", "ls", "list"}
)
"""The verbs that operate on an existing run.

They live under `cstar run`; `cstar workplan` still accepts them as deprecated
aliases and warns when one is used. Kept apart from the group so that the
workplan group can import it without importing the sub-apps `cstar run` attaches.
"""
