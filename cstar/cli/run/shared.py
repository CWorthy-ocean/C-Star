import os
import typing as t
from pathlib import Path

import typer

WORKPLAN_FILE_SUFFIXES: t.Final[frozenset[str]] = frozenset({".yaml", ".yml"})
"""Suffixes that identify a workplan file, which `cstar workplan check` validates."""


def refuse_workplan_file(_context: typer.Context, target: str) -> str:
    """Refuse a workplan file: a run command acts on a run, not a workplan.

    Parameters
    ----------
    _context : typer.Context
        The typer context (unused).
    target : str
        The user-supplied target.

    Returns
    -------
    str

    Raises
    ------
    typer.BadParameter
        Raised when the target names a YAML file.
    """
    if Path(target).suffix.casefold() in WORKPLAN_FILE_SUFFIXES:
        msg = (
            f"{target!r} is a workplan file, not a run. "
            f"To validate it, use `cstar workplan check {target}`."
        )
        raise typer.BadParameter(msg)

    return target


def is_path_like(target: str) -> bool:
    """Whether a target is spelled as a path rather than as a run-id.

    A run-id is a slug, so it never holds a path separator or starts with a dot
    or a tilde.

    Parameters
    ----------
    target : str
        The user-supplied target.

    Returns
    -------
    bool
    """
    return os.sep in target or target.startswith((".", "~"))
