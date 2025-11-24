import os
import re
import shutil
import typing as t
from pathlib import Path


def slugify(source: str) -> str:
    """Convert a source string into a URL-safe slug.

    Parameters
    ----------
    source : str
        The string to be converted.

    Returns
    -------
    str
        The slugified version of the source string.
    """
    if not source:
        raise ValueError

    alphanumeric = re.sub(r"\W", "", source.casefold())
    return re.sub(r"\s+", "-", alphanumeric)


def clear_working_dir(path: Path) -> None:
    """Clear specific paths under the working directory if CSTAR_CLOBBER_WORKING_DIR is set.

    Parameters
    ----------
    path: the working directory to be cleared

    Returns
    -------
    None
    """
    if os.getenv("CSTAR_CLOBBER_WORKING_DIR") == "1":
        print(f"clearing {path}")
        shutil.rmtree(path / "ROMS", ignore_errors=True)
        shutil.rmtree(path / "output", ignore_errors=True)
        shutil.rmtree(path / "JOINED_OUTPUT", ignore_errors=True)


def deep_merge(d1: dict[str, t.Any], d2: dict[str, t.Any]) -> dict[str, t.Any]:
    """Deep merge two dictionaries.

    Parameters
    ----------
    d1 : dict[str, t.Any]
        The first dictionary.
    d2 : dict[str, t.Any]
        The second dictionary.

    Returns
    -------
    dict[str, t.Any]
        The merged dictionaries.
    """
    for k, v in d2.items():
        if isinstance(v, dict):
            d1[k] = deep_merge(d1.get(k, {}), v)
        else:
            d1[k] = v
    return d1
