import os
import re
import shutil
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

    return re.sub(r"\s+", "-", source.casefold())


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
