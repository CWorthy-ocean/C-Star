import os
import re
import shutil
from pathlib import Path

from cstar.orchestration.models import RomsMarblBlueprint, Step
from cstar.orchestration.serialization import deserialize


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


def clear_working_dir(step: Step) -> None:
    """Clear the working directory for a step if CSTAR_CLOBBER_WORKING_DIR is set.

    Parameters
    ----------
    step: the step who's working directory should be cleared

    Returns
    -------
    None
    """
    if os.getenv("CSTAR_CLOBBER_WORKING_DIR") == "1":
        _bp = deserialize(Path(step.blueprint), RomsMarblBlueprint)
        out_path = _bp.runtime_params.output_dir
        print(f"clearing {out_path}")
        shutil.rmtree(out_path / "ROMS", ignore_errors=True)
        shutil.rmtree(out_path / "output", ignore_errors=True)
        shutil.rmtree(out_path / "JOINED_OUTPUT", ignore_errors=True)
