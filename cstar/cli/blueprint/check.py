import typing as t
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from cstar.base.utils import copy_local
from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[str, typer.Argument(help="Path to a blueprint file.")],
) -> bool:
    """Perform content validation on a user-supplied blueprint.

    Returns
    -------
    bool
        `True` if valid
    """
    is_remote = path.startswith("http")
    bp: RomsMarblBlueprint | None = None
    bp_path: Path | None = None

    try:
        with TemporaryDirectory() as tmp_dir:
            bp_path = copy_local(path, Path(tmp_dir)) if is_remote else Path(path)
            bp = deserialize(bp_path, RomsMarblBlueprint)
    except ValueError as ex:
        print(f"The blueprint is invalid: {ex}")
    except FileNotFoundError:
        print(f"Blueprint not found at path: {path}")
    else:
        print("The blueprint is valid")
    finally:
        if is_remote and bp_path and bp_path.exists():
            bp_path.unlink()

    return bp is not None
