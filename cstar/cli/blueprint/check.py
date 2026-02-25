import typing as t
from pathlib import Path

import typer

from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[Path, typer.Argument(help="Path to a blueprint file.")],
) -> bool:
    """Perform content validation on a user-supplied blueprint.

    Returns
    -------
    bool
        `True` if valid
    """
    try:
        _ = deserialize(path, RomsMarblBlueprint)
        print("The blueprint is valid")
        return True
    except FileNotFoundError:
        print(f"Blueprint not found at path: {path}")
    except ValueError as ex:
        print(f"The blueprint is invalid: {ex}")

    return False
