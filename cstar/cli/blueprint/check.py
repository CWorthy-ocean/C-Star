import typing as t

import typer

from cstar.execution.file_system import local_copy
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
    bp: RomsMarblBlueprint | None = None

    try:
        with local_copy(path) as bp_path:
            bp = deserialize(bp_path, RomsMarblBlueprint)
    except ValueError as ex:
        print(f"The blueprint is invalid: {ex}")
    except FileNotFoundError:
        print(f"Blueprint not found at path: {path}")
    else:
        print("The blueprint is valid")

    return bp is not None
