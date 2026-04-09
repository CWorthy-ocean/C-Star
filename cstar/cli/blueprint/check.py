import typing as t

import typer

from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import validate_serialized_entity

app = typer.Typer()


@app.command(
    name="check",
    help="Perform content validation on a user-supplied blueprint.",
)
def check(
    path: t.Annotated[str, typer.Argument(help="Path to a blueprint file.")],
) -> None:
    """Perform content validation on a user-supplied blueprint.

    Returns
    -------
    bool
        `True` if valid
    """
    result = validate_serialized_entity(path, RomsMarblBlueprint)
    if result.item is None:
        print(result.error_msg)
        return

    print(f"The blueprint `{result.item.name}` is valid")
