import typing as t

import typer

from cstar.cli.workplan.shared import get_registered_bp
from cstar.orchestration.models import Blueprint
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
    result = validate_serialized_entity(path, Blueprint)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    bp_type = get_registered_bp(result.item.application)
    result = validate_serialized_entity(path, bp_type)

    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    print(f"The blueprint `{result.item.name}` is valid")
