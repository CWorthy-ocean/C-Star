import typing as t

import typer

from cstar.applications.core import get_application
from cstar.orchestration.models import Blueprint, BlueprintMeta
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
    result = validate_serialized_entity(path, BlueprintMeta)
    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    bp_type: type[Blueprint] = get_application(result.item.application).blueprint
    result = validate_serialized_entity(path, bp_type)

    if result.item is None:
        raise typer.BadParameter(result.error_msg)

    print(f"The blueprint `{result.item.name}` is valid")
