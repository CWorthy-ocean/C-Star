import typing as t

import typer

from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import validate_serialized_entity

app = typer.Typer()


@app.command(
    name="check",
    help="Perform content validation on a user-supplied workplan.",
)
def check(
    path: t.Annotated[str, typer.Argument(help="Path to the workplan")],
) -> None:
    """Perform content validation on the workplan supplied by the user.

    Returns
    -------
    bool
        `True` if valid
    """
    result = validate_serialized_entity(path, Workplan)
    if result.item:
        print(f"The workplan `{result.item.name}` is valid")
        raise typer.Exit(0)

    # stash original error in case it's a live workplan
    error = result.error_msg

    result = validate_serialized_entity(path, LiveWorkplan)
    if result.item:
        print(f"The workplan `{result.item.name}` is valid")
        raise typer.Exit(0)

    raise typer.BadParameter(error)
