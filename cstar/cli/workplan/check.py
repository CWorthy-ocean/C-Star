import typing as t
from pathlib import Path

import typer

from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[Path, typer.Argument(help="Path to the workplan")],
) -> bool:
    """Perform content validation on the workplan supplied by the user.

    Returns
    -------
    bool
        `True` if valid
    """
    try:
        _ = deserialize(path, Workplan)
        print("The workplan is valid")
        return True
    except FileNotFoundError:
        print(f"Workplan not found at path: {path}")
    except ValueError as ex:
        print(f"The workplan is invalid: {ex}")

    return False
