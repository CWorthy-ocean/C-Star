import typing as t

import typer

from cstar.execution.file_system import local_copy
from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[str, typer.Argument(help="Path to the workplan")],
) -> bool:
    """Perform content validation on the workplan supplied by the user.

    Returns
    -------
    bool
        `True` if valid
    """
    wp: Workplan | None = None

    try:
        with local_copy(path) as wp_path:
            wp = deserialize(wp_path, Workplan)
    except ValueError as ex:
        print(f"The workplan is invalid: {ex}")
    except FileNotFoundError:
        print(f"Workplan not found at path: {path}")
    else:
        print("The workplan is valid")

    return wp is not None
