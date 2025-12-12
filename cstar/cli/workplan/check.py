import typing as t
from pathlib import Path

import typer

from cstar.orchestration.models import Workplan
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[Path, typer.Argument(help="Path to the workplan")],
) -> None:
    """Perform content validation on the workplan supplied by the user."""
    try:
        _ = deserialize(path, Workplan)
        print("The workplan is valid")
    except ValueError as ex:
        print(f"The workplan is invalid: {ex}")


def main() -> None:
    """Entrypoint for check-workplan command."""
    app()


if __name__ == "__main__":
    main()
