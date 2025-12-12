import typing as t
from pathlib import Path

import typer

from cstar.orchestration.models import RomsMarblBlueprint
from cstar.orchestration.serialization import deserialize

app = typer.Typer()


@app.command()
def check(
    path: t.Annotated[Path, typer.Argument(help="Path to a blueprint file.")],
) -> None:
    """Perform content validation on a user-supplied blueprint."""
    try:
        _ = deserialize(path, RomsMarblBlueprint)
        print("The blueprint is valid")
    except ValueError as ex:
        print(f"The blueprint is invalid: {ex}")


def main() -> None:
    """Entrypoint for the check-blueprint command."""
    app()


if __name__ == "__main__":
    main()
