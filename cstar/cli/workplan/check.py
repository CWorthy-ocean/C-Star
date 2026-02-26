import typing as t
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from cstar.base.utils import copy_local
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
    is_remote = path.startswith("http")
    wp: Workplan | None = None
    wp_path: Path | None = None

    try:
        with TemporaryDirectory() as tmp_dir:
            wp_path = copy_local(path, Path(tmp_dir)) if is_remote else Path(path)
            wp = deserialize(wp_path, Workplan)
    except ValueError as ex:
        print(f"The workplan is invalid: {ex}")
    except FileNotFoundError:
        print(f"Workplan not found at path: {path}")
    else:
        print("The workplan is valid")

    return wp is not None
