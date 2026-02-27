import typing as t
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from cstar.base.utils import slugify
from cstar.orchestration.models import RomsMarblBlueprint, Step, Workplan, WorkplanState
from cstar.orchestration.serialization import deserialize, serialize

app = typer.Typer()
console = Console()

if t.TYPE_CHECKING:
    BPResult: t.TypeAlias = tuple[Path, RomsMarblBlueprint]


def _locate_blueprints(search_dir: Path) -> t.Iterable["BPResult"]:
    """Iterate through the contents of a directory to locate `Blueprints`.

    Parameters
    ----------
    search_dir : Path
        The directory to search for blueprints.
    """
    files = search_dir.glob("*.yaml")
    valid_blueprints: list[BPResult] = []

    for file in files:
        try:
            bp = deserialize(file, RomsMarblBlueprint)
            valid_blueprints.append((file, bp))
        except Exception:
            print(f"File {file} is not a valid RomsMarblBlueprint")

    return valid_blueprints


def _display_order(blueprints: list["BPResult"]) -> None:
    """Display a table of the current execution order for the blueprints.

    Parameters
    ----------
    blueprints : BPResult
        The blueprint results to display.
    """
    print("Blueprints will be executed in the folllowing order:")
    table = Table("Order", "Name", "Path")

    for i, (path, bp) in enumerate(blueprints):
        table.add_row(str(i + 1), bp.name, path.as_posix())

    console.print(table)


def _populate_workplan(search_dir: Path, blueprints: list["BPResult"]) -> Workplan:
    """Populate a new workplan.

    Parameters
    ----------
    search_dir : Path
        The directory to persist the workplan in.
    blueprints : list[BPResult]
        The blueprints to be executed in the plan.

    Returns
    -------
    Workplan
        The newly generated workplan.
    """
    steps = [
        Step(name=bp.name, application=bp.application, blueprint=p)
        for p, bp in blueprints
    ]
    wp_name = input("Enter the name of your workplan: ")

    return Workplan(
        name=wp_name,
        description=f"Auto-generated workplan from {search_dir}",
        steps=steps,
        state=WorkplanState.Draft,
    )


@app.command()
def generate(
    search_directory: t.Annotated[
        str,
        typer.Argument(help="Specify the path to a directory containing blueprints"),
    ],
) -> None:
    """Interactively generate a new workplan using pre-existing blueprints."""
    search_dir = Path(search_directory)
    if not search_dir.exists():
        print(f"No directory was found at: {search_dir!r}")

    results = list(_locate_blueprints(search_dir))
    if results:
        print(f"Found {len(results)} blueprints in {search_dir}")
        _display_order(results)

        while "y" in input("Do you want to change the order? (yes/no): ").lower():
            from itertools import permutations
            from random import choice

            possible = list(permutations(list(range(len(results)))))
            example = list(choice(possible))
            example = [x + 1 for x in example]

            order_input = input(
                f"Specify the new order by providing the current order numbers in a new arrangement, (e.g. {', '.join(str(ex) for ex in example)}): "
            )
            order = order_input.replace(" ", "").split(",")
            provided = set(int(x) for x in order)
            required = set(x + 1 for x in range(len(results)))

            diff = required.difference(provided)

            if diff:
                print(
                    f"You must specify all values. Please include the following items, too: {', '.join(str(x) for x in diff)}"
                )
                continue

            if diff:
                print(f"You can only specify values in the range 1 to {len(results)}")

            indices = [int(x) for x in order_input.replace(" ", "").split(",")]
            results = [results[x - 1] for x in indices]

            _display_order(results)

        wp = _populate_workplan(search_dir, results)
        plan_stem = slugify(wp.name)
        wp_path = search_dir / f"{plan_stem}.yaml"

        serialize(wp_path, wp)
        print(f"Your workplan has been saved to: {wp_path}")
        print(f"Execute your workplan with `cstar workplan run {wp_path}`")

    else:
        print(f"No blueprints found in: {search_dir}")
