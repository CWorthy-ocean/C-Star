import typing as t
from collections.abc import Iterable
from itertools import permutations
from pathlib import Path
from random import choice

import typer
from rich.console import Console
from rich.table import Table

from cstar.applications.core import ApplicationDefinition, get_application
from cstar.base.utils import slugify
from cstar.orchestration.models import (
    Blueprint,
    Step,
    Workplan,
    WorkplanState,
)
from cstar.orchestration.serialization import deserialize, serialize

if t.TYPE_CHECKING:
    from cstar.entrypoint.runner import BlueprintRunner

    BPResult: t.TypeAlias = tuple[Path, Blueprint]


CMD_NAME = "generate"
CMD_HELP = "Interactively generate a new workplan using pre-existing blueprints."

DEFAULT_BP_EXT: t.Final[str] = ".yml"

app = typer.Typer()
console = Console()


def _locate_blueprints(
    search_dir: Path,
    *,
    extension: str = ".yaml",
    pattern: str = "",
) -> Iterable["BPResult"]:
    """Iterate through the contents of a directory to locate `Blueprints`.

    Parameters
    ----------
    search_dir : Path
        The directory to search for blueprints.
    """
    if not pattern:
        pattern = f"*{extension}"

    print(f"Searching {str(search_dir)!r} for files matching pattern {pattern!r}")
    files = [f for f in search_dir.glob(pattern) if f.is_file()]

    valid_blueprints: list[BPResult] = []

    try:
        for file in files:
            base_bp = deserialize(file, Blueprint)

            app: ApplicationDefinition[Blueprint, BlueprintRunner[Blueprint]] = (
                get_application(base_bp.application)
            )
            bp_type = app.blueprint
            bp = bp_type(**base_bp.model_dump())

            valid_blueprints.append((file, bp))
    except ValueError as ex:
        print(ex)
        msg = "File contains an invalid blueprint"
        raise typer.BadParameter(msg) from ex

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


def _populate_workplan(
    search_dir: Path,
    blueprints: list["BPResult"],
    non_interactive: bool,
) -> Workplan:
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
    if not non_interactive:
        wp_name = input("Enter the name for your workplan: ")
    else:
        path0, _ = blueprints[0]
        wp_name = "generated"

    return Workplan(
        name=wp_name,
        description=f"Auto-generated workplan from {search_dir}",
        steps=steps,
        state=WorkplanState.Draft,
    )


def exclusion_callback(ctx: typer.Context, value: str) -> str:
    """Warn the user if attempting to use the mutually-exclusive options for
    `extension` and `pattern` simultaneously.

    ctx : typer.Context
        The typer context object
    value : str
        The value of the pattern parameter.

    Returns
    -------
    str
    """
    ext_value = ctx.params.get("extension", DEFAULT_BP_EXT)
    if ext_value != DEFAULT_BP_EXT and value:
        msg = f"The following parameters cannot be combined: extension, pattern. You said: {ext_value!r} and {value!r}"
        raise typer.BadParameter(msg)
    return value


@app.command(
    name=CMD_NAME,
    help=CMD_HELP,
)
def generate(
    search_directory: t.Annotated[
        Path,
        typer.Argument(
            help="Specify the path to a directory containing blueprints",
            dir_okay=True,
            file_okay=False,
            exists=True,
            resolve_path=True,
        ),
    ],
    extension: t.Annotated[
        str,
        typer.Option(
            "--ext",
            help="Pass an alternative extension to search for (e.g. yaml, yml)",
        ),
    ] = DEFAULT_BP_EXT,
    pattern: t.Annotated[
        str,
        typer.Option(
            "--pattern",
            help="Specify a glob pattern to use for matching blueprint file names in the search directory",
            callback=exclusion_callback,
        ),
    ] = "",
    non_interactive: t.Annotated[
        bool,
        typer.Option(
            "--non-interactive",
            help="Set this flag to enable non-interactive mode",
        ),
    ] = False,
) -> None:
    """Interactively generate a new workplan using pre-existing blueprints."""
    search_dir = Path(search_directory)
    if not search_dir.exists():
        print(f"No directory was found at: {search_dir!r}")

    results = list(_locate_blueprints(search_dir))
    if not results:
        msg = f"No blueprints found in: {search_dir}"
        raise typer.BadParameter(msg)

    print(f"Found {len(results)} blueprints in {search_dir}")
    for bp_path in results:
        print(f"\t- {bp_path}")

    if len(results) > 1:
        _display_order(results)

        while (
            not non_interactive
            and "y" in input("Do you want to change the order? (yes/no): ").lower()
        ):
            possible = list(permutations(list(range(len(results)))))
            example = list(choice(possible))
            example = [x + 1 for x in example]

            order_input = input(
                f"Specify the new order by providing the current order numbers in a new arrangement, (e.g. {', '.join(str(ex) for ex in example)}): "
            )
            order = order_input.replace(" ", "").split(",")
            provided = {int(x) for x in order}
            required = {x + 1 for x in range(len(results))}

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

        wp = _populate_workplan(search_dir, results, non_interactive)
        plan_stem = slugify(wp.name)
        wp_path = search_dir / "generated" / f"{plan_stem}{extension}"

        serialize(wp_path, wp)
        print(f"Your workplan has been saved to: {wp_path}")
        print(f"Execute your workplan with `cstar workplan run {wp_path}`")
