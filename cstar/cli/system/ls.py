import typer

from cstar.base.env import load_user_config_for_repair
from cstar.base.exceptions import CstarError
from cstar.system.manager import (
    display_path,
    registered_names,
    select_system,
    system_stack,
)
from cstar.system.spec import SystemFile

app = typer.Typer()

LAYER_CODE = "code"
"""The layer shown for a context registered in code, which has no file."""
NOTE_IN_USE = "in use"
"""The note on the system C-Star uses on this machine."""


def _row(name: str, system_file: SystemFile | None, note: str) -> tuple[str, ...]:
    """Describe one system file for the listing."""
    if system_file is None:
        return (name, LAYER_CODE, "", note)
    return (name, system_file.layer, display_path(system_file.path), note)


def system_rows(in_use: str) -> list[tuple[str, ...]]:
    """List every system C-Star can see: name, layer, path and a note.

    A file that another hides is listed below the one in effect, noted as
    shadowed by it.

    Parameters
    ----------
    in_use : str
        The name of the system C-Star uses, noted as such; empty to mark none.

    Returns
    -------
    list[tuple[str, ...]]
        One row per file, the systems by name.
    """
    layers = system_stack().layers
    rows: list[tuple[str, ...]] = []
    for name in registered_names():
        top = layers.files.get(name)
        rows.append(_row(name, top, NOTE_IN_USE if name == in_use else ""))
        if top is not None:
            by = f"shadowed by {display_path(top.path)}"
            rows += [_row(name, below, by) for below in layers.shadowed.get(name, ())]
    return rows


@app.command(
    name="list", help="List the systems C-Star can see, and where each is read from."
)
def list_systems() -> None:
    """List the systems C-Star can see.

    One line each: the name, the layer its file is in (`user`, `shared` or
    `bundled`; `code` for a system with no file), the file, and a note on the one
    C-Star uses here and on files another hides. Files C-Star refuses follow, with
    the reason, then the directories searched.
    """
    load_user_config_for_repair()
    problem = ""
    try:
        in_use = select_system().context.name
    except (CstarError, OSError) as ex:
        in_use, problem = "", str(ex)

    rows = system_rows(in_use)
    widths = [max(len(row[i]) for row in rows) for i in range(3)]
    for name, layer, path, note in rows:
        line = f"{name:<{widths[0]}}  {layer:<{widths[1]}}  {path:<{widths[2]}}  {note}"
        typer.echo(line.rstrip())

    layers = system_stack().layers
    for refused in layers.refused:
        typer.echo(f"refused: {display_path(refused.path)}: {refused.reason}")
    typer.echo(f"Searched: {', '.join(display_path(d) for d in layers.searched)}")
    if problem:
        typer.echo(f"The system in use could not be determined: {problem}")
