import typer

from cstar.base.exceptions import CstarError
from cstar.system.manager import registered_context, system_file_of

app = typer.Typer()


@app.command(name="show", help="Print the system file C-Star uses for a system.")
def show(
    name: str = typer.Argument(help="The system's name, as `cstar system list` shows."),
) -> None:
    """Print the file in effect for a system, exactly as it is on disk.

    Nothing else goes to standard output, so the file can be saved and edited:
    ``cstar system show anvil > mycluster.yaml``.

    Parameters
    ----------
    name : str
        The system's name.

    Raises
    ------
    typer.Exit
        Code 1 if no system is registered under the name, or the system is
        defined in code and has no file.
    """
    try:
        context = registered_context(name)
    except CstarError as ex:
        typer.echo(str(ex), err=True)
        raise typer.Exit(1) from ex

    system_file = system_file_of(context)
    if system_file is None:
        typer.echo(
            f"The system {context.name!r} is defined in code: no file.", err=True
        )
        raise typer.Exit(1)
    typer.echo(system_file.path.read_text(encoding="utf-8"), nl=False)
