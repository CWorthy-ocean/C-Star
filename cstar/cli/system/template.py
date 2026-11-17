import typer

from cstar.system.template import render_system_template

app = typer.Typer()


@app.command(
    name="template", help="Print a commented system file to start a new one from."
)
def template() -> None:
    """Print a commented starting system file, every key described.

    Nothing else goes to standard output, so the file can be saved and edited:
    ``cstar system template > mycluster.yaml``. It validates as it is, and its
    comments come from the installed C-Star's own schema.
    """
    typer.echo(render_system_template(), nl=False)
