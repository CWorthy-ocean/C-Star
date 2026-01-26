import typing as t

import typer

import cstar

app = typer.Typer()


def version_callback(value: bool) -> None:
    """Print the version of C-Star and exit."""
    if value:
        typer.echo(cstar.__version__)
        raise typer.Exit()


def common_callback(
    ctx: typer.Context,
    version: t.Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            is_eager=True,
            callback=version_callback,
            help="Show the version of C-Star and exit.",
        ),
    ] = False,
) -> None:
    """Common callback for all commands."""
    pass
