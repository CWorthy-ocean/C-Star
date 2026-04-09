import typing as t

import typer

import cstar

app = typer.Typer()

HELP_SHORT = "Print the current version of the C-Star package and exit."


def version_callback(value: bool) -> None:
    """Print the current version of the C-Star package and exit."""
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
            help=HELP_SHORT,
        ),
    ] = False,
) -> None:
    pass
