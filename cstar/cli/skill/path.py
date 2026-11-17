import typer

from cstar.base.skills import SkillError, UnknownSkillError, select_skills
from cstar.cli.skill.verbs import COMMAND_LIST, COMMAND_PATH, NAME

app = typer.Typer()


@app.command(name=COMMAND_PATH, help="Print the folder of a skill C-Star ships.")
def path(
    name: str = typer.Argument(
        help=f"The skill's name, as `cstar {NAME} {COMMAND_LIST}` shows it."
    ),
) -> None:
    """Print the folder that holds a shipped skill in this C-Star installation.

    Raises
    ------
    typer.BadParameter
        If the name matches no shipped skill.
    typer.Exit
        Code 1 if a shipped skill cannot be read.
    """
    try:
        [skill] = select_skills([name])
    except UnknownSkillError as ex:
        raise typer.BadParameter(str(ex), param_hint="NAME") from ex
    except SkillError as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex
    typer.echo(skill.path)
