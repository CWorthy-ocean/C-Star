import typer

from cstar.base.skills import (
    InstallState,
    SkillError,
    default_install_dirs,
    install_state,
    is_cstar_skill_target,
    shipped_skills,
)
from cstar.cli.skill.verbs import COMMAND_INSTALL, COMMAND_LIST, NAME
from cstar.system.manager import display_path

app = typer.Typer()


def describe_state(state: InstallState, target: str) -> str:
    """Say what is at a skill's place in an agent's skills folder."""
    match state:
        case InstallState.LINKED:
            return "linked to this C-Star"
        case InstallState.OTHER_CSTAR:
            return f"linked to another C-Star installation ({target})"
        case InstallState.DANGLING:
            return f"broken link to {target}"
        case InstallState.FOREIGN:
            return f"a link to {target}, not to a C-Star skill"
        case InstallState.NOT_A_LINK:
            return "a folder, not a link (a copy, or not C-Star's)"
        case InstallState.MISSING:
            return "not installed"


@app.command(
    name=COMMAND_LIST, help="List the skills C-Star ships, and where each is installed."
)
def list_skills() -> None:
    """List the shipped skills: each one's name and what it does, then what is at
    its place in each skills folder `install` writes to, ending with a line that
    says to run `install` when a skill is missing or an older link is there.

    Raises
    ------
    typer.Exit
        Code 1 if a shipped skill cannot be read.
    """
    try:
        skills = shipped_skills()
    except SkillError as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex
    if not skills:
        typer.echo("This C-Star ships no skills.")
        return

    folders = default_install_dirs()
    outdated = False
    for skill in skills:
        typer.echo(f"{skill.name}: {skill.summary}")
        for folder in folders:
            state, target = install_state(skill, folder)
            place = display_path(folder / skill.name)
            typer.echo(f"  {place}: {describe_state(state, target)}")
            outdated |= state in (InstallState.MISSING, InstallState.OTHER_CSTAR) or (
                state is InstallState.DANGLING
                and is_cstar_skill_target(target, skill.name)
            )
    if outdated:
        typer.echo(
            f"Run `cstar {NAME} {COMMAND_INSTALL}` to install the missing "
            "skills and update the older links."
        )
