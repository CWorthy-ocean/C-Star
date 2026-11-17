import typing as t
from pathlib import Path

import typer

from cstar.base.skills import (
    AGENTS_HOME,
    CLAUDE_HOME,
    SKILLS_DIR_NAME,
    Skill,
    SkillError,
    SkillInstallError,
    UnknownSkillError,
    default_install_dirs,
    install_skill,
    select_skills,
)
from cstar.cli.skill.verbs import COMMAND_INSTALL, COMMAND_LIST, NAME
from cstar.entrypoint.utils import ARG_COPY, ARG_DIR, ARG_FORCE
from cstar.system.manager import display_path

if t.TYPE_CHECKING:
    from collections.abc import Callable, Sequence

app = typer.Typer()

HELP_SHORT: t.Final[str] = "Install C-Star's skills where your coding agents find them."
HELP_LONG: t.Final[str] = f"""\
{HELP_SHORT}

Installs the named skills, or every skill C-Star ships when none is named, into
~/{AGENTS_HOME}/{SKILLS_DIR_NAME} (read by Codex, Gemini CLI and GitHub Copilot)
and, when ~/{CLAUDE_HOME} exists, into ~/{CLAUDE_HOME}/{SKILLS_DIR_NAME} (read by
Claude Code). {ARG_DIR} installs into one folder of your choice instead.

Each skill is a link into this C-Star installation, so it follows upgrades;
it breaks if the environment is removed, and it follows whichever environment
ran this command last. {ARG_COPY} copies the files instead: a copy survives the
environment but does not change when C-Star does.

A link a C-Star install made is updated. Anything else in the way is left alone
and reported: {ARG_FORCE} replaces a link that points elsewhere, and a real
folder or file is never replaced. Run the command again after upgrading C-Star
to add skills that are new in that version.
"""


def install_skills(
    skills: "Sequence[Skill]",
    folders: "Sequence[Path]",
    copy: bool = False,
    force: bool = False,
    log: "Callable[[str], None]" = typer.echo,
) -> list[SkillInstallError]:
    """Install each skill into each folder, saying what was written as it goes.

    Every place is tried; one that is refused does not stop the others.

    Parameters
    ----------
    skills : Sequence[Skill]
        The shipped skills to install.
    folders : Sequence[Path]
        The skills folders to install them into.
    copy : bool
        Copy the files instead of linking.
    force : bool
        Replace a link that points somewhere other than a C-Star skill.
    log : Callable[[str], None]
        Receives one line per place written or already current.

    Returns
    -------
    list[SkillInstallError]
        What could not be installed, for the caller to report.
    """
    failed: list[SkillInstallError] = []
    for folder in folders:
        for skill in skills:
            place = display_path(folder / skill.name)
            try:
                written = install_skill(skill, folder, copy=copy, force=force)
            except SkillInstallError as ex:
                failed.append(ex)
                continue
            if not written:
                log(f"{place}: already linked to this C-Star")
            elif copy:
                log(f"Copied {skill.name} to {place}")
            else:
                log(f"Linked {place} -> {skill.path.resolve()}")
    return failed


def describe_failure(error: SkillInstallError) -> str:
    """Say why a skill was not installed, and how to replace what is in the way."""
    hint = f" {ARG_FORCE} replaces it." if error.replaceable else ""
    return f"{error}{hint}"


@app.command(name=COMMAND_INSTALL, help=HELP_LONG, short_help=HELP_SHORT)
def install(
    names: t.Annotated[
        list[str] | None,
        typer.Argument(
            help=f"The skills to install, as `cstar {NAME} {COMMAND_LIST}` names them; "
            "every shipped skill when none is given.",
            show_default=False,
            metavar="[NAME]...",
        ),
    ] = None,
    directory: t.Annotated[
        Path | None,
        typer.Option(
            ARG_DIR,
            help="Install into this skills folder only, instead of the agents' own.",
            file_okay=False,
        ),
    ] = None,
    copy: t.Annotated[
        bool,
        typer.Option(
            ARG_COPY,
            help="Copy the files instead of linking to them; a copy does not "
            "follow C-Star upgrades.",
        ),
    ] = False,
    force: t.Annotated[
        bool,
        typer.Option(
            ARG_FORCE,
            help="Replace a link that points somewhere other than a C-Star skill. "
            "A real folder or file is never replaced.",
        ),
    ] = False,
) -> None:
    """Install shipped skills into the coding agents' skills folders.

    Raises
    ------
    typer.BadParameter
        If a name matches no shipped skill.
    typer.Exit
        Code 1 if a shipped skill cannot be read or any place was not written.
    """
    try:
        skills = select_skills(names or [])
    except UnknownSkillError as ex:
        raise typer.BadParameter(str(ex), param_hint="NAME") from ex
    except SkillError as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex

    folders = [directory] if directory is not None else default_install_dirs()
    failed = install_skills(skills, folders, copy=copy, force=force)
    for error in failed:
        typer.echo(f"Error: {describe_failure(error)}", err=True)
    if failed:
        raise typer.Exit(1)
