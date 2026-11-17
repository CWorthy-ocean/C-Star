"""The agent skills C-Star ships, and installing them where coding agents look.

A skill is a folder in the Agent Skills format (https://agentskills.io): a
``SKILL.md`` whose front matter gives the skill's name and says when to use it,
beside the files it refers to. The skills meant for C-Star's users are package
data, so an install from conda carries the skills that match its version;
``cstar skill install`` links them into the folders coding agents read.
"""

import os
import shutil
import typing as t
from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path

import yaml

from cstar.base.exceptions import CstarError
from cstar.base.utils import additional_files_dir

if t.TYPE_CHECKING:
    from collections.abc import Sequence

SKILLS_DIR_NAME: t.Final[str] = "skills"
"""The folder that holds skills, in the package and in each agent's home folder."""
SKILL_FILE_NAME: t.Final[str] = "SKILL.md"
"""The file that makes a folder a skill."""
_FRONT_MATTER_FENCE: t.Final[str] = "---"
"""The line that opens and closes a skill's front matter."""
_PACKAGE_PARTS: t.Final[tuple[str, ...]] = (
    "cstar",
    "additional_files",
    SKILLS_DIR_NAME,
)
"""Where any C-Star's shipped skills sit, as the last parts of their folder's path."""

AGENTS_HOME: t.Final[str] = ".agents"
"""The home folder whose ``skills`` Codex, Gemini CLI and GitHub Copilot read."""
CLAUDE_HOME: t.Final[str] = ".claude"
"""The home folder whose ``skills`` Claude Code reads; it reads no other."""
AGENT_HOMES: t.Final[tuple[str, ...]] = (
    CLAUDE_HOME,
    AGENTS_HOME,
    ".codex",
    ".gemini",
    ".copilot",
)
"""Home folders whose presence says this user runs a coding agent."""


class SkillError(CstarError):
    """A shipped skill cannot be read, or a name matches no shipped skill."""


class UnknownSkillError(SkillError):
    """A name matches no skill this C-Star ships."""


class SkillInstallError(CstarError):
    """A skill could not be installed at one place.

    Attributes
    ----------
    place : Path
        Where the skill was to go: its name in a skills folder.
    replaceable : bool
        Whether the install was refused only because a link that points elsewhere
        is in the way, which ``--force`` replaces.
    """

    def __init__(self, message: str, place: Path, replaceable: bool = False) -> None:
        """Initialize the error with its message, the place, and whether forcing
        would help.
        """
        super().__init__(message)
        self.place = place
        self.replaceable = replaceable


@dataclass(frozen=True)
class Skill:
    """One skill C-Star ships."""

    name: str
    """The skill's name, which is also its folder's name."""
    path: Path
    """The skill's folder in the installed package."""
    description: str
    """What the skill does and when to use it, from its front matter."""

    @property
    def summary(self) -> str:
        """The first sentence of the description."""
        head, sep, _ = self.description.partition(". ")
        return f"{head}." if sep else self.description


class InstallState(StrEnum):
    """What is at a skill's place in an agent's skills folder."""

    LINKED = auto()
    """A link to this C-Star's copy of the skill."""
    OTHER_CSTAR = auto()
    """A link to the skill in another C-Star installation, which install updates."""
    DANGLING = auto()
    """A link whose target is gone."""
    FOREIGN = auto()
    """A link to something other than a C-Star skill."""
    NOT_A_LINK = auto()
    """A real folder or file: a copy, or something C-Star did not put there."""
    MISSING = auto()
    """Nothing."""


def home_dir() -> Path:
    """The user's home directory, under which the agents' folders live."""
    return Path.home()


def shipped_skills_dir() -> Path:
    """The folder of the skills this C-Star ships."""
    return additional_files_dir() / SKILLS_DIR_NAME


def agent_homes() -> list[Path]:
    """The agents' home folders that exist for this user, in `AGENT_HOMES` order."""
    home = home_dir()
    return [home / name for name in AGENT_HOMES if (home / name).is_dir()]


def default_install_dirs() -> list[Path]:
    """The skills folders `install` writes to when no folder is given.

    Always ``~/.agents/skills``, which Codex, Gemini CLI and GitHub Copilot read;
    also ``~/.claude/skills`` when ``~/.claude`` exists, because Claude Code reads
    only that one.
    """
    home = home_dir()
    dirs = [home / AGENTS_HOME / SKILLS_DIR_NAME]
    if (home / CLAUDE_HOME).is_dir():
        dirs.append(home / CLAUDE_HOME / SKILLS_DIR_NAME)
    return dirs


def read_skill(folder: Path) -> Skill:
    """Read a skill's name and description from the front matter of its SKILL.md.

    Raises
    ------
    SkillError
        If the file cannot be read, has no front matter, or lacks either field.
    """
    path = folder / SKILL_FILE_NAME
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as ex:
        raise SkillError(f"Cannot read {path}: {ex}") from ex
    end = next(
        (
            i
            for i, line in enumerate(lines[1:], 1)
            if line.strip() == _FRONT_MATTER_FENCE
        ),
        None,
    )
    if not lines or lines[0].strip() != _FRONT_MATTER_FENCE or end is None:
        raise SkillError(f"{path} does not start with front matter between --- lines.")
    try:
        meta = yaml.safe_load("\n".join(lines[1:end]))
    except yaml.YAMLError as ex:
        raise SkillError(f"The front matter of {path} is not valid YAML: {ex}") from ex
    fields = meta if isinstance(meta, dict) else {}
    name, description = fields.get("name"), fields.get("description")
    if not isinstance(name, str) or not isinstance(description, str):
        raise SkillError(f"The front matter of {path} needs a name and a description.")
    return Skill(name=name, path=folder, description=" ".join(description.split()))


def shipped_skills() -> list[Skill]:
    """The skills this C-Star ships, by name.

    Raises
    ------
    SkillError
        If a shipped skill's SKILL.md cannot be read, or names a skill other than
        its folder.
    """
    root = shipped_skills_dir()
    if not root.is_dir():
        return []
    skills = []
    for folder in root.iterdir():
        if not (folder / SKILL_FILE_NAME).is_file():
            continue
        skill = read_skill(folder)
        if skill.name != folder.name:
            raise SkillError(
                f"{folder / SKILL_FILE_NAME} names the skill {skill.name!r}, "
                "not its folder's name."
            )
        skills.append(skill)
    return sorted(skills, key=lambda s: s.name)


def select_skills(names: "Sequence[str]") -> list[Skill]:
    """The shipped skills of the given names, or all of them when none is given.

    Raises
    ------
    UnknownSkillError
        Naming every name that matches no shipped skill.
    SkillError
        If a shipped skill cannot be read.
    """
    skills = shipped_skills()
    if not names:
        return skills
    by_name = {s.name: s for s in skills}
    if unknown := [n for n in names if n not in by_name]:
        known = ", ".join(by_name) or "none"
        raise UnknownSkillError(
            f"No shipped skill is named {', '.join(map(repr, unknown))}; "
            f"C-Star ships: {known}."
        )
    return [by_name[n] for n in dict.fromkeys(names)]


def is_cstar_skill_target(target: str, name: str) -> bool:
    """Whether a link's target is the skill `name` in some C-Star's package folder.

    Any installation counts, this one or another, present or removed: a link that
    `install` made in an environment since removed or replaced is still C-Star's
    own, and `install` updates it.
    """
    return Path(target).parts[-4:] == (*_PACKAGE_PARTS, name)


def install_state(skill: Skill, folder: Path) -> tuple[InstallState, str]:
    """Say what is at a skill's place in a skills folder.

    Returns
    -------
    tuple[InstallState, str]
        The state, and the target of the link that is there (empty when there is
        no link).
    """
    place = folder / skill.name
    # before exists(): a dangling link does not exist, and a link to a folder is_dir
    if place.is_symlink():
        target = os.readlink(place)
        if not place.exists():
            return InstallState.DANGLING, target
        if place.resolve() == skill.path.resolve():
            return InstallState.LINKED, target
        if is_cstar_skill_target(target, skill.name):
            return InstallState.OTHER_CSTAR, target
        return InstallState.FOREIGN, target
    if place.exists():
        return InstallState.NOT_A_LINK, ""
    return InstallState.MISSING, ""


def install_skill(
    skill: Skill, folder: Path, copy: bool = False, force: bool = False
) -> bool:
    """Link (or copy) a skill into a skills folder, creating the folder if needed.

    A link already there is updated when it points at a C-Star skill of the same
    name, in this installation or another; one that points anywhere else is
    replaced only when `force` is given. A real folder or file is never replaced.

    Parameters
    ----------
    skill : Skill
        The shipped skill.
    folder : Path
        The skills folder, such as ``~/.agents/skills``.
    copy : bool
        Copy the skill's files instead of linking to its folder.
    force : bool
        Replace a link that points somewhere other than a C-Star skill.

    Returns
    -------
    bool
        Whether anything was written; False when a link to this C-Star's skill
        was there already (and `copy` was not asked for).

    Raises
    ------
    SkillInstallError
        If something that may not be replaced is in the way, or the write fails.
    """
    place = folder / skill.name
    state, target = install_state(skill, folder)
    if state is InstallState.LINKED and not copy:
        return False
    if state is InstallState.NOT_A_LINK:
        raise SkillInstallError(
            f"{place} is already there and is not a link, so C-Star leaves it alone; "
            "remove it to install the skill there.",
            place,
        )
    if (
        state in (InstallState.FOREIGN, InstallState.DANGLING)
        and not is_cstar_skill_target(target, skill.name)
        and not force
    ):
        raise SkillInstallError(
            f"{place} is a link to {target}, which is not a C-Star skill.",
            place,
            replaceable=True,
        )
    source = skill.path.resolve()
    # written beside the place and renamed over it, so an interrupted install
    # leaves the old link or nothing, never a half-written skill
    scratch = folder / f".{skill.name}.cstar-install-{os.getpid()}"
    try:
        folder.mkdir(parents=True, exist_ok=True)
        if copy:
            shutil.copytree(source, scratch, symlinks=False)
            if place.is_symlink():
                place.unlink()
        else:
            scratch.symlink_to(source, target_is_directory=True)
        scratch.replace(place)
    except OSError as ex:
        if scratch.is_symlink() or scratch.is_file():
            scratch.unlink(missing_ok=True)
        elif scratch.is_dir():
            shutil.rmtree(scratch, ignore_errors=True)
        raise SkillInstallError(
            f"Cannot install {skill.name} at {place}: {ex}", place
        ) from ex
    return True
