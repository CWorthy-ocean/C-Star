"""Tests for the shipped agent skills and the `cstar skill` commands."""

import importlib.resources
import re
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

import cstar
from cstar.base.skills import (
    SKILL_FILE_NAME,
    InstallState,
    Skill,
    default_install_dirs,
    install_skill,
    install_state,
    is_cstar_skill_target,
    read_skill,
    shipped_skills,
    shipped_skills_dir,
)
from cstar.cli.cli import app as root_app
from cstar.system.manager import SYSTEM_SKILL

SKILL_NAME = SYSTEM_SKILL
"""The shipped skill the install tests use."""
OTHER_INSTALL_TARGET = (
    "old-env/lib/site-packages/cstar/additional_files/skills/" + SKILL_NAME
)
"""Where another C-Star installation's copy of the skill would be, under tmp_path."""
INSTALL_HINT = "Run `cstar skill install`"
"""The start of the line `list` prints when something is missing or out of date."""
SCRIPT_PATH = re.compile(r"\S*scripts/[\w.-]+\.sh")
"""A path to one of a skill's scripts, wherever it occurs in a line."""
BASH_BEFORE = re.compile(r"(?:\bbash(?:\s+-\S+)*\s+|<\s+)$")
"""What must precede a script's path: `bash` with optional flags, or `< `."""
DOT_SLASH_SCRIPT = re.compile(r"(?<![\w/.-])\./\S*\.sh\b")
"""A script run as `./name.sh`, which needs the executable bit that a copy loses."""


def run_skill(*args: str) -> Result:
    """Invoke `cstar skill` through the root app."""
    return CliRunner().invoke(root_app, ["skill", *args], color=False)


@pytest.fixture
def skill() -> Skill:
    """The shipped skill the install tests use."""
    return next(s for s in shipped_skills() if s.name == SKILL_NAME)


@pytest.fixture
def skills_dir(tmp_path: Path) -> Path:
    """An empty skills folder to install into with `--dir`."""
    return tmp_path / "skills"


@pytest.fixture
def old_install_target(tmp_path: Path) -> Path:
    """The path of the skill in another C-Star installation, which does not exist."""
    return tmp_path / OTHER_INSTALL_TARGET


def test_the_package_ships_the_system_skill() -> None:
    """The shipped skills include the one `cstar` points users at."""
    assert SKILL_NAME in [s.name for s in shipped_skills()]


def test_the_shipped_skills_folder_is_package_data() -> None:
    """The folder the code reads is the one `importlib.resources` finds in `cstar`."""
    packaged = importlib.resources.files("cstar") / "additional_files" / "skills"

    assert Path(str(packaged)).resolve() == shipped_skills_dir().resolve()


@pytest.mark.parametrize("skill", shipped_skills(), ids=lambda s: s.name)
def test_a_shipped_skill_is_named_for_its_folder(skill: Skill) -> None:
    """The front matter's name equals the folder name, and a description is given."""
    assert read_skill(skill.path).name == skill.path.name
    assert skill.description


@pytest.mark.parametrize("skill", shipped_skills(), ids=lambda s: s.name)
def test_a_shipped_skill_runs_its_scripts_through_bash(skill: Skill) -> None:
    """No script is called as `./x.sh`, and every path to one follows `bash` and its
    flags or a `< ` redirection, so the call does not need the executable bit.
    """
    for line in (skill.path / SKILL_FILE_NAME).read_text().splitlines():
        assert not DOT_SLASH_SCRIPT.search(line), line
        for found in SCRIPT_PATH.finditer(line):
            assert BASH_BEFORE.search(line[: found.start()]), line


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (f"/a/b/cstar/additional_files/skills/{SKILL_NAME}", True),
        (f"cstar/additional_files/skills/{SKILL_NAME}", True),
        (f"/a/b/cstar/additional_files/skills/other-{SKILL_NAME}", False),
        (f"/a/b/additional_files/skills/{SKILL_NAME}", False),
        (f"/a/b/notcstar/additional_files/skills/{SKILL_NAME}", False),
        ("/somewhere/else", False),
    ],
)
def test_is_cstar_skill_target(target: str, expected: bool) -> None:
    """Only a path ending `cstar/additional_files/skills/<name>` is a C-Star skill."""
    assert is_cstar_skill_target(target, SKILL_NAME) is expected


def test_install_without_a_claude_home_writes_only_the_agents_folder(
    isolate_agent_home: Path, skill: Skill
) -> None:
    """With no `~/.claude`, install writes `~/.agents/skills` and creates no
    `~/.claude`.
    """
    result = run_skill("install")

    place = isolate_agent_home / ".agents" / "skills" / SKILL_NAME
    assert result.exit_code == 0, result.output
    assert place.is_symlink()
    assert place.resolve() == skill.path.resolve()
    assert str(place) in result.output
    assert not (isolate_agent_home / ".claude").exists()


@pytest.mark.parametrize("names", [[], [SKILL_NAME]], ids=["all", "named"])
def test_install_with_a_claude_home_writes_both_folders(
    isolate_agent_home: Path, skill: Skill, names: list[str]
) -> None:
    """With `~/.claude` present, install links the skills into both folders."""
    (isolate_agent_home / ".claude").mkdir()

    result = run_skill("install", *names)

    assert result.exit_code == 0, result.output
    for home in (".agents", ".claude"):
        place = isolate_agent_home / home / "skills" / SKILL_NAME
        assert place.is_symlink()
        assert place.resolve() == skill.path.resolve()
        assert str(place) in result.output


def test_install_into_a_directory_links(skills_dir: Path, skill: Skill) -> None:
    """`--dir` installs a link into that folder only."""
    result = run_skill("install", SKILL_NAME, "--dir", str(skills_dir))

    place = skills_dir / SKILL_NAME
    assert result.exit_code == 0, result.output
    assert place.is_symlink()
    assert place.resolve() == skill.path.resolve()


def test_install_with_copy_writes_a_real_folder(skills_dir: Path, skill: Skill) -> None:
    """`--copy` writes the skill's files, not a link."""
    result = run_skill("install", SKILL_NAME, "--dir", str(skills_dir), "--copy")

    place = skills_dir / SKILL_NAME
    assert result.exit_code == 0, result.output
    assert place.is_dir()
    assert not place.is_symlink()
    assert (place / SKILL_FILE_NAME).is_file()
    for script in ("probe.sh", "try-modules.sh"):
        assert (place / "scripts" / script).is_file()


def test_install_again_leaves_its_own_link(skills_dir: Path, skill: Skill) -> None:
    """A second install succeeds, changes nothing and says it is already linked."""
    run_skill("install", "--dir", str(skills_dir))
    before = (skills_dir / SKILL_NAME).readlink()

    result = run_skill("install", "--dir", str(skills_dir))

    assert result.exit_code == 0, result.output
    assert "already linked" in result.output
    assert (skills_dir / SKILL_NAME).readlink() == before


@pytest.mark.parametrize("target_exists", [True, False], ids=["present", "dangling"])
def test_install_updates_a_link_from_another_cstar(
    skills_dir: Path, skill: Skill, old_install_target: Path, target_exists: bool
) -> None:
    """A link into another C-Star installation, even a removed one, is replaced
    without `--force`.
    """
    if target_exists:
        old_install_target.mkdir(parents=True)
    skills_dir.mkdir()
    place = skills_dir / SKILL_NAME
    place.symlink_to(old_install_target)

    result = run_skill("install", "--dir", str(skills_dir))

    assert result.exit_code == 0, result.output
    assert place.resolve() == skill.path.resolve()


@pytest.mark.parametrize("target_exists", [True, False], ids=["present", "dangling"])
def test_install_refuses_a_foreign_link_without_force(
    tmp_path: Path, skills_dir: Path, target_exists: bool
) -> None:
    """A link to another folder is refused, with a hint to use `--force`, and left
    as it was.
    """
    foreign = tmp_path / "mine"
    if target_exists:
        foreign.mkdir()
    skills_dir.mkdir()
    place = skills_dir / SKILL_NAME
    place.symlink_to(foreign)

    result = run_skill("install", "--dir", str(skills_dir))

    assert result.exit_code == 1
    assert "--force" in result.stderr
    assert place.readlink() == foreign


@pytest.mark.parametrize("target_exists", [True, False], ids=["present", "dangling"])
def test_force_replaces_a_foreign_link(
    tmp_path: Path, skills_dir: Path, skill: Skill, target_exists: bool
) -> None:
    """`--force` replaces a link to another folder."""
    foreign = tmp_path / "mine"
    if target_exists:
        foreign.mkdir()
    skills_dir.mkdir()
    place = skills_dir / SKILL_NAME
    place.symlink_to(foreign)

    result = run_skill("install", "--dir", str(skills_dir), "--force")

    assert result.exit_code == 0, result.output
    assert place.resolve() == skill.path.resolve()


@pytest.mark.parametrize("kind", ["folder", "file"])
@pytest.mark.parametrize("force", [False, True], ids=["plain", "force"])
def test_install_never_replaces_a_real_folder_or_file(
    skills_dir: Path, kind: str, force: bool
) -> None:
    """A real folder or file in the way is refused, `--force` or not, and kept."""
    skills_dir.mkdir()
    place = skills_dir / SKILL_NAME
    if kind == "folder":
        place.mkdir()
        (place / "mine.txt").write_text("keep")
    else:
        place.write_text("keep")

    result = run_skill(
        "install", "--dir", str(skills_dir), *(["--force"] if force else [])
    )

    assert result.exit_code == 1
    assert "Error:" in result.stderr
    assert not place.is_symlink()
    if kind == "folder":
        assert (place / "mine.txt").read_text() == "keep"
        assert [p.name for p in place.iterdir()] == ["mine.txt"]
    else:
        assert place.read_text() == "keep"


def test_a_refusal_in_one_place_does_not_stop_the_others(
    isolate_agent_home: Path, skill: Skill
) -> None:
    """A real folder in `~/.claude/skills` is reported and `~/.agents/skills` is
    still written; the exit code is 1.
    """
    blocker = isolate_agent_home / ".claude" / "skills" / SKILL_NAME
    blocker.mkdir(parents=True)

    result = run_skill("install")

    agents_place = isolate_agent_home / ".agents" / "skills" / SKILL_NAME
    assert result.exit_code == 1
    assert str(blocker) in result.stderr
    assert agents_place.resolve() == skill.path.resolve()
    assert not blocker.is_symlink()


def test_install_of_an_unknown_name_is_a_usage_error() -> None:
    """A name that is no shipped skill exits 2."""
    result = run_skill("install", "nope")

    assert result.exit_code == 2
    assert "nope" in result.output


def test_install_names_every_unknown_name() -> None:
    """With two unknown names, both appear in the message."""
    result = run_skill("install", "nope", SKILL_NAME, "neither")

    assert result.exit_code == 2
    assert "nope" in result.output
    assert "neither" in result.output


def test_unknown_name_installs_nothing(isolate_agent_home: Path) -> None:
    """A usage error writes no skill, not even the valid ones."""
    run_skill("install", SKILL_NAME, "nope")

    assert not (isolate_agent_home / ".agents").exists()


def test_list_reports_a_skill_that_is_not_installed(isolate_agent_home: Path) -> None:
    """A missing skill is listed as not installed, followed by the install hint."""
    result = run_skill("list")

    place = isolate_agent_home / ".agents" / "skills" / SKILL_NAME
    assert result.exit_code == 0, result.output
    assert f"{SKILL_NAME}:" in result.output
    assert f"{place}: not installed" in result.output
    assert INSTALL_HINT in result.output
    assert str(isolate_agent_home / ".claude") not in result.output


def test_list_shows_the_claude_folder_only_when_it_exists(
    isolate_agent_home: Path,
) -> None:
    """`~/.claude/skills` is listed once `~/.claude` exists."""
    (isolate_agent_home / ".claude").mkdir()

    result = run_skill("list")

    assert str(isolate_agent_home / ".claude" / "skills" / SKILL_NAME) in result.output


def test_list_reports_a_link_to_this_cstar(isolate_agent_home: Path) -> None:
    """An installed skill is listed as linked, with no install hint."""
    run_skill("install")

    result = run_skill("list")

    assert result.exit_code == 0, result.output
    assert "linked to this C-Star" in result.output
    assert INSTALL_HINT not in result.output


def test_list_reports_a_dangling_cstar_link(
    isolate_agent_home: Path, old_install_target: Path
) -> None:
    """A broken link into a removed C-Star is listed as broken, with the hint."""
    folder = isolate_agent_home / ".agents" / "skills"
    folder.mkdir(parents=True)
    (folder / SKILL_NAME).symlink_to(old_install_target)

    result = run_skill("list")

    assert f"broken link to {old_install_target}" in result.output
    assert INSTALL_HINT in result.output


def test_list_reports_a_foreign_link_without_the_hint(
    isolate_agent_home: Path, tmp_path: Path
) -> None:
    """A link to something that is not a C-Star skill is listed as such, and install
    is not suggested.
    """
    foreign = tmp_path / "mine"
    foreign.mkdir()
    folder = isolate_agent_home / ".agents" / "skills"
    folder.mkdir(parents=True)
    (folder / SKILL_NAME).symlink_to(foreign)

    result = run_skill("list")

    assert f"a link to {foreign}, not to a C-Star skill" in result.output
    assert INSTALL_HINT not in result.output


def test_path_prints_the_shipped_folder(skill: Skill) -> None:
    """`path` prints the folder of the skill in this installation."""
    result = run_skill("path", SKILL_NAME)

    assert result.exit_code == 0, result.output
    assert Path(result.output.strip()).resolve() == skill.path.resolve()


def test_path_of_an_unknown_name_is_a_usage_error() -> None:
    """`path` exits 2 for a name that is no shipped skill."""
    result = run_skill("path", "nope")

    assert result.exit_code == 2


def test_install_skill_says_whether_it_wrote(skills_dir: Path, skill: Skill) -> None:
    """`install_skill` returns True when it writes and False when the link is current."""
    assert install_skill(skill, skills_dir) is True
    assert install_skill(skill, skills_dir) is False


def test_copy_over_a_current_link_replaces_it_with_a_folder(
    skills_dir: Path, skill: Skill
) -> None:
    """Asking for a copy where the link is current still writes, and returns True."""
    install_skill(skill, skills_dir)

    written = install_skill(skill, skills_dir, copy=True)

    place = skills_dir / SKILL_NAME
    assert written is True
    assert place.is_dir()
    assert not place.is_symlink()


def test_install_state_of_each_kind(
    tmp_path: Path, skills_dir: Path, skill: Skill, old_install_target: Path
) -> None:
    """`install_state` tells apart every `InstallState`, and gives the link target."""
    skills_dir.mkdir()
    foreign = tmp_path / "mine"
    foreign.mkdir()
    old_install_target.mkdir(parents=True)
    places = {
        "linked": skill.path,
        "other": old_install_target,
        "dangling": tmp_path / "gone",
        "foreign": foreign,
    }
    states: dict[str, tuple[InstallState, str]] = {}
    for label, target in places.items():
        folder = skills_dir / label
        folder.mkdir()
        (folder / SKILL_NAME).symlink_to(target)
        states[label] = install_state(skill, folder)
    (skills_dir / "real").mkdir()
    (skills_dir / "real" / SKILL_NAME).mkdir()

    assert states["linked"] == (InstallState.LINKED, str(skill.path))
    assert states["other"] == (InstallState.OTHER_CSTAR, str(old_install_target))
    assert states["dangling"] == (InstallState.DANGLING, str(tmp_path / "gone"))
    assert states["foreign"] == (InstallState.FOREIGN, str(foreign))
    assert install_state(skill, skills_dir / "real") == (InstallState.NOT_A_LINK, "")
    assert install_state(skill, skills_dir) == (InstallState.MISSING, "")


def test_default_install_dirs_follow_the_agent_home(isolate_agent_home: Path) -> None:
    """The default folders are `~/.agents/skills`, plus `~/.claude/skills` once
    `~/.claude` exists.
    """
    assert default_install_dirs() == [isolate_agent_home / ".agents" / "skills"]

    (isolate_agent_home / ".claude").mkdir()

    assert default_install_dirs() == [
        isolate_agent_home / ".agents" / "skills",
        isolate_agent_home / ".claude" / "skills",
    ]


@pytest.mark.parametrize("agent_dir", [".agents", ".claude"])
def test_the_checkout_links_the_shipped_skill(agent_dir: str, skill: Skill) -> None:
    """In a checkout, `.agents` and `.claude` link to the shipped skill's folder."""
    link = Path(cstar.__file__).parent.parent / agent_dir / "skills" / SKILL_NAME
    if not link.is_symlink() and not link.exists():
        pytest.skip("not a checkout")

    assert link.is_symlink()
    assert link.resolve() == skill.path.resolve()
