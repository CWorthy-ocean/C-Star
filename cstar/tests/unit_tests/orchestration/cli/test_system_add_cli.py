"""Tests for `cstar system add`, which copies a system file into a catalog."""

import os
import stat
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.env import BUNDLED_CATALOG_ROOT, ENV_CSTAR_CATALOG
from cstar.cli.system import add as add_module
from cstar.system.layers import systems_dir
from cstar.system.manager import display_path, reload_system_files, system_stack
from cstar.system.spec import SYSTEMS_DIR_NAME

BUNDLED_SYSTEMS = BUNDLED_CATALOG_ROOT / SYSTEMS_DIR_NAME


@pytest.fixture(autouse=True)
def isolated(
    isolate_user_config: Path,
    system_file_layers: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read the layers from the `CSTAR_CATALOG` a test sets, with no config file."""
    monkeypatch.delenv(ENV_CSTAR_CATALOG, raising=False)
    reload_system_files()


@pytest.fixture
def user_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Your own catalog, the only `CSTAR_CATALOG` entry."""
    root = tmp_path / "mine"
    monkeypatch.setenv(ENV_CSTAR_CATALOG, str(root))
    return root


@pytest.fixture
def draft(tmp_path: Path) -> Callable[..., Path]:
    """Make a valid system file with the given name, from the generic Linux one."""

    def make(
        name: str = "mycluster", extra: str = "", bundled: str = "linux_x86_64"
    ) -> Path:
        text = (BUNDLED_SYSTEMS / f"{bundled}.yaml").read_text()
        text = text.replace(f"name: {bundled}", f"name: {name}") + extra
        path = tmp_path / "drafts" / f"{name}-{len(extra)}.yaml"
        path.parent.mkdir(exist_ok=True)
        path.write_text(text)
        return path

    return make


@pytest.fixture
def detected(monkeypatch: pytest.MonkeyPatch) -> Callable[[bool], None]:
    """Decide whether detection selects the system."""

    def decide(value: bool) -> None:
        monkeypatch.setattr(add_module, "detection_would_select", lambda spec: value)

    return decide


def add(*args: str | Path) -> Result:
    return CliRunner().invoke(add_module.app, [str(a) for a in args], color=False)


def flat(result: Result) -> str:
    return " ".join(result.output.split())


def test_adds_a_draft_under_its_system_name(
    user_root: Path, draft: Callable[..., Path], detected: Callable[[bool], None]
) -> None:
    """A file named otherwise installs as `<name>.yaml` in your own catalog, and
    the same process sees it.
    """
    detected(True)
    source = draft("mycluster")

    result = add(source)

    target = systems_dir(user_root) / "mycluster.yaml"
    assert result.exit_code == 0, result.output
    assert target.read_bytes() == source.read_bytes()
    assert "Added the system 'mycluster'" in flat(result)
    assert "C-Star selects 'mycluster' on this machine." in flat(result)
    assert system_stack().layers.files["mycluster"].path == target


def test_the_default_catalog_is_used_when_the_variable_is_unset(
    tmp_path: Path,
    draft: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With `CSTAR_CATALOG` unset your own catalog is ~/cstar/catalog."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    result = add(draft())

    assert result.exit_code == 0, result.output
    assert (tmp_path / "home/cstar/catalog/systems/mycluster.yaml").is_file()


def test_an_identical_file_is_left_alone(
    user_root: Path, draft: Callable[..., Path]
) -> None:
    source = draft()
    assert add(source).exit_code == 0
    target = systems_dir(user_root) / "mycluster.yaml"
    before = target.stat().st_mtime_ns

    result = add(source)

    assert result.exit_code == 0, result.output
    assert "already identical" in flat(result)
    assert target.stat().st_mtime_ns == before


def test_a_different_file_needs_replace(
    user_root: Path, draft: Callable[..., Path]
) -> None:
    first = draft()
    assert add(first).exit_code == 0
    second = draft(extra="\n# a change\n")
    target = systems_dir(user_root) / "mycluster.yaml"

    refused = add(second)

    assert refused.exit_code == 1
    assert "--replace" in refused.output
    assert str(target) in flat(refused)
    assert target.read_bytes() == first.read_bytes()

    replaced = add(second, "--replace")

    assert replaced.exit_code == 0, replaced.output
    assert target.read_bytes() == second.read_bytes()


def test_a_catalog_outside_the_stack_says_how_to_add_it(
    user_root: Path, tmp_path: Path, draft: Callable[..., Path]
) -> None:
    """The file is written, and C-Star is told to read the root after your own."""
    group = tmp_path / "group"

    result = add(draft(), "--catalog", group)

    assert result.exit_code == 0, result.output
    assert (systems_dir(group) / "mycluster.yaml").is_file()
    assert (
        f"cstar env set {ENV_CSTAR_CATALOG} {user_root}{os.pathsep}{group.resolve()}"
        in flat(result)
    )


def test_the_default_catalog_is_first_in_the_line_when_the_variable_is_unset(
    tmp_path: Path, draft: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    group = tmp_path / "group"

    result = add(draft(), "--catalog", group)

    mine = (tmp_path / "home/cstar/catalog").resolve()
    assert f"{ENV_CSTAR_CATALOG} {mine}{os.pathsep}{group.resolve()}" in flat(result)


def test_a_catalog_in_the_stack_gets_no_catalog_line(
    user_root: Path,
    tmp_path: Path,
    draft: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = tmp_path / "group"
    monkeypatch.setenv(ENV_CSTAR_CATALOG, f"{user_root}{os.pathsep}{group}")

    result = add(draft(), "--catalog", group)

    assert result.exit_code == 0, result.output
    assert "does not read" not in flat(result)
    assert (systems_dir(group) / "mycluster.yaml").is_file()


def test_the_bundled_catalog_is_refused(draft: Callable[..., Path]) -> None:
    result = add(draft(), "--catalog", BUNDLED_CATALOG_ROOT)

    assert result.exit_code == 1
    assert "read-only" in flat(result)
    assert not (BUNDLED_SYSTEMS / "mycluster.yaml").exists()


@pytest.mark.parametrize(
    "url",
    ["https://example.org/mycluster.yaml", "http://x/y.yaml", "git@github.com:a/b"],
)
def test_a_url_is_refused(url: str, user_root: Path) -> None:
    result = add(url)

    assert result.exit_code == 1
    assert "does not fetch" in flat(result)
    assert not user_root.exists()


def test_a_file_that_is_not_a_system_is_refused(
    user_root: Path, tmp_path: Path
) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: [unterminated\n")

    result = add(bad)

    assert result.exit_code == 1
    assert "is refused" in flat(result)
    assert not user_root.exists()


def test_a_missing_file_is_refused(user_root: Path, tmp_path: Path) -> None:
    result = add(tmp_path / "nope.yaml")

    assert result.exit_code == 1
    assert "is refused" in flat(result)


def test_the_installed_file_is_not_world_writable(
    user_root: Path, draft: Callable[..., Path]
) -> None:
    """A source anyone can write is copied without that permission, which would
    make C-Star refuse the copy.
    """
    source = draft()
    source.chmod(0o666)

    result = add(source)

    assert result.exit_code == 0, result.output
    mode = (systems_dir(user_root) / "mycluster.yaml").stat().st_mode
    assert not mode & stat.S_IWOTH
    assert "mycluster" in system_stack().layers.files


def test_the_detection_line_when_detection_misses(
    user_root: Path, draft: Callable[..., Path], detected: Callable[[bool], None]
) -> None:
    detected(False)

    result = add(draft())

    assert "cstar env set CSTAR_SYSTEM mycluster" in flat(result)
    assert "C-Star selects" not in flat(result)


def test_a_bundled_name_is_shadowed(
    user_root: Path, draft: Callable[..., Path]
) -> None:
    result = add(draft("perlmutter", bundled="anvil"))

    assert result.exit_code == 0, result.output
    assert f"It shadows {display_path(BUNDLED_SYSTEMS / 'perlmutter.yaml')}" in flat(
        result
    )


def test_a_world_writable_systems_directory_is_refused(
    user_root: Path, draft: Callable[..., Path]
) -> None:
    directory = systems_dir(user_root)
    directory.mkdir(parents=True)
    directory.chmod(0o777)

    result = add(draft())

    assert result.exit_code == 1
    assert "writable by every user" in flat(result)
    assert not (directory / "mycluster.yaml").exists()


def test_a_remote_first_entry_needs_a_catalog_option(
    draft: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV_CSTAR_CATALOG, "https://github.com/a/b")

    result = add(draft())

    assert result.exit_code == 1
    assert "--catalog" in flat(result)
