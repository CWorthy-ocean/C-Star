"""Tests for the ``cstar --version`` output and the common CLI callback."""

from importlib.metadata import PackageNotFoundError
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

import cstar
from cstar.base.env import REMOVED_SETTINGS
from cstar.cli.common import common_callback, exit_on_refused_settings
from cstar.roms.namelist import NEWEST_KNOWN_UCLA_ROMS, format_roms_version

NEWEST_KNOWN_LINE = (
    f"Newest known ucla-roms release: {format_roms_version(NEWEST_KNOWN_UCLA_ROMS)}"
)


def make_app() -> typer.Typer:
    app = typer.Typer(callback=common_callback)

    @app.command()
    def noop() -> None:
        pass

    return app


def fake_pkg_version(installed: dict[str, str]):
    def _version(pkg: str) -> str:
        try:
            return installed[pkg]
        except KeyError:
            raise PackageNotFoundError(pkg) from None

    return _version


def test_version_all_packages_installed() -> None:
    """All lines appear, in order, when the companion packages are installed,
    ending with the newest ucla-roms release this C-Star knows.
    """
    runner = CliRunner()
    installed = {"roms-tools": "4.5.6"}

    with mock.patch(
        "cstar.cli.common._pkg_version", side_effect=fake_pkg_version(installed)
    ):
        result = runner.invoke(make_app(), ["--version"], color=False)

    assert result.exit_code == 0
    lines = result.stdout.strip().splitlines()
    assert lines[0].startswith("cstar executable location: ")
    assert lines[1] == f"C-Star version: {cstar.__version__}"
    assert lines[2] == "roms-tools version: 4.5.6"
    assert lines[3] == NEWEST_KNOWN_LINE


def test_version_companion_packages_missing() -> None:
    """Lines for uninstalled companion packages are omitted; the newest known
    ucla-roms release is always printed.
    """
    runner = CliRunner()

    with mock.patch("cstar.cli.common._pkg_version", side_effect=fake_pkg_version({})):
        result = runner.invoke(make_app(), ["--version"], color=False)

    assert result.exit_code == 0
    lines = result.stdout.strip().splitlines()
    assert lines[0].startswith("cstar executable location: ")
    assert lines[1] == f"C-Star version: {cstar.__version__}"
    assert lines[2] == NEWEST_KNOWN_LINE
    assert len(lines) == 3


@pytest.mark.parametrize("name", sorted(REMOVED_SETTINGS))
def test_removed_setting_warns_on_stderr(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """An exported removed setting is reported on stderr, with its guidance,
    and never on stdout; the command still runs.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to export the setting.
    name : str
        The name of a removed setting.
    """
    monkeypatch.setenv(name, "1")

    result = CliRunner().invoke(make_app(), ["noop"], color=False)

    assert result.exit_code == 0
    assert f"{name} has no effect; {REMOVED_SETTINGS[name].guidance}" in result.stderr
    assert "Refusing to run" not in result.stderr
    assert name not in result.stdout


@pytest.mark.parametrize("name", sorted(REMOVED_SETTINGS))
def test_exit_on_refused_settings(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """A command checking for refused settings stops with exit code 2 while a
    refused removed setting is exported, and carries on for any other.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to export the setting.
    name : str
        The name of a removed setting.
    """
    monkeypatch.setenv(name, "1")
    ran: list[str] = []
    app = typer.Typer()

    @app.command()
    def noop() -> None:
        exit_on_refused_settings()
        ran.append("noop")

    result = CliRunner().invoke(app, [], color=False)

    refused = REMOVED_SETTINGS[name].refused
    assert result.exit_code == (2 if refused else 0)
    assert ("Refusing to run" in result.stderr) is refused
    assert bool(ran) is not refused


def test_no_warning_without_removed_settings() -> None:
    """Nothing is written to stderr when no removed setting is exported."""
    result = CliRunner().invoke(make_app(), ["noop"], color=False)

    assert result.exit_code == 0
    assert not result.stderr
