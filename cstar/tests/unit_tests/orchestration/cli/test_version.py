"""Tests for the ``cstar --version`` output."""

from importlib.metadata import PackageNotFoundError
from unittest import mock

import typer
from typer.testing import CliRunner

import cstar
from cstar.cli.common import common_callback


def make_app() -> typer.Typer:
    app = typer.Typer(callback=common_callback)

    @app.command()
    def noop() -> None:  # pragma: no cover - never invoked with --version
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
    """All lines appear, in order, when the companion packages are installed."""
    runner = CliRunner()
    installed = {"cstar-forge": "1.2.3", "roms-tools": "4.5.6"}

    with mock.patch(
        "cstar.cli.common._pkg_version", side_effect=fake_pkg_version(installed)
    ):
        result = runner.invoke(make_app(), ["--version"], color=False)

    assert result.exit_code == 0
    lines = result.stdout.strip().splitlines()
    assert lines[0].startswith("cstar executable location: ")
    assert lines[1] == f"C-Star version: {cstar.__version__}"
    assert lines[2] == "cstar-forge version: 1.2.3"
    assert lines[3] == "roms-tools version: 4.5.6"


def test_version_companion_packages_missing() -> None:
    """Lines for uninstalled companion packages are omitted."""
    runner = CliRunner()

    with mock.patch("cstar.cli.common._pkg_version", side_effect=fake_pkg_version({})):
        result = runner.invoke(make_app(), ["--version"], color=False)

    assert result.exit_code == 0
    lines = result.stdout.strip().splitlines()
    assert lines[0].startswith("cstar executable location: ")
    assert lines[1] == f"C-Star version: {cstar.__version__}"
    assert len(lines) == 2
