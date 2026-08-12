"""Tests for the `cstar forge` CLI sub-app (cstar_forge/cli.py)."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import cstar_forge.cli as cli

runner = CliRunner()


class TestRunPassthrough:
    def test_args_are_passed_through_verbatim(self):
        argv = ["some_blueprint.yaml", "--clobber", "--only-inputs", "grid", "tidal"]
        with patch("cstar_forge.run.main", return_value=0) as mock_main:
            result = runner.invoke(cli.app, ["run", *argv])
        assert result.exit_code == 0
        mock_main.assert_called_once_with(argv, prog="cstar forge run")

    def test_exit_code_is_propagated(self):
        with patch("cstar_forge.run.main", return_value=3):
            result = runner.invoke(cli.app, ["run", "bp.yaml"])
        assert result.exit_code == 3

    def test_help_flag_reaches_argparse_not_typer(self):
        # --help must reach the executor's argparse (which lists the real
        # options and SystemExits), not be swallowed by typer's own help.
        result = runner.invoke(cli.app, ["run", "--help"])
        assert "--only-inputs" in result.output
        # ...and the usage line names the command the user actually typed.
        assert "cstar forge run" in result.output
        assert "python -m cstar_forge.run" not in result.output


class TestWizard:
    def test_builds_voila_argv_with_default_port(self):
        with patch.object(cli, "_exec_voila") as mock_exec:
            result = runner.invoke(cli.app, ["wizard"])
        assert result.exit_code == 0
        argv = mock_exec.call_args.args[0]
        assert argv[0] == "voila"
        assert argv[1].endswith("forge-blueprint-wizard-app.ipynb")
        assert "--port=8866" in argv

    def test_port_option_and_extra_args_forwarded(self):
        with patch.object(cli, "_exec_voila") as mock_exec:
            result = runner.invoke(
                cli.app, ["wizard", "--port", "9999", "--no-browser"]
            )
        assert result.exit_code == 0
        argv = mock_exec.call_args.args[0]
        assert "--port=9999" in argv
        assert "--no-browser" in argv

    def test_missing_voila_exits_nonzero_with_hint(self):
        with patch.object(cli.shutil, "which", return_value=None):
            result = runner.invoke(cli.app, ["wizard"])
        assert result.exit_code == 1
        assert "voila is not installed" in result.output


class TestEntryPointRegistration:
    def test_pyproject_registers_cstar_cli_entry_point(self):
        # The metadata contract with C-Star's discovery hook: group cstar.cli,
        # name forge, target cstar_forge.cli:app.
        import pathlib

        import cstar_forge

        pyproject = pathlib.Path(cstar_forge.__file__).parents[1] / "pyproject.toml"
        if not pyproject.is_file():
            pytest.skip("no source checkout (installed package)")
        text = pyproject.read_text()
        assert '[project.entry-points."cstar.cli"]' in text
        assert 'forge = "cstar_forge.cli:app"' in text

    def test_pyproject_registers_cstar_applications_entry_point(self):
        # The metadata contract with C-Star's application registry: group
        # cstar.applications, name forge (the blueprint's `application` value),
        # target a bare module path C-Star imports so @register_application runs.
        # This is the only mechanism C-Star offers for out-of-tree applications:
        # without it, `cstar blueprint run <forge_blueprint.yaml>` cannot resolve
        # `application: forge` at all.
        import pathlib

        import cstar_forge

        pyproject = pathlib.Path(cstar_forge.__file__).parents[1] / "pyproject.toml"
        if not pyproject.is_file():
            pytest.skip("no source checkout (installed package)")
        text = pyproject.read_text()
        assert '[project.entry-points."cstar.applications"]' in text
        assert 'forge = "cstar_forge.forge.app"' in text

    def test_registered_app_module_registers_the_forge_application(self):
        # The entry-point target must be a module whose import registers `forge`
        # in C-Star's registry -- a valid module path that registers nothing (or
        # under a different name) would satisfy the metadata check above while
        # leaving `cstar blueprint run` unable to resolve a forge blueprint.
        import importlib

        core = pytest.importorskip("cstar.applications.core")
        importlib.import_module("cstar_forge.forge.app")
        assert "forge" in core._registry
