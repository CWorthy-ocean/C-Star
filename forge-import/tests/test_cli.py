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
        assert argv[1].endswith("ui/_voila_app.ipynb")
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


class TestCopyNotebook:
    @staticmethod
    def _packaged() -> bytes:
        from importlib.resources import files

        return (files("cstar_forge") / "forge-blueprint-wizard.ipynb").read_bytes()

    def test_copies_packaged_notebook_to_dest(self, tmp_path):
        dest = tmp_path / "nested" / "wizard.ipynb"
        result = runner.invoke(cli.app, ["copy-notebook", "--dest", str(dest)])
        assert result.exit_code == 0
        assert dest.read_bytes() == self._packaged()
        assert not dest.is_symlink()
        assert str(dest) in result.output

    def test_identical_existing_copy_is_a_noop(self, tmp_path):
        dest = tmp_path / "wizard.ipynb"
        dest.write_bytes(self._packaged())
        result = runner.invoke(cli.app, ["copy-notebook", "--dest", str(dest)])
        assert result.exit_code == 0
        assert "Already up to date" in result.output

    def test_modified_existing_copy_requires_force(self, tmp_path):
        dest = tmp_path / "wizard.ipynb"
        dest.write_bytes(b"user edits")
        result = runner.invoke(cli.app, ["copy-notebook", "--dest", str(dest)])
        assert result.exit_code == 1
        assert "--force" in result.output
        assert dest.read_bytes() == b"user edits"  # untouched

    def test_force_overwrites_modified_copy(self, tmp_path):
        dest = tmp_path / "wizard.ipynb"
        dest.write_bytes(b"user edits")
        result = runner.invoke(
            cli.app, ["copy-notebook", "--dest", str(dest), "--force"]
        )
        assert result.exit_code == 0
        assert dest.read_bytes() == self._packaged()

    def test_symlink_dest_is_replaced_by_real_copy_only_with_force(self, tmp_path):
        # A pre-existing symlink (e.g. someone's manual shortcut into
        # site-packages) must never be written through — that would push
        # bytes into the installed package.
        link_target = tmp_path / "target.ipynb"
        link_target.write_bytes(b"original target bytes")
        dest = tmp_path / "wizard.ipynb"
        dest.symlink_to(link_target)

        result = runner.invoke(cli.app, ["copy-notebook", "--dest", str(dest)])
        assert result.exit_code == 1
        assert "symlink" in result.output

        result = runner.invoke(
            cli.app, ["copy-notebook", "--dest", str(dest), "--force"]
        )
        assert result.exit_code == 0
        assert not dest.is_symlink()
        assert dest.read_bytes() == self._packaged()
        assert link_target.read_bytes() == b"original target bytes"

    def test_dest_directory_errors(self, tmp_path):
        result = runner.invoke(cli.app, ["copy-notebook", "--dest", str(tmp_path)])
        assert result.exit_code == 1
        assert "directory" in result.output

    def test_default_dest_is_under_home_cstar(self):
        result = runner.invoke(cli.app, ["copy-notebook", "--help"])
        assert "~/cstar/forge-blueprint-wizard.ipynb" in result.output


class TestRegisterKernel:
    def test_options_map_to_register_kernel_kwargs(self):
        with patch("cstar_forge.register_kernel.register_kernel") as mock_register:
            result = runner.invoke(
                cli.app,
                [
                    "register-kernel",
                    "--name",
                    "my-kernel",
                    "--clean",
                    "--package-manager",
                    "micromamba",
                    "--micromamba-bin",
                    "/repo/bin/micromamba",
                ],
            )
        assert result.exit_code == 0
        kwargs = mock_register.call_args.kwargs
        assert kwargs["name"] == "my-kernel"
        assert kwargs["display_name"] is None
        assert kwargs["clean"] is True
        assert kwargs["package_manager"] == "micromamba"
        assert kwargs["micromamba_bin"] == "/repo/bin/micromamba"

    def test_defaults(self):
        with patch("cstar_forge.register_kernel.register_kernel") as mock_register:
            result = runner.invoke(cli.app, ["register-kernel"])
        assert result.exit_code == 0
        kwargs = mock_register.call_args.kwargs
        assert kwargs["name"] is None
        assert kwargs["clean"] is False
        assert kwargs["package_manager"] == "auto"

    def test_register_kernel_error_exits_nonzero_with_message(self):
        from cstar_forge.register_kernel import RegisterKernelError

        with patch(
            "cstar_forge.register_kernel.register_kernel",
            side_effect=RegisterKernelError("not inside a conda env"),
        ):
            result = runner.invoke(cli.app, ["register-kernel"])
        assert result.exit_code == 1
        assert "not inside a conda env" in result.output


class TestShowPaths:
    def test_human_readable_output(self):
        result = runner.invoke(cli.app, ["show-paths"])
        assert result.exit_code == 0
        assert "System tag :" in result.output
        assert "Paths:" in result.output

    def test_json_flag_emits_parseable_json(self):
        import json

        result = runner.invoke(cli.app, ["show-paths", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert {"system", "hostname", "paths"} <= payload.keys()
        assert isinstance(payload["paths"], dict)

    def test_delegates_to_config_format_paths(self):
        with patch(
            "cstar_forge.config.format_paths", return_value="SENTINEL"
        ) as mock_fmt:
            result = runner.invoke(cli.app, ["show-paths"])
        assert result.exit_code == 0
        assert "SENTINEL" in result.output
        mock_fmt.assert_called_once_with(as_json=False)


class TestImportCost:
    def test_plugin_import_does_not_load_scientific_stack(self):
        # C-Star ``ep.load()``s the ``cstar.cli`` plugin on *every* ``cstar``
        # invocation, so importing ``cstar_forge.cli`` (and hence the package
        # ``__init__``) must stay cheap: no roms-tools / xarray / dask. Those are
        # resolved lazily via PEP 562 ``__getattr__`` in ``cstar_forge/__init__``.
        # Run in a subprocess so this process's already-imported modules don't
        # mask a regression.
        import subprocess
        import sys

        code = (
            "import sys, cstar_forge.cli; "
            "heavy = sorted(m for m in ('roms_tools', 'xarray', 'dask', "
            "'copernicusmarine', 'cstar_forge.forge.source_data', "
            "'cstar_forge.forge.executor') if m in sys.modules); "
            "print(','.join(heavy))"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )
        assert result.stdout.strip() == "", (
            f"importing cstar_forge.cli pulled in: {result.stdout.strip()}"
        )


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
