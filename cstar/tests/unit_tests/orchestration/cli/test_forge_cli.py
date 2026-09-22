"""Tests for the `cstar forge` CLI sub-app (cstar_forge/cli.py)."""

import re
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from cstar_forge import cli

runner = CliRunner()

# rich colours the help when the environment forces colour (GitHub Actions does),
# and its option highlighter emits style changes inside a flag name, so assertions
# on help text compare against the escape-stripped output.
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI.sub("", text)


class TestRun:
    def test_help_lists_every_option(self):
        # Wide terminal: rich's default 80-column help renderer truncates long
        # flag names (e.g. "--serialize-dask-write" -> "--serialize-dask-wr…").
        result = runner.invoke(
            cli.app, ["run", "--help"], env={"COLUMNS": "250", "LINES": "50"}
        )
        assert result.exit_code == 0
        output = _plain(result.output)
        for option in (
            "--no-data",
            "--no-generate",
            "--no-configure",
            "--clobber",
            "--no-dask",
            "--dask-num-workers",
            "--serialize-dask-write",
            "--no-serialize-dask-write",
            "--subchunk",
            "--no-subchunk",
            "--only-inputs",
            "--host-only",
            "--verbose",
            "--working-dir",
            "--dask",
            "--dask-workers",
            "--dask-threads-per-worker",
            "--dask-memory-limit",
            "--dask-processes",
            "--no-dask-processes",
            "--dask-dashboard-address",
        ):
            assert option in output, option
        assert "python -m cstar_forge.run" not in output

    def test_options_map_to_run_blueprint_kwargs(self):
        with patch(
            "cstar_forge.run.run_blueprint", return_value=0
        ) as mock_run_blueprint:
            result = runner.invoke(
                cli.app,
                [
                    "run",
                    "bp.yaml",
                    "--clobber",
                    "--only-inputs",
                    "grid,surface",
                    "--only-inputs",
                    "tidal",
                    "--dask-num-workers",
                    "4",
                    "--no-serialize-dask-write",
                ],
            )
        assert result.exit_code == 0, result.output
        kwargs = mock_run_blueprint.call_args.kwargs
        assert kwargs["forge_blueprint"] == "bp.yaml"
        assert kwargs["clobber"] is True
        assert kwargs["only_inputs"] == ["grid", "surface", "tidal"]
        assert kwargs["dask_num_workers"] == 4
        assert kwargs["serialize_dask_write"] is False
        assert kwargs["subchunk"] is True
        assert kwargs["no_data"] is False
        assert kwargs["no_generate"] is False
        assert kwargs["no_configure"] is False
        assert kwargs["no_dask"] is False
        assert kwargs["host_only"] is False
        assert kwargs["verbose"] is False
        assert kwargs["working_dir"] is None
        assert kwargs["dask"] is False
        assert kwargs["dask_workers"] is None
        assert kwargs["dask_threads_per_worker"] is None
        assert kwargs["dask_memory_limit"] is None
        assert kwargs["dask_processes"] is None
        assert kwargs["dask_dashboard_address"] is None

    def test_no_only_inputs_passes_none(self):
        with patch(
            "cstar_forge.run.run_blueprint", return_value=0
        ) as mock_run_blueprint:
            result = runner.invoke(cli.app, ["run", "bp.yaml"])
        assert result.exit_code == 0, result.output
        assert mock_run_blueprint.call_args.kwargs["only_inputs"] is None

    def test_exit_code_is_propagated(self):
        with patch("cstar_forge.run.run_blueprint", return_value=3):
            result = runner.invoke(cli.app, ["run", "bp.yaml"])
        assert result.exit_code == 3


class TestWizard:
    def test_builds_voila_argv_with_default_port(self):
        with patch.object(cli, "_exec_voila") as mock_exec:
            result = runner.invoke(cli.app, ["wizard"])
        assert result.exit_code == 0
        argv = mock_exec.call_args.args[0]
        assert argv[0] == "voila"
        assert argv[1].endswith("ui/_voila_app.ipynb")
        assert "--port=8866" in argv

    def test_denies_notebook_labextension(self):
        # voila 0.5.12 bundles JupyterLab 4.2.5; notebook 7.x's labextension is
        # built against 4.4+/4.6, and loading it crashes voila's frontend
        # bundle (blank page + kernel-websocket 404s). It must stay denied, and
        # must come before the pass-through args so a caller can override it.
        with patch.object(cli, "_exec_voila") as mock_exec:
            result = runner.invoke(cli.app, ["wizard"])
        assert result.exit_code == 0
        argv = mock_exec.call_args.args[0]
        denylist = [a for a in argv if "extension_denylist" in a]
        assert denylist == [
            '--VoilaConfiguration.extension_denylist=["@jupyter-notebook/lab-extension"]'
        ]

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
            "'copernicusmarine', 'cstar_forge.forge.source_datasets', "
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
