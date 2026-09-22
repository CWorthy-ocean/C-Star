"""Tests for the `cstar forge` CLI sub-app (cstar/cli/forge/__init__.py)."""

import re
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import cstar.cli.forge as cli

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
        assert "python -m cstar.applications.forge.runtime" not in output

    def test_options_map_to_run_blueprint_kwargs(self):
        with patch(
            "cstar.applications.forge.runtime.run_blueprint", return_value=0
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
            "cstar.applications.forge.runtime.run_blueprint", return_value=0
        ) as mock_run_blueprint:
            result = runner.invoke(cli.app, ["run", "bp.yaml"])
        assert result.exit_code == 0, result.output
        assert mock_run_blueprint.call_args.kwargs["only_inputs"] is None

    def test_exit_code_is_propagated(self):
        with patch("cstar.applications.forge.runtime.run_blueprint", return_value=3):
            result = runner.invoke(cli.app, ["run", "bp.yaml"])
        assert result.exit_code == 3


class TestWizard:
    def test_builds_voila_argv_with_default_port(self):
        with patch.object(cli, "_exec_voila") as mock_exec:
            result = runner.invoke(cli.app, ["wizard"])
        assert result.exit_code == 0
        argv = mock_exec.call_args.args[0]
        assert argv[0] == "voila"
        assert argv[1].endswith("wizard/_voila_app.ipynb")
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

        return (files("cstar.wizard") / "forge-blueprint-wizard.ipynb").read_bytes()

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
            "cstar.applications.forge.config.format_paths", return_value="SENTINEL"
        ) as mock_fmt:
            result = runner.invoke(cli.app, ["show-paths"])
        assert result.exit_code == 0
        assert "SENTINEL" in result.output
        mock_fmt.assert_called_once_with(as_json=False)


class TestImportCost:
    def test_plugin_import_does_not_load_scientific_stack(self):
        # ``cstar.cli.cli`` imports ``cstar.cli.forge`` directly on *every*
        # ``cstar`` invocation (it's a core subcommand now, not a lazily
        # loaded ``cstar.cli`` entry-point plugin), so importing it must stay
        # cheap: no roms-tools / xarray / dask. Those stay behind lazy,
        # in-function imports in the command bodies (see ``run`` and
        # ``show_paths`` above).
        # Run in a subprocess so this process's already-imported modules don't
        # mask a regression.
        import subprocess
        import sys

        code = (
            "import sys, cstar.cli.forge; "
            "heavy = sorted(m for m in ('roms_tools', 'xarray', 'dask', "
            "'copernicusmarine', 'cstar.applications.forge.source_datasets', "
            "'cstar.applications.forge.executor') if m in sys.modules); "
            "print(','.join(heavy))"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )
        assert result.stdout.strip() == "", (
            f"importing cstar.cli.forge pulled in: {result.stdout.strip()}"
        )


class TestCoreSubcommandRegistration:
    # Forge is in-tree now: both the `cstar forge` CLI group and the `forge`
    # application register directly (no `cstar.cli` / `cstar.applications`
    # entry points remain in pyproject.toml for either).

    def test_forge_is_attached_as_a_core_subcommand(self):
        from cstar.cli.cli import app as root_app

        names = {g.name for g in root_app.registered_groups}
        assert "forge" in names

    def test_root_help_lists_forge(self):
        from cstar.cli.cli import app as root_app

        result = runner.invoke(root_app, ["--help"])
        assert result.exit_code == 0
        assert "forge" in _plain(result.output)

    def test_registered_app_module_registers_the_forge_application(self):
        # `cstar blueprint run <forge_blueprint.yaml>` resolves `application:
        # forge` by importing `cstar.applications.forge.app`, whose import
        # runs `@register_application`. A module path that imports cleanly
        # but registers nothing (or under a different name) would leave that
        # resolution broken.
        import importlib

        core = pytest.importorskip("cstar.applications.core")
        importlib.import_module("cstar.applications.forge.app")
        assert "forge" in core._registry
