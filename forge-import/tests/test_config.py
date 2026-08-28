"""
Tests for the config.py module.

Tests cover:
- DataPaths dataclass
- System detection functions
- Path resolution and layout functions
- CLI functionality
"""

import json
import os
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import patch

import pytest

import cstar_forge.config as config_module
from cstar_forge.config import (
    SYSTEM_LAYOUT_REGISTRY,
    DataPaths,
    _default_cluster_type,
    _detect_system,
    _get_hostname,
    get_data_paths,
    main,
    register_system,
    with_catalog,
)
from cstar_forge.domain_catalog import user_catalog_root


class TestDataPaths:
    """Tests for DataPaths dataclass."""

    def test_datapaths_creation(self, tmp_path):
        """Test creating DataPaths with all required fields."""
        cat = tmp_path / "catalog"
        paths = DataPaths(
            here=tmp_path,
            source_data=tmp_path / "source-data",
            input_data=tmp_path / "input-data",
            scratch=tmp_path / "run-dir",
            catalog=cat,
            blueprints=cat / "blueprints",
            models_yaml=tmp_path / "models.yaml",
            builds_yaml=tmp_path / "builds.yaml",
        )

        assert paths.here == tmp_path
        assert paths.source_data == tmp_path / "source-data"
        assert paths.input_data == tmp_path / "input-data"
        assert paths.scratch == tmp_path / "run-dir"
        assert paths.catalog == cat
        assert paths.blueprints == cat / "blueprints"
        assert paths.models_yaml == tmp_path / "models.yaml"
        assert paths.builds_yaml == tmp_path / "builds.yaml"

    def test_datapaths_frozen(self, tmp_path):
        """Test that DataPaths is frozen (immutable)."""
        cat = tmp_path / "catalog"
        paths = DataPaths(
            here=tmp_path,
            source_data=tmp_path / "source-data",
            input_data=tmp_path / "input-data",
            scratch=tmp_path / "run-dir",
            catalog=cat,
            blueprints=cat / "blueprints",
            models_yaml=tmp_path / "models.yaml",
            builds_yaml=tmp_path / "builds.yaml",
        )

        with pytest.raises(FrozenInstanceError):
            paths.here = tmp_path / "new"

    def test_with_catalog(self, tmp_path):
        """Relocating catalog updates blueprints."""
        cat = tmp_path / "catalog"
        paths = DataPaths(
            here=tmp_path,
            source_data=tmp_path / "source-data",
            input_data=tmp_path / "input-data",
            scratch=tmp_path / "run-dir",
            catalog=cat,
            blueprints=cat / "blueprints",
            models_yaml=tmp_path / "models.yaml",
            builds_yaml=tmp_path / "builds.yaml",
        )
        other = tmp_path / "other_catalog"
        moved = with_catalog(paths, other)
        assert moved.catalog == other
        assert moved.blueprints == other / "blueprints"
        assert moved.here == paths.here


# NB: catalog_root anchoring (resolve_catalog_dir) was removed with the executor's
# config/catalog decoupling — the forge app writes under the injected host.working_dir.


class TestUserCatalogRoot:
    """Tests for domain_catalog.user_catalog_root (the writable catalog layer's
    root), imported here because config.paths.catalog is now just
    ``user_catalog_root()``.
    """

    def test_env_override_uses_first_pathsep_entry(self, monkeypatch, tmp_path):
        first = tmp_path / "first-catalog"
        second = tmp_path / "second-catalog"
        monkeypatch.setenv(
            "CSTAR_FORGE_CATALOG", os.pathsep.join([str(first), str(second)])
        )
        assert user_catalog_root() == first.expanduser().resolve()

    def test_env_override_single_entry(self, monkeypatch, tmp_path):
        entry = tmp_path / "only-catalog"
        monkeypatch.setenv("CSTAR_FORGE_CATALOG", str(entry))
        assert user_catalog_root() == entry.expanduser().resolve()

    def test_default_is_home_anchored_when_env_unset(self, monkeypatch, tmp_path):
        # conftest.py forces CSTAR_FORGE_CATALOG globally for test isolation, so
        # this test must monkeypatch (auto-undone), never delete it globally.
        monkeypatch.delenv("CSTAR_FORGE_CATALOG", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert user_catalog_root() == tmp_path / "cstar-forge-data" / "catalog"

    def test_does_not_create_the_directory(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CSTAR_FORGE_CATALOG", str(tmp_path / "not-yet-created"))
        result = user_catalog_root()
        assert not result.exists()


class TestSystemDetection:
    """Tests for system detection functions."""

    @patch("cstar_forge.config.platform.system")
    @patch("cstar_forge.config._get_hostname")
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_macos(self, mock_hostname, mock_system):
        """Test system detection for MacOS."""
        mock_system.return_value = "Darwin"
        result = _detect_system()
        assert result == "MacOS"

    @patch("cstar_forge.config.platform.system")
    @patch("cstar_forge.config._get_hostname")
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_anvil(self, mock_hostname, mock_system):
        """Test system detection for RCAC Anvil."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "anvil-login01"
        result = _detect_system()
        assert result == "RCAC_anvil"

    @patch("cstar_forge.config.platform.system")
    @patch("cstar_forge.config._get_hostname")
    @patch.dict(os.environ, {"NERSC_HOST": "perlmutter"})
    def test_detect_system_perlmutter(self, mock_hostname, mock_system):
        """Test system detection for NERSC Perlmutter."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "unknown"
        result = _detect_system()
        assert result == "NERSC_perlmutter"

    @patch("cstar_forge.config.platform.system")
    @patch("cstar_forge.config._get_hostname")
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_unknown(self, mock_hostname, mock_system):
        """Test system detection for unknown system."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "unknown-host"
        result = _detect_system()
        assert result == "unknown"

    @patch.dict(os.environ, {"HOSTNAME": "test-host"})
    @patch("cstar_forge.config.socket.gethostname", return_value="")
    @patch("cstar_forge.config.platform.node", return_value="")
    def test_get_hostname_from_env(self, mock_node, mock_gethostname):
        """Test getting hostname from HOSTNAME environment variable."""
        result = _get_hostname()
        assert result == "test-host"
        mock_gethostname.assert_called_once()
        mock_node.assert_called_once()

    @patch.dict(os.environ, {}, clear=True)
    @patch("cstar_forge.config.socket.gethostname")
    @patch("cstar_forge.config.platform.node")
    def test_get_hostname_from_socket(self, mock_node, mock_gethostname):
        """Test getting hostname from socket.gethostname()."""
        mock_gethostname.return_value = "socket-host"
        mock_node.return_value = "platform-host"
        result = _get_hostname()
        assert result == "socket-host"
        mock_node.assert_not_called()

    @patch.dict(os.environ, {}, clear=True)
    @patch("cstar_forge.config.socket.gethostname")
    @patch("cstar_forge.config.platform.node")
    def test_get_hostname_from_platform(self, mock_node, mock_gethostname):
        """Test getting hostname from platform.node() as fallback."""
        mock_gethostname.return_value = None
        mock_node.return_value = "platform-host"
        result = _get_hostname()
        assert result == "platform-host"

    @patch.dict(os.environ, {}, clear=True)
    @patch("cstar_forge.config.socket.gethostname")
    @patch("cstar_forge.config.platform.node")
    def test_get_hostname_unknown(self, mock_node, mock_gethostname):
        """Test getting hostname when all methods fail."""
        mock_gethostname.return_value = None
        mock_node.return_value = None
        result = _get_hostname()
        assert result == "unknown"


class TestSystemLayoutRegistry:
    """Tests for system layout registry."""

    def test_system_layout_registry_has_defaults(self):
        """Test that default system layouts are registered."""
        assert "MacOS" in SYSTEM_LAYOUT_REGISTRY
        assert "RCAC_anvil" in SYSTEM_LAYOUT_REGISTRY
        assert "NERSC_perlmutter" in SYSTEM_LAYOUT_REGISTRY
        assert "unknown" in SYSTEM_LAYOUT_REGISTRY

    def test_register_system_decorator(self):
        """Test registering a custom system layout."""

        @register_system("test_system")
        def test_layout(home: Path, env: dict):
            return (
                home / "test-source",
                home / "test-input",
                home / "test-run",
                home / "test-code",
            )

        assert "test_system" in SYSTEM_LAYOUT_REGISTRY
        assert SYSTEM_LAYOUT_REGISTRY["test_system"] == test_layout

        # Clean up
        del SYSTEM_LAYOUT_REGISTRY["test_system"]

    def test_macos_layout(self, tmp_path):
        """Test MacOS layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["MacOS"]
        source_data, input_data, scratch = layout_fn(tmp_path, {})

        assert source_data == tmp_path / "cstar-forge-data" / "source-data"
        assert input_data == tmp_path / "cstar-forge-data" / "input-data"
        assert scratch == tmp_path / "cstar" / "_forge_bp_runs"

    def test_unknown_layout(self, tmp_path):
        """Test unknown layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["unknown"]
        source_data, input_data, scratch = layout_fn(tmp_path, {})

        assert source_data == tmp_path / "cstar-forge-data" / "source-data"
        assert input_data == tmp_path / "cstar-forge-data" / "input-data"
        assert scratch == tmp_path / "cstar" / "_forge_bp_runs"

    def test_anvil_layout(self, tmp_path):
        """Test RCAC Anvil layout function."""
        from cstar_forge.config import USER

        layout_fn = SYSTEM_LAYOUT_REGISTRY["RCAC_anvil"]
        env = {"WORK": str(tmp_path / "work"), "SCRATCH": str(tmp_path / "scratch")}
        source_data, input_data, scratch = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "work" / "cstar-forge-data" / "source-data"
        assert (
            input_data == tmp_path / "work" / "cstar-forge-data" / USER / "input-data"
        )
        assert scratch == tmp_path / "scratch" / "cstar" / "_forge_bp_runs"

    def test_perlmutter_layout(self, tmp_path):
        """Test NERSC Perlmutter layout function."""
        from cstar_forge.config import USER

        layout_fn = SYSTEM_LAYOUT_REGISTRY["NERSC_perlmutter"]
        env = {"SCRATCH": str(tmp_path / "scratch")}
        source_data, input_data, scratch = layout_fn(tmp_path, env)

        assert source_data == tmp_path / "scratch" / "cstar-forge-data" / "source-data"
        assert (
            input_data
            == tmp_path / "scratch" / "cstar-forge-data" / USER / "input-data"
        )
        assert scratch == tmp_path / "scratch" / "cstar" / "_forge_bp_runs"


class TestRelocateWorkingDir:
    """Tests for relocate_working_dir (default-form paths rebase onto HPC scratch)."""

    def test_default_path_rebases_to_scratch_on_perlmutter(self, tmp_path):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        env = {"SCRATCH": str(tmp_path / "scratch")}
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="NERSC_perlmutter",
            env=env,
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_default_path_rebases_to_scratch_on_anvil(self, tmp_path):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        env = {"WORK": str(tmp_path / "work"), "SCRATCH": str(tmp_path / "scratch")}
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="RCAC_anvil",
            env=env,
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_anvil_falls_back_to_work_scratch(self, tmp_path):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        env = {"WORK": str(tmp_path / "work")}
        wd = relocate_working_dir(
            home / "cstar" / "_forge_bp_runs" / "my-run",
            system_tag="RCAC_anvil",
            env=env,
            home=home,
        )
        assert (
            wd == tmp_path / "work" / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"
        )

    def test_legacy_cstar_forge_run_root_rebases_to_scratch(self, tmp_path):
        """The legacy sentinel (``~/cstar-forge-run``, the default before this
        rename) rebases onto the *current* scratch working root, so old
        blueprints no longer write into the old sibling location on HPC.
        """
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-run" / "my-run",
            system_tag="NERSC_perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_non_hpc_leaves_path_alone(self, tmp_path):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-run" / "my-run",
            system_tag="MacOS",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == home / "cstar-forge-run" / "my-run"

    def test_custom_path_passes_through_on_hpc(self, tmp_path):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        custom = tmp_path / "elsewhere" / "my-run"
        wd = relocate_working_dir(
            custom,
            system_tag="NERSC_perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == custom

    def test_legacy_default_root_rebases_to_scratch(self, tmp_path):
        """The legacy sentinel (``~/cstar-forge-data/cstar-forge-run``, from blueprints
        authored before the default was renamed) rebases onto the *current* scratch
        working root, so old blueprints no longer write into home on HPC.
        """
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        wd = relocate_working_dir(
            home / "cstar-forge-data" / "cstar-forge-run" / "my-run",
            system_tag="NERSC_perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"

    def test_bare_cstar_forge_data_path_passes_through(self, tmp_path):
        """The legacy match is deliberately narrow: only the nested
        ``cstar-forge-data/cstar-forge-run`` sentinel rebases. A bare path under
        ``~/cstar-forge-data`` (which is also the mac/unknown source_data and
        input_data base) is a user choice and passes through untouched.
        """
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        custom = home / "cstar-forge-data" / "my-hand-picked-run"
        wd = relocate_working_dir(
            custom,
            system_tag="NERSC_perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == custom

    def test_home_rooted_nondefault_warns_on_hpc(self, tmp_path, caplog):
        """A home-rooted path that matches no default root is left in home on HPC;
        warn so an unrelocated (e.g. very old default) run doesn't go unnoticed.
        """
        import logging

        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        custom = home / "cstar-forge-data" / "my-hand-picked-run"
        with caplog.at_level(logging.WARNING, logger="cstar_forge.config"):
            wd = relocate_working_dir(
                custom,
                system_tag="NERSC_perlmutter",
                env={"SCRATCH": str(tmp_path / "scratch")},
                home=home,
            )
        assert wd == custom
        assert "was not relocated to scratch" in caplog.text

    def test_off_home_custom_path_does_not_warn(self, tmp_path, caplog):
        """A deliberate path outside home is normal and must not warn."""
        import logging

        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        custom = tmp_path / "elsewhere" / "my-run"
        with caplog.at_level(logging.WARNING, logger="cstar_forge.config"):
            wd = relocate_working_dir(
                custom,
                system_tag="NERSC_perlmutter",
                env={"SCRATCH": str(tmp_path / "scratch")},
                home=home,
            )
        assert wd == custom
        assert caplog.text == ""

    def test_tilde_default_expands_then_rebases(self, tmp_path, monkeypatch):
        from cstar_forge.config import relocate_working_dir

        home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(home))
        wd = relocate_working_dir(
            "~/cstar/_forge_bp_runs/my-run",
            system_tag="NERSC_perlmutter",
            env={"SCRATCH": str(tmp_path / "scratch")},
            home=home,
        )
        assert wd == tmp_path / "scratch" / "cstar" / "_forge_bp_runs" / "my-run"


class TestGetDataPaths:
    """Tests for get_data_paths function."""

    @patch("cstar_forge.config._detect_system")
    def test_get_data_paths(self, mock_detect, tmp_path, monkeypatch):
        """Test get_data_paths returns DataPaths object without creating directories.

        Importing cstar_forge.config must not have filesystem side effects, so the
        default (``create=False``) only builds Path objects.
        """
        mock_detect.return_value = "MacOS"

        # conftest.py forces CSTAR_FORGE_CATALOG to an already-created temp dir
        # (for global test isolation), which would make the "not exists()"
        # assertions below meaningless -- point it at a not-yet-created path
        # instead so this test still checks that get_data_paths() itself
        # creates nothing.
        monkeypatch.setenv("CSTAR_FORGE_CATALOG", str(tmp_path / "not-yet-created"))
        # Use a real home directory that exists for the test
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            paths = get_data_paths()

        assert isinstance(paths, DataPaths)
        # 'here' is the parent of __file__, so it should exist and be a directory
        # (it's not created by get_data_paths, it's the package directory)
        assert paths.here.exists(), f"'here' path does not exist: {paths.here}"
        assert paths.here.is_dir(), f"'here' path is not a directory: {paths.here}"
        # No directories are created by default
        assert not paths.source_data.exists()
        assert not paths.input_data.exists()
        assert not paths.scratch.exists()
        assert not paths.catalog.exists()
        assert not paths.blueprints.exists()
        assert paths.catalog == user_catalog_root()
        assert paths.blueprints == paths.catalog / "blueprints"

    @patch("cstar_forge.config._detect_system")
    def test_get_data_paths_creates_directories(
        self, mock_detect, tmp_path, monkeypatch
    ):
        """Test that get_data_paths(create=True) creates necessary directories."""
        mock_detect.return_value = "MacOS"

        # See test_get_data_paths above: repoint the catalog at a not-yet-created
        # path so this test actually exercises directory creation for it too.
        monkeypatch.setenv("CSTAR_FORGE_CATALOG", str(tmp_path / "not-yet-created"))
        # Use a temporary directory as HOME for the test
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            paths = get_data_paths(create=True)

        # Verify directories were created (they should exist after get_data_paths)
        assert paths.source_data.exists()
        assert paths.input_data.exists()
        assert paths.scratch.exists()
        assert paths.catalog.exists()
        assert paths.blueprints.exists()


class TestCLI:
    """Tests for CLI functionality."""

    def test_cli_show_paths(self, capsys):
        """Test show-paths command."""
        # Create a real DataPaths object for testing
        test_paths = DataPaths(
            here=Path("/test/here"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            scratch=Path("/test/run"),
            catalog=Path("/test/catalog"),
            blueprints=Path("/test/catalog/blueprints"),
            models_yaml=Path("/test/models.yaml"),
            builds_yaml=Path("/test/builds.yaml"),
        )

        # Patch everything in one context manager
        with (
            patch.object(config_module, "paths", test_paths),
            patch("cstar_forge.config._detect_system", return_value="MacOS"),
            patch("cstar_forge.config._get_hostname", return_value="test-host"),
        ):
            exit_code = main(["show-paths"])

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "System tag" in captured.out
        assert "MacOS" in captured.out
        assert "test-host" in captured.out

    def test_cli_show_paths_json(self, capsys):
        """Test show-paths command with --json flag."""
        # Create a real DataPaths object for testing
        test_paths = DataPaths(
            here=Path("/test/here"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            scratch=Path("/test/run"),
            catalog=Path("/test/catalog"),
            blueprints=Path("/test/catalog/blueprints"),
            models_yaml=Path("/test/models.yaml"),
            builds_yaml=Path("/test/builds.yaml"),
        )

        # Patch everything in one context manager
        with (
            patch.object(config_module, "paths", test_paths),
            patch("cstar_forge.config._detect_system", return_value="MacOS"),
            patch("cstar_forge.config._get_hostname", return_value="test-host"),
        ):
            exit_code = main(["show-paths", "--json"])

        assert exit_code == 0
        captured = capsys.readouterr()
        # Should be valid JSON
        data = json.loads(captured.out)
        assert data["system"] == "MacOS"
        assert data["hostname"] == "test-host"
        assert "paths" in data

    def test_cli_default_command(self, capsys):
        """Test that default command is show-paths."""
        # Create a real DataPaths object for testing
        test_paths = DataPaths(
            here=Path("/test"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            scratch=Path("/test/run"),
            catalog=Path("/test/catalog"),
            blueprints=Path("/test/catalog/blueprints"),
            models_yaml=Path("/test/models.yaml"),
            builds_yaml=Path("/test/builds.yaml"),
        )

        with (
            patch.object(config_module, "paths", test_paths),
            patch("cstar_forge.config._detect_system") as mock_detect,
            patch("cstar_forge.config._get_hostname") as mock_hostname,
        ):
            mock_detect.return_value = "MacOS"
            mock_hostname.return_value = "test-host"

            exit_code = main([])

            assert exit_code == 0
            captured = capsys.readouterr()
            assert "System tag" in captured.out

    def test_cli_unknown_command(self, capsys):
        """Test CLI with unknown command."""
        # argparse raises SystemExit(2) for invalid commands
        with pytest.raises(SystemExit) as exc_info:
            main(["unknown-command"])

        # argparse exits with code 2 for invalid arguments
        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert (
            "error" in captured.err.lower() or "invalid choice" in captured.err.lower()
        )


class TestClusterType:
    """Tests for ClusterType class and _default_cluster_type function."""

    def test_cluster_type_constants(self):
        """Test that ClusterType constants are defined correctly."""
        assert config_module.ClusterType.LOCAL == "LocalCluster"
        assert config_module.ClusterType.SLURM == "SLURMCluster"
        assert config_module.ClusterType.PBS == "PBSCluster"

    def test_default_cluster_type_macos(self):
        """Test default cluster type for MacOS."""
        result = _default_cluster_type("MacOS")
        assert result == config_module.ClusterType.LOCAL

    def test_default_cluster_type_unknown(self):
        """Test default cluster type for unknown system."""
        result = _default_cluster_type("unknown")
        assert result == config_module.ClusterType.LOCAL

    def test_default_cluster_type_anvil(self):
        """Test default cluster type for RCAC Anvil."""
        result = _default_cluster_type("RCAC_anvil")
        assert result == config_module.ClusterType.SLURM

    def test_default_cluster_type_perlmutter(self):
        """Test default cluster type for NERSC Perlmutter."""
        result = _default_cluster_type("NERSC_perlmutter")
        assert result == config_module.ClusterType.SLURM

    def test_default_cluster_type_unsupported(self):
        """Test that unsupported systems raise NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc_info:
            _default_cluster_type("unsupported_system")
        assert "unsupported_system" in str(exc_info.value)

    def test_cluster_type_module_level(self):
        """Test that config.cluster_type is set correctly."""
        # The cluster_type should be set based on the detected system
        assert hasattr(config_module, "cluster_type")
        assert config_module.cluster_type in [
            config_module.ClusterType.LOCAL,
            config_module.ClusterType.SLURM,
            config_module.ClusterType.PBS,
        ]
