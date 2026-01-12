"""
Tests for the config.py module.

Tests cover:
- DataPaths dataclass
- MachineConfig dataclass
- System detection functions
- Path resolution and layout functions
- Machine configuration loading
- CLI functionality
"""
from pathlib import Path
import os
import pytest
import yaml
import json
from dataclasses import FrozenInstanceError
from unittest.mock import patch, MagicMock

import cson_forge.config as config_module

from cson_forge.config import (
    DataPaths,
    MachineConfig,
    _detect_system,
    _get_hostname,
    _default_cluster_type,
    get_data_paths,
    load_machine_config,
    SYSTEM_LAYOUT_REGISTRY,
    register_system,
    main,
)


class TestDataPaths:
    """Tests for DataPaths dataclass."""
    
    def test_datapaths_creation(self, tmp_path):
        """Test creating DataPaths with all required fields."""
        paths = DataPaths(
            here=tmp_path,
            model_configs=tmp_path / "model-configs",
            source_data=tmp_path / "source-data",
            input_data=tmp_path / "input-data",
            run_dir=tmp_path / "run-dir",
            code_root=tmp_path / "code-root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        assert paths.here == tmp_path
        assert paths.model_configs == tmp_path / "model-configs"
        assert paths.source_data == tmp_path / "source-data"
        assert paths.input_data == tmp_path / "input-data"
        assert paths.run_dir == tmp_path / "run-dir"
        assert paths.code_root == tmp_path / "code-root"
        assert paths.blueprints == tmp_path / "blueprints"
        assert paths.models_yaml == tmp_path / "models.yml"
        assert paths.builds_yaml == tmp_path / "builds.yml"
        assert paths.machines_yaml == tmp_path / "machines.yml"
    
    def test_datapaths_frozen(self, tmp_path):
        """Test that DataPaths is frozen (immutable)."""
        paths = DataPaths(
            here=tmp_path,
            model_configs=tmp_path / "model-configs",
            source_data=tmp_path / "source-data",
            input_data=tmp_path / "input-data",
            run_dir=tmp_path / "run-dir",
            code_root=tmp_path / "code-root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        with pytest.raises(FrozenInstanceError):
            paths.here = tmp_path / "new"


class TestMachineConfig:
    """Tests for MachineConfig dataclass."""
    
    def test_machineconfig_creation_minimal(self):
        """Test creating MachineConfig with no fields."""
        config = MachineConfig()
        assert config.account is None
        assert config.pes_per_node is None
        assert config.queues is None
    
    def test_machineconfig_creation_with_fields(self):
        """Test creating MachineConfig with all fields."""
        config = MachineConfig(
            account="test_account",
            pes_per_node=64,
            queues={"default": "normal", "premium": "high"}
        )
        assert config.account == "test_account"
        assert config.pes_per_node == 64
        assert config.queues == {"default": "normal", "premium": "high"}


class TestSystemDetection:
    """Tests for system detection functions."""
    
    @patch('cson_forge.config.platform.system')
    @patch('cson_forge.config._get_hostname')
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_macos(self, mock_hostname, mock_system):
        """Test system detection for MacOS."""
        mock_system.return_value = "Darwin"
        result = _detect_system()
        assert result == "MacOS"
    
    @patch('cson_forge.config.platform.system')
    @patch('cson_forge.config._get_hostname')
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_anvil(self, mock_hostname, mock_system):
        """Test system detection for RCAC Anvil."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "anvil-login01"
        result = _detect_system()
        assert result == "RCAC_anvil"
    
    @patch('cson_forge.config.platform.system')
    @patch('cson_forge.config._get_hostname')
    @patch.dict(os.environ, {"NERSC_HOST": "perlmutter"})
    def test_detect_system_perlmutter(self, mock_hostname, mock_system):
        """Test system detection for NERSC Perlmutter."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "unknown"
        result = _detect_system()
        assert result == "NERSC_perlmutter"
    
    @patch('cson_forge.config.platform.system')
    @patch('cson_forge.config._get_hostname')
    @patch.dict(os.environ, {}, clear=True)
    def test_detect_system_unknown(self, mock_hostname, mock_system):
        """Test system detection for unknown system."""
        mock_system.return_value = "Linux"
        mock_hostname.return_value = "unknown-host"
        result = _detect_system()
        assert result == "unknown"
    
    @patch.dict(os.environ, {"HOSTNAME": "test-host"})
    @patch('cson_forge.config.socket.gethostname')
    @patch('cson_forge.config.platform.node')
    def test_get_hostname_from_env(self, mock_node, mock_gethostname):
        """Test getting hostname from HOSTNAME environment variable."""
        result = _get_hostname()
        assert result == "test-host"
        mock_gethostname.assert_not_called()
        mock_node.assert_not_called()
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('cson_forge.config.socket.gethostname')
    @patch('cson_forge.config.platform.node')
    def test_get_hostname_from_socket(self, mock_node, mock_gethostname):
        """Test getting hostname from socket.gethostname()."""
        mock_gethostname.return_value = "socket-host"
        mock_node.return_value = "platform-host"
        result = _get_hostname()
        assert result == "socket-host"
        mock_node.assert_not_called()
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('cson_forge.config.socket.gethostname')
    @patch('cson_forge.config.platform.node')
    def test_get_hostname_from_platform(self, mock_node, mock_gethostname):
        """Test getting hostname from platform.node() as fallback."""
        mock_gethostname.return_value = None
        mock_node.return_value = "platform-host"
        result = _get_hostname()
        assert result == "platform-host"
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('cson_forge.config.socket.gethostname')
    @patch('cson_forge.config.platform.node')
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
                home / "test-code"
            )
        
        assert "test_system" in SYSTEM_LAYOUT_REGISTRY
        assert SYSTEM_LAYOUT_REGISTRY["test_system"] == test_layout
        
        # Clean up
        del SYSTEM_LAYOUT_REGISTRY["test_system"]
    
    def test_macos_layout(self, tmp_path):
        """Test MacOS layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["MacOS"]
        source_data, input_data, run_dir, code_root = layout_fn(tmp_path, {})
        
        assert source_data == tmp_path / "cson-forge-data" / "source-data"
        assert input_data == tmp_path / "cson-forge-data" / "input-data"
        assert run_dir == tmp_path / "cson-forge-data" / "cson-forge-run"
        assert code_root == tmp_path / "cson-forge-data" / "codes"
    
    def test_unknown_layout(self, tmp_path):
        """Test unknown layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["unknown"]
        source_data, input_data, run_dir, code_root = layout_fn(tmp_path, {})
        
        assert source_data == tmp_path / "cson-forge-data" / "source-data"
        assert input_data == tmp_path / "cson-forge-data" / "input-data"
        assert run_dir == tmp_path / "cson-forge-data" / "cson-forge-run"
        assert code_root == tmp_path / "cson-forge-data" / "codes"
    
    def test_anvil_layout(self, tmp_path):
        """Test RCAC Anvil layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["RCAC_anvil"]
        env = {"WORK": str(tmp_path / "work"), "SCRATCH": str(tmp_path / "scratch")}
        source_data, input_data, run_dir, code_root = layout_fn(tmp_path, env)
        
        assert source_data == tmp_path / "work" / "cson-forge-data" / "source-data"
        assert input_data == tmp_path / "work" / "cson-forge-data" / "input-data"
        assert run_dir == tmp_path / "scratch" / "cson-forge-run"
        assert code_root == tmp_path / "work" / "cson-forge-data" / "codes"
    
    def test_perlmutter_layout(self, tmp_path):
        """Test NERSC Perlmutter layout function."""
        layout_fn = SYSTEM_LAYOUT_REGISTRY["NERSC_perlmutter"]
        env = {"SCRATCH": str(tmp_path / "scratch")}
        source_data, input_data, run_dir, code_root = layout_fn(tmp_path, env)
        
        assert source_data == tmp_path / "scratch" / "cson-forge-data" / "source-data"
        assert input_data == tmp_path / "scratch" / "cson-forge-data" / "input-data"
        assert run_dir == tmp_path / "scratch" / "cson-forge-data" / "cson-forge-run"
        assert code_root == tmp_path / "scratch" / "cson-forge-data" / "codes"


class TestGetDataPaths:
    """Tests for get_data_paths function."""
    
    @patch('cson_forge.config._detect_system')
    def test_get_data_paths(self, mock_detect, tmp_path):
        """Test get_data_paths returns DataPaths object."""
        mock_detect.return_value = "MacOS"
        
        # Use a real home directory that exists for the test
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            paths = get_data_paths()
        
        assert isinstance(paths, DataPaths)
        # 'here' is the parent of __file__, so it should exist and be a directory
        # (it's not created by get_data_paths, it's the package directory)
        assert paths.here.exists(), f"'here' path does not exist: {paths.here}"
        assert paths.here.is_dir(), f"'here' path is not a directory: {paths.here}"
        # These directories are created by get_data_paths
        assert paths.source_data.exists()
        assert paths.input_data.exists()
        assert paths.run_dir.exists()
        assert paths.code_root.exists()
        assert paths.blueprints.exists()
        assert paths.model_configs.exists()
    
    @patch('cson_forge.config._detect_system')
    def test_get_data_paths_creates_directories(self, mock_detect, tmp_path):
        """Test that get_data_paths creates necessary directories."""
        mock_detect.return_value = "MacOS"
        
        # Use a temporary directory as HOME for the test
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            paths = get_data_paths()
        
        # Verify directories were created (they should exist after get_data_paths)
        assert paths.source_data.exists()
        assert paths.input_data.exists()
        assert paths.run_dir.exists()
        assert paths.code_root.exists()


class TestLoadMachineConfig:
    """Tests for load_machine_config function."""
    
    def test_load_machine_config_nonexistent_file(self, tmp_path):
        """Test loading machine config when file doesn't exist."""
        machines_yaml = tmp_path / "machines.yml"
        config = load_machine_config("test_system", machines_yaml)
        
        assert isinstance(config, MachineConfig)
        assert config.account is None
        assert config.pes_per_node is None
        assert config.queues is None
    
    def test_load_machine_config_existing_machine(self, tmp_path):
        """Test loading machine config for existing machine."""
        machines_yaml = tmp_path / "machines.yml"
        machines_data = {
            "NERSC_perlmutter": {
                "account": "test_account",
                "pes_per_node": 128,
                "queues": {
                    "default": "normal",
                    "premium": "premium"
                }
            }
        }
        
        with machines_yaml.open("w") as f:
            yaml.safe_dump(machines_data, f)
        
        config = load_machine_config("NERSC_perlmutter", machines_yaml)
        
        assert config.account == "test_account"
        assert config.pes_per_node == 128
        assert config.queues == {"default": "normal", "premium": "premium"}
    
    def test_load_machine_config_nonexistent_machine(self, tmp_path):
        """Test loading machine config for machine not in file."""
        machines_yaml = tmp_path / "machines.yml"
        machines_data = {
            "NERSC_perlmutter": {
                "account": "test_account"
            }
        }
        
        with machines_yaml.open("w") as f:
            yaml.safe_dump(machines_data, f)
        
        config = load_machine_config("unknown_machine", machines_yaml)
        
        assert isinstance(config, MachineConfig)
        assert config.account is None
    
    def test_load_machine_config_invalid_yaml(self, tmp_path):
        """Test loading machine config with invalid YAML."""
        machines_yaml = tmp_path / "machines.yml"
        machines_yaml.write_text("invalid: yaml: content: [")
        
        config = load_machine_config("test_system", machines_yaml)
        
        # Should return empty config on error
        assert isinstance(config, MachineConfig)
        assert config.account is None


class TestCLI:
    """Tests for CLI functionality."""
    
    def test_cli_show_paths(self, capsys):
        """Test show-paths command."""
        # Create a real DataPaths object for testing
        test_paths = DataPaths(
            here=Path("/test/here"),
            model_configs=Path("/test/model-configs"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            run_dir=Path("/test/run"),
            code_root=Path("/test/code"),
            blueprints=Path("/test/blueprints"),
            models_yaml=Path("/test/models.yml"),
            builds_yaml=Path("/test/builds.yml"),
            machines_yaml=Path("/test/machines.yml"),
        )
        
        # Patch everything in one context manager
        with patch.object(config_module, 'paths', test_paths), \
             patch('cson_forge.config._detect_system', return_value="MacOS"), \
             patch('cson_forge.config._get_hostname', return_value="test-host"):
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
            model_configs=Path("/test/model-configs"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            run_dir=Path("/test/run"),
            code_root=Path("/test/code"),
            blueprints=Path("/test/blueprints"),
            models_yaml=Path("/test/models.yml"),
            builds_yaml=Path("/test/builds.yml"),
            machines_yaml=Path("/test/machines.yml"),
        )
        
        # Patch everything in one context manager
        with patch.object(config_module, 'paths', test_paths), \
             patch('cson_forge.config._detect_system', return_value="MacOS"), \
             patch('cson_forge.config._get_hostname', return_value="test-host"):
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
            model_configs=Path("/test/model-configs"),
            source_data=Path("/test/source"),
            input_data=Path("/test/input"),
            run_dir=Path("/test/run"),
            code_root=Path("/test/code"),
            blueprints=Path("/test/blueprints"),
            models_yaml=Path("/test/models.yml"),
            builds_yaml=Path("/test/builds.yml"),
            machines_yaml=Path("/test/machines.yml"),
        )
        
        with patch.object(config_module, 'paths', test_paths), \
             patch('cson_forge.config._detect_system') as mock_detect, \
             patch('cson_forge.config._get_hostname') as mock_hostname:
            
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
        assert "error" in captured.err.lower() or "invalid choice" in captured.err.lower()


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
        assert hasattr(config_module, 'cluster_type')
        assert config_module.cluster_type in [
            config_module.ClusterType.LOCAL,
            config_module.ClusterType.SLURM,
            config_module.ClusterType.PBS
        ]

