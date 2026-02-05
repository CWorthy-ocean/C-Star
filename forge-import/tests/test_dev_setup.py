"""
Test suite for dev-setup.sh script.

Tests the functionality of the development environment setup script,
including environment creation, package installation, and cleanup.
"""
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
import pytest
import yaml


@pytest.fixture
def test_dir():
    """Create a temporary directory for testing."""
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.fixture
def fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def dev_setup_script():
    """Path to dev-setup.sh script."""
    return Path(__file__).parent.parent / "dev-setup.sh"


@pytest.fixture
def test_environment(test_dir, fixtures_dir):
    """Set up a test environment with minimal files."""
    # Copy environment.yml
    env_file = fixtures_dir / "test-environment.yml"
    shutil.copy(env_file, Path(test_dir) / "environment.yml")
    
    # Create mock cson_forge package
    cson_forge_dir = Path(test_dir) / "cson_forge"
    cson_forge_dir.mkdir()
    init_file = fixtures_dir / "cson_forge" / "__init__.py"
    shutil.copy(init_file, cson_forge_dir / "__init__.py")
    
    # Copy setup.py
    setup_file = fixtures_dir / "setup.py"
    shutil.copy(setup_file, Path(test_dir) / "setup.py")
    
    # Copy dev-setup.sh
    dev_setup = Path(__file__).parent.parent / "dev-setup.sh"
    shutil.copy(dev_setup, Path(test_dir) / "dev-setup.sh")
    os.chmod(Path(test_dir) / "dev-setup.sh", 0o755)
    
    return test_dir


class TestDevSetupScript:
    """Tests for dev-setup.sh script."""
    
    def test_script_exists(self, dev_setup_script):
        """Test that dev-setup.sh exists and is executable."""
        assert dev_setup_script.exists(), "dev-setup.sh does not exist"
        assert os.access(dev_setup_script, os.X_OK), "dev-setup.sh is not executable"
    
    def test_script_has_shebang(self, dev_setup_script):
        """Test that script has correct shebang."""
        with open(dev_setup_script) as f:
            first_line = f.readline().strip()
        assert first_line == "#!/bin/bash", "Script missing correct shebang"
    
    def test_parse_environment_name(self, test_environment):
        """Test that script can parse environment name from environment.yml."""
        env_file = Path(test_environment) / "environment.yml"
        
        # Read and parse environment.yml
        with open(env_file) as f:
            env_data = yaml.safe_load(f)
        
        assert "name" in env_data, "environment.yml missing 'name' field"
        assert env_data["name"] == "test-cson-forge", "Environment name mismatch"
    
    def test_environment_yml_structure(self, test_environment):
        """Test that test environment.yml has correct structure."""
        env_file = Path(test_environment) / "environment.yml"
        
        with open(env_file) as f:
            env_data = yaml.safe_load(f)
        
        assert "name" in env_data
        assert "channels" in env_data
        assert "dependencies" in env_data
        assert isinstance(env_data["dependencies"], list)
    
    def test_mock_package_structure(self, test_environment):
        """Test that mock cson_forge package is set up correctly."""
        cson_forge_dir = Path(test_environment) / "cson_forge"
        init_file = cson_forge_dir / "__init__.py"
        
        assert cson_forge_dir.exists(), "cson_forge directory does not exist"
        assert init_file.exists(), "cson_forge/__init__.py does not exist"
        
        # Check that __init__.py has version
        with open(init_file) as f:
            content = f.read()
        assert "__version__" in content, "__init__.py missing __version__"
    
    def test_setup_py_exists(self, test_environment):
        """Test that setup.py exists for pip install."""
        setup_file = Path(test_environment) / "setup.py"
        assert setup_file.exists(), "setup.py does not exist"
    
    def test_script_accepts_clean_flag(self, test_environment, dev_setup_script):
        """Test that script accepts --clean flag."""
        # Just check that --clean doesn't cause immediate syntax errors
        # We can't fully test without actually running conda/micromamba
        script_path = Path(test_environment) / "dev-setup.sh"
        
        # Check that script contains --clean handling
        with open(script_path) as f:
            content = f.read()
        assert "--clean" in content, "Script does not handle --clean flag"
        assert "CLEAN_MODE" in content, "Script does not define CLEAN_MODE variable"
    
    def test_script_detects_os(self, dev_setup_script):
        """Test that script can detect OS type."""
        with open(dev_setup_script) as f:
            content = f.read()
        
        # Check for OS detection logic
        assert "uname" in content, "Script missing OS detection"
        assert "Darwin" in content or "Linux" in content, "Script missing OS checks"
    
    def test_script_handles_micromamba(self, dev_setup_script):
        """Test that script handles micromamba detection."""
        with open(dev_setup_script) as f:
            content = f.read()
        
        assert "micromamba" in content, "Script missing micromamba support"
        assert "MICROMAMBA_CMD" in content, "Script missing MICROMAMBA_CMD variable"
    
    def test_script_handles_conda_fallback(self, dev_setup_script):
        """Test that script falls back to conda."""
        with open(dev_setup_script) as f:
            content = f.read()
        
        assert "conda" in content, "Script missing conda fallback"
        assert "PACKAGE_MANAGER" in content, "Script missing PACKAGE_MANAGER variable"
    
    def test_script_installs_compilers_on_mac(self, dev_setup_script):
        """Test that script installs compilers on macOS."""
        with open(dev_setup_script) as f:
            content = f.read()
        
        assert "Darwin" in content, "Script missing macOS detection"
        assert "compilers" in content, "Script missing compiler installation"
    
    def test_script_sets_up_jupyter_kernel(self, dev_setup_script):
        """Test that script sets up Jupyter kernel."""
        with open(dev_setup_script) as f:
            content = f.read()
        
        assert "ipykernel" in content, "Script missing Jupyter kernel setup"
        assert "KernelSpecManager" in content, "Script missing kernel detection"
    
    def test_script_does_not_clone_cstar(self, dev_setup_script):
        """Test that script does not clone C-Star (C-Star is installed via environment.yml)."""
        with open(dev_setup_script) as f:
            content = f.read()
        assert "git clone" not in content, (
            "dev-setup.sh should not clone C-Star; C-Star is installed via environment.yml pip section"
        )
    
    @pytest.mark.skipif(
        not shutil.which("bash"),
        reason="bash not available"
    )
    def test_script_syntax_valid(self, dev_setup_script):
        """Test that script has valid bash syntax."""
        result = subprocess.run(
            ["bash", "-n", str(dev_setup_script)],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Script has syntax errors: {result.stderr}"
    
    def test_environment_yml_has_required_packages(self, fixtures_dir):
        """Test that test environment.yml has required packages."""
        env_file = fixtures_dir / "test-environment.yml"
        
        with open(env_file) as f:
            env_data = yaml.safe_load(f)
        
        deps = env_data.get("dependencies", [])
        dep_names = [d if isinstance(d, str) else list(d.keys())[0] for d in deps]
        
        assert "python" in str(deps), "Missing python dependency"
        assert "ipykernel" in dep_names, "Missing ipykernel dependency"
        assert "pip" in dep_names, "Missing pip dependency"
