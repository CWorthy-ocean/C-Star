"""
Pytest configuration and shared fixtures for cson-forge tests.
"""
from pathlib import Path
import sys
import tempfile
import yaml

# Add project root to path so we can import cson_forge package
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")


@pytest.fixture
def test_data_dir():
    """Path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def sample_models_yaml():
    """Sample models.yml content for testing."""
    return {
        "roms-marbl": {
            "opt_base_dir": "opt_base/opt_base_roms-marbl-cson-default",
            "conda_env": "roms_env",
            "repos": {
                "roms": {
                    "url": "https://github.com/CWorthy-ocean/ucla-roms.git",
                    "default_dirname": "ucla-roms",
                },
                "marbl": {
                    "url": "https://github.com/marbl-ecosys/MARBL.git",
                    "default_dirname": "MARBL",
                    "checkout": "marbl0.45.0",
                },
            },
            "master_settings_file": "roms.in",
            "settings_input_files": ["roms.in", "marbl_in"],
            "inputs": {
                "grid": {
                    "topography_source": "ETOPO5",
                },
                "initial_conditions": {
                    "source": {
                        "name": "GLORYS",
                    },
                    "bgc_source": {
                        "name": "UNIFIED",
                        "climatology": True,
                    },
                },
                "surface_forcing": {
                    "source": {
                        "name": "ERA5",
                    },
                    "type": "physics",
                },
            },
        }
    }


@pytest.fixture
def temp_models_yaml(tmp_path, sample_models_yaml):
    """Create a temporary models.yml file for testing."""
    yaml_path = tmp_path / "models.yml"
    with yaml_path.open("w") as f:
        yaml.safe_dump(sample_models_yaml, f)
    return yaml_path


@pytest.fixture
def workflows_dir():
    """Path to workflows directory."""
    return Path(__file__).parent.parent / "workflows"


@pytest.fixture
def real_models_yaml():
    """Path to the actual models.yml file in the cson_forge package."""
    # Use the same pattern as config.py: get path relative to package location
    import cson_forge
    return Path(cson_forge.config.paths.models_yaml)

