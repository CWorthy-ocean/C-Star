"""
Pytest configuration and shared fixtures for cstar-forge tests.
"""
from pathlib import Path
import sys
import tempfile

# Add project root to path so we can import cstar_forge package
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
def workflows_dir():
    """Path to workflows directory."""
    return Path(__file__).parent.parent / "workflows"


@pytest.fixture
def real_models_yaml():
    """Path to the actual models.yml file in the cstar_forge package."""
    # Use the same pattern as config.py: get path relative to package location
    import cstar_forge
    return Path(cstar_forge.config.paths.models_yaml)

