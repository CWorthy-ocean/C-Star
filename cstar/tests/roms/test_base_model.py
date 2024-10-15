import pytest
from cstar.roms.base_model import ROMSBaseModel

@pytest.fixture
def roms_base_model():

    source_repo = 'https://github.com/CESR-lab/ucla-roms.git'
    checkout_target = '246c11fa537145ba5868f2256dfb4964aeb09a25'
    return ROMSBaseModel(source_repo=source_repo, checkout_target=checkout_target)

def test_default_source_repo(roms_base_model):
    """Test if the default source repo is set correctly."""
    assert roms_base_model.default_source_repo == "https://github.com/CESR-lab/ucla-roms.git"

def test_default_checkout_target(roms_base_model):
    """Test if the default checkout target is set correctly."""
    assert roms_base_model.default_checkout_target == "main"

def test_expected_env_var(roms_base_model):
    """Test if the expected environment variable is set correctly."""
    assert roms_base_model.expected_env_var == "ROMS_ROOT"

def test_defaults_are_set():
    
    roms_base_model = ROMSBaseModel()
    assert roms_base_model.source_repo == "https://github.com/CESR-lab/ucla-roms.git"
    assert roms_base_model.checkout_target == "main"
