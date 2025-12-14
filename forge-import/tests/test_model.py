"""
Tests for the model.py module.

Tests cover:
- RepoSpec dataclass instantiation
- ModelSpec dataclass instantiation
- load_models_yaml function with various YAML configurations
- Error handling for missing or invalid YAML files
"""
from pathlib import Path
import pytest
import yaml

# Import the module under test
import sys

from cson_forge.model import (
    RepoSpec,
    ModelSpec,
    load_models_yaml,
    list_models,
)


class TestRepoSpec:
    """Tests for RepoSpec dataclass."""
    
    def test_repospec_creation_minimal(self):
        """Test creating RepoSpec with minimal required fields."""
        repo = RepoSpec(
            name="roms",
            url="https://github.com/test/repo.git",
            default_dirname="test-repo"
        )
        assert repo.name == "roms"
        assert repo.url == "https://github.com/test/repo.git"
        assert repo.default_dirname == "test-repo"
        assert repo.checkout is None
    
    def test_repospec_creation_with_checkout(self):
        """Test creating RepoSpec with checkout specified."""
        repo = RepoSpec(
            name="marbl",
            url="https://github.com/test/marbl.git",
            default_dirname="MARBL",
            checkout="v1.0.0"
        )
        assert repo.name == "marbl"
        assert repo.checkout == "v1.0.0"


class TestModelSpec:
    """Tests for ModelSpec dataclass."""
    
    def test_modelspec_creation(self):
        """Test creating ModelSpec with all required fields."""
        repos = {
            "roms": RepoSpec(
                name="roms",
                url="https://github.com/test/roms.git",
                default_dirname="roms"
            )
        }
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test",
            conda_env="test_env",
            repos=repos,
            inputs={"grid": {}},
            datasets=["GLORYS"],
            settings_input_files=["roms.in"],
            master_settings_file="roms.in"
        )
        
        assert spec.name == "test-model"
        assert spec.opt_base_dir == "opt_base/test"
        assert spec.conda_env == "test_env"
        assert len(spec.repos) == 1
        assert "roms" in spec.repos
        assert spec.master_settings_file == "roms.in"


class TestIntegration:
    """Integration tests for model.py with real-world scenarios."""
    
    def test_full_model_spec_instantiation(self, real_models_yaml):
        """Test full ModelSpec instantiation from real models.yml."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        for model_key in list_models(real_models_yaml):
            spec = load_models_yaml(real_models_yaml, model_key)
            # Verify all required fields are present
            assert spec.name == model_key
            assert spec.opt_base_dir
            assert spec.conda_env
            assert spec.master_settings_file
            assert len(spec.repos) > 0
            assert len(spec.inputs) > 0
            assert isinstance(spec.datasets, list)
            assert isinstance(spec.settings_input_files, list)
            
            # Verify repo structure
            for repo_name, repo_spec in spec.repos.items():
                assert isinstance(repo_spec, RepoSpec)
                assert repo_spec.name == repo_name
                assert repo_spec.url
                assert repo_spec.default_dirname
            
            # Verify inputs structure
            for input_name, input_config in spec.inputs.items():
                assert isinstance(input_config, dict)


class TestListModels:
    """Tests for list_models function."""
    
    def test_list_models_from_temp_yaml(self, temp_models_yaml):
        """Test listing models from a temporary YAML file."""
        models = list_models(temp_models_yaml)
        
        assert isinstance(models, list)
        assert len(models) > 0
    
    def test_list_models_from_real_yaml(self, real_models_yaml):
        """Test listing models from the actual models.yml file."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        models = list_models(real_models_yaml)
        
        assert isinstance(models, list)
        assert len(models) > 0
    
    def test_list_models_nonexistent_file(self, tmp_path):
        """Test that list_models returns empty list for nonexistent file."""
        nonexistent = tmp_path / "nonexistent.yml"
        models = list_models(nonexistent)
        assert models == []
    
    def test_list_models_empty_file(self, tmp_path):
        """Test that list_models returns empty list for empty file."""
        empty_yaml = tmp_path / "empty.yml"
        empty_yaml.write_text("")
        models = list_models(empty_yaml)
        assert models == []
    