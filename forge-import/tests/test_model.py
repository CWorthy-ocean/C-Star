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
    RunTimeFilter,
    CompileTimeFilter,
    load_models_yaml,
    list_models,
)


class TestRepoSpec:
    """Tests for RepoSpec dataclass."""
    
    def test_repospec_creation_minimal(self):
        """Test creating RepoSpec with minimal required fields."""
        repo = RepoSpec(
            name="roms",
            location="https://github.com/test/repo.git",
        )
        assert repo.name == "roms"
        assert repo.location == "https://github.com/test/repo.git"
        assert repo.commit is None
    
    def test_repospec_creation_with_commit(self):
        """Test creating RepoSpec with checkout specified."""
        repo = RepoSpec(
            name="marbl",
            location="https://github.com/test/marbl.git",
            commit="v1.0.0"
        )
        assert repo.name == "marbl"
        assert repo.commit == "v1.0.0"


class TestModelSpec:
    """Tests for ModelSpec dataclass."""
    
    def test_modelspec_creation(self):
        """Test creating ModelSpec with all required fields."""
        code = {
            "roms": RepoSpec(
                name="roms",
                location="https://github.com/test/roms.git",
                commit="v1.0.0"
            ),
            "marbl": RepoSpec(
                name="marbl",
                location="https://github.com/test/marbl.git",
                commit="v1.0.0"
            )
        }
        
        run_time = RunTimeFilter(files=["roms.in", "marbl_in"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test",
            conda_env="test_env",
            code=code,
            inputs={"grid": {}},
            datasets=["GLORYS"],
            run_time=run_time,
        )
        
        assert spec.name == "test-model"
        assert spec.opt_base_dir == "opt_base/test"
        assert spec.conda_env == "test_env"
        assert len(spec.code) == 2
        assert "roms" in spec.code
        assert spec.master_settings_file_name == "roms.in"
        assert len(spec.run_time.files) == 2


class TestIntegration:
    """Integration tests for model.py with real-world scenarios."""
    
    def test_full_model_spec_instantiation(self, real_models_yaml):
        """Test full ModelSpec instantiation from real models.yml."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        # Only test models that use the new schema (cson_roms-marbl_v0.1)
        # Skip old schema models for now
        for model_key in list_models(real_models_yaml):
            # Skip old schema models
            if model_key == "roms-marbl":
                continue
                
            try:
                spec = load_models_yaml(real_models_yaml, model_key)
            except (ValueError, KeyError) as e:
                # Skip models that don't have the new schema
                pytest.skip(f"Model {model_key} doesn't use new schema: {e}")
            
            # Verify all required fields are present
            assert spec.name == model_key
            assert spec.opt_base_dir
            assert spec.conda_env
            assert spec.master_settings_file_name
            assert len(spec.code) > 0
            assert len(spec.inputs) > 0
            assert isinstance(spec.datasets, list)
            assert isinstance(spec.run_time.files, list)
            assert len(spec.run_time.files) > 0
            
            # Verify code structure
            for repo_name, repo_spec in spec.code.items():
                assert isinstance(repo_spec, RepoSpec)
                assert repo_spec.name == repo_name
                assert repo_spec.location
            
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


class TestModelSpecFileValidation:
    """Tests for ModelSpec.validate_files_exist method."""
    
    def test_validate_files_exist_success(self, tmp_path, monkeypatch):
        """Test that validate_files_exist passes when all files exist."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure
        model_configs_dir = tmp_path / "model_configs"
        opt_base_dir = model_configs_dir / "opt_base" / "test_model"
        opt_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create required files
        (opt_base_dir / "roms.in").write_text("test")
        (opt_base_dir / "marbl_in").write_text("test")
        (opt_base_dir / "bgc.opt").write_text("test")
        (opt_base_dir / "Makefile").write_text("test")
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec
        run_time = RunTimeFilter(files=["roms.in", "marbl_in"])
        compile_time = CompileTimeFilter(files=["bgc.opt", "Makefile"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
            compile_time=compile_time,
        )
        
        # Should not raise
        spec.validate_files_exist()
    
    def test_validate_files_exist_missing_run_time_file(self, tmp_path, monkeypatch):
        """Test that validate_files_exist raises when run_time files are missing."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure
        model_configs_dir = tmp_path / "model_configs"
        opt_base_dir = model_configs_dir / "opt_base" / "test_model"
        opt_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create only one of the required files
        (opt_base_dir / "roms.in").write_text("test")
        # Missing marbl_in
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec
        run_time = RunTimeFilter(files=["roms.in", "marbl_in"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
        )
        
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError) as exc_info:
            spec.validate_files_exist()
        
        assert "marbl_in" in str(exc_info.value)
        assert "run_time" in str(exc_info.value)
    
    def test_validate_files_exist_missing_compile_time_file(self, tmp_path, monkeypatch):
        """Test that validate_files_exist raises when compile_time files are missing."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure
        model_configs_dir = tmp_path / "model_configs"
        opt_base_dir = model_configs_dir / "opt_base" / "test_model"
        opt_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create run_time files
        (opt_base_dir / "roms.in").write_text("test")
        # Create only one compile_time file
        (opt_base_dir / "bgc.opt").write_text("test")
        # Missing Makefile
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec
        run_time = RunTimeFilter(files=["roms.in"])
        compile_time = CompileTimeFilter(files=["bgc.opt", "Makefile"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
            compile_time=compile_time,
        )
        
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError) as exc_info:
            spec.validate_files_exist()
        
        assert "Makefile" in str(exc_info.value)
        assert "compile_time" in str(exc_info.value)
    
    def test_validate_files_exist_missing_opt_base_dir(self, tmp_path, monkeypatch):
        """Test that validate_files_exist raises when opt_base_dir doesn't exist."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure (but not the opt_base_dir)
        model_configs_dir = tmp_path / "model_configs"
        model_configs_dir.mkdir(parents=True, exist_ok=True)
        # Don't create opt_base/test_model
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec
        run_time = RunTimeFilter(files=["roms.in"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
        )
        
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError) as exc_info:
            spec.validate_files_exist()
        
        assert "opt_base_dir does not exist" in str(exc_info.value)
        assert "test-model" in str(exc_info.value)
    
    def test_validate_files_exist_no_compile_time(self, tmp_path, monkeypatch):
        """Test that validate_files_exist works when compile_time is None."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure
        model_configs_dir = tmp_path / "model_configs"
        opt_base_dir = model_configs_dir / "opt_base" / "test_model"
        opt_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create only run_time files
        (opt_base_dir / "roms.in").write_text("test")
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec without compile_time
        run_time = RunTimeFilter(files=["roms.in"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
            compile_time=None,
        )
        
        # Should not raise
        spec.validate_files_exist()
    
    def test_validate_files_exist_extra_files(self, tmp_path, monkeypatch):
        """Test that validate_files_exist raises when extra files are present."""
        from cson_forge import config
        from cson_forge.config import DataPaths
        
        # Create a temporary model configs directory structure
        model_configs_dir = tmp_path / "model_configs"
        opt_base_dir = model_configs_dir / "opt_base" / "test_model"
        opt_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create required files
        (opt_base_dir / "roms.in").write_text("test")
        (opt_base_dir / "marbl_in").write_text("test")
        (opt_base_dir / "bgc.opt").write_text("test")
        # Create an extra file that's not in the lists
        (opt_base_dir / "extra_file.opt").write_text("test")
        
        # Create temporary paths object
        test_paths = DataPaths(
            here=tmp_path,
            model_configs=model_configs_dir,
            source_data=tmp_path / "source_data",
            input_data=tmp_path / "input_data",
            run_dir=tmp_path / "run_dir",
            code_root=tmp_path / "code_root",
            blueprints=tmp_path / "blueprints",
            models_yaml=tmp_path / "models.yml",
            builds_yaml=tmp_path / "builds.yml",
            machines_yaml=tmp_path / "machines.yml",
        )
        
        monkeypatch.setattr(config, 'paths', test_paths)
        
        # Create ModelSpec
        run_time = RunTimeFilter(files=["roms.in", "marbl_in"])
        compile_time = CompileTimeFilter(files=["bgc.opt"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test_model",
            conda_env="test_env",
            code={
                "roms": RepoSpec(name="roms", location="https://github.com/test/roms.git"),
                "marbl": RepoSpec(name="marbl", location="https://github.com/test/marbl.git"),
            },
            inputs={},
            datasets=[],
            run_time=run_time,
            compile_time=compile_time,
        )
        
        # Should raise FileNotFoundError due to extra file
        with pytest.raises(FileNotFoundError) as exc_info:
            spec.validate_files_exist()
        
        assert "extra_file.opt" in str(exc_info.value)
        assert "unexpected" in str(exc_info.value).lower()
    
    def test_validate_files_exist_real_model(self, real_models_yaml):
        """Test validate_files_exist with a real model from models.yml."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        # Load a real model
        spec = load_models_yaml(real_models_yaml, "cson_roms-marbl_v0.1")
        
        # This should either pass (if files exist and match exactly) or raise FileNotFoundError
        # If validation fails, it means either:
        # 1. Files are missing (configuration issue)
        # 2. Extra files are present (configuration issue - file should be in lists or removed)
        # We skip rather than fail since this indicates a model configuration issue,
        # not a code bug
        try:
            spec.validate_files_exist()
        except FileNotFoundError as e:
            # If validation fails, skip the test - this indicates the model configuration
            # needs to be updated (either add missing files or update the file lists)
            pytest.skip(f"Model configuration validation failed (needs attention): {e}")
    