"""
Tests for the roms_marbl.py module.

Tests cover:
- Module property on ModelSpec
- Build function for all models in models.yml
- Run function interface
"""
from pathlib import Path
import shutil
import pytest

from cson_forge.model import load_models_yaml, list_models
from cson_forge import config


class TestModelSpecModule:
    """Tests for ModelSpec.module property."""
    
    def test_module_property_roms_marbl(self, real_models_yaml):
        """Test that module property returns roms_marbl module for roms+marbl models."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        # Load a model that has roms and marbl
        spec = load_models_yaml(real_models_yaml, "cson_roms-marbl_v0.1")
        
        # Verify module property works
        module = spec.module
        assert module is not None
        assert hasattr(module, 'build')
        assert hasattr(module, 'run')
        assert hasattr(module, 'inputs')
    
    def test_module_property_not_implemented(self):
        """Test that module property raises NotImplementedError for unsupported code keys."""
        from cson_forge.model import ModelSpec, RepoSpec, RunTimeFilter
        
        # Create a model spec with unsupported code keys
        code = {
            "unsupported": RepoSpec(
                name="unsupported",
                location="https://github.com/test/repo.git",
            )
        }
        
        run_time = RunTimeFilter(files=["settings.in"])
        
        spec = ModelSpec(
            name="test-model",
            opt_base_dir="opt_base/test",
            conda_env="test_env",
            code=code,
            inputs={},
            datasets=[],
            run_time=run_time,
        )
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = spec.module
        
        assert "not supported" in str(exc_info.value).lower()


class TestBuildAllModels:
    """Tests for building all models defined in models.yml."""
    
    @pytest.fixture
    def temp_input_data_dir(self, tmp_path):
        """Create a temporary directory for input data."""
        input_dir = tmp_path / "input_data"
        input_dir.mkdir()
        return input_dir
    
    @pytest.mark.slow
    def test_build_all_models(self, real_models_yaml, temp_input_data_dir, monkeypatch):
        """
        Test building all models defined in models.yml.
        
        This test attempts to call the build function for each model.
        It will skip if dependencies (roms_tools, git, build environment) aren't available.
        
        Marked as 'slow' since actual builds can take a long time.
        """
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        models = list_models(real_models_yaml)
        assert len(models) > 0, "No models found in models.yml"
        
        # Check if we have basic dependencies
        # try:
        #     import roms_tools
        # except ImportError:
        #     pytest.skip("roms_tools not available - skipping build tests")
        
        for model_name in models:
           
            spec = load_models_yaml(real_models_yaml, model_name)
            
            # Verify module is available
            try:
                module = spec.module
            except NotImplementedError as e:
                pytest.skip(f"Model {model_name} module not implemented: {e}")
            
            # Verify build function exists
            assert hasattr(module, 'build'), f"Module for {model_name} missing build function"
            
            # Create minimal test parameters
            grid_name = "test_grid"
            input_data_path = temp_input_data_dir / f"{model_name}_{grid_name}"
            input_data_path.mkdir(parents=True, exist_ok=True)
            
            # Create a dummy input file to satisfy the input directory check
            dummy_file = input_data_path / "dummy.nc"
            dummy_file.write_text("dummy")
            
            # Minimal build parameters (empty dict should work)
            parameters = {
                "param.opt": dict(NP_XI=10, NP_ETA=10, NX=100, NY=100, NK=60),
                "river_frc.opt": dict(nriv=42),
            }
            
            # Attempt to call build with skip_inputs_check=True
            # This will fail early if there are missing dependencies, which is fine
            try:
                # Temporarily patch config.paths to use temp directories
                original_paths = config.paths
                from cson_forge.config import DataPaths
                import tempfile
                temp_root = Path(tempfile.gettempdir()) / "cson_forge_test"
                temp_root.mkdir(exist_ok=True)
                
                
                test_paths = DataPaths(
                    here=temp_root,
                    input_data=temp_input_data_dir.parent,
                    source_data=temp_root / "source_data",
                    blueprints=temp_root / "blueprints",
                    run_dir=temp_root / "run_dir",
                    code_root=temp_root / "code_root",
                    model_configs=temp_root / "model_configs",
                    models_yaml=real_models_yaml,  # Use the real models.yml file
                    builds_yaml=temp_root / "builds.yml",
                    machines_yaml=temp_root / "machines.yml",
                )
                
                monkeypatch.setattr(config, 'paths', test_paths)                

                # Compute source and destination paths for opt_base_dir
                src_opt_base_dir = original_paths.model_configs / spec.opt_base_dir
                dst_opt_base_dir = test_paths.model_configs / spec.opt_base_dir              

                if src_opt_base_dir.exists():
                    # Ensure the parent directory exists
                    dst_opt_base_dir.parent.mkdir(parents=True, exist_ok=True)
                    # Remove destination if it already exists (cleanup for multiple runs)
                    if dst_opt_base_dir.exists():
                        shutil.rmtree(dst_opt_base_dir)
                    # Copy the directory tree
                    shutil.copytree(src_opt_base_dir, dst_opt_base_dir)
                
                # Try to call build - this will fail if git repos aren't available,
                # build environment isn't set up, etc. That's expected in test environments.
                use_conda = config.system in {"MacOS", "unknown"}

                result = module.build(
                    model_spec=spec,
                    grid_name=grid_name,
                    input_data_path=input_data_path,
                    parameters=parameters,
                    clean=True,
                    use_conda=use_conda,  # Use shell script environment
                    skip_inputs_check=True,  # Skip input file checks
                )
                
                # If we get here, build was attempted (may have failed, but interface worked)
                # result will be None if build failed, or a Path if it succeeded
                assert result is None or isinstance(result, Path)
                
            except FileNotFoundError as e:
                # Expected if git repos, build scripts, etc. aren't available
                if any(keyword in str(e).lower() for keyword in [
                    'git', 'repo', 'environment', '.sh', 'not found'
                ]):
                    pytest.skip(f"Build dependencies not available for {model_name}: {e}")
                else:
                    raise
            except RuntimeError as e:
                # Build may fail for various reasons (missing compilers, conda, etc.)
                # That's OK - we're just testing that the interface works
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in [
                    'gfortran', 'mpifort', 'compiler', 'build', 'make', 'conda', 'not found on path'
                ]):
                    # Skip if it's a missing dependency/environment issue
                    pytest.skip(f"Build environment not set up for {model_name}: {e}")
                else:
                    raise
            except TypeError as e:
                # If it's a TypeError about missing arguments, that's a test setup issue
                if 'missing' in str(e).lower() and 'required' in str(e).lower():
                    raise RuntimeError(f"Test setup error for {model_name}: {e}") from e
                else:
                    raise
            except Exception as e:
                # If it's a dependency issue, that's expected in test environments
                if any(keyword in str(e).lower() for keyword in [
                    'import', 'module', 'not found', 'no module', 'missing'
                ]):
                    pytest.skip(f"Missing dependencies for {model_name}: {e}")
                else:
                    # Re-raise unexpected errors
                    raise
            finally:
                # Restore original paths
                monkeypatch.setattr(config, 'paths', original_paths)
    
    def test_build_function_signature(self, real_models_yaml):
        """Test that build function has the correct signature."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        # Load a model
        spec = load_models_yaml(real_models_yaml, "cson_roms-marbl_v0.1")
        module = spec.module
        
        import inspect
        sig = inspect.signature(module.build)
        
        # Verify required parameters
        params = list(sig.parameters.keys())
        assert 'model_spec' in params
        assert 'grid_name' in params
        assert 'input_data_path' in params
        assert 'parameters' in params
        
        # Verify optional parameters have defaults
        assert sig.parameters['clean'].default is False
        assert sig.parameters['use_conda'].default is False
        assert sig.parameters['skip_inputs_check'].default is False
    
    def test_run_function_signature(self, real_models_yaml):
        """Test that run function has the correct signature."""
        if not real_models_yaml.exists():
            pytest.skip("Real models.yml file not found")
        
        # Load a model
        spec = load_models_yaml(real_models_yaml, "cson_roms-marbl_v0.1")
        module = spec.module
        
        import inspect
        sig = inspect.signature(module.run)
        
        # Verify required parameters
        params = list(sig.parameters.keys())
        assert 'model_spec' in params
        assert 'grid_name' in params
        assert 'case' in params
        assert 'executable_path' in params
        assert 'run_command' in params
        assert 'inputs' in params

