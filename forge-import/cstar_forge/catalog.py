"""
Catalog package: API-driven access to blueprint information.

The ``BlueprintCatalog`` class discovers and loads blueprint YAML files under
``config.paths.blueprints`` (by default ``<package>/catalog/blueprints``).

On disk, this package directory also holds the catalog layout: ``blueprints/`` and
``builds/`` next to this ``__init__.py``. Import as ``cstar_forge.catalog`` or
``from cstar_forge import catalog``.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml
import pandas as pd

from . import config
import roms_tools as rt


class BlueprintCatalog:
    """
    API-driven access to blueprint information stored in the blueprints directory.
    
    Provides methods to discover and load blueprint data, including conversion
    to pandas DataFrames for easy querying and instantiation of OcnModel objects.
    """
    
    def __init__(self, blueprints_dir: Optional[Path] = None):
        """
        Initialize the blueprint catalog.
        
        Parameters
        ----------
        blueprints_dir : Path, optional
            Directory containing blueprint YAML files. Defaults to config.paths.blueprints.
        """
        if blueprints_dir is None:
            blueprints_dir = config.paths.blueprints
        self.blueprints_dir = Path(blueprints_dir)
    
    def find_blueprint_files(self, stage: Optional[str] = None) -> List[Path]:
        """
        Recursively find all blueprint files in the blueprints directory.
        
        Parameters
        ----------
        stage : str, optional
            Filter by blueprint stage (preconfig, postconfig, build, run).
            If None, returns all blueprint files.
        
        Returns
        -------
        List[Path]
            List of paths to blueprint YAML files matching pattern B_*.yml.
        """
        if stage:
            pattern = f"B_*_{stage}.yml"
        else:
            pattern = "B_*.yml"
        
        blueprint_files = list(self.blueprints_dir.rglob(pattern))
        # Filter out checkpoint directories and run blueprints with datestr
        blueprint_files = [
            f for f in blueprint_files 
            if ".ipynb_checkpoints" not in str(f) and not f.name.endswith("_run_*.yml")
        ]
        # Also find run blueprints with datestr pattern
        if not stage or stage == "run":
            run_files = list(self.blueprints_dir.rglob("B_*_run_*.yml"))
            blueprint_files.extend([f for f in run_files if ".ipynb_checkpoints" not in str(f)])
        
        return sorted(blueprint_files)
    
    def load_blueprint(self, blueprint_path: Path) -> Dict[str, Any]:
        """
        Load a single blueprint YAML file.
        
        Parameters
        ----------
        blueprint_path : Path
            Path to the blueprint YAML file.
        
        Returns
        -------
        Dict[str, Any]
            Parsed blueprint data.
        
        Raises
        ------
        FileNotFoundError
            If the blueprint file does not exist.
        yaml.YAMLError
            If the YAML file cannot be parsed.
        """
        if not blueprint_path.exists():
            raise FileNotFoundError(f"Blueprint file not found: {blueprint_path}")
        
        with blueprint_path.open("r") as f:
            data = yaml.safe_load(f) or {}
        
        return data
    
    def load_grid_kwargs(self, grid_yaml_path: Path) -> Dict[str, Any]:
        """
        Load grid keyword arguments from a grid YAML file.
        
        Parameters
        ----------
        grid_yaml_path : Path
            Path to the grid YAML file (e.g., _grid.yml).
        
        Returns
        -------
        Dict[str, Any]
            Grid keyword arguments suitable for OcnModel initialization.
        
        Raises
        ------
        FileNotFoundError
            If the grid YAML file does not exist.
        KeyError
            If the Grid section is missing from the YAML file.
        """
        if not grid_yaml_path.exists():
            raise FileNotFoundError(f"Grid YAML file not found: {grid_yaml_path}")

        with grid_yaml_path.open("r") as f:
            docs = list(yaml.safe_load_all(f))            
        
        if len(docs) != 2:
            raise ValueError(f"Expected 2 documents in {grid_yaml_path}, but found {len(docs)}")
        grid_data = docs[1]
        
        if "Grid" not in grid_data:
            raise KeyError(f"Grid section not found in {grid_yaml_path}")
        
        return grid_data["Grid"]
    
    def _extract_model_and_grid_name(self, blueprint_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract model name and grid name from blueprint name.
        
        Blueprint names follow the pattern: {model_name}_{grid_name}
        e.g., "cson_roms-marbl_v0.1_test-tiny" -> ("cson_roms-marbl_v0.1", "test-tiny")
        
        Parameters
        ----------
        blueprint_name : str
            The blueprint name field.
        
        Returns
        -------
        tuple[Optional[str], Optional[str]]
            (model_name, grid_name) tuple. Returns (None, None) if pattern doesn't match.
        """
        if not blueprint_name:
            return None, None
        
        # Try to split on the last underscore
        parts = blueprint_name.rsplit("_", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        
        return None, None
    
    def load(self, stage: Optional[str] = "postconfig") -> pd.DataFrame:
        """
        Load all blueprints and return a pandas DataFrame with all data.
        
        Parameters
        ----------
        stage : str, optional
            Blueprint stage to load (preconfig, postconfig, build, run).
            Defaults to "postconfig" which has the most complete data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            - model_name: Model name extracted from blueprint name
            - grid_name: Grid name extracted from blueprint name
            - blueprint_name: Full blueprint name
            - description: Blueprint description
            - start_time: Simulation start time (valid_start_date)
            - end_time: Simulation end time (valid_end_date)
            - blueprint_path: Path to the blueprint YAML file
            - grid_yaml_path: Path to the grid YAML file (_grid.yml) if found
            - stage: Blueprint stage
        
        Notes
        -----
        Blueprints that cannot be parsed or are missing required fields
        will be skipped with a warning message.
        """
        blueprint_files = self.find_blueprint_files(stage=stage)
        
        records = []
        for bp_file in blueprint_files:
            try:
                blueprint = self.load_blueprint(bp_file)
                
                # Extract blueprint name and parse model/grid names
                blueprint_name = blueprint.get("name")
                if not blueprint_name:
                    print(f"⚠️  Skipping {bp_file}: missing 'name' field")
                    continue
                
                model_name, grid_name = self._extract_model_and_grid_name(blueprint_name)
                if not model_name or not grid_name:
                    print(f"⚠️  Skipping {bp_file}: could not extract model/grid name from '{blueprint_name}'")
                    continue
                
                # Extract dates
                start_time = blueprint.get("valid_start_date")
                end_time = blueprint.get("valid_end_date")
                
                # Extract description
                description = blueprint.get("description")
                
                # Try to find grid YAML file
                grid_yaml_path = None
                # Look for _grid.yml in the same directory as the blueprint
                grid_yaml_path = bp_file.parent / "_grid.yml"
                if not grid_yaml_path.exists():
                    grid_yaml_path = None
                
                # Determine stage from filename
                file_stage = None
                if "_preconfig" in bp_file.name:
                    file_stage = "preconfig"
                elif "_postconfig" in bp_file.name:
                    file_stage = "postconfig"
                elif "_build" in bp_file.name:
                    file_stage = "build"
                elif "_run" in bp_file.name:
                    file_stage = "run"
                
                records.append({
                    "model_name": model_name,
                    "grid_name": grid_name,
                    "blueprint_name": blueprint_name,
                    "description": description,
                    "start_time": start_time,
                    "end_time": end_time,
                    "blueprint_path": bp_file,
                    "grid_yaml_path": grid_yaml_path if grid_yaml_path and grid_yaml_path.exists() else None,
                    "stage": file_stage,
                })
                
            except Exception as e:
                print(f"⚠️  Could not parse {bp_file}: {e}")
                continue
        
        if not records:
            return pd.DataFrame()
        
        return pd.DataFrame(records)


# Convenience instance
blueprint = BlueprintCatalog()
