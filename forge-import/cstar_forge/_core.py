"""
CstarSpecBuilder - Pydantic-based builder for C-Star blueprints.

This class provides a Pydantic-based interface for building RomsMarblBlueprint objects.
"""
from __future__ import annotations

import asyncio
import copy
import os
import sys
import time
import warnings
from dataclasses import asdict as dataclass_asdict
from datetime import datetime
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import xarray as xr
import yaml
from cstar.orchestration.models import Resource
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

import cstar.applications.roms_marbl.models as cstar_models
from cstar.orchestration.serialization import deserialize
from cstar.roms import ROMSSimulation
from cstar.execution.handler import ExecutionStatus
from cstar.entrypoint.config import get_job_config, get_service_config

from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.applications.roms_marbl.app import RomsMarblRunner
from cstar.applications.core import RunnerRequest
from . import config
from . import source_data
from . import models as forge_models
from . import input_data
from .settings import render_roms_settings
from .util import compute_timestep_from_cfl, roms_tools_default_nesting_period_seconds
import roms_tools as rt


def resolve_catalog_dir(catalog_root: Optional[Union[str, Path]]) -> Path:
    """
    Resolve the absolute inner *catalog* directory (direct parent of ``blueprints/`` and ``builds/``).

    Parameters
    ----------
    catalog_root
        - ``None``: use ``config.paths.catalog`` (default data-tree location).
        - ``"local"`` (case-insensitive string): use the package layout
          ``<cstar_forge>/catalog`` (same as ``config.paths.here / "catalog"``); no extra
          ``/catalog`` suffix is applied.
        - Any other ``str`` or ``Path``: *outer* catalog anchor; the inner directory is
          ``<resolved_anchor>/catalog`` (i.e. blueprints live at ``.../catalog/blueprints``).
    """
    if catalog_root is None:
        return config.paths.catalog
    if isinstance(catalog_root, str) and catalog_root.strip().lower() == "local":
        return config.paths.here / "catalog"
    outer = Path(catalog_root).expanduser().resolve()
    return outer / "catalog"


def _schedule_coroutine(coro):
    """
    Schedule a coroutine on the running loop, returns a Task.

    This is needed for seamless execution of async co-routines (like running a cstar worker) from Jupyter notebooks.
    """
    try:
        loop = asyncio.get_running_loop()
        return loop.create_task(coro)
    except RuntimeError:
        # No running loop, just run it directly
        return asyncio.run(coro)

class DatasetsDict(dict):
    """Dictionary-like class that supports method call with key parameter."""
    
    def __call__(self, key: Optional[str] = None):
        """
        Return a specific dataset by key, or the whole dictionary if key is None.
        
        Parameters
        ----------
        key : str, optional
            Key to retrieve. If None, returns self.
        
        Returns
        -------
        Union[xr.Dataset, List[xr.Dataset], dict]
            The dataset(s) for the key, or the whole dictionary if key is None.
        """
        if key is None:
            return self
        return self.get(key)


class BlueprintStage:
    """
    Blueprint stage constants and validation.
    
    Valid stages:
    - PRECONFIG: Blueprint before configuration
    - POSTCONFIG: Blueprint after configuration
    - BUILD: Blueprint after building/compiling the model
    - RUN: Blueprint for running the simulation
    """
    PRECONFIG: str = "preconfig"
    POSTCONFIG: str = "postconfig"
    BUILD: str = "build"
    RUN: str = "run"
    
    # Numerical values for stage comparison
    N_PRECONFIG: int = 0
    N_POSTCONFIG: int = 1
    N_BUILD: int = 2
    N_RUN: int = 3
    
    @classmethod
    def validate_stage(cls, stage: str) -> str:
        """Validate that stage is one of the valid values."""
        valid_stages = {cls.PRECONFIG, cls.POSTCONFIG, cls.BUILD, cls.RUN}
        if stage not in valid_stages:
            raise ValueError(f"stage must be one of {valid_stages}, got {stage}")
        return stage
    
    @classmethod
    def get_stage_value(cls, stage: str) -> int:
        """Get the numerical value of a stage for comparison."""
        stage_map = {
            cls.PRECONFIG: cls.N_PRECONFIG,
            cls.POSTCONFIG: cls.N_POSTCONFIG,
            cls.BUILD: cls.N_BUILD,
            cls.RUN: cls.N_RUN,
        }
        return stage_map.get(stage, -1)


def _deep_merge_settings_dict(target: Dict[str, Any], update: Dict[str, Any]) -> None:
    """
    Recursively merge ``update`` into ``target`` (mutates ``target`` in place).

    Nested dicts are merged key-by-key. Any key whose new value is not a dict,
    or whose existing value is not a dict, replaces the value at that key.
    """
    for k, v in update.items():
        if k in target and isinstance(target[k], dict) and isinstance(v, dict):
            _deep_merge_settings_dict(target[k], v)
        else:
            target[k] = copy.deepcopy(v)


class CstarSpecBuilder(BaseModel):
    """
    Builder for C-Star RomsMarblBlueprint specifications.
    
    This class provides a Pydantic-based interface for constructing
    and managing ROMS-MARBL blueprints through a staged workflow.
    
    **Workflow and Stage Progression:**
    
    The builder progresses through distinct stages, each representing a
    phase of the model configuration and execution pipeline:
    
    1. **PRECONFIG** (initialization):
       - Created during `model_post_init()` via `_initialize_blueprint()`
       - Blueprint structure initialized with placeholder data
       - Settings dictionaries initialized from model defaults
       - Blueprint persisted to disk
       
    2. **POSTCONFIG** (input generation):
       - Achieved by calling `generate_inputs()`
       - Source data prepared, input files generated (grid, initial conditions, forcing)
       - Blueprint updated with actual data file locations
       - Settings updated with input-specific values
       - Blueprint persisted to disk
       
    3. **BUILD** (configuration):
       - Achieved by calling `configure_build()`
       - Jinja2 templates rendered with current settings
       - Blueprint updated with rendered code locations
       - Blueprint persisted to disk
       - ROMSSimulation instance created
       
    4. **RUN** (execution):
       - Achieved by calling `run()` after `build()`
       - Blueprint persisted with runtime parameters
       - Model executable runs

    **Settings overrides via files:**

    Use the optional ``override`` argument to pass one or more YAML files with
    settings overrides. Each file may be either:
    - a direct mapping of settings keys for one settings tree, or
    - a mapping with ``compile_time`` and/or ``run_time`` sections.

    Files are merged after model defaults are loaded. Only top-level keys that
    already exist in the model defaults are applied; unknown keys are ignored
    with a warning. Nested dicts are deep-merged so sparse override files can
    change subsets of settings. Run-time overrides are applied after dynamic
    fields (case title, output paths, and default timestepping from CFL) are
    set, so you can still tune e.g. ``ndtfast`` or ``dt``.

    **Key Concepts:**
    
    - Settings are stored in sidecar YAML files (not in blueprint itself)
    - Blueprint state is persisted to disk at each stage transition
    - Grid object is created during initialization and reused throughout
    - Source data can be prepared independently via `ensure_source_data()`
    - Optional ``catalog_root`` selects an *outer* anchor so blueprints and builds live under
      ``<catalog_root>/catalog/`` (or use ``catalog_root='local'`` for the in-repo
      ``cstar_forge/catalog`` tree; default uses ``config.paths.catalog``).
    
    .. warning::
        This functionality is under active development and not yet fully implemented.
        Some methods (e.g., `build()` and `run()`) may raise `NotImplementedError`.
        Use with caution.
    """
    
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    
    # User inputs
    description: str = "Generated blueprint"
    model_name: str
    grid_name: str
    grid_kwargs: Dict[str, Any]
    grid_kwargs_parent: Optional[Dict[str, Any]] = Field(default=None, validate_default=False)
    grid_kwargs_child: Optional[Dict[str, Any]] = Field(default=None, validate_default=False)
    open_boundaries: forge_models.OpenBoundaries
    partitioning: cstar_models.PartitioningParameterSet
    start_date: datetime = Field(alias="start_time")
    end_date: datetime = Field(alias="end_time")
    cdr_forcing: Optional[dict] = Field(
        default=None,
        alias="CDR_forcing",
        validate_default=False,
    )
    override: Optional[List[Union[str, Path]]] = Field(default=None, validate_default=False)
    ensemble_id: Optional[int] = Field(default=None, validate_default=False)
    catalog_root: Optional[Union[str, Path]] = Field(
        default=None,
        validate_default=False,
        description=(
            "Optional *outer* catalog anchor. Blueprints and builds use "
            "``<catalog_root>/catalog/blueprints`` and ``<catalog_root>/catalog/builds``. "
            "Omit to use ``config.paths.catalog``. Use ``catalog_root='local'`` for the "
            "in-repo ``cstar_forge/catalog`` package directory (no extra ``/catalog`` suffix)."
        ),
    )
    # Internal attributes (computed/loaded)
    blueprint: Optional[cstar_models.RomsMarblBlueprint] = Field(
        default=None,
        init=False,
        validate_default=False,
        validate_assignment=False
    )
    src_data: Optional[source_data.SourceData] = Field(
        default=None,
        init=False,
        validate_default=False
    )
    grid: Optional[rt.Grid] = Field(
        default=None,
        init=False,
        validate_default=False,
        exclude=True
    )
    grid_parent: Optional[rt.Grid] = Field(
        default=None,
        init=False,
        validate_default=False,
        exclude=True
    )
    grid_child: Optional[rt.Grid] = Field(
        default=None,
        init=False,
        validate_default=False,
        exclude=True
    )
    metadata_child: Optional[dict[str, Any]] = Field(
        default=None,
        init=False,
        validate_default=False,
        exclude=True
    )
    _model_spec: Optional[forge_models.ModelSpec] = PrivateAttr(default=None)
    _datasets: Optional[Dict[str, Union[xr.Dataset, List[xr.Dataset]]]] = PrivateAttr(default=None)
    _stage: Optional[str] = PrivateAttr(default=None)
    _cstar_simulation: Optional[Any] = PrivateAttr(default=None)
    _settings_compile_time: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _settings_run_time: Dict[str, Any] = PrivateAttr(default_factory=dict)
    
    @model_validator(mode="after")
    def _validate_dates(self) -> "CstarSpecBuilder":
        """Validate that start_date precedes end_date."""
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self
    
    @property
    def input_data_dir(self) -> Path:
        """Directory for generated input NetCDF files (grid, forcing, etc.)."""
        # Match ``InputData`` / ``_forcing_filename``: dirname uses the same sanitization
        # as NetCDF basenames (no ``.`` except ``.nc``), so planned-output prints match
        # paths on disk.
        safe = input_data.netcdf_filename_component(self.name)
        return config.paths.input_data / safe
    
    def model_post_init(self, __context: Any) -> None:
        """
        Post-initialization hook called automatically after model validation.
        
        This method is called by Pydantic after the instance is validated and
        performs critical initialization:
        
        1. Creates the grid object from `grid_kwargs`
        2. Initializes the blueprint structure (calls `_initialize_blueprint()`)
        
        After this method completes, the blueprint is in the **PRECONFIG** stage
        and has been persisted to disk.
        """
        
        # Create grids, 4 cases:
        # has child and no parent, has child and parent, has parent and no child, no parent no child

        # I am a parent but not a child
        if self.grid_kwargs_child is not None and self.grid_kwargs_parent is None:
            # Make both parent and child, to make the nesting data. 
            grid_kwargs_child = {k: v for k, v in self.grid_kwargs_child.items() if k != "metadata"}

            self.grid_child = rt.Grid(**grid_kwargs_child)
            self.grid = rt.Grid(**self.grid_kwargs)
            self.grid_child = rt.align_grids(self.grid, self.grid_child)

            if 'metadata' in self.grid_kwargs_child:
                self.metadata_child = self.grid_kwargs_child['metadata']

        # I am a parent and a child
        elif self.grid_kwargs_child is not None and self.grid_kwargs_parent is not None:
            grid_kwargs_child = {k: v for k, v in self.grid_kwargs_child.items() if k != "metadata"}
            grid_kwargs_parent = {k: v for k, v in self.grid_kwargs_parent.items() if k != "metadata"}
            grid_kwargs = {k: v for k, v in self.grid_kwargs.items() if k != "metadata"}

            # Adapt this grid to its parent, but create nesting data for its child
            self.grid_parent = rt.Grid(**grid_kwargs_parent)
            self.grid_child = rt.Grid(**grid_kwargs_child)
            self.grid = rt.Grid(**grid_kwargs)

            self.grid = rt.align_grids(self.grid_parent, self.grid)
            self.grid_child = rt.align_grids(self.grid, self.grid_child)

            if 'metadata' in self.grid_kwargs_child:
                self.metadata_child = self.grid_kwargs_child['metadata']

        # I am a child but not a parent
        elif self.grid_kwargs_child is None and self.grid_kwargs_parent is not None:
            grid_kwargs_parent = {k: v for k, v in self.grid_kwargs_parent.items() if k != "metadata"}
            grid_kwargs = {k: v for k, v in self.grid_kwargs.items() if k != "metadata"}

            # Adapt this grid to its parent. no nesting data needed
            self.grid_parent = rt.Grid(**grid_kwargs_parent)
            self.grid = rt.Grid(**grid_kwargs)

            self.grid = rt.align_grids(self.grid_parent, self.grid)
        else:
            self.grid = rt.Grid(**self.grid_kwargs)

        # Initialize blueprint with basic structure
        self._initialize_blueprint()

        self._print_planned_netcdf_outputs()
        self._print_output_paths()

    def _planned_netcdf_outputs(self) -> List[Path]:
        """Return the list of NetCDF files expected from input generation."""
        if self._model_spec is None:
            self._load_model_spec()

        input_data_dir = self.input_data_dir
        planned_paths: List[Path] = []

        def _add_nc(stem: str) -> None:
            base = input_data.netcdf_filename_component(self.name)
            part = input_data.netcdf_filename_component(stem)
            path = (input_data_dir / f"{base}_{part}.nc").resolve()
            if path not in planned_paths:
                planned_paths.append(path)

        model_inputs = getattr(self._model_spec, "inputs", None)
        if model_inputs is None:
            return planned_paths

        inputs_cfg: Dict[str, Any] = {}
        if hasattr(model_inputs, "model_dump"):
            dumped_inputs = model_inputs.model_dump(exclude_none=True)
            if isinstance(dumped_inputs, dict):
                inputs_cfg = dumped_inputs
        elif isinstance(model_inputs, dict):
            inputs_cfg = model_inputs

        if inputs_cfg.get("grid"):
            _add_nc("grid")
            if self.grid_child is not None:
                _add_nc("grid_child")
                _add_nc("nesting")

        if inputs_cfg.get("initial_conditions"):
            _add_nc("initial_conditions")

        forcing_cfg = inputs_cfg.get("forcing") or {}
        if isinstance(forcing_cfg, dict):
            for category, entries in forcing_cfg.items():
                if not entries:
                    continue

                if category == "boundary":
                    boundaries = self.open_boundaries.model_dump()
                    if not any(boundaries.values()):
                        continue

                if category in {"surface", "boundary"} and isinstance(entries, list):
                    for entry in entries:
                        forcing_type = None
                        if isinstance(entry, dict):
                            forcing_type = entry.get("type")
                        elif hasattr(entry, "model_dump"):
                            forcing_type = entry.model_dump().get("type")
                        elif hasattr(entry, "type"):
                            forcing_type = entry.type

                        stem = f"{category}-{forcing_type}" if forcing_type else category
                        _add_nc(stem)
                    continue

                _add_nc(category)

        if inputs_cfg.get("cdr_forcing"):
            _add_nc(input_data.CDR_FORCING_NETCDF_STEM)

        return planned_paths

    def _print_planned_netcdf_outputs(self) -> None:
        """Print the list of expected NetCDF files to stdout."""
        planned_paths = self._planned_netcdf_outputs()
        print("CstarSpecBuilder: planned NetCDF outputs")
        if not planned_paths:
            print("  (none)")
        else:
            for path in planned_paths:
                print(f"  - {path}")
        print()

    def _print_output_paths(self) -> None:
        """Print absolute paths where generated NetCDF and YAML files are stored."""
        netcdf_dir = self.input_data_dir.resolve()
        yaml_dir = self.blueprint_dir.resolve()
        lines = [
            "CstarSpecBuilder: output locations",
            f"  NetCDF files: {netcdf_dir}",
            f"  YAML files: {yaml_dir}",
        ]
        lines.extend(
            [
                f"  Compile-time code: {self.compile_time_code_dir.resolve()}",
                f"  Run-time code: {self.run_time_code_dir.resolve()}",
                f"  Simulation output (scratch): {self.run_output_dir.resolve()}",
            ]
        )
        print("\n".join(lines))
        print()

    @property
    def name(self) -> str:
        """
        Return the name of this blueprint as '{model_spec.name}_{grid_name}'.
        
        This property sets blueprint.name when the blueprint is created.
        """
        ensemble_str = f"_{self.ensemble_id:03d}" if self.ensemble_id is not None else ""
        return f"{self._model_spec.name}_{self.grid_name}_{self.n_procs}procs{ensemble_str}"

    @property
    def n_procs(self) -> int:
        """Return the number of processors."""
        return self.partitioning.n_procs_x * self.partitioning.n_procs_y

    @property
    def datestr(self) -> str:
        """Return the date string."""
        return f"{self.start_date.strftime('%Y%m%d')}-{self.end_date.strftime('%Y%m%d')}"

    @property
    def casename(self) -> str:
        """Return the case name."""
        return f"{self.name}_{self.datestr}"

    @property
    def run_output_dir(self) -> Path:
        """Simulation scratch directory under ``config.paths.scratch`` (primary data tree)."""
        return config.paths.scratch / self.casename
    
    @property
    def default_runtime_params(self) -> cstar_models.RuntimeParameterSet:
        """
        Get default runtime parameters.
        
        Returns a RuntimeParameterSet with default values based on the builder's
        configuration (start_date, end_date, output_dir).
        """
        return cstar_models.RuntimeParameterSet(
            start_date=self.start_date,
            end_date=self.end_date,
            output_dir=self.run_output_dir,
        )

    @property
    def resolved_catalog_dir(self) -> Path:
        """Absolute inner *catalog* directory (contains ``blueprints/`` and ``builds/``)."""
        return resolve_catalog_dir(self.catalog_root)

    @property
    def blueprint_dir(self) -> Path:
        """Return the blueprint directory path."""
        return self.resolved_catalog_dir / "blueprints" / config.system_id / self.name
    
    @property
    def compile_time_code_dir(self) -> Path:
        """Compile-time rendered templates under this builder's ``builds`` tree."""
        return self.resolved_catalog_dir / "builds" / self.name / "compile-time"
    
    @property
    def run_time_code_dir(self) -> Path:
        """Run-time rendered templates under this builder's ``builds`` tree."""
        return self.resolved_catalog_dir / "builds" / self.name / "run-time"

    def persist(self) -> None:
        """
        Persist the current blueprint state to a YAML file.
        
        Saves the blueprint to disk at the file path determined by the current
        stage (PRECONFIG, POSTCONFIG, BUILD, or RUN). Also saves settings to
        a sidecar file.
        
        **File Structure:**
        
        - Blueprint: `B_{name}_{stage}.yml` (or with datestr for RUN stage)
        - Settings: `settings_B_{name}_{stage}.yml` (sidecar file)
        
        The settings are stored separately from the blueprint to avoid
        cluttering the blueprint with configuration details.
        
        **Notes:**
        
        - The directory is created if it doesn't exist
        - Serialization warnings are suppressed (expected for placeholder values)
        - Path objects are converted to strings for YAML compatibility
        
        Raises
        ------
        ValueError
            If blueprint is None, if _stage is None, if stage is "run" but
            runtime_params is not available, or if stage is not a valid
            blueprint stage.
        """
        if self.blueprint is None:
            raise ValueError("Cannot persist: blueprint is not initialized")
        
        if self._stage is None:
            raise ValueError("Cannot persist: _stage is not set")
        
        # Validate stage
        stage = BlueprintStage.validate_stage(self._stage)
        
        # Determine run_params for path_blueprint if stage is "run"
        run_params = None
        if stage == BlueprintStage.RUN:
            if self.blueprint.runtime_params is None:
                raise ValueError("Cannot persist run blueprint: runtime_params is not set")
            run_params = self.blueprint.runtime_params
        
        # Get the file path using path_blueprint
        bp_path = self.path_blueprint(stage=stage, run_params=run_params)
        
        # Ensure directory exists
        bp_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save blueprint to YAML file
        # Use mode='json' to ensure all values are JSON/YAML-serializable (no Python objects)
        # Use exclude_none=True to handle placeholder values gracefully
        # Suppress expected serialization warnings for placeholder values created with model_construct()
        with warnings.catch_warnings():
            # Filter all Pydantic serialization warnings
            # These occur because placeholder values (None) don't match expected types
            warnings.filterwarnings(
                'ignore',
                message='.*Pydantic.*',
                category=UserWarning
            )
            warnings.filterwarnings(
                'ignore',
                message='.*serialization.*',
                category=UserWarning
            )
            blueprint_dict = self.blueprint.model_dump(mode='json', exclude_none=True)
        
        with bp_path.open("w") as f:
            yaml.safe_dump(blueprint_dict, f, default_flow_style=False, sort_keys=False)
        
        # Write settings to sidecar file
        self._persist_settings(bp_path)
    


    
    def _path_settings_file(self, blueprint_path: Path) -> Path:
        """
        Return the path to the settings sidecar file for a given blueprint path.
        
        The settings file has the same name as the blueprint file, with "settings_" prepended.
        For example: "B_model_postconfig.yml" -> "settings_B_model_postconfig.yml"
        
        Parameters
        ----------
        blueprint_path : Path
            Path to the blueprint file.
        
        Returns
        -------
        Path
            Path to the settings sidecar file.
        """
        # Get the directory and filename
        directory = blueprint_path.parent
        filename = blueprint_path.name
        
        # Prepend "settings_" to the filename
        settings_filename = f"settings_{filename}"
        
        return directory / settings_filename
    
    def _convert_paths_to_strings(self, obj: Any) -> Any:
        """
        Recursively convert Path objects to strings and replace non-serializable
        objects (e.g. xarray.Dataset) with placeholders for YAML serialization.
        
        Parameters
        ----------
        obj : Any
            Object to process (can be dict, list, Path, or other types).
        
        Returns
        -------
        Any
            Object with Path converted to strings and non-serializable types replaced.
        """
        if isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_paths_to_strings(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return type(obj)(self._convert_paths_to_strings(item) for item in obj)
        elif isinstance(obj, xr.Dataset):
            return "<xarray.Dataset>"
        elif isinstance(obj, xr.DataArray):
            return "<xarray.DataArray>"
        else:
            return obj



    def _rewrite_roms_input_paths_to_staged_runtime_paths(self) -> None:
        """
        Point ``roms.in`` input paths at C-Star staged runtime datasets.

        Generated inputs are first written under ``self.input_data_dir``. During setup,
        C-Star stages/symlinks these files under
        ``<run_output_dir>/input/input_datasets``. ROMS reads from this staged
        directory at run time, so we rewrite relevant ``roms.in`` path fields to
        match that location.
        """
        rt_cfg = self._settings_run_time.get("roms.in")
        if not isinstance(rt_cfg, dict):
            return

        source_root = self.input_data_dir.resolve()
        staged_root = (self.run_output_dir / "input" / "input_datasets").resolve()
        sections = ("grid", "initial", "forcing")

        for section_name in sections:
            section = rt_cfg.get(section_name)
            if not isinstance(section, dict):
                continue

            for key, value in list(section.items()):
                if not isinstance(value, str) or not value.strip():
                    continue
                if key not in {"grid_file", "initial_file"} and not key.endswith("_path"):
                    continue

                candidate = Path(value).expanduser()
                if not candidate.is_absolute():
                    continue

                try:
                    candidate.resolve().relative_to(source_root)
                except ValueError:
                    continue

                section[key] = str(staged_root / candidate.name)


    def _rewrite_staged_runtime_roms_in_paths(self) -> None:
        """
        Rewrite staged ``input/runtime_code/roms.in`` to use staged dataset paths.

        This is a final safety pass after C-Star setup has staged runtime files.
        """
        roms_in = self.run_output_dir / "input" / "runtime_code" / "roms.in"
        if not roms_in.is_file():
            return

        source_prefix = str(self.input_data_dir.resolve())
        staged_prefix = str((self.run_output_dir / "input" / "input_datasets").resolve())

        try:
            text = roms_in.read_text()
        except OSError:
            return

        if source_prefix not in text:
            return

        updated = text.replace(source_prefix, staged_prefix)
        if updated == text:
            return

        roms_in.write_text(updated)
    
    def _persist_settings(self, blueprint_path: Path) -> None:
        """
        Persist settings dictionaries to a sidecar file.
        
        Writes compile_time and run_time settings to a YAML file with the same
        name as the blueprint file, prepended with "settings_".
        
        Parameters
        ----------
        blueprint_path : Path
            Path to the blueprint file (used to determine settings file path).
        """
        settings_path = self._path_settings_file(blueprint_path)
        
        # Prepare settings dictionary
        settings_dict = {
            "compile_time": self._settings_compile_time,
            "run_time": self._settings_run_time
        }
        
        # Convert all Path objects to strings for YAML serialization
        settings_dict = self._convert_paths_to_strings(settings_dict)
        
        # Ensure directory exists
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write settings to YAML file
        with settings_path.open("w") as f:
            yaml.safe_dump(settings_dict, f, default_flow_style=False, sort_keys=False)
    
    def _load_settings_from_file(self, blueprint_path: Path) -> None:
        """
        Load settings dictionaries from a sidecar file.
        
        Reads compile_time and run_time settings from a YAML file with the same
        name as the blueprint file, prepended with "settings_".
        
        If the settings file doesn't exist, leaves settings dictionaries unchanged.
        
        Parameters
        ----------
        blueprint_path : Path
            Path to the blueprint file (used to determine settings file path).
        """
        settings_path = self._path_settings_file(blueprint_path)
        
        if not settings_path.exists():
            # Settings file doesn't exist, leave settings unchanged
            return
        
        try:
            with settings_path.open("r") as f:
                settings_dict = yaml.safe_load(f)
            
            # Update settings dictionaries if they exist in the file
            if settings_dict:
                if "compile_time" in settings_dict:
                    self._settings_compile_time = settings_dict["compile_time"]
                if "run_time" in settings_dict:
                    self._settings_run_time = settings_dict["run_time"]
        except Exception as e:
            # If loading fails, issue a warning but don't fail
            warnings.warn(
                f"Failed to load settings from {settings_path}: {type(e).__name__}: {e}",
                UserWarning,
                stacklevel=2
            )
    
    def path_blueprint(self, stage: Optional[str] = None, run_params: Optional[cstar_models.RuntimeParameterSet] = None) -> Path:
        """
        Return the path to the blueprint file for a given stage.
        
        Parameters
        ----------
        stage : str, optional
            The blueprint stage. If not provided, uses the blueprint's current state.
        run_params : RuntimeParameterSet, optional
            Runtime parameters for the simulation. Required if stage="run", optional otherwise.
            Used to generate a unique filename for the run blueprint.
        
        Returns
        -------
        Path
            Path to the blueprint YAML file for the specified stage.
        
        Raises
        ------
        AssertionError
            If stage is not one of the valid values.
        ValueError
            If stage="run" and run_params is not provided, or if stage is None and blueprint is None.
        """
        if stage is None:
            if self.blueprint is None:
                raise ValueError("stage must be provided if blueprint is not initialized")
            stage = self.blueprint.state
        BlueprintStage.validate_stage(stage)
        
        if stage == BlueprintStage.RUN:
            if run_params is None:
                raise ValueError("run_params is required when stage='run'")
            # Generate a unique identifier from run_params for the filename
            # Using start_date and end_date to create a unique identifier

            return self.blueprint_dir / f"B_{self.name}_{stage}_{self.datestr}.yml"
        else:
            return self.blueprint_dir / f"B_{self.name}_{stage}.yml"

    @property
    def datasets(self) -> DatasetsDict:
        """
        Return a dictionary of xarray Datasets loaded from blueprint data files.
        
        This property lazily loads xarray Datasets from the NetCDF files referenced
        in the blueprint. The datasets are cached in `_datasets` for efficiency.
        
        **Supported Fields:**
        
        The dictionary includes datasets for all data fields in the blueprint:
        - "grid": Grid dataset
        - "initial_conditions": Initial conditions dataset
        - "forcing.boundary": Boundary forcing datasets
        - "forcing.surface": Surface forcing datasets
        - "forcing.tidal": Tidal forcing datasets
        - "forcing.rivers": River forcing datasets
        - "cdr_forcing": CDR forcing dataset
        
        **Usage:**
        
        Supports both dictionary-style and method-style access:
        - `datasets["grid"]` - dictionary indexing
        - `datasets(key="grid")` - method call with key parameter
        - `datasets()` or `datasets` - returns all datasets
        
        **Data Loading:**
        
        Datasets are loaded lazily from the blueprint's data file locations.
        If a field doesn't exist in the blueprint, it is skipped. Datasets are
        opened in read-only mode (lazy loading).
        
        Returns
        -------
        DatasetsDict
            Dictionary-like object mapping field names to xarray Datasets.
            Returns empty DatasetsDict if blueprint is not initialized.
            
        Warns
        -----
        UserWarning
            If blueprint is not initialized. Returns empty DatasetsDict.
        """
        
        if self.blueprint is None:
            warnings.warn(
                "Blueprint is not initialized. Cannot retrieve datasets.",
                UserWarning,
                stacklevel=2
            )
            return DatasetsDict()
        
        # Populate all datasets from blueprint if not already done
        if self._datasets is None:
            self._datasets = {}
        
        # Dynamically generate list of fields that contain data entries
        # Start with grid and initial_conditions
        data_fields = ["grid", "initial_conditions"]
        
        # Add forcing fields from model_spec.inputs.forcing
        
        if self._model_spec and self._model_spec.inputs and self._model_spec.inputs.forcing:
            # Loop over all fields in the forcing configuration
            for field_name in self._model_spec.inputs.forcing.model_fields.keys():
                data_fields.append(f"forcing.{field_name}")
        
        # Add cdr_forcing (not part of inputs, but a separate blueprint field)
        data_fields.append("cdr_forcing")
        
        # Loop over all data fields and call get_ds for each
        # Suppress Pydantic warnings when accessing datasets
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
            warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*serializer.*", category=UserWarning)
            for field in data_fields:
                # Skip if already populated
                if field in self._datasets:
                    continue
                
                # Call get_ds to get the datasets (it will return None if field doesn't exist)
                ds_list = self.get_ds(field, from_file=False)
                if ds_list is not None and len(ds_list) > 0:
                    # Store single dataset or list
                    self._datasets[field] = ds_list[0] if len(ds_list) == 1 else ds_list
        
        # Return as DatasetsDict to support both dict access and method call
        return DatasetsDict(self._datasets)
    
    def _load_model_spec(self):
        """Load ModelSpec from models.yml."""
        self._model_spec = forge_models.load_models_yaml(
            config.paths.models_yaml,
            self.model_name
        )

    def _prompt_yes_no(self, message: str) -> bool:
        """Prompt user for a yes/no answer in interactive runs."""
        while True:
            try:
                response = input(f"{message} [yes/no]: ").strip().lower()
            except EOFError:
                # Non-interactive environment: default to "no" (reuse existing blueprint).
                print(f"{message} [yes/no]: no")
                return False
            if response in {"yes", "y"}:
                return True
            if response in {"no", "n"}:
                return False
            print("Please answer 'yes' or 'no'.")

    def _delete_blueprint_and_settings(self, blueprint_path: Path) -> None:
        """Delete a blueprint file and its settings sidecar if present."""
        settings_path = self._path_settings_file(blueprint_path)
        for path in (blueprint_path, settings_path):
            if path.exists():
                path.unlink()
                print(f"🗑️  Deleted existing file: {path}")

    def _canonicalize_stored_input_netcdf_path(self, path: Union[str, Path]) -> Path:
        """
        Map NetCDF paths as stored in older blueprints to paths matching current input
        naming (``netcdf_filename_component(self.name)`` for dirname and file prefix).

        Blueprints may still list directories or basenames containing ``.`` (e.g. ``v0.1``)
        while generated files use underscores.
        """
        p = Path(path).expanduser()
        if p.suffix.lower() != ".nc":
            return p
        safe = input_data.netcdf_filename_component(self.name)
        raw = self.name
        if not p.is_absolute():
            if len(p.parts) == 1:
                name = p.name
                if name.startswith(raw + "_"):
                    name = safe + name[len(raw) :]
                return (self.input_data_dir / name).resolve()
            p = (Path.cwd() / p).resolve()
        else:
            p = p.resolve()
        parts = list(p.parts)
        new_parts = [safe if part == raw else part for part in parts]
        p2 = Path(*new_parts)
        nm = p2.name
        if nm.startswith(raw + "_"):
            nm = safe + nm[len(raw) :]
            p2 = p2.parent / nm
        return p2

    def _required_netcdf_paths_from_blueprint(
        self, blueprint: cstar_models.RomsMarblBlueprint
    ) -> List[Path]:
        """Extract local required NetCDF paths referenced by a blueprint."""
        bp_dict = blueprint.model_dump(mode="json", exclude_none=True)
        required_paths: List[Path] = []

        def _append_dataset_locations(dataset_dict: Any) -> None:
            if not isinstance(dataset_dict, dict):
                return
            resources = dataset_dict.get("data")
            if not isinstance(resources, list):
                return
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                location = resource.get("location")
                if not isinstance(location, str):
                    continue
                if location.startswith("http://") or location.startswith("https://"):
                    continue
                path = Path(location)
                if path.suffix == ".nc" and path not in required_paths:
                    required_paths.append(path)

        for field_name in ("grid", "initial_conditions", "cdr_forcing", "nesting_info"):
            _append_dataset_locations(bp_dict.get(field_name))

        forcing = bp_dict.get("forcing")
        if isinstance(forcing, dict):
            for forcing_dataset in forcing.values():
                _append_dataset_locations(forcing_dataset)

        return required_paths

    def _initialize_blueprint(self) -> None:
        """
        Initialize blueprint with basic structure and set stage to PRECONFIG.
        
        This method creates the initial blueprint structure with placeholder data.
        It is called automatically during initialization via `model_post_init()`.
        
        **Process:**
        
        1. Loads the model specification from models.yml
        2. Initializes compile-time and run-time settings from defaults
        3. Creates blueprint with:
           - Basic metadata (name, description, dates, partitioning)
           - Code repository specifications from model_spec
           - Placeholder Resource objects for grid, initial_conditions, forcing
        4. Sets `_stage` to PRECONFIG
        5. Persists blueprint to disk
        
        The blueprint at this stage has the correct structure but contains
        placeholder data (None locations). Actual data files are added during
        the POSTCONFIG stage via `generate_inputs()`.
        """

        # Load model spec
        self._load_model_spec()

        # Initialize settings from defaults
        self._init_settings_compile_time()
        self._init_settings_run_time()
                    
        # Create placeholder Resource objects to satisfy validation requirements
        placeholder_resource = Resource.model_construct(
            location=None,
            partitioned=False
        )
        forcing_config = cstar_models.ForcingConfiguration.model_construct(
            boundary=cstar_models.Dataset.model_construct(data=[placeholder_resource]),
            surface=cstar_models.Dataset.model_construct(data=[placeholder_resource]),
        )       
        empty_dataset = cstar_models.Dataset.model_construct(data=[placeholder_resource])
               
        # Use model_construct to bypass validation during initialization
        # The blueprint will be validated later when data is populated
        # Use placeholder datasets to satisfy structure requirements
        self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(
            name=self.name,
            description=self.description,
            valid_start_date=self.start_date,
            valid_end_date=self.end_date,
            partitioning=self.partitioning,
            model_params=None,  # stored in sidecar files
            runtime_params=None,  # stored in sidecar files
            code=self._model_spec.code,
            grid=empty_dataset,
            initial_conditions=empty_dataset,
            forcing=forcing_config,
            cdr_forcing=None,
        )
        self._stage = BlueprintStage.PRECONFIG
        self.persist()
    
    def _compare_dicts_recursive(self, dict1: Dict[str, Any], dict2: Dict[str, Any], path: str = "") -> bool:
        """
        Recursively compare two dictionaries, handling nested structures, lists, and datetime normalization.
        
        Parameters
        ----------
        dict1 : Dict[str, Any]
            First dictionary to compare.
        dict2 : Dict[str, Any]
            Second dictionary to compare.
        path : str, optional
            Current path in the dictionary structure (for error messages). Default is "".
        
        Returns
        -------
        bool
            True if dictionaries match, False otherwise.
        """
        # Handle None cases
        if dict1 is None and dict2 is None:
            return True
        if dict1 is None or dict2 is None:
            warnings.warn(f"One dict is None at path '{path}': dict1={dict1}, dict2={dict2}", UserWarning, stacklevel=2)
            return False
        
        # Check for missing or extra keys
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        if keys1 != keys2:
            missing = keys2 - keys1
            extra = keys1 - keys2
            msg_parts = []
            if missing:
                msg_parts.append(f"missing keys: {missing}")
            if extra:
                msg_parts.append(f"extra keys: {extra}")
            warnings.warn(f"Key mismatch at path '{path}': {', '.join(msg_parts)}", UserWarning, stacklevel=2)
            return False
        
        # Recursively compare all values
        for key in keys1:
            val1 = dict1[key]
            val2 = dict2[key]
            current_path = f"{path}.{key}" if path else key
            
            # Skip 'data' field when path is "grid"
            if path == "grid" and key == "data":
                continue
            
            # Handle nested dictionaries
            if isinstance(val1, dict) and isinstance(val2, dict):
                if not self._compare_dicts_recursive(val1, val2, current_path):
                    return False
                continue
            
            # Handle lists
            if isinstance(val1, list) and isinstance(val2, list):
                if len(val1) != len(val2):
                    warnings.warn(
                        f"different list lengths at path '{current_path}': {len(val1)} vs {len(val2)}",
                        UserWarning, stacklevel=2
                    )
                    return False
                for idx, (item1, item2) in enumerate(zip(val1, val2)):
                    item_path = f"{current_path}[{idx}]"
                    if isinstance(item1, dict) and isinstance(item2, dict):
                        if not self._compare_dicts_recursive(item1, item2, item_path):
                            return False
                    elif item1 != item2:
                        warnings.warn(
                            f"List item mismatch at path '{item_path}': {item1} != {item2}",
                            UserWarning, stacklevel=2
                        )
                        return False
                continue
            
            # Handle datetime normalization (datetime objects vs strings)
            if isinstance(val1, datetime) and isinstance(val2, str):
                if val1.isoformat() == val2 or val1.strftime("%Y-%m-%dT%H:%M:%S") == val2:
                    continue
            elif isinstance(val1, str) and isinstance(val2, datetime):
                if val1 == val2.isoformat() or val1 == val2.strftime("%Y-%m-%dT%H:%M:%S"):
                    continue
            
            # Direct comparison for other types
            if val1 != val2:
                warnings.warn(
                    f"Value mismatch at path '{current_path}': {val1} != {val2}",
                    UserWarning, stacklevel=2
                )
                return False
        
        return True

    def _file_blueprint_data_match(self, partition_files: bool = False) -> bool:
        """
        Check if the POSTCONFIG blueprint from file matches the current blueprint configuration.
        
        Compares specific blueprint fields, grid dataset, and partitioned flags to determine
        if the existing POSTCONFIG blueprint from file can be reused.
        
        Parameters
        ----------
        partition_files : bool, optional
            Expected value for all partitioned flags. Defaults to False.
        
        Returns
        -------
        bool
            True if the POSTCONFIG blueprint from file matches, False otherwise.
        """
        # Load POSTCONFIG blueprint from file (skip loading settings file)
        postconfig_blueprint = self._load_blueprint_file(stage=BlueprintStage.POSTCONFIG, load_settings=False)
        if postconfig_blueprint is None:
            return False
        
        # Convert both blueprints to dictionaries for comparison
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
            warnings.filterwarnings('ignore', message='.*Pydantic.*', category=UserWarning)
            warnings.filterwarnings('ignore', message='.*serialization.*', category=UserWarning)
            current_dict = self.blueprint.model_dump(mode='json')
            file_dict = postconfig_blueprint.model_dump(mode='json')
        
        # Compare specific fields: name, description, valid_start_date, valid_end_date, partitioning, code
        fields_to_compare = ['name', 'description', 'valid_start_date', 'valid_end_date', 'partitioning', 'code']
        
        def compare_dict_field(current_val: Any, file_val: Any, field_name: str, path: str = "") -> Tuple[bool, Optional[str]]:
            """
            Compare two values, handling dictionaries recursively.
            
            Returns (is_match, error_message)
            """
            # Handle None cases
            if current_val is None and file_val is None:
                return True, None
            if current_val is None or file_val is None:
                return False, f"One value is None: current={current_val}, file={file_val}"
            
            # For dictionaries, compare recursively
            if isinstance(current_val, dict) and isinstance(file_val, dict):
                # Check for missing or extra keys
                current_keys = set(current_val.keys())
                file_keys = set(file_val.keys())
                
                if current_keys != file_keys:
                    missing = file_keys - current_keys
                    extra = current_keys - file_keys
                    msg_parts = []
                    if missing:
                        msg_parts.append(f"missing keys: {missing}")
                    if extra:
                        msg_parts.append(f"extra keys: {extra}")
                    return False, f"Key mismatch: {', '.join(msg_parts)}"
                
                # Recursively compare all values
                for key in current_keys:
                    current_item = current_val[key]
                    file_item = file_val[key]
                    item_path = f"{path}.{key}" if path else key
                    is_match, error_msg = compare_dict_field(current_item, file_item, field_name, item_path)
                    if not is_match:
                        return False, f"At {item_path}: {error_msg}"
                
                return True, None
            
            # For lists, compare element by element
            if isinstance(current_val, list) and isinstance(file_val, list):
                if len(current_val) != len(file_val):
                    return False, f"List length mismatch: current={len(current_val)}, file={len(file_val)}"
                for idx, (current_item, file_item) in enumerate(zip(current_val, file_val)):
                    item_path = f"{path}[{idx}]" if path else f"[{idx}]"
                    is_match, error_msg = compare_dict_field(current_item, file_item, field_name, item_path)
                    if not is_match:
                        return False, f"At {item_path}: {error_msg}"
                return True, None
            
            # For other types, direct comparison
            if current_val != file_val:
                return False, f"Value mismatch: current={current_val}, file={file_val}"
            
            return True, None
        
        for field in fields_to_compare:
            current_value = current_dict.get(field)
            file_value = file_dict.get(field)
            
            is_match, error_msg = compare_dict_field(current_value, file_value, field)
            if not is_match:
                warnings.warn(
                    f"Blueprint field '{field}' does not match POSTCONFIG blueprint from file. "
                    f"{error_msg}",
                    UserWarning,
                    stacklevel=2
                )
                return False
        
        # Compare grid datasets (drop xi_coarse dimension from self.grid.ds before comparison)
        # Extract grid dataset from POSTCONFIG blueprint
        # Handle both Pydantic model and dict cases (model_construct may leave nested objects as dicts)
        grid_obj = postconfig_blueprint.grid
        if grid_obj:
            # Get grid data - handle both Pydantic model and dict
            if isinstance(grid_obj, dict):
                grid_data = grid_obj.get("data")
            else:
                grid_data = grid_obj.data if hasattr(grid_obj, 'data') else None
            
            if grid_data:
                # Get the first resource location from the grid data
                grid_resource = grid_data[0] if isinstance(grid_data, list) and len(grid_data) > 0 else None
                # Handle both Pydantic model and dict for resource
                if grid_resource:
                    if isinstance(grid_resource, dict):
                        grid_location = grid_resource.get("location")
                    else:
                        grid_location = grid_resource.location if hasattr(grid_resource, 'location') else None
                    
                    if grid_location:
                        try:
                            # Load grid dataset from blueprint
                            grid_location_str = str(grid_location)
                            file_blueprint_grid_ds = xr.open_dataset(grid_location_str)
                            
                            # Prepare current grid dataset (drop xi_coarse if present)
                            # This is a hack to get around the fact that the grid file has a 
                            # xi_coarse dimension that is not supported by the patition_netcdf function.
                            # https://github.com/CWorthy-ocean/roms-tools/issues/518
                            current_grid_ds = self.grid.ds.copy()
                            if "xi_coarse" in current_grid_ds.dims:
                                current_grid_ds = current_grid_ds.drop_dims("xi_coarse")
                            
                            # Prepare blueprint grid dataset (drop xi_coarse if present)
                            if "xi_coarse" in file_blueprint_grid_ds.dims:
                                file_blueprint_grid_ds = file_blueprint_grid_ds.drop_dims("xi_coarse")
                            
                            # Compare datasets
                            if not current_grid_ds.equals(file_blueprint_grid_ds):
                                warnings.warn(
                                    "Grid dataset does not match POSTCONFIG blueprint grid dataset.",
                                    UserWarning,
                                    stacklevel=2
                                )
                                file_blueprint_grid_ds.close()
                                return False
                            
                            file_blueprint_grid_ds.close()
                        except Exception as e:
                            warnings.warn(
                                f"Failed to compare grid datasets: {e}",
                                UserWarning,
                                stacklevel=2
                            )
                            return False
        
        # Find all instances of "partitioned" in "data" fields and ensure they all match partition_files
        def extract_partitioned_flags(obj: Any) -> List[bool]:
            """Recursively extract all partitioned flags from 'data' fields."""
            partitioned_flags = []
            
            if obj is None:
                return partitioned_flags
            
            # If it's a dict, check for "data" key and recurse
            if isinstance(obj, dict):
                # If this dict has a "data" key, extract partitioned flags from it
                if "data" in obj:
                    data_value = obj["data"]
                    if isinstance(data_value, list):
                        for item in data_value:
                            if isinstance(item, dict) and "partitioned" in item:
                                partitioned_flags.append(item["partitioned"])
                    elif isinstance(data_value, dict) and "partitioned" in data_value:
                        partitioned_flags.append(data_value["partitioned"])
                # Recurse into all values to find nested "data" fields
                for value in obj.values():
                    partitioned_flags.extend(extract_partitioned_flags(value))
                return partitioned_flags
            
            # If it's a list, recurse into items
            if isinstance(obj, list):
                for item in obj:
                    partitioned_flags.extend(extract_partitioned_flags(item))
                return partitioned_flags
            
            return partitioned_flags
        
        # Extract all partitioned flags from the POSTCONFIG blueprint
        blueprint_partitioned_flags = extract_partitioned_flags(file_dict)
        
        # Check if all partitioned flags match partition_files
        if blueprint_partitioned_flags:
            mismatched_flags = [flag for flag in blueprint_partitioned_flags if flag != partition_files]
            if mismatched_flags:
                warnings.warn(
                    f"Partitioned flags in POSTCONFIG blueprint do not match partition_files={partition_files}. "
                    f"Found flags: {set(blueprint_partitioned_flags)}",
                    UserWarning,
                    stacklevel=2
                )
                return False
        
        return True
    
    def _load_blueprint_file(self, stage: Optional[str] = None, load_settings: bool = True) -> Optional[cstar_models.RomsMarblBlueprint]:
        """
        Load blueprint from file for the specified stage.
        
        Parameters
        ----------
        stage : Optional[str], optional
            Blueprint stage to load. If None, uses self._stage.
            If self._stage is also None, defaults to POSTCONFIG.
        load_settings : bool, optional
            If True, load settings from sidecar file. Defaults to True.
        
        Returns
        -------
        Optional[cstar_models.RomsMarblBlueprint]
            Loaded blueprint or None if file doesn't exist or loading fails.
        """
        # Determine which stage to use
        if stage is None:
            stage = self._stage if self._stage is not None else BlueprintStage.PRECONFIG
        
        # Get blueprint file path for this stage
        bp_path = self.path_blueprint(stage=stage, run_params=None)
        
        if not bp_path.exists():
            return None
        
        try:
            # Try to deserialize with full validation first
            # Suppress Pydantic serialization warnings (YAML may contain dicts where models expected)
            with warnings.catch_warnings():
                # Filter all UserWarnings from pydantic module and pydantic.main
                warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
                warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
                # Also filter warnings with "Pydantic" in the message
                warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
                warnings.filterwarnings("ignore", message=".*serializer.*", category=UserWarning)
                blueprint = deserialize(bp_path, cstar_models.RomsMarblBlueprint)
        except Exception as e:
            # If validation fails (e.g., files don't exist), try lenient loading
            try:
                # Load YAML as dict and use model_construct to bypass validation
                with bp_path.open("r") as f:
                    blueprint_data = yaml.safe_load(f)
                # Use model_construct to bypass validation (files may not exist)
                blueprint = cstar_models.RomsMarblBlueprint.model_construct(**blueprint_data)
            except Exception as e2:
                # If lenient loading also fails, issue a warning and return None
                warnings.warn(
                    f"Failed to load blueprint from {bp_path}: "
                    f"{type(e).__name__}: {e}. "
                    f"Lenient loading also failed: {type(e2).__name__}: {e2}",
                    UserWarning,
                    stacklevel=2
                )
                return None
        
        # Load settings from sidecar file if blueprint was loaded and load_settings is True
        if blueprint is not None and load_settings:
            self._load_settings_from_file(bp_path)
        
        return blueprint
    
    @property
    def blueprint_from_file(self) -> Optional[cstar_models.RomsMarblBlueprint]:
        """
        Load and return blueprint from file based on current stage.
        
        Uses self._stage to determine which blueprint file to load.
        If self._stage is None, defaults to POSTCONFIG stage.
        
        Returns
        -------
        Optional[cstar_models.RomsMarblBlueprint]
            Loaded blueprint or None if file doesn't exist or loading fails.
        """
        # Suppress Pydantic warnings when loading blueprint
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
            warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*serializer.*", category=UserWarning)
            return self._load_blueprint_file()
    
    def get_ds(self, field: str, from_file: bool = True) -> Optional[List[xr.Dataset]]:
        """
        Load xarray Datasets from NetCDF files referenced in a blueprint field.
        
        This method reads the file locations from a specific blueprint field and
        returns lazy-loaded xarray Datasets. Returns a list of datasets even for
        single files to maintain consistency and avoid alignment issues.
        
        **Field Paths:**
        
        Field paths can be simple (e.g., "grid") or nested (e.g., "forcing.surface").
        Nested paths are resolved by traversing the blueprint structure.
        
        **Data Source:**
        
        The `from_file` parameter determines which blueprint to use:
        - `True`: Uses blueprint loaded from disk (default, recommended)
        - `False`: Uses in-memory blueprint (may not reflect persisted state)
        
        Parameters
        ----------
        field : str
            Field path in blueprint. Examples:
            - "grid": Grid dataset
            - "initial_conditions": Initial conditions dataset
            - "forcing.surface": Surface forcing datasets
            - "forcing.tidal": Tidal forcing datasets
            - "cdr_forcing": CDR forcing dataset
        from_file : bool, optional
            If True, loads blueprint from disk first (recommended).
            If False, uses in-memory blueprint.
            Default is True.
        
        Returns
        -------
        Optional[List[xr.Dataset]]
            List of lazy-loaded xarray Datasets, one per file referenced in the field.
            Returns None if the field doesn't exist or has no file locations.
            Always returns a list, even for single files.
        """
        # Select which blueprint to use
        # Suppress Pydantic warnings when accessing blueprint
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
            warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*serializer.*", category=UserWarning)
            if from_file:
                blueprint = self.blueprint_from_file
            else:
                blueprint = self.blueprint
        
        if blueprint is None:
            return None
        
        # Navigate to the field (handle nested fields like "forcing.surface")
        # Handle both model instances and dicts at each level
        # Suppress warnings during navigation as well
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
            warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*serializer.*", category=UserWarning)
            field_parts = field.split(".")
            data = blueprint
            for part in field_parts:
                # Convert model instances to dicts for easier navigation
                if hasattr(data, 'model_dump'):
                    data = data.model_dump()
                
                if isinstance(data, dict):
                    if part not in data:
                        return None
                    data = data[part]
                elif hasattr(data, part):
                    data = getattr(data, part)
                else:
                    return None
        
        # Convert Dataset to dict if it's a model instance
        if isinstance(data, cstar_models.Dataset):
            data = data.model_dump()
        
        # Extract locations from dict structure        
        if isinstance(data, dict) and "data" in data:
            location_list = [
                item.get("location") 
                for item in data["data"] 
                if isinstance(item, dict) and item.get("location")
            ]
        else:
            return None

        
        if not location_list:
            return None
        
        # Convert locations to strings (handle Path and HttpUrl objects)
        location_strs = []
        for location in location_list:
            if isinstance(location, Path):
                location_strs.append(str(location))
            elif hasattr(location, '__str__'):
                location_strs.append(str(location))
            else:
                location_strs.append(location)
        
        # Return a list of datasets (one per file) instead of combining them
        # This avoids alignment errors when datasets have incompatible dimensions
        # Suppress xarray FutureWarning about timedelta decoding
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning, module='xarray')
            return [xr.open_dataset(location, decode_timedelta=False) for location in location_strs]
    
    def ensure_source_data(self, include_streamable: bool = False):
        """
        Ensure source data is prepared and ready for input file generation.
        
        This method prepares all required source datasets (grid, initial conditions,
        forcing data, etc.) using the model specification's dataset requirements.
        The prepared data is stored in `self.src_data` and used by `generate_inputs()`
        to create input files.
        
        **When to Call:**
        
        This method is called automatically by `generate_inputs()` if source data
        hasn't been prepared. It can also be called explicitly to prepare data
        before generating inputs, or to re-prepare data with different options.
        
        Parameters
        ----------
        include_streamable : bool, optional
            If True, include streamable datasets in preparation (datasets that
            can be accessed on-demand rather than pre-downloaded).
            Default is False.
        
        Raises
        ------
        RuntimeError
            If grid is not initialized (should be created during initialization).
        """
        if self.grid is None:
            raise RuntimeError(
                "Grid must be created before preparing source data. "
                "This should have been created during initialization."
            )
        
        if self._model_spec is None:
            self._load_model_spec()
        
        self.src_data = source_data.SourceData(
            datasets=self._model_spec.datasets,
            clobber=False,
            grid=self.grid,
            grid_name=self.grid_name,
            start_time=self.start_date,
            end_time=self.end_date,
        ).prepare_all(include_streamable=include_streamable)
    
    def generate_inputs(
        self,
        clobber: bool = False,
        use_dask: bool = True,
        partition_files: bool = False,
        test: bool = False,
        prompt_if_files_exist: bool = True,
    ) -> cstar_models.RomsMarblBlueprint:
        """
        Generate ROMS input files and advance blueprint to POSTCONFIG stage.
        
        This method generates all required input files (grid, initial conditions,
        forcing, etc.) and updates the blueprint with actual file locations.
        
        **Process:**
        
        1. Checks if existing POSTCONFIG blueprint matches current configuration
        2. If not matching or `clobber=True`:
           - Prepares source data if needed (calls `ensure_source_data()`)
           - Generates all input files via `RomsMarblInputData.generate_all()`
           - Updates settings dictionaries with input-specific values
           - Updates blueprint with actual data file locations
           - Sets `_stage` to POSTCONFIG
           - Persists blueprint to disk
        3. If matching blueprint exists:
           - Loads existing blueprint from disk (skips regeneration)
           
        **Stage Transition:**
        
        - **Input:** Blueprint in PRECONFIG stage (placeholder data)
        - **Output:** Blueprint in POSTCONFIG stage (actual data files)
        
        **Settings:**
        
        Settings dictionaries are updated with values generated during input
        file creation (e.g., grid dimensions, file paths). These updates are
        merged with existing settings to preserve user overrides.

        Parameters
        ----------
        clobber : bool, optional
            If True, overwrite existing input files even if blueprint matches.
            Default is False.
        use_dask : bool, optional
            If True, use dask for parallel computations. Default is True.
        partition_files : bool, optional
            If True, partition input files across tiles. Currently not implemented.
            Default is False.
        test : bool, optional
            If True, truncate the generation loop after 2 iterations for testing.
            Default is False.
            
        Returns
        -------
        cstar_models.RomsMarblBlueprint
            The blueprint updated with all input file locations (POSTCONFIG stage).
            
        Raises
        ------
        RuntimeError
            If blueprint is not initialized, or if settings are not initialized.
        NotImplementedError
            If partition_files is True (functionality not yet implemented).
        """
        # Raise error if partition_files is True (functionality under development)
        if partition_files:
            raise NotImplementedError(
                "File partitioning functionality is not yet fully implemented. "
                "Please set partition_files=False."
            )

        if self.blueprint is None:
            raise RuntimeError("Blueprint must be initialized before generating inputs")

        postconfig_path = self.path_blueprint(stage=BlueprintStage.POSTCONFIG)
        force_regenerate = False
        if postconfig_path.exists() and not clobber:
            make_new_blueprint = self._prompt_yes_no(
                f"POSTCONFIG blueprint already exists at {postconfig_path}. Create a new blueprint?"
            ) if prompt_if_files_exist else False
            if make_new_blueprint:
                self._delete_blueprint_and_settings(postconfig_path)
                force_regenerate = True
            else:
                existing_postconfig_blueprint = self._load_blueprint_file(
                    stage=BlueprintStage.POSTCONFIG,
                    load_settings=False,
                )
                if existing_postconfig_blueprint is not None:
                    seen: set[Path] = set()
                    missing_required: List[Path] = []
                    for p in self._required_netcdf_paths_from_blueprint(
                        existing_postconfig_blueprint
                    ):
                        canon = self._canonicalize_stored_input_netcdf_path(p).resolve()
                        if canon in seen:
                            continue
                        seen.add(canon)
                        if not canon.exists():
                            missing_required.append(canon)
                    if missing_required:
                        print(
                            "ℹ️  Existing POSTCONFIG blueprint references missing NetCDF files. "
                            "Missing files will be generated:"
                        )
                        for missing_path in missing_required:
                            print(f"  - {missing_path}")
                        force_regenerate = True

        if force_regenerate or not self._file_blueprint_data_match(partition_files=partition_files) or clobber:
            # Ensure settings are initialized before generating inputs
            # If settings are not present or empty, something has gone wrong
            if not hasattr(self, '_settings_compile_time') or not self._settings_compile_time:
                raise RuntimeError(
                    "_settings_compile_time is not initialized or is empty. "
                )
            if not hasattr(self, '_settings_run_time') or not self._settings_run_time:
                raise RuntimeError(
                    "_settings_run_time is not initialized or is empty. "
                )
            
            # Prepare source data if not already done
            if self.src_data is None:
                self.ensure_source_data(include_streamable=False)
            
            # Create inputs instance
            blueprint_elements, settings_compile_time, settings_run_time = input_data.RomsMarblInputData(
                domain_name=self.name,
                start_date=self.start_date,
                end_date=self.end_date,
                input_data_dir_override=self.input_data_dir,
                model_spec=self._model_spec,
                grid=self.grid,
                grid_child=self.grid_child,
                metadata_child=self.metadata_child,
                boundaries=self.open_boundaries,
                source_data=self.src_data,
                blueprint_dir=self.blueprint_dir,
                partitioning=self.partitioning,
                cdr_forcing=self.cdr_forcing,
                use_dask=use_dask,
            ).generate_all(partition_files=partition_files, clobber=clobber, test=test)
            
            if blueprint_elements is None:
                raise RuntimeError(
                    "Blueprint mismatch detected, but input files exist. "
                    "Set clobber=True to overwrite existing input files."
                )

           # Apply settings from input data generation (deep merge to preserve existing settings)
            self._update_settings_compile_time(settings_compile_time)
            self._update_settings_run_time(settings_run_time)

            if test:
               return

            # Map blueprint_elements to self.blueprint
            # Update the blueprint with the generated input data
            blueprint_dict = self.blueprint.model_dump()
            blueprint_dict["grid"] = blueprint_elements.grid.model_dump() if blueprint_elements.grid else None
            blueprint_dict["initial_conditions"] = blueprint_elements.initial_conditions.model_dump() if blueprint_elements.initial_conditions else None
            blueprint_dict["forcing"] = blueprint_elements.forcing.model_dump() if blueprint_elements.forcing else None
            blueprint_dict["cdr_forcing"] = blueprint_elements.cdr_forcing.model_dump() if blueprint_elements.cdr_forcing else None
            blueprint_dict["nesting_info"] = blueprint_elements.nesting_info.model_dump() if blueprint_elements.nesting_info else None
                    
             
            # TODO: Uncomment this when settings are implemented in the blueprint
            # At present, we're using a sidecar file to store settings
            # blueprint_dict["model_params"] = settings_compile_time
            # blueprint_dict["runtime_params"] = settings_run_time
            # Set to None since they're stored in sidecar files
            blueprint_dict["model_params"] = None
            blueprint_dict["runtime_params"] = None

            self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(**blueprint_dict)
            self._stage = BlueprintStage.POSTCONFIG
            
            # Persist blueprint to YAML file (skip in test mode)
            self.persist()
        else:            
            # Use existing blueprint from file
            print(f"ℹ️  Using existing blueprint from file: {self.path_blueprint(stage=BlueprintStage.POSTCONFIG).name}")
            self.blueprint = self._load_blueprint_file(stage=BlueprintStage.POSTCONFIG, load_settings=True)
            self._stage = BlueprintStage.POSTCONFIG
        
        return self.blueprint

    def _merge_settings_override_file(self, path: Path, kind: str) -> None:
        """
        Load a YAML override file and merge into compile- or run-time settings.

        Parameters
        ----------
        path : Path
            Path to an override YAML file.
        kind : str
            ``\"compile\"`` or ``\"run\"``.
        """
        if kind not in ("compile", "run"):
            raise ValueError(f"kind must be 'compile' or 'run', got {kind!r}")
        if not path.is_file():
            warnings.warn(
                f"Override file {path} does not exist; skipping.",
                UserWarning,
                stacklevel=2,
            )
            return
        try:
            with path.open("r") as f:
                raw = yaml.safe_load(f)
        except OSError as exc:
            warnings.warn(
                f"Could not read settings override file {path}: {exc}",
                UserWarning,
                stacklevel=2,
            )
            return
        if raw is None:
            return
        if not isinstance(raw, dict):
            warnings.warn(
                f"Override file {path} must contain a YAML mapping at the top level; ignoring.",
                UserWarning,
                stacklevel=2,
            )
            return

        section = "compile_time" if kind == "compile" else "run_time"
        other_section = "run_time" if kind == "compile" else "compile_time"
        payload: Dict[str, Any]
        if section in raw:
            section_data = raw.get(section)
            if section_data is None:
                return
            if not isinstance(section_data, dict):
                warnings.warn(
                    f"Override file {path} has non-mapping '{section}' section; ignoring.",
                    UserWarning,
                    stacklevel=2,
                )
                return
            payload = section_data
        elif other_section in raw:
            # File is explicitly scoped to the opposite settings tree.
            return
        else:
            payload = raw

        target = self._settings_compile_time if kind == "compile" else self._settings_run_time
        label = "compile-time" if kind == "compile" else "run-time"
        merged: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in target:
                merged[key] = value
            else:
                warnings.warn(
                    f"Ignoring unknown {label} override top-level key {key!r} in {path}; "
                    "it is not present in the model defaults.",
                    UserWarning,
                    stacklevel=2,
                )
        if not merged:
            return
        if kind == "compile":
            self._update_settings_compile_time(merged)
        else:
            self._update_settings_run_time(merged)
        print(f"ℹ️  Merged {label} settings from {path.resolve()}")

    @property
    def _override_paths(self) -> List[Path]:
        if not self.override:
            return []
        return [Path(p).expanduser().resolve() for p in self.override]

    def _merge_settings_override_files(self, kind: str) -> None:
        for path in self._override_paths:
            self._merge_settings_override_file(path, kind)

    def _init_settings_compile_time(self) -> None:
        """
        Initialize compile-time settings dictionary from model defaults.
        
        Loads default compile-time settings from the model specification and
        stores them in `_settings_compile_time`. This dictionary is used as
        the basis for template rendering during `configure_build()`.
        
        Settings are deep-copied from the model spec to avoid modifying the
        original defaults. User overrides can be applied via `_update_settings_compile_time()`
        or by passing `compile_time_settings` to `configure_build()`.

        **Called by:** `_initialize_blueprint()` during initialization.
        """
        # Initialize from defaults (deep copy to avoid modifying the original)
        self._settings_compile_time = copy.deepcopy(self._model_spec.settings.compile_time.settings_dict)
        if self.grid_child is not None:
            period_default = roms_tools_default_nesting_period_seconds()
            if "metadata" in self.grid_kwargs_child:
               if "period" in self.grid_kwargs_child["metadata"]:
                  self._settings_compile_time["extract_data"]["extract_period"] = self.grid_kwargs_child["metadata"]["period"]
               else:
                  self._settings_compile_time["extract_data"]["extract_period"] = period_default
            else:
               self._settings_compile_time["extract_data"]["extract_period"] = period_default

        self._merge_settings_override_files("compile")

    def _init_settings_run_time(self, dt: Optional[float] = None) -> None:
        """
        Initialize run-time settings dictionary from model defaults.
        
        Loads default run-time settings from the model specification and stores
        them in `_settings_run_time`. This dictionary is used as the basis for
        template rendering during `configure_build()`.
        
        **Dynamic Values:**
        
        Some settings are set dynamically based on instance properties:
        - `title.casename`: Set from `self.casename`
        - `output_root_name.output_root_name`: Set from `self.run_output_dir`
        - `time_stepping`: Calculated based on simulation dates and timestep
        
        Settings are deep-copied from the model spec to avoid modifying the
        original defaults. User overrides can be applied via `_update_settings_run_time()`
        or by passing `run_time_settings` to `configure_build()`.
        
        Parameters
        ----------
        dt : Optional[float], optional
            Timestep in seconds for time_stepping calculation. If None, computed
            from CFL criterion using grid properties. Default is None.

        **Called by:** `_initialize_blueprint()` during initialization.
        """
        # Initialize from defaults (deep copy to avoid modifying the original)
        self._settings_run_time = copy.deepcopy(self._model_spec.settings.run_time.settings_dict)
        
        # Set dynamic values that depend on instance properties
        self._settings_run_time["roms.in"]["title"] = dict( 
            casename = self.casename,   
        )
        self._settings_run_time["roms.in"]["output_root_name"] = dict( 
            output_root_name = str(self.run_output_dir / "output" / self.casename),
        )
        
        # Set timestepping defaults (will compute dt from CFL if dt is None)
        self._set_run_time_settings_timestepping_defaults(dt=dt)

        self._merge_settings_override_files("run")

    def _update_settings_compile_time(self, settings_compile_time: Dict[str, Any]) -> None:
        """
        Update compile-time settings by recursively merging nested dictionaries.
        
        Top-level keys must already exist on the builder. Nested dicts are merged
        recursively so partial overrides do not drop sibling keys.
        
        **Merging Behavior:**
        
        - If key exists in both: nested dicts are merged recursively
        - If key exists only in new settings: raises ValueError (unknown key)
        - Non-dict values, or dict replacing a non-dict: replaced directly
        
        Parameters
        ----------
        settings_compile_time : Dict[str, Any]
            Dictionary of compile-time settings to merge into `_settings_compile_time`.
            Top-level keys must match existing keys in `_settings_compile_time`.
            
        Raises
        ------
        ValueError
            If a top-level key in `settings_compile_time` is not present in
            `_settings_compile_time` (unknown setting key).
        """
        if not settings_compile_time:
            return
        
        for key, value in settings_compile_time.items():
            if key in self._settings_compile_time:
                if isinstance(self._settings_compile_time[key], dict) and isinstance(value, dict):
                    value_copy = copy.deepcopy(value)
                    _deep_merge_settings_dict(self._settings_compile_time[key], value_copy)
                else:
                    self._settings_compile_time[key] = (
                        copy.deepcopy(value)
                        if not isinstance(value, (str, int, float, bool, type(None)))
                        else value
                    )
            else:
                raise ValueError(
                    f"Unknown compile-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_compile_time.keys())}"
                )
    
    def _update_settings_run_time(self, settings_run_time: Dict[str, Any]) -> None:
        """
        Update run-time settings by recursively merging nested dictionaries.
        
        Top-level keys must already exist on the builder. Nested dicts are merged
        recursively so partial overrides (e.g. only ``dt`` under ``time_stepping``)
        do not remove sibling keys populated from defaults.
        
        **Merging Behavior:**
        
        - If key exists in both: nested dicts are merged recursively
        - If key exists only in new settings: raises ValueError (unknown key)
        - Non-dict values, or dict replacing a non-dict: replaced directly
        
        Parameters
        ----------
        settings_run_time : Dict[str, Any]
            Dictionary of run-time settings to merge into `_settings_run_time`.
            Top-level keys must match existing keys in `_settings_run_time`.
            
        Raises
        ------
        ValueError
            If a top-level key in `settings_run_time` is not present in
            `_settings_run_time` (unknown setting key).
        """
        # TODO: Consider adding a test for the merge operation passing {} to make sure it properly handles the edge case of an empty input.
        # Consider adding a test for the merge operation passing {"nothing-shared": "foo"} to test the no-intersection edge case.
        if not settings_run_time:
            return
        
        for key, value in settings_run_time.items():
            if key in self._settings_run_time:
                if isinstance(self._settings_run_time[key], dict) and isinstance(value, dict):
                    value_copy = copy.deepcopy(value)
                    # TODO: Evaluate whether corrective logic for passed-in values should live here
                    # Do we need to correct anything else?
                    if key == "roms.in" and "time_stepping" in value_copy:
                        ts = value_copy["time_stepping"]
                        if isinstance(ts, dict) and "ntimes" in ts:
                            ts["ntimes"] = int(round(ts["ntimes"]))
                    _deep_merge_settings_dict(self._settings_run_time[key], value_copy)
                else:
                    self._settings_run_time[key] = (
                        copy.deepcopy(value)
                        if not isinstance(value, (str, int, float, bool, type(None)))
                        else value
                    )
            else:
                # Unknown key - raise error
                raise ValueError(
                    f"Unknown run-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_run_time.keys())}"
                )
    
    def _set_run_time_settings_timestepping_defaults(self, dt: Optional[float] = None):
        """
        Update run-time timestepping settings in the settings dictionary.
        
        Sets the `time_stepping` section of `_settings_run_time["roms.in"]` with
        calculated values based on simulation dates and timestep.
        
        **Timestep Calculation:**
        
        If `dt` is not provided, it is computed from CFL criterion:
        1. Computes minimum grid spacing (dx, dy) from size_x/nx and size_y/ny
        2. Estimates fastest gravity wave speed: c = sqrt(g * H_max)
        3. Applies CFL condition: dt = CFL * dx_min / c
        
        **Values Set:**
        
        - `ntimes`: Number of timesteps (calculated from simulation duration / dt)
        - `dt`: Timestep in seconds (provided or computed)
        - `ndtfast`: Number of fast timesteps per baroclinic timestep (default: 60)
        - `ninfo`: Frequency of information output (default: 1)
        
        Parameters
        ----------
        dt : Optional[float]
            Timestep in seconds. If None, computed from CFL criterion using grid
            properties. Default is None.
        
        **Called by:** `_init_settings_run_time()` during initialization.
        """
        
        if dt is None:
            dt = compute_timestep_from_cfl(
                grid_size_x=self.grid.size_x,
                grid_size_y=self.grid.size_y,
                grid_nx=self.grid.nx,
                grid_ny=self.grid.ny,
                grid_ds=self.grid.ds,
            )
            
        ntimes = int(round((self.end_date - self.start_date).days * 24 * 3600 / dt))
        self._settings_run_time["roms.in"]["time_stepping"] = dict(
            ntimes = ntimes,
            dt = dt,
            ndtfast = 60, # TODO: Think about if how to better NDTFAST based on this dt
            ninfo = 1,
        )
   
    def configure_build(
        self,
        compile_time_settings: Dict[str, Any] = None,
        run_time_settings: Dict[str, Any] = None,
        **kwargs
    ):
        """
        Configure blueprint by rendering templates and advance to BUILD stage.
        
        This method renders Jinja2 templates with current settings to produce
        configuration files needed for model compilation and execution.
        
        **Process:**
        
        1. Validates blueprint is initialized and template configuration exists
        2. Merges user-provided settings overrides with existing settings
        3. Clears compile-time and run-time code output directories
        4. Renders Jinja2 templates:
           - Compile-time templates (e.g., bgc.opt, cppdefs.opt, param.opt)
           - Run-time templates (e.g., roms.in)
        5. Updates blueprint with rendered code locations and file lists
        6. Sets blueprint model_params and runtime_params
        7. Sets `_stage` to BUILD
        8. Persists blueprint to disk
        9. Creates ROMSSimulation instance from blueprint
        
        **Stage Transition:**
        
        - **Input:** Blueprint in POSTCONFIG stage (with input data files)
        - **Output:** Blueprint in BUILD stage (with rendered configuration files)
        
        **Settings:**
        
        Settings are merged using deep merge, preserving existing values while
        allowing user overrides. Run-time timestep (`dt`) can be provided
        explicitly or will be computed from CFL criterion.
        
        **Template Rendering:**
        
        Templates are rendered from the model specification's template locations
        using the current settings dictionaries. The rendered files are written
        to the code output directories and the blueprint is updated with their
        locations.
        
        Parameters
        ----------
        compile_time_settings : Dict[str, Any], optional
            Compile-time settings to override defaults. Merged with existing
            settings using deep merge. Defaults to empty dict.
        run_time_settings : Dict[str, Any], optional
            Run-time settings to override defaults. If a "time_stepping" dict
            with a "dt" key is provided, it will be used for timestep calculation;
            otherwise, the timestep is computed from CFL criterion.
            Defaults to empty dict.
        **kwargs
            Additional keyword arguments (currently unused, reserved for future use).
        
        Returns
        -------
        ROMSSimulation
            The C-Star simulation instance created from the configured blueprint.
        
        Raises
        ------
        RuntimeError
            If blueprint is not initialized (must call `generate_inputs()` first).
        ValueError
            If the model spec does not have required template configuration or
            properties (e.g., n_tracers).
        """
        # Initialize to empty dict if None
        if compile_time_settings is None:
            compile_time_settings = {}
        if run_time_settings is None:
            run_time_settings = {}

        # Validate that blueprint is initialized
        if self.blueprint is None:
            raise RuntimeError("Blueprint must be initialized before configuration. Call generate_inputs() first.")

        # Validate template configuration
        if (self._model_spec.templates is None or
            self._model_spec.templates.compile_time is None or
            self._model_spec.templates.compile_time.filter is None):
            raise ValueError("Model spec must have templates.compile_time.filter with files list")
        if (self._model_spec.templates.run_time is None or
            self._model_spec.templates.run_time.filter is None):
            raise ValueError("Model spec must have templates.run_time.filter with files list")

        # Initialize settings from defaults if not already initialized
        if not hasattr(self, '_settings_compile_time') or self._settings_compile_time is None:
            self._init_settings_compile_time()
        if not hasattr(self, '_settings_run_time') or self._settings_run_time is None:
            self._init_settings_run_time()

        # Update settings with user-provided overrides (deep merge to preserve existing settings)
        self._update_settings_compile_time(compile_time_settings)
        self._update_settings_run_time(run_time_settings)



        # Ensure ntimes is an integer (don't recalculate, just ensure type is correct)
        if "roms.in" in self._settings_run_time and "time_stepping" in self._settings_run_time["roms.in"]:
            if "ntimes" in self._settings_run_time["roms.in"]["time_stepping"]:
                ntimes = self._settings_run_time["roms.in"]["time_stepping"]["ntimes"]
                # Convert to integer if it's a float
                if isinstance(ntimes, float):
                    self._settings_run_time["roms.in"]["time_stepping"]["ntimes"] = int(round(ntimes))


            
        # Render templates and get location and file list
        # Get n_tracers from model_spec properties
        if self._model_spec.settings.properties is not None:
            n_tracers = self._model_spec.settings.properties.n_tracers
        else:
            raise ValueError("Model spec must have settings.properties.n_tracers")

        # Ensure build output directories exist before rendering templates.
        self.compile_time_code_dir.mkdir(parents=True, exist_ok=True)
        self.run_time_code_dir.mkdir(parents=True, exist_ok=True)
        
        compile_time_code = render_roms_settings(
            template_files=self._model_spec.templates.compile_time.filter.files,
            template_dir=self._model_spec.templates.compile_time.location,
            settings_dict=self._settings_compile_time,
            code_output_dir=self.compile_time_code_dir,
            n_tracers=n_tracers,
        )
        run_time_code = render_roms_settings(
            template_files=self._model_spec.templates.run_time.filter.files,
            template_dir=self._model_spec.templates.run_time.location,
            settings_dict=self._settings_run_time,
            code_output_dir=self.run_time_code_dir,
            n_tracers=n_tracers,
        )

        # Suppress Pydantic serialization warnings when using model_dump(mode='json') and model_construct
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            
            blueprint_dict = self.blueprint.model_dump(mode='json')
            code_dict = blueprint_dict["code"]
            # Convert dicts from render_roms_settings to CodeRepository objects
            code_dict["compile_time"] = cstar_models.CodeRepository.model_construct(**compile_time_code)
            code_dict["run_time"] = cstar_models.CodeRepository.model_construct(**run_time_code)
            blueprint_dict["code"] = cstar_models.ROMSCompositeCodeRepository.model_construct(**code_dict)

            blueprint_dict["model_params"] = {
                "time_step": self._settings_run_time["roms.in"]["time_stepping"]["dt"],
            }
            blueprint_dict["runtime_params"] = {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "output_dir": self.run_output_dir,
            }

            self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(**blueprint_dict)
            self._stage = BlueprintStage.BUILD
            self.persist()

        return

    @staticmethod
    def prep_cstar_environment(
            account_key: Optional[str] = None,
            queue_name: Optional[str] = None,
            walltime: str | None = None,
            clobber: bool = True,
            on_compute_node: bool = False,
            n_procs_available: int | None = None,
        ):

        """
        Configure the appropriate settings for the C-Star executable.

        Parameters
        ----------
        account_key: Account name for slurm jobs. Defaults to machine config if None.
        queue_name: Queue name for slurm jobs. Defaults to machine config if None.
        walltime: Max wall time for slurm jobs. Defaults to 6 hours.
        clobber: Whether to clear the working directory for this simulation before running. Defaults to True. If False,
            C-star will fail if files exist already.
        on_compute_node: Whether to run ROMS on the current node. Defaults to False (will submit slurm jobs if on HPC).
        n_procs_available: How many processors to utilize for joining operations. If 0, auto-detect. If you leave it 0
            and you're on a shared or login node, you're probably going to use too many and get booted. If None, don't
            change it (e.g. you have set it externally)
        """


        mc = config.machine_config
        queues = mc.queues or {}

        # precedence: passed variable > pre-existing env-var setting > internal machine config > some default
        account_key = account_key or os.getenv("CSTAR_SLURM_ACCOUNT") or mc.account or ""
        queue_name = queue_name or os.getenv("CSTAR_SLURM_QUEUE") or queues.get("default") or ""
        walltime = walltime or os.getenv("CSTAR_SLURM_WALLTIME") or "6:00:00"
        clobber = "1" if clobber else os.getenv("CSTAR_CLOBBER_WORKING_DIR", "0")
        in_active_alloc = "1" if on_compute_node else os.getenv("CSTAR_IN_ACTIVE_ALLOCATION", "0")

        # set everything
        os.environ["CSTAR_CLOBBER_WORKING_DIR"] = clobber
        os.environ["CSTAR_IN_ACTIVE_ALLOCATION"] = in_active_alloc
        os.environ["CSTAR_SLURM_ACCOUNT"] = account_key
        os.environ["CSTAR_SLURM_QUEUE"] = queue_name
        os.environ["CSTAR_SLURM_WALLTIME"] = walltime

        if n_procs_available:
            os.environ["CSTAR_NPROCS_POST"] = str(n_procs_available)
        elif n_procs_available == 0:
            if os.getenv("CSTAR_NPROCS_POST"):
                del os.environ["CSTAR_NPROCS_POST"]
        #implicit: elif n_procs_available is None, do nothing

        if config.system == "RCAC_anvil":
            # find the right conda path to this environment and put it in the front of the path
            # otherwise, it might find the wrong cstar executable
            bin_dir = Path(sys.executable).parent
            cstar_exe = bin_dir / "cstar"
            assert cstar_exe.is_file()

            new_link = Path.cwd() / "cstar"
            if new_link.exists():
                new_link.unlink()

            # make symlink in current dir to correct cstar
            os.symlink(cstar_exe, new_link)
            _current_path = os.environ["PATH"]
            os.environ["PATH"] = str(Path.cwd()) + os.pathsep + _current_path


    async def run(
        self,
    ):
        """
        Run C-Star for this Builder's BUILD blueprint
        """

        self.prep_cstar_environment()

        request = RunnerRequest(uri=str(self.path_blueprint(stage=BlueprintStage.BUILD)), bp_type=RomsMarblBlueprint, name=self.casename)
        service_cfg = get_service_config(log_level="INFO")
        job_cfg = get_job_config()
        runner = RomsMarblRunner(request=request, service_cfg=service_cfg, job_cfg=job_cfg)
        await runner.execute()

        # Persist blueprint to file
        self._stage = BlueprintStage.RUN
        self.persist()



    def set_blueprint_state(self, state: str) -> None:
        """
        Set the state of the blueprint.

        Parameters
        ----------
        state : str
            The new state for the blueprint. Must be a valid BlueprintState value from cstar.applications.roms_marbl.models.
            Common values include "notset", "draft", "configured", "ready", etc.
            See cstar_models.BlueprintState for the complete list of valid values.
        
        Raises
        ------
        ValueError
            If blueprint is None or if state is not a valid BlueprintState value.
        """
        if self.blueprint is None:
            raise ValueError("Cannot set state: blueprint is not initialized")
        
        # Validate state if BlueprintState is available
        try:
            from cstar.applications.roms_marbl.models import BlueprintState
            # Try to validate the state value
            if hasattr(BlueprintState, '__members__'):
                valid_states = set(BlueprintState.__members__.values())
                if state not in valid_states:
                    raise ValueError(
                        f"Invalid state '{state}'. Must be one of: {sorted(valid_states)}"
                    )
        except (ImportError, AttributeError):
            # BlueprintState might not be available or might not be an enum
            # In this case, we'll let Pydantic validation handle it
            pass
        
        # Update blueprint with new state
        # Use model_dump with exclude_none and mode='json' to handle placeholder values
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
            warnings.filterwarnings('ignore', message='.*Pydantic.*', category=UserWarning)
            warnings.filterwarnings('ignore', message='.*serialization.*', category=UserWarning)
            blueprint_dict = self.blueprint.model_dump(mode='json', exclude_none=True)
        blueprint_dict["state"] = state
        # Use model_construct to bypass validation for placeholder values
        self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(**blueprint_dict)
    
    def dump(self, file_path: Union[str, Path]) -> None:
        """
        Dump the exact state of CstarSpecBuilder to a YAML file.
        
        This method serializes all serializable fields including:
        - Regular Pydantic model fields (description, model_name, grid_name, etc.)
        - PrivateAttr fields (_model_spec, _stage, _settings_compile_time, _settings_run_time)
        - Complex nested objects (blueprint, src_data)
        
        Fields that cannot be serialized are excluded:
        - grid (excluded from model, but grid_kwargs is saved)
        - _datasets (xarray.Dataset objects - not directly YAML-serializable)
        - _cstar_simulation (runtime object - not serializable)
        
        Parameters
        ----------
        file_path : Union[str, Path]
            Path to the YAML file where the state will be saved.
        
        Notes
        -----
        - xarray.Dataset objects in _datasets are not serialized. They can be
          reconstructed from the blueprint's data entries after loading.
        - The grid object is not serialized, but grid_kwargs is saved, allowing
          the grid to be reconstructed using rt.Grid(**grid_kwargs).
        """
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Start with Pydantic model dump (includes all regular fields)
        state_dict = self.model_dump(mode='json', exclude_none=True)
        
        # Add PrivateAttr fields that can be serialized
        private_attrs = {}
        
        # Serialize _model_spec if it exists (Pydantic model)
        if self._model_spec is not None:
            private_attrs["_model_spec"] = self._model_spec.model_dump(mode='json', exclude_none=True)
        
        # Serialize _stage (simple string)
        if self._stage is not None:
            private_attrs["_stage"] = self._stage
        
        # Serialize settings dictionaries
        if self._settings_compile_time:
            private_attrs["_settings_compile_time"] = self._convert_paths_to_strings(self._settings_compile_time)
        if self._settings_run_time:
            private_attrs["_settings_run_time"] = self._convert_paths_to_strings(self._settings_run_time)
        
        # Serialize src_data if it exists (dataclass)
        if self.src_data is not None:
            # Convert dataclass to dict, but exclude grid object
            src_data_dict = dataclass_asdict(self.src_data)
            # Remove grid object if present (not serializable)
            src_data_dict.pop("grid", None)
            # Convert Path objects to strings
            private_attrs["src_data"] = self._convert_paths_to_strings(src_data_dict)
        
        # Note: _datasets and _cstar_simulation are intentionally excluded
        # as they contain xarray.Dataset objects and runtime objects that
        # cannot be easily serialized to YAML.
        
        # Combine state with private attrs
        state_dict["_private_attrs"] = private_attrs
        
        # Convert all Path objects to strings for YAML serialization
        state_dict = self._convert_paths_to_strings(state_dict)
        
        # Write to YAML file
        with file_path.open("w") as f:
            yaml.safe_dump(state_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    @classmethod
    def load(cls, file_path: Union[str, Path]) -> "CstarSpecBuilder":
        """
        Load CstarSpecBuilder state from a YAML file.
        
        This method deserializes a previously saved state and reconstructs
        the CstarSpecBuilder instance. After loading:
        - Regular Pydantic fields are restored
        - PrivateAttr fields are restored where possible
        - The grid object is reconstructed from grid_kwargs
        - The blueprint object is restored
        
        Fields that cannot be deserialized remain uninitialized:
        - _datasets: Will be populated when accessed (via datasets property)
        - _cstar_simulation: Will be initialized when build() is called
        
        Parameters
        ----------
        file_path : Union[str, Path]
            Path to the YAML file containing the saved state.
        
        Returns
        -------
        CstarSpecBuilder
            A new CstarSpecBuilder instance with state restored from the file.
        
        Notes
        -----
        - The grid object is automatically reconstructed from grid_kwargs
          in model_post_init().
        - xarray.Dataset objects in _datasets can be loaded later from
          the blueprint's data entries if needed.
        - Model validation and post-init hooks are executed, so the instance
          will be fully initialized and validated.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CstarSpecBuilder state file not found: {file_path}")
        
        # Load YAML file
        with file_path.open("r") as f:
            state_dict = yaml.safe_load(f) or {}
        
        # Extract private attributes
        private_attrs = state_dict.pop("_private_attrs", {})
        
        # Restore _model_spec if present
        model_spec_dict = private_attrs.pop("_model_spec", None)
        
        # Handle blueprint separately - use model_construct to handle None values
        blueprint_dict = state_dict.pop("blueprint", None)
        
        # Create instance using Pydantic model_validate
        # This will trigger model_post_init which creates the grid
        instance = cls.model_validate(state_dict)
        
        # Restore blueprint using model_construct to handle None values
        if blueprint_dict is not None:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
                warnings.filterwarnings('ignore', message='.*Pydantic.*', category=UserWarning)
                warnings.filterwarnings('ignore', message='.*serialization.*', category=UserWarning)
                instance.blueprint = cstar_models.RomsMarblBlueprint.model_construct(**blueprint_dict)
        
        # Restore PrivateAttr fields after instance creation
        if model_spec_dict is not None:
            # Use model_construct to handle None values and missing required fields
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
                warnings.filterwarnings('ignore', message='.*Pydantic.*', category=UserWarning)
                warnings.filterwarnings('ignore', message='.*serialization.*', category=UserWarning)
                instance._model_spec = forge_models.ModelSpec.model_construct(**model_spec_dict)
        
        # Restore _stage
        if "_stage" in private_attrs:
            instance._stage = private_attrs["_stage"]
        
        # Restore settings dictionaries
        if "_settings_compile_time" in private_attrs:
            instance._settings_compile_time = private_attrs["_settings_compile_time"]
        if "_settings_run_time" in private_attrs:
            instance._settings_run_time = private_attrs["_settings_run_time"]
        
        # Restore src_data if present
        if "src_data" in private_attrs:
            src_data_dict = private_attrs["src_data"]
            # Convert string paths back to Path objects where appropriate
            # Note: grid object cannot be restored from src_data_dict
            # as it was excluded during serialization
            instance.src_data = source_data.SourceData(**src_data_dict)
        
        # Note: _datasets and _cstar_simulation are not restored here.
        # They will be initialized when accessed or when build() is called.
        
        return instance


class CstarSpecEngine:
    """
    Engine for executing CstarSpecBuilder workflows from domain configurations.
    
    This class provides a convenient interface for loading domain configurations
    from a YAML file and executing the complete workflow:
    1. ensure_source_data()
    2. generate_inputs()
    3. configure_build()
    4. build()
    5. pre_run()
    
    **Usage:**
    
    ```python
    from cstar_forge import CstarSpecEngine
    
    # Load and execute workflow for a domain
    engine = CstarSpecEngine(domains_file="domains.yml")
    builder = engine.generate_domain("test-tiny")
    ```
    
    **Domain Configuration:**
    
    Domain configurations are stored in a YAML file with the following structure:
    
    ```yaml
    grid_name:
      description: str
      model_name: str
      grid_name: str
      start_time: str  # ISO format: "YYYY-MM-DD"
      end_time: str    # ISO format: "YYYY-MM-DD"
      grid_kwargs: dict
      open_boundaries: dict
      partitioning: dict
    ```
    """
    
    def __init__(
        self,
        domains_file: Union[str, Path],
        *,
        catalog_root: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize CstarSpecEngine.
        
        Parameters
        ----------
        domains_file : Union[str, Path]
            Path to domains YAML file.
        catalog_root : str or Path, optional
            Default *outer* anchor passed to every ``CstarSpecBuilder`` (inner paths are
            ``<catalog_root>/catalog/blueprints`` and ``<catalog_root>/catalog/builds``, except
            ``catalog_root="local"`` which uses the in-repo ``cstar_forge/catalog`` directory).
        """
        domains_file = Path(domains_file)
        
        self.domains_file = domains_file
        self._domains: Optional[Dict[str, Any]] = None
        self.builder: Optional[Dict[str, CstarSpecBuilder]] = None
        self._engine_catalog_root: Optional[Union[str, Path]] = catalog_root
        # Load domains on initialization
        self._load_domains()
    
    def _load_domains(self) -> Dict[str, Any]:
        """
        Load domain configurations from YAML file.
        
        Returns
        -------
        Dict[str, Any]
            Dictionary mapping grid_name to domain configuration.
        """
        if self._domains is None:
            if not self.domains_file.exists():
                raise FileNotFoundError(
                    f"Domains file not found: {self.domains_file}"
                )
            with self.domains_file.open("r") as f:
                self._domains = yaml.safe_load(f) or {}
        return self._domains
    
    @property
    def domains(self) -> List[str]:
        """
        Return a list of available domain names (grid names).
        
        Returns
        -------
        List[str]
            Sorted list of domain names available in the domains file.
        """
        return sorted(self._load_domains().keys())
    
    def _get_domain_config(self, domain_name: str) -> Dict[str, Any]:
        """
        Get domain configuration for a specific grid name.
        
        Parameters
        ----------
        domain_name : str
            Name of the grid/domain to load.
        
        Returns
        -------
        Dict[str, Any]
            Domain configuration dictionary.
        
        Raises
        ------
        KeyError
            If domain_name is not found in domains file.
        """
        domains = self._load_domains()
        if domain_name not in domains:
            raise KeyError(
                f"Domain '{domain_name}' not found in {self.domains_file}. "
                f"Available domains: {sorted(domains.keys())}"
            )
        return domains[domain_name]
    
    def _create_builder(
        self,
        domain_name: str,
        overrides: Optional[Dict[str, Any]] = None,
        ensemble_id: Optional[int] = None,
        catalog_root: Optional[Union[str, Path]] = None,
    ) -> CstarSpecBuilder:
        """
        Create a CstarSpecBuilder instance from domain configuration.
        
        Parameters
        ----------
        domain_name : str
            Name of the grid/domain to load.
        overrides : Optional[Dict[str, Any]], optional
            Dictionary of configuration overrides to apply. These will
            override values from the domain configuration. Default is None.
        ensemble_id : Optional[int], optional
            Ensemble ID to use for this builder. If None, uses default (1).
            Default is None.
        catalog_root : str or Path, optional
            If set, written into the builder config (overrides domain YAML unless
            already set there). Outer anchor for ``CstarSpecBuilder.catalog_root``.
            ``None`` leaves domain/engine defaults only.
        
        Returns
        -------
        CstarSpecBuilder
            Configured CstarSpecBuilder instance.
        """
        config_dict = self._get_domain_config(domain_name).copy()
        
        # Apply ensemble_id if provided
        if ensemble_id is not None:
            config_dict["ensemble_id"] = ensemble_id
        

        if "_parent_grid_name" in config_dict:
            parent_grid_config = self._get_domain_config(config_dict["_parent_grid_name"]).copy()
            config_dict["grid_kwargs_parent"] = parent_grid_config["grid_kwargs"]
            config_dict.pop("_parent_grid_name", None)

        if "_child_grid_name" in config_dict:
            child_grid_config = self._get_domain_config(config_dict["_child_grid_name"]).copy()
            config_dict["grid_kwargs_child"] = child_grid_config["grid_kwargs"]
            config_dict.pop("_child_grid_name", None)

        # Apply overrides if provided
        if overrides:
            config_dict.update(overrides)

        # catalog_root: explicit call wins, else engine default, else domain YAML only
        if catalog_root is not None:
            config_dict["catalog_root"] = catalog_root
        elif getattr(self, "_engine_catalog_root", None) is not None:
            config_dict.setdefault("catalog_root", self._engine_catalog_root)
        
        # Convert date strings to datetime objects
        if "start_time" in config_dict:
            if isinstance(config_dict["start_time"], str):
                config_dict["start_time"] = datetime.fromisoformat(config_dict["start_time"])
        if "end_time" in config_dict:
            if isinstance(config_dict["end_time"], str):
                config_dict["end_time"] = datetime.fromisoformat(config_dict["end_time"])
        
        # Convert open_boundaries dict to OpenBoundaries model
        if "open_boundaries" in config_dict:
            config_dict["open_boundaries"] = forge_models.OpenBoundaries(**config_dict["open_boundaries"])
        
        # Convert partitioning dict to PartitioningParameterSet
        if "partitioning" in config_dict:
            config_dict["partitioning"] = cstar_models.PartitioningParameterSet(**config_dict["partitioning"])


        # Create and return CstarSpecBuilder
        return CstarSpecBuilder(**config_dict)
    
    def generate_domain(
        self,
        domain_name: str,
        overrides: Optional[Dict[str, Any]] = None,
        clobber_inputs: bool = True,
        partition_files: bool = False,
        test: bool = False,
        compile_time_settings: Optional[Dict[str, Any]] = None,
        run_time_settings: Optional[Dict[str, Any]] = None,
        ensemble_id: Optional[int] = None,
        catalog_root: Optional[Union[str, Path]] = None,
    ) -> CstarSpecBuilder:
        """
        Execute the complete workflow for a domain.
        
        This method executes the full workflow:
        1. ensure_source_data()
        2. generate_inputs()
        3. configure_build()
        4. build()
        5. pre_run()
        
        Parameters
        ----------
        domain_name : str
            Name of the grid/domain to process.
        overrides : Optional[Dict[str, Any]], optional
            Dictionary of configuration overrides to apply. Default is None.
        clobber_inputs : bool, optional
            If True, overwrite existing input files. Default is True.
        partition_files : bool, optional
            If True, partition input files across tiles. Default is False.
        test : bool, optional
            If True, truncate generation loop after 2 iterations. Default is False.
        compile_time_settings : Optional[Dict[str, Any]], optional
            Compile-time settings overrides. Default is None.
        run_time_settings : Optional[Dict[str, Any]], optional
            Run-time settings overrides. Default is None.
        ensemble_id : Optional[int], optional
            Ensemble ID to use for this domain. If None, uses default (1).
            Default is None.
        catalog_root : str or Path, optional
            Overrides ``CstarSpecEngine`` default and domain YAML for this call only.
            Outer anchor: blueprints and builds resolve under ``<catalog_root>/catalog/``.
            Same rules as ``CstarSpecBuilder.catalog_root`` (including ``"local"``).
        
        Returns
        -------
        CstarSpecBuilder
            The CstarSpecBuilder instance after completing the workflow.
        """
        # Create builder from domain configuration
        builder = self._create_builder(
            domain_name,
            overrides=overrides,
            ensemble_id=ensemble_id,
            catalog_root=catalog_root,
        )
        
        # Execute workflow
        builder.ensure_source_data()
        builder.generate_inputs(
            clobber=clobber_inputs,
            partition_files=partition_files,
            test=test
        )
        builder.configure_build(
            compile_time_settings=compile_time_settings or {},
            run_time_settings=run_time_settings or {}
        )
        builder.build()
        builder.pre_run()
        
        return builder
    
    def generate_all(
        self,
        overrides: Optional[Dict[str, Any]] = None,
        clobber_inputs: bool = True,
        partition_files: bool = False,
        test: bool = False,
        compile_time_settings: Optional[Dict[str, Any]] = None,
        run_time_settings: Optional[Dict[str, Any]] = None,
        ensemble_id: Optional[int] = None,
        stop_on_failure: bool = False,
        catalog_root: Optional[Union[str, Path]] = None,
    ) -> Dict[str, CstarSpecBuilder]:
        """
        Execute the complete workflow for all domains (generation only).
        
        This method calls `generate_domain()` for each domain in the domains file.
        The generated builders are stored in `self.builder` attribute.
        To run the simulations, call `run_all()` after this method.
        
        Parameters
        ----------
        overrides : Optional[Dict[str, Any]], optional
            Dictionary of configuration overrides to apply to all domains. Default is None.
        clobber_inputs : bool, optional
            If True, overwrite existing input files. Default is True.
        partition_files : bool, optional
            If True, partition input files across tiles. Default is False.
        test : bool, optional
            If True, truncate generation loop after 2 iterations. Default is False.
        compile_time_settings : Optional[Dict[str, Any]], optional
            Compile-time settings overrides. Default is None.
        run_time_settings : Optional[Dict[str, Any]], optional
            Run-time settings overrides. Default is None.
        ensemble_id : Optional[int], optional
            Ensemble ID to use for all domains. If None, uses default (1).
            Default is None.
        catalog_root : str or Path, optional
            Passed through to each ``generate_domain`` call (overrides engine default for
            this batch only). Outer anchor: ``<catalog_root>/catalog/{blueprints,builds}``.
        
        Returns
        -------
        Dict[str, CstarSpecBuilder]
            Dictionary mapping grid_name to CstarSpecBuilder instance for each domain.
            Also stored in `self.builder` attribute.
        """
        builders = {}
        domain_list = self.domains
        total_domains = len(domain_list)
        
        print(f"\n{'='*80}")
        print(f"Starting generation for {total_domains} domain(s)")
        print(f"{'='*80}\n")
        
        
        failed_domains = []
        
        for idx, grid_name in enumerate(domain_list, start=1):
            print(f"\n{'-'*80}")
            print(f"[{idx}/{total_domains}] Processing domain: {grid_name}")
            print(f"{'-'*80}")
            
            # TEMPORARY: Remove stale cache between runs
            # TODO: Remove this once C-Star has a proper cache management system.
            # https://cworthy.atlassian.net/browse/CSD-538
            cache_dir = Path.home() / ".cache" / "cstar"
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
                print("⚠ Removed ~/.cache/cstar to avoid stale C-Star cache between runs")
                warnings.warn(
                    "~/.cache/cstar was removed to avoid stale cache state",
                    UserWarning,
                    stacklevel=2,
                )
                        
            
            try:
                builders[grid_name] = self.generate_domain(
                    domain_name=grid_name,
                    overrides=overrides,
                    clobber_inputs=clobber_inputs,
                    partition_files=partition_files,
                    test=test,
                    compile_time_settings=compile_time_settings,
                    run_time_settings=run_time_settings,
                    ensemble_id=ensemble_id,
                    catalog_root=catalog_root,
                )
                print(f"\n✓ Successfully completed domain: {grid_name}")
            except Exception as e:
                print(f"\n✗ Error processing domain {grid_name}: {e}")
                if stop_on_failure:
                    raise e
                warnings.warn(
                    f"Error processing domain {grid_name}: {e}",
                    UserWarning,
                    stacklevel=2
                )
                failed_domains.append((grid_name, str(e)))
        
        print(f"\n{'='*80}")
        print(f"Completed generation for all {total_domains} domain(s)")
        if failed_domains:
            print(f"⚠ {len(failed_domains)} domain(s) failed:")
            for grid_name, error in failed_domains:
                print(f"  - {grid_name}: {error}")
        print(f"{'='*80}\n")
        
        # Store builders as builder attribute (only successful ones)
        self.builder = builders
        
        # Warn if any domains failed
        if failed_domains:
            failed_names = [name for name, _ in failed_domains]
            warnings.warn(
                f"{len(failed_domains)} domain(s) failed during generation: {', '.join(failed_names)}",
                UserWarning,
                stacklevel=2
            )
        
        return builders
    
    def run_all(
        self,
        poll_interval: int = 30,
    ) -> Dict[str, Any]:
        """
        Run all simulations and wait for completion.
        
        This method runs each simulation from the `builder` attribute (set by `generate_all()`)
        and polls execution status until each simulation reaches a terminal state before
        moving to the next one.
        
        Parameters
        ----------
        poll_interval : int, optional
            Number of seconds between status checks. Default is 30.
        
        Returns
        -------
        Dict[str, Any]
            Dictionary containing:
            - "builders": Dict[str, CstarSpecBuilder] mapping grid_name to CstarSpecBuilder instances
            - "execution_handlers": Dict[str, ExecutionHandler] mapping grid_name to execution handler instances
        
        Raises
        ------
        RuntimeError
            If `builder` attribute is None (must call `generate_all()` first).
            If any simulation fails or is cancelled.
        """
        if self.builder is None:
            raise RuntimeError(
                "No builders available. Call generate_all() first to create builders."
            )
        
        builders = self.builder
        total_domains = len(builders)
        
        print(f"\n{'='*80}")
        print(f"Starting execution for {total_domains} domain(s)")
        print(f"{'='*80}\n")
        
        execution_results = {}
        failed_simulations = []
        
        for idx, (grid_name, builder) in enumerate(builders.items(), start=1):
            print(f"\n{'-'*80}")
            print(f"[{idx}/{total_domains}] Running simulation: {grid_name}")
            print(f"{'-'*80}")
            
            try:
                # Start the simulation
                builder.prep_cstar_environment(
                    account_key = None,  # None gets from machine config or override here
                    queue_name = None,  # None gets from machine config or override here
                    walltime = "12:00:00",  # don't know how long this will take, so give it a long time
                    clobber = True,  # recommend True, but it will clear previous results from this run 
                    n_procs_available = 0,  # 0 is auto-detect, change if on a login or shared node to not overuse resources
                )
                builder.run()

                # deprecated run handling - leaving here for now
                # TODO: remove this once we run_all() behvior is refactored
                # ------------------------------------------------------------------------------
                # execution_handler = builder.run()
                
                # # Poll execution status until terminal
                # print(f"Monitoring execution status for {grid_name}...")
                # while True:
                #     status = execution_handler.status
                #     print(f"  Status: {status}")
                    
                #     if ExecutionStatus.is_terminal(status):
                #         if status == ExecutionStatus.COMPLETED:
                #             print(f"\n✓ Simulation completed successfully: {grid_name}")
                #         elif status == ExecutionStatus.FAILED:
                #             print(f"\n✗ Simulation failed: {grid_name}")
                #             warnings.warn(
                #                 f"Simulation {grid_name} failed with status {status}",
                #                 UserWarning,
                #                 stacklevel=2
                #             )
                #             failed_simulations.append((grid_name, status, "failed"))
                #         elif status == ExecutionStatus.CANCELLED:
                #             print(f"\n⚠ Simulation was cancelled: {grid_name}")
                #             warnings.warn(
                #                 f"Simulation {grid_name} was cancelled",
                #                 UserWarning,
                #                 stacklevel=2
                #             )
                #             failed_simulations.append((grid_name, status, "cancelled"))
                #         break
                    
                #     # Wait before next status check
                #     time.sleep(poll_interval)
                
                # execution_results[grid_name] = execution_handler
                # ------------------------------------------------------------------------------


            except Exception as e:
                print(f"\n✗ Error running simulation {grid_name}: {e}")
                warnings.warn(
                    f"Error running simulation {grid_name}: {e}",
                    UserWarning,
                    stacklevel=2
                )
                failed_simulations.append((grid_name, None, f"error: {e}"))
        
        print(f"\n{'='*80}")
        print(f"Completed execution for all {total_domains} domain(s)")
        if failed_simulations:
            print(f"⚠ {len(failed_simulations)} simulation(s) failed or were cancelled:")
            for grid_name, status, reason in failed_simulations:
                print(f"  - {grid_name}: {reason}")
        print(f"{'='*80}\n")
        
        # Warn if any simulations failed
        if failed_simulations:
            failed_names = [name for name, _, _ in failed_simulations]
            warnings.warn(
                f"{len(failed_simulations)} simulation(s) failed or were cancelled: {', '.join(failed_names)}",
                UserWarning,
                stacklevel=2
            )
        
        return execution_results
