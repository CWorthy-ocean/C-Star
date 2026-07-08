"""
ForgeExecutor - Pydantic-based builder for C-Star blueprints.

This class provides a Pydantic-based interface for building RomsMarblBlueprint objects.
"""

from __future__ import annotations

import asyncio
import copy
import os
import shutil
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import cstar.applications.roms_marbl.models as cstar_models
import roms_tools as rt
import xarray as xr
import yaml
from cstar.applications.core import RunnerRequest
from cstar.applications.roms_marbl.app import RomsMarblRunner
from cstar.applications.roms_marbl.models import RomsMarblBlueprint
from cstar.base.additional_code import AdditionalCode
from cstar.base.env import (
    ENV_CSTAR_CLOBBER_WORKING_DIR,
    ENV_CSTAR_IN_ACTIVE_ALLOCATION,
    ENV_CSTAR_NPROCS_POST,
)
from cstar.entrypoint.config import get_job_config, get_service_config
from cstar.orchestration.models import Resource
from cstar.orchestration.serialization import deserialize
from cstar.orchestration.utils import (
    ENV_CSTAR_SLURM_ACCOUNT,
    ENV_CSTAR_SLURM_MAX_WALLTIME,
    ENV_CSTAR_SLURM_QUEUE,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from cstar_forge import models as forge_models
from cstar_forge.forge import input_data, source_data
from cstar_forge.forge.host import HostPaths
from cstar_forge.forge.settings import render_roms_settings, write_roms_namelist


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

    def __call__(self, key: str | None = None):
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


def _deep_merge_settings_dict(target: dict[str, Any], update: dict[str, Any]) -> None:
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


class ForgeExecutor(BaseModel):
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

    **Settings:**

    The compile-time (``cppdefs``) and run-time (namelist) settings are seeded from the
    resolved ``resolved_settings`` (the SpecConfig ``model_settings``); ``configure_build``
    can overlay further overrides on top.

    **Key Concepts:**

    - Settings are stored in sidecar YAML files (not in blueprint itself)
    - Blueprint state is persisted to disk at each stage transition
    - Grid object is created during initialization and reused throughout
    - Source data can be prepared independently via `ensure_source_data()`
    - All produced artifacts (inputs, blueprints, builds, run output) live under the
      injected ``host.working_dir``.

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
    grid_kwargs: dict[str, Any]
    grid_kwargs_parent: dict[str, Any] | None = Field(
        default=None, validate_default=False
    )
    grid_kwargs_child: dict[str, Any] | None = Field(
        default=None, validate_default=False
    )
    open_boundaries: forge_models.OpenBoundaries
    partitioning: cstar_models.PartitioningParameterSet
    start_date: datetime = Field(alias="start_time")
    end_date: datetime = Field(alias="end_time")
    cdr_forcing: dict | None = Field(
        default=None,
        alias="CDR_forcing",
        validate_default=False,
    )
    forcing_override: dict[str, Any] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "When provided, overrides model_spec.inputs for initial_conditions and forcing "
            "categories. Set by process_spec_config when cfg.sources contains an authored "
            "ForcingSpec selection; ignored when None (model defaults are used)."
        ),
    )
    model_reference_date: datetime | None = Field(
        default=None,
        validate_default=False,
        description=(
            "ROMS model reference date (t=0). Forwarded to every rt object that accepts it. "
            "None uses the roms-tools default (2000-01-01)."
        ),
    )
    host: HostPaths | None = Field(
        default=None,
        validate_default=False,
        exclude=True,
        description=(
            "Injected runtime location (working_dir + source_data_cache + machine "
            "identity). Required for all produced-artifact paths and source-data caching; "
            "the executor reads no host paths from cstar_forge.config."
        ),
    )
    source_dataset_keys: list[str] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Resolved source-dataset keys to prepare (from the SpecConfig ``datasets``). "
            "Distinct from the ``datasets`` property, which returns the *loaded* datasets."
        ),
    )
    resolved_settings: dict[str, Any] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Fully-resolved, host-independent settings (the SpecConfig ``model_settings``): "
            "a flat mapping with ``cppdefs`` (compile-time) alongside every namelist "
            "(run-time) section. When set, it is the authoritative base for the compile-time "
            "and run-time settings dictionaries (the executor no longer re-derives them from "
            "the catalog ModelSpec). None keeps the legacy ModelSpec-derived base "
            "(transitional; being phased out)."
        ),
    )
    code_spec: Any | None = Field(
        default=None,
        validate_default=False,
        description=(
            "The SpecConfig ``code`` (roms + marbl repos and compile-/run-time template "
            "refs). The blueprint code repository and the template render directories are "
            "built from it. Required (fed by from_spec_config)."
        ),
    )
    ensemble_id: int | None = Field(default=None, validate_default=False)
    # Internal attributes (computed/loaded)
    blueprint: cstar_models.RomsMarblBlueprint | None = Field(
        default=None, init=False, validate_default=False, validate_assignment=False
    )
    src_data: source_data.SourceData | None = Field(
        default=None, init=False, validate_default=False
    )
    grid: rt.Grid | None = Field(
        default=None, init=False, validate_default=False, exclude=True
    )
    grid_parent: rt.Grid | None = Field(
        default=None, init=False, validate_default=False, exclude=True
    )
    grid_child: rt.Grid | None = Field(
        default=None, init=False, validate_default=False, exclude=True
    )
    metadata_child: dict[str, Any] | None = Field(
        default=None, init=False, validate_default=False, exclude=True
    )
    _datasets: dict[str, xr.Dataset | list[xr.Dataset]] | None = PrivateAttr(
        default=None
    )
    _stage: str | None = PrivateAttr(default=None)
    _cstar_simulation: Any | None = PrivateAttr(default=None)
    _settings_compile_time: dict[str, Any] = PrivateAttr(default_factory=dict)
    _settings_run_time: dict[str, Any] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def _validate_dates(self) -> ForgeExecutor:
        """Validate that start_date precedes end_date."""
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self

    def _require_host(self) -> HostPaths:
        """Return the injected host, raising a clear error if it was not provided.

        Every produced-artifact path routes under ``host.working_dir``; the executor
        reads no host paths from ``cstar_forge.config``, so a host is mandatory.
        """
        if self.host is None:
            raise ValueError(
                "ForgeExecutor requires an injected host (HostPaths). Construct it via "
                "ForgeExecutor.from_spec_config(cfg, host=...) or pass host=... directly."
            )
        return self.host

    @property
    def input_data_dir(self) -> Path:
        """Directory for generated input NetCDF files (grid, forcing, etc.)."""
        return self._require_host().working_dir / "input_data"

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
        # Fail fast if no host was injected — every artifact path needs it.
        self._require_host()

        # Create grids, 4 cases:
        # has child and no parent, has child and parent, has parent and no child, no parent no child

        # I am a parent but not a child
        if self.grid_kwargs_child is not None and self.grid_kwargs_parent is None:
            # Make both parent and child, to make the nesting data.
            grid_kwargs_child = {
                k: v for k, v in self.grid_kwargs_child.items() if k != "metadata"
            }

            self.grid_child = rt.Grid(**grid_kwargs_child)
            self.grid = rt.Grid(**self.grid_kwargs)
            self.grid_child = rt.align_grids(self.grid, self.grid_child)

            if "metadata" in self.grid_kwargs_child:
                self.metadata_child = self.grid_kwargs_child["metadata"]

        # I am a parent and a child
        elif self.grid_kwargs_child is not None and self.grid_kwargs_parent is not None:
            grid_kwargs_child = {
                k: v for k, v in self.grid_kwargs_child.items() if k != "metadata"
            }
            grid_kwargs_parent = {
                k: v for k, v in self.grid_kwargs_parent.items() if k != "metadata"
            }
            grid_kwargs = {k: v for k, v in self.grid_kwargs.items() if k != "metadata"}

            # Adapt this grid to its parent, but create nesting data for its child
            self.grid_parent = rt.Grid(**grid_kwargs_parent)
            self.grid_child = rt.Grid(**grid_kwargs_child)
            self.grid = rt.Grid(**grid_kwargs)

            self.grid = rt.align_grids(self.grid_parent, self.grid)
            self.grid_child = rt.align_grids(self.grid, self.grid_child)

            if "metadata" in self.grid_kwargs_child:
                self.metadata_child = self.grid_kwargs_child["metadata"]

        # I am a child but not a parent
        elif self.grid_kwargs_child is None and self.grid_kwargs_parent is not None:
            grid_kwargs_parent = {
                k: v for k, v in self.grid_kwargs_parent.items() if k != "metadata"
            }
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

    def _planned_netcdf_outputs(self) -> list[Path]:
        """Return the list of NetCDF files expected from input generation.

        Derived from the resolved ``forcing_override`` (the executor always generates
        the grid from the injected grid object) — never from the catalog ModelSpec.
        """
        input_data_dir = self.input_data_dir
        planned_paths: list[Path] = []

        def _add_nc(stem: str) -> None:
            base = input_data.netcdf_filename_component(self.name)
            part = input_data.netcdf_filename_component(stem)
            path = (input_data_dir / f"{base}_{part}.nc").resolve()
            if path not in planned_paths:
                planned_paths.append(path)

        inputs_cfg: dict[str, Any] = dict(self.forcing_override or {})

        # The grid is always generated from the injected grid object.
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

                        source_name = entry.get("source").get("name")
                        if entry.get("type") == "bgc" and source_name == "MBL_co2":
                            stem = (
                                f"{category}-{forcing_type}-co2"
                                if forcing_type
                                else category
                            )
                        else:
                            stem = (
                                f"{category}-{forcing_type}"
                                if forcing_type
                                else category
                            )
                        _add_nc(stem)
                    continue

                _add_nc(category)

        if self.cdr_forcing:
            _add_nc(input_data.CDR_FORCING_NETCDF_STEM)

        return planned_paths

    def _print_planned_netcdf_outputs(self) -> None:
        """Print the list of expected NetCDF files to stdout."""
        planned_paths = self._planned_netcdf_outputs()
        print("ForgeExecutor: planned NetCDF outputs")
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
            "ForgeExecutor: output locations",
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
        Return the name of this blueprint as '{model_name}_{grid_name}_{n_procs}procs'.

        This property sets blueprint.name when the blueprint is created.
        """
        ensemble_str = (
            f"_{self.ensemble_id:03d}" if self.ensemble_id is not None else ""
        )
        return f"{self.model_name}_{self.grid_name}_{self.n_procs}procs{ensemble_str}"

    @property
    def n_procs(self) -> int:
        """Return the number of processors."""
        return self.partitioning.n_procs_x * self.partitioning.n_procs_y

    @property
    def datestr(self) -> str:
        """Return the date string."""
        return (
            f"{self.start_date.strftime('%Y%m%d')}-{self.end_date.strftime('%Y%m%d')}"
        )

    @property
    def casename(self) -> str:
        """Return the case name."""
        return f"{self.name}_{self.datestr}"

    @property
    def run_output_dir(self) -> Path:
        """Per-run output root — the injected ``host.working_dir`` (all produced
        artifacts live under it).
        """
        return self._require_host().working_dir

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
    def blueprint_dir(self) -> Path:
        """Return the blueprint directory path (under host.working_dir)."""
        return self._require_host().working_dir / "blueprints"

    @property
    def compile_time_code_dir(self) -> Path:
        """Compile-time rendered templates under host.working_dir/builds."""
        return self._require_host().working_dir / "builds" / "compile-time"

    @property
    def run_time_code_dir(self) -> Path:
        """Run-time rendered templates under host.working_dir/builds."""
        return self._require_host().working_dir / "builds" / "run-time"

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
                raise ValueError(
                    "Cannot persist run blueprint: runtime_params is not set"
                )
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
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serialization.*", category=UserWarning
            )
            blueprint_dict = self.blueprint.model_dump(mode="json", exclude_none=True)

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
            "run_time": self._settings_run_time,
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
                stacklevel=2,
            )

    def path_blueprint(
        self,
        stage: str | None = None,
        run_params: cstar_models.RuntimeParameterSet | None = None,
    ) -> Path:
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
                raise ValueError(
                    "stage must be provided if blueprint is not initialized"
                )
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
                stacklevel=2,
            )
            return DatasetsDict()

        # Populate all datasets from blueprint if not already done
        if self._datasets is None:
            self._datasets = {}

        # Dynamically generate list of fields that contain data entries
        # Start with grid and initial_conditions
        data_fields = ["grid", "initial_conditions"]

        # Add forcing categories present in the resolved forcing_override.
        forcing_cfg = (self.forcing_override or {}).get("forcing") or {}
        for field_name in forcing_cfg:
            data_fields.append(f"forcing.{field_name}")

        # Add cdr_forcing (not part of inputs, but a separate blueprint field)
        data_fields.append("cdr_forcing")

        # Loop over all data fields and call get_ds for each
        # Suppress Pydantic warnings when accessing datasets
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            warnings.filterwarnings(
                "ignore", category=UserWarning, module="pydantic.main"
            )
            warnings.filterwarnings(
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serializer.*", category=UserWarning
            )
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

    def _get_machine_config(self):
        """Return the injected host's machine config (account / queues / pes_per_node)."""
        return self._require_host().machine_config

    def _cstar_code_repository(self) -> cstar_models.ROMSCompositeCodeRepository:
        """Build the blueprint's cstar ``ROMSCompositeCodeRepository`` from the resolved
        ``code_spec`` (SpecConfig ``code``): roms + optional marbl repos, with placeholder
        run_time/compile_time entries that ``configure_build`` overwrites with the rendered
        code directories.
        """
        if self.code_spec is None:
            raise ValueError(
                "ForgeExecutor requires code_spec (the SpecConfig ``code``) to build the "
                "blueprint code repository; construct via ForgeExecutor.from_spec_config."
            )

        def _repo(spec: Any) -> cstar_models.CodeRepository:
            kwargs: dict[str, Any] = {"location": spec.location}
            if spec.commit:
                kwargs["commit"] = spec.commit
            elif spec.branch:
                kwargs["branch"] = spec.branch
            else:
                kwargs["branch"] = "main"
            return cstar_models.CodeRepository(**kwargs)

        return cstar_models.ROMSCompositeCodeRepository(
            roms=_repo(self.code_spec.roms),
            marbl=_repo(self.code_spec.marbl) if self.code_spec.marbl else None,
            run_time=cstar_models.CodeRepository(
                location="placeholder://run_time", branch="main"
            ),
            compile_time=cstar_models.CodeRepository(
                location="placeholder://compile_time", branch="main"
            ),
        )

    def _template_repo_args(self, stage: str) -> dict[str, Any]:
        """C-Star :class:`AdditionalCode` constructor args for the ``stage``
        (``compile_time`` / ``run_time``) templates, read purely from ``code_spec``.

        The SpecConfig carries the template repo (git ``location`` + ``commit``/``branch``,
        an in-repo ``directory``, and the ``files`` filter). This is the single seam the
        offline test fixture overrides to point ``location`` at a local template directory
        (see ``tests/conftest.py``); production leaves the git ref intact.
        """
        repo = getattr(self.code_spec, f"templates_{stage}")
        return {
            "location": str(repo.location),
            "subdir": repo.directory or "",
            "checkout_target": repo.commit or repo.branch or "",
            "files": list(repo.files),
        }

    def _stage_templates(self, stage: str) -> Path:
        """Stage the ``stage`` template files locally and return their directory.

        Reuses C-Star's :class:`AdditionalCode` to materialize the templates from the
        ``code_spec`` git ref: a remote repo (``https://…`` + commit/branch) fetches the
        filtered files; a local directory copies them. Nothing is read from the bundled
        catalog, so the executor stays relocatable (decision #4 in the portability plan).

        **Cross-repo contract (unguarded in CI):** we rely on C-Star staging the filtered
        files *flat* into ``local_dir`` (``dest/cppdefs.opt.j2``, NOT ``dest/<subdir>/…``),
        because ``render_roms_settings`` reads ``template_dir/<file>`` directly. Verified
        against the real ``REMOTE_REPOSITORY`` path once; the offline test seam forces
        ``subdir=""`` so CI never exercises the subdir-preserving case. If C-Star changes
        its stager layout this breaks silently — see docs/executor-portability-plan.md.

        Reproducibility caveat (deferred follow-up): the resolver currently pins the
        template repo by ``branch`` (``main``), not a commit, and ``code.templates_*.location``
        participates in ``content_hash`` — so a template edit changes build output without a
        hash bump until a commit is pinned. Tracked in docs/executor-portability-plan.md.
        """
        dest = self._require_host().working_dir / "templates" / stage
        if dest.exists():
            shutil.rmtree(dest)
        AdditionalCode(**self._template_repo_args(stage)).get(local_dir=dest)
        return dest

    def _template_files(self, stage: str) -> list[str]:
        """File list for the ``stage`` templates (from ``code_spec``)."""
        return list(getattr(self.code_spec, f"templates_{stage}").files)

    def _initialize_blueprint(self) -> None:
        """
        Initialize blueprint with basic structure and set stage to PRECONFIG.

        This method creates the initial blueprint structure with placeholder data.
        It is called automatically during initialization via `model_post_init()`.

        **Process:**

        1. Initializes compile-time and run-time settings from the resolved SpecConfig
        2. Creates blueprint with:
           - Basic metadata (name, description, dates, partitioning)
           - Code repository specifications from code_spec
           - Placeholder Resource objects for grid, initial_conditions, forcing
        3. Sets `_stage` to PRECONFIG
        4. Persists blueprint to disk

        The blueprint at this stage has the correct structure but contains
        placeholder data (None locations). Actual data files are added during
        the POSTCONFIG stage via `generate_inputs()`.
        """
        # Initialize settings from the resolved SpecConfig
        self._init_settings_compile_time()
        self._init_settings_run_time()

        # Create placeholder Resource objects to satisfy validation requirements
        placeholder_resource = Resource.model_construct(
            location=None, partitioned=False
        )
        forcing_config = cstar_models.ForcingConfiguration.model_construct(
            boundary=cstar_models.Dataset.model_construct(data=[placeholder_resource]),
            surface=cstar_models.Dataset.model_construct(data=[placeholder_resource]),
        )
        empty_dataset = cstar_models.Dataset.model_construct(
            data=[placeholder_resource]
        )

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
            code=self._cstar_code_repository(),
            grid=empty_dataset,
            initial_conditions=empty_dataset,
            forcing=forcing_config,
            cdr_forcing=None,
        )
        self._stage = BlueprintStage.PRECONFIG
        self.persist()

    def _load_blueprint_file(
        self, stage: str | None = None, load_settings: bool = True
    ) -> cstar_models.RomsMarblBlueprint | None:
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
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module="pydantic"
                )
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module="pydantic.main"
                )
                # Also filter warnings with "Pydantic" in the message
                warnings.filterwarnings(
                    "ignore", message=".*Pydantic.*", category=UserWarning
                )
                warnings.filterwarnings(
                    "ignore", message=".*serializer.*", category=UserWarning
                )
                blueprint = deserialize(bp_path, cstar_models.RomsMarblBlueprint)
        except Exception as e:
            # If validation fails (e.g., files don't exist), try lenient loading
            try:
                # Load YAML as dict and use model_construct to bypass validation
                with bp_path.open("r") as f:
                    blueprint_data = yaml.safe_load(f)
                # Use model_construct to bypass validation (files may not exist)
                blueprint = cstar_models.RomsMarblBlueprint.model_construct(
                    **blueprint_data
                )
            except Exception as e2:
                # If lenient loading also fails, issue a warning and return None
                warnings.warn(
                    f"Failed to load blueprint from {bp_path}: "
                    f"{type(e).__name__}: {e}. "
                    f"Lenient loading also failed: {type(e2).__name__}: {e2}",
                    UserWarning,
                    stacklevel=2,
                )
                return None

        # Load settings from sidecar file if blueprint was loaded and load_settings is True
        if blueprint is not None and load_settings:
            self._load_settings_from_file(bp_path)

        return blueprint

    @property
    def blueprint_from_file(self) -> cstar_models.RomsMarblBlueprint | None:
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
            warnings.filterwarnings(
                "ignore", category=UserWarning, module="pydantic.main"
            )
            warnings.filterwarnings(
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serializer.*", category=UserWarning
            )
            return self._load_blueprint_file()

    def get_ds(self, field: str, from_file: bool = True) -> list[xr.Dataset] | None:
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
            warnings.filterwarnings(
                "ignore", category=UserWarning, module="pydantic.main"
            )
            warnings.filterwarnings(
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serializer.*", category=UserWarning
            )
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
            warnings.filterwarnings(
                "ignore", category=UserWarning, module="pydantic.main"
            )
            warnings.filterwarnings(
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serializer.*", category=UserWarning
            )
            field_parts = field.split(".")
            data = blueprint
            for part in field_parts:
                # Convert model instances to dicts for easier navigation
                if hasattr(data, "model_dump"):
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
            elif hasattr(location, "__str__"):
                location_strs.append(str(location))
            else:
                location_strs.append(location)

        # Return a list of datasets (one per file) instead of combining them
        # This avoids alignment errors when datasets have incompatible dimensions
        # Suppress xarray FutureWarning about timedelta decoding
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning, module="xarray")
            return [
                xr.open_dataset(location, decode_timedelta=False)
                for location in location_strs
            ]

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

        if self.source_dataset_keys is None:
            raise ValueError(
                "source_dataset_keys is required (the resolved SpecConfig ``datasets``); "
                "construct via ForgeExecutor.from_spec_config."
            )

        self.src_data = source_data.SourceData(
            datasets=self.source_dataset_keys,
            clobber=False,
            grid=self.grid,
            grid_name=self.grid_name,
            start_time=self.start_date,
            end_time=self.end_date,
            source_data_dir=self._require_host().source_data_cache,
        ).prepare_all(include_streamable=include_streamable)

    def generate_inputs(
        self,
        clobber: bool = False,
        use_dask: bool = True,
        partition_files: bool = False,
        test: bool = False,
    ) -> cstar_models.RomsMarblBlueprint:
        """
        Generate ROMS input files and advance blueprint to POSTCONFIG stage.

        Always regenerates the blueprint (and settings sidecar). Existing NetCDF files
        are preserved and reused per-step when ``clobber=False``; pass ``clobber=True``
        to delete and re-create them.

        Parameters
        ----------
        clobber : bool, optional
            If True, delete and regenerate existing NetCDF input files. Default False.
        use_dask : bool, optional
            Use dask for parallel computations. Default True.
        partition_files : bool, optional
            Partition input files across tiles. Currently not implemented.
        test : bool, optional
            Truncate the generation loop after 2 iterations (for unit tests).

        Returns
        -------
        cstar_models.RomsMarblBlueprint
            The blueprint updated with all input file locations (POSTCONFIG stage).

        Raises
        ------
        RuntimeError
            If blueprint is not initialized, or if settings are not initialized.
        NotImplementedError
            If partition_files is True.
        """
        if partition_files:
            raise NotImplementedError(
                "File partitioning functionality is not yet fully implemented. "
                "Please set partition_files=False."
            )

        if self.blueprint is None:
            raise RuntimeError("Blueprint must be initialized before generating inputs")

        # Ensure settings are initialized before generating inputs.
        if (
            not hasattr(self, "_settings_compile_time")
            or not self._settings_compile_time
        ):
            raise RuntimeError("_settings_compile_time is not initialized or is empty.")
        if not hasattr(self, "_settings_run_time") or not self._settings_run_time:
            raise RuntimeError("_settings_run_time is not initialized or is empty.")

        # Prepare source data if not already done.
        if self.src_data is None:
            self.ensure_source_data(include_streamable=False)

        blueprint_elements, settings_compile_time, settings_run_time = (
            input_data.RomsMarblInputData(
                domain_name=self.name,
                start_date=self.start_date,
                end_date=self.end_date,
                input_data_dir=self.input_data_dir,
                grid=self.grid,
                grid_parent=self.grid_parent,
                grid_child=self.grid_child,
                metadata_child=self.metadata_child,
                boundaries=self.open_boundaries,
                source_data=self.src_data,
                forcing_override=self.forcing_override,
                model_reference_date=self.model_reference_date,
                blueprint_dir=self.blueprint_dir,
                partitioning=self.partitioning,
                cdr_forcing=self.cdr_forcing,
                use_dask=use_dask,
            ).generate_all(partition_files=partition_files, clobber=clobber, test=test)
        )

        if blueprint_elements is None:
            raise RuntimeError(
                "Blueprint mismatch detected, but input files exist. "
                "Set clobber=True to overwrite existing input files."
            )

        # Apply settings from input data generation (deep merge to preserve existing
        # settings). allow_new=True: the SpecConfig base omits the sections that input
        # generation fills (grid/initial/forcing/s_coord), so they arrive as new keys.
        self._update_settings_compile_time(settings_compile_time, allow_new=True)
        self._update_settings_run_time(settings_run_time, allow_new=True)

        if test:
            return

        # Update the blueprint with the generated input data.
        blueprint_dict = self.blueprint.model_dump()
        blueprint_dict["grid"] = (
            blueprint_elements.grid.model_dump() if blueprint_elements.grid else None
        )
        blueprint_dict["initial_conditions"] = (
            blueprint_elements.initial_conditions.model_dump()
            if blueprint_elements.initial_conditions
            else None
        )
        blueprint_dict["forcing"] = (
            blueprint_elements.forcing.model_dump()
            if blueprint_elements.forcing
            else None
        )
        blueprint_dict["cdr_forcing"] = (
            blueprint_elements.cdr_forcing.model_dump()
            if blueprint_elements.cdr_forcing
            else None
        )
        blueprint_dict["nesting_info"] = (
            blueprint_elements.nesting_info.model_dump()
            if blueprint_elements.nesting_info
            else None
        )

        # Settings are stored in a sidecar YAML, not in the blueprint itself.
        blueprint_dict["model_params"] = None
        blueprint_dict["runtime_params"] = None

        self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(
            **blueprint_dict
        )
        self._stage = BlueprintStage.POSTCONFIG
        self.persist()
        return self.blueprint

    def _init_settings_compile_time(self) -> None:
        """
        Initialize the compile-time settings dictionary from the resolved SpecConfig.

        ``cppdefs`` is the only compile-time section. It is used as the basis for
        template rendering during `configure_build()`; user overrides can still be
        applied via `_update_settings_compile_time()` or `configure_build()`.

        **Called by:** `_initialize_blueprint()` during initialization.
        """
        if self.resolved_settings is None:
            raise ValueError(
                "resolved_settings is required (the SpecConfig ``model_settings``); "
                "construct via ForgeExecutor.from_spec_config."
            )
        self._settings_compile_time = {
            "cppdefs": copy.deepcopy(self.resolved_settings.get("cppdefs", {}))
        }

    def _init_settings_run_time(self) -> None:
        """
        Initialize the run-time settings dictionary from the resolved SpecConfig.

        The authoritative, host-independent base is ``resolved_settings`` (the SpecConfig
        ``model_settings``): every non-``cppdefs`` section, deep-copied. The resolver
        already carries the genuinely-computed numerics (``time_stepping``, ``v_sponge``,
        ``extract_data``), so they are NOT re-derived here — that would clobber e.g. an
        explicitly-resolved ``dt``. Only the sections the config deliberately omits because
        they embed host/identity are ADDED: ``title`` and ``output_root_name``.

        **Called by:** `_initialize_blueprint()` during initialization.
        """
        if self.resolved_settings is None:
            raise ValueError(
                "resolved_settings is required (the SpecConfig ``model_settings``); "
                "construct via ForgeExecutor.from_spec_config."
            )
        self._settings_run_time = {
            k: copy.deepcopy(v)
            for k, v in self.resolved_settings.items()
            if k != "cppdefs"
        }
        self._settings_run_time["title"] = dict(
            casename=self.casename,
        )
        self._settings_run_time["output_root_name"] = dict(
            output_root_name=str(self.run_output_dir / "output" / self.casename),
        )

    def _update_settings_compile_time(
        self, settings_compile_time: dict[str, Any], allow_new: bool = False
    ) -> None:
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
                if isinstance(self._settings_compile_time[key], dict) and isinstance(
                    value, dict
                ):
                    value_copy = copy.deepcopy(value)
                    _deep_merge_settings_dict(
                        self._settings_compile_time[key], value_copy
                    )
                else:
                    self._settings_compile_time[key] = (
                        copy.deepcopy(value)
                        if not isinstance(value, (str, int, float, bool, type(None)))
                        else value
                    )
            elif allow_new:
                # Generation-overlay path: the SpecConfig base omits sections that are
                # filled at processing time, so accept new top-level keys.
                self._settings_compile_time[key] = copy.deepcopy(value)
            else:
                raise ValueError(
                    f"Unknown compile-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_compile_time.keys())}"
                )

    def _update_settings_run_time(
        self, settings_run_time: dict[str, Any], allow_new: bool = False
    ) -> None:
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
                if isinstance(self._settings_run_time[key], dict) and isinstance(
                    value, dict
                ):
                    value_copy = copy.deepcopy(value)
                    # TODO: Evaluate whether corrective logic for passed-in values should live here
                    # Do we need to correct anything else?
                    if key == "time_stepping" and "ntimes" in value_copy:
                        value_copy["ntimes"] = round(value_copy["ntimes"])
                    _deep_merge_settings_dict(self._settings_run_time[key], value_copy)
                else:
                    self._settings_run_time[key] = (
                        copy.deepcopy(value)
                        if not isinstance(value, (str, int, float, bool, type(None)))
                        else value
                    )
            elif allow_new:
                # Generation-overlay path: the SpecConfig base omits sections that are
                # filled at processing time (grid/initial/forcing/s_coord), so accept
                # new top-level keys instead of raising.
                self._settings_run_time[key] = copy.deepcopy(value)
            else:
                # Unknown key - raise error
                raise ValueError(
                    f"Unknown run-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_run_time.keys())}"
                )

    @classmethod
    def from_spec_config(cls, cfg: Any, host: HostPaths | None = None) -> ForgeExecutor:
        """Canonical constructor: build the executor directly from a resolved
        ``SpecConfig`` (the forge application's blueprint) + the injected ``host``.

        This is the single derivation path from a SpecConfig to a runnable builder. The
        domain-catalog path routes through the Phase-1 resolver
        (``spec_config_resolve.build_spec_config``) to produce the ``SpecConfig`` and then
        here — so there is one place that maps blueprint → builder inputs
        (``spec_config_engine.spec_config_to_builder_kwargs``).
        """
        from cstar_forge.forge.spec_config_engine import spec_config_to_builder_kwargs

        return cls(**spec_config_to_builder_kwargs(cfg), host=host)

    def configure_build(
        self,
        compile_time_settings: dict[str, Any] | None = None,
        run_time_settings: dict[str, Any] | None = None,
        **kwargs,
    ):
        """
        Configure blueprint by rendering templates and advance to BUILD stage.

        This method renders Jinja2 templates with current settings to produce
        configuration files needed for model compilation and execution.

        **Process:**

        1. Validates blueprint is initialized and template configuration exists
        2. Merges user-provided settings overrides with existing settings
        3. Clears compile-time and run-time code output directories
        4. Produces configuration files:
           - Compile-time: renders cppdefs.opt from its Jinja2 template
           - Run-time: writes namelist.nml (write_roms_namelist) and copies
             static run-time files (e.g., marbl_in)
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
            raise RuntimeError(
                "Blueprint must be initialized before configuration. Call generate_inputs() first."
            )

        # Initialize settings from defaults if not already initialized
        if (
            not hasattr(self, "_settings_compile_time")
            or self._settings_compile_time is None
        ):
            self._init_settings_compile_time()
        if not hasattr(self, "_settings_run_time") or self._settings_run_time is None:
            self._init_settings_run_time()

        # Update settings with user-provided overrides (deep merge to preserve existing settings)
        self._update_settings_compile_time(compile_time_settings)
        self._update_settings_run_time(run_time_settings)

        # Ensure ntimes is an integer (don't recalculate, just ensure type is correct)
        if "time_stepping" in self._settings_run_time:
            if "ntimes" in self._settings_run_time["time_stepping"]:
                ntimes = self._settings_run_time["time_stepping"]["ntimes"]
                # Convert to integer if it's a float
                if isinstance(ntimes, float):
                    self._settings_run_time["time_stepping"]["ntimes"] = round(ntimes)

        # Derive n_tracers: prefer the value passed by the Phase-2 engine; otherwise
        # derive it from the resolved settings (T + S + BGC ntrc_bio + passive).
        if "n_tracers" in kwargs:
            n_tracers = int(kwargs["n_tracers"])
        elif self.resolved_settings is not None:
            param = self.resolved_settings.get("param", {}) or {}
            n_tracers = (
                2 + int(param.get("ntrc_bio", 0)) + int(param.get("nt_passive", 0))
            )
        else:
            raise ValueError(
                "n_tracers could not be determined: neither passed as a kwarg nor "
                "derivable from resolved_settings."
            )

        # Ensure build output directories exist before writing files.
        self.compile_time_code_dir.mkdir(parents=True, exist_ok=True)
        self.run_time_code_dir.mkdir(parents=True, exist_ok=True)

        # ------------------------------------------------------------------
        # 1. Compile-time: render cppdefs.opt from its Jinja2 template.
        #    All other former *.opt outputs are now handled by the namelist.
        #    The template may reference run-time namelist sections (e.g.
        #    cdr_frc, upscale_output) to gate CPP flags, so we merge the
        #    run-time settings into the render context alongside the
        #    compile-time settings. Extra keys are ignored by the template.
        # ------------------------------------------------------------------
        cppdefs_render_dict = {
            **self._settings_compile_time,
            **self._settings_run_time,
        }
        compile_time_code = render_roms_settings(
            template_files=["cppdefs.opt.j2"],
            template_dir=self._stage_templates("compile_time"),
            settings_dict=cppdefs_render_dict,
            code_output_dir=self.compile_time_code_dir,
            n_tracers=n_tracers,
        )

        # ------------------------------------------------------------------
        # 2. Run-time static files: copy any non-template files listed in the
        #    run_time filter (e.g. marbl_in).
        #    These are model-specific support files that aren't templated.
        # ------------------------------------------------------------------
        run_time_static_files = [
            f for f in self._template_files("run_time") if not f.endswith(".j2")
        ]
        copied_run_time_files: list[str] = []
        if run_time_static_files:
            _copied = render_roms_settings(
                template_files=run_time_static_files,
                template_dir=self._stage_templates("run_time"),
                settings_dict={},  # plain copy — no template variables needed
                code_output_dir=self.run_time_code_dir,
            )
            copied_run_time_files = _copied["filter"]["files"]

        # ------------------------------------------------------------------
        # 3. Run-time namelist: write namelist.nml from merged settings.
        #    This replaces the former roms.in template output and absorbs
        #    all former *.opt parameters (except cppdefs.opt).
        # ------------------------------------------------------------------
        write_roms_namelist(
            settings_run_time=self._settings_run_time,
            output_dir=self.run_time_code_dir,
            n_tracers=n_tracers,
        )

        # Build the run-time code descriptor: namelist + any copied static files.
        run_time_code = {
            "location": str(self.run_time_code_dir.resolve()),
            "branch": "na",
            "filter": {"files": sorted(["namelist.nml", *copied_run_time_files])},
        }

        # Suppress Pydantic serialization warnings when using model_dump(mode='json') and model_construct
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

            blueprint_dict = self.blueprint.model_dump(mode="json")
            code_dict = blueprint_dict["code"]
            # Convert dicts from render_roms_settings / write_roms_namelist to CodeRepository objects
            code_dict["compile_time"] = cstar_models.CodeRepository.model_construct(
                **compile_time_code
            )
            code_dict["run_time"] = cstar_models.CodeRepository.model_construct(
                **run_time_code
            )
            blueprint_dict["code"] = (
                cstar_models.ROMSCompositeCodeRepository.model_construct(**code_dict)
            )

            blueprint_dict["model_params"] = {
                "time_step": self._settings_run_time["time_stepping"]["dt"],
            }
            blueprint_dict["runtime_params"] = {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "output_dir": self.run_output_dir,
            }
            blueprint_dict["working_dir"] = self.run_output_dir

            self.blueprint = cstar_models.RomsMarblBlueprint.model_construct(
                **blueprint_dict
            )
            self._stage = BlueprintStage.BUILD
            self.persist()

        return

    def prep_cstar_environment(
        self,
        account_key: str | None = None,
        queue_name: str | None = None,
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
        mc = self._get_machine_config()
        queues = mc.queues or {}

        # precedence: passed variable > pre-existing env-var setting > internal machine config > some default
        account_key = (
            account_key or os.getenv(ENV_CSTAR_SLURM_ACCOUNT) or mc.account or ""
        )
        queue_name = (
            queue_name
            or os.getenv(ENV_CSTAR_SLURM_QUEUE)
            or queues.get("default")
            or ""
        )
        walltime = walltime or os.getenv(ENV_CSTAR_SLURM_MAX_WALLTIME) or "6:00:00"
        clobber = "1" if clobber else os.getenv(ENV_CSTAR_CLOBBER_WORKING_DIR, "0")
        in_active_alloc = (
            "1" if on_compute_node else os.getenv(ENV_CSTAR_IN_ACTIVE_ALLOCATION, "0")
        )

        # set everything
        os.environ[ENV_CSTAR_CLOBBER_WORKING_DIR] = clobber
        os.environ[ENV_CSTAR_IN_ACTIVE_ALLOCATION] = in_active_alloc
        os.environ[ENV_CSTAR_SLURM_ACCOUNT] = account_key
        os.environ[ENV_CSTAR_SLURM_QUEUE] = queue_name
        os.environ[ENV_CSTAR_SLURM_MAX_WALLTIME] = walltime

        if n_procs_available:
            os.environ[ENV_CSTAR_NPROCS_POST] = str(n_procs_available)
        elif n_procs_available == 0:
            if os.getenv(ENV_CSTAR_NPROCS_POST):
                del os.environ[ENV_CSTAR_NPROCS_POST]
        # implicit: elif n_procs_available is None, do nothing

        if self._require_host().system == "RCAC_anvil":
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
        """Run C-Star for this Builder's BUILD blueprint"""
        self.prep_cstar_environment()

        request = RunnerRequest(
            uri=str(self.path_blueprint(stage=BlueprintStage.BUILD)),
            bp_type=RomsMarblBlueprint,
            name=self.casename,
        )
        service_cfg = get_service_config(log_level="INFO")
        job_cfg = get_job_config()
        runner = RomsMarblRunner(
            request=request, service_cfg=service_cfg, job_cfg=job_cfg
        )
        await runner.execute()

        # Persist blueprint to file
        self._stage = BlueprintStage.RUN
        self.persist()
