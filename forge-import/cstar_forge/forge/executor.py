"""
ForgeExecutor - Pydantic-based builder for C-Star blueprints.

This class provides a Pydantic-based interface for building RomsMarblBlueprint objects.
"""

from __future__ import annotations

import asyncio
import copy
import os
import sys
import warnings
from dataclasses import asdict as dataclass_asdict
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

from cstar_forge import config
from cstar_forge import models as forge_models
from cstar_forge.forge import input_data, source_data
from cstar_forge.forge.settings import render_roms_settings, write_roms_namelist
from cstar_forge.util import (
    compute_timestep_from_cfl,
    compute_v_sponge_from_grid,
    roms_tools_default_nesting_period_seconds,
)


def resolve_catalog_dir(catalog_root: str | Path | None) -> Path:
    """
    Resolve the absolute inner *catalog* directory (direct parent of ``blueprints/``).

    Parameters
    ----------
    catalog_root
        - ``None``: use ``config.paths.catalog`` (default data-tree location).
        - ``"default"`` (case-insensitive string): same as ``None``; uses the user's
          writable catalog at ``config.paths.catalog`` but without suppressing validation.
        - ``"local"`` (case-insensitive string): use the package layout
          ``<cstar_forge>/catalog`` (same as ``config.paths.here / "catalog"``); no extra
          ``/catalog`` suffix is applied.
        - Any other ``str`` or ``Path``: *outer* catalog anchor; the inner directory is
          ``<resolved_anchor>/catalog`` (i.e. blueprints live at ``.../catalog/blueprints``).
    """
    if catalog_root is None:
        return config.paths.catalog
    if isinstance(catalog_root, str) and catalog_root.strip().lower() in ("local",):
        return config.paths.here / "catalog"
    if isinstance(catalog_root, str) and catalog_root.strip().lower() == "default":
        return config.paths.catalog
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
    override: list[str | Path] | None = Field(default=None, validate_default=False)
    ensemble_id: int | None = Field(default=None, validate_default=False)
    catalog_root: str | Path | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Optional *outer* catalog anchor. Blueprints live under "
            "``<catalog_root>/catalog/blueprints/<machine>/<name>/``. "
            "Omit (None) to use the bundled internal catalog for ModelSpec/MachineSpec. "
            "Use ``'default'`` to open the user data-tree catalog at "
            "``config.paths.catalog`` with full validation. "
            "Use ``'local'`` for the in-repo ``cstar_forge/catalog`` package directory."
        ),
    )
    initialize_catalog_from: str | Path | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Merge Machines/, ModelSpec/, and DomainSpec/ from this source catalog "
            "into the resolved catalog_root before use. "
            "Pass ``'local'`` to merge from the built-in package catalog."
        ),
    )
    initialize_catalog_clobber: bool = Field(
        default=False,
        validate_default=False,
        description=(
            "When merging via ``initialize_catalog_from``, silently overwrite "
            "files that already exist at the destination. "
            "If False (default) and conflicts are found, raises ValueError listing them."
        ),
    )
    suppress_catalog_validation: bool = Field(
        default=True,
        validate_default=False,
        description=(
            "Skip the catalog structure validation check when opening the catalog. "
            "Defaults to True so that ForgeExecutor can operate on an empty or "
            "partially populated catalog without raising an error."
        ),
    )
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
    _model_spec: forge_models.ModelSpec | None = PrivateAttr(default=None)
    _datasets: dict[str, xr.Dataset | list[xr.Dataset]] | None = PrivateAttr(
        default=None
    )
    _stage: str | None = PrivateAttr(default=None)
    _cstar_simulation: Any | None = PrivateAttr(default=None)
    _settings_compile_time: dict[str, Any] = PrivateAttr(default_factory=dict)
    _settings_run_time: dict[str, Any] = PrivateAttr(default_factory=dict)
    _catalog_instance: Any | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _validate_dates(self) -> ForgeExecutor:
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
        if self.catalog_root is None:
            print(
                "Note: No catalog_root specified. Using internal cstar-forge catalog for ModelSpec "
                "and MachineSpec (default/example values).\n"
                f"      Blueprints will be written to: {config.paths.catalog}\n"
                "      To use a custom catalog, set catalog_root=<path> or catalog_root='default' "
                "when creating ForgeExecutor."
            )

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
        """Return the list of NetCDF files expected from input generation."""
        if self._model_spec is None:
            self._load_model_spec()

        input_data_dir = self.input_data_dir
        planned_paths: list[Path] = []

        def _add_nc(stem: str) -> None:
            base = input_data.netcdf_filename_component(self.name)
            part = input_data.netcdf_filename_component(stem)
            path = (input_data_dir / f"{base}_{part}.nc").resolve()
            if path not in planned_paths:
                planned_paths.append(path)

        model_inputs = getattr(self._model_spec, "inputs", None)
        if model_inputs is None:
            return planned_paths

        inputs_cfg: dict[str, Any] = {}
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

        if inputs_cfg.get("cdr_forcing"):
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
        Return the name of this blueprint as '{model_spec.name}_{grid_name}'.

        This property sets blueprint.name when the blueprint is created.
        """
        ensemble_str = (
            f"_{self.ensemble_id:03d}" if self.ensemble_id is not None else ""
        )
        return f"{self._model_spec.name}_{self.grid_name}_{self.n_procs}procs{ensemble_str}"

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

    def _get_catalog(self) -> Any:
        """Return (and cache) a DomainCatalog for this builder's resolved catalog directory."""
        if self._catalog_instance is None:
            from cstar_forge.domain_catalog import DomainCatalog

            self._catalog_instance = DomainCatalog(
                catalog_root=self.resolved_catalog_dir,
                initialize_catalog_from=self.initialize_catalog_from,
                initialize_catalog_clobber=self.initialize_catalog_clobber,
                suppress_validation=self.suppress_catalog_validation,
            )
        return self._catalog_instance

    @property
    def resolved_catalog_dir(self) -> Path:
        """Absolute inner *catalog* directory (contains ``blueprints/``)."""
        return resolve_catalog_dir(self.catalog_root)

    @property
    def blueprint_dir(self) -> Path:
        """Return the blueprint directory path."""
        return self._get_catalog().blueprint_dir_for(config.system_id, self.name)

    @property
    def compile_time_code_dir(self) -> Path:
        """Compile-time rendered templates inside this blueprint's Build/ directory."""
        return (
            self._get_catalog().build_dir_for(config.system_id, self.name)
            / "compile-time"
        )

    @property
    def run_time_code_dir(self) -> Path:
        """Run-time rendered templates inside this blueprint's Build/ directory."""
        return (
            self._get_catalog().build_dir_for(config.system_id, self.name) / "run-time"
        )

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

        # Add forcing fields from model_spec.inputs.forcing

        if (
            self._model_spec
            and self._model_spec.inputs
            and self._model_spec.inputs.forcing
        ):
            # Loop over all fields in the forcing configuration
            for field_name in self._model_spec.inputs.forcing.model_fields.keys():
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

    def _load_model_spec(self):
        """Load ModelSpec from the builder's catalog when catalog_root is set, else from the default catalog."""
        if self._uses_explicit_catalog:
            self._model_spec = self._get_catalog().load_model_spec(self.model_name)
        else:
            from cstar_forge.domain_catalog import default_catalog

            self._model_spec = default_catalog.load_model_spec(self.model_name)

    @property
    def _uses_explicit_catalog(self) -> bool:
        """True when catalog_root routes to a user-managed catalog (not the bundled fallback)."""
        return self.catalog_root is not None

    def _get_machine_config(self):
        """Return MachineConfig from the builder's catalog when catalog_root is set, else from config."""
        if self._uses_explicit_catalog:
            from cstar_forge.config import MachineConfig

            try:
                data = self._get_catalog().machine_data(config.system)
                return MachineConfig(
                    account=data.get("account"),
                    pes_per_node=data.get("pes_per_node"),
                    queues=data.get("queues"),
                )
            except KeyError:
                return MachineConfig()
        return config.machine_config

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
            code=self._model_spec.code,
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

        if self._model_spec is None:
            self._load_model_spec()

        self.src_data = source_data.SourceData(
            datasets=self._model_spec.datasets,
            clobber=False,
            grid=self.grid,
            grid_name=self.grid_name,
            start_time=self.start_date,
            end_time=self.end_date,
            source_data_dir=config.paths.source_data,
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
                model_spec=self._model_spec,
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

        # Apply settings from input data generation (deep merge to preserve existing settings).
        self._update_settings_compile_time(settings_compile_time)
        self._update_settings_run_time(settings_run_time)

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

    def _merge_settings_override_file(self, path: Path, kind: str) -> None:
        r"""
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
        payload: dict[str, Any]
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

        target = (
            self._settings_compile_time
            if kind == "compile"
            else self._settings_run_time
        )
        label = "compile-time" if kind == "compile" else "run-time"
        merged: dict[str, Any] = {}
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
    def _override_paths(self) -> list[Path]:
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
        self._settings_compile_time = copy.deepcopy(
            self._model_spec.settings.compile_time.settings_dict
        )

        self._merge_settings_override_files("compile")

    def _init_settings_run_time(self, dt: float | None = None) -> None:
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
        - `v_sponge.v_sponge`: Set from grid spacing (``size_x / nx`` in meters) / 10

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
        self._settings_run_time = copy.deepcopy(
            self._model_spec.settings.run_time.settings_dict
        )

        # Set dynamic values that depend on instance properties
        self._settings_run_time["title"] = dict(
            casename=self.casename,
        )
        self._settings_run_time["output_root_name"] = dict(
            output_root_name=str(self.run_output_dir / "output" / self.casename),
        )

        # Set timestepping defaults (will compute dt from CFL if dt is None)
        self._set_run_time_settings_timestepping_defaults(dt=dt)

        # Set extract_data.extract_period from child grid metadata (nesting case)
        if self.grid_child is not None:
            period_default = roms_tools_default_nesting_period_seconds()
            if "metadata" in self.grid_kwargs_child:
                if "period" in self.grid_kwargs_child["metadata"]:
                    self._settings_run_time["extract_data"]["extract_period"] = (
                        self.grid_kwargs_child["metadata"]["period"]
                    )
                else:
                    self._settings_run_time["extract_data"]["extract_period"] = (
                        period_default
                    )
            else:
                self._settings_run_time["extract_data"]["extract_period"] = (
                    period_default
                )

        self._apply_v_sponge_default_from_grid()

        self._merge_settings_override_files("run")

    def _apply_v_sponge_default_from_grid(self) -> None:
        """
        Set ``v_sponge.v_sponge`` from grid spacing.

        Default: ``(size_x / nx)`` in meters, divided by 10. Values merged later
        (override files or ``configure_build(run_time_settings=...)``) take
        priority.
        """
        v_sponge = compute_v_sponge_from_grid(self.grid.size_x, self.grid.nx)
        self._settings_run_time.setdefault("v_sponge", {})["v_sponge"] = v_sponge

    def _update_settings_compile_time(
        self, settings_compile_time: dict[str, Any]
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
            else:
                raise ValueError(
                    f"Unknown compile-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_compile_time.keys())}"
                )

    def _update_settings_run_time(self, settings_run_time: dict[str, Any]) -> None:
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
            else:
                # Unknown key - raise error
                raise ValueError(
                    f"Unknown run-time setting key: '{key}'. "
                    f"Valid keys are: {sorted(self._settings_run_time.keys())}"
                )

    def _set_run_time_settings_timestepping_defaults(self, dt: float | None = None):
        """
        Update run-time timestepping settings in the settings dictionary.

        Sets the `time_stepping` section of `_settings_run_time` with
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

        ntimes = round((self.end_date - self.start_date).days * 24 * 3600 / dt)
        self._settings_run_time["time_stepping"] = dict(
            ntimes=ntimes,
            dt=dt,
            ndtfast=60,  # TODO: Think about if how to better NDTFAST based on this dt
            ninfo=1,
        )

    @classmethod
    def from_spec_config(cls, cfg: Any) -> ForgeExecutor:
        """Canonical constructor: build the executor directly from a resolved
        ``SpecConfig`` (the forge application's blueprint).

        This is the single derivation path from a SpecConfig to a runnable builder. The
        domain-catalog path routes through the Phase-1 resolver
        (``spec_config_resolve.build_spec_config``) to produce the ``SpecConfig`` and then
        here — so there is one place that maps blueprint → builder inputs
        (``spec_config_engine.spec_config_to_builder_kwargs``).
        """
        from cstar_forge.forge.spec_config_engine import spec_config_to_builder_kwargs

        return cls(**spec_config_to_builder_kwargs(cfg))

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

        # Validate template configuration
        if (
            self._model_spec.templates is None
            or self._model_spec.templates.compile_time is None
            or self._model_spec.templates.compile_time.filter is None
        ):
            raise ValueError(
                "Model spec must have templates.compile_time.filter with files list"
            )
        if (
            self._model_spec.templates.run_time is None
            or self._model_spec.templates.run_time.filter is None
        ):
            raise ValueError(
                "Model spec must have templates.run_time.filter with files list"
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

        # Derive n_tracers: if the caller computed it from model_settings (Phase 2
        # engine path), use that directly; otherwise fall back to model_spec.properties
        # for back-compat with the legacy builder-only path.
        if "n_tracers" in kwargs:
            n_tracers = int(kwargs["n_tracers"])
        elif self._model_spec.settings.properties is not None:
            n_tracers = self._model_spec.settings.properties.n_tracers
        else:
            raise ValueError(
                "n_tracers could not be determined: neither passed as a kwarg nor "
                "available from model_spec.settings.properties."
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
            template_dir=self._model_spec.templates.compile_time.location,
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
            f
            for f in self._model_spec.templates.run_time.filter.files
            if not f.endswith(".j2")
        ]
        copied_run_time_files: list[str] = []
        if run_time_static_files:
            _copied = render_roms_settings(
                template_files=run_time_static_files,
                template_dir=self._model_spec.templates.run_time.location,
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

    def dump(self, file_path: str | Path) -> None:
        """
        Dump the exact state of ForgeExecutor to a YAML file.

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
        state_dict = self.model_dump(mode="json", exclude_none=True)

        # Add PrivateAttr fields that can be serialized
        private_attrs = {}

        # Serialize _model_spec if it exists (Pydantic model)
        if self._model_spec is not None:
            private_attrs["_model_spec"] = self._model_spec.model_dump(
                mode="json", exclude_none=True
            )

        # Serialize _stage (simple string)
        if self._stage is not None:
            private_attrs["_stage"] = self._stage

        # Serialize settings dictionaries
        if self._settings_compile_time:
            private_attrs["_settings_compile_time"] = self._convert_paths_to_strings(
                self._settings_compile_time
            )
        if self._settings_run_time:
            private_attrs["_settings_run_time"] = self._convert_paths_to_strings(
                self._settings_run_time
            )

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
            yaml.safe_dump(
                state_dict,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

    @classmethod
    def load(cls, file_path: str | Path) -> ForgeExecutor:
        """
        Load ForgeExecutor state from a YAML file.

        This method deserializes a previously saved state and reconstructs
        the ForgeExecutor instance. After loading:
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
        ForgeExecutor
            A new ForgeExecutor instance with state restored from the file.

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
            raise FileNotFoundError(f"ForgeExecutor state file not found: {file_path}")

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
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module="pydantic"
                )
                warnings.filterwarnings(
                    "ignore", message=".*Pydantic.*", category=UserWarning
                )
                warnings.filterwarnings(
                    "ignore", message=".*serialization.*", category=UserWarning
                )
                instance.blueprint = cstar_models.RomsMarblBlueprint.model_construct(
                    **blueprint_dict
                )

        # Restore PrivateAttr fields after instance creation
        if model_spec_dict is not None:
            # Use model_construct to handle None values and missing required fields
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module="pydantic"
                )
                warnings.filterwarnings(
                    "ignore", message=".*Pydantic.*", category=UserWarning
                )
                warnings.filterwarnings(
                    "ignore", message=".*serialization.*", category=UserWarning
                )
                instance._model_spec = forge_models.ModelSpec.model_construct(
                    **model_spec_dict
                )

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
