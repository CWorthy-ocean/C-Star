"""
Input data generation classes for CSFORGE models.

This module provides classes for generating input data files for ocean models.
The base InputData class defines the interface, and RomsMarblInputData provides
the ROMS-MARBL specific implementation.
"""
from __future__ import annotations

import inspect
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field
import xarray as xr

import cstar.applications.roms_marbl.models as cstar_models
from cstar.orchestration.models import Resource

from . import config
from . import models as forge_models
from . import source_data
from .util import roms_tools_nesting_writer
import roms_tools as rt

# Basename stem for CDR NetCDF: ``{domain_name}_cdr.nc``. The full name must contain the
# substring ``cdr.nc`` so C-Star's ROMS build check on ``cdr_frc.opt`` passes.
CDR_FORCING_NETCDF_STEM = "cdr"


def netcdf_filename_component(component: str) -> str:
    """
    Sanitize a domain or input-name segment for ``{a}_{b}.nc`` basenames.

    Generated NetCDF files must not contain ``.`` except the ``.nc`` suffix (e.g. version
    strings like ``v0.1`` become ``v0_1``).
    """
    return str(component).replace(".", "_")


class RomsMarblBlueprintInputData(BaseModel):
    """
    Subset of RomsMarblBlueprint containing only input data fields.
    
    This includes only the fields related to input data generation:
    - grid
    - initial_conditions
    - forcing
    - cdr_forcing
    """
    
    model_config = ConfigDict(extra="forbid")
    
    grid: Optional[cstar_models.Dataset] = Field(default=None, validate_default=False)
    """Grid dataset."""
    
    initial_conditions: Optional[cstar_models.Dataset] = Field(default=None, validate_default=False)
    """Initial conditions dataset."""
    
    forcing: Optional[cstar_models.ForcingConfiguration] = Field(default=None, validate_default=False)
    """Forcing configuration."""
    
    cdr_forcing: Optional[cstar_models.Dataset] = Field(default=None, validate_default=False)
    """CDR forcing dataset."""

    nesting_info: Optional[cstar_models.Dataset] = Field(default=None, validate_default=False)
    """Nesting info dataset (only set when a child grid is present)."""


@dataclass
class InputData:
    """
    Base class for generating input data files for ocean models.
    
    This class defines the interface for input data generation. Subclasses
    should implement the model-specific generation methods.
    """
    
    # Core configuration
    domain_name: str
    start_date: Any
    end_date: Any

    # Derived paths
    input_data_dir: Path = field(init=False)
    
    def __post_init__(self):
        """Initialize paths and storage."""
        # Subclasses (e.g. RomsMarblInputData) may define input_data_dir_override as a
        # trailing optional field; base InputData does not declare it (dataclass ordering).
        override = getattr(self, "input_data_dir_override", None)
        if override is not None:
            self.input_data_dir = Path(override)
        else:
            self.input_data_dir = (
                config.paths.input_data / netcdf_filename_component(self.domain_name)
            )
        self.input_data_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_all(self):
        """
        Generate all input files for this model.
        
        Subclasses should implement this method to generate all required inputs.
        """
        raise NotImplementedError("Subclasses must implement generate_all()")
    
    def _forcing_filename(self, input_name: str) -> Path:
        """Construct the NetCDF filename for a given input name."""
        d = netcdf_filename_component(self.domain_name)
        stem = netcdf_filename_component(input_name)
        return self.input_data_dir / f"{d}_{stem}.nc"

    def _ensure_empty_or_clobber(self, clobber: bool) -> bool:
        """
        Ensure the input_data_dir is either empty or, if clobber=True,
        remove existing .nc files.
        """
        existing = list(self.input_data_dir.glob("*.nc"))
        
        if existing and not clobber:
            # Count is all *.nc in the directory; reuse applies only to *planned* outputs
            # (see generate_all), which may be fewer — e.g. partitioned/suffixed names or
            # leftover files from other runs.
            print(
                f"ℹ️  Input directory contains {len(existing)} .nc file(s): {self.input_data_dir}\n"
                "   (Continuing without clobber; per-step reuse follows the planned output list.)"
            )
            return True
        
        if existing and clobber:
            print(
                f"⚠️  Clobber=True: removing {len(existing)} existing .nc files in "
                f"{self.input_data_dir}..."
            )
            for f in existing:
                f.unlink()
        
        return True


# Input generation registry
class InputStep:
    """Metadata for a single ROMS input generation step."""

    def __init__(self, name: str, order: int, label: str, handler: Callable):
        self.name = name  # canonical key used for filenames & paths
        self.order = order  # execution order
        self.label = label  # human-readable label
        self.handler = handler  # function expecting `self` (RomsMarblInputData instance)


INPUT_REGISTRY: Dict[str, InputStep] = {}


def register_input(name: str, order: int, label: str | None = None):
    """
    Decorator to register an input-generation step.

    Parameters
    ----------
    name : str
        Key for this input (e.g., 'grid', 'initial_conditions', 'forcing.surface').
        This will be used in filenames, and to index the registry.
    order : int
        Execution order in `generate_all()`. Lower numbers run first.
    label : str, optional
        Human-readable label for progress messages. If omitted, `name` is used.
    """

    def decorator(func: Callable):
        step_label = label or name
        INPUT_REGISTRY[name] = InputStep(
            name=name,
            order=order,
            label=step_label,
            handler=func,
        )
        return func

    return decorator


@dataclass
class RomsMarblInputData(InputData):
    """
    ROMS-MARBL specific input data generation.
    
    This class handles generation of all ROMS-MARBL input files including:
    - Grid
    - Initial conditions
    - Surface forcing
    - Boundary forcing
    - Tidal forcing
    - River forcing
    - CDR forcing
    - Corrections
    """
    
    model_spec: forge_models.ModelSpec
    grid: rt.Grid
    boundaries: forge_models.OpenBoundaries
    source_data: source_data.SourceData
    blueprint_dir: Path
    partitioning: cstar_models.PartitioningParameterSet
    cdr_forcing: Optional[dict] = None
    grid_child: Optional[rt.Grid] = None
    metadata_child: Optional[dict[str, Any]] = None
    use_dask: bool = True
    input_data_dir_override: Optional[Path] = None
    """If set, NetCDF inputs are written here; otherwise under ``config.paths.input_data`` using a
    sanitized ``domain_name`` (same rule as NetCDF basenames: no ``.`` in the dirname)."""

    # Blueprint elements containing input data
    blueprint_elements: RomsMarblBlueprintInputData = field(init=False)
    
    # Settings dictionaries
    _settings_compile_time: dict = field(init=False)
    _settings_run_time: dict = field(init=False)
    
    # Coarse grid dimension flag (set during surface forcing generation)
    include_coarse_dims: Optional[bool] = field(default=None)
    _clobber: bool = field(default=False, init=False)
    _existing_planned_outputs: set[Path] = field(default_factory=set, init=False)

    def __post_init__(self):
        """Initialize paths, storage, and input list."""
        super().__post_init__()
        
        # Derive input_list from model_spec.inputs
        input_list = []
        
        # Get model inputs from model_spec
        model_inputs = self.model_spec.inputs
        
        # Process grid
        if model_inputs.grid:
            kwargs = model_inputs.grid.model_dump() if hasattr(model_inputs.grid, 'model_dump') else {}
            input_list.append(("grid", kwargs))
        
        # Process initial_conditions
        if model_inputs.initial_conditions:
            kwargs = model_inputs.initial_conditions.model_dump() if hasattr(model_inputs.initial_conditions, 'model_dump') else {}
            input_list.append(("initial_conditions", kwargs))
        
        # Process forcing
        if model_inputs.forcing:
            # Loop over all keys in forcing (e.g., surface, boundary, tidal, river, etc.)
            for category in model_inputs.forcing.model_fields.keys():
                items = getattr(model_inputs.forcing, category, None)
                if items is not None:
                    for item in items:
                        kwargs = item.model_dump() if hasattr(item, 'model_dump') else dict(item)
                        input_list.append((f"forcing.{category}", kwargs))

        # Optional user-provided CDR forcing via builder kwarg.
        # Merge with model-specified cdr_list if that input already exists.
        if self.cdr_forcing:
            input_list.append(("cdr_forcing", {"cdr_kwargs": self.cdr_forcing}))
        
        self.input_list = input_list
        
        # Sanity check: verify all function keys are registered
        unique_keys = {fk for fk, _ in self.input_list}
        registry_keys = set(INPUT_REGISTRY.keys())
        missing = sorted(unique_keys - registry_keys)
        if missing:
            raise ValueError(
                "The following inputs are listed in `input_list` but "
                f"have no registered handlers: {', '.join(missing)}"
            )
        
        # Initialize blueprint_elements with empty datasets
        forcing_keys = {"boundary", "surface", "tidal", "river", "corrections"}
        forcing_dict = {}
        for key in unique_keys:
            # Extract subkey for forcing categories
            if key.startswith("forcing."):
                subkey = key.split(".", 1)[1]
                if subkey in forcing_keys:
                    forcing_dict[subkey] = cstar_models.Dataset(data=[])
        
        # Check that required forcing categories are present
        if forcing_dict:
            if "boundary" not in forcing_dict:
                raise ValueError(
                    "Missing required 'boundary' forcing category. "
                    "Boundary forcing must be specified in model_spec.inputs."
                )
            if "surface" not in forcing_dict:
                raise ValueError(
                    "Missing required 'surface' forcing category. "
                    "Surface forcing must be specified in model_spec.inputs."
                )
        
        # Create ForcingConfiguration if we have forcing categories
        forcing_config = None
        if forcing_dict:
            forcing_config = cstar_models.ForcingConfiguration(**forcing_dict)
        
        # Initialize blueprint_elements
        self.blueprint_elements = RomsMarblBlueprintInputData(
            grid=cstar_models.Dataset(data=[]) if "grid" in unique_keys else None,
            initial_conditions=cstar_models.Dataset(data=[]) if "initial_conditions" in unique_keys else None,
            forcing=forcing_config,
            cdr_forcing=cstar_models.Dataset(data=[]) if "cdr_forcing" in unique_keys else None,
        )
        
        # Initialize settings dictionaries to empty dicts
        self._settings_compile_time = defaultdict(dict)
        self._settings_run_time = {"roms.in": {}}
    
    def generate_all(self, clobber: bool = False, partition_files: bool = False, test: bool = False):
        """
        Generate all ROMS input files for this grid using the registered
        steps whose names appear in `input_list`.

        Parameters
        ----------
        clobber : bool, optional
            If True, overwrite existing input files.
        partition_files : bool, optional
            If True, partition input files across tiles.
        test : bool, optional
            If True, truncate the loop after 2 iterations for testing purposes.
        """
        self._clobber = clobber
        if not self._ensure_empty_or_clobber(clobber):
            return None, {}, {}
        
        # Build list of (step, kwargs) tuples, sorted by order
        step_kwargs_list = []
        for function_key, kwargs in self.input_list:
            if function_key in INPUT_REGISTRY:
                step = INPUT_REGISTRY[function_key]
                step_kwargs_list.append((step, kwargs))
        
        step_kwargs_list.sort(key=lambda x: x[0].order)
        total = len(step_kwargs_list) + (1 if partition_files else 0)

        # Compute planned outputs once at the start of execution, and record which already exist.
        planned = self._planned_netcdf_outputs(step_kwargs_list)
        self._existing_planned_outputs = {
            path.resolve()
            for path in planned
            if self._planned_netcdf_already_present(path)
        }
        n_planned = len(planned)
        n_already = len(self._existing_planned_outputs)
        if n_already:
            print(
                f"ℹ️  Planned NetCDF outputs this run: {n_planned}; "
                f"{n_already} already on disk (exact or stem match, e.g. *_0001.nc) — "
                "generation/save will be skipped for those."
            )
        
        # Execute
        for idx, (step, kwargs) in enumerate(step_kwargs_list, start=1):
            if step.name == "forcing.boundary" and not any(self.boundaries.model_dump().values()):
                print(f"\n⏭️  [{idx}/{total}] Skipping boundary forcing (all open boundaries are False).")
                continue
            if test and step.name != "forcing.boundary":
                continue
            print(f"\n▶️  [{idx}/{total}] {step.label}...")
            step.handler(self, key=step.name, **kwargs)
            # Truncate after 2 iterations if test mode is enabled
            if test and idx >= 2:
                print(f"\n⚠️  Test mode: truncated after {idx} iterations\n")
                break
        # Partition step (optional)
        if partition_files:
            print(f"\n▶️  [{total}/{total}] Partitioning input files across tiles...")
            self._partition_files()
            print("\n✅ All input files generated and partitioned.\n")
        else:
            print("\n✅ All input files generated.\n")
        
        return self.blueprint_elements, self._settings_compile_time, self._settings_run_time

    def _planned_netcdf_outputs(self, step_kwargs_list: List[tuple[InputStep, Dict[str, Any]]]) -> List[Path]:
        """Return the planned NetCDF outputs for this generation run."""
        planned: List[Path] = []
        for step, kwargs in step_kwargs_list:
            if step.name == "grid":
                planned.append(self._forcing_filename("grid"))
                if self.grid_child is not None:
                    planned.append(self._forcing_filename("grid_child"))
                    planned.append(self._forcing_filename("nesting"))
                continue

            if step.name == "initial_conditions":
                planned.append(self._forcing_filename("initial_conditions"))
                continue

            if step.name == "forcing.boundary" and not any(self.boundaries.model_dump().values()):
                # Keep planned outputs consistent with generate_all(), which skips this step
                # when all open boundaries are disabled.
                continue

            if step.name in {"forcing.surface", "forcing.boundary"}:
                forcing_type = kwargs.get("type") if isinstance(kwargs, dict) else None
                suffix = f"{step.name.split('.', 1)[1]}-{forcing_type}" if forcing_type else step.name.split(".", 1)[1]
                planned.append(self._forcing_filename(suffix))
                continue

            if step.name.startswith("forcing."):
                planned.append(self._forcing_filename(step.name.split(".", 1)[1]))
                continue

            if step.name == "cdr_forcing":
                planned.append(self._forcing_filename(CDR_FORCING_NETCDF_STEM))

        # Preserve order while deduplicating
        deduped: List[Path] = []
        for p in planned:
            if p not in deduped:
                deduped.append(p)
        return deduped

    def _planned_netcdf_already_present(self, path: Path) -> bool:
        """
        True if this planned output is already on disk: exact path, or roms_tools-style
        suffixed files sharing the same stem (``stem*.nc``).
        """
        if path.exists():
            return True
        pattern = f"{path.stem}*.nc"
        return bool(list(path.parent.glob(pattern)))

    def _should_reuse_existing_output(self, path: Path) -> bool:
        """Return True when this planned output already exists and clobber=False."""
        if self._clobber:
            return False
        return path.resolve() in self._existing_planned_outputs

    def _existing_output_paths(self, path: Path) -> List[str]:
        """
        Return existing NetCDF paths that correspond to a planned output path.

        Some roms_tools writers produce suffixed outputs that share the same stem.
        For example, planning may include ``foo_surface-physics.nc`` while existing
        files are ``foo_surface-physics_0001.nc`` etc.
        """
        if self._clobber:
            return []

        matches: List[Path] = []
        if path.exists():
            matches.append(path)
        else:
            pattern = f"{path.stem}_*.nc"
            matches.extend(sorted(path.parent.glob(pattern)))

        # De-duplicate while preserving order.
        unique: List[str] = []
        for match in matches:
            match_str = str(match)
            if match_str not in unique:
                unique.append(match_str)
        return unique

    def _interp_frc_surface_reuse(
        self, input_args: Dict[str, Any], nc_path: Path
    ) -> int:
        """
        Infer blk/bgc ``interp_frc`` when reusing NetCDF without a ``SurfaceForcing`` instance.

        Uses ``coarse_grid_mode`` when unambiguous; for ``auto``, peeks at the existing file.
        """
        mode = input_args.get("coarse_grid_mode", "auto")
        if mode == "never":
            return 0
        if mode == "always":
            return 1
        try:
            with xr.open_dataset(nc_path, decode_times=False) as ds:
                sizes = getattr(ds, "sizes", ds.dims)
                for dim in ("xi_coarse", "eta_coarse"):
                    if dim in sizes:
                        return 1
        except Exception:
            pass
        return 0

    def _yaml_filename(self, input_name: str) -> Path:
        """Construct the YAML filename for a given input key."""
        self.blueprint_dir.mkdir(parents=True, exist_ok=True)
        return self.blueprint_dir / f"_{input_name}.yml"
    
    def _resolve_source_block(self, block: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Normalize a "source"/"bgc_source" block and inject a 'path'
        based on SourceData.
        """
        if isinstance(block, str):
            name = block
            out: Dict[str, Any] = {"name": name}
        elif isinstance(block, dict):
            out = dict(block)
            name = out.get("name")
            if not name:
                raise ValueError(
                    f"Source block {block!r} is missing a 'name' field."
                )
        else:
            raise TypeError(f"Unsupported source block type: {type(block)}")
        
        glorys_layout = (
            out.get("glorys_layout") if name.upper() == "GLORYS" else None
        )
        
        # Get the mapped dataset key to check if it's streamable
        dataset_key = self.source_data.dataset_key_for_source(
            name, glorys_layout=glorys_layout
        )
        
        # If streamable and no path was explicitly provided in YAML, don't add path field
        if dataset_key in source_data.STREAMABLE_SOURCES:
            if "path" not in out:
                return out
            return out
        
        path = self.source_data.path_for_source(name, glorys_layout=glorys_layout)
        if path is not None:
            out.setdefault("path", path)
        return out
    
    def _build_input_args(self, key: str, extra: Optional[Dict[str, Any]] = None, base_kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Merge per-input defaults with runtime arguments.
        
        Uses base_kwargs if provided (from input_list), otherwise looks up in model_spec.inputs.
        Resolves "source" and "bgc_source" through SourceData.
        Merges with extra, where extra overrides defaults.
        """
        # Use base_kwargs if provided (this comes from input_list)
        if base_kwargs is not None:
            cfg = dict(base_kwargs)
        else:
            # Fallback: try to get from model_spec.inputs structure
            # This shouldn't normally be needed since base_kwargs should be provided
            cfg = {}
            if key == "grid":
                if self.model_spec.inputs.grid:
                    cfg = self.model_spec.inputs.grid.model_dump()
            elif key == "initial_conditions":
                if self.model_spec.inputs.initial_conditions:
                    cfg = self.model_spec.inputs.initial_conditions.model_dump()
            # For forcing categories, base_kwargs should always be provided from input_list
        
        # Resolve source blocks (convert SourceSpec Pydantic models to dicts with paths)
        for field_name in ("source", "bgc_source"):
            if field_name in cfg:
                # If it's a Pydantic model (SourceSpec), convert to dict first
                if hasattr(cfg[field_name], 'model_dump'):
                    cfg[field_name] = cfg[field_name].model_dump()
                cfg[field_name] = self._resolve_source_block(cfg[field_name])
        
        # extra overrides defaults
        if extra:
            return {**cfg, **extra}
        return cfg
    
    # These are registered with @register_input decorator
    @register_input(name="grid", order=10, label="Writing ROMS grid")
    def _generate_grid(self, key: str = "grid", **kwargs):
        """Generate grid input file."""
        out_path = self._forcing_filename(input_name="grid")
        yaml_path = self._yaml_filename(key)
        
        if self._should_reuse_existing_output(out_path):
            print(f"   ↪ Reusing existing file: {out_path}")
        else:
            self.grid.save(out_path)

        try:
            self.grid.to_yaml(yaml_path)
        except Exception as e:
            warnings.warn(
                f"Failed to save grid YAML to {yaml_path}: {e}",
                UserWarning,
                stacklevel=2,
            )

        out_path_nesting = None
        if self.grid_child is not None:
            out_path_child = self._forcing_filename(input_name="grid_child")
            if self._should_reuse_existing_output(out_path_child):
                print(f"   ↪ Reusing existing file: {out_path_child}")
            else:
                self.grid_child.save(out_path_child)
            yaml_path_child = self._yaml_filename(key + "_child")

            try:
                self.grid_child.to_yaml(yaml_path_child)
            except Exception as e:
                warnings.warn(
                    f"Failed to save child grid YAML to {yaml_path_child}: {e}",
                    UserWarning,
                    stacklevel=2,
                )

            out_path_nesting = self._forcing_filename(input_name="nesting")
            if self._should_reuse_existing_output(out_path_nesting):
                print(f"   ↪ Reusing existing file: {out_path_nesting}")
            else:
                # This section of code is needed when doing nesting with BGC.  ROMS_Tools has a flag called "include_bgc" which
                # defaults to false when we are making child boundary conditions, but it needs to be set to true in order to
                # save the BGC variables.
                nesting_writer = roms_tools_nesting_writer()
                nesting_kwargs = dict(self.metadata_child or {})
                has_marbl = (
                    self.model_spec.settings.properties is not None
                    and self.model_spec.settings.properties.marbl
                )
                if has_marbl and "include_bgc" in inspect.signature(
                    nesting_writer
                ).parameters:
                    # ROMS-Tools: include_bgc=True sets output_vars to include "bgc" on nesting.nc.
                    nesting_kwargs.setdefault("include_bgc", True)
                nesting_writer(
                    self.grid,
                    self.grid_child,
                    out_path_nesting,
                    **nesting_kwargs,
                )
            self.blueprint_elements.nesting_info = cstar_models.Dataset(
                data=[Resource(location=str(out_path_nesting), partitioned=False)]
            )

        # Append Resource directly to blueprint_elements.grid
        resource = Resource(location=str(out_path), partitioned=False)
        self.blueprint_elements.grid.data.append(resource)

        self._settings_run_time["roms.in"]["grid"] = dict(
            grid_file = out_path,
        )        

        if "cppdefs" not in self._settings_compile_time:
            self._settings_compile_time["cppdefs"] = {}
        self._settings_compile_time["cppdefs"]["obc_west"] = self.boundaries.west
        self._settings_compile_time["cppdefs"]["obc_east"] = self.boundaries.east
        self._settings_compile_time["cppdefs"]["obc_north"] = self.boundaries.north
        self._settings_compile_time["cppdefs"]["obc_south"] = self.boundaries.south

        if "param" not in self._settings_compile_time:
            self._settings_compile_time["param"] = {}
        self._settings_compile_time["param"]["LLm"] = self.grid.nx
        self._settings_compile_time["param"]["MMm"] = self.grid.ny
        self._settings_compile_time["param"]["N"] = self.grid.N
        self._settings_compile_time["param"]["NP_XI"] = self.partitioning.n_procs_x
        self._settings_compile_time["param"]["NP_ETA"] = self.partitioning.n_procs_y
        self._settings_compile_time["param"]["NSUB_X"] = 1
        self._settings_compile_time["param"]["NSUB_E"] = 1

        if out_path_nesting is not None:
            if "extract_data" not in self._settings_compile_time:
                self._settings_compile_time["extract_data"] = {}
            self._settings_compile_time["extract_data"]["do_extract"] = True
            self._settings_compile_time["extract_data"]["extract_file"] = "nesting.nc"
            self._settings_compile_time["extract_data"]["N_chd"] = self.grid_child.N
            self._settings_compile_time["extract_data"]["theta_s_chd"] = self.grid_child.theta_s
            self._settings_compile_time["extract_data"]["theta_b_chd"] = self.grid_child.theta_b
            self._settings_compile_time["extract_data"]["hc_chd"] = self.grid_child.hc

        self._settings_run_time["roms.in"]["s_coord"] = dict(
            tcline = self.grid.hc,
            theta_b = self.grid.theta_b,
            theta_s = self.grid.theta_s,
        )
        
    @register_input(name="initial_conditions", order=20, label="Generating initial conditions")
    def _generate_initial_conditions(self, key: str = "initial_conditions", **kwargs):
        """Generate initial conditions input file."""
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(input_name="initial_conditions")
        extra = dict(
            ini_time=self.start_date,
            use_dask=self.use_dask,
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        
        if self._should_reuse_existing_output(output_path):
            print(f"   ↪ Reusing existing file: {output_path}")
            paths = [str(output_path)]
            ic = None
        else:
            ic = rt.InitialConditions(grid=self.grid, **input_args)
            paths = ic.save(output_path)

        # See here: https://github.com/CWorthy-ocean/roms-tools/issues/553
        if ic is not None:
            try:
                ic.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save initial conditions YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )

        # Append Resources directly to blueprint_elements.initial_conditions
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                self.blueprint_elements.initial_conditions.data.append(resource)
        else:
            resource = Resource(location=paths, partitioned=False)
            self.blueprint_elements.initial_conditions.data.append(resource)

        self._settings_run_time["roms.in"]["initial"] = dict(
            nrrec = 1,
            initial_file = paths[0],
        )
    
    @register_input(name="forcing.surface", order=30, label="Generating surface forcing")
    def _generate_surface_forcing(self, key: str = "forcing.surface", **kwargs):
        """Generate surface forcing input files."""
        # Extract subkey from "forcing.surface" -> "surface"
        subkey = key.split(".", 1)[1] if "." in key else key
        
        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
            use_dask=self.use_dask,
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        type = input_args.get("type")
        if type is None:
            raise ValueError(
                f"Missing required 'type' key in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )
        if type not in {"physics", "bgc", "restoring"}:
            raise ValueError(
                f"Invalid 'type' value '{type}' in input_args for '{key}'. "
                f"Expected 'type' to be 'physics', 'bgc', or 'restoring'."
            )

        source_name = input_args.get("source").get("name")
        if input_args.get("type") == "bgc" and source_name == "MBL_co2":
            yaml_path = self._yaml_filename(f"{key}-{type}-co2")
            output_path = self._forcing_filename(input_name=f"surface-{type}-co2")
        else:
            yaml_path = self._yaml_filename(f"{key}-{type}")
            output_path = self._forcing_filename(input_name=f"surface-{type}")

        existing_paths = self._existing_output_paths(output_path)
        frc = None
        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            if not yaml_path.exists():
                warnings.warn(
                    f"Surface forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                    "constructing SurfaceForcing once to write YAML (this may be slow).",
                    UserWarning,
                    stacklevel=2,
                )
                frc = rt.SurfaceForcing(grid=self.grid, **input_args)
                try:
                    frc.to_yaml(yaml_path)
                except Exception as e:
                    warnings.warn(
                        f"Failed to save surface forcing YAML to {yaml_path}: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        else:
            frc = rt.SurfaceForcing(grid=self.grid, **input_args)
            paths = frc.save(output_path)
            try:
                frc.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save surface forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )

        if input_args["type"] == "restoring":
            if "sss" in input_args["restoring_forces"]:
                self._settings_compile_time["cppdefs"]["sal_restore"] = True
        elif input_args["type"] == "bgc" and input_args["source"]["name"] == "MBL_co2":
            self._settings_compile_time["cppdefs"]["co2_tvarying"] = True

        # Append Resources directly to blueprint_elements.forcing[subkey]
        
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.blueprint_elements.forcing, subkey).data.append(resource)

        # TODO: Update self._settings_compile_time with related forcing parameter sets and cppdefs for surface forcing
        if frc is not None and hasattr(frc, "use_coarse_grid"):
            interp_frc = 1 if frc.use_coarse_grid else 0
        else:
            interp_frc = self._interp_frc_surface_reuse(input_args, Path(paths[0]))
        
        # Only touch 'bgc' if the model has MARBL/BGC (from PropertiesSpec).
        has_bgc_compile = (
            self.model_spec.settings.properties is not None
            and self.model_spec.settings.properties.marbl
        )
        
        # Set interp_frc in the appropriate section based on forcing type
        # blk_frc.interp_frc is for physics surface forcing
        # bgc.interp_frc is for bgc surface forcing (only if model has bgc)
        # Both should have the same value when present (enforced by check below)
        if "blk_frc" not in self._settings_compile_time:
            self._settings_compile_time["blk_frc"] = {}
        if has_bgc_compile and "bgc" not in self._settings_compile_time:
            self._settings_compile_time["bgc"] = {}
        
        # Check for consistency: all surface forcing types should use the same coarse grid setting
        if "interp_frc" in self._settings_compile_time["blk_frc"]:
            if interp_frc != self._settings_compile_time["blk_frc"]["interp_frc"]:
                raise ValueError("Mismatch in coarse grid settings between surface forcing types")
        if has_bgc_compile and "interp_frc" in self._settings_compile_time["bgc"]:
            if interp_frc != self._settings_compile_time["bgc"]["interp_frc"]:
                raise ValueError("Mismatch in coarse grid settings between surface forcing types")
        
        # Set interp_frc for the appropriate section based on type (only set bgc if model has bgc)
        if "bgc" in type and has_bgc_compile:
            self._settings_compile_time["bgc"]["interp_frc"] = interp_frc
        else:
            self._settings_compile_time["blk_frc"]["interp_frc"] = interp_frc
        
        self.include_coarse_dims = interp_frc == 1
        
        if "forcing" not in self._settings_run_time["roms.in"]:
            self._settings_run_time["roms.in"]["forcing"] = {}

        if "bgc" in type:
            self._settings_run_time["roms.in"]["forcing"]["surface_forcing_bgc_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths
        else:
            self._settings_run_time["roms.in"]["forcing"]["surface_forcing_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths
    
    @register_input(name="forcing.boundary", order=40, label="Generating boundary forcing")
    def _generate_boundary_forcing(self, key: str = "forcing.boundary", **kwargs):
        """Generate boundary forcing input files."""
        if hasattr(self, 'grid_parent'):
            return
        # Extract subkey from "forcing.boundary" -> "boundary"
        subkey = key.split(".", 1)[1] if "." in key else key
        
        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
            boundaries=self.boundaries.model_dump() if hasattr(self.boundaries, 'model_dump') else self.boundaries,
            use_dask=self.use_dask,
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        type = input_args.get("type")
        if type is None:
            raise ValueError(
                f"Missing required 'type' key in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )
        if type not in {"physics", "bgc"}:
            raise ValueError(
                f"Invalid 'type' value '{type}' in input_args for '{key}'. "
                f"Expected 'type' to be 'physics' or 'bgc'."
            )
        
        yaml_path = self._yaml_filename(f"{key}-{type}")
        output_path = self._forcing_filename(input_name=f"boundary-{type}")
       
        existing_paths = self._existing_output_paths(output_path)
        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            if not yaml_path.exists():
                warnings.warn(
                    f"Boundary forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                    "constructing BoundaryForcing once to write YAML (this may be slow).",
                    UserWarning,
                    stacklevel=2,
                )
                bry = rt.BoundaryForcing(grid=self.grid, **input_args)
                try:
                    bry.to_yaml(yaml_path)
                except Exception as e:
                    warnings.warn(
                        f"Failed to save boundary forcing YAML to {yaml_path}: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        else:
            bry = rt.BoundaryForcing(grid=self.grid, **input_args)
            paths = bry.save(output_path)
            try:
                bry.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save boundary forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
        # Append Resources directly to blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.blueprint_elements.forcing, subkey).data.append(resource)

        # TODO: Update self._settings_compile_time with related forcing parameter sets and cppdefs
        
        if "forcing" not in self._settings_run_time["roms.in"]:
            self._settings_run_time["roms.in"]["forcing"] = {}

        if "bgc" in type:
            self._settings_run_time["roms.in"]["forcing"]["boundary_forcing_bgc_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths
        else:
            self._settings_run_time["roms.in"]["forcing"]["boundary_forcing_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths
    
    @register_input(name="forcing.tidal", order=50, label="Generating tidal forcing")
    def _generate_tidal_forcing(self, key: str = "forcing.tidal", **kwargs):
        """Generate tidal forcing input files."""
        subkey = key.split(".", 1)[1] if "." in key else key
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(subkey)
        extra = dict(
            use_dask=self.use_dask,
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        existing_paths = self._existing_output_paths(output_path)
        tidal: rt.TidalForcing = None
        if existing_paths and yaml_path.exists():
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            with yaml_path.open() as f:
                # roms_tools may emit multi-document YAML (e.g. version header + Grid/TidalForcing).
                ntides = None
                for doc in yaml.safe_load_all(f):
                    if doc and isinstance(doc, dict) and "TidalForcing" in doc:
                        ntides = doc["TidalForcing"].get("ntides")
                        break
                if ntides is None:
                    raise ValueError(
                        f"No TidalForcing.ntides found in YAML (expected multi-document "
                        f"roms_tools output): {yaml_path}"
                    )
        elif existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = existing_paths
            warnings.warn(
                f"Tidal forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                "constructing TidalForcing once to write YAML (this may be slow).",
                UserWarning,
                stacklevel=2,
            )
            tidal = rt.TidalForcing(grid=self.grid, **input_args)
            try:
                tidal.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save tidal forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
        else:
            tidal = rt.TidalForcing(grid=self.grid, **input_args)
            paths = tidal.save(output_path)
            try:
                tidal.to_yaml(yaml_path)
            except Exception as e:
                warnings.warn(
                    f"Failed to save tidal forcing YAML to {yaml_path}: {e}",
                    UserWarning,
                    stacklevel=2,
                )
            
        # Append Resources directly to blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
        
        # Update settings_dict with tidal forcing parameters
        self._settings_compile_time["tides"] = dict(
            ntides = ntides if tidal is None else tidal.ntides,
            bry_tides = True,
            pot_tides = True,
            ana_tides = False
        )

        if "forcing" not in self._settings_run_time["roms.in"]:
            self._settings_run_time["roms.in"]["forcing"] = {}
        self._settings_run_time["roms.in"]["forcing"]["tidal_forcing_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths

    @register_input(name="forcing.river", order=60, label="Generating river forcing")
    def _generate_river_forcing(self, key: str = "forcing.river", **kwargs):
        """Generate river forcing input files."""
        # Extract subkey from "forcing.river" -> "river"
        subkey = key.split(".", 1)[1] if "." in key else key
        yaml_path = self._yaml_filename(key)
        output_path = self._forcing_filename(subkey)
        extra = dict(
            start_time=self.start_date,
            end_time=self.end_date,
        )
        input_args = self._build_input_args(key, extra=extra, base_kwargs=kwargs)
        existing_paths = self._existing_output_paths(output_path)

        if existing_paths and yaml_path.exists():
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = list(existing_paths)
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning, module="xarray")
                with xr.open_dataset(Path(paths[0]), decode_timedelta=False) as ds:
                    if "river_volume" not in ds.variables:
                        raise ValueError("river_volume is not in the dataset")
                    if "river_tracer" not in ds.variables:
                        raise ValueError("river_tracer is not in the dataset")
                    nriv = int(ds.sizes["nriver"])
            if "river_frc" not in self._settings_compile_time:
                self._settings_compile_time["river_frc"] = {}
            self._settings_compile_time["river_frc"]["river_source"] = True
            self._settings_compile_time["river_frc"]["analytical"] = False
            self._settings_compile_time["river_frc"]["nriv"] = nriv
            self._settings_compile_time["river_frc"]["rvol_vname"] = "river_volume"
            self._settings_compile_time["river_frc"]["rvol_tname"] = "river_time"
            self._settings_compile_time["river_frc"]["rtrc_vname"] = "river_tracer"
            self._settings_compile_time["river_frc"]["rtrc_tname"] = "river_time"
            if "forcing" not in self._settings_run_time["roms.in"]:
                self._settings_run_time["roms.in"]["forcing"] = {}
            self._settings_run_time["roms.in"]["forcing"]["river_path"] = (
                paths[0] if isinstance(paths, (list, tuple)) else paths
            )
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
            return

        try:
            river = rt.RiverForcing(grid=self.grid, **input_args)
        except ValueError as e:
            warnings.warn(
                f"Skipping river forcing generation due to invalid river configuration: {e}",
                UserWarning,
                stacklevel=2,
            )
            if self.blueprint_elements.forcing is not None:
                self.blueprint_elements.forcing.river = None
            river = rt.RiverForcing.__new__(rt.RiverForcing)
            return river
        if existing_paths:
            print(f"   ↪ Reusing existing file(s): {', '.join(existing_paths)}")
            paths = list(existing_paths)
            warnings.warn(
                f"River forcing NetCDF exists but YAML sidecar is missing ({yaml_path}); "
                "constructing RiverForcing once to write YAML (this may be slow).",
                UserWarning,
                stacklevel=2,
            )
        else:
            paths = river.save(output_path)
        if isinstance(paths, (list, tuple)) and len(paths) == 0:
            if self.blueprint_elements.forcing is not None:
                self.blueprint_elements.forcing.river = None
            return river
        try:
            river.to_yaml(yaml_path)
        except Exception as e:
            warnings.warn(
                f"Failed to save river forcing YAML to {yaml_path}: {e}",
                UserWarning,
                stacklevel=2,
            )
        # Append Resources directly to blueprint_elements.forcing[subkey]
        if isinstance(paths, (list, tuple)):
            for path in paths:
                resource = Resource(location=path, partitioned=False)
                getattr(self.blueprint_elements.forcing, subkey).data.append(resource)
        else:
            resource = Resource(location=paths, partitioned=False)
            getattr(self.blueprint_elements.forcing, subkey).data.append(resource)

        # updates settings_dict
        if "river_frc" not in self._settings_compile_time:            
            self._settings_compile_time["river_frc"] = {}

        self._settings_compile_time["river_frc"]["river_source"] = True
        self._settings_compile_time["river_frc"]["analytical"] = False
        self._settings_compile_time["river_frc"]["nriv"] = river.ds.sizes["nriver"]
        
        # check to make sure river_volume and river_tracer are in the dataset
        if "river_volume" not in river.ds.variables:
            raise ValueError("river_volume is not in the dataset")
        if "river_tracer" not in river.ds.variables:
            raise ValueError("river_tracer is not in the dataset")
        
        self._settings_compile_time["river_frc"]["rvol_vname"] = "river_volume"
        self._settings_compile_time["river_frc"]["rvol_tname"] = "river_time"
        self._settings_compile_time["river_frc"]["rtrc_vname"] = "river_tracer"
        self._settings_compile_time["river_frc"]["rtrc_tname"] = "river_time"

        if "forcing" not in self._settings_run_time["roms.in"]:
            self._settings_run_time["roms.in"]["forcing"] = {}
        self._settings_run_time["roms.in"]["forcing"]["river_path"] = paths[0] if isinstance(paths, (list, tuple)) else paths

    @register_input(name="cdr_forcing", order=80, label="Generating CDR forcing")
    def _generate_cdr_forcing(self, key: str = "cdr_forcing", cdr_kwargs=None, **kwargs):
        """Generate CDR forcing input files."""
        cdr_kwargs = cdr_kwargs or {}
        if not cdr_kwargs:
            return
        
        yaml_path = self._yaml_filename(key)

        input_args = self._build_input_args(key, base_kwargs=cdr_kwargs)

        cdr = rt.CDRForcing(**input_args)
        output_path = self._forcing_filename(CDR_FORCING_NETCDF_STEM)
        if self._should_reuse_existing_output(output_path):
            print(f"   ↪ Reusing existing file: {output_path}")
            paths = [str(output_path)]
        else:
            paths = cdr.save(output_path)

        # Normalize output paths to absolute strings so downstream template
        # settings can reliably embed full file locations.
        normalized_paths: List[str] = []
        if isinstance(paths, (list, tuple)):
            raw_paths = list(paths)
        else:
            raw_paths = [paths]
        for raw_path in raw_paths:
            path_obj = Path(str(raw_path))
            if not path_obj.is_absolute():
                path_obj = output_path.parent / path_obj
            normalized_paths.append(str(path_obj.resolve()))
        paths = normalized_paths

        cdr.to_yaml(yaml_path)
        # Append Resources directly to blueprint_elements.cdr_forcing
        for path in paths:
            resource = Resource(location=path, partitioned=False)
            self.blueprint_elements.cdr_forcing.data.append(resource)

        self._settings_compile_time["cppdefs"]["cdr_forcing"] = True
        # always set this to cdr.nc per conventions; c-star will symlink to the real path in the blueprint
        self._settings_compile_time["cdr_frc"]["cdr_file"] = "cdr.nc"
        self._settings_compile_time["cdr_frc"]["cdr_source"] = True
        self._settings_compile_time["cdr_frc"]["ncdr_parm"] = len(cdr.releases)
        self._settings_compile_time["cdr_frc"]["forcing_parameterized"] = True
        self._settings_compile_time["cdr_frc"]["cdr_volume"] = cdr.releases.release_type == "volume"
        # enable cdr output
        self._settings_compile_time["cdr_output"]["do_cdr"] = True

    @register_input(name="forcing.corrections", order=90, label="Generating corrections forcing")
    def _generate_corrections(self, key: str = "corrections", **kwargs):
        """Generate corrections forcing (not implemented)."""
        raise NotImplementedError("Corrections forcing generation is not yet implemented.")
    
    def _partition_files(self, **kwargs):
        """
        Partition whole input files across tiles using roms_tools.partition_netcdf.
        
        Uses the paths stored in `blueprint_elements` to build the list of whole-field files,
        and records the partitioned paths in the Resource objects.
        """

        input_args = dict(
            np_eta=self.partitioning.n_procs_y,
            np_xi=self.partitioning.n_procs_x,
            output_dir=self.input_data_dir,
            include_coarse_dims=self.include_coarse_dims,
        )
        
        for function_key, _ in self.input_list:
            name = function_key
            dataset = None
            
            # Get the appropriate dataset from blueprint_elements
            if name == "grid":
                dataset = self.blueprint_elements.grid
            elif name == "initial_conditions":
                dataset = self.blueprint_elements.initial_conditions
            elif name.startswith("forcing."):
                # Extract subkey from "forcing.surface" -> "surface"
                subkey = name.split(".", 1)[1]
                if self.blueprint_elements.forcing is not None:
                    dataset = getattr(self.blueprint_elements.forcing, subkey, None)
            elif name == "cdr_forcing":
                dataset = self.blueprint_elements.cdr_forcing
            
            if dataset is None or not dataset.data:
                print(f"⚠️  Skipping {name} because it is empty")
                continue
            
            # Partition each Resource in the dataset
            # We need to collect new resources because partitioning creates multiple files
            new_resources = []
            for resource in dataset.data:
                if resource.location is None:
                    new_resources.append(resource)
                    continue
                partitioned_paths = rt.partition_netcdf(resource.location, **input_args)
                # partition_netcdf returns a list of paths (one per partition)
                # Create a Resource for each partitioned file
                if isinstance(partitioned_paths, list):
                    for partitioned_path in partitioned_paths:
                        resource_dict = resource.model_dump()
                        resource_dict["location"] = partitioned_path
                        resource_dict["partitioned"] = True
                        new_resources.append(Resource(**resource_dict))
                else:
                    # If it returns a single path (shouldn't happen, but handle it)
                    resource_dict = resource.model_dump()
                    resource_dict["location"] = partitioned_paths
                    resource_dict["partitioned"] = True
                    new_resources.append(Resource(**resource_dict))
            # Replace all resources in the dataset with the new partitioned resources
            dataset.data = new_resources

