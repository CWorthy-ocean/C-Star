"""
Pydantic models for CSFORGE model input specifications.

These models represent the structure of inputs in models.yml, providing
type validation and structure for grid, initial_conditions, and forcing
configurations.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, PrivateAttr, model_validator

import cstar.applications.roms_marbl.models as models
from cstar.applications.roms_marbl.models import CodeRepository, ROMSCompositeCodeRepository

        

class SourceSpec(BaseModel):
    """
    Specification for a data source.
    
    Parameters
    ----------
    name : str
        Name of the source (e.g., "GLORYS", "ERA5", "UNIFIED").
    climatology : bool, optional
        Whether to use climatology data. Default is False.
    glorys_layout : str, optional
        When ``name`` is ``GLORYS``, selects GLORYS_GLOBAL vs GLORYS_REGIONAL
        preparation and paths. If omitted, defaults to ``regional``.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    name: str
    climatology: bool = Field(default=False, validate_default=False)
    glorys_layout: Optional[Literal["global", "regional"]] = Field(
        default=None,
        validate_default=False,
    )

    @model_validator(mode="after")
    def _glorys_layout_only_for_glorys(self) -> "SourceSpec":
        if self.glorys_layout is not None and self.name.upper() != "GLORYS":
            raise ValueError("glorys_layout is only valid when name is GLORYS")
        return self


class GridInput(BaseModel):
    """
    Grid input specification.

    Parameters
    ----------
    topography_source : str
        Source for topography data (e.g., "ETOPO5").

    Any additional keyword arguments are passed directly to the roms-tools
    Grid constructor.
    """

    model_config = ConfigDict(extra="allow")

    topography_source: str


class InitialConditionsInput(BaseModel):
    """
    Initial conditions input specification.

    Parameters
    ----------
    source : SourceSpec
        Source specification for physics initial conditions.
    bgc_source : Optional[SourceSpec]
        Source specification for biogeochemical initial conditions.

    Any additional keyword arguments are passed directly to the roms-tools
    InitialConditions constructor (e.g. ``apply_2d_horizontal_fill=True``).
    """

    model_config = ConfigDict(extra="allow")

    source: SourceSpec
    bgc_source: Optional[SourceSpec] = Field(default=None, validate_default=False)


class SurfaceForcingItem(BaseModel):
    """
    Individual surface forcing item specification.

    Parameters
    ----------
    source : SourceSpec
        Source specification for this forcing item.
    type : str
        Type of forcing: "physics", "bgc", or "restoring".
    correct_radiation : bool, optional
        Whether to correct radiation. Default is False.
    coarse_grid_mode : Optional[str], optional
        Coarse grid mode for interpolation. Default is "auto".
        Common values: "auto", "always", "never".
    restoring_forces: Optional[list], optional
        List of variables to create restoring forces data for.

    Any additional keyword arguments are passed directly to the roms-tools
    SurfaceForcing constructor.
    """

    model_config = ConfigDict(extra="allow")

    source: SourceSpec
    type: str = Field(pattern="^(physics|bgc|restoring)$")
    correct_radiation: bool = Field(default=False, validate_default=False)
    coarse_grid_mode: Optional[str] = Field(default="auto", validate_default=False)
    restoring_forces: Optional[list] = Field(default=None, validate_default=False)


class BoundaryForcingItem(BaseModel):
    """
    Individual boundary forcing item specification.

    Parameters
    ----------
    source : SourceSpec
        Source specification for this forcing item.
    type : str
        Type of forcing: "physics" or "bgc".

    Any additional keyword arguments are passed directly to the roms-tools
    BoundaryForcing constructor (e.g. ``apply_2d_horizontal_fill=True``).
    """

    model_config = ConfigDict(extra="allow")

    source: SourceSpec
    type: str = Field(pattern="^(physics|bgc)$")


class TidalForcingItem(BaseModel):
    """
    Individual tidal forcing item specification.

    Parameters
    ----------
    source : SourceSpec
        Source specification for tidal data.
    ntides : int, optional
        Number of tidal constituents. Default is None.

    Any additional keyword arguments are passed directly to the roms-tools
    TidalForcing constructor.
    """

    model_config = ConfigDict(extra="allow")

    source: SourceSpec
    ntides: Optional[int] = Field(default=None, validate_default=False)


class RiverForcingItem(BaseModel):
    """
    Individual river forcing item specification.

    Parameters
    ----------
    source : SourceSpec
        Source specification for river data. Note: climatology can be
        specified either in the source or at the top level (for backward compatibility).
    include_bgc : bool, optional
        Whether to include biogeochemical components. Default is False.

    Any additional keyword arguments are passed directly to the roms-tools
    RiverForcing constructor.
    """

    model_config = ConfigDict(extra="allow")

    source: SourceSpec
    include_bgc: bool = Field(default=False, validate_default=False)


class ForcingInput(BaseModel):
    """
    Forcing input specification containing all forcing categories.
    
    Parameters
    ----------
    surface : List[SurfaceForcingItem]
        List of surface forcing configurations.
    boundary : List[BoundaryForcingItem]
        List of boundary forcing configurations.
    tidal : Optional[List[TidalForcingItem]]
        List of tidal forcing configurations. Optional.
    river : Optional[List[RiverForcingItem]]
        List of river forcing configurations. Optional.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    surface: List[SurfaceForcingItem]
    boundary: List[BoundaryForcingItem]
    tidal: Optional[List[TidalForcingItem]] = Field(default=None, validate_default=False)
    river: Optional[List[RiverForcingItem]] = Field(default=None, validate_default=False)


class ModelInputs(BaseModel):
    """
    Top-level model inputs specification.
    
    This represents the complete inputs structure from models.yml,
    containing grid, initial_conditions, and forcing configurations.
    
    Parameters
    ----------
    grid : GridInput
        Grid input specification.
    initial_conditions : InitialConditionsInput
        Initial conditions input specification.
    forcing : ForcingInput
        Forcing input specification.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    grid: GridInput
    initial_conditions: InitialConditionsInput
    forcing: ForcingInput


class Filter(BaseModel):
    """
    Filter specification containing a list of files.
    
    Parameters
    ----------
    files : List[str]
        List of files to process.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    files: List[str]


class RunTime(BaseModel):
    """
    Run-time configuration.
    
    Parameters
    ----------
    filter : Filter
        Filter specification for run-time files to copy from the rendered opt directory
        to the run directory before executing the model (e.g., ["roms.in", "marbl_in"]).
        The first file is typically the master settings file.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    filter: Filter


class CompileTime(BaseModel):
    """
    Compile-time configuration.
    
    Parameters
    ----------
    filter : Filter
        Filter specification for compile-time files to copy from templates to the
        build opt directory during compilation (e.g., ["bgc.opt", "cppdefs.opt"]).
    """
    
    model_config = ConfigDict(extra="forbid")
    
    filter: Filter


class SettingsStage(BaseModel):
    """
    Settings stage specification (compile_time or run_time).
    
    Parameters
    ----------
    _default_config_yaml : str
        Private path to default configuration YAML file (may contain template variables).
    settings_dict : Dict[str, Any]
        Dictionary populated from the YAML file on initialization.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    _default_config_yaml: str = PrivateAttr()
    settings_dict: Dict[str, Any] = Field(default_factory=dict)
    
    def __init__(self, **data):
        """Initialize SettingsStage and load settings from YAML file."""
        # Extract _default_config_yaml path (handle both _default_config_yaml and default_config_yaml keys)
        default_config_yaml = data.pop("_default_config_yaml", data.pop("default_config_yaml", ""))
        if not default_config_yaml:
            raise ValueError("_default_config_yaml is required for SettingsStage")

        # Load settings from YAML file (path should already be resolved to absolute by caller)
        yaml_path = Path(default_config_yaml)
        if yaml_path.exists():
            with yaml_path.open('r') as f:
                settings_dict = yaml.safe_load(f) or {}
        else:
            raise FileNotFoundError(
                f"Settings YAML file not found: {default_config_yaml}"
            )

        # Update data with loaded settings
        data["settings_dict"] = settings_dict

        # Initialize the model first
        super().__init__(**data)

        # Set the private attribute after initialization
        object.__setattr__(self, "_default_config_yaml", default_config_yaml)
    
    @property
    def default_config_yaml(self) -> str:
        """Public property to access the default config YAML path."""
        return self._default_config_yaml


class PropertiesSpec(BaseModel):
    """
    Model properties specification.
    
    Parameters
    ----------
    n_tracers : int
        Number of tracers for the model configuration.
    marbl : bool
        Whether the model includes MARBL biogeochemistry.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    n_tracers: int = Field(description="Number of tracers")
    marbl: bool = Field(default=False, description="Whether the model includes MARBL biogeochemistry")


class SettingsSpec(BaseModel):
    """
    Settings specification containing compile_time and run_time stages.
    
    Parameters
    ----------
    properties : Optional[PropertiesSpec]
        Model properties specification.
    compile_time : Optional[SettingsStage]
        Compile-time settings stage specification.
    run_time : Optional[SettingsStage]
        Run-time settings stage specification.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    properties: Optional[PropertiesSpec] = None
    compile_time: Optional[SettingsStage] = None
    run_time: Optional[SettingsStage] = None


class TemplatesSpec(BaseModel):
    """
    Templates specification containing compile_time and run_time stages.
    
    Now uses CodeRepository structure for compile_time and run_time.
    
    Parameters
    ----------
    compile_time : Optional[CodeRepository]
        Compile-time template repository specification.
    run_time : Optional[CodeRepository]
        Run-time template repository specification.
    """
    
    model_config = ConfigDict(extra="forbid")
    
    compile_time: Optional[CodeRepository] = None
    run_time: Optional[CodeRepository] = None


class ModelSpec(BaseModel):
    """
    Description of an ocean model configuration (e.g., ROMS/MARBL).
    
    This is a Pydantic version of the ModelSpec dataclass from model.py.
    
    Parameters
    ----------
    name : str
        Logical name of the model (e.g., "cson_roms-marbl_v0.1").
    templates : Optional[TemplatesSpec]
        Template specifications containing compile_time and run_time stages (as CodeRepository).
    settings : SettingsSpec
        Settings specifications containing compile_time and run_time stages.
    code : ROMSCompositeCodeRepository
        Composite code repository containing roms, run_time, compile_time, and optionally marbl.
    inputs : ModelInputs
        Model inputs specification (grid, initial_conditions, forcing).
    datasets : List[str]
        SourceData dataset keys required for this model (derived from inputs
        or explicitly listed in models.yml).
    """
    
    model_config = ConfigDict(extra="forbid")
    
    name: str
    templates: Optional[TemplatesSpec] = None
    settings: SettingsSpec
    code: ROMSCompositeCodeRepository
    inputs: ModelInputs
    datasets: List[str]
    
    @model_validator(mode="after")
    def _validate_code(self) -> "ModelSpec":
        """Validate that code contains required components."""
        # Check if code is the correct type
        if not isinstance(self.code, ROMSCompositeCodeRepository):
            raise ValueError(
                f"Model spec 'code' must be a ROMSCompositeCodeRepository, "
                f"got {type(self.code).__name__}"
            )
        
        # Validate required components
        if self.code.roms is None:
            raise ValueError("Model spec must include 'roms' in code")
        if self.code.run_time is None:
            raise ValueError("Model spec must include 'run_time' in code")
        if self.code.compile_time is None:
            raise ValueError("Model spec must include 'compile_time' in code")
        return self
    
    @model_validator(mode="after")
    def _validate_settings_templates_cross_ref(self) -> "ModelSpec":
        """
        Cross-validate that template files have corresponding settings keys.
        
        Only files ending with .j2 in templates.compile_time.filter.files are validated.
        Each .j2 template file should have a corresponding key in settings.compile_time.settings_dict.
        Non-template files are skipped.
        """
        if self.templates is None or self.settings is None:
            return self
        
        # Validate compile_time
        if (self.templates.compile_time is not None and 
            self.templates.compile_time.filter is not None and
            self.settings.compile_time is not None):
            
            template_files = self.templates.compile_time.filter.files or []
            # Only validate .j2 template files - skip non-template files
            # Remove .j2 extension and extract base name for comparison with settings keys
            # e.g., "bgc.opt.j2" -> "bgc.opt" -> should match "bgc" key in settings_dict
            template_base_names = set()
            for f in template_files:
                # Only process files that end with .j2 (template files)
                if not f.endswith('.j2'):
                    continue
                # Remove .j2 extension
                base_name = f.replace('.j2', '')
                # Extract the section name (before first dot) for settings_dict key
                # e.g., "bgc.opt" -> "bgc", "cppdefs.opt" -> "cppdefs"
                if '.' in base_name:
                    section_name = base_name.split('.')[0]
                    template_base_names.add(section_name)
                else:
                    # If no dot, use the whole name
                    template_base_names.add(base_name)
            
            settings_keys = set(self.settings.compile_time.settings_dict.keys())
            
            # Check that each template file section has a corresponding settings key
            missing_keys = template_base_names - settings_keys
            if missing_keys:
                raise ValueError(
                    f"Template files with sections {sorted(missing_keys)} do not have corresponding keys "
                    f"in settings.compile_time.settings_dict. Available keys: {sorted(settings_keys)}"
                )
        
        return self
    
    @model_validator(mode="after")
    def _validate_template_files_exist(self) -> "ModelSpec":
        """
        Validate that all template files listed in filters actually exist.
        
        Checks both compile_time and run_time template files.
        """
        if self.templates is None:
            return self
        
        # Validate compile_time templates
        if (self.templates.compile_time is not None and 
            self.templates.compile_time.filter is not None and
            self.templates.compile_time.location):
            template_dir = Path(self.templates.compile_time.location)
            if template_dir.exists():
                template_files = self.templates.compile_time.filter.files or []
                missing_files = []
                for template_file in template_files:
                    template_path = template_dir / template_file
                    if not template_path.exists():
                        missing_files.append(template_file)
                
                if missing_files:
                    raise FileNotFoundError(
                        f"Template files listed in model spec do not exist in {template_dir}:\n"
                        f"  Missing files: {sorted(missing_files)}\n"
                        f"  Available files: {sorted([f.name for f in template_dir.iterdir() if f.is_file()])}"
                    )
        
        # Validate run_time templates
        if (self.templates.run_time is not None and 
            self.templates.run_time.filter is not None and
            self.templates.run_time.location):
            template_dir = Path(self.templates.run_time.location)
            if template_dir.exists():
                template_files = self.templates.run_time.filter.files or []
                missing_files = []
                for template_file in template_files:
                    template_path = template_dir / template_file
                    if not template_path.exists():
                        missing_files.append(template_file)
                
                if missing_files:
                    raise FileNotFoundError(
                        f"Template files listed in model spec do not exist in {template_dir}:\n"
                        f"  Missing files: {sorted(missing_files)}\n"
                        f"  Available files: {sorted([f.name for f in template_dir.iterdir() if f.is_file()])}"
                    )
        
        return self
    

class OpenBoundaries(BaseModel):
    """Open boundary configuration."""
    
    model_config = ConfigDict(extra="forbid")
    
    north: bool = False
    south: bool = False
    east: bool = False
    west: bool = False


def _extract_source_name(block: Any) -> Optional[str]:
    """Extract source name from a block (string, dict, or None)."""
    if block is None:
        return None
    if isinstance(block, str):
        return block
    if isinstance(block, dict):
        return block.get("name")
    return None


def _dataset_keys_from_inputs(inputs: ModelInputs) -> set[str]:
    """
    Extract dataset keys from ModelInputs configuration.
    
    Note: This function requires source_data module to be available.
    If source_data module cannot be imported, returns empty set.
    """
    # Lazy import to avoid dependency issues during testing
    try:
        from . import source_data
    except ImportError:
        # If source_data is not available, return empty set
        return set()
    
    dataset_keys: set[str] = set()
    
    def extract_from_source_spec(source_spec: SourceSpec) -> None:
        """Extract dataset key from a SourceSpec."""
        name = source_spec.name
        if not name:
            return
        try:
            if name.upper() == "GLORYS":
                layout = source_spec.glorys_layout or "regional"
                dataset_key = (
                    "GLORYS_GLOBAL" if layout == "global" else "GLORYS_REGIONAL"
                )
            else:
                dataset_key = source_data.map_source_to_dataset_key(name)
            if dataset_key in source_data.DATASET_REGISTRY:
                dataset_keys.add(dataset_key)
        except (AttributeError, ImportError) as e:
            # If source_data functions aren't available, raise ValueError
            raise ValueError(
                f"source_data module functions are not available. "
                f"Cannot map source '{name}' to dataset key. "
                f"Original error: {e}"
            ) from e
    
    # Extract from grid topography_source
    topo_name = inputs.grid.topography_source
    if topo_name:
        try:
            dataset_key = source_data.map_source_to_dataset_key(topo_name)
            if dataset_key in source_data.DATASET_REGISTRY:
                dataset_keys.add(dataset_key)
        except (AttributeError, ImportError):
            pass
    
    # Extract from initial_conditions
    extract_from_source_spec(inputs.initial_conditions.source)
    if inputs.initial_conditions.bgc_source is not None:
        extract_from_source_spec(inputs.initial_conditions.bgc_source)
    
    # Extract from forcing
    for surface_item in inputs.forcing.surface:
        extract_from_source_spec(surface_item.source)
    for boundary_item in inputs.forcing.boundary:
        extract_from_source_spec(boundary_item.source)
    if inputs.forcing.tidal is not None:
        for tidal_item in inputs.forcing.tidal:
            extract_from_source_spec(tidal_item.source)
    if inputs.forcing.river is not None:
        for river_item in inputs.forcing.river:
            extract_from_source_spec(river_item.source)
    
    return dataset_keys


def _collect_datasets(block: Dict[str, Any], inputs: ModelInputs) -> List[str]:
    """Collect dataset keys from explicit datasets list and from inputs."""
    dataset_keys: set[str] = set()
    
    # Get explicit datasets from block
    explicit = block.get("datasets") or []
    for item in explicit:
        if not item:
            continue
        dataset_keys.add(str(item).upper())
    
    # Get datasets from inputs
    dataset_keys.update(_dataset_keys_from_inputs(inputs))
    return sorted(dataset_keys)


def load_models_yaml(path: Path, model_name: str) -> ModelSpec:
    """
    Load model specification from a YAML file and return a Pydantic ModelSpec.
    
    Parameters
    ----------
    path : Path
        Path to the models.yaml file.
    model_name : str
        Name of the model block to load (e.g., "cson_roms-marbl_v0.1").
    
    Returns
    -------
    ModelSpec
        Parsed Pydantic model specification including repository metadata and
        per-input defaults.
    
    Raises
    ------
    KeyError
        If the requested model is not present in the YAML file.
    ValueError
        If required fields are missing from the model specification.
    """
    with path.open() as f:
        data = yaml.safe_load(f) or {}
    
    _SINGLE_MODEL_KEYS = frozenset({"templates", "settings", "code", "inputs"})
    if model_name in data:
        block = data[model_name]  # multi-model file (e.g., models.yml)
    elif _SINGLE_MODEL_KEYS.intersection(data.keys()):
        block = data  # single-model file: content at top level, filename is model name
    else:
        raise KeyError(f"Model '{model_name}' not found in models YAML file: {path}")
    
    # Parse code
    if "code" not in block:
        raise ValueError(f"Model '{model_name}' must specify 'code' in models.yml")
    
    code: Dict[str, CodeRepository] = {}
    for key, val in block["code"].items():
        # CodeRepository requires exactly one of commit or branch (not both, and at least one)
        commit = val.get("commit")
        branch = val.get("branch")
        
        # Create CodeRepository instance
        repo_kwargs = {
            "location": val["location"],
            "filter": None,  # PathFilter can be added later if needed
        }
        
        # Set commit or branch - CodeRepository validation will ensure exactly one is provided
        if commit is not None and commit:
            repo_kwargs["commit"] = str(commit)
        elif branch is not None and branch:
            repo_kwargs["branch"] = branch
        else:
            # Neither provided - default to "main" branch for backward compatibility
            repo_kwargs["branch"] = "main"
        
        code[key] = CodeRepository(**repo_kwargs)
    
    # Parse inputs - convert to ModelInputs
    inputs_dict = block.get("inputs", {}) or {}
    model_inputs = ModelInputs(**inputs_dict)
    
    # Collect datasets
    datasets = _collect_datasets(block, model_inputs)
    
    # Helper function to resolve relative paths against the model directory
    model_dir = path.parent

    def resolve_relative_path(path_str: str, model_dir: Path) -> str:
        """Resolve a path string relative to the model directory, or return as-is if absolute."""
        if not path_str:
            return path_str
        p = Path(path_str)
        if p.is_absolute():
            return path_str
        return str(model_dir / p)

    # Parse templates (optional) as TemplatesSpec - now uses CodeRepository structure
    templates_spec: Optional[TemplatesSpec] = None
    if "templates" in block:
        templates_dict = block["templates"]
        compile_time_repo = None
        run_time_repo = None

        # Parse compile_time as CodeRepository
        if "compile_time" in templates_dict:
            compile_time_dict = templates_dict["compile_time"]
            compile_time_filter = None
            if "filter" in compile_time_dict and compile_time_dict["filter"]:
                filter_files = compile_time_dict["filter"].get("files", [])
                if filter_files:
                    compile_time_filter = models.PathFilter(files=filter_files)

            # Resolve relative path in location
            location = resolve_relative_path(
                compile_time_dict.get("location", ""),
                model_dir
            )

            # Create CodeRepository for compile_time
            compile_time_repo_kwargs = {
                "location": location,
                "filter": compile_time_filter,
                "branch": "main",  # Default branch
            }
            if "branch" in compile_time_dict:
                compile_time_repo_kwargs["branch"] = compile_time_dict["branch"]
            elif "commit" in compile_time_dict:
                compile_time_repo_kwargs["commit"] = str(compile_time_dict["commit"])
                compile_time_repo_kwargs.pop("branch")

            compile_time_repo = CodeRepository(**compile_time_repo_kwargs)

        # Parse run_time as CodeRepository
        if "run_time" in templates_dict:
            run_time_dict = templates_dict["run_time"]
            run_time_filter = None
            if "filter" in run_time_dict and run_time_dict["filter"]:
                filter_files = run_time_dict["filter"].get("files", [])
                if filter_files:
                    run_time_filter = models.PathFilter(files=filter_files)

            # Resolve relative path in location
            location = resolve_relative_path(
                run_time_dict.get("location", ""),
                model_dir
            )

            # Create CodeRepository for run_time
            run_time_repo_kwargs = {
                "location": location,
                "filter": run_time_filter,
                "branch": "main",  # Default branch
            }
            if "branch" in run_time_dict:
                run_time_repo_kwargs["branch"] = run_time_dict["branch"]
            elif "commit" in run_time_dict:
                run_time_repo_kwargs["commit"] = str(run_time_dict["commit"])
                run_time_repo_kwargs.pop("branch")

            run_time_repo = CodeRepository(**run_time_repo_kwargs)

        if compile_time_repo or run_time_repo:
            templates_spec = TemplatesSpec(
                compile_time=compile_time_repo,
                run_time=run_time_repo
            )

    # Parse settings as SettingsSpec (required; build empty if missing)
    settings_spec: SettingsSpec
    if "settings" in block:
        settings_dict = block["settings"]
        properties_spec = None
        compile_time_settings = None
        run_time_settings = None

        # Parse properties
        if "properties" in settings_dict:
            properties_dict = settings_dict["properties"]
            properties_spec = PropertiesSpec(**properties_dict)

        # Parse compile_time settings
        if "compile_time" in settings_dict:
            compile_time_settings_dict = settings_dict["compile_time"]
            # Resolve relative path in _default_config_yaml
            default_config_yaml = resolve_relative_path(
                compile_time_settings_dict.get("_default_config_yaml", ""),
                model_dir
            )

            compile_time_settings = SettingsStage(
                _default_config_yaml=default_config_yaml
            )

        # Parse run_time settings
        if "run_time" in settings_dict:
            run_time_settings_dict = settings_dict["run_time"]
            # Resolve relative path in _default_config_yaml
            default_config_yaml = resolve_relative_path(
                run_time_settings_dict.get("_default_config_yaml", ""),
                model_dir
            )

            run_time_settings = SettingsStage(
                _default_config_yaml=default_config_yaml
            )

        if properties_spec or compile_time_settings or run_time_settings:
            settings_spec = SettingsSpec(
                properties=properties_spec,
                compile_time=compile_time_settings,
                run_time=run_time_settings
            )
        else:
            settings_spec = SettingsSpec()
    else:
        settings_spec = SettingsSpec()
    
    # Create placeholder CodeRepository objects for run_time and compile_time
    # These will be populated with actual files and locations during build()
    # The filter files will be determined from what was actually rendered
    run_time_repo_kwargs = {
        "location": "placeholder://run_time",
        "filter": None,  # Will be populated during build() from rendered files
        "branch": "main",  # Default branch
    }
    code["run_time"] = CodeRepository(**run_time_repo_kwargs)
    
    compile_time_repo_kwargs = {
        "location": "placeholder://compile_time",
        "filter": None,  # Will be populated during build() from rendered files
        "branch": "main",  # Default branch
    }
    code["compile_time"] = CodeRepository(**compile_time_repo_kwargs)
    
    # Construct ROMSCompositeCodeRepository from code dictionary
    roms_code = code.get("roms")
    run_time_code = code.get("run_time")
    compile_time_code = code.get("compile_time")
    marbl_code = code.get("marbl")
    
    if not roms_code or not run_time_code or not compile_time_code:
        raise ValueError(
            f"Model '{model_name}' must include 'roms', 'run_time', and 'compile_time' in code"
        )
    
    code_repo = ROMSCompositeCodeRepository(
        roms=roms_code,
        run_time=run_time_code,
        compile_time=compile_time_code,
        marbl=marbl_code if marbl_code else None,
    )
   
     
    return ModelSpec(
        name=model_name,
        templates=templates_spec,
        settings=settings_spec,
        code=code_repo,
        inputs=model_inputs,
        datasets=datasets,
    )

