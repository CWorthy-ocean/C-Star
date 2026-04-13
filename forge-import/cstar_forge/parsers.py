"""Helpers for parsing YAML and ROMS-Tools inputs."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import BaseModel, Field

try:
    import cartopy.crs as ccrs
    CARTOPY_AVAILABLE = True
except ImportError:
    CARTOPY_AVAILABLE = False
    ccrs = None



class NotebookConfig(BaseModel):
    """Schema for a notebook configuration."""

    parameters: Dict[str, Any]
    output_path: Union[Path, str]


class NotebookEntry(BaseModel):
    """Schema for a named notebook entry."""

    notebook_name: str
    config: NotebookConfig


class NotebookSection(BaseModel):
    """Schema for a titled notebook section."""

    title: str
    name: Optional[str] = None
    description: Optional[str] = None
    children: list[NotebookEntry]
    use_dask_cluster: bool = False

    def to_toc_entry(self, base_dir: Optional[Path] = None) -> Dict[str, Any]:
        children = []
        for entry in self.children:
            output_path = Path(entry.config.output_path)
            if base_dir is not None and output_path.is_absolute():
                try:
                    output_path = output_path.relative_to(base_dir)
                except ValueError:
                    pass
            children.append({"file": str(output_path)})
        return {"title": self.title, "children": children}


class NotebookList(BaseModel):
    """Schema for a list of notebook sections."""

    sections: list[NotebookSection]

    def iter_entries(self):
        for section in self.sections:
            for entry in section.children:
                yield entry

    def to_toc_entries(self, base_dir: Optional[Path] = None) -> list[Dict[str, Any]]:
        return [section.to_toc_entry(base_dir=base_dir) for section in self.sections]

class DaskClusterKwargs(BaseModel):
    """Schema for dask_cluster_kwargs configuration."""

    account: Optional[str] = None
    queue_name: Optional[str] = None
    n_nodes: Optional[int] = None
    n_tasks_per_node: Optional[int] = None
    wallclock: Optional[str] = None
    scheduler_file: Optional[str] = None


class ProjectionConfig(BaseModel):
    """Schema for cartopy projection configuration."""

    type: str = Field(..., description="Projection type (e.g., 'LambertConformal')")
    kwargs: Dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the projection constructor")
    
    @classmethod
    def model_validate(cls, obj: Any):
        """Override to ensure kwargs is properly handled."""
        if isinstance(obj, dict):
            # Ensure kwargs exists and is a dict
            if "kwargs" not in obj:
                obj["kwargs"] = {}
            elif obj.get("kwargs") is None:
                obj["kwargs"] = {}
        return super().model_validate(obj)


class DomainVisualizationSettings(BaseModel):
    """Schema for domain-specific visualization settings."""

    projection: ProjectionConfig = Field(..., description="Cartopy projection configuration")
    
    @property
    def projection_kwargs(self) -> Dict[str, Any]:
        """Return projection keyword arguments as a dictionary for use with cartopy."""
        return self.projection.kwargs
    
    @property
    def projection_object(self) -> Any:
        """Return instantiated cartopy projection object."""
        if not CARTOPY_AVAILABLE:
            raise ImportError("cartopy is required to instantiate projection objects")
        
        projection_type = self.projection.type
        if not hasattr(ccrs, projection_type):
            raise ValueError(f"Unknown projection type: {projection_type}. Available types: {[attr for attr in dir(ccrs) if not attr.startswith('_') and isinstance(getattr(ccrs, attr), type)]}")
        
        ccrs_proj_func = getattr(ccrs, projection_type)
        return ccrs_proj_func(**self.projection.kwargs)


class VariableVisualizationSettings(BaseModel):
    """Schema for variable-specific visualization settings."""

    cmap: str = Field(..., description="Colormap name (e.g., 'curl')")
    dc: float = Field(..., description="Colorbar step size")
    cmin: float = Field(..., description="Minimum value for colorbar")
    cmax: float = Field(..., description="Maximum value for colorbar")


class VisualizationSettings(BaseModel):
    """Schema for visualization settings configuration."""

    domains: Dict[str, DomainVisualizationSettings] = Field(
        ..., description="Dictionary mapping domain names to their visualization settings"
    )
    variables: Dict[str, VariableVisualizationSettings] = Field(
        ..., description="Dictionary mapping variable names to their visualization settings"
    )
    
    def get_domain_settings(self, domain_name: str) -> DomainVisualizationSettings:
        """Get visualization settings for a specific domain."""
        if domain_name not in self.domains:
            raise ValueError(f"Domain '{domain_name}' not found in settings. Available domains: {list(self.domains.keys())}")
        return self.domains[domain_name]


class AppConfig(BaseModel):
    """Top-level API schema for parameters.yml."""

    dask_cluster_kwargs: Optional[DaskClusterKwargs] = None
    notebook_list: NotebookList


def _parse_notebook_entry_list(raw_entries: list[Any], base_dir: Path) -> list[NotebookEntry]:
    entries = []
    for item in raw_entries:
        if not isinstance(item, dict):
            raise ValueError("Each notebook entry must be a single-key mapping.")
        if len(item) == 1:
            notebook_name, payload = next(iter(item.items()))
            if not isinstance(payload, dict):
                raise ValueError("Notebook entry payload must be a mapping.")
        else:
            notebook_keys = [key for key in item.keys() if isinstance(key, str) and key.endswith(".ipynb")]
            if len(notebook_keys) != 1:
                raise ValueError("Each notebook entry must be a single-key mapping.")
            notebook_name = notebook_keys[0]
            payload = {
                "parameters": item.get("parameters", {}),
                "output_path": item.get("output_path"),
            }
        parameters = dict(payload.get("parameters", {}))
        grid_yaml = parameters.get("grid_yaml")
        if isinstance(grid_yaml, str):
            grid_path = Path(grid_yaml)
            if not grid_path.is_absolute():
                parameters["grid_yaml"] = str(base_dir / grid_path)
        output_path = payload.get("output_path")
        if isinstance(output_path, str):
            output_path_value = Path(output_path)
            if not output_path_value.is_absolute():
                output_path = str(base_dir / output_path_value)
        config = NotebookConfig(
            parameters=parameters,
            output_path=output_path,
        )
        entries.append(NotebookEntry(notebook_name=notebook_name, config=config))
    return entries


def _parse_notebook_entries(raw_entries: Any, base_dir: Path) -> NotebookList:
    if isinstance(raw_entries, dict):
        title = raw_entries.get("title", "Untitled")
        children = raw_entries.get("children") or raw_entries.get("notebooks")
        if not isinstance(children, list):
            raise ValueError("children must be a list of entries.")
        sections = [NotebookSection(title=title, children=_parse_notebook_entry_list(children, base_dir))]
        return NotebookList(sections=sections)

    if isinstance(raw_entries, list):
        if raw_entries and all(isinstance(item, dict) and "children" in item for item in raw_entries):
            sections = []
            for section in raw_entries:
                title = section.get("title", "Untitled")
                name = section.get("name")
                description = section.get("description")
                children = section.get("children")
                if not isinstance(children, list):
                    raise ValueError("children must be a list of entries.")
                use_dask_cluster = bool(section.get("use_dask_cluster", False))
                sections.append(
                    NotebookSection(
                        title=title,
                        name=name,
                        description=description,
                        children=_parse_notebook_entry_list(children, base_dir),
                        use_dask_cluster=use_dask_cluster,
                    )
                )
            return NotebookList(sections=sections)
        # Fall back to a single untitled section
        return NotebookList(
            sections=[
                NotebookSection(
                    title="Untitled",
                    children=_parse_notebook_entry_list(raw_entries, base_dir),
                )
            ]
        )

    raise ValueError("notebooks must be a list of sections.")


def load_yaml_params(path: Optional[Union[Path, str]]) -> Dict[str, Any]:
    """Load parameters from one or more YAML documents."""
    if path is None:
        return {}
    path_obj = Path(path)
    with path_obj.open("r", encoding="utf-8") as handle:
        docs = [doc for doc in yaml.safe_load_all(handle) if doc]
    merged: Dict[str, Any] = {}
    for doc in docs:
        if not isinstance(doc, dict):
            raise ValueError("YAML documents must be mappings.")
        merged.update(doc)
    return merged


def normalize_file_type(file_type: Optional[str]) -> Optional[str]:
    """Normalize a file type string."""
    if file_type is None:
        return None
    normalized = file_type.replace("_", "-").lower()
    if normalized not in {"roms-tools", "app-config"}:
        raise ValueError("Supported file types are 'roms-tools', 'roms_tools', or 'app-config'.")
    return normalized


def _select_roms_tools_class_name(yaml_params: Dict[str, Any]) -> str:
    """Determine the roms_tools class name to use based on YAML keys."""
    if "Grid" not in yaml_params:
        raise ValueError("ROMS-Tools YAML must include a 'Grid' section.")
    other_keys = sorted(
        key for key in yaml_params.keys() if key not in {"Grid", "roms_tools_version"}
    )
    if not other_keys:
        return "Grid"
    if len(other_keys) > 1:
        raise ValueError("ROMS-Tools YAML must include only one non-Grid section.")
    return other_keys[0]



def load_app_config(path: Union[Path, str]) -> AppConfig:
    """Load parameters.yml into an AppConfig object."""
    path_obj = Path(path)
    raw = load_yaml_params(path_obj)
    dask_kwargs = raw.get("dask_cluster_kwargs")
    if "notebooks" not in raw:
        raise ValueError("notebooks must be a list of entries.")
    notebooks_raw = raw.get("notebooks")
    notebook_list = _parse_notebook_entries(
        notebooks_raw,
        base_dir=path_obj.parent.resolve(),
    )
    return AppConfig(
        dask_cluster_kwargs=DaskClusterKwargs(**dask_kwargs) if dask_kwargs else None,
        notebook_list=notebook_list,
    )


def load_roms_tools_object(
    yaml_path: Union[Path, str],
    roms_tools_module: Any = None,
) -> Any:
    """Load a roms_tools object via its from_yaml method."""
    module = roms_tools_module
    if module is None:
        try:
            import roms_tools  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised via explicit error path
            raise RuntimeError("roms_tools is required to load this YAML file.") from exc
        module = roms_tools

    yaml_path_obj = Path(yaml_path)
    yaml_params = load_yaml_params(yaml_path_obj)
    class_name = _select_roms_tools_class_name(yaml_params)

    if not hasattr(module, class_name):
        raise ValueError(f"roms_tools has no attribute '{class_name}'.")

    cls = getattr(module, class_name)

    if not hasattr(cls, "from_yaml"):
        raise ValueError(f"roms_tools.{class_name} has no from_yaml method.")
    return cls.from_yaml(str(yaml_path_obj))


def load_visualization_settings(path: Union[Path, str]) -> VisualizationSettings:
    """
    Load visualization settings from a YAML file.
    
    Parameters
    ----------
    path : Path or str
        Path to the visualization settings YAML file.
        
    Returns
    -------
    VisualizationSettings
        Visualization settings containing domains and variables configuration.
    """
    path_obj = Path(path)
    raw = load_yaml_params(path_obj)
    
    if not isinstance(raw, dict):
        raise ValueError("Visualization settings must be a dictionary.")
    
    # Parse domains
    domains_raw = raw.get("domains", {})
    if not isinstance(domains_raw, dict):
        raise ValueError("'domains' must be a dictionary mapping domain names to settings.")
    
    domains: Dict[str, DomainVisualizationSettings] = {}
    for domain_name, domain_config in domains_raw.items():
        if not isinstance(domain_config, dict):
            raise ValueError(f"Settings for domain '{domain_name}' must be a dictionary.")
        # Debug: Check if projection kwargs are in the raw data
        if "projection" in domain_config:
            projection_raw = domain_config["projection"]
            if isinstance(projection_raw, dict) and "kwargs" in projection_raw:
                # Ensure kwargs is a dict (not None or empty)
                if projection_raw["kwargs"] is None:
                    projection_raw["kwargs"] = {}
        domains[domain_name] = DomainVisualizationSettings.model_validate(domain_config)
    
    # Parse variables
    variables_raw = raw.get("variables", {})
    if not isinstance(variables_raw, dict):
        raise ValueError("'variables' must be a dictionary mapping variable names to settings.")
    
    variables: Dict[str, VariableVisualizationSettings] = {}
    for variable_name, variable_config in variables_raw.items():
        if not isinstance(variable_config, dict):
            raise ValueError(f"Settings for variable '{variable_name}' must be a dictionary.")
        variables[variable_name] = VariableVisualizationSettings.model_validate(variable_config)
    
    return VisualizationSettings(domains=domains, variables=variables)


def parse_slurm_job_id(file_path: Union[str, Path]) -> Optional[str]:
    """
    Search for SLURM Job ID line in a file and return the job ID.
    
    Parameters
    ----------
    file_path : str or Path
        Path to the log file to search
        
    Returns
    -------
    str or None
        The SLURM job ID if found, None otherwise
        
    Examples
    --------
    >>> from pathlib import Path
    >>> job_id = parse_slurm_job_id("logfile.out")
    >>> print(f"SLURM Job ID: {job_id}")
    """
    file_path = Path(file_path)
    if not file_path.exists():
        return None
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    for line in lines:
        if "SLURM Job ID:" in line:
            # Extract job ID using regex
            match = re.search(r'SLURM Job ID:\s*(\d+)', line)
            if match:
                return match.group(1)
    
    return None


def _tres_count(tres_list: list[dict[str, Any]] | None, tres_type: str) -> Optional[int]:
    """Return integer count for a given TRES type (e.g., 'cpu', 'mem') from a Slurm TRES list."""
    for item in (tres_list or []):
        if item.get("type") == tres_type:
            try:
                return int(item.get("count"))
            except Exception:
                return None
    return None


def _job_is_completed(job: Dict[str, Any]) -> bool:
    state = job.get("state") or {}
    current = state.get("current") or []
    # current is typically a list like ["COMPLETED"]
    return "COMPLETED" in current


def sacct_summary(jobid: int | str, *, model_step_name: str = "roms") -> Dict[str, Any]:
    """
    Return a minimal, extensible summary of a Slurm job from `sacct --json`.

    Returned fields (top-level):
      - elapsed_time: int seconds (model step elapsed)
      - ntasks: int (model step tasks.count)
      - allocated_cores: int (model step allocated cpu TRES)
      - allocated_core_hours: float | None (only if COMPLETED)
      - ntasks_core_hours: float | None (only if COMPLETED)
      - memory_* fields (allocated + max/avg, if available in JSON)
      - sacct_json: full raw JSON payload (for extensibility/debug)

    Notes:
      - Core-hours are only returned if the *job* status is COMPLETED.
      - The summary focuses on the model step (default: step name "roms").
    """
    jobid = str(jobid)
    cmd = ["sacct", "--json", "-j", jobid]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"sacct --json failed (rc={proc.returncode}) for jobid={jobid}\nSTDERR:\n{proc.stderr.strip()}"
        )

    payload = json.loads(proc.stdout)
    jobs = payload.get("jobs") or []
    if not jobs:
        raise RuntimeError(f"No jobs returned by sacct for jobid={jobid}")

    # Prefer exact job_id match; otherwise first entry.
    job: Dict[str, Any] = next((j for j in jobs if str(j.get("job_id")) == jobid), jobs[0])

    completed = _job_is_completed(job)

    # Identify the model step.
    steps = job.get("steps") or []
    model_step: Optional[Dict[str, Any]] = next(
        (s for s in steps if (s.get("step") or {}).get("name") == model_step_name),
        None,
    )
    # Fallback: pick ".0" if present
    if model_step is None:
        model_step = next(
            (s for s in steps if isinstance((s.get("step") or {}).get("id"), str) and (s.get("step") or {}).get("id", "").endswith(".0")),
            None,
        )

    if model_step is None:
        raise RuntimeError(f"Could not find model step {model_step_name!r} (or a .0 fallback) for jobid={jobid}")

    # Extract core accounting from the model step.
    step_time = model_step.get("time") or {}
    elapsed_seconds = step_time.get("elapsed")
    elapsed_seconds = int(elapsed_seconds) if elapsed_seconds is not None else None

    ntasks = (model_step.get("tasks") or {}).get("count")
    ntasks = int(ntasks) if ntasks is not None else None

    tres = model_step.get("tres") or {}
    allocated_cores = _tres_count(tres.get("allocated"), "cpu")

    # Memory: TRES units vary by site/version. We return raw counts and defer unit interpretation.
    # - allocated mem: from tres.allocated where type == "mem"
    # - requested max/avg mem: from tres.requested.average/max where type == "mem"
    # - max/avg observed memory sometimes appears under "tres.requested.max/min" (already in your sample).
    allocated_mem = _tres_count(tres.get("allocated"), "mem")
    requested_mem_avg = _tres_count((tres.get("requested") or {}).get("average"), "mem") if isinstance(tres.get("requested"), dict) else None
    requested_mem_max = _tres_count((tres.get("requested") or {}).get("max"), "mem") if isinstance(tres.get("requested"), dict) else None
    requested_mem_min = _tres_count((tres.get("requested") or {}).get("min"), "mem") if isinstance(tres.get("requested"), dict) else None

    # Compute core-hours only if job is COMPLETED.
    allocated_core_hours = None
    ntasks_core_hours = None
    if completed and elapsed_seconds is not None:
        if allocated_cores is not None:
            allocated_core_hours = (allocated_cores * elapsed_seconds) / 3600.0
        if ntasks is not None:
            ntasks_core_hours = (ntasks * elapsed_seconds) / 3600.0

    summary: Dict[str, Any] = {
        "jobid": jobid,
        "status": (job.get("state") or {}).get("current"),
        "elapsed_time": elapsed_seconds,
        "ntasks": ntasks,
        "allocated_cores": allocated_cores,
        "allocated_core_hours": allocated_core_hours,
        "ntasks_core_hours": ntasks_core_hours,
        # memory-related fields (raw counts; unit interpretation can be added later)
        "allocated_mem": allocated_mem,
        "requested_mem_avg": requested_mem_avg,
        "requested_mem_max": requested_mem_max,
        "requested_mem_min": requested_mem_min,
        # extensibility/debug
        "sacct_json": payload,
    }
    return summary
