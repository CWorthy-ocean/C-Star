"""
ForgeExecutor - Pydantic-based builder for C-Star blueprints.

This class provides a Pydantic-based interface for building RomsMarblBlueprint objects.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
import shutil
import sys
import warnings
from contextlib import contextmanager
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
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    model_validator,
)

from cstar_forge.forge import input_data, source_data
from cstar_forge.forge.forge_blueprint import (
    DEFAULT_WORKING_ROOT,
    ROMS_RUN_SEGMENT,
    OpenBoundaries,
)
from cstar_forge.forge.host import HostPaths
from cstar_forge.forge.namelist_model import ensure_cdr_output_marbl_diagnostics
from cstar_forge.forge.settings import render_roms_settings, write_roms_namelist
from cstar_forge.utils import mem_log

log = logging.getLogger(__name__)


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


@contextmanager
def _suppress_pydantic_warnings():
    """Suppress the Pydantic UserWarnings expected when reading/serializing a
    blueprint built with ``model_construct`` (placeholder/partial values don't
    match the declared field types).
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
        warnings.filterwarnings("ignore", category=UserWarning, module="pydantic.main")
        warnings.filterwarnings("ignore", message=".*Pydantic.*", category=UserWarning)
        warnings.filterwarnings(
            "ignore", message=".*serializer.*", category=UserWarning
        )
        yield


class ForgeExecutor(BaseModel):
    """
    Builder for C-Star RomsMarblBlueprint specifications.

    This class provides a Pydantic-based interface for constructing and
    managing a ROMS-MARBL blueprint. It builds up the blueprint in memory
    across three steps, and persists it to disk exactly once, at the end:

    1. **Initialization** (`model_post_init()` / `_initialize_roms_marbl_blueprint()`):
       - Blueprint structure initialized in memory with placeholder data
       - Settings dictionaries initialized from model defaults

    2. **Input generation** (`generate_inputs()`):
       - Source data prepared, input files generated (grid, initial conditions, forcing)
       - In-memory blueprint updated with actual data file locations
       - In-memory settings updated with input-specific values

    3. **Build configuration** (`configure_build()`):
       - Jinja2 templates rendered with current settings
       - Blueprint updated with rendered code locations
       - **Blueprint persisted to disk** -- the only time this happens

    `run()` then hands the persisted blueprint's path to C-Star for execution.

    **Settings:**

    The compile-time (``cppdefs``) and run-time (namelist) settings are seeded from the
    resolved ``resolved_settings`` (the ForgeBlueprint ``model_settings``); ``configure_build``
    can overlay further overrides on top.

    **Key Concepts:**

    - Settings are stored in a sidecar YAML file (not in blueprint itself)
    - The blueprint is persisted to disk exactly once, in `configure_build()`
    - Grid object is created during initialization and reused throughout
    - Source data can be prepared independently via `ensure_source_data()`
    - All produced artifacts (inputs, blueprint, build, run output) live under the
      injected ``host.working_dir``.

    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # User inputs
    description: str = "Generated blueprint"
    name: str  # the blueprint's canonical name (ForgeBlueprint.name, stored)
    grid_name: str  # results-affecting: SourceData keys its cache filenames off it
    grid_kwargs: dict[str, Any]
    grid_kwargs_parent: dict[str, Any] | None = Field(
        default=None, validate_default=False
    )
    grid_kwargs_child: dict[str, Any] | None = Field(
        default=None, validate_default=False
    )
    topography_source: str = Field(
        default="ETOPO5",
        description=(
            "Topography data-source name (from ForgeBlueprint ``domain.topography_source``). "
            "``ETOPO5`` uses roms-tools' built-in fetch at grid build; any other supported "
            "source (e.g. ``SRTM15``) is staged by Forge and injected into every "
            "``grid_kwargs`` as ``{'name', 'path'}`` before the grid is constructed."
        ),
    )
    topography_path: str | None = Field(
        default=None,
        description=(
            "Explicit path to a custom topography file (from ForgeBlueprint "
            "``domain.topography_path``). When set, it is used verbatim instead of "
            "staging/fetching, for any source name."
        ),
    )
    open_boundaries: OpenBoundaries
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
            "categories. Set by process_forge_blueprint when cfg.sources contains an authored "
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
    verbose: bool = Field(
        default=False,
        exclude=True,
        description=(
            "Runtime diagnostic flag (not results-affecting, never serialized): enables "
            "timestamped executor logging, verbose=True on the roms-tools calls that "
            "support it (Grid, align_grids, make_nesting_info), and timing/memory "
            "instrumentation around every roms-tools constructor and .save()."
        ),
    )
    source_dataset_keys: list[str] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Resolved source-dataset keys to prepare (from the ForgeBlueprint ``datasets``). "
            "Distinct from the ``datasets`` property, which returns the *loaded* datasets."
        ),
    )
    resolved_datasets: dict[str, dict] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Snapshot of ForgeBlueprint.forcing.resolved_datasets (logical name -> "
            "{dataset_key, dataset_id, url, streamable}). Authoritative for key/"
            "streamable resolution at processing time (fed into SourceData); "
            "source_registry is the fallback for names not in the snapshot."
        ),
    )
    resolved_settings: dict[str, Any] | None = Field(
        default=None,
        validate_default=False,
        description=(
            "Fully-resolved, host-independent settings (the ForgeBlueprint ``model_settings``): "
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
            "The ForgeBlueprint ``code`` (roms + marbl repos and compile-/run-time template "
            "refs). The blueprint code repository and the template render directories are "
            "built from it. Required (fed by from_forge_blueprint)."
        ),
    )
    # Internal attributes (computed/loaded)
    roms_marbl_blueprint: cstar_models.RomsMarblBlueprint | None = Field(
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
    _inputs_generated: bool = PrivateAttr(default=False)
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
                "ForgeExecutor.from_forge_blueprint(cfg, host=...) or pass host=... directly."
            )
        return self.host

    @property
    def input_data_dir(self) -> Path:
        """Directory for generated input NetCDF files (grid, forcing, etc.)."""
        return self._require_host().working_dir / "input_data"

    def _resolve_topography_source(self) -> dict[str, str] | None:
        """Stage a non-ETOPO5 topography file and return the roms-tools
        ``topography_source`` dict (``{'name', 'path'}``), or ``None`` for ETOPO5
        (which roms-tools fetches itself at grid build). An explicit
        ``topography_path`` short-circuits this and is used verbatim for any source.

        The topo file is a plain download requiring no grid, so it can be staged
        here — before grid construction — which resolves the ordering constraint
        that topography is a prerequisite of the grid it is built into. ``name`` is
        emitted as a plain string (never the ``TopographySource`` enum) so it is
        safe to serialize when the grid is later written to YAML.
        """
        name = getattr(self.topography_source, "value", self.topography_source)
        # An explicit custom path overrides staging/fetch, regardless of source name.
        if self.topography_path:
            return {"name": name, "path": self.topography_path}
        if name == "ETOPO5":
            return None
        sd = source_data.SourceData(
            datasets=[name],
            source_data_dir=self._require_host().source_data_cache,
        )
        sd.prepare_all()
        return {"name": name, "path": str(sd.path_for_source(name))}

    def model_post_init(self, __context: Any) -> None:
        """
        Post-initialization hook called automatically after model validation.

        This method is called by Pydantic after the instance is validated and
        performs critical initialization:

        1. Creates the grid object from `grid_kwargs`
        2. Initializes the blueprint structure (calls `_initialize_roms_marbl_blueprint()`)

        After this method completes, `self.roms_marbl_blueprint` holds the initial
        in-memory structure (placeholder data, not yet persisted). Nothing is written
        to disk until `configure_build()` completes.
        """
        log.debug(
            "model_post_init: entering for %r (verbose=%s)", self.name, self.verbose
        )

        # Fail fast if no host was injected — every artifact path needs it.
        self._require_host()

        # Topography is a prerequisite of grid construction: roms-tools requires a
        # staged 'path' for any non-ETOPO5 source and silently falls back to ETOPO5
        # otherwise. Stage the topo file (a plain download, no grid needed) and inject
        # it into every grid_kwargs BEFORE the rt.Grid calls below. ETOPO5 is left
        # untouched — roms-tools fetches it itself at grid build.
        topo = self._resolve_topography_source()
        if topo is not None:
            self.grid_kwargs = {**self.grid_kwargs, "topography_source": topo}
            if self.grid_kwargs_parent is not None:
                self.grid_kwargs_parent = {
                    **self.grid_kwargs_parent,
                    "topography_source": topo,
                }
            if self.grid_kwargs_child is not None:
                self.grid_kwargs_child = {
                    **self.grid_kwargs_child,
                    "topography_source": topo,
                }

        # Create grids, 4 cases:
        # has child and no parent, has child and parent, has parent and no child, no parent no child

        # I am a parent but not a child
        if self.grid_kwargs_child is not None and self.grid_kwargs_parent is None:
            # Make both parent and child, to make the nesting data.
            grid_kwargs_child = {
                k: v for k, v in self.grid_kwargs_child.items() if k != "metadata"
            }

            with mem_log("Grid(child)", enabled=self.verbose):
                self.grid_child = rt.Grid(**grid_kwargs_child, verbose=self.verbose)
            with mem_log("Grid(self)", enabled=self.verbose):
                self.grid = rt.Grid(**self.grid_kwargs, verbose=self.verbose)
            with mem_log("align_grids(self, child)", enabled=self.verbose):
                self.grid_child = rt.align_grids(
                    self.grid, self.grid_child, verbose=self.verbose
                )

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
            with mem_log("Grid(parent)", enabled=self.verbose):
                self.grid_parent = rt.Grid(**grid_kwargs_parent, verbose=self.verbose)
            with mem_log("Grid(child)", enabled=self.verbose):
                self.grid_child = rt.Grid(**grid_kwargs_child, verbose=self.verbose)
            with mem_log("Grid(self)", enabled=self.verbose):
                self.grid = rt.Grid(**grid_kwargs, verbose=self.verbose)

            with mem_log("align_grids(parent, self)", enabled=self.verbose):
                self.grid = rt.align_grids(
                    self.grid_parent, self.grid, verbose=self.verbose
                )
            with mem_log("align_grids(self, child)", enabled=self.verbose):
                self.grid_child = rt.align_grids(
                    self.grid, self.grid_child, verbose=self.verbose
                )

            if "metadata" in self.grid_kwargs_child:
                self.metadata_child = self.grid_kwargs_child["metadata"]

        # I am a child but not a parent
        elif self.grid_kwargs_child is None and self.grid_kwargs_parent is not None:
            grid_kwargs_parent = {
                k: v for k, v in self.grid_kwargs_parent.items() if k != "metadata"
            }
            grid_kwargs = {k: v for k, v in self.grid_kwargs.items() if k != "metadata"}

            # Adapt this grid to its parent. no nesting data needed
            with mem_log("Grid(parent)", enabled=self.verbose):
                self.grid_parent = rt.Grid(**grid_kwargs_parent, verbose=self.verbose)
            with mem_log("Grid(self)", enabled=self.verbose):
                self.grid = rt.Grid(**grid_kwargs, verbose=self.verbose)

            with mem_log("align_grids(parent, self)", enabled=self.verbose):
                self.grid = rt.align_grids(
                    self.grid_parent, self.grid, verbose=self.verbose
                )
        else:
            with mem_log("Grid(self)", enabled=self.verbose):
                self.grid = rt.Grid(**self.grid_kwargs, verbose=self.verbose)

        # Initialize blueprint with basic structure
        log.debug("model_post_init: initializing blueprint structure for %r", self.name)
        self._initialize_roms_marbl_blueprint()

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
        yaml_dir = self.roms_marbl_blueprint_dir.resolve()
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
    def roms_blueprint_working_dir(self) -> Path:
        """Working dir for the emitted ROMS blueprint.

        Mirrors ``run_output_dir`` but under a ``cstar-roms-run`` root instead of
        the forge run's ``cstar-forge-run`` root, so the two stages don't share a dir.
        """
        forge_seg = Path(DEFAULT_WORKING_ROOT).name  # "cstar-forge-run"
        blueprint_seg = ROMS_RUN_SEGMENT
        run_dir = self.run_output_dir
        if forge_seg in run_dir.parts:
            return Path(
                *(blueprint_seg if p == forge_seg else p for p in run_dir.parts)
            )
        return run_dir / blueprint_seg

    @property
    def default_runtime_params(self) -> cstar_models.RuntimeParameterSet:
        """
        Get default runtime parameters.

        Returns a RuntimeParameterSet with default values based on the builder's
        configuration (start_date, end_date). The run output location is NOT
        carried here: ``runtime_params.output_dir`` is a pre-2.0.0 blueprint
        field (C-Star migrates it to the blueprint ``working_dir``, which the
        executor sets explicitly in ``configure_build``).
        """
        return cstar_models.RuntimeParameterSet(
            start_date=self.start_date,
            end_date=self.end_date,
        )

    @property
    def roms_marbl_blueprint_dir(self) -> Path:
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

    @property
    def _use_pio(self) -> bool:
        """Whether ROMS is built against ParallelIO, read from compile-time cppdefs."""
        return bool(
            (self._settings_compile_time.get("cppdefs") or {}).get("use_pio", False)
        )

    def _validated_roms_marbl_blueprint(self) -> cstar_models.RomsMarblBlueprint:
        """Re-validate the assembled blueprint against the installed C-Star models.

        The blueprint is assembled with ``model_construct`` because the
        pre-generation stages are deliberately partial (placeholder resources;
        ``model_params``/``runtime_params`` live in the sidecar until
        ``configure_build``), so nothing checks it against the cstar models --
        which are ``extra="forbid"`` -- until C-Star loads the persisted file.
        Round-tripping the exact serialized form (the same ``model_dump`` that
        ``persist`` writes, minus the ``$schema`` key that ``deserialize``
        strips) through ``model_validate`` fails at emit time instead, pointing
        at the offending field. Returns the validated instance so the executor
        holds -- and persists -- a fully validated blueprint from here on.

        Only meaningful once ``generate_inputs`` has filled in real data
        (``_inputs_generated``); the placeholder blueprint cannot validate.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*Pydantic.*", category=UserWarning
            )
            warnings.filterwarnings(
                "ignore", message=".*serialization.*", category=UserWarning
            )
            data = self.roms_marbl_blueprint.model_dump(mode="json", exclude_none=True)
        data.pop("$schema", None)
        try:
            return cstar_models.RomsMarblBlueprint.model_validate(data)
        except ValidationError as exc:
            msg = (
                "The emitted ROMS-MARBL blueprint does not validate against the "
                f"installed C-Star models ({cstar_models.__file__}); C-Star would "
                "reject it at load time. Most likely Forge emits a field this "
                "C-Star version does not declare (its models are extra='forbid'), "
                f"or a value fails a cstar validator:\n{exc}"
            )
            raise ValueError(msg) from exc

    def persist(self) -> None:
        """
        Persist the current blueprint to a YAML file.

        Saves the blueprint to disk (overwriting any previous save for this
        executor). Also saves settings to a sidecar file.

        **File Structure:**

        - Blueprint: `B_{name}.yaml`
        - Settings: `settings_B_{name}.yaml` (sidecar file)

        The settings are stored separately from the blueprint to avoid
        cluttering the blueprint with configuration details.

        **Notes:**

        - The directory is created if it doesn't exist
        - Serialization warnings are suppressed (expected for placeholder values)
        - Path objects are converted to strings for YAML compatibility

        Raises
        ------
        ValueError
            If blueprint is not initialized.
        """
        if self.roms_marbl_blueprint is None:
            raise ValueError("Cannot persist: blueprint is not initialized")

        # Get the file path using path_roms_marbl_blueprint
        bp_path = self.path_roms_marbl_blueprint()

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
            roms_marbl_blueprint_dict = self.roms_marbl_blueprint.model_dump(
                mode="json", exclude_none=True
            )

        # The Blueprint serializer injects a "$schema" ref into every dump, but
        # the canonical C-Star file format carries it as a yaml-language-server
        # comment, not a document key (cstar's model_to_yaml pops it the same
        # way). A "$schema" key would also be rejected as an extra field by any
        # C-Star deserializer that doesn't strip it before validating.
        schema_url = str(roms_marbl_blueprint_dict.pop("$schema", "") or "")

        with bp_path.open("w") as f:
            if schema_url:
                f.write(f"# yaml-language-server: $schema={schema_url}\n")
            yaml.safe_dump(
                roms_marbl_blueprint_dict, f, default_flow_style=False, sort_keys=False
            )

        # Write settings to sidecar file
        self._persist_settings()

    def _path_settings_file(self) -> Path:
        """
        Return the path to this executor's settings sidecar file.

        The settings file has the same name as the blueprint file, with "settings_"
        prepended. For example: "B_model.yaml" -> "settings_B_model.yaml"
        """
        bp_path = self.path_roms_marbl_blueprint()
        return bp_path.parent / f"settings_{bp_path.name}"

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

    def _persist_settings(self) -> None:
        """
        Persist settings dictionaries to this executor's sidecar file.

        Writes compile_time and run_time settings to a YAML file with the same
        name as the blueprint file, prepended with "settings_".
        """
        settings_path = self._path_settings_file()

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

    def _load_settings_from_file(self) -> None:
        """
        Load settings dictionaries from this executor's sidecar file.

        Reads compile_time and run_time settings from a YAML file with the same
        name as the blueprint file, prepended with "settings_".

        If the settings file doesn't exist, leaves settings dictionaries unchanged.
        """
        settings_path = self._path_settings_file()

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
        except (OSError, yaml.YAMLError) as e:
            # If loading fails, issue a warning but don't fail
            log.warning(
                "Failed to load settings from %s: %s", settings_path, e, exc_info=True
            )
            warnings.warn(
                f"Failed to load settings from {settings_path}: {type(e).__name__}: {e}",
                UserWarning,
                stacklevel=2,
            )

    def path_roms_marbl_blueprint(self) -> Path:
        """
        Return the path to this executor's single blueprint file.

        Returns
        -------
        Path
            Path to the blueprint YAML file: `B_{name}.yaml`.
        """
        return self.roms_marbl_blueprint_dir / f"B_{self.name}.yaml"

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
        if self.roms_marbl_blueprint is None:
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
        ``code_spec`` (ForgeBlueprint ``code``): roms + optional marbl repos, with placeholder
        run_time/compile_time entries that ``configure_build`` overwrites with the rendered
        code directories.
        """
        if self.code_spec is None:
            raise ValueError(
                "ForgeExecutor requires code_spec (the ForgeBlueprint ``code``) to build the "
                "blueprint code repository; construct via ForgeExecutor.from_forge_blueprint."
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

        # The cstar models are extra="forbid": optional repos must be omitted
        # (not passed as None) so the blueprint validates against C-Star
        # versions that predate the optional field (e.g. ``pio``, cstar #594).
        repo_kwargs: dict[str, Any] = {}
        if self.code_spec.marbl:
            repo_kwargs["marbl"] = _repo(self.code_spec.marbl)
        if self.code_spec.pio:
            repo_kwargs["pio"] = _repo(self.code_spec.pio)
        return cstar_models.ROMSCompositeCodeRepository(
            roms=_repo(self.code_spec.roms),
            run_time=cstar_models.CodeRepository(
                location="placeholder://run_time", branch="main"
            ),
            compile_time=cstar_models.CodeRepository(
                location="placeholder://compile_time", branch="main"
            ),
            **repo_kwargs,
        )

    def _template_repo_args(self, stage: str) -> dict[str, Any]:
        """C-Star :class:`AdditionalCode` constructor args for the ``stage``
        (``compile_time`` / ``run_time``) templates, read purely from ``code_spec``.

        The ForgeBlueprint carries the template repo (git ``location`` + ``commit``/``branch``,
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
        its stager layout this breaks silently — see docs/dev-notes/executor-portability-plan.md.

        Reproducibility caveat (deferred follow-up): the resolver currently pins the
        template repo by ``branch`` (``main``), not a commit, and ``code.templates_*.location``
        participates in ``content_hash`` — so a template edit changes build output without a
        hash bump until a commit is pinned. Tracked in docs/dev-notes/executor-portability-plan.md.
        """
        dest = self._require_host().working_dir / "templates" / stage
        if dest.exists():
            shutil.rmtree(dest)
        AdditionalCode(**self._template_repo_args(stage)).get(local_dir=dest)
        return dest

    def _template_files(self, stage: str) -> list[str]:
        """File list for the ``stage`` templates (from ``code_spec``)."""
        return list(getattr(self.code_spec, f"templates_{stage}").files)

    def _initialize_roms_marbl_blueprint(self) -> None:
        """
        Initialize the in-memory blueprint with its basic structure.

        This method creates the initial blueprint structure with placeholder data.
        It is called automatically during initialization via `model_post_init()`.

        **Process:**

        1. Initializes compile-time and run-time settings from the resolved ForgeBlueprint
        2. Creates blueprint with:
           - Basic metadata (name, description, dates, partitioning)
           - Code repository specifications from code_spec
           - Placeholder Resource objects for grid, initial_conditions, forcing

        The blueprint at this point has the correct structure but contains
        placeholder data (None locations); nothing is persisted to disk yet.
        Actual data files are added by `generate_inputs()`, and the blueprint is
        only ever written to disk once, at the end of `configure_build()`.
        """
        # Initialize settings from the resolved ForgeBlueprint
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
        self.roms_marbl_blueprint = cstar_models.RomsMarblBlueprint.model_construct(
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

    @property
    def roms_marbl_blueprint_from_file(self) -> cstar_models.RomsMarblBlueprint | None:
        """
        Load and return the persisted blueprint from disk, refreshing the
        in-memory settings dicts from its sidecar file.

        Returns
        -------
        Optional[cstar_models.RomsMarblBlueprint]
            Loaded blueprint or None if the file doesn't exist or loading fails.
        """
        bp_path = self.path_roms_marbl_blueprint()

        if not bp_path.exists():
            return None

        try:
            # Try to deserialize with full validation first (Pydantic warns on
            # placeholder/partial values persisted via model_construct).
            with _suppress_pydantic_warnings():
                roms_marbl_blueprint = deserialize(
                    bp_path, cstar_models.RomsMarblBlueprint
                )
        except (OSError, yaml.YAMLError, ValidationError) as e:
            # Strict validation routinely fails for blueprints persisted via
            # model_construct with placeholder resources (e.g. before
            # configure_build() runs); that's expected, so log at debug, not
            # warning, and fall back to lenient loading below.
            log.debug(
                "Strict load of blueprint from %s failed, falling back to lenient "
                "loading: %s",
                bp_path,
                e,
                exc_info=True,
            )
            try:
                # Load YAML as dict and use model_construct to bypass validation
                with bp_path.open("r") as f:
                    roms_marbl_blueprint_data = yaml.safe_load(f)
                # Use model_construct to bypass validation (files may not exist)
                roms_marbl_blueprint = cstar_models.RomsMarblBlueprint.model_construct(
                    **roms_marbl_blueprint_data
                )
            except (OSError, yaml.YAMLError, TypeError) as e2:
                # If lenient loading also fails, issue a warning and return None
                log.warning(
                    "Lenient load of blueprint from %s also failed: %s",
                    bp_path,
                    e2,
                    exc_info=True,
                )
                warnings.warn(
                    f"Failed to load blueprint from {bp_path}: "
                    f"{type(e).__name__}: {e}. "
                    f"Lenient loading also failed: {type(e2).__name__}: {e2}",
                    UserWarning,
                    stacklevel=2,
                )
                return None

        if roms_marbl_blueprint is not None:
            self._load_settings_from_file()

        return roms_marbl_blueprint

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
        with _suppress_pydantic_warnings():
            if from_file:
                roms_marbl_blueprint = self.roms_marbl_blueprint_from_file
            else:
                roms_marbl_blueprint = self.roms_marbl_blueprint

        if roms_marbl_blueprint is None:
            return None

        # Navigate to the field (handle nested fields like "forcing.surface")
        # Handle both model instances and dicts at each level
        with _suppress_pydantic_warnings():
            field_parts = field.split(".")
            data = roms_marbl_blueprint
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
            if isinstance(location, Path) or hasattr(location, "__str__"):
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
        log.debug(
            "ensure_source_data: entering for %r (include_streamable=%s)",
            self.name,
            include_streamable,
        )

        if self.grid is None:
            raise RuntimeError(
                "Grid must be created before preparing source data. "
                "This should have been created during initialization."
            )

        if self.source_dataset_keys is None:
            raise ValueError(
                "source_dataset_keys is required (the resolved ForgeBlueprint ``datasets``); "
                "construct via ForgeExecutor.from_forge_blueprint."
            )

        # An explicit topography_path was already staged verbatim by
        # _resolve_topography_source (before grid construction, in model_post_init) and
        # used as-is regardless of source name. Drop the topography key from this main
        # pass so a verify-only handler (e.g. EMOD/SRTM15) doesn't spuriously re-check
        # the conventional source_data_dir location when the user pointed elsewhere.
        dataset_keys = self.source_dataset_keys
        if self.topography_path:
            topo_name = str(
                getattr(self.topography_source, "value", self.topography_source)
            ).upper()
            dataset_keys = [k for k in dataset_keys if k.upper() != topo_name]

        self.src_data = source_data.SourceData(
            datasets=dataset_keys,
            clobber=False,
            grid=self.grid,
            grid_name=self.grid_name,
            start_time=self.start_date,
            end_time=self.end_date,
            source_data_dir=self._require_host().source_data_cache,
            resolved_datasets=self.resolved_datasets,
        ).prepare_all(include_streamable=include_streamable)

    def generate_inputs(
        self,
        clobber: bool = False,
        use_dask: bool = True,
        dask_num_workers: int = 8,
        subchunk: bool = True,
        test: bool = False,
        only: set[str] | None = None,
    ) -> cstar_models.RomsMarblBlueprint:
        """
        Generate ROMS input files and update the in-memory blueprint in place.

        Always regenerates the in-memory blueprint (and in-memory settings).
        Existing NetCDF files are preserved and reused per-step when
        ``clobber=False``; pass ``clobber=True`` to delete and re-create them.
        Nothing is persisted to disk here -- the blueprint is written once, at
        the end of `configure_build()`.

        Parameters
        ----------
        clobber : bool, optional
            If True, delete and regenerate existing NetCDF input files. Default False.
        use_dask : bool, optional
            Use dask for parallel computations. Default True.
        dask_num_workers : int, optional
            Cap on dask's default threaded-scheduler worker count while generating
            inputs (paired with pinning BLAS/OpenMP to 1 thread), to avoid thread
            oversubscription hangs on high-core HPC nodes. Only applied when
            ``use_dask`` is True. Default 8.
        subchunk : bool, optional
            Just-in-time build a kerchunk-subchunked reference for multi-file
            GLORYS sources (see ``glorys_subchunk.py``) and read from it
            instead of the raw per-day files. Default True; pass False to
            read the raw files directly.
        test : bool, optional
            Truncate the generation loop after 2 iterations (for unit tests).
        only : set[str], optional
            Canonical input-category keys (see
            ``input_data.resolve_input_selection``) to restrict generation to.
            The resulting in-memory blueprint/settings only reflect the generated
            subset (plus grid, which always runs) -- callers doing a subset run
            should not proceed to ``configure_build()``.

        Returns
        -------
        cstar_models.RomsMarblBlueprint
            The blueprint updated with all input file locations.

        Raises
        ------
        RuntimeError
            If blueprint is not initialized, or if settings are not initialized.

        """
        log.debug(
            "generate_inputs: entering for %r (clobber=%s, use_dask=%s, subchunk=%s, "
            "start_date=%s, end_date=%s, only=%s)",
            self.name,
            clobber,
            use_dask,
            subchunk,
            self.start_date,
            self.end_date,
            only,
        )

        if self.roms_marbl_blueprint is None:
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

        roms_marbl_blueprint_elements, settings_compile_time, settings_run_time = (
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
                roms_marbl_blueprint_dir=self.roms_marbl_blueprint_dir,
                partitioning=self.partitioning,
                cdr_forcing=self.cdr_forcing,
                use_dask=use_dask,
                dask_num_workers=dask_num_workers,
                subchunk=subchunk,
                use_pio=self._use_pio,
                verbose=self.verbose,
            ).generate_all(clobber=clobber, test=test, only=only)
        )

        if roms_marbl_blueprint_elements is None:
            raise RuntimeError(
                "Blueprint mismatch detected, but input files exist. "
                "Set clobber=True to overwrite existing input files."
            )

        # Apply settings from input data generation (deep merge to preserve existing
        # settings). allow_new=True: the ForgeBlueprint base omits the sections that input
        # generation fills (grid/initial/forcing/s_coord), so they arrive as new keys.
        self._update_settings_compile_time(settings_compile_time, allow_new=True)
        self._update_settings_run_time(settings_run_time, allow_new=True)

        if test:
            return

        # Update the blueprint with the generated input data.
        roms_marbl_blueprint_dict = self.roms_marbl_blueprint.model_dump()
        roms_marbl_blueprint_dict["grid"] = (
            roms_marbl_blueprint_elements.grid.model_dump()
            if roms_marbl_blueprint_elements.grid
            else None
        )
        roms_marbl_blueprint_dict["initial_conditions"] = (
            roms_marbl_blueprint_elements.initial_conditions.model_dump()
            if roms_marbl_blueprint_elements.initial_conditions
            else None
        )
        roms_marbl_blueprint_dict["forcing"] = (
            roms_marbl_blueprint_elements.forcing.model_dump()
            if roms_marbl_blueprint_elements.forcing
            else None
        )
        roms_marbl_blueprint_dict["cdr_forcing"] = (
            roms_marbl_blueprint_elements.cdr_forcing.model_dump()
            if roms_marbl_blueprint_elements.cdr_forcing
            else None
        )
        roms_marbl_blueprint_dict["nesting_info"] = (
            roms_marbl_blueprint_elements.nesting_info.model_dump()
            if roms_marbl_blueprint_elements.nesting_info
            else None
        )

        # Settings are stored in a sidecar YAML, not in the blueprint itself.
        roms_marbl_blueprint_dict["model_params"] = None
        roms_marbl_blueprint_dict["runtime_params"] = None

        self.roms_marbl_blueprint = cstar_models.RomsMarblBlueprint.model_construct(
            **roms_marbl_blueprint_dict
        )
        # The blueprint now carries real input locations (not placeholders), so
        # configure_build can re-validate it against the cstar models at emit time.
        self._inputs_generated = True
        return self.roms_marbl_blueprint

    def _init_settings_compile_time(self) -> None:
        """
        Initialize the compile-time settings dictionary from the resolved ForgeBlueprint.

        ``cppdefs`` is the only compile-time section. It is used as the basis for
        template rendering during `configure_build()`; user overrides can still be
        applied via `_update_settings_compile_time()` or `configure_build()`.

        **Called by:** `_initialize_roms_marbl_blueprint()` during initialization.
        """
        if self.resolved_settings is None:
            raise ValueError(
                "resolved_settings is required (the ForgeBlueprint ``model_settings``); "
                "construct via ForgeExecutor.from_forge_blueprint."
            )
        self._settings_compile_time = {
            "cppdefs": copy.deepcopy(self.resolved_settings.get("cppdefs", {}))
        }

    def _init_settings_run_time(self) -> None:
        """
        Initialize the run-time settings dictionary from the resolved ForgeBlueprint.

        The authoritative, host-independent base is ``resolved_settings`` (the ForgeBlueprint
        ``model_settings``): every non-``cppdefs`` section, deep-copied. The resolver
        already carries the genuinely-computed numerics (``time_stepping``, ``v_sponge``,
        ``extract_data``), so they are NOT re-derived here — that would clobber e.g. an
        explicitly-resolved ``dt``. Only the sections the config deliberately omits because
        they embed host/identity or a run-level value are ADDED: ``title``,
        ``output_root_name``, and ``reference_date_settings`` (from ``model_reference_date``).

        **Called by:** `_initialize_roms_marbl_blueprint()` during initialization.
        """
        if self.resolved_settings is None:
            raise ValueError(
                "resolved_settings is required (the ForgeBlueprint ``model_settings``); "
                "construct via ForgeExecutor.from_forge_blueprint."
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
        # Emit the model reference date (t=0) into the namelist, derived from the
        # same blueprint value passed to the roms-tools input objects. The namelist
        # has no sub-day granularity, so only year/month/day are carried.
        mrd = self.model_reference_date or datetime(2000, 1, 1)
        self._settings_run_time["reference_date_settings"] = dict(
            reference_date=[mrd.year, mrd.month, mrd.day],
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
                # Generation-overlay path: the ForgeBlueprint base omits sections that are
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
                # Generation-overlay path: the ForgeBlueprint base omits sections that are
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
    def from_forge_blueprint(
        cls, cfg: Any, host: HostPaths | None = None, verbose: bool = False
    ) -> ForgeExecutor:
        """Canonical constructor: build the executor directly from a resolved
        ``ForgeBlueprint`` (the forge application's blueprint) + the injected ``host``.

        This is the single derivation path from a ForgeBlueprint to a runnable builder. The
        domain-catalog path routes through the resolver
        (``forge_blueprint_resolve.build_forge_blueprint``) to produce the ``ForgeBlueprint`` and then
        here — so there is one place that maps blueprint → builder inputs
        (``forge_blueprint_engine.forge_blueprint_to_builder_kwargs``).

        ``verbose`` must be supplied here (rather than only at ``generate_inputs``)
        because the Grid/``align_grids`` calls run during ``model_post_init``, i.e.
        before any post-construction method call could set it.
        """
        from cstar_forge.forge.forge_blueprint_engine import (
            forge_blueprint_to_builder_kwargs,
        )

        return cls(**forge_blueprint_to_builder_kwargs(cfg), host=host, verbose=verbose)

    def configure_build(
        self,
        compile_time_settings: dict[str, Any] | None = None,
        run_time_settings: dict[str, Any] | None = None,
        **kwargs,
    ):
        """
        Configure blueprint by rendering templates, then persist the final blueprint.

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
        7. Re-validates the final blueprint against the installed C-Star models
           (only when `generate_inputs()` has run -- the placeholder blueprint
           cannot validate), so an extra="forbid" mismatch fails at emit time
        8. Persists the blueprint to disk -- the only time it is written

        This is expected to run after `generate_inputs()` has populated the
        in-memory blueprint with real input data file locations.

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
        log.debug("configure_build: entering for %r", self.name)

        # Initialize to empty dict if None
        if compile_time_settings is None:
            compile_time_settings = {}
        if run_time_settings is None:
            run_time_settings = {}

        # Validate that blueprint is initialized
        if self.roms_marbl_blueprint is None:
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

        # CDR-output consistency net, mirroring the resolver's consistency block:
        # stored blueprints reach configure_build without re-resolving, and wizard
        # accordion overrides apply after the resolver, so this is the enforcement
        # point of record. A generated CDR forcing implies CDR output regardless of
        # the stored snapshot.
        if self.cdr_forcing:
            self._settings_run_time.setdefault("cdr_output", {})["do_cdr_output"] = True
        # do_cdr_output requires MARBL plus the CDR_FORCING cppdef (both gate
        # compiling ucla-roms' cdr_output.F90), and the MARBL diagnostics ucla-roms
        # looks up by name, unchecked.
        if self._settings_run_time.get("cdr_output", {}).get("do_cdr_output"):
            cppdefs = self._settings_compile_time.setdefault("cppdefs", {})
            if not cppdefs.get("marbl", False):
                raise ValueError(
                    "cdr_output.do_cdr_output is True but cppdefs.marbl is False: "
                    "CDR output requires MARBL (ucla-roms only compiles "
                    "cdr_output.F90 under MARBL && CDR_FORCING)."
                )
            if not cppdefs.get("cdr_forcing"):
                cppdefs["cdr_forcing"] = True
                log.info(
                    "configure_build: do_cdr_output is True; forcing cppdefs.cdr_forcing=True "
                    "(CDR_FORCING gates ucla-roms' cdr_output.F90)."
                )
            marbl = self._settings_run_time.setdefault("marbl_bgc", {})
            before = list(marbl.get("marbl_diagnostics_to_write") or [])
            after = ensure_cdr_output_marbl_diagnostics(before)
            if after != before:
                marbl["marbl_diagnostics_to_write"] = after
                log.info(
                    "configure_build: do_cdr_output is True; added missing required MARBL "
                    "diagnostics to marbl_bgc.marbl_diagnostics_to_write (%s).",
                    sorted(set(after) - set(before)),
                )

        # Derive n_tracers: prefer the value passed by the processing engine; otherwise
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

            roms_marbl_blueprint_dict = self.roms_marbl_blueprint.model_dump(
                mode="json"
            )
            code_dict = roms_marbl_blueprint_dict["code"]
            # Convert dicts from render_roms_settings / write_roms_namelist to CodeRepository objects
            code_dict["compile_time"] = cstar_models.CodeRepository.model_construct(
                **compile_time_code
            )
            code_dict["run_time"] = cstar_models.CodeRepository.model_construct(
                **run_time_code
            )
            roms_marbl_blueprint_dict["code"] = (
                cstar_models.ROMSCompositeCodeRepository.model_construct(**code_dict)
            )

            roms_marbl_blueprint_dict["model_params"] = {
                "time_step": self._settings_run_time["time_stepping"]["dt"],
            }
            if self._use_pio:
                roms_marbl_blueprint_dict["model_params"]["use_pio"] = True
            # No output_dir here: it is a pre-2.0.0 field superseded by the
            # blueprint working_dir (set just below).
            roms_marbl_blueprint_dict["runtime_params"] = {
                "start_date": self.start_date,
                "end_date": self.end_date,
            }
            roms_marbl_blueprint_dict["working_dir"] = self.roms_blueprint_working_dir

            self.roms_marbl_blueprint = cstar_models.RomsMarblBlueprint.model_construct(
                **roms_marbl_blueprint_dict
            )
            # With real (generated) inputs the blueprint is complete: re-validate
            # it against the installed C-Star models before it is persisted, so a
            # mismatch fails here instead of when C-Star loads the file.
            if self._inputs_generated:
                self.roms_marbl_blueprint = self._validated_roms_marbl_blueprint()
            self.persist()

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

        # A stale `cstar` on PATH (e.g. a pip fallback under ~/.local/bin on some
        # HPC systems) can shadow this environment's `cstar` executable. Force the
        # running env's bin dir to the front of PATH so its `cstar` always wins,
        # on every host, without relying on a machine-specific check.
        bin_dir = Path(sys.executable).parent
        cstar_exe = bin_dir / "cstar"
        if not cstar_exe.is_file():
            log.warning(
                "Expected cstar executable not found at %s; prepending to PATH anyway",
                cstar_exe,
            )
        current_path = os.environ.get("PATH", "")
        current_path_entries = current_path.split(os.pathsep) if current_path else []
        if not current_path_entries or current_path_entries[0] != str(bin_dir):
            os.environ["PATH"] = os.pathsep.join([str(bin_dir), *current_path_entries])

    async def run(
        self,
    ):
        """Run C-Star for this Builder's blueprint"""
        log.debug("run: entering for %r", self.name)
        self.prep_cstar_environment()

        request = RunnerRequest(
            uri=str(self.path_roms_marbl_blueprint()),
            bp_type=RomsMarblBlueprint,
            name=self.casename,
        )
        service_cfg = get_service_config(log_level="INFO")
        job_cfg = get_job_config()
        runner = RomsMarblRunner(
            request=request, service_cfg=service_cfg, job_cfg=job_cfg
        )
        await runner.execute()
