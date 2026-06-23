"""
``SpecConfig``: the single authoritative, fully-resolved input to processing.

This is a **draft / starting point** for the planned refactor that splits
``CstarSpecBuilder`` into two phases (see ``docs/spec-config-inventory.md``):

1. **Collection / curation** — assemble every option from its source (constructor
   args, the ModelSpec, machine config, hardcoded conventions, and the *pure*
   derived values), validate it, and write one reviewable ``spec_config.yml``.
2. **Processing** — ingest that file on any machine and run the heavy work
   (``generate_inputs`` + ``configure_build``).

``SpecConfig`` is the contract between the two phases: it is plain, validated data
with **no** ``rt.Grid`` objects, **no** source downloads, and **no** file I/O.

Design rules (from the inventory doc):

* ``SpecConfig`` is **input-only**. Values that can only be known *after* artifacts
  are materialized — ``s_coord`` (theta_s/theta_b/tcline read from the generated
  grid file) and all file paths (grid/initial/forcing) — are **outputs** and belong
  in the resulting blueprint, NOT here.
* **Pure-derived** values (timestep, ntimes, v_sponge, param dims, obc flags,
  casename, output root, extract_period) ARE included: they are deterministic
  functions of the config inputs and are frozen here for review.
* Hardcoded registries / URLs / ``roms_tools`` defaults are **snapshotted** into the
  file (``sources.resolved_datasets``, ``conventions``) so a reviewer sees the real
  values and a different processing host cannot silently drift.

NOTE: This module is not yet wired into ``CstarSpecBuilder``. It defines the target
schema and the ``to_yaml``/``from_yaml`` round-trip so the resolver (Phase 1) and
engine (Phase 2) can be built against a stable contract.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field

SPEC_CONFIG_VERSION = 1


class _Section(BaseModel):
    # Strict by default: an unknown key is a bug in the resolver or a stale file.
    model_config = ConfigDict(extra="forbid")


# ===========================================================================
# Identity & run window
# ===========================================================================
class Identity(_Section):
    """Names and IDs. ``casename`` is pre-derived (``{name}_{datestr}``)."""

    model_name: str
    grid_name: str
    description: str = "Generated blueprint"
    ensemble_id: Optional[int] = None
    name: str  # "{model}_{grid}_{n_procs}procs[_{ensemble:03d}]"
    casename: str  # "{name}_{start:%Y%m%d}-{end:%Y%m%d}"


class RunWindow(_Section):
    start_date: datetime
    end_date: datetime


# ===========================================================================
# A. Grid / domain geometry
# ===========================================================================
class OpenBoundaries(_Section):
    north: bool = False
    south: bool = False
    east: bool = False
    west: bool = False


class Domain(_Section):
    """Grid construction inputs (kwargs only — the ``rt.Grid`` is built in Phase 2).

    ``grid_kwargs_parent`` / ``grid_kwargs_child`` are present only for nested
    domains; ``metadata_child`` holds the child grid's ``metadata`` block (e.g.
    ``period``).
    """

    grid_kwargs: Dict[str, Any]
    topography_source: str  # e.g. "ETOPO5"
    open_boundaries: OpenBoundaries
    grid_kwargs_parent: Optional[Dict[str, Any]] = None
    grid_kwargs_child: Optional[Dict[str, Any]] = None
    metadata_child: Optional[Dict[str, Any]] = None


# ===========================================================================
# B. Forcing & source data
# ===========================================================================
class SourceSpec(_Section):
    """A resolved data source. ``name`` is the logical name; ``dataset_key`` is the
    registry key it resolved to (e.g. GLORYS -> GLORYS_REGIONAL)."""

    name: str
    dataset_key: Optional[str] = None
    climatology: bool = False
    glorys_layout: Optional[str] = None  # "regional" | "global"


class SurfaceForcingItem(_Section):
    source: SourceSpec
    type: str  # "physics" | "bgc" | "restoring"
    correct_radiation: bool = False
    coarse_grid_mode: str = "auto"  # "auto" | "always" | "never"
    restoring_forces: Optional[List[str]] = None


class BoundaryForcingItem(_Section):
    source: SourceSpec
    type: str  # "physics" | "bgc"


class TidalForcingItem(_Section):
    source: SourceSpec
    ntides: Optional[int] = None


class RiverForcingItem(_Section):
    source: SourceSpec
    include_bgc: bool = False


class InitialConditions(_Section):
    source: SourceSpec
    bgc_source: Optional[SourceSpec] = None


class Forcing(_Section):
    surface: List[SurfaceForcingItem] = Field(default_factory=list)
    boundary: List[BoundaryForcingItem] = Field(default_factory=list)
    tidal: List[TidalForcingItem] = Field(default_factory=list)
    river: List[RiverForcingItem] = Field(default_factory=list)


class ResolvedDataset(_Section):
    """A snapshot of how a logical source resolves — frozen from the hardcoded
    registry so the processing host uses exactly these IDs/URLs."""

    dataset_key: str
    dataset_id: Optional[str] = None
    url: Optional[str] = None
    streamable: bool = False


class Sources(_Section):
    initial_conditions: InitialConditions
    forcing: Forcing
    cdr_forcing: Optional[Dict[str, Any]] = None
    # logical-name -> resolved registry entry (snapshot of source_data.py tables)
    resolved_datasets: Dict[str, ResolvedDataset] = Field(default_factory=dict)


# ===========================================================================
# C/D/E. Model settings (the namelist + cppdefs vocabulary)
# ===========================================================================
class ModelSettings(_Section):
    """The settings dicts, fully merged (defaults from the ModelSpec YAMLs, then
    override files, then user ``configure_build`` overrides).

    These stay as free-form dicts here because the authoritative validation already
    lives in ``cstar_forge.namelist_model.RunTimeSettings`` /
    ``cstar.roms.namelist.RomsNamelist``. Phase 2 should validate ``run_time``
    through ``RunTimeSettings`` before writing the namelist.

    ``run_time`` here holds ONLY config-time-knowable values. The artifact-derived
    sections (``s_coord`` theta/tcline; ``grid``/``initial``/``forcing`` file paths)
    are intentionally absent — they are produced by processing and written to the
    blueprint.
    """

    compile_time: Dict[str, Any]  # {"cppdefs": {...}}
    run_time: Dict[str, Any]  # ~25 namelist sections (no artifact paths)
    properties: Dict[str, Any]  # {"n_tracers": 34, "marbl": True}
    # Pure-derived values, surfaced explicitly for review (also present inside
    # run_time, but duplicated here as the audit trail of what Phase 1 computed).
    derived: Dict[str, Any] = Field(default_factory=dict)


# ===========================================================================
# F. Code, templates, execution
# ===========================================================================
class CodeRepo(_Section):
    location: str
    commit: Optional[str] = None
    branch: Optional[str] = None


class TemplateSpec(_Section):
    location: str
    files: List[str] = Field(default_factory=list)


class Code(_Section):
    roms: CodeRepo
    marbl: Optional[CodeRepo] = None
    templates_compile_time: TemplateSpec
    templates_run_time: TemplateSpec


class Partitioning(_Section):
    n_procs_x: int
    n_procs_y: int


class Machine(_Section):
    tag: str  # "MacOS" | "RCAC_anvil" | "NERSC_perlmutter" | "unknown"
    account: str = ""
    pes_per_node: Optional[int] = None
    queues: Dict[str, str] = Field(default_factory=dict)
    cluster_type: Optional[str] = None  # "LOCAL" | "SLURM"


class Paths(_Section):
    """Resolved on the *generating* machine. Phase 2 may re-resolve these from the
    processing host's environment (``--paths-from-env``) for portability."""

    source_data: str
    input_data: str
    scratch: str
    catalog: str


class Execution(_Section):
    partitioning: Partitioning
    machine: Machine
    paths: Paths
    run_output_dir: str


# ===========================================================================
# Conventions & provenance
# ===========================================================================
class Conventions(_Section):
    """Hardcoded values lifted out of ``input_data.py`` so they are visible and
    overridable rather than implicit. Defaults mirror today's behavior."""

    nsub_x: int = 1
    nsub_e: int = 1
    nrrec: int = 1
    ndtfast: int = 60
    ninfo: int = 1
    cdr_file: str = "cdr.nc"
    nesting_file: str = "nesting.nc"
    nesting_period_fallback_seconds: float = 3600.0
    bry_tides: bool = True
    pot_tides: bool = True
    ana_tides: bool = False


class Provenance(_Section):
    """Audit trail. Timestamps are passed in (never generated inside a resolver, to
    keep Phase 1 deterministic/reproducible)."""

    generated_at: Optional[datetime] = None
    forge_version: Optional[str] = None
    roms_tools_version: Optional[str] = None
    override_files_applied: List[str] = Field(default_factory=list)
    notes: Optional[str] = None


# ===========================================================================
# Top-level authoritative config
# ===========================================================================
class SpecConfig(_Section):
    """The complete, sufficient, reviewable input to processing.

    Round-trips to a single ``spec_config.yml`` via :meth:`to_yaml` / :meth:`from_yaml`.
    """

    spec_config_version: int = SPEC_CONFIG_VERSION
    identity: Identity
    run: RunWindow
    domain: Domain
    sources: Sources
    model_settings: ModelSettings
    code: Code
    execution: Execution
    conventions: Conventions = Field(default_factory=Conventions)
    provenance: Provenance = Field(default_factory=Provenance)

    # ---- serialization ----
    def to_yaml(self, path: Union[str, Path]) -> Path:
        """Write the authoritative config to ``path`` and return it."""
        path = Path(path)
        data = self.model_dump(mode="json", exclude_none=False)
        path.write_text(yaml.safe_dump(data, sort_keys=False, default_flow_style=False))
        return path

    def to_yaml_str(self) -> str:
        data = self.model_dump(mode="json", exclude_none=False)
        return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "SpecConfig":
        """Load and validate a ``spec_config.yml`` (Phase 2 entry point)."""
        data = yaml.safe_load(Path(path).read_text())
        return cls.model_validate(data)

    def with_paths(self, paths: Paths) -> "SpecConfig":
        """Return a copy with ``execution.paths`` replaced — for re-resolving paths
        on a different processing host without re-running Phase 1."""
        return self.model_copy(
            update={"execution": self.execution.model_copy(update={"paths": paths})}
        )
