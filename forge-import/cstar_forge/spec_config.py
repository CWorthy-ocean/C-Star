"""
``SpecConfig``: the single authoritative, fully-resolved input to processing.

This is a **draft / starting point** for the planned refactor that splits
``CstarSpecBuilder`` into two phases (see ``docs/spec-config-inventory.md``):

1. **Collection / curation** — assemble every option from its source (constructor
   args, the ModelSpec, and the *pure* derived values), validate it, and write one
   reviewable ``spec_config.yml``.
2. **Processing** — ingest that file on any machine and run the heavy work
   (``generate_inputs`` + ``configure_build``).

``SpecConfig`` is the contract between the two phases: plain, validated data with
**no** ``rt.Grid`` objects, **no** source downloads, and **no** file I/O.

Single governing principle
--------------------------
The config stores ONLY host-independent, single-source-of-truth inputs. Anything
mechanically derivable is computed at **processing** time, never stored:

* **Host/machine** — the machine tag, account, queues, ``pes_per_node``, and every
  data path (source_data / input_data / scratch / catalog) are resolved at
  processing time from ``cstar_forge.config`` on the machine that runs the work.
  ``run_output_dir`` and the namelist ``output_root_name`` (which embed the scratch
  path) are therefore derived there too.
* **Naming** — the only atomic identity inputs are ``model_name`` and ``grid_name``
  (plus ``ensemble_id`` and the run dates). ``name``, ``casename``,
  the namelist ``title``, ``output_root_name``, and ``run_output_dir`` are
  deterministic functions of those and are exposed as computed properties /
  helpers — never stored as independent values.
* **Artifacts** — ``s_coord`` (theta_s/theta_b/tcline, read from the generated grid
  file) and all file paths (grid / initial / forcing) are processing outputs and
  belong in the resulting blueprint, not here.

What IS stored: curated inputs (grid kwargs, partitioning, sources, code/template
pins) and the model settings — including the *pure-derived* numerics (timestep,
ntimes, v_sponge, param dims, obc flags) which carry scientific review value and may
be hand-edited before processing. Fixed implementation details (e.g. the ``cdr.nc`` /
``nesting.nc`` filenames, ``nrrec``, the tide flags set during generation) are NOT
stored — they are deterministic and set by the processing step.

NOTE: This module is not yet wired into ``CstarSpecBuilder``. It defines the target
schema and the ``to_yaml``/``from_yaml`` round-trip so the resolver (Phase 1) and
engine (Phase 2) can be built against a stable contract.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field

# Top-level sections EXCLUDED from the integrity hash: provenance (where the hash
# lives), composition + identity (labels/provenance, not results-affecting), and the
# schema version. Everything else (application, run, domain, sources, properties,
# model_settings, code) is hashed.
_HASH_EXCLUDE = {"spec_config_version", "identity", "composition", "provenance"}

# Bumped only on a BREAKING schema change. Additive fields (with defaults) are
# backward-compatible — old files still load — so they do NOT bump this. ``from_yaml``
# rejects files declaring a *newer* version than this build understands.
SPEC_CONFIG_VERSION = 2

# Identifies which C-Star application this blueprint targets (the planned home of the
# processing engine). Stable across schema/field iteration; used to route the blueprint.
DEFAULT_APPLICATION = "roms_marbl"


class _Section(BaseModel):
    # Strict by default: an unknown key is a bug in the resolver or a stale file.
    model_config = ConfigDict(extra="forbid")


# ===========================================================================
# Identity (atomic naming inputs only) & run window
# ===========================================================================
class Identity(_Section):
    """The *atomic* naming inputs. Everything else (``name``, ``casename``,
    namelist ``title``, ``output_root_name``, ``run_output_dir``) is derived from
    these + the run dates + ``partitioning`` — see :class:`SpecConfig` properties.
    """

    model_name: str  # the ModelSpec id, e.g. "cson_roms-marbl_v0.1"
    grid_name: str  # e.g. "test-tiny"
    ensemble_id: Optional[int] = None
    description: str = "Generated blueprint"


class RunWindow(_Section):
    start_date: datetime
    end_date: datetime
    model_reference_date: datetime = datetime(2000, 1, 1)
    """The ROMS model reference date (t=0). Passed to every rt object that accepts it
    (InitialConditions, SurfaceForcing, BoundaryForcing, TidalForcing, CDRForcing).
    Defaults to 2000-01-01, which is the roms-tools default."""


# ===========================================================================
# A. Grid / domain geometry (incl. partitioning — a host-independent
#    decomposition choice that belongs to the domain)
# ===========================================================================
class OpenBoundaries(_Section):
    north: bool = False
    south: bool = False
    east: bool = False
    west: bool = False


class Partitioning(_Section):
    n_procs_x: int
    n_procs_y: int


class Domain(_Section):
    """Grid construction inputs (kwargs only — the ``rt.Grid`` is built in Phase 2).

    ``grid_kwargs`` is the single source for grid geometry, including ``theta_s`` /
    ``theta_b`` / ``hc`` when provided; the namelist ``s_coord`` section is filled at
    processing from the generated grid rather than duplicated here.
    """

    grid_kwargs: Dict[str, Any]
    topography_source: str  # e.g. "ETOPO5"
    open_boundaries: OpenBoundaries
    partitioning: Partitioning
    grid_kwargs_parent: Optional[Dict[str, Any]] = None
    grid_kwargs_child: Optional[Dict[str, Any]] = None
    metadata_child: Optional[Dict[str, Any]] = None
    nesting_include_pressure_fluxes: bool = False
    """Whether to include baroclinic pressure fluxes in the nesting extraction file
    (passed to make_nesting_info / make_edata as include_pressure_fluxes)."""


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
    wind_dropoff: bool = False  # coastal wind-speed reduction


class BoundaryForcingItem(_Section):
    source: SourceSpec
    type: str  # "physics" | "bgc"
    apply_2d_horizontal_fill: bool = False  # 2D horizontal fill before regridding
    use_density_interpolation: bool = False  # BGC density-space interp


class TidalForcingItem(_Section):
    source: SourceSpec
    ntides: Optional[int] = None


class RiverForcingItem(_Section):
    source: SourceSpec
    include_bgc: bool = False
    convert_to_climatology: str = "if_any_missing"  # "never"|"if_any_missing"|"always"
    bgc_source: Optional[Dict[str, Any]] = None  # separate river-BGC dataset config


class InitialConditions(_Section):
    source: SourceSpec
    bgc_source: Optional[SourceSpec] = None
    use_density_interpolation: bool = False  # BGC density-space interp
    allow_flex_time: bool = False  # ±24h search window around ini_time


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
# C. Code, templates
# ===========================================================================
class CodeRepo(_Section):
    location: str
    commit: Optional[str] = None
    branch: Optional[str] = None


class TemplateRepo(CodeRepo):
    """A template source pulled from a repo — like ``code.roms`` / ``code.marbl``
    (``location`` + one of ``commit`` / ``branch``) but with a file filter: the
    ``files`` to pull, optionally under an in-repo ``directory``."""

    directory: Optional[str] = None
    files: List[str] = Field(default_factory=list)


class Code(_Section):
    roms: CodeRepo
    marbl: Optional[CodeRepo] = None
    templates_compile_time: TemplateRepo
    templates_run_time: TemplateRepo


# ===========================================================================
# Composition (which catalog pieces produced this config) & provenance
# ===========================================================================
class PieceRef(_Section):
    """Records where one composable piece (model / domain / forcing) came from.

    Supports the "pick from a catalog or build your own" workflow: a UI can show,
    for each piece, whether it was a catalog selection (and which one), whether the
    user edited it, or whether it was authored from scratch.
    """

    name: Optional[str] = None  # catalog entry name, or None if authored from scratch
    origin: str = "custom"  # "catalog" | "custom" | "model_default"
    modified: bool = False  # True if a catalog piece was edited after selection


class Composition(_Section):
    """The composable pieces selected to build this config. The resolved data lives
    in the sections above; this records provenance for review/UI.

    ``forcing`` corresponds to the ``sources`` section (initial conditions + surface/
    boundary/tidal/river + CDR)."""

    model: PieceRef = Field(default_factory=PieceRef)
    domain: PieceRef = Field(default_factory=PieceRef)
    forcing: PieceRef = Field(default_factory=PieceRef)
    output: PieceRef = Field(default_factory=PieceRef)  # output-settings piece (OutputSpec)
    # Manual edits applied on top of the composed pieces: a sparse
    # {section: {field: value}} (or {section: scalar}) layer. ``model_settings`` already
    # reflects these; this records *which* values were overridden vs. composed/derived.
    overrides: Dict[str, Any] = Field(default_factory=dict)


class Provenance(_Section):
    """Audit trail. Timestamps are passed in (never generated inside a resolver, to
    keep Phase 1 deterministic/reproducible)."""

    generated_at: Optional[datetime] = None
    forge_version: Optional[str] = None
    roms_tools_version: Optional[str] = None
    override_files_applied: List[str] = Field(default_factory=list)
    # sha256 of the results-affecting data (set on save by SpecConfig.to_yaml*).
    # Processing recomputes and compares it to detect hand-edits since write-out.
    content_hash: Optional[str] = None
    notes: Optional[str] = None


# ===========================================================================
# Top-level authoritative config
# ===========================================================================
class SpecConfig(_Section):
    """The complete, sufficient, reviewable input to processing.

    Round-trips to a single ``spec_config.yml`` via :meth:`to_yaml` / :meth:`from_yaml`.

    ``model_settings`` is a FLAT mapping of settings sections: ``cppdefs`` (compile
    time) sits at the same level as every namelist section (``lateral_visc``,
    ``bottom_drag``, ``param``, ``ocean_vars``, ``time_stepping``, ``v_sponge``, …).
    It deliberately OMITS the sections that are filled at processing time:
    ``title`` and ``output_root_name`` (derived from identity + host scratch path),
    ``s_coord`` (read from the generated grid), and ``grid`` / ``initial`` /
    ``forcing`` (artifact file paths). Validate ``model_settings`` through
    ``cstar_forge.namelist_model.RunTimeSettings`` (after the processing step fills
    the omitted sections) before writing the namelist.
    """

    spec_config_version: int = SPEC_CONFIG_VERSION
    application: str = DEFAULT_APPLICATION  # which C-Star application consumes this blueprint
    identity: Identity
    run: RunWindow
    domain: Domain
    sources: Sources
    properties: Dict[str, Any] = Field(default_factory=dict)  # {"n_tracers", "marbl"}
    model_settings: Dict[str, Any] = Field(default_factory=dict)  # flat sections
    code: Code
    composition: Composition = Field(default_factory=Composition)
    provenance: Provenance = Field(default_factory=Provenance)

    # ---- derived naming (single source of truth: identity + dates + n_procs) ----
    @property
    def n_procs(self) -> int:
        return self.domain.partitioning.n_procs_x * self.domain.partitioning.n_procs_y

    @property
    def name(self) -> str:
        base = f"{self.identity.model_name}_{self.identity.grid_name}_{self.n_procs}procs"
        if self.identity.ensemble_id is not None:
            base += f"_{self.identity.ensemble_id:03d}"
        return base

    @property
    def datestr(self) -> str:
        return f"{self.run.start_date:%Y%m%d}-{self.run.end_date:%Y%m%d}"

    @property
    def casename(self) -> str:
        return f"{self.name}_{self.datestr}"

    def run_output_dir(self, scratch: Union[str, Path]) -> Path:
        """Derived at processing time from the host scratch path: ``scratch/casename``."""
        return Path(scratch) / self.casename

    def output_root_name(self, scratch: Union[str, Path]) -> str:
        """Namelist ``output_root_name``, derived at processing time:
        ``<run_output_dir>/output/<casename>``."""
        return str(self.run_output_dir(scratch) / "output" / self.casename)

    # ---- integrity hash ----
    def content_hash(self) -> str:
        """sha256 over the *results-affecting* data (everything except the sections in
        ``_HASH_EXCLUDE``). Deterministic across a YAML round-trip — used to detect
        hand-edits between write-out and processing."""
        data = self.model_dump(mode="json")
        for key in _HASH_EXCLUDE:
            data.pop(key, None)
        blob = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ---- serialization ----
    def to_yaml(self, path: Union[str, Path]) -> Path:
        """Write the authoritative config to ``path`` and return it."""
        path = Path(path)
        path.write_text(self.to_yaml_str())
        return path

    def to_yaml_str(self) -> str:
        # stamp the integrity hash into provenance on the way out (the hash itself
        # excludes provenance, so this doesn't perturb it)
        stamped = self.model_copy(update={
            "provenance": self.provenance.model_copy(update={"content_hash": self.content_hash()})})
        data = stamped.model_dump(mode="json", exclude_none=False)
        return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "SpecConfig":
        """Load and validate a ``spec_config.yml`` (Phase 2 entry point)."""
        data = yaml.safe_load(Path(path).read_text())
        version = (data or {}).get("spec_config_version")
        if version is not None and version > SPEC_CONFIG_VERSION:
            raise ValueError(
                f"spec_config_version {version} is newer than this build supports "
                f"({SPEC_CONFIG_VERSION}); upgrade cstar-forge to read this file."
            )
        return cls.model_validate(data)
