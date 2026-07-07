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
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

# ===========================================================================
# Enums for roms-tools constrained string parameters.
# Values mirror the validation logic in the installed roms-tools constructors
# (SurfaceForcing._input_checks, BoundaryForcing._input_checks, etc.).
# These should eventually move into roms-tools itself.
# ===========================================================================


class SurfaceType(str, Enum):
    """Accepted values for ``SurfaceForcing.type``."""

    PHYSICS = "physics"  # wind, heat, freshwater fluxes (ERA5)
    BGC = "bgc"  # pCO₂ / iron deposition (UNIFIED, CESM_REGRIDDED, MBL_co2)
    RESTORING = "restoring"  # SSS restoring (WOA, UNIFIED)


class BoundaryType(str, Enum):
    """Accepted values for ``BoundaryForcing.type``."""

    PHYSICS = "physics"  # T, S, u, v, ζ (GLORYS)
    BGC = "bgc"  # BGC tracers (UNIFIED, CESM_REGRIDDED)


class CoarseGridMode(str, Enum):
    """Accepted values for ``SurfaceForcing.coarse_grid_mode``."""

    AUTO = "auto"  # coarsen only when source is coarser than ROMS grid (default)
    ALWAYS = "always"  # always interpolate onto a factor-2 coarsened grid
    NEVER = "never"  # always use the full-resolution source


class RestoringForce(str, Enum):
    """Variables accepted in ``SurfaceForcing.restoring_forces``."""

    SSS = "sss"  # sea-surface salinity restoring (WOA or UNIFIED)


class ClimatologyMode(str, Enum):
    """Accepted values for ``RiverForcing.convert_to_climatology``."""

    NEVER = "never"
    IF_ANY_MISSING = "if_any_missing"  # default: compute if any months absent
    ALWAYS = "always"


class BgcInterpMethod(str, Enum):
    """Accepted values for ``bgc_interpolation_method`` on ``InitialConditions``
    and ``BoundaryForcing`` (roms-tools >=4). Selects the vertical interpolation
    used for BGC tracers.
    """

    DEPTH = "depth"  # default: linear interpolation in depth
    DENSITY = "density"  # linear interpolation in potential-density (isopycnal) space
    DENSITY_MLD = "density_mld"  # mixed-layer-depth-anchored density interpolation


class Prefill(str, Enum):
    """Accepted values for ``BoundaryForcing.prefill`` (roms-tools >=4): how to
    fill NaN (land/void) cells in the *source* before regridding. ``None`` (the
    default, expressed as an absent field) applies no source prefill.
    """

    LATERAL_FILL_2D = "2d_lateral_fill"  # legacy AMG Poisson fill (smoothest, slow)
    INVERSE_DIST = "inverse_dist"  # xESMF inverse-distance source fill
    NEAREST_S2D = "nearest_s2d"  # xESMF nearest-source fill
    NEAREST_NEIGHBOR = (
        "nearest_neighbor"  # cheap scipy distance-transform fill (no xESMF)
    )


class RegridMethod(str, Enum):
    """Accepted values for ``BoundaryForcing.regrid_method`` (roms-tools >=4):
    the horizontal regrid engine, chosen independently of ``prefill``.
    """

    AUTO = "auto"  # xESMF if installed, else scipy (default when unset)
    XESMF = "xesmf"  # force xESMF (raises if absent)
    SCIPY = "scipy"  # force scipy interp (byte-reproducible with prefill)


class ExtrapMethod(str, Enum):
    """Accepted values for ``BoundaryForcing.extrap_method`` (roms-tools >=4):
    xESMF destination extrapolation on the default (prefill=None) path. Ignored
    when ``prefill`` is set.
    """

    INVERSE_DIST = "inverse_dist"  # inverse-distance-weighted (effective default)
    NEAREST_S2D = "nearest_s2d"  # single nearest source point


class FillValues(str, Enum):
    """Accepted values for ``VolumeRelease.fill_values``."""

    AUTO = "auto"  # fill missing tracer concentrations with dataset defaults
    ZERO = "zero"  # fill missing tracer concentrations with zero


# --- Valid source names per object + type -----------------------------------
# Mirrors the dataset-registry dicts / if-elif chains in the installed roms-tools.


class PhysicsSurfaceSource(str, Enum):
    """Source names accepted by SurfaceForcing when type='physics'."""

    ERA5 = "ERA5"


class BgcSurfaceSource(str, Enum):
    """Source names accepted by SurfaceForcing when type='bgc'."""

    UNIFIED = "UNIFIED"
    CESM_REGRIDDED = "CESM_REGRIDDED"
    MBL_CO2 = "MBL_co2"


class RestoringSurfaceSource(str, Enum):
    """Source names accepted by SurfaceForcing when type='restoring'."""

    WOA = "WOA"
    UNIFIED = "UNIFIED"


class PhysicsBoundarySource(str, Enum):
    """Source names accepted by BoundaryForcing when type='physics'."""

    GLORYS = "GLORYS"


class BgcBoundarySource(str, Enum):
    """Source names accepted by BoundaryForcing when type='bgc'."""

    UNIFIED = "UNIFIED"
    CESM_REGRIDDED = "CESM_REGRIDDED"


class InitialConditionsSource(str, Enum):
    """Source names accepted by InitialConditions (physics)."""

    GLORYS = "GLORYS"


class BgcInitialConditionsSource(str, Enum):
    """Source names accepted by InitialConditions (bgc_source)."""

    UNIFIED = "UNIFIED"
    CESM_REGRIDDED = "CESM_REGRIDDED"


class TidalSource(str, Enum):
    """Source names accepted by TidalForcing."""

    TPXO = "TPXO"


class RiverSource(str, Enum):
    """Source names accepted by RiverForcing."""

    DAI = "DAI"


class TopographySource(str, Enum):
    """Source names accepted by Grid (without a custom path)."""

    ETOPO5 = "ETOPO5"


# Top-level sections EXCLUDED from the integrity hash: provenance (where the hash
# lives), composition + identity (labels/provenance, not results-affecting), and the
# schema version. Everything else (application, run, domain, sources, properties,
# model_settings, code) is hashed.
_HASH_EXCLUDE = {"spec_config_version", "identity", "composition", "provenance"}
# Note: "properties" is no longer a top-level SpecConfig field (removed); n_tracers
# and marbl are derived from model_settings at processing time.

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
    ensemble_id: int | None = None
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

    grid_kwargs: dict[str, Any]
    topography_source: TopographySource | str = TopographySource.ETOPO5
    # str fallback allows a custom path dict to be passed through grid_kwargs
    open_boundaries: OpenBoundaries
    partitioning: Partitioning
    grid_kwargs_parent: dict[str, Any] | None = None
    grid_kwargs_child: dict[str, Any] | None = None
    metadata_child: dict[str, Any] | None = None
    nesting_include_pressure_fluxes: bool = False
    """Whether to include baroclinic pressure fluxes in the nesting extraction file
    (passed to make_nesting_info / make_edata as include_pressure_fluxes)."""


# ===========================================================================
# B. Forcing & source data
# ===========================================================================
class SourceSpec(_Section):
    """A forcing source item as the user authors it.

    ``name`` is the logical/friendly name (e.g. ``"GLORYS"``, ``"ERA5"``,
    ``"UNIFIED"``). The resolved registry key (``"GLORYS_REGIONAL"``,
    ``"UNIFIED_BGC"``, …) is derivable at any time via
    :func:`cstar_forge.source_registry.resolve_dataset_key(name, glorys_layout)`
    and is not stored here — the necessary disambiguation is already carried by
    ``glorys_layout``. The canonical registry snapshot lives in
    ``Forcing.resolved_datasets`` (keyed by logical name → ``ResolvedDataset``).
    """

    name: str
    climatology: bool = False
    glorys_layout: str | None = None  # "regional" | "global"


class SurfaceForcingItem(_Section):
    source: SourceSpec
    type: SurfaceType = SurfaceType.PHYSICS
    correct_radiation: bool = False
    coarse_grid_mode: CoarseGridMode = CoarseGridMode.AUTO
    restoring_forces: list[RestoringForce] | None = None
    wind_dropoff: bool = False  # coastal wind-speed reduction


class BoundaryForcingItem(_Section):
    source: SourceSpec
    type: BoundaryType = BoundaryType.PHYSICS
    bgc_interpolation_method: BgcInterpMethod = (
        BgcInterpMethod.DEPTH
    )  # BGC vertical interp (type='bgc')
    prefill: Prefill | None = None  # source NaN prefill before regridding
    prefill_kwargs: dict[str, Any] | None = None
    regrid_method: RegridMethod | None = None  # horizontal regrid engine (None -> auto)
    extrap_method: ExtrapMethod | None = (
        None  # destination extrapolation (default path)
    )
    extrap_kwargs: dict[str, Any] | None = None


class TidalForcingItem(_Section):
    source: SourceSpec
    ntides: int | None = None


class RiverForcingItem(_Section):
    source: SourceSpec
    include_bgc: bool = False
    convert_to_climatology: ClimatologyMode = ClimatologyMode.IF_ANY_MISSING
    bgc_source: dict[str, Any] | None = None  # separate river-BGC dataset config
    coast_snap_buffer_km: float | None = (
        None  # override coastal snap buffer (km); None -> dataset default
    )
    domain_edge_buffer: int = (
        20  # grid cells beyond domain edge kept in the bounding-box pre-filter
    )


class InitialConditions(_Section):
    source: SourceSpec
    bgc_source: SourceSpec | None = None
    bgc_interpolation_method: BgcInterpMethod = (
        BgcInterpMethod.DEPTH
    )  # BGC vertical interp
    allow_flex_time: bool = False  # ±24h search window around ini_time


class ResolvedDataset(_Section):
    """A snapshot of how a logical source resolves — frozen from the hardcoded
    registry so the processing host uses exactly these IDs/URLs.
    """

    dataset_key: str
    dataset_id: str | None = None
    url: str | None = None
    streamable: bool = False


class Forcing(_Section):
    """All forcing inputs: initial conditions, surface / boundary / tidal / river
    forcing items, optional CDR, and the resolved dataset registry snapshot.

    The former ``Sources`` / inner ``Forcing`` two-level nesting is gone — the
    items are flat here under a single ``forcing:`` key in the YAML.
    """

    initial_conditions: InitialConditions
    surface: list[SurfaceForcingItem] = Field(default_factory=list)
    boundary: list[BoundaryForcingItem] = Field(default_factory=list)
    tidal: list[TidalForcingItem] = Field(default_factory=list)
    river: list[RiverForcingItem] = Field(default_factory=list)
    cdr_forcing: dict[str, Any] | None = None
    # logical-name -> resolved registry entry (snapshot of source_data.py tables)
    resolved_datasets: dict[str, ResolvedDataset] = Field(default_factory=dict)


# Back-compat alias: code that imported Sources can import Forcing instead.
Sources = Forcing


# ===========================================================================
# C. Code, templates
# ===========================================================================
class CodeRepo(_Section):
    location: str
    commit: str | None = None
    branch: str | None = None


class TemplateRepo(CodeRepo):
    """A template source pulled from a repo — like ``code.roms`` / ``code.marbl``
    (``location`` + one of ``commit`` / ``branch``) but with a file filter: the
    ``files`` to pull, optionally under an in-repo ``directory``.
    """

    directory: str | None = None
    files: list[str] = Field(default_factory=list)


class Code(_Section):
    roms: CodeRepo
    marbl: CodeRepo | None = None
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

    name: str | None = None  # catalog entry name, or None if authored from scratch
    origin: str = "custom"  # "catalog" | "custom" | "model_default"
    modified: bool = False  # True if a catalog piece was edited after selection


class Composition(_Section):
    """The composable pieces selected to build this config. The resolved data lives
    in the sections above; this records provenance for review/UI.

    ``forcing`` corresponds to the ``sources`` section (initial conditions + surface/
    boundary/tidal/river + CDR).
    """

    model: PieceRef = Field(default_factory=PieceRef)
    domain: PieceRef = Field(default_factory=PieceRef)
    forcing: PieceRef = Field(default_factory=PieceRef)
    output: PieceRef = Field(
        default_factory=PieceRef
    )  # output-settings piece (OutputSpec)
    # Manual edits applied on top of the composed pieces: a sparse
    # {section: {field: value}} (or {section: scalar}) layer. ``model_settings`` already
    # reflects these; this records *which* values were overridden vs. composed/derived.
    overrides: dict[str, Any] = Field(default_factory=dict)


class Provenance(_Section):
    """Audit trail. Timestamps are passed in (never generated inside a resolver, to
    keep Phase 1 deterministic/reproducible).
    """

    generated_at: datetime | None = None
    forge_version: str | None = None
    roms_tools_version: str | None = None
    override_files_applied: list[str] = Field(default_factory=list)
    # sha256 of the results-affecting data (set on save by SpecConfig.to_yaml*).
    # Processing recomputes and compares it to detect hand-edits since write-out.
    content_hash: str | None = None
    notes: str | None = None


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
    application: str = (
        DEFAULT_APPLICATION  # which C-Star application consumes this blueprint
    )
    identity: Identity
    run: RunWindow
    domain: Domain
    forcing: Forcing
    model_settings: dict[str, Any] = Field(default_factory=dict)  # flat sections
    # n_tracers is NOT stored — it is derived at processing time as
    # model_settings["param"]["ntrc_bio"] + model_settings["param"]["nt_passive"] + 2
    # (temperature + salinity). marbl is read from model_settings["cppdefs"]["marbl"].
    code: Code
    composition: Composition = Field(default_factory=Composition)
    provenance: Provenance = Field(default_factory=Provenance)

    # ---- derived naming (single source of truth: identity + dates + n_procs) ----
    @property
    def n_procs(self) -> int:
        return self.domain.partitioning.n_procs_x * self.domain.partitioning.n_procs_y

    @property
    def name(self) -> str:
        base = (
            f"{self.identity.model_name}_{self.identity.grid_name}_{self.n_procs}procs"
        )
        if self.identity.ensemble_id is not None:
            base += f"_{self.identity.ensemble_id:03d}"
        return base

    @property
    def datestr(self) -> str:
        return f"{self.run.start_date:%Y%m%d}-{self.run.end_date:%Y%m%d}"

    @property
    def casename(self) -> str:
        return f"{self.name}_{self.datestr}"

    def run_output_dir(self, scratch: str | Path) -> Path:
        """Derived at processing time from the host scratch path: ``scratch/casename``."""
        return Path(scratch) / self.casename

    def output_root_name(self, scratch: str | Path) -> str:
        """Namelist ``output_root_name``, derived at processing time:
        ``<run_output_dir>/output/<casename>``.
        """
        return str(self.run_output_dir(scratch) / "output" / self.casename)

    # ---- integrity hash ----
    def content_hash(self) -> str:
        """sha256 over the *results-affecting* data (everything except the sections in
        ``_HASH_EXCLUDE``). Deterministic across a YAML round-trip — used to detect
        hand-edits between write-out and processing.
        """
        data = self.model_dump(mode="json")
        for key in _HASH_EXCLUDE:
            data.pop(key, None)
        blob = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ---- serialization ----
    def to_yaml(self, path: str | Path) -> Path:
        """Write the authoritative config to ``path`` and return it."""
        path = Path(path)
        path.write_text(self.to_yaml_str())
        return path

    def to_yaml_str(self) -> str:
        # stamp the integrity hash into provenance on the way out (the hash itself
        # excludes provenance, so this doesn't perturb it)
        stamped = self.model_copy(
            update={
                "provenance": self.provenance.model_copy(
                    update={"content_hash": self.content_hash()}
                )
            }
        )
        data = stamped.model_dump(mode="json", exclude_none=False)
        return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)

    @classmethod
    def from_yaml(cls, path: str | Path) -> SpecConfig:
        """Load and validate a ``spec_config.yml`` (Phase 2 entry point)."""
        data = yaml.safe_load(Path(path).read_text())
        version = (data or {}).get("spec_config_version")
        if version is not None and version > SPEC_CONFIG_VERSION:
            raise ValueError(
                f"spec_config_version {version} is newer than this build supports "
                f"({SPEC_CONFIG_VERSION}); upgrade cstar-forge to read this file."
            )
        return cls.model_validate(data)
