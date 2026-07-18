"""
``ForgeBlueprint``: the single authoritative, fully-resolved input to processing — the
forge application's blueprint. It is fully wired into ``ForgeExecutor`` (see
``cstar_forge.forge.executor.ForgeExecutor.from_forge_blueprint`` and
``cstar_forge.forge.forge_blueprint_engine.process_forge_blueprint``), split into two phases
(see ``docs/developer-guide.md``):

1. **Collection / curation** — assemble every option from its source (constructor
   args, the ModelSpec, and the *pure* derived values), validate it, and write one
   reviewable ``forge_blueprint.yaml`` (``cstar_forge.forge_blueprint_resolve.build_forge_blueprint``).
2. **Processing** — ingest that file on any machine and run the heavy work
   (``generate_inputs`` + ``configure_build``).

``ForgeBlueprint`` is the contract between the two phases: plain, validated data with
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
* **Naming** — the canonical ``name`` is a user-editable atomic input (``identity.name``),
  defaulting to a derived value (``{model_name}_{grid_name}_{n_procs}procs``) computed
  once by the resolver. ``casename``, the namelist ``title``, ``output_root_name``, and
  ``run_output_dir`` are deterministic functions of ``name`` + the run dates and are
  exposed as computed properties / helpers — never stored as independent values.
* **Artifacts** — ``s_coord`` (theta_s/theta_b/tcline, read from the generated grid
  file) and all file paths (grid / initial / forcing) are processing outputs and
  belong in the resulting blueprint, not here.

What IS stored: curated inputs (grid kwargs, partitioning, sources, code/template
pins) and the model settings — including the *pure-derived* numerics (timestep,
ntimes, v_sponge, param dims, obc flags) which carry scientific review value and may
be hand-edited before processing. Fixed implementation details (e.g. the ``cdr.nc`` /
``nesting.nc`` filenames, ``nrrec``, the tide flags set during generation) are NOT
stored — they are deterministic and set by the processing step.

"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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
    GLOFAS = "GLOFAS"


class TopographySource(str, Enum):
    """Source names accepted by Grid (without a custom path)."""

    ETOPO5 = "ETOPO5"


# Top-level sections EXCLUDED from the integrity hash: provenance (where the hash
# lives), composition + identity (labels/provenance, not results-affecting), and the
# schema version. Everything else (application, run, domain, sources, properties,
# model_settings, code) is hashed.
_HASH_EXCLUDE = {
    "forge_blueprint_version",
    "identity",
    "composition",
    "provenance",
    # host/location only — runtime-overridden per host; must not change the content hash.
    "working_dir",
}
# Note: "properties" is no longer a top-level ForgeBlueprint field (removed); n_tracers
# and marbl are derived from model_settings at processing time.

# Bumped only on a BREAKING schema change. Additive fields (with defaults) are
# backward-compatible — old files still load — so they do NOT bump this. ``from_yaml``
# rejects files declaring a *newer* version than this build understands.
#
# v3 (2026-07): ``identity`` dropped ``model_name``/``grid_name``/``ensemble_id`` in
# favor of a single user-editable ``name``; ``grid_name`` moved onto ``domain`` (it is
# results-affecting -- SourceData keys cache filenames off it -- so it belongs in the
# hashed section, not the excluded ``identity`` block). ``from_yaml`` migrates v2 files.
FORGE_BLUEPRINT_VERSION = 3

# Identifies the C-Star application that CONSUMES this blueprint — i.e. the "forge"
# application (this processing engine), whose blueprint IS the ForgeBlueprint. Do not confuse
# with the downstream roms_marbl application (whose blueprint this run *emits*). Stable
# across schema/field iteration; used by C-Star to route the blueprint to its application.
DEFAULT_APPLICATION = "forge"

# Default per-run artifact root. The bare root (no run-name subdirectory) is the
# spec-default sentinel: ForgeBlueprint expands it to ``<root>/<name>`` on validation,
# and host providers (Forge's ``config.resolve_host``; eventually C-Star) may rebase
# default-form paths onto host scratch at run time.
DEFAULT_WORKING_ROOT = "~/cstar-forge-data"

_NAME_UNSAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")
_NAME_RUN_RE = re.compile(r"[_.-]{2,}")


def sanitize_name(raw: str) -> str:
    """Normalize a free-form blueprint ``name`` into a filesystem/URL-safe token.

    Used for both user-supplied names (the wizard's editable Export field, a
    hand-edited YAML) and the resolver's derived default, so the two are
    idempotent with each other. The result feeds ``working_dir``, ``casename``,
    ``B_{name}.yaml``, and netCDF filename stems -- keep the charset conservative
    ([A-Za-z0-9._-]); anything else collapses to a single ``_``.
    """
    s = _NAME_UNSAFE_RE.sub("_", raw.strip())
    s = _NAME_RUN_RE.sub(lambda m: m.group(0)[0], s).strip("_.-")
    if not s:
        raise ValueError(f"name {raw!r} is empty after sanitization")
    return s


def migrate_forge_blueprint_data(data: dict[str, Any] | None) -> dict[str, Any]:
    """Version-check + forward-migrate a parsed ``forge_blueprint.yaml`` dict.

    Rejects a file declaring a *newer* version than this build understands. For
    v<=2 files (pre-v3 ``identity`` shape: ``model_name``/``grid_name``/
    ``ensemble_id`` instead of a single ``name``), rewrites ``identity`` to the v3
    shape and moves ``grid_name`` onto ``domain``, reproducing the exact old
    derived name (including the ``_{ensemble_id:03d}`` suffix) so ``name``/
    ``casename``/``working_dir`` are preserved bit-for-bit for existing files.

    Note: a migrated file's *recorded* ``provenance.content_hash`` was computed
    without ``domain.grid_name`` in the hashed data, so it no longer matches the
    recomputed hash post-migration -- ``verify_content_hash`` only warns on a
    mismatch, and re-saving via ``to_yaml`` recomputes the hash.
    """
    data = dict(data or {})
    version = data.get("forge_blueprint_version")
    if version is not None and version > FORGE_BLUEPRINT_VERSION:
        raise ValueError(
            f"forge_blueprint_version {version} is newer than this build supports "
            f"({FORGE_BLUEPRINT_VERSION}); upgrade cstar-forge to read this file."
        )
    if version is not None and version >= 3:
        return data
    identity = dict(data.get("identity") or {})
    model_name = identity.get("model_name")
    grid_name = identity.get("grid_name")
    if model_name is None or grid_name is None:
        # Already v3-shaped identity (e.g. a hand-authored dict without a version
        # stamp) -- nothing to migrate.
        data["forge_blueprint_version"] = FORGE_BLUEPRINT_VERSION
        return data
    ensemble_id = identity.get("ensemble_id")
    domain = dict(data.get("domain") or {})
    partitioning = domain.get("partitioning") or {}
    n_procs = int(partitioning.get("n_procs_x", 1)) * int(
        partitioning.get("n_procs_y", 1)
    )
    name = f"{model_name}_{grid_name}_{n_procs}procs"
    if ensemble_id is not None:
        name += f"_{int(ensemble_id):03d}"
    domain["grid_name"] = grid_name
    data["domain"] = domain
    data["identity"] = {
        "name": sanitize_name(name),
        "description": identity.get("description", "Generated blueprint"),
    }
    data["forge_blueprint_version"] = FORGE_BLUEPRINT_VERSION
    return data


class _Section(BaseModel):
    # Strict by default: an unknown key is a bug in the resolver or a stale file.
    model_config = ConfigDict(extra="forbid")


# ===========================================================================
# Identity (atomic naming inputs only) & run window
# ===========================================================================
class Identity(_Section):
    """The blueprint's canonical name + human description.

    ``name`` is the single source of truth for ``ForgeBlueprint.name``: everything
    else derived from it (``casename``, namelist ``title``, ``output_root_name``,
    ``run_output_dir``, the default ``working_dir``, ``B_{name}.yaml``) is a
    deterministic function -- see :class:`ForgeBlueprint` properties. The resolver
    computes a sensible default (``{model_name}_{grid_name}_{n_procs}procs``) but a
    user may override it; ``model_name``/``grid_name`` themselves live in
    ``composition.model.name``/``domain.grid_name`` (they are provenance/functional
    inputs, not naming inputs, once ``name`` is stored directly).
    """

    name: str  # canonical blueprint name (user-editable; defaults to a derived value)
    description: str = "Generated blueprint"

    @field_validator("name")
    @classmethod
    def _sanitize(cls, v: str) -> str:
        return sanitize_name(v)


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

    grid_name: (
        str  # e.g. "test-tiny" -- results-affecting (SourceData cache keys off it)
    )
    grid_kwargs: dict[str, Any]
    topography_source: TopographySource | str = TopographySource.ETOPO5
    # str fallback allows a custom path dict to be passed through grid_kwargs
    topography_path: str | None = None
    """Explicit path to a custom topography file. ``None`` (the default) means the
    executor derives it: staged from :class:`SourceData` for non-ETOPO5 sources, or
    fetched by roms-tools itself for ETOPO5. Set this to point at a non-default file."""
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
    :func:`cstar_forge.forge.source_registry.resolve_dataset_key(name, glorys_layout)`
    and is not stored here — the necessary disambiguation is already carried by
    ``glorys_layout``. The canonical registry snapshot lives in
    ``Forcing.resolved_datasets`` (keyed by logical name → ``ResolvedDataset``).
    """

    name: str
    climatology: bool = False
    glorys_layout: Literal["global", "regional"] | None = None
    path: str | None = None
    """Explicit dataset path override. ``None`` (the default) means the path is
    derived from :class:`SourceData` at processing time (the standard staged/streamed
    location). Set this only to point at a non-default local file."""

    @model_validator(mode="after")
    def _glorys_layout_only_for_glorys(self) -> SourceSpec:
        if self.glorys_layout is not None and self.name.upper() != "GLORYS":
            raise ValueError("glorys_layout is only valid when name is GLORYS")
        return self


# The sanctioned escape hatch: raw roms-tools constructor kwargs that have NOT (yet)
# been promoted to typed fields on these item models. Merged in
# ``RomsMarblInputData._build_input_args`` AFTER the typed defaults and BEFORE the
# run-time injections (``extra``). Lets a new roms-tools parameter be operated
# end-to-end with no schema change; promote it to a typed field later for validation,
# UI, and discoverability. ``cstar_forge.models`` re-exports these item models rather
# than redefining them; single-sourcing here is enforced by
# ``tests/test_roms_tools_coverage.py::test_forge_item_models_are_single_sourced``.
_OPTIONS_HELP = (
    "Raw roms-tools constructor kwargs not promoted to typed fields; merged after the "
    "typed defaults and before run-time injections. The sanctioned escape hatch for "
    "operating a new roms-tools parameter without a schema change."
)


class SurfaceForcingItem(_Section):
    source: SourceSpec
    type: SurfaceType = SurfaceType.PHYSICS
    correct_radiation: bool = False
    coarse_grid_mode: CoarseGridMode = CoarseGridMode.AUTO
    restoring_forces: list[RestoringForce] | None = None
    wind_dropoff: bool = False  # coastal wind-speed reduction
    options: dict[str, Any] = Field(default_factory=dict, description=_OPTIONS_HELP)


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
    options: dict[str, Any] = Field(default_factory=dict, description=_OPTIONS_HELP)


class TidalForcingItem(_Section):
    source: SourceSpec
    ntides: int | None = None
    options: dict[str, Any] = Field(default_factory=dict, description=_OPTIONS_HELP)


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
    options: dict[str, Any] = Field(default_factory=dict, description=_OPTIONS_HELP)


class InitialConditions(_Section):
    source: SourceSpec
    bgc_source: SourceSpec | None = None
    bgc_interpolation_method: BgcInterpMethod = (
        BgcInterpMethod.DEPTH
    )  # BGC vertical interp
    allow_flex_time: bool = False  # ±24h search window around ini_time
    options: dict[str, Any] = Field(default_factory=dict, description=_OPTIONS_HELP)


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
    pio: CodeRepo | None = None
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
    # sha256 of the results-affecting data (set on save by ForgeBlueprint.to_yaml*).
    # Processing recomputes and compares it to detect hand-edits since write-out.
    content_hash: str | None = None
    notes: str | None = None


# ===========================================================================
# Top-level authoritative config
# ===========================================================================
class ForgeBlueprint(_Section):
    """The complete, sufficient, reviewable input to processing.

    Round-trips to a single ``forge_blueprint.yaml`` via :meth:`to_yaml` / :meth:`from_yaml`.

    ``model_settings`` is a FLAT mapping of settings sections: ``cppdefs`` (compile
    time) sits at the same level as every namelist section (``lateral_visc``,
    ``bottom_drag``, ``param``, ``ocean_vars``, ``time_stepping``, ``v_sponge``, …).
    It deliberately OMITS the sections that are filled at processing time:
    ``title`` and ``output_root_name`` (derived from identity + host scratch path),
    ``s_coord`` (read from the generated grid), and ``grid`` / ``initial`` /
    ``forcing`` (artifact file paths). Validate ``model_settings`` through
    ``cstar_forge.forge.namelist_model.RunTimeSettings`` (after the processing step fills
    the omitted sections) before writing the namelist.
    """

    forge_blueprint_version: int = FORGE_BLUEPRINT_VERSION
    application: str = (
        DEFAULT_APPLICATION  # which C-Star application consumes this blueprint
    )
    # Per-run artifact root: everything the executor PRODUCES (input netCDFs, namelist,
    # cppdefs, the emitted roms_marbl blueprint, build dirs) lands under here. Stored with a
    # sensible default but OVERRIDDEN at runtime by C-Star / the Forge executor for the host.
    # Host/location only -> excluded from content_hash (see _HASH_EXCLUDE).
    # The bare root is a sentinel: a validator expands it to ``<root>/<name>`` so each
    # run gets its own subdirectory (see ``_default_working_dir_includes_name``).
    working_dir: str = DEFAULT_WORKING_ROOT
    identity: Identity
    run: RunWindow
    domain: Domain
    forcing: Forcing
    # Resolved list of host-independent source-dataset keys the executor must prepare
    # (forcing/IC sources + topography), e.g. ["GLORYS_REGIONAL", "UNIFIED_BGC", "ETOPO5"].
    # Cache paths resolve at processing from the injected source_data_cache. Results-affecting
    # -> stays IN the hash.
    datasets: list[str] = Field(default_factory=list)
    model_settings: dict[str, Any] = Field(default_factory=dict)  # flat sections
    # n_tracers is NOT stored — it is derived at processing time as
    # model_settings["param"]["ntrc_bio"] + model_settings["param"]["nt_passive"] + 2
    # (temperature + salinity). marbl is read from model_settings["cppdefs"]["marbl"].
    code: Code
    composition: Composition = Field(default_factory=Composition)
    provenance: Provenance = Field(default_factory=Provenance)

    @model_validator(mode="after")
    def _default_working_dir_includes_name(self) -> ForgeBlueprint:
        """Expand a bare default ``working_dir`` to include the run name.

        Only the sentinel (the bare ``DEFAULT_WORKING_ROOT``, as stored by older files
        or an unset field) is expanded to ``<root>/<name>``; any other value is a
        deliberate choice and passes through untouched.
        """
        if self.working_dir.rstrip("/") == DEFAULT_WORKING_ROOT:
            self.working_dir = f"{DEFAULT_WORKING_ROOT}/{self.name}"
        return self

    # ---- derived naming (single source of truth: identity.name + dates) ----
    @property
    def n_procs(self) -> int:
        return self.domain.partitioning.n_procs_x * self.domain.partitioning.n_procs_y

    @property
    def n_tracers(self) -> int:
        """Total ROMS tracer count = T + S + BGC (ntrc_bio) + passive (nt_passive),
        derived from ``model_settings['param']``.
        """
        param = self.model_settings.get("param", {}) or {}
        return 2 + int(param.get("ntrc_bio", 0)) + int(param.get("nt_passive", 0))

    @property
    def name(self) -> str:
        """The stored, authoritative canonical name (``identity.name``). The
        resolver computes its default (``{model_name}_{grid_name}_{n_procs}procs``)
        once at build time; this property no longer re-derives it.
        """
        return self.identity.name

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
        # ``location`` (the fetch address — a git URL or, in tests, a local path) is
        # host/transport, not content: the same commit checked out from a different
        # remote (or a local mirror) must hash identically. Only commit/branch/
        # directory/files are results-affecting, so scrub `location` from each code
        # repo before hashing.
        code = data.get("code")
        if code:
            for repo_key in (
                "roms",
                "marbl",
                "pio",
                "templates_compile_time",
                "templates_run_time",
            ):
                repo = code.get(repo_key)
                if repo:
                    repo.pop("location", None)
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
    def from_yaml(cls, path: str | Path) -> ForgeBlueprint:
        """Load and validate a ``forge_blueprint.yaml`` (Phase 2 entry point)."""
        data = yaml.safe_load(Path(path).read_text())
        return cls.model_validate(migrate_forge_blueprint_data(data))

    @classmethod
    def from_yaml_data(cls, data: dict[str, Any]) -> ForgeBlueprint:
        """Validate an already-parsed dict (e.g. a browser upload), applying the
        same version check + migration as :meth:`from_yaml`.
        """
        return cls.model_validate(migrate_forge_blueprint_data(data))
