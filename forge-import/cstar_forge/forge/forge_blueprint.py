"""
``ForgeBlueprint``: the single authoritative, fully-resolved input to processing — the
forge application's blueprint. It is fully wired into ``ForgeExecutor`` (see
``cstar_forge.forge.executor.ForgeExecutor.from_forge_blueprint`` and
``cstar_forge.forge.forge_blueprint_engine.process_forge_blueprint``), split into two phases
(see ``docs/architecture-details.md``):

1. **Collection / curation** — assemble every option from its source (constructor
   args, the ModelSpec, and the *pure* derived values), validate it, and write one
   reviewable ``forge_blueprint.yaml`` (``cstar_forge.forge_blueprint_resolve.build_forge_blueprint``).
2. **Processing** — ingest that file on any machine and run the heavy work
   (``generate_inputs`` + ``configure_build``).

``ForgeBlueprint`` is the contract between the two phases: plain, validated data with
**no** ``rt.Grid`` objects, **no** source downloads, and **no** file I/O.

``ForgeBlueprint`` subclasses ``cstar.orchestration.models.Blueprint`` (see
``cstar/applications/hello_world.py`` for the minimal shape of this contract), which is
what makes forge a real C-Star application: ``cstar_forge.forge.app.ForgeRunner`` +
``ForgeApplication`` (registered through the ``cstar.applications`` entry point
cstar-forge declares) let C-Star's own entrypoint (``cstar blueprint run``) discover
and drive it directly.

Single governing principle
--------------------------
The config stores ONLY host-independent, single-source-of-truth inputs. Anything
mechanically derivable is computed at **processing** time, never stored:

* **Host/machine** — the machine tag, account, queues, ``pes_per_node``, and every
  data path (source_data / input_data / scratch / catalog) are resolved at
  processing time from ``cstar_forge.config`` on the machine that runs the work.
  ``run_output_dir`` and the namelist ``output_root_name`` (which embed the scratch
  path) are therefore derived there too.
* **Naming** — the canonical ``name`` is a user-editable atomic input (required by the
  ``Blueprint`` base), defaulting to a derived value
  (``{model_name}_{grid_name}_{n_procs}procs``) computed once by the resolver.
  ``casename``, the namelist ``title``, ``output_root_name``, and ``run_output_dir`` are
  deterministic functions of ``name`` + the run dates and are exposed as computed
  properties / helpers — never stored as independent values.
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
import math
import re
import subprocess
from datetime import UTC, datetime
from enum import Enum
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Any, Literal, get_args

import yaml
from cstar.orchestration.models import Blueprint
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Repo root anchor for ``_forge_version()`` -- three levels up from this file
# (cstar_forge/forge/forge_blueprint.py -> forge/ -> cstar_forge/ -> repo root).
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _forge_version() -> str | None:
    """Best-effort identifier for the Forge code that saved this blueprint -- lets a
    later reader know which Forge commit to check out to reproduce it.

    Prefers ``git describe`` on this file's own checkout (a ``-dirty`` suffix flags
    uncommitted changes at save time); Forge's own package version is static
    (unlike ``cstar-ocean``/``roms-tools``, it carries no ``setuptools_scm`` commit
    info), so a wheel/PyPI install without a ``.git`` directory falls back to that
    static version, and no install info at all falls back to ``None``. Never
    raises -- this is provenance, not a dependency.
    """
    if (_REPO_ROOT / ".git").exists():
        try:
            result = subprocess.run(
                ["git", "describe", "--always", "--dirty"],
                cwd=_REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=2,
                check=True,
            )
            return result.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
    try:
        return f"cstar-forge=={_pkg_version('cstar-forge')}"
    except PackageNotFoundError:
        return None


def _installed_version(package_name: str) -> str | None:
    """Best-effort installed version of ``package_name``, or ``None`` if it isn't
    installed. Unlike ``_forge_version``, no separate git-describe step is needed:
    both ``cstar-ocean`` and ``roms-tools`` version themselves via
    ``setuptools_scm``, so an editable/dev checkout's installed version already
    embeds commit info (e.g. ``0.8.1.dev2+gcb931baef``). Never raises.
    """
    try:
        return f"{package_name}=={_pkg_version(package_name)}"
    except PackageNotFoundError:
        return None


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
    """Accepted values for ``prefill`` on ``SurfaceForcing``, ``BoundaryForcing``,
    ``TidalForcing``, and ``InitialConditions`` (roms-tools >=4): how to fill NaN
    (land/void) cells in the *source* before regridding. ``None`` (the default,
    expressed as an absent field) applies no source prefill.
    """

    LATERAL_FILL_2D = "2d_lateral_fill"  # legacy AMG Poisson fill (smoothest, slow)
    INVERSE_DIST = "inverse_dist"  # xESMF inverse-distance source fill
    NEAREST_S2D = "nearest_s2d"  # xESMF nearest-source fill
    NEAREST_NEIGHBOR = (
        "nearest_neighbor"  # cheap scipy distance-transform fill (no xESMF)
    )


class RegridMethod(str, Enum):
    """Accepted values for ``regrid_method`` on ``SurfaceForcing``,
    ``BoundaryForcing``, ``TidalForcing``, and ``InitialConditions`` (roms-tools
    >=4): the horizontal regrid engine, chosen independently of ``prefill``.
    """

    AUTO = "auto"  # xESMF if installed, else scipy (default when unset)
    XESMF = "xesmf"  # force xESMF (raises if absent)
    SCIPY = "scipy"  # force scipy interp (byte-reproducible with prefill)


class ExtrapMethod(str, Enum):
    """Accepted values for ``extrap_method`` on ``SurfaceForcing``,
    ``BoundaryForcing``, ``TidalForcing``, and ``InitialConditions`` (roms-tools
    >=4): xESMF destination extrapolation on the default (prefill=None) path.
    Ignored when ``prefill`` is set.
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
    CUSTOM_FILE = "CUSTOM_FILE"  # user-supplied pre-made river forcing netCDF


class RiverBgcSource(str, Enum):
    """Source names accepted by RiverForcing's ``bgc_source`` (river biogeochemistry)."""

    CONSTANTS = "CONSTANTS"
    RIVR2O = "RIVR2O"


class TopographySource(str, Enum):
    """Source names accepted by Grid (without a custom path)."""

    ETOPO5 = "ETOPO5"
    SRTM15 = "SRTM15"
    EMOD = "EMOD"


# Top-level sections EXCLUDED from the integrity hash: provenance (where the hash
# lives), composition + name/description (labels/provenance, not results-affecting),
# and the schema version/state (Blueprint-base metadata, not content). Everything else
# (application, run, domain, sources, properties, model_settings, code) is hashed.
_HASH_EXCLUDE = {
    "forge_blueprint_version",
    "name",
    "description",
    "composition",
    "provenance",
    # host/location only — runtime-overridden per host; must not change the content hash.
    "working_dir",
    # Blueprint-base (cstar.orchestration.models) metadata, not blueprint content.
    "state",
    "schema_version",
    # injected only by Blueprint's serializer (never a real field) -- pop unconditionally.
    "$schema",
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
# v4 (2026-07): ``ForgeBlueprint`` became a ``cstar.orchestration.models.Blueprint``
# subclass, which requires top-level ``name``/``description`` fields; the ``identity``
# sub-model is gone, flattened onto the blueprint directly. Migration folded into a
# ``model_validator(mode="before")`` so it fires on every entry point (C-Star's own
# ``deserialize``/``model_validate`` included), not just ``from_yaml``.
# v5 (2026-08): ``model_settings.cdr_output.do_cdr`` renamed ``do_cdr_output`` to
# match the ROMS namelist key directly (CDR output became independently
# user-controllable, decoupled from CDR forcing -- see ``forge_blueprint_resolve``'s
# CDR-output consistency block).
# v6 (2026-08): user-provided input files. New optional ``UserProvidedFile``-typed
# fields -- ``domain.grid_file``, ``forcing.river[*].custom_file``,
# ``forcing.cdr_forcing_file`` -- let a user supply a pre-made netCDF instead of
# having Forge generate it. All additive (default ``None``); no migration beyond
# the version bump is needed for existing v5 files.
# v7 (2026-08): CDR overhaul. All CDR-forcing config moves out of ``forcing``
# into a new top-level ``cdr`` section (``CdrSpec``, with 5 modes -- see its
# docstring); ``forcing.cdr_forcing``/``forcing.cdr_forcing_file`` are gone.
# ``composition.cdr`` (a ``SpecRef``) records CDR's own catalog provenance,
# independent of ``composition.forcing``. Migration moves the two old
# ``forcing`` leaves onto ``cdr``, inferring ``mode`` from which one (if
# either) was populated.
FORGE_BLUEPRINT_VERSION = 7

# Identifies the C-Star application that CONSUMES this blueprint — i.e. the "forge"
# application (this processing engine), whose blueprint IS the ForgeBlueprint. Do not confuse
# with the downstream roms_marbl application (whose blueprint this run *emits*). Stable
# across schema/field iteration; used by C-Star to route the blueprint to its application.
DEFAULT_APPLICATION = "forge"

# Default per-run artifact root. The bare root (no run-name subdirectory) is the
# spec-default sentinel: ForgeBlueprint expands it to ``<root>/<name>`` on validation,
# and host providers (Forge's ``config.resolve_host``; eventually C-Star) may rebase
# default-form paths onto host scratch at run time.
DEFAULT_WORKING_ROOT = "~/cstar/_forge_bp_runs"

# Sibling root segment under the shared ``cstar/`` root (alongside
# DEFAULT_WORKING_ROOT's) for the emitted roms-marbl blueprint's working_dir; see
# ForgeExecutor.roms_blueprint_working_dir in executor.py.
ROMS_RUN_SEGMENT = "_roms_bp_runs"

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

    Rejects a file declaring a *newer* version than this build understands.

    **v2 -> v3**: pre-v3 ``identity`` shape (``model_name``/``grid_name``/
    ``ensemble_id`` instead of a single ``name``) is rewritten to a single derived
    name and ``grid_name`` is moved onto ``domain``, reproducing the exact old
    derived name (including the ``_{ensemble_id:03d}`` suffix) so ``name``/
    ``casename``/``working_dir`` are preserved bit-for-bit for existing files.

    **v3 -> v4**: the ``identity`` sub-model (``name``/``description``) is flattened
    onto the blueprint's own top-level ``name``/``description`` fields -- required by
    ``ForgeBlueprint``'s ``cstar.orchestration.models.Blueprint`` base, which declares
    ``name``/``description`` as its own top-level fields.

    **v4 -> v5**: ``model_settings.cdr_output.do_cdr`` is renamed ``do_cdr_output``
    (matches the ROMS namelist key; ``CdrOutputCfg`` also carries a
    ``validation_alias`` for this spelling, but the migration keeps the on-disk
    dict itself current).

    **v5 -> v6**: no-op beyond the version bump -- the new user-provided-file
    fields (``domain.grid_file``, ``forcing.river[*].custom_file``,
    ``forcing.cdr_forcing_file``) are purely additive and default to ``None``, so
    a v5 file already validates against the v6 schema unchanged.

    **v6 -> v7**: ``forcing.cdr_forcing``/``forcing.cdr_forcing_file`` (if
    present) are popped and moved onto a new top-level ``cdr`` dict, with
    ``mode`` inferred from which one (if either) was populated: a non-null
    ``cdr_forcing`` dict -> ``"yaml"``; else a non-null ``cdr_forcing_file`` ->
    ``"netcdf"``; else -> ``"none"``. A no-op when ``cdr`` is already present
    (e.g. direct keyword construction passing ``cdr=`` explicitly -- never
    overwrite an explicit value with an inferred one).

    Idempotent and a no-op on already-current data (e.g. direct keyword
    construction, ``ForgeBlueprint(name=..., ...)``) -- called automatically from a
    ``model_validator(mode="before")`` so it fires on every entry point
    (``from_yaml``, ``from_yaml_data``, and C-Star's own ``deserialize``/
    ``model_validate``), not just ``from_yaml``.

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

    if version is None or version < 3:
        identity = dict(data.get("identity") or {})
        model_name = identity.get("model_name")
        grid_name = identity.get("grid_name")
        if model_name is not None and grid_name is not None:
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
        # else: already v3-shaped identity (or no identity at all) -- nothing to do.

    if version is None or version < 4:
        identity = data.pop("identity", None)
        if identity is not None:
            data.setdefault("name", identity.get("name"))
            data.setdefault(
                "description", identity.get("description", "Generated blueprint")
            )

    if version is None or version < 5:
        model_settings = data.get("model_settings")
        if isinstance(model_settings, dict):
            cdr_output = model_settings.get("cdr_output")
            if isinstance(cdr_output, dict) and (
                "do_cdr" in cdr_output and "do_cdr_output" not in cdr_output
            ):
                cdr_output["do_cdr_output"] = cdr_output.pop("do_cdr")

    if (version is None or version < 7) and "cdr" not in data:
        forcing = data.get("forcing")
        if isinstance(forcing, dict):
            old_cdr_forcing = forcing.pop("cdr_forcing", None)
            old_cdr_forcing_file = forcing.pop("cdr_forcing_file", None)
            # Mode inference shared with the resolver's convenience kwargs and
            # ForgeExecutor's direct-construction back-compat (defined below,
            # next to CdrSpec) -- the three entry points can't drift apart.
            data["cdr"] = {
                "mode": infer_cdr_mode(old_cdr_forcing, old_cdr_forcing_file)
            }
            if old_cdr_forcing is not None:
                data["cdr"]["cdr_forcing"] = old_cdr_forcing
            elif old_cdr_forcing_file is not None:
                data["cdr"]["cdr_forcing_file"] = old_cdr_forcing_file
        # else: ``forcing`` isn't a plain dict (e.g. an already-built ``Forcing``
        # instance passed via direct keyword construction) -- nothing to migrate
        # off of it; ``cdr`` stays absent so the field default (mode="none")
        # applies.

    data["forge_blueprint_version"] = FORGE_BLUEPRINT_VERSION
    return data


class _Section(BaseModel):
    # Strict by default: an unknown key is a bug in the resolver or a stale file.
    model_config = ConfigDict(extra="forbid")


class UserProvidedFile(_Section):
    """A user-supplied, pre-made netCDF used in place of one Forge would
    otherwise generate (grid / river forcing / CDR forcing -- see
    ``Domain.grid_file``, ``RiverForcingItem.custom_file``,
    ``CdrSpec.cdr_forcing_file``).

    ``location`` is a path on the machine that will run the executor -- it must
    exist there at processing time (a hard error if not: see
    ``cstar_forge.forge.user_files.verify_user_file``). It is host/transport, not
    results-affecting content, so it is excluded from :meth:`ForgeBlueprint.content_hash`.

    ``content_hash`` pins the file's *data content* (via
    ``cstar_forge.forge.user_files.hash_netcdf_contents``) as it was when the
    blueprint was authored. It IS results-affecting and stays in the content hash;
    a mismatch at processing time is a warning (the file may have been
    legitimately regenerated), not an error.
    """

    location: str
    content_hash: str


# The vertical-coordinate keys roms-tools accepts alongside ``filename=`` when
# loading a grid from an existing netCDF (``rt.Grid(filename=..., theta_s=...,
# theta_b=..., hc=..., N=...)``) -- must be passed all-or-none.
_GRID_FILE_VERT_KEYS = ("theta_s", "theta_b", "hc", "N")


def vert_kwargs_from_grid_kwargs(grid_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Extract the ``theta_s``/``theta_b``/``hc``/``N`` subset of ``grid_kwargs``
    for use alongside a user-supplied ``Domain.grid_file``
    (``rt.Grid(filename=..., **vert_kwargs_from_grid_kwargs(grid_kwargs))``).

    roms-tools requires these four passed all-or-none alongside ``filename`` --
    raises a clear ``ValueError`` here (before ever calling roms-tools) if 1-3 of
    them (not 0 or 4) are present. Returns ``{}`` when none are present, so
    roms-tools derives them from the file's own attrs (or its own defaults, with
    a warning, as a last resort).
    """
    present = {k: grid_kwargs[k] for k in _GRID_FILE_VERT_KEYS if k in grid_kwargs}
    if present and len(present) != len(_GRID_FILE_VERT_KEYS):
        missing = sorted(set(_GRID_FILE_VERT_KEYS) - present.keys())
        raise ValueError(
            "grid_file requires theta_s/theta_b/hc/N to be supplied all-or-none in "
            f"grid_kwargs (roms-tools' rule); missing {missing}, present "
            f"{sorted(present)}."
        )
    return present


# ===========================================================================
# Run window
# ===========================================================================
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
def estimate_forge_cpus(nx: int, ny: int, n_levels: int) -> int:
    """Ballpark CPU count for a forge run (input generation) on a grid this size.

    Deliberately imprecise -- roughly one CPU per 150k grid cells
    (``nx * ny * N``), with a floor of 16 and no upper cap here. The forge run is
    a single process (``ForgeBlueprint.single_node``), so the real ceiling is one
    node's worth of CPUs on the target partition -- which only the launcher
    knows (C-Star clamps the request to the queue's CPUs per node at submit
    time). Calibration anchors: a toy domain (20x20x10) gets the 16 floor;
    hvalfjordur-0 (512x384x100, ~2.0e7 cells) lands near a 128-core node; an
    exceptionally large domain (1856x960x100, ~1.8e8 cells) asks for far more
    than any node has and is clamped to a full node.
    """
    cells = nx * ny * n_levels
    return max(16, math.ceil(cells / 150_000))


class OpenBoundaries(_Section):
    north: bool = False
    south: bool = False
    east: bool = False
    west: bool = False


class Partitioning(_Section):
    n_procs_x: int | None = None
    n_procs_y: int | None = None
    auto_tiling: bool = False
    """Let ROMS pick NP_XI/NP_ETA at runtime from the land mask (ucla-roms 0.5.0's
    MPI_MASKING cppdef), instead of the fixed n_procs_x/n_procs_y decomposition.
    Requires PARALLEL_IO (use_pio)."""
    n_cores: int | None = None
    """Total number of cores to use when ``auto_tiling`` selects the tiling layout.
    Forge policy (stricter than C-Star's): always required alongside
    ``auto_tiling`` -- n_procs_x/n_procs_y are never combined with auto_tiling."""

    @model_validator(mode="after")
    def _validate_partitioning(self) -> Partitioning:
        violations: list[str] = []

        if not self.auto_tiling and (self.n_procs_x is None or self.n_procs_y is None):
            violations.append(
                "n_procs_x and n_procs_y are required unless auto_tiling is enabled"
            )

        if self.n_cores is not None and not self.auto_tiling:
            violations.append("n_cores is only accepted with auto_tiling")

        if self.auto_tiling and self.n_cores is None:
            violations.append("auto_tiling requires n_cores")

        if self.auto_tiling and (
            self.n_procs_x is not None or self.n_procs_y is not None
        ):
            violations.append(
                "n_procs_x/n_procs_y must not be set when auto_tiling is enabled"
            )

        if violations:
            raise ValueError("; ".join(violations))

        return self


class Domain(_Section):
    """Grid construction inputs (kwargs only — the ``rt.Grid`` is built at processing time).

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
    v_sponge: float | None = None
    """Sponge-layer viscosity. A first-class, domain-owned property (mirrors
    ``open_boundaries``): the resolver derives it from grid spacing
    (``cstar_forge.forge.util.compute_v_sponge_from_grid``) when not explicitly
    supplied, and is the sole writer of both this field and the identical
    ``model_settings["v_sponge"]["v_sponge"]`` leaf -- the two must never diverge."""
    dt: float | None = None
    """Baroclinic timestep (seconds). A first-class, domain-owned property (mirrors
    ``v_sponge``): the resolver derives it from the CFL criterion
    (``cstar_forge.forge.util``, via a grid build) when not explicitly supplied, and
    is the sole writer of both this field and the identical
    ``model_settings["time_stepping"]["dt"]`` leaf -- the two must never diverge."""
    grid_file: UserProvidedFile | None = None
    """A user-supplied pre-made grid netCDF, used in place of generating one from
    ``grid_kwargs``. When set, ``grid_kwargs`` must carry only the vertical-coord
    keys roms-tools still accepts alongside a ``filename`` (``theta_s``/
    ``theta_b``/``hc``/``N``) -- not the generation-geometry keys (see
    ``_grid_file_excludes_generation_geometry``) -- and nesting
    (``grid_kwargs_parent``/``grid_kwargs_child``) is unsupported (v1)."""

    @property
    def is_child(self) -> bool:
        """Whether this domain is nested inside a coarser parent grid."""
        return self.grid_kwargs_parent is not None

    @property
    def is_parent(self) -> bool:
        """Whether this domain is a parent that extracts nesting data for a child."""
        return self.grid_kwargs_child is not None

    @model_validator(mode="after")
    def _grid_file_excludes_generation_geometry(self) -> Domain:
        if self.grid_file is None:
            return self
        # Whitelist, not blacklist: everything except the vertical-coord keys is
        # silently dropped by the grid-file pathway (vert_kwargs_from_grid_kwargs
        # forwards only theta_s/theta_b/hc/N to rt.Grid(filename=...)), so a
        # generation-only key like mask_shapefile or hmin alongside grid_file is
        # a config that would never take effect -- reject it loudly instead.
        extras = set(self.grid_kwargs) - set(_GRID_FILE_VERT_KEYS)
        if extras:
            raise ValueError(
                "domain.grid_file is set (a user-supplied grid) but grid_kwargs "
                f"still carries generation-only keys {sorted(extras)}; these "
                "describe a grid to *build* and would be silently ignored once a "
                "grid file is supplied directly. Only theta_s/theta_b/hc/N are "
                "allowed alongside grid_file."
            )
        if self.grid_kwargs_parent is not None or self.grid_kwargs_child is not None:
            raise ValueError(
                "domain.grid_file (a user-supplied grid) cannot be combined with "
                "nesting (grid_kwargs_parent/grid_kwargs_child); custom grid + "
                "nesting is unsupported."
            )
        return self


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
    prefill: Prefill | None = None  # source NaN prefill before regridding
    prefill_kwargs: dict[str, Any] | None = None
    regrid_method: RegridMethod | None = None  # horizontal regrid engine (None -> auto)
    extrap_method: ExtrapMethod | None = (
        None  # destination extrapolation (default path)
    )
    extrap_kwargs: dict[str, Any] | None = None
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
    prefill: Prefill | None = None  # source NaN prefill before regridding
    prefill_kwargs: dict[str, Any] | None = None
    regrid_method: RegridMethod | None = None  # horizontal regrid engine (None -> auto)
    extrap_method: ExtrapMethod | None = (
        None  # destination extrapolation (default path)
    )
    extrap_kwargs: dict[str, Any] | None = None
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
    custom_file: UserProvidedFile | None = None
    """A user-supplied pre-made river-forcing netCDF, used in place of building
    river forcing from a ``DAI``/``GLOFAS`` source. Required iff
    ``source.name`` is ``RiverSource.CUSTOM_FILE`` (see
    ``_custom_file_matches_custom_source``); a custom file has no separate
    river-BGC dataset config, so ``bgc_source`` must be unset (see
    ``_custom_file_excludes_bgc_source``)."""

    @model_validator(mode="after")
    def _bgc_source_requires_include_bgc(self) -> RiverForcingItem:
        # roms-tools silently ignores `bgc_source` unless `include_bgc=True` (and
        # river BGC tracers additionally require MARBL compiled in — see the
        # `bgc_signals` cppdef flip in forge_blueprint_resolve.py). Catch the silent
        # no-op here rather than let a configured RIVR2O/CONSTANTS source vanish.
        if self.bgc_source is not None:
            if not self.include_bgc:
                raise ValueError(
                    "river bgc_source is set but include_bgc is False; roms-tools "
                    "ignores bgc_source unless include_bgc=True"
                )
            name = self.bgc_source.get("name")
            valid = {m.value for m in RiverBgcSource}
            if name is not None and str(name).upper() not in valid:
                raise ValueError(
                    f"river bgc_source name {name!r} is not one of {sorted(valid)}"
                )
        return self

    @model_validator(mode="after")
    def _custom_file_matches_custom_source(self) -> RiverForcingItem:
        is_custom_source = self.source.name.upper() == RiverSource.CUSTOM_FILE.value
        if is_custom_source and self.custom_file is None:
            raise ValueError(
                f"river source is {RiverSource.CUSTOM_FILE.value!r} but custom_file "
                "is not set; a user-provided river source requires custom_file"
            )
        if not is_custom_source and self.custom_file is not None:
            raise ValueError(
                "river custom_file is set but source is "
                f"{self.source.name!r}, not {RiverSource.CUSTOM_FILE.value!r}; "
                "custom_file is only valid with a CUSTOM_FILE source"
            )
        return self

    @model_validator(mode="after")
    def _custom_file_excludes_bgc_source(self) -> RiverForcingItem:
        if self.custom_file is not None and self.bgc_source is not None:
            raise ValueError(
                "river custom_file and bgc_source are mutually exclusive; a "
                "user-supplied river file has no separate river-BGC dataset config"
            )
        return self


class InitialConditions(_Section):
    source: SourceSpec
    bgc_source: SourceSpec | None = None
    bgc_interpolation_method: BgcInterpMethod = (
        BgcInterpMethod.DEPTH
    )  # BGC vertical interp
    allow_flex_time: bool = False  # ±24h search window around ini_time
    prefill: Prefill | None = None  # source NaN prefill before regridding
    prefill_kwargs: dict[str, Any] | None = None
    regrid_method: RegridMethod | None = None  # horizontal regrid engine (None -> auto)
    extrap_method: ExtrapMethod | None = (
        None  # destination extrapolation (default path)
    )
    extrap_kwargs: dict[str, Any] | None = None
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
    """All forcing inputs: initial conditions and surface / boundary / tidal /
    river forcing items, plus the resolved dataset registry snapshot.

    The former ``Sources`` / inner ``Forcing`` two-level nesting is gone — the
    items are flat here under a single ``forcing:`` key in the YAML. CDR forcing
    is NOT part of this section -- see the top-level ``cdr`` section
    (:class:`CdrSpec`) on ``ForgeBlueprint``, which carries CDR's own composable
    selection independently of the rest of forcing.
    """

    initial_conditions: InitialConditions | None = None
    """None only when the domain has a parent grid: child domains inherit state
    from the parent's nesting extraction, so IC is optional for them (a child
    may still provide one explicitly to override the inherited state)."""
    surface: list[SurfaceForcingItem] = Field(default_factory=list)
    boundary: list[BoundaryForcingItem] = Field(default_factory=list)
    tidal: list[TidalForcingItem] = Field(default_factory=list)
    river: list[RiverForcingItem] = Field(default_factory=list)
    # logical-name -> resolved registry entry (snapshot of source_data.py tables)
    resolved_datasets: dict[str, ResolvedDataset] = Field(default_factory=dict)


# Back-compat alias: code that imported Sources can import Forcing instead.
Sources = Forcing


# ===========================================================================
# B2. CDR (carbon dioxide removal) forcing -- a top-level, independently
#     composable section (its own catalog spec + Composition.cdr SpecRef),
#     NOT nested under Forcing. See CdrSpec.mode's docstring for the five modes.
# ===========================================================================
# The single source of truth for the CDR mode vocabulary: CdrSpec.mode's
# annotation below. CDR_MODES is derived from it for membership checks
# elsewhere (ForgeExecutor.cdr_mode validation, catalog register_cdr) so a
# future sixth mode is added in exactly one place.
CdrMode = Literal["none", "simple", "yaml", "netcdf", "upscaled"]
CDR_MODES: tuple[str, ...] = get_args(CdrMode)


def infer_cdr_mode(cdr_forcing: dict[str, Any] | None, cdr_forcing_file: Any) -> str:
    """Infer a CdrSpec mode from which (if either) CDR payload is populated.

    The shared pre-CdrSpec compatibility rule -- a bare ``cdr_forcing`` dict has
    "yaml" provenance (the only way one used to exist), a bare file is
    "netcdf", neither is "none". Used by the v6->v7 blueprint migration, the
    resolver's convenience-kwarg path, and ``ForgeExecutor``'s direct-construction
    backward compat, so the three entry points can never drift apart. "simple"
    is deliberately NOT inferable: a compiled dict's shape doesn't distinguish
    it from "yaml" (see ``CdrSpec.mode``'s docstring) -- it must be stated.
    """
    if cdr_forcing is not None:
        return "yaml"
    if cdr_forcing_file is not None:
        return "netcdf"
    return "none"


class CdrSpec(_Section):
    """The CDR-forcing selection: which of five mutually-exclusive modes drives
    ``model_settings["cdr_frc"]`` / the ``CDR_FORCING`` cppdef / the emitted
    roms_marbl blueprint's ``cdr_forcing`` dataset, and (for two of the modes)
    the CDR-forcing config itself.

    ``mode`` values:

    * ``"none"`` -- no CDR forcing at all. ``cdr_forcing``/``cdr_forcing_file``
      must both be unset.
    * ``"simple"`` -- a compiled roms-tools ``CDRForcing`` kwargs dict authored
      directly (e.g. hand-built or from a simpler UI form), carried in
      ``cdr_forcing``. ``cdr_forcing_file`` must be unset.
    * ``"yaml"`` -- the same compiled kwargs dict, but sourced from an uploaded
      roms-tools ``CDRForcing.to_yaml(...)`` dump (see
      ``forge_blueprint_resolve.read_cdr_forcing_yaml``). Carried in the same
      ``cdr_forcing`` field as ``"simple"`` -- the distinction is provenance
      (how the dict was authored), not shape; both are generated at processing
      time via ``rt.CDRForcing(**cdr_forcing)``. ``cdr_forcing_file`` must be
      unset.
    * ``"netcdf"`` -- a user-supplied, pre-made CDR-forcing netCDF used in place
      of generating one, carried in ``cdr_forcing_file``. ``cdr_forcing`` must
      be unset.
    * ``"upscaled"`` -- CDR tracers are supplied by C-Star's runtime orchestrator
      from an upscaled (e.g. multi-domain/ensemble) run, not generated by Forge
      at all. Both ``cdr_forcing``/``cdr_forcing_file`` must be unset; the
      resolver instead sets static ``model_settings["cdr_frc"]`` values (no
      generation step runs to derive them) and the engine emits a placeholder
      ``Resource`` into the roms_marbl blueprint's ``cdr_forcing`` dataset for
      C-Star's orchestrator to later replace with the real runtime path. Real
      CDR tracers exist at runtime under this mode, so it participates in the
      CDR-output consistency block (do_cdr_output/MARBL diagnostics/marbl
      requirement) exactly like ``"simple"``/``"yaml"``/``"netcdf"``.
    """

    mode: CdrMode = "none"
    cdr_forcing: dict[str, Any] | None = None
    """Compiled roms-tools ``CDRForcing`` constructor kwargs -- required (and the
    only populated field) for ``mode in ("simple", "yaml")``."""
    cdr_forcing_file: UserProvidedFile | None = None
    """A user-supplied pre-made CDR-forcing netCDF -- required (and the only
    populated field) for ``mode == "netcdf"``."""

    @model_validator(mode="after")
    def _fields_match_mode(self) -> CdrSpec:
        if self.mode in ("none", "upscaled"):
            if self.cdr_forcing is not None or self.cdr_forcing_file is not None:
                raise ValueError(
                    f"cdr.mode={self.mode!r} requires both cdr_forcing and "
                    "cdr_forcing_file to be unset (this mode carries no CDR "
                    "config of its own)."
                )
        elif self.mode in ("simple", "yaml"):
            if self.cdr_forcing is None:
                raise ValueError(
                    f"cdr.mode={self.mode!r} requires cdr_forcing to be set."
                )
            if self.cdr_forcing_file is not None:
                raise ValueError(
                    f"cdr.mode={self.mode!r} requires cdr_forcing_file to be unset "
                    "(mutually exclusive with cdr_forcing)."
                )
        elif self.mode == "netcdf":
            if self.cdr_forcing_file is None:
                raise ValueError(
                    "cdr.mode='netcdf' requires cdr_forcing_file to be set."
                )
            if self.cdr_forcing is not None:
                raise ValueError(
                    "cdr.mode='netcdf' requires cdr_forcing to be unset "
                    "(mutually exclusive with cdr_forcing_file)."
                )
        return self


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
# Composition (which catalog specs produced this config) & provenance
# ===========================================================================
class SpecRef(_Section):
    """Records where one composable spec (model / domain / forcing) came from.

    Supports the "pick from a catalog or build your own" workflow: a UI can show,
    for each spec, whether it was a catalog selection (and which one), whether the
    user edited it, or whether it was authored from scratch.
    """

    name: str | None = None  # catalog entry name, or None if authored from scratch
    origin: str = "custom"  # "catalog" | "custom" | "model_default"
    modified: bool = False  # True if a catalog spec was edited after selection


class Composition(_Section):
    """The composable specs selected to build this config. The resolved data lives
    in the sections above; this records provenance for review/UI.

    ``forcing`` corresponds to the ``sources`` section (initial conditions +
    surface/boundary/tidal/river). ``cdr`` corresponds to the top-level ``cdr``
    section (:class:`CdrSpec`) -- CDR is its own independently composable spec,
    not part of ``forcing``.
    """

    model: SpecRef = Field(default_factory=SpecRef)
    domain: SpecRef = Field(default_factory=SpecRef)
    forcing: SpecRef = Field(default_factory=SpecRef)
    cdr: SpecRef = Field(default_factory=SpecRef)  # CDR spec (CdrSpec)
    output: SpecRef = Field(
        default_factory=SpecRef
    )  # output-settings spec (OutputSpec)
    # Manual edits applied on top of the composed specs: a sparse
    # {section: {field: value}} (or {section: scalar}) layer. ``model_settings`` already
    # reflects these; this records *which* values were overridden vs. composed/derived.
    overrides: dict[str, Any] = Field(default_factory=dict)


class Provenance(_Section):
    """Audit trail. ``generated_at``/``forge_version``/``cstar_version``/
    ``roms_tools_version`` are never computed inside the resolver (to keep resolution
    deterministic/reproducible, and because ``roms_tools`` isn't guaranteed
    installed there) -- ``ForgeBlueprint.to_yaml_str`` stamps each on first save
    only (a later resave preserves the original value, same as an explicit
    constructor override); a caller may still pass one explicitly (e.g. carrying
    an original value forward through a re-resolve).
    """

    generated_at: datetime | None = None
    forge_version: str | None = None
    cstar_version: str | None = None
    roms_tools_version: str | None = None
    override_files_applied: list[str] = Field(default_factory=list)
    # sha256 of the results-affecting data (set on save by ForgeBlueprint.to_yaml*).
    # Processing recomputes and compares it to detect hand-edits since write-out.
    content_hash: str | None = None
    notes: str | None = None


# ===========================================================================
# Top-level authoritative config
# ===========================================================================
class ForgeBlueprint(Blueprint):
    """The complete, sufficient, reviewable input to processing -- and the forge
    C-Star application's own blueprint (see ``cstar.orchestration.models.Blueprint``,
    ``cstar_forge.forge.app.ForgeApplication``).

    Round-trips to a single ``forge_blueprint.yaml`` via :meth:`to_yaml` / :meth:`from_yaml`.

    ``model_settings`` is a FLAT mapping of settings sections: ``cppdefs`` (compile
    time) sits at the same level as every namelist section (``lateral_visc``,
    ``bottom_drag``, ``param``, ``ocean_vars``, ``time_stepping``, ``v_sponge``, …).
    It deliberately OMITS the sections that are filled at processing time:
    ``title`` and ``output_root_name`` (derived from name + host scratch path),
    ``s_coord`` (read from the generated grid), and ``grid`` / ``initial`` /
    ``forcing`` (artifact file paths). Validate ``model_settings`` through
    ``cstar_forge.forge.namelist_model.RunTimeSettings`` (after the processing step fills
    the omitted sections) before writing the namelist.
    """

    # Strict by default (overriding the ``Blueprint``/``ConfiguredBaseModel`` base's
    # extra="allow"): an unknown key is a bug in the resolver or a stale file. The
    # ``$schema`` key the base's serializer injects is stripped in
    # ``_migrate_and_clean`` below, before validation ever sees "forbid".
    model_config = ConfigDict(extra="forbid")

    forge_blueprint_version: int = FORGE_BLUEPRINT_VERSION
    application: str = (
        DEFAULT_APPLICATION  # which C-Star application consumes this blueprint
    )
    # The blueprint's canonical name + human description (``Blueprint`` base fields).
    # ``name`` is the single source of truth for every derived name (``casename``,
    # namelist ``title``, ``output_root_name``, ``run_output_dir``, the default
    # ``working_dir``, ``B_{name}.yaml``) -- see the properties below. The resolver
    # computes a sensible default (``{model_name}_{grid_name}_{n_procs}procs``) but a
    # user may override it; ``model_name``/``grid_name`` themselves live in
    # ``composition.model.name``/``domain.grid_name`` (they are provenance/functional
    # inputs, not naming inputs, once ``name`` is stored directly).
    name: str
    description: str = "Generated blueprint"
    # Per-run artifact root: everything the executor PRODUCES (input netCDFs, namelist,
    # cppdefs, the emitted roms_marbl blueprint, build dirs) lands under here. Stored with a
    # sensible default but OVERRIDDEN at runtime by C-Star / the Forge executor for the host.
    # Host/location only -> excluded from content_hash (see _HASH_EXCLUDE).
    # The bare root is a sentinel: a validator expands it to ``<root>/<name>`` so each
    # run gets its own subdirectory (see ``_default_working_dir_includes_name``).
    #
    # Redeclared (``str``, not the base's ``Path``) to keep this sentinel behavior --
    # see ``_resolve_out_dir`` below, which overrides the base's eager
    # expanduser()/resolve() so the sentinel stays recognizable until then.
    working_dir: str = DEFAULT_WORKING_ROOT
    run: RunWindow
    domain: Domain
    forcing: Forcing
    cdr: CdrSpec = Field(default_factory=CdrSpec)
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

    @model_validator(mode="before")
    @classmethod
    def _migrate_and_clean(cls, data: Any) -> Any:
        """Forward-migrate + strip the injected ``$schema`` key before validation.

        Runs on *every* entry point -- direct construction, ``from_yaml``,
        ``from_yaml_data``, and (critically) C-Star's own
        ``deserialize()``/``model_validate()`` path, which does not go through
        ``ForgeBlueprint.from_yaml`` at all. See ``migrate_forge_blueprint_data``.
        """
        if not isinstance(data, dict):
            return data
        data = dict(data)
        data.pop("$schema", None)
        return migrate_forge_blueprint_data(data)

    @field_validator("name")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        return sanitize_name(v)

    @field_validator("working_dir", mode="before")
    @classmethod
    def _coerce_working_dir(cls, value: Any) -> Any:
        """Accept a ``Path`` where the field is declared ``str``: C-Star's workplan
        scheduler system-override (``get_system_overrides``) injects
        ``step.fsm.root_dir`` as a ``Path``, which pydantic will not coerce.
        """
        if isinstance(value, Path):
            return value.as_posix()
        return value

    @field_validator("working_dir", mode="after")
    @classmethod
    def _resolve_out_dir(cls, value: str, _info: Any) -> str:
        """Override the ``Blueprint`` base validator of the same name, which expects
        a ``Path`` and eagerly expands/resolves it. Forge's ``working_dir`` is a
        ``str`` sentinel that ``_default_working_dir_includes_name`` (below) rewrites
        relative to the blueprint name; expansion to an absolute host path happens
        later, at processing time, once the real host's scratch root is known.
        """
        return value

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

    # ---- derived naming (single source of truth: name + dates) ----
    @property
    def n_procs(self) -> int:
        partitioning = self.domain.partitioning
        if partitioning.n_cores is not None:
            return partitioning.n_cores
        if partitioning.n_procs_x is not None and partitioning.n_procs_y is not None:
            return partitioning.n_procs_x * partitioning.n_procs_y
        raise ValueError(
            "cannot determine n_procs: partitioning has neither n_cores nor both "
            "n_procs_x/n_procs_y set"
        )

    @property
    def cpus_needed(self) -> int:
        """Ballpark CPU count for *processing* this blueprint (the forge run
        itself: roms-tools input generation), overriding the ``Blueprint`` base
        default of 1. Deliberately NOT ``n_procs`` -- that is the ROMS
        partitioning the downstream simulation uses, not what generating the
        inputs needs. See :func:`estimate_forge_cpus`. Uncapped above its floor:
        because the run is :attr:`single_node`, the launcher clamps this to the
        target partition's CPUs per node.

        When ``domain.grid_file`` is set, ``grid_kwargs`` carries no ``nx``/``ny``/
        ``N`` (a user-supplied grid has no generation-geometry keys -- see
        ``Domain._grid_file_excludes_generation_geometry``); the resolver stamps
        the loaded grid's dims onto ``model_settings["param"]["llm"/"mmm"/"n"]``
        instead, so fall back to those when ``grid_kwargs`` lacks them.
        """
        gk = self.domain.grid_kwargs
        param = self.model_settings.get("param", {}) or {}
        nx = gk.get("nx", param.get("llm", 0))
        ny = gk.get("ny", param.get("mmm", 0))
        nvert = gk.get("N", param.get("n", 0))
        return estimate_forge_cpus(int(nx or 0), int(ny or 0), int(nvert or 0))

    @property
    def single_node(self) -> bool:
        """The forge run is one Python process (roms-tools on dask's threaded
        scheduler), not an MPI job: it can never use more than one node. Marking
        it so lets C-Star's launcher pin the forge step to ``--nodes=1`` and clamp
        :attr:`cpus_needed` to the target partition's CPUs per node, instead of
        requesting extra nodes the run would leave idle. Overrides the
        ``Blueprint`` base default of ``False``.
        """
        return True

    @property
    def n_tracers(self) -> int:
        """Total ROMS tracer count = T + S + BGC (ntrc_bio) + passive (nt_passive),
        derived from ``model_settings['param']``.
        """
        param = self.model_settings.get("param", {}) or {}
        return 2 + int(param.get("ntrc_bio", 0)) + int(param.get("nt_passive", 0))

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
        # Same rationale as `code.<repo>.location` above: a user-provided file's
        # `location` is host/transport (where the executor finds it on *this*
        # machine), not results-affecting content -- the same file staged at a
        # different path must hash identically. Its `content_hash` leaf (the pin on
        # the file's actual data) stays in and IS what's results-affecting.
        domain = data.get("domain")
        if domain:
            grid_file = domain.get("grid_file")
            if grid_file:
                grid_file.pop("location", None)
        cdr = data.get("cdr")
        if cdr:
            cdr_forcing_file = cdr.get("cdr_forcing_file")
            if cdr_forcing_file:
                cdr_forcing_file.pop("location", None)
        forcing = data.get("forcing")
        if forcing:
            for river in forcing.get("river") or []:
                custom_file = river.get("custom_file")
                if custom_file:
                    custom_file.pop("location", None)
        blob = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ---- serialization ----
    def to_yaml(self, path: str | Path) -> Path:
        """Write the authoritative config to ``path`` and return it."""
        path = Path(path)
        path.write_text(self.to_yaml_str())
        return path

    def to_yaml_str(self) -> str:
        # Stamp provenance on the way out (the hash itself excludes provenance, so
        # this doesn't perturb it). content_hash always recomputes (it must reflect
        # current content, for hand-edit detection); generated_at/forge_version/
        # cstar_version/roms_tools_version are stamped only if not already set --
        # first save wins, so a later resave preserves the original values.
        prov = self.provenance
        updates: dict[str, Any] = {"content_hash": self.content_hash()}
        if prov.generated_at is None:
            updates["generated_at"] = datetime.now(UTC)
        if prov.forge_version is None:
            updates["forge_version"] = _forge_version()
        if prov.cstar_version is None:
            updates["cstar_version"] = _installed_version("cstar-ocean")
        if prov.roms_tools_version is None:
            updates["roms_tools_version"] = _installed_version("roms-tools")
        stamped = self.model_copy(
            update={"provenance": prov.model_copy(update=updates)}
        )
        data = stamped.model_dump(mode="json", exclude_none=False)
        return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)

    @classmethod
    def from_yaml(cls, path: str | Path) -> ForgeBlueprint:
        """Load and validate a ``forge_blueprint.yaml`` (processing entry point).

        Version-check + migration happen automatically via the
        ``_migrate_and_clean`` before-validator -- no need to call
        ``migrate_forge_blueprint_data`` here.
        """
        data = yaml.safe_load(Path(path).read_text())
        return cls.model_validate(data)

    @classmethod
    def from_yaml_data(cls, data: dict[str, Any]) -> ForgeBlueprint:
        """Validate an already-parsed dict (e.g. a browser upload). Same automatic
        migration as :meth:`from_yaml`.
        """
        return cls.model_validate(data)
