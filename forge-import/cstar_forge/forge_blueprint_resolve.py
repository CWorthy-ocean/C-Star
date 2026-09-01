"""
Resolver: ``build_forge_blueprint`` — assemble a validated :class:`ForgeBlueprint`
from the composable specs (a ModelSpec + a domain selection + a run window).

This is the *collection / curation* half of the planned split (see
``docs/dev-notes/forge-blueprint-inventory.md``). It is intentionally **dependency-light**: it
reads the ModelSpec YAML directly and computes everything it can from plain inputs,
so a UI backend or a user's laptop can assemble and review a config without ROMS /
roms_tools installed or built (``ForgeBlueprint`` itself now imports
``cstar.orchestration.models.Blueprint`` -- ``cstar-ocean`` is a required pip
dependency of this package regardless, see ``pyproject.toml``, so this doesn't add a
new heavy install; it's the ROMS/MARBL build + roms_tools stack this stays free of).
The only value that needs a grid (``dt`` via the CFL criterion, which needs the grid
spacing ``ds``) is optional: pass ``dt=`` to stay fully lightweight, or leave it
``None`` to have it computed (lazily importing ``roms_tools`` + ``cstar_forge.forge.util``).

What this does NOT do (by design — it is host- and artifact-independent):
* no machine / path resolution (done at processing time, on the run host),
* no source downloads or grid/forcing file generation (processing),
* no ``s_coord`` / file paths / ``title`` / ``output_root_name`` (filled at
  processing or derived from the blueprint's own ``name``).

NOTE: the dataset registry below is a *snapshot* of ``cstar_forge.forge.source_data``
mappings, duplicated here to keep this module importable without the heavy stack.
It should be unified with ``source_data.py`` once the two-phase refactor lands.
"""

from __future__ import annotations

import copy
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import yaml

# Dual import: package context (production) or standalone file (lightweight / UI / test).
try:  # pragma: no cover - exercised both ways
    from cstar_forge.forge.forge_blueprint import (
        BoundaryForcingItem,
        CdrSpec,
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        ForgeBlueprint,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        Provenance,
        ResolvedDataset,
        RiverForcingItem,
        RunWindow,
        SourceSpec,
        SpecRef,
        SurfaceForcingItem,
        TemplateRepo,
        TidalForcingItem,
        TopographySource,
        UserProvidedFile,
        infer_cdr_mode,
        sanitize_name,
        vert_kwargs_from_grid_kwargs,
    )
except ImportError:  # pragma: no cover
    from forge_blueprint import (  # type: ignore
        BoundaryForcingItem,
        CdrSpec,
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        ForgeBlueprint,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        Provenance,
        ResolvedDataset,
        RiverForcingItem,
        RunWindow,
        SourceSpec,
        SpecRef,
        SurfaceForcingItem,
        TemplateRepo,
        TidalForcingItem,
        TopographySource,
        infer_cdr_mode,
        sanitize_name,
    )

# Source-name resolution (alias map, metadata, streamable) — single source of truth,
# dependency-free. Dual import to keep the resolver standalone-importable.
try:  # pragma: no cover - exercised both ways
    from cstar_forge.forge.source_registry import resolve_dataset_key, resolve_source
except ImportError:  # pragma: no cover
    from source_registry import resolve_dataset_key, resolve_source  # type: ignore

# Default repo serving the render templates (now at the forge repo root `templates/`,
# decoupled from the ModelSpec). A ModelSpec pins the serving commit via
# `templates.commit:`; until pinned we track branch `main`.
DEFAULT_TEMPLATE_REPO = CodeRepo(
    location="https://github.com/CWorthy-ocean/cstar-forge.git", branch="main"
)

# Canonical CDR-output diagnostics helper lives in namelist_model (forge side) so
# the executor can share it. Dual import keeps the resolver standalone-importable.
try:  # pragma: no cover - exercised both ways
    from cstar_forge.forge.namelist_model import (
        RunTimeSettings,
        canonical_output_sections_for_precheck,
        check_extract_divides_rst,
        check_output_streams_divide_rst,
        check_rst_period_divisible,
        cppdefs_for_precheck,
        ensure_cdr_output_marbl_diagnostics,
        run_time_settings_for_ref,
    )
except ImportError:  # pragma: no cover
    from namelist_model import (  # type: ignore
        check_rst_period_divisible,
        ensure_cdr_output_marbl_diagnostics,
    )


# Source-name resolution is single-sourced in ``source_registry`` (a lightweight,
# dependency-free module also used by ``source_data``) — no duplicate table here.
def _resolve_dataset_key(name: str, glorys_layout: str | None = None) -> str:
    return resolve_dataset_key(name, glorys_layout)


def _resolved_dataset(name: str, glorys_layout: str | None = None) -> ResolvedDataset:
    return ResolvedDataset(**resolve_source(name, glorys_layout))


def _parse_source(block: Any) -> SourceSpec:
    """A model.yaml ``source`` block: a bare name string or a dict."""
    if isinstance(block, str):
        name = block
        d: dict[str, Any] = {}
    else:
        d = dict(block or {})
        name = d.get("name")
    layout = d.get("glorys_layout")
    return SourceSpec(
        name=name,
        climatology=bool(d.get("climatology", False)),
        glorys_layout=layout,
        path=d.get("path"),
    )


def _normalize_user_file(
    value: str | Path | dict[str, Any] | UserProvidedFile, label: str
) -> UserProvidedFile:
    """Normalize a user-supplied netCDF reference into a :class:`UserProvidedFile`.

    Accepts the same three forms every user-provided-file field takes: a bare
    ``str``/``Path`` (hashed here via ``hash_netcdf_contents`` -- the file must
    exist at authoring time, else a clear ``FileNotFoundError``); a plain dict
    already carrying ``location``/``content_hash`` (trusted as-is, e.g. the
    wizard's rebuild path -- no rehashing); or an existing ``UserProvidedFile``
    instance (passed through unchanged). ``label`` names the field in the raised
    error message.
    """
    if isinstance(value, UserProvidedFile):
        return value
    if isinstance(value, dict):
        return UserProvidedFile(**value)

    from cstar_forge.forge.user_files import hash_netcdf_contents

    path = Path(value)
    if not path.exists():
        raise FileNotFoundError(
            f"{label} {path} does not exist; a user-provided file must be "
            "readable at authoring time so its contents can be hashed into "
            "the blueprint."
        )
    return UserProvidedFile(location=str(path), content_hash=hash_netcdf_contents(path))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base`` (override wins). Returns base.

    Values taken from ``override`` are deep-copied (mirroring the executor's
    ``_deep_merge_settings_dict``): assigning by reference would alias e.g. a
    whole OutputSpec section dict into every blueprint resolved from it, so a
    later in-place edit of one resolved ``model_settings`` could silently
    mutate the shared spec dict and cross-contaminate other resolves.
    """
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = copy.deepcopy(v)
    return base


def load_model_spec_data(model_dir: str | Path) -> dict[str, Any]:
    """Read a ModelSpec directory into a plain dict (no heavy deps).

    A ModelSpec is a single ``model.yaml`` with two top-level sections: ``code``
    (roms/marbl/pio + template refs) and ``model_settings`` (flat, mirrors
    ``ForgeBlueprint.model_settings`` 1:1 -- no separate compile/run-time defaults
    files to resolve). Returns ``{"model_name": <dir name>, "model": <model.yaml dict>}``.
    """
    model_dir = Path(model_dir)
    model_path = model_dir / "model.yaml"
    if not model_path.exists():
        model_path = model_dir / "model.yml"  # legacy extension, still readable
    model = yaml.safe_load(model_path.read_text())
    # single-model file: sections at top level; else unwrap a single named block
    if not any(k in model for k in ("code", "model_settings")):
        if len(model) == 1:
            model = next(iter(model.values()))

    return {
        "model_name": model_dir.name,
        "model": model,
    }


# Sections that are filled at processing time (host/artifact/identity-derived) and
# therefore omitted from the stored, flat model_settings.
_PROCESSING_FILLED_SECTIONS = (
    "grid",
    "initial",
    "forcing",
    "s_coord",
    "title",
    "output_root_name",
)

# The "output settings" spec (OutputSpec): whole model_settings sections that are
# output controls, plus the MARBL output write-lists (a partial of marbl_bgc).
OUTPUT_SECTIONS = (
    "ocean_vars",
    "surf_flux",
    "diagnostics",
    "stdout_diag",
    "ts_output",
    "frc_output",
    "cdr_output",
    "upscale_output",
    "zslice",
    "random_output",
)
OUTPUT_MARBL_FIELDS = ("marbl_tracers_to_write", "marbl_diagnostics_to_write")
# BGC output-write-control fields: OutputSpec's partial of the `bgc` section (the
# rest -- nbgc_flx/interp_frc/xco2air_default -- are ModelSpec physics defaults).
OUTPUT_BGC_FIELDS = (
    "wrt_his",
    "output_period_his",
    "nrpf_his",
    "wrt_avg",
    "output_period_avg",
    "nrpf_avg",
    "wrt_his_dia",
    "output_period_his_dia",
    "nrpf_his_dia",
    "wrt_avg_dia",
    "output_period_avg_dia",
    "nrpf_avg_dia",
)
# Sections OutputSpec owns only *partially* (the rest of the section stays a
# ModelSpec default): section name -> the fields OutputSpec supplies.
PARTIAL_OUTPUT_SECTIONS: dict[str, tuple[str, ...]] = {
    "marbl_bgc": OUTPUT_MARBL_FIELDS,
    "bgc": OUTPUT_BGC_FIELDS,
}


def extract_output_settings(model_settings: dict[str, Any]) -> dict[str, Any]:
    """Pull the output-settings subset out of a full model_settings dict (used to
    seed an OutputSpec catalog entry and to gather the spec for save).
    """
    out: dict[str, Any] = {}
    for sec in OUTPUT_SECTIONS:
        if sec in model_settings:
            out[sec] = copy.deepcopy(model_settings[sec])
    for sec, fields in PARTIAL_OUTPUT_SECTIONS.items():
        src = model_settings.get(sec, {}) or {}
        partial = {f: src[f] for f in fields if f in src}
        if partial:
            out[sec] = copy.deepcopy(partial)
    return out


# ``river_frc``/``cdr_frc`` namelist run-time defaults: these are Forcing-owned (not
# a model-level physics default), but the fields are required by
# ``namelist_model.RunTimeSettings`` and their presence/counts (river_source/nriv,
# cdr_source/ncdr_parm) are only knowable at *generation* time from the actual
# roms-tools output -- so the resolver seeds this disabled baseline (matching a
# forcing with no rivers/CDR), which a ForcingSpec may override with its own
# top-level ``river_frc``/``cdr_frc`` block, and which generation then overwrites
# in turn (see ``GENERATION_DERIVED_LEAF_KEYS`` in ``forge_blueprint_engine.py``).
_RIVER_FRC_DEFAULT: dict[str, Any] = {
    "river_source": False,
    "analytical": False,
    "nriv": 0,
    "rvol_vname": "river_volume",
    "rvol_tname": "river_time",
    "rtrc_vname": "river_tracer",
    "rtrc_tname": "river_time",
}
_CDR_FRC_DEFAULT: dict[str, Any] = {
    "cdr_source": False,
    "cdr_file": "cdr.nc",
    "ncdr_parm": 1,
    "forcing_depth_profiles": False,
    "forcing_3d": False,
    "forcing_parameterized": True,
    "time_interpolation": False,
    "relocate_to_wet_pts": True,
    "cdr_volume": False,
    "cdrvol_vname": "cdr_volume",
    "cdrvol_tname": "cdr_time",
    "cdrtrc_vname": "cdr_tracer",
    "cdrtrc_tname": "cdr_time",
    "cdrflx_vname": "cdr_trcflx",
    "cdrflx_tname": "cdr_time",
    "cdr_loc_lon": "cdr_lon",
    "cdr_loc_lat": "cdr_lat",
    "cdr_loc_dep": "cdr_dep",
    "cdr_scl_hor": "cdr_hsc",
    "cdr_scl_vrt": "cdr_vsc",
    "nz_chd": 50,
}

# ``extract_data`` (child-grid nesting extraction) is likewise a required namelist
# section whose active values come from a *second, separately-selected* DomainSpec
# (the nesting child's grid_kwargs/metadata, passed as grid_kwargs_child/
# metadata_child) rather than the domain/forcing being resolved -- so, like
# river_frc/cdr_frc, the resolver seeds this disabled baseline and the nesting block
# below overwrites it when a child grid is actually provided.
_EXTRACT_DATA_DEFAULT: dict[str, Any] = {
    "do_extract": False,
    "extract_file": "sample_edata.nc",
    "nrpf": 24,
    "n_chd": 90,
    "theta_s_chd": 5.0,
    "theta_b_chd": 2.0,
    "hc_chd": 250.0,
    "extract_period": 3600.0,
    "extract_root_name": "child",
}


def read_cdr_forcing_yaml(source: str | Path) -> dict[str, Any]:
    """Read a roms-tools ``CDRForcing.to_yaml(...)`` dump into a plain kwargs dict.

    ``source`` may be a filesystem path to the YAML file, or the raw YAML text itself
    (e.g. content read from an uploaded file). The roms-tools dump is multi-document
    (a version-header doc, then a ``CDRForcing:`` doc); this pulls out the inner
    mapping and strips ``_tracer_metadata`` (written by ``to_yaml`` for human
    readability, not a ``CDRForcing`` constructor field). Dates are left as-is (ISO
    strings or YAML-native datetimes) — roms-tools coerces either.

    Deliberately pure-``yaml`` (no ``roms_tools`` import) to keep this module's
    dependency-light guarantee; validating the result as an actual ``CDRForcing``
    is the caller's job (the wizard does this eagerly on upload).
    """
    path = Path(source)
    try:
        is_path = path.exists()
    except OSError:  # pragma: no cover - defensive (e.g. NUL bytes in "path")
        is_path = False
    text = path.read_text() if is_path else str(source)

    docs = [d for d in yaml.safe_load_all(text) if d]
    block = next(
        (d["CDRForcing"] for d in docs if isinstance(d, dict) and "CDRForcing" in d),
        None,
    )
    if block is None:
        raise ValueError(
            "Uploaded file is not a roms-tools CDRForcing YAML dump (no top-level "
            "'CDRForcing:' document found)."
        )
    block = dict(block)
    block.pop("_tracer_metadata", None)
    return block


def build_forge_blueprint(
    *,
    model_dir: str | Path,
    grid_name: str,
    grid_kwargs: dict[str, Any],
    open_boundaries: dict[str, bool],
    partitioning: dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    model_reference_date: datetime | None = None,
    name: str | None = None,
    description: str = "Generated blueprint",
    cdr: dict[str, Any] | CdrSpec | None = None,
    cdr_forcing: dict[str, Any] | None = None,
    cdr_forcing_yaml: str | Path | None = None,
    forcing_inputs: dict[str, Any] | None = None,
    output_settings: dict[str, Any] | None = None,
    grid_kwargs_child: dict[str, Any] | None = None,
    grid_kwargs_parent: dict[str, Any] | None = None,
    metadata_child: dict[str, Any] | None = None,
    nesting_include_pressure_fluxes: bool = False,
    grid_file: str | Path | dict[str, Any] | UserProvidedFile | None = None,
    cdr_forcing_file: str | Path | dict[str, Any] | UserProvidedFile | None = None,
    topography_path: str | None = None,
    topography_source: str | TopographySource = TopographySource.ETOPO5,
    use_pio: bool | None = None,
    bgc_mode: Literal["marbl", "none"] | None = None,
    roms_ref: str | None = None,
    marbl_ref: str | None = None,
    run_time_overrides: dict[str, Any] | None = None,
    compile_time_overrides: dict[str, Any] | None = None,
    dt: float | None = None,
    v_sponge: float | None = None,
    grid: Any = None,
    templates_repo: CodeRepo | None = None,
    composition: Composition | None = None,
    generated_at: datetime | None = None,
    forge_version: str | None = None,
    roms_tools_version: str | None = None,
    notes: str | None = None,
) -> ForgeBlueprint:
    """Resolve the composable specs into a validated, host-independent ``ForgeBlueprint``.

    Parameters mirror the logical inputs a UI would collect. ``dt`` may be supplied
    directly (fully lightweight); if ``None`` it is computed from the CFL criterion,
    which lazily imports ``roms_tools`` (to build the grid for ``ds``) and
    ``cstar_forge.forge.util``.

    ``bgc_mode`` is a per-run toggle mirroring ``use_pio``: it overwrites
    ``cppdefs.marbl`` and gates whether ``code.marbl`` is populated (raising if
    ``"marbl"`` is requested but the ModelSpec has no ``code.marbl`` repository).
    ``bgc_mode="none"`` raises if the resolved forcing selection still requests BGC
    forcing (a bgc-type surface/boundary item, an IC bgc_source, or a river with
    ``include_bgc=True``); it also forces ``cppdefs.nhy_forcing``/``nox_forcing``
    off regardless of the ModelSpec/advanced-settings default. If ``None`` (the
    default), it falls back to the ModelSpec's own ``bgc_mode`` (itself defaulting
    to ``"marbl"``) -- the ModelSpec is the single source of the default; pass an
    explicit value to override it for one run.

    ``roms_ref``/``marbl_ref`` are per-run checkout-target overrides (commit hash,
    tag, or branch) for ``code.roms``/``code.marbl``. If ``None`` (the default),
    the ModelSpec's own pin is snapshotted verbatim; when set, the override is
    stored in ``commit`` with ``branch`` cleared (C-Star's CodeRepository treats
    all three as the same checkout target). ``marbl_ref`` only takes effect when
    ``bgc_mode == "marbl"`` (otherwise ``code.marbl`` is not populated at all).

    ``use_pio`` is a per-run toggle mirroring ``bgc_mode``: it overwrites
    ``cppdefs.use_pio`` and gates whether ``code.pio`` is populated (raising if PIO
    is requested but the ModelSpec has no ``code.pio`` repository). If ``None`` (the
    default), it falls back to the ModelSpec's own ``use_pio`` (itself defaulting to
    ``False``) -- the ModelSpec is the single source of the default; pass an
    explicit value to override it for one run.

    ``partitioning["auto_tiling"]`` (default ``False``) overwrites ``cppdefs.auto_tiling``,
    ucla-roms 0.5.0's MPI_MASKING cppdef: ROMS picks NP_XI/NP_ETA at runtime from the
    land mask instead of the fixed decomposition, so ``np_xi``/``np_eta`` are written
    as placeholder ``1``s. Requires ``use_pio=True`` and ``partitioning["n_cores"]``,
    which is derived as ``n_procs_x * n_procs_y`` when an explicit grid is supplied
    instead (both may be given only when consistent). The emitted blueprint always
    carries ``n_cores`` alone (the resolved ``Partitioning`` is stricter than
    C-Star's: ``n_procs_x``/``n_procs_y`` are nulled under ``auto_tiling``).
    ``n_procs_x``/``n_procs_y`` are required only when ``auto_tiling`` is off, and
    ``n_cores`` is rejected when it is off.

    ``cdr`` is the full CDR-forcing selection (a :class:`CdrSpec` or an
    equivalent dict, e.g. a catalog CDR-spec entry's data), covering all five
    modes ("none"/"simple"/"yaml"/"netcdf"/"upscaled" -- see ``CdrSpec.mode``'s
    docstring). ``cdr_forcing``/``cdr_forcing_yaml``/``cdr_forcing_file`` remain
    as documented conveniences for the two-mode subset they've always covered
    (generated-from-dict/yaml -> "yaml", or a pre-made file -> "netcdf"); passing
    ``cdr`` together with any of the three raises ``ValueError`` (supply one or
    the other, not both). A ``cdr`` dict's own ``cdr_forcing``/``cdr_forcing_file``
    sub-values get the same treatment the convenience kwargs get below (
    ``_tracer_metadata`` stripped from ``cdr_forcing``; ``cdr_forcing_file``
    normalized via :func:`_normalize_user_file`) before validating as a
    :class:`CdrSpec`; a ``CdrSpec`` instance is used as-is.

    ``cdr_forcing_yaml``, if given, takes precedence over ``cdr_forcing``: it is a
    path to (or the raw text of) a roms-tools ``CDRForcing.to_yaml(...)`` dump, read
    via :func:`read_cdr_forcing_yaml`. This is the wizard's upload path made
    resolver-native; a caller may pass either kwarg, not both meaningfully at once.
    Both funnel into ``CdrSpec(mode="yaml", ...)`` -- "simple" mode (an authored,
    not-uploaded, kwargs dict) is reachable only via ``cdr=``.

    ``name`` is the blueprint's canonical name (``ForgeBlueprint.name``, a top-level
    field); if omitted, this computes and sanitizes the default
    (``{model_name}_{grid_name}_{n_procs}procs``).

    ``model_reference_date`` is the ROMS model reference date (t=0), passed to every
    rt object that accepts it. If ``None`` (the default), ``RunWindow`` falls back to
    its own default (2000-01-01, the roms-tools default).

    ``v_sponge`` and ``dt`` are both domain-owned numerics with the same pattern:
    if ``None`` (the default), each is derived from the grid -- ``v_sponge`` from
    grid spacing via ``cstar_forge.forge.util.compute_v_sponge_from_grid``, ``dt``
    from the CFL criterion via ``_compute_dt_from_cfl`` (builds a grid); pass an
    explicit value (e.g. one restored from a saved DomainSpec) to use it verbatim
    instead. The resolver is the sole writer of both ``domain.v_sponge`` /
    ``domain.dt`` and the identical ``model_settings["v_sponge"]["v_sponge"]`` /
    ``model_settings["time_stepping"]["dt"]`` leaves -- each pair is always
    written together and must never diverge.

    ``forge_version``/``roms_tools_version`` are left ``None`` here by default --
    ``ForgeBlueprint.to_yaml_str`` stamps each with a best-effort value on first
    save (see ``cstar_forge.forge.forge_blueprint._forge_version`` /
    ``_installed_version``), preserving an explicit value passed here instead
    (e.g. carrying one forward through a re-resolve).

    ``grid_file``, if given, is a user-supplied pre-made grid netCDF used in place
    of one Forge would otherwise generate from ``grid_kwargs``. A ``str``/``Path``
    is normalized into a :class:`UserProvidedFile` by hashing the file's contents
    (``cstar_forge.forge.user_files.hash_netcdf_contents``) -- the file must exist
    at authoring time. A dict or ``UserProvidedFile`` carrying both ``location``
    and ``content_hash`` is trusted as-is (the wizard passes this to avoid
    rehashing on every rebuild). Either way, the grid is loaded once here
    (``rt.Grid(filename=..., **vert)``, where ``vert`` is the theta_s/theta_b/
    hc/N subset of ``grid_kwargs`` -- see :func:`vert_kwargs_from_grid_kwargs`)
    to read back ``nx``/``ny``/``N`` for ``model_settings["param"]`` and, when
    ``dt``/``v_sponge`` are not supplied explicitly, ``size_x``/``size_y`` for
    their derivation -- which raises a clear ``ValueError`` if the file lacks
    those attrs (a hand-made file roms-tools never wrote), since there is then
    no way to derive them and an explicit value must be passed instead.
    ``grid_kwargs`` itself must then carry only the vertical-coord keys (the
    schema validator rejects generation-geometry keys and nesting alongside
    ``grid_file`` -- see ``Domain._grid_file_excludes_generation_geometry``).

    ``cdr_forcing_file``, if given, is a user-supplied pre-made CDR-forcing netCDF
    used in place of building one via ``rt.CDRForcing`` from ``cdr_forcing``/
    ``cdr_forcing_yaml``. Normalized into a :class:`UserProvidedFile` the same way
    as ``grid_file`` (see :func:`_normalize_user_file`) -- a ``str``/``Path`` is
    hashed here (must exist at authoring time), a dict/``UserProvidedFile`` is
    trusted as-is. Mutually exclusive with ``cdr_forcing``/``cdr_forcing_yaml``
    (raised here, before ``CdrSpec``'s own validator would, so the caller gets
    a resolver-level message); like a set ``cdr_forcing``, it forces
    ``do_cdr_output=True``, requires ``bgc_mode == "marbl"``, sets
    ``cppdefs.cdr_forcing = True``, and ensures the CDR-output MARBL diagnostics.
    """
    if cdr is not None and (
        cdr_forcing is not None
        or cdr_forcing_yaml is not None
        or cdr_forcing_file is not None
    ):
        raise ValueError(
            "cdr and cdr_forcing/cdr_forcing_yaml/cdr_forcing_file are mutually "
            "exclusive; supply either a full cdr= selection or the individual "
            "convenience kwargs, not both."
        )

    def _strip_tracer_metadata(d: dict[str, Any]) -> dict[str, Any]:
        # defensive strip -- a caller may hand us a dict copied straight from a
        # to_yaml() dump (which still carries the human-readable metadata block).
        return {k: v for k, v in d.items() if k != "_tracer_metadata"}

    if cdr is not None:
        if isinstance(cdr, CdrSpec):
            cdr_spec = cdr
        else:
            cdr_kwargs = dict(cdr)
            # A catalog CDR-spec entry (catalog.cdr_data(name)) carries a
            # `description` provenance key that isn't part of CdrSpec (which is
            # extra="forbid") -- drop it so cdr= accepts the dict verbatim.
            cdr_kwargs.pop("description", None)
            raw_cdr_forcing = cdr_kwargs.get("cdr_forcing")
            if raw_cdr_forcing is not None:
                cdr_kwargs["cdr_forcing"] = _strip_tracer_metadata(raw_cdr_forcing)
            raw_cdr_forcing_file = cdr_kwargs.get("cdr_forcing_file")
            if raw_cdr_forcing_file is not None:
                cdr_kwargs["cdr_forcing_file"] = _normalize_user_file(
                    raw_cdr_forcing_file, label="cdr.cdr_forcing_file"
                )
            cdr_spec = CdrSpec(**cdr_kwargs)
    else:
        if cdr_forcing_file is not None and (
            cdr_forcing is not None or cdr_forcing_yaml is not None
        ):
            raise ValueError(
                "cdr_forcing_file and cdr_forcing/cdr_forcing_yaml are mutually "
                "exclusive; supply either a generated-CDR config or a pre-made "
                "CDR-forcing file, not both."
            )

        if cdr_forcing_yaml is not None:
            cdr_forcing = read_cdr_forcing_yaml(cdr_forcing_yaml)
        elif cdr_forcing is not None:
            cdr_forcing = _strip_tracer_metadata(cdr_forcing)

        cdr_forcing_file_obj = (
            _normalize_user_file(cdr_forcing_file, label="cdr_forcing_file")
            if cdr_forcing_file is not None
            else None
        )
        # Mode inference shared with the v6->v7 migration and ForgeExecutor's
        # direct-construction back-compat (infer_cdr_mode lives next to CdrSpec).
        cdr_spec = CdrSpec(
            mode=infer_cdr_mode(cdr_forcing, cdr_forcing_file_obj),
            cdr_forcing=cdr_forcing,
            cdr_forcing_file=cdr_forcing_file_obj,
        )

    spec = load_model_spec_data(model_dir)
    model = spec["model"]
    model_name = spec["model_name"]
    if bgc_mode is None:
        bgc_mode = model.get("bgc_mode", "marbl")
    if use_pio is None:
        use_pio = bool(model.get("use_pio", False))
    # ModelSpec no longer embeds a default forcing/output selection -- a ForcingSpec and
    # an OutputSpec must always be supplied explicitly (from the catalog or hand-authored).
    if forcing_inputs is None:
        raise ValueError(
            "forcing_inputs is required: ModelSpec no longer provides a default "
            "forcing -- select or supply a ForcingSpec (e.g. catalog.forcing_data(name))."
        )
    if not output_settings:
        raise ValueError(
            "output_settings is required: ModelSpec no longer provides default output "
            "settings -- select or supply an OutputSpec (e.g. catalog.output_data(name))."
        )
    inputs = forcing_inputs

    # ----- optional user-provided grid file ----------------------------------
    # Normalize `grid_file` into a UserProvidedFile, then load the grid ONCE
    # (nx/ny/N/dt/v_sponge derivation below all read from this single object
    # rather than re-opening the file or re-hashing it).
    grid_file_obj: UserProvidedFile | None = None
    loaded_grid: Any = None
    if grid_file is not None:
        grid_file_obj = _normalize_user_file(grid_file, label="grid_file")

        if grid is not None:
            # A caller (the wizard, rebuilding on every widget edit) may pass the
            # already-loaded grid to avoid re-reading the netCDF each rebuild.
            loaded_grid = grid
        else:
            vert = vert_kwargs_from_grid_kwargs(grid_kwargs)
            import roms_tools as rt

            loaded_grid = rt.Grid(filename=grid_file_obj.location, **vert)

    if loaded_grid is not None:
        nx = loaded_grid.nx
        ny = loaded_grid.ny
        nvert = loaded_grid.N
    else:
        nx = grid_kwargs["nx"]
        ny = grid_kwargs["ny"]
        nvert = grid_kwargs["N"]
    npx = partitioning.get("n_procs_x")
    npy = partitioning.get("n_procs_y")
    auto_tiling = bool(partitioning.get("auto_tiling", False))
    n_cores = partitioning.get("n_cores")
    if auto_tiling and not use_pio:
        raise ValueError("partitioning.auto_tiling requires use_pio=True")
    if not auto_tiling and n_cores is not None:
        raise ValueError("partitioning.n_cores is only accepted with auto_tiling")
    if not auto_tiling and (npx is None or npy is None):
        raise ValueError(
            "partitioning.n_procs_x and partitioning.n_procs_y are required "
            "unless partitioning.auto_tiling is enabled"
        )
    if auto_tiling:
        # Graceful path for callers that switch auto_tiling on while still
        # carrying an explicit n_procs_x/n_procs_y grid (e.g. a saved
        # DomainSpec/blueprint): derive n_cores from the product rather than
        # making the user multiply by hand. When both are given they must
        # agree; the emitted Partitioning always carries only n_cores
        # (n_procs_x/n_procs_y are nulled below).
        if n_cores is None:
            if npx is None or npy is None:
                raise ValueError(
                    "partitioning.auto_tiling requires partitioning.n_cores "
                    "(or both n_procs_x/n_procs_y to derive it from)"
                )
            n_cores = npx * npy
        elif npx is not None and npy is not None and npx * npy != n_cores:
            raise ValueError(
                f"partitioning.n_cores={n_cores} is inconsistent with "
                f"n_procs_x*n_procs_y={npx * npy}; with auto_tiling, set "
                "n_cores alone (or a matching n_procs_x/n_procs_y pair)"
            )

    # ----- derived numerics --------------------------------------------------
    if dt is None:
        if loaded_grid is not None and loaded_grid.size_x is None:
            raise ValueError(
                "dt was not provided and the grid_file grid has no size_x/size_y "
                "(a hand-made file roms-tools never wrote those attrs to) -- CFL "
                "derivation is impossible; pass dt= explicitly."
            )
        dt = _compute_dt_from_cfl(
            grid_kwargs, loaded_grid if loaded_grid is not None else grid
        )
    n_days = (end_date - start_date).days
    ntimes = round(n_days * 24 * 3600 / dt)
    # v_sponge default = grid spacing (m) / 10 -- a caller (e.g. the wizard, restoring
    # a user override saved into a DomainSpec) may pass an explicit value instead.
    if v_sponge is None:
        if loaded_grid is not None and loaded_grid.size_x is None:
            raise ValueError(
                "v_sponge was not provided and the grid_file grid has no size_x "
                "(a hand-made file roms-tools never wrote that attr to) -- "
                "derivation from grid spacing is impossible; pass v_sponge= "
                "explicitly."
            )
        v_sponge = _compute_v_sponge_default(grid_kwargs, loaded_grid)

    # ----- flat model_settings ----------------------------------------------
    settings: dict[str, Any] = copy.deepcopy(model.get("model_settings", {}) or {})
    for sec in _PROCESSING_FILLED_SECTIONS:
        settings.pop(sec, None)
    settings["time_stepping"] = {"ntimes": ntimes, "dt": dt, "ndtfast": 60, "ninfo": 1}
    settings["v_sponge"] = {"v_sponge": v_sponge}
    # river_frc/cdr_frc are Forcing-owned, not a ModelSpec default: seed the disabled
    # baseline, then let the ForcingSpec's own top-level river_frc/cdr_frc (if any)
    # override it. Generation later overwrites the presence/count leaves for real
    # (see _RIVER_FRC_DEFAULT/_CDR_FRC_DEFAULT docstring above).
    settings["river_frc"] = _deep_merge(
        copy.deepcopy(_RIVER_FRC_DEFAULT), inputs.get("river_frc") or {}
    )
    settings["cdr_frc"] = _deep_merge(
        copy.deepcopy(_CDR_FRC_DEFAULT), inputs.get("cdr_frc") or {}
    )
    if cdr_spec.mode == "upscaled":
        # Upscaled CDR tracers come from C-Star's runtime orchestrator, not from
        # anything Forge generates -- no generation step runs to derive these
        # values (unlike "simple"/"yaml"/"netcdf", where _generate_cdr_forcing/
        # _generate_cdr_forcing_from_custom_file overwrite the presence/count
        # leaves for real -- see GENERATION_DERIVED_LEAF_KEYS in
        # forge_blueprint_engine.py). This resolver call is therefore the SINGLE
        # source of these statics. C-Star's orchestrator symlinks the real,
        # run-specific CDR forcing file to the fixed "cdr.nc" name at run time
        # (the same filename a generated/custom-file CDR run also opens -- see
        # CDR_FORCING_NETCDF_STEM in input_data.py), so ROMS always opens "cdr.nc"
        # regardless of which CDR mode produced it.
        settings["cdr_frc"].update(
            {
                "cdr_source": True,
                "cdr_file": "cdr.nc",
                "forcing_depth_profiles": True,
                "forcing_parameterized": False,
                "cdr_volume": False,
                "relocate_to_wet_pts": False,
            }
        )
    # extract_data (child-grid nesting) is Domain-owned when active (the nesting
    # block below overwrites it from grid_kwargs_child/metadata_child); seed the
    # disabled baseline here so it's always present when not nesting.
    settings["extract_data"] = copy.deepcopy(_EXTRACT_DATA_DEFAULT)
    param = dict(settings.get("param", {}))
    param.update(
        {
            "llm": nx,
            "mmm": ny,
            "n": nvert,
            # Placeholder namelist values under MPI_MASKING: np_xi/np_eta are
            # required namelist fields (see ParamCfg), but ROMS overwrites them
            # at runtime from the land mask when auto_tiling is on.
            "np_xi": npx if not auto_tiling else 1,
            "np_eta": npy if not auto_tiling else 1,
            "nsub_x": 1,
            "nsub_e": 1,
        }
    )
    settings["param"] = param

    # cppdefs (compile-time) sits at the same flat level as the namelist sections
    cppdefs = dict(settings.get("cppdefs", {}))
    cppdefs["obc_west"] = bool(open_boundaries.get("west", False))
    cppdefs["obc_east"] = bool(open_boundaries.get("east", False))
    cppdefs["obc_north"] = bool(open_boundaries.get("north", False))
    cppdefs["obc_south"] = bool(open_boundaries.get("south", False))
    cppdefs["cdr_forcing"] = cdr_spec.mode != "none"
    cppdefs["use_pio"] = bool(use_pio)
    cppdefs["auto_tiling"] = bool(auto_tiling)
    cppdefs["marbl"] = bgc_mode == "marbl"
    # nhy_forcing/nox_forcing default from the ModelSpec (advanced-settings editable)
    # but are always forced off when BGC is disabled.
    cppdefs["nhy_forcing"] = (
        bool(cppdefs.get("nhy_forcing", True)) and bgc_mode != "none"
    )
    cppdefs["nox_forcing"] = (
        bool(cppdefs.get("nox_forcing", True)) and bgc_mode != "none"
    )
    surface_items = (inputs.get("forcing", {}) or {}).get("surface", []) or []
    cppdefs["co2_tvarying"] = any(
        (it.get("type") == "bgc")
        and str((it.get("source") or {}).get("name", "")).upper() == "MBL_CO2"
        for it in surface_items
        if isinstance(it, dict)
    )
    # restoring surface forcing with an SSS component -> sal_restore
    cppdefs["sal_restore"] = any(
        it.get("type") == "restoring" and "sss" in (it.get("restoring_forces") or [])
        for it in surface_items
        if isinstance(it, dict)
    )
    # TIDES tracks whether any tidal forcing item is actually being generated.
    tidal_items = (inputs.get("forcing", {}) or {}).get("tidal", []) or []
    cppdefs["tides"] = bool(tidal_items)
    settings["cppdefs"] = cppdefs

    # ntides is Forcing-owned (no ModelSpec default): drive it from the tidal
    # forcing item's ntides, or 0 when there is no tidal forcing -- see
    # input_data._generate_tidal_forcing, which re-derives the same value at
    # generation time.
    _ntides = next(
        (
            it.get("ntides")
            for it in tidal_items
            if isinstance(it, dict) and it.get("ntides")
        ),
        None,
    )
    settings.setdefault("tides", {})["ntides"] = (
        int(_ntides) if _ntides is not None else 0
    )

    # ----- nesting (child domain) -------------------------------------------
    # A child grid means this domain's parent extracts data for it: enable the
    # extract_data block. Child s-coord/levels come from grid_kwargs_child; the
    # extract period from the child metadata (else the roms_tools default).
    if grid_kwargs_child is not None:
        extract = dict(settings.get("extract_data", {}))
        extract["do_extract"] = True
        extract["extract_file"] = "nesting.nc"  # fixed convention (see input_data)
        if "N" in grid_kwargs_child:
            extract["n_chd"] = grid_kwargs_child["N"]
        for src, dst in (
            ("theta_s", "theta_s_chd"),
            ("theta_b", "theta_b_chd"),
            ("hc", "hc_chd"),
        ):
            if src in grid_kwargs_child:
                extract[dst] = grid_kwargs_child[src]
        period = (metadata_child or {}).get("period")
        extract["extract_period"] = float(period) if period is not None else 3600.0
        settings["extract_data"] = extract

    # OutputSpec spec: deep-merge the output-settings selection over the model
    # defaults (before manual overrides, so a hand override still wins).
    _deep_merge(settings, output_settings)

    # overrides win (mirror ForgeExecutor.configure_build precedence)
    if compile_time_overrides:
        _deep_merge(
            settings["cppdefs"],
            compile_time_overrides.get("cppdefs", compile_time_overrides),
        )
    if run_time_overrides:
        _deep_merge(settings, run_time_overrides)

    # A child grid (has a parent) gets its boundaries from the parent's nesting.nc
    # extraction: no boundary tides and no sponge ub_tune. Force both off after the
    # override merge so an explicit bry_tides=True / ub_tune=True override can't win.
    if grid_kwargs_parent is not None:
        settings.setdefault("tides", {})["bry_tides"] = False
        settings.setdefault("sponge_tune", {})["ub_tune"] = False

    # No tidal forcing item: force the run-time tide switches off too. ROMS enables
    # tides via bry_tides/pot_tides in TIDAL_FRC_SETTINGS (the TIDES cppdef only
    # stamps a netCDF attribute), so leaving the ModelSpec's defaults on would send
    # ROMS looking for tidal input data that was never generated. Applied after the
    # override merge so an explicit bry_tides=True override can't reintroduce the
    # crash -- except with ana_tides, which computes tides analytically and needs
    # no input file.
    if not tidal_items and not settings.get("tides", {}).get("ana_tides", False):
        tides_settings = settings.setdefault("tides", {})
        tides_settings["bry_tides"] = False
        tides_settings["pot_tides"] = False

    # ----- CDR output consistency --------------------------------------------
    # CDR output is valid without CDR forcing (cdr_frc.cdr_source stays false;
    # ROMS opens no CDR file), and any active CDR mode (generated -- "simple"/
    # "yaml", a user-supplied file -- "netcdf", or runtime-supplied -- "upscaled")
    # implies CDR output. "upscaled" is included here even though Forge never
    # generates its CDR data: real CDR tracers exist at runtime under that mode
    # too, so the same MARBL/diagnostics requirements apply. Either way the
    # CDR_FORCING cppdef must be on (it gates compiling ucla-roms' cdr_output.F90)
    # and the MARBL diagnostics ucla-roms looks up by name, unchecked, must be in
    # the write list.
    cdr_out = settings.setdefault("cdr_output", {})
    do_cdr_output = bool(cdr_out.get("do_cdr_output")) or cdr_spec.mode != "none"
    cdr_out["do_cdr_output"] = do_cdr_output
    if do_cdr_output:
        if bgc_mode != "marbl":
            raise ValueError(
                'do_cdr_output=True (or CDR forcing was supplied) but bgc_mode != "marbl": '
                "ucla-roms only compiles cdr_output.F90 under MARBL && CDR_FORCING."
            )
        settings["cppdefs"]["cdr_forcing"] = True
        marbl = settings.setdefault("marbl_bgc", {})
        marbl["marbl_diagnostics_to_write"] = ensure_cdr_output_marbl_diagnostics(
            marbl.get("marbl_diagnostics_to_write")
        )

    # ----- restart period consistency ----------------------------------------
    # Fail fast at authoring time (mirrors the executor's net for hand-edited
    # blueprints): restart writes must land on a timestep.
    check_rst_period_divisible(
        settings.get("time_stepping", {}).get("dt"), settings.get("ocean_vars", {})
    )
    # Nesting extraction files must roll on restart boundaries under
    # ucla-roms >= 0.5.0 (its check_output_divides_rst aborts the run
    # otherwise); older releases don't enforce it, so gate on the schema the
    # pinned ref selects. The extract values are resolver-derived (child
    # DomainSpec metadata 'period' x a seeded nrpf), so authoring time is the
    # only place the author sees the failure with the knobs still in hand.
    roms_block = (model.get("code") or {}).get("roms") or {}
    effective_roms_ref = (
        roms_ref or roms_block.get("commit") or roms_block.get("branch")
    )
    with warnings.catch_warnings():
        # A non-semver pin (e.g. pio-dev's 'main') warns on every schema
        # selection; this internal version probe shouldn't repeat it -- the
        # engine/executor selection paths already surface it once per run.
        warnings.simplefilter("ignore", UserWarning)
        settings_cls = run_time_settings_for_ref(
            str(effective_roms_ref) if effective_roms_ref is not None else None
        )
    if settings_cls is not RunTimeSettings:
        check_extract_divides_rst(
            settings.get("ocean_vars", {}), settings.get("extract_data", {})
        )
        # check_output_streams_divide_rst's general table (C-Star,
        # cstar.roms.precheck) mirrors the *complete* do_precheck call list,
        # which includes `extract` -- but check_extract_divides_rst above
        # already covers that stream with a more actionable message (it names
        # the child DomainSpec 'period' knob the author actually edits).
        # include_extract=False drops `extract_data` from the canonical dump
        # below so `extract` isn't double-checked (and double-raised on) here.
        #
        # The checker is keyed on C-Star's canonical namelist vocabulary
        # (RomsNamelistBase group field names / real Fortran keys), not
        # forge's settings-dict vocabulary -- canonical_output_sections_for_
        # precheck() translates just the output-relevant sections via each
        # forge Cfg class's own serialization_alias (the single source of
        # truth for that renaming). A full build_namelist()/RunTimeSettings.
        # model_validate() round-trip isn't possible here: `settings` is still
        # missing the processing-filled sections (title/grid/initial/forcing/
        # s_coord/reference_date_settings, populated later at generate_inputs()/
        # executor time) -- none of which affect any output-stream field.
        check_output_streams_divide_rst(
            canonical_output_sections_for_precheck(settings, include_extract=False),
            cppdefs_for_precheck(
                settings.get("cppdefs", {}), settings.get("upscale_output", {})
            ),
        )

    # ----- forcing (initial conditions + surface/boundary/tidal/river) --------
    # A child grid (has a parent) receives its boundary values from the parent's
    # nesting.nc extraction, not from reanalysis boundary forcing -- the executor
    # already skips boundary-forcing generation for a child (grid_parent is not
    # None, see ForgeExecutor/RomsMarblInputData). Skip building boundary items
    # here too (defense-in-depth, the wizard also clears this at the UI layer) --
    # done inside _build_forcing (not by zeroing sources.boundary afterward) so a
    # boundary-only source is never noted into resolved_datasets/datasets either.
    # CDR is no longer part of Forcing -- it lives on the blueprint's own top-level
    # `cdr` field (cdr_spec, built above), so _build_forcing takes no CDR params.
    sources = _build_forcing(
        inputs,
        # A user-provided grid file has its topography baked in -- the executor
        # skips topography staging entirely, so don't note the configured source
        # into resolved_datasets/datasets (a user-staged source like EMOD would
        # otherwise hard-fail ensure_source_data over a file that is never used).
        None if grid_file_obj is not None else topography_source,
        is_child=grid_kwargs_parent is not None,
    )  # kept as `sources` locally for brevity

    # ----- bgc_mode consistency check ----------------------------------------
    # A BGC-disabled build can't carry BGC-type forcing -- catch it here, before
    # code/settings resolution, with a message naming every offending item.
    if bgc_mode == "none":
        bgc_signals: list[str] = []
        for i, it in enumerate(sources.surface):
            if it.type == "bgc":
                src_name = it.source.name if it.source else "?"
                bgc_signals.append(f"surface[{i}] (source={src_name}, type=bgc)")
        for i, it in enumerate(sources.boundary):
            if it.type == "bgc":
                src_name = it.source.name if it.source else "?"
                bgc_signals.append(f"boundary[{i}] (source={src_name}, type=bgc)")
        if (
            sources.initial_conditions is not None
            and sources.initial_conditions.bgc_source is not None
        ):
            bgc_signals.append(
                "initial_conditions.bgc_source (source="
                f"{sources.initial_conditions.bgc_source.name})"
            )
        for i, it in enumerate(sources.river):
            if it.include_bgc:
                bgc_signals.append(f"river[{i}] (include_bgc=True)")
        if bgc_signals:
            raise ValueError(
                'bgc_mode="none" but the ForcingSpec requests BGC forcing:\n  - '
                + "\n  - ".join(bgc_signals)
                + '\nSet bgc_mode="marbl" or remove these BGC forcing items from '
                "the ForcingSpec."
            )

    # ----- code + templates --------------------------------------------------
    code = _build_code(
        model,
        templates_repo or DEFAULT_TEMPLATE_REPO,
        use_pio=use_pio,
        bgc_mode=bgc_mode,
        roms_ref=roms_ref,
        marbl_ref=marbl_ref,
    )

    default_n_procs = n_cores if auto_tiling else npx * npy
    default_name = sanitize_name(f"{model_name}_{grid_name}_{default_n_procs}procs")
    return ForgeBlueprint(
        name=name or default_name,
        description=description,
        run=RunWindow(
            start_date=start_date,
            end_date=end_date,
            **(
                {"model_reference_date": model_reference_date}
                if model_reference_date is not None
                else {}
            ),
        ),
        domain=Domain(
            grid_name=grid_name,
            grid_kwargs=grid_kwargs,
            topography_source=topography_source,
            topography_path=topography_path,
            open_boundaries=OpenBoundaries(
                **{
                    k: bool(open_boundaries.get(k, False))
                    for k in ("north", "south", "east", "west")
                }
            ),
            partitioning=Partitioning(
                n_procs_x=npx if not auto_tiling else None,
                n_procs_y=npy if not auto_tiling else None,
                auto_tiling=auto_tiling,
                n_cores=n_cores,
            ),
            grid_kwargs_child=grid_kwargs_child,
            grid_kwargs_parent=grid_kwargs_parent,
            metadata_child=metadata_child,
            nesting_include_pressure_fluxes=nesting_include_pressure_fluxes,
            # Read back from ``settings`` (not the local ``v_sponge``) so a
            # ``run_time_overrides={"v_sponge": ...}`` caller (the generic override
            # mechanism, orthogonal to the ``v_sponge=`` param) can't desync this
            # field from ``model_settings["v_sponge"]["v_sponge"]`` -- both must
            # always agree.
            v_sponge=settings["v_sponge"]["v_sponge"],
            # Same reasoning as v_sponge above: read back from ``settings`` so a
            # ``run_time_overrides={"time_stepping": {"dt": ...}}`` caller can't
            # desync this field from ``model_settings["time_stepping"]["dt"]``.
            dt=settings["time_stepping"]["dt"],
            grid_file=grid_file_obj,
        ),
        forcing=sources,
        cdr=cdr_spec,
        # Host-independent source-dataset keys to prepare (forcing/IC sources + topography),
        # derived from the resolved sources so the executor never reads model_spec.datasets.
        datasets=sorted(
            {
                rd.dataset_key
                for rd in sources.resolved_datasets.values()
                if rd.dataset_key
            }
        ),
        model_settings=settings,
        code=code,
        composition=composition
        or Composition(
            model=SpecRef(name=model_name, origin="catalog"),
            domain=SpecRef(name=grid_name, origin="custom"),
            # forcing_inputs/output_settings are always supplied now (no more
            # "model_default" fallback); a caller not tracking finer-grained
            # catalog/custom provenance (e.g. direct/test callers -- the wizard
            # builds its own Composition via _composition()) gets "custom".
            forcing=SpecRef(name=None, origin="custom"),
            cdr=SpecRef(name=None, origin="custom"),
            output=SpecRef(name=None, origin="custom"),
        ),
        provenance=Provenance(
            generated_at=generated_at,
            forge_version=forge_version,
            roms_tools_version=roms_tools_version,
            override_files_applied=[],
            notes=notes,
        ),
    )


def _build_forcing(
    inputs: dict[str, Any],
    topography_source: str | TopographySource | None = None,
    is_child: bool = False,
) -> Forcing:
    """Build the flat ``Forcing`` object from model inputs.

    The former ``Sources / inner Forcing`` two-level nesting is flattened here:
    initial_conditions and surface/boundary/tidal/river items all live directly on
    ``Forcing``. CDR is NOT part of this -- it lives on the blueprint's own
    top-level ``cdr`` section (:class:`CdrSpec`), built independently in
    :func:`build_forge_blueprint`.

    ``is_child`` (this domain has a parent grid) skips boundary items entirely --
    not just clears them afterward -- so a boundary-only source is never noted
    into ``resolved_datasets``/``datasets`` either. The same applies to
    initial_conditions when no source is given: it resolves to ``None`` for a
    child domain (state comes from the parent's nesting extraction) instead of
    the hard error a non-child domain gets.
    """
    ic_block = inputs.get("initial_conditions", {}) or {}
    forcing_block = inputs.get("forcing", {}) or {}

    def _items(key, cls, extra):
        out = []
        for it in forcing_block.get(key, []) or []:
            it = it or {}
            kw = {"source": _parse_source(it.get("source"))}
            for f in extra:
                if f in it:
                    if f == "custom_file":
                        # Accept the same three forms as grid_file (str/Path/dict/
                        # instance) -- see _normalize_user_file. Only river carries
                        # this today; the label generalizes to any future item.
                        kw[f] = _normalize_user_file(it[f], label=f"{key} custom_file")
                    else:
                        kw[f] = it[f]
            out.append(cls(**kw))
        return out

    # Source-prefill / horizontal-regrid / destination-extrapolation knobs shared by
    # SurfaceForcing, BoundaryForcing, TidalForcing, and InitialConditions (roms-tools
    # >=4). Kept as one tuple so all four load-back whitelists stay in lockstep with
    # each other and with tests/test_roms_tools_coverage.py::_FORGE_FIELDS.
    _REGRID_FIELDS = (
        "prefill",
        "prefill_kwargs",
        "regrid_method",
        "extrap_method",
        "extrap_kwargs",
    )

    if not ic_block.get("source"):
        # No IC source given. A child domain gets its state from the parent's
        # nesting extraction, so IC is optional -- skip building it entirely,
        # the same way boundary items are skipped above, so nothing leaks into
        # resolved_datasets/datasets. A non-child domain has no other source of
        # state, so this is a hard error.
        if not is_child:
            raise ValueError(
                "initial_conditions is required unless the domain has a parent "
                "grid (child domains inherit state from the parent's nesting "
                "extraction)"
            )
        ic = None
    else:
        ic_kw = {
            "source": _parse_source(ic_block.get("source")),
            "bgc_source": _parse_source(ic_block["bgc_source"])
            if ic_block.get("bgc_source")
            else None,
        }
        for f in ("bgc_interpolation_method", "allow_flex_time", *_REGRID_FIELDS):
            if f in ic_block:
                ic_kw[f] = ic_block[f]
        ic = InitialConditions(**ic_kw)

    surface = _items(
        "surface",
        SurfaceForcingItem,
        (
            "type",
            "correct_radiation",
            "coarse_grid_mode",
            "restoring_forces",
            *_REGRID_FIELDS,
        ),
    )
    boundary = (
        []
        if is_child
        else _items(
            "boundary",
            BoundaryForcingItem,
            (
                "type",
                "bgc_interpolation_method",
                *_REGRID_FIELDS,
            ),
        )
    )
    tidal = _items("tidal", TidalForcingItem, ("ntides", *_REGRID_FIELDS))
    river = _items(
        "river",
        RiverForcingItem,
        (
            "include_bgc",
            "convert_to_climatology",
            "bgc_source",
            "coast_snap_buffer_km",
            "domain_edge_buffer",
            "custom_file",
        ),
    )

    # snapshot every distinct logical source touched
    resolved: dict[str, ResolvedDataset] = {}

    def _note(src: SourceSpec):
        if not (src and src.name):
            return
        # CUSTOM_FILE (river.source) has no registry entry -- the file is
        # verified/staged directly from RiverForcingItem.custom_file, not from
        # SourceData, so noting it here would either raise "Unknown dataset"
        # downstream or cause bogus staging of a source that is never used.
        if src.name.upper() == "CUSTOM_FILE":
            return
        # An explicit path (SourceSpec.path) bypasses staging entirely -- mirrors
        # topography_path semantics -- so the item is never noted into
        # resolved_datasets/datasets (see input_data._resolve_source_block, which
        # returns an explicit path verbatim without ever calling path_for_source).
        if src.path:
            return
        resolved.setdefault(src.name, _resolved_dataset(src.name, src.glorys_layout))

    if ic is not None:
        _note(ic.source)
        _note(ic.bgc_source)
    for grp in (surface, boundary, tidal, river):
        for it in grp:
            _note(it.source)
    # River BGC source (a plain dict, not a SourceSpec — separate from it.source, the
    # river discharge source). CONSTANTS is not noted: it is roms-tools' own
    # auto-downloaded default and has no Forge SourceData handler/registry entry, so
    # staging it here would raise "Unknown dataset" downstream. Only a genuinely
    # Forge-staged BGC source (e.g. RIVR2O) needs to land in resolved_datasets/datasets
    # so the executor verifies it.
    for it in river:
        bgc_name = (it.bgc_source or {}).get("name")
        if bgc_name and str(bgc_name).upper() != "CONSTANTS":
            resolved.setdefault(str(bgc_name).upper(), _resolved_dataset(bgc_name))
    # topography source (now a Domain-level input, not read from ForcingSpec)
    topo = getattr(topography_source, "value", topography_source)
    if topo:
        resolved.setdefault(topo, _resolved_dataset(topo))

    return Forcing(
        initial_conditions=ic,
        surface=surface,
        boundary=boundary,
        tidal=tidal,
        river=river,
        resolved_datasets=resolved,
    )


# Back-compat alias used by some internal call sites
_build_sources = _build_forcing


def _build_code(
    model: dict[str, Any],
    templates_repo: CodeRepo,
    use_pio: bool = False,
    bgc_mode: str = "marbl",
    roms_ref: str | None = None,
    marbl_ref: str | None = None,
) -> Code:
    code_block = model.get("code", {}) or {}

    def _repo(name) -> CodeRepo | None:
        b = code_block.get(name)
        if not b:
            return None
        commit = b.get("commit")
        return CodeRepo(
            location=b.get("location"),
            commit=str(commit) if commit is not None else None,
            branch=b.get("branch"),
        )

    # Optional per-ModelSpec pin of the forge commit serving the templates. When set it
    # overrides the default branch (main); until pinned, template edits change build
    # output without a content_hash bump (see docs/dev-notes/executor-portability-plan.md).
    pinned_commit = code_block.get("templates_commit")

    def _template(stage) -> TemplateRepo:
        t = code_block.get(f"templates_{stage}", {}) or {}
        files = t.get("files", []) or []
        return TemplateRepo(
            location=templates_repo.location,
            commit=pinned_commit or templates_repo.commit,
            branch=None if pinned_commit else templates_repo.branch,
            # repo-root-relative dir (legacy key: `location`)
            directory=t.get("directory", t.get("location")),
            files=list(files),
        )

    roms = _repo("roms")
    if roms is None:
        raise ValueError("ModelSpec model.yaml is missing a code.roms repository")
    if roms_ref:
        # User override: commit/branch/tag are all valid `git checkout` targets and
        # collapse to the same checkout_target downstream (C-Star CodeRepository), so
        # store the override in `commit` and clear `branch` to satisfy C-Star's
        # "exactly one of commit/branch" validator.
        roms = CodeRepo(location=roms.location, commit=roms_ref, branch=None)
    pio = None
    if use_pio:
        pio = _repo("pio")
        if pio is None:
            raise ValueError(
                "use_pio=True but the ModelSpec model.yaml has no code.pio repository "
                "(Forge pins codebases for reproducibility)"
            )
    marbl = None
    if bgc_mode == "marbl":
        marbl = _repo("marbl")
        if marbl is None:
            raise ValueError(
                'bgc_mode="marbl" but the ModelSpec model.yaml has no code.marbl '
                "repository (Forge pins codebases for reproducibility)"
            )
        if marbl_ref:
            # Same convention as roms_ref above: any git checkout target lands in
            # `commit`, `branch` is cleared for C-Star's exactly-one validator.
            marbl = CodeRepo(location=marbl.location, commit=marbl_ref, branch=None)
    return Code(
        roms=roms,
        marbl=marbl,
        pio=pio,
        templates_compile_time=_template("compile_time"),
        templates_run_time=_template("run_time"),
    )


def _compute_v_sponge_default(grid_kwargs: dict[str, Any], grid: Any = None) -> float:
    """Lazily compute the default sponge viscosity from grid spacing (no grid build
    needed -- pure arithmetic on ``nx``/``size_x``). Mirrors ``_compute_dt_from_cfl``'s
    lazy-import pattern to keep this module dependency-light when unused.

    ``grid``, if given (a user-supplied ``grid_file``'s loaded grid, whose
    ``grid_kwargs`` carries no ``nx``/``size_x`` -- see
    ``Domain._grid_file_excludes_generation_geometry``), reads ``size_x``/``nx``
    from the grid object instead of ``grid_kwargs``.
    """
    try:
        from cstar_forge.forge.util import compute_v_sponge_from_grid
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "v_sponge was not provided and could not be computed: importing "
            "cstar_forge.forge.util failed. Pass v_sponge= explicitly to keep "
            f"resolution dependency-light. ({exc})"
        ) from exc
    if grid is not None:
        return compute_v_sponge_from_grid(grid.size_x, grid.nx)
    return compute_v_sponge_from_grid(grid_kwargs["size_x"], grid_kwargs["nx"])


def _compute_dt_from_cfl(grid_kwargs: dict[str, Any], grid: Any) -> float:
    """Lazily compute dt from the CFL criterion (needs roms_tools + cstar_forge.forge.util)."""
    try:
        from cstar_forge.forge.util import compute_timestep_from_cfl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "dt was not provided and could not be computed: importing "
            "cstar_forge.forge.util failed. Pass dt= explicitly to keep resolution "
            f"dependency-light. ({exc})"
        ) from exc
    if grid is None:
        import roms_tools as rt

        grid = rt.Grid(**grid_kwargs)
    return compute_timestep_from_cfl(
        grid_size_x=grid.size_x,
        grid_size_y=grid.size_y,
        grid_nx=grid.nx,
        grid_ny=grid.ny,
        grid_ds=grid.ds,
    )
