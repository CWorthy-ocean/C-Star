"""
Phase 1 resolver: ``build_forge_blueprint`` — assemble a validated :class:`ForgeBlueprint`
from the composable pieces (a ModelSpec + a domain selection + a run window).

This is the *collection / curation* half of the planned split (see
``docs/forge-blueprint-inventory.md``). It is intentionally **dependency-light**: it
reads the ModelSpec YAML directly and computes everything it can from plain inputs,
so a UI backend or a user's laptop can assemble and review a config without ROMS /
C-Star / roms_tools installed. The only value that needs a grid (``dt`` via the CFL
criterion, which needs the grid spacing ``ds``) is optional: pass ``dt=`` to stay
fully lightweight, or leave it ``None`` to have it computed (lazily importing
``roms_tools`` + ``cstar_forge.forge.util``).

What this does NOT do (by design — it is host- and artifact-independent):
* no machine / path resolution (Phase 2, on the run host),
* no source downloads or grid/forcing file generation (Phase 2),
* no ``s_coord`` / file paths / ``title`` / ``output_root_name`` (filled at
  processing or derived from identity).

NOTE: the dataset registry below is a *snapshot* of ``cstar_forge.forge.source_data``
mappings, duplicated here to keep this module importable without the heavy stack.
It should be unified with ``source_data.py`` once the two-phase refactor lands.
"""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import yaml

# Dual import: package context (production) or standalone file (lightweight / UI / test).
try:  # pragma: no cover - exercised both ways
    from cstar_forge.forge.forge_blueprint import (
        BoundaryForcingItem,
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        ForgeBlueprint,
        Identity,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        PieceRef,
        Provenance,
        ResolvedDataset,
        RiverForcingItem,
        RunWindow,
        SourceSpec,
        SurfaceForcingItem,
        TemplateRepo,
        TidalForcingItem,
        TopographySource,
        sanitize_name,
    )
except ImportError:  # pragma: no cover
    from forge_blueprint import (  # type: ignore
        BoundaryForcingItem,
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        ForgeBlueprint,
        Identity,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        PieceRef,
        Provenance,
        ResolvedDataset,
        RiverForcingItem,
        RunWindow,
        SourceSpec,
        SurfaceForcingItem,
        TemplateRepo,
        TidalForcingItem,
        TopographySource,
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
    )


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base`` (override wins). Returns base."""
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
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

# The "output settings" piece (OutputSpec): whole model_settings sections that are
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
    seed an OutputSpec catalog entry and to gather the piece for save).
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
    partitioning: dict[str, int],
    start_date: datetime,
    end_date: datetime,
    name: str | None = None,
    description: str = "Generated blueprint",
    cdr_forcing: dict[str, Any] | None = None,
    cdr_forcing_yaml: str | Path | None = None,
    forcing_inputs: dict[str, Any] | None = None,
    output_settings: dict[str, Any] | None = None,
    grid_kwargs_child: dict[str, Any] | None = None,
    grid_kwargs_parent: dict[str, Any] | None = None,
    metadata_child: dict[str, Any] | None = None,
    nesting_include_pressure_fluxes: bool = False,
    topography_path: str | None = None,
    topography_source: str | TopographySource = TopographySource.ETOPO5,
    use_pio: bool | None = None,
    bgc_mode: Literal["marbl", "none"] | None = None,
    roms_ref: str | None = None,
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
    """Resolve the composable pieces into a validated, host-independent ``ForgeBlueprint``.

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

    ``use_pio`` is a per-run toggle mirroring ``bgc_mode``: it overwrites
    ``cppdefs.use_pio`` and gates whether ``code.pio`` is populated (raising if PIO
    is requested but the ModelSpec has no ``code.pio`` repository). If ``None`` (the
    default), it falls back to the ModelSpec's own ``use_pio`` (itself defaulting to
    ``False``) -- the ModelSpec is the single source of the default; pass an
    explicit value to override it for one run.

    ``cdr_forcing_yaml``, if given, takes precedence over ``cdr_forcing``: it is a
    path to (or the raw text of) a roms-tools ``CDRForcing.to_yaml(...)`` dump, read
    via :func:`read_cdr_forcing_yaml`. This is the wizard's upload path made
    resolver-native; a caller may pass either kwarg, not both meaningfully at once.

    ``name`` is the blueprint's canonical name (``identity.name``); if omitted, this
    computes and sanitizes the default (``{model_name}_{grid_name}_{n_procs}procs``).

    ``v_sponge`` and ``dt`` are both domain-owned numerics with the same pattern:
    if ``None`` (the default), each is derived from the grid -- ``v_sponge`` from
    grid spacing via ``cstar_forge.forge.util.compute_v_sponge_from_grid``, ``dt``
    from the CFL criterion via ``_compute_dt_from_cfl`` (builds a grid); pass an
    explicit value (e.g. one restored from a saved DomainSpec) to use it verbatim
    instead. The resolver is the sole writer of both ``domain.v_sponge`` /
    ``domain.dt`` and the identical ``model_settings["v_sponge"]["v_sponge"]`` /
    ``model_settings["time_stepping"]["dt"]`` leaves -- each pair is always
    written together and must never diverge.
    """
    if cdr_forcing_yaml is not None:
        cdr_forcing = read_cdr_forcing_yaml(cdr_forcing_yaml)
    elif cdr_forcing is not None:
        # defensive strip -- a caller may hand us a dict copied straight from a
        # to_yaml() dump (which still carries the human-readable metadata block).
        cdr_forcing = {k: v for k, v in cdr_forcing.items() if k != "_tracer_metadata"}

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

    nx = grid_kwargs["nx"]
    ny = grid_kwargs["ny"]
    nvert = grid_kwargs["N"]
    npx = partitioning["n_procs_x"]
    npy = partitioning["n_procs_y"]

    # ----- derived numerics --------------------------------------------------
    if dt is None:
        dt = _compute_dt_from_cfl(grid_kwargs, grid)
    n_days = (end_date - start_date).days
    ntimes = round(n_days * 24 * 3600 / dt)
    # v_sponge default = grid spacing (m) / 10 -- a caller (e.g. the wizard, restoring
    # a user override saved into a DomainSpec) may pass an explicit value instead.
    if v_sponge is None:
        v_sponge = _compute_v_sponge_default(grid_kwargs)

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
            "np_xi": npx,
            "np_eta": npy,
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
    cppdefs["cdr_forcing"] = cdr_forcing is not None
    cppdefs["use_pio"] = bool(use_pio)
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

    # OutputSpec piece: deep-merge the output-settings selection over the model
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

    # ----- forcing (initial conditions + surface/boundary/tidal/river + CDR) --
    # A child grid (has a parent) receives its boundary values from the parent's
    # nesting.nc extraction, not from reanalysis boundary forcing -- the executor
    # already skips boundary-forcing generation for a child (grid_parent is not
    # None, see ForgeExecutor/RomsMarblInputData). Skip building boundary items
    # here too (defense-in-depth, the wizard also clears this at the UI layer) --
    # done inside _build_forcing (not by zeroing sources.boundary afterward) so a
    # boundary-only source is never noted into resolved_datasets/datasets either.
    sources = _build_forcing(
        inputs,
        cdr_forcing,
        topography_source,
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
        if sources.initial_conditions.bgc_source is not None:
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
    )

    default_name = sanitize_name(f"{model_name}_{grid_name}_{npx * npy}procs")
    return ForgeBlueprint(
        identity=Identity(
            name=name or default_name,
            description=description,
        ),
        run=RunWindow(start_date=start_date, end_date=end_date),
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
            partitioning=Partitioning(n_procs_x=npx, n_procs_y=npy),
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
        ),
        forcing=sources,
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
            model=PieceRef(name=model_name, origin="catalog"),
            domain=PieceRef(name=grid_name, origin="custom"),
            # forcing_inputs/output_settings are always supplied now (no more
            # "model_default" fallback); a caller not tracking finer-grained
            # catalog/custom provenance (e.g. direct/test callers -- the wizard
            # builds its own Composition via _composition()) gets "custom".
            forcing=PieceRef(name=None, origin="custom"),
            output=PieceRef(name=None, origin="custom"),
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
    cdr_forcing: dict[str, Any] | None,
    topography_source: str | TopographySource | None = None,
    is_child: bool = False,
) -> Forcing:
    """Build the flat ``Forcing`` object from model inputs + CDR config.

    The former ``Sources / inner Forcing`` two-level nesting is flattened here:
    initial_conditions and surface/boundary/tidal/river items all live directly on
    ``Forcing``.

    ``is_child`` (this domain has a parent grid) skips boundary items entirely --
    not just clears them afterward -- so a boundary-only source is never noted
    into ``resolved_datasets``/``datasets`` either.
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
        ),
    )

    # snapshot every distinct logical source touched
    resolved: dict[str, ResolvedDataset] = {}

    def _note(src: SourceSpec):
        if src and src.name:
            resolved.setdefault(
                src.name, _resolved_dataset(src.name, src.glorys_layout)
            )

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
        cdr_forcing=cdr_forcing,
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
    # output without a content_hash bump (see docs/executor-portability-plan.md).
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
    return Code(
        roms=roms,
        marbl=marbl,
        pio=pio,
        templates_compile_time=_template("compile_time"),
        templates_run_time=_template("run_time"),
    )


def _compute_v_sponge_default(grid_kwargs: dict[str, Any]) -> float:
    """Lazily compute the default sponge viscosity from grid spacing (no grid build
    needed -- pure arithmetic on ``nx``/``size_x``). Mirrors ``_compute_dt_from_cfl``'s
    lazy-import pattern to keep this module dependency-light when unused.
    """
    try:
        from cstar_forge.forge.util import compute_v_sponge_from_grid
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "v_sponge was not provided and could not be computed: importing "
            "cstar_forge.forge.util failed. Pass v_sponge= explicitly to keep "
            f"Phase 1 dependency-light. ({exc})"
        ) from exc
    return compute_v_sponge_from_grid(grid_kwargs["size_x"], grid_kwargs["nx"])


def _compute_dt_from_cfl(grid_kwargs: dict[str, Any], grid: Any) -> float:
    """Lazily compute dt from the CFL criterion (needs roms_tools + cstar_forge.forge.util)."""
    try:
        from cstar_forge.forge.util import compute_timestep_from_cfl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "dt was not provided and could not be computed: importing "
            "cstar_forge.forge.util failed. Pass dt= explicitly to keep Phase 1 "
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
