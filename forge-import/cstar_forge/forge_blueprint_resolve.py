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
from typing import Any

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
    """A model.yml ``source`` block: a bare name string or a dict."""
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
    """Read a ModelSpec directory into plain dicts (no heavy deps).

    Returns ``{"model": <model.yml dict>, "compile_defaults": {...},
    "run_defaults": {...}, "model_name": <dir name>}``. Resolves the
    ``_default_config_yaml`` paths relative to ``model_dir``.
    """
    model_dir = Path(model_dir)
    model = yaml.safe_load((model_dir / "model.yml").read_text())
    # single-model file: sections at top level; else unwrap a single named block
    if not any(k in model for k in ("templates", "settings", "code", "inputs")):
        if len(model) == 1:
            model = next(iter(model.values()))

    def _load_defaults(stage: str) -> dict[str, Any]:
        ref = model.get("settings", {}).get(stage, {}) or {}
        rel = ref.get("_default_config_yaml") or ref.get("default_config_yaml")
        if not rel:
            return {}
        p = Path(rel)
        if not p.is_absolute():
            p = model_dir / rel
        return yaml.safe_load(Path(p).read_text()) or {}

    return {
        "model_name": model_dir.name,
        "model": model,
        "compile_defaults": _load_defaults("compile_time"),
        "run_defaults": _load_defaults("run_time"),
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


def marbl_from_model_settings(model_settings: dict[str, Any]) -> bool:
    """Return whether MARBL is enabled, read from ``model_settings["cppdefs"]["marbl"]``.

    Replaces reads of ``model_spec.settings.properties.marbl`` in input generation.
    """
    return bool((model_settings.get("cppdefs") or {}).get("marbl", False))


def extract_output_settings(model_settings: dict[str, Any]) -> dict[str, Any]:
    """Pull the output-settings subset out of a full model_settings dict (used to
    seed an OutputSpec catalog entry and to gather the piece for save).
    """
    out: dict[str, Any] = {}
    for sec in OUTPUT_SECTIONS:
        if sec in model_settings:
            out[sec] = copy.deepcopy(model_settings[sec])
    marbl = model_settings.get("marbl_bgc", {}) or {}
    marbl_out = {f: marbl[f] for f in OUTPUT_MARBL_FIELDS if f in marbl}
    if marbl_out:
        out["marbl_bgc"] = copy.deepcopy(marbl_out)
    return out


def build_forge_blueprint(
    *,
    model_dir: str | Path,
    grid_name: str,
    grid_kwargs: dict[str, Any],
    open_boundaries: dict[str, bool],
    partitioning: dict[str, int],
    start_date: datetime,
    end_date: datetime,
    ensemble_id: int | None = None,
    description: str = "Generated blueprint",
    cdr_forcing: dict[str, Any] | None = None,
    forcing_inputs: dict[str, Any] | None = None,
    output_settings: dict[str, Any] | None = None,
    grid_kwargs_child: dict[str, Any] | None = None,
    grid_kwargs_parent: dict[str, Any] | None = None,
    metadata_child: dict[str, Any] | None = None,
    nesting_include_pressure_fluxes: bool = False,
    topography_path: str | None = None,
    run_time_overrides: dict[str, Any] | None = None,
    compile_time_overrides: dict[str, Any] | None = None,
    dt: float | None = None,
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
    """
    spec = load_model_spec_data(model_dir)
    model = spec["model"]
    model_name = spec["model_name"]
    run_defaults = copy.deepcopy(spec["run_defaults"])
    compile_defaults = copy.deepcopy(spec["compile_defaults"])
    # forcing inputs: an explicit selection (a ForcingSpec or UI-edited dict) overrides
    # the model's default `inputs`; both share the same shape.
    inputs = (
        forcing_inputs
        if forcing_inputs is not None
        else (model.get("inputs", {}) or {})
    )

    nx = grid_kwargs["nx"]
    ny = grid_kwargs["ny"]
    nvert = grid_kwargs["N"]
    size_x = grid_kwargs["size_x"]
    npx = partitioning["n_procs_x"]
    npy = partitioning["n_procs_y"]

    # ----- derived numerics --------------------------------------------------
    if dt is None:
        dt = _compute_dt_from_cfl(grid_kwargs, grid)
    n_days = (end_date - start_date).days
    ntimes = round(n_days * 24 * 3600 / dt)
    # v_sponge default = grid spacing (m) / 10  (== cstar_forge.forge.util.compute_v_sponge_from_grid)
    v_sponge = (size_x / nx) * 1000.0 / 10.0

    # ----- flat model_settings ----------------------------------------------
    settings: dict[str, Any] = copy.deepcopy(run_defaults)
    for sec in _PROCESSING_FILLED_SECTIONS:
        settings.pop(sec, None)
    settings["time_stepping"] = {"ntimes": ntimes, "dt": dt, "ndtfast": 60, "ninfo": 1}
    settings["v_sponge"] = {"v_sponge": v_sponge}
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
    cppdefs = dict(compile_defaults.get("cppdefs", {}))
    cppdefs["obc_west"] = bool(open_boundaries.get("west", False))
    cppdefs["obc_east"] = bool(open_boundaries.get("east", False))
    cppdefs["obc_north"] = bool(open_boundaries.get("north", False))
    cppdefs["obc_south"] = bool(open_boundaries.get("south", False))
    cppdefs["cdr_forcing"] = cdr_forcing is not None
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
    settings["cppdefs"] = cppdefs

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
    if output_settings:
        _deep_merge(settings, output_settings)

    # overrides win (mirror ForgeExecutor.configure_build precedence)
    if compile_time_overrides:
        _deep_merge(
            settings["cppdefs"],
            compile_time_overrides.get("cppdefs", compile_time_overrides),
        )
    if run_time_overrides:
        _deep_merge(settings, run_time_overrides)

    # ----- forcing (initial conditions + surface/boundary/tidal/river + CDR) --
    sources = _build_forcing(
        inputs, cdr_forcing
    )  # kept as `sources` locally for brevity

    # ----- code + templates --------------------------------------------------
    code = _build_code(model, templates_repo or DEFAULT_TEMPLATE_REPO)

    return ForgeBlueprint(
        identity=Identity(
            model_name=model_name,
            grid_name=grid_name,
            ensemble_id=ensemble_id,
            description=description,
        ),
        run=RunWindow(start_date=start_date, end_date=end_date),
        domain=Domain(
            grid_kwargs=grid_kwargs,
            topography_source=(inputs.get("grid", {}) or {}).get(
                "topography_source", "ETOPO5"
            ),
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
            forcing=PieceRef(
                name=None,
                origin="custom" if forcing_inputs is not None else "model_default",
            ),
            output=PieceRef(
                name=None,
                origin="custom" if output_settings is not None else "model_default",
            ),
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
    inputs: dict[str, Any], cdr_forcing: dict[str, Any] | None
) -> Forcing:
    """Build the flat ``Forcing`` object from model inputs + CDR config.

    The former ``Sources / inner Forcing`` two-level nesting is flattened here:
    initial_conditions and surface/boundary/tidal/river items all live directly on
    ``Forcing``.
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

    ic_kw = {
        "source": _parse_source(ic_block.get("source")),
        "bgc_source": _parse_source(ic_block["bgc_source"])
        if ic_block.get("bgc_source")
        else None,
    }
    for f in ("bgc_interpolation_method", "allow_flex_time"):
        if f in ic_block:
            ic_kw[f] = ic_block[f]
    ic = InitialConditions(**ic_kw)

    surface = _items(
        "surface",
        SurfaceForcingItem,
        ("type", "correct_radiation", "coarse_grid_mode", "restoring_forces"),
    )
    boundary = _items(
        "boundary",
        BoundaryForcingItem,
        (
            "type",
            "bgc_interpolation_method",
            "prefill",
            "prefill_kwargs",
            "regrid_method",
            "extrap_method",
            "extrap_kwargs",
        ),
    )
    tidal = _items("tidal", TidalForcingItem, ("ntides",))
    river = _items(
        "river",
        RiverForcingItem,
        ("include_bgc", "coast_snap_buffer_km", "domain_edge_buffer"),
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
    # topography source
    topo = (inputs.get("grid", {}) or {}).get("topography_source")
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


def _build_code(model: dict[str, Any], templates_repo: CodeRepo) -> Code:
    code_block = model.get("code", {}) or {}

    def _repo(name) -> CodeRepo | None:
        b = code_block.get(name)
        if not b:
            return None
        return CodeRepo(
            location=b.get("location"), commit=b.get("commit"), branch=b.get("branch")
        )

    templates = model.get("templates", {}) or {}

    # Optional per-ModelSpec pin of the forge commit serving the templates. When set it
    # overrides the default branch (main); until pinned, template edits change build
    # output without a content_hash bump (see docs/executor-portability-plan.md).
    pinned_commit = templates.get("commit")

    def _template(stage) -> TemplateRepo:
        t = templates.get(stage, {}) or {}
        files = (t.get("filter", {}) or {}).get("files", []) or []
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
        raise ValueError("ModelSpec model.yml is missing a code.roms repository")
    return Code(
        roms=roms,
        marbl=_repo("marbl"),
        templates_compile_time=_template("compile_time"),
        templates_run_time=_template("run_time"),
    )


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
