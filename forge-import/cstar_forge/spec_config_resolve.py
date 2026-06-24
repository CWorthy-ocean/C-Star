"""
Phase 1 resolver: ``build_spec_config`` — assemble a validated :class:`SpecConfig`
from the composable pieces (a ModelSpec + a domain selection + a run window).

This is the *collection / curation* half of the planned split (see
``docs/spec-config-inventory.md``). It is intentionally **dependency-light**: it
reads the ModelSpec YAML directly and computes everything it can from plain inputs,
so a UI backend or a user's laptop can assemble and review a config without ROMS /
C-Star / roms_tools installed. The only value that needs a grid (``dt`` via the CFL
criterion, which needs the grid spacing ``ds``) is optional: pass ``dt=`` to stay
fully lightweight, or leave it ``None`` to have it computed (lazily importing
``roms_tools`` + ``cstar_forge.util``).

What this does NOT do (by design — it is host- and artifact-independent):
* no machine / path resolution (Phase 2, on the run host),
* no source downloads or grid/forcing file generation (Phase 2),
* no ``s_coord`` / file paths / ``title`` / ``output_root_name`` (filled at
  processing or derived from identity).

NOTE: the dataset registry below is a *snapshot* of ``cstar_forge.source_data``
mappings, duplicated here to keep this module importable without the heavy stack.
It should be unified with ``source_data.py`` once the two-phase refactor lands.
"""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

# Dual import: package context (production) or standalone file (lightweight / UI / test).
try:  # pragma: no cover - exercised both ways
    from .spec_config import (
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        Identity,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        PieceRef,
        Provenance,
        ResolvedDataset,
        RunWindow,
        SourceSpec,
        SpecConfig,
        SurfaceForcingItem,
        BoundaryForcingItem,
        TidalForcingItem,
        RiverForcingItem,
        Sources,
        TemplateRepo,
    )
except ImportError:  # pragma: no cover
    from spec_config import (  # type: ignore
        Code,
        CodeRepo,
        Composition,
        Domain,
        Forcing,
        Identity,
        InitialConditions,
        OpenBoundaries,
        Partitioning,
        PieceRef,
        Provenance,
        ResolvedDataset,
        RunWindow,
        SourceSpec,
        SpecConfig,
        SurfaceForcingItem,
        BoundaryForcingItem,
        TidalForcingItem,
        RiverForcingItem,
        Sources,
        TemplateRepo,
    )

# Default repo the bundled ModelSpec templates live in (until model.yml carries
# explicit template repo coordinates).
DEFAULT_TEMPLATE_REPO = CodeRepo(
    location="https://github.com/CWorthy-ocean/cstar-forge.git", branch="main"
)

# --- snapshot of source_data.py logical-name -> dataset resolution -----------
# (dataset_key, dataset_id, url, streamable). Keep in sync with source_data.py.
_DATASET_REGISTRY: Dict[str, Dict[str, Any]] = {
    "GLORYS_REGIONAL": {"dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m"},
    "GLORYS_GLOBAL": {"dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m"},
    "UNIFIED_BGC": {"url": "https://drive.google.com/uc?id=1wUNwVeJsd6yM7o-5kCx-vM3wGwlnGSiq"},
    "SRTM15_V2.7": {"url": "https://topex.ucsd.edu/pub/srtm15_plus/SRTM15_V2.7.nc"},
    "MBL_CO2": {"url": "https://gml.noaa.gov/ccgg/mbl/tmp/co2_GHGreference.1785677502_surface.txt"},
    "TPXO": {},
    "WOA": {"url": "https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/"},
    "ETOPO5": {},
    "ERA5": {"streamable": True},
    "DAI": {"streamable": True},
}


def _resolve_dataset_key(name: str, glorys_layout: Optional[str] = None) -> str:
    """Map a logical source name to a registry key (mirrors SourceData)."""
    up = name.upper()
    if up == "GLORYS":
        return "GLORYS_GLOBAL" if (glorys_layout or "").lower() == "global" else "GLORYS_REGIONAL"
    if up == "UNIFIED":
        return "UNIFIED_BGC"
    if up == "SRTM15":
        return "SRTM15_V2.7"
    if up == "MBL_CO2":
        return "MBL_CO2"
    return up if up in _DATASET_REGISTRY else name


def _resolved_dataset(name: str, glorys_layout: Optional[str] = None) -> ResolvedDataset:
    key = _resolve_dataset_key(name, glorys_layout)
    info = _DATASET_REGISTRY.get(key, {})
    return ResolvedDataset(
        dataset_key=key,
        dataset_id=info.get("dataset_id"),
        url=info.get("url"),
        streamable=bool(info.get("streamable", False)),
    )


def _parse_source(block: Any) -> SourceSpec:
    """A model.yml ``source`` block: a bare name string or a dict."""
    if isinstance(block, str):
        name = block
        d: Dict[str, Any] = {}
    else:
        d = dict(block or {})
        name = d.get("name")
    layout = d.get("glorys_layout")
    return SourceSpec(
        name=name,
        dataset_key=_resolve_dataset_key(name, layout),
        climatology=bool(d.get("climatology", False)),
        glorys_layout=layout,
    )


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``override`` into ``base`` (override wins). Returns base."""
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def load_model_spec_data(model_dir: Union[str, Path]) -> Dict[str, Any]:
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

    def _load_defaults(stage: str) -> Dict[str, Any]:
        ref = (model.get("settings", {}).get(stage, {}) or {})
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
_PROCESSING_FILLED_SECTIONS = ("grid", "initial", "forcing", "s_coord", "title", "output_root_name")


def build_spec_config(
    *,
    model_dir: Union[str, Path],
    grid_name: str,
    grid_kwargs: Dict[str, Any],
    open_boundaries: Dict[str, bool],
    partitioning: Dict[str, int],
    start_date: datetime,
    end_date: datetime,
    ensemble_id: Optional[int] = None,
    description: str = "Generated blueprint",
    cdr_forcing: Optional[Dict[str, Any]] = None,
    run_time_overrides: Optional[Dict[str, Any]] = None,
    compile_time_overrides: Optional[Dict[str, Any]] = None,
    dt: Optional[float] = None,
    grid: Any = None,
    templates_repo: Optional[CodeRepo] = None,
    composition: Optional[Composition] = None,
    generated_at: Optional[datetime] = None,
    forge_version: Optional[str] = None,
    roms_tools_version: Optional[str] = None,
    notes: Optional[str] = None,
) -> SpecConfig:
    """Resolve the composable pieces into a validated, host-independent ``SpecConfig``.

    Parameters mirror the logical inputs a UI would collect. ``dt`` may be supplied
    directly (fully lightweight); if ``None`` it is computed from the CFL criterion,
    which lazily imports ``roms_tools`` (to build the grid for ``ds``) and
    ``cstar_forge.util``.
    """
    spec = load_model_spec_data(model_dir)
    model = spec["model"]
    model_name = spec["model_name"]
    run_defaults = copy.deepcopy(spec["run_defaults"])
    compile_defaults = copy.deepcopy(spec["compile_defaults"])
    inputs = model.get("inputs", {}) or {}

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
    ntimes = int(round(n_days * 24 * 3600 / dt))
    # v_sponge default = grid spacing (m) / 10  (== cstar_forge.util.compute_v_sponge_from_grid)
    v_sponge = (size_x / nx) * 1000.0 / 10.0

    # ----- flat model_settings ----------------------------------------------
    settings: Dict[str, Any] = copy.deepcopy(run_defaults)
    for sec in _PROCESSING_FILLED_SECTIONS:
        settings.pop(sec, None)
    settings["time_stepping"] = {"ntimes": ntimes, "dt": dt, "ndtfast": 60, "ninfo": 1}
    settings["v_sponge"] = {"v_sponge": v_sponge}
    param = dict(settings.get("param", {}))
    param.update({"llm": nx, "mmm": ny, "n": nvert, "np_xi": npx, "np_eta": npy,
                  "nsub_x": 1, "nsub_e": 1})
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
    settings["cppdefs"] = cppdefs

    # overrides win (mirror CstarSpecBuilder.configure_build precedence)
    if compile_time_overrides:
        _deep_merge(settings["cppdefs"], compile_time_overrides.get("cppdefs", compile_time_overrides))
    if run_time_overrides:
        _deep_merge(settings, run_time_overrides)

    # ----- sources (the "forcing" piece) -------------------------------------
    sources = _build_sources(inputs, cdr_forcing)

    # ----- code + templates --------------------------------------------------
    code = _build_code(model, templates_repo or DEFAULT_TEMPLATE_REPO)

    return SpecConfig(
        identity=Identity(model_name=model_name, grid_name=grid_name,
                          ensemble_id=ensemble_id, description=description),
        run=RunWindow(start_date=start_date, end_date=end_date),
        domain=Domain(
            grid_kwargs=grid_kwargs,
            topography_source=(inputs.get("grid", {}) or {}).get("topography_source", "ETOPO5"),
            open_boundaries=OpenBoundaries(**{k: bool(open_boundaries.get(k, False))
                                              for k in ("north", "south", "east", "west")}),
            partitioning=Partitioning(n_procs_x=npx, n_procs_y=npy)),
        sources=sources,
        properties=dict(model.get("settings", {}).get("properties", {}) or {}),
        model_settings=settings,
        code=code,
        composition=composition or Composition(
            model=PieceRef(name=model_name, origin="catalog"),
            domain=PieceRef(name=grid_name, origin="custom"),
            forcing=PieceRef(name=None, origin="model_default")),
        provenance=Provenance(generated_at=generated_at, forge_version=forge_version,
                              roms_tools_version=roms_tools_version,
                              override_files_applied=[], notes=notes),
    )


def _build_sources(inputs: Dict[str, Any], cdr_forcing: Optional[Dict[str, Any]]) -> Sources:
    ic_block = inputs.get("initial_conditions", {}) or {}
    forcing_block = inputs.get("forcing", {}) or {}

    def _items(key, cls, extra):
        out = []
        for it in (forcing_block.get(key, []) or []):
            it = it or {}
            kw = {"source": _parse_source(it.get("source"))}
            for f in extra:
                if f in it:
                    kw[f] = it[f]
            out.append(cls(**kw))
        return out

    forcing = Forcing(
        surface=_items("surface", SurfaceForcingItem,
                       ("type", "correct_radiation", "coarse_grid_mode", "restoring_forces")),
        boundary=_items("boundary", BoundaryForcingItem, ("type",)),
        tidal=_items("tidal", TidalForcingItem, ("ntides",)),
        river=_items("river", RiverForcingItem, ("include_bgc",)),
    )

    ic = InitialConditions(
        source=_parse_source(ic_block.get("source")),
        bgc_source=_parse_source(ic_block["bgc_source"]) if ic_block.get("bgc_source") else None,
    )

    # snapshot every distinct logical source touched
    resolved: Dict[str, ResolvedDataset] = {}
    def _note(src: SourceSpec):
        if src and src.name:
            resolved.setdefault(src.name, _resolved_dataset(src.name, src.glorys_layout))
    _note(ic.source)
    _note(ic.bgc_source)
    for grp in (forcing.surface, forcing.boundary, forcing.tidal, forcing.river):
        for it in grp:
            _note(it.source)
    # topography source
    topo = (inputs.get("grid", {}) or {}).get("topography_source")
    if topo:
        resolved.setdefault(topo, _resolved_dataset(topo))

    return Sources(initial_conditions=ic, forcing=forcing,
                   cdr_forcing=cdr_forcing, resolved_datasets=resolved)


def _build_code(model: Dict[str, Any], templates_repo: CodeRepo) -> Code:
    code_block = model.get("code", {}) or {}

    def _repo(name) -> Optional[CodeRepo]:
        b = code_block.get(name)
        if not b:
            return None
        return CodeRepo(location=b.get("location"), commit=b.get("commit"), branch=b.get("branch"))

    templates = model.get("templates", {}) or {}

    def _template(stage) -> TemplateRepo:
        t = templates.get(stage, {}) or {}
        files = ((t.get("filter", {}) or {}).get("files", []) or [])
        return TemplateRepo(location=templates_repo.location, commit=templates_repo.commit,
                            branch=templates_repo.branch, directory=t.get("location"), files=list(files))

    roms = _repo("roms")
    if roms is None:
        raise ValueError("ModelSpec model.yml is missing a code.roms repository")
    return Code(roms=roms, marbl=_repo("marbl"),
                templates_compile_time=_template("compile_time"),
                templates_run_time=_template("run_time"))


def _compute_dt_from_cfl(grid_kwargs: Dict[str, Any], grid: Any) -> float:
    """Lazily compute dt from the CFL criterion (needs roms_tools + cstar_forge.util)."""
    try:
        from cstar_forge.util import compute_timestep_from_cfl
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "dt was not provided and could not be computed: importing "
            "cstar_forge.util failed. Pass dt= explicitly to keep Phase 1 "
            f"dependency-light. ({exc})"
        ) from exc
    if grid is None:
        import roms_tools as rt  # noqa
        grid = rt.Grid(**grid_kwargs)
    return compute_timestep_from_cfl(
        grid_size_x=grid.size_x, grid_size_y=grid.size_y,
        grid_nx=grid.nx, grid_ny=grid.ny, grid_ds=grid.ds,
    )
