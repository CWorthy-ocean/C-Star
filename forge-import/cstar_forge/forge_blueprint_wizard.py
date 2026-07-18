"""
An ``ipywidgets`` wizard for assembling and reviewing a :class:`ForgeBlueprint`.

This is a thin UI shell over :func:`cstar_forge.forge_blueprint_resolve.build_forge_blueprint`:
the widgets only *collect inputs and display the resolved result* — all resolution
and validation stay in the resolver. That keeps the notebook UI interchangeable with
any future app/WASM front-end and lets the logic be tested without rendering.

Usage (in a Jupyter notebook)::

    from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizard
    wiz = ForgeBlueprintWizard()
    wiz.display()
    # ... pick a model + domain, tweak fields, review the live YAML, Save ...
    cfg = wiz.config            # the current resolved ForgeBlueprint (or None if invalid)

The wizard discovers Models from ``catalog/ModelSpec/`` and Domains from
``catalog/DomainSpec/``; selecting a cataloged Domain prefills the grid kwargs,
boundaries, partitioning, and dates. The live preview re-resolves on every change
(using the ``dt`` field, so it never builds a grid); the "Compute dt (CFL)" button
is the only action that builds a grid (needs ``roms_tools``).
"""

from __future__ import annotations

import base64
import copy
import json
import re
import typing
from datetime import date, datetime
from pathlib import Path
from typing import Any, get_args, get_origin

import yaml
from pydantic import BaseModel

from cstar_forge.forge.forge_blueprint import (
    BgcBoundarySource,
    BgcInitialConditionsSource,
    BgcInterpMethod,
    BgcSurfaceSource,
    BoundaryType,
    ClimatologyMode,
    CoarseGridMode,
    Composition,
    ExtrapMethod,
    ForgeBlueprint,
    InitialConditionsSource,
    PhysicsBoundarySource,
    PhysicsSurfaceSource,
    PieceRef,
    Prefill,
    RegridMethod,
    RestoringSurfaceSource,
    RiverSource,
    SurfaceType,
    TidalSource,
)
from cstar_forge.forge.namelist_model import RunTimeSettings, validate_run_time_sections
from cstar_forge.forge_blueprint_resolve import (
    OUTPUT_SECTIONS,
    PARTIAL_OUTPUT_SECTIONS,
    build_forge_blueprint,
    extract_output_settings,
    load_model_spec_data,
    read_cdr_forcing_yaml,
)

# ===========================================================================
# Help text — shown as widget tooltips on hover (tooltip= kwarg, all widgets)
#
# Automated: namelist fields are looked up live from the cstar.roms.namelist
# field descriptions (set in the RomsNamelist / group models).
# Manual: forcing/grid/nesting/run-window knobs listed in HELP_TEXT below.
# ===========================================================================

# Keyed by (context, field_name). "context" is a category string: a forcing
# category ("surface","boundary","tidal","river","ic"), a grid section ("grid"),
# a nesting section ("nesting"), or a run-window field ("run").
# Falls back to just field_name for unambiguous global names.
HELP_TEXT: dict[str, str] = {
    # ---- identity / run window ------------------------------------------------
    (
        "run",
        "model_ref_date",
    ): "ROMS model t=0 reference date. All time values in the output files are relative "
    "to this date. Passed to every roms-tools object (InitialConditions, SurfaceForcing, "
    "BoundaryForcing, TidalForcing, CDRForcing). Default: 2000-01-01.",
    ("run", "start"): "Start date of the simulation run window.",
    ("run", "end"): "End date of the simulation run window.",
    (
        "run",
        "dt",
    ): "Barotropic time step in seconds. Leave blank to compute from the CFL criterion "
    "(click 'Compute dt (CFL)' — requires roms_tools).",
    ("run", "description"): "Human-readable description of this blueprint.",
    ("export", "name"): "Canonical blueprint name. Drives the save filename, "
    "working_dir, casename, and generated file stems. Defaults to a derived "
    "name (model_grid_NprocsProcs); edit to override.",
    # ---- grid ------------------------------------------------------------------
    ("grid", "nx"): "Number of horizontal grid points in the x-direction (longitude).",
    ("grid", "ny"): "Number of horizontal grid points in the y-direction (latitude).",
    ("grid", "size_x"): "Domain extent in the x-direction in kilometers.",
    ("grid", "size_y"): "Domain extent in the y-direction in kilometers.",
    ("grid", "center_lon"): "Longitude of the domain center in degrees East.",
    ("grid", "center_lat"): "Latitude of the domain center in degrees North.",
    (
        "grid",
        "rot",
    ): "Rotation of the grid x-axis from lines of constant latitude, in degrees "
    "(positive = counterclockwise).",
    ("grid", "N"): "Number of vertical sigma levels.",
    ("grid", "theta_s"): "S-coordinate surface stretching parameter (0 < θs ≤ 10). "
    "Larger values concentrate levels near the surface.",
    ("grid", "theta_b"): "S-coordinate bottom stretching parameter (0 < θb ≤ 4). "
    "Larger values concentrate levels near the bottom.",
    (
        "grid",
        "hc",
    ): "S-coordinate critical depth in meters. Controls the transition from "
    "sigma to stretched coordinates near the surface.",
    (
        "grid",
        "hmin",
    ): "Minimum allowable ocean depth in meters (default 5.0). Grid cells shallower "
    "than hmin are set to exactly hmin during grid generation.",
    (
        "grid",
        "close_narrow_channels",
    ): "If checked, narrow water channels (width < one grid cell) in the land mask "
    "are closed after the mask is generated from NaturalEarth coastlines.",
    (
        "grid",
        "mask_shapefile",
    ): "Path to a custom shapefile used to determine the land/sea mask instead of the "
    "default NaturalEarth 10 m coastlines. Leave blank to use the default.",
    (
        "grid",
        "topography_path",
    ): "Path to a custom topography file for the grid's topography source. Leave "
    "blank to use the default: staged for non-ETOPO5 sources, fetched by roms-tools "
    "for ETOPO5.",
    # ---- nesting ---------------------------------------------------------------
    (
        "nesting",
        "nest_enable",
    ): "Enable a child (nested) domain. Generates nesting.nc to supply boundary "
    "conditions for the inner domain from the outer model.",
    (
        "nesting",
        "nest_domain_dd",
    ): "Optionally prefill the child grid kwargs from a cataloged DomainSpec. "
    "You can still edit any child-grid field after selecting.",
    (
        "nesting",
        "nest_period",
    ): "How often (seconds) boundary values are written to nesting.nc. "
    "Must divide evenly into the parent model output interval.",
    (
        "nesting",
        "nest_pressure_fluxes",
    ): "Include baroclinic pressure flux variables in nesting.nc. Required when "
    "the namelist calc_pflx setting is enabled in the child domain.",
    # ---- partitioning ----------------------------------------------------------
    (
        "domain",
        "npx",
    ): "Number of MPI tiles in the x-direction (xi). Total MPI tasks = npx × npy.",
    (
        "domain",
        "npy",
    ): "Number of MPI tiles in the y-direction (eta). Total MPI tasks = npx × npy.",
    # Generic (context-independent) fallback for any source's path box.
    "path": "Explicit path to a custom dataset file for this source. "
    "Leave blank to use the default derived path (staged/streamed location).",
    # ---- IC --------------------------------------------------------------------
    (
        "ic",
        "ic_name",
    ): "Logical source name for physics initial conditions, e.g. 'GLORYS'. "
    "The catalog alias map resolves this to the actual dataset registry key.",
    (
        "ic",
        "ic_layout",
    ): "GLORYS spatial layout override: 'regional' (default) or 'global'. "
    "Leave blank for non-GLORYS sources.",
    (
        "ic",
        "ic_bgc_name",
    ): "Logical source name for BGC initial conditions, e.g. 'UNIFIED'. "
    "Leave blank to omit BGC initial conditions.",
    (
        "ic",
        "ic_bgc_clim",
    ): "Use the BGC source as a climatology (annual-mean repeated each year) "
    "rather than a time-varying dataset.",
    (
        "ic",
        "ic_bgc_interp",
    ): "Vertical interpolation for BGC tracer initial conditions. 'depth' — linear in "
    "depth (default). 'density' — linear in potential-density (isopycnal) space, "
    "reducing errors across sloping isopycnals. 'density_mld' — density interpolation "
    "anchored to the mixed-layer depth, preserving sub-mixed-layer feature depths.",
    (
        "ic",
        "ic_flex_time",
    ): "Allow a ±24-hour search window when looking for the requested ini_time in the "
    "source dataset. Useful when the exact timestamp is absent.",
    # ---- surface forcing -------------------------------------------------------
    (
        "surface",
        "name",
    ): "Logical source name, e.g. 'ERA5' for surface physics or 'UNIFIED' for BGC surface. "
    "The catalog alias map resolves this to the actual dataset.",
    (
        "surface",
        "type",
    ): "Forcing type: 'physics' (u/v wind, heat, freshwater fluxes), 'bgc' (pCO₂ / "
    "iron deposition), or 'restoring' (SST or SSS relaxation).",
    (
        "surface",
        "climatology",
    ): "Treat the source as a climatology repeated each year rather than interannual.",
    (
        "surface",
        "glorys_layout",
    ): "GLORYS spatial layout: 'regional' (default) or 'global'. Leave blank for ERA5/UNIFIED.",
    (
        "surface",
        "correct_radiation",
    ): "Apply shortwave and longwave radiation corrections to ERA5 fluxes using an "
    "observational climatology. Recommended for most physics surface forcing.",
    (
        "surface",
        "wind_dropoff",
    ): "Apply exponential coastal wind-speed reduction with a 12.5 km e-folding "
    "scale. Reduces spurious upwelling near the coast from gridded wind products.",
    (
        "surface",
        "coarse_grid_mode",
    ): "'auto' — interpolate onto a factor-2 coarsened grid only when roms-tools detects "
    "the source is coarser than the ROMS grid (default). 'always' — always coarsen. "
    "'never' — always use the full-resolution source.",
    (
        "surface",
        "restoring_forces",
    ): "Comma-separated list of variables to apply restoring forces to, e.g. 'sss' or "
    "'sst,sss'. Only relevant for type='restoring'. Enables sal_restore/SST correction.",
    # ---- boundary forcing ------------------------------------------------------
    (
        "boundary",
        "name",
    ): "Logical source name for boundary conditions, e.g. 'GLORYS' (physics) or "
    "'UNIFIED' (BGC). Resolved via the catalog alias map.",
    (
        "boundary",
        "type",
    ): "Boundary forcing type: 'physics' (T, S, u, v, ζ) or 'bgc' (BGC tracers).",
    (
        "boundary",
        "climatology",
    ): "Treat as an annual climatology rather than interannual time series.",
    (
        "boundary",
        "glorys_layout",
    ): "GLORYS spatial layout: 'regional' or 'global'. Leave blank for non-GLORYS.",
    (
        "boundary",
        "bgc_interpolation_method",
    ): "Vertical interpolation for BGC boundary tracers (type='bgc'). 'depth' (default), "
    "'density' (isopycnal space), or 'density_mld' (mixed-layer-depth anchored). "
    "Density methods build a physics BoundaryForcing companion to supply the target T/S.",
    (
        "boundary",
        "prefill",
    ): "Fill NaN (land/void) source cells before regridding. Blank = no source prefill "
    "(NaN-aware regrid + extrapolation, recommended with xESMF). '2d_lateral_fill' = "
    "legacy AMG Poisson fill; 'inverse_dist'/'nearest_s2d' = xESMF source fills; "
    "'nearest_neighbor' = cheap scipy fill (also the fallback when xESMF is absent).",
    (
        "boundary",
        "regrid_method",
    ): "Horizontal regrid engine. Blank/'auto' = xESMF if installed else scipy. 'xesmf' = "
    "force xESMF (errors if absent). 'scipy' = force scipy (byte-reproducible with prefill).",
    (
        "boundary",
        "extrap_method",
    ): "Destination extrapolation on the default (no-prefill) path to guarantee NaN-free "
    "boundaries. Blank = 'inverse_dist' (effective default). 'nearest_s2d' = single "
    "nearest source point. Ignored when a prefill is set.",
    # ---- tidal forcing ---------------------------------------------------------
    (
        "tidal",
        "name",
    ): "Tidal forcing dataset name. Currently 'TPXO' (TPXO10.v2, user-provided). "
    "Files must exist under source_data/TPXO/TPXO10.v2/.",
    (
        "tidal",
        "ntides",
    ): "Number of tidal constituents to include (max 15). The TPXO atlas provides M2, "
    "S2, N2, K2, K1, O1, P1, Q1, Mf, Mm, M4, MS4, MN4, Mtm, Mf. Default: 10.",
    # ---- river forcing ---------------------------------------------------------
    (
        "river",
        "name",
    ): "River dataset source name. 'DAI' uses the Dai-Trenberth global river discharge "
    "climatology (streamable, no download required). 'GLOFAS' uses the GloFAS v4.0 "
    "daily discharge dataset (higher resolution, more rivers); the preprocessed file "
    "must be manually placed at source_data/GLOFAS/ (see Copernicus CDS).",
    (
        "river",
        "climatology",
    ): "Treat river data as an annual climatology repeated each year.",
    (
        "river",
        "include_bgc",
    ): "Include BGC tracer concentrations (e.g. nutrients) in the river forcing. "
    "Requires a BGC river dataset (bgc_source configured in model.yml).",
    (
        "river",
        "convert_to_climatology",
    ): "When to compute a river climatology from the raw data: "
    "'if_any_missing' (default) — compute if any requested months are absent, "
    "'never' — always use raw data, 'always' — always compute a climatology.",
    (
        "river",
        "coast_snap_buffer_km",
    ): "Override the coastal snap buffer (km) used to move river mouths onto the coast. "
    "Blank uses the dataset default (200 km for Dai, 50 km for GloFAS).",
    (
        "river",
        "domain_edge_buffer",
    ): "Number of grid cells beyond the domain edge kept in the bounding-box pre-filter "
    "when selecting rivers. Default: 20.",
    # ---- output settings -------------------------------------------------------
    (
        "output",
        "output_dd",
    ): "Select an OutputSpec (named output-settings configuration) to seed the output "
    "sections. Fine-tune any value under Advanced settings → the relevant section.",
    # ---- timestep --------------------------------------------------------------
    (
        "timestep",
        "dt_btn",
    ): "Build the grid from the current grid kwargs and compute the barotropic "
    "time step from the CFL criterion. Requires roms_tools to be installed.",
}


def _namelist_tooltip(group_name: str, field_name: str) -> str:
    """Look up the tooltip for a namelist field from the RomsNamelist schema.

    Returns the field's ``description`` string if present, else an empty string.
    Called when building the Advanced settings accordion so every namelist field
    gets an automated tooltip from the ROMS namelist model docstrings.
    """
    try:
        from cstar.roms.namelist import RomsNamelist

        gf = RomsNamelist.model_fields.get(group_name)
        if gf is None:
            return ""
        cls = gf.annotation
        if not hasattr(cls, "model_fields"):
            return ""
        ff = cls.model_fields.get(field_name)
        return ff.description or "" if ff is not None else ""
    except Exception:
        return ""


def _tip(context: str, field: str) -> str:
    """Return the help text for a (context, field) pair, falling back to ''."""
    return HELP_TEXT.get((context, field), HELP_TEXT.get(field, ""))


def _unwrap_type(ann):
    """Reduce ``Optional[X]`` / ``Annotated[X, ...]`` / ``List[X]`` to a base type
    (``bool``/``int``/``float``/``str``/``list``), best-effort.
    """
    if getattr(ann, "__metadata__", None) is not None:  # Annotated[...]
        ann = get_args(ann)[0]
    origin = get_origin(ann)
    if origin is typing.Union:
        non_none = [a for a in get_args(ann) if a is not type(None)]
        if non_none:
            return _unwrap_type(non_none[0])
    if origin in (list, list):
        return list
    return ann


def _base_type(ann, value):
    """The widget type to use for a field: from the annotation if known, else
    inferred from the current value.
    """
    if ann is not None:
        base = _unwrap_type(ann)
        if base in (bool, int, float, str, list):
            return base
    if isinstance(value, bool):
        return bool
    if isinstance(value, int):
        return int
    if isinstance(value, float):
        return float
    if isinstance(value, (list, tuple)):
        return list
    return str


def _make_field_widget(W, name: str, base: type, value: Any, tooltip: str = ""):
    style = {"description_width": "170px"}
    wide = W.Layout(width="430px")
    num = W.Layout(width="300px")
    kw = {"tooltip": tooltip} if tooltip else {}
    if base is bool:
        return W.Checkbox(
            value=bool(value) if value is not None else False,
            description=name,
            indent=False,
            **kw,
        )
    if base is int:
        return W.IntText(
            value=int(value) if value is not None else 0,
            description=name,
            style=style,
            layout=num,
            **kw,
        )
    if base is float:
        return W.FloatText(
            value=float(value) if value is not None else 0.0,
            description=name,
            style=style,
            layout=num,
            **kw,
        )
    if base is list:
        joined = ", ".join(str(x) for x in (value or []))
        return W.Text(
            value=joined,
            description=name,
            style=style,
            layout=wide,
            placeholder="comma-separated",
            **kw,
        )
    return W.Text(
        value="" if value is None else str(value),
        description=name,
        style=style,
        layout=wide,
        **kw,
    )


def _read_field_widget(widget, base: type, original: Any = None) -> Any:
    v = widget.value
    if base is bool:
        return bool(v)
    if base is int:
        return int(v)
    if base is float:
        return float(v)
    if base is list:
        parts = [p.strip() for p in str(v).split(",") if p.strip()]
        out = []
        for p in parts:
            try:
                out.append(int(p))
            except ValueError:
                try:
                    out.append(float(p))
                except ValueError:
                    out.append(p)
        return out
    return v


def _section_submodel(section: str):
    """The RunTimeSettings sub-model for a section, or None (scalar / unknown)."""
    field = RunTimeSettings.model_fields.get(section)
    if field is None:
        return None
    ann = field.annotation
    return ann if isinstance(ann, type) and issubclass(ann, BaseModel) else None


# --- overrides layer: effective = composed(pieces) ⊕ overrides -----------------
# overrides are keyed (section, field) with field=None for scalar sections.
def _apply_overrides(
    composed: dict[str, Any], overrides: dict[Any, Any]
) -> dict[str, Any]:
    import copy as _copy

    eff = _copy.deepcopy(composed)
    for (section, field), value in overrides.items():
        if field is None:
            eff[section] = value
        else:
            eff.setdefault(section, {})[field] = value
    return eff


def _overrides_nested(overrides: dict[Any, Any]) -> dict[str, Any]:
    """Convert the sparse (section, field)->value map to nested {section:{field:value}}
    (or {section: scalar}) for storage in composition.overrides.
    """
    out: dict[str, Any] = {}
    for (section, field), value in overrides.items():
        if field is None:
            out[section] = value
        else:
            out.setdefault(section, {})[field] = value
    return out


# ---------------------------------------------------------------------------
# "Save modified pieces to catalog" -- per-piece extractors + round-trip verify
# ---------------------------------------------------------------------------
# Whole sections that are Domain/Forcing-owned or purely resolver-derived (never
# a ModelSpec default) -- the exact inverse of the mutations build_forge_blueprint
# applies on top of a ModelSpec's model_settings (forge_blueprint_resolve.py).
_RESOLVER_DERIVED_SECTIONS = (
    "time_stepping",
    "v_sponge",
    "river_frc",
    "cdr_frc",
    "extract_data",
)
# Leaves within a kept section that are still resolver-derived (grid/partitioning/
# forcing/bgc-mode driven), so a saved ModelSpec must not bake in a stale value.
_PARAM_DERIVED_LEAVES = frozenset(
    {"llm", "mmm", "n", "np_xi", "np_eta", "nsub_x", "nsub_e"}
)
_CPPDEFS_DERIVED_LEAVES = frozenset(
    {
        "obc_west",
        "obc_east",
        "obc_north",
        "obc_south",
        "cdr_forcing",
        "use_pio",
        "marbl",
        "co2_tvarying",
        "sal_restore",
        "tides",
    }
)
_TIDES_DERIVED_LEAVES = frozenset({"ntides"})


def _model_owned_settings(effective: dict[str, Any]) -> dict[str, Any]:
    """The model-owned subset of a resolved ``model_settings`` dict.

    The exact inverse of ``build_forge_blueprint``'s mutations (forge_blueprint_
    resolve.py): drops whole Domain/Forcing-owned or resolver-derived sections,
    every OUTPUT section/partial-output field (an OutputSpec's job), and the
    resolver-derived leaves within otherwise-kept sections (``param``'s grid/
    partitioning dims, ``cppdefs``'s domain/forcing/bgc-driven flags, ``tides``'s
    ``ntides``). What remains is safe to bake into a new ModelSpec's
    ``model_settings`` and reusable across domains/forcing/output selections.
    """
    out = copy.deepcopy(effective)
    for sec in _RESOLVER_DERIVED_SECTIONS:
        out.pop(sec, None)
    for sec in OUTPUT_SECTIONS:
        out.pop(sec, None)
    if "param" in out:
        for leaf in _PARAM_DERIVED_LEAVES:
            out["param"].pop(leaf, None)
    if "cppdefs" in out:
        for leaf in _CPPDEFS_DERIVED_LEAVES:
            out["cppdefs"].pop(leaf, None)
    if "tides" in out:
        for leaf in _TIDES_DERIVED_LEAVES:
            out["tides"].pop(leaf, None)
    for sec, fields in PARTIAL_OUTPUT_SECTIONS.items():
        if sec in out:
            for f in fields:
                out[sec].pop(f, None)
    return out


def _split_forcing_data(
    d: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Split a raw ForcingSpec dict (as read from ``Forcing.yml``) into the
    ``{initial_conditions, forcing}`` shape the forcing editor expects, plus the
    optional embedded ``cdr_forcing`` block (``None`` if absent). Strips
    ``description`` (not part of either). Shared by the round-trip verifier and
    ``_on_forcing_spec``/``_populate_from`` so CDR routes consistently everywhere
    a ForcingSpec is read.
    """
    d = dict(d)
    d.pop("description", None)
    cdr = d.pop("cdr_forcing", None)
    return d, cdr


_SPEC_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _valid_spec_name(name: str) -> bool:
    """A safe catalog entry name: non-empty, single path component."""
    return bool(name) and _SPEC_NAME_RE.fullmatch(name) is not None


def _is_output_key(section: str, field: Any) -> bool:
    """True if an (section, field) override key belongs to the Output piece
    (vs. the Model piece) -- shared by `_on_output_spec` (clearing stale overrides
    on a fresh OutputSpec pick) and `_rebuild` (deriving `composition.output.modified`
    / `composition.model.modified` from the same override map).
    """
    return section in OUTPUT_SECTIONS or (
        section in PARTIAL_OUTPUT_SECTIONS and field in PARTIAL_OUTPUT_SECTIONS[section]
    )


def _diff_overrides(
    effective: dict[str, Any], composed: dict[str, Any]
) -> dict[Any, Any]:
    """Every field in ``effective`` that differs from ``composed`` becomes an override
    (used on load to reconstruct the layer from a saved/edited config).

    Skips fields in ``_ACCORDION_EXCLUDED_FIELDS`` -- those have a dedicated widget
    (or are resolver-derived with none), never an accordion override, so a stale or
    hand-edited saved file must not silently reintroduce one with no widget to
    display or clear it.
    """
    ov: dict[Any, Any] = {}
    for section, val in effective.items():
        cval = composed.get(section)
        if isinstance(val, dict):
            excluded = _ACCORDION_EXCLUDED_FIELDS.get(section, frozenset())
            for field, v in val.items():
                if field in excluded:
                    continue
                if not isinstance(cval, dict) or cval.get(field) != v:
                    ov[(section, field)] = v
        elif cval != val:
            ov[(section, None)] = val
    return ov


# Fields shown by dedicated wizard widgets elsewhere -> hidden from the generic
# Advanced-settings accordion to avoid duplicate/competing editors (e.g. the PIO
# checkbox and open-boundary checkboxes already edit these; letting the accordion
# edit them too would silently record a competing override, see _diff_overrides).
_ACCORDION_EXCLUDED_FIELDS: dict[str, frozenset[str]] = {
    "cppdefs": frozenset(
        {"use_pio", "obc_west", "obc_east", "obc_north", "obc_south", "marbl"}
    ),
    "param": frozenset({"llm", "mmm", "n", "np_xi", "np_eta"}),
    "time_stepping": frozenset({"dt"}),
}


# Modeler-facing consolidation of the raw namelist sections into a few
# Advanced-settings panes, grouped the way an ocean modeler categorizes knobs
# rather than by the exact namelist sub-groups. DISPLAY-ONLY: each widget stays
# keyed by its real ``(section, field)``, so the overrides layer, ``_diff_overrides``,
# and the output/model "modified" tracking are all unaffected by the grouping.
#
# Sections deliberately absent (time_stepping, reference_date_settings, grid,
# s_coord, param, title, output_root_name, initial, forcing) are filled
# dynamically at resolve/run time (ntimes from the run duration, grid/IC/forcing
# paths from generated files) or edited by a dedicated widget elsewhere in the
# wizard (theta_s/theta_b/hc, dt, np_xi/np_eta, reference date, PIO/open-boundary
# checkboxes). Their resolver-composed value still flows through untouched --
# omitting the pane only removes an editor that would be clobbered or duplicated,
# never the value.
#
# ``cppdefs`` is almost entirely resolver-derived (obc_*/marbl/use_pio/cdr_forcing/
# co2_tvarying/sal_restore/tides) and stays out of the accordion for those fields --
# only the handful with no other UI (``sponge_tune``, ``nhy_forcing``/``nox_forcing``)
# are opted in, via ``_CPPDEFS_PANE_FIELDS``, so a user override can never collide
# with a resolver-owned flag.
#
# ``bgc``/``marbl_bgc`` are SPLIT at field granularity along the existing
# ``PARTIAL_OUTPUT_SECTIONS`` seam (the same split the OutputSpec dropdown seeds):
# their output write-controls live under Output, the rest under Biogeochemistry.
_OUTPUT_CATEGORY = "Output & diagnostics"
_ADVANCED_CATEGORIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Physics & subgrid tuning",
        (
            "lateral_visc",
            "vertical_mixing",
            "tracer_diff2",
            "bottom_drag",
            "v_sponge",
            "sponge_tune",
            "gamma2",
            "ubind",
            "lin_rho_eos",
            "cppdefs",
        ),
    ),
    (
        "Surface & lateral forcing",
        (
            "blk_frc",
            "flux_frc",
            "tides",
            "river_frc",
            "pipe_frc",
            "sss_correction",
            "sst_correction",
        ),
    ),
    (
        "Biogeochemistry (BGC / MARBL)",
        ("bgc", "marbl_bgc", "dic_alk_correction", "cppdefs"),
    ),
    (
        "Carbon dioxide removal (CDR)",
        ("cdr_frc", "cdr_output"),
    ),
    (
        _OUTPUT_CATEGORY,
        (
            "ocean_vars",
            "surf_flux",
            "diagnostics",
            "stdout_diag",
            "ts_output",
            "frc_output",
            "upscale_output",
            "zslice",
            "random_output",
            "calc_pflx",
            "particles",
            "extract_data",
            "bgc",
            "marbl_bgc",
        ),
    ),
)


# cppdefs fields exposed per accordion pane -- everything else in cppdefs (obc_*/
# marbl/use_pio/cdr_forcing/co2_tvarying/sal_restore/tides) is resolver-derived and
# has no widget anywhere; leaving it out of both sets keeps it that way even if a
# future pane adds "cppdefs" without remembering to scope it down.
_CPPDEFS_PANE_FIELDS: dict[str, frozenset[str]] = {
    "Physics & subgrid tuning": frozenset({"sponge_tune"}),
    "Biogeochemistry (BGC / MARBL)": frozenset({"nhy_forcing", "nox_forcing"}),
}


def _split_fields(
    category_title: str, section: str
) -> tuple[frozenset | None, frozenset]:
    """Field filter ``(include, exclude)`` for a section shown in ``category_title``.

    ``include=None`` means "all fields"; otherwise only those in ``include`` are
    shown. ``exclude`` is always dropped. Sections split across panes (``bgc`` /
    ``marbl_bgc`` via :data:`PARTIAL_OUTPUT_SECTIONS`) keep their output write-
    controls under Output and the rest under their feature pane. ``cppdefs`` is
    split the same way via :data:`_CPPDEFS_PANE_FIELDS` -- each pane only sees the
    handful of compile-time flags it owns.
    """
    if section == "cppdefs":
        return _CPPDEFS_PANE_FIELDS.get(category_title, frozenset()), frozenset()
    parts = PARTIAL_OUTPUT_SECTIONS.get(section)
    if parts is None:
        return None, frozenset()
    if category_title == _OUTPUT_CATEGORY:
        return frozenset(parts), frozenset()
    return None, frozenset(parts)


class _SettingsEditor:
    """A collapsible (Accordion) editor over the *editable* model_settings sections,
    consolidated into modeler-facing category panes (see :data:`_ADVANCED_CATEGORIES`).

    Each pane groups several namelist sections under a sub-header per section, so the
    grouping reads by ocean-modeling concern (physics, forcing, BGC, CDR, output)
    while every widget is still keyed by its real ``(section, field)``. Auto-generates
    typed widgets per field using the ``RunTimeSettings`` sub-model schema (falling
    back to value-type inference). All panes are collapsed by default. ``sync()``
    pushes values in (used on load); ``read()`` returns a single field. Fields listed
    in ``_ACCORDION_EXCLUDED_FIELDS`` (and sections not named in a category) are
    skipped -- their value still flows through from the resolver-composed settings
    dict (this editor never authors that dict, only a sparse overrides layer), so
    hiding the widget cannot drop or reset the value.
    """

    def __init__(self, W, model_settings: dict[str, Any], on_edit=None):
        self.W = W
        # (section, field|None) -> (widget, base_type)
        self._widgets: dict[Any, Any] = {}
        self._section_fields: dict[str, list[str | None]] = {}
        # category title -> sections shown under it (a section may appear under two
        # panes when split along PARTIAL_OUTPUT_SECTIONS, e.g. bgc/marbl_bgc).
        self._pane_sections: dict[str, list[str]] = {}
        panes, titles = [], []
        for title, members in _ADVANCED_CATEGORIES:
            blocks = []
            for section in members:
                if section not in model_settings:
                    continue
                include, exclude = _split_fields(title, section)
                box, fields = self._build_section(
                    section, model_settings[section], include, exclude
                )
                if not fields:
                    continue
                self._pane_sections.setdefault(title, []).append(section)
                blocks.append(
                    W.HTML(
                        f"<div style='font-weight:600;margin:8px 0 2px;color:#555'>"
                        f"{section}</div>"
                    )
                )
                blocks.append(box)
                self._section_fields[section] = fields
            if not blocks:
                continue
            panes.append(W.VBox(blocks))
            titles.append(title)
        self.accordion = W.Accordion(children=panes, selected_index=None)
        for i, title in enumerate(titles):
            self.accordion.set_title(i, title)
        if on_edit is not None:
            for (section, field), (widget, _base) in self._widgets.items():
                widget.observe(
                    lambda _ch, s=section, f=field: on_edit(s, f), names="value"
                )

    def sync(self, model_settings: dict[str, Any]):
        """Set every widget to the effective values (caller suspends edit tracking)."""
        for (section, field), (widget, base) in self._widgets.items():
            if section not in model_settings:
                continue
            sec = model_settings[section]
            value = (
                sec
                if field is None
                else (sec.get(field) if isinstance(sec, dict) else None)
            )
            try:
                if base is list:
                    widget.value = ", ".join(str(x) for x in (value or []))
                elif value is not None:
                    widget.value = base(value)
            except (ValueError, TypeError):
                pass

    def read(self, section, field):
        widget, base = self._widgets[(section, field)]
        return _read_field_widget(widget, base)

    def _build_section(
        self,
        section: str,
        value: Any,
        include: frozenset | None = None,
        exclude: frozenset = frozenset(),
    ):
        W = self.W
        sub = _section_submodel(section)
        if not isinstance(value, dict):  # scalar section (e.g. gamma2, ubind)
            base = _base_type(None, value)
            tip = _namelist_tooltip(section, section)
            w = _make_field_widget(W, section, base, value, tooltip=tip)
            self._widgets[(section, None)] = (w, base)
            return W.VBox([w]), [None]
        excluded = _ACCORDION_EXCLUDED_FIELDS.get(section, frozenset())
        rows, fields = [], []
        for key, val in value.items():
            if key in excluded or key in exclude:
                continue
            if include is not None and key not in include:
                continue
            # Skip metadata-name keys (*_vname/*_tname, cdb_min, ...): the settings
            # sub-models are extra="ignore", so anything not a typed field is dropped
            # by the namelist transform and has no business in the editor.
            if sub is not None and key not in sub.model_fields:
                continue
            ann = (
                sub.model_fields[key].annotation
                if (sub and key in sub.model_fields)
                else None
            )
            base = _base_type(ann, val)
            tip = _namelist_tooltip(section, key)
            w = _make_field_widget(W, key, base, val, tooltip=tip)
            self._widgets[(section, key)] = (w, base)
            rows.append(w)
            fields.append(key)
        return W.VBox(rows), fields


# Dropdown option lists derived from the enums so the wizard and schema stay in sync
_SURFACE_TYPES = [e.value for e in SurfaceType]
_BOUNDARY_TYPES = [e.value for e in BoundaryType]
_COARSE_MODES = [e.value for e in CoarseGridMode]
_BGC_INTERP_METHODS = [e.value for e in BgcInterpMethod]
# Optional dropdowns include a blank sentinel meaning "leave unset (roms-tools default)"
_PREFILL_OPTS = [""] + [e.value for e in Prefill]
_REGRID_OPTS = [""] + [e.value for e in RegridMethod]
_EXTRAP_OPTS = [""] + [e.value for e in ExtrapMethod]
_FORCING_CATEGORIES = ("surface", "boundary", "tidal", "river")
_GLORYS_LAYOUT_OPTS = ["", "regional", "global"]  # "" = not specified

# Valid source names per (category, type).  Drives name dropdowns in the forcing editor.
_SOURCE_OPTS: dict[Any, list[str]] = {
    ("surface", SurfaceType.PHYSICS.value): [e.value for e in PhysicsSurfaceSource],
    ("surface", SurfaceType.BGC.value): [e.value for e in BgcSurfaceSource],
    ("surface", SurfaceType.RESTORING.value): [e.value for e in RestoringSurfaceSource],
    ("boundary", BoundaryType.PHYSICS.value): [e.value for e in PhysicsBoundarySource],
    ("boundary", BoundaryType.BGC.value): [e.value for e in BgcBoundarySource],
    ("tidal", None): [e.value for e in TidalSource],
    ("river", None): [e.value for e in RiverSource],
}
_IC_SOURCE_OPTS = [e.value for e in InitialConditionsSource]
_IC_BGC_SOURCE_OPTS = [""] + [e.value for e in BgcInitialConditionsSource]


def _source_opts_for(cat: str, type_val: str | None) -> list[str]:
    """Return the valid source names for a given forcing category and type."""
    return _SOURCE_OPTS.get((cat, type_val), _SOURCE_OPTS.get((cat, None), []))


# The `options` passthrough (see forge_blueprint.py `_OPTIONS_HELP`) is a free-form dict of
# raw roms-tools kwargs, so it can't be rendered as typed controls. We surface it as a
# small JSON editor per item — visible and round-tripped — rather than hidden. This is
# the advanced/transitional hatch; promoting a knob to a typed field gives it a proper
# widget above.
_OPTIONS_PLACEHOLDER = (
    'advanced roms-tools kwargs (JSON object), e.g. {"chunks": {"time": 1}}'
)


def _parse_options(text: str) -> dict[str, Any]:
    """Parse the per-item `options` JSON editor into a dict.

    Empty/whitespace → ``{}``. Invalid JSON or a non-object → ``{}`` (the raw text
    stays in the widget, so nothing is lost — it simply isn't emitted into the spec
    until it parses to an object). Values are forwarded verbatim to the rt constructor.
    """
    text = (text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dump_options(options: Any) -> str:
    """Serialize an item's `options` dict back into the editor (empty → '')."""
    if not options:
        return ""
    try:
        return json.dumps(options, indent=2, default=str)
    except (TypeError, ValueError):
        return ""


def _options_editor(W, value: Any, description: str = "options:"):
    """A compact JSON Textarea for an item's `options` passthrough dict."""
    return W.Textarea(
        value=_dump_options(value),
        description=description,
        placeholder=_OPTIONS_PLACEHOLDER,
        style={"description_width": "70px"},
        layout=W.Layout(width="360px", height="48px"),
    )


class _ForcingEditor:
    """Editor for the forcing piece: initial conditions + per-category forcing items,
    with add/remove. ``gather()`` returns an ``inputs``-shaped dict the resolver
    accepts via ``forcing_inputs=``.
    """

    def __init__(self, W, forcing_inputs: dict[str, Any], on_change):
        self.W = W
        self.on_change = on_change
        fi = forcing_inputs or {}
        ic = fi.get("initial_conditions", {}) or {}
        forc = fi.get("forcing", {}) or {}

        # initial conditions
        _ic_name_val = str((ic.get("source") or {}).get("name", _IC_SOURCE_OPTS[0]))
        if _ic_name_val not in _IC_SOURCE_OPTS:
            _ic_name_val = _IC_SOURCE_OPTS[0]
        self.ic_name = W.Dropdown(
            options=_IC_SOURCE_OPTS,
            value=_ic_name_val,
            description="IC source:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "ic_name"),
        )
        _ic_layout_val = str((ic.get("source") or {}).get("glorys_layout") or "")
        if _ic_layout_val not in _GLORYS_LAYOUT_OPTS:
            _ic_layout_val = ""
        self.ic_layout = W.Dropdown(
            options=_GLORYS_LAYOUT_OPTS,
            value=_ic_layout_val,
            description="glorys_layout:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "ic_layout"),
        )
        self.ic_path = W.Text(
            value=str((ic.get("source") or {}).get("path") or ""),
            description="IC path:",
            placeholder="(default)",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            tooltip=_tip("ic", "path"),
        )
        bgc = ic.get("bgc_source") or {}
        _ic_bgc_val = str(bgc.get("name", "") or "")
        if _ic_bgc_val not in _IC_BGC_SOURCE_OPTS:
            _ic_bgc_val = ""
        self.ic_bgc_name = W.Dropdown(
            options=_IC_BGC_SOURCE_OPTS,
            value=_ic_bgc_val,
            description="IC bgc src:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "ic_bgc_name"),
        )
        self.ic_bgc_clim = W.Checkbox(
            value=bool(bgc.get("climatology", False)),
            description="bgc climatology",
            indent=False,
            tooltip=_tip("ic", "ic_bgc_clim"),
        )
        self.ic_bgc_path = W.Text(
            value=str(bgc.get("path") or ""),
            description="bgc path:",
            placeholder="(default)",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            tooltip=_tip("ic", "path"),
        )
        _ic_bgc_interp = str(
            ic.get("bgc_interpolation_method", BgcInterpMethod.DEPTH.value)
        )
        if _ic_bgc_interp not in _BGC_INTERP_METHODS:
            _ic_bgc_interp = BgcInterpMethod.DEPTH.value
        self.ic_bgc_interp = W.Dropdown(
            options=_BGC_INTERP_METHODS,
            value=_ic_bgc_interp,
            description="bgc interp:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "ic_bgc_interp"),
        )
        self.ic_flex_time = W.Checkbox(
            value=bool(ic.get("allow_flex_time", False)),
            description="flex time",
            indent=False,
            tooltip=_tip("ic", "ic_flex_time"),
        )
        self.ic_options = _options_editor(W, ic.get("options"))
        for _w in (
            self.ic_name,
            self.ic_layout,
            self.ic_path,
            self.ic_bgc_name,
            self.ic_bgc_clim,
            self.ic_bgc_path,
            self.ic_bgc_interp,
            self.ic_flex_time,
            self.ic_options,
        ):
            _w.observe(lambda _ch: on_change(), names="value")

        # "glorys_layout" only applies to a GLORYS source (item 7).
        def _sync_ic_layout_visibility(_change=None):
            self.ic_layout.layout.display = (
                "" if self.ic_name.value == "GLORYS" else "none"
            )

        self.ic_name.observe(_sync_ic_layout_visibility, names="value")
        _sync_ic_layout_visibility()

        # per-category item rows: list of dicts of widgets
        self._rows: dict[str, list] = {c: [] for c in _FORCING_CATEGORIES}
        self._containers: dict[str, Any] = {}
        for cat in _FORCING_CATEGORIES:
            container = W.VBox([])
            self._containers[cat] = container
            for item in forc.get(cat, []) or []:
                self._rows[cat].append(self._make_row(cat, item))
            self._render(cat)

    # ---- one item row --------------------------------------------------------
    @staticmethod
    def _apply_row_visibility(w: dict[str, Any]) -> None:
        """Show/hide widgets whose relevance depends on the row's `type`/source name.

        No field is ever removed from the gathered dict by this — it only toggles
        display so the form doesn't show options that don't apply to the current
        selection (e.g. "restore:" only for type=restoring, "layout:" only for a
        GLORYS source, corr_rad/wind_dropoff only for type=physics surface rows).
        """

        def show(widget, on):
            widget.layout.display = "" if on else "none"

        t = w["type"].value if "type" in w else None
        name = w["name"].value if "name" in w else None
        if "restoring_forces" in w:
            show(w["restoring_forces"], t == SurfaceType.RESTORING.value)
        if "correct_radiation" in w:
            show(w["correct_radiation"], t == SurfaceType.PHYSICS.value)
        if "wind_dropoff" in w:
            show(w["wind_dropoff"], t == SurfaceType.PHYSICS.value)
        if "glorys_layout" in w:
            show(w["glorys_layout"], name == "GLORYS")

    def _make_row(self, cat: str, item: dict[str, Any]):
        W = self.W
        src = item.get("source") or {}
        w: dict[str, Any] = {}
        small = {"description_width": "70px"}

        # Source name: Dropdown driven by category + type (for surface/boundary) or fixed.
        _cur_type = item.get("type", "physics")
        _name_opts = _source_opts_for(cat, _cur_type)
        _name_val = str(src.get("name", ""))
        if _name_val not in _name_opts and _name_opts:
            _name_val = _name_opts[0]
        w["name"] = W.Dropdown(
            options=_name_opts or [""],
            value=_name_val
            if _name_val in (_name_opts or [""])
            else (_name_opts or [""])[0],
            description="src:",
            style=small,
            layout=W.Layout(width="160px"),
            tooltip=_tip(cat, "name"),
        )

        # Optional custom dataset path; blank -> backend derives the default path.
        w["path"] = W.Text(
            value=str(src.get("path") or ""),
            description="path:",
            placeholder="(default)",
            style=small,
            layout=W.Layout(width="260px"),
            tooltip=_tip(cat, "path"),
        )

        if cat in ("surface", "boundary"):
            _type_opts = _SURFACE_TYPES if cat == "surface" else _BOUNDARY_TYPES
            _type_val = _cur_type if _cur_type in _type_opts else _type_opts[0]
            w["type"] = W.Dropdown(
                options=_type_opts,
                value=_type_val,
                description="type:",
                style=small,
                layout=W.Layout(width="160px"),
                tooltip=_tip(cat, "type"),
            )

            # When type changes → update the source name dropdown to the valid options.
            def _on_type_change(change, name_dd=w["name"], c=cat, ws=w):
                new_opts = _source_opts_for(c, change["new"])
                name_dd.options = new_opts or [""]
                if name_dd.value not in name_dd.options:
                    name_dd.value = name_dd.options[0]
                self._apply_row_visibility(ws)
                self.on_change()

            w["type"].observe(_on_type_change, names="value")

            w["climatology"] = W.Checkbox(
                value=bool(src.get("climatology", False)),
                description="clim",
                indent=False,
                tooltip=_tip(cat, "climatology"),
            )
            _layout_val = str(src.get("glorys_layout") or "")
            if _layout_val not in _GLORYS_LAYOUT_OPTS:
                _layout_val = ""
            w["glorys_layout"] = W.Dropdown(
                options=_GLORYS_LAYOUT_OPTS,
                value=_layout_val,
                description="layout:",
                style=small,
                layout=W.Layout(width="150px"),
                tooltip=_tip(cat, "glorys_layout"),
            )

            # When the source name changes → the "layout:" box only applies to GLORYS.
            def _on_name_change(_change, ws=w):
                self._apply_row_visibility(ws)

            w["name"].observe(_on_name_change, names="value")
        if cat == "surface":
            w["correct_radiation"] = W.Checkbox(
                value=bool(item.get("correct_radiation", False)),
                description="corr_rad",
                indent=False,
                tooltip=_tip("surface", "correct_radiation"),
            )
            w["wind_dropoff"] = W.Checkbox(
                value=bool(item.get("wind_dropoff", False)),
                description="wind_dropoff",
                indent=False,
                tooltip=_tip("surface", "wind_dropoff"),
            )
            w["coarse_grid_mode"] = W.Dropdown(
                options=_COARSE_MODES,
                value=item.get("coarse_grid_mode", "auto"),
                description="coarse:",
                style=small,
                layout=W.Layout(width="150px"),
                tooltip=_tip("surface", "coarse_grid_mode"),
            )
            w["restoring_forces"] = W.Text(
                value=", ".join(item.get("restoring_forces") or []),
                description="restore:",
                style=small,
                layout=W.Layout(width="150px"),
                placeholder="sss,sst",
                tooltip=_tip("surface", "restoring_forces"),
            )
        if cat == "boundary":
            _b_interp = str(
                item.get("bgc_interpolation_method", BgcInterpMethod.DEPTH.value)
            )
            if _b_interp not in _BGC_INTERP_METHODS:
                _b_interp = BgcInterpMethod.DEPTH.value
            w["bgc_interpolation_method"] = W.Dropdown(
                options=_BGC_INTERP_METHODS,
                value=_b_interp,
                description="bgc interp:",
                style=small,
                layout=W.Layout(width="180px"),
                tooltip=_tip("boundary", "bgc_interpolation_method"),
            )
            _prefill_val = str(item.get("prefill") or "")
            if _prefill_val not in _PREFILL_OPTS:
                _prefill_val = ""
            w["prefill"] = W.Dropdown(
                options=_PREFILL_OPTS,
                value=_prefill_val,
                description="prefill:",
                style=small,
                layout=W.Layout(width="200px"),
                tooltip=_tip("boundary", "prefill"),
            )
            _regrid_val = str(item.get("regrid_method") or "")
            if _regrid_val not in _REGRID_OPTS:
                _regrid_val = ""
            w["regrid_method"] = W.Dropdown(
                options=_REGRID_OPTS,
                value=_regrid_val,
                description="regrid:",
                style=small,
                layout=W.Layout(width="150px"),
                tooltip=_tip("boundary", "regrid_method"),
            )
            _extrap_val = str(item.get("extrap_method") or "")
            if _extrap_val not in _EXTRAP_OPTS:
                _extrap_val = ""
            w["extrap_method"] = W.Dropdown(
                options=_EXTRAP_OPTS,
                value=_extrap_val,
                description="extrap:",
                style=small,
                layout=W.Layout(width="170px"),
                tooltip=_tip("boundary", "extrap_method"),
            )
        if cat == "tidal":
            w["ntides"] = W.IntText(
                value=int(item.get("ntides") or 0),
                description="ntides:",
                style=small,
                layout=W.Layout(width="130px"),
                tooltip=_tip("tidal", "ntides"),
            )
        if cat == "river":
            w["climatology"] = W.Checkbox(
                value=bool(src.get("climatology", False)),
                description="clim",
                indent=False,
                tooltip=_tip("river", "climatology"),
            )
            w["include_bgc"] = W.Checkbox(
                value=bool(item.get("include_bgc", False)),
                description="bgc",
                indent=False,
                tooltip=_tip("river", "include_bgc"),
            )
            _ctc_opts = [e.value for e in ClimatologyMode]
            _ctc_val = item.get(
                "convert_to_climatology", ClimatologyMode.IF_ANY_MISSING.value
            )
            w["convert_to_climatology"] = W.Dropdown(
                options=_ctc_opts,
                value=_ctc_val
                if _ctc_val in _ctc_opts
                else ClimatologyMode.IF_ANY_MISSING.value,
                description="clim mode:",
                style=small,
                layout=W.Layout(width="180px"),
                tooltip=_tip("river", "convert_to_climatology"),
            )
            w["coast_snap_buffer_km"] = W.FloatText(
                value=float(item.get("coast_snap_buffer_km") or 0.0),
                description="coast snap km:",
                style=small,
                layout=W.Layout(width="180px"),
                tooltip=_tip("river", "coast_snap_buffer_km"),
            )
            w["domain_edge_buffer"] = W.IntText(
                value=int(item.get("domain_edge_buffer", 20)),
                description="edge buffer:",
                style=small,
                layout=W.Layout(width="160px"),
                tooltip=_tip("river", "domain_edge_buffer"),
            )
        # Advanced passthrough: raw roms-tools kwargs not (yet) typed above.
        w["options"] = _options_editor(W, item.get("options"))
        remove = W.Button(
            description="✕", layout=W.Layout(width="36px"), tooltip="Remove this item"
        )
        remove.on_click(lambda _b, c=cat, ws=w: self._remove(c, ws))
        for widget in w.values():
            widget.observe(lambda _ch: self.on_change(), names="value")
        w["_remove_btn"] = remove
        self._apply_row_visibility(w)
        return w

    def _row_box(self, w):
        # `type` (when present) drives the other options in the row, so show it first.
        keys = [k for k in w if k != "_remove_btn"]
        if "type" in keys:
            keys = ["type", *[k for k in keys if k != "type"]]
        return self.W.HBox([*(w[k] for k in keys), w["_remove_btn"]])

    def _render(self, cat: str):
        W = self.W
        add = W.Button(
            description=f"+ add {cat}", icon="plus", layout=W.Layout(width="130px")
        )
        add.on_click(lambda _b, c=cat: self._add(c))
        self._containers[cat].children = [self._row_box(w) for w in self._rows[cat]] + [
            add
        ]

    def _add(self, cat: str):
        self._rows[cat].append(self._make_row(cat, {"source": {"name": ""}}))
        self._render(cat)
        self.on_change()

    def _remove(self, cat: str, ws):
        self._rows[cat] = [w for w in self._rows[cat] if w is not ws]
        self._render(cat)
        self.on_change()

    # ---- gather --------------------------------------------------------------
    def _gather_item(self, cat: str, w) -> dict[str, Any]:
        src: dict[str, Any] = {"name": w["name"].value}
        if "climatology" in w and w["climatology"].value:
            src["climatology"] = True
        if "glorys_layout" in w and w["glorys_layout"].value:  # Dropdown: "" = omit
            src["glorys_layout"] = w["glorys_layout"].value
        if "path" in w and w["path"].value.strip():  # blank = derive default path
            src["path"] = w["path"].value.strip()
        item: dict[str, Any] = {"source": src}
        if "type" in w:
            item["type"] = w["type"].value
        if "correct_radiation" in w and w["correct_radiation"].value:
            item["correct_radiation"] = True
        if "wind_dropoff" in w and w["wind_dropoff"].value:
            item["wind_dropoff"] = True
        if "coarse_grid_mode" in w:
            item["coarse_grid_mode"] = w["coarse_grid_mode"].value
        if "restoring_forces" in w and w["restoring_forces"].value.strip():
            item["restoring_forces"] = [
                p.strip() for p in w["restoring_forces"].value.split(",") if p.strip()
            ]
        # Boundary regrid/interp knobs: only emit non-default values to keep specs clean.
        if (
            "bgc_interpolation_method" in w
            and w["bgc_interpolation_method"].value != BgcInterpMethod.DEPTH.value
        ):
            item["bgc_interpolation_method"] = w["bgc_interpolation_method"].value
        if "prefill" in w and w["prefill"].value:  # Dropdown: "" = leave unset
            item["prefill"] = w["prefill"].value
        if "regrid_method" in w and w["regrid_method"].value:
            item["regrid_method"] = w["regrid_method"].value
        if "extrap_method" in w and w["extrap_method"].value:
            item["extrap_method"] = w["extrap_method"].value
        if "ntides" in w:
            item["ntides"] = int(w["ntides"].value)
        if "include_bgc" in w and w["include_bgc"].value:
            item["include_bgc"] = True
        if "convert_to_climatology" in w:
            item["convert_to_climatology"] = w["convert_to_climatology"].value
        if (
            "coast_snap_buffer_km" in w and w["coast_snap_buffer_km"].value
        ):  # 0.0 = leave unset
            item["coast_snap_buffer_km"] = float(w["coast_snap_buffer_km"].value)
        if "domain_edge_buffer" in w and int(w["domain_edge_buffer"].value) != 20:
            item["domain_edge_buffer"] = int(w["domain_edge_buffer"].value)
        if "options" in w:  # advanced passthrough; omit when empty/unparseable
            opts = _parse_options(w["options"].value)
            if opts:
                item["options"] = opts
        return item

    def gather(self) -> dict[str, Any]:
        ic_source = {"name": self.ic_name.value}
        if self.ic_layout.value:  # Dropdown: "" means not specified
            ic_source["glorys_layout"] = self.ic_layout.value
        if self.ic_path.value.strip():  # blank = derive default path
            ic_source["path"] = self.ic_path.value.strip()
        ic: dict[str, Any] = {"source": ic_source}
        if self.ic_bgc_name.value:  # Dropdown: "" means no bgc source
            ic["bgc_source"] = {
                "name": self.ic_bgc_name.value,
                "climatology": bool(self.ic_bgc_clim.value),
            }
            if self.ic_bgc_path.value.strip():  # blank = derive default path
                ic["bgc_source"]["path"] = self.ic_bgc_path.value.strip()
        if (
            self.ic_bgc_interp.value
            and self.ic_bgc_interp.value != BgcInterpMethod.DEPTH.value
        ):
            ic["bgc_interpolation_method"] = self.ic_bgc_interp.value
        if self.ic_flex_time.value:
            ic["allow_flex_time"] = True
        ic_opts = _parse_options(self.ic_options.value)
        if ic_opts:
            ic["options"] = ic_opts
        forcing = {
            cat: [self._gather_item(cat, w) for w in self._rows[cat]]
            for cat in _FORCING_CATEGORIES
        }
        return {
            "initial_conditions": ic,
            "forcing": forcing,
        }

    @property
    def widget(self):
        W = self.W
        ic_box = W.VBox(
            [
                W.HTML("<i>initial conditions</i>"),
                W.HBox([self.ic_name, self.ic_layout]),
                self.ic_path,
                W.HBox([self.ic_bgc_name, self.ic_bgc_clim]),
                self.ic_bgc_path,
                W.HBox([self.ic_bgc_interp, self.ic_flex_time]),
                self.ic_options,
            ]
        )
        panes = [ic_box] + [self._containers[c] for c in _FORCING_CATEGORIES]
        acc = W.Accordion(children=panes, selected_index=None)
        for i, title in enumerate(["initial_conditions", *_FORCING_CATEGORIES]):
            acc.set_title(i, title)
        return acc


_GRID_INT = ("nx", "ny", "N")
_GRID_FLOAT = ("size_x", "size_y", "center_lon", "center_lat", "rot")
_SCOORD = ("theta_s", "theta_b", "hc")
_DEFAULT_GRID = dict(
    nx=6,
    ny=2,
    size_x=500.0,
    size_y=1000.0,
    center_lon=0.0,
    center_lat=55.0,
    rot=10.0,
    N=3,
    theta_s=5.0,
    theta_b=2.0,
    hc=250.0,
)


def _get_catalog():
    """Return the bundled DomainCatalog (read-only discovery of pieces)."""
    from cstar_forge.domain_catalog import default_catalog

    return default_catalog


def _schedule_coroutine(coro):
    """Schedule a coroutine on the running loop (returns a Task), or run it to
    completion directly if there is no running loop. Mirrors
    ``cstar_forge.forge.executor._schedule_coroutine`` -- needed for seamless
    execution of async code (like streaming a subprocess) from a synchronous
    ipywidgets ``on_click`` handler, both inside and outside Jupyter.
    """
    import asyncio

    try:
        return asyncio.get_running_loop().create_task(coro)
    except RuntimeError:
        return asyncio.run(coro)


class ForgeBlueprintWizard:
    """Build/curate a :class:`ForgeBlueprint` interactively. ``self.config`` holds the
    latest successfully-resolved config (``None`` while inputs are invalid).
    """

    def __init__(self, catalog: Any = None):
        import ipywidgets as W  # imported here so the package doesn't require ipywidgets

        self.W = W
        self.catalog = catalog or _get_catalog()
        self.config: ForgeBlueprint | None = None

        models = list(self.catalog.model_names)
        domains = list(self.catalog.domain_names)

        # --- load / import an existing forge_blueprint.yml ---
        self.load_path = W.Text(
            value="",
            placeholder="path to forge_blueprint.yml",
            description="Load file:",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
        )
        self.load_btn = W.Button(description="Load", icon="upload")
        self.upload = W.FileUpload(
            accept=".yml,.yaml", multiple=False, description="…or upload"
        )
        self.load_status = W.HTML("")

        # --- piece selectors ---
        self.model_dd = W.Dropdown(
            options=models,
            description="Model:",
            value=(models[0] if models else None),
            style={"description_width": "110px"},
        )
        self.bgc_dd = W.Dropdown(
            options=["marbl", "none"],
            value="marbl",
            description="BGC:",
            style={"description_width": "110px"},
            tooltip=(
                "Biogeochemistry mode. 'marbl' builds ROMS-MARBL and includes the "
                "MARBL codebase; 'none' builds physics-only (no MARBL, no BGC forcing)."
            ),
        )
        self.domain_dd = W.Dropdown(
            options=["<custom>", *domains],
            description="Domain:",
            value="<custom>",
            style={"description_width": "110px"},
        )
        self.grid_name = W.Text(
            value="my-grid",
            description="Grid name:",
            style={"description_width": "110px"},
            tooltip="Short name for this grid configuration. Used in the domain name.",
        )

        # --- grid kwargs ---
        self.grid_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.grid_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k),
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.grid_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k),
            )
        self.scoord_chk = W.Checkbox(
            value=True,
            description="specify s-coord (theta_s/theta_b/hc)",
            indent=False,
            tooltip="When checked, theta_s, theta_b, and hc are passed to roms-tools "
            "to set the vertical stretching. Required for ROMS simulations.",
        )

        # --- boundaries / partitioning ---
        self.bnd = {
            d: W.Checkbox(
                value=(d in ("east", "north")),
                description=d,
                indent=False,
                tooltip=f"Enable the {d} open boundary for ocean exchange.",
            )
            for d in ("north", "south", "east", "west")
        }
        self.npx = W.IntText(
            value=1,
            description="n_procs_x:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("domain", "npx"),
        )
        self.npy = W.IntText(
            value=1,
            description="n_procs_y:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("domain", "npy"),
        )
        self.use_pio_chk = W.Checkbox(
            value=False,
            description="use ParallelIO (PIO)",
            indent=False,
            tooltip="Build ROMS against the ParallelIO library: inputs are written as "
            "classic-format (CDF-5) netCDF and ROMS reads/writes joined files.",
        )
        self.roms_ref = W.Text(
            value="",  # populated from the selected Model's pinned default below
            description="ucla-roms ref:",
            style={"description_width": "120px"},
            layout=W.Layout(width="260px"),
            placeholder="commit / tag / branch",
            tooltip="ucla-roms checkout target (commit hash, tag, or branch). "
            "Prefilled from the selected Model's pinned default; edit to override.",
        )

        # --- nesting (optional child grid) ---
        self.nest_enable = W.Checkbox(
            value=False,
            description="enable nesting (child grid)",
            indent=False,
            tooltip=_tip("nesting", "nest_enable"),
        )
        self.nest_domain_dd = W.Dropdown(
            options=["<custom>", *domains],
            description="Child from:",
            value="<custom>",
            style={"description_width": "110px"},
            tooltip=_tip("nesting", "nest_domain_dd"),
        )
        self.child_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.child_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (child/inner grid)",
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.child_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (child/inner grid)",
            )
        self.nest_period = W.FloatText(
            value=3600.0,
            description="extract period (s):",
            style={"description_width": "130px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "nest_period"),
        )
        self.nest_pressure_fluxes = W.Checkbox(
            value=False,
            description="include pressure fluxes",
            indent=False,
            tooltip=_tip("nesting", "nest_pressure_fluxes"),
        )
        # --- nesting plot (parent+child boundary overlay, separate from the Grid
        # section's parent-only plot) ---
        self.nest_plot_btn = W.Button(
            description="Refresh plot",
            icon="refresh",
            tooltip="Build the parent and child grids from current settings and "
            "render both via plot_nesting (parent+child boundary overlay).",
        )
        self.nest_plot_status = W.HTML("")
        self.nest_plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px"),
        )

        # --- run window ---
        self.start = W.DatePicker(
            value=date(2012, 1, 1),
            description="Start:",
            style={"description_width": "110px"},
            tooltip=_tip("run", "start"),
        )
        self.end = W.DatePicker(
            value=date(2012, 1, 2),
            description="End:",
            style={"description_width": "110px"},
            tooltip=_tip("run", "end"),
        )
        self.model_ref_date = W.DatePicker(
            value=date(2000, 1, 1),
            description="Model ref date:",
            style={"description_width": "130px"},
            tooltip=_tip("run", "model_ref_date"),
        )
        self.description = W.Text(
            value="Generated blueprint",
            description="Description:",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("run", "description"),
        )
        # --- grid extended options ---
        self.hmin = W.FloatText(
            value=5.0,
            description="hmin (m):",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("grid", "hmin"),
        )
        self.close_narrow_chk = W.Checkbox(
            value=False,
            description="close narrow channels",
            indent=False,
            tooltip=_tip("grid", "close_narrow_channels"),
        )
        self.mask_shapefile = W.Text(
            value="",
            description="mask shapefile:",
            style={"description_width": "120px"},
            layout=W.Layout(width="380px"),
            placeholder="path to custom land-mask shapefile (optional)",
            tooltip=_tip("grid", "mask_shapefile"),
        )
        self.topo_source = W.Dropdown(
            options=["ETOPO5", "SRTM15"],
            value="ETOPO5",
            description="topo source:",
            style={"description_width": "120px"},
            tooltip=_tip("grid", "topography_source"),
        )
        self.topo_path = W.Text(
            value="",
            description="topo path:",
            style={"description_width": "120px"},
            layout=W.Layout(width="380px"),
            placeholder="(default)",
            tooltip=_tip("grid", "topography_path"),
        )

        # --- timestep ---
        self.dt = W.FloatText(
            value=7200.0,
            description="dt (s):",
            style={"description_width": "90px"},
            layout=W.Layout(width="220px"),
            tooltip=_tip("run", "dt"),
        )
        self.dt_btn = W.Button(
            description="Compute dt (CFL)",
            icon="calculator",
            tooltip=_tip("timestep", "dt_btn"),
        )
        self.dt_status = W.HTML("")

        # --- grid plot ---
        self.plot_btn = W.Button(
            description="Refresh plot",
            icon="refresh",
            tooltip="Build the grid from current settings and render it. Updates "
            "automatically when a domain is selected from the catalog.",
        )
        self.plot_status = W.HTML("")
        self.plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px"),
        )

        # --- output / preview ---
        # --- forcing piece (ForcingSpec selection + add/remove/edit editor) ---
        # A ForcingSpec must always be explicitly selected -- ModelSpec no longer
        # embeds a default forcing.
        _forcing_names = list(self.catalog.forcing_names)
        self.forcing_dd = W.Dropdown(
            options=_forcing_names,
            value=(_forcing_names[0] if _forcing_names else None),
            description="Forcing:",
            style={"description_width": "110px"},
            tooltip="Select a named ForcingSpec from the catalog to seed all forcing "
            "fields. Edit individual items in the Forcing section below.",
        )
        self.forcing_box = W.VBox([])
        self._forcing_editor: _ForcingEditor | None = None
        # snapshot of the forcing editor's gather() at the last catalog pick;
        # compared in _rebuild() to detect a deviation (composition.forcing.modified)
        self._forcing_seed: dict[str, Any] | None = None

        # --- CDR (Carbon Dioxide Removal) forcing: uploaded roms-tools YAML ---
        # Parsed dict lives on the instance (not a widget) since FileUpload can't be
        # repopulated with the original file on load; _gather()/_populate_from read
        # and write this directly.
        self._cdr_forcing: dict[str, Any] | None = None
        self.cdr_upload = W.FileUpload(
            accept=".yml,.yaml",
            multiple=False,
            description="Upload CDR YAML",
            tooltip=(
                "A roms-tools CDRForcing.to_yaml(...) dump. Validated immediately on "
                "upload; toggles the CDR compile/run-time settings on when accepted."
            ),
        )
        self.cdr_clear_btn = W.Button(description="Clear CDR", icon="times")
        self.cdr_status = W.HTML("")

        # --- output settings piece (OutputSpec selection) ---
        # The output sections themselves are edited in the Advanced settings accordion;
        # this dropdown selects a named OutputSpec that seeds those sections. An
        # OutputSpec must always be explicitly selected -- ModelSpec no longer embeds
        # default output settings.
        _output_names = list(self.catalog.output_names)
        self.output_dd = W.Dropdown(
            options=_output_names,
            value=(_output_names[0] if _output_names else None),
            description="Output:",
            style={"description_width": "110px"},
            tooltip=_tip("output", "output_dd"),
        )

        # --- advanced settings editor (built lazily on first rebuild) ---
        self.editor: _SettingsEditor | None = None
        self._editor_model: str | None = None
        self.editor_box = W.VBox([])  # placeholder; filled with the editor's accordion
        # sparse manual overrides layer: (section, field|None) -> value
        self._overrides: dict[Any, Any] = {}
        self._syncing = False  # True while pushing composed values into editor widgets
        # snapshot of the domain-defining widgets at the last catalog Domain pick;
        # compared in _rebuild() to detect a deviation (composition.domain.modified).
        # None means no catalog domain has been picked yet (or domain_dd == "<custom>").
        self._domain_seed: dict[str, Any] | None = None

        self.derived = W.HTML("")
        self.validation = W.HTML("")
        self.preview = W.Output(
            layout=W.Layout(
                border="1px solid #ccc",
                padding="6px",
                max_height="380px",
                overflow="auto",
            )
        )
        # Browser download (works in Voilà / JupyterLab without server file access)
        self.download_link = W.HTML("")
        # Save to the server/working-dir filesystem (handy for local or HPC use)
        self.save_path = W.Text(
            value="forge_blueprint.yml",
            description="Save to:",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
        )
        self.save_btn = W.Button(description="Save to disk", icon="save")
        self.save_status = W.HTML("")

        # --- canonical blueprint name (also in the Export section) ---
        # Defaults to the resolver's derived name and keeps tracking it until the user
        # edits the field (self._name_touched flips True on their first real edit;
        # programmatic backfills in _rebuild() happen under _suspend() so they don't
        # themselves flip it). save_path tracks the derived name the same way.
        self._name_touched = False
        self.name = W.Text(
            value="",
            description="Name:",
            placeholder="(derived from model/grid/procs)",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("export", "name"),
        )
        self.name.observe(self._on_name_change, names="value")
        self._save_path_touched = False
        self.save_path.observe(self._on_save_path_change, names="value")

        # --- save modified pieces to catalog (name + button + status per piece) ---
        def _piece_save_row(placeholder):
            name_w = W.Text(
                value="",
                description="Name:",
                placeholder=placeholder,
                style={"description_width": "60px"},
                layout=W.Layout(width="260px"),
            )
            btn = W.Button(description="Save as new spec", icon="save")
            status = W.HTML("")
            return name_w, btn, status

        self.save_output_name, self.save_output_btn, self.save_output_status = (
            _piece_save_row("(new OutputSpec name)")
        )
        self.save_model_name, self.save_model_btn, self.save_model_status = (
            _piece_save_row("(new ModelSpec name)")
        )
        self.save_domain_name, self.save_domain_btn, self.save_domain_status = (
            _piece_save_row("(new DomainSpec name)")
        )
        self.save_forcing_name, self.save_forcing_btn, self.save_forcing_status = (
            _piece_save_row("(new ForcingSpec name)")
        )

        # --- run (invokes the executor CLI on the just-saved blueprint) ---
        from cstar_forge.config import system as _detected_system

        self.run_warning = W.HTML(
            "<b style='color:#b58900'>⚠ Processing a blueprint can use substantial "
            "memory and CPU depending on grid size.</b> Run this from a compute node "
            "(or another host) with resources appropriate for your domain — this is "
            f"not checked automatically. Detected host: <code>{_detected_system}</code>."
            "<br><span style='color:#666'>ℹ This interface is likely to change in the "
            "near future (e.g. once the executor moves into C-Star, this may become a "
            "<code>cstar blueprint run</code> command).</span>"
        )
        self.run_later_note = W.HTML(
            "<span style='color:#666'>ℹ To run this later, or on a different "
            "machine, save the blueprint above and then (from the "
            "<code>cstar-forge</code> environment) call: "
            "<code>python -m cstar_forge.run &lt;path/to/forge_blueprint.yml&gt;</code></span>"
        )
        self.run_btn = W.Button(description="Run", icon="play")
        self.run_status = W.HTML("")
        self.run_output = W.Output(
            layout=W.Layout(
                border="1px solid #ccc",
                padding="6px",
                max_height="380px",
                overflow="auto",
            )
        )

        self.roms_ref.value = self._model_default_roms_ref()
        self.bgc_dd.value = self._model_default_bgc_mode()
        self.use_pio_chk.value = self._model_default_use_pio()
        self._build_forcing_editor(self.catalog.forcing_data(self.forcing_dd.value))
        self._forcing_seed = self._forcing_editor.gather()
        self._wire()
        self._rebuild()

    # ---- wiring --------------------------------------------------------------
    def _wire(self):
        self.domain_dd.observe(self._on_domain, names="value")
        self.forcing_dd.observe(self._on_forcing_spec, names="value")
        self.dt_btn.on_click(self._on_compute_dt)
        self.plot_btn.on_click(self._on_plot)
        self.nest_plot_btn.on_click(self._on_nest_plot)
        self.save_btn.on_click(self._on_save)
        self.save_output_btn.on_click(self._on_save_output)
        self.save_model_btn.on_click(self._on_save_model)
        self.save_domain_btn.on_click(self._on_save_domain)
        self.save_forcing_btn.on_click(self._on_save_forcing)
        self.run_btn.on_click(self._on_run)
        self.load_btn.on_click(self._on_load_path)
        self.upload.observe(self._on_upload, names="value")
        self.cdr_upload.observe(self._on_cdr_upload, names="value")
        self.cdr_clear_btn.on_click(self._on_cdr_clear)
        self.model_dd.observe(self._on_model_change, names="value")
        self.nest_domain_dd.observe(self._on_nest_domain, names="value")
        self.output_dd.observe(self._on_output_spec, names="value")
        watched = [
            self.grid_name,
            self.scoord_chk,
            self.npx,
            self.npy,
            self.use_pio_chk,
            self.bgc_dd,
            self.roms_ref,
            self.start,
            self.end,
            self.model_ref_date,
            self.description,
            self.dt,
            self.hmin,
            self.close_narrow_chk,
            self.mask_shapefile,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.nest_period,
            self.nest_pressure_fluxes,
            *self.grid_w.values(),
            *self.bnd.values(),
            *self.child_w.values(),
        ]
        for w in watched:
            w.observe(self._rebuild, names="value")

    def _on_name_change(self, _change):
        # A programmatic backfill (see _rebuild) happens under _suspend() -- only a
        # real user edit should "lock in" the field against further auto-updates.
        if getattr(self, "_suspended", False):
            return
        self._name_touched = True
        self._rebuild()

    def _on_save_path_change(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._save_path_touched = True

    def _on_model_change(self, _change):
        # a different model has different defaults -> existing overrides no longer apply.
        # Forcing/Output are independent catalog dimensions from the model (a ForcingSpec/
        # OutputSpec doesn't reference a model), so switching models never touches them.
        if getattr(self, "_suspended", False):
            return
        self._overrides = {}
        self.roms_ref.value = self._model_default_roms_ref()
        self.bgc_dd.value = self._model_default_bgc_mode()
        self.use_pio_chk.value = self._model_default_use_pio()
        self._rebuild()

    def _on_editor_edit(self, section, field):
        # a user edit in the advanced editor -> record as an override (vs composed)
        if self._syncing or getattr(self, "_suspended", False):
            return
        self._overrides[(section, field)] = self.editor.read(section, field)
        self._rebuild()

    def _on_nest_domain(self, _change):
        """Prefill the child grid from a selected DomainSpec (and enable nesting)."""
        name = self.nest_domain_dd.value
        if name == "<custom>":
            return
        gk = self.catalog.domain_data(name).get("grid_kwargs", {}) or {}
        with self._suspend():
            self.nest_enable.value = True
            for k, w in self.child_w.items():
                if k in gk:
                    w.value = gk[k]
        self._rebuild()
        self._on_nest_plot(None)

    # ---- forcing piece -------------------------------------------------------
    def _model_default_roms_ref(self) -> str:
        """The selected model's pinned ucla-roms checkout target (commit or branch)."""
        try:
            data = load_model_spec_data(self.catalog.model_dir(self.model_dd.value))
            roms = data["model"].get("code", {}).get("roms", {}) or {}
            return roms.get("commit") or roms.get("branch") or ""
        except Exception:
            return ""

    def _model_default_bgc_mode(self) -> str:
        """The selected model's ModelSpec-declared bgc_mode (prepopulates self.bgc_dd)."""
        try:
            data = load_model_spec_data(self.catalog.model_dir(self.model_dd.value))
            return data["model"].get("bgc_mode", "marbl")
        except Exception:
            return "marbl"

    def _model_default_use_pio(self) -> bool:
        """The selected model's ModelSpec-declared use_pio (prepopulates self.use_pio_chk)."""
        try:
            data = load_model_spec_data(self.catalog.model_dir(self.model_dd.value))
            return bool(data["model"].get("use_pio", False))
        except Exception:
            return False

    def _build_forcing_editor(self, base_inputs: dict[str, Any]):
        self._forcing_editor = _ForcingEditor(
            self.W, base_inputs, on_change=self._on_forcing_change
        )
        self.forcing_box.children = [self._forcing_editor.widget]

    def _on_forcing_spec(self, _change):
        """Selecting a ForcingSpec reseeds the forcing editor (and any embedded
        CDR forcing -- see ``_split_forcing_data``).
        """
        if getattr(self, "_suspended", False):
            return
        fi, cdr = _split_forcing_data(self.catalog.forcing_data(self.forcing_dd.value))
        self._build_forcing_editor(fi)
        self._cdr_forcing = cdr
        self.cdr_status.value = (
            f"<span style='color:#080'>✓ CDR loaded from ForcingSpec: "
            f"{len(cdr.get('releases', []))} release(s)</span>"
            if cdr
            else ""
        )
        self._forcing_seed = self._forcing_editor.gather()
        self._rebuild()

    def _on_forcing_change(self):
        # composition.forcing.modified is derived in _rebuild() by comparing the
        # current gather() to self._forcing_seed -- no flag to set here.
        if getattr(self, "_suspended", False):
            return
        self._rebuild()

    def _on_output_spec(self, _change):
        """Selecting an OutputSpec seeds the output sections. Clear any manual
        overrides on those sections/fields so the selection takes effect cleanly.
        """
        if getattr(self, "_suspended", False):
            return
        self._overrides = {
            (s, f): v
            for (s, f), v in self._overrides.items()
            if not _is_output_key(s, f)
        }
        self._rebuild()

    def _output_settings(self) -> dict[str, Any]:
        """The selected OutputSpec's settings."""
        return self.catalog.output_data(self.output_dd.value)

    def _composition(self) -> Composition:
        # Every piece keeps origin="catalog" when picked from the catalog (never
        # flips to "custom" on edit) -- `modified` is what signals a deviation.
        # `modified` itself is computed afterward in `_rebuild()`, where the
        # composed baseline, effective settings, and per-piece seeds are all
        # available; the base PieceRefs built here always start `modified=False`.
        dom = (
            PieceRef(name=self.domain_dd.value, origin="catalog")
            if self.domain_dd.value != "<custom>"
            else PieceRef(name=self.grid_name.value, origin="custom")
        )
        # forcing/output are always an explicit catalog selection now (no more
        # "model_default" origin -- ModelSpec no longer provides either as a fallback).
        forcing = PieceRef(name=self.forcing_dd.value, origin="catalog")
        output = PieceRef(name=self.output_dd.value, origin="catalog")
        return Composition(
            model=PieceRef(name=self.model_dd.value, origin="catalog"),
            domain=dom,
            forcing=forcing,
            output=output,
        )

    def _verify_piece_roundtrip(self, piece: str, new_name: str) -> bool:
        """Side-effect-free check: does re-resolving with ``piece`` sourced from
        its freshly-written catalog file (``new_name``) reproduce the exact same
        resolved blueprint currently shown (``self.config``)?

        Reads ``self._gather()``/``self._overrides``/``self.catalog`` only --
        mutates nothing on the wizard. Compares ``content_hash()``, which covers
        exactly the results-affecting data (excludes identity/composition/
        provenance/working_dir -- see ``ForgeBlueprint._HASH_EXCLUDE``), so a
        match proves the saved piece is a safe substitute for what's currently
        composed/edited and the piece can be marked ``modified=False``.
        """
        if self.config is None:
            return False
        kw = self._gather()
        overrides2 = dict(self._overrides)
        try:
            if piece == "output":
                kw["output_settings"] = self.catalog.output_data(new_name)
                overrides2 = {
                    k: v for k, v in overrides2.items() if not _is_output_key(*k)
                }
            elif piece == "model":
                kw["model_dir"] = self.catalog.model_dir(new_name)
                overrides2 = {k: v for k, v in overrides2.items() if _is_output_key(*k)}
            elif piece == "forcing":
                fi, cdr = _split_forcing_data(self.catalog.forcing_data(new_name))
                kw["forcing_inputs"] = fi
                kw["cdr_forcing"] = cdr
            elif piece == "domain":
                d = self.catalog.domain_data(new_name)
                kw["grid_kwargs"] = d.get("grid_kwargs", {})
                kw["open_boundaries"] = d.get("open_boundaries", {})
                kw["partitioning"] = d.get("partitioning", {})
                kw["topography_source"] = d.get("topography_source", "ETOPO5")
                kw.pop("topography_path", None)
                if d.get("topography_path"):
                    kw["topography_path"] = d["topography_path"]
                for k in ("grid_kwargs_child", "grid_kwargs_parent", "metadata_child"):
                    kw.pop(k, None)
                    if d.get(k) is not None:
                        kw[k] = d[k]
                kw["nesting_include_pressure_fluxes"] = bool(
                    d.get("nesting_include_pressure_fluxes", False)
                )
            else:
                raise ValueError(f"unknown piece {piece!r}")
            cfg2 = build_forge_blueprint(**kw)
        except Exception:
            return False
        eff2 = _apply_overrides(cfg2.model_settings, overrides2)
        cfg2 = cfg2.model_copy(update={"model_settings": eff2})
        return cfg2.content_hash() == self.config.content_hash()

    @staticmethod
    def _sources_to_inputs(cfg: ForgeBlueprint) -> dict[str, Any]:
        """Reconstruct an ``inputs``-shaped forcing dict from a ForgeBlueprint's sources
        (reverse of the resolver) so a loaded config seeds the forcing editor.
        """

        def src(spec):
            d = {"name": spec.name}
            if spec.climatology:
                d["climatology"] = True
            if spec.glorys_layout:
                d["glorys_layout"] = spec.glorys_layout
            if getattr(spec, "path", None):
                d["path"] = spec.path
            return d

        f = cfg.forcing
        ic = {"source": src(f.initial_conditions.source)}
        if f.initial_conditions.bgc_source:
            ic["bgc_source"] = src(f.initial_conditions.bgc_source)
        _ic_interp = getattr(f.initial_conditions, "bgc_interpolation_method", None)
        if (
            _ic_interp is not None
            and getattr(_ic_interp, "value", _ic_interp) != BgcInterpMethod.DEPTH.value
        ):
            ic["bgc_interpolation_method"] = getattr(_ic_interp, "value", _ic_interp)
        if getattr(f.initial_conditions, "options", None):
            ic["options"] = dict(f.initial_conditions.options)
        forcing: dict[str, Any] = {}
        for cat, items in (
            ("surface", f.surface),
            ("boundary", f.boundary),
            ("tidal", f.tidal),
            ("river", f.river),
        ):
            out = []
            for it in items:
                d: dict[str, Any] = {"source": src(it.source)}
                for f in ("type", "coarse_grid_mode"):
                    v = getattr(it, f, None)
                    if v is not None:
                        d[f] = v
                if getattr(it, "correct_radiation", False):
                    d["correct_radiation"] = True
                if getattr(it, "restoring_forces", None):
                    d["restoring_forces"] = it.restoring_forces
                if getattr(it, "ntides", None) is not None:
                    d["ntides"] = it.ntides
                if getattr(it, "include_bgc", False):
                    d["include_bgc"] = True
                # roms-tools >=4 boundary regrid/interp knobs. getattr(v, "value", v)
                # normalizes (str, Enum) members to their plain string value.
                _b_interp = getattr(it, "bgc_interpolation_method", None)
                if (
                    _b_interp is not None
                    and getattr(_b_interp, "value", _b_interp)
                    != BgcInterpMethod.DEPTH.value
                ):
                    d["bgc_interpolation_method"] = getattr(
                        _b_interp, "value", _b_interp
                    )
                for f2 in ("prefill", "regrid_method", "extrap_method"):
                    v2 = getattr(it, f2, None)
                    if v2 is not None:
                        d[f2] = getattr(v2, "value", v2)
                # river coastal/edge buffers
                if getattr(it, "coast_snap_buffer_km", None) is not None:
                    d["coast_snap_buffer_km"] = it.coast_snap_buffer_km
                if getattr(it, "domain_edge_buffer", 20) != 20:
                    d["domain_edge_buffer"] = it.domain_edge_buffer
                if getattr(it, "options", None):
                    d["options"] = dict(it.options)
                out.append(d)
            forcing[cat] = out
        return {
            "initial_conditions": ic,
            "forcing": forcing,
        }

    def _on_domain(self, _change):
        """Prefill from a cataloged Domain.yml when one is selected."""
        name = self.domain_dd.value
        if name == "<custom>":
            self._domain_seed = None
            return
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        with self._suspend():
            self.grid_name.value = data.get("grid_name", name)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            for d, w in self.bnd.items():
                w.value = bool((data.get("open_boundaries", {}) or {}).get(d, False))
            part = data.get("partitioning", {}) or {}
            self.npx.value = int(part.get("n_procs_x", self.npx.value))
            self.npy.value = int(part.get("n_procs_y", self.npy.value))
            for key, picker in (("start_time", self.start), ("end_time", self.end)):
                if data.get(key):
                    picker.value = datetime.fromisoformat(str(data[key])).date()
            if data.get("model_name") in self.model_dd.options:
                self.model_dd.value = data["model_name"]
            self.topo_source.value = data.get("topography_source", "ETOPO5")
            self.topo_path.value = data.get("topography_path", "") or ""
            # Nesting: mirrors _populate_nesting (loaded-blueprint path) but reads
            # from a DomainSpec's Domain.yml (only present if the spec was saved
            # via register_domain_from_dict with nesting active).
            child = data.get("grid_kwargs_child")
            self.nest_enable.value = child is not None
            if child:
                for k, w in self.child_w.items():
                    if k in child:
                        w.value = child[k]
                period = (data.get("metadata_child") or {}).get("period")
                if period is not None:
                    self.nest_period.value = float(period)
                self.nest_pressure_fluxes.value = bool(
                    data.get("nesting_include_pressure_fluxes", False)
                )
        self._domain_seed = self._domain_snapshot()
        self._rebuild()
        self._on_plot(None)

    def _domain_snapshot(self) -> dict[str, Any]:
        """The domain-defining widget values at the moment of a catalog Domain pick.
        Compared against the current values in `_rebuild()` to detect an edit made
        after selection (`composition.domain.modified`).
        """
        return {
            "grid_name": self.grid_name.value,
            "grid_w": {k: w.value for k, w in self.grid_w.items()},
            "scoord_chk": self.scoord_chk.value,
            "bnd": {d: w.value for d, w in self.bnd.items()},
            "npx": self.npx.value,
            "npy": self.npy.value,
            "topo_source": self.topo_source.value,
            "topo_path": self.topo_path.value,
            "nest_enable": self.nest_enable.value,
            "child_w": {k: w.value for k, w in self.child_w.items()},
            "nest_period": self.nest_period.value,
            "nest_pressure_fluxes": self.nest_pressure_fluxes.value,
        }

    def _domain_piece_data(self) -> dict[str, Any]:
        """Build a ``Domain.yml``-shaped dict from the current widget state (the
        domain-piece extractor for "save modified pieces to catalog"). Includes
        topography and nesting -- both results-affecting -- so a saved DomainSpec
        actually round-trips (see ``_verify_piece_roundtrip``); ``register_domain``
        (the ForgeExecutor-driven path) predates these and omits them.
        """
        kw = self._gather()
        data: dict[str, Any] = {
            "description": self.description.value,
            "model_name": self.model_dd.value,
            "grid_name": self.grid_name.value,
            "start_time": self.start.value.isoformat(),
            "end_time": self.end.value.isoformat(),
            "grid_kwargs": kw["grid_kwargs"],
            "open_boundaries": kw["open_boundaries"],
            "partitioning": kw["partitioning"],
            "topography_source": self.topo_source.value,
        }
        if self.topo_path.value.strip():
            data["topography_path"] = self.topo_path.value.strip()
        for k in (
            "grid_kwargs_child",
            "grid_kwargs_parent",
            "metadata_child",
            "nesting_include_pressure_fluxes",
        ):
            if k in kw:
                data[k] = kw[k]
        return data

    class _Suspender:
        def __init__(self, wiz):
            self.wiz = wiz

        def __enter__(self):
            self.wiz._suspended = True

        def __exit__(self, *a):
            self.wiz._suspended = False

    def _suspend(self):
        return ForgeBlueprintWizard._Suspender(self)

    # ---- load / import an existing config ------------------------------------
    def _on_load_path(self, _):
        path = self.load_path.value.strip()
        if not path:
            self.load_status.value = (
                "<span style='color:#b00'>Enter a path first.</span>"
            )
            return
        try:
            cfg = ForgeBlueprint.from_yaml(path)
        except Exception as exc:
            self.load_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _on_upload(self, _change):
        files = self.upload.value
        if not files:
            return
        item = (
            files[0] if isinstance(files, (list, tuple)) else next(iter(files.values()))
        )
        self._load_bytes(bytes(item["content"]))

    def _load_bytes(self, content: bytes):
        """Parse + load a forge_blueprint from raw YAML bytes (browser upload path)."""
        try:
            cfg = ForgeBlueprint.from_yaml_data(yaml.safe_load(content))
        except Exception as exc:
            self.load_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _set_load_status(self, cfg: ForgeBlueprint, loaded_problems):
        msg = f"<span style='color:#080'>Loaded {cfg.casename}</span>"
        if loaded_problems:
            msg += (
                f" &nbsp;<span style='color:#b00'>⚠ {len(loaded_problems)} invalid "
                "settings value(s) in the file</span>"
            )
        self.load_status.value = msg

    # ---- CDR forcing upload ----------------------------------------------------
    def _on_cdr_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        content = bytes(item["content"])
        try:
            parsed = read_cdr_forcing_yaml(content.decode("utf-8"))
            # Eager-validate against the real roms-tools class -- the resolver never
            # constructs a CDRForcing, so a structurally-valid-but-semantically-broken
            # upload (empty releases, bad time ordering, unknown tracer, an
            # incompatible roms-tools version) would otherwise pass silently through
            # _rebuild() and only fail much later, during Phase-2 execution.
            import roms_tools as rt

            cdr = rt.CDRForcing(**parsed)
        except Exception as exc:
            self._cdr_forcing = None
            self.cdr_status.value = (
                f"<span style='color:#b00'>CDR invalid: {type(exc).__name__}: "
                f"{exc}</span>"
            )
            self._rebuild()
            return
        self._cdr_forcing = parsed
        self.cdr_status.value = (
            f"<span style='color:#080'>✓ CDR: {len(cdr.releases)} release(s)</span>"
        )
        self._rebuild()

    def _on_cdr_clear(self, _):
        self._cdr_forcing = None
        self.cdr_upload.value = ()
        self.cdr_status.value = ""
        self._rebuild()

    def _populate_from(self, cfg: ForgeBlueprint):
        """Set the widgets from a loaded ForgeBlueprint, then re-resolve once.

        Round-trips the authoring inputs (identity / run / domain / partitioning /
        nesting / dt). Any value in the file that differs from what the composed pieces
        would produce is reconstructed as a manual override (so load is non-lossy and
        the overrides layer is rebuilt), then applied on top in ``_rebuild``.

        Returns any validation problems found in the *loaded file's* model_settings.
        """
        loaded_problems = validate_run_time_sections(cfg.model_settings)
        with self._suspend():
            # domain dropdown -> custom (the file, not a catalog entry, is authoritative)
            self.domain_dd.value = "<custom>"
            if cfg.composition.model.name in self.model_dd.options:
                self.model_dd.value = cfg.composition.model.name
            self.grid_name.value = cfg.domain.grid_name
            self.description.value = cfg.identity.description
            self.name.value = cfg.identity.name
            self._name_touched = (
                True  # a loaded name is a deliberate choice, not a default
            )
            self.save_path.value = f"{cfg.name}.forge_blueprint.yml"
            self._save_path_touched = True
            self.start.value = cfg.run.start_date.date()
            self.end.value = cfg.run.end_date.date()
            gk = cfg.domain.grid_kwargs
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            for d, w in self.bnd.items():
                w.value = bool(getattr(cfg.domain.open_boundaries, d))
            self.npx.value = cfg.domain.partitioning.n_procs_x
            self.npy.value = cfg.domain.partitioning.n_procs_y
            self.use_pio_chk.value = bool(
                (cfg.model_settings.get("cppdefs") or {}).get("use_pio", False)
            )
            self.bgc_dd.value = (
                "marbl"
                if bool((cfg.model_settings.get("cppdefs") or {}).get("marbl", True))
                else "none"
            )
            # Show the file's actual pinned ref, falling back to the (now-selected)
            # model's default when the file matches it exactly.
            stored_ref = cfg.code.roms.commit or cfg.code.roms.branch or ""
            default_ref = self._model_default_roms_ref()
            self.roms_ref.value = (
                default_ref if stored_ref == default_ref else stored_ref
            )
            self.topo_source.value = getattr(
                cfg.domain.topography_source, "value", cfg.domain.topography_source
            )
            self.topo_path.value = cfg.domain.topography_path or ""
            # CDR: FileUpload can't be repopulated with the original file, but the
            # parsed dict persists on the instance and re-emits via _gather(), so
            # load stays non-lossy.
            self._cdr_forcing = cfg.forcing.cdr_forcing or None
            self.cdr_upload.value = ()
            self.cdr_status.value = (
                f"<span style='color:#080'>✓ CDR loaded: "
                f"{len(self._cdr_forcing.get('releases', []))} release(s)</span>"
                if self._cdr_forcing
                else ""
            )
            dt = (cfg.model_settings.get("time_stepping", {}) or {}).get("dt")
            if dt is not None:
                self.dt.value = float(dt)
            self._populate_nesting(cfg)
            # forcing: reconstruct the editor from the loaded sources. Forcing/output
            # dropdowns always need a valid catalog selection (no more "model_default"
            # fallback value); fall back to the first available option for an older
            # file recorded with origin="model_default" or an unknown/missing name.
            fname = cfg.composition.forcing.name
            if fname in self.forcing_dd.options:
                self.forcing_dd.value = fname
            elif self.forcing_dd.options:
                self.forcing_dd.value = self.forcing_dd.options[0]
            self._build_forcing_editor(self._sources_to_inputs(cfg))
            # Seed forcing.modified against the *catalog* piece (not the just-loaded
            # sources) so a deviation is detected the same way as during authoring --
            # a file that matches its recorded catalog forcing loads as unmodified;
            # one that was hand-edited before saving loads as modified.
            try:
                fi, _cdr = _split_forcing_data(
                    self.catalog.forcing_data(self.forcing_dd.value)
                )
                self._forcing_seed = _ForcingEditor(
                    self.W, fi, on_change=lambda: None
                ).gather()
            except Exception:
                self._forcing_seed = None
            # output piece selection
            oname = cfg.composition.output.name
            if oname in self.output_dd.options:
                self.output_dd.value = oname
            elif self.output_dd.options:
                self.output_dd.value = self.output_dd.options[0]
        # Reconstruct the overrides layer = diff(loaded model_settings, composed). This
        # captures every manual deviation regardless of the file's recorded provenance,
        # making load fully non-lossy.
        try:
            composed = build_forge_blueprint(**self._gather()).model_settings
            self._overrides = _diff_overrides(cfg.model_settings, composed)
        except Exception:
            self._overrides = {}
        self._rebuild()
        return loaded_problems

    # ---- gather + resolve ----------------------------------------------------
    def _gather(self) -> dict[str, Any]:
        gk: dict[str, Any] = {}
        for k in _GRID_INT:
            gk[k] = int(self.grid_w[k].value)
        for k in _GRID_FLOAT:
            gk[k] = float(self.grid_w[k].value)
        if self.scoord_chk.value:
            for k in _SCOORD:
                gk[k] = float(self.grid_w[k].value)
        # hmin + close_narrow_channels + mask_shapefile injected into grid_kwargs
        if self.hmin.value != 5.0:
            gk["hmin"] = float(self.hmin.value)
        if self.close_narrow_chk.value:
            gk["close_narrow_channels"] = True
        if self.mask_shapefile.value.strip():
            gk["mask_shapefile"] = self.mask_shapefile.value.strip()
        kw = dict(
            model_dir=self.catalog.model_dir(self.model_dd.value),
            grid_name=self.grid_name.value,
            grid_kwargs=gk,
            open_boundaries={d: w.value for d, w in self.bnd.items()},
            partitioning={
                "n_procs_x": int(self.npx.value),
                "n_procs_y": int(self.npy.value),
            },
            start_date=datetime.combine(self.start.value, datetime.min.time()),
            end_date=datetime.combine(self.end.value, datetime.min.time()),
            description=self.description.value,
            # Untouched: always pass None so the resolver recomputes a fresh default
            # (self.name.value may still hold the *previous* rebuild's backfilled
            # default -- reusing it here would freeze it instead of tracking inputs).
            name=(self.name.value.strip() or None) if self._name_touched else None,
            dt=float(self.dt.value),
        )
        if self.topo_path.value.strip():  # blank = derive default topography path
            kw["topography_path"] = self.topo_path.value.strip()
        kw["topography_source"] = self.topo_source.value
        kw["use_pio"] = self.use_pio_chk.value
        kw["bgc_mode"] = self.bgc_dd.value
        if self.roms_ref.value.strip():
            kw["roms_ref"] = self.roms_ref.value.strip()
        if self.model_ref_date.value and self.model_ref_date.value != date(2000, 1, 1):
            kw["model_reference_date"] = datetime.combine(
                self.model_ref_date.value, datetime.min.time()
            )
        if self.nest_enable.value:
            ck: dict[str, Any] = {}
            for k in _GRID_INT:
                ck[k] = int(self.child_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                ck[k] = float(self.child_w[k].value)
            kw["grid_kwargs_child"] = ck
            kw["metadata_child"] = {"period": float(self.nest_period.value)}
            if self.nest_pressure_fluxes.value:
                kw["nesting_include_pressure_fluxes"] = True
        # forcing/output are always required now (no more model-default fallback).
        kw["forcing_inputs"] = self._forcing_editor.gather()
        kw["output_settings"] = self._output_settings()
        kw["composition"] = self._composition()
        if self._cdr_forcing:
            kw["cdr_forcing"] = self._cdr_forcing
        return kw

    def _populate_nesting(self, cfg: ForgeBlueprint):
        """Set the nesting widgets from a loaded config (called inside _suspend)."""
        child = cfg.domain.grid_kwargs_child
        self.nest_enable.value = child is not None
        if child:
            for k, w in self.child_w.items():
                if k in child:
                    w.value = child[k]
            period = (cfg.domain.metadata_child or {}).get("period")
            if period is not None:
                self.nest_period.value = float(period)

    def _rebuild(self, *_):
        if getattr(self, "_suspended", False):
            return
        self.preview.clear_output(wait=True)
        if self.start.value is None or self.end.value is None:
            self.config = None
            self.derived.value = "<i>Set start and end dates…</i>"
            self.download_link.value = ""
            return
        try:
            cfg = build_forge_blueprint(**self._gather())
        except Exception as exc:  # validation or input error → show, don't crash
            self.config = None
            self.derived.value = (
                f"<b style='color:#b00'>Invalid:</b> {type(exc).__name__}"
            )
            self.download_link.value = ""
            with self.preview:
                print(f"{type(exc).__name__}: {exc}")
            return

        # Advanced settings editor: every section is editable. The resolver composes
        # a baseline from the pieces; the user's manual edits are a sparse overrides
        # layer applied on top (effective = composed ⊕ overrides). The editor is
        # rebuilt only when the *model* changes (its field set depends on the model).
        composed = cfg.model_settings
        if self.editor is None or self._editor_model != self.model_dd.value:
            self.editor = _SettingsEditor(
                self.W, composed, on_edit=self._on_editor_edit
            )
            self._editor_model = self.model_dd.value
            self.editor_box.children = [self.editor.accordion]

        effective = _apply_overrides(composed, self._overrides)
        self._syncing = True
        try:
            self.editor.sync(effective)  # display effective; don't re-record as edits
        finally:
            self._syncing = False

        # composition.*.modified: "did the user deviate from what the catalog piece
        # seeded" -- editing then reverting clears the flag. Model/output share the
        # accordion overrides layer (a true value-diff via _diff_overrides, so a
        # no-op edit never counts); domain/forcing are widget-based pieces compared
        # against a snapshot captured at the moment of the last catalog pick.
        deviations = _diff_overrides(effective, composed)
        model_modified = any(not _is_output_key(s, f) for s, f in deviations)
        output_modified = any(_is_output_key(s, f) for s, f in deviations)
        domain_modified = (
            self.domain_dd.value != "<custom>"
            and self._domain_seed is not None
            and self._domain_snapshot() != self._domain_seed
        )
        forcing_modified = (
            self._forcing_seed is not None
            and self._forcing_editor.gather() != self._forcing_seed
        )

        comp = cfg.composition.model_copy(
            update={
                "overrides": _overrides_nested(self._overrides),
                "model": cfg.composition.model.model_copy(
                    update={"modified": model_modified}
                ),
                "domain": cfg.composition.domain.model_copy(
                    update={"modified": domain_modified}
                ),
                "forcing": cfg.composition.forcing.model_copy(
                    update={"modified": forcing_modified}
                ),
                "output": cfg.composition.output.model_copy(
                    update={"modified": output_modified}
                ),
            }
        )
        cfg = cfg.model_copy(update={"model_settings": effective, "composition": comp})

        self.config = cfg
        # Backfill the Export name/save-path fields with the current derived default
        # until the user edits either -- their edit "locks in" that field (see
        # _on_name_change/_on_save_path_change). Under _suspend() so these
        # programmatic writes don't themselves flip the touched flags or recurse.
        if not self._name_touched or not self._save_path_touched:
            with self._suspend():
                if not self._name_touched:
                    self.name.value = cfg.name
                if not self._save_path_touched:
                    self.save_path.value = f"{cfg.name}.forge_blueprint.yml"
        self.download_link.value = self._download_html(cfg)
        problems = validate_run_time_sections(cfg.model_settings)
        if problems:
            self.validation.value = (
                "<b style='color:#b00'>⚠ settings validation:</b><br>"
                + "<br>".join(f"&nbsp;&nbsp;{p}" for p in problems[:10])
            )
        else:
            self.validation.value = "<span style='color:#080'>✓ settings valid</span>"
        comp = cfg.composition
        self.derived.value = (
            f"<b>name</b>: <code>{cfg.name}</code> &nbsp; "
            f"<b>casename</b>: <code>{cfg.casename}</code><br>"
            f"<b>composition</b>: model=<code>{comp.model.name}</code> ({comp.model.origin}), "
            f"domain=<code>{comp.domain.name or '—'}</code> ({comp.domain.origin}), "
            f"forcing ({comp.forcing.origin})"
        )
        with self.preview:
            print(cfg.to_yaml_str())

    @staticmethod
    def _download_html(cfg: ForgeBlueprint) -> str:
        """A data-URI download link for the resolved YAML — works in the browser
        (Voilà / JupyterLab) with no server-side file access.
        """
        payload = cfg.to_yaml_str().encode("utf-8")
        b64 = base64.b64encode(payload).decode("ascii")
        fname = f"{cfg.name}.forge_blueprint.yml"
        return (
            f'⬇ <a download="{fname}" href="data:text/yaml;base64,{b64}">'
            f"Download <code>{fname}</code></a>"
        )

    # ---- actions -------------------------------------------------------------
    def _on_compute_dt(self, _):
        self.dt_status.value = "<i>computing…</i>"
        try:
            kw = self._gather()
            kw["dt"] = None  # force CFL computation (builds the grid via roms_tools)
            cfg = build_forge_blueprint(**kw)
            self.dt.value = float(cfg.model_settings["time_stepping"]["dt"])
            self.dt_status.value = (
                f"<span style='color:#080'>dt = {self.dt.value:g} s (CFL)</span>"
            )
        except Exception as exc:
            self.dt_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    def _on_plot(self, _):
        """Build a roms_tools.Grid from the current grid kwargs and render it."""
        self.plot_status.value = "<i>building grid…</i>"
        try:
            import io

            import matplotlib.pyplot as plt
            from roms_tools import Grid

            gk: dict[str, Any] = {}
            for k in _GRID_INT:
                gk[k] = int(self.grid_w[k].value)
            for k in _GRID_FLOAT:
                gk[k] = float(self.grid_w[k].value)
            if self.scoord_chk.value:
                for k in _SCOORD:
                    gk[k] = float(self.grid_w[k].value)
            if self.hmin.value != 5.0:
                gk["hmin"] = float(self.hmin.value)
            if self.close_narrow_chk.value:
                gk["close_narrow_channels"] = True
            if self.mask_shapefile.value.strip():
                gk["mask_shapefile"] = self.mask_shapefile.value.strip()

            plt.ioff()
            try:
                grid = Grid(**gk)
                grid.plot()
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.plot_img.value = buf.read()
            self.plot_status.value = "<span style='color:#080'>✓</span>"
        except Exception as exc:
            self.plot_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    def _on_nest_plot(self, _):
        """Build the parent+child roms_tools Grids and render ``plot_nesting``
        (parent+child boundary overlay) into the Nesting section's own plot,
        separate from the parent-only plot in the Grid section.
        """
        self.nest_plot_status.value = "<i>building grids…</i>"
        try:
            import io

            import matplotlib.pyplot as plt
            from roms_tools import Grid, plot_nesting

            gk: dict[str, Any] = {}
            for k in _GRID_INT:
                gk[k] = int(self.grid_w[k].value)
            for k in _GRID_FLOAT:
                gk[k] = float(self.grid_w[k].value)
            if self.scoord_chk.value:
                for k in _SCOORD:
                    gk[k] = float(self.grid_w[k].value)
            if self.hmin.value != 5.0:
                gk["hmin"] = float(self.hmin.value)
            if self.close_narrow_chk.value:
                gk["close_narrow_channels"] = True
            if self.mask_shapefile.value.strip():
                gk["mask_shapefile"] = self.mask_shapefile.value.strip()

            ck: dict[str, Any] = {}
            for k in _GRID_INT:
                ck[k] = int(self.child_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                ck[k] = float(self.child_w[k].value)

            plt.ioff()
            try:
                parent = Grid(**gk)
                child = Grid(**ck)
                # plot_nesting calls plt.show() internally (no way to suppress it,
                # no return value); under Jupyter's inline backend that renders-
                # and-closes the figure immediately, so plt.gcf() right after
                # would return a fresh blank figure instead of the one just
                # drawn. Neutralize show() for the duration so we can grab and
                # save the actual figure ourselves.
                _real_show, plt.show = plt.show, lambda *a, **k: None
                try:
                    plot_nesting(parent, child)
                finally:
                    plt.show = _real_show
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.nest_plot_img.value = buf.read()
            self.nest_plot_status.value = "<span style='color:#080'>✓</span>"
        except Exception as exc:
            self.nest_plot_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    def _on_save(self, _):
        if self.config is None:
            self.save_status.value = (
                "<span style='color:#b00'>Nothing to save — config is invalid.</span>"
            )
            return
        try:
            p = self.config.to_yaml(Path(self.save_path.value))
            self.save_status.value = f"<span style='color:#080'>Saved {p}</span>"
        except Exception as exc:
            self.save_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    # ---- save modified pieces to catalog --------------------------------------
    # Each handler: validate name -> extract that piece from current state ->
    # write it to the catalog (durable) -> side-effect-free round-trip verify ->
    # on match, commit the minimal state change (repoint the dropdown, drop that
    # piece's overrides / reset its seed) under _suspend() + a single _rebuild();
    # on mismatch, the file is still written but nothing else changes (options
    # refreshed so the new name is selectable, but .value/overrides/seed untouched).
    def _on_save_output(self, _):
        name = self.save_output_name.value.strip()
        if not _valid_spec_name(name):
            self.save_output_status.value = (
                "<span style='color:#b00'>Invalid name.</span>"
            )
            return
        if self.config is None:
            self.save_output_status.value = (
                "<span style='color:#b00'>Config invalid — nothing to save.</span>"
            )
            return
        try:
            self.catalog.register_output(
                name,
                extract_output_settings(self.config.model_settings),
                description=self.description.value,
            )
        except FileExistsError:
            self.save_output_status.value = (
                f"<span style='color:#b00'>'{name}' already exists.</span>"
            )
            return
        except Exception as exc:
            self.save_output_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_piece_roundtrip("output", name)
        with self._suspend():
            # ipywidgets resets .value to the first option on a bare `.options =`
            # reassignment even when the old value is still present -- restore it
            # explicitly so a mismatch genuinely leaves the selection untouched.
            old_value = self.output_dd.value
            self.output_dd.options = list(self.catalog.output_names)
            self.output_dd.value = name if ok else old_value
            if ok:
                self._overrides = {
                    k: v for k, v in self._overrides.items() if not _is_output_key(*k)
                }
        if ok:
            self._rebuild()
            self.save_output_status.value = (
                f"<span style='color:#080'>Saved '{name}' ✓ (now unmodified).</span>"
            )
        else:
            self.save_output_status.value = (
                f"<span style='color:#b58900'>Saved '{name}', but round-trip "
                "differs — piece kept as modified.</span>"
            )

    def _on_save_model(self, _):
        name = self.save_model_name.value.strip()
        if not _valid_spec_name(name):
            self.save_model_status.value = (
                "<span style='color:#b00'>Invalid name.</span>"
            )
            return
        if self.config is None:
            self.save_model_status.value = (
                "<span style='color:#b00'>Config invalid — nothing to save.</span>"
            )
            return
        try:
            self.catalog.register_model_from_settings(
                name,
                _model_owned_settings(self.config.model_settings),
                self.catalog.model_dir(self.model_dd.value),
                description=self.description.value,
            )
        except FileExistsError:
            self.save_model_status.value = (
                f"<span style='color:#b00'>'{name}' already exists.</span>"
            )
            return
        except Exception as exc:
            self.save_model_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_piece_roundtrip("model", name)
        with self._suspend():
            old_value = self.model_dd.value
            self.model_dd.options = list(self.catalog.model_names)
            self.model_dd.value = name if ok else old_value
            if ok:
                self._overrides = {
                    k: v for k, v in self._overrides.items() if _is_output_key(*k)
                }
        if ok:
            self._rebuild()
            self.save_model_status.value = (
                f"<span style='color:#080'>Saved '{name}' ✓ (now unmodified).</span>"
            )
        else:
            self.save_model_status.value = (
                f"<span style='color:#b58900'>Saved '{name}', but round-trip "
                "differs — piece kept as modified.</span>"
            )

    def _on_save_domain(self, _):
        name = self.save_domain_name.value.strip()
        if not _valid_spec_name(name):
            self.save_domain_status.value = (
                "<span style='color:#b00'>Invalid name.</span>"
            )
            return
        if self.config is None:
            self.save_domain_status.value = (
                "<span style='color:#b00'>Config invalid — nothing to save.</span>"
            )
            return
        try:
            self.catalog.register_domain_from_dict(name, self._domain_piece_data())
        except FileExistsError:
            self.save_domain_status.value = (
                f"<span style='color:#b00'>'{name}' already exists.</span>"
            )
            return
        except Exception as exc:
            self.save_domain_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_piece_roundtrip("domain", name)
        with self._suspend():
            old_value = self.domain_dd.value
            self.domain_dd.options = ["<custom>", *self.catalog.domain_names]
            self.domain_dd.value = name if ok else old_value
            if ok:
                self._domain_seed = self._domain_snapshot()
        if ok:
            self._rebuild()
            self.save_domain_status.value = (
                f"<span style='color:#080'>Saved '{name}' ✓ (now unmodified).</span>"
            )
        else:
            self.save_domain_status.value = (
                f"<span style='color:#b58900'>Saved '{name}', but round-trip "
                "differs — piece kept as modified.</span>"
            )

    def _on_save_forcing(self, _):
        name = self.save_forcing_name.value.strip()
        if not _valid_spec_name(name):
            self.save_forcing_status.value = (
                "<span style='color:#b00'>Invalid name.</span>"
            )
            return
        if self.config is None:
            self.save_forcing_status.value = (
                "<span style='color:#b00'>Config invalid — nothing to save.</span>"
            )
            return
        try:
            self.catalog.register_forcing(
                name,
                self._forcing_editor.gather(),
                cdr_forcing=self._cdr_forcing,
                description=self.description.value,
            )
        except FileExistsError:
            self.save_forcing_status.value = (
                f"<span style='color:#b00'>'{name}' already exists.</span>"
            )
            return
        except Exception as exc:
            self.save_forcing_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_piece_roundtrip("forcing", name)
        with self._suspend():
            old_value = self.forcing_dd.value
            self.forcing_dd.options = list(self.catalog.forcing_names)
            self.forcing_dd.value = name if ok else old_value
            if ok:
                self._forcing_seed = self._forcing_editor.gather()
        if ok:
            self._rebuild()
            self.save_forcing_status.value = (
                f"<span style='color:#080'>Saved '{name}' ✓ (now unmodified).</span>"
            )
        else:
            self.save_forcing_status.value = (
                f"<span style='color:#b58900'>Saved '{name}', but round-trip "
                "differs — piece kept as modified.</span>"
            )

    def _build_run_command(self, blueprint_path: str) -> list[str]:
        """Command the Run button invokes. Isolated here so swapping this for a
        future ``cstar blueprint run <path>`` (once the executor moves into the
        C-Star repo) is a one-line change.
        """
        import sys

        return [sys.executable, "-m", "cstar_forge.run", blueprint_path]

    def _on_run(self, _):
        if self.config is None:
            self.run_status.value = (
                "<span style='color:#b00'>Nothing to run — config is invalid.</span>"
            )
            return
        _schedule_coroutine(self._run_async())

    async def _run_async(self):
        """Save the current blueprint, then launch the executor CLI as a
        subprocess and stream its combined stdout/stderr into ``run_output`` line
        by line as it arrives (not all at once at the end).
        """
        import asyncio

        self.run_btn.disabled = True
        self.run_output.clear_output(wait=True)
        self.run_status.value = "<i>saving blueprint…</i>"
        try:
            path = self.config.to_yaml(Path(self.save_path.value))
            cmd = self._build_run_command(str(path))
            self.run_status.value = f"<i>running: {' '.join(cmd)}</i>"
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            # append_stdout (not `with self.run_output: print(...)`) appends
            # directly to the widget's output list -- it works whether or not a
            # live Jupyter kernel is routing stdout via iopub messaging, so lines
            # land in the log both in a real notebook and in tests.
            async for line in proc.stdout:
                self.run_output.append_stdout(line.decode(errors="replace"))
            code = await proc.wait()
            self.run_status.value = (
                "<span style='color:#080'>✓ finished</span>"
                if code == 0
                else f"<span style='color:#b00'>exited with code {code}</span>"
            )
        except Exception as exc:
            self.run_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
        finally:
            self.run_btn.disabled = False

    # ---- layout / display ----------------------------------------------------
    @property
    def widget(self):
        W = self.W

        def section(title, *rows):
            return W.VBox(
                [W.HTML(f"<b>{title}</b>"), *rows],
                layout=W.Layout(
                    border="1px solid #e0e0e0", padding="8px", margin="4px 0"
                ),
            )

        grid_box = W.GridBox(
            [self.grid_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"),
        )
        child_box = W.GridBox(
            [self.child_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"),
        )
        return W.VBox(
            [
                W.HTML(
                    "<h3>ForgeBlueprint wizard</h3>"
                    "<i>Pick a Model and (optionally) a Domain, tweak fields, review, save. "
                    "Or load an existing forge_blueprint.yml to edit it. Fine-tune model "
                    "settings under “Advanced settings”.</i>"
                ),
                section(
                    "Load existing (optional)",
                    W.HBox([self.load_path, self.load_btn]),
                    self.upload,
                    self.load_status,
                ),
                section(
                    "Pieces",
                    W.HBox([self.model_dd, self.roms_ref, self.bgc_dd]),
                    self.forcing_dd,
                    self.output_dd,
                    self.domain_dd,
                    self.grid_name,
                ),
                section(
                    "Grid",
                    W.HBox(
                        [
                            W.VBox(
                                [
                                    grid_box,
                                    self.scoord_chk,
                                    W.HBox([self.hmin, self.close_narrow_chk]),
                                    self.mask_shapefile,
                                    self.topo_source,
                                    self.topo_path,
                                ]
                            ),
                            W.VBox(
                                [
                                    W.HBox([self.plot_btn, self.plot_status]),
                                    self.plot_img,
                                ],
                                layout=W.Layout(padding="0 0 0 20px"),
                            ),
                        ]
                    ),
                ),
                section("Open boundaries", W.HBox(list(self.bnd.values()))),
                section(
                    "Nesting (optional)",
                    W.HBox(
                        [
                            W.VBox(
                                [
                                    self.nest_enable,
                                    self.nest_domain_dd,
                                    child_box,
                                    W.HBox(
                                        [self.nest_period, self.nest_pressure_fluxes]
                                    ),
                                ]
                            ),
                            W.VBox(
                                [
                                    W.HBox([self.nest_plot_btn, self.nest_plot_status]),
                                    self.nest_plot_img,
                                ],
                                layout=W.Layout(padding="0 0 0 20px"),
                            ),
                        ]
                    ),
                ),
                section("Forcing", self.forcing_box),
                section(
                    "CDR forcing (optional)",
                    W.HBox([self.cdr_upload, self.cdr_clear_btn]),
                    self.cdr_status,
                ),
                section(
                    "Partitioning",
                    W.HBox([self.npx, self.npy, self.use_pio_chk]),
                ),
                section(
                    "Run window",
                    self.start,
                    self.end,
                    self.model_ref_date,
                    self.description,
                ),
                section("Timestep", W.HBox([self.dt, self.dt_btn]), self.dt_status),
                section(
                    "Advanced settings (model defaults — collapsed; click to edit)",
                    self.editor_box,
                ),
                section(
                    "Review (resolved ForgeBlueprint)",
                    self.derived,
                    self.validation,
                    self.preview,
                ),
                section(
                    "Save modified pieces to catalog",
                    W.HTML(
                        "<i>Promote an edited piece to a new named catalog entry. "
                        "Only marked unmodified if the saved file re-resolves to "
                        "the identical blueprint.</i>"
                    ),
                    W.HBox(
                        [
                            self.save_output_name,
                            self.save_output_btn,
                            self.save_output_status,
                        ]
                    ),
                    W.HBox(
                        [
                            self.save_model_name,
                            self.save_model_btn,
                            self.save_model_status,
                        ]
                    ),
                    W.HBox(
                        [
                            self.save_domain_name,
                            self.save_domain_btn,
                            self.save_domain_status,
                        ]
                    ),
                    W.HBox(
                        [
                            self.save_forcing_name,
                            self.save_forcing_btn,
                            self.save_forcing_status,
                        ]
                    ),
                ),
                section(
                    "Export",
                    self.name,
                    self.download_link,
                    W.HBox([self.save_path, self.save_btn]),
                    self.save_status,
                ),
                section(
                    "Run",
                    self.run_warning,
                    self.run_later_note,
                    W.HBox([self.run_btn, self.run_status]),
                    self.run_output,
                ),
            ]
        )

    def display(self):
        from IPython.display import display

        display(self.widget)
