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
import warnings
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
    Prefill,
    RegridMethod,
    RestoringSurfaceSource,
    RiverBgcSource,
    RiverSource,
    SpecRef,
    SurfaceType,
    TidalSource,
)
from cstar_forge.forge.namelist_model import (
    RunTimeSettings,
    run_time_settings_for_ref,
    validate_run_time_sections,
    version_gated_section_names,
)
from cstar_forge.forge.user_files import hash_netcdf_contents
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
    (
        "grid",
        "grid_file",
    ): "Attach a pre-made grid netCDF instead of generating one from the kwargs "
    "above. Locks the grid/topography/nesting fields (their values are read from "
    "the file); the file must exist at this exact path on the machine that runs "
    "the executor.",
    (
        "cdr",
        "cdr_file",
    ): "Attach a pre-made CDR-forcing netCDF instead of building one from the "
    "uploaded roms-tools YAML above. Mutually exclusive with it; the file must "
    "exist at this exact path on the machine that runs the executor.",
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
    (
        "nesting",
        "parent_enable",
    ): "Declare that this grid is nested inside a coarser parent grid. The grid "
    "is aligned to the parent (roms_tools.align_grids) and its boundary values "
    "come from the parent's nesting.nc extraction rather than reanalysis "
    "boundary forcing, which is cleared automatically.",
    (
        "nesting",
        "parent_domain_dd",
    ): "Optionally prefill the parent grid kwargs from a cataloged DomainSpec. "
    "You can still edit any parent-grid field after selecting.",
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
    (
        "ic",
        "prefill",
    ): "Fill NaN (land/void) source cells before regridding. Blank = no source prefill "
    "(NaN-aware regrid + extrapolation, recommended with xESMF). '2d_lateral_fill' = "
    "legacy AMG Poisson fill; 'inverse_dist'/'nearest_s2d' = xESMF source fills; "
    "'nearest_neighbor' = cheap scipy fill (also the fallback when xESMF is absent).",
    (
        "ic",
        "regrid_method",
    ): "Horizontal regrid engine. Blank/'auto' = xESMF if installed else scipy. 'xesmf' = "
    "force xESMF (errors if absent). 'scipy' = force scipy (byte-reproducible with prefill).",
    (
        "ic",
        "extrap_method",
    ): "Destination extrapolation on the default (no-prefill) path to guarantee NaN-free "
    "initial conditions. Blank = 'inverse_dist' (effective default). 'nearest_s2d' = single "
    "nearest source point. Ignored when a prefill is set.",
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
    (
        "surface",
        "prefill",
    ): "Fill NaN (land/void) source cells before regridding. Blank = no source prefill "
    "(NaN-aware regrid + extrapolation, recommended with xESMF). '2d_lateral_fill' = "
    "legacy AMG Poisson fill; 'inverse_dist'/'nearest_s2d' = xESMF source fills; "
    "'nearest_neighbor' = cheap scipy fill (also the fallback when xESMF is absent).",
    (
        "surface",
        "regrid_method",
    ): "Horizontal regrid engine. Blank/'auto' = xESMF if installed else scipy. 'xesmf' = "
    "force xESMF (errors if absent). 'scipy' = force scipy (byte-reproducible with prefill).",
    (
        "surface",
        "extrap_method",
    ): "Destination extrapolation on the default (no-prefill) path to guarantee NaN-free "
    "surface forcing. Blank = 'inverse_dist' (effective default). 'nearest_s2d' = single "
    "nearest source point. Ignored when a prefill is set.",
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
    (
        "tidal",
        "prefill",
    ): "Fill NaN (land/void) source cells before regridding. Blank = no source prefill "
    "(NaN-aware regrid + extrapolation, recommended with xESMF). '2d_lateral_fill' = "
    "legacy AMG Poisson fill; 'inverse_dist'/'nearest_s2d' = xESMF source fills; "
    "'nearest_neighbor' = cheap scipy fill (also the fallback when xESMF is absent).",
    (
        "tidal",
        "regrid_method",
    ): "Horizontal regrid engine. Blank/'auto' = xESMF if installed else scipy. 'xesmf' = "
    "force xESMF (errors if absent). 'scipy' = force scipy (byte-reproducible with prefill).",
    (
        "tidal",
        "extrap_method",
    ): "Destination extrapolation on the default (no-prefill) path to guarantee NaN-free "
    "tidal fields. Blank = 'inverse_dist' (effective default). 'nearest_s2d' = single "
    "nearest source point. Ignored when a prefill is set.",
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
    "Select a 'bgc src' below to choose the tracer dataset (roms-tools ignores "
    "bgc src unless this is checked).",
    (
        "river",
        "convert_to_climatology",
    ): "When to compute a river climatology from the raw data: "
    "'if_any_missing' (default) — compute if any requested months are absent, "
    "'never' — always use raw data, 'always' — always compute a climatology.",
    (
        "river",
        "bgc_source_name",
    ): "River BGC tracer dataset. 'CONSTANTS' uses fixed MARBL default concentrations "
    "(auto-downloaded by roms-tools). 'RIVR2O' uses the RIVR2O river export product "
    "(annual files, 1903-2024); the files must be manually placed at "
    "source_data/RIVR2O/*.nc. Blank + include_bgc checked also falls back to CONSTANTS.",
    (
        "river",
        "bgc_source_path",
    ): "Optional explicit path/glob to the BGC dataset file(s) "
    "(e.g. a custom RIVR2O location). Blank derives the default staged location.",
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
    (
        "river",
        "custom_file",
    ): "Attach a pre-made river-forcing netCDF instead of building one from a "
    "DAI/GLOFAS source (selected via 'src: CUSTOM_FILE' above). The file must "
    "exist at this exact path on the machine that runs the executor.",
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

# Label overrides for namelist-section widgets built via _build_section /
# _make_field_widget. Keyed the same way as HELP_TEXT: (section, field_name).
# Falls back to the raw field name when no override is present.
LABEL_TEXT: dict[tuple[str, str], str] = {
    ("bgc", "xco2air_default"): "Static xco2air (if co2_tvarying is False)",
}


def _namelist_label(section: str, field_name: str) -> str:
    """Look up the display label override for a namelist field, else field_name."""
    return LABEL_TEXT.get((section, field_name), field_name)


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


# Boolean fields that read better as a two-option mode dropdown than a checkbox.
# ``base`` stays bool everywhere else (widgets dict, overrides layer) -- only the
# rendering + read/sync string<->bool mapping is special-cased for these keys.
_BOOL_DROPDOWN_FIELDS: dict[tuple[str, str], tuple[str, str]] = {
    ("cdr_output", "do_avg"): ("averaged", "instantaneous"),
    ("cdr_output", "monthly_averages"): ("monthly", "periodic"),
}


def _make_field_widget(
    W,
    name: str,
    base: type,
    value: Any,
    tooltip: str = "",
    bool_dropdown: tuple[str, str] | None = None,
):
    style = {"description_width": "170px"}
    wide = W.Layout(width="430px")
    num = W.Layout(width="300px")
    kw = {"tooltip": tooltip} if tooltip else {}
    if bool_dropdown is not None:
        true_label, false_label = bool_dropdown
        return W.Dropdown(
            options=(true_label, false_label),
            value=true_label if value else false_label,
            description=name,
            style=style,
            layout=num,
            **kw,
        )
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


def _read_field_widget(
    widget, base: type, original: Any = None, key: tuple[str, str] | None = None
) -> Any:
    v = widget.value
    labels = _BOOL_DROPDOWN_FIELDS.get(key) if key is not None else None
    if labels is not None:
        true_label, _false_label = labels
        return v == true_label
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


def _section_submodel(section: str, settings_cls: type[BaseModel] = RunTimeSettings):
    """The ``settings_cls`` sub-model for a section, or None (scalar / unknown).

    ``settings_cls`` defaults to the legacy :class:`RunTimeSettings` for
    back-compat; callers that know the blueprint's ucla-roms ref should pass
    the class picked by :func:`_wizard_settings_cls_for_ref` instead, so a
    section that varies by ucla-roms version (e.g. ``ocean_vars``) is
    introspected against the right field set.
    """
    field = settings_cls.model_fields.get(section)
    if field is None:
        return None
    ann = field.annotation
    return ann if isinstance(ann, type) and issubclass(ann, BaseModel) else None


def _wizard_settings_cls_for_ref(roms_ref: str | None) -> type[BaseModel]:
    """``run_time_settings_for_ref`` with the non-semver-ref ``UserWarning`` swallowed.

    The wizard calls this on every live rebuild -- including every keystroke
    typed into the ``roms_ref`` override text box -- so letting a branch-name
    pin's warning (e.g. ``"main"``) through would spam the notebook output on
    each edit; the schema selection itself is unaffected, only the warning is
    dropped.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return run_time_settings_for_ref(roms_ref)


# --- overrides layer: effective = composed(specs) ⊕ overrides -----------------
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
# "Save modified specs to catalog" -- per-spec extractors + round-trip verify
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
    """Split a raw ForcingSpec dict (as read from ``Forcing.yaml``) into the
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
    """True if an (section, field) override key belongs to the Output spec
    (vs. the Model spec) -- shared by `_on_output_spec` (clearing stale overrides
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
    # v_sponge is a first-class domain property (self.v_sponge, in "Domain-derived
    # properties") resolved via build_forge_blueprint's own v_sponge= param, not
    # the generic overrides layer -- excluding it here keeps a loaded blueprint's
    # domain.v_sponge from being reconstructed as a phantom accordion override.
    "v_sponge": frozenset({"v_sponge"}),
}


# Modeler-facing consolidation of the raw namelist sections into a few
# Advanced-settings panes, grouped the way an ocean modeler categorizes knobs
# rather than by the exact namelist sub-groups. DISPLAY-ONLY: each widget stays
# keyed by its real ``(section, field)``, so the overrides layer, ``_diff_overrides``,
# and the output/model "modified" tracking are all unaffected by the grouping.
#
# Sections deliberately absent (time_stepping, reference_date_settings, grid,
# s_coord, param, title, output_root_name, initial, forcing, river_frc) are filled
# dynamically at resolve/run time (ntimes from the run duration, grid/IC/forcing
# paths from generated files, river_frc entirely generation-derived) or edited by a
# dedicated widget elsewhere in the wizard (theta_s/theta_b/hc, dt, np_xi/np_eta,
# reference date, PIO/open-boundary checkboxes). Their resolver-composed value still
# flows through untouched -- omitting the pane only removes an editor that would be
# clobbered or duplicated, never the value.
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
            # v_sponge is deliberately absent: it's a first-class domain property
            # with its own dedicated widget in "Domain-derived properties"
            # (self.v_sponge), not a generic model-settings override -- see
            # _ACCORDION_EXCLUDED_FIELDS.
            "sponge_tune",
            "gamma2",
            "ubind",
            "lin_rho_eos",
            # pio_settings is ModelSpec-owned (like the rest of this pane), NOT an
            # output section: edits here must land in composition.model / "Save
            # Model spec", so keep it out of _OUTPUT_CATEGORY.
            "pio_settings",
            "cppdefs",
        ),
    ),
    (
        "Surface & lateral forcing",
        (
            "blk_frc",
            "flux_frc",
            "tides",
            # river_frc is deliberately absent: all three of its typed fields
            # (river_source/analytical/nriv) are generation-derived -- see
            # GENERATION_DERIVED_LEAF_KEYS["river_frc"] in forge_blueprint_engine.py
            # -- so the executor overwrites every one from the actual river forcing
            # (nriv = the "nriver" dimension of the generated/attached dataset).
            # An accordion editor here would only record overrides that generation
            # discards. The resolver-composed value still flows through untouched.
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


# Section names modeled by at least one registered run-time settings tier (e.g.
# pio_settings, only on RunTimeSettingsV0_6_0) -- used by _SettingsEditor to skip
# a section that's version-gated behind a namelist schema boundary but absent
# from the *active* settings_cls, rather than rendering a type-inferred widget
# whose edits an extra="ignore" top-level model would silently discard. A
# section NEVER modeled by any tier (e.g. cppdefs) is not in this set and keeps
# rendering via type-inference as before -- see version_gated_section_names().
_VERSION_GATED_SECTIONS = version_gated_section_names()

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
    typed widgets per field using the ``settings_cls`` sub-model schema (falling
    back to value-type inference). ``settings_cls`` defaults to the legacy
    :class:`RunTimeSettings` for back-compat; pass the class matching the
    blueprint's ucla-roms ref (see :func:`_wizard_settings_cls_for_ref`) so a
    version-varying section (``ocean_vars``, ``particles``) is introspected
    against the right field set -- e.g. ``ocean_vars.nrpf_rst`` only exists pre
    ucla-roms 0.5.0, so a widget for it is only generated when `settings_cls`
    is the legacy class. All panes are collapsed by default. ``sync()``
    pushes values in (used on load); ``read()`` returns a single field. Fields listed
    in ``_ACCORDION_EXCLUDED_FIELDS`` (and sections not named in a category) are
    skipped -- their value still flows through from the resolver-composed settings
    dict (this editor never authors that dict, only a sparse overrides layer), so
    hiding the widget cannot drop or reset the value.
    """

    def __init__(
        self,
        W,
        model_settings: dict[str, Any],
        on_edit=None,
        settings_cls: type[BaseModel] = RunTimeSettings,
    ):
        self.W = W
        self._settings_cls = settings_cls
        # (section, field|None) -> (widget, base_type)
        self._widgets: dict[Any, Any] = {}
        self._section_fields: dict[str, list[str | None]] = {}
        # True while sync() is pushing composed values into widgets -- distinct from
        # the wizard's own _syncing flag, which guards the *wizard-level* on_edit
        # override recording. This one guards the editor-internal forcing rule (see
        # _register_field_rule_observers) so a sync-driven value push never mutates
        # a sibling widget the way a real user edit is allowed to.
        self._syncing_internal = False
        # category title -> sections shown under it (a section may appear under two
        # panes when split along PARTIAL_OUTPUT_SECTIONS, e.g. bgc/marbl_bgc).
        self._pane_sections: dict[str, list[str]] = {}
        panes, titles = [], []
        for title, members in _ADVANCED_CATEGORIES:
            blocks = []
            for section in members:
                if section not in model_settings:
                    continue
                if (
                    section not in self._settings_cls.model_fields
                    and section in _VERSION_GATED_SECTIONS
                ):
                    # section is modeled by some registered settings tier (e.g.
                    # pio_settings on RunTimeSettingsV0_6_0) but not by the
                    # active settings_cls -- e.g. a pre-0.6.0 roms_ref override
                    # with a ModelSpec whose model_settings still carries the
                    # key. Rendering a type-inferred widget here would let the
                    # user edit a value the active (extra="ignore") schema
                    # silently discards downstream. cppdefs is NOT in
                    # _VERSION_GATED_SECTIONS (no tier ever models it) and so
                    # is unaffected -- it keeps rendering via type-inference.
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
        self._register_field_rule_observers()
        self._apply_field_rules()

    def sync(self, model_settings: dict[str, Any]):
        """Set every widget to the effective values (caller suspends edit tracking)."""
        self._syncing_internal = True
        try:
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
                    labels = _BOOL_DROPDOWN_FIELDS.get((section, field))
                    if labels is not None and value is not None:
                        true_label, false_label = labels
                        widget.value = true_label if bool(value) else false_label
                    elif base is list:
                        widget.value = ", ".join(str(x) for x in (value or []))
                    elif value is not None:
                        widget.value = base(value)
                except (ValueError, TypeError):
                    pass
        finally:
            self._syncing_internal = False
        self._apply_field_rules()

    def read(self, section, field):
        widget, base = self._widgets[(section, field)]
        return _read_field_widget(widget, base, key=(section, field))

    # ocean_vars/cdr_output cross-field rules -----------------------------------
    def _register_field_rule_observers(self) -> None:
        """Wire the controlling widgets to `_apply_field_rules` for instant
        show/hide + disable feedback, plus the one forcing rule (monthly
        restarts implies file restarts) on its own dedicated handler.
        """

        def _rerun(_change=None):
            self._apply_field_rules()

        for key in (
            ("ocean_vars", "wrt_file_rst"),
            ("ocean_vars", "monthly_restarts"),
            ("cdr_output", "do_cdr_output"),
            ("cdr_output", "do_avg"),
            ("cdr_output", "monthly_averages"),
        ):
            entry = self._widgets.get(key)
            if entry is not None:
                entry[0].observe(_rerun, names="value")

        wrt_entry = self._widgets.get(("ocean_vars", "wrt_file_rst"))
        monthly_entry = self._widgets.get(("ocean_vars", "monthly_restarts"))
        if wrt_entry is not None and monthly_entry is not None:
            wrt_widget = wrt_entry[0]

            def _force_wrt_on_monthly(change) -> None:
                # monthly_restarts is meaningless without wrt_file_rst -- a real
                # user edit that turns it on pulls wrt_file_rst on too, so the
                # setting isn't silently ignored downstream. Skipped during
                # sync() (self._syncing_internal), which pushes both widgets'
                # values independently and must not cross-mutate them.
                if self._syncing_internal:
                    return
                if change["new"] and not wrt_widget.value:
                    wrt_widget.value = True

            monthly_entry[0].observe(_force_wrt_on_monthly, names="value")

    def _apply_field_rules(self) -> None:
        """Show/hide and enable/disable ocean_vars/cdr_output widgets from their
        CURRENT values -- never mutates a value, only visibility/``disabled``.

        Rationale: the dependent fields are meaningless (and ignored by the
        namelist) once their master switch is off, and `output_period_*` has no
        effect once a "monthly" cadence fixes the period implicitly -- hiding/
        disabling them keeps the form from offering a control with no effect.
        """

        def _show(entry, on: bool) -> None:
            if entry is not None:
                entry[0].layout.display = "" if on else "none"

        wrt = self._widgets.get(("ocean_vars", "wrt_file_rst"))
        monthly_r = self._widgets.get(("ocean_vars", "monthly_restarts"))
        nrpf_r = self._widgets.get(("ocean_vars", "nrpf_rst"))
        period_r = self._widgets.get(("ocean_vars", "output_period_rst"))
        if wrt is not None:
            wrt_on = bool(wrt[0].value)
            _show(monthly_r, wrt_on)
            _show(nrpf_r, wrt_on)
            _show(period_r, wrt_on)
            if monthly_r is not None and period_r is not None:
                period_r[0].disabled = bool(monthly_r[0].value)

        cdr_on = self._widgets.get(("cdr_output", "do_cdr_output"))
        do_avg = self._widgets.get(("cdr_output", "do_avg"))
        monthly_avg = self._widgets.get(("cdr_output", "monthly_averages"))
        period = self._widgets.get(("cdr_output", "output_period"))
        nrpf = self._widgets.get(("cdr_output", "nrpf"))
        if cdr_on is not None:
            on = bool(cdr_on[0].value)
            _show(do_avg, on)
            _show(monthly_avg, on)
            _show(period, on)
            _show(nrpf, on)
            if on and do_avg is not None:
                is_avg = self.read("cdr_output", "do_avg")
                _show(monthly_avg, is_avg)
                if period is not None:
                    is_monthly = (
                        self.read("cdr_output", "monthly_averages")
                        if is_avg and monthly_avg is not None
                        else False
                    )
                    period[0].disabled = bool(is_avg and is_monthly)

    def _build_section(
        self,
        section: str,
        value: Any,
        include: frozenset | None = None,
        exclude: frozenset = frozenset(),
    ):
        W = self.W
        sub = _section_submodel(section, self._settings_cls)
        if not isinstance(value, dict):  # scalar section (e.g. gamma2, ubind)
            base = _base_type(None, value)
            tip = _namelist_tooltip(section, section)
            label = _namelist_label(section, section)
            w = _make_field_widget(W, label, base, value, tooltip=tip)
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
            label = _namelist_label(section, key)
            bool_dropdown = _BOOL_DROPDOWN_FIELDS.get((section, key))
            w = _make_field_widget(
                W, label, base, val, tooltip=tip, bool_dropdown=bool_dropdown
            )
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
# Sentinel dropdown value meaning "no initial conditions" -- valid only for a
# child domain (state comes from the parent's nesting extraction instead).
_IC_NONE = "(none)"
_IC_SOURCE_OPTS = [e.value for e in InitialConditionsSource] + [_IC_NONE]
_IC_BGC_SOURCE_OPTS = [""] + [e.value for e in BgcInitialConditionsSource]
_RIVER_BGC_SOURCE_OPTS = [""] + [e.value for e in RiverBgcSource]


def _add_regrid_widgets(W, w: dict[str, Any], cat: str, item: dict[str, Any], small):
    """Build the shared prefill/regrid_method/extrap_method dropdowns onto row-widget
    dict ``w``, tooltipped for ``cat``. Used by surface, boundary, and tidal forcing —
    the three-of-five roms-tools >=4 regrid knobs with wizard UI (``prefill_kwargs``/
    ``extrap_kwargs`` stay typed-but-UI-less, reachable via the ``options`` editor).
    """
    _prefill_val = str(item.get("prefill") or "")
    if _prefill_val not in _PREFILL_OPTS:
        _prefill_val = ""
    w["prefill"] = W.Dropdown(
        options=_PREFILL_OPTS,
        value=_prefill_val,
        description="prefill:",
        style=small,
        layout=W.Layout(width="200px"),
        tooltip=_tip(cat, "prefill"),
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
        tooltip=_tip(cat, "regrid_method"),
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
        tooltip=_tip(cat, "extrap_method"),
    )


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


# ===========================================================================
# User-provided-file attach flows (grid / CDR forcing / river custom_file):
# shared status-HTML rendering and browser-upload staging, used by the grid
# section, the CDR section, and each river row's custom-file widgets alike.
# ===========================================================================


def _user_file_status_html(file_dict: dict[str, Any] | None) -> str:
    """Status HTML for an attached user-provided-file dict (``{"location",
    "content_hash"}``): the attached filename, a short hash prefix, and the
    persistent host-path warning (this file is host/transport, not shipped
    with the blueprint -- see ``UserProvidedFile``). Empty string when nothing
    is attached.
    """
    if not file_dict:
        return ""
    name = Path(file_dict["location"]).name
    short = file_dict["content_hash"][:12]
    return (
        f"<span style='color:#080'>✓ attached {name} (sha256 {short}…)</span><br>"
        "<span style='color:#b58900'>⚠ This file must exist at this exact path "
        "on the machine where the executor runs.</span>"
    )


def _stage_uploaded_netcdf(filename: str, content: bytes) -> Path:
    """Write browser-uploaded netCDF bytes to a stable server/kernel-filesystem
    location, then treat it exactly like a server-side path Attach.

    ``ipywidgets.FileUpload`` only hands over raw bytes (no filesystem path),
    but every attach flow needs a real path to hash/load/pass through to the
    resolver as ``UserProvidedFile.location``. Lands under
    ``Path.cwd()/"forge_user_files"/<original filename>``; an existing file of
    the same name is overwritten (last upload wins).
    """
    dest_dir = Path.cwd() / "forge_user_files"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    dest.write_bytes(content)
    return dest


class _ForcingEditor:
    """Editor for the forcing spec: initial conditions + per-category forcing items,
    with add/remove. ``gather()`` returns an ``inputs``-shaped dict the resolver
    accepts via ``forcing_inputs=``.
    """

    def __init__(self, W, forcing_inputs: dict[str, Any], on_change):
        self.W = W
        self.on_change = on_change
        fi = forcing_inputs or {}
        # An explicit `None` (emitted by `_sources_to_inputs` for a loaded child
        # blueprint with no IC) means "seed as (none)"; a plain missing key (fresh-
        # wizard startup, which always supplies a real initial_conditions block)
        # keeps the historical GLORYS default.
        if "initial_conditions" in fi and fi["initial_conditions"] is None:
            ic: dict[str, Any] = {}
            _ic_default = _IC_NONE
        else:
            ic = fi.get("initial_conditions") or {}
            _ic_default = _IC_SOURCE_OPTS[0]
        forc = fi.get("forcing", {}) or {}

        # initial conditions
        _ic_name_val = str((ic.get("source") or {}).get("name", _ic_default))
        if _ic_name_val not in _IC_SOURCE_OPTS:
            _ic_name_val = _ic_default
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
        _ic_prefill_val = str(ic.get("prefill") or "")
        if _ic_prefill_val not in _PREFILL_OPTS:
            _ic_prefill_val = ""
        self.ic_prefill = W.Dropdown(
            options=_PREFILL_OPTS,
            value=_ic_prefill_val,
            description="prefill:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "prefill"),
        )
        _ic_regrid_val = str(ic.get("regrid_method") or "")
        if _ic_regrid_val not in _REGRID_OPTS:
            _ic_regrid_val = ""
        self.ic_regrid_method = W.Dropdown(
            options=_REGRID_OPTS,
            value=_ic_regrid_val,
            description="regrid:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "regrid_method"),
        )
        _ic_extrap_val = str(ic.get("extrap_method") or "")
        if _ic_extrap_val not in _EXTRAP_OPTS:
            _ic_extrap_val = ""
        self.ic_extrap_method = W.Dropdown(
            options=_EXTRAP_OPTS,
            value=_ic_extrap_val,
            description="extrap:",
            style={"description_width": "110px"},
            tooltip=_tip("ic", "extrap_method"),
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
            self.ic_prefill,
            self.ic_regrid_method,
            self.ic_extrap_method,
            self.ic_options,
        ):
            _w.observe(lambda _ch: on_change(), names="value")

        # "glorys_layout" only applies to a GLORYS source (item 7). When "(none)"
        # is selected, this domain carries no IC at all -- hide every other IC
        # widget too so the UI doesn't imply they still apply.
        def _sync_ic_layout_visibility(_change=None):
            has_ic = self.ic_name.value != _IC_NONE
            self.ic_layout.layout.display = (
                "" if has_ic and self.ic_name.value == "GLORYS" else "none"
            )
            for w in (
                self.ic_path,
                self.ic_bgc_name,
                self.ic_bgc_clim,
                self.ic_bgc_path,
                self.ic_bgc_interp,
                self.ic_flex_time,
                self.ic_prefill,
                self.ic_regrid_method,
                self.ic_extrap_method,
                self.ic_options,
            ):
                w.layout.display = "" if has_ic else "none"

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
            _add_regrid_widgets(W, w, "surface", item, small)
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
            _add_regrid_widgets(W, w, "boundary", item, small)
        if cat == "tidal":
            w["ntides"] = W.IntText(
                value=int(item.get("ntides") or 0),
                description="ntides:",
                style=small,
                layout=W.Layout(width="130px"),
                tooltip=_tip("tidal", "ntides"),
            )
            _add_regrid_widgets(W, w, "tidal", item, small)
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
            _bgc_src = item.get("bgc_source") or {}
            _bgc_name_val = str(_bgc_src.get("name", "") or "")
            if _bgc_name_val not in _RIVER_BGC_SOURCE_OPTS:
                _bgc_name_val = ""
            w["bgc_source_name"] = W.Dropdown(
                options=_RIVER_BGC_SOURCE_OPTS,
                value=_bgc_name_val,
                description="bgc src:",
                style=small,
                layout=W.Layout(width="150px"),
                tooltip=_tip("river", "bgc_source_name"),
            )
            w["bgc_source_path"] = W.Text(
                value=str(_bgc_src.get("path") or ""),
                description="bgc path:",
                placeholder="(default)",
                style=small,
                layout=W.Layout(width="220px"),
                tooltip=_tip("river", "bgc_source_path"),
            )

            # bgc_source only takes effect when include_bgc is checked (roms-tools
            # silently ignores it otherwise — see RiverForcingItem validation).
            def _sync_river_bgc_visibility(_change=None, ws=w):
                on = bool(ws["include_bgc"].value)
                ws["bgc_source_name"].layout.display = "" if on else "none"
                ws["bgc_source_path"].layout.display = "" if on else "none"

            w["include_bgc"].observe(_sync_river_bgc_visibility, names="value")
            _sync_river_bgc_visibility()

            # RiverSource.CUSTOM_FILE replaces this whole standard-source row
            # (climatology/bgc/coast-snap/edge-buffer/generic path) with a
            # single attach flow -- see _sync_river_custom_visibility below.
            def _sync_river_custom_visibility(_change=None, ws=w):
                is_custom = ws["name"].value == RiverSource.CUSTOM_FILE.value
                for key in (
                    "climatology",
                    "include_bgc",
                    "convert_to_climatology",
                    "coast_snap_buffer_km",
                    "domain_edge_buffer",
                    "path",
                ):
                    ws[key].layout.display = "none" if is_custom else ""
                for key in (
                    "custom_file_path",
                    "custom_file_attach_btn",
                    "custom_file_upload",
                    "custom_file_status",
                ):
                    ws[key].layout.display = "" if is_custom else "none"
                if is_custom:
                    # Keep the bgc widgets hidden regardless of include_bgc --
                    # a custom-file river item carries no bgc_source (see
                    # RiverForcingItem._custom_file_excludes_bgc_source).
                    ws["bgc_source_name"].layout.display = "none"
                    ws["bgc_source_path"].layout.display = "none"
                else:
                    # Restored from custom-file mode (or never in it): let
                    # include_bgc's own sync decide bgc widget visibility again,
                    # rather than unconditionally showing them here.
                    _sync_river_bgc_visibility()

            w["name"].observe(_sync_river_custom_visibility, names="value")
        # Advanced passthrough: raw roms-tools kwargs not (yet) typed above.
        w["options"] = _options_editor(W, item.get("options"))
        remove = W.Button(
            description="✕", layout=W.Layout(width="36px"), tooltip="Remove this item"
        )
        remove.on_click(lambda _b, c=cat, ws=w: self._remove(c, ws))
        for widget in w.values():
            widget.observe(lambda _ch: self.on_change(), names="value")
        w["_remove_btn"] = remove
        if cat == "river":
            # Custom-file attach row (RiverSource.CUSTOM_FILE): added after the
            # generic per-widget on_change wiring above (mirrors _remove_btn) --
            # the Text/Button/FileUpload here are wired to _attach_custom_file
            # explicitly, which calls self.on_change() itself on a successful
            # attach, so they don't need the generic per-keystroke wiring too.
            # `_custom_file` is plain cached state (not a widget), also excluded
            # from that loop and from _row_box below.
            _cf = item.get("custom_file")
            w["_custom_file"] = dict(_cf) if _cf else None
            w["custom_file_path"] = W.Text(
                value=str((_cf or {}).get("location") or ""),
                description="file:",
                placeholder="path to a pre-made river-forcing netCDF",
                style=small,
                layout=W.Layout(width="320px"),
                tooltip=_tip("river", "custom_file"),
            )
            w["custom_file_attach_btn"] = W.Button(
                description="Attach", icon="link", layout=W.Layout(width="90px")
            )
            w["custom_file_upload"] = W.FileUpload(
                accept=".nc", multiple=False, description="…or upload"
            )
            w["custom_file_status"] = W.HTML(_user_file_status_html(w["_custom_file"]))

            def _attach_river_custom_file(path_str: str, ws=w) -> None:
                ws["custom_file_status"].value = "<i>attaching…</i>"
                try:
                    path = Path(path_str).expanduser().resolve()
                    if not path.exists():
                        raise FileNotFoundError(f"river custom file not found: {path}")
                    content_hash = hash_netcdf_contents(path)
                except Exception as exc:
                    ws[
                        "custom_file_status"
                    ].value = (
                        f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
                    )
                    return
                ws["_custom_file"] = {
                    "location": str(path),
                    "content_hash": content_hash,
                }
                ws["custom_file_status"].value = _user_file_status_html(
                    ws["_custom_file"]
                )
                self.on_change()

            def _on_river_custom_file_attach_click(_btn, ws=w) -> None:
                path_str = ws["custom_file_path"].value.strip()
                if not path_str:
                    ws[
                        "custom_file_status"
                    ].value = "<span style='color:#b00'>Enter a path first.</span>"
                    return
                _attach_river_custom_file(path_str)

            def _on_river_custom_file_upload(change, ws=w) -> None:
                items = change["new"]
                if not items:
                    return
                up_item = (
                    items[0]
                    if isinstance(items, (list, tuple))
                    else next(iter(items.values()))
                )
                dest = _stage_uploaded_netcdf(
                    up_item["name"], bytes(up_item["content"])
                )
                ws["custom_file_path"].value = str(dest)
                _attach_river_custom_file(str(dest))

            w["custom_file_attach_btn"].on_click(_on_river_custom_file_attach_click)
            w["custom_file_upload"].observe(_on_river_custom_file_upload, names="value")
            _sync_river_custom_visibility()
        self._apply_row_visibility(w)
        return w

    def _row_box(self, w):
        # `type` (when present) drives the other options in the row, so show it first.
        keys = [k for k in w if k not in ("_remove_btn", "_custom_file")]
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

    def clear_category(self, cat: str):
        """Remove all rows for a forcing category (e.g. ``"boundary"`` for a
        child/nested grid, which receives boundaries from the parent's
        nesting.nc extraction instead of reanalysis boundary forcing).
        """
        self._rows[cat] = []
        self._render(cat)
        self.on_change()

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
        if cat == "river" and w["name"].value == RiverSource.CUSTOM_FILE.value:
            # A custom-file river item bypasses roms-tools' RiverForcing entirely
            # (the executor stages the file directly -- see
            # RiverForcingItem.custom_file) -- none of the standard-source fields
            # (climatology/include_bgc/convert_to_climatology/coast_snap_buffer_km/
            # domain_edge_buffer/bgc_source/options) apply, and the schema
            # forbids most of them from being paired with a source path.
            item: dict[str, Any] = {"source": {"name": w["name"].value}}
            if w.get("_custom_file"):
                item["custom_file"] = dict(w["_custom_file"])
            return item
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
        # Shared surface/boundary/tidal regrid/interp knobs: only emit non-default
        # values to keep specs clean.
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
            if "bgc_source_name" in w and w["bgc_source_name"].value:
                bgc_src: dict[str, Any] = {"name": w["bgc_source_name"].value}
                if (
                    "bgc_source_path" in w and w["bgc_source_path"].value.strip()
                ):  # blank = derive default path
                    bgc_src["path"] = w["bgc_source_path"].value.strip()
                item["bgc_source"] = bgc_src
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
        forcing = {
            cat: [self._gather_item(cat, w) for w in self._rows[cat]]
            for cat in _FORCING_CATEGORIES
        }
        if self.ic_name.value == _IC_NONE:
            # No IC for this domain -- omit the key entirely (the resolver
            # requires a hard error for a non-child domain with no IC, and
            # treats an omitted/sourceless block as "inherit from parent" for
            # a child domain).
            return {"forcing": forcing}

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
        if self.ic_prefill.value:  # Dropdown: "" = leave unset
            ic["prefill"] = self.ic_prefill.value
        if self.ic_regrid_method.value:
            ic["regrid_method"] = self.ic_regrid_method.value
        if self.ic_extrap_method.value:
            ic["extrap_method"] = self.ic_extrap_method.value
        ic_opts = _parse_options(self.ic_options.value)
        if ic_opts:
            ic["options"] = ic_opts
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
                W.HBox([self.ic_prefill, self.ic_regrid_method, self.ic_extrap_method]),
                self.ic_options,
            ]
        )
        panes = [ic_box] + [self._containers[c] for c in _FORCING_CATEGORIES]
        acc = W.Accordion(children=panes, selected_index=None)
        for i, title in enumerate(["initial_conditions", *_FORCING_CATEGORIES]):
            acc.set_title(i, title)
        return acc


# Preselected in the Model dropdown when present in the catalog (falls back to
# the first catalog model otherwise).
_DEFAULT_MODEL = "pio-dev"

# Preselected in the Output dropdown when present in the catalog (falls back to
# the first catalog spec otherwise). 'daily-restarts' conforms to the
# ucla-roms >= 0.5.0 check_output_divides_rst precheck; 'standard' is kept
# unchanged for blueprints that reference it.
_DEFAULT_OUTPUT_SPEC = "daily-restarts"

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
    """Return the default catalog stack for spec discovery.

    A layered stack: the user's writable catalog layer (``~/cstar-forge-data/
    catalog`` by default, or ``CSTAR_FORGE_CATALOG``) over the read-only
    bundled in-repo catalog. Reads resolve top-first; writes (``register_*``)
    land in the user layer.
    """
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


_STREAM_READ_SIZE = 2**16  # bytes per subprocess read; also bounds the flush below
# A run whose child never emits a newline for a long stretch -- classically a
# ``\r``-redrawn progress bar (git clone, tqdm, download progress) -- would grow an
# unbounded "line". Flush an unterminated run once it reaches this so memory stays
# bounded and the log still updates. (This is also what a line-oriented reader could
# not survive: asyncio's StreamReader.readline() raises ValueError, "Separator is not
# found, and chunk exceed the limit", at its 64 KiB default -- the bug this avoids.)
_STREAM_MAX_LINE = 2**16


def _drain_stream_buffer(
    buf: bytes, *, at_eof: bool = False
) -> tuple[list[str], bytes]:
    r"""Split accumulated subprocess bytes into display lines, returning
    ``(lines, remainder)``.

    Segments are cut on ``\r\n``, ``\r`` or ``\n`` and each terminator is normalised
    to a single trailing ``\n``, so ``\r``-only progress redraws surface as successive
    log lines instead of one ever-growing line. A lone trailing ``\r`` is held back in
    the remainder (unless at EOF) so a ``\r\n`` split across two reads is not mistaken
    for a bare ``\r`` plus a spurious blank line. When not at EOF, an unterminated
    remainder that reaches ``_STREAM_MAX_LINE`` is emitted as a partial line to keep
    memory bounded; at EOF any remainder is flushed.
    """
    held = b""
    if not at_eof and buf.endswith(b"\r"):
        buf, held = buf[:-1], b"\r"
    tokens = re.split(rb"(\r\n|\r|\n)", buf)
    remainder = tokens.pop() + held
    lines = [
        tokens[i].decode(errors="replace") + "\n" for i in range(0, len(tokens), 2)
    ]
    if at_eof:
        if remainder:
            lines.append(remainder.decode(errors="replace"))
        remainder = b""
    elif len(remainder) >= _STREAM_MAX_LINE:
        lines.append(remainder.decode(errors="replace"))
        remainder = b""
    return lines, remainder


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

        # --- load / import an existing forge_blueprint.yaml ---
        self.load_path = W.Text(
            value="",
            placeholder="path to forge_blueprint.yaml",
            description="Load file:",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
        )
        self.load_btn = W.Button(description="Load", icon="upload")
        self.upload = W.FileUpload(
            accept=".yml,.yaml", multiple=False, description="…or upload"
        )
        self.load_status = W.HTML("")

        # --- spec selectors ---
        self.model_dd = W.Dropdown(
            options=self._dd_options(models, "model"),
            description="Model:",
            value=(
                _DEFAULT_MODEL
                if _DEFAULT_MODEL in models
                else (models[0] if models else None)
            ),
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
            options=self._dd_options(domains, "domain", prefix=["<custom>"]),
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
        # --- domain-derived properties: v_sponge + (the above) open boundaries.
        # Both auto-derive from the grid unless touched by a manual edit or
        # restored from a saved DomainSpec -- see _on_derive_domain_properties /
        # _v_sponge_touched / _boundaries_touched / _boundaries_derived.
        self.v_sponge = W.FloatText(
            value=0.0,
            description="v_sponge:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip="Sponge-layer viscosity. Auto-derived from grid spacing "
            "(spacing / 10) unless you edit it or it was saved into a DomainSpec.",
        )
        self.derive_btn = W.Button(
            description="Derive from grid",
            icon="refresh",
            layout=W.Layout(width="160px"),
            tooltip="Build the grid and set any untouched v_sponge/open-boundary "
            "values from it (sponge = spacing/10; boundaries from the land mask).",
        )
        self.derive_status = W.HTML("")
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
        self.auto_tiling_chk = W.Checkbox(
            value=False,
            description="auto tiling",
            indent=False,
            tooltip="Pick the MPI tiling at runtime from the land mask "
            "(ucla-roms >= 0.5.0 MPI_MASKING). Requires PIO -- forces it on and "
            "locks it. In development.",
        )
        self.n_cores = W.IntText(
            value=1,
            description="n_cores:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px", display="none"),
            tooltip="Total MPI ranks; the tiling itself is chosen at runtime "
            "from the land mask when auto tiling is on.",
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
        self.marbl_ref = W.Text(
            value="",  # populated from the selected Model's pinned default below
            description="MARBL ref:",
            style={"description_width": "120px"},
            layout=W.Layout(width="260px"),
            placeholder="commit / tag / branch",
            tooltip="MARBL checkout target (commit hash, tag, or branch). "
            "Prefilled from the selected Model's pinned default; edit to override. "
            'Only used when BGC mode is "marbl".',
        )

        # --- nesting (optional child grid) ---
        self.nest_enable = W.Checkbox(
            value=False,
            description="This grid is a parent (enter child grid info below).",
            indent=False,
            tooltip=_tip("nesting", "nest_enable"),
        )
        self.nest_help = W.HTML(
            "<div style='font-style:italic;color:#777;margin:0 0 6px'>"
            "This will enable the extract_data module and output boundary info "
            "for your child grid.</div>"
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

        # --- parent (optional: this grid is a child nested inside a parent) ---
        self.parent_enable = W.Checkbox(
            value=False,
            description="This is a child grid (enter parent grid info below).",
            indent=False,
            tooltip=_tip("nesting", "parent_enable"),
        )
        self.parent_help = W.HTML(
            "<div style='font-style:italic;color:#777;margin:0 0 6px'>"
            "This will align the mask and topography with the parent grid. It "
            "will also disable boundary tides, switch sponge ub_tune to False, "
            "and skip boundary forcing generation.</div>"
        )
        self.parent_domain_dd = W.Dropdown(
            options=["<custom>", *domains],
            description="Parent from:",
            value="<custom>",
            style={"description_width": "110px"},
            tooltip=_tip("nesting", "parent_domain_dd"),
        )
        self.parent_w: dict[str, Any] = {}
        for k in _GRID_INT:
            self.parent_w[k] = W.IntText(
                value=int(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (parent/outer grid)",
            )
        for k in _GRID_FLOAT + _SCOORD:
            self.parent_w[k] = W.FloatText(
                value=float(_DEFAULT_GRID[k]),
                description=f"{k}:",
                style={"description_width": "90px"},
                layout=W.Layout(width="200px"),
                tooltip=_tip("grid", k) + " (parent/outer grid)",
            )
        # --- parent plot (this grid's boundary within its parent) ---
        self.parent_plot_btn = W.Button(
            description="Refresh plot",
            icon="refresh",
            tooltip="Build the parent grid and this grid from current settings and "
            "render both via plot_nesting (parent+this-grid boundary overlay).",
        )
        self.parent_plot_status = W.HTML("")
        self.parent_plot_img = W.Image(
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
            options=["ETOPO5", "SRTM15", "EMOD"],
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

        # --- user-provided grid file (attach a pre-made grid netCDF instead of
        # generating one from the kwargs above) ---
        # `_grid_file` = the cached {"location","content_hash"} dict emitted via
        # grid_file=; `_grid_file_grid` = the loaded roms_tools.Grid, cached so
        # _rebuild()/_gather() never reload or rehash the file (see
        # _finish_grid_file_attach). Both None when detached.
        self._grid_file: dict[str, Any] | None = None
        self._grid_file_grid: Any | None = None
        # Snapshot of grid_w/scoord_chk taken the moment an attach first
        # overwrites them with the loaded file's own values (see
        # _finish_grid_file_attach) -- restored by _on_grid_file_detach so the
        # Detach button gives back the user's own pre-attach geometry instead
        # of leaving the (unlocked, but now-wrong) file's values sitting there.
        # None means "nothing to restore" (never attached, or already restored).
        self._grid_widgets_snapshot: dict[str, Any] | None = None
        self.grid_file_path = W.Text(
            value="",
            description="Grid file:",
            placeholder="path to a pre-made grid netCDF",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("grid", "grid_file"),
        )
        self.grid_file_attach_btn = W.Button(description="Attach", icon="link")
        self.grid_file_detach_btn = W.Button(description="Detach", icon="unlink")
        self.grid_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.grid_file_status = W.HTML("")

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
        # --- forcing spec (ForcingSpec selection + add/remove/edit editor) ---
        # A ForcingSpec must always be explicitly selected -- ModelSpec no longer
        # embeds a default forcing.
        _forcing_names = list(self.catalog.forcing_names)
        self.forcing_dd = W.Dropdown(
            options=self._dd_options(_forcing_names, "forcing"),
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

        # --- CDR forcing: user-provided pre-made netCDF (mutually exclusive with
        # the uploaded YAML above -- attaching one clears the other, see
        # _attach_cdr_file_from_path / _on_cdr_upload). ---
        self._cdr_forcing_file: dict[str, Any] | None = None
        self.cdr_file_path = W.Text(
            value="",
            description="CDR file:",
            placeholder="path to a pre-made CDR-forcing netCDF",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("cdr", "cdr_file"),
        )
        self.cdr_file_attach_btn = W.Button(description="Attach", icon="link")
        self.cdr_file_clear_btn = W.Button(description="Clear", icon="times")
        self.cdr_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.cdr_file_status = W.HTML("")

        # --- output settings spec (OutputSpec selection) ---
        # The output sections themselves are edited in the Advanced settings accordion;
        # this dropdown selects a named OutputSpec that seeds those sections. An
        # OutputSpec must always be explicitly selected -- ModelSpec no longer embeds
        # default output settings.
        _output_names = list(self.catalog.output_names)
        self.output_dd = W.Dropdown(
            options=self._dd_options(_output_names, "output"),
            # Prefer the bundled default explicitly -- output_names is sorted
            # alphabetically, so relying on position would silently change the
            # default whenever a new spec sorts first.
            value=(
                _DEFAULT_OUTPUT_SPEC
                if _DEFAULT_OUTPUT_SPEC in _output_names
                else (_output_names[0] if _output_names else None)
            ),
            description="Output:",
            style={"description_width": "110px"},
            tooltip=_tip("output", "output_dd"),
        )

        # --- advanced settings editor (built lazily on first rebuild) ---
        self.editor: _SettingsEditor | None = None
        self._editor_model: str | None = None
        # The RunTimeSettings variant the editor was last built against -- rebuilt
        # not only on a model switch but also when the effective ucla-roms ref
        # crosses a schema boundary (e.g. editing the roms_ref override across the
        # 0.5.0 line while keeping the same ModelSpec selected).
        self._editor_settings_cls: type[BaseModel] | None = None
        self.editor_box = W.VBox([])  # placeholder; filled with the editor's accordion
        # sparse manual overrides layer: (section, field|None) -> value
        self._overrides: dict[Any, Any] = {}
        self._syncing = False  # True while pushing composed values into editor widgets
        # Reentrancy depth for _suspend()/_Suspender -- lets a programmatic backfill
        # (e.g. _populate_grid_widgets_from_grid) nest inside an already-suspended
        # block (e.g. _populate_from's outer _suspend()) without the inner
        # context's __exit__ prematurely clearing the outer one's suspension.
        self._suspend_depth = 0
        # snapshot of the domain-defining widgets at the last catalog Domain pick;
        # compared in _rebuild() to detect a deviation (composition.domain.modified).
        # None means no catalog domain has been picked yet (or domain_dd == "<custom>").
        self._domain_seed: dict[str, Any] | None = None
        # Domain-derived properties (v_sponge, open boundaries): "touched" means a
        # manual edit (or a value restored from a loaded blueprint/DomainSpec) has
        # made this the user's authoritative value -- nothing auto-overwrites it
        # again. v_sponge is cheap (pure arithmetic on grid spacing) and is kept
        # live-derived in _rebuild() when untouched, so it needs no separate
        # "derived" flag. Boundaries need a grid build (mask-based), so
        # _boundaries_derived tracks whether that's happened since the last
        # grid-affecting edit -- see _on_grid_kwarg_change/_on_derive_domain_properties.
        self._v_sponge_touched = False
        self._boundaries_touched = False
        self._boundaries_derived = False

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
            value="forge_blueprint.yaml",
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

        # --- save modified specs to catalog (name + button + status per spec) ---
        def _spec_save_row(placeholder):
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
            _spec_save_row("(new OutputSpec name)")
        )
        self.save_model_name, self.save_model_btn, self.save_model_status = (
            _spec_save_row("(new ModelSpec name)")
        )
        self.save_domain_name, self.save_domain_btn, self.save_domain_status = (
            _spec_save_row("(new DomainSpec name)")
        )
        self.save_forcing_name, self.save_forcing_btn, self.save_forcing_status = (
            _spec_save_row("(new ForcingSpec name)")
        )

        # --- run (invokes the C-Star CLI on the just-saved blueprint) ---
        from cstar_forge.config import system as _detected_system

        self.run_warning = W.HTML(
            "<b style='color:#b58900'>⚠ Processing a blueprint can use substantial "
            "memory and CPU depending on grid size.</b> Run this from a compute node "
            "(or another host) with resources appropriate for your domain — this is "
            f"not checked automatically. Detected host: <code>{_detected_system}</code>."
        )
        self.run_later_note = W.HTML(
            "<span style='color:#666'>ℹ To run this later, or on a different "
            "machine, save the blueprint above and then (from the "
            "<code>cstar-forge</code> environment) call: "
            "<code>cstar blueprint run &lt;path/to/forge_blueprint.yaml&gt;</code>. "
            "</span>"
        )
        self.run_btn = W.Button(description="Run", icon="play")
        self.run_status = W.HTML("")

        # --- workplan export (two-step C-Star workplan: forge -> roms_marbl) ---
        self.workplan_note = W.HTML(
            "<span style='color:#b00'>⚠ Experimental — workplan export does not "
            "work yet; the saved workplan is not currently runnable.</span><br>"
            "<span style='color:#666'>ℹ Saves the blueprint plus a two-step C-Star "
            "workplan: step <code>forge</code> generates the ROMS-MARBL inputs and "
            "blueprint, step <code>roms_marbl</code> runs the simulation from that "
            "generated (deferred) blueprint. The workplan is saved only — run it "
            "yourself with the printed command.</span>"
        )
        self.workplan_btn = W.Button(description="Save workplan", icon="sitemap")
        self.workplan_status = W.HTML("")

        self.run_output = W.Output(
            layout=W.Layout(
                border="1px solid #ccc",
                padding="6px",
                max_height="380px",
                overflow="auto",
            )
        )

        self.roms_ref.value = self._model_default_roms_ref()
        self.marbl_ref.value = self._model_default_marbl_ref()
        self.bgc_dd.value = self._model_default_bgc_mode()
        self.use_pio_chk.value = self._model_default_use_pio()
        self._sync_marbl_ref_visibility()
        self._sync_auto_tiling()
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
        self.workplan_btn.on_click(self._on_save_workplan)
        self.load_btn.on_click(self._on_load_path)
        self.upload.observe(self._on_upload, names="value")
        self.cdr_upload.observe(self._on_cdr_upload, names="value")
        self.cdr_clear_btn.on_click(self._on_cdr_clear)
        self.grid_file_attach_btn.on_click(self._on_grid_file_attach)
        self.grid_file_detach_btn.on_click(self._on_grid_file_detach)
        self.grid_file_upload.observe(self._on_grid_file_upload, names="value")
        self.cdr_file_attach_btn.on_click(self._on_cdr_file_attach)
        self.cdr_file_clear_btn.on_click(self._on_cdr_file_clear)
        self.cdr_file_upload.observe(self._on_cdr_file_upload, names="value")
        self.model_dd.observe(self._on_model_change, names="value")
        self.bgc_dd.observe(self._sync_marbl_ref_visibility, names="value")
        self.auto_tiling_chk.observe(self._sync_auto_tiling, names="value")
        self.nest_domain_dd.observe(self._on_nest_domain, names="value")
        self.parent_domain_dd.observe(self._on_parent_domain, names="value")
        self.parent_plot_btn.on_click(self._on_parent_plot)
        self.parent_enable.observe(self._on_parent_toggle, names="value")
        self.output_dd.observe(self._on_output_spec, names="value")
        self.derive_btn.on_click(self._on_derive_domain_properties)
        watched = [
            self.grid_name,
            self.scoord_chk,
            self.npx,
            self.npy,
            self.use_pio_chk,
            self.auto_tiling_chk,
            self.n_cores,
            self.bgc_dd,
            self.roms_ref,
            self.marbl_ref,
            self.start,
            self.end,
            self.model_ref_date,
            self.description,
            self.dt,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.nest_period,
            self.nest_pressure_fluxes,
            self.parent_enable,
            *self.child_w.values(),
            *self.parent_w.values(),
        ]
        for w in watched:
            w.observe(self._rebuild, names="value")
        # Grid-geometry/mask-affecting widgets: a change invalidates any prior
        # mask-derived boundaries (a stale mask must never be silently reused),
        # in addition to the plain rebuild every other watched widget gets.
        for w in (
            *self.grid_w.values(),
            self.hmin,
            self.close_narrow_chk,
            self.mask_shapefile,
        ):
            w.observe(self._on_grid_kwarg_change, names="value")
        # Domain-derived properties: a manual edit "touches" the property so
        # nothing auto-overwrites it again (mirrors _on_editor_edit for the
        # advanced-settings accordion).
        for w in self.bnd.values():
            w.observe(self._on_boundary_edit, names="value")
        self.v_sponge.observe(self._on_v_sponge_edit, names="value")

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

    def _dd_options(
        self, names: list[str], kind: str, *, prefix: list[str] | None = None
    ) -> list:
        """Badge dropdown ``options`` with their source layer when it isn't the
        top (writable) one -- ipywidgets-homogeneous.

        ipywidgets' ``Dropdown`` requires ``options`` to be either ALL bare
        values or ALL ``(label, value)`` 2-tuples: ``_make_options`` only takes
        the "pairs" branch when *every* entry is a 2-tuple, so a mix silently
        falls through to treating each element (tuples included) as a literal
        value, and ``dd.value`` ends up holding a raw ``(label, value)`` tuple
        instead of the bare name. So: if no *names* entry needs a badge, this
        returns plain names unchanged (matching pre-layering behavior, safe to
        mix with a plain sentinel like domain_dd's ``"<custom>"``); otherwise
        *every* entry -- including any *prefix* sentinels -- is emitted as an
        explicit ``(label, value)`` tuple so the whole list stays homogeneous.
        ``dd.value`` is always the bare name/sentinel either way.

        A ``KeyError`` from ``entry_source`` (name absent, shouldn't happen for
        names drawn from the same catalog) is treated as "no badge".
        """
        prefix = prefix or []
        entry_source = getattr(self.catalog, "entry_source", None)
        top = getattr(self.catalog, "top", None)
        top_label = top.label if top is not None else None

        badges: dict[str, str] = {}
        if entry_source is not None:
            for name in names:
                try:
                    source = entry_source(kind, name)
                except KeyError:
                    continue
                if source != top_label:
                    badges[name] = source

        if not badges:
            return [*prefix, *names]

        options: list[Any] = [(p, p) for p in prefix]
        for name in names:
            label = f"{name} ({badges[name]})" if name in badges else name
            options.append((label, name))
        return options

    @staticmethod
    def _dd_values(dd) -> list:
        """Bare values of a dropdown's ``options``, whether badged
        ``(label, value)`` tuples (see ``_dd_options``) or plain strings
        (ipywidgets' label == value shorthand). Use this instead of reading
        ``dd.options`` directly for membership tests / indexing.
        """
        return [o[1] if isinstance(o, tuple) else o for o in dd.options]

    def _default_blueprint_path(self, name: str) -> str:
        """Default "Save to:" path for a blueprint named *name*.

        Prefers the active catalog's ``blueprints/`` directory so a save lands
        where the wizard's other catalog-aware specs look; falls back to a
        bare filename (CWD-relative) when the catalog isn't a local filesystem
        (e.g. loaded from a GitHub/http URL) and so isn't writable.
        """
        fname = f"{name}.forge_blueprint.yaml"
        cat = getattr(self, "catalog", None)
        try:
            # A read-only catalog (e.g. the bundled one loaded as a single
            # store) must not be offered as a save destination.
            if (
                cat is not None
                and getattr(cat, "_is_local", False)
                and not getattr(cat, "read_only", False)
            ):
                return str(cat.roms_marbl_blueprints_dir / fname)
        except Exception:
            pass
        return fname

    def _on_grid_kwarg_change(self, _change):
        # A geometry/mask-affecting edit invalidates any prior mask-derived
        # boundaries -- a stale mask must never be silently reused. Only the
        # "derived" freshness flag resets; an already-touched (manually set or
        # loaded) value is never overwritten by this. Clearing derive_status lets
        # _rebuild's "not derived yet" warning reappear instead of leaving a
        # stale "✓ derived" message visible against the now-invalidated mask.
        if not getattr(self, "_suspended", False):
            self._boundaries_derived = False
            self.derive_status.value = ""
        self._rebuild()

    # ---- user-provided grid file ----------------------------------------------
    def _set_grid_widgets_locked(self, locked: bool) -> None:
        """Disable/re-enable every grid-generation widget a user-provided grid
        file makes meaningless: geometry/vertical kwargs, mask/topography, and
        nesting (custom grid + nesting is unsupported -- see
        ``Domain._grid_file_excludes_generation_geometry``). Open boundaries and
        v_sponge stay editable/derivable -- both work fine off a loaded grid.
        """
        for w in (
            *self.grid_w.values(),
            self.scoord_chk,
            self.hmin,
            self.close_narrow_chk,
            self.mask_shapefile,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.parent_enable,
        ):
            w.disabled = locked

    def _populate_grid_widgets_from_grid(self, grid: Any) -> None:
        """Display a loaded grid file's own attributes on the (now-locked) grid
        widgets, skipping any that are ``None`` (a hand-made file may lack
        ``size_x``/``size_y`` -- see ``build_forge_blueprint``'s grid_file
        docstring). Purely cosmetic here -- ``_gather()`` never reads these
        widget values while a grid file is attached (``grid_kwargs`` is ``{}``).
        """
        with self._suspend():
            for key in _GRID_INT + _GRID_FLOAT + _SCOORD:
                val = getattr(grid, key, None)
                if val is not None:
                    self.grid_w[key].value = val
            if all(
                getattr(grid, a, None) is not None for a in ("theta_s", "theta_b", "hc")
            ):
                self.scoord_chk.value = True

    def _finish_grid_file_attach(
        self, file_dict: dict[str, Any], grid: Any, *, snapshot: bool = True
    ) -> None:
        """Common tail of a successful (or reused-hash) grid-file attach: cache
        state, populate + lock widgets, invalidate any prior mask-derived
        boundaries (a new grid means a new mask -- mirrors
        ``_on_grid_kwarg_change``), and show the attach status.

        ``snapshot=False`` skips capturing the pre-attach widget snapshot -- used
        by ``_reattach_grid_file`` (blueprint load-back), where the widgets hold
        leftover values from whatever was on screen before the load, not a
        user-authored geometry worth restoring on Detach.
        """
        if snapshot and self._grid_widgets_snapshot is None:
            # Capture the user's own pre-attach geometry exactly once per
            # detached->attached transition -- a second attach while already
            # attached (e.g. picking a different file without detaching first)
            # must not overwrite the ORIGINAL values with the first file's.
            self._grid_widgets_snapshot = {
                **{k: w.value for k, w in self.grid_w.items()},
                "scoord_chk": self.scoord_chk.value,
            }
        self._grid_file = file_dict
        self._grid_file_grid = grid
        self._populate_grid_widgets_from_grid(grid)
        self._set_grid_widgets_locked(True)
        self._boundaries_derived = False
        self.derive_status.value = ""
        self.grid_file_status.value = _user_file_status_html(file_dict)

    def _attach_grid_file_from_path(self, path_str: str) -> None:
        self.grid_file_status.value = "<i>attaching…</i>"
        try:
            path = Path(path_str).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"grid file not found: {path}")
            import roms_tools as rt

            grid = rt.Grid(filename=str(path))
            content_hash = hash_netcdf_contents(path)
        except Exception as exc:
            self.grid_file_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        self._finish_grid_file_attach(
            {"location": str(path), "content_hash": content_hash}, grid
        )
        self._rebuild()

    def _on_grid_file_attach(self, _btn):
        path_str = self.grid_file_path.value.strip()
        if not path_str:
            self.grid_file_status.value = (
                "<span style='color:#b00'>Enter a path first.</span>"
            )
            return
        self._attach_grid_file_from_path(path_str)

    def _on_grid_file_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        self.grid_file_path.value = str(dest)
        self._attach_grid_file_from_path(str(dest))

    def _detach_grid_file(self) -> None:
        """Pure state reset (no _rebuild(), no widget-value restore) -- shared
        by the Detach button and _on_domain/_populate_from (a catalog Domain
        pick or a freshly-loaded grid-kwargs blueprint both set grid_w to their
        OWN authoritative values immediately after calling this, which must
        win outright -- restoring the pre-attach snapshot here would clobber
        them with stale values from a since-superseded attach). Only
        _on_grid_file_detach (the explicit user action, with nothing else
        about to overwrite grid_w) restores the snapshot -- see there.
        """
        self._grid_file = None
        self._grid_file_grid = None
        self._grid_widgets_snapshot = None
        self._set_grid_widgets_locked(False)
        self._boundaries_derived = False
        self.derive_status.value = ""
        self.grid_file_path.value = ""
        self.grid_file_upload.value = ()
        self.grid_file_status.value = ""

    def _on_grid_file_detach(self, _btn):
        snapshot = (
            self._grid_widgets_snapshot
        )  # read before _detach_grid_file clears it
        self._detach_grid_file()
        if snapshot is not None:
            with self._suspend():
                for k, v in snapshot.items():
                    if k == "scoord_chk":
                        self.scoord_chk.value = v
                    elif k in self.grid_w:
                        self.grid_w[k].value = v
        self._rebuild()

    def _reattach_grid_file(self, file_obj: Any) -> None:
        """``_populate_from``'s grid_file restore: reload the grid but REUSE the
        blueprint's recorded content_hash (never recompute it on load -- mirrors
        the CDR-file restore). On a reload failure, keep the grid_file dict and
        widgets locked (rather than silently falling back to the generic/default
        grid_kwargs, which would gather a completely different blueprint) and
        surface the error -- the resolver will raise the same "file not found"
        on the next _gather()/_rebuild(), which is the loud failure this must
        produce instead of a silent wrong answer.
        """
        file_dict = {
            "location": file_obj.location,
            "content_hash": file_obj.content_hash,
        }
        self._grid_file = file_dict
        # Whatever sits in the grid widgets right now predates this load and is
        # not a geometry worth restoring on Detach -- drop any stale snapshot.
        self._grid_widgets_snapshot = None
        self.grid_file_path.value = file_dict["location"]
        self._set_grid_widgets_locked(True)
        try:
            path = Path(file_dict["location"]).expanduser()
            if not path.exists():
                raise FileNotFoundError(f"grid file not found: {path}")
            import roms_tools as rt

            grid = rt.Grid(filename=str(path))
        except Exception as exc:
            self._grid_file_grid = None
            self._boundaries_derived = False
            self.derive_status.value = ""
            self.grid_file_status.value = (
                f"<span style='color:#b00'>⚠ could not re-attach grid_file: "
                f"{type(exc).__name__}: {exc}</span>"
            )
            return
        self._finish_grid_file_attach(file_dict, grid, snapshot=False)

    def _on_boundary_edit(self, _change):
        # A programmatic backfill (see _apply_grid_derived_properties, _on_domain,
        # _populate_from) happens under _suspend() -- only a real user edit should
        # "touch" the property against further auto-derivation.
        if getattr(self, "_suspended", False):
            return
        self._boundaries_touched = True
        self.derive_status.value = ""
        self._rebuild()

    def _on_v_sponge_edit(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._v_sponge_touched = True
        self._rebuild()

    def _sync_marbl_ref_visibility(self, _change=None):
        # MARBL ref is inert without MARBL, so hide it (value is kept, not cleared)
        self.marbl_ref.layout.display = "" if self.bgc_dd.value == "marbl" else "none"

    def _sync_auto_tiling(self, _change=None):
        # Auto tiling picks n_procs_x/y at runtime from the land mask, so those
        # boxes become meaningless (disabled) and n_cores takes over; it also
        # requires PIO, so use_pio is forced on and locked while it's active.
        on = self.auto_tiling_chk.value
        if on and _change is not None and _change.get("old") is False:
            # Real off->on toggle (user click, or a load path flipping the
            # checkbox): seed n_cores from the explicit grid already entered,
            # so the user doesn't multiply by hand. Load paths restore a saved
            # n_cores AFTER setting the checkbox, so this seed never clobbers
            # it; direct _sync_auto_tiling() calls (_change=None) never seed.
            self.n_cores.value = int(self.npx.value) * int(self.npy.value)
        self.npx.disabled = on
        self.npy.disabled = on
        self.n_cores.layout.display = "" if on else "none"
        if on:
            self.use_pio_chk.value = True
        self.use_pio_chk.disabled = on

    def _on_model_change(self, _change):
        # a different model has different defaults -> existing overrides no longer apply.
        # Forcing/Output are independent catalog dimensions from the model (a ForcingSpec/
        # OutputSpec doesn't reference a model), so switching models never touches them.
        if getattr(self, "_suspended", False):
            return
        self._overrides = {}
        self.roms_ref.value = self._model_default_roms_ref()
        self.marbl_ref.value = self._model_default_marbl_ref()
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

    def _on_parent_domain(self, _change):
        """Prefill the parent grid from a selected DomainSpec (and enable it)."""
        name = self.parent_domain_dd.value
        if name == "<custom>":
            return
        gk = self.catalog.domain_data(name).get("grid_kwargs", {}) or {}
        with self._suspend():
            self.parent_enable.value = True
            for k, w in self.parent_w.items():
                if k in gk:
                    w.value = gk[k]
        self._clear_boundary_forcing()
        self._clear_initial_conditions()
        self._rebuild()
        self._on_parent_plot(None)

    def _on_parent_toggle(self, change):
        """Enabling a parent clears boundary forcing: a child grid receives its
        boundary values from the parent's nesting.nc extraction, not reanalysis
        boundary forcing (open-boundary edge flags are left untouched -- the
        edges stay open, just fed differently). It also clears IC to "(none)"
        as a *default* -- unlike boundary, IC is widget-state-authoritative, so
        the user can undo this by re-selecting a source.

        Disabling a parent restores the IC default (if it's still "(none)")
        so a non-child domain isn't stranded on the resolver's hard error for
        a missing IC -- a user who had manually re-added an explicit IC while
        parented is left alone.
        """
        if getattr(self, "_suspended", False):
            return
        if change["new"]:
            self._clear_boundary_forcing()
            self._clear_initial_conditions()
        else:
            self._restore_initial_conditions_default()

    def _clear_boundary_forcing(self):
        """Remove any boundary-forcing rows from the forcing editor (UX mirror of
        the durable clear in ``_gather()``, which is the source of truth).
        """
        if getattr(self, "_forcing_editor", None) is not None:
            self._forcing_editor.clear_category("boundary")

    def _clear_initial_conditions(self):
        """Default a child domain's IC dropdown to "(none)" -- a child receives
        state from the parent's nesting extraction, so IC is optional. Unlike
        boundary forcing, this is only a default: the user can re-select a
        source (e.g. GLORYS) to re-add an explicit IC for the child.
        """
        if getattr(self, "_forcing_editor", None) is not None:
            self._forcing_editor.ic_name.value = _IC_NONE

    def _restore_initial_conditions_default(self):
        """Undo ``_clear_initial_conditions``'s default when a parent is turned
        back off -- a non-child domain has no other source of state, so leaving
        IC at "(none)" would strand the user on the resolver's hard error. Only
        resets it when it's still at the "(none)" default; a user who manually
        re-added an explicit IC while parented is left alone.
        """
        if (
            getattr(self, "_forcing_editor", None) is not None
            and self._forcing_editor.ic_name.value == _IC_NONE
        ):
            self._forcing_editor.ic_name.value = _IC_SOURCE_OPTS[0]

    # ---- forcing spec -------------------------------------------------------
    def _model_spec_declared(self) -> dict[str, Any]:
        """The selected ModelSpec's declared ``roms_ref``/``marbl_ref``/``bgc_mode``/
        ``use_pio``.

        Single re-parse of ``model.yaml`` backing ``_model_default_*`` below and
        the spec-deviation check in ``_rebuild`` -- both need the same
        catalog-declared values to compare live widget state against.
        """
        try:
            data = load_model_spec_data(self.catalog.model_dir(self.model_dd.value))
            model = data["model"]
            roms = model.get("code", {}).get("roms", {}) or {}
            marbl = model.get("code", {}).get("marbl", {}) or {}
            return {
                "roms_ref": roms.get("commit") or roms.get("branch") or "",
                "marbl_ref": marbl.get("commit") or marbl.get("branch") or "",
                "bgc_mode": model.get("bgc_mode", "marbl"),
                "use_pio": bool(model.get("use_pio", False)),
            }
        except Exception:
            return {
                "roms_ref": "",
                "marbl_ref": "",
                "bgc_mode": "marbl",
                "use_pio": False,
            }

    def _model_default_roms_ref(self) -> str:
        """The selected model's pinned ucla-roms checkout target (commit or branch)."""
        return self._model_spec_declared()["roms_ref"]

    def _model_default_marbl_ref(self) -> str:
        """The selected model's pinned MARBL checkout target (commit or branch)."""
        return self._model_spec_declared()["marbl_ref"]

    def _model_default_bgc_mode(self) -> str:
        """The selected model's ModelSpec-declared bgc_mode (prepopulates self.bgc_dd)."""
        return self._model_spec_declared()["bgc_mode"]

    def _model_default_use_pio(self) -> bool:
        """The selected model's ModelSpec-declared use_pio (prepopulates self.use_pio_chk)."""
        return self._model_spec_declared()["use_pio"]

    def _effective_roms_ref(self) -> str | None:
        """The ucla-roms ref that determines the run-time-settings schema.

        Mirrors the resolver's own precedence for ``roms_ref`` (see
        ``_gather()``): the live override in ``self.roms_ref`` when non-blank,
        else the selected ModelSpec's pinned ``code.roms`` ref. ``None`` when
        neither is set, so :func:`_wizard_settings_cls_for_ref`/
        ``run_time_settings_for_ref`` fall back to the legacy schema instead of
        warning on an empty ref.
        """
        return self.roms_ref.value.strip() or self._model_default_roms_ref() or None

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
        if self.parent_enable.value:
            # A child grid (has a parent) gets its boundaries from the parent's
            # nesting.nc extraction, not reanalysis boundary forcing -- strip any
            # boundary items the freshly-built editor just reseeded from the spec.
            self._clear_boundary_forcing()
            # Same reasoning for IC: the freshly-built editor reseeds from the
            # spec's real IC block, which would silently undo the "(none)"
            # default a parent toggle already applied.
            self._clear_initial_conditions()
        self._cdr_forcing = cdr
        if cdr and self._cdr_forcing_file is not None:
            # A ForcingSpec carrying CDR forcing is mutually exclusive with an
            # attached CDR file (Forcing's own validator forbids both).
            self._cdr_forcing_file = None
            self.cdr_file_path.value = ""
            self.cdr_file_upload.value = ()
            self.cdr_file_status.value = (
                "<span style='color:#666'>(cleared -- mutually exclusive with "
                "the ForcingSpec's CDR forcing)</span>"
            )
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
        # Every spec keeps origin="catalog" when picked from the catalog (never
        # flips to "custom" on edit) -- `modified` is what signals a deviation.
        # `modified` itself is computed afterward in `_rebuild()`, where the
        # composed baseline, effective settings, and per-spec seeds are all
        # available; the base SpecRefs built here always start `modified=False`.
        dom = (
            SpecRef(name=self.domain_dd.value, origin="catalog")
            if self.domain_dd.value != "<custom>"
            else SpecRef(name=self.grid_name.value, origin="custom")
        )
        # forcing/output are always an explicit catalog selection now (no more
        # "model_default" origin -- ModelSpec no longer provides either as a fallback).
        forcing = SpecRef(name=self.forcing_dd.value, origin="catalog")
        output = SpecRef(name=self.output_dd.value, origin="catalog")
        return Composition(
            model=SpecRef(name=self.model_dd.value, origin="catalog"),
            domain=dom,
            forcing=forcing,
            output=output,
        )

    def _verify_spec_roundtrip(self, spec: str, new_name: str) -> bool:
        """Side-effect-free check: does re-resolving with ``spec`` sourced from
        its freshly-written catalog file (``new_name``) reproduce the exact same
        resolved blueprint currently shown (``self.config``)?

        Reads ``self._gather()``/``self._overrides``/``self.catalog`` only --
        mutates nothing on the wizard. Compares ``content_hash()``, which covers
        exactly the results-affecting data (excludes identity/composition/
        provenance/working_dir -- see ``ForgeBlueprint._HASH_EXCLUDE``), so a
        match proves the saved spec is a safe substitute for what's currently
        composed/edited and the spec can be marked ``modified=False``.
        """
        if self.config is None:
            return False
        kw = self._gather()
        overrides2 = dict(self._overrides)
        try:
            if spec == "output":
                kw["output_settings"] = self.catalog.output_data(new_name)
                overrides2 = {
                    k: v for k, v in overrides2.items() if not _is_output_key(*k)
                }
            elif spec == "model":
                kw["model_dir"] = self.catalog.model_dir(new_name)
                # Let the saved spec speak for these: the resolver falls back to the
                # ModelSpec when use_pio/bgc_mode are None (resolve.py:418-421) and to
                # code.roms/code.marbl verbatim when roms_ref/marbl_ref are absent.
                # Re-applying the live widget values here would apply them to BOTH
                # sides and make the verifier structurally blind to a spec that
                # dropped them.
                for k in ("use_pio", "bgc_mode", "roms_ref", "marbl_ref"):
                    kw.pop(k, None)
                overrides2 = {k: v for k, v in overrides2.items() if _is_output_key(*k)}
            elif spec == "forcing":
                fi, cdr = _split_forcing_data(self.catalog.forcing_data(new_name))
                if self.parent_enable.value:
                    # Mirror the _gather() durable clear so re-verifying against a
                    # freshly-picked ForcingSpec doesn't spuriously show "modified"
                    # for a child grid whose boundary forcing is always stripped.
                    fi.setdefault("forcing", {})["boundary"] = []
                kw["forcing_inputs"] = fi
                kw["cdr_forcing"] = cdr
                # Mutually exclusive with cdr_forcing_file (Forcing's own
                # validator) -- a live _gather() may carry the file key even
                # though the freshly-picked ForcingSpec's cdr wins here.
                kw.pop("cdr_forcing_file", None)
            elif spec == "domain":
                d = self.catalog.domain_data(new_name)
                kw["grid_kwargs"] = d.get("grid_kwargs", {})
                # open_boundaries/v_sponge: only override from the saved file
                # when it actually carries them (touched at save time). Absence
                # means "derive fresh" -- exactly what _on_domain would leave
                # untouched on a real reload -- so kw already holds the right
                # comparison value from _gather() (the live checkbox state /
                # None-to-derive respectively); forcing a grid build here just
                # to verify an intentionally-omitted value would be pointless.
                if d.get("open_boundaries") is not None:
                    kw["open_boundaries"] = d["open_boundaries"]
                if d.get("v_sponge") is not None:
                    kw["v_sponge"] = d["v_sponge"]
                # dt is always saved (no touched gate, see _domain_spec_data),
                # so an absent value here only happens for an older DomainSpec
                # predating this field -- fall back to kw's existing _gather()
                # value (the live widget dt) rather than force one in.
                if d.get("dt") is not None:
                    kw["dt"] = d["dt"]
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
                raise ValueError(f"unknown spec {spec!r}")
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
        if f.initial_conditions is None:
            # Explicit sentinel (as opposed to a plain missing key) so
            # `_ForcingEditor.__init__` seeds the "(none)" dropdown option
            # instead of falling back to the fresh-wizard GLORYS default.
            ic: dict[str, Any] | None = None
        else:
            ic = {"source": src(f.initial_conditions.source)}
            if f.initial_conditions.bgc_source:
                ic["bgc_source"] = src(f.initial_conditions.bgc_source)
            _ic_interp = getattr(f.initial_conditions, "bgc_interpolation_method", None)
            if (
                _ic_interp is not None
                and getattr(_ic_interp, "value", _ic_interp)
                != BgcInterpMethod.DEPTH.value
            ):
                ic["bgc_interpolation_method"] = getattr(
                    _ic_interp, "value", _ic_interp
                )
            if getattr(f.initial_conditions, "allow_flex_time", False):
                ic["allow_flex_time"] = True
            for f2 in ("prefill", "regrid_method", "extrap_method"):
                v2 = getattr(f.initial_conditions, f2, None)
                if v2 is not None:
                    ic[f2] = getattr(v2, "value", v2)
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
                _custom_file = getattr(it, "custom_file", None)
                if _custom_file is not None:
                    # A CUSTOM_FILE river item carries no other fields (see
                    # RiverForcingItem's mutual-exclusion validators) -- skip the
                    # rest of the per-item extraction below entirely so a loaded
                    # CUSTOM_FILE blueprint round-trips into the wizard's row
                    # instead of raising when re-gathered.
                    d["custom_file"] = {
                        "location": _custom_file.location,
                        "content_hash": _custom_file.content_hash,
                    }
                    out.append(d)
                    continue
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
                if getattr(it, "bgc_source", None):
                    d["bgc_source"] = dict(it.bgc_source)
                _ctc = getattr(it, "convert_to_climatology", None)
                if _ctc is not None:
                    _ctc_val = getattr(_ctc, "value", _ctc)
                    if _ctc_val != ClimatologyMode.IF_ANY_MISSING.value:
                        d["convert_to_climatology"] = _ctc_val
                # roms-tools >=4 regrid/interp knobs, shared across surface/boundary/
                # tidal. getattr(v, "value", v) normalizes (str, Enum) members to
                # their plain string value.
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
        """Prefill from a cataloged Domain.yaml when one is selected."""
        name = self.domain_dd.value
        if name == "<custom>":
            self._domain_seed = None
            return
        if self._grid_file is not None:
            # A catalog Domain pick replaces the grid wholesale via grid_kwargs,
            # which a user-provided grid file forbids carrying alongside it
            # (Domain._grid_file_excludes_generation_geometry) -- detach first so
            # the catalog's grid_kwargs land on now-unlocked widgets instead of
            # being silently discarded.
            self._detach_grid_file()
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        with self._suspend():
            self.grid_name.value = data.get("grid_name", name)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            # Domain-derived properties: a picked catalog Domain replaces the
            # current grid entirely, so reset touched/derived state first, then
            # restore only what the saved Domain.yaml actually carries --
            # absence means "derive fresh from the grid" (click "Derive from
            # grid", or the Save/Run safety net), mirroring
            # _domain_spec_data's save-only-when-touched symmetry. Leaves the
            # boundary checkboxes untouched (rather than resetting to False)
            # when absent, since there's nothing to derive from without a grid
            # build -- the derive_status warning surfaces that instead.
            self._v_sponge_touched = False
            self._boundaries_touched = False
            self._boundaries_derived = False
            self.derive_status.value = ""
            saved_v_sponge = data.get("v_sponge")
            if saved_v_sponge is not None:
                self.v_sponge.value = float(saved_v_sponge)
                self._v_sponge_touched = True
            saved_bnd = data.get("open_boundaries")
            if saved_bnd is not None:
                for d, w in self.bnd.items():
                    if d in saved_bnd:
                        w.value = bool(saved_bnd[d])
                self._boundaries_touched = True
            # dt has no touched flag (see _domain_spec_data) -- a saved
            # DomainSpec always carries it, but an older file might not, so
            # restore only if present; otherwise leave the widget's current
            # value as-is (there's no live re-derive to fall back on for dt).
            saved_dt = data.get("dt")
            if saved_dt is not None:
                self.dt.value = float(saved_dt)
            part = data.get("partitioning", {}) or {}
            saved_npx = part.get("n_procs_x")
            if saved_npx is not None:
                self.npx.value = int(saved_npx)
            saved_npy = part.get("n_procs_y")
            if saved_npy is not None:
                self.npy.value = int(saved_npy)
            self.auto_tiling_chk.value = bool(part.get("auto_tiling", False))
            saved_n_cores = part.get("n_cores")
            if saved_n_cores is not None:
                self.n_cores.value = int(saved_n_cores)
            self._sync_auto_tiling()
            for key, picker in (("start_time", self.start), ("end_time", self.end)):
                if data.get(key):
                    picker.value = datetime.fromisoformat(str(data[key])).date()
            # model_name in Domain.yaml is provenance (the model in use when the
            # domain was saved), not a preference: picking a domain must not
            # override the user's Model selection, so it is deliberately NOT
            # restored here (unlike the load-a-full-blueprint path, where the
            # blueprint's composition.model is authoritative).
            self.topo_source.value = data.get("topography_source", "ETOPO5")
            self.topo_path.value = data.get("topography_path", "") or ""
            # Nesting: mirrors _populate_nesting (loaded-blueprint path) but reads
            # from a DomainSpec's Domain.yaml (only present if the spec was saved
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
            # Parent: mirrors the child block above, reading grid_kwargs_parent
            # (only present if the spec was saved with a parent grid active).
            parent = data.get("grid_kwargs_parent")
            self.parent_enable.value = parent is not None
            if parent:
                for k, w in self.parent_w.items():
                    if k in parent:
                        w.value = parent[k]
        # v_sponge (unlike every other snapshot field) isn't finalized by the
        # widget writes above when untouched -- _rebuild() itself live-derives
        # it from the new grid. Settle that first, then snapshot, then rebuild
        # again so domain_modified is computed against the now-correct seed
        # (otherwise the seed would capture the *previous* domain's v_sponge,
        # spuriously flagging modified=True immediately after picking).
        self._rebuild()
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
            "v_sponge": self.v_sponge.value,
            "dt": self.dt.value,
            "npx": self.npx.value,
            "npy": self.npy.value,
            "auto_tiling": self.auto_tiling_chk.value,
            "n_cores": self.n_cores.value,
            "topo_source": self.topo_source.value,
            "topo_path": self.topo_path.value,
            "nest_enable": self.nest_enable.value,
            "child_w": {k: w.value for k, w in self.child_w.items()},
            "nest_period": self.nest_period.value,
            "nest_pressure_fluxes": self.nest_pressure_fluxes.value,
            "parent_enable": self.parent_enable.value,
            "parent_w": {k: w.value for k, w in self.parent_w.items()},
        }

    def _domain_spec_data(self) -> dict[str, Any]:
        """Build a ``Domain.yaml``-shaped dict from the current widget state (the
        domain-spec extractor for "save modified specs to catalog"). Includes
        topography and nesting -- both results-affecting -- so a saved DomainSpec
        actually round-trips (see ``_verify_spec_roundtrip``); ``register_domain``
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
            "partitioning": kw["partitioning"],
            "topography_source": self.topo_source.value,
            # dt (unlike v_sponge/open_boundaries below) has no touched flag --
            # the widget is always authoritative (default / CFL-computed / typed)
            # and _gather() always passes it explicitly -- so it's always saved,
            # not gated on a "was this edited" check.
            "dt": kw["dt"],
        }
        # v_sponge/open_boundaries: included only when the user has touched
        # them (a manual edit or a value restored from a prior save/load) --
        # otherwise omitted so a reload re-derives fresh from the grid (see
        # _on_domain's symmetric restore-only-if-present, and
        # _ensure_boundaries_derived for the pre-save safety net that fills
        # kw["open_boundaries"] with a real mask-derived value first).
        if self._v_sponge_touched:
            data["v_sponge"] = kw["v_sponge"]
        if self._boundaries_touched:
            data["open_boundaries"] = kw["open_boundaries"]
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
        """Reentrant: nesting ``with self._suspend():`` blocks (e.g. a grid-file
        reattach's own suspend running inside ``_populate_from``'s outer suspend)
        only clears ``_suspended`` once the outermost block exits -- a naive
        unconditional ``False`` on ``__exit__`` would prematurely un-suspend the
        outer block and let its remaining programmatic writes flip touched flags.
        """

        def __init__(self, wiz):
            self.wiz = wiz

        def __enter__(self):
            self.wiz._suspend_depth = getattr(self.wiz, "_suspend_depth", 0) + 1
            self.wiz._suspended = True

        def __exit__(self, *a):
            self.wiz._suspend_depth = max(0, getattr(self.wiz, "_suspend_depth", 1) - 1)
            if self.wiz._suspend_depth == 0:
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
            # _rebuild() and only fail much later, during blueprint processing.
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
        cleared_note = ""
        if self._cdr_forcing_file is not None:
            # Mutually exclusive with an attached CDR file (Forcing's own
            # validator forbids both) -- the upload wins, matching the
            # attach-clears-upload direction in _attach_cdr_file_from_path.
            self._cdr_forcing_file = None
            self.cdr_file_path.value = ""
            self.cdr_file_upload.value = ()
            self.cdr_file_status.value = ""
            cleared_note = "<br><span style='color:#666'>(cleared the attached CDR file -- mutually exclusive)</span>"
        self.cdr_status.value = (
            f"<span style='color:#080'>✓ CDR: {len(cdr.releases)} release(s)</span>"
            f"{cleared_note}"
        )
        self._rebuild()

    def _on_cdr_clear(self, _):
        self._cdr_forcing = None
        self.cdr_upload.value = ()
        self.cdr_status.value = ""
        self._rebuild()

    # ---- CDR forcing: user-provided pre-made netCDF ---------------------------
    def _attach_cdr_file_from_path(self, path_str: str) -> None:
        self.cdr_file_status.value = "<i>attaching…</i>"
        try:
            path = Path(path_str).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"CDR-forcing file not found: {path}")
            content_hash = hash_netcdf_contents(path)
        except Exception as exc:
            self.cdr_file_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        # Light validation only (never blocks the attach -- the executor is the
        # hard gate): warn if the file doesn't even look like CDR forcing.
        warning = ""
        try:
            import xarray as xr

            with xr.open_dataset(path) as ds:
                if "ncdr" not in ds.dims:
                    warning = (
                        "<br><span style='color:#b58900'>⚠ no 'ncdr' dimension "
                        "found -- is this really a CDR-forcing file?</span>"
                    )
        except Exception:
            pass
        self._cdr_forcing_file = {"location": str(path), "content_hash": content_hash}
        cleared_note = ""
        if self._cdr_forcing is not None:
            # Mutually exclusive with an uploaded CDR YAML (Forcing's own
            # validator forbids both) -- the file attach wins here.
            self._cdr_forcing = None
            self.cdr_upload.value = ()
            self.cdr_status.value = ""
            cleared_note = (
                "<br><span style='color:#666'>(cleared the uploaded CDR "
                "YAML -- mutually exclusive)</span>"
            )
        self.cdr_file_status.value = (
            _user_file_status_html(self._cdr_forcing_file) + warning + cleared_note
        )
        self._rebuild()

    def _on_cdr_file_attach(self, _btn):
        path_str = self.cdr_file_path.value.strip()
        if not path_str:
            self.cdr_file_status.value = (
                "<span style='color:#b00'>Enter a path first.</span>"
            )
            return
        self._attach_cdr_file_from_path(path_str)

    def _on_cdr_file_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        self.cdr_file_path.value = str(dest)
        self._attach_cdr_file_from_path(str(dest))

    def _on_cdr_file_clear(self, _btn):
        self._cdr_forcing_file = None
        self.cdr_file_path.value = ""
        self.cdr_file_upload.value = ()
        self.cdr_file_status.value = ""
        self._rebuild()

    def _populate_from(self, cfg: ForgeBlueprint):
        """Set the widgets from a loaded ForgeBlueprint, then re-resolve once.

        Round-trips the authoring inputs (name / description / run / domain /
        partitioning / nesting / dt). Any value in the file that differs from what the composed specs
        would produce is reconstructed as a manual override (so load is non-lossy and
        the overrides layer is rebuilt), then applied on top in ``_rebuild``.

        Returns any validation problems found in the *loaded file's* model_settings.
        """
        # The file's own pinned ucla-roms ref selects the schema variant it was
        # authored against -- not the (possibly different) currently-selected
        # model's default. Computed once here and reused below when seeding
        # self.roms_ref, so the two never drift apart.
        stored_ref = cfg.code.roms.commit or cfg.code.roms.branch or ""
        loaded_problems = validate_run_time_sections(
            cfg.model_settings, roms_ref=stored_ref or None
        )
        with self._suspend():
            # domain dropdown -> custom (the file, not a catalog entry, is authoritative)
            self.domain_dd.value = "<custom>"
            if cfg.composition.model.name in self._dd_values(self.model_dd):
                self.model_dd.value = cfg.composition.model.name
            self.grid_name.value = cfg.domain.grid_name
            self.description.value = cfg.description
            self.name.value = cfg.name
            self._name_touched = (
                True  # a loaded name is a deliberate choice, not a default
            )
            self.save_path.value = self._default_blueprint_path(cfg.name)
            self._save_path_touched = True
            self.start.value = cfg.run.start_date.date()
            self.end.value = cfg.run.end_date.date()
            self.model_ref_date.value = cfg.run.model_reference_date.date()
            gk = cfg.domain.grid_kwargs
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.hmin.value = float(gk.get("hmin", 5.0))
            self.close_narrow_chk.value = bool(gk.get("close_narrow_channels", False))
            self.mask_shapefile.value = str(gk.get("mask_shapefile") or "")
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            for d, w in self.bnd.items():
                w.value = bool(getattr(cfg.domain.open_boundaries, d))
            # A loaded file's boundaries/v_sponge are a deliberate, already-resolved
            # choice (mirrors _name_touched above), not a default to keep
            # auto-deriving over -- freeze both as touched. v_sponge falls back to
            # the model_settings leaf for a pre-domain.v_sponge file (backward
            # compat: older blueprints only ever wrote it there).
            self._boundaries_touched = True
            self._boundaries_derived = False
            self.derive_status.value = ""
            loaded_v_sponge = cfg.domain.v_sponge
            if loaded_v_sponge is None:
                loaded_v_sponge = (cfg.model_settings.get("v_sponge") or {}).get(
                    "v_sponge"
                )
            if loaded_v_sponge is not None:
                self.v_sponge.value = float(loaded_v_sponge)
            self._v_sponge_touched = loaded_v_sponge is not None
            if cfg.domain.partitioning.n_procs_x is not None:
                self.npx.value = cfg.domain.partitioning.n_procs_x
            if cfg.domain.partitioning.n_procs_y is not None:
                self.npy.value = cfg.domain.partitioning.n_procs_y
            self.use_pio_chk.value = bool(
                (cfg.model_settings.get("cppdefs") or {}).get("use_pio", False)
            )
            self.auto_tiling_chk.value = cfg.domain.partitioning.auto_tiling
            loaded_n_cores = cfg.domain.partitioning.n_cores
            if loaded_n_cores is not None:
                self.n_cores.value = int(loaded_n_cores)
            # auto_tiling_chk.observe fires synchronously above regardless of
            # _suspend() (which only sets a flag other handlers check -- see
            # _Suspender), so the disabled/visible state is already correct;
            # call again explicitly for robustness in case that ever changes.
            self._sync_auto_tiling()
            self.bgc_dd.value = (
                "marbl"
                if bool((cfg.model_settings.get("cppdefs") or {}).get("marbl", True))
                else "none"
            )
            # Show the file's actual pinned ref, falling back to the (now-selected)
            # model's default when the file matches it exactly. (stored_ref was
            # already computed above, before the suspend block, to feed the
            # loaded-file validation with the right schema variant.)
            default_ref = self._model_default_roms_ref()
            self.roms_ref.value = (
                default_ref if stored_ref == default_ref else stored_ref
            )
            # Same for MARBL -- code.marbl is absent when the file was saved with
            # bgc_mode="none", in which case fall back to the model's default so
            # re-enabling BGC picks the pinned ref back up.
            stored_marbl = ""
            if cfg.code.marbl is not None:
                stored_marbl = cfg.code.marbl.commit or cfg.code.marbl.branch or ""
            default_marbl = self._model_default_marbl_ref()
            self.marbl_ref.value = (
                stored_marbl
                if stored_marbl and stored_marbl != default_marbl
                else default_marbl
            )
            self.topo_source.value = getattr(
                cfg.domain.topography_source, "value", cfg.domain.topography_source
            )
            self.topo_path.value = cfg.domain.topography_path or ""
            # Grid file: re-attach from its recorded location (reusing the stored
            # content_hash, never recomputing it) if the file still exists;
            # missing/failed reload leaves the grid_file state + locked widgets in
            # place with the error surfaced -- see _reattach_grid_file.
            if cfg.domain.grid_file is not None:
                self._reattach_grid_file(cfg.domain.grid_file)
            else:
                self._detach_grid_file()
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
            # CDR file: trust the stored hash (never recompute); a missing file
            # is a warning, not a blocker -- Save must still round-trip losslessly.
            cff = cfg.forcing.cdr_forcing_file
            if cff is not None:
                self._cdr_forcing_file = {
                    "location": cff.location,
                    "content_hash": cff.content_hash,
                }
                self.cdr_file_path.value = cff.location
                missing = not Path(cff.location).expanduser().exists()
                self.cdr_file_status.value = _user_file_status_html(
                    self._cdr_forcing_file
                ) + (
                    "<br><span style='color:#b00'>⚠ file not found at this path</span>"
                    if missing
                    else ""
                )
            else:
                self._cdr_forcing_file = None
                self.cdr_file_path.value = ""
                self.cdr_file_status.value = ""
            # dt: prefer the first-class domain.dt field; fall back to the
            # model_settings leaf for a pre-domain.dt file (backward compat --
            # older blueprints only ever wrote it there).
            loaded_dt = cfg.domain.dt
            if loaded_dt is None:
                loaded_dt = (cfg.model_settings.get("time_stepping") or {}).get("dt")
            if loaded_dt is not None:
                self.dt.value = float(loaded_dt)
            self._populate_nesting(cfg)
            # forcing: reconstruct the editor from the loaded sources. Forcing/output
            # dropdowns always need a valid catalog selection (no more "model_default"
            # fallback value); fall back to the first available option for an older
            # file recorded with origin="model_default" or an unknown/missing name.
            fname = cfg.composition.forcing.name
            _forcing_values = self._dd_values(self.forcing_dd)
            if fname in _forcing_values:
                self.forcing_dd.value = fname
            elif _forcing_values:
                self.forcing_dd.value = _forcing_values[0]
            self._build_forcing_editor(self._sources_to_inputs(cfg))
            if self.parent_enable.value:
                # A loaded file may (inconsistently) carry boundary forcing for a
                # child grid -- _gather() strips it either way, but clear the
                # visible rows too so the editor doesn't show stale entries.
                self._clear_boundary_forcing()
            # Seed forcing.modified against the *catalog* spec (not the just-loaded
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
            # output spec selection
            oname = cfg.composition.output.name
            _output_values = self._dd_values(self.output_dd)
            if oname in _output_values:
                self.output_dd.value = oname
            elif _output_values:
                self.output_dd.value = _output_values[0]
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
        if self._grid_file is None:
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
        # else: a user-provided grid file forbids the generation-geometry keys
        # above alongside it (Domain._grid_file_excludes_generation_geometry) --
        # grid_kwargs stays {} and grid_file=/grid= (below) carry the grid instead.
        kw = dict(
            model_dir=self.catalog.model_dir(self.model_dd.value),
            grid_name=self.grid_name.value,
            grid_kwargs=gk,
            open_boundaries={d: w.value for d, w in self.bnd.items()},
            partitioning=(
                {"auto_tiling": True, "n_cores": int(self.n_cores.value)}
                if self.auto_tiling_chk.value
                else {
                    "n_procs_x": int(self.npx.value),
                    "n_procs_y": int(self.npy.value),
                }
            ),
            start_date=datetime.combine(self.start.value, datetime.min.time()),
            end_date=datetime.combine(self.end.value, datetime.min.time()),
            description=self.description.value,
            # Untouched: always pass None so the resolver recomputes a fresh default
            # (self.name.value may still hold the *previous* rebuild's backfilled
            # default -- reusing it here would freeze it instead of tracking inputs).
            name=(self.name.value.strip() or None) if self._name_touched else None,
            dt=float(self.dt.value),
            # Untouched: pass None so the resolver derives a fresh default from
            # the current grid (see _rebuild, which mirrors it back into the
            # widget for live display); touched: the user's/loaded value wins.
            v_sponge=float(self.v_sponge.value) if self._v_sponge_touched else None,
        )
        if self.topo_path.value.strip():  # blank = derive default topography path
            kw["topography_path"] = self.topo_path.value.strip()
        kw["topography_source"] = self.topo_source.value
        kw["use_pio"] = self.use_pio_chk.value
        kw["bgc_mode"] = self.bgc_dd.value
        if self.roms_ref.value.strip():
            kw["roms_ref"] = self.roms_ref.value.strip()
        if self.marbl_ref.value.strip():
            kw["marbl_ref"] = self.marbl_ref.value.strip()
        if self.model_ref_date.value and self.model_ref_date.value != date(2000, 1, 1):
            kw["model_reference_date"] = datetime.combine(
                self.model_ref_date.value, datetime.min.time()
            )
        if self._grid_file is not None:
            kw["grid_file"] = self._grid_file
            # Pass the already-loaded grid through to skip re-reading/re-hashing
            # the file (the resolver accepts a pre-built `grid=` alongside
            # `grid_file=` for exactly this -- see build_forge_blueprint). Omitted
            # (not None) when a reattach failed to reload -- the resolver then
            # reloads from `grid_file["location"]` itself and raises the same
            # "file not found", surfacing loudly via _rebuild()'s Invalid status.
            if self._grid_file_grid is not None:
                kw["grid"] = self._grid_file_grid
        # Nesting is unsupported alongside a user-provided grid file (see
        # Domain._grid_file_excludes_generation_geometry) -- omit both blocks
        # while attached even if the (now-disabled) checkboxes are still checked.
        if self.nest_enable.value and self._grid_file is None:
            ck: dict[str, Any] = {}
            for k in _GRID_INT:
                ck[k] = int(self.child_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                ck[k] = float(self.child_w[k].value)
            kw["grid_kwargs_child"] = ck
            kw["metadata_child"] = {"period": float(self.nest_period.value)}
            if self.nest_pressure_fluxes.value:
                kw["nesting_include_pressure_fluxes"] = True
        if self.parent_enable.value and self._grid_file is None:
            pk: dict[str, Any] = {}
            for k in _GRID_INT:
                pk[k] = int(self.parent_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                pk[k] = float(self.parent_w[k].value)
            kw["grid_kwargs_parent"] = pk
        # forcing/output are always required now (no more model-default fallback).
        kw["forcing_inputs"] = self._forcing_editor.gather()
        if self.parent_enable.value and self._grid_file is None:
            # Durable guarantee (authoritative, independent of forcing-editor UI
            # state): a child grid (has a parent) receives its boundary values
            # from the parent's nesting.nc extraction, not reanalysis boundary
            # forcing. Open-boundary edge flags are untouched -- the edges stay
            # open, just fed differently.
            kw["forcing_inputs"]["forcing"]["boundary"] = []
        kw["output_settings"] = self._output_settings()
        kw["composition"] = self._composition()
        if self._cdr_forcing:
            kw["cdr_forcing"] = self._cdr_forcing
        if self._cdr_forcing_file:
            kw["cdr_forcing_file"] = self._cdr_forcing_file
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
        self.nest_pressure_fluxes.value = bool(
            cfg.domain.nesting_include_pressure_fluxes
        )
        parent = cfg.domain.grid_kwargs_parent
        self.parent_enable.value = parent is not None
        if parent:
            for k, w in self.parent_w.items():
                if k in parent:
                    w.value = parent[k]

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

        # v_sponge is cheap (pure arithmetic on grid spacing, no grid build) --
        # keep it live-derived in the widget whenever untouched, by reading back
        # what the resolver just computed. Under _suspend() so this programmatic
        # write doesn't itself flip _v_sponge_touched (see _on_v_sponge_edit).
        if not self._v_sponge_touched:
            with self._suspend():
                self.v_sponge.value = float(cfg.domain.v_sponge)

        # Advanced settings editor: every section is editable. The resolver composes
        # a baseline from the specs; the user's manual edits are a sparse overrides
        # layer applied on top (effective = composed ⊕ overrides). The editor is
        # rebuilt when the *model* changes (its field set depends on the model) or
        # when the effective ucla-roms ref crosses a schema boundary (e.g. editing
        # the roms_ref override across the 0.5.0 or 0.6.0 line with the same model
        # selected) -- see run_time_settings_for_ref / RunTimeSettingsV0_5_0 /
        # RunTimeSettingsV0_6_0.
        composed = cfg.model_settings
        # Computed once per rebuild (each call re-reads the ModelSpec YAML) and
        # reused for the validation call below.
        effective_roms_ref = self._effective_roms_ref()
        settings_cls = _wizard_settings_cls_for_ref(effective_roms_ref)
        if (
            self.editor is None
            or self._editor_model != self.model_dd.value
            or self._editor_settings_cls is not settings_cls
        ):
            self.editor = _SettingsEditor(
                self.W,
                composed,
                on_edit=self._on_editor_edit,
                settings_cls=settings_cls,
            )
            self._editor_model = self.model_dd.value
            self._editor_settings_cls = settings_cls
            self.editor_box.children = [self.editor.accordion]

        effective = _apply_overrides(composed, self._overrides)
        self._syncing = True
        try:
            self.editor.sync(effective)  # display effective; don't re-record as edits
        finally:
            self._syncing = False

        # composition.*.modified: "did the user deviate from what the catalog spec
        # seeded" -- editing then reverting clears the flag. Model/output share the
        # accordion overrides layer (a true value-diff via _diff_overrides, so a
        # no-op edit never counts); domain/forcing are widget-based specs compared
        # against a snapshot captured at the moment of the last catalog pick.
        deviations = _diff_overrides(effective, composed)
        # model_settings-level deviations (accordion overrides) plus the three
        # per-run toggles that live outside model_settings entirely (use_pio,
        # bgc_mode, roms_ref) -- these are resolver kwargs, not settings leaves, so
        # _diff_overrides can never see them (see _model_owned_settings /
        # _CPPDEFS_DERIVED_LEAVES). Without this, flipping PIO or editing the roms
        # ref and saving the blueprint (without pressing "Save as new spec") would
        # silently record composition.model.modified=False.
        declared = self._model_spec_declared()
        spec_deviation = (
            self.use_pio_chk.value != declared["use_pio"]
            or self.bgc_dd.value != declared["bgc_mode"]
            or bool(
                (ref := self.roms_ref.value.strip()) and ref != declared["roms_ref"]
            )
            or bool(
                (mref := self.marbl_ref.value.strip()) and mref != declared["marbl_ref"]
            )
        )
        model_modified = (
            any(not _is_output_key(s, f) for s, f in deviations) or spec_deviation
        )
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
                    self.save_path.value = self._default_blueprint_path(cfg.name)
        self.download_link.value = self._download_html(cfg)
        # Surface (never silently ship) provisional open-boundary defaults: the
        # checkboxes currently reflect whatever's live, but that's only a real
        # mask-derived value once _boundaries_derived is True or the user has
        # touched them -- see _ensure_boundaries_derived for the Save/Run
        # guarantee. Only fills a *blank* status -- an actual derive/error
        # message (set by _on_derive_domain_properties) stays visible instead
        # of being clobbered by this generic warning on every rebuild.
        if (
            not self._boundaries_touched
            and not self._boundaries_derived
            and not self.derive_status.value
        ):
            self.derive_status.value = (
                "<span style='color:#b58900'>⚠ boundaries not derived yet — "
                'click "Derive from grid" (Save/Run derive automatically)</span>'
            )
        # Suppressed for the same reason as _wizard_settings_cls_for_ref above --
        # _rebuild runs on every keystroke, including typing into the roms_ref
        # override box, so a non-semver ref's UserWarning would otherwise spam.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            problems = validate_run_time_sections(
                cfg.model_settings, roms_ref=effective_roms_ref
            )
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
        fname = f"{cfg.name}.forge_blueprint.yaml"
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

    def _build_grid_from_widgets(self) -> Any:
        """Build a ``roms_tools.Grid`` from the current (main-domain) grid kwargs
        -- or return the already-loaded grid file's grid, when one is attached.

        Shared by the plot button, "Derive from grid", and the export-time
        safety net -- the only three places that pay the cost of an actual
        grid build (the live preview / ``_rebuild`` never does, so typing in
        the grid fields stays instant).
        """
        if self._grid_file_grid is not None:
            return self._grid_file_grid
        if self._grid_file is not None:
            # Attached but not currently loaded (a reattach failed -- e.g. the
            # file went missing since it was recorded -- see _reattach_grid_file).
            # Must not silently fall through to building a grid from whatever
            # stale/default values happen to be sitting in the (locked, but
            # still holding old values) grid_w widgets -- that would plot/derive
            # boundaries and v_sponge from a completely different, wrong grid
            # with no indication anything is amiss.
            raise RuntimeError(
                "grid_file is attached but failed to (re)load -- see "
                "grid_file_status; re-attach a valid file first."
            )

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
        return Grid(**gk)

    def _apply_grid_derived_properties(self, grid: Any) -> None:
        """Set any untouched v_sponge/open-boundary values from a built grid.

        Boundaries come from roms-tools' own ``check_and_set_boundaries`` (the
        same logic ``rt.Grid``/downstream tools use to infer active edges from
        the land mask); v_sponge from the existing grid-spacing formula. Only
        overwrites properties the user hasn't touched -- see the module-level
        touched/derived state-machine notes on ``_v_sponge_touched`` etc.
        """
        from roms_tools.setup.utils import check_and_set_boundaries

        from cstar_forge.forge.util import compute_v_sponge_from_grid

        mask = grid.ds.get("mask_rho")
        if mask is None:
            raise ValueError(
                "grid.ds has no 'mask_rho' -- cannot derive open boundaries from it"
            )
        boundaries = check_and_set_boundaries(None, mask)
        with self._suspend():
            if not self._boundaries_touched:
                for d, w in self.bnd.items():
                    if d in boundaries:
                        w.value = bool(boundaries[d])
            if not self._v_sponge_touched:
                self.v_sponge.value = compute_v_sponge_from_grid(grid.size_x, grid.nx)
        self._boundaries_derived = True

    def _on_derive_domain_properties(self, _):
        """Handle the "Derive from grid" button: build the grid once and
        refresh any untouched v_sponge/open-boundary values from it.
        """
        self.derive_status.value = "<i>building grid…</i>"
        try:
            grid = self._build_grid_from_widgets()
            self._apply_grid_derived_properties(grid)
            self.derive_status.value = (
                "<span style='color:#080'>✓ derived from grid</span>"
            )
        except Exception as exc:
            self.derive_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
        self._rebuild()

    def _ensure_boundaries_derived(self) -> bool:
        """Export-time safety net for Save/Save-domain/Run: if boundaries are
        untouched and have never been derived against the current grid, force
        one grid build now so those actions can never ship the provisional
        checkbox defaults (east+north) silently.

        Returns True if boundaries are known-good (touched, already derived, or
        just successfully derived here); False if an explicit derive was needed
        and failed (see ``derive_status`` for the error) -- callers MUST abort
        rather than proceed and silently persist the provisional defaults.

        The passive browser download link (a plain data-URI anchor, regenerated
        on every ``_rebuild``) has no click-time Python hook to apply this to --
        forcing a grid build on every edit would reintroduce the UI-stall this
        design explicitly avoids. Its freshness is only as good as the last
        explicit derive/rebuild; ``derive_status`` surfaces that instead of
        shipping it silently.
        """
        if self._boundaries_touched or self._boundaries_derived:
            return True
        self._on_derive_domain_properties(None)
        return self._boundaries_derived

    def _on_plot(self, _):
        """Build a roms_tools.Grid from the current grid kwargs and render it."""
        self.plot_status.value = "<i>building grid…</i>"
        try:
            import io

            import matplotlib.pyplot as plt

            plt.ioff()
            try:
                grid = self._build_grid_from_widgets()
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

    def _on_parent_plot(self, _):
        """Build the parent+this-grid roms_tools Grids and render ``plot_nesting``
        (boundary overlay of this grid within its parent) into the Parent
        section's own plot. This grid is the fine/inner grid; the parent is the
        coarse/outer grid it's aligned into.
        """
        self.parent_plot_status.value = "<i>building grids…</i>"
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

            pk: dict[str, Any] = {}
            for k in _GRID_INT:
                pk[k] = int(self.parent_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                pk[k] = float(self.parent_w[k].value)

            plt.ioff()
            try:
                parent = Grid(**pk)
                this_grid = Grid(**gk)
                # See _on_nest_plot for why plt.show() is neutralized here.
                _real_show, plt.show = plt.show, lambda *a, **k: None
                try:
                    plot_nesting(parent, this_grid)
                finally:
                    plt.show = _real_show
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.ion()

            buf.seek(0)
            self.parent_plot_img.value = buf.read()
            self.parent_plot_status.value = "<span style='color:#080'>✓</span>"
        except Exception as exc:
            self.parent_plot_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    def _on_save(self, _):
        if not self._ensure_boundaries_derived():
            self.save_status.value = (
                "<span style='color:#b00'>Save aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). A save must never ship provisional defaults; fix "
                "the grid or set boundaries manually, then retry.</span>"
            )
            return
        if self.config is None:
            self.save_status.value = (
                "<span style='color:#b00'>Nothing to save — config is invalid.</span>"
            )
            return
        try:
            save_path = Path(self.save_path.value)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            p = self.config.to_yaml(save_path)
            self.save_status.value = f"<span style='color:#080'>Saved {p}</span>"
        except Exception as exc:
            self.save_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

    # ---- save modified specs to catalog --------------------------------------
    # Each handler: validate name -> extract that spec from current state ->
    # write it to the catalog (durable) -> side-effect-free round-trip verify ->
    # on match, commit the minimal state change (repoint the dropdown, drop that
    # spec's overrides / reset its seed) under _suspend() + a single _rebuild();
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
        except FileExistsError as exc:
            # str(exc) names the owning layer (e.g. "...already exists in the
            # 'bundled' catalog layer") -- a plain single-line message, no
            # traceback, so it's rendered verbatim rather than genericized.
            self.save_output_status.value = f"<span style='color:#b00'>{exc}</span>"
            return
        except Exception as exc:
            self.save_output_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_spec_roundtrip("output", name)
        with self._suspend():
            # ipywidgets resets .value to the first option on a bare `.options =`
            # reassignment even when the old value is still present -- restore it
            # explicitly so a mismatch genuinely leaves the selection untouched.
            old_value = self.output_dd.value
            self.output_dd.options = self._dd_options(
                list(self.catalog.output_names), "output"
            )
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
                "differs — spec kept as modified.</span>"
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
                # Live widget values, using _gather()'s exact conventions (use_pio/
                # bgc_mode unconditional, roms_ref/marbl_ref only when non-blank) --
                # the round-trip verifier below compares against _gather()'s kw, so
                # any divergence here would be a spurious mismatch.
                bgc_mode=self.bgc_dd.value,
                use_pio=self.use_pio_chk.value,
                roms_ref=self.roms_ref.value.strip() or None,
                marbl_ref=self.marbl_ref.value.strip() or None,
            )
        except FileExistsError as exc:
            self.save_model_status.value = f"<span style='color:#b00'>{exc}</span>"
            return
        except Exception as exc:
            self.save_model_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_spec_roundtrip("model", name)
        with self._suspend():
            old_value = self.model_dd.value
            self.model_dd.options = self._dd_options(
                list(self.catalog.model_names), "model"
            )
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
                "differs — spec kept as modified.</span>"
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
        if self._grid_file is not None:
            # DomainSpec (catalog/DomainSpec/*.yaml) has no grid_file field --
            # saving now would silently produce a geometry-less entry (grid_kwargs
            # is {} while attached). Detach first.
            self.save_domain_status.value = (
                "<span style='color:#b00'>Detach the grid file first — DomainSpec "
                "has no grid_file field.</span>"
            )
            return
        # Unlike _on_save/_on_run (which emit concrete boundaries into the
        # blueprint right now), an untouched open_boundaries is deliberately
        # OMITTED from a saved DomainSpec (see _domain_spec_data) so it can
        # re-derive fresh on next load -- there's nothing here for a failed
        # derive to protect, and forcing one would block a legitimate
        # checkpoint-save of a domain whose grid can't build yet (e.g.
        # topography not sorted out). No _ensure_boundaries_derived() call.
        try:
            self.catalog.register_domain_from_dict(name, self._domain_spec_data())
        except FileExistsError as exc:
            self.save_domain_status.value = f"<span style='color:#b00'>{exc}</span>"
            return
        except Exception as exc:
            self.save_domain_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_spec_roundtrip("domain", name)
        with self._suspend():
            old_value = self.domain_dd.value
            self.domain_dd.options = self._dd_options(
                list(self.catalog.domain_names), "domain", prefix=["<custom>"]
            )
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
                "differs — spec kept as modified.</span>"
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
        except FileExistsError as exc:
            self.save_forcing_status.value = f"<span style='color:#b00'>{exc}</span>"
            return
        except Exception as exc:
            self.save_forcing_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )
            return
        ok = self._verify_spec_roundtrip("forcing", name)
        with self._suspend():
            old_value = self.forcing_dd.value
            self.forcing_dd.options = self._dd_options(
                list(self.catalog.forcing_names), "forcing"
            )
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
                "differs — spec kept as modified.</span>"
            )

    def _build_run_command(self, blueprint_path: str) -> list[str]:
        """Command the Run button invokes: ``cstar blueprint run <path>``.

        The same command the docs give for both pipeline steps, and the same one
        the emitted ``roms_marbl`` blueprint is run with -- the button exposes no
        per-run flags, so the full-option ``cstar forge run`` passthrough buys it
        nothing. Resolving ``application: forge`` this way goes through C-Star's
        registry, which finds the app through cstar-forge's ``cstar.applications``
        entry point.

        Uses the ``cstar`` console script installed alongside the running
        interpreter, so the subprocess stays in this environment rather than
        taking whatever is first on PATH. Where that script is absent (C-Star's
        CLI not installed), falls back to ``python -m cstar_forge.run``, which
        drives the same executor without needing C-Star's CLI at all.
        """
        import sys

        cstar_exe = Path(sys.executable).with_name("cstar")
        if cstar_exe.exists():
            return [str(cstar_exe), "blueprint", "run", blueprint_path]
        return [sys.executable, "-m", "cstar_forge.run", blueprint_path]

    def _on_run(self, _):
        if not self._ensure_boundaries_derived():
            self.run_status.value = (
                "<span style='color:#b00'>Run aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). Fix the grid or set boundaries manually, then "
                "retry.</span>"
            )
            return
        if self.config is None:
            self.run_status.value = (
                "<span style='color:#b00'>Nothing to run — config is invalid.</span>"
            )
            return
        _schedule_coroutine(self._run_async())

    async def _run_async(self):
        """Save the current blueprint, then launch the C-Star CLI as a subprocess
        and stream its combined stdout/stderr into ``run_output`` line by line as
        it arrives (not all at once at the end).
        """
        import asyncio

        self.run_btn.disabled = True
        self.run_output.clear_output(wait=True)
        self.run_status.value = "<i>saving blueprint…</i>"
        cmd: list[str] | None = None
        try:
            save_path = Path(self.save_path.value)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            path = self.config.to_yaml(save_path)
            cmd = self._build_run_command(str(path))
            self.run_status.value = f"<i>running: {' '.join(cmd)}</i>"
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            # Read fixed-size chunks and reassemble lines ourselves rather than
            # `async for line in proc.stdout` (StreamReader.readline), which raises
            # ValueError, "Separator is not found, and chunk exceed the limit", when a
            # child emits 64 KiB without a newline -- e.g. a \r-redrawn progress bar.
            # append_stdout (not `with self.run_output: print(...)`) appends directly
            # to the widget's output list, so lines land in the log whether or not a
            # live Jupyter kernel is routing stdout via iopub messaging.
            buf = b""
            while True:
                chunk = await proc.stdout.read(_STREAM_READ_SIZE)
                if not chunk:
                    break
                lines, buf = _drain_stream_buffer(buf + chunk)
                for line in lines:
                    self.run_output.append_stdout(line)
            lines, buf = _drain_stream_buffer(buf, at_eof=True)
            for line in lines:
                self.run_output.append_stdout(line)
            code = await proc.wait()
            # On success the log above names the emitted roms_marbl blueprint (the
            # runner logs where it published it). Point at it rather than restating
            # a path this widget would have to derive for itself.
            self.run_status.value = (
                "<span style='color:#080'>✓ finished</span> — run the emitted "
                "ROMS-MARBL blueprint named in the log with <code>cstar blueprint "
                "run &lt;path&gt;</code>"
                if code == 0
                else f"<span style='color:#b00'>exited with code {code}</span>"
            )
        except Exception as exc:
            # Name the command in the status so a failure isn't a context-free
            # error string (cmd is None only if saving the blueprint threw first).
            where = f" while running: {' '.join(cmd)}" if cmd else ""
            self.run_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}{where}</span>"
            )
        finally:
            self.run_btn.disabled = False

    # ---- workplan export -------------------------------------------------------
    @staticmethod
    def _workplan_path(blueprint_path: Path) -> Path:
        """Sibling ``{name}.workplan.yaml`` for a saved blueprint path (stripping a
        ``.forge_blueprint.yaml`` suffix when present, so the default save path
        ``{name}.forge_blueprint.yaml`` yields ``{name}.workplan.yaml``).
        """
        name = blueprint_path.name
        suffix = ".forge_blueprint.yaml"
        stem = name[: -len(suffix)] if name.endswith(suffix) else blueprint_path.stem
        return blueprint_path.with_name(f"{stem}.workplan.yaml")

    def _workplan_dest(self, blueprint_path: Path) -> Path:
        """Destination for a saved workplan: the active catalog's ``workplans/``
        directory when the catalog is local (mirroring ``_default_blueprint_path``'s
        preference for the catalog), else a sibling of the blueprint.
        """
        fname = self._workplan_path(blueprint_path).name
        cat = getattr(self, "catalog", None)
        try:
            # Mirror _default_blueprint_path: never offer a read-only catalog.
            if (
                cat is not None
                and getattr(cat, "_is_local", False)
                and not getattr(cat, "read_only", False)
            ):
                return cat.workplans_dir / fname
        except Exception:
            pass
        return blueprint_path.with_name(fname)

    def _build_workplan(self, blueprint_path: Path):
        """Build the two-step C-Star workplan for the current config: step ``forge``
        runs the forge application on the saved blueprint; step ``roms_marbl`` runs
        the ``B_{name}.yaml`` blueprint the forge step generates -- a *deferred*
        blueprint (``from_step``), since it does not exist until step 1 has run.

        Returns
        -------
        cstar.orchestration.models.Workplan
        """
        try:
            from cstar.orchestration.models import (
                DeferredBlueprintRef,
                Step,
                Workplan,
            )
        except ImportError as exc:
            msg = (
                "Workplan export requires a C-Star version with workplan and "
                "deferred-blueprint support"
            )
            raise RuntimeError(msg) from exc

        cfg = self.config
        # No cpus override for the forge step: the scheduler falls back to
        # ForgeBlueprint.cpus_needed, which is the grid-sized estimate.
        forge_step = Step(
            name="forge",
            application="forge",
            blueprint=blueprint_path.expanduser().resolve(),
        )
        roms_step = Step(
            name="roms_marbl",
            application="roms_marbl",
            depends_on=["forge"],
            blueprint=DeferredBlueprintRef(
                from_step="forge",
                filename=f"B_{cfg.name}.yaml",
            ),
            # A deferred blueprint cannot be inspected at submit time, so the
            # scheduler defaults the step to 1 CPU -- size it from the
            # partitioning explicitly.
            compute_overrides={"cpus": cfg.n_procs},
        )
        return Workplan(
            name=cfg.name,
            description=f"Forge inputs + ROMS-MARBL run for {cfg.name}",
            steps=[forge_step, roms_step],
        )

    def _on_save_workplan(self, _):
        if not self._ensure_boundaries_derived():
            self.workplan_status.value = (
                "<span style='color:#b00'>Save aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). Fix the grid or set boundaries manually, then "
                "retry.</span>"
            )
            return
        if self.config is None:
            self.workplan_status.value = (
                "<span style='color:#b00'>Nothing to save — config is invalid.</span>"
            )
            return
        try:
            save_path = Path(self.save_path.value)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            bp_path = self.config.to_yaml(save_path)
            workplan = self._build_workplan(Path(bp_path))
            wp_path = self._workplan_dest(Path(bp_path))
            wp_path.parent.mkdir(parents=True, exist_ok=True)

            from cstar.orchestration.serialization import serialize

            serialize(wp_path, workplan)
            # No env-var prefix: an installed cstar-forge registers the forge app
            # through its ``cstar.applications`` entry point, so C-Star's registry
            # resolves ``application: forge`` in the scheduling process and in the
            # jobs it spawns, without anything being propagated by hand.
            cmd = f"cstar workplan run {wp_path}"
            self.workplan_status.value = (
                f"<span style='color:#080'>Saved {bp_path} and {wp_path}</span><br>"
                f"Run it with: <code>{cmd}</code>"
            )
        except Exception as exc:
            self.workplan_status.value = (
                f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            )

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
        parent_box = W.GridBox(
            [self.parent_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"),
        )

        nesting_accordion = W.Accordion(
            children=[
                W.VBox(
                    [
                        section(
                            "Child grid",
                            W.HBox(
                                [
                                    W.VBox(
                                        [
                                            self.nest_enable,
                                            self.nest_help,
                                            self.nest_domain_dd,
                                            child_box,
                                            W.HBox(
                                                [
                                                    self.nest_period,
                                                    self.nest_pressure_fluxes,
                                                ]
                                            ),
                                        ]
                                    ),
                                    W.VBox(
                                        [
                                            W.HBox(
                                                [
                                                    self.nest_plot_btn,
                                                    self.nest_plot_status,
                                                ]
                                            ),
                                            self.nest_plot_img,
                                        ],
                                        layout=W.Layout(padding="0 0 0 20px"),
                                    ),
                                ]
                            ),
                        ),
                        section(
                            "Parent grid",
                            W.HBox(
                                [
                                    W.VBox(
                                        [
                                            self.parent_enable,
                                            self.parent_help,
                                            self.parent_domain_dd,
                                            parent_box,
                                        ]
                                    ),
                                    W.VBox(
                                        [
                                            W.HBox(
                                                [
                                                    self.parent_plot_btn,
                                                    self.parent_plot_status,
                                                ]
                                            ),
                                            self.parent_plot_img,
                                        ],
                                        layout=W.Layout(padding="0 0 0 20px"),
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            ],
            selected_index=None,
        )
        nesting_accordion.set_title(0, "Parent and child grid settings")
        return W.VBox(
            [
                W.HTML(
                    "<h3>ForgeBlueprint wizard</h3>"
                    "<i>Pick a Model and (optionally) a Domain, tweak fields, review, save. "
                    "Or load an existing forge_blueprint.yaml to edit it. Fine-tune model "
                    "settings under “Advanced settings”.</i>"
                ),
                section(
                    "Load existing (optional)",
                    W.HBox([self.load_path, self.load_btn]),
                    self.upload,
                    self.load_status,
                ),
                section(
                    "Specs",
                    W.HBox([self.model_dd, self.roms_ref, self.bgc_dd, self.marbl_ref]),
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
                                    section(
                                        "Attach a pre-made grid file (optional)",
                                        W.HBox(
                                            [
                                                self.grid_file_path,
                                                self.grid_file_attach_btn,
                                                self.grid_file_detach_btn,
                                            ]
                                        ),
                                        self.grid_file_upload,
                                        self.grid_file_status,
                                    ),
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
                section(
                    "Domain-derived properties",
                    W.HBox([self.v_sponge, self.derive_btn, self.derive_status]),
                    W.HBox([self.dt, self.dt_btn, self.dt_status]),
                    section("Open boundaries", W.HBox(list(self.bnd.values()))),
                ),
                nesting_accordion,
                section("Forcing", self.forcing_box),
                section(
                    "CDR forcing (optional)",
                    W.HBox([self.cdr_upload, self.cdr_clear_btn]),
                    self.cdr_status,
                    W.HBox(
                        [
                            self.cdr_file_path,
                            self.cdr_file_attach_btn,
                            self.cdr_file_clear_btn,
                        ]
                    ),
                    self.cdr_file_upload,
                    self.cdr_file_status,
                ),
                section(
                    "Partitioning",
                    W.HBox(
                        [
                            self.npx,
                            self.npy,
                            self.n_cores,
                            self.use_pio_chk,
                            self.auto_tiling_chk,
                        ]
                    ),
                ),
                section(
                    "Run window",
                    self.start,
                    self.end,
                    self.model_ref_date,
                    self.description,
                ),
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
                    "Save modified specs to catalog",
                    W.HTML(
                        "<i>Promote an edited spec to a new named catalog entry. "
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
                section(
                    "Workplan (experimental)",
                    self.workplan_note,
                    W.HBox([self.workplan_btn]),
                    self.workplan_status,
                ),
            ]
        )

    def display(self):
        from IPython.display import display

        display(self.widget)


class ForgeBlueprintWizardApp:
    """Thin wrapper around :class:`ForgeBlueprintWizard` that adds a catalog-location
    bar above it. Blank input loads the default layered stack (your writable
    ``~/cstar-forge-data/catalog`` -- or ``CSTAR_FORGE_CATALOG`` -- layer over the
    read-only bundled in-repo catalog, see
    :func:`~cstar_forge.domain_catalog.default_catalog_stack`). Entering one or more
    ``os.pathsep``-separated local paths builds a
    :class:`~cstar_forge.domain_catalog.LayeredCatalog` exactly like the same value
    in ``CSTAR_FORGE_CATALOG`` would (first = writable top, rest read-only, bundled
    catalog appended at the bottom). Entering a single GitHub/http URL or the
    literal ``"local"`` loads exactly that one store, read-only, for browsing
    (saves then default to CWD-relative filenames, and the status line says so).
    Clicking Reload rebuilds the wizard against the new catalog.

    Usage (in a Jupyter notebook)::

        from cstar_forge.forge_blueprint_wizard import ForgeBlueprintWizardApp
        app = ForgeBlueprintWizardApp()
        app.display()
        # ... optionally enter a different catalog path/URL above and click Reload ...
        cfg = app.inner.config       # the current resolved ForgeBlueprint (or None)
    """

    def __init__(self, catalog_root: str | None = None):
        import ipywidgets as W

        self.W = W
        self.inner: ForgeBlueprintWizard | None = None

        self._cat_input = W.Text(
            value="",
            placeholder="catalog path(s), ':'-separated top-first, or GitHub URL "
            "(blank = your catalog over the bundled one)",
            description="Catalog:",
            style={"description_width": "110px"},
            layout=W.Layout(width="520px"),
        )
        self._cat_reload_btn = W.Button(description="Reload catalog", icon="refresh")
        self._cat_status = W.HTML("")
        self._cat_reload_btn.on_click(self._reload)

        self._outer = W.VBox([])
        self._load(catalog_root)

    def _load(self, catalog_root_value: str | None) -> None:
        import os

        from cstar_forge.domain_catalog import (
            DomainCatalog,
            LayeredCatalog,
            _is_github_catalog_url,
            build_catalog_stack,
            default_catalog_stack,
        )

        val = (catalog_root_value or "").strip()
        try:
            if not val:
                # Blank -> the default layered stack (writable user layer over
                # the read-only bundled catalog), not the bundled catalog alone.
                cat = default_catalog_stack()
            else:
                entries = [e for e in val.split(os.pathsep) if e]
                if len(entries) == 1 and (
                    _is_github_catalog_url(entries[0])
                    or entries[0].startswith("http")
                    or entries[0].strip().lower() == "local"
                ):
                    # A remote URL or the literal "local" (bundled catalog)
                    # can never be a writable top layer, so load it as exactly
                    # one read-only store for browsing.
                    cat = DomainCatalog(catalog_root=entries[0])
                else:
                    # Same builder as the CSTAR_FORGE_CATALOG env handling
                    # (first entry writable top, rest read-only, bundled
                    # appended at the bottom) so the two paths cannot drift --
                    # a single local path gets the bundled layer underneath,
                    # exactly like the same value in the env var.
                    cat = build_catalog_stack(entries)
            inner = ForgeBlueprintWizard(catalog=cat)
        except Exception as exc:
            self._cat_status.value = (
                f"<span style='color:#b00'>Failed to load catalog "
                f"{val or '(default)'!r}: {exc}</span>"
            )
            return

        self.inner = inner
        if isinstance(cat, LayeredCatalog):
            layers = " over ".join(
                f"{store.label} {store.catalog_root} ({len(store.domain_names)} domains)"
                if store is cat.top
                else f"{store.label} ({len(store.domain_names)} domains)"
                for store in cat.stores
            )
            self._cat_status.value = (
                f"<span style='color:#2a2'>Loaded {layers} -- "
                f"{len(cat.model_names)} models, "
                f"{len(cat.roms_marbl_blueprint_names)} blueprints</span>"
            )
        else:
            # Single stores can be read-only (a remote URL or "local"): the
            # save-path defaults then silently fall back to CWD-relative
            # filenames, so say so instead of leaving the fallback invisible.
            ro_note = (
                " <span style='color:#b60'>(read-only catalog -- saves default "
                "to the current directory)</span>"
                if getattr(cat, "read_only", False)
                else ""
            )
            self._cat_status.value = (
                f"<span style='color:#2a2'>Loaded {cat.catalog_root} -- "
                f"{len(cat.model_names)} models, "
                f"{len(cat.roms_marbl_blueprint_names)} blueprints</span>{ro_note}"
            )
        self._outer.children = [
            self.W.VBox(
                [
                    self.W.HTML("<h4>Catalog location</h4>"),
                    self.W.HBox([self._cat_input, self._cat_reload_btn]),
                    self._cat_status,
                ],
                layout=self.W.Layout(
                    border="1px solid #e0e0e0", padding="8px", margin="4px 0"
                ),
            ),
            inner.widget,
        ]

    def _reload(self, _btn):
        self._load(self._cat_input.value)

    def display(self):
        from IPython.display import display

        display(self._outer)
