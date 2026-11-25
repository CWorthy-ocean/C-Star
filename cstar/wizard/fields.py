"""Help text, field widgets, catalog dropdowns and attached-file helpers shared
across the wizard.

The wizard's cards, its forcing and settings editors and the Workplan page all use
these. Like the rest of :mod:`cstar.wizard`, nothing here imports ``ipywidgets`` at
module scope: the widget builders take the module as ``W``.
"""

from __future__ import annotations

import re
import types
import typing
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_args, get_origin

from cstar.base.digest import short_digest
from cstar.wizard.ui import components

if TYPE_CHECKING:
    from collections.abc import Collection

# ===========================================================================
# Help text — shown as widget tooltips on hover (tooltip= kwarg, all widgets)
#
# Automated: namelist fields are looked up live from the cstar.roms.namelist
# field descriptions (set in the RomsNamelist / group models), unless the
# label glossary carries a hint for the field.
# Manual: forcing/grid/nesting/run-window knobs listed in HELP_TEXT below.
# ===========================================================================

# Keyed by (context, field_name). "context" is a category string: a forcing
# category ("surface","boundary","tidal","river","ic"), a grid section ("grid"),
# a nesting section ("nesting"), or a run-window field ("run").
# Falls back to just field_name for unambiguous global names.
HELP_TEXT: dict[str | tuple[str, str], str] = {
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
    ): "Baroclinic time step in seconds. Derived from the grid (CFL criterion) until "
    "you type a value; 'Use CFL value' goes back to the derived value.",
    ("export", "name"): "Canonical blueprint name. Drives the save filename, the "
    "default working directory, casename, and generated file stems. Defaults to a "
    "derived name (model_grid_NprocsProcs); edit to override.",
    ("export", "description"): "One line describing this blueprint. Derived from "
    "the grid, run window, model, BGC mode, forcing and CDR release until you type "
    "your own.",
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
    (
        "nesting",
        "parent_topo_source",
    ): "Topography dataset the PARENT grid is rebuilt with (for align_grids: this "
    "grid's mask/bathymetry is blended toward the parent's along the nesting "
    "boundaries), so it should match what the parent run was actually built "
    "with. '(same as this grid)' inherits this grid's topo source AND path; "
    "picking a dataset here uses that dataset at its default location unless a "
    "parent topo path is also given. Prefilled from the parent DomainSpec.",
    (
        "nesting",
        "parent_topo_path",
    ): "Explicit topography file for the PARENT grid (on the executor machine). "
    "Blank: the parent dataset's default location -- or, with '(same as this "
    "grid)', this grid's topo path.",
    (
        "nesting",
        "child_topo_source",
    ): "Topography dataset the CHILD grid is built with when extracting nesting "
    "data. '(same as this grid)' inherits this grid's topo source AND path.",
    (
        "nesting",
        "child_topo_path",
    ): "Explicit topography file for the CHILD grid (on the executor machine). "
    "Blank: the child dataset's default location -- or, with '(same as this "
    "grid)', this grid's topo path.",
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
    # Generic (context-independent) fallbacks shared by IC-BGC/boundary/surface bgc
    # rows -- SourceSpec.use_vars/constants/esper_method/esper_equation.
    "use_vars": "Comma-separated list of BGC variables this source contributes "
    "(bgc sources only), e.g. 'ALK,DIC'. Required when multiple bgc-type sources are "
    "present so their variable sets don't overlap; the rest are derived/filled by MARBL.",
    "constants": "Depth-invariant constant value(s) for a 'constants' source, as "
    "comma- or newline-separated 'key=value' pairs (mmol/m^3), e.g. 'Fe=3.0e-3, ALK=2300'.",
    "esper_method": "PyESPER estimation method for an 'ESPER' source: 'lir' "
    "(locally interpolated regression), 'nn' (neural network, roms-tools default), "
    "or 'mixed'.",
    "esper_equation": "PyESPER predictor equation for an 'ESPER' source: 8 "
    "(salinity + temperature) or 16 (salinity only). Blank = roms-tools default (8).",
    "esper_salinity_conditioning": "Blend the salinity PyESPER feeds its nets toward "
    "the WOA23 annual climatology below 31-34 PSU, where ESPER has no training data "
    "and gives unphysical values in river plumes (silicate > 100 µmol/kg, negative "
    "nutrients, DIC above ALK), then dilute ALK and DIC back to the model salinity. "
    "Forge downloads the one WOA23 file this needs. Requires the CWorthy PyESPER fork "
    "(tag v1.1.1+cworthy.1 or later).",
    # ---- IC BGC sources (row list) ----------------------------------------------
    (
        "ic_bgc",
        "name",
    ): "Logical BGC source name for this initial-conditions contributor, e.g. "
    "'UNIFIED', 'GLODAP', 'WOA_BGC', 'constants', or 'ESPER'. Add multiple rows to "
    "combine sources (down-select each via use_vars). 'WOA_BGC' supplies only "
    "NO3/PO4/SiO3/O2, so pair it with a source for DIC/ALK/Fe.",
    (
        "ic_bgc",
        "climatology",
    ): "Use this BGC source as a climatology (annual-mean repeated each year) "
    "rather than a time-varying dataset.",
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
        "ic_validate",
    ): "Run roms-tools' post-construction validation (NaN-at-wet-point checks across "
    "physics and every bgc source). On by default; uncheck to skip validation "
    "entirely (bypass_validation=True) -- a last-resort escape hatch, not a "
    "recommended default.",
    (
        "ic",
        "ic_groom",
    ): "Groom the tracers this section's ESPER source(s) supply: roms-tools detects "
    "implausible values (spikes, zeros beside ordinary water, DIC inconsistent with "
    "ALK) and refills them from the surrounding water. On by default (roms-tools' "
    "default); uncheck to turn it off (groom=False). No effect without an ESPER "
    "source. Threshold overrides (groom_settings) are set only by hand in the "
    "blueprint YAML; unchecking leaves them out, since roms-tools applies them only "
    "while grooming is on.",
    (
        "boundary",
        "boundary_groom",
    ): "Groom the tracers this section's ESPER source(s) supply: roms-tools detects "
    "implausible values (spikes, zeros beside ordinary water, DIC inconsistent with "
    "ALK) and refills them from the surrounding water. On by default (roms-tools' "
    "default); uncheck to turn it off (groom=False). No effect without an ESPER "
    "source. Threshold overrides (groom_settings) are set only by hand in the "
    "blueprint YAML; unchecking leaves them out, since roms-tools applies them only "
    "while grooming is on.",
    (
        "boundary",
        "boundary_validate",
    ): "Run roms-tools' post-construction validation (NaN-at-wet-point checks across "
    "physics and every bgc source). On by default; uncheck to skip validation "
    "entirely (bypass_validation=True) -- a last-resort escape hatch, not a "
    "recommended default.",
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
    (
        "ic",
        "bottom_fill_blend_m",
    ): "Metres over which tracers below a source column's bottom blend from that column's "
    "own bottom value to the value of the nearest source column at the same depth. Blank = "
    "roms-tools' default, right for most domains. Use 100 or more for shelf-only "
    "domains, especially on a grid much finer than its source. '0' takes the neighbour's "
    "value at once; 'inf' copies each column's bottom value down.",
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
        "blend_fallback",
    ): "Global product that fills every grid point the regional source does not cover. "
    "'none' uses the regional source alone, which needs it to cover the whole domain.",
    (
        "surface",
        "blend_fallback_correct_radiation",
    ): "Apply the shortwave and longwave radiation corrections to the fallback product. "
    "The regional source takes none.",
    (
        "surface",
        "blend_width_km",
    ): "Width of the band, just inside the regional source's edge, over which the two "
    "products are averaged; 0 switches at the edge.",
    (
        "surface",
        "blend_profile",
    ): "How the weight moves from the regional source to the fallback across the band: "
    "'smoothstep' eases in and out, 'linear' changes at a constant rate.",
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
    "'UNIFIED' / 'WOA_BGC' (BGC). Resolved via the catalog alias map.",
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
    "Density methods build a physics BoundaryForcing companion to supply the target T/S "
    "and place the BGC source on every physics record: the source data is still a "
    "climatology cycled by day of year, but its vertical placement follows the "
    "physics, so the output is written on the physics time axis (grouped like the "
    "physics files) instead of as one cycled climatology. 'depth' keeps a "
    "climatological source as a cycled climatology. Without a physics companion or "
    "source T/S, density falls back to depth.",
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
    (
        "boundary",
        "bottom_fill_blend_m",
    ): "Metres over which tracers below a source column's bottom blend from that column's "
    "own bottom value to the value of the nearest source column at the same depth. Blank = "
    "roms-tools' default, right for most domains. Use 0 to 25 on deep, steep-slope "
    "domains, and 100 or more for shelf-only domains, especially on a grid much finer than "
    "its source. '0' takes the neighbour's value at once; 'inf' copies each column's bottom "
    "value down.",
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
    "daily discharge dataset (higher resolution, more rivers); Forge downloads the "
    "preprocessed file (about 10 GB) into source_data/GLOFAS/ the first time it is "
    "needed.",
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
        "surface_forcing_source_name",
    ): "Derive river temperature from ERA5 air temperature sampled at each river "
    "mouth (smoothed, floored at 0 °C) instead of roms-tools' flat constant "
    "15.6 °C default. Blank keeps the constant.",
    (
        "river",
        "surface_forcing_source_path",
    ): "Optional explicit ERA5 path/glob. Blank reads the remote ARCO ERA5 archive "
    "roms-tools defaults to -- no local staging required.",
    (
        "river",
        "river_temp_smoothing_window_days",
    ): "Rolling-mean window (days) applied to the sampled air temperature, a "
    "deliberately simple stand-in for a river's thermal inertia. Default: 30.",
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
    "streams. Fine-tune any value in the streams below. Particle tracking, "
    "pressure-flux and child-grid extraction settings belong to the model/grid and "
    "are kept when you pick a preset.",
    # ---- timestep --------------------------------------------------------------
    (
        "timestep",
        "dt_btn",
    ): "Go back to the time step derived from the CFL criterion for the current grid "
    "size and resolution; it then follows later grid edits. No grid is built, so it "
    "works while other settings are still incomplete.",
}


def _tip(context: str, field: str) -> str:
    """Return the help text for a (context, field) pair, falling back to ''."""
    return HELP_TEXT.get((context, field), HELP_TEXT.get(field, ""))


# ``Optional[X]`` and ``X | None`` have different origins.
_UNION_ORIGINS = (typing.Union, types.UnionType)


def _unwrap_type(ann):
    """Reduce ``Optional[X]`` / ``Annotated[X, ...]`` / ``List[X]`` to a base type
    (``bool``/``int``/``float``/``str``/``list``), best-effort.

    A union of several types (``list[str] | str``) is returned as is, so the
    widget follows the field's current value instead of the first member.
    """
    if getattr(ann, "__metadata__", None) is not None:  # Annotated[...]
        ann = get_args(ann)[0]
    origin = get_origin(ann)
    if origin in _UNION_ORIGINS:
        non_none = [a for a in get_args(ann) if a is not type(None)]
        if len(non_none) == 1:
            return _unwrap_type(non_none[0])
    if origin in (list, list):
        return list
    return ann


def _is_optional_numeric(ann) -> bool:
    """Whether ``ann`` is ``Optional[int]`` / ``Optional[float]`` (through
    ``Annotated``): a numeric field whose ``None`` a number box cannot show.
    """
    if getattr(ann, "__metadata__", None) is not None:  # Annotated[...]
        ann = get_args(ann)[0]
    if get_origin(ann) not in _UNION_ORIGINS:
        return False
    args = get_args(ann)
    return type(None) in args and _unwrap_type(ann) in (int, float)


def _is_optional_number_widget(widget, base: type) -> bool:
    """Whether ``widget`` is the blank-able text box of an optional numeric field
    (a numeric field otherwise gets a number box, whose value is never a ``str``).
    """
    return base in (int, float) and isinstance(widget.value, str)


def _literal_choices(ann) -> tuple[str, ...] | None:
    """The options of a ``Literal`` of strings annotation (through ``Optional`` /
    ``Annotated``), else ``None``: the one source for a dropdown's choices.
    """
    base = _unwrap_type(ann)
    if get_origin(base) is typing.Literal:
        choices = get_args(base)
        if all(isinstance(c, str) for c in choices):
            return choices
    return None


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
    ("cdr_lite_output", "do_avg"): ("averaged", "instantaneous"),
    ("cdr_lite_output", "monthly_averages"): ("monthly", "periodic"),
    ("cdr_gas_exch_output", "do_avg"): ("averaged", "instantaneous"),
    ("cdr_gas_exch_output", "monthly_averages"): ("monthly", "periodic"),
}


def _make_field_widget(
    W,
    name: str,
    base: type,
    value: Any,
    tooltip: str = "",
    bool_dropdown: tuple[str, str] | None = None,
    choices: tuple[str, ...] | None = None,
    optional: bool = False,
    multiline: bool = False,
):
    style = {"description_width": "260px"}
    wide = W.Layout(width="560px")
    num = W.Layout(width="380px")
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
    # A value outside the choices (a hand-edited blueprint) gets a Text box instead,
    # which can show it; the live validation reports it.
    if choices is not None and value in choices:
        return W.Dropdown(
            options=choices,
            value=value,
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
    if optional and base in (int, float):
        return W.Text(
            value="" if value is None else str(value),
            description=name,
            style=style,
            layout=num,
            placeholder="(blank: automatic)",
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
        if multiline:  # a long list wraps, so all of it shows
            return W.Textarea(
                value=joined,
                description=name,
                style=style,
                layout=wide,
                rows=4,
                placeholder="comma-separated",
                **kw,
            )
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
    if _is_optional_number_widget(widget, base):
        return _parse_optional_number(v, base)
    if base is int:
        return int(v)
    if base is float:
        return float(v)
    if base is list:
        # Commas or line breaks (a multiline list box, see _make_field_widget).
        parts = [p.strip() for p in re.split(r"[,\n]", str(v)) if p.strip()]
        out: list[int | float | str] = []
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


def _parse_optional_number(text: str, base: type) -> Any:
    """Parse a blank-able numeric text box (blank → ``None``, i.e. unset).

    Text ``base`` (``int`` or ``float``) accepts → that number; anything else → the
    stripped text, so blueprint validation reports it instead of dropping it.
    """
    text = (text or "").strip()
    if not text:
        return None
    try:
        return base(text)
    except ValueError:
        return text


# ===========================================================================
# Catalog dropdowns: the spec pickers of every card (model, domain, forcing, ...)
# ===========================================================================


def _dd_options(
    catalog: Any,
    names: list[str],
    kind: str,
    *,
    prefix: list[str] | None = None,
    archived: Collection[str] = (),
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
    names drawn from the same catalog) is treated as "no badge". Names in
    *archived* are labelled ``archived``, combined with the layer badge as
    ``name (layer, archived)``.
    """
    prefix = prefix or []
    entry_source = getattr(catalog, "entry_source", None)
    top = getattr(catalog, "top", None)
    top_label = top.label if top is not None else None

    badges: dict[str, list[str]] = {}
    if entry_source is not None:
        for name in names:
            try:
                source = entry_source(kind, name)
            except KeyError:
                continue
            if source != top_label:
                badges[name] = [source]
    for name in names:
        if name in archived:
            badges.setdefault(name, []).append("archived")

    if not badges:
        return [*prefix, *names]

    options: list[Any] = [(p, p) for p in prefix]
    for name in names:
        label = f"{name} ({', '.join(badges[name])})" if name in badges else name
        options.append((label, name))
    return options


def _dd_values(dd: Any) -> list:
    """Bare values of a dropdown's ``options``, whether badged
    ``(label, value)`` tuples (see ``_dd_options``) or plain strings
    (ipywidgets' label == value shorthand). Use this instead of reading
    ``dd.options`` directly for membership tests / indexing.
    """
    return [o[1] if isinstance(o, tuple) else o for o in dd.options]


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
    short = short_digest(file_dict["content_hash"])
    return (
        components.status_span("ok", f"✓ attached {name} (sha256 {short}…)")
        + "<br>"
        + components.status_span(
            "warn",
            "⚠ This file must exist at this exact path "
            "on the machine where the executor runs.",
        )
    )


# Shown in a user-provided-file status slot until a file is attached, where the
# blueprint is invalid without one (a CUSTOM_FILE river row; CDR mode "netcdf").
_FILE_NOT_ATTACHED_HINT = components.status_span(
    "warn",
    "No file attached yet -- enter a path and press "
    "Enter (or click Attach / upload a file). The blueprint is invalid until "
    "a file is attached.",
)
_RIVER_CUSTOM_FILE_HINT = _FILE_NOT_ATTACHED_HINT
_CDR_FILE_HINT = _FILE_NOT_ATTACHED_HINT
# The carbonate-sensitivity twin (a directory, so no upload): shown while the
# source is "path" and nothing is attached, where the blueprint is invalid.
_CARBONATE_DIR_HINT = components.status_span(
    "warn",
    "No directory attached yet -- enter a path and "
    "press Enter (or click Attach). The blueprint is invalid until a directory "
    "is attached.",
)


def _carbonate_status_html(n_files: int, detail: str = "") -> str:
    """Status HTML for ``n_files`` attached carbonate sensitivity files: the
    count (plus ``detail``, e.g. the timestamp span) and the persistent
    host-path warning -- the files are host/transport, like every
    user-provided file (see ``UserProvidedFile``).
    """
    suffix = f", {detail}" if detail else ""
    return (
        components.status_span(
            "ok",
            f"✓ attached {n_files} carbonate sensitivity file(s){suffix}",
        )
        + "<br>"
        + components.status_span(
            "warn",
            "⚠ These files must exist at these exact "
            "paths on the machine where the executor runs.",
        )
    )


def _same_attached_file(attached: dict[str, Any] | None, path_str: str) -> bool:
    """True when ``path_str`` resolves to the already-attached file dict's
    location -- the dedupe every path-Text submit observer uses so re-submitting
    (or programmatically re-setting) the same path doesn't re-hash/re-load.
    """
    if not attached:
        return False
    try:
        return attached["location"] == str(Path(path_str).expanduser().resolve())
    except (OSError, RuntimeError):
        return False


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


def _schedule_coroutine(coro):
    """Schedule a coroutine on the running loop (returns a Task), or run it to
    completion directly if there is no running loop. Mirrors
    ``cstar.applications.forge.executor._schedule_coroutine`` -- needed for seamless
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
