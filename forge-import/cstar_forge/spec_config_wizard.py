"""
An ``ipywidgets`` wizard for assembling and reviewing a :class:`SpecConfig`.

This is a thin UI shell over :func:`cstar_forge.spec_config_resolve.build_spec_config`:
the widgets only *collect inputs and display the resolved result* — all resolution
and validation stay in the resolver. That keeps the notebook UI interchangeable with
any future app/WASM front-end and lets the logic be tested without rendering.

Usage (in a Jupyter notebook)::

    from cstar_forge.spec_config_wizard import SpecConfigWizard
    wiz = SpecConfigWizard()
    wiz.display()
    # ... pick a model + domain, tweak fields, review the live YAML, Save ...
    cfg = wiz.config            # the current resolved SpecConfig (or None if invalid)

The wizard discovers Models from ``catalog/ModelSpec/`` and Domains from
``catalog/DomainSpec/``; selecting a cataloged Domain prefills the grid kwargs,
boundaries, partitioning, and dates. The live preview re-resolves on every change
(using the ``dt`` field, so it never builds a grid); the "Compute dt (CFL)" button
is the only action that builds a grid (needs ``roms_tools``).
"""

from __future__ import annotations

import base64
import typing
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, get_args, get_origin

import yaml
from pydantic import BaseModel

from .namelist_model import RunTimeSettings, validate_run_time_sections
from .spec_config import Composition, PieceRef, SpecConfig
from .spec_config_resolve import (
    OUTPUT_MARBL_FIELDS,
    OUTPUT_SECTIONS,
    build_spec_config,
    load_model_spec_data,
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
HELP_TEXT: Dict[str, str] = {
    # ---- identity / run window ------------------------------------------------
    ("run", "model_ref_date"):
        "ROMS model t=0 reference date. All time values in the output files are relative "
        "to this date. Passed to every roms-tools object (InitialConditions, SurfaceForcing, "
        "BoundaryForcing, TidalForcing, CDRForcing). Default: 2000-01-01.",
    ("run", "start"):
        "Start date of the simulation run window.",
    ("run", "end"):
        "End date of the simulation run window.",
    ("run", "dt"):
        "Barotropic time step in seconds. Leave blank to compute from the CFL criterion "
        "(click 'Compute dt (CFL)' — requires roms_tools).",
    ("run", "description"):
        "Human-readable description of this blueprint.",
    ("run", "ensemble_id"):
        "Optional integer ensemble member identifier. Appended to the domain name "
        "as a zero-padded three-digit suffix (e.g. _003).",

    # ---- grid ------------------------------------------------------------------
    ("grid", "nx"):
        "Number of horizontal grid points in the x-direction (longitude).",
    ("grid", "ny"):
        "Number of horizontal grid points in the y-direction (latitude).",
    ("grid", "size_x"):
        "Domain extent in the x-direction in kilometers.",
    ("grid", "size_y"):
        "Domain extent in the y-direction in kilometers.",
    ("grid", "center_lon"):
        "Longitude of the domain center in degrees East.",
    ("grid", "center_lat"):
        "Latitude of the domain center in degrees North.",
    ("grid", "rot"):
        "Rotation of the grid x-axis from lines of constant latitude, in degrees "
        "(positive = counterclockwise).",
    ("grid", "N"):
        "Number of vertical sigma levels.",
    ("grid", "theta_s"):
        "S-coordinate surface stretching parameter (0 < θs ≤ 10). "
        "Larger values concentrate levels near the surface.",
    ("grid", "theta_b"):
        "S-coordinate bottom stretching parameter (0 < θb ≤ 4). "
        "Larger values concentrate levels near the bottom.",
    ("grid", "hc"):
        "S-coordinate critical depth in meters. Controls the transition from "
        "sigma to stretched coordinates near the surface.",
    ("grid", "hmin"):
        "Minimum allowable ocean depth in meters (default 5.0). Grid cells shallower "
        "than hmin are set to exactly hmin during grid generation.",
    ("grid", "close_narrow_channels"):
        "If checked, narrow water channels (width < one grid cell) in the land mask "
        "are closed after the mask is generated from NaturalEarth coastlines.",
    ("grid", "mask_shapefile"):
        "Path to a custom shapefile used to determine the land/sea mask instead of the "
        "default NaturalEarth 10 m coastlines. Leave blank to use the default.",

    # ---- nesting ---------------------------------------------------------------
    ("nesting", "nest_enable"):
        "Enable a child (nested) domain. Generates nesting.nc to supply boundary "
        "conditions for the inner domain from the outer model.",
    ("nesting", "nest_domain_dd"):
        "Optionally prefill the child grid kwargs from a cataloged DomainSpec. "
        "You can still edit any child-grid field after selecting.",
    ("nesting", "nest_period"):
        "How often (seconds) boundary values are written to nesting.nc. "
        "Must divide evenly into the parent model output interval.",
    ("nesting", "nest_pressure_fluxes"):
        "Include baroclinic pressure flux variables in nesting.nc. Required when "
        "the namelist calc_pflx setting is enabled in the child domain.",

    # ---- partitioning ----------------------------------------------------------
    ("domain", "npx"):
        "Number of MPI tiles in the x-direction (xi). Total MPI tasks = npx × npy.",
    ("domain", "npy"):
        "Number of MPI tiles in the y-direction (eta). Total MPI tasks = npx × npy.",

    # ---- IC --------------------------------------------------------------------
    ("ic", "ic_name"):
        "Logical source name for physics initial conditions, e.g. 'GLORYS'. "
        "The catalog alias map resolves this to the actual dataset registry key.",
    ("ic", "ic_layout"):
        "GLORYS spatial layout override: 'regional' (default) or 'global'. "
        "Leave blank for non-GLORYS sources.",
    ("ic", "ic_bgc_name"):
        "Logical source name for BGC initial conditions, e.g. 'UNIFIED'. "
        "Leave blank to omit BGC initial conditions.",
    ("ic", "ic_bgc_clim"):
        "Use the BGC source as a climatology (annual-mean repeated each year) "
        "rather than a time-varying dataset.",
    ("ic", "ic_density_interp"):
        "Interpolate BGC tracer initial conditions in density space rather than depth "
        "space. Reduces interpolation errors across sloping isopycnals.",
    ("ic", "ic_flex_time"):
        "Allow a ±24-hour search window when looking for the requested ini_time in the "
        "source dataset. Useful when the exact timestamp is absent.",

    # ---- surface forcing -------------------------------------------------------
    ("surface", "name"):
        "Logical source name, e.g. 'ERA5' for surface physics or 'UNIFIED' for BGC surface. "
        "The catalog alias map resolves this to the actual dataset.",
    ("surface", "type"):
        "Forcing type: 'physics' (u/v wind, heat, freshwater fluxes), 'bgc' (pCO₂ / "
        "iron deposition), or 'restoring' (SST or SSS relaxation).",
    ("surface", "climatology"):
        "Treat the source as a climatology repeated each year rather than interannual.",
    ("surface", "glorys_layout"):
        "GLORYS spatial layout: 'regional' (default) or 'global'. Leave blank for ERA5/UNIFIED.",
    ("surface", "correct_radiation"):
        "Apply shortwave and longwave radiation corrections to ERA5 fluxes using an "
        "observational climatology. Recommended for most physics surface forcing.",
    ("surface", "wind_dropoff"):
        "Apply exponential coastal wind-speed reduction with a 12.5 km e-folding "
        "scale. Reduces spurious upwelling near the coast from gridded wind products.",
    ("surface", "coarse_grid_mode"):
        "'auto' — interpolate onto a factor-2 coarsened grid only when roms-tools detects "
        "the source is coarser than the ROMS grid (default). 'always' — always coarsen. "
        "'never' — always use the full-resolution source.",
    ("surface", "restoring_forces"):
        "Comma-separated list of variables to apply restoring forces to, e.g. 'sss' or "
        "'sst,sss'. Only relevant for type='restoring'. Enables sal_restore/SST correction.",

    # ---- boundary forcing ------------------------------------------------------
    ("boundary", "name"):
        "Logical source name for boundary conditions, e.g. 'GLORYS' (physics) or "
        "'UNIFIED' (BGC). Resolved via the catalog alias map.",
    ("boundary", "type"):
        "Boundary forcing type: 'physics' (T, S, u, v, ζ) or 'bgc' (BGC tracers).",
    ("boundary", "climatology"):
        "Treat as an annual climatology rather than interannual time series.",
    ("boundary", "glorys_layout"):
        "GLORYS spatial layout: 'regional' or 'global'. Leave blank for non-GLORYS.",
    ("boundary", "apply_2d_horizontal_fill"):
        "Perform 2D horizontal fill on the source data before regridding to the ROMS "
        "open boundaries. Fills land-contaminated grid cells near the coast.",
    ("boundary", "use_density_interpolation"):
        "Interpolate BGC boundary conditions in density space rather than depth "
        "space. Reduces errors across sloping isopycnals at open boundaries.",

    # ---- tidal forcing ---------------------------------------------------------
    ("tidal", "name"):
        "Tidal forcing dataset name. Currently 'TPXO' (TPXO10.v2, user-provided). "
        "Files must exist under source_data/TPXO/TPXO10.v2/.",
    ("tidal", "ntides"):
        "Number of tidal constituents to include (max 15). The TPXO atlas provides M2, "
        "S2, N2, K2, K1, O1, P1, Q1, Mf, Mm, M4, MS4, MN4, Mtm, Mf. Default: 10.",

    # ---- river forcing ---------------------------------------------------------
    ("river", "name"):
        "River dataset source name. 'DAI' uses the Dai-Trenberth global river discharge "
        "climatology (streamable, no download required).",
    ("river", "climatology"):
        "Treat river data as an annual climatology repeated each year.",
    ("river", "include_bgc"):
        "Include BGC tracer concentrations (e.g. nutrients) in the river forcing. "
        "Requires a BGC river dataset (bgc_source configured in model.yml).",
    ("river", "convert_to_climatology"):
        "When to compute a river climatology from the raw data: "
        "'if_any_missing' (default) — compute if any requested months are absent, "
        "'never' — always use raw data, 'always' — always compute a climatology.",

    # ---- output settings -------------------------------------------------------
    ("output", "output_dd"):
        "Select an OutputSpec (named output-settings configuration) to seed the output "
        "sections. Fine-tune any value under Advanced settings → the relevant section.",

    # ---- timestep --------------------------------------------------------------
    ("timestep", "dt_btn"):
        "Build the grid from the current grid kwargs and compute the barotropic "
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
    (``bool``/``int``/``float``/``str``/``list``), best-effort."""
    if getattr(ann, "__metadata__", None) is not None:  # Annotated[...]
        ann = get_args(ann)[0]
    origin = get_origin(ann)
    if origin is typing.Union:
        non_none = [a for a in get_args(ann) if a is not type(None)]
        if non_none:
            return _unwrap_type(non_none[0])
    if origin in (list, List):
        return list
    return ann


def _base_type(ann, value):
    """The widget type to use for a field: from the annotation if known, else
    inferred from the current value."""
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
        return W.Checkbox(value=bool(value) if value is not None else False,
                          description=name, indent=False, **kw)
    if base is int:
        return W.IntText(value=int(value) if value is not None else 0,
                         description=name, style=style, layout=num, **kw)
    if base is float:
        return W.FloatText(value=float(value) if value is not None else 0.0,
                           description=name, style=style, layout=num, **kw)
    if base is list:
        joined = ", ".join(str(x) for x in (value or []))
        return W.Text(value=joined, description=name, style=style, layout=wide,
                      placeholder="comma-separated", **kw)
    return W.Text(value="" if value is None else str(value),
                  description=name, style=style, layout=wide, **kw)


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
def _apply_overrides(composed: Dict[str, Any], overrides: Dict[Any, Any]) -> Dict[str, Any]:
    import copy as _copy
    eff = _copy.deepcopy(composed)
    for (section, field), value in overrides.items():
        if field is None:
            eff[section] = value
        else:
            eff.setdefault(section, {})[field] = value
    return eff


def _overrides_nested(overrides: Dict[Any, Any]) -> Dict[str, Any]:
    """Convert the sparse (section, field)->value map to nested {section:{field:value}}
    (or {section: scalar}) for storage in composition.overrides."""
    out: Dict[str, Any] = {}
    for (section, field), value in overrides.items():
        if field is None:
            out[section] = value
        else:
            out.setdefault(section, {})[field] = value
    return out


def _diff_overrides(effective: Dict[str, Any], composed: Dict[str, Any]) -> Dict[Any, Any]:
    """Every field in ``effective`` that differs from ``composed`` becomes an override
    (used on load to reconstruct the layer from a saved/edited config)."""
    ov: Dict[Any, Any] = {}
    for section, val in effective.items():
        cval = composed.get(section)
        if isinstance(val, dict):
            for field, v in val.items():
                if not isinstance(cval, dict) or cval.get(field) != v:
                    ov[(section, field)] = v
        elif cval != val:
            ov[(section, None)] = val
    return ov


class _SettingsEditor:
    """A collapsible (Accordion) editor over the *editable* model_settings sections.

    Auto-generates typed widgets per field using the ``RunTimeSettings`` sub-model
    schema (falling back to value-type inference, e.g. for ``cppdefs``). All panes are
    collapsed by default. ``gather()`` returns the edited sections; ``set_from()``
    pushes values in (used on load)."""

    def __init__(self, W, model_settings: Dict[str, Any], sections: List[str], on_edit=None):
        self.W = W
        # (section, field|None) -> (widget, base_type)
        self._widgets: Dict[Any, Any] = {}
        self._section_fields: Dict[str, List[Optional[str]]] = {}
        panes, titles = [], []
        for section in sections:
            if section not in model_settings:
                continue
            value = model_settings[section]
            box, fields = self._build_section(section, value)
            panes.append(box)
            titles.append(section)
            self._section_fields[section] = fields
        self.accordion = W.Accordion(children=panes, selected_index=None)
        for i, title in enumerate(titles):
            self.accordion.set_title(i, title)
        if on_edit is not None:
            for (section, field), (widget, _base) in self._widgets.items():
                widget.observe(
                    lambda _ch, s=section, f=field: on_edit(s, f), names="value")

    def sync(self, model_settings: Dict[str, Any]):
        """Set every widget to the effective values (caller suspends edit tracking)."""
        for (section, field), (widget, base) in self._widgets.items():
            if section not in model_settings:
                continue
            sec = model_settings[section]
            value = sec if field is None else (sec.get(field) if isinstance(sec, dict) else None)
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

    def _build_section(self, section: str, value: Any):
        W = self.W
        sub = _section_submodel(section)
        if not isinstance(value, dict):  # scalar section (e.g. gamma2, ubind)
            base = _base_type(None, value)
            tip = _namelist_tooltip(section, section)
            w = _make_field_widget(W, section, base, value, tooltip=tip)
            self._widgets[(section, None)] = (w, base)
            return W.VBox([w]), [None]
        rows, fields = [], []
        for key, val in value.items():
            ann = sub.model_fields[key].annotation if (sub and key in sub.model_fields) else None
            base = _base_type(ann, val)
            tip = _namelist_tooltip(section, key)
            w = _make_field_widget(W, key, base, val, tooltip=tip)
            self._widgets[(section, key)] = (w, base)
            rows.append(w)
            fields.append(key)
        return W.VBox(rows), fields


from .spec_config import (
    BgcBoundarySource,
    BgcInitialConditionsSource,
    BgcSurfaceSource,
    BoundaryType,
    ClimatologyMode,
    CoarseGridMode,
    InitialConditionsSource,
    PhysicsBoundarySource,
    PhysicsSurfaceSource,
    RestoringForce,
    RestoringSurfaceSource,
    RiverSource,
    SurfaceType,
    TidalSource,
    TopographySource,
)

# Dropdown option lists derived from the enums so the wizard and schema stay in sync
_SURFACE_TYPES = [e.value for e in SurfaceType]
_BOUNDARY_TYPES = [e.value for e in BoundaryType]
_COARSE_MODES = [e.value for e in CoarseGridMode]
_FORCING_CATEGORIES = ("surface", "boundary", "tidal", "river")
_GLORYS_LAYOUT_OPTS = ["", "regional", "global"]  # "" = not specified

# Valid source names per (category, type).  Drives name dropdowns in the forcing editor.
_SOURCE_OPTS: Dict[Any, List[str]] = {
    ("surface", SurfaceType.PHYSICS.value): [e.value for e in PhysicsSurfaceSource],
    ("surface", SurfaceType.BGC.value):     [e.value for e in BgcSurfaceSource],
    ("surface", SurfaceType.RESTORING.value): [e.value for e in RestoringSurfaceSource],
    ("boundary", BoundaryType.PHYSICS.value): [e.value for e in PhysicsBoundarySource],
    ("boundary", BoundaryType.BGC.value):     [e.value for e in BgcBoundarySource],
    ("tidal", None): [e.value for e in TidalSource],
    ("river", None): [e.value for e in RiverSource],
}
_IC_SOURCE_OPTS = [e.value for e in InitialConditionsSource]
_IC_BGC_SOURCE_OPTS = [""] + [e.value for e in BgcInitialConditionsSource]


def _source_opts_for(cat: str, type_val: Optional[str]) -> List[str]:
    """Return the valid source names for a given forcing category and type."""
    return _SOURCE_OPTS.get((cat, type_val), _SOURCE_OPTS.get((cat, None), []))


class _ForcingEditor:
    """Editor for the forcing piece: initial conditions + per-category forcing items,
    with add/remove. ``gather()`` returns an ``inputs``-shaped dict the resolver
    accepts via ``forcing_inputs=``."""

    def __init__(self, W, forcing_inputs: Dict[str, Any], on_change):
        self.W = W
        self.on_change = on_change
        fi = forcing_inputs or {}
        self._topo = (fi.get("grid", {}) or {}).get("topography_source", "ETOPO5")
        ic = fi.get("initial_conditions", {}) or {}
        forc = fi.get("forcing", {}) or {}

        # initial conditions
        _ic_name_val = str((ic.get("source") or {}).get("name", _IC_SOURCE_OPTS[0]))
        if _ic_name_val not in _IC_SOURCE_OPTS:
            _ic_name_val = _IC_SOURCE_OPTS[0]
        self.ic_name = W.Dropdown(options=_IC_SOURCE_OPTS, value=_ic_name_val,
                                  description="IC source:", style={"description_width": "110px"},
                                  tooltip=_tip("ic", "ic_name"))
        _ic_layout_val = str((ic.get("source") or {}).get("glorys_layout") or "")
        if _ic_layout_val not in _GLORYS_LAYOUT_OPTS:
            _ic_layout_val = ""
        self.ic_layout = W.Dropdown(options=_GLORYS_LAYOUT_OPTS, value=_ic_layout_val,
                                    description="glorys_layout:", style={"description_width": "110px"},
                                    tooltip=_tip("ic", "ic_layout"))
        bgc = ic.get("bgc_source") or {}
        _ic_bgc_val = str(bgc.get("name", "") or "")
        if _ic_bgc_val not in _IC_BGC_SOURCE_OPTS:
            _ic_bgc_val = ""
        self.ic_bgc_name = W.Dropdown(options=_IC_BGC_SOURCE_OPTS, value=_ic_bgc_val,
                                      description="IC bgc src:", style={"description_width": "110px"},
                                      tooltip=_tip("ic", "ic_bgc_name"))
        self.ic_bgc_clim = W.Checkbox(value=bool(bgc.get("climatology", False)),
                                      description="bgc climatology", indent=False,
                                      tooltip=_tip("ic", "ic_bgc_clim"))
        self.ic_density_interp = W.Checkbox(
            value=bool(ic.get("use_density_interpolation", False)),
            description="density interp", indent=False,
            tooltip=_tip("ic", "ic_density_interp"))
        self.ic_flex_time = W.Checkbox(
            value=bool(ic.get("allow_flex_time", False)),
            description="flex time", indent=False,
            tooltip=_tip("ic", "ic_flex_time"))
        for _w in (self.ic_name, self.ic_layout, self.ic_bgc_name,
                   self.ic_bgc_clim, self.ic_density_interp, self.ic_flex_time):
            _w.observe(lambda _ch: on_change(), names="value")

        # per-category item rows: list of dicts of widgets
        self._rows: Dict[str, list] = {c: [] for c in _FORCING_CATEGORIES}
        self._containers: Dict[str, Any] = {}
        for cat in _FORCING_CATEGORIES:
            container = W.VBox([])
            self._containers[cat] = container
            for item in (forc.get(cat, []) or []):
                self._rows[cat].append(self._make_row(cat, item))
            self._render(cat)

    # ---- one item row --------------------------------------------------------
    def _make_row(self, cat: str, item: Dict[str, Any]):
        W = self.W
        src = item.get("source") or {}
        w: Dict[str, Any] = {}
        small = {"description_width": "70px"}

        # Source name: Dropdown driven by category + type (for surface/boundary) or fixed.
        _cur_type = item.get("type", "physics")
        _name_opts = _source_opts_for(cat, _cur_type)
        _name_val = str(src.get("name", ""))
        if _name_val not in _name_opts and _name_opts:
            _name_val = _name_opts[0]
        w["name"] = W.Dropdown(options=_name_opts or [""],
                               value=_name_val if _name_val in (_name_opts or [""]) else (_name_opts or [""])[0],
                               description="src:", style=small,
                               layout=W.Layout(width="160px"),
                               tooltip=_tip(cat, "name"))

        if cat in ("surface", "boundary"):
            _type_opts = _SURFACE_TYPES if cat == "surface" else _BOUNDARY_TYPES
            _type_val = _cur_type if _cur_type in _type_opts else _type_opts[0]
            w["type"] = W.Dropdown(options=_type_opts, value=_type_val, description="type:",
                                   style=small, layout=W.Layout(width="160px"),
                                   tooltip=_tip(cat, "type"))

            # When type changes → update the source name dropdown to the valid options.
            def _on_type_change(change, name_dd=w["name"], c=cat):
                new_opts = _source_opts_for(c, change["new"])
                name_dd.options = new_opts or [""]
                if name_dd.value not in name_dd.options:
                    name_dd.value = name_dd.options[0]
                self.on_change()
            w["type"].observe(_on_type_change, names="value")

            w["climatology"] = W.Checkbox(value=bool(src.get("climatology", False)),
                                          description="clim", indent=False,
                                          tooltip=_tip(cat, "climatology"))
            _layout_val = str(src.get("glorys_layout") or "")
            if _layout_val not in _GLORYS_LAYOUT_OPTS:
                _layout_val = ""
            w["glorys_layout"] = W.Dropdown(options=_GLORYS_LAYOUT_OPTS, value=_layout_val,
                                            description="layout:", style=small,
                                            layout=W.Layout(width="150px"),
                                            tooltip=_tip(cat, "glorys_layout"))
        if cat == "surface":
            w["correct_radiation"] = W.Checkbox(value=bool(item.get("correct_radiation", False)),
                                                description="corr_rad", indent=False,
                                                tooltip=_tip("surface", "correct_radiation"))
            w["wind_dropoff"] = W.Checkbox(value=bool(item.get("wind_dropoff", False)),
                                           description="wind_dropoff", indent=False,
                                           tooltip=_tip("surface", "wind_dropoff"))
            w["coarse_grid_mode"] = W.Dropdown(options=_COARSE_MODES,
                                               value=item.get("coarse_grid_mode", "auto"),
                                               description="coarse:", style=small,
                                               layout=W.Layout(width="150px"),
                                               tooltip=_tip("surface", "coarse_grid_mode"))
            w["restoring_forces"] = W.Text(value=", ".join(item.get("restoring_forces") or []),
                                           description="restore:", style=small,
                                           layout=W.Layout(width="150px"), placeholder="sss,sst",
                                           tooltip=_tip("surface", "restoring_forces"))
        if cat == "boundary":
            w["apply_2d_horizontal_fill"] = W.Checkbox(
                value=bool(item.get("apply_2d_horizontal_fill", False)),
                description="2d_fill", indent=False,
                tooltip=_tip("boundary", "apply_2d_horizontal_fill"))
            w["use_density_interpolation"] = W.Checkbox(
                value=bool(item.get("use_density_interpolation", False)),
                description="dens_interp", indent=False,
                tooltip=_tip("boundary", "use_density_interpolation"))
        if cat == "tidal":
            w["ntides"] = W.IntText(value=int(item.get("ntides") or 0), description="ntides:",
                                    style=small, layout=W.Layout(width="130px"),
                                    tooltip=_tip("tidal", "ntides"))
        if cat == "river":
            w["climatology"] = W.Checkbox(value=bool(src.get("climatology", False)),
                                          description="clim", indent=False,
                                          tooltip=_tip("river", "climatology"))
            w["include_bgc"] = W.Checkbox(value=bool(item.get("include_bgc", False)),
                                          description="bgc", indent=False,
                                          tooltip=_tip("river", "include_bgc"))
            _ctc_opts = [e.value for e in ClimatologyMode]
            _ctc_val = item.get("convert_to_climatology", ClimatologyMode.IF_ANY_MISSING.value)
            w["convert_to_climatology"] = W.Dropdown(options=_ctc_opts,
                value=_ctc_val if _ctc_val in _ctc_opts else ClimatologyMode.IF_ANY_MISSING.value,
                description="clim mode:", style=small, layout=W.Layout(width="180px"),
                tooltip=_tip("river", "convert_to_climatology"))
        remove = W.Button(description="✕", layout=W.Layout(width="36px"), tooltip="Remove this item")
        remove.on_click(lambda _b, c=cat, ws=w: self._remove(c, ws))
        for widget in w.values():
            widget.observe(lambda _ch: self.on_change(), names="value")
        w["_remove_btn"] = remove
        return w

    def _row_box(self, w):
        widgets = [v for k, v in w.items() if k != "_remove_btn"]
        return self.W.HBox(widgets + [w["_remove_btn"]])

    def _render(self, cat: str):
        W = self.W
        add = W.Button(description=f"+ add {cat}", icon="plus", layout=W.Layout(width="130px"))
        add.on_click(lambda _b, c=cat: self._add(c))
        self._containers[cat].children = [self._row_box(w) for w in self._rows[cat]] + [add]

    def _add(self, cat: str):
        self._rows[cat].append(self._make_row(cat, {"source": {"name": ""}}))
        self._render(cat)
        self.on_change()

    def _remove(self, cat: str, ws):
        self._rows[cat] = [w for w in self._rows[cat] if w is not ws]
        self._render(cat)
        self.on_change()

    # ---- gather --------------------------------------------------------------
    def _gather_item(self, cat: str, w) -> Dict[str, Any]:
        src: Dict[str, Any] = {"name": w["name"].value}
        if "climatology" in w and w["climatology"].value:
            src["climatology"] = True
        if "glorys_layout" in w and w["glorys_layout"].value:  # Dropdown: "" = omit
            src["glorys_layout"] = w["glorys_layout"].value
        item: Dict[str, Any] = {"source": src}
        if "type" in w:
            item["type"] = w["type"].value
        if "correct_radiation" in w and w["correct_radiation"].value:
            item["correct_radiation"] = True
        if "wind_dropoff" in w and w["wind_dropoff"].value:
            item["wind_dropoff"] = True
        if "coarse_grid_mode" in w:
            item["coarse_grid_mode"] = w["coarse_grid_mode"].value
        if "restoring_forces" in w and w["restoring_forces"].value.strip():
            item["restoring_forces"] = [p.strip() for p in w["restoring_forces"].value.split(",") if p.strip()]
        if "apply_2d_horizontal_fill" in w and w["apply_2d_horizontal_fill"].value:
            item["apply_2d_horizontal_fill"] = True
        if "use_density_interpolation" in w and w["use_density_interpolation"].value:
            item["use_density_interpolation"] = True
        if "ntides" in w:
            item["ntides"] = int(w["ntides"].value)
        if "include_bgc" in w and w["include_bgc"].value:
            item["include_bgc"] = True
        if "convert_to_climatology" in w:
            item["convert_to_climatology"] = w["convert_to_climatology"].value
        return item

    def gather(self) -> Dict[str, Any]:
        ic_source = {"name": self.ic_name.value}
        if self.ic_layout.value:  # Dropdown: "" means not specified
            ic_source["glorys_layout"] = self.ic_layout.value
        ic: Dict[str, Any] = {"source": ic_source}
        if self.ic_bgc_name.value:  # Dropdown: "" means no bgc source
            ic["bgc_source"] = {"name": self.ic_bgc_name.value,
                                "climatology": bool(self.ic_bgc_clim.value)}
        if self.ic_density_interp.value:
            ic["use_density_interpolation"] = True
        if self.ic_flex_time.value:
            ic["allow_flex_time"] = True
        forcing = {cat: [self._gather_item(cat, w) for w in self._rows[cat]]
                   for cat in _FORCING_CATEGORIES}
        return {"grid": {"topography_source": self._topo},
                "initial_conditions": ic, "forcing": forcing}

    @property
    def widget(self):
        W = self.W
        ic_box = W.VBox([W.HTML("<i>initial conditions</i>"),
                         W.HBox([self.ic_name, self.ic_layout]),
                         W.HBox([self.ic_bgc_name, self.ic_bgc_clim]),
                         W.HBox([self.ic_density_interp, self.ic_flex_time])])
        panes = [ic_box] + [self._containers[c] for c in _FORCING_CATEGORIES]
        acc = W.Accordion(children=panes, selected_index=None)
        for i, title in enumerate(["initial_conditions", *_FORCING_CATEGORIES]):
            acc.set_title(i, title)
        return acc


_GRID_INT = ("nx", "ny", "N")
_GRID_FLOAT = ("size_x", "size_y", "center_lon", "center_lat", "rot")
_SCOORD = ("theta_s", "theta_b", "hc")
_DEFAULT_GRID = dict(nx=6, ny=2, size_x=500.0, size_y=1000.0, center_lon=0.0,
                     center_lat=55.0, rot=10.0, N=3, theta_s=5.0, theta_b=2.0, hc=250.0)


def _get_catalog():
    """Return the bundled DomainCatalog (read-only discovery of pieces)."""
    from .domain_catalog import default_catalog
    return default_catalog


class SpecConfigWizard:
    """Build/curate a :class:`SpecConfig` interactively. ``self.config`` holds the
    latest successfully-resolved config (``None`` while inputs are invalid)."""

    def __init__(self, catalog: Any = None):
        import ipywidgets as W  # imported here so the package doesn't require ipywidgets

        self.W = W
        self.catalog = catalog or _get_catalog()
        self.config: Optional[SpecConfig] = None

        models = list(self.catalog.model_names)
        domains = list(self.catalog.domain_names)

        # --- load / import an existing spec_config.yml ---
        self.load_path = W.Text(value="", placeholder="path to spec_config.yml",
                                description="Load file:", style={"description_width": "110px"},
                                layout=W.Layout(width="420px"))
        self.load_btn = W.Button(description="Load", icon="upload")
        self.upload = W.FileUpload(accept=".yml,.yaml", multiple=False,
                                   description="…or upload")
        self.load_status = W.HTML("")

        # --- piece selectors ---
        self.model_dd = W.Dropdown(options=models, description="Model:",
                                   value=(models[0] if models else None),
                                   style={"description_width": "110px"})
        self.domain_dd = W.Dropdown(options=["<custom>"] + domains, description="Domain:",
                                    value="<custom>", style={"description_width": "110px"})
        self.grid_name = W.Text(value="my-grid", description="Grid name:",
                                style={"description_width": "110px"},
                                tooltip="Short name for this grid configuration. Used in the domain name.")

        # --- grid kwargs ---
        self.grid_w: Dict[str, Any] = {}
        for k in _GRID_INT:
            self.grid_w[k] = W.IntText(value=int(_DEFAULT_GRID[k]), description=f"{k}:",
                                       style={"description_width": "90px"}, layout=W.Layout(width="200px"),
                                       tooltip=_tip("grid", k))
        for k in _GRID_FLOAT + _SCOORD:
            self.grid_w[k] = W.FloatText(value=float(_DEFAULT_GRID[k]), description=f"{k}:",
                                         style={"description_width": "90px"}, layout=W.Layout(width="200px"),
                                         tooltip=_tip("grid", k))
        self.scoord_chk = W.Checkbox(value=True, description="specify s-coord (theta_s/theta_b/hc)",
                                     indent=False,
                                     tooltip="When checked, theta_s, theta_b, and hc are passed to roms-tools "
                                             "to set the vertical stretching. Required for ROMS simulations.")

        # --- boundaries / partitioning ---
        self.bnd = {d: W.Checkbox(value=(d in ("east", "north")), description=d, indent=False,
                                  tooltip=f"Enable the {d} open boundary for ocean exchange.")
                    for d in ("north", "south", "east", "west")}
        self.npx = W.IntText(value=1, description="n_procs_x:", style={"description_width": "90px"},
                             layout=W.Layout(width="200px"), tooltip=_tip("domain", "npx"))
        self.npy = W.IntText(value=1, description="n_procs_y:", style={"description_width": "90px"},
                             layout=W.Layout(width="200px"), tooltip=_tip("domain", "npy"))

        # --- nesting (optional child grid) ---
        self.nest_enable = W.Checkbox(value=False, description="enable nesting (child grid)",
                                      indent=False, tooltip=_tip("nesting", "nest_enable"))
        self.nest_domain_dd = W.Dropdown(options=["<custom>"] + domains,
                                         description="Child from:", value="<custom>",
                                         style={"description_width": "110px"},
                                         tooltip=_tip("nesting", "nest_domain_dd"))
        self.child_w: Dict[str, Any] = {}
        for k in _GRID_INT:
            self.child_w[k] = W.IntText(value=int(_DEFAULT_GRID[k]), description=f"{k}:",
                                        style={"description_width": "90px"}, layout=W.Layout(width="200px"),
                                        tooltip=_tip("grid", k) + " (child/inner grid)")
        for k in _GRID_FLOAT + _SCOORD:
            self.child_w[k] = W.FloatText(value=float(_DEFAULT_GRID[k]), description=f"{k}:",
                                          style={"description_width": "90px"}, layout=W.Layout(width="200px"),
                                          tooltip=_tip("grid", k) + " (child/inner grid)")
        self.nest_period = W.FloatText(value=3600.0, description="extract period (s):",
                                       style={"description_width": "130px"}, layout=W.Layout(width="260px"),
                                       tooltip=_tip("nesting", "nest_period"))
        self.nest_pressure_fluxes = W.Checkbox(value=False,
                                               description="include pressure fluxes",
                                               indent=False,
                                               tooltip=_tip("nesting", "nest_pressure_fluxes"))

        # --- run window ---
        self.start = W.DatePicker(value=date(2012, 1, 1), description="Start:",
                                  style={"description_width": "110px"},
                                  tooltip=_tip("run", "start"))
        self.end = W.DatePicker(value=date(2012, 1, 2), description="End:",
                                style={"description_width": "110px"},
                                tooltip=_tip("run", "end"))
        self.model_ref_date = W.DatePicker(value=date(2000, 1, 1),
                                           description="Model ref date:",
                                           style={"description_width": "130px"},
                                           tooltip=_tip("run", "model_ref_date"))
        self.description = W.Text(value="Generated blueprint", description="Description:",
                                  style={"description_width": "110px"}, layout=W.Layout(width="420px"),
                                  tooltip=_tip("run", "description"))
        self.ensemble = W.Text(value="", description="Ensemble id:", placeholder="(optional int)",
                               style={"description_width": "110px"}, layout=W.Layout(width="260px"),
                               tooltip=_tip("run", "ensemble_id"))

        # --- grid extended options ---
        self.hmin = W.FloatText(value=5.0, description="hmin (m):",
                                style={"description_width": "90px"}, layout=W.Layout(width="200px"),
                                tooltip=_tip("grid", "hmin"))
        self.close_narrow_chk = W.Checkbox(value=False, description="close narrow channels",
                                           indent=False, tooltip=_tip("grid", "close_narrow_channels"))
        self.mask_shapefile = W.Text(value="", description="mask shapefile:",
                                     style={"description_width": "120px"},
                                     layout=W.Layout(width="380px"),
                                     placeholder="path to custom land-mask shapefile (optional)",
                                     tooltip=_tip("grid", "mask_shapefile"))

        # --- timestep ---
        self.dt = W.FloatText(value=7200.0, description="dt (s):",
                              style={"description_width": "90px"}, layout=W.Layout(width="220px"),
                              tooltip=_tip("run", "dt"))
        self.dt_btn = W.Button(description="Compute dt (CFL)", icon="calculator",
                               tooltip=_tip("timestep", "dt_btn"))
        self.dt_status = W.HTML("")

        # --- grid plot ---
        self.plot_btn = W.Button(
            description="Refresh plot", icon="refresh",
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
        self.forcing_dd = W.Dropdown(options=["<model default>"] + list(self.catalog.forcing_names),
                                     value="<model default>", description="Forcing:",
                                     style={"description_width": "110px"},
                                     tooltip="Select a named ForcingSpec from the catalog to seed all forcing "
                                             "fields, or keep '<model default>' to use the model's defaults. "
                                             "Edit individual items in the Forcing section below.")
        self.forcing_box = W.VBox([])
        self._forcing_editor: Optional[_ForcingEditor] = None
        self._forcing_edited = False

        # --- output settings piece (OutputSpec selection) ---
        # The output sections themselves are edited in the Advanced settings accordion;
        # this dropdown selects a named OutputSpec that seeds those sections.
        self.output_dd = W.Dropdown(options=["<model default>"] + list(self.catalog.output_names),
                                    value="<model default>", description="Output:",
                                    style={"description_width": "110px"},
                                    tooltip=_tip("output", "output_dd"))

        # --- advanced settings editor (built lazily on first rebuild) ---
        self.editor: Optional[_SettingsEditor] = None
        self._editor_model: Optional[str] = None
        self.editor_box = W.VBox([])  # placeholder; filled with the editor's accordion
        # sparse manual overrides layer: (section, field|None) -> value
        self._overrides: Dict[Any, Any] = {}
        self._syncing = False  # True while pushing composed values into editor widgets

        self.derived = W.HTML("")
        self.validation = W.HTML("")
        self.preview = W.Output(layout=W.Layout(border="1px solid #ccc", padding="6px",
                                                 max_height="380px", overflow="auto"))
        # Browser download (works in Voilà / JupyterLab without server file access)
        self.download_link = W.HTML("")
        # Save to the server/working-dir filesystem (handy for local or HPC use)
        self.save_path = W.Text(value="spec_config.yml", description="Save to:",
                                style={"description_width": "110px"}, layout=W.Layout(width="420px"))
        self.save_btn = W.Button(description="Save to disk", icon="save")
        self.save_status = W.HTML("")

        self._build_forcing_editor(self._model_default_inputs())
        self._wire()
        self._rebuild()

    # ---- wiring --------------------------------------------------------------
    def _wire(self):
        self.domain_dd.observe(self._on_domain, names="value")
        self.forcing_dd.observe(self._on_forcing_spec, names="value")
        self.dt_btn.on_click(self._on_compute_dt)
        self.plot_btn.on_click(self._on_plot)
        self.save_btn.on_click(self._on_save)
        self.load_btn.on_click(self._on_load_path)
        self.upload.observe(self._on_upload, names="value")
        self.model_dd.observe(self._on_model_change, names="value")
        self.nest_domain_dd.observe(self._on_nest_domain, names="value")
        self.output_dd.observe(self._on_output_spec, names="value")
        watched = [self.grid_name, self.scoord_chk, self.npx, self.npy,
                   self.start, self.end, self.model_ref_date, self.description, self.ensemble, self.dt,
                   self.hmin, self.close_narrow_chk, self.mask_shapefile,
                   self.nest_enable, self.nest_period, self.nest_pressure_fluxes,
                   *self.grid_w.values(), *self.bnd.values(), *self.child_w.values()]
        for w in watched:
            w.observe(self._rebuild, names="value")

    def _on_model_change(self, _change):
        # a different model has different defaults -> existing overrides no longer apply
        if getattr(self, "_suspended", False):
            return
        self._overrides = {}
        if self.forcing_dd.value == "<model default>":  # reseed default forcing for new model
            self._forcing_edited = False
            self._build_forcing_editor(self._model_default_inputs())
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
        gk = (self.catalog.domain_data(name).get("grid_kwargs", {}) or {})
        with self._suspend():
            self.nest_enable.value = True
            for k, w in self.child_w.items():
                if k in gk:
                    w.value = gk[k]
        self._rebuild()

    # ---- forcing piece -------------------------------------------------------
    def _model_default_inputs(self) -> Dict[str, Any]:
        """The selected model's default forcing inputs (from its model.yml)."""
        try:
            data = load_model_spec_data(self.catalog.model_dir(self.model_dd.value))
            return data["model"].get("inputs", {}) or {}
        except Exception:
            return {}

    def _build_forcing_editor(self, base_inputs: Dict[str, Any]):
        self._forcing_editor = _ForcingEditor(self.W, base_inputs, on_change=self._on_forcing_change)
        self.forcing_box.children = [self._forcing_editor.widget]

    def _on_forcing_spec(self, _change):
        """Selecting a ForcingSpec (or <model default>) reseeds the forcing editor."""
        if getattr(self, "_suspended", False):
            return
        name = self.forcing_dd.value
        base = (self._model_default_inputs() if name == "<model default>"
                else self.catalog.forcing_data(name))
        self._forcing_edited = False
        self._build_forcing_editor(base)
        self._rebuild()

    def _on_forcing_change(self):
        if getattr(self, "_suspended", False):
            return
        self._forcing_edited = True
        self._rebuild()

    def _on_output_spec(self, _change):
        """Selecting an OutputSpec seeds the output sections. Clear any manual
        overrides on those sections/fields so the selection takes effect cleanly."""
        if getattr(self, "_suspended", False):
            return
        self._overrides = {
            (s, f): v for (s, f), v in self._overrides.items()
            if not (s in OUTPUT_SECTIONS or (s == "marbl_bgc" and f in OUTPUT_MARBL_FIELDS))
        }
        self._rebuild()

    def _output_settings(self) -> Optional[Dict[str, Any]]:
        """The selected OutputSpec's settings (None = use the model default)."""
        if self.output_dd.value == "<model default>":
            return None
        return self.catalog.output_data(self.output_dd.value)

    def _composition(self) -> Composition:
        dom = (PieceRef(name=self.domain_dd.value, origin="catalog")
               if self.domain_dd.value != "<custom>"
               else PieceRef(name=self.grid_name.value, origin="custom"))
        if self._forcing_edited:
            forcing = PieceRef(
                name=None if self.forcing_dd.value == "<model default>" else self.forcing_dd.value,
                origin="custom", modified=True)
        elif self.forcing_dd.value != "<model default>":
            forcing = PieceRef(name=self.forcing_dd.value, origin="catalog")
        else:
            forcing = PieceRef(name=None, origin="model_default")
        output = (PieceRef(name=self.output_dd.value, origin="catalog")
                  if self.output_dd.value != "<model default>"
                  else PieceRef(name=None, origin="model_default"))
        return Composition(model=PieceRef(name=self.model_dd.value, origin="catalog"),
                           domain=dom, forcing=forcing, output=output)

    @staticmethod
    def _sources_to_inputs(cfg: SpecConfig) -> Dict[str, Any]:
        """Reconstruct an ``inputs``-shaped forcing dict from a SpecConfig's sources
        (reverse of the resolver) so a loaded config seeds the forcing editor."""
        def src(spec):
            d = {"name": spec.name}
            if spec.climatology:
                d["climatology"] = True
            if spec.glorys_layout:
                d["glorys_layout"] = spec.glorys_layout
            return d

        f = cfg.forcing
        ic = {"source": src(f.initial_conditions.source)}
        if f.initial_conditions.bgc_source:
            ic["bgc_source"] = src(f.initial_conditions.bgc_source)
        forcing: Dict[str, Any] = {}
        for cat, items in (("surface", f.surface), ("boundary", f.boundary),
                           ("tidal", f.tidal), ("river", f.river)):
            out = []
            for it in items:
                d: Dict[str, Any] = {"source": src(it.source)}
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
                out.append(d)
            forcing[cat] = out
        return {"grid": {"topography_source": cfg.domain.topography_source},
                "initial_conditions": ic, "forcing": forcing}

    def _on_domain(self, _change):
        """Prefill from a cataloged Domain.yml when one is selected."""
        name = self.domain_dd.value
        if name == "<custom>":
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
        self._rebuild()
        self._on_plot(None)

    class _Suspender:
        def __init__(self, wiz): self.wiz = wiz
        def __enter__(self): self.wiz._suspended = True
        def __exit__(self, *a): self.wiz._suspended = False

    def _suspend(self):
        return SpecConfigWizard._Suspender(self)

    # ---- load / import an existing config ------------------------------------
    def _on_load_path(self, _):
        path = self.load_path.value.strip()
        if not path:
            self.load_status.value = "<span style='color:#b00'>Enter a path first.</span>"
            return
        try:
            cfg = SpecConfig.from_yaml(path)
        except Exception as exc:
            self.load_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _on_upload(self, _change):
        files = self.upload.value
        if not files:
            return
        item = files[0] if isinstance(files, (list, tuple)) else next(iter(files.values()))
        self._load_bytes(bytes(item["content"]))

    def _load_bytes(self, content: bytes):
        """Parse + load a spec_config from raw YAML bytes (browser upload path)."""
        try:
            cfg = SpecConfig.model_validate(yaml.safe_load(content))
        except Exception as exc:
            self.load_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _set_load_status(self, cfg: SpecConfig, loaded_problems):
        msg = f"<span style='color:#080'>Loaded {cfg.casename}</span>"
        if loaded_problems:
            msg += (f" &nbsp;<span style='color:#b00'>⚠ {len(loaded_problems)} invalid "
                    "settings value(s) in the file</span>")
        self.load_status.value = msg

    def _populate_from(self, cfg: SpecConfig):
        """Set the widgets from a loaded SpecConfig, then re-resolve once.

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
            if cfg.identity.model_name in self.model_dd.options:
                self.model_dd.value = cfg.identity.model_name
            self.grid_name.value = cfg.identity.grid_name
            self.ensemble.value = ("" if cfg.identity.ensemble_id is None
                                   else str(cfg.identity.ensemble_id))
            self.description.value = cfg.identity.description
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
            dt = (cfg.model_settings.get("time_stepping", {}) or {}).get("dt")
            if dt is not None:
                self.dt.value = float(dt)
            self._populate_nesting(cfg)
            # forcing: reconstruct the editor from the loaded sources
            forig = cfg.composition.forcing.origin
            fname = cfg.composition.forcing.name
            self._forcing_edited = (forig == "custom")
            self.forcing_dd.value = (fname if forig == "catalog"
                                     and fname in self.forcing_dd.options else "<model default>")
            self._build_forcing_editor(self._sources_to_inputs(cfg))
            # output piece selection
            oname = cfg.composition.output.name
            self.output_dd.value = (oname if cfg.composition.output.origin == "catalog"
                                    and oname in self.output_dd.options else "<model default>")
        # Reconstruct the overrides layer = diff(loaded model_settings, composed). This
        # captures every manual deviation regardless of the file's recorded provenance,
        # making load fully non-lossy.
        try:
            composed = build_spec_config(**self._gather()).model_settings
            self._overrides = _diff_overrides(cfg.model_settings, composed)
        except Exception:
            self._overrides = {}
        self._rebuild()
        return loaded_problems

    # ---- gather + resolve ----------------------------------------------------
    def _gather(self) -> Dict[str, Any]:
        gk: Dict[str, Any] = {}
        for k in _GRID_INT:
            gk[k] = int(self.grid_w[k].value)
        for k in _GRID_FLOAT:
            gk[k] = float(self.grid_w[k].value)
        if self.scoord_chk.value:
            for k in _SCOORD:
                gk[k] = float(self.grid_w[k].value)
        ens = self.ensemble.value.strip()
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
            partitioning={"n_procs_x": int(self.npx.value), "n_procs_y": int(self.npy.value)},
            start_date=datetime.combine(self.start.value, datetime.min.time()),
            end_date=datetime.combine(self.end.value, datetime.min.time()),
            description=self.description.value,
            ensemble_id=int(ens) if ens else None,
            dt=float(self.dt.value),
        )
        if self.model_ref_date.value and self.model_ref_date.value != date(2000, 1, 1):
            kw["model_reference_date"] = datetime.combine(
                self.model_ref_date.value, datetime.min.time())
        if self.nest_enable.value:
            ck: Dict[str, Any] = {}
            for k in _GRID_INT:
                ck[k] = int(self.child_w[k].value)
            for k in _GRID_FLOAT + _SCOORD:
                ck[k] = float(self.child_w[k].value)
            kw["grid_kwargs_child"] = ck
            kw["metadata_child"] = {"period": float(self.nest_period.value)}
            if self.nest_pressure_fluxes.value:
                kw["nesting_include_pressure_fluxes"] = True
        # forcing: <model default> & unedited -> None (resolver uses model.yml inputs);
        # otherwise pass the editor's selection/edits.
        if self._forcing_editor is not None and not (
                self.forcing_dd.value == "<model default>" and not self._forcing_edited):
            kw["forcing_inputs"] = self._forcing_editor.gather()
        output_settings = self._output_settings()
        if output_settings is not None:
            kw["output_settings"] = output_settings
        kw["composition"] = self._composition()
        return kw

    def _populate_nesting(self, cfg: SpecConfig):
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
            cfg = build_spec_config(**self._gather())
        except Exception as exc:  # validation or input error → show, don't crash
            self.config = None
            self.derived.value = f"<b style='color:#b00'>Invalid:</b> {type(exc).__name__}"
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
            self.editor = _SettingsEditor(self.W, composed, list(composed),
                                          on_edit=self._on_editor_edit)
            self._editor_model = self.model_dd.value
            self.editor_box.children = [self.editor.accordion]

        effective = _apply_overrides(composed, self._overrides)
        self._syncing = True
        try:
            self.editor.sync(effective)   # display effective; don't re-record as edits
        finally:
            self._syncing = False

        comp = cfg.composition.model_copy(update={"overrides": _overrides_nested(self._overrides)})
        cfg = cfg.model_copy(update={"model_settings": effective, "composition": comp})

        self.config = cfg
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
    def _download_html(cfg: SpecConfig) -> str:
        """A data-URI download link for the resolved YAML — works in the browser
        (Voilà / JupyterLab) with no server-side file access."""
        payload = cfg.to_yaml_str().encode("utf-8")
        b64 = base64.b64encode(payload).decode("ascii")
        fname = f"{cfg.casename}.spec_config.yml"
        return (f'⬇ <a download="{fname}" href="data:text/yaml;base64,{b64}">'
                f"Download <code>{fname}</code></a>")

    # ---- actions -------------------------------------------------------------
    def _on_compute_dt(self, _):
        self.dt_status.value = "<i>computing…</i>"
        try:
            kw = self._gather()
            kw["dt"] = None  # force CFL computation (builds the grid via roms_tools)
            cfg = build_spec_config(**kw)
            self.dt.value = float(cfg.model_settings["time_stepping"]["dt"])
            self.dt_status.value = f"<span style='color:#080'>dt = {self.dt.value:g} s (CFL)</span>"
        except Exception as exc:
            self.dt_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"

    def _on_plot(self, _):
        """Build a roms_tools.Grid from the current grid kwargs and render it."""
        self.plot_status.value = "<i>building grid…</i>"
        try:
            import io

            import matplotlib.pyplot as plt
            from roms_tools import Grid

            gk: Dict[str, Any] = {}
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

    def _on_save(self, _):
        if self.config is None:
            self.save_status.value = "<span style='color:#b00'>Nothing to save — config is invalid.</span>"
            return
        try:
            p = self.config.to_yaml(Path(self.save_path.value))
            self.save_status.value = f"<span style='color:#080'>Saved {p}</span>"
        except Exception as exc:
            self.save_status.value = f"<span style='color:#b00'>{type(exc).__name__}: {exc}</span>"

    # ---- layout / display ----------------------------------------------------
    @property
    def widget(self):
        W = self.W
        def section(title, *rows):
            return W.VBox([W.HTML(f"<b>{title}</b>"), *rows],
                          layout=W.Layout(border="1px solid #e0e0e0", padding="8px", margin="4px 0"))
        grid_box = W.GridBox(
            [self.grid_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"))
        child_box = W.GridBox(
            [self.child_w[k] for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)],
            layout=W.Layout(grid_template_columns="repeat(3, 210px)"))
        return W.VBox([
            W.HTML("<h3>SpecConfig wizard</h3>"
                   "<i>Pick a Model and (optionally) a Domain, tweak fields, review, save. "
                   "Or load an existing spec_config.yml to edit it. Fine-tune model "
                   "settings under “Advanced settings”.</i>"),
            section("Load existing (optional)",
                    W.HBox([self.load_path, self.load_btn]), self.upload, self.load_status),
            section("Pieces", self.model_dd, self.domain_dd, self.grid_name),
            section("Grid",
                    W.HBox([
                        W.VBox([grid_box, self.scoord_chk,
                                W.HBox([self.hmin, self.close_narrow_chk]),
                                self.mask_shapefile]),
                        W.VBox([W.HBox([self.plot_btn, self.plot_status]),
                                self.plot_img],
                               layout=W.Layout(padding="0 0 0 20px")),
                    ])),
            section("Nesting (optional)", self.nest_enable, self.nest_domain_dd,
                    child_box, W.HBox([self.nest_period, self.nest_pressure_fluxes])),
            section("Open boundaries", W.HBox(list(self.bnd.values()))),
            section("Forcing", self.forcing_dd, self.forcing_box),
            section("Partitioning", W.HBox([self.npx, self.npy])),
            section("Run window", self.start, self.end, self.model_ref_date,
                    self.description, self.ensemble),
            section("Timestep", W.HBox([self.dt, self.dt_btn]), self.dt_status),
            section("Output settings",
                    W.HTML("<i>Select an OutputSpec to seed the output sections; "
                           "fine-tune them under Advanced settings.</i>"),
                    self.output_dd),
            section("Advanced settings (model defaults — collapsed; click to edit)",
                    self.editor_box),
            section("Review (resolved SpecConfig)", self.derived, self.validation, self.preview),
            section("Export",
                    self.download_link,
                    W.HBox([self.save_path, self.save_btn]), self.save_status),
        ])

    def display(self):
        from IPython.display import display
        display(self.widget)
