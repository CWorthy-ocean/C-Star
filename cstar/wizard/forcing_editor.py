"""The Forcing card's editor: forcing rows, their sources and options.

:class:`_ForcingEditor` builds the surface, boundary, tidal, river and
initial-conditions panes and reads them back into a ``ForcingSpec``; the module
also holds the source option tables and the row helpers it uses.
"""

from __future__ import annotations

import copy
import html
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cstar.applications.forge.blueprint import (
    REGIONAL_PHYSICS_SURFACE_SOURCES,
    BgcBoundarySource,
    BgcInitialConditionsSource,
    BgcInterpMethod,
    BgcSurfaceSource,
    BlendProfile,
    ClimatologyMode,
    CoarseGridMode,
    ExtrapMethod,
    InitialConditionsSource,
    PhysicsBoundarySource,
    PhysicsSurfaceSource,
    Prefill,
    RegridMethod,
    RestoringSurfaceSource,
    RiverBgcSource,
    RiverForcingItem,
    RiverSource,
    RiverTemperatureSource,
    SurfaceBlend,
    SurfaceBlendFallback,
    SurfaceForcingItem,
    SurfaceType,
    TidalSource,
)
from cstar.base.digest import hash_netcdf_contents
from cstar.wizard.fields import (
    _RIVER_CUSTOM_FILE_HINT,
    _parse_optional_number,
    _same_attached_file,
    _stage_uploaded_netcdf,
    _tip,
    _user_file_status_html,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.labels import label_for

if TYPE_CHECKING:
    from collections.abc import Callable


# Dropdown option lists derived from the enums so the wizard and schema stay in sync.
_SURFACE_TYPES = [e.value for e in SurfaceType]
_COARSE_MODES = [e.value for e in CoarseGridMode]
_BLEND_PROFILES = [e.value for e in BlendProfile]
# A blend's fallback is a global physics source; "" = no blend (the row stands alone).
_BLEND_NONE = ""
_BLEND_FALLBACK_OPTS: list[tuple[str, str]] = [
    ("none", _BLEND_NONE),
    *(
        (e.value, e.value)
        for e in PhysicsSurfaceSource
        if e.value not in REGIONAL_PHYSICS_SURFACE_SOURCES
    ),
]
_BGC_INTERP_METHODS = [e.value for e in BgcInterpMethod]
# Optional dropdowns include a blank sentinel meaning "leave unset (roms-tools default)"
_PREFILL_OPTS = [""] + [e.value for e in Prefill]
_REGRID_OPTS = [""] + [e.value for e in RegridMethod]
_EXTRAP_OPTS = [""] + [e.value for e in ExtrapMethod]
# Per-bgc-source interp-method override: blank = inherit the section's own default
# (BgcSourceItem.bgc_interpolation_method=None) -- same blank-sentinel convention as
# _PREFILL_OPTS/_REGRID_OPTS/_EXTRAP_OPTS above.
_BGC_INTERP_OPTS_WITH_DEFAULT = ["", *_BGC_INTERP_METHODS]
_FORCING_CATEGORIES = ("surface", "tidal", "river")
# Row categories managed by _ForcingEditor's generic add/remove-row machinery
# (_rows/_make_row/_add/_remove/_render). "ic_bgc" and "boundary_bgc" are pseudo-
# categories: their rows feed `initial_conditions.bgc_sources` and
# `forcing.boundary.bgc_sources` respectively, not their own `Forcing` list field,
# so they're kept out of `_FORCING_CATEGORIES` (which drives the `forcing:` dict in
# `gather()`) but still rendered/added/removed exactly like the others. Boundary's
# physics source is a required scalar (like IC's), not a row list -- see the
# `boundary_*` widgets built alongside `ic_*` in `__init__`, mirroring `ic_box`.
_ROW_CATEGORIES = ("ic_bgc", "boundary_bgc", *_FORCING_CATEGORIES)
# Section-level default-BGC-interpolation widget attr name for each bgc row
# category -- shared by `_ForcingEditor._sync_bgc` so a "Copy ... bgc ->" click
# can copy the section default alongside the rows themselves.
_DEFAULT_INTERP_ATTR = {
    "ic_bgc": "ic_bgc_interp",
    "boundary_bgc": "boundary_bgc_interp",
}
_GLORYS_LAYOUT_OPTS = ["", "regional", "global"]  # "" = not specified

# Each forcing category's name as the editor shows it: a pane's title is its first
# category's (see `_FORCING_PANES`), and the rest are the places messages name
# ("BGC initial conditions, row 1" is a Biogeochemistry row). Cosmetic: the internal
# keys above (used as dict keys/lookups everywhere else) are unchanged.
_CATEGORY_TITLES: dict[str, str] = {
    "initial_conditions": "Initial conditions",
    "ic_bgc": "BGC initial conditions",
    "surface": "Surface forcing",
    "boundary": "Boundary forcing",
    "boundary_bgc": "BGC boundary forcing",
    "tidal": "Tidal forcing",
    "river": "River forcing",
}

# Forcing categories a blueprint cannot resolve without (the rest are optional
# add-ons) -- drives the Required/Optional in `_ForcingEditor.retitle`.
_REQUIRED_FORCING_CATEGORIES = frozenset(
    {"initial_conditions", "ic_bgc", "surface", "boundary", "boundary_bgc"}
)

# The forcing editor's panes, in order, each with the categories it shows; a pane's
# title is its first category's (`_CATEGORY_TITLES`), and it is Required when any of
# its categories is (`_REQUIRED_FORCING_CATEGORIES`).
_FORCING_PANES: tuple[tuple[str, ...], ...] = (
    ("initial_conditions", "ic_bgc"),
    ("surface",),
    ("boundary", "boundary_bgc"),
    ("tidal",),
    ("river",),
)

# The section-wide initial-conditions/boundary settings under each pane's options
# link, by widget-name suffix (`ic_<suffix>`, `boundary_<suffix>`), each with the
# widget value that leaves it out of the blueprint (see `_ForcingEditor.gather`).
_SECTION_OPTION_DEFAULTS: dict[str, Any] = {
    "flex_time": False,
    "validate": True,
    "prefill": "",
    "regrid_method": "",
    "extrap_method": "",
    "bottom_fill_blend_m": "",
    "options": "",
}


def _row_place(cat: str, n: int) -> str:
    """Row ``n`` (1-based) of the forcing editor's ``cat`` rows, as the editor
    shows it ("Surface forcing, row 2"); the ``ic_bgc``/``boundary_bgc`` places are
    the Biogeochemistry rows of the Initial conditions / Boundary forcing panes.
    """
    return f"{_CATEGORY_TITLES.get(cat, cat)}, row {n}"


def _use_vars_rule() -> str:
    """What Forge asks of two or more BGC sources, in the wizard's words."""
    label = _row_desc("use_vars", "use_vars")
    return (
        f"With two or more sources, give each one its {label}, so each tracer "
        "comes from one source."
    )


# Valid source names per (category, type).  Drives name dropdowns in the forcing editor.
# "boundary"/"boundary_bgc" have no `type` dropdown of their own (physics is a
# required scalar, bgc is its own row-list -- see BoundaryForcing), so both are keyed
# by `None`, mirroring "ic_bgc" (also type-less: physics is IC's own scalar source).
_SOURCE_OPTS: dict[Any, list[str]] = {
    ("surface", SurfaceType.PHYSICS.value): [e.value for e in PhysicsSurfaceSource],
    # MBL_co2 is set in the pane's own "Atmospheric CO2" block, not as a row.
    ("surface", SurfaceType.BGC.value): [
        e.value for e in BgcSurfaceSource if e is not BgcSurfaceSource.MBL_CO2
    ],
    ("surface", SurfaceType.RESTORING.value): [e.value for e in RestoringSurfaceSource],
    ("boundary", None): [e.value for e in PhysicsBoundarySource],
    ("boundary_bgc", None): [e.value for e in BgcBoundarySource],
    ("tidal", None): [e.value for e in TidalSource],
    ("river", None): [e.value for e in RiverSource],
    ("ic_bgc", None): [e.value for e in BgcInitialConditionsSource],
}
# BGC sources that carry no time axis of their own: "constants" is inline values,
# "ESPER" is derived from the physics T/S, and GLODAP ships a single static field.
# None can be a "climatology" (a repeating annual cycle needs a year to repeat), and
# roms-tools raises ValueError when one is handed climatology=True -- so the wizard
# hides the checkbox for them and never writes the field (see `_apply_row_visibility`
# and `_gather_item`).
_STATIC_BGC_SOURCES = frozenset(
    {
        BgcBoundarySource.CONSTANTS.value,
        BgcBoundarySource.ESPER.value,
        BgcBoundarySource.GLODAP.value,
    }
)
# Derived/inline pseudo-sources computed at generation time (no dataset, no
# path, no regridding knobs): gates both which per-row widgets are shown
# (`_apply_row_visibility`) and which keys `_gather_item` emits, so the two
# can never disagree. Mirrors source_registry.DERIVED_BGC_SOURCES (upper-case).
_DERIVED_BGC_SOURCES = frozenset(
    {BgcBoundarySource.CONSTANTS.value, BgcBoundarySource.ESPER.value}
)
# Dropdown labels that differ from the stored source name. Blueprints always store
# the plain enum value; only what the user sees changes.
_SOURCE_LABELS: dict[str, str] = {
    BgcBoundarySource.ESPER.value: "ESPER (experimental)",
}


def _blend_fallback_source(item: dict[str, Any]) -> dict[str, Any]:
    """The seed item's blend fallback source as a dict (a bare name is accepted)."""
    fsrc = ((item.get("blend") or {}).get("fallback") or {}).get("source") or {}
    return {"name": fsrc} if isinstance(fsrc, str) else dict(fsrc)


def _is_regional_surface_source(name: str) -> bool:
    """Whether a surface source covers only part of the globe (see
    ``REGIONAL_PHYSICS_SURFACE_SOURCES``); such a row needs a blend fallback unless
    the domain lies wholly inside the source.
    """
    return name.upper() in REGIONAL_PHYSICS_SURFACE_SOURCES


def _source_dropdown_opts(names: list[str]) -> list[tuple[str, str]]:
    """``(label, value)`` pairs for a source-name Dropdown, so a source can be shown
    under a friendlier label while the widget's ``.value`` stays the blueprint's
    source name. Compare/assign against the plain name list, not ``.options``.
    """
    return [(_SOURCE_LABELS.get(n, n), n) for n in names]


# Sentinel dropdown value meaning "no initial conditions" -- valid only for a
# child domain (state comes from the parent's nesting extraction instead).
_IC_NONE = "(none)"
_IC_SOURCE_OPTS = [e.value for e in InitialConditionsSource] + [_IC_NONE]
# Sentinel dropdown value meaning "no boundary forcing" -- valid for any domain
# (not just a child grid, which already gets this behavior unconditionally via
# `_gather()`'s durable nesting-based clear, see `ForgeBlueprintWizard._gather`).
# Mirrors `_IC_NONE`: `gather()` below emits `forcing["boundary"] = None` outright
# instead of a sourceless dict, matching `Forcing.boundary: BoundaryForcing | None`.
_BOUNDARY_NONE = "(none)"
_RIVER_BGC_SOURCE_OPTS = [""] + [e.value for e in RiverBgcSource]
_RIVER_TEMP_SOURCE_OPTS = [""] + [e.value for e in RiverTemperatureSource]
# ESPER source fields (SourceSpec.esper_method/esper_equation; the
# esper_salinity_conditioning checkbox beside them); "" = unset (roms-tools
# default: method="nn", equation=8).
_ESPER_METHOD_OPTS = ["", "lir", "nn", "mixed"]
_ESPER_EQUATION_OPTS = ["", "8", "16"]


def _row_desc(key: str, default: str) -> str:
    """Per-row widget description text: the glossary label for
    ``forcing.row.<key>`` (falling back to ``default``, the historical
    hardcoded caption). No trailing colon: a row tile sets each label above its
    box.
    """
    return label_for(f"forcing.row.{key}", default=default).label


_ROW_REGRID_DEFAULTS = {"prefill": "", "regrid_method": "", "extrap_method": ""}
# The fields of a repeatable forcing row that sit under its options disclosure
# (_ROW_OPTIONS_TITLES) rather than on its first line, by row category, each with
# the widget value that leaves it at its default: the disclosure counts the shown
# ones and how many differ, _make_row seeds a missing field from it, and
# _gather_item omits a field still at it. Every other field of a row is a primary
# one. The BGC source rows (ic_bgc, boundary_bgc) have nothing to split.
_ROW_OPTION_DEFAULTS: dict[str, dict[str, Any]] = {
    "surface": {
        "coarse_grid_mode": SurfaceForcingItem.model_fields[
            "coarse_grid_mode"
        ].default.value,
        "blend_fallback": _BLEND_NONE,
        "blend_fallback_correct_radiation": SurfaceBlendFallback.model_fields[
            "correct_radiation"
        ].default,
        "blend_width_km": SurfaceBlend.model_fields["width_km"].default,
        "blend_profile": SurfaceBlend.model_fields["profile"].default.value,
        **_ROW_REGRID_DEFAULTS,
        "options": "",
    },
    "tidal": {**_ROW_REGRID_DEFAULTS, "options": ""},
    "river": {
        "convert_to_climatology": RiverForcingItem.model_fields[
            "convert_to_climatology"
        ].default.value,
        "coast_snap_buffer_km": 0.0,  # unset: the dataset's own buffer
        "domain_edge_buffer": RiverForcingItem.model_fields[
            "domain_edge_buffer"
        ].default,
        "surface_forcing_source_name": "",
        "surface_forcing_source_path": "",
        "river_temp_smoothing_window_days": RiverForcingItem.model_fields[
            "river_temp_smoothing_window_days"
        ].default,
        "options": "",
    },
}
_ROW_OPTIONS_TITLES = {"river": "River options"}
_ROW_OPTIONS_TITLE = "Regridding & roms-tools options"
# A row whose custom-file box sits under its disclosure too (the time-varying CO2).
_ROW_OPTIONS_WITH_FILE_TITLE = "Custom file and roms-tools options"


def _row_option_changed(default: Any, value: Any) -> bool:
    """Whether an options-disclosure field's ``value`` differs from its ``default``
    (blank text counts as unset).
    """
    if isinstance(default, str):
        return str(value).strip() != default
    return value != default


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
        description=_row_desc("prefill", "prefill"),
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
        description=_row_desc("regrid_method", "regrid"),
        style=small,
        layout=W.Layout(width="190px"),
        tooltip=_tip(cat, "regrid_method"),
    )
    _extrap_val = str(item.get("extrap_method") or "")
    if _extrap_val not in _EXTRAP_OPTS:
        _extrap_val = ""
    w["extrap_method"] = W.Dropdown(
        options=_EXTRAP_OPTS,
        value=_extrap_val,
        description=_row_desc("extrap_method", "extrap"),
        style=small,
        layout=W.Layout(width="190px"),
        tooltip=_tip(cat, "extrap_method"),
    )


def _source_opts_for(cat: str, type_val: str | None) -> list[str]:
    """Return the valid source names for a given forcing category and type."""
    return _SOURCE_OPTS.get((cat, type_val), _SOURCE_OPTS.get((cat, None), []))


def _dump_constants(constants: Any) -> str:
    """Serialize a ``SourceSpec.constants`` mapping into the compact textbox format
    (``"key=value, key2=value2"``); empty/``None`` -> ``''``.
    """
    if not constants:
        return ""
    return ", ".join(f"{k}={v}" for k, v in constants.items())


def _parse_constants(text: str) -> dict[str, float]:
    """Parse the constants-mapping textbox: comma- or newline-separated ``key=value``
    pairs (e.g. ``"Fe=3.0e-3, ALK=2300"``) into ``{"Fe": 0.003, "ALK": 2300.0}``.

    Blank parts (a trailing comma, an empty line) are skipped. Raises ``ValueError``
    naming every other part that is not ``key=number``, so a typo stops the blueprint
    instead of removing that tracer from it.
    """
    out: dict[str, float] = {}
    bad: list[str] = []
    for part in re.split(r"[,\n]", text or ""):
        part = part.strip()
        if not part:
            continue
        k, _, v = part.partition("=")
        k, v = k.strip(), v.strip()
        try:
            if not k:
                raise ValueError(part)
            out[k] = float(v)
        except ValueError:
            bad.append(part)
    if bad:
        raise ValueError(
            "cannot read " + ", ".join(repr(p) for p in bad) + " (write key=number)"
        )
    return out


def _add_bgc_source_widgets(W, w: dict[str, Any], cat: str, src: dict[str, Any], small):
    """Build the shared ``constants``-mapping textbox and ``esper_method``/
    ``esper_equation`` dropdowns onto row-widget dict ``w`` (see ``SourceSpec.
    constants``/``esper_method``/``esper_equation``). Used by IC-BGC, boundary, and
    surface bgc rows -- ``_apply_row_visibility`` only shows the relevant widget when
    the row's source name is ``"constants"``/``"ESPER"``, so this can be called
    unconditionally even where the source enum doesn't (yet) offer those names (e.g.
    surface's ``BgcSurfaceSource``, per roms-tools' ``SurfaceForcing`` not supporting
    either) -- the widgets simply stay hidden there.
    """
    w["constants"] = W.Textarea(
        value=_dump_constants(src.get("constants")),
        description=_row_desc("constants", "constants"),
        placeholder="key=value pairs, e.g. Fe=3.0e-3, ALK=2300",
        style=small,
        layout=W.Layout(width="320px"),
        tooltip=_tip(cat, "constants"),
    )
    _method_val = str(src.get("esper_method") or "")
    if _method_val not in _ESPER_METHOD_OPTS:
        _method_val = ""
    w["esper_method"] = W.Dropdown(
        options=_ESPER_METHOD_OPTS,
        value=_method_val,
        description=_row_desc("esper_method", "esper method"),
        style=small,
        layout=W.Layout(width="190px"),
        tooltip=_tip(cat, "esper_method"),
    )
    _equation_val = str(src.get("esper_equation") or "")
    if _equation_val not in _ESPER_EQUATION_OPTS:
        _equation_val = ""
    w["esper_equation"] = W.Dropdown(
        options=_ESPER_EQUATION_OPTS,
        value=_equation_val,
        description=_row_desc("esper_equation", "esper eqn"),
        style=small,
        layout=W.Layout(width="170px"),
        tooltip=_tip(cat, "esper_equation"),
    )
    w["esper_salinity_conditioning"] = W.Checkbox(
        value=bool(src.get("esper_salinity_conditioning", False)),
        description=_row_desc("esper_salinity_conditioning", "condition salinity"),
        indent=False,
        tooltip=_tip(cat, "esper_salinity_conditioning"),
    )


# The `options` passthrough (see forge_blueprint.py `_OPTIONS_HELP`) is a free-form dict of
# raw roms-tools kwargs, so it can't be rendered as typed controls. We surface it as a
# small JSON editor per item — visible and round-tripped — rather than hidden. This is
# the advanced/transitional hatch; promoting a knob to a typed field gives it a proper
# widget above.
# What an empty optional box shows, by what blank means there. Grey text reading
# "(default)" looked like a value already filled in.
_PATH_PLACEHOLDER = "Leave blank to use the default"
_BLEND_M_PLACEHOLDER = "Leave blank for the default depth"

_OPTIONS_PLACEHOLDER = (
    'advanced roms-tools kwargs (JSON object), e.g. {"chunks": {"time": 1}}'
)


def _parse_options(text: str) -> dict[str, Any]:
    """Parse the per-item `options` JSON editor into a dict.

    Empty/whitespace → ``{}``. Raises ``ValueError`` for invalid JSON or JSON that is
    not an object, so a requested option is never silently left out. Values are
    forwarded verbatim to the rt constructor.
    """
    text = (text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON ({exc.msg}, line {exc.lineno} column {exc.colno})"
        ) from None
    if not isinstance(parsed, dict):
        raise ValueError(
            f"must be a JSON object ({{...}}), not {type(parsed).__name__}"
        )
    return parsed


def _use_vars_list(text: str) -> list[str]:
    """The variables a BGC row's "Variables to read" box lists, comma-separated."""
    return [p.strip() for p in text.split(",") if p.strip()]


def _parse_field(parser: Callable[[str], Any], text: str, where: str) -> Any:
    """``parser(text)``, with ``where`` (the field's place in the wizard) put in
    front of a ``ValueError`` so the status names the box to fix.
    """
    try:
        return parser(text)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from None


# Item and section fields that have no widget. A loaded value is carried and
# written back unchanged (see ``_carried_fields``), so editing a blueprint in the
# wizard never drops it.
_CARRIED_FIELDS = ("prefill_kwargs", "extrap_kwargs")


def _carried_fields(item: dict[str, Any]) -> dict[str, Any]:
    """The :data:`_CARRIED_FIELDS` that ``item`` sets, copied."""
    return {k: copy.deepcopy(item[k]) for k in _CARRIED_FIELDS if item.get(k)}


def _parse_blend_m(text: str) -> float | str | None:
    """Parse the ``bottom_fill_blend_m`` text box (blank → ``None``, i.e. unset).

    Text ``float()`` accepts (``inf`` included) → that float; anything else → the
    stripped text, so blueprint validation reports it instead of dropping it.
    """
    return _parse_optional_number(text, float)


def _blend_m_text(value: Any) -> str:
    """Show a loaded ``bottom_fill_blend_m`` in its text box (unset → '').

    A float uses ``repr`` (the shortest text that reads back as the same float), minus
    a trailing ``.0``, so a re-save never changes the value.
    """
    if value is None:
        return ""
    return repr(value).removesuffix(".0") if isinstance(value, float) else str(value)


def _dump_options(options: Any) -> str:
    """Serialize an item's `options` dict back into the editor (empty → '')."""
    if not options:
        return ""
    try:
        return json.dumps(options, indent=2, default=str)
    except (TypeError, ValueError):
        return ""


def _options_editor(
    W, value: Any, description: str = "options:", *, height: str | None = "48px"
):
    """A compact JSON Textarea for an item's `options` passthrough dict; a forcing
    row passes ``height=None`` (its tile sets the height, label above the box).
    """
    return W.Textarea(
        value=_dump_options(value),
        description=description,
        placeholder=_OPTIONS_PLACEHOLDER,
        style={"description_width": "initial"},
        layout=W.Layout(width="420px", height=height),
    )


def _row_seed_items(
    cat: str, ic: dict[str, Any], forc: dict[str, Any]
) -> list[dict[str, Any]]:
    """The items a forcing-editor row category is seeded from, read off a
    ForcingSpec's ``initial_conditions`` (*ic*) and ``forcing`` (*forc*) blocks.
    "ic_bgc"/"boundary_bgc" are pseudo-categories fed by those blocks'
    ``bgc_sources``; every other category is its own ``forcing`` list. An
    ``MBL_co2`` surface item is not a row: the "Atmospheric CO2" block takes it.
    """
    if cat == "ic_bgc":
        return ic.get("bgc_sources") or []
    if cat == "boundary_bgc":
        return (forc.get("boundary") or {}).get("bgc_sources") or []
    if cat == "surface":
        return [it for it in forc.get("surface") or [] if not _is_co2_item(it)]
    return forc.get(cat) or []


_CO2_SOURCE = BgcSurfaceSource.MBL_CO2.value
"""The surface source that forces MARBL's atmospheric CO2 in time."""
# What the time-varying CO2 tile shows in place of a Source box.
_CO2_SOURCE_TEXT = (
    "NOAA marine boundary layer CO2 record, 1979 to 2024 (Forge downloads it)"
)
_CO2_STATIC = "static"
_CO2_TIME_VARYING = "time-varying"
_CO2_MODE_OPTS = (
    ("Static", _CO2_STATIC),
    ("Time-varying", _CO2_TIME_VARYING),
)


def _is_co2_item(item: Any) -> bool:
    """Whether a surface forcing item is the ``MBL_co2`` atmospheric CO2 one."""
    return (
        isinstance(item, dict)
        and item.get("type") == SurfaceType.BGC.value
        and (item.get("source") or {}).get("name") == _CO2_SOURCE
    )


def _options_link(
    W: Any, body: Any, title: str, fields: dict[str, Any], defaults: dict[str, Any]
) -> Any:
    """An inline disclosure of *body*, titled with how many of *fields* it shows and
    how many differ from their *defaults* (a preset may set some: "non-default",
    not "changed"); hidden while it shows none.
    """
    pane = components.collapsible(W, body, title, variant="inline")

    def _retitle(_change: Any = None) -> None:
        shown = [k for k, f in fields.items() if f.layout.display != "none"]
        changed = sum(_row_option_changed(defaults[k], fields[k].value) for k in shown)
        n = f"{len(shown)} setting{'' if len(shown) == 1 else 's'}"
        pane.set_title(
            components.accordion_title(
                title,
                components.Pill(f"{n} · {changed} non-default", "mod")
                if changed
                else components.Pill(f"{n} · defaults", "off"),
            )
        )
        pane.layout.display = "" if shown else "none"

    for f in fields.values():
        f.observe(_retitle, names="value")
        f.layout.observe(_retitle, names="display")
    _retitle()
    return pane


class _ForcingEditor:
    """Editor for the forcing spec: initial conditions + per-category forcing items,
    with add/remove. ``gather()`` returns an ``inputs``-shaped dict the resolver
    accepts via ``forcing_inputs=``.

    The Surface forcing pane opens with an "Atmospheric CO2" block, MARBL's air
    CO2: Static (the ModelSpec's constant ``bgc.xco2air_default``, whose widget the
    wizard mounts in ``co2_static``, a settings-editor widget like the Model card's)
    or Time-varying (the one ``MBL_co2`` surface item, edited here instead of as a
    row).
    """

    def __init__(
        self, W, forcing_inputs: dict[str, Any], on_change, *, co2_static: Any = None
    ):
        self.W = W
        self.on_change = on_change
        self._bgc_enabled = True  # the model has MARBL BGC (see `show_bgc`)
        # Deep-copied so no `.get(...)` below can mutate the caller's dict (a
        # catalog-parsed ForcingSpec, or a resolved config's `_sources_to_inputs`
        # reconstruction), both at the current schema.
        fi = copy.deepcopy(forcing_inputs) if forcing_inputs else {}
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
            placeholder=_PATH_PLACEHOLDER,
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            tooltip=_tip("ic", "path"),
        )
        # IC BGC source(s): a row-list (see ``_make_row``/``_ROW_CATEGORIES``'s
        # "ic_bgc" pseudo-category), not a fixed widget group -- ``InitialConditions.
        # bgc_sources`` is a list (v6+), each row a source + use_vars down-select.
        # ``bgc_interpolation_method``/``prefill``/etc. below stay scalar: they are
        # shared across every bgc_sources row, not per-source.
        _ic_bgc_interp = str(
            ic.get("bgc_interpolation_method", BgcInterpMethod.DEPTH.value)
        )
        if _ic_bgc_interp not in _BGC_INTERP_METHODS:
            _ic_bgc_interp = BgcInterpMethod.DEPTH.value
        self.ic_bgc_interp = W.Dropdown(
            options=_BGC_INTERP_METHODS,
            value=_ic_bgc_interp,
            description="Default BGC interpolation:",
            style={"description_width": "180px"},
            layout=W.Layout(width="320px"),
            tooltip=_tip("ic", "ic_bgc_interp"),
        )
        self.ic_flex_time = W.Checkbox(
            value=bool(ic.get("allow_flex_time", False)),
            description="flex time",
            indent=False,
            tooltip=_tip("ic", "ic_flex_time"),
        )
        self.ic_validate = W.Checkbox(
            value=not bool(ic.get("bypass_validation", False)),
            description="validate",
            indent=False,
            tooltip=_tip("ic", "ic_validate"),
        )
        # `groom`: on is roms-tools' default, so only an explicit False loads
        # unchecked (and only unchecked is written back, like `bypass_validation`).
        self.ic_groom = W.Checkbox(
            value=ic.get("groom") is not False,
            description="groom ESPER tracers",
            indent=False,
            tooltip=_tip("ic", "ic_groom"),
        )
        # `groom_settings` has no widget (set only by hand in the blueprint YAML);
        # carried through untouched so editing a loaded blueprint keeps it. It is
        # written back only while the box is checked: the section refuses it with
        # `groom: False`, and re-checking the box brings it back.
        self._ic_groom_settings = copy.deepcopy(ic.get("groom_settings") or {})
        self._ic_carried = _carried_fields(ic)
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
        self.ic_bottom_fill_blend_m = W.Text(
            value=_blend_m_text(ic.get("bottom_fill_blend_m")),
            description="bottom blend (m):",
            placeholder=_BLEND_M_PLACEHOLDER,
            style={"description_width": "110px"},
            tooltip=_tip("ic", "bottom_fill_blend_m"),
        )
        self.ic_options = _options_editor(W, ic.get("options"))
        for _w in (
            self.ic_name,
            self.ic_layout,
            self.ic_path,
            self.ic_bgc_interp,
            self.ic_flex_time,
            self.ic_validate,
            self.ic_groom,
            self.ic_prefill,
            self.ic_regrid_method,
            self.ic_extrap_method,
            self.ic_bottom_fill_blend_m,
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
                self.ic_bgc_interp,
                self.ic_flex_time,
                self.ic_validate,
                self.ic_groom,
                self.ic_prefill,
                self.ic_regrid_method,
                self.ic_extrap_method,
                self.ic_bottom_fill_blend_m,
                self.ic_options,
            ):
                w.layout.display = "" if has_ic else "none"
            self._sync_bgc_display()

        self.ic_name.observe(_sync_ic_layout_visibility, names="value")
        _sync_ic_layout_visibility()

        # boundary forcing: a structural mirror of the IC widgets above --
        # BoundaryForcing.source is a required scalar (like InitialConditions.source),
        # not a row list, so it gets the same scalar-widget-group treatment as IC's
        # physics source (only "boundary_bgc" below is a row-list, mirroring "ic_bgc").
        # An explicit `None` (a resolved config with no boundary forcing at all,
        # or the nesting-derived clear for a child grid) means "seed as (none)";
        # a plain missing key (fresh-wizard startup) keeps the historical GLORYS
        # default -- mirrors the IC "(none)" handling above.
        _boundary_is_none = "boundary" in forc and forc["boundary"] is None
        boundary_block = forc.get("boundary") or {}
        _boundary_source = boundary_block.get("source") or {}
        _boundary_opts = _source_opts_for("boundary", None)
        if _boundary_is_none:
            _boundary_name_val = _BOUNDARY_NONE
        else:
            _boundary_name_val = str(_boundary_source.get("name", _boundary_opts[0]))
            if _boundary_name_val not in _boundary_opts:
                _boundary_name_val = _boundary_opts[0]
        self.boundary_name = W.Dropdown(
            options=[*_boundary_opts, _BOUNDARY_NONE],
            value=_boundary_name_val,
            description="boundary source:",
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "name"),
        )
        _boundary_layout_val = str(_boundary_source.get("glorys_layout") or "")
        if _boundary_layout_val not in _GLORYS_LAYOUT_OPTS:
            _boundary_layout_val = ""
        self.boundary_layout = W.Dropdown(
            options=_GLORYS_LAYOUT_OPTS,
            value=_boundary_layout_val,
            description="glorys_layout:",
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "glorys_layout"),
        )
        self.boundary_path = W.Text(
            value=str(_boundary_source.get("path") or ""),
            description="boundary path:",
            placeholder=_PATH_PLACEHOLDER,
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            tooltip=_tip("boundary", "path"),
        )
        # "boundary_bgc" (below) is a row-list of BgcSourceItem, each optionally
        # overriding this default -- mirrors "ic_bgc"/self.ic_bgc_interp exactly.
        _boundary_bgc_interp = str(
            boundary_block.get("bgc_interpolation_method", BgcInterpMethod.DEPTH.value)
        )
        if _boundary_bgc_interp not in _BGC_INTERP_METHODS:
            _boundary_bgc_interp = BgcInterpMethod.DEPTH.value
        self.boundary_bgc_interp = W.Dropdown(
            options=_BGC_INTERP_METHODS,
            value=_boundary_bgc_interp,
            description="Default BGC interpolation:",
            style={"description_width": "180px"},
            layout=W.Layout(width="320px"),
            tooltip=_tip("boundary", "bgc_interpolation_method"),
        )
        _boundary_prefill_val = str(boundary_block.get("prefill") or "")
        if _boundary_prefill_val not in _PREFILL_OPTS:
            _boundary_prefill_val = ""
        self.boundary_prefill = W.Dropdown(
            options=_PREFILL_OPTS,
            value=_boundary_prefill_val,
            description="prefill:",
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "prefill"),
        )
        _boundary_regrid_val = str(boundary_block.get("regrid_method") or "")
        if _boundary_regrid_val not in _REGRID_OPTS:
            _boundary_regrid_val = ""
        self.boundary_regrid_method = W.Dropdown(
            options=_REGRID_OPTS,
            value=_boundary_regrid_val,
            description="regrid:",
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "regrid_method"),
        )
        _boundary_extrap_val = str(boundary_block.get("extrap_method") or "")
        if _boundary_extrap_val not in _EXTRAP_OPTS:
            _boundary_extrap_val = ""
        self.boundary_extrap_method = W.Dropdown(
            options=_EXTRAP_OPTS,
            value=_boundary_extrap_val,
            description="extrap:",
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "extrap_method"),
        )
        self.boundary_bottom_fill_blend_m = W.Text(
            value=_blend_m_text(boundary_block.get("bottom_fill_blend_m")),
            description="bottom blend (m):",
            placeholder=_BLEND_M_PLACEHOLDER,
            style={"description_width": "110px"},
            tooltip=_tip("boundary", "bottom_fill_blend_m"),
        )
        self.boundary_options = _options_editor(W, boundary_block.get("options"))
        self.boundary_validate = W.Checkbox(
            value=not bool(boundary_block.get("bypass_validation", False)),
            description="validate",
            indent=False,
            tooltip=_tip("boundary", "boundary_validate"),
        )
        # Mirrors self.ic_groom / self._ic_groom_settings above.
        self.boundary_groom = W.Checkbox(
            value=boundary_block.get("groom") is not False,
            description="groom ESPER tracers",
            indent=False,
            tooltip=_tip("boundary", "boundary_groom"),
        )
        self._boundary_groom_settings = copy.deepcopy(
            boundary_block.get("groom_settings") or {}
        )
        self._boundary_carried = _carried_fields(boundary_block)
        for _w in (
            self.boundary_name,
            self.boundary_layout,
            self.boundary_path,
            self.boundary_bgc_interp,
            self.boundary_validate,
            self.boundary_groom,
            self.boundary_prefill,
            self.boundary_regrid_method,
            self.boundary_extrap_method,
            self.boundary_bottom_fill_blend_m,
            self.boundary_options,
        ):
            _w.observe(lambda _ch: on_change(), names="value")

        def _sync_boundary_layout_visibility(_change=None):
            has_boundary = self.boundary_name.value != _BOUNDARY_NONE
            self.boundary_layout.layout.display = (
                "" if has_boundary and self.boundary_name.value == "GLORYS" else "none"
            )
            for w in (
                self.boundary_path,
                self.boundary_bgc_interp,
                self.boundary_validate,
                self.boundary_groom,
                self.boundary_prefill,
                self.boundary_regrid_method,
                self.boundary_extrap_method,
                self.boundary_bottom_fill_blend_m,
                self.boundary_options,
            ):
                w.layout.display = "" if has_boundary else "none"
            self._sync_bgc_display()

        self.boundary_name.observe(_sync_boundary_layout_visibility, names="value")
        _sync_boundary_layout_visibility()

        # per-category item rows: list of dicts of widgets. "ic_bgc"/"boundary_bgc"
        # are pseudo-categories (not their own `Forcing` list field) -- their rows
        # feed `initial_conditions.bgc_sources`/`forcing["boundary"]["bgc_sources"]`
        # respectively, instead of `forcing[cat]` directly; handled specially here
        # and in `gather()`.
        # IC<->boundary bgc-source sync: the two panels are usually configured
        # identically (same BGC datasets for initial conditions and boundaries), so
        # a one-shot copy button beats re-entering every row by hand. Not a live
        # link -- each click snapshots the source panel's current rows and replaces
        # the target panel's rows with fresh copies. Each button lives at the
        # bottom of the panel it copies *from* (rendered into that panel's own
        # container by `_render`, not a standalone box above the accordion), so
        # the rows being copied are visible right above the button that copies
        # them. Built before the seeding loop below, which calls `_render` (and
        # so needs these to already exist) for every row category.
        self._sync_to_boundary_btn = W.Button(
            description="Copy IC bgc → Boundary",
            layout=W.Layout(width="200px"),
            tooltip=(
                "Replace the boundary bgc sources AND default BGC interpolation "
                "with a copy of the IC bgc sources and default"
            ),
        )
        self._sync_to_boundary_btn.on_click(
            lambda _b: self._sync_bgc("ic_bgc", "boundary_bgc")
        )
        self._sync_to_ic_btn = W.Button(
            description="Copy Boundary bgc → IC",
            layout=W.Layout(width="200px"),
            tooltip=(
                "Replace the IC bgc sources AND default BGC interpolation with a "
                "copy of the boundary bgc sources and default"
            ),
        )
        self._sync_to_ic_btn.on_click(
            lambda _b: self._sync_bgc("boundary_bgc", "ic_bgc")
        )

        self._rows: dict[str, list] = {c: [] for c in _ROW_CATEGORIES}
        # Before the rows: `_render("surface")` puts the CO2 block above them.
        surface = forc.get("surface") or []
        co2_index = next((i for i, it in enumerate(surface) if _is_co2_item(it)), None)
        self._build_co2(
            surface[co2_index] if co2_index is not None else None,
            co2_index,
            co2_static,
        )
        # The BGC panes' note on sources that do not partition the tracers (see
        # `refresh_bgc_notes`); `_render` puts it above the rows.
        self._bgc_notes: dict[str, Any] = {}
        for cat in ("ic_bgc", "boundary_bgc"):
            self._bgc_notes[cat] = components.banner_widget(W, "warn", "")
            self._bgc_notes[cat].layout.display = "none"
        self._containers: dict[str, Any] = {}
        for cat in _ROW_CATEGORIES:
            self._containers[cat] = W.VBox([])
            self._set_rows(cat, _row_seed_items(cat, ic, forc))

        # The rows are hidden, not cleared, with the section: switching back to a
        # real source restores them.
        self._bgc_sections: dict[str, Any] = {
            cat: components.subsection(
                W, "forcing.bgc", self._bgc_settings(cat), self._containers[cat]
            )
            for cat in ("ic_bgc", "boundary_bgc")
        }
        for cat in self._bgc_sections:
            for w in self._bgc_setting_widgets(cat):
                w.observe(lambda _ch: self._sync_bgc_display(), names="value")

        # The sections now exist; re-run so a seeded "(none)" IC/boundary hides its
        # Biogeochemistry section (the call during widget construction above could
        # not reach it yet).
        _sync_ic_layout_visibility()
        _sync_boundary_layout_visibility()

    # ---- atmospheric CO2 -----------------------------------------------------
    def _build_co2(
        self, item: dict[str, Any] | None, index: int | None, co2_static: Any
    ) -> None:
        """Build the "Atmospheric CO2" block: Time-varying when the forcing has an
        ``MBL_co2`` surface item (*item*, at *index* in the surface list, where
        ``gather`` puts it back), else Static.

        The time-varying fields are a surface row fixed to a bgc ``MBL_co2`` source,
        without its type, source and climatology boxes (roms-tools refuses an
        ``MBL_co2`` climatology). The static value is the settings editor's
        ``bgc.xco2air_default`` widget, mounted by the wizard in *co2_static*.
        """
        W = self.W
        self._co2_index = index
        seed = copy.deepcopy(item) if item is not None else {}
        seed["type"] = SurfaceType.BGC.value
        seed.setdefault("source", {}).update(name=_CO2_SOURCE, climatology=False)
        self._co2_row = self._make_row(
            "surface", seed, fixed_source=_CO2_SOURCE, path_in_options=True
        )
        for key in ("type", "name", "climatology"):
            self._co2_row[key].layout.display = "none"
        self.co2_mode = W.RadioButtons(
            options=_CO2_MODE_OPTS,
            value=_CO2_TIME_VARYING if item is not None else _CO2_STATIC,
            layout=W.Layout(width="auto"),
        )
        self._co2_static_box = W.VBox(
            [co2_static if co2_static is not None else W.VBox([])]
        )
        # The source as text where other rows have their Source box, so the tile
        # does not read as a box to fill in; its custom file is an option.
        co2_tile = self._row_box(self._co2_row, removable=False)
        source = W.HTML(
            f"<div class='forge-frow-static-l'>{html.escape(_row_desc('name', 'src'))}"
            f"</div><div class='forge-frow-static-v'>{_CO2_SOURCE_TEXT}</div>"
        )
        source.add_class("forge-frow-static")
        co2_tile.children = (source, *co2_tile.children)
        self._co2_varying_box = W.VBox([co2_tile])
        self.co2_block = components.subsection(
            W,
            "forcing.surface.co2",
            components.field_row(W, "forcing.co2.mode", self.co2_mode),
            self._co2_static_box,
            self._co2_varying_box,
        )
        self.co2_mode.observe(self._on_co2_mode, names="value")
        self._sync_co2_mode()
        # Heads the rows below the block, so the two read as separate parts.
        self._surface_rows_header = components.subsection(W, "forcing.surface.sources")

    def _sync_co2_mode(self) -> None:
        static = self.co2_mode.value == _CO2_STATIC
        self._co2_static_box.layout.display = "" if static else "none"
        self._co2_varying_box.layout.display = "none" if static else ""

    def _on_co2_mode(self, _change) -> None:
        self._sync_co2_mode()
        self.on_change()

    def show_co2(self, visible: bool) -> None:
        """Show the "Atmospheric CO2" block (only MARBL has an air CO2)."""
        self.co2_block.layout.display = "" if visible else "none"

    def show_bgc(self, visible: bool) -> None:
        """Say whether the model has MARBL BGC: without it a Biogeochemistry section
        shows only while it holds something (see :meth:`bgc_shown`).
        """
        self._bgc_enabled = visible
        self._sync_bgc_display()

    def _bgc_carried(self, cat: str) -> bool:
        """Whether the blueprint carries *cat*'s section (IC or boundary not "(none)")."""
        if cat == "ic_bgc":
            return self.ic_name.value != _IC_NONE
        return self.boundary_name.value != _BOUNDARY_NONE

    def _bgc_setting_widgets(self, cat: str) -> tuple[Any, Any]:
        """*cat*'s section-wide BGC settings: the default BGC interpolation and the
        ESPER grooming box.
        """
        prefix = "ic" if cat == "ic_bgc" else "boundary"
        return getattr(self, f"{prefix}_bgc_interp"), getattr(self, f"{prefix}_groom")

    def _bgc_holds_something(self, cat: str) -> bool:
        """Whether *cat*'s Biogeochemistry section holds anything ``gather`` writes:
        rows, or a BGC setting off its default.
        """
        interp, groom = self._bgc_setting_widgets(cat)
        return (
            bool(self._rows[cat])
            or interp.value != BgcInterpMethod.DEPTH.value
            or not groom.value
        )

    def bgc_shown(self, cat: str) -> bool:
        """Whether *cat*'s Biogeochemistry section shows: its section is carried and
        the model has MARBL BGC or the section holds something to see or reset.
        """
        return self._bgc_carried(cat) and (
            self._bgc_enabled or self._bgc_holds_something(cat)
        )

    def _sync_bgc_display(self) -> None:
        """Show or hide each Biogeochemistry section by :meth:`bgc_shown`. Tolerates
        the calls made while ``__init__`` is still building the sections.
        """
        for cat, section in getattr(self, "_bgc_sections", {}).items():
            section.layout.display = "" if self.bgc_shown(cat) else "none"

    def bgc_needs_fix(self, cat: str) -> bool:
        """Whether *cat*'s partition note (see :meth:`refresh_bgc_notes`) is shown."""
        return self._bgc_notes[cat].layout.display != "none"

    def _with_co2(self, surface: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """*surface* (the gathered rows) with the ``MBL_co2`` item added when CO2 is
        time-varying: where the seeded item was, else after the last bgc row (where
        every bundled ForcingSpec has it), else last.
        """
        if self.co2_mode.value != _CO2_TIME_VARYING:
            return surface
        index = self._co2_index
        if index is None:
            bgc = [i for i, it in enumerate(surface) if it.get("type") == "bgc"]
            index = bgc[-1] + 1 if bgc else len(surface)
        surface = list(surface)
        surface.insert(
            min(index, len(surface)), self._gather_item("surface", self._co2_row)
        )
        return surface

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
            # The radiation correction is ERA5-specific, so a regional source cannot
            # take it (SurfaceForcingItem refuses it); the fallback has its own.
            w["correct_radiation"].disabled = _is_regional_surface_source(name or "")
        if "blend_fallback" in w:
            physics = t == SurfaceType.PHYSICS.value
            show(w["blend_fallback"], physics)
            for key in (
                "blend_fallback_correct_radiation",
                "blend_width_km",
                "blend_profile",
            ):
                show(w[key], physics and w["blend_fallback"].value != _BLEND_NONE)
        if "wind_dropoff" in w:
            show(w["wind_dropoff"], t == SurfaceType.PHYSICS.value)
        if "glorys_layout" in w:
            show(w["glorys_layout"], name == "GLORYS")
        # use_vars is only ever built for ic_bgc/boundary_bgc rows (see _make_row --
        # SurfaceForcingItem has no such field), so it's always relevant when
        # present; no type-based gating needed here.
        if "use_vars" in w:
            show(w["use_vars"], True)
        # constants/ESPER fields (see _add_bgc_source_widgets) only apply to their
        # matching source name.
        if "constants" in w:
            show(w["constants"], name == "constants")
        if "esper_method" in w:
            show(w["esper_method"], name == "ESPER")
        if "esper_equation" in w:
            show(w["esper_equation"], name == "ESPER")
        if "esper_salinity_conditioning" in w:
            show(w["esper_salinity_conditioning"], name == "ESPER")
        # A 'constants' source takes no path (SourceSpec._constants_only_for_
        # constants_source forbids pairing them) -- gated on the "constants" key's
        # presence, which only ic_bgc/boundary_bgc rows carry, so this never
        # touches surface/tidal/river rows' own `path` widget (river's is instead
        # owned outright by `_sync_river_custom_visibility` in custom-file mode).
        if "path" in w and "constants" in w:
            show(w["path"], name != "constants")
        # `_serialize_dask` (carried opaquely, see `_make_row`/`_gather_item` -- no
        # widget to show/hide here) is only ever set on an ESPER source: it is the
        # one bgc source whose per-chunk cost makes the concurrent write a memory
        # risk. constants/ESPER are derived/inline pseudo-sources, not a regridded
        # dataset: boundary's regridding knobs (prefill/regrid_method/extrap_method/
        # bgc_interpolation_method) only make sense for a dataset-backed source being
        # regridded onto the grid.
        is_derived_bgc = name in _DERIVED_BGC_SOURCES
        # A CUSTOM_FILE river row is owned by `_sync_river_custom_visibility`, which
        # hides these outright (the attach flow replaces the standard-source row).
        # Without this, the re-show below would undo that hide on every row rebuild.
        hide_dataset_knobs = is_derived_bgc or name == RiverSource.CUSTOM_FILE.value
        # "climatology" has a wider gate than the regrid knobs: GLODAP is a static
        # field that can't be one either, but -- unlike the derived sources -- it is
        # still regridded, so it keeps the knobs below.
        if "climatology" in w:
            show(
                w["climatology"],
                name not in _STATIC_BGC_SOURCES
                and name != RiverSource.CUSTOM_FILE.value,
            )
        for key in (
            "prefill",
            "regrid_method",
            "extrap_method",
            "bgc_interpolation_method",
        ):
            if key in w:
                show(w[key], not hide_dataset_knobs)

    def _add_blend_widgets(
        self, w: dict[str, Any], item: dict[str, Any], small: dict[str, str]
    ) -> None:
        """Build a surface row's blend controls (fallback source, its radiation flag,
        band width, profile) and the observers tying them to the row.

        The fallback's ``options`` and any source fields beyond its name (``path``...)
        have no widget; ``_make_row`` carries them as ``_blend_fallback_*`` data, like
        a bgc row's ``_serialize_dask``, so loading then saving does not drop them.
        """
        W = self.W
        blend = item.get("blend") or {}
        fallback = blend.get("fallback") or {}
        fname = str(_blend_fallback_source(item).get("name") or _BLEND_NONE)
        opts = list(_BLEND_FALLBACK_OPTS)
        # Case-insensitive match to an offered source; an unknown name is kept as an
        # extra option so the blueprint validation reports it rather than it vanishing.
        known = {v.upper(): v for _, v in opts}
        fname = known.get(fname.upper(), fname)
        if fname not in known.values():
            opts.append((fname, fname))
        w["blend_fallback"] = W.Dropdown(
            options=opts,
            value=fname,
            description=_row_desc("blend_fallback", "fallback"),
            style=small,
            layout=W.Layout(width="190px"),
            tooltip=_tip("surface", "blend_fallback"),
        )
        w["blend_fallback_correct_radiation"] = W.Checkbox(
            value=bool(
                fallback.get(
                    "correct_radiation",
                    SurfaceBlendFallback.model_fields["correct_radiation"].default,
                )
            ),
            description=_row_desc(
                "blend_fallback_correct_radiation", "fallback corr_rad"
            ),
            indent=False,
            tooltip=_tip("surface", "blend_fallback_correct_radiation"),
        )
        w["blend_width_km"] = W.FloatText(
            value=float(
                blend.get("width_km", SurfaceBlend.model_fields["width_km"].default)
            ),
            description=_row_desc("blend_width_km", "blend width km"),
            style=small,
            layout=W.Layout(width="230px"),
            tooltip=_tip("surface", "blend_width_km"),
        )
        profile = str(
            blend.get("profile") or SurfaceBlend.model_fields["profile"].default.value
        )
        w["blend_profile"] = W.Dropdown(
            options=_BLEND_PROFILES,
            value=profile if profile in _BLEND_PROFILES else _BLEND_PROFILES[0],
            description=_row_desc("blend_profile", "blend profile"),
            style=small,
            layout=W.Layout(width="210px"),
            tooltip=_tip("surface", "blend_profile"),
        )

        def _on_surface_name_change(_change, ws=w) -> None:
            # A regional source covers only part of the globe, so offer the first
            # global product as its fallback. The user may set "none" again (a domain
            # wholly inside the source); Forge checks coverage at generation.
            is_physics = ws["type"].value == SurfaceType.PHYSICS.value
            if is_physics and _is_regional_surface_source(ws["name"].value):
                if ws["blend_fallback"].value == _BLEND_NONE:
                    # The correction the user had on the (global) primary moves to
                    # the global product that now fills in for it.
                    ws["blend_fallback_correct_radiation"].value = ws[
                        "correct_radiation"
                    ].value
                    ws["blend_fallback"].value = _BLEND_FALLBACK_OPTS[1][1]
                ws["correct_radiation"].value = False
            self._apply_row_visibility(ws)

        def _on_fallback_change(change, ws=w) -> None:
            # A blend needs every surface item on one explicit mode (Forcing refuses
            # "auto" and a mix), so a newly chosen fallback pins the "auto" rows to
            # this row's mode, or to "never" when this row is on "auto" too.
            if change["old"] == _BLEND_NONE and change["new"] != _BLEND_NONE:
                own = ws["coarse_grid_mode"].value
                pin = (
                    CoarseGridMode.NEVER.value
                    if own == CoarseGridMode.AUTO.value
                    else own
                )
                for row in self._rows["surface"]:
                    if row["coarse_grid_mode"].value == CoarseGridMode.AUTO.value:
                        row["coarse_grid_mode"].value = pin
            self._apply_row_visibility(ws)

        w["name"].observe(_on_surface_name_change, names="value")
        w["blend_fallback"].observe(_on_fallback_change, names="value")

    def _make_row(
        self,
        cat: str,
        item: dict[str, Any],
        *,
        fixed_source: str | None = None,
        path_in_options: bool = False,
    ):
        """One editable row for an item of *cat*. *fixed_source* pins the source
        name (the "Atmospheric CO2" block's ``MBL_co2`` row, a source the row
        dropdowns do not offer); *path_in_options* puts its custom-file box under
        the row's options disclosure (see ``_add_row_options``).
        """
        W = self.W
        src = item.get("source") or {}
        w: dict[str, Any] = {}
        small = {"description_width": "initial"}

        # Source name: Dropdown driven by category + type (for surface, which still
        # mixes physics/bgc/restoring in one list) or fixed. "boundary_bgc" (bgc-only
        # row-list, mirroring "ic_bgc") has a fixed implied type -- no per-row type
        # dropdown needed (boundary's physics source is its own scalar, see __init__).
        if cat == "boundary_bgc":
            _cur_type = None
            _name_opts = _source_opts_for("boundary_bgc", None)
        else:
            _cur_type = item.get("type", "physics")
            _name_opts = (
                [fixed_source] if fixed_source else _source_opts_for(cat, _cur_type)
            )
        _name_val = str(src.get("name", ""))
        if _name_val not in _name_opts and _name_opts:
            _name_val = _name_opts[0]
        w["name"] = W.Dropdown(
            options=_source_dropdown_opts(_name_opts or [""]),
            value=_name_val
            if _name_val in (_name_opts or [""])
            else (_name_opts or [""])[0],
            description=_row_desc("name", "src"),
            style=small,
            layout=W.Layout(width="200px"),
            tooltip=_tip(cat, "name"),
        )

        # Optional custom dataset path; blank -> backend derives the default path.
        w["path"] = W.Text(
            value=str(src.get("path") or ""),
            description=_row_desc("path", "path"),
            placeholder=_PATH_PLACEHOLDER,
            style=small,
            layout=W.Layout(width="260px"),
            tooltip=_tip(cat, "path"),
        )

        if cat == "surface":
            # Surface still mixes physics/bgc/restoring in one list -- keep its
            # 3-way type dropdown driving the name-options lookup.
            _type_opts = _SURFACE_TYPES
            _type_val = _cur_type if _cur_type in _type_opts else _type_opts[0]
            w["type"] = W.Dropdown(
                options=_type_opts,
                value=_type_val,
                description=_row_desc("type", "type"),
                style=small,
                layout=W.Layout(width="200px"),
                tooltip=_tip(cat, "type"),
            )

            # When type changes → update the source name dropdown to the valid options.
            def _on_type_change(change, name_dd=w["name"], c=cat, ws=w):
                new_opts = _source_opts_for(c, change["new"]) or [""]
                name_dd.options = _source_dropdown_opts(new_opts)
                # `.options` now holds (label, value) pairs -- membership has to be
                # tested against the bare names or it never matches.
                if name_dd.value not in new_opts:
                    name_dd.value = new_opts[0]
                self._apply_row_visibility(ws)
                self.on_change()

            w["type"].observe(_on_type_change, names="value")

        if cat in ("surface", "boundary_bgc", "ic_bgc"):
            w["climatology"] = W.Checkbox(
                value=bool(src.get("climatology", False)),
                description=_row_desc("climatology", "climatology"),
                indent=False,
                # "boundary_bgc" has no own tooltip entry -- reuse "boundary"'s.
                tooltip=_tip(
                    "boundary" if cat == "boundary_bgc" else cat, "climatology"
                ),
            )

            # When the source name changes → constants/ESPER fields only apply to
            # their matching source name (and, for surface, "layout:" only applies
            # to GLORYS).
            def _on_name_change(_change, ws=w):
                self._apply_row_visibility(ws)

            w["name"].observe(_on_name_change, names="value")

        if cat == "surface":
            # glorys_layout only ever applies to a GLORYS source -- never offered by
            # BgcBoundarySource/BgcSurfaceSource, so omitted from boundary_bgc/bgc rows
            # (boundary's own physics glorys_layout is a scalar widget, see __init__).
            _layout_val = str(src.get("glorys_layout") or "")
            if _layout_val not in _GLORYS_LAYOUT_OPTS:
                _layout_val = ""
            w["glorys_layout"] = W.Dropdown(
                options=_GLORYS_LAYOUT_OPTS,
                value=_layout_val,
                description=_row_desc("glorys_layout", "layout"),
                style=small,
                layout=W.Layout(width="190px"),
                tooltip=_tip(cat, "glorys_layout"),
            )

        if cat in ("ic_bgc", "boundary_bgc"):
            # BGC-only knobs: down-select which vars this source contributes, and
            # constants/ESPER-specific fields (visibility gated to the matching
            # source name by _apply_row_visibility). Only "ic_bgc"/"boundary_bgc"
            # rows carry these -- unlike them, `SurfaceForcingItem` has no
            # `use_vars` field at all (extra="forbid" would reject it), and
            # `BgcSurfaceSource` offers neither "constants" nor "ESPER", so a
            # surface bgc row never needs (and, for use_vars, must never emit)
            # these widgets. "boundary_bgc" has no own tooltip entries -- reuse
            # "boundary"'s (same underlying fields).
            _tip_cat = "boundary" if cat == "boundary_bgc" else "ic_bgc"
            w["use_vars"] = W.Text(
                value=", ".join(item.get("use_vars") or []),
                description=_row_desc("use_vars", "use_vars"),
                style=small,
                layout=W.Layout(width="260px"),
                placeholder="ALK,DIC,...",
                tooltip=_tip(_tip_cat, "use_vars"),
            )
            _add_bgc_source_widgets(W, w, _tip_cat, src, small)
        if cat in ("ic_bgc", "boundary_bgc"):
            # Per-source interp-method override (BgcSourceItem.bgc_interpolation_method):
            # blank = inherit the section's own default (self.ic_bgc_interp /
            # self.boundary_bgc_interp). Identical widget for both panels.
            _interp_val = str(item.get("bgc_interpolation_method") or "")
            if _interp_val not in _BGC_INTERP_OPTS_WITH_DEFAULT:
                _interp_val = ""
            w["bgc_interpolation_method"] = W.Dropdown(
                options=_BGC_INTERP_OPTS_WITH_DEFAULT,
                value=_interp_val,
                description=_row_desc("bgc_interpolation_method", "bgc interp"),
                style=small,
                layout=W.Layout(width="200px"),
                tooltip=_tip(
                    "ic" if cat == "ic_bgc" else "boundary", "bgc_interpolation_method"
                )
                or _tip("ic", "ic_bgc_interp"),
            )
        if cat == "surface":
            w["correct_radiation"] = W.Checkbox(
                value=bool(item.get("correct_radiation", False)),
                description=_row_desc("correct_radiation", "corr_rad"),
                indent=False,
                tooltip=_tip("surface", "correct_radiation"),
            )
            w["wind_dropoff"] = W.Checkbox(
                value=bool(item.get("wind_dropoff", False)),
                description=_row_desc("wind_dropoff", "wind_dropoff"),
                indent=False,
                tooltip=_tip("surface", "wind_dropoff"),
            )
            w["coarse_grid_mode"] = W.Dropdown(
                options=_COARSE_MODES,
                value=item.get(
                    "coarse_grid_mode",
                    _ROW_OPTION_DEFAULTS["surface"]["coarse_grid_mode"],
                ),
                description=_row_desc("coarse_grid_mode", "coarse"),
                style=small,
                layout=W.Layout(width="190px"),
                tooltip=_tip("surface", "coarse_grid_mode"),
            )
            self._add_blend_widgets(w, item, small)
            w["restoring_forces"] = W.Text(
                value=", ".join(item.get("restoring_forces") or []),
                description=_row_desc("restoring_forces", "restore"),
                style=small,
                layout=W.Layout(width="220px"),
                placeholder="sss,sst",
                tooltip=_tip("surface", "restoring_forces"),
            )
            _add_regrid_widgets(W, w, "surface", item, small)
        if cat == "tidal":
            w["ntides"] = W.IntText(
                value=int(item.get("ntides") or 0),
                description=_row_desc("ntides", "ntides"),
                style=small,
                layout=W.Layout(width="220px"),
                tooltip=_tip("tidal", "ntides"),
            )
            _add_regrid_widgets(W, w, "tidal", item, small)
        if cat == "river":
            w["climatology"] = W.Checkbox(
                value=bool(src.get("climatology", False)),
                description=_row_desc("climatology", "climatology"),
                indent=False,
                tooltip=_tip("river", "climatology"),
            )
            w["include_bgc"] = W.Checkbox(
                value=bool(item.get("include_bgc", False)),
                description=_row_desc("include_bgc", "bgc"),
                indent=False,
                tooltip=_tip("river", "include_bgc"),
            )
            _ctc_opts = [e.value for e in ClimatologyMode]
            river_defaults = _ROW_OPTION_DEFAULTS["river"]
            _ctc_val = item.get(
                "convert_to_climatology", river_defaults["convert_to_climatology"]
            )
            w["convert_to_climatology"] = W.Dropdown(
                options=_ctc_opts,
                value=_ctc_val
                if _ctc_val in _ctc_opts
                else river_defaults["convert_to_climatology"],
                description=_row_desc("convert_to_climatology", "clim mode"),
                style=small,
                layout=W.Layout(width="210px"),
                tooltip=_tip("river", "convert_to_climatology"),
            )
            w["coast_snap_buffer_km"] = W.FloatText(
                value=float(item.get("coast_snap_buffer_km") or 0.0),
                description=_row_desc("coast_snap_buffer_km", "coast snap km"),
                style=small,
                layout=W.Layout(width="260px"),
                tooltip=_tip("river", "coast_snap_buffer_km"),
            )
            w["domain_edge_buffer"] = W.IntText(
                value=int(
                    item.get("domain_edge_buffer", river_defaults["domain_edge_buffer"])
                ),
                description=_row_desc("domain_edge_buffer", "edge buffer"),
                style=small,
                layout=W.Layout(width="180px"),
                tooltip=_tip("river", "domain_edge_buffer"),
            )
            _bgc_src = item.get("bgc_source") or {}
            _bgc_name_val = str(_bgc_src.get("name", "") or "")
            if _bgc_name_val not in _RIVER_BGC_SOURCE_OPTS:
                _bgc_name_val = ""
            w["bgc_source_name"] = W.Dropdown(
                options=_RIVER_BGC_SOURCE_OPTS,
                value=_bgc_name_val,
                description=_row_desc("bgc_source_name", "bgc src"),
                style=small,
                layout=W.Layout(width="170px"),
                tooltip=_tip("river", "bgc_source_name"),
            )
            w["bgc_source_path"] = W.Text(
                value=str(_bgc_src.get("path") or ""),
                description=_row_desc("bgc_source_path", "bgc path"),
                placeholder=_PATH_PLACEHOLDER,
                style=small,
                layout=W.Layout(width="260px"),
                tooltip=_tip("river", "bgc_source_path"),
            )
            _temp_src = item.get("surface_forcing_source") or {}
            # Validator accepts any case ("era5"); the dropdown options are upper.
            _temp_name_val = str(_temp_src.get("name") or "").upper()
            if _temp_name_val not in _RIVER_TEMP_SOURCE_OPTS:
                _temp_name_val = ""
            w["surface_forcing_source_name"] = W.Dropdown(
                options=_RIVER_TEMP_SOURCE_OPTS,
                value=_temp_name_val,
                description=_row_desc("surface_forcing_source_name", "temp. source"),
                style=small,
                layout=W.Layout(width="210px"),
                tooltip=_tip("river", "surface_forcing_source_name"),
            )
            w["surface_forcing_source_path"] = W.Text(
                value=str(_temp_src.get("path") or ""),
                description=_row_desc("surface_forcing_source_path", "temp. path"),
                placeholder=_PATH_PLACEHOLDER,
                style=small,
                layout=W.Layout(width="260px"),
                tooltip=_tip("river", "surface_forcing_source_path"),
            )
            w["river_temp_smoothing_window_days"] = W.FloatText(
                value=float(
                    item.get(
                        "river_temp_smoothing_window_days",
                        river_defaults["river_temp_smoothing_window_days"],
                    )
                ),
                description=_row_desc(
                    "river_temp_smoothing_window_days", "temp. smoothing (days)"
                ),
                style=small,
                layout=W.Layout(width="260px"),
                tooltip=_tip("river", "river_temp_smoothing_window_days"),
            )

            # bgc_source only takes effect when include_bgc is checked (roms-tools
            # silently ignores it otherwise — see RiverForcingItem validation).
            def _sync_river_bgc_visibility(_change=None, ws=w):
                on = bool(ws["include_bgc"].value)
                ws["bgc_source_name"].layout.display = "" if on else "none"
                ws["bgc_source_path"].layout.display = "" if on else "none"

            w["include_bgc"].observe(_sync_river_bgc_visibility, names="value")
            _sync_river_bgc_visibility()

            # The smoothing window/path only matter once a temperature source is
            # picked (mirrors the bgc pair above).
            def _sync_river_temp_visibility(_change=None, ws=w):
                on = bool(ws["surface_forcing_source_name"].value)
                ws["surface_forcing_source_path"].layout.display = "" if on else "none"
                ws["river_temp_smoothing_window_days"].layout.display = (
                    "" if on else "none"
                )

            w["surface_forcing_source_name"].observe(
                _sync_river_temp_visibility, names="value"
            )
            _sync_river_temp_visibility()

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
                    # Unlike bgc_source_name/surface_forcing_source_path below, the
                    # temp-source dropdown itself has no other gate -- it's simply
                    # unconditional on custom-mode, like domain_edge_buffer.
                    "surface_forcing_source_name",
                    # A custom-file item takes no roms-tools options (see
                    # _gather_item), so its passthrough box goes too.
                    "options",
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
                    # Keep the bgc/temperature widgets hidden regardless of
                    # include_bgc/surface_forcing_source_name -- a custom-file
                    # river item carries neither (see
                    # RiverForcingItem._custom_file_excludes_bgc_source and
                    # ._custom_file_excludes_surface_forcing_source).
                    ws["bgc_source_name"].layout.display = "none"
                    ws["bgc_source_path"].layout.display = "none"
                    ws["surface_forcing_source_path"].layout.display = "none"
                    ws["river_temp_smoothing_window_days"].layout.display = "none"
                    # Until a file is attached the gathered item fails
                    # RiverForcingItem validation ("custom_file is not set");
                    # say what to do rather than leave the blank status to be
                    # explained only by the pydantic dump in the preview.
                    if (
                        not ws.get("_custom_file")
                        and not ws["custom_file_status"].value
                    ):
                        ws["custom_file_status"].value = _RIVER_CUSTOM_FILE_HINT
                else:
                    # Restored from custom-file mode (or never in it): let each
                    # field's own sync decide widget visibility again, rather
                    # than unconditionally showing them here.
                    _sync_river_bgc_visibility()
                    _sync_river_temp_visibility()

            w["name"].observe(_sync_river_custom_visibility, names="value")
        # Advanced passthrough: raw roms-tools kwargs not (yet) typed above.
        # BgcSourceItem (shared by "ic_bgc"/"boundary_bgc") has no `options` field
        # (extra="forbid") -- it isn't its own roms-tools object, just a
        # source+use_vars(+bgc_interpolation_method) contributor to its section's
        # single `options` passthrough (already on the ic_*/boundary_* scalar
        # widgets), so no per-row editor for either.
        if cat not in ("ic_bgc", "boundary_bgc"):
            w["options"] = _options_editor(
                W,
                item.get("options"),
                description=_row_desc("options", "options"),
                height=None,
            )
        remove = W.Button(
            description="✕", layout=W.Layout(width="36px"), tooltip="Remove this item"
        )
        remove.on_click(lambda _b, c=cat, ws=w: self._remove(c, ws))
        for widget in w.values():
            widget.observe(lambda _ch: self.on_change(), names="value")
        w["_remove_btn"] = remove
        # `BgcSourceItem.serialize_dask` is deliberately NOT offered as a widget.
        # PyESPER serialises entry into its own numba kernels with a per-process
        # semaphore, which is the hazard this flag existed to avoid, so setting it
        # no longer buys protection -- it just forces the rest of that write (a
        # boundary's physics companion, or the whole merged IC dataset) onto the
        # one-task-at-a-time path. It survives as a blueprint field and the
        # `--serialize-dask-write` CLI flag for manual troubleshooting. Carried
        # opaquely -- and, like `_remove_btn`, only after the loop above, since it
        # is not a widget to observe -- so editing an existing blueprint in the
        # wizard round-trips the field instead of silently dropping it.
        if cat in ("ic_bgc", "boundary_bgc") and item.get("serialize_dask"):
            w["_serialize_dask"] = True
        # Same reason: `prefill_kwargs`/`extrap_kwargs` have no widget.
        if carried := _carried_fields(item):
            w["_carried"] = carried
        if cat == "surface":
            # Carried like `_serialize_dask` (and for the same reason: after the
            # observe loop, since neither is a widget): the blend fallback's
            # `options` and non-name source fields have no widget.
            w["_blend_fallback_source"] = _blend_fallback_source(item)
            w["_blend_fallback_options"] = dict(
                (item.get("blend") or {}).get("fallback", {}).get("options") or {}
            )
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
                description=_row_desc("custom_file_path", "file"),
                placeholder="path to a pre-made river-forcing netCDF",
                style=small,
                layout=W.Layout(width="320px"),
                tooltip=_tip("river", "custom_file"),
                # Fire `value` only on Enter / focus-out, not per keystroke, so
                # the observer below can auto-attach a finished path without
                # hashing every partially-typed prefix.
                continuous_update=False,
            )
            w["custom_file_attach_btn"] = W.Button(
                description=label_for("buttons.attach", "Attach").label,
                icon="link",
                layout=W.Layout(width="90px"),
            )
            w["custom_file_upload"] = W.FileUpload(
                accept=".nc", multiple=False, description="…or upload"
            )
            w["custom_file_status"] = W.HTML(_user_file_status_html(w["_custom_file"]))

            def _attach_river_custom_file(
                path_str: str, ws=w, *, notify: bool = True
            ) -> None:
                ws["custom_file_status"].value = "<i>attaching…</i>"
                try:
                    path = Path(path_str).expanduser().resolve()
                    if not path.exists():
                        raise FileNotFoundError(f"river custom file not found: {path}")
                    content_hash = hash_netcdf_contents(path)
                except Exception as exc:
                    ws["custom_file_status"].value = components.status_span(
                        "err", f"{type(exc).__name__}: {exc}"
                    )
                    return
                ws["_custom_file"] = {
                    "location": str(path),
                    "content_hash": content_hash,
                }
                ws["custom_file_status"].value = _user_file_status_html(
                    ws["_custom_file"]
                )
                if notify:
                    self.on_change()

            def _maybe_attach_river_custom_file(ws=w, *, notify: bool = True) -> None:
                """Attach whatever path is typed in the box, unless it's blank or
                already the attached file. Called on Enter/focus-out of the path
                Text and as a last-resort from `_gather_item`, so a user who
                types (or uploads) a path and moves on without clicking Attach
                still gets a valid CUSTOM_FILE item instead of a
                RiverForcingItem validation error for a missing custom_file.
                """
                path_str = ws["custom_file_path"].value.strip()
                if not path_str:
                    return
                if _same_attached_file(ws.get("_custom_file"), path_str):
                    return
                _attach_river_custom_file(path_str, notify=notify)

            def _on_river_custom_file_attach_click(_btn, ws=w) -> None:
                path_str = ws["custom_file_path"].value.strip()
                if not path_str:
                    ws["custom_file_status"].value = components.status_span(
                        "err", "Enter a path first."
                    )
                    return
                # Explicit click always re-hashes (the file may have changed on
                # disk since the last attach) -- no `_maybe_` dedupe here.
                _attach_river_custom_file(path_str)

            def _on_river_custom_file_path_submit(_change, ws=w) -> None:
                _maybe_attach_river_custom_file(ws)

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
                # Setting the Text value fires the path-submit observer, which
                # attaches via `_maybe_`; the explicit call below covers the
                # re-upload-same-name case where the value doesn't change (and
                # is a no-op dedupe when the observer already attached it).
                ws["custom_file_path"].value = str(dest)
                _maybe_attach_river_custom_file(ws)

            w["custom_file_attach_btn"].on_click(_on_river_custom_file_attach_click)
            w["custom_file_path"].observe(
                _on_river_custom_file_path_submit, names="value"
            )
            w["custom_file_upload"].observe(_on_river_custom_file_upload, names="value")
            # Exposed (underscore-prefixed, so not a layout child -- see _row_box)
            # for `_gather_item`'s last-resort attach; the closures above are
            # otherwise unreachable from outside this row.
            w["_maybe_attach_custom_file"] = _maybe_attach_river_custom_file
            _sync_river_custom_visibility()
        self._apply_row_visibility(w)
        self._add_row_options(cat, w, with_path=path_in_options)
        return w

    def _add_row_options(
        self, cat: str, w: dict[str, Any], *, with_path: bool = False
    ) -> None:
        """Put *cat*'s option fields of row ``w`` (:data:`_ROW_OPTION_DEFAULTS`),
        and its custom-file box too when ``with_path`` (blank: the default dataset),
        under a disclosure kept in ``w["_options"]``, titled with how many it shows
        and how many differ from their defaults (a preset may set some: "non-default",
        not "changed"); hidden while it shows none (a custom-file river). Built once
        per row, so ``_render`` keeps it open.
        """
        defaults = {
            **({"path": ""} if with_path else {}),
            **_ROW_OPTION_DEFAULTS.get(cat, {}),
        }
        keys = [k for k in w if k in defaults]
        if not keys:
            return
        W = self.W
        title = (
            _ROW_OPTIONS_WITH_FILE_TITLE
            if with_path
            else _ROW_OPTIONS_TITLES.get(cat, _ROW_OPTIONS_TITLE)
        )
        body = W.HBox(
            [w[k] for k in keys],
            layout=W.Layout(flex_flow="row wrap", align_items="flex-end"),
        )
        body.add_class("forge-frow-opts")
        pane = _options_link(
            W, body, title, {k: w[k] for k in keys}, {k: defaults[k] for k in keys}
        )
        w["_options"] = pane
        w["_option_keys"] = tuple(keys)

    def _row_box(self, w, *, removable: bool = True):
        # `type` (when present) drives the other options in the row, so show it first.
        # The remove button goes at the FRONT (not the end): a row can grow quite wide
        # (name/path/climatology/constants/esper dropdowns/options editor), and a
        # trailing button is easily clipped off-screen in a notebook without
        # horizontal scrolling -- putting it first keeps it reachable regardless of
        # how many fields are visible.
        # Underscore-prefixed keys are not layout children: `_remove_btn` is
        # placed explicitly below, and `_custom_file`/`_serialize_dask` are carried
        # data rather than widgets. Filter by prefix so a future carry cannot be
        # forgotten here and end up handed to HBox as a child. A row that is not
        # *removable* (the "Atmospheric CO2" block's) gets no remove button.
        option_keys = w.get("_option_keys", ())
        keys = [k for k in w if not k.startswith("_") and k not in option_keys]
        if "type" in keys:
            keys = ["type", *[k for k in keys if k != "type"]]
        w["_remove_btn"].layout.display = ""
        lead = [w["_remove_btn"]] if removable else []
        # The option fields (see _add_row_options) follow on their own line.
        trail = [w["_options"]] if "_options" in w else []
        # Wrap onto further lines instead of overflowing horizontally: a fully
        # populated row (source, path, climatology, constants, ESPER…) is wider
        # than any sensible page. Fields align on their input boxes, since each
        # label sits above its box.
        row = self.W.HBox(
            [*lead, *(w[k] for k in keys), *trail],
            layout=self.W.Layout(flex_flow="row wrap", align_items="flex-end"),
        )
        row.add_class("forge-frow")  # a tile per row
        return row

    def _render(self, cat: str):
        W = self.W
        boxes = [self._row_box(w) for w in self._rows[cat]]
        label = "add bgc source" if cat in ("ic_bgc", "boundary_bgc") else f"add {cat}"
        add = W.Button(description=label, icon="plus", layout=W.Layout(width="150px"))
        add.on_click(lambda _b, c=cat: self._add(c))
        # The IC<->boundary sync button lives at the bottom of the panel it
        # copies *from*, so the rows about to be copied are visible right above
        # it -- rebuilt here (not set once) since `_render` fully replaces
        # `.children` on every add/remove/sync.
        extra = []
        if cat == "ic_bgc":
            extra = [self._sync_to_boundary_btn]
        elif cat == "boundary_bgc":
            extra = [self._sync_to_ic_btn]
        lead = [self.co2_block, self._surface_rows_header] if cat == "surface" else []
        if cat in self._bgc_notes:
            lead.append(self._bgc_notes[cat])
        buttons = W.HBox([add, *extra])
        buttons.add_class("forge-row-btns")
        self._containers[cat].children = [*lead, *boxes, buttons]
        if cat in self._bgc_notes:
            self._sync_bgc_display()

    def clear_category(self, cat: str):
        """Remove all rows for a row category (e.g. ``"boundary_bgc"`` for a
        child/nested grid, which receives boundaries from the parent's
        nesting.nc extraction instead of reanalysis boundary forcing).
        """
        self._rows[cat] = []
        self._render(cat)
        self.on_change()

    def reseed_category(self, cat: str, forcing_inputs: dict[str, Any]):
        """Undo ``clear_category``: seed an empty row category from
        *forcing_inputs* (a ForcingSpec's ``{initial_conditions, forcing}``) the
        way ``__init__`` does. A category that has rows (ones the user re-added
        by hand) is left alone.
        """
        if self._rows[cat]:
            return
        ic = forcing_inputs.get("initial_conditions") or {}
        forc = forcing_inputs.get("forcing") or {}
        self._set_rows(cat, _row_seed_items(cat, ic, forc))
        self.on_change()

    def _set_rows(self, cat: str, items: list[dict[str, Any]]):
        """Replace *cat*'s rows with fresh ones built from *items* (no
        ``on_change``; callers notify).
        """
        self._rows[cat] = [self._make_row(cat, item) for item in items]
        self._render(cat)

    def _sync_bgc(self, from_cat: str, to_cat: str):
        """One-shot copy of every bgc-source row, AND the section-level default
        BGC interpolation method, from ``from_cat`` to ``to_cat`` (``"ic_bgc"``/
        ``"boundary_bgc"``) -- snapshots the source panel's current values and
        replaces the target panel's rows/default with fresh copies built from
        them; not a live link, so later edits to either panel don't affect the
        other until the button is pressed again.

        The default matters alongside the rows, not just the rows themselves: a
        copied row with a blank (inherit-the-default) per-row
        ``bgc_interpolation_method`` would silently change *meaning* if the
        source and target panels' defaults differ (see ``self.ic_bgc_interp``/
        ``self.boundary_bgc_interp``).
        """
        try:
            items = [self._gather_item(from_cat, w) for w in self._rows[from_cat]]
        except ValueError:
            # A box the editor cannot read: copy nothing; the status names it.
            self.on_change()
            return
        self._set_rows(to_cat, items)
        from_attr = _DEFAULT_INTERP_ATTR[from_cat]
        to_attr = _DEFAULT_INTERP_ATTR[to_cat]
        getattr(self, to_attr).value = getattr(self, from_attr).value
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
            # domain_edge_buffer/bgc_source/surface_forcing_source/
            # river_temp_smoothing_window_days/options) apply, and the schema
            # forbids most of them from being paired with a source path.
            item: dict[str, Any] = {"source": {"name": w["name"].value}}
            if not w.get("_custom_file") and w.get("_maybe_attach_custom_file"):
                # Last resort for a path that was typed but never submitted
                # (no Enter / focus-out / Attach click before gather ran):
                # attach it now, without on_change() -- gather is already
                # running inside the wizard's rebuild. A blank box or a bad
                # path leaves `_custom_file` unset and the item is emitted
                # without custom_file, so the schema error still surfaces.
                w["_maybe_attach_custom_file"](w, notify=False)
            if w.get("_custom_file"):
                item["custom_file"] = dict(w["_custom_file"])
            return item
        name_val = w["name"].value
        # `constants`/`ESPER` are derived/inline pseudo-sources: SourceSpec's own
        # validators reject `constants`/`esper_method`/`esper_equation` paired with
        # any other source name, and reject `path` paired with `constants` -- so
        # each is only ever emitted for its matching source name, never leaked in
        # from a hidden widget that still holds a stale value from a previously
        # selected source (see `_apply_row_visibility`).
        is_derived_bgc = name_val in _DERIVED_BGC_SOURCES
        src: dict[str, Any] = {"name": name_val}
        # The checkbox is hidden rather than destroyed when a static source is
        # picked, so a value left over from a previously selected source would
        # otherwise leak into the blueprint and trip roms-tools' ValueError.
        if (
            "climatology" in w
            and w["climatology"].value
            and name_val not in _STATIC_BGC_SOURCES
        ):
            src["climatology"] = True
        if "glorys_layout" in w and w["glorys_layout"].value:  # Dropdown: "" = omit
            src["glorys_layout"] = w["glorys_layout"].value
        if (
            "path" in w and w["path"].value.strip() and name_val != "constants"
        ):  # blank = derive default path; a 'constants' source takes no path
            src["path"] = w["path"].value.strip()
        if "constants" in w and name_val == "constants":  # {"key": float, ...}
            constants = _parse_field(
                _parse_constants,
                w["constants"].value,
                f"{self._row_label(cat, w)}, {_row_desc('constants', 'constants')}",
            )
            if constants:
                src["constants"] = constants
        if (
            "esper_method" in w and w["esper_method"].value and name_val == "ESPER"
        ):  # Dropdown: "" = unset
            src["esper_method"] = w["esper_method"].value
        if "esper_equation" in w and w["esper_equation"].value and name_val == "ESPER":
            src["esper_equation"] = int(w["esper_equation"].value)
        if (
            "esper_salinity_conditioning" in w
            and w["esper_salinity_conditioning"].value
            and name_val == "ESPER"
        ):  # Checkbox: unchecked = the model default (off), emitted by neither side
            src["esper_salinity_conditioning"] = True
        item = {"source": src}
        if "type" in w:
            item["type"] = w["type"].value
        if "use_vars" in w and (use_vars := _use_vars_list(w["use_vars"].value)):
            item["use_vars"] = use_vars
        if "correct_radiation" in w and w["correct_radiation"].value:
            item["correct_radiation"] = True
        if "wind_dropoff" in w and w["wind_dropoff"].value:
            item["wind_dropoff"] = True
        if "coarse_grid_mode" in w:
            item["coarse_grid_mode"] = w["coarse_grid_mode"].value
        if (
            "blend_fallback" in w
            and w["blend_fallback"].value != _BLEND_NONE
            and w["type"].value == SurfaceType.PHYSICS.value
        ):
            fname = w["blend_fallback"].value
            carried = w["_blend_fallback_source"]
            # Source fields beyond the name only carry over to the same source.
            fsrc = (
                {**carried, "name": fname}
                if str(carried.get("name", "")).upper() == fname.upper()
                else {"name": fname}
            )
            fallback: dict[str, Any] = {"source": fsrc}
            if w["blend_fallback_correct_radiation"].value:
                fallback["correct_radiation"] = True
            if w["_blend_fallback_options"]:
                fallback["options"] = dict(w["_blend_fallback_options"])
            item["blend"] = {
                "fallback": fallback,
                "width_km": float(w["blend_width_km"].value),
                "profile": w["blend_profile"].value,
            }
        if "restoring_forces" in w and w["restoring_forces"].value.strip():
            item["restoring_forces"] = [
                p.strip() for p in w["restoring_forces"].value.split(",") if p.strip()
            ]
        # Shared surface/tidal regrid knobs: only emit non-default values to keep
        # specs clean. bgc_interpolation_method (ic_bgc/boundary_bgc rows) uses the
        # blank-sentinel convention instead (blank = inherit the section default);
        # constants/ESPER are derived/inline pseudo-sources, not a regridded
        # dataset, so it never applies to them (mirrors the hidden-widget gate in
        # `_apply_row_visibility`).
        if (
            "bgc_interpolation_method" in w
            and w["bgc_interpolation_method"].value
            and not is_derived_bgc
        ):
            item["bgc_interpolation_method"] = w["bgc_interpolation_method"].value
        # Item level, not `src`: serialize_dask is a Forge write option, not a
        # roms-tools source parameter. No longer editable here (see `_make_row`);
        # re-emitted only when the loaded blueprint already carried it, so a
        # round-trip through the wizard neither invents nor drops it.
        if w.get("_serialize_dask"):
            item["serialize_dask"] = True
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
        if (
            "surface_forcing_source_name" in w
            and w["surface_forcing_source_name"].value
        ):
            temp_src: dict[str, Any] = {"name": w["surface_forcing_source_name"].value}
            if (
                "surface_forcing_source_path" in w
                and w["surface_forcing_source_path"].value.strip()
            ):  # blank = the remote ARCO ERA5 archive
                temp_src["path"] = w["surface_forcing_source_path"].value.strip()
            item["surface_forcing_source"] = temp_src
        # Emitted whenever non-default, even with no temperature source selected
        # (roms-tools ignores it then): dropping it would silently change the
        # content_hash of a loaded blueprint that carries the value -- same
        # round-trip-fidelity rule as domain_edge_buffer. Exception: while the
        # widget is hidden (no source), a stale non-positive value is dropped
        # rather than emitted -- otherwise the schema's `> 0` check would reject
        # the blueprint naming a field the user cannot see to fix.
        if "river_temp_smoothing_window_days" in w:
            window = float(w["river_temp_smoothing_window_days"].value)
            visible = bool(
                w.get("surface_forcing_source_name")
                and w["surface_forcing_source_name"].value
            )
            default_window = _ROW_OPTION_DEFAULTS["river"][
                "river_temp_smoothing_window_days"
            ]
            if window != default_window and (visible or window > 0):
                item["river_temp_smoothing_window_days"] = window
        if "convert_to_climatology" in w:
            item["convert_to_climatology"] = w["convert_to_climatology"].value
        if (
            "coast_snap_buffer_km" in w and w["coast_snap_buffer_km"].value
        ):  # 0.0 = leave unset
            item["coast_snap_buffer_km"] = float(w["coast_snap_buffer_km"].value)
        default_edge = _ROW_OPTION_DEFAULTS["river"]["domain_edge_buffer"]
        if "domain_edge_buffer" in w and int(w["domain_edge_buffer"].value) != (
            default_edge
        ):
            item["domain_edge_buffer"] = int(w["domain_edge_buffer"].value)
        item.update(copy.deepcopy(w.get("_carried") or {}))
        if "options" in w:  # advanced passthrough; omit when empty
            opts = _parse_field(
                _parse_options,
                w["options"].value,
                f"{self._row_label(cat, w)}, {_row_desc('options', 'options')}",
            )
            if opts:
                item["options"] = opts
        return item

    def _row_label(self, cat: str, w: dict[str, Any]) -> str:
        """Where row ``w`` of ``cat`` sits, as the editor shows it
        ("Surface forcing, row 2"), for a message about one of its boxes.
        """
        for n, row in enumerate(self._rows.get(cat, []), start=1):
            if row is w:
                return _row_place(cat, n)
        return _CATEGORY_TITLES.get(cat, cat)

    def gather(self) -> dict[str, Any]:
        items = {
            cat: [self._gather_item(cat, w) for w in self._rows[cat]]
            for cat in _FORCING_CATEGORIES
        }
        items["surface"] = self._with_co2(items["surface"])
        forcing: dict[str, list[dict[str, Any]] | dict[str, Any] | None] = dict(items)
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
        bgc_sources = [self._gather_item("ic_bgc", w) for w in self._rows["ic_bgc"]]
        if bgc_sources:
            ic["bgc_sources"] = bgc_sources
        if (
            self.ic_bgc_interp.value
            and self.ic_bgc_interp.value != BgcInterpMethod.DEPTH.value
        ):
            ic["bgc_interpolation_method"] = self.ic_bgc_interp.value
        if self.ic_flex_time.value:
            ic["allow_flex_time"] = True
        if not self.ic_validate.value:  # checked ("validate") is the default
            ic["bypass_validation"] = True
        if not self.ic_groom.value:  # checked ("groom") is the default
            ic["groom"] = False
        if self.ic_groom.value and self._ic_groom_settings:
            ic["groom_settings"] = copy.deepcopy(self._ic_groom_settings)
        if self.ic_prefill.value:  # Dropdown: "" = leave unset
            ic["prefill"] = self.ic_prefill.value
        if self.ic_regrid_method.value:
            ic["regrid_method"] = self.ic_regrid_method.value
        if self.ic_extrap_method.value:
            ic["extrap_method"] = self.ic_extrap_method.value
        ic_blend = _parse_blend_m(self.ic_bottom_fill_blend_m.value)
        if ic_blend is not None:  # blank = leave unset (roms-tools' default)
            ic["bottom_fill_blend_m"] = ic_blend
        ic.update(copy.deepcopy(self._ic_carried))
        ic_opts = _parse_field(
            _parse_options,
            self.ic_options.value,
            f"{_CATEGORY_TITLES['initial_conditions']}, "
            f"{label_for('ic.ic_options', default='options').label}",
        )
        if ic_opts:
            ic["options"] = ic_opts

        # Boundary: a structural mirror of the IC dict above -- BoundaryForcing has
        # the identical source/bgc_sources/bgc_interpolation_method/prefill/etc.
        # shape as InitialConditions (see forge_blueprint.BoundaryForcing) -- except
        # boundary has no IC-style omit-the-key convention: `Forcing.boundary` is
        # `BoundaryForcing | None`, so "(none)" emits an explicit `None`, not an
        # omitted key (mirrors the durable child-grid clear in
        # `ForgeBlueprintWizard._gather`, which forces this same field to `None`).
        if self.boundary_name.value == _BOUNDARY_NONE:
            boundary: dict[str, Any] | None = None
        else:
            boundary_source = {"name": self.boundary_name.value}
            if self.boundary_layout.value:
                boundary_source["glorys_layout"] = self.boundary_layout.value
            if self.boundary_path.value.strip():
                boundary_source["path"] = self.boundary_path.value.strip()
            boundary = {"source": boundary_source}
            boundary_bgc_sources = [
                self._gather_item("boundary_bgc", w) for w in self._rows["boundary_bgc"]
            ]
            if boundary_bgc_sources:
                boundary["bgc_sources"] = boundary_bgc_sources
            if (
                self.boundary_bgc_interp.value
                and self.boundary_bgc_interp.value != BgcInterpMethod.DEPTH.value
            ):
                boundary["bgc_interpolation_method"] = self.boundary_bgc_interp.value
            if not self.boundary_validate.value:  # checked ("validate") is default
                boundary["bypass_validation"] = True
            if not self.boundary_groom.value:  # checked ("groom") is the default
                boundary["groom"] = False
            if self.boundary_groom.value and self._boundary_groom_settings:
                boundary["groom_settings"] = copy.deepcopy(
                    self._boundary_groom_settings
                )
            if self.boundary_prefill.value:
                boundary["prefill"] = self.boundary_prefill.value
            if self.boundary_regrid_method.value:
                boundary["regrid_method"] = self.boundary_regrid_method.value
            if self.boundary_extrap_method.value:
                boundary["extrap_method"] = self.boundary_extrap_method.value
            boundary_blend = _parse_blend_m(self.boundary_bottom_fill_blend_m.value)
            if boundary_blend is not None:
                boundary["bottom_fill_blend_m"] = boundary_blend
            boundary.update(copy.deepcopy(self._boundary_carried))
            boundary_opts = _parse_field(
                _parse_options,
                self.boundary_options.value,
                f"{_CATEGORY_TITLES['boundary']}, "
                f"{label_for('boundary.boundary_options', default='options').label}",
            )
            if boundary_opts:
                boundary["options"] = boundary_opts

        forcing["boundary"] = boundary
        return {
            "initial_conditions": ic,
            "forcing": forcing,
        }

    def _bgc_settings(self, cat: str) -> Any:
        """The Biogeochemistry subsection's settings row for *cat*."""
        pre = "ic" if cat == "ic_bgc" else "boundary"
        return components.field_grid(
            self.W,
            [
                components.field_row(
                    self.W,
                    f"{pre}.{pre}_bgc_interp",
                    getattr(self, f"{pre}_bgc_interp"),
                ),
                components.field_row(
                    self.W, f"{pre}.{pre}_groom", getattr(self, f"{pre}_groom")
                ),
            ],
        )

    def _section_pane(self, prefix: str, bgc_cat: str) -> Any:
        """The Initial conditions / Boundary forcing pane body: the physics source,
        the Biogeochemistry sources, then the settings both share.
        """
        W = self.W

        def row(suffix: str, **kw: Any) -> Any:
            return components.field_row(
                W,
                f"{prefix}.{prefix}_{suffix}",
                getattr(self, f"{prefix}_{suffix}"),
                **kw,
            )

        fields = {
            s: w
            for s in _SECTION_OPTION_DEFAULTS
            if (w := getattr(self, f"{prefix}_{s}", None)) is not None
        }
        body = W.VBox(
            [
                components.field_grid(W, [row(s) for s in fields if s != "options"]),
                row("options", width="520px"),
            ]
        )
        options = _options_link(
            W,
            body,
            _ROW_OPTIONS_TITLE,
            fields,
            {s: _SECTION_OPTION_DEFAULTS[s] for s in fields},
        )
        options.add_class("forge-sec-opts")  # set off like a subsection
        physics = components.subsection(
            W,
            "forcing.physics",
            components.field_grid(W, [row("name"), row("layout"), row("path")]),
        )
        return W.VBox([physics, self._bgc_sections[bgc_cat], options])

    @property
    def widget(self):
        if getattr(self, "_acc", None) is not None:
            return self._acc
        panes = {
            "initial_conditions": self._section_pane("ic", "ic_bgc"),
            "boundary": self._section_pane("boundary", "boundary_bgc"),
        }
        acc = components.open_accordion(
            self.W,
            [
                panes.get(cats[0], self._containers.get(cats[0]))
                for cats in _FORCING_PANES
            ],
            [_CATEGORY_TITLES[cats[0]] for cats in _FORCING_PANES],
        )
        self._acc = acc
        return acc

    def refresh_bgc_notes(self) -> None:
        """Warn on the Biogeochemistry sections, before Forge refuses the blueprint,
        when two or more sources do not partition the tracers, and mark the boxes to
        fix.

        Mirrors Forge's ``_require_partitioned_bgc_use_vars``: with two or more
        sources each one needs its own "Variables to read", and no variable may be
        read twice. Only for a section the blueprint carries (IC or boundary not
        "(none)"); a child grid's boundary rows are already cleared.
        """
        label = _row_desc("use_vars", "use_vars")
        for cat, note in self._bgc_notes.items():
            rows = self._rows[cat]
            lines: list[str] = []
            flagged: set[int] = set()
            if self._bgc_carried(cat) and len(rows) > 1:
                read_by: dict[str, int] = {}
                for n, w in enumerate(rows, start=1):
                    use_vars = _use_vars_list(w["use_vars"].value)
                    name = w["name"].value
                    if not use_vars:
                        source = f" ({html.escape(name)})" if name else ""
                        lines.append(f"Row {n}{source} has no {label}.")
                        flagged.add(n)
                    for var in use_vars:
                        if var in read_by:
                            other = read_by[var]
                            lines.append(
                                f"Row {n} lists {html.escape(var)} twice."
                                if other == n
                                else f"Rows {other} and {n} both read "
                                f"{html.escape(var)}; list it under one source only."
                            )
                            flagged.update((other, n))
                        read_by.setdefault(var, n)
            for n, w in enumerate(rows, start=1):
                if n in flagged:
                    w["use_vars"].add_class("forge-need")
                else:
                    w["use_vars"].remove_class("forge-need")
            if lines:
                # One block: the banner lays out its children side by side.
                note.value = components.banner(
                    "warn",
                    f"<div>{html.escape(_use_vars_rule())}<br>{'<br>'.join(lines)}</div>",
                )
                note.layout.display = ""
            else:
                note.value = ""
                note.layout.display = "none"

    def retitle(self, summary_for) -> None:
        """Refresh each pane's header with its live summary pills and a quiet
        Required/Optional (see :func:`cstar.wizard.ui.components.accordion_title`).

        ``summary_for(cat)`` returns the pills for category ``cat``; a pane that
        shows two categories carries both categories' pills. A no-op before
        ``widget`` has been built.
        """
        if getattr(self, "_acc", None) is None:
            return
        for i, cats in enumerate(_FORCING_PANES):
            chip = (
                "Required"
                if _REQUIRED_FORCING_CATEGORIES.intersection(cats)
                else "Optional"
            )
            self._acc.set_title(
                i,
                components.accordion_title(
                    _CATEGORY_TITLES[cats[0]],
                    *(pill for cat in cats for pill in summary_for(cat)),
                    chip=chip,
                ),
            )
