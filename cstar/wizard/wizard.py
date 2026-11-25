"""An ``ipywidgets`` wizard for assembling and reviewing a :class:`ForgeBlueprint`.

This is a thin UI shell over :func:`cstar.applications.forge.resolve.build_forge_blueprint`:
the widgets only *collect inputs and display the resolved result* — all resolution
and validation stay in the resolver. That keeps the notebook UI interchangeable with
any future app/WASM front-end and lets the logic be tested without rendering.

Usage (in a Jupyter notebook)::

    from cstar.wizard.wizard import ForgeBlueprintWizard
    wiz = ForgeBlueprintWizard()
    wiz.display()
    # ... pick a model + domain, tweak fields, review the live YAML, Save ...
    cfg = wiz.config            # the current resolved ForgeBlueprint (or None if invalid)

The wizard discovers Models from ``catalog/specs/ModelSpec/`` and Domains from
``catalog/specs/DomainSpec/``; selecting a cataloged Domain prefills the grid kwargs,
boundaries, partitioning, and dates. The live preview re-resolves on every change
and never builds a grid for it: the time step is derived from the grid size by
plain CFL arithmetic until the user types one.
"""

from __future__ import annotations

import base64
import copy
import html
import re
import warnings
from collections import Counter
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, NamedTuple

import yaml
from pydantic import BaseModel, ValidationError

from cstar.applications.forge.blueprint import (
    DEFAULT_APPLICATION,
    PLACEHOLDER_DESCRIPTION,
    BgcSourceItem,
    BoundaryForcing,
    CdrSpec,
    Composition,
    ForgeBlueprint,
    ForgeProvenance,
    InitialConditions,
    RiverForcingItem,
    SourceSpec,
    SpecRef,
    SurfaceBlendFallback,
    SurfaceForcingItem,
    TidalForcingItem,
    grid_build_kwargs,
    split_child_nesting_metadata,
)
from cstar.applications.forge.namelist_model import (
    cdr_tracer_role,
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import (
    OUTPUT_SECTIONS,
    PARTIAL_OUTPUT_SECTIONS,
    build_forge_blueprint,
    cfl_dt,
    default_blueprint_description,
    extract_output_settings,
    force_switches_without_input_off,
    load_model_spec_data,
    read_cdr_forcing_yaml,
    resolver_warnings,
    with_cdr_lite_tracer_set,
)
from cstar.base.digest import hash_netcdf_contents
from cstar.wizard.app import ForgeBlueprintWizardApp as ForgeBlueprintWizardApp
from cstar.wizard.fields import (
    _CDR_FILE_HINT,
    _dd_options,
    _dd_values,
    _same_attached_file,
    _stage_uploaded_netcdf,
    _tip,
    _user_file_status_html,
)
from cstar.wizard.fields import HELP_TEXT as HELP_TEXT
from cstar.wizard.forcing_editor import (
    _BOUNDARY_NONE,
    _CATEGORY_TITLES,
    _IC_NONE,
    _IC_SOURCE_OPTS,
    _PATH_PLACEHOLDER,
    _ForcingEditor,
    _is_co2_item,
    _row_desc,
    _row_place,
    _use_vars_rule,
)
from cstar.wizard.model_card import ModelCard, _pinned_roms_ref
from cstar.wizard.settings_editor import (
    _ACCORDION_EXCLUDED_FIELDS,
    _FORCING_CARD,
    _FORCING_CARD_FIELDS,
    _MODEL_CARD,
    _MODEL_CARD_FIELDS,
    _OUTPUT_CARD,
    _OUTPUT_MORE_TITLE,
    _applies_in,
    _EditorKey,
    _in_forcing_card,
    _in_model_card,
    _in_output_card,
    _SettingsEditor,
)
from cstar.wizard.settings_editor import LABEL_TEXT as LABEL_TEXT
from cstar.wizard.ui import components
from cstar.wizard.ui.catalog_bar import schedule_confirm_reset
from cstar.wizard.ui.labels import known_keys, label_for, section_for

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence

WIZARD_TOOL: Final[str] = "wizard"
"""The ``provenance.generated_by.tool`` of a blueprint the wizard writes."""
# What the Download links say while they wait (see _refresh_download_links).
_DOWNLOAD_WAITS_FOR_BOUNDARIES: Final[str] = (
    "The download is ready once the open boundaries are derived from the grid: "
    "press Derive from grid in the Grid section, set the boundaries by hand, or "
    "save the blueprint."
)


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
        "cdr_lite",
        "use_pio",
        "marbl",
        "co2_tvarying",
        "sal_restore",
        "tides",
    }
)
_TIDES_DERIVED_LEAVES = frozenset({"ntides"})
# ``v_sponge.v_sponge`` is the domain's (grid-derived or explicit); ``v_sponge_trc``
# is the model's.
_V_SPONGE_DERIVED_LEAVES = frozenset({"v_sponge"})


def _model_owned_settings(effective: dict[str, Any]) -> dict[str, Any]:
    """The model-owned subset of a resolved ``model_settings`` dict.

    The exact inverse of ``build_forge_blueprint``'s mutations (forge_blueprint_
    resolve.py): drops whole Domain/Forcing-owned or resolver-derived sections,
    every OUTPUT section/partial-output field (an OutputSpec's job), and the
    resolver-derived leaves within otherwise-kept sections (``param``'s grid/
    partitioning dims, ``cppdefs``'s domain/forcing/bgc-driven flags, ``tides``'s
    ``ntides``, ``v_sponge``'s ``v_sponge``). What remains is safe to bake into a new ModelSpec's
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
    if "v_sponge" in out:
        for leaf in _V_SPONGE_DERIVED_LEAVES:
            out["v_sponge"].pop(leaf, None)
        if not out["v_sponge"]:  # nothing model-owned left (ucla-roms < 0.13.0)
            del out["v_sponge"]
    for sec, fields in PARTIAL_OUTPUT_SECTIONS.items():
        if sec in out:
            for f in fields:
                out[sec].pop(f, None)
    return out


def _split_forcing_data(
    d: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Split a raw ForcingSpec dict (as read from its catalog file) into the
    ``{initial_conditions, forcing}`` shape the forcing editor expects, plus the
    optional embedded ``cdr_forcing`` block (``None`` if absent). Strips
    ``description`` (not part of either). Shared by the round-trip verifier and
    ``_on_forcing_spec``/``_populate_from`` so CDR routes consistently everywhere
    a ForcingSpec is read.

    ``d`` is already at the current schema (``catalog.forcing_data``); deep-copied
    first so this never mutates the catalog's own parsed dict.
    """
    d = copy.deepcopy(d)
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


# Values the wizard puts over the ModelSpec's while the setting's widget is shown,
# until the user edits it; a fresh session and a model pick seed them, a loaded
# blueprint keeps its own values. The ModelSpec cannot carry these:
# check_cdr_lite_sections refuses a carbonate-sensitivity hold outside bgc_mode
# "cdr_lite". UCLA-ROMS writes the sensitivities time-averaged by default
# (cdr_gas_exch_output.do_avg), and an averaged record is held, not interpolated.
_WIZARD_SETTING_DEFAULTS: dict[tuple[str, str], Any] = {
    ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation"): False,
}


def _display_round(value: float) -> float:
    """A derived number rounded for display: one decimal, or three significant
    digits below 1 (where one decimal could round it to zero).
    """
    return round(value, 1) if abs(value) >= 1 else float(f"{value:.3g}")


# Sticky-bar step links (card key -> label), in the order the cards appear.
_STICKY_STEPS = (
    ("model", "Model"),
    ("grid", "Grid"),
    ("forcing", "Forcing"),
    ("run", "Run"),
    ("output", "Output"),
    ("advanced", "Advanced"),
    ("review", "Save"),
    ("next", "Next steps"),
)

# ---------------------------------------------------------------------------
# An invalid blueprint's message: the card that owns it, in the wizard's words
# ---------------------------------------------------------------------------

# The card (a `_STICKY_STEPS` key) that owns each part of a blueprint, so a
# blueprint that does not resolve says so on the card the user fixes it in (see
# `_blueprint_problems`). Keys are dotted blueprint paths -- where a pydantic error
# sits, or the path Forge's own messages start with -- and the place names the
# wizard's own messages start with. The longest key that starts the text wins, so
# ``domain.partitioning`` (the Run card's processor layout) beats ``domain``.
# ``model_settings.<section>.<field>`` is placed by field (see `_settings_card`).
_ERROR_CARDS: dict[str, str] = {
    "run": "run",
    "description": "run",
    "cdr": "run",
    "partitioning": "run",
    "domain.partitioning": "run",
    "domain": "grid",
    "forcing": "forcing",
    "initial_conditions": "forcing",
    "boundary": "forcing",
    "surface": "forcing",
    "tidal": "forcing",
    "river": "forcing",
    "bgc_mode": "model",
    "code": "model",
    "carbonate_sensitivity": "model",
    "carbonate sensitivities": "model",
    **dict.fromkeys(_CATEGORY_TITLES.values(), "forcing"),
}
_ERROR_CARD_KEYS = sorted(_ERROR_CARDS, key=len, reverse=True)
# What may follow a key for it to start the text: a path goes on (".", "["), or a
# message goes on about it.
_ERROR_KEY_ENDS = frozenset(".[,:=( ")

# Where each Forge section model sits in a blueprint. Forge builds most sections on
# their own before the blueprint itself, so a pydantic error's location is relative
# to the model that raised (the error's ``title``), not to the blueprint.
_ERROR_MODEL_PATHS: dict[str, str] = {
    "ForgeBlueprint": "",
    "RunWindow": "run",
    "Domain": "domain",
    "OpenBoundaries": "domain.open_boundaries",
    "Partitioning": "domain.partitioning",
    "Forcing": "forcing",
    "InitialConditions": "initial_conditions",
    "BoundaryForcing": "forcing.boundary",
    "SurfaceForcingItem": "forcing.surface",
    "SurfaceBlend": "forcing.surface",
    "SurfaceBlendFallback": "forcing.surface",
    "TidalForcingItem": "forcing.tidal",
    "RiverForcingItem": "forcing.river",
    "BgcSourceItem": "forcing",
    "SourceSpec": "forcing",
    "CdrSpec": "cdr",
    "CarbonateSensitivitySpec": "carbonate_sensitivity",
}

# The forcing editor's row category behind each list path in a blueprint, so a
# message about ``initial_conditions.bgc_sources[0]`` names the row the editor shows
# ("BGC initial conditions, row 1"), and one about a dotted list path as a whole
# names its pane ("BGC initial conditions").
_ROW_PATHS: dict[str, str] = {
    "initial_conditions.bgc_sources": "ic_bgc",
    "forcing.boundary.bgc_sources": "boundary_bgc",
    "boundary.bgc_sources": "boundary_bgc",
    "forcing.surface": "surface",
    "surface": "surface",
    "forcing.tidal": "tidal",
    "tidal": "tidal",
    "forcing.river": "river",
    "river": "river",
}
_ROW_PATH_RE = re.compile(
    r"(?<![\w.])("
    + "|".join(re.escape(p) for p in sorted(_ROW_PATHS, key=len, reverse=True))
    + r")(?:\[(\d+)\](?:\.(\w+))?|(?![\w.]))"
)
# A snake_case name (one with an underscore), which a message may use for a box.
_FIELD_NAME_RE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")
# Forge's refusals of BGC sources that do not partition the tracers (see
# `_require_partitioned_bgc_use_vars` in the Forge blueprint), which the wizard says
# in its own words (see `_use_vars_rule`).
_MISSING_USE_VARS_RE = re.compile(
    r"(\S+) \(source='([^']*)'\) has no use_vars, but multiple bgc_sources"
)
_SHARED_USE_VARS_RE = re.compile(
    r"(\S+) \(source='[^']*'\) and bgc_sources\[(\d+)\] \(source='[^']*'\) "
    r"both claim '([^']*)' via use_vars"
)
# How much of one problem a card banner shows; the preview has the full message.
_PROBLEM_CHARS = 600
_SUMMARY_CHARS = 200


@dataclass(frozen=True)
class _BlueprintProblem:
    """One reason the blueprint does not resolve: the card that owns it (a
    `_STICKY_STEPS` key) and what to fix, in the wizard's words (plain text).
    """

    card: str
    text: str


def _settings_card(section: str, field: str | None) -> str:
    """The card that shows the ``(section, field)`` setting."""
    if _in_output_card(section, field):
        return "output"
    if _in_model_card(section, field):
        return "model"
    if _in_forcing_card(section, field):
        return "forcing"
    return "advanced"


def _error_card(text: str) -> str | None:
    """The card that owns the blueprint path or wizard place ``text`` starts with
    (see `_ERROR_CARDS`), or ``None`` when it starts with neither.
    """
    if text.startswith("model_settings."):
        section, _, rest = text.removeprefix("model_settings.").partition(".")
        field = re.match(r"\w*", rest)
        return _settings_card(section, field[0] if field and field[0] else None)
    for key in _ERROR_CARD_KEYS:
        if text.startswith(key) and (
            len(text) == len(key) or text[len(key)] in _ERROR_KEY_ENDS
        ):
            return _ERROR_CARDS[key]
    return None


def _row_field_label(name: str) -> str | None:
    """The forcing row box behind field ``name`` (``use_vars`` -> "Variables to
    read"), or ``None`` when no row box has that name.
    """
    key = f"forcing.row.{name}"
    return label_for(key).label if key in known_keys() else None


def _wizard_words(text: str) -> str:
    """``text`` with the blueprint paths and field names it uses replaced by the
    place and label the wizard shows: ``initial_conditions.bgc_sources[0]`` ->
    "BGC initial conditions, row 1", ``use_vars`` -> "Variables to read". Forge's
    refusal of BGC sources that do not partition the tracers is reworded whole.
    """
    if m := _MISSING_USE_VARS_RE.match(text):
        label = _row_desc("use_vars", "use_vars")
        return f"{_wizard_words(m[1])} ({m[2]}) has no {label}. {_use_vars_rule()}"
    if m := _SHARED_USE_VARS_RE.match(text):
        return (
            f"{_wizard_words(m[1])} and row {int(m[2]) + 1} both read {m[3]}. "
            "List each variable under one source only."
        )

    def row(m: re.Match[str]) -> str:
        cat = _ROW_PATHS[m[1]]
        if m[2] is None:  # the whole list: a bare word ("river") may be prose
            return _CATEGORY_TITLES[cat] if "." in m[1] else m[0]
        place = _row_place(cat, int(m[2]) + 1)
        if m[3]:
            place += f", {_row_field_label(m[3]) or m[3]}"
        return place

    text = _ROW_PATH_RE.sub(row, text)
    return _FIELD_NAME_RE.sub(lambda m: _row_field_label(m[0]) or m[0], text)


def _message_lines(text: str) -> list[str]:
    """``text``'s non-blank lines, stripped, without the "- " that starts each
    finding of a message listing them one per line under a lead-in ending in ":".
    """
    lines = (line.strip().removeprefix("- ") for line in text.splitlines())
    return [line for line in lines if line]


def _error_path(base: str, loc: tuple[int | str, ...]) -> str:
    """A pydantic error location under ``base``, as a dotted blueprint path."""
    path = base
    for part in loc:
        if isinstance(part, int):
            path += f"[{part}]"
        else:
            path = f"{path}.{part}" if path else str(part)
    return path


def _blueprint_problems(exc: Exception) -> list[_BlueprintProblem]:
    """Why building the blueprint raised ``exc``: one problem per pydantic error
    (one for any other exception), each placed on the card that owns it.

    The card comes from the path the message starts with (Forge's own messages
    name one, and the wizard's name its place) or, for a message that lists its
    findings as "  - " lines, the first of those that names one; else from where
    the error sits. A problem none of these places goes to Review.
    """
    if not isinstance(exc, ValidationError):
        message = str(exc)
        card = next(filter(None, map(_error_card, _message_lines(message))), "review")
        return [_BlueprintProblem(card, _wizard_words(message))]
    base = _ERROR_MODEL_PATHS.get(exc.title, "")
    problems = []
    for err in exc.errors():
        # A validator's own ValueError, without pydantic's "Value error, " prefix.
        cause = (err.get("ctx") or {}).get("error")
        message = str(cause) if cause is not None else err["msg"]
        path = _error_path(base, err["loc"])
        text = _wizard_words(message)
        owner = _error_card(message)
        if owner is None:
            owner = _error_card(path) or "review"
            # Where in the section; the card itself says which section.
            if err["loc"]:
                text = f"{_wizard_words(path)}: {text}"
        problems.append(_BlueprintProblem(owner, text))
    return problems


def _problem_html(text: str, limit: int, cut_note: str) -> str:
    """``text`` escaped, its line breaks kept; past ``limit`` characters it is cut
    and ``cut_note`` (HTML, pointing at the full message) follows.
    """
    if len(text) <= limit:
        return html.escape(text).replace("\n", "<br>")
    cut = html.escape(text[: limit - 1].rstrip()).replace("\n", "<br>")
    return f"{cut}…{cut_note}"


def _problem_summary(problems: Sequence[_BlueprintProblem]) -> str:
    """Review's one-line HTML summary of why the blueprint does not resolve: the
    first problem's first line, and a link to the card that owns it.
    """
    if not problems:
        return ""
    first = problems[0]
    lines = _message_lines(first.text) or [""]
    # A message that lists its findings: its lead-in and the first of them.
    shown = 2 if lines[0].endswith(":") else 1
    line = " ".join(lines[:shown])
    cut_note = " — full message in the preview below"
    text = _problem_html(line, _SUMMARY_CHARS, cut_note)
    if len(lines) > shown and len(line) <= _SUMMARY_CHARS:  # not cut, but more
        text += cut_note
    if len(problems) > 1:
        text += f" (and {len(problems) - 1} more)"
    if first.card == "review":
        return text
    title = section_for(first.card).title
    return f"{title}: {text} <a href='#forge-sec-{first.card}'>Go to {title}</a>"


# A nested grid's topography file: blank follows its topography source dropdown
# (this grid's file when that reads "(same as this grid)").
_NESTED_TOPO_PATH_PLACEHOLDER = "Leave blank to use the source above"


def _run_time_label(value: datetime) -> str:
    """A run datetime for the status bar: the date, with the time when it is not
    midnight (a loaded nested run starts at 06:00).
    """
    if value.time() == datetime.min.time():
        return value.date().isoformat()
    return value.isoformat(sep=" ", timespec="minutes")


def _run_time_iso(value: datetime) -> str:
    """A run datetime as a DomainSpec writes it: the date alone at midnight, else
    the full ISO-8601 datetime.
    """
    if value.time() == datetime.min.time():
        return value.date().isoformat()
    return value.isoformat()


def _iso_time(value: Any) -> Any:
    """``value`` as an ISO-8601 string when it is a date, a datetime or text
    that reads as one; anything else unchanged.
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return value
    return value


def _cdr_times_as_iso(cdr_forcing: dict[str, Any]) -> dict[str, Any]:
    """A copy of a ``CDRForcing`` kwargs dict with its times as ISO-8601 text,
    so a dict read from YAML (datetimes) compares equal to a compiled one.
    """
    out = copy.deepcopy(cdr_forcing)
    for key in ("start_time", "end_time"):
        if key in out:
            out[key] = _iso_time(out[key])
    for release in out.get("releases") or []:
        if isinstance(release, dict) and isinstance(release.get("times"), list):
            release["times"] = [_iso_time(t) for t in release["times"]]
    return out


def _cdr_simple_kept_reason(cdr_forcing: dict[str, Any]) -> str:
    """What a CDR forcing has beyond one release at a constant ALK flux, for the
    simple panel's note ("2 releases, a DIC flux").
    """
    releases = [r for r in cdr_forcing.get("releases") or [] if isinstance(r, dict)]
    reasons = []
    if len(releases) != 1:
        reasons.append(f"{len(releases)} releases")
    fluxes = [r.get("tracer_fluxes") or {} for r in releases]
    if others := sorted({t for f in fluxes for t in f} - {"ALK"}):
        reasons.append(f"a flux of {', '.join(others)}")
    if any(
        isinstance(v, list) and any(x != v[0] for x in v)
        for f in fluxes
        for v in f.values()
    ):
        reasons.append("a flux that changes in time")
    return ", ".join(reasons) or "settings these fields do not set"


def _cdr_forcing_from_netcdf(path: str | Path, grid: Any) -> Any:
    """Reconstruct a roms-tools ``CDRForcing`` from a pre-made CDR-forcing
    netCDF, for the CDR plot widget's "netcdf" mode (there is no
    ``CDRForcing.from_netcdf`` in roms-tools -- this is the wizard's own
    best-effort inverse of ``CDRForcingDatasetBuilder.build()``, see
    ``roms_tools.setup.cdr_forcing``).

    Every variable is selected by dimension name (``ncdr``/``ntracers``/
    ``time``), never by positional axis order, since the builder doesn't
    guarantee one. A file with generated tracers (``CDR_*``/``passive_tracer*``,
    ``tracer_set`` ``cdr_lite``/``passive``) names the release feeding each in
    its ``tracer_release`` coordinate (roms-tools >= 5.1): such a release is
    rebuilt from its own columns, keyed by role (``ALK``/``DIC``/
    ``passive_tracer``) with its ``tracer_set``. Any other release is a MARBL
    one: its tracer-perturbation fluxes are the ``ALK`` column alone when ``ALK``
    is on the axis (otherwise every non-generated column), and its volume
    concentrations every non-generated column. Three things this can't recover
    exactly, all acceptable for a plot-only reconstruction:

    * The builder interpolates every release onto the *union* of all releases'
      original times before writing the file, so the file carries one shared
      ``time`` axis -- a reconstructed release's ``times`` is that shared
      axis, not necessarily whatever the original release actually used.
    * A tracer-perturbation file with no ``tracer_name`` coordinate at all
      (unusual -- normal output always carries one) has no way to know which
      tracer a flux series belongs to; it's arbitrarily labeled "ALK".
    * A passive volume release's ``temp``/``salt`` (the file's rows for them
      hold the discharged water's physics) are left to roms-tools' defaults.

    Raises a ``ValueError`` prefixed ``"could not reconstruct releases from
    <path>: ..."`` for any failure, including the ones ``rt.CDRForcing``'s own
    validators would otherwise raise a more confusing pydantic error for (e.g.
    a single-time-point file, which fails its strict ``start_time <
    end_time``).
    """
    import pandas as pd
    import roms_tools as rt
    import xarray as xr

    path = Path(path)
    try:
        with xr.open_dataset(path) as ds:
            if "release_name" not in ds.coords:
                raise ValueError(
                    "no 'release_name' coordinate found -- is this really a "
                    "CDR-forcing file?"
                )
            release_names = [str(n) for n in ds["release_name"].values]
            times = [t.to_pydatetime() for t in pd.to_datetime(ds["time"].values)]
            if len(times) < 2 or times[0] >= times[-1]:
                raise ValueError(
                    "fewer than two distinct times on the shared 'time' axis -- "
                    "a CDRForcing requires start_time < end_time"
                )
            tracer_names = (
                [str(t) for t in ds["tracer_name"].values]
                if "tracer_name" in ds.coords
                else None
            )
            roles = [cdr_tracer_role(t) for t in tracer_names or ()]
            feeding: dict[str, list[tuple[int, str, str]]] = {}
            if any(roles):
                if "tracer_release" not in ds.coords:
                    raise ValueError(
                        "generated CDR tracers but no 'tracer_release' "
                        "coordinate -- the file predates roms-tools 5.1"
                    )
                for j, release in enumerate(ds["tracer_release"].values):
                    if (role := roles[j]) is not None:
                        feeding.setdefault(str(release), []).append((j, *role))
            marbl_columns = [
                (tn, j)
                for j, (tn, role) in enumerate(zip(tracer_names or (), roles))
                if role is None
            ]
            is_tracer_perturbation = "cdr_trcflx" in ds.data_vars
            is_volume = "cdr_volume" in ds.data_vars
            if not (is_tracer_perturbation or is_volume):
                raise ValueError(
                    "neither 'cdr_trcflx' nor 'cdr_volume' found -- "
                    "unrecognized CDR-forcing layout"
                )

            releases: list[Any] = []
            for i, name in enumerate(release_names):
                common = {
                    "name": name,
                    "lat": float(ds["cdr_lat"].isel(ncdr=i).item()),
                    "lon": float(ds["cdr_lon"].isel(ncdr=i).item()),
                    "depth": float(ds["cdr_dep"].isel(ncdr=i).item()),
                    "hsc": float(ds["cdr_hsc"].isel(ncdr=i).item()),
                    "vsc": float(ds["cdr_vsc"].isel(ncdr=i).item()),
                    "times": times,
                    "tracer_set": feeding[name][0][1] if name in feeding else "marbl",
                }
                if is_tracer_perturbation:
                    var = ds["cdr_trcflx"].isel(ncdr=i)
                    if name in feeding:
                        tracer_fluxes = {
                            key: var.isel(ntracers=j).values.tolist()
                            for j, _, key in feeding[name]
                        }
                    elif tracer_names is not None and "ALK" in tracer_names:
                        # ALK-only: this reconstruction only feeds the plot
                        # widget's "Tracer flux (ALK)" option.
                        idx = tracer_names.index("ALK")
                        tracer_fluxes = {"ALK": var.isel(ntracers=idx).values.tolist()}
                    elif tracer_names is not None:
                        tracer_fluxes = {
                            tn: var.isel(ntracers=j).values.tolist()
                            for tn, j in marbl_columns
                        }
                    elif "ntracers" in var.dims:
                        # No tracer_name coordinate at all -- can't identify the
                        # tracer(s); best-effort label the first as "ALK".
                        tracer_fluxes = {"ALK": var.isel(ntracers=0).values.tolist()}
                    else:
                        tracer_fluxes = {"ALK": var.values.tolist()}
                    releases.append(
                        rt.TracerPerturbation(
                            **common,
                            tracer_fluxes=tracer_fluxes,
                            release_type="tracer_perturbation",
                        )
                    )
                else:
                    volume_fluxes = ds["cdr_volume"].isel(ncdr=i).values.tolist()
                    tracer_var = ds.get("cdr_tracer")
                    if tracer_var is not None and tracer_names:
                        columns = (
                            [(key, j) for j, _, key in feeding[name]]
                            if name in feeding
                            else marbl_columns
                        )
                        tracer_concentrations = {
                            tn: tracer_var.isel(ncdr=i, ntracers=j).values.tolist()
                            for tn, j in columns
                        }
                    else:
                        tracer_concentrations = {}
                    releases.append(
                        rt.VolumeRelease(
                            **common,
                            volume_fluxes=volume_fluxes,
                            tracer_concentrations=tracer_concentrations,
                            release_type="volume",
                        )
                    )
        return rt.CDRForcing(
            grid=grid, start_time=times[0], end_time=times[-1], releases=releases
        )
    except Exception as exc:
        raise ValueError(f"could not reconstruct releases from {path}: {exc}") from exc


# Preselected in the Output dropdown when present in the catalog (falls back to
# the first catalog spec otherwise). 'daily-restarts' conforms to the
# ucla-roms >= 0.5.0 check_output_divides_rst precheck; 'standard' is kept
# unchanged for blueprints that reference it.
_DEFAULT_OUTPUT_SPEC = "daily-restarts"

# Preselected in the Forcing dropdown when present in the catalog (falls back to
# the first catalog spec otherwise). Named explicitly for the same reason as the
# output default above: forcing_names is sorted alphabetically, so relying on
# position would silently change every user's default whenever a newly bundled
# ForcingSpec happens to sort first.
_DEFAULT_FORCING_SPEC = "glorys-era5-unified"

# The same default for a run without MARBL (bgc_mode "none"/"cdr_lite"): the
# resolver rejects any BGC forcing there, which _DEFAULT_FORCING_SPEC carries.
_DEFAULT_PHYSICS_FORCING_SPEC = "glorys-era5-physics"

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

    A layered stack: the user's writable catalog layer (``~/cstar/catalog``
    by default, or ``CSTAR_CATALOG``) over the read-only bundled in-repo
    catalog. Reads resolve top-first; writes (``register_*``) land in the
    user layer.
    """
    from cstar.catalog.domain_catalog import default_catalog

    return default_catalog


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


# Nested-grid topography: the parent/child grid kwargs may carry their own
# ``topography_source``/``topography_path`` (Forge inputs, popped before rt.Grid --
# see ForgeExecutor._nested_topography_pair for the inheritance rule). The
# dropdown's first entry is the "inherit this grid's pair" sentinel.
_NESTED_TOPO_INHERIT = "(same as this grid)"
_TOPO_SOURCES = ["ETOPO5", "SRTM15", "EMOD"]


def _nested_topo_kwargs(source_value: str, path_value: str) -> dict[str, str]:
    """The ``topography_source``/``topography_path`` keys a nested grid's
    widgets contribute to its grid_kwargs dict (empty = inherit the domain's).
    """
    out: dict[str, str] = {}
    if source_value and source_value != _NESTED_TOPO_INHERIT:
        out["topography_source"] = source_value
    if path_value.strip():
        out["topography_path"] = path_value.strip()
    return out


# The grid_kwargs keys the main grid's widgets leave out, each with the value
# that leaves it out (``_base_grid_kwargs`` writes them only when they differ).
_GRID_KWARGS_LEFT_OUT: Final[dict[str, Any]] = {
    "hmin": 5.0,
    "close_narrow_channels": False,
    "mask_shapefile": None,
}
# The keys of a parent's or child's grid_kwargs that its topography widgets set
# (see _nested_topo_kwargs); the geometry keys come from its grid boxes.
_NESTED_TOPO_KEYS: Final = ("topography_source", "topography_path")


def _merge_loaded_kwargs(
    loaded: dict[str, Any],
    gathered: dict[str, Any],
    owned: Collection[str],
    left_out: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A grid_kwargs dict read from the widgets (``gathered``) laid over the one
    last loaded into them (``loaded``, from a blueprint or a DomainSpec).

    Widgets cannot hold every key a file may carry (a parent's ``hmin``, a
    ``verbose`` flag), so a key no widget sets (not in ``owned``) is kept as
    loaded. A key the widgets own but left out (``left_out`` maps it to the
    value that leaves it out) keeps its loaded spelling while the widget still
    shows that value -- a file's explicit ``hmin: 5.0`` -- and is dropped once
    the widget shows something else. Loaded keys keep their order.
    """
    left_out = left_out or {}
    out: dict[str, Any] = {}
    for k, v in loaded.items():
        if k in gathered:
            out[k] = gathered[k]
        elif k not in owned or (k in left_out and v == left_out[k]):
            out[k] = copy.deepcopy(v)
    for k, v in gathered.items():
        out.setdefault(k, v)
    return out


def _effective_nested_topo(
    source_value: str, path_value: str, domain_source: str, domain_path: str
) -> tuple[str, str]:
    """Mirror of ``ForgeExecutor._nested_topography_pair`` on widget values:
    ``(name, path)`` a nested grid resolves to, with ``""`` for no path.
    """
    path = path_value.strip()
    if source_value == _NESTED_TOPO_INHERIT:
        return domain_source, (path or domain_path.strip())
    return source_value, path


def _plot_topography_source(name: str, path: str) -> tuple[dict[str, str] | None, str]:
    """What a wizard-side grid build (plots / derive-from-grid) can use for
    ``rt.Grid(topography_source=...)`` on THIS machine, plus a status note.

    Returns ``(None, "")`` for default ETOPO5 (roms-tools fetches it itself) and
    ``({'name','path'}, "")`` when ``path`` exists locally. A non-default dataset
    whose file isn't available here (the common laptop case -- the path is an
    executor-side path) falls back to ETOPO5 with a visible note, rather than
    failing the plot: the executor is the hard gate, this is a preview.
    """
    path = (path or "").strip()
    if path:
        local = Path(path).expanduser()
        if local.exists():
            return {"name": name, "path": str(local)}, ""
    if name == "ETOPO5" and not path:
        return None, ""
    why = f"file not found here: {path}" if path else "no local file"
    return None, (
        " "
        + components.status_span(
            "warn", f"(plotted with default ETOPO5 topography; {name}: {why})"
        )
    )


_SAVE_FIRST = "Save the blueprint above first."
"""Card 8's message while the blueprint is not saved as it stands."""


class _SavedBlueprint(NamedTuple):
    """The last blueprint file the wizard wrote, and what it wrote."""

    requested: Path  #: the Save path the save was made with, as given
    path: Path  #: the file written, resolved
    text: str  #: ``to_yaml_str()`` of the config written


class WorkplanHooks(NamedTuple):
    """What the Blueprint page needs from a Workplan page, when it sits beside one."""

    open_steps: Callable[[Path, str], None]
    """Add the forge-then-run steps for (saved blueprint path, blueprint name) to the
    Workplan page and show that page. May raise ``ValueError``."""

    saved: Callable[[], None]
    """A workplan file was written into the catalog: refresh the Workplan page's
    Load list."""


class ForgeBlueprintWizard:
    """Build/curate a :class:`ForgeBlueprint` interactively. ``self.config`` holds the
    latest successfully-resolved config (``None`` while inputs are invalid).
    """

    def __init__(self, catalog: Any = None):
        import ipywidgets as W  # imported here so the package doesn't require ipywidgets

        self.W = W
        self.catalog = catalog or _get_catalog()
        self.config: ForgeBlueprint | None = None
        # The provenance of the last blueprint loaded or saved, handed to every
        # re-resolve (see `_gather`) so its stamp survives until the content changes
        # (see `ForgeBlueprint.stamp_provenance`).
        self._carried_provenance = ForgeProvenance()
        # Cache for the `widget` property -- built once, see `widget`.
        self._root: Any | None = None

        domains = list(self.catalog.domain_names)

        # --- load / import an existing forge_blueprint.yaml ---
        # Pick a forge blueprint straight from the catalog (sits above the
        # free-text path box below); reuses the same load path once selected.
        _fb_names = self.catalog.forge_blueprint_names
        self.load_catalog_dd = W.Dropdown(
            options=_dd_options(self.catalog, _fb_names, "forge_blueprint"),
            description="From catalog:",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
        )
        self.load_catalog_btn = W.Button(
            description="Load selected",
            icon="upload",
            disabled=not _fb_names,
        )
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
        # True only while load_status holds a load *failure* (an exception from
        # _on_load_path/_load_bytes) -- distinguishes that from a load *success*
        # message (_set_load_status, which may itself carry an amber "N invalid
        # settings value(s)" warning that must NOT be swept away by _rebuild).
        self._load_status_is_error = False

        # --- the Model card (ModelSpec picker, BGC mode, code refs, carbonate
        # sensitivities): see cstar.wizard.model_card ---
        self.model_card = ModelCard(
            W,
            self.catalog,
            rebuild=self._rebuild,
            suspend=self._suspend,
            is_suspended=lambda: getattr(self, "_suspended", False),
            use_pio=lambda: self.use_pio_chk.value,
        )
        self.domain_dd = W.Dropdown(
            options=_dd_options(self.catalog, domains, "domain", prefix=["<custom>"]),
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
            description=label_for(
                "scoord_chk", "specify s-coord (theta_s/theta_b/hc)"
            ).label,
            indent=False,
            tooltip="When checked, theta_s, theta_b, and hc are passed to roms-tools "
            "to set the vertical stretching. Required for ROMS simulations.",
        )

        # --- boundaries / partitioning ---
        self.bnd = {
            d: W.Checkbox(
                value=(d in ("east", "north")),
                description=d.capitalize(),
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
            description=label_for("buttons.derive", "Derive from grid").label,
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
            "locks it. Experimental.",
        )
        self.n_cores = W.IntText(
            value=1,
            description="n_cores:",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px", display="none"),
            tooltip="Total MPI ranks; the tiling itself is chosen at runtime "
            "from the land mask when auto tiling is on.",
        )
        # --- nesting (optional child grid) ---
        self.nest_enable = W.Checkbox(
            value=False,
            description="This grid is a parent (enter child grid info below).",
            indent=False,
            tooltip=_tip("nesting", "nest_enable"),
            # sized to its label: the default 300px cut the sentence off
            layout=W.Layout(width="auto"),
        )
        self.nest_help = W.HTML(
            "<div class='forge-aside'>"
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
        self.child_topo_source = W.Dropdown(
            options=[_NESTED_TOPO_INHERIT, *_TOPO_SOURCES],
            value=_NESTED_TOPO_INHERIT,
            description="child topo:",
            style={"description_width": "90px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "child_topo_source"),
        )
        self.child_topo_path = W.Text(
            value="",
            description="child topo path:",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            placeholder=_NESTED_TOPO_PATH_PLACEHOLDER,
            tooltip=_tip("nesting", "child_topo_path"),
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
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the parent and child grids from current settings and "
            "render both via plot_nesting (parent+child boundary overlay).",
        )
        self.nest_plot_status = W.HTML("Click Refresh preview to draw the grids.")
        self.nest_plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px", display="none"),
        )

        # --- parent (optional: this grid is a child nested inside a parent) ---
        self.parent_enable = W.Checkbox(
            value=False,
            description="This is a child grid (enter parent grid info below).",
            indent=False,
            tooltip=_tip("nesting", "parent_enable"),
            # sized to its label: the default 300px cut the sentence off
            layout=W.Layout(width="auto"),
        )
        self.parent_help = W.HTML(
            "<div class='forge-aside'>"
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
        self.parent_topo_source = W.Dropdown(
            options=[_NESTED_TOPO_INHERIT, *_TOPO_SOURCES],
            value=_NESTED_TOPO_INHERIT,
            description="parent topo:",
            style={"description_width": "90px"},
            layout=W.Layout(width="260px"),
            tooltip=_tip("nesting", "parent_topo_source"),
        )
        self.parent_topo_path = W.Text(
            value="",
            description="parent topo path:",
            style={"description_width": "110px"},
            layout=W.Layout(width="360px"),
            placeholder=_NESTED_TOPO_PATH_PLACEHOLDER,
            tooltip=_tip("nesting", "parent_topo_path"),
        )
        # --- parent plot (this grid's boundary within its parent) ---
        self.parent_plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the parent grid and this grid from current settings and "
            "render both via plot_nesting (parent+this-grid boundary overlay).",
        )
        self.parent_plot_status = W.HTML("Click Refresh preview to draw the grids.")
        self.parent_plot_img = W.Image(
            format="png",
            layout=W.Layout(min_width="400px", max_width="600px", display="none"),
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
        # --- grid extended options ---
        self.hmin = W.FloatText(
            value=_GRID_KWARGS_LEFT_OUT["hmin"],
            description="hmin (m):",
            style={"description_width": "90px"},
            layout=W.Layout(width="200px"),
            tooltip=_tip("grid", "hmin"),
        )
        self.close_narrow_chk = W.Checkbox(
            value=_GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
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
            placeholder=_PATH_PLACEHOLDER,
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
            # Fire `value` on Enter / focus-out only (not per keystroke) so
            # _on_grid_file_path_submit can auto-attach a finished path.
            continuous_update=False,
        )
        self.grid_file_attach_btn = W.Button(
            description=label_for("buttons.attach", "Attach").label, icon="link"
        )
        self.grid_file_detach_btn = W.Button(
            description=label_for("buttons.detach", "Detach").label, icon="unlink"
        )
        self.grid_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.grid_file_status = W.HTML("")

        # --- timestep ---
        self.dt = W.FloatText(
            value=0.0,  # the derived dt replaces it on the first _rebuild
            description="dt (s):",
            style={"description_width": "90px"},
            layout=W.Layout(width="220px"),
            tooltip=_tip("run", "dt"),
        )
        self.dt_btn = W.Button(
            description=label_for("buttons.compute_dt", "Use CFL value").label,
            icon="calculator",
            tooltip=_tip("timestep", "dt_btn"),
        )
        self.dt_status = W.HTML("")

        # --- grid plot ---
        self.plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="refresh",
            tooltip="Build the grid from current settings and render it. Updates "
            "automatically when a domain is selected from the catalog.",
        )
        self.plot_status = W.HTML("Click Refresh preview to draw the grid.")
        # width 100% of its column (400-600px): a fixed-size image would hold its
        # column at 600px and squeeze the fields beside it
        self.plot_img = W.Image(
            format="png",
            layout=W.Layout(
                width="100%", min_width="400px", max_width="600px", display="none"
            ),
        )

        # --- output / preview ---
        # --- forcing spec (ForcingSpec selection + add/remove/edit editor) ---
        # A ForcingSpec must always be explicitly selected -- ModelSpec no longer
        # embeds a default forcing.
        self.forcing_dd = W.Dropdown(
            options=_dd_options(
                self.catalog, list(self.catalog.forcing_names), "forcing"
            ),
            value=self._default_forcing_spec(self.model_card.default_bgc_mode()),
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

        # The Forcing card's "Atmospheric CO2" Static value: the settings editor's
        # forcing_box, mounted the same way, inside every forcing editor built.
        self.co2_static_box = W.VBox([])

        # --- CDR (Carbon Dioxide Removal) forcing -----------------------------
        # A top-level, independently composable spec (CdrSpec/Composition.cdr) --
        # NOT part of Forcing. ``cdr_dd`` picks a named CdrSpec from the catalog
        # (mirrors forcing_dd/output_dd/domain_dd); ``cdr_mode_dd`` picks which of
        # the five CdrSpec.mode variants is active and shows/hides the matching
        # sub-panel below (see ``_apply_cdr_mode``). Picking a catalog CdrSpec
        # loads its mode + fields; hand-editing anything afterward is reported via
        # composition.cdr.modified (a snapshot diff against ``_cdr_seed``, mirroring
        # domain/forcing -- the dropdown itself never auto-reverts to "<custom>").
        _cdr_names = list(self.catalog.cdr_names)
        self.cdr_dd = W.Dropdown(
            options=_dd_options(self.catalog, _cdr_names, "cdr", prefix=["<custom>"]),
            value="<custom>",
            description="CDR spec:",
            style={"description_width": "110px"},
            tooltip="Select a named CdrSpec from the catalog to seed the CDR mode "
            "and fields below, or leave as <custom> to author one by hand.",
        )
        # snapshot of _cdr_snapshot() at the last catalog pick; None means no
        # catalog CdrSpec is currently selected (mirrors _domain_seed/_forcing_seed).
        self._cdr_seed: dict[str, Any] | None = None

        self.cdr_mode_dd = W.Dropdown(
            options=[
                ("None", "none"),
                ("Simple perturbation", "simple"),
                ("Import ROMS-tools YAML", "yaml"),
                ("Import netCDF", "netcdf"),
                ("Upscaled CDR forcing", "upscaled"),
            ],
            value="none",
            description="Mode:",
            style={"description_width": "110px"},
            tooltip=_tip("cdr", "mode"),
        )
        # Shown only while bgc_dd is "cdr_lite" (_sync_cdr_lite_hint):
        # in that mode a CDR forcing is required and its releases must target the
        # CDR-lite tracers.
        self.cdr_lite_hint = W.HTML(
            components.status_span(
                "note",
                "BGC mode <code>cdr_lite</code>: releases "
                "drive the CDR-lite tracers, and a CDR forcing is required. Simple "
                "mode builds one ALK (OAE) release; a YAML or netCDF forcing must use "
                "<code>tracer_set: cdr_lite</code>.",
            )
        )
        # The mode _apply_cdr_mode() last actually applied -- lets it detect a
        # real transition (for leaving-mode cleanup) whether invoked via the
        # dropdown's own .observe or a direct call (e.g. from _populate_from,
        # which must call it explicitly since suspended observers are no-ops).
        self._cdr_mode_active: str | None = None

        # --- CDR mode "simple": a single hand-authored tracer-perturbation
        # release, compiled into a roms-tools CDRForcing kwargs dict at gather
        # time (see _simple_cdr_forcing_dict). Laid out as field rows (labels and
        # units from the glossary) in cdr_simple_box below. ---
        self.cdr_simple_name = W.Text(value="my_cdr", layout=W.Layout(width="220px"))
        self.cdr_simple_lat = W.FloatText(value=0.0)
        self.cdr_simple_lon = W.FloatText(value=0.0)
        self.cdr_simple_depth = W.FloatText(value=1.0)
        self.cdr_simple_hsc = W.FloatText(value=10.0)
        self.cdr_simple_vsc = W.FloatText(value=10.0)
        self.cdr_simple_flux = W.FloatText(value=2 * 10**6)
        self.cdr_simple_start = W.DatePicker()
        self.cdr_simple_end = W.DatePicker()
        self._cdr_simple_fields = (
            self.cdr_simple_name,
            self.cdr_simple_lat,
            self.cdr_simple_lon,
            self.cdr_simple_depth,
            self.cdr_simple_hsc,
            self.cdr_simple_vsc,
            self.cdr_simple_flux,
            self.cdr_simple_start,
            self.cdr_simple_end,
        )
        # A loaded "simple" CDR forcing these fields cannot show (several
        # releases, a flux ramp, another tracer, a time of day): written back
        # unchanged, with the fields locked, until the user replaces it (see
        # _populate_cdr_simple).
        self._cdr_simple_loaded: dict[str, Any] | None = None
        # The values entering "simple" mode seeded into the fields (see
        # _apply_cdr_mode); a field still showing its seed follows the grid
        # center and the run window (see _reseed_cdr_simple).
        self._cdr_simple_seeded: dict[str, Any] = {}
        self.cdr_simple_note = W.HTML("")
        self.cdr_simple_replace_btn = W.Button(
            description="Edit as one release",
            icon="pencil",
            layout=W.Layout(width="auto", display="none"),
            tooltip=(
                "Replace the loaded CDR forcing with one release at a constant "
                "ALK flux, taken from its first release, and unlock the fields."
            ),
        )
        self.cdr_simple_box = W.VBox(
            [
                self.cdr_simple_note,
                self.cdr_simple_replace_btn,
                components.field_row(W, "cdr_simple_name", self.cdr_simple_name),
                *(
                    components.field_row(W, key, widget, width="140px")
                    for key, widget in (
                        ("cdr_simple_lat", self.cdr_simple_lat),
                        ("cdr_simple_lon", self.cdr_simple_lon),
                        ("cdr_simple_depth", self.cdr_simple_depth),
                        ("cdr_simple_hsc", self.cdr_simple_hsc),
                        ("cdr_simple_vsc", self.cdr_simple_vsc),
                        ("cdr_simple_flux", self.cdr_simple_flux),
                    )
                ),
                components.field_row(W, "cdr_simple_start", self.cdr_simple_start),
                components.field_row(W, "cdr_simple_end", self.cdr_simple_end),
            ]
        )

        # --- CDR mode "yaml": an uploaded roms-tools CDRForcing.to_yaml() dump.
        # Parsed dict lives on the instance (not a widget) since FileUpload can't
        # be repopulated with the original file on load; _gather()/_populate_from
        # read and write this directly. ---
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
        _cdr_yaml_help = W.HTML(
            components.status_span(
                "note",
                "Follow the <a href="
                "'https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html' "
                "target='_blank'>roms-tools CDR forcing documentation</a> to build a "
                "<code>CDRForcing</code> and export it with <code>to_yaml()</code>, "
                "then upload that file here.",
            )
        )
        self.cdr_yaml_box = W.VBox(
            [
                _cdr_yaml_help,
                W.HBox([self.cdr_upload, self.cdr_clear_btn]),
                self.cdr_status,
            ]
        )

        # --- CDR mode "netcdf": a user-supplied pre-made CDR-forcing netCDF. ---
        self._cdr_forcing_file: dict[str, Any] | None = None
        self.cdr_file_path = W.Text(
            value="",
            description="CDR file:",
            placeholder="path to a pre-made CDR-forcing netCDF",
            style={"description_width": "110px"},
            layout=W.Layout(width="420px"),
            tooltip=_tip("cdr", "cdr_file"),
            continuous_update=False,  # see grid_file_path
        )
        self.cdr_file_attach_btn = W.Button(
            description=label_for("buttons.attach", "Attach").label, icon="link"
        )
        # "Clear" removes the attached file exactly like "Detach" elsewhere (grid,
        # river custom-file) -- same verb pair everywhere, not a distinct action.
        self.cdr_file_clear_btn = W.Button(
            description=label_for("buttons.detach", "Detach").label, icon="times"
        )
        self.cdr_file_upload = W.FileUpload(
            accept=".nc", multiple=False, description="…or upload"
        )
        self.cdr_file_status = W.HTML("")
        _cdr_netcdf_help = W.HTML(
            components.status_span(
                "note",
                "Follow the <a href="
                "'https://roms-tools.readthedocs.io/en/latest/cdr_forcing.html' "
                "target='_blank'>roms-tools CDR forcing documentation</a> to build and "
                "<code>save()</code> the forcing as netCDF (or otherwise produce a "
                "CDR-forcing netCDF), then attach or upload it here.",
            )
        )
        self.cdr_netcdf_box = W.VBox(
            [
                _cdr_netcdf_help,
                W.HBox(
                    [
                        self.cdr_file_path,
                        self.cdr_file_attach_btn,
                        self.cdr_file_clear_btn,
                    ]
                ),
                self.cdr_file_upload,
                self.cdr_file_status,
            ]
        )

        # --- CDR mode "upscaled": nothing to configure -- ROMS reads CDR forcing
        # supplied at runtime from a child domain's upscaled signal; the blueprint
        # carries a placeholder path C-Star's orchestrator replaces. No dataset
        # exists at authoring time, so no plot either. ---
        self.cdr_upscaled_box = W.VBox(
            [
                W.HTML(
                    components.status_span(
                        "note",
                        "ROMS will read CDR forcing supplied "
                        "at runtime from a child domain's upscaled signal. The "
                        "blueprint carries a placeholder path that C-Star's "
                        "orchestrator replaces at run time -- nothing to configure "
                        "here.",
                    )
                )
            ]
        )

        # --- CDR plotting (WP6): visible for simple/yaml/netcdf only. The built
        # roms-tools CDRForcing (plus the grid it was built against) and rendered
        # PNGs are cached on the instance -- see _rebuild()'s cache-invalidation
        # comment -- so switching plot type/release re-renders without rebuilding.
        self._cdr_plot_object: Any = None  # last-built rt.CDRForcing, or None
        self._cdr_plot_grid: Any = None  # the rt.Grid it was built against
        self._cdr_plot_cache: dict[tuple[str, str | None], bytes] = {}
        # snapshot of _cdr_plot_inputs_fingerprint() at the last cache clear --
        # lets _invalidate_cdr_plot_cache skip clearing on rebuilds triggered by
        # plot-unrelated edits (e.g. the description field).
        self._cdr_plot_fingerprint: tuple | None = None
        self.cdr_plot_btn = W.Button(
            description=label_for("buttons.refresh_plot", "Refresh plot").label,
            icon="area-chart",
        )
        self.cdr_plot_status = W.HTML("")
        self.cdr_plot_type_dd = W.Dropdown(
            options=[
                ("Release locations", "locations"),
                ("Distribution", "distribution"),
                ("Tracer flux (ALK)", "tracer_flux"),
            ],
            value="locations",
            description="Plot:",
            style={"description_width": "110px"},
        )
        self.cdr_plot_release_dd = W.Dropdown(
            options=[], description="Release:", style={"description_width": "110px"}
        )
        self.cdr_plot_release_dd.layout.display = "none"  # shown only if >1 release
        # sized to its column like the grid preview (self.plot_img)
        self.cdr_plot_img = W.Image(
            format="png",
            layout=W.Layout(
                width="100%", min_width="400px", max_width="600px", display="none"
            ),
        )
        self.cdr_plot_box = W.VBox(
            [
                W.HTML("<b>Generate plots</b>"),
                W.HBox([self.cdr_plot_btn, self.cdr_plot_status]),
                W.HBox([self.cdr_plot_type_dd, self.cdr_plot_release_dd]),
                self.cdr_plot_img,
            ],
            # Sits to the RIGHT of the per-mode panels (same side-by-side HBox
            # arrangement as the grid plot) -- the padding separates the columns.
            layout=W.Layout(padding="0 0 0 20px"),
        )
        self.cdr_plot_box.layout.display = "none"  # only for simple/yaml/netcdf

        # --- output settings spec (OutputSpec selection) ---
        # The output streams themselves are edited in the Output card below this
        # dropdown; it selects a named OutputSpec that seeds those streams. An
        # OutputSpec must always be explicitly selected -- ModelSpec no longer embeds
        # default output settings.
        _output_names = list(self.catalog.output_names)
        self.output_dd = W.Dropdown(
            options=_dd_options(self.catalog, _output_names, "output"),
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
        # What the editor was built for (None while there is none): rebuilt not only
        # on a model switch but also when the effective ucla-roms ref crosses a schema
        # boundary (e.g. editing the roms_ref override across the 0.5.0 line while
        # keeping the same ModelSpec selected), or the BGC mode or nesting role change.
        self._editor_key: _EditorKey | None = None
        self.editor_box = W.VBox([])  # placeholder; filled with the editor's accordion
        # ... and the Output card's body, filled with the editor's output_box.
        self.output_editor_box = W.VBox([])
        # sparse manual overrides layer: (section, field|None) -> value
        self._overrides: dict[Any, Any] = {}
        # The _WIZARD_SETTING_DEFAULTS keys the wizard still owns: neither edited by
        # the user nor set by a loaded blueprint (see _seed_wizard_defaults).
        self._wizard_default_keys = set(_WIZARD_SETTING_DEFAULTS)
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
        # The full grid-derived v_sponge the widget shows rounded (see
        # _show_derived_v_sponge), so a snapshot keeps its precision.
        self._v_sponge_derived: float | None = None
        self._boundaries_touched = False
        self._boundaries_derived = False
        # What the widgets cannot hold of the last loaded blueprint or DomainSpec,
        # written back on save: the grid_kwargs of "grid", "parent" and "child"
        # (see _merge_loaded_kwargs) and the run datetimes, whose time of day the
        # date pickers drop (see _run_datetime).
        self._loaded_grid_kwargs: dict[str, dict[str, Any]] = {}
        self._loaded_run_times: dict[str, datetime] = {}
        # The composition names of the last loaded blueprint that this catalog
        # lacks, for the load status (see _set_load_status).
        self._load_unknown_specs: list[str] = []
        # dt follows v_sponge: derived from the grid (CFL) and mirrored into the
        # widget until a typed value, a loaded blueprint or a DomainSpec carrying
        # dt sets it; "Use CFL value" clears it. See _sync_derived_dt.
        self._dt_touched = False
        # The resolver-composed model_settings behind the editor (see _show_editor);
        # _on_editor_edit drops an edit that only restores one of its values.
        self._composed_settings: dict[str, Any] | None = None

        self.derived = W.HTML("")
        self.validation = W.HTML("")
        self.preview = W.Output(
            layout=W.Layout(
                padding="6px",
                max_height="380px",
                overflow="auto",
            )
        )
        self.preview.add_class("forge-log")
        # Browser download (works in Voilà / JupyterLab without server file access)
        self.download_link = W.HTML("")
        # Save to the server/working-dir filesystem (handy for local or HPC use)
        self.save_path = W.Text(
            value="forge_blueprint.yaml",
            description="Save to:",
            style={"description_width": "110px"},
            layout=W.Layout(width="520px"),
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
            layout=W.Layout(width="320px"),
            tooltip=_tip("export", "name"),
        )
        self.name.observe(self._on_name_change, names="value")
        # The blueprint description follows the resolver's derived one-line summary
        # the same way, until the user types in it (_description_touched).
        self._description_touched = False
        self.description = W.Text(
            value="",
            description="Description:",
            placeholder="(derived from the grid, run window, model and forcing)",
            style={"description_width": "110px"},
            layout=W.Layout(width="100%", max_width="760px"),
            tooltip=_tip("export", "description"),
        )
        self.description.observe(self._on_description_change, names="value")
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
        self.save_cdr_name, self.save_cdr_btn, self.save_cdr_status = _spec_save_row(
            "(new CdrSpec name)"
        )

        # --- run (invokes the C-Star CLI on the just-saved blueprint) ---
        from cstar.system.manager import system_label

        self.run_warning = W.HTML(
            f"<b class='{components.status_class('warn')}'>⚠ Processing a blueprint can use substantial "
            "memory and CPU depending on grid size.</b> Run this from a compute node "
            "(or another host) with resources appropriate for your domain — this is "
            f"not checked automatically. System: <code>{html.escape(system_label())}</code>."
        )
        self.run_later_note = W.HTML(
            components.status_span(
                "note",
                "ℹ To run this later, or on a different "
                "machine, save the blueprint above and then (from the "
                "<code>cstar-env</code> environment) call: "
                "<code>cstar blueprint run &lt;path/to/forge_blueprint.yaml&gt;</code>. ",
            )
        )
        self.run_btn = W.Button(description="Run", icon="play")
        self.run_status = W.HTML("")

        # --- next steps: what to do with the saved blueprint ---
        self.next_status = W.HTML("")
        self.save_workplan_btn = W.Button(
            description="Save workplan", icon="save", button_style="primary"
        )
        self.open_workplan_btn = W.Button(
            description="Open in the Workplan builder",
            icon="arrow-right",
            layout=W.Layout(display="none"),
        )
        self.workplan_status = W.HTML("")
        self.workplan_download = W.HTML("")
        self._run_in_progress = False
        self._workplan_hooks: WorkplanHooks | None = None
        self._workplan_confirm: Path | None = None
        self._workplan_confirm_timer: Any = None
        self._saved_blueprint: _SavedBlueprint | None = None

        # Keep the run log pinned to the LATEST lines instead of snapping to the
        # top on every append. Two cooperating pieces (verified live in voila):
        # 1. display=flex + column-reverse on this node anchors its scroll
        #    position at the flex start, which column-reverse puts at the bottom.
        # 2. The stylesheet below stops the inner .jp-OutputArea from being its
        #    own scroll container: JupyterLab CSS bounds its height and gives it
        #    overflow auto, so IT would do the scrolling (resetting to the top on
        #    each append) while this node never overflows. flex: 0 0 auto lets it
        #    grow to its content so the scrolling happens on this node instead.
        #    (Layout can only style this node itself, hence a real stylesheet.)
        # Safe because _run_async only ever calls append_stdout, and the frontend
        # merges consecutive same-stream outputs into a single block -- a second
        # output type (stderr, display_data) would render above it, reversed.
        self.run_output = W.Output(
            layout=W.Layout(
                padding="6px",
                max_height="380px",
                overflow="auto",
                display="flex",
                flex_flow="column-reverse",
            )
        )
        self.run_output.add_class("forge-run-log")
        self.run_output.add_class("forge-log")
        self._run_log_style = W.HTML(
            "<style>.forge-run-log > .jp-OutputArea "
            "{ flex: 0 0 auto; height: auto; max-height: none; }</style>"
        )

        # --- redesigned-UI status widgets (sticky bar, per-card chips, banners) ---
        # Built eagerly, before anything below can trigger a `_rebuild` (via
        # `_apply_cdr_mode`/`_build_forcing_editor`) -- `_rebuild` ->
        # `_update_status` touches them on every edit, including from tests
        # that never access `.widget` at all.
        self.sticky_bar = W.HTML("")
        self.sticky_bar.add_class("forge-sticky-html")
        self.card_chips: dict[str, Any] = {
            key: W.HTML("")
            for key in (
                "model",
                "grid",
                "forcing",
                "run",
                "output",
                "advanced",
                "review",
            )
        }
        # What stops the blueprint resolving, on the card that owns it (see
        # `_update_status`); Review's one-line summary is `derived`.
        self.card_errors: dict[str, Any] = {}
        for key in ("model", "grid", "forcing", "run", "output", "advanced"):
            self.card_errors[key] = components.banner_widget(W, "err", "")
            self.card_errors[key].layout.display = "none"
        self.grid_lock_banner = components.banner_widget(
            W,
            "warn",
            "<b>Grid fields are locked</b> because a pre-made grid file is "
            "attached. Detach it to edit the grid here.",
        )
        self.grid_lock_banner.layout.display = "none"
        self.forcing_banner = components.banner_widget(W, "info", "")
        # A second, plain HTML mirror of `download_link` for the Review card --
        # `download_link` itself lives once, in the sticky bar (see `widget`).
        self.download_link_review = W.HTML("")

        self.use_pio_chk.value = self.model_card.default_use_pio()
        self._sync_cdr_lite_hint()
        self._sync_auto_tiling()
        self._build_forcing_editor(self.catalog.forcing_data(self.forcing_dd.value))
        if self._forcing_editor is None:
            raise RuntimeError("_build_forcing_editor did not set _forcing_editor")
        self._forcing_seed = self._forcing_editor.gather()
        self._apply_cdr_mode()
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
        self.save_cdr_btn.on_click(self._on_save_cdr)
        self.run_btn.on_click(self._on_run)
        self.save_workplan_btn.on_click(self._on_save_workplan)
        self.open_workplan_btn.on_click(self._on_open_workplan)
        self.load_btn.on_click(self._on_load_path)
        self.load_catalog_btn.on_click(self._on_load_from_catalog)
        self.upload.observe(self._on_upload, names="value")
        self.cdr_dd.observe(self._on_cdr_spec, names="value")
        self.cdr_mode_dd.observe(self._on_cdr_mode_change, names="value")
        self.cdr_upload.observe(self._on_cdr_upload, names="value")
        self.cdr_clear_btn.on_click(self._on_cdr_clear)
        self.grid_file_attach_btn.on_click(self._on_grid_file_attach)
        self.grid_file_detach_btn.on_click(self._on_grid_file_detach)
        self.grid_file_path.observe(self._on_grid_file_path_submit, names="value")
        self.grid_file_upload.observe(self._on_grid_file_upload, names="value")
        self.cdr_file_attach_btn.on_click(self._on_cdr_file_attach)
        self.cdr_file_clear_btn.on_click(self._on_cdr_file_clear)
        self.cdr_file_path.observe(self._on_cdr_file_path_submit, names="value")
        self.cdr_file_upload.observe(self._on_cdr_file_upload, names="value")
        self.cdr_plot_btn.on_click(self._on_cdr_plot_generate)
        self.cdr_plot_type_dd.observe(self._on_cdr_plot_option_change, names="value")
        self.cdr_plot_release_dd.observe(self._on_cdr_plot_option_change, names="value")
        for w in self._cdr_simple_fields:
            w.observe(self._rebuild, names="value")
        self.cdr_simple_replace_btn.on_click(self._on_cdr_simple_replace)
        self.model_card.model_dd.observe(self._on_model_change, names="value")
        # The card's own observers first: on bgc_dd, its visibility syncs run before
        # the reactions of the other cards below.
        self.model_card.wire()
        self.model_card.bgc_dd.observe(self._sync_cdr_lite_hint, names="value")
        self.model_card.bgc_dd.observe(
            self._sync_marbl_forcing_visibility, names="value"
        )
        self.model_card.bgc_dd.observe(self._on_bgc_mode, names="value")
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
            self.model_card.cs_source_dd,
            self.model_card.roms_ref,
            self.model_card.marbl_ref,
            self.start,
            self.end,
            self.model_ref_date,
            self.topo_source,
            self.topo_path,
            self.nest_enable,
            self.nest_period,
            self.nest_pressure_fluxes,
            self.parent_enable,
            *self.child_w.values(),
            *self.parent_w.values(),
            self.child_topo_source,
            self.child_topo_path,
            self.parent_topo_source,
            self.parent_topo_path,
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
        self.dt.observe(self._on_dt_edit, names="value")

    def _on_name_change(self, _change):
        # A programmatic backfill (see _rebuild) happens under _suspend() -- only a
        # real user edit should "lock in" the field against further auto-updates.
        if getattr(self, "_suspended", False):
            return
        self._name_touched = True
        self._rebuild()

    def _on_description_change(self, _change):
        # Mirrors _on_name_change: the backfill in _rebuild runs under _suspend().
        # Not in _rebuild's watched list, so the flag is set before the rebuild
        # that would otherwise backfill over the user's first keystroke.
        if getattr(self, "_suspended", False):
            return
        self._description_touched = True
        self._rebuild()

    def _on_save_path_change(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._save_path_touched = True
        self._sync_next_steps()

    def _default_blueprint_path(self, name: str) -> str:
        """Default "Save to:" path for a blueprint named *name*.

        Prefers the active catalog's ``blueprints/forge/`` directory so a save lands
        where the wizard's other catalog-aware specs look; falls back to a
        bare filename (CWD-relative) when the catalog isn't a local filesystem
        (e.g. loaded from a GitHub/http URL) and so isn't writable.
        """
        fname = f"{name}.yaml"
        cat = getattr(self, "catalog", None)
        try:
            # A read-only catalog (e.g. the bundled one loaded as a single
            # store) must not be offered as a save destination.
            if (
                cat is not None
                and getattr(cat, "_is_local", False)
                and not getattr(cat, "read_only", False)
            ):
                return str(cat.blueprint_dir(DEFAULT_APPLICATION) / fname)
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
        self.grid_lock_banner.layout.display = "" if locked else "none"

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
            self.grid_file_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        self._finish_grid_file_attach(
            {"location": str(path), "content_hash": content_hash}, grid
        )
        self._rebuild()

    def _on_grid_file_attach(self, _btn):
        path_str = self.grid_file_path.value.strip()
        if not path_str:
            self.grid_file_status.value = components.status_span(
                "err", "Enter a path first."
            )
            return
        # Explicit click always re-loads/re-hashes (the file may have changed on
        # disk since the last attach) -- no dedupe here.
        self._attach_grid_file_from_path(path_str)

    def _maybe_attach_grid_file(self) -> None:
        """Attach the path in the grid-file box unless it's blank or already the
        attached file. Fired on Enter/focus-out of the Text (and after an
        upload lands), so a user who types/pastes a path and moves on without
        clicking Attach doesn't silently keep building from the grid_kwargs
        widgets instead of the file they meant to use.
        """
        path_str = self.grid_file_path.value.strip()
        if not path_str or _same_attached_file(self._grid_file, path_str):
            return
        self._attach_grid_file_from_path(path_str)

    def _on_grid_file_path_submit(self, _change) -> None:
        if getattr(self, "_suspended", False):
            return
        self._maybe_attach_grid_file()

    def _on_grid_file_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        # Setting the Text fires _on_grid_file_path_submit (attach); the explicit
        # call covers a same-name re-upload where the value doesn't change.
        self.grid_file_path.value = str(dest)
        self._maybe_attach_grid_file()

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
            self.grid_file_status.value = components.status_span(
                "err", f"⚠ could not re-attach grid_file: {type(exc).__name__}: {exc}"
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

    def _show_derived_v_sponge(self, value: float) -> None:
        """Show the grid-derived sponge viscosity ``value`` rounded for display
        (8333.333333333332 reads 8333.3), keeping the full value for
        `_v_sponge_value`. The blueprint never takes it from the widget while it
        is derived: the resolver derives it again from ``v_sponge=None``.
        """
        self._v_sponge_derived = value
        self.v_sponge.value = _display_round(value)

    def _v_sponge_value(self) -> float:
        """The sponge viscosity the widget stands for: the full derived value while
        it is untouched and shows that value rounded, else what it holds.
        """
        derived = self._v_sponge_derived
        if (
            not self._v_sponge_touched
            and derived is not None
            and self.v_sponge.value == _display_round(derived)
        ):
            return derived
        return float(self.v_sponge.value)

    def _on_v_sponge_edit(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._v_sponge_touched = True
        self._rebuild()

    def _on_dt_edit(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._dt_touched = True
        self._rebuild()

    def _sync_derived_dt(self) -> None:
        """While dt is untouched, show the CFL dt for the current grid, which is
        what the resolver derives from ``dt=None``. Pure arithmetic, so it works
        while the rest of the blueprint does not resolve.
        """
        if self._dt_touched:
            self.dt_status.value = ""
            return
        try:
            derived = float(cfl_dt(self._base_grid_kwargs(), self._attached_grid()))
        except Exception as exc:
            self.dt_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc} Type a time step."
            )
            return
        with self._suspend():
            self.dt.value = derived
        self.dt_status.value = components.status_span(
            "ok", "derived from the grid (CFL)"
        )

    def _sync_cdr_lite_hint(self, _change=None):
        # The CDR panel's hint only matters in BGC mode "cdr_lite", like the Model
        # card's carbonate sensitivity group.
        cdr_lite = self.model_card.bgc_mode == "cdr_lite"
        self.cdr_lite_hint.layout.display = "" if cdr_lite else "none"

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
        # OutputSpec doesn't reference a model), so switching models never re-picks them
        # -- except that an untouched default ForcingSpec follows the new model's
        # declared bgc_mode, via the bgc_dd change apply_spec_defaults makes (see
        # _on_bgc_mode).
        if getattr(self, "_suspended", False):
            return
        self._overrides = {}
        self._wizard_default_keys = set(_WIZARD_SETTING_DEFAULTS)
        self.model_card.apply_spec_defaults()
        self.use_pio_chk.value = self.model_card.default_use_pio()
        self._rebuild()

    def _on_editor_edit(self, section, field):
        # a user edit in the advanced editor -> record as an override (vs composed)
        if self._syncing or getattr(self, "_suspended", False):
            return
        value = self.editor.read(section, field)
        composed = self._composed_settings
        if (
            composed is not None
            and section in composed
            and (
                composed[section] == value
                if field is None
                else isinstance(composed[section], dict)
                and field in composed[section]
                and composed[section][field] == value
            )
        ):
            # Back to what the specs and the resolver give: not an override, so
            # it cannot pin a value the resolver would later change (e.g.
            # bry_tides once the grid gets a parent).
            self._overrides.pop((section, field), None)
        else:
            self._overrides[(section, field)] = value
        self._wizard_default_keys.discard((section, field))
        self._rebuild()

    def _seed_wizard_defaults(self) -> None:
        """Put each wizard-owned :data:`_WIZARD_SETTING_DEFAULTS` value in the
        overrides layer while the editor shows its widget, and take it out while it
        does not: the editor builds a widget only where the ucla-roms release has the
        field, the ModelSpec declares it and the BGC mode reads it, so a default never
        reaches a blueprint that would refuse it.
        """
        for key in self._wizard_default_keys:
            if self.editor is not None and key in self.editor._widgets:
                self._overrides[key] = _WIZARD_SETTING_DEFAULTS[key]
            else:
                self._overrides.pop(key, None)

    def _on_nest_domain(self, _change):
        """Prefill the child grid from a selected DomainSpec (and enable nesting)."""
        name = self.nest_domain_dd.value
        if name == "<custom>":
            return
        data = self.catalog.domain_data(name)
        split = self._split_child_nesting(
            data.get("grid_kwargs", {}) or {},
            None,
            bool(self.nest_pressure_fluxes.value),
        )
        if split is None:
            return
        gk, metadata, pressure_fluxes = split
        gk = gk or {}
        with self._suspend():
            self.nest_enable.value = True
            # The child spec's own grid_kwargs (its hmin, its channel closing)
            # describe the child, so they go with it (see _nested_grid_kwargs).
            self._set_loaded_grid_kwargs("child", gk)
            if (period := (metadata or {}).get("period")) is not None:
                self.nest_period.value = float(period)
            self.nest_pressure_fluxes.value = pressure_fluxes
            for k, w in self.child_w.items():
                if k in gk:
                    w.value = gk[k]
            # The child spec's own topography, not this grid's (see
            # _on_parent_domain for why this matters).
            self.child_topo_source.value = data.get("topography_source", "ETOPO5")
            self.child_topo_path.value = data.get("topography_path", "") or ""
        self._rebuild()
        self._on_nest_plot(None)

    def _split_child_nesting(
        self,
        grid_kwargs_child: dict[str, Any] | None,
        metadata_child: dict[str, Any] | None,
        include_pressure_fluxes: bool,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool] | None:
        """Fold a spec's child nesting settings out of its ``grid_kwargs_child``.

        See :func:`split_child_nesting_metadata`. A spec it refuses is reported in
        the status banner and gives ``None``, so the caller leaves the widgets be.
        """
        try:
            return split_child_nesting_metadata(
                grid_kwargs_child, metadata_child, include_pressure_fluxes
            )
        except ValueError as exc:
            self.derived.value = components.banner(
                "err", f"<b>Invalid child domain.</b> {html.escape(str(exc))}"
            )
            return None

    def _on_parent_domain(self, _change):
        """Prefill the parent grid from a selected DomainSpec (and enable it)."""
        name = self.parent_domain_dd.value
        if name == "<custom>":
            return
        data = self.catalog.domain_data(name)
        gk = data.get("grid_kwargs", {}) or {}
        with self._suspend():
            self.parent_enable.value = True
            self._set_loaded_grid_kwargs("parent", gk)
            for k, w in self.parent_w.items():
                if k in gk:
                    w.value = gk[k]
            # Carry the parent spec's topography too: the parent is rebuilt so
            # align_grids can blend this grid toward the parent's mask/bathymetry,
            # so it must be built from what the PARENT run used -- not this grid's
            # (typically finer, differently-tiled) dataset, which may not even
            # cover the larger parent footprint. Explicit, never the inherit
            # sentinel: the spec said what it was built with.
            self.parent_topo_source.value = data.get("topography_source", "ETOPO5")
            self.parent_topo_path.value = data.get("topography_path", "") or ""
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

        Disabling a parent undoes both (see ``_restore_non_child_defaults``) so a
        non-child domain isn't stranded on the resolver's errors for a missing IC
        or (under MARBL) boundary BGC sources.
        """
        if getattr(self, "_suspended", False):
            return
        if change["new"]:
            self._clear_boundary_forcing()
            self._clear_initial_conditions()
        else:
            self._restore_non_child_defaults()

    def _restore_non_child_defaults(self):
        """Undo what enabling a parent cleared, for a domain that no longer has
        one: the IC default and the selected ForcingSpec's boundary BGC rows. Each
        is restored only while still cleared; what the user re-added by hand
        while parented is left alone.
        """
        self._restore_initial_conditions_default()
        self._restore_boundary_forcing_default()

    def _clear_boundary_forcing(self):
        """Remove any boundary-bgc rows from the forcing editor (UX mirror of the
        durable clear in ``_gather()``, which is the source of truth and forces
        ``forcing.boundary`` to ``None`` outright regardless of this UI state).

        Boundary's physics scalar widgets (``self.boundary_name``/etc., see
        ``__init__``) are intentionally left as-is -- there's nothing meaningful to
        reset them to, and ``_gather()`` ignores them entirely for a child grid.
        """
        if getattr(self, "_forcing_editor", None) is not None:
            self._forcing_editor.clear_category("boundary_bgc")

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

    def _restore_boundary_forcing_default(self):
        """Undo ``_clear_boundary_forcing`` when a parent is turned back off:
        reseed the boundary-bgc rows from the selected ForcingSpec, if they are
        still empty. Read from the catalog, not ``_forcing_seed``, which has no
        boundary rows when the spec was picked while parented.
        """
        if getattr(self, "_forcing_editor", None) is None:
            return
        fi, _cdr = _split_forcing_data(self.catalog.forcing_data(self.forcing_dd.value))
        self._forcing_editor.reseed_category("boundary_bgc", fi)

    # ---- forcing spec -------------------------------------------------------
    def _default_forcing_spec(self, bgc_mode: str) -> str | None:
        """The ForcingSpec preselected for *bgc_mode*: the named bundled default
        when the catalog has it (see ``_DEFAULT_FORCING_SPEC``), else the plain
        default, else the first spec.
        """
        names = list(self.catalog.forcing_names)
        preferred = (
            _DEFAULT_FORCING_SPEC
            if bgc_mode == "marbl"
            else _DEFAULT_PHYSICS_FORCING_SPEC
        )
        for name in (preferred, _DEFAULT_FORCING_SPEC):
            if name in names:
                return name
        return names[0] if names else None

    def _forcing_modified(self) -> bool:
        """Whether the forcing editor deviates from the last catalog pick. A box
        the editor cannot read counts as an edit: the pick's own boxes always read.
        """
        editor = self._forcing_editor
        if editor is None or self._forcing_seed is None:
            return False
        try:
            return editor.gather() != self._forcing_seed
        except ValueError:
            return True

    @property
    def _active_overrides(self) -> dict[Any, Any]:
        """The overrides that apply in the selected BGC mode -- the only ones that
        reach the blueprint, the saved file and the counts. ``self._overrides`` is
        the user's whole record, which keeps what a mode switch hides, so switching
        back brings it back.
        """
        return {
            (section, field): value
            for (section, field), value in self._overrides.items()
            if _applies_in(section, field, self.model_card.bgc_mode)
        }

    def _build_forcing_editor(self, base_inputs: dict[str, Any]):
        self._forcing_editor = _ForcingEditor(
            self.W,
            base_inputs,
            on_change=self._on_forcing_change,
            co2_static=self.co2_static_box,
        )
        self._forcing_editor.show_co2(self.model_card.bgc_mode == "marbl")
        self._forcing_editor.show_bgc(self.model_card.bgc_mode == "marbl")
        self.forcing_box.children = [self._forcing_editor.widget]

    def _sync_marbl_forcing_visibility(self, _change=None) -> None:
        """Show the forcing editor's "Atmospheric CO2" block and its Biogeochemistry
        sections (unless they hold something) under MARBL only.
        """
        if self._forcing_editor is not None:
            marbl = self.model_card.bgc_mode == "marbl"
            self._forcing_editor.show_co2(marbl)
            self._forcing_editor.show_bgc(marbl)

    def _on_forcing_spec(self, _change):
        """Selecting a ForcingSpec reseeds the forcing editor. CDR is its own
        independently composable spec (CdrSpec, see ``self.cdr_mode_dd``) and is
        normally untouched by a ForcingSpec pick -- the one exception is a
        *legacy* ForcingSpec predating the CdrSpec split that still embeds a
        ``cdr_forcing`` block (see ``_split_forcing_data``); that embedded dict is
        routed into the CDR box as if it had been uploaded as a YAML.
        """
        if getattr(self, "_suspended", False):
            return
        fi, legacy_cdr = _split_forcing_data(
            self.catalog.forcing_data(self.forcing_dd.value)
        )
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
        if legacy_cdr:
            self.cdr_mode_dd.value = "yaml"
            self._apply_cdr_mode()
            self._cdr_forcing = legacy_cdr
            self.cdr_status.value = components.status_span(
                "ok",
                f"✓ CDR loaded from legacy ForcingSpec "
                f"embed: {len(legacy_cdr.get('releases', []))} release(s)",
            )
        self._forcing_seed = self._forcing_editor.gather()
        self._rebuild()

    def _on_bgc_mode(self, change) -> None:
        """A BGC mode change re-resolves once. An untouched default ForcingSpec
        follows the mode (the BGC-carrying default for MARBL, the physics-only one
        otherwise); a hand-edited or non-default forcing is left alone.
        """
        if getattr(self, "_suspended", False) or self._syncing:
            return
        old_default = self._default_forcing_spec(change["old"])
        new_default = self._default_forcing_spec(change["new"])
        forcing_modified = self._forcing_modified()
        if (
            self.forcing_dd.value == old_default
            and new_default != old_default
            and not forcing_modified
        ):
            self.forcing_dd.value = new_default  # _on_forcing_spec rebuilds
        else:
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
        # CDR is independently composable (CdrSpec) and, unlike domain, has no
        # meaningful "custom name" of its own when unpicked -- <custom> just
        # means "hand-authored, not saved as a spec" (name=None).
        cdr = (
            SpecRef(name=self.cdr_dd.value, origin="catalog")
            if self.cdr_dd.value != "<custom>"
            else SpecRef()
        )
        return Composition(
            model=SpecRef(name=self.model_card.name, origin="catalog"),
            domain=dom,
            forcing=forcing,
            cdr=cdr,
            output=output,
        )

    def _verify_spec_roundtrip(self, spec: str, new_name: str) -> bool:
        """Side-effect-free check: does re-resolving with ``spec`` sourced from
        its freshly-written catalog file (``new_name``) reproduce the exact same
        resolved blueprint currently shown (``self.config``)?

        Reads ``self._gather()``/``self._active_overrides``/``self.catalog`` only --
        mutates nothing on the wizard. Compares ``content_hash()``, which covers
        exactly the results-affecting data (excludes identity/composition/
        provenance/working_dir -- see ``ForgeBlueprint._hashed_content``), so a
        match proves the saved spec is a safe substitute for what's currently
        composed/edited and the spec can be marked ``modified=False``.
        """
        if self.config is None:
            return False
        kw = self._gather()
        overrides2 = self._active_overrides
        try:
            if spec == "output":
                kw["output_settings"] = self.catalog.output_data(new_name)
                overrides2 = {
                    k: v for k, v in overrides2.items() if not _is_output_key(*k)
                }
            elif spec == "model":
                kw["model_path"] = self.catalog.model_path(new_name)
                # Let the saved spec speak for these: the resolver falls back to the
                # ModelSpec when use_pio/bgc_mode are None (resolve.py:418-421) and to
                # code.roms/code.marbl verbatim when roms_ref/marbl_ref are absent.
                # Re-applying the live widget values here would apply them to BOTH
                # sides and make the verifier structurally blind to a spec that
                # dropped them.
                for k in ("use_pio", "bgc_mode", "roms_ref", "marbl_ref"):
                    kw.pop(k, None)
                # The wizard-only settings are not saved into the spec but stay on
                # the live config, so they stay on both sides too.
                overrides2 = {
                    k: v
                    for k, v in overrides2.items()
                    if _is_output_key(*k) or k in _WIZARD_SETTING_DEFAULTS
                }
            elif spec == "forcing":
                # CDR is its own independently composable spec (CdrSpec, see
                # self.cdr_mode_dd) -- picking a ForcingSpec never touches it, so
                # kw["cdr"] (already set by _gather()) is left as-is here.
                fi, _legacy_cdr = _split_forcing_data(
                    self.catalog.forcing_data(new_name)
                )
                if self.parent_enable.value:
                    # Mirror the _gather() durable clear so re-verifying against a
                    # freshly-picked ForcingSpec doesn't spuriously show "modified"
                    # for a child grid whose boundary forcing is always stripped.
                    fi.setdefault("forcing", {})["boundary"] = None
                kw["forcing_inputs"] = fi
            elif spec == "cdr":
                # catalog.cdr_data() carries a "description" key CdrSpec forbids
                # (extra="forbid") -- build_forge_blueprint's cdr= path pops it,
                # same as the other catalog-dict-shaped kwargs above.
                kw["cdr"] = self.catalog.cdr_data(new_name)
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
        # As _show_editor does for self.config.
        force_switches_without_input_off(
            eff2,
            is_child=cfg2.domain.grid_kwargs_parent is not None,
            has_tidal_forcing=bool(cfg2.forcing.tidal),
        )
        cfg2 = cfg2.model_copy(update={"model_settings": eff2})
        return cfg2.content_hash() == self.config.content_hash()

    @staticmethod
    def _sources_to_inputs(cfg: ForgeBlueprint) -> dict[str, Any]:
        """Reconstruct an ``inputs``-shaped forcing dict from a ForgeBlueprint's sources
        (reverse of the resolver) so a loaded config seeds the forcing editor.

        The set of plain (non-``source``) fields to copy per item is derived from the
        item model's own ``model_fields`` -- mirrors ``forge_blueprint_resolve.
        _build_forcing``'s ``_items()`` helper, which replaced an equivalent hand-
        maintained whitelist here after it was confirmed to silently drop fields added
        to the schema but forgotten in the whitelist (``wind_dropoff``/``options``).
        This fixes the reconstruction into the *seed dict* this function returns; note
        that ``prefill_kwargs``/``extrap_kwargs`` still have no dedicated widget in
        ``_make_row``, so they're correctly seeded here but then dropped again on the
        very next ``gather()`` -- a pre-existing UI gap (no widget ever read them,
        before or after this change), not something this fix resolves end-to-end. A
        field is only emitted when it differs from the model's own default (an unset
        optional, a false flag, an empty dict/list, or the type's default enum member)
        -- matches the old hand-written checks and keeps a reloaded spec gathering back
        to the same dict the resolver would have produced.
        """

        def src(spec) -> dict[str, Any]:
            # SourceSpec's own fields (climatology/glorys_layout/path/constants/
            # esper_method/esper_equation) -- generic `plain()` handles all of
            # them uniformly (esper_method/esper_equation are plain Literal
            # strings/ints, not Enum members, so no special-casing needed).
            d: dict[str, Any] = {"name": spec.name}
            d.update(plain(spec, SourceSpec, skip=("name",)))
            return d

        def plain(it, cls, skip=("source",)) -> dict[str, Any]:
            """Copy every field ``cls`` declares (except ``skip``) that differs from
            its schema default, normalizing Enum members to their plain value.
            """
            d: dict[str, Any] = {}
            for name, finfo in cls.model_fields.items():
                if name in skip:
                    continue
                v = getattr(it, name, None)
                if v is None or v == finfo.default:
                    continue
                if isinstance(v, (list, dict)):
                    if v:  # skip an explicit-but-empty list/dict (== "unset")
                        d[name] = list(v) if isinstance(v, list) else dict(v)
                    continue
                d[name] = getattr(v, "value", v)
            return d

        def bgc_section(spec) -> dict[str, Any]:
            """Reconstruct an InitialConditions/BoundaryForcing-shaped seed dict --
            shared by IC and boundary, which have this identical shape (see
            forge_blueprint.BgcSourceItem).
            """
            d: dict[str, Any] = {"source": src(spec.source)}
            bgc_sources = []
            for bs in spec.bgc_sources:
                # BgcSourceItem's own fields (use_vars/bgc_interpolation_method/
                # serialize_dask) -- carrying serialize_dask back into the seed
                # matters: editing an existing blueprint in the wizard would
                # otherwise silently drop it, and a large domain would go back
                # to the write that fails.
                bd: dict[str, Any] = {"source": src(bs.source)}
                bd.update(plain(bs, BgcSourceItem))
                bgc_sources.append(bd)
            if bgc_sources:
                d["bgc_sources"] = bgc_sources
            return d

        f = cfg.forcing
        if f.initial_conditions is None:
            # Explicit sentinel (as opposed to a plain missing key) so
            # `_ForcingEditor.__init__` seeds the "(none)" dropdown option
            # instead of falling back to the fresh-wizard GLORYS default.
            ic: dict[str, Any] | None = None
        else:
            ic = bgc_section(f.initial_conditions)
            ic.update(
                plain(
                    f.initial_conditions,
                    InitialConditions,
                    skip=("source", "bgc_sources"),
                )
            )

        forcing: dict[str, Any] = {}
        if f.boundary is None:
            # Explicit sentinel (as opposed to an omitted key) so
            # `_ForcingEditor.__init__` seeds the "(none)" dropdown option
            # instead of falling back to the fresh-wizard default source.
            forcing["boundary"] = None
        else:
            boundary = bgc_section(f.boundary)
            boundary.update(
                plain(f.boundary, BoundaryForcing, skip=("source", "bgc_sources"))
            )
            forcing["boundary"] = boundary
        for cat, items, cls in (
            ("surface", f.surface, SurfaceForcingItem),
            ("tidal", f.tidal, TidalForcingItem),
            ("river", f.river, RiverForcingItem),
        ):
            out = []
            for it in items:
                d: dict[str, Any] = {"source": src(it.source)}
                _custom_file = getattr(it, "custom_file", None)
                if _custom_file is not None:
                    # A CUSTOM_FILE river item carries no other fields (see
                    # RiverForcingItem's mutual-exclusion validators). Emit just the
                    # file and skip the generic copy below, which would otherwise
                    # embed the UserProvidedFile model object itself rather than the
                    # plain location/content_hash dict the seed dict needs.
                    d["custom_file"] = {
                        "location": _custom_file.location,
                        "content_hash": _custom_file.content_hash,
                    }
                    out.append(d)
                    continue
                # `blend` is a nested model: plain() would hand back the object, so it
                # is turned into plain data here (the row widgets read it as a dict).
                d.update(plain(it, cls, skip=("source", "blend")))
                blend = getattr(it, "blend", None)
                if blend is not None:
                    d["blend"] = {
                        "fallback": {
                            "source": src(blend.fallback.source),
                            **plain(blend.fallback, SurfaceBlendFallback),
                        },
                        "width_km": blend.width_km,
                        "profile": blend.profile.value,
                    }
                out.append(d)
            forcing[cat] = out
        return {
            "initial_conditions": ic,
            "forcing": forcing,
        }

    def _on_domain(self, _change):
        """Prefill from a cataloged DomainSpec when one is selected."""
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
        split = self._split_child_nesting(
            data.get("grid_kwargs_child"),
            data.get("metadata_child"),
            bool(data.get("nesting_include_pressure_fluxes", False)),
        )
        if split is None:
            return
        child, child_metadata, pressure_fluxes = split
        was_parented = self.parent_enable.value
        with self._suspend():
            self.grid_name.value = data.get("grid_name", name)
            self._set_loaded_grid_kwargs("grid", gk)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.scoord_chk.value = any(k in gk for k in _SCOORD)
            # The grid_kwargs keys with their own widgets (as _populate_from).
            self.hmin.value = float(gk.get("hmin", _GRID_KWARGS_LEFT_OUT["hmin"]))
            self.close_narrow_chk.value = bool(
                gk.get(
                    "close_narrow_channels",
                    _GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
                )
            )
            self.mask_shapefile.value = str(gk.get("mask_shapefile") or "")
            # Domain-derived properties: a picked catalog Domain replaces the
            # current grid entirely, so reset touched/derived state first, then
            # restore only what the saved DomainSpec actually carries --
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
            saved_dt = data.get("dt")
            if saved_dt is not None:
                self.dt.value = float(saved_dt)
            self._dt_touched = saved_dt is not None
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
            for key, run_key, picker in (
                ("start_time", "start", self.start),
                ("end_time", "end", self.end),
            ):
                if data.get(key):
                    when = datetime.fromisoformat(str(data[key]))
                    picker.value = when.date()
                    self._loaded_run_times[run_key] = when
            # model_name in a DomainSpec is provenance (the model in use when the
            # domain was saved), not a preference: picking a domain must not
            # override the user's Model selection, so it is deliberately NOT
            # restored here (unlike the load-a-full-blueprint path, where the
            # blueprint's composition.model is authoritative).
            self.topo_source.value = data.get("topography_source", "ETOPO5")
            self.topo_path.value = data.get("topography_path", "") or ""
            # Nesting: mirrors _populate_nesting (loaded-blueprint path) but reads
            # from a DomainSpec (only present if the spec was saved
            # via register_domain_from_dict with nesting active).
            self._set_loaded_grid_kwargs("child", child)
            self.nest_enable.value = child is not None
            if child:
                for k, w in self.child_w.items():
                    if k in child:
                        w.value = child[k]
                self._set_nested_topo_widgets("child", child)
                period = (child_metadata or {}).get("period")
                if period is not None:
                    self.nest_period.value = float(period)
                self.nest_pressure_fluxes.value = pressure_fluxes
            # Parent: mirrors the child block above, reading grid_kwargs_parent
            # (only present if the spec was saved with a parent grid active).
            parent = data.get("grid_kwargs_parent")
            self._set_loaded_grid_kwargs("parent", parent)
            self.parent_enable.value = parent is not None
            if parent:
                for k, w in self.parent_w.items():
                    if k in parent:
                        w.value = parent[k]
                self._set_nested_topo_widgets("parent", parent)
            elif was_parented:
                # Suspended, so _on_parent_toggle skipped this; the rebuilds
                # below pick up the restored rows.
                self._restore_non_child_defaults()
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
            "v_sponge": self._v_sponge_value(),
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
            "child_topo": (self.child_topo_source.value, self.child_topo_path.value),
            "parent_topo": (
                self.parent_topo_source.value,
                self.parent_topo_path.value,
            ),
        }

    def _set_nested_topo_widgets(self, which: str, gk: dict[str, Any]) -> None:
        """Load a nested grid's optional ``topography_source``/``topography_path``
        (from a blueprint or DomainSpec ``grid_kwargs_parent``/``_child`` dict)
        into its widgets; absent keys mean "inherit this grid's" (the sentinel /
        blank). Called inside a ``_suspend()`` block by the populate paths.
        """
        src_w = getattr(self, f"{which}_topo_source")
        path_w = getattr(self, f"{which}_topo_path")
        src = gk.get("topography_source")
        if isinstance(src, dict):  # hand-authored roms-tools {'name','path'}
            path_w.value = str(gk.get("topography_path") or src.get("path") or "")
            src = src.get("name")
        else:
            path_w.value = str(gk.get("topography_path") or "")
        src = getattr(src, "value", src)
        src_w.value = str(src) if src in _TOPO_SOURCES else _NESTED_TOPO_INHERIT

    def _spec_description(self) -> str:
        """The description a catalog spec saved from this session gets.

        The text the user typed for the blueprint, or empty while the field holds
        the derived one: that summarizes the whole run (grid, dates, forcing, CDR),
        so it would describe no single spec. Empty leaves a ModelSpec with the
        description of the spec it was saved from (``register_model_from_settings``).
        """
        return self.description.value.strip() if self._description_touched else ""

    def _domain_spec_data(self) -> dict[str, Any]:
        """Build a DomainSpec-shaped dict from the current widget state (the
        domain-spec extractor for "save modified specs to catalog"). Includes
        topography and nesting -- both results-affecting -- so a saved DomainSpec
        actually round-trips (see ``_verify_spec_roundtrip``); ``register_domain``
        (the ForgeExecutor-driven path) predates these and omits them.
        """
        kw = self._gather()
        data: dict[str, Any] = {
            "description": self._spec_description(),
            "model_name": self.model_card.name,
            "grid_name": self.grid_name.value,
            "start_time": _run_time_iso(self._run_datetime("start", self.start.value)),
            "end_time": _run_time_iso(self._run_datetime("end", self.end.value)),
            "grid_kwargs": kw["grid_kwargs"],
            "partitioning": kw["partitioning"],
            "topography_source": self.topo_source.value,
        }
        # v_sponge/open_boundaries: included only when the user has touched
        # them (a manual edit or a value restored from a prior save/load) --
        # otherwise omitted so a reload re-derives fresh from the grid (see
        # _on_domain's symmetric restore-only-if-present, and
        # _ensure_boundaries_derived for the pre-save safety net that fills
        # kw["open_boundaries"] with a real mask-derived value first).
        if self._v_sponge_touched:
            data["v_sponge"] = kw["v_sponge"]
        if self._dt_touched:
            data["dt"] = kw["dt"]
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
            self.load_status.value = components.status_span(
                "err", "Enter a path first."
            )
            return
        try:
            cfg = ForgeBlueprint.from_yaml(path)
        except Exception as exc:
            self.load_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            self._load_status_is_error = True
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _on_load_from_catalog(self, _):
        name = self.load_catalog_dd.value
        if not name:
            self.load_status.value = components.status_span(
                "err", "No catalog blueprint selected."
            )
            self._load_status_is_error = True
            return
        try:
            path = self.catalog.forge_blueprint_path(name)
        except Exception as exc:
            self.load_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            self._load_status_is_error = True
            return
        # Surface the resolved path, then reuse the free-text load path so
        # parsing, error handling, and state update stay identical.
        self.load_path.value = str(path)
        self._on_load_path(None)

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
            self.load_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            self._load_status_is_error = True
            return
        self._set_load_status(cfg, self._populate_from(cfg))

    def _set_load_status(self, cfg: ForgeBlueprint, loaded_problems):
        msg = components.status_span("ok", f"Loaded {cfg.casename}")
        if loaded_problems:
            msg += " &nbsp;" + components.status_span(
                "err", f"⚠ {len(loaded_problems)} invalid settings value(s) in the file"
            )
        if self._load_unknown_specs:
            # The settings still come from the file; only the recorded name
            # changes to the spec the dropdown shows.
            msg += " &nbsp;" + components.status_span(
                "warn",
                "⚠ not in this catalog, saved "
                "under the spec selected instead: "
                + html.escape(", ".join(self._load_unknown_specs)),
            )
        self.load_status.value = msg
        self._load_status_is_error = False

    # ---- CDR spec selection + mode -------------------------------------------
    def _on_cdr_spec(self, _change):
        """Selecting a CdrSpec loads its mode + fields into the box."""
        if getattr(self, "_suspended", False):
            return
        name = self.cdr_dd.value
        if name == "<custom>":
            self._cdr_seed = None
            self._rebuild()
            return
        data = self.catalog.cdr_data(name)
        # Set the mode under _suspend() (its observer would run _apply_cdr_mode
        # itself) so the single explicit call below is the only apply -- and its
        # "simple" seeding runs before _populate_cdr_simple overwrites it.
        with self._suspend():
            self.cdr_mode_dd.value = data.get("mode", "none")
        self._apply_cdr_mode()
        mode = data.get("mode", "none")
        if mode == "simple":
            self._populate_cdr_simple(data.get("cdr_forcing") or {})
        elif mode == "yaml":
            cdr = data.get("cdr_forcing")
            self._cdr_forcing = cdr
            self.cdr_status.value = (
                components.status_span(
                    "ok",
                    f"✓ CDR loaded from spec '{name}': "
                    f"{len(cdr.get('releases', []))} release(s)",
                )
                if cdr
                else ""
            )
        elif mode == "netcdf":
            cff = data.get("cdr_forcing_file")
            self._cdr_forcing_file = cff
            if cff:
                self.cdr_file_path.value = cff.get("location", "")
                self.cdr_file_status.value = _user_file_status_html(cff)
        self._cdr_seed = self._cdr_snapshot()
        self._rebuild()

    def _cdr_snapshot(self) -> dict[str, Any]:
        """The CDR-defining state at the moment of a catalog CdrSpec pick.
        Compared against the current state in `_rebuild()` to detect an edit made
        after selection (`composition.cdr.modified`) -- mirrors
        `_domain_snapshot`/`_forcing_seed`.
        """
        return self._current_cdr_dict()

    def _on_cdr_mode_change(self, _change):
        if getattr(self, "_suspended", False):
            return
        self._apply_cdr_mode()

    def _apply_cdr_mode(self) -> None:
        """Show/hide the per-mode sub-panel for the current ``cdr_mode_dd`` value,
        seed defaults on first activation of "simple", clear stale state left
        over from whichever mode is being left, invalidate the plot cache (a
        mode switch always changes what would be plotted), and rebuild.

        Called both from ``cdr_mode_dd``'s own ``.observe`` (via
        ``_on_cdr_mode_change``) and directly wherever ``cdr_mode_dd.value`` is
        set programmatically under `_suspend()` (e.g. `_populate_from`,
        `_on_cdr_spec`) -- suspended observers are no-ops, so those callers must
        invoke this themselves.
        """
        old_mode = self._cdr_mode_active
        mode = self.cdr_mode_dd.value
        if old_mode == "yaml" and mode != "yaml":
            self._cdr_forcing = None
            self._clear_cdr_upload_value()
            self.cdr_status.value = ""
        if old_mode == "netcdf" and mode != "netcdf":
            self._cdr_forcing_file = None
            self.cdr_file_path.value = ""
            self.cdr_file_upload.value = ()
            self.cdr_file_status.value = ""
        if old_mode == "simple" and mode != "simple":
            self._keep_cdr_simple(None)
            self._cdr_simple_seeded = {}
        self._cdr_mode_active = mode

        self.cdr_simple_box.layout.display = "" if mode == "simple" else "none"
        self.cdr_yaml_box.layout.display = "" if mode == "yaml" else "none"
        self.cdr_netcdf_box.layout.display = "" if mode == "netcdf" else "none"
        self.cdr_upscaled_box.layout.display = "" if mode == "upscaled" else "none"
        self.cdr_plot_box.layout.display = (
            "" if mode in ("simple", "yaml", "netcdf") else "none"
        )
        if (
            mode == "netcdf"
            and self._cdr_forcing_file is None
            and not self.cdr_file_status.value
        ):
            # CdrSpec rejects mode="netcdf" without cdr_forcing_file; say what to
            # do here rather than leave only the pydantic dump in the preview.
            self.cdr_file_status.value = _CDR_FILE_HINT

        if mode == "simple" and old_mode != "simple":
            # Seed from the grid center / run window; a field the user leaves
            # alone keeps following them (see _reseed_cdr_simple).
            with self._suspend():
                self.cdr_simple_lat.value = float(self.grid_w["center_lat"].value)
                self.cdr_simple_lon.value = float(self.grid_w["center_lon"].value)
                if self.start.value is not None:
                    self.cdr_simple_start.value = self.start.value
                if self.end.value is not None:
                    self.cdr_simple_end.value = self.end.value
            self._cdr_simple_seeded = {
                key: widget.value
                for key, widget in self._cdr_simple_seed_targets().items()
            }

        self._invalidate_cdr_plot_cache()
        self._rebuild()

    def _resolved_cdr_dict(self, current: dict[str, Any]) -> dict[str, Any]:
        """``current`` (a :meth:`_current_cdr_dict`) with ``cdr_forcing`` as the
        resolver will hand it to roms-tools: under ``bgc_mode: cdr_lite``, with
        the tracer set it defaults (``with_cdr_lite_tracer_set`` decides which
        modes that is). The plot builds from this, so what it shows is what
        will be generated.
        """
        if self.model_card.bgc_mode != "cdr_lite":
            return current
        spec = with_cdr_lite_tracer_set(CdrSpec(**current))
        return {**current, "cdr_forcing": spec.cdr_forcing}

    def _cdr_plot_inputs_fingerprint(self) -> tuple[str, tuple]:
        """A cheap, comparable snapshot of everything the CDR plot depends on:
        the CDR selection it builds from (mode + compiled/attached config, as
        resolved for the BGC mode) and the grid geometry the ``rt.CDRForcing``
        would be built against.
        """
        selection = self._current_cdr_dict()
        try:
            selection = self._resolved_cdr_dict(selection)
        except ValueError:
            pass  # incomplete or invalid selection: building the plot reports it
        return (
            repr(selection),
            tuple(sorted((k, repr(w.value)) for k, w in self.grid_w.items())),
        )

    def _invalidate_cdr_plot_cache(self) -> None:
        """Drop the built ``rt.CDRForcing``/grid and every rendered plot PNG --
        but only when a plot-affecting input actually changed.

        Hooked into ``_rebuild()`` itself (see its own comment) rather than onto
        each individual CDR/grid widget -- the file's least-invasive idiom for
        "any of several unrelated inputs invalidates this": ``_rebuild()`` already
        fires on every CDR-panel edit, every mode switch (via ``_apply_cdr_mode``),
        and every grid-geometry edit (``watched``/``_on_grid_kwarg_change`` both
        end in a rebuild). Because ``_rebuild()`` ALSO fires for edits that can't
        affect the plot (e.g. the description field), the fingerprint check below
        skips the clear when nothing plot-affecting changed -- an already-rendered
        plot survives unrelated edits instead of silently blanking.
        """
        fingerprint = self._cdr_plot_inputs_fingerprint()
        if fingerprint == getattr(self, "_cdr_plot_fingerprint", None):
            return
        self._cdr_plot_fingerprint = fingerprint
        self._cdr_plot_object = None
        self._cdr_plot_grid = None
        self._cdr_plot_cache = {}
        self.cdr_plot_img.value = b""
        self.cdr_plot_img.layout.display = "none"
        self.cdr_plot_status.value = ""
        self.cdr_plot_release_dd.options = []
        self.cdr_plot_release_dd.layout.display = "none"

    def _current_cdr_dict(self) -> dict[str, Any]:
        """The current CDR selection as a ``cdr=`` kwarg dict for
        ``build_forge_blueprint`` (shape matches :class:`CdrSpec`, and --
        ``cdr_forcing``/``cdr_forcing_file`` always both present, unused ones
        ``None`` -- matches ``catalog.cdr_data()`` too, so ``_cdr_snapshot()``
        can compare directly against a freshly-picked catalog entry). Used by
        ``_gather()``, ``_cdr_snapshot()``, and the plot widget (simple/yaml
        share this dict's ``cdr_forcing`` as the ``rt.CDRForcing`` kwargs).
        """
        mode = self.cdr_mode_dd.value
        cdr_forcing = None
        cdr_forcing_file = None
        if mode == "simple":
            cdr_forcing = self._simple_cdr_forcing_dict()
        elif mode == "yaml":
            cdr_forcing = self._cdr_forcing
        elif mode == "netcdf":
            cdr_forcing_file = self._cdr_forcing_file
        return {
            "mode": mode,
            "cdr_forcing": cdr_forcing,
            "cdr_forcing_file": cdr_forcing_file,
        }

    def _simple_cdr_forcing_dict(self) -> dict[str, Any]:
        """The "simple" mode's ``cdr_forcing``: a loaded one the panel cannot
        show, unchanged (see ``_populate_cdr_simple``), else the panel compiled
        (see ``_compile_simple_cdr``).
        """
        if self._cdr_simple_loaded is not None:
            return copy.deepcopy(self._cdr_simple_loaded)
        return self._compile_simple_cdr()

    def _compile_simple_cdr(self) -> dict[str, Any]:
        """Compile the "simple" panel into a roms-tools ``CDRForcing`` kwargs
        dict: a single tracer-perturbation release, a flat two-point ALK pulse
        (the same static flux at both the run's start and end) -- see the CDR
        Overhaul plan's "simple mode compile" note. Values are ISO-8601 strings
        (not ``datetime`` objects) so the dict stays YAML/JSON-serializable on
        the blueprint; both ``rt.CDRForcing`` (pydantic) and ``CdrSpec`` parse
        an ISO string back into a datetime on their own. A date the run shares
        takes the run's time of day (see ``_run_datetime``).
        """
        start = self.cdr_simple_start.value or self.start.value
        end = self.cdr_simple_end.value or self.end.value
        start_iso = (
            self._run_datetime("start", start).isoformat()
            if start is not None
            else None
        )
        end_iso = (
            self._run_datetime("end", end).isoformat() if end is not None else None
        )
        flux = float(self.cdr_simple_flux.value)
        return {
            "start_time": start_iso,
            "end_time": end_iso,
            "releases": [
                {
                    "name": self.cdr_simple_name.value,
                    "lat": float(self.cdr_simple_lat.value),
                    "lon": float(self.cdr_simple_lon.value),
                    "depth": float(self.cdr_simple_depth.value),
                    "hsc": float(self.cdr_simple_hsc.value),
                    "vsc": float(self.cdr_simple_vsc.value),
                    "times": [start_iso, end_iso],
                    "tracer_fluxes": {"ALK": [flux, flux]},
                    "release_type": "tracer_perturbation",
                }
            ],
        }

    def _populate_cdr_simple(self, cdr_forcing: dict[str, Any]) -> None:
        """Reverse-derive the "simple" panel's widget values from a compiled CDR
        forcing dict (the shape ``_simple_cdr_forcing_dict`` produces) -- the
        load-back half of "simple" mode's round trip. A dict the fields would
        not compile back to unchanged is kept as loaded instead (see
        ``_keep_cdr_simple``), with the fields showing its first release.
        """
        releases = cdr_forcing.get("releases") or []
        release = releases[0] if releases else {}
        with self._suspend():
            self.cdr_simple_name.value = release.get("name", "my_cdr")
            self.cdr_simple_lat.value = float(release.get("lat", 0.0))
            self.cdr_simple_lon.value = float(release.get("lon", 0.0))
            self.cdr_simple_depth.value = float(release.get("depth", 1.0))
            self.cdr_simple_hsc.value = float(release.get("hsc", 10.0))
            self.cdr_simple_vsc.value = float(release.get("vsc", 10.0))
            fluxes = (release.get("tracer_fluxes") or {}).get("ALK")
            flux = fluxes[0] if isinstance(fluxes, list) else fluxes
            self.cdr_simple_flux.value = float(flux) if flux is not None else 2 * 10**6
            times = release.get("times") or []
            if times:
                self.cdr_simple_start.value = datetime.fromisoformat(
                    str(times[0])
                ).date()
                self.cdr_simple_end.value = datetime.fromisoformat(
                    str(times[-1])
                ).date()
        # A loaded release is the user's, not a seed to move with the grid.
        self._cdr_simple_seeded = {}

        # The fields hold one release at a constant ALK flux; anything else
        # (a second release, a ramp, DIC, a time of day) is kept as loaded. Both
        # sides are compared as resolved: under cdr_lite a saved release carries
        # the tracer_set the resolver gave it.
        def resolved(forcing: dict[str, Any]) -> dict[str, Any]:
            selection = {"mode": "simple", "cdr_forcing": forcing}
            try:
                forcing = self._resolved_cdr_dict(
                    {**selection, "cdr_forcing_file": None}
                )["cdr_forcing"]
            except ValueError:  # e.g. a volume release under cdr_lite
                pass
            return _cdr_times_as_iso(forcing)

        shown = resolved(self._compile_simple_cdr())
        self._keep_cdr_simple(None if resolved(cdr_forcing) == shown else cdr_forcing)

    def _keep_cdr_simple(self, cdr_forcing: dict[str, Any] | None) -> None:
        """Keep ``cdr_forcing`` as the "simple" mode's forcing, unchanged, and
        lock the panel's fields with a note saying why; ``None`` unlocks them.
        """
        self._cdr_simple_loaded = copy.deepcopy(cdr_forcing)
        kept = cdr_forcing is not None
        for widget in self._cdr_simple_fields:
            widget.disabled = kept
        self.cdr_simple_replace_btn.layout.display = "" if kept else "none"
        self.cdr_simple_note.value = (
            components.banner(
                "warn",
                "<b>Kept as loaded.</b> This CDR forcing has "
                f"{html.escape(_cdr_simple_kept_reason(cdr_forcing))}, which the "
                "fields below cannot show; it is saved unchanged. "
                "<b>Edit as one release</b> replaces it with its first release at "
                "a constant ALK flux.",
            )
            if cdr_forcing is not None
            else ""
        )

    def _on_cdr_simple_replace(self, _btn) -> None:
        """Handle "Edit as one release": drop the kept CDR forcing for what the
        fields show.
        """
        self._keep_cdr_simple(None)
        self._rebuild()

    def _cdr_simple_seed_targets(self) -> dict[str, Any]:
        """The simple CDR fields that entering "simple" mode seeds, by key."""
        return {
            "lat": self.cdr_simple_lat,
            "lon": self.cdr_simple_lon,
            "start": self.cdr_simple_start,
            "end": self.cdr_simple_end,
        }

    def _reseed_cdr_simple(self) -> None:
        """Move each simple CDR field that still shows its seed to the current
        grid center or run window, so moving either does not leave the release
        behind. A field the user edited, a kept or loaded release, and another
        CDR mode are left alone.
        """
        if self.cdr_mode_dd.value != "simple" or self._cdr_simple_loaded is not None:
            return
        now = {
            "lat": float(self.grid_w["center_lat"].value),
            "lon": float(self.grid_w["center_lon"].value),
            "start": self.start.value,
            "end": self.end.value,
        }
        with self._suspend():
            for key, widget in self._cdr_simple_seed_targets().items():
                if (
                    key in self._cdr_simple_seeded
                    and widget.value == self._cdr_simple_seeded[key]
                    and now[key] is not None
                    and now[key] != widget.value
                ):
                    widget.value = now[key]
                    self._cdr_simple_seeded[key] = now[key]

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
            self.cdr_status.value = components.status_span(
                "err", f"CDR invalid: {type(exc).__name__}: {exc}"
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
            cleared_note = "<br>" + components.status_span(
                "note", "(cleared the attached CDR file -- mutually exclusive)"
            )
        self.cdr_status.value = (
            components.status_span("ok", f"✓ CDR: {len(cdr.releases)} release(s)")
            + f"{cleared_note}"
        )
        self._rebuild()

    def _clear_cdr_upload_value(self):
        """Reset ``cdr_upload.value`` back to its empty state.

        ``FileUpload.value`` is a read-only trait (traitlets raises ``TraitError`` on
        a plain ``widget.value = ...`` assignment) -- ``set_trait`` is the sanctioned
        way to write a read-only trait from outside the widget's own JS-driven update
        path. The empty container's type also differs by ipywidgets major version:
        a dict in 7.x, a tuple of dicts in 8.x (mirroring the tuple/dict duality
        `_on_cdr_upload` already handles on read) -- match whatever the widget's
        current value already is, so this works under either version.
        """
        empty = () if isinstance(self.cdr_upload.value, (list, tuple)) else {}
        self.cdr_upload.set_trait("value", empty)

    def _on_cdr_clear(self, _):
        self._cdr_forcing = None
        self._clear_cdr_upload_value()
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
            self.cdr_file_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        # Light validation only (never blocks the attach -- the executor is the
        # hard gate): warn if the file doesn't even look like CDR forcing.
        warning = ""
        try:
            import xarray as xr

            with xr.open_dataset(path) as ds:
                if "ncdr" not in ds.dims:
                    warning = "<br>" + components.status_span(
                        "warn",
                        "⚠ no 'ncdr' dimension "
                        "found -- is this really a CDR-forcing file?",
                    )
                if (
                    self.model_card.bgc_mode == "cdr_lite"
                    and "tracer_name" in ds.coords
                    and not any(
                        role is not None and role[0] == "cdr_lite"
                        for role in map(
                            cdr_tracer_role, map(str, ds["tracer_name"].values)
                        )
                    )
                ):
                    warning += "<br>" + components.status_span(
                        "warn",
                        "⚠ BGC mode is cdr_lite "
                        "but this file's tracer_name axis has no CDR_* tracers -- "
                        "its releases need tracer_set cdr_lite.",
                    )
        except Exception:
            pass
        self._cdr_forcing_file = {"location": str(path), "content_hash": content_hash}
        cleared_note = ""
        if self._cdr_forcing is not None:
            # Mutually exclusive with an uploaded CDR YAML (Forcing's own
            # validator forbids both) -- the file attach wins here.
            self._cdr_forcing = None
            self._clear_cdr_upload_value()
            self.cdr_status.value = ""
            cleared_note = "<br>" + components.status_span(
                "note", "(cleared the uploaded CDR YAML -- mutually exclusive)"
            )
        self.cdr_file_status.value = (
            _user_file_status_html(self._cdr_forcing_file) + warning + cleared_note
        )
        self._rebuild()

    def _on_cdr_file_attach(self, _btn):
        path_str = self.cdr_file_path.value.strip()
        if not path_str:
            self.cdr_file_status.value = components.status_span(
                "err", "Enter a path first."
            )
            return
        # Explicit click always re-hashes -- no dedupe here.
        self._attach_cdr_file_from_path(path_str)

    def _maybe_attach_cdr_file(self) -> None:
        """CDR-file twin of `_maybe_attach_grid_file`: attach the typed path on
        Enter/focus-out unless blank or already attached. CDR mode "netcdf"
        requires cdr_forcing_file (CdrSpec), so a typed-but-unattached path
        used to leave the blueprint invalid with no hint as to why.
        """
        path_str = self.cdr_file_path.value.strip()
        if not path_str or _same_attached_file(self._cdr_forcing_file, path_str):
            return
        self._attach_cdr_file_from_path(path_str)

    def _on_cdr_file_path_submit(self, _change) -> None:
        if getattr(self, "_suspended", False):
            return
        self._maybe_attach_cdr_file()

    def _on_cdr_file_upload(self, change):
        items = change["new"]
        if not items:
            return
        item = (
            items[0] if isinstance(items, (list, tuple)) else next(iter(items.values()))
        )
        dest = _stage_uploaded_netcdf(item["name"], bytes(item["content"]))
        self.cdr_file_path.value = str(dest)  # fires _on_cdr_file_path_submit
        self._maybe_attach_cdr_file()  # same-name re-upload: value unchanged

    def _on_cdr_file_clear(self, _btn):
        self._cdr_forcing_file = None
        self.cdr_file_path.value = ""
        self.cdr_file_upload.value = ()
        # Still in "netcdf" mode with nothing attached -> invalid until re-attached.
        self.cdr_file_status.value = _CDR_FILE_HINT
        self._rebuild()

    # ---- CDR plotting (WP6) ----------------------------------------------------
    # The built rt.CDRForcing/grid (self._cdr_plot_object/_cdr_plot_grid) and every
    # rendered PNG (self._cdr_plot_cache, keyed by (plot_type, release_name)) are
    # cached on the instance -- invalidated in one place, _rebuild() (see its own
    # comment and _invalidate_cdr_plot_cache's docstring). Switching plot type or
    # release therefore re-renders (or reuses a cached PNG) WITHOUT rebuilding --
    # _on_cdr_plot_option_change/_render_cdr_plot below never call _rebuild().
    def _build_cdr_forcing_for_plot(self) -> Any:
        """Build (or return the cached) roms-tools ``CDRForcing`` for the
        current CDR mode. Raises if the current mode has nothing plottable yet
        (upscaled/none never reach here -- the plot panel is hidden for them)
        or the current selection is incomplete (e.g. yaml mode with nothing
        uploaded).
        """
        if self._cdr_plot_object is not None:
            return self._cdr_plot_object
        import roms_tools as rt

        mode = self.cdr_mode_dd.value
        current = self._current_cdr_dict()
        grid = self._build_grid_from_widgets()
        if mode in ("simple", "yaml"):
            if not current["cdr_forcing"]:
                raise ValueError("No CDR forcing uploaded yet.")
            forcing = self._resolved_cdr_dict(current)["cdr_forcing"]
            cdr = rt.CDRForcing(grid=grid, **forcing)
        elif mode == "netcdf":
            cff = current["cdr_forcing_file"]
            if not cff:
                raise ValueError("No CDR-forcing netCDF attached yet.")
            cdr = _cdr_forcing_from_netcdf(cff["location"], grid)
        else:
            raise ValueError(f"CDR plotting is not available in mode {mode!r}.")
        self._cdr_plot_object = cdr
        self._cdr_plot_grid = grid
        return cdr

    def _sync_cdr_plot_release_options(self) -> None:
        """After a (re)build, refresh the release dropdown (shown only when the
        forcing has more than one release) and hide the tracer flux option for a
        volume-type forcing (``plot_tracer_flux`` raises for those -- ALK-only
        per the CDR Overhaul design brief). A forcing with generated tracers
        (cdr_lite/passive) has no ``ALK`` tracer, so its option plots the
        selected release's own tracer instead.
        """
        cdr = self._cdr_plot_object
        names = [r.name for r in cdr.releases]
        is_volume = cdr.release_type == "volume"
        flux_label = "Tracer flux" if cdr.release_tracers else "Tracer flux (ALK)"
        options = [
            ("Release locations", "locations"),
            ("Distribution", "distribution"),
            *([] if is_volume else [(flux_label, "tracer_flux")]),
        ]
        with self._suspend():
            self.cdr_plot_release_dd.options = names
            if names:
                self.cdr_plot_release_dd.value = names[0]
            self.cdr_plot_release_dd.layout.display = "" if len(names) > 1 else "none"
            old_type = self.cdr_plot_type_dd.value
            self.cdr_plot_type_dd.options = options
            self.cdr_plot_type_dd.value = (
                old_type if old_type in dict(options).values() else options[0][1]
            )
        if is_volume:
            self.cdr_plot_status.value = components.status_span(
                "warn",
                "⚠ volume-type CDR forcing -- "
                "tracer flux plotting (ALK-only) isn't available.",
            )

    def _render_cdr_plot(self) -> None:
        """Render the current plot-type/release selection, from the cached
        ``rt.CDRForcing`` and a cached PNG when one already exists for this
        exact (plot_type, release) pair.
        """
        cdr = self._cdr_plot_object
        if cdr is None:
            return
        plot_type = self.cdr_plot_type_dd.value
        release_name = (
            self.cdr_plot_release_dd.value if self.cdr_plot_release_dd.options else None
        )
        key = (
            plot_type,
            release_name if plot_type in ("distribution", "tracer_flux") else None,
        )
        cached = self._cdr_plot_cache.get(key)
        if cached is None:
            import io

            import matplotlib.pyplot as plt

            plt.ioff()
            # None of plot_locations/plot_distribution/plot_tracer_flux call
            # plt.show() as of this roms-tools version, but neutralize it
            # anyway (cheap insurance against a future version that does --
            # see _on_nest_plot for why this matters under the inline backend).
            _real_show, plt.show = plt.show, lambda *a, **k: None
            try:
                if plot_type == "locations":
                    cdr.plot_locations()
                elif plot_type == "distribution":
                    if release_name is None:
                        raise ValueError("No release to plot a distribution for.")
                    cdr.plot_distribution(release_name)
                elif plot_type == "tracer_flux":
                    # A generated-tracer release plots its own tracer; a MARBL
                    # one (alone or beside generated ones) plots ALK.
                    tracer = (cdr.release_tracers.get(release_name) or ("ALK",))[0]
                    only = (
                        {}
                        if release_name is None
                        else {"release_names": [release_name]}
                    )
                    cdr.plot_tracer_flux(tracer, **only)
                else:
                    raise ValueError(f"Unknown CDR plot type {plot_type!r}.")
                fig = plt.gcf()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=80, bbox_inches="tight")
                plt.close(fig)
            finally:
                plt.show = _real_show
                plt.ion()
            cached = buf.getvalue()
            self._cdr_plot_cache[key] = cached
        self.cdr_plot_img.value = cached
        self.cdr_plot_img.layout.display = ""

    def _on_cdr_plot_generate(self, _btn):
        self.cdr_plot_status.value = "<i>building…</i>"
        try:
            self._build_cdr_forcing_for_plot()
            # Clear the building marker BEFORE the sync step: _sync may set a
            # volume-type ⚠ that must survive, and the ✓ below only fills an
            # empty status (leaving the marker in place would stick forever).
            self.cdr_plot_status.value = ""
            self._sync_cdr_plot_release_options()
            self._render_cdr_plot()
            if not self.cdr_plot_status.value:
                self.cdr_plot_status.value = components.status_span("ok", "✓")
        except Exception as exc:
            self._cdr_plot_object = None
            self._cdr_plot_grid = None
            self.cdr_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )

    def _on_cdr_plot_option_change(self, _change):
        if getattr(self, "_suspended", False):
            return
        if self._cdr_plot_object is None:
            return
        self._render_cdr_plot()

    def _populate_from(self, cfg: ForgeBlueprint):
        """Set the widgets from a loaded ForgeBlueprint, then re-resolve once.

        Round-trips the authoring inputs (name / description / run / domain /
        partitioning / nesting / dt). Any value in the file that differs from what the composed specs
        would produce is reconstructed as a manual override (so load is non-lossy and
        the overrides layer is rebuilt), then applied on top in ``_rebuild``.

        Returns any validation problems found in the *loaded file's* model_settings.
        """
        # Before any re-resolve below, so the first rebuilt config carries it.
        self._carried_provenance = cfg.provenance
        self._load_unknown_specs = []
        # The file's values are the user's: no wizard default goes over them.
        self._wizard_default_keys = set()
        # The file's own pinned ucla-roms ref selects the schema variant it was
        # authored against -- not the (possibly different) currently-selected
        # model's default. The Model card seeds its roms_ref from the same
        # _pinned_roms_ref, so the two never drift apart.
        loaded_problems = validate_run_time_sections(
            cfg.model_settings, roms_ref=_pinned_roms_ref(cfg) or None
        )
        with self._suspend():
            # domain dropdown -> custom (the file, not a catalog entry, is authoritative)
            self.domain_dd.value = "<custom>"
            self.model_card.select_model(cfg, self._load_unknown_specs)
            self.grid_name.value = cfg.domain.grid_name
            # A file's own description is kept, unless it is no more than a default:
            # none, the old placeholder, or the text the resolver derives for it
            # (saved untouched). Those follow the inputs again, like a new blueprint.
            self._description_touched = cfg.description.strip() not in {
                "",
                PLACEHOLDER_DESCRIPTION,
                default_blueprint_description(cfg),
            }
            self.description.value = (
                cfg.description if self._description_touched else ""
            )
            self.name.value = cfg.name
            self._name_touched = (
                True  # a loaded name is a deliberate choice, not a default
            )
            self.save_path.value = self._default_blueprint_path(cfg.name)
            self._save_path_touched = True
            self.start.value = cfg.run.start_date.date()
            self.end.value = cfg.run.end_date.date()
            self.model_ref_date.value = cfg.run.model_reference_date.date()
            self._loaded_run_times = {
                "start": cfg.run.start_date,
                "end": cfg.run.end_date,
                "model_reference_date": cfg.run.model_reference_date,
            }
            gk = cfg.domain.grid_kwargs
            self._set_loaded_grid_kwargs("grid", gk)
            for k, w in self.grid_w.items():
                if k in gk:
                    w.value = gk[k]
            self.hmin.value = float(gk.get("hmin", _GRID_KWARGS_LEFT_OUT["hmin"]))
            self.close_narrow_chk.value = bool(
                gk.get(
                    "close_narrow_channels",
                    _GRID_KWARGS_LEFT_OUT["close_narrow_channels"],
                )
            )
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
            self.model_card.populate(cfg)
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
            # CDR: a top-level, independently composable section (CdrSpec) -- see
            # ``self.cdr_mode_dd``/``_apply_cdr_mode``. FileUpload can't be
            # repopulated with the original file, but the parsed dict persists on
            # the instance and re-emits via _gather(), so load stays non-lossy.
            self.cdr_mode_dd.value = cfg.cdr.mode
            self._apply_cdr_mode()
            if cfg.cdr.mode == "simple":
                self._populate_cdr_simple(cfg.cdr.cdr_forcing or {})
            elif cfg.cdr.mode == "yaml":
                self._cdr_forcing = cfg.cdr.cdr_forcing or None
                self._clear_cdr_upload_value()
                self.cdr_status.value = (
                    components.status_span(
                        "ok",
                        f"✓ CDR loaded: "
                        f"{len(self._cdr_forcing.get('releases', []))} release(s)",
                    )
                    if self._cdr_forcing
                    else ""
                )
            elif cfg.cdr.mode == "netcdf":
                # Trust the stored hash (never recompute); a missing file is a
                # warning, not a blocker -- Save must still round-trip losslessly.
                cff = cfg.cdr.cdr_forcing_file
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
                        "<br>"
                        + components.status_span("err", "⚠ file not found at this path")
                        if missing
                        else ""
                    )
                else:
                    self._cdr_forcing_file = None
                    self.cdr_file_path.value = ""
                    self.cdr_file_status.value = _CDR_FILE_HINT
            # dt: prefer the first-class domain.dt field; fall back to the
            # model_settings leaf for a pre-domain.dt file (backward compat --
            # older blueprints only ever wrote it there).
            loaded_dt = cfg.domain.dt
            if loaded_dt is None:
                loaded_dt = (cfg.model_settings.get("time_stepping") or {}).get("dt")
            if loaded_dt is not None:
                self.dt.value = float(loaded_dt)
            self._dt_touched = loaded_dt is not None
            self._populate_nesting(cfg)
            # forcing: reconstruct the editor from the loaded sources. Forcing/output
            # dropdowns always need a valid catalog selection (no more "model_default"
            # fallback value); fall back to the first available option for an older
            # file recorded with origin="model_default" or an unknown/missing name.
            fname = cfg.composition.forcing.name
            _forcing_values = _dd_values(self.forcing_dd)
            if fname in _forcing_values:
                self.forcing_dd.value = fname
            elif _forcing_values:
                self.forcing_dd.value = _forcing_values[0]
                if fname:
                    self._load_unknown_specs.append(f"forcing '{fname}'")
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
            _output_values = _dd_values(self.output_dd)
            if oname in _output_values:
                self.output_dd.value = oname
            elif _output_values:
                self.output_dd.value = _output_values[0]
                if oname:
                    self._load_unknown_specs.append(f"output '{oname}'")
            # CDR spec selection: unlike forcing/output, "<custom>" (no catalog
            # entry) is a normal, common state (mode="none" is the default) --
            # fall back to "<custom>"/no seed rather than the first catalog
            # entry. The mode/fields above already reflect the loaded file's
            # actual CDR state; only the seed is (re)computed here, against the
            # catalog spec (not the just-loaded values) so a file that matches
            # its recorded CdrSpec loads as unmodified and a hand-edited one
            # loads as modified -- mirrors the forcing_seed block above.
            cname = cfg.composition.cdr.name
            _cdr_values = _dd_values(self.cdr_dd)
            if cname is not None and cname in _cdr_values:
                self.cdr_dd.value = cname
                try:
                    seed = self.catalog.cdr_data(cname)
                    self._cdr_seed = {
                        k: v for k, v in seed.items() if k != "description"
                    }
                except Exception:
                    self._cdr_seed = None
            else:
                self.cdr_dd.value = "<custom>"
                self._cdr_seed = None
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
    def _base_grid_kwargs(self) -> dict[str, Any]:
        """The main grid's generation kwargs, read from the grid widgets.

        ``{}`` while a user-provided grid file is attached: it forbids the
        generation-geometry keys alongside it
        (``Domain._grid_file_excludes_generation_geometry``), so ``grid_file=``/
        ``grid=`` carry the grid instead. Otherwise laid over the grid_kwargs last
        loaded into the widgets, so a loaded key with no widget survives a save.
        """
        gk: dict[str, Any] = {}
        if self._grid_file is not None:
            return gk
        for k in _GRID_INT:
            gk[k] = int(self.grid_w[k].value)
        for k in _GRID_FLOAT:
            gk[k] = float(self.grid_w[k].value)
        if self.scoord_chk.value:
            for k in _SCOORD:
                gk[k] = float(self.grid_w[k].value)
        # hmin + close_narrow_channels + mask_shapefile injected into grid_kwargs
        if self.hmin.value != _GRID_KWARGS_LEFT_OUT["hmin"]:
            gk["hmin"] = float(self.hmin.value)
        if self.close_narrow_chk.value:
            gk["close_narrow_channels"] = True
        if self.mask_shapefile.value.strip():
            gk["mask_shapefile"] = self.mask_shapefile.value.strip()
        return _merge_loaded_kwargs(
            self._loaded_grid_kwargs.get("grid", {}),
            gk,
            owned={*self.grid_w, *_GRID_KWARGS_LEFT_OUT},
            left_out=_GRID_KWARGS_LEFT_OUT,
        )

    def _nested_grid_kwargs(self, which: str) -> dict[str, Any]:
        """``which`` in ("parent", "child"): that grid's generation kwargs, read
        from its widgets and laid over the grid_kwargs last loaded into them. A
        parent's ``hmin`` or ``close_narrow_channels`` has no widget here, and the
        child is aligned to a parent rebuilt from exactly these kwargs.
        """
        boxes = getattr(self, f"{which}_w")
        gk: dict[str, Any] = {}
        for k in _GRID_INT:
            gk[k] = int(boxes[k].value)
        for k in _GRID_FLOAT + _SCOORD:
            gk[k] = float(boxes[k].value)
        gk.update(
            _nested_topo_kwargs(
                getattr(self, f"{which}_topo_source").value,
                getattr(self, f"{which}_topo_path").value,
            )
        )
        return _merge_loaded_kwargs(
            self._loaded_grid_kwargs.get(which, {}),
            gk,
            owned={*boxes, *_NESTED_TOPO_KEYS},
        )

    def _set_loaded_grid_kwargs(self, which: str, gk: dict[str, Any] | None) -> None:
        """Record ``gk`` as the grid_kwargs just loaded into ``which``'s widgets
        ("grid", "parent" or "child"); see :func:`_merge_loaded_kwargs`.
        """
        self._loaded_grid_kwargs[which] = copy.deepcopy(dict(gk or {}))

    def _attached_grid(self) -> Any:
        """The attached grid file's loaded ``roms_tools.Grid``, or ``None`` when no
        grid file is attached.
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
        return None

    def _run_datetime(self, key: str, day: date) -> datetime:
        """The datetime for run date picker ``key`` ("start", "end",
        "model_reference_date") showing ``day``: the time of day last loaded into
        it while it still shows the loaded date (the pickers hold whole days, and
        a nested run starts at 06:00), otherwise midnight.
        """
        loaded = self._loaded_run_times.get(key)
        if loaded is not None and loaded.date() == day:
            return loaded
        return datetime.combine(day, datetime.min.time())

    def _gather(self) -> dict[str, Any]:
        kw = dict(
            model_path=self.model_card.model_path(),
            grid_name=self.grid_name.value,
            grid_kwargs=self._base_grid_kwargs(),
            open_boundaries={d: w.value for d, w in self.bnd.items()},
            partitioning=(
                {"auto_tiling": True, "n_cores": int(self.n_cores.value)}
                if self.auto_tiling_chk.value
                else {
                    "n_procs_x": int(self.npx.value),
                    "n_procs_y": int(self.npy.value),
                }
            ),
            start_date=self._run_datetime("start", self.start.value),
            end_date=self._run_datetime("end", self.end.value),
            # Untouched (or cleared): None, so the resolver derives a fresh one-line
            # description from the inputs, as it does the name below.
            description=(
                (self.description.value.strip() or None)
                if self._description_touched
                else None
            ),
            # Untouched: always pass None so the resolver recomputes a fresh default
            # (self.name.value may still hold the *previous* rebuild's backfilled
            # default -- reusing it here would freeze it instead of tracking inputs).
            name=(self.name.value.strip() or None) if self._name_touched else None,
            # Untouched: None, so the resolver derives it from the grid (CFL).
            dt=float(self.dt.value) if self._dt_touched else None,
            # Untouched: pass None so the resolver derives a fresh default from
            # the current grid (see _rebuild, which mirrors it back into the
            # widget for live display); touched: the user's/loaded value wins.
            v_sponge=float(self.v_sponge.value) if self._v_sponge_touched else None,
        )
        if self.topo_path.value.strip():  # blank = derive default topography path
            kw["topography_path"] = self.topo_path.value.strip()
        kw["topography_source"] = self.topo_source.value
        kw["use_pio"] = self.use_pio_chk.value
        kw.update(self.model_card.gather())
        if self.model_ref_date.value:
            ref = self._run_datetime("model_reference_date", self.model_ref_date.value)
            if ref != datetime(2000, 1, 1):
                kw["model_reference_date"] = ref
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
        is_parent, is_child = self._nesting_role()
        if is_parent:
            kw["grid_kwargs_child"] = self._nested_grid_kwargs("child")
            kw["metadata_child"] = {"period": float(self.nest_period.value)}
            if self.nest_pressure_fluxes.value:
                kw["nesting_include_pressure_fluxes"] = True
        if is_child:
            kw["grid_kwargs_parent"] = self._nested_grid_kwargs("parent")
        # forcing/output are always required now (no more model-default fallback).
        if self._forcing_editor is None:
            raise RuntimeError("_gather called before the forcing editor was built")
        kw["forcing_inputs"] = self._forcing_editor.gather()
        if is_child:
            # Durable guarantee (authoritative, independent of forcing-editor UI
            # state): a child grid (has a parent) receives its boundary values
            # from the parent's nesting.nc extraction, not reanalysis boundary
            # forcing. Open-boundary edge flags are untouched -- the edges stay
            # open, just fed differently. `None` (not `[]`) is "no boundary
            # section at all" under the new singular BoundaryForcing schema.
            kw["forcing_inputs"]["forcing"]["boundary"] = None
        if self.model_card.bgc_mode != "marbl":
            # Only MARBL has an air CO2: the hidden "Atmospheric CO2" block keeps its
            # choice for a switch back, but a time-varying one reaches no blueprint.
            surface = kw["forcing_inputs"]["forcing"]["surface"]
            kw["forcing_inputs"]["forcing"]["surface"] = [
                item for item in surface if not _is_co2_item(item)
            ]
        kw["output_settings"] = self._output_settings()
        kw["composition"] = self._composition()
        kw["cdr"] = self._current_cdr_dict()
        kw["provenance"] = self._carried_provenance
        return kw

    def _populate_nesting(self, cfg: ForgeBlueprint):
        """Set the nesting widgets from a loaded config (called inside _suspend)."""
        child = cfg.domain.grid_kwargs_child
        self._set_loaded_grid_kwargs("child", child)
        self.nest_enable.value = child is not None
        if child:
            for k, w in self.child_w.items():
                if k in child:
                    w.value = child[k]
            self._set_nested_topo_widgets("child", child)
            # The period comes from metadata_child (generation writes the namelist's
            # extract_period from it), except an older file's Advanced
            # extract_period override, which that file ran with; the save then
            # records it in metadata_child.
            period = (cfg.composition.overrides.get("extract_data") or {}).get(
                "extract_period", cfg.domain.child_extract_period
            )
            if period is not None:
                self.nest_period.value = float(period)
        self.nest_pressure_fluxes.value = bool(
            cfg.domain.nesting_include_pressure_fluxes
        )
        parent = cfg.domain.grid_kwargs_parent
        self._set_loaded_grid_kwargs("parent", parent)
        self.parent_enable.value = parent is not None
        if parent:
            for k, w in self.parent_w.items():
                if k in parent:
                    w.value = parent[k]
            self._set_nested_topo_widgets("parent", parent)

    def _nesting_role(self) -> tuple[bool, bool]:
        """``(is_parent, is_child)`` for the grid being edited, as the nesting
        checkboxes request it. Nesting is unsupported alongside a user-provided grid
        file (see ``Domain._grid_file_excludes_generation_geometry``), so an attached
        file ignores both even if the (now-disabled) checkboxes are still checked.
        Read by ``_gather`` (what is resolved) and by the settings editor's key, so
        the Output card follows a nesting toggle even before the blueprint is valid.
        """
        if self._grid_file is not None:
            return False, False
        return bool(self.nest_enable.value), bool(self.parent_enable.value)

    def _show_editor(
        self,
        settings: dict[str, Any],
        key: _EditorKey,
        cfg: ForgeBlueprint | None = None,
    ) -> dict[str, Any]:
        """Show ``settings``, with the active overrides over them, in the settings
        editor for ``key``, building that editor first when the current one is for
        another key; return the effective settings shown. A resolved editor fills the
        Advanced panes, the Output card and the Model card; an unresolved one (see
        ``_show_unresolved_editor``) only the Model card, with a placeholder in the
        other two.

        ``cfg``, the blueprint ``settings`` were resolved for, is given only by a
        successful resolve: its settings become the baseline ``_on_editor_edit``
        compares edits with, and the resolver's switches for input the blueprint
        does not generate are forced off again over the overrides.
        """
        self._composed_settings = settings if cfg is not None else None
        if self.editor is None or self._editor_key != key:
            self.editor = _SettingsEditor(
                self.W,
                settings,
                on_edit=self._on_editor_edit,
                settings_cls=key.settings_cls,
                bgc_mode=key.bgc_mode,
                is_parent=key.is_parent,
                is_child=key.is_child,
            )
            self._editor_key = key
            if key.resolved:
                self.editor_box.children = [self.editor.accordion]
                self.output_editor_box.children = [self.editor.output_box]
            else:
                placeholder = components.banner_widget(
                    self.W,
                    "info",
                    "Settings for this selection appear once the blueprint is valid "
                    "— see the status above.",
                )
                self.editor_box.children = [placeholder]
                self.output_editor_box.children = [placeholder]
            self.model_card.show_settings(
                self.editor.model_box
                if _MODEL_CARD in self.editor._group_fields
                else None
            )
            self.co2_static_box.children = (
                [self.editor.forcing_box]
                if _FORCING_CARD in self.editor._group_fields
                else []
            )
        self._seed_wizard_defaults()

        effective = _apply_overrides(settings, self._active_overrides)
        if cfg is not None:
            force_switches_without_input_off(
                effective,
                is_child=cfg.domain.grid_kwargs_parent is not None,
                has_tidal_forcing=bool(cfg.forcing.tidal),
            )
        self._syncing = True
        try:
            self.editor.sync(effective)  # display effective; don't re-record as edits
        finally:
            self._syncing = False
        return effective

    def _show_unresolved_editor(self) -> None:
        """After a failed resolve, replace an editor built for another model, BGC
        mode, nesting role or ucla-roms tier with one built from the selected
        ModelSpec's Model and Forcing card settings alone (see
        :data:`_MODEL_CARD_FIELDS`, :data:`_FORCING_CARD_FIELDS`), so those cards
        keep their settings on an invalid blueprint: the carbonate
        sensitivity switch shows as soon as ``cdr_lite`` is picked, before a CDR
        forcing makes the blueprint valid. The Advanced panes and the Output card
        show a placeholder until the next valid resolve rebuilds the editor: the old
        one would keep showing streams that no longer apply, e.g. the MARBL ones
        after a switch to ``cdr_lite``, or no child-grid extraction after "This grid
        is a parent" is ticked. Overrides are applied after the resolve, never fed to
        it, so no failure comes from an editor value and nothing is lost; the user's
        record stays. An editor still matching the selection stays, resolved or not.
        """
        is_parent, is_child = self._nesting_role()
        key = _EditorKey(
            self.model_card.name,
            _wizard_settings_cls_for_ref(self.model_card.effective_roms_ref()),
            self.model_card.bgc_mode,
            is_parent,
            is_child,
            resolved=False,
        )
        if self._editor_key is not None and (
            replace(self._editor_key, resolved=False) == key
        ):
            return
        try:
            spec = self.catalog.model_data(self.model_card.name)["model_settings"]
        except Exception:  # an unreadable spec: the failed resolve reports it
            spec = {}
        self._show_editor(
            {
                section: {f: v for f, v in spec[section].items() if f in fields}
                for card in (_MODEL_CARD_FIELDS, _FORCING_CARD_FIELDS)
                for section, fields in card.items()
                if section in spec
            },
            key,
        )

    def _rebuild(self, *_):
        if getattr(self, "_suspended", False):
            return
        self._reseed_cdr_simple()
        # CDR plot cache: _rebuild() is the one hook that reliably fires for
        # every CDR-panel edit, every mode switch, and every grid-geometry edit
        # (see _invalidate_cdr_plot_cache's own docstring for why this spot was
        # chosen over a bespoke .observe on each CDR/grid widget).
        self._invalidate_cdr_plot_cache()
        self._sync_derived_dt()
        # Captured before self.config is reassigned below -- used to detect a
        # real rename (vs. an unrelated edit re-running _rebuild) for the
        # save_path filename re-sync further down.
        prev_name = self.config.name if self.config is not None else None
        self.preview.clear_output(wait=True)
        # self.validation reflects the *last successful* resolve; every early
        # return below (missing dates, a build/validation exception) leaves a
        # stale ✓/⚠ message sitting under self.derived's fresh error otherwise
        # -- clear it once up front so only the success path below re-sets it.
        self.validation.value = ""
        if self.start.value is None or self.end.value is None:
            self.config = None
            self.derived.value = "<i>Set start and end dates…</i>"
            self.download_link.value = ""
            self.download_link_review.value = ""
            self._show_unresolved_editor()
            self._update_status(
                None,
                problems=[_BlueprintProblem("run", "Set the start and end dates.")],
            )
            return
        try:
            cfg = build_forge_blueprint(**self._gather())
        except Exception as exc:  # validation or input error → show, don't crash
            self.config = None
            self.download_link.value = ""
            self.download_link_review.value = ""
            with self.preview:
                print(f"{type(exc).__name__}: {exc}")
            problems = _blueprint_problems(exc)
            self.derived.value = components.banner(
                "err",
                f"<div><b>Invalid blueprint.</b> {_problem_summary(problems)}</div>",
            )
            self._show_unresolved_editor()
            self._update_status(None, problems=problems)
            return

        # v_sponge is cheap (pure arithmetic on grid spacing, no grid build) --
        # keep it live-derived in the widget whenever untouched, by reading back
        # what the resolver just computed. Under _suspend() so this programmatic
        # write doesn't itself flip _v_sponge_touched (see _on_v_sponge_edit).
        if not self._v_sponge_touched:
            with self._suspend():
                self._show_derived_v_sponge(float(cfg.domain.v_sponge))

        # Advanced/Output settings editor: every section is editable. The resolver
        # composes a baseline from the specs; the user's manual edits are a sparse
        # overrides layer applied on top (effective = composed ⊕ overrides). The
        # editor is rebuilt when the *model* changes (its field set depends on the
        # model), when the BGC mode or the grid's nesting role changes (which output
        # streams apply), or when the effective ucla-roms ref crosses a schema
        # boundary (e.g. editing the roms_ref override across the 0.4.0, 0.5.0, 0.6.0,
        # or 0.7.0 line with the same model selected) -- see
        # run_time_settings_for_ref / RunTimeSettingsV0_4_0 / RunTimeSettingsV0_5_0 /
        # RunTimeSettingsV0_6_0 / RunTimeSettingsV0_7_0.
        composed = cfg.model_settings
        # Computed once per rebuild (each call re-reads the ModelSpec YAML) and
        # reused for the validation call below.
        effective_roms_ref = self.model_card.effective_roms_ref()
        settings_cls = _wizard_settings_cls_for_ref(effective_roms_ref)
        is_parent, is_child = self._nesting_role()
        key = _EditorKey(
            self.model_card.name,
            settings_cls,
            self.model_card.bgc_mode,
            is_parent,
            is_child,
            resolved=True,
        )
        effective = self._show_editor(composed, key, cfg)
        active_overrides = self._active_overrides

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
        spec_deviation = self.model_card.deviates_from_spec()
        model_modified = (
            any(not _is_output_key(s, f) for s, f in deviations) or spec_deviation
        )
        output_modified = any(_is_output_key(s, f) for s, f in deviations)
        domain_modified = (
            self.domain_dd.value != "<custom>"
            and self._domain_seed is not None
            and self._domain_snapshot() != self._domain_seed
        )
        forcing_modified = self._forcing_modified()
        cdr_modified = (
            self.cdr_dd.value != "<custom>"
            and self._cdr_seed is not None
            and self._cdr_snapshot() != self._cdr_seed
        )

        comp = cfg.composition.model_copy(
            update={
                "overrides": _overrides_nested(active_overrides),
                "model": cfg.composition.model.model_copy(
                    update={"modified": model_modified}
                ),
                "domain": cfg.composition.domain.model_copy(
                    update={"modified": domain_modified}
                ),
                "forcing": cfg.composition.forcing.model_copy(
                    update={"modified": forcing_modified}
                ),
                "cdr": cfg.composition.cdr.model_copy(
                    update={"modified": cdr_modified}
                ),
                "output": cfg.composition.output.model_copy(
                    update={"modified": output_modified}
                ),
            }
        )
        cfg = cfg.model_copy(update={"model_settings": effective, "composition": comp})

        self.config = cfg
        # A validly-resolving config means whatever state (fixed dropdowns, a
        # corrected field, ...) got us here has superseded any earlier failed
        # Load -- don't leave that stale red error sitting around forever. But
        # load_status also carries a load *success* message (_set_load_status,
        # possibly with an amber "N invalid settings value(s)" warning found
        # nowhere else in the UI) -- only clear it when it's currently holding
        # an error, never a success message.
        if self._load_status_is_error:
            self.load_status.value = ""
            self._load_status_is_error = False
        # Backfill the Export name/description/save-path fields with the current
        # derived defaults until the user edits one -- their edit "locks in" that
        # field (see _on_name_change/_on_description_change/_on_save_path_change).
        # Under _suspend() so these programmatic writes don't themselves flip the
        # touched flags or recurse.
        if (
            not self._name_touched
            or not self._description_touched
            or not self._save_path_touched
        ):
            with self._suspend():
                if not self._name_touched:
                    self.name.value = cfg.name
                if not self._description_touched:
                    self.description.value = cfg.description
                if not self._save_path_touched:
                    self.save_path.value = self._default_blueprint_path(cfg.name)
        # Independent of the backfill above (and of _name_touched): the derived
        # name can also change from a grid/procs/model edit with Name left on
        # auto-derive but Save-to customized to a specific directory -- that
        # combination skips the backfill (save_path IS touched) entirely, so
        # without this the filename would never re-sync. Re-sync only on a
        # genuine rename, and only when the current filename still exactly
        # tracks the *previous* derived name (i.e. the user hasn't customized
        # the filename itself) -- preserves the user's directory and any
        # deliberately-custom filename. Under _suspend() so this doesn't
        # itself flip _save_path_touched or recurse.
        if (
            self._save_path_touched
            and prev_name is not None
            and cfg.name != prev_name
            and Path(self.save_path.value).name == f"{prev_name}.yaml"
        ):
            with self._suspend():
                self.save_path.value = str(
                    Path(self.save_path.value).parent / f"{cfg.name}.yaml"
                )
        self._refresh_download_links(cfg)
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
            self.derive_status.value = components.status_span(
                "warn", "⚠ not derived yet"
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
            self.validation.value = components.banner(
                "err",
                "<b>Settings validation:</b><br>"
                + "<br>".join(f"&nbsp;&nbsp;{p}" for p in problems[:10]),
            )
        else:
            self.validation.value = components.banner(
                "ok", "<b>Blueprint is valid.</b> All settings pass validation."
            )
        # What the run would use that nothing refuses but its author likely did not
        # mean (zero BGC inputs on an older ucla-roms). The resolver only logs these,
        # which a widget callback never shows, so show them here.
        if notes := resolver_warnings(cfg, roms_ref=effective_roms_ref):
            self.validation.value += components.banner(
                "warn",
                "<br>".join(f"<b>Warning:</b> {html.escape(note)}" for note in notes),
            )
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
        self._update_status(cfg)

    def _forcing_summary(self, cat: str) -> tuple[components.Pill, ...]:
        """The current state of forcing category ``cat`` as pills for its pane
        header (see `_update_status`): the source names, with a default path or an
        empty category muted. A Biogeochemistry category (``ic_bgc``,
        ``boundary_bgc``) adds its sources to its pane's header, none while the
        section is hidden.
        """
        fe = self._forcing_editor
        if fe is None:
            return ()
        none = (components.Pill("none", "off"),)
        if cat in ("ic_bgc", "boundary_bgc"):
            if not fe.bgc_shown(cat):
                return ()
            names = [row["name"].value for row in fe._rows[cat]]
            bgc: tuple[components.Pill, ...] = (
                components.Pill("BGC " + " + ".join(names))
                if names
                else components.Pill("no BGC", "off"),
            )
            if fe.bgc_needs_fix(cat):
                bgc += (components.Pill("check BGC sources", "warn"),)
            return bgc
        if cat in ("initial_conditions", "boundary"):
            is_ic = cat == "initial_conditions"
            name_w = fe.ic_name if is_ic else fe.boundary_name
            path_w = fe.ic_path if is_ic else fe.boundary_path
            none_val = _IC_NONE if is_ic else _BOUNDARY_NONE
            if name_w.value == none_val:
                return none
            if path_w.value.strip():
                return (components.Pill(name_w.value), components.Pill("custom path"))
            return (
                components.Pill(name_w.value),
                components.Pill("default path", "off"),
            )
        pills = [components.Pill(row["name"].value) for row in fe._rows.get(cat, [])]
        if cat == "surface" and self.model_card.bgc_mode == "marbl":
            pills.append(components.Pill(f"CO2 {fe.co2_mode.value}"))
        return tuple(pills) or none

    def _retitle_settings_editor(self) -> None:
        """Refresh each settings container's titles with its field count and how many
        of its fields carry a manual override: the Advanced panes' and "More output
        streams" accordion titles, and the Output card's section headers (see
        `_SettingsEditor._group_fields`).
        """
        editor = self.editor
        if editor is None:
            return

        active = self._active_overrides

        def counts(fields_by_section: dict[str, list[str | None]]) -> tuple[int, int]:
            keys = {(s, f) for s, fields in fields_by_section.items() for f in fields}
            return (
                sum(len(fields) for fields in fields_by_section.values()),
                sum(1 for k in active if k in keys),
            )

        def title(
            name: str, fields_by_section: dict[str, list[str | None]]
        ) -> components.PaneTitle:
            n_fields, n_mod = counts(fields_by_section)
            return components.accordion_title(
                name,
                f"{n_fields} settings",
                components.Pill(f"{n_mod} modified", "mod")
                if n_mod
                else components.Pill("all defaults", "off"),
            )

        for i, name in enumerate(editor._pane_sections):
            editor.accordion.set_title(i, title(name, editor._group_fields[name]))
        if more := editor._group_fields.get(_OUTPUT_MORE_TITLE):
            editor.more_streams.set_title(0, title(_OUTPUT_MORE_TITLE, more))
        # Per header, so a hosted field's own section (no header) is counted under
        # its host only.
        for (group, section), header in editor._headers.items():
            if group != _OUTPUT_CARD:
                continue
            _n_fields, n_mod = counts(
                {
                    section: editor._group_fields[group][section],
                    **editor._hosted.get((group, section), {}),
                }
            )
            header.value = editor.header_html(group, section, n_mod)

    def _update_status(
        self,
        cfg: ForgeBlueprint | None,
        *,
        problems: Sequence[_BlueprintProblem] = (),
    ) -> None:
        """Refresh the sticky bar, the per-card status chips and error banners, and
        the forcing/advanced/output titles from the outcome of a `_rebuild` call.

        Called at the end of every `_rebuild` path: on success with the resolved
        ``cfg``; on missing dates or a build/validation failure with ``cfg=None``
        and the ``problems`` that stop the blueprint resolving. Only the card that
        owns a problem shows it, with a problem chip and a banner atop its body;
        every other card keeps its own state.
        """
        valid = cfg is not None
        model_name = self.model_card.name or "—"
        step_links = "".join(
            f"<a class='step' href='#forge-sec-{key}'>{i} {title}</a>"
            for i, (key, title) in enumerate(_STICKY_STEPS, start=1)
        )
        if cfg is not None:
            domain_label = cfg.composition.domain.name or "custom"
            summary = (
                f"Model <b>{model_name}</b> &middot; "
                f"Domain <b>{domain_label}</b> ({cfg.composition.domain.origin}) "
                f"&middot; Run <b>{_run_time_label(cfg.run.start_date)} &rarr; "
                f"{_run_time_label(cfg.run.end_date)}</b>"
            )
            status_chip = components.chip("● Valid", "ok")
        else:
            summary = f"Model <b>{model_name}</b>"
            status_chip = components.chip("● Invalid", "warn")
            if problems:
                # Jumps to the card that owns the (first) problem.
                status_chip = (
                    f"<a class='forge-status-link' "
                    f"href='#forge-sec-{problems[0].card}'>{status_chip}</a>"
                )
        # The summary fills the rest of its line (see WIZARD_CSS), so the status
        # chip sits at the row's right edge, next to the download button.
        self.sticky_bar.value = (
            f"{step_links}<span class='summary'>{summary}</span>{status_chip}"
        )

        for key, banner in self.card_errors.items():
            texts = [p.text for p in problems if p.card == key]
            banner.layout.display = "" if texts else "none"
            banner.value = (
                components.banner(
                    "err",
                    # One block: the banner lays out its children side by side.
                    "<div><b>Fix this to make the blueprint valid.</b><br>"
                    + "<br>".join(
                        _problem_html(
                            text,
                            _PROBLEM_CHARS,
                            " <a href='#forge-sec-review'>Full message in the "
                            "Review preview</a>",
                        )
                        for text in texts
                    )
                    + "</div>",
                )
                if texts
                else ""
            )

        chips = self.card_chips
        # A card that owns a problem says so in place of its usual state.
        problem_chips = {
            key: components.chip(f"● {n} problem{'' if n == 1 else 's'}", "warn")
            for key, n in Counter(p.card for p in problems).items()
        }
        complete = components.chip("● Complete", "ok")
        chips["model"].value = problem_chips.get("model", self.model_card.status_chip())
        chips["run"].value = problem_chips.get("run", complete)
        chips["review"].value = (
            components.chip("● Ready", "ok")
            if valid
            else components.chip("● Invalid", "warn")
        )
        if not self._boundaries_derived and not self._boundaries_touched:
            n = 1 + (1 if not self._v_sponge_touched else 0)
            grid_chip = components.chip(f"● {n} fields to check", "warn")
        else:
            grid_chip = complete
        chips["grid"].value = problem_chips.get("grid", grid_chip)
        # Placement, not OutputSpec ownership: the two chips partition the overrides
        # the Model and Forcing cards do not show; the Forcing card's (its static
        # CO2) count as a forcing change.
        active = self._active_overrides
        n_output = sum(1 for key in active if _in_output_card(*key))
        n_model = sum(1 for key in active if _in_model_card(*key))
        n_forcing = sum(1 for key in active if _in_forcing_card(*key))
        n_advanced = len(active) - n_output - n_model - n_forcing
        # From the editor itself when the build failed, which has no composition.
        forcing_modified = (
            cfg.composition.forcing.modified
            if cfg is not None
            else self._forcing_modified()
        ) or bool(n_forcing)
        chips["forcing"].value = problem_chips.get(
            "forcing",
            components.chip("● Modified", "info")
            if forcing_modified
            else components.chip("At preset defaults", "def"),
        )
        chips["output"].value = problem_chips.get(
            "output",
            components.chip(f"● {n_output} modified", "info")
            if n_output
            else components.chip("At preset defaults", "def"),
        )
        chips["advanced"].value = problem_chips.get(
            "advanced",
            components.chip(f"● {n_advanced} modified", "info")
            if n_advanced
            else components.chip("All defaults", "def"),
        )

        preset_name = (
            getattr(self.forcing_dd, "label", None) or self.forcing_dd.value or "—"
        )
        self.forcing_banner.value = components.banner(
            "info",
            f"Defaults come from the forcing preset <b>{preset_name}</b>. Expand "
            "a section only to override its source or options; collapsed "
            "sections show their current values.",
        )
        if self._forcing_editor is not None:
            self._forcing_editor.refresh_bgc_notes()
            self._forcing_editor.retitle(self._forcing_summary)
        self._retitle_settings_editor()
        if self.editor is not None:
            self.editor.set_run_window(self._run_window_seconds())
        self._sync_next_steps()

    def _run_window_seconds(self) -> float | None:
        """The run window's length in seconds, or ``None`` while it is unset or
        empty.
        """
        if self.start.value is None or self.end.value is None:
            return None
        seconds = (
            self._run_datetime("end", self.end.value)
            - self._run_datetime("start", self.start.value)
        ).total_seconds()
        return seconds if seconds > 0 else None

    def _refresh_download_links(self, cfg: ForgeBlueprint) -> None:
        """Point both download links at ``cfg``, stamped once so they agree.

        A download is a copy for the user, not a save: a stamp minted here is not
        carried (that is `_save_config`'s job). A ``cfg`` that is already stamped,
        as the config just saved is, comes back from ``stamp_provenance`` as it is,
        so its links hand out exactly the file that was written.

        While the open boundaries are the provisional checkbox defaults (neither
        touched nor derived from the grid), the links are withheld: Save and Run
        derive them first (`_ensure_boundaries_derived`), but a link has no click
        hook to do that, and building a grid on every rebuild would stall typing.
        """
        if not self._boundaries_touched and not self._boundaries_derived:
            self.download_link.value = (
                f"<span class='forge-dl-off {components.status_class('warn')}' "
                f'title="{html.escape(_DOWNLOAD_WAITS_FOR_BOUNDARIES)}">'
                "⬇ Derive boundaries to download</span>"
            )
            self.download_link_review.value = components.status_span(
                "warn",
                f"⚠ {html.escape(_DOWNLOAD_WAITS_FOR_BOUNDARIES)}",
            )
            return
        download_cfg = cfg.stamp_provenance(WIZARD_TOOL)
        self.download_link.value = self._download_html(
            download_cfg, caption="Download blueprint"
        )
        self.download_link_review.value = self._download_html(download_cfg)

    @staticmethod
    def _download_html(cfg: ForgeBlueprint, caption: str | None = None) -> str:
        """A data-URI download link for the resolved YAML — works in the browser
        (Voilà / JupyterLab) with no server-side file access.

        ``caption`` overrides the link text with a fixed, short caption (the
        filename moves into ``title=`` instead) -- used by the sticky bar,
        which has no room for a full filename. The default keeps the Review
        card's "Download <code>fname</code>" copy.
        """
        payload = cfg.to_yaml_str().encode("utf-8")
        b64 = base64.b64encode(payload).decode("ascii")
        fname = f"{cfg.name}.forge_blueprint.yaml"
        if caption is None:
            text, title_attr = f"Download <code>{fname}</code>", ""
        else:
            text, title_attr = caption, f' title="{fname}"'
        return (
            f'⬇ <a class="forge-dl-btn" download="{fname}"{title_attr} '
            f'href="data:text/yaml;base64,{b64}">'
            f"{text}</a>"
        )

    # ---- actions -------------------------------------------------------------
    def _on_compute_dt(self, _):
        # Back to the derived dt (_rebuild shows it), which then follows grid
        # edits again.
        self._dt_touched = False
        self._rebuild()

    def _build_grid_from_widgets(self) -> Any:
        """Build a ``roms_tools.Grid`` from the current (main-domain) grid kwargs
        -- or return the already-loaded grid file's grid, when one is attached.

        Shared by the plot button, "Derive from grid", and the export-time
        safety net -- the only three places that pay the cost of an actual
        grid build (the live preview / ``_rebuild`` never does, so typing in
        the grid fields stays instant).
        """
        attached = self._attached_grid()
        if attached is not None:
            return attached

        from roms_tools import Grid

        # A loaded key with no widget is kept for the save (_merge_loaded_kwargs),
        # but Forge's own keys (a nesting ``metadata`` block) are not Grid's.
        gk = grid_build_kwargs(self._base_grid_kwargs())
        topo, self._grid_build_topo_note = self._this_grid_plot_topography()
        if topo is not None:
            gk["topography_source"] = topo
        return Grid(**gk)

    def _this_grid_plot_topography(self) -> tuple[dict[str, str] | None, str]:
        """This grid's ``topography_source`` for a wizard-side build, or the
        ETOPO5 fallback + note when the file isn't available here.
        """
        return _plot_topography_source(self.topo_source.value, self.topo_path.value)

    def _nested_plot_topography(self, which: str) -> tuple[dict[str, str] | None, str]:
        """``which`` in ("parent", "child"): that grid's effective topography
        (its own widgets, or this grid's when set to inherit) for a wizard-side
        build, with the same local-availability fallback.
        """
        name, path = _effective_nested_topo(
            getattr(self, f"{which}_topo_source").value,
            getattr(self, f"{which}_topo_path").value,
            self.topo_source.value,
            self.topo_path.value,
        )
        topo, note = _plot_topography_source(name, path)
        return topo, note.replace("(plotted", f"({which} grid plotted", 1)

    def _apply_grid_derived_properties(self, grid: Any) -> None:
        """Set any untouched v_sponge/open-boundary values from a built grid.

        Boundaries come from roms-tools' own ``check_and_set_boundaries`` (the
        same logic ``rt.Grid``/downstream tools use to infer active edges from
        the land mask); v_sponge from the existing grid-spacing formula. Only
        overwrites properties the user hasn't touched -- see the module-level
        touched/derived state-machine notes on ``_v_sponge_touched`` etc.
        """
        from roms_tools.setup.utils import check_and_set_boundaries

        from cstar.applications.forge.util import compute_v_sponge_from_grid

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
                self._show_derived_v_sponge(
                    compute_v_sponge_from_grid(grid.size_x, grid.nx)
                )
        self._boundaries_derived = True

    def _on_derive_domain_properties(self, _):
        """Handle the "Derive from grid" button: build the grid once and
        refresh any untouched v_sponge/open-boundary values from it.
        """
        self.derive_status.value = "<i>building grid…</i>"
        try:
            grid = self._build_grid_from_widgets()
            self._apply_grid_derived_properties(grid)
            self.derive_status.value = components.status_span(
                "ok", "✓ derived from grid"
            )
        except Exception as exc:
            self.derive_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
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
        design explicitly avoids. ``_refresh_download_links`` withholds it
        instead until the boundaries are touched or derived.
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
            self.plot_img.layout.display = ""
            self.plot_status.value = components.status_span("ok", "✓") + getattr(
                self, "_grid_build_topo_note", ""
            )
        except Exception as exc:
            self.plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
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

            # This grid as Derive and the grid plot build it: an attached grid
            # file, or the kwargs a save writes with its topography resolved for
            # this machine.
            this_grid = self._build_grid_from_widgets()
            note_self = (
                "" if self._grid_file is not None else self._grid_build_topo_note
            )

            ck = {
                k: v
                for k, v in grid_build_kwargs(self._nested_grid_kwargs("child")).items()
                if k not in _NESTED_TOPO_KEYS
            }

            ck_topo, note_child = self._nested_plot_topography("child")
            if ck_topo is not None:
                ck["topography_source"] = ck_topo

            plt.ioff()
            try:
                parent = this_grid
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
            self.nest_plot_img.layout.display = ""
            self.nest_plot_status.value = (
                components.status_span("ok", "✓") + note_self + note_child
            )
        except Exception as exc:
            self.nest_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
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

            pk = {
                k: v
                for k, v in grid_build_kwargs(
                    self._nested_grid_kwargs("parent")
                ).items()
                if k not in _NESTED_TOPO_KEYS
            }

            # Each grid is built with ITS topography where the file is available
            # on this machine (mirrors the executor's per-grid resolution), so a
            # parent that falls outside its dataset's coverage fails here, in the
            # preview, instead of only at executor time.
            pk_topo, note_parent = self._nested_plot_topography("parent")
            if pk_topo is not None:
                pk["topography_source"] = pk_topo

            plt.ioff()
            try:
                parent = Grid(**pk)
                # This grid as Derive and the grid plot build it: an attached
                # grid file, or the kwargs a save writes with its topography
                # resolved for this machine.
                this_grid = self._build_grid_from_widgets()
                note_self = (
                    "" if self._grid_file is not None else self._grid_build_topo_note
                )
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
            self.parent_plot_img.layout.display = ""
            self.parent_plot_status.value = (
                components.status_span("ok", "✓") + note_parent + note_self
            )
        except Exception as exc:
            self.parent_plot_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )

    def _save_config(self, path: Path) -> Path:
        """Write the current config to ``path``, stamped as written by the wizard,
        and return the path.

        The stamp becomes the carried provenance, so a later save or rebuild keeps
        it while the content is unchanged (see ``ForgeBlueprint.stamp_provenance``),
        and the download links are pointed at the file written. Every write of the
        current config to disk goes through here.
        """
        if self.config is None:
            raise RuntimeError("no valid configuration to save")
        stamped = self.config.stamp_provenance(WIZARD_TOOL)
        written = stamped.to_yaml(path)
        self._carried_provenance = stamped.provenance
        self.config = stamped
        self._refresh_download_links(stamped)
        self._saved_blueprint = _SavedBlueprint(
            Path(path), Path(written).expanduser().resolve(), stamped.to_yaml_str()
        )
        self._sync_next_steps()
        return written

    def _saved_path(self) -> Path | None:
        """The file the current config was last saved to, or ``None`` when there is
        no such file: nothing saved yet, the config edited since, or the Save path
        changed. Compared in memory (this runs on every rebuild), so a file deleted
        or edited on disk is not noticed here.
        """
        saved = self._saved_blueprint
        if (
            self.config is None
            or saved is None
            or Path(self.save_path.value) != saved.requested
            or self.config.to_yaml_str() != saved.text
        ):
            return None
        return saved.path

    # ---- next steps ------------------------------------------------------------
    def set_workplan_hooks(self, hooks: WorkplanHooks | None) -> None:
        """Connect this page to a Workplan page (or disconnect it with ``None``).

        Without hooks only "Save workplan" shows: there is no page to open.
        """
        self._workplan_hooks = hooks
        self.open_workplan_btn.layout.display = "" if hooks else "none"
        self._sync_next_steps()

    def _sync_next_steps(self) -> None:
        """Make card 8 follow whether the blueprint above is saved and current."""
        path = self._saved_path()
        ready = path is not None
        saved = self._saved_blueprint
        if path is not None:
            note = f"Saved to <code>{html.escape(str(path))}</code>."
        elif saved is not None and self.config is not None:
            note = (
                f"{_SAVE_FIRST} It has changed since you saved it to "
                f"<code>{html.escape(str(saved.path))}</code>."
            )
        else:
            note = _SAVE_FIRST
        self.next_status.value = f"<span class='forge-hint'>{note}</span>"
        self.save_workplan_btn.disabled = self.open_workplan_btn.disabled = not ready
        self.run_btn.disabled = self._run_in_progress or not ready
        if not ready:
            self.workplan_download.value = ""
            self._reset_workplan_confirm()

    def _workplan_msg(self, kind: str, message: str) -> None:
        self.workplan_status.value = f"<span class='forge-msg-{kind}'>{message}</span>"

    def _reset_workplan_confirm(self) -> None:
        """Disarm the overwrite confirm: cancel its timer, restore the button."""
        if self._workplan_confirm_timer is not None:
            self._workplan_confirm_timer.cancel()
        self._workplan_confirm = self._workplan_confirm_timer = None
        self.save_workplan_btn.description = "Save workplan"
        self.save_workplan_btn.button_style = "primary"

    def _on_save_workplan(self, _btn: Any) -> None:
        """Write the forge-then-run workplan for the saved blueprint to the catalog's
        ``workplans/`` (beside the blueprint when there is no writable catalog), and
        offer it for download. Replacing a different file takes a second click.
        """
        from cstar.wizard.workplan_builder import (
            WorkplanBuilderPage,
            default_workplan_path,
            forge_then_run_workplan,
            yaml_text,
        )

        path = self._saved_path()
        if path is None or self.config is None:
            self._workplan_msg("err", _SAVE_FIRST)
            return
        if not path.exists():
            self._workplan_msg(
                "err",
                f"The saved blueprint <code>{html.escape(str(path))}</code> is "
                "gone; save it again above.",
            )
            return
        try:
            workplan = forge_then_run_workplan(path, self.config.name)
        except ValueError as exc:
            self._workplan_msg("err", html.escape(str(exc)))
            return
        text = yaml_text(workplan)
        target = default_workplan_path(self.catalog, workplan.name, path.parent)
        shown = html.escape(str(target))
        if target.exists() and self._workplan_confirm != target:
            try:
                differs = target.read_text() != text
            except OSError:
                differs = True
            if differs:
                self._reset_workplan_confirm()
                self._workplan_confirm = target
                self.save_workplan_btn.description = "Confirm overwrite"
                self.save_workplan_btn.button_style = "danger"
                self._workplan_confirm_timer = schedule_confirm_reset(
                    self._reset_workplan_confirm
                )
                self._workplan_msg(
                    "warn",
                    f"A different workplan is already saved at <code>{shown}</code>. "
                    "Click Confirm overwrite to replace it.",
                )
                return
        self._reset_workplan_confirm()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        except OSError as exc:
            self._workplan_msg("err", html.escape(f"{type(exc).__name__}: {exc}"))
            return
        self._workplan_msg(
            "ok",
            f"Saved <code>{shown}</code>. Submit it with <code>cstar workplan submit "
            f"{shown}</code>, or open it in the Workplan builder to add compute "
            "settings.",
        )
        self.workplan_download.value = WorkplanBuilderPage._download_html(
            workplan, text
        )
        if self._workplan_hooks is not None:
            self._workplan_hooks.saved()

    def _on_open_workplan(self, _btn: Any) -> None:
        """Add the forge and run steps for the saved blueprint to the Workplan page."""
        hooks = self._workplan_hooks
        path = self._saved_path()
        if hooks is None or path is None or self.config is None:
            self._workplan_msg("err", _SAVE_FIRST)
            return
        try:
            hooks.open_steps(path, self.config.name)
        except ValueError as exc:
            self._workplan_msg("err", html.escape(str(exc)))
            return
        self._workplan_msg(
            "ok", "Opened the Workplan page with the forge and run steps."
        )

    def _on_save(self, _):
        if not self._ensure_boundaries_derived():
            self.save_status.value = components.status_span(
                "err",
                "Save aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). A save must never ship provisional defaults; fix "
                "the grid or set boundaries manually, then retry.",
            )
            return
        if self.config is None:
            self.save_status.value = components.status_span(
                "err", "Nothing to save — config is invalid."
            )
            return
        try:
            save_path = Path(self.save_path.value)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            p = self._save_config(save_path)
            self.save_status.value = components.status_span("ok", f"Saved {p}")
        except Exception as exc:
            self.save_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
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
            self.save_output_status.value = components.status_span(
                "err", "Invalid name."
            )
            return
        if self.config is None:
            self.save_output_status.value = components.status_span(
                "err", "Config invalid — nothing to save."
            )
            return
        try:
            self.catalog.register_output(
                name,
                extract_output_settings(self.config.model_settings),
                description=self._spec_description(),
            )
        except FileExistsError as exc:
            # str(exc) names the owning layer (e.g. "...already exists in the
            # 'bundled' catalog layer") -- a plain single-line message, no
            # traceback, so it's rendered verbatim rather than genericized.
            self.save_output_status.value = components.status_span("err", f"{exc}")
            return
        except Exception as exc:
            self.save_output_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        ok = self._verify_spec_roundtrip("output", name)
        with self._suspend():
            # ipywidgets resets .value to the first option on a bare `.options =`
            # reassignment even when the old value is still present -- restore it
            # explicitly so a mismatch genuinely leaves the selection untouched.
            old_value = self.output_dd.value
            self.output_dd.options = _dd_options(
                self.catalog, list(self.catalog.output_names), "output"
            )
            self.output_dd.value = name if ok else old_value
            if ok:
                # The saved OutputSpec carries the active output overrides; one a
                # mode switch hides was not saved, so it stays on the record.
                active = self._active_overrides
                self._overrides = {
                    k: v
                    for k, v in self._overrides.items()
                    if k not in active or not _is_output_key(*k)
                }
        if ok:
            self._rebuild()
            self.save_output_status.value = components.status_span(
                "ok", f"Saved '{name}' ✓ (now unmodified)."
            )
        else:
            self.save_output_status.value = components.status_span(
                "warn",
                f"Saved '{name}', but round-trip differs — spec kept as modified.",
            )

    def _on_save_model(self, _):
        name = self.save_model_name.value.strip()
        if not _valid_spec_name(name):
            self.save_model_status.value = components.status_span(
                "err", "Invalid name."
            )
            return
        if self.config is None:
            self.save_model_status.value = components.status_span(
                "err", "Config invalid — nothing to save."
            )
            return
        try:
            base_path = self.model_card.model_path()
            owned = _model_owned_settings(self.config.model_settings)
            base_settings = load_model_spec_data(base_path)["model"].get(
                "model_settings", {}
            )
            if not self.config.model_settings["cppdefs"].get("marbl", False):
                # The resolver zeroes ntrc_bio for a run without MARBL; keep the
                # base model's MARBL tracer count so the saved spec still works
                # when bgc_mode is switched back to "marbl".
                base_param = base_settings.get("param") or {}
                if "ntrc_bio" in base_param:
                    owned.setdefault("param", {})["ntrc_bio"] = base_param["ntrc_bio"]
            # The spec cannot carry a wizard-only setting (_WIZARD_SETTING_DEFAULTS),
            # whoever set it: save the base value, which validates in every BGC
            # mode, and keep the session's value as an override (below).
            for section, field in _WIZARD_SETTING_DEFAULTS:
                if field in (base_section := base_settings.get(section) or {}):
                    owned.setdefault(section, {})[field] = base_section[field]
                else:
                    owned.get(section, {}).pop(field, None)
            self.catalog.register_model_from_settings(
                name,
                owned,
                base_path,
                description=self._spec_description(),
                # Live widget values, using _gather()'s exact conventions (use_pio/
                # bgc_mode unconditional, roms_ref/marbl_ref only when non-blank) --
                # the round-trip verifier below compares against _gather()'s kw, so
                # any divergence here would be a spurious mismatch.
                bgc_mode=self.model_card.bgc_mode,
                use_pio=self.use_pio_chk.value,
                roms_ref=self.model_card.typed_roms_ref or None,
                marbl_ref=self.model_card.typed_marbl_ref or None,
            )
        except FileExistsError as exc:
            self.save_model_status.value = components.status_span("err", f"{exc}")
            return
        except Exception as exc:
            self.save_model_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        ok = self._verify_spec_roundtrip("model", name)
        with self._suspend():
            self.model_card.reselect(name if ok else self.model_card.name)
            if ok:
                active = self._active_overrides
                self._overrides = {
                    k: v
                    for k, v in self._overrides.items()
                    if k not in active
                    or _is_output_key(*k)
                    or k in _WIZARD_SETTING_DEFAULTS
                }
        if ok:
            self._rebuild()
            # A wizard-only setting held over the saved spec leaves it modified.
            modified = self.config is None or self.config.composition.model.modified
            note = "" if modified else " (now unmodified)"
            self.save_model_status.value = components.status_span(
                "ok", f"Saved '{name}' ✓{note}."
            )
        else:
            self.save_model_status.value = components.status_span(
                "warn",
                f"Saved '{name}', but round-trip differs — spec kept as modified.",
            )

    def _on_save_domain(self, _):
        name = self.save_domain_name.value.strip()
        if not _valid_spec_name(name):
            self.save_domain_status.value = components.status_span(
                "err", "Invalid name."
            )
            return
        if self.config is None:
            self.save_domain_status.value = components.status_span(
                "err", "Config invalid — nothing to save."
            )
            return
        if self._grid_file is not None:
            # DomainSpec (catalog/specs/DomainSpec/*.yaml) has no grid_file field --
            # saving now would silently produce a geometry-less entry (grid_kwargs
            # is {} while attached). Detach first.
            self.save_domain_status.value = components.status_span(
                "err", "Detach the grid file first — DomainSpec has no grid_file field."
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
            self.save_domain_status.value = components.status_span("err", f"{exc}")
            return
        except Exception as exc:
            self.save_domain_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        ok = self._verify_spec_roundtrip("domain", name)
        with self._suspend():
            old_value = self.domain_dd.value
            self.domain_dd.options = _dd_options(
                self.catalog,
                list(self.catalog.domain_names),
                "domain",
                prefix=["<custom>"],
            )
            self.domain_dd.value = name if ok else old_value
            if ok:
                self._domain_seed = self._domain_snapshot()
        if ok:
            self._rebuild()
            self.save_domain_status.value = components.status_span(
                "ok", f"Saved '{name}' ✓ (now unmodified)."
            )
        else:
            self.save_domain_status.value = components.status_span(
                "warn",
                f"Saved '{name}', but round-trip differs — spec kept as modified.",
            )

    def _on_save_forcing(self, _):
        name = self.save_forcing_name.value.strip()
        if not _valid_spec_name(name):
            self.save_forcing_status.value = components.status_span(
                "err", "Invalid name."
            )
            return
        if self.config is None:
            self.save_forcing_status.value = components.status_span(
                "err", "Config invalid — nothing to save."
            )
            return
        try:
            self.catalog.register_forcing(
                name,
                self._forcing_editor.gather(),
                description=self._spec_description(),
            )
        except FileExistsError as exc:
            self.save_forcing_status.value = components.status_span("err", f"{exc}")
            return
        except Exception as exc:
            self.save_forcing_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        ok = self._verify_spec_roundtrip("forcing", name)
        with self._suspend():
            old_value = self.forcing_dd.value
            self.forcing_dd.options = _dd_options(
                self.catalog, list(self.catalog.forcing_names), "forcing"
            )
            self.forcing_dd.value = name if ok else old_value
            if ok:
                self._forcing_seed = self._forcing_editor.gather()
        if ok:
            self._rebuild()
            self.save_forcing_status.value = components.status_span(
                "ok", f"Saved '{name}' ✓ (now unmodified)."
            )
        else:
            self.save_forcing_status.value = components.status_span(
                "warn",
                f"Saved '{name}', but round-trip differs — spec kept as modified.",
            )

    def _on_save_cdr(self, _):
        name = self.save_cdr_name.value.strip()
        if not _valid_spec_name(name):
            self.save_cdr_status.value = components.status_span("err", "Invalid name.")
            return
        if self.config is None:
            self.save_cdr_status.value = components.status_span(
                "err", "Config invalid — nothing to save."
            )
            return
        mode = self.cdr_mode_dd.value
        if mode == "none":
            self.save_cdr_status.value = components.status_span(
                "warn", "Mode is 'None' — nothing to save."
            )
            return
        current = self._current_cdr_dict()
        try:
            self.catalog.register_cdr(
                name,
                description=self._spec_description(),
                mode=mode,
                cdr_forcing=current["cdr_forcing"],
                cdr_forcing_file=current["cdr_forcing_file"],
            )
        except FileExistsError as exc:
            self.save_cdr_status.value = components.status_span("err", f"{exc}")
            return
        except Exception as exc:
            self.save_cdr_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}"
            )
            return
        ok = self._verify_spec_roundtrip("cdr", name)
        with self._suspend():
            old_value = self.cdr_dd.value
            self.cdr_dd.options = _dd_options(
                self.catalog, list(self.catalog.cdr_names), "cdr", prefix=["<custom>"]
            )
            self.cdr_dd.value = name if ok else old_value
            if ok:
                self._cdr_seed = self._cdr_snapshot()
        if ok:
            self._rebuild()
            self.save_cdr_status.value = components.status_span(
                "ok", f"Saved '{name}' ✓ (now unmodified)."
            )
        else:
            self.save_cdr_status.value = components.status_span(
                "warn",
                f"Saved '{name}', but round-trip differs — spec kept as modified.",
            )

    def _build_run_command(self, blueprint_path: str) -> list[str]:
        """Command the Run button invokes: ``cstar blueprint run <path>``.

        The same command the docs give for both pipeline steps, and the same one
        the emitted ``ucla_roms`` blueprint is run with -- the button exposes no
        per-run flags, so the full-option ``cstar forge run`` passthrough buys it
        nothing. Resolving ``application: forge`` this way goes through C-Star's
        registry, which finds the app as the in-tree
        ``cstar.applications.forge`` module -- no separate install required.

        Uses the ``cstar`` console script installed alongside the running
        interpreter, so the subprocess stays in this environment rather than
        taking whatever is first on PATH. Where that script is absent (the
        ``cstar`` entry point not installed), falls back to
        ``python -m cstar.cli.cli forge run``, the same unified CLI's ``forge``
        subcommand invoked as a module.
        """
        import sys

        cstar_exe = Path(sys.executable).with_name("cstar")
        if cstar_exe.exists():
            return [str(cstar_exe), "blueprint", "run", blueprint_path]
        return [sys.executable, "-m", "cstar.cli.cli", "forge", "run", blueprint_path]

    def _on_run(self, _):
        if not self._ensure_boundaries_derived():
            self.run_status.value = components.status_span(
                "err",
                "Run aborted — open boundaries could "
                "not be derived from the grid (see the Domain-derived properties "
                "status above). Fix the grid or set boundaries manually, then "
                "retry.",
            )
            return
        if self.config is None:
            self.run_status.value = components.status_span(
                "err", "Nothing to run — config is invalid."
            )
            return
        _schedule_coroutine(self._run_async())

    async def _run_async(self):
        """Save the current blueprint, then launch the C-Star CLI as a subprocess
        and stream its combined stdout/stderr into ``run_output`` line by line as
        it arrives (not all at once at the end).
        """
        import asyncio

        self._run_in_progress = True
        self.run_btn.disabled = True
        self.run_output.clear_output(wait=True)
        self.run_status.value = "<i>saving blueprint…</i>"
        cmd: list[str] | None = None
        try:
            save_path = Path(self.save_path.value)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            path = self._save_config(save_path)
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
            # On success the log above names the emitted ucla_roms blueprint (the
            # runner logs where it published it). Point at it rather than restating
            # a path this widget would have to derive for itself.
            self.run_status.value = (
                components.status_span("ok", "✓ finished")
                + " — run the emitted "
                + "UCLA-ROMS blueprint named in the log with <code>cstar blueprint "
                "run &lt;path&gt;</code>"
                if code == 0
                else components.status_span("err", f"exited with code {code}")
            )
        except Exception as exc:
            # Name the command in the status so a failure isn't a context-free
            # error string (cmd is None only if saving the blueprint threw first).
            where = f" while running: {' '.join(cmd)}" if cmd else ""
            self.run_status.value = components.status_span(
                "err", f"{type(exc).__name__}: {exc}{where}"
            )
        finally:
            self._run_in_progress = False
            self._sync_next_steps()

    # ---- layout / display ----------------------------------------------------
    @property
    def widget(self):
        """The wizard's root widget: a ``forge-app``/``forge-wizard`` ``W.VBox``
        of a sticky status bar, an intro line, and the nine cards (Start,
        Model, Grid, Forcing, Run, Output, Advanced, Review, Next steps). Built
        once and cached in ``self._root`` -- repeated access returns the same
        tree, so widgets placed in it keep their identity across a rebuild.
        """
        if self._root is not None:
            return self._root
        W = self.W

        # Danger-styled destructive actions.
        self.grid_file_detach_btn.button_style = "danger"
        self.cdr_clear_btn.button_style = "danger"
        self.cdr_file_clear_btn.button_style = "danger"

        sticky = W.HBox([self.sticky_bar, self.download_link])
        sticky.add_class("forge-sticky")
        # sticky_bar fills the remaining width (its own content -- step links,
        # summary, spacer, status chip -- pushes the chip to its right edge,
        # see _update_status); download_link stays sized to its fixed caption.
        self.sticky_bar.layout.flex = "1 1 auto"
        self.download_link.layout.flex = "0 0 auto"

        intro = W.HTML(
            "<p>Pick a model and a domain, adjust the grid, boundaries, "
            "partitioning and run window, then review and download the "
            "blueprint.</p>"
            "<p><span class='req'>*</span> Required &middot; everything else "
            "has a model default</p>"
        )

        start_card = components.card(
            W,
            "start",
            components.field_row(
                W,
                "load_catalog_dd",
                self.load_catalog_dd,
                extra=(self.load_catalog_btn,),
            ),
            components.field_row(
                W, "load_path", self.load_path, extra=(self.load_btn, self.upload)
            ),
            self.load_status,
        )

        model_card = components.card(
            W,
            "model",
            self.card_errors["model"],
            *self.model_card.rows(),
            components.field_row(W, "forcing_dd", self.forcing_dd),
            num=1,
            chips_widget=self.card_chips["model"],
        )

        # --- grid card ---
        geometry_rows = [
            components.field_row(
                W, f"grid.{k}", self.grid_w[k], width="110px", label_width="170px"
            )
            for k in (_GRID_INT + _GRID_FLOAT)
        ]
        # The stylesheet sizes the fields and the preview column (`forge-geo-row`
        # in WIZARD_CSS): the fields keep their room, and in a narrow card the
        # preview moves below them.
        grid_plot_col = W.VBox(
            [self.plot_btn, self.plot_status, self.plot_img],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        geometry_grid = components.field_grid(W, geometry_rows)
        geometry_row = W.HBox([geometry_grid, grid_plot_col])
        geometry_row.add_class("forge-geo-row")
        geometry_sub = components.subsection(W, "grid.geometry", geometry_row)
        vertical_sub = components.subsection(
            W,
            "grid.vertical",
            self.scoord_chk,
            components.field_grid(
                W,
                [
                    components.field_row(W, f"grid.{k}", self.grid_w[k], width="110px")
                    for k in _SCOORD
                ],
            ),
        )
        bathymetry_sub = components.subsection(
            W,
            "grid.bathymetry",
            components.field_grid(
                W,
                [
                    components.field_row(W, "topo_source", self.topo_source),
                    components.field_row(W, "topo_path", self.topo_path),
                    components.field_row(W, "hmin", self.hmin),
                    components.field_row(W, "close_narrow_chk", self.close_narrow_chk),
                ],
            ),
            components.field_row(W, "mask_shapefile", self.mask_shapefile),
        )
        self.gridfile_accordion = components.collapsible(
            W,
            W.VBox(
                [
                    W.HBox(
                        [
                            self.grid_file_path,
                            self.grid_file_attach_btn,
                            self.grid_file_detach_btn,
                        ]
                    ),
                    self.grid_file_upload,
                    self.grid_file_status,
                ]
            ),
            section_for("grid.gridfile").title,
        )
        derived_sub = components.subsection(
            W,
            "grid.derived",
            components.banner_widget(
                W,
                "info",
                "Open boundaries, sponge viscosity and the time step are "
                "derived from the grid until you set them. The sponge viscosity "
                "and time step shown follow the grid; derive the boundaries now "
                "to check or edit them.",
            ),
            components.field_row(W, "bnd", W.HBox(list(self.bnd.values()))),
            components.field_row(
                W,
                "v_sponge",
                self.v_sponge,
                extra=(self.derive_btn, self.derive_status),
            ),
            components.field_row(W, "dt", self.dt, extra=(self.dt_btn, self.dt_status)),
        )

        child_grid_rows = [
            components.field_row(W, f"grid.{k}", self.child_w[k], width="110px")
            for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)
        ]
        parent_grid_rows = [
            components.field_row(W, f"grid.{k}", self.parent_w[k], width="110px")
            for k in (_GRID_INT + _GRID_FLOAT + _SCOORD)
        ]
        child_plot_col = W.VBox(
            [W.HBox([self.nest_plot_btn, self.nest_plot_status]), self.nest_plot_img],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        child_section = components.subsection(
            W,
            "grid.nesting.child",
            self.nest_enable,
            self.nest_help,
            W.HBox(
                [
                    W.VBox(
                        [
                            components.field_row(
                                W, "nest_domain_dd", self.nest_domain_dd
                            ),
                            components.field_grid(W, child_grid_rows),
                            components.field_row(
                                W, "child_topo_source", self.child_topo_source
                            ),
                            components.field_row(
                                W, "child_topo_path", self.child_topo_path
                            ),
                            components.field_row(W, "nest_period", self.nest_period),
                            components.field_row(
                                W, "nest_pressure_fluxes", self.nest_pressure_fluxes
                            ),
                        ]
                    ),
                    child_plot_col,
                ]
            ),
            default_title="Child grid",
        )
        parent_plot_col = W.VBox(
            [
                W.HBox([self.parent_plot_btn, self.parent_plot_status]),
                self.parent_plot_img,
            ],
            layout=W.Layout(padding="0 0 0 20px"),
        )
        parent_section = components.subsection(
            W,
            "grid.nesting.parent",
            self.parent_enable,
            self.parent_help,
            W.HBox(
                [
                    W.VBox(
                        [
                            components.field_row(
                                W, "parent_domain_dd", self.parent_domain_dd
                            ),
                            components.field_grid(W, parent_grid_rows),
                            components.field_row(
                                W, "parent_topo_source", self.parent_topo_source
                            ),
                            components.field_row(
                                W, "parent_topo_path", self.parent_topo_path
                            ),
                        ]
                    ),
                    parent_plot_col,
                ]
            ),
            default_title="Parent grid",
        )
        nesting_accordion = components.collapsible(
            W,
            W.VBox([child_section, parent_section]),
            section_for("grid.nesting").title,
        )

        grid_card = components.card(
            W,
            "grid",
            self.card_errors["grid"],
            components.field_grid(
                W,
                [
                    components.field_row(W, "domain_dd", self.domain_dd),
                    components.field_row(W, "grid_name", self.grid_name),
                ],
            ),
            self.gridfile_accordion,
            self.grid_lock_banner,
            geometry_sub,
            vertical_sub,
            bathymetry_sub,
            derived_sub,
            nesting_accordion,
            num=2,
            chips_widget=self.card_chips["grid"],
        )

        forcing_card = components.card(
            W,
            "forcing",
            self.card_errors["forcing"],
            self.forcing_banner,
            self.forcing_box,
            num=3,
            chips_widget=self.card_chips["forcing"],
        )

        cdr_row = W.HBox(
            [
                W.VBox(
                    [
                        self.cdr_simple_box,
                        self.cdr_yaml_box,
                        self.cdr_netcdf_box,
                        self.cdr_upscaled_box,
                    ]
                ),
                self.cdr_plot_box,
            ]
        )
        cdr_row.add_class("forge-cdr-row")

        run_card = components.card(
            W,
            "run",
            self.card_errors["run"],
            components.subsection(
                W,
                "run.window",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "start", self.start),
                        components.field_row(W, "end", self.end),
                        components.field_row(W, "model_ref_date", self.model_ref_date),
                    ],
                ),
            ),
            components.subsection(
                W,
                "run.partitioning",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "npx", self.npx),
                        components.field_row(W, "npy", self.npy),
                        components.field_row(W, "n_cores", self.n_cores),
                        components.field_row(
                            W, "auto_tiling_chk", self.auto_tiling_chk
                        ),
                    ],
                ),
                components.field_row(W, "use_pio_chk", self.use_pio_chk),
            ),
            components.subsection(
                W,
                "run.cdr",
                components.field_grid(
                    W,
                    [
                        components.field_row(W, "cdr_dd", self.cdr_dd),
                        components.field_row(W, "cdr_mode_dd", self.cdr_mode_dd),
                    ],
                ),
                self.cdr_lite_hint,
                # Per-mode panels on the left, plot column on the right --
                # the same side-by-side arrangement as the Grid card, and the
                # plot moves below them in a narrow card (see WIZARD_CSS).
                cdr_row,
            ),
            num=4,
            chips_widget=self.card_chips["run"],
        )

        output_card = components.card(
            W,
            "output",
            self.card_errors["output"],
            components.field_row(W, "output_dd", self.output_dd),
            self.output_editor_box,
            num=5,
            chips_widget=self.card_chips["output"],
        )

        advanced_card = components.card(
            W,
            "advanced",
            self.card_errors["advanced"],
            self.editor_box,
            num=6,
            chips_widget=self.card_chips["advanced"],
        )

        self.save_specs_accordion = components.collapsible(
            W,
            W.VBox(
                [
                    W.HTML(
                        "<i>Promote an edited spec to a new named catalog "
                        "entry. Only marked unmodified if the saved file "
                        "re-resolves to the identical blueprint.</i>"
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
                    W.HBox(
                        [
                            self.save_cdr_name,
                            self.save_cdr_btn,
                            self.save_cdr_status,
                        ]
                    ),
                ]
            ),
            components.accordion_title(
                section_for("next.save_specs").title,
                components.Pill(section_for("next.save_specs").desc, "off"),
            ),
        )

        review_card = components.card(
            W,
            "review",
            self.validation,
            self.derived,
            self.preview,
            components.field_row(W, "name", self.name),
            components.field_row(W, "description", self.description),
            self.download_link_review,
            components.field_row(
                W, "save_path", self.save_path, extra=(self.save_btn,)
            ),
            self.save_status,
            num=7,
            chips_widget=self.card_chips["review"],
            required_chip=False,
        )

        run_forge = section_for("next.run_forge")
        self.run_forge_pane = components.collapsible(
            W,
            W.VBox(
                [
                    W.HTML(
                        "<p class='forge-hint'>Runs Forge in this session to build "
                        "the input files and the UCLA-ROMS blueprint. It does not "
                        "run UCLA-ROMS; to do both, use a workplan above.</p>"
                    ),
                    self.run_warning,
                    self.run_later_note,
                    W.HBox([self.run_btn, self.run_status]),
                    self._run_log_style,
                    self.run_output,
                ]
            ),
            components.accordion_title(
                run_forge.title, components.Pill(run_forge.desc, "off")
            ),
        )
        workplan_sub = components.subsection(
            W,
            "next.workplan",
            W.HTML(
                "<p class='forge-hint'>Most runs need both steps: Forge builds the "
                "inputs and the UCLA-ROMS blueprint, then UCLA-ROMS runs it. Save a "
                "workplan that does both.</p>"
            ),
            W.HBox([self.save_workplan_btn, self.open_workplan_btn]),
            self.workplan_status,
            self.workplan_download,
        )
        next_card = components.card(
            W,
            "next",
            self.next_status,
            workplan_sub,
            self.run_forge_pane,
            self.save_specs_accordion,
            num=8,
        )

        root = W.VBox(
            [
                components.style_widget(W),
                sticky,
                intro,
                start_card,
                model_card,
                grid_card,
                forcing_card,
                run_card,
                output_card,
                advanced_card,
                review_card,
                next_card,
            ]
        )
        root.add_class("forge-app")
        root.add_class("forge-wizard")
        self._root = root
        return root

    def display(self):
        from IPython.display import display

        display(self.widget)
