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

import copy
import html
import re
import warnings
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel, ValidationError

from cstar.applications.forge.blueprint import (
    BgcSourceItem,
    BoundaryForcing,
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
)
from cstar.applications.forge.namelist_model import (
    run_time_settings_for_ref,
    validate_run_time_sections,
)
from cstar.applications.forge.resolve import (
    OUTPUT_SECTIONS,
    PARTIAL_OUTPUT_SECTIONS,
    build_forge_blueprint,
    extract_output_settings,
    force_switches_without_input_off,
    load_model_spec_data,
    resolver_warnings,
)
from cstar.wizard.app import ForgeBlueprintWizardApp as ForgeBlueprintWizardApp
from cstar.wizard.domain_card import DomainCard
from cstar.wizard.fields import HELP_TEXT as HELP_TEXT
from cstar.wizard.fields import (
    _dd_options,
    _dd_values,
    _tip,
)
from cstar.wizard.forcing_editor import (
    _BOUNDARY_NONE,
    _CATEGORY_TITLES,
    _IC_NONE,
    _IC_SOURCE_OPTS,
    _ForcingEditor,
    _is_co2_item,
    _row_desc,
    _row_place,
    _use_vars_rule,
)
from cstar.wizard.model_card import ModelCard, _pinned_roms_ref
from cstar.wizard.review_card import WIZARD_TOOL, ReviewCard
from cstar.wizard.review_card import WorkplanHooks as WorkplanHooks
from cstar.wizard.run_card import RunCard, _run_time_label
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
from cstar.wizard.ui.labels import known_keys, label_for, section_for

if TYPE_CHECKING:
    from collections.abc import Sequence


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


def _get_catalog():
    """Return the default catalog stack for spec discovery.

    A layered stack: the user's writable catalog layer (``~/cstar/catalog``
    by default, or ``CSTAR_CATALOG``) over the read-only bundled in-repo
    catalog. Reads resolve top-first; writes (``register_*``) land in the
    user layer.
    """
    from cstar.catalog.domain_catalog import default_catalog

    return default_catalog


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
        # True while a _suspend() block is open: handlers skip programmatic writes.
        # Set before the cards are built, since their is_suspended reads it.
        self._suspended = False
        # Reentrancy depth for _suspend()/_Suspender -- lets a programmatic backfill
        # (e.g. _populate_grid_widgets_from_grid) nest inside an already-suspended
        # block (e.g. _populate_from's outer _suspend()) without the inner
        # context's __exit__ prematurely clearing the outer one's suspension.
        self._suspend_depth = 0

        # --- the Model card (ModelSpec picker, BGC mode, code refs, carbonate
        # sensitivities): see cstar.wizard.model_card ---
        self.model_card = ModelCard(
            W,
            self.catalog,
            rebuild=self._rebuild,
            suspend=self._suspend,
            is_suspended=lambda: self._suspended,
            use_pio=lambda: self.run_card.use_pio,
        )
        # --- the Grid card (Domain pick, grid, topography, grid file, derived
        # properties, previews and nesting): see cstar.wizard.domain_card ---
        self.domain_card = DomainCard(
            W,
            self.catalog,
            rebuild=self._rebuild,
            suspend=self._suspend,
            is_suspended=lambda: self._suspended,
            show_summary=lambda summary: self.review_card.show_summary(summary),
        )
        # --- the Run card (run window, processor layout, CDR): see
        # cstar.wizard.run_card ---
        self.run_card = RunCard(
            W,
            self.catalog,
            rebuild=self._rebuild,
            suspend=self._suspend,
            is_suspended=lambda: self._suspended,
            cdr_lite=lambda: self.model_card.is_cdr_lite,
            grid_center=lambda: self.domain_card.grid_center,
            grid_fingerprint=lambda: self.domain_card.grid_fingerprint,
            # Late-bound: tests replace build_grid_from_widgets on the card.
            build_grid=lambda: self.domain_card.build_grid_from_widgets(),
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
        # snapshot of the domain-defining widgets at the last catalog Domain pick;
        # compared in _rebuild() to detect a deviation (composition.domain.modified).
        # None means no catalog domain has been picked yet (or domain_dd == "<custom>").
        self._domain_seed: dict[str, Any] | None = None
        # The composition names of the last loaded blueprint that this catalog
        # lacks, for the load status (see _set_load_status).
        self._load_unknown_specs: list[str] = []
        # The resolver-composed model_settings behind the editor (see _show_editor);
        # _on_editor_edit drops an edit that only restores one of its values.
        self._composed_settings: dict[str, Any] | None = None

        # --- Save and Next steps cards (cards 7 and 8) ---
        self.review_card = ReviewCard(
            W,
            self.catalog,
            rebuild=self._rebuild,
            suspend=self._suspend,
            is_suspended=lambda: self._suspended,
            config=lambda: self.config,
            # Late-binding: tests replace these on the wizard and the Grid card.
            save_config=lambda path: self._save_config(path),
            ensure_boundaries_derived=lambda: (
                self.domain_card.ensure_boundaries_derived()
            ),
            boundaries_pending=lambda: self.domain_card.boundaries_pending,
        )

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
        self.forcing_banner = components.banner_widget(W, "info", "")

        self.run_card.set_use_pio(self.model_card.default_use_pio())
        self._sync_cdr_lite_hint()
        self._build_forcing_editor(self.catalog.forcing_data(self.forcing_dd.value))
        if self._forcing_editor is None:
            raise RuntimeError("_build_forcing_editor did not set _forcing_editor")
        self._forcing_seed = self._forcing_editor.gather()
        self.run_card.apply_cdr_mode()
        self._wire()
        self._rebuild()

    # ---- wiring --------------------------------------------------------------
    def _wire(self):
        self.domain_card.domain_dd.observe(self._on_domain, names="value")
        # The Grid card's own observers and buttons.
        self.domain_card.wire()
        self.forcing_dd.observe(self._on_forcing_spec, names="value")
        # The Save and Next steps cards' observers and buttons.
        self.review_card.wire()
        self.save_output_btn.on_click(self._on_save_output)
        self.save_model_btn.on_click(self._on_save_model)
        self.save_domain_btn.on_click(self._on_save_domain)
        self.save_forcing_btn.on_click(self._on_save_forcing)
        self.save_cdr_btn.on_click(self._on_save_cdr)
        self.load_btn.on_click(self._on_load_path)
        self.load_catalog_btn.on_click(self._on_load_from_catalog)
        self.upload.observe(self._on_upload, names="value")
        # The Run card's own observers (its CDR handlers and auto-tiling sync),
        # before the rebuilds registered on its widgets below.
        self.run_card.wire()
        self.model_card.model_dd.observe(self._on_model_change, names="value")
        # The card's own observers first: on bgc_dd, its visibility syncs run before
        # the reactions of the other cards below.
        self.model_card.wire()
        self.model_card.bgc_dd.observe(self._sync_cdr_lite_hint, names="value")
        self.model_card.bgc_dd.observe(
            self._sync_marbl_forcing_visibility, names="value"
        )
        self.model_card.bgc_dd.observe(self._on_bgc_mode, names="value")
        self.domain_card.parent_domain_dd.observe(self._on_parent_domain, names="value")
        self.domain_card.parent_enable.observe(self._on_parent_toggle, names="value")
        self.output_dd.observe(self._on_output_spec, names="value")
        # Last, so on each widget the card's own observers and the reactions above
        # run before the rebuild.
        for card in (
            self.domain_card,
            self.run_card,
            self.model_card,
            self.review_card,
        ):
            for w in card.rebuild_on:
                w.observe(self._rebuild, names="value")

    def _sync_cdr_lite_hint(self, _change=None):
        # The Run card's CDR hint follows the Model card's BGC mode.
        self.run_card.sync_cdr_lite_hint()

    def _on_model_change(self, _change):
        # a different model has different defaults -> existing overrides no longer apply.
        # Forcing/Output are independent catalog dimensions from the model (a ForcingSpec/
        # OutputSpec doesn't reference a model), so switching models never re-picks them
        # -- except that an untouched default ForcingSpec follows the new model's
        # declared bgc_mode, via the bgc_dd change apply_spec_defaults makes (see
        # _on_bgc_mode).
        if self._suspended:
            return
        self._overrides = {}
        self._wizard_default_keys = set(_WIZARD_SETTING_DEFAULTS)
        self.model_card.apply_spec_defaults()
        self.run_card.set_use_pio(self.model_card.default_use_pio())
        self._rebuild()

    def _on_editor_edit(self, section, field):
        # a user edit in the advanced editor -> record as an override (vs composed)
        if self._syncing or self._suspended:
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

    def _on_parent_domain(self, _change):
        """Prefill the parent grid from a selected DomainSpec (and enable it), then
        clear what a child grid does not take: the boundary forcing and the
        initial conditions.
        """
        if not self.domain_card.load_picked_parent():
            return
        self._clear_boundary_forcing()
        self._clear_initial_conditions()
        self._rebuild()
        self.domain_card.on_parent_plot(None)

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
        if self._suspended:
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
        self._forcing_editor.show_co2(self.model_card.uses_marbl)
        self._forcing_editor.show_bgc(self.model_card.uses_marbl)
        self.forcing_box.children = [self._forcing_editor.widget]

    def _sync_marbl_forcing_visibility(self, _change=None) -> None:
        """Show the forcing editor's "Atmospheric CO2" block and its Biogeochemistry
        sections (unless they hold something) under MARBL only.
        """
        if self._forcing_editor is not None:
            marbl = self.model_card.uses_marbl
            self._forcing_editor.show_co2(marbl)
            self._forcing_editor.show_bgc(marbl)

    def _on_forcing_spec(self, _change):
        """Selecting a ForcingSpec reseeds the forcing editor. CDR is its own
        independently composable spec (CdrSpec, the Run card's) and is
        normally untouched by a ForcingSpec pick -- the one exception is a
        *legacy* ForcingSpec predating the CdrSpec split that still embeds a
        ``cdr_forcing`` block (see ``_split_forcing_data``); that embedded dict is
        routed into the CDR box as if it had been uploaded as a YAML.
        """
        if self._suspended:
            return
        fi, legacy_cdr = _split_forcing_data(
            self.catalog.forcing_data(self.forcing_dd.value)
        )
        self._build_forcing_editor(fi)
        if self.domain_card.parent_enabled:
            # A child grid (has a parent) gets its boundaries from the parent's
            # nesting.nc extraction, not reanalysis boundary forcing -- strip any
            # boundary items the freshly-built editor just reseeded from the spec.
            self._clear_boundary_forcing()
            # Same reasoning for IC: the freshly-built editor reseeds from the
            # spec's real IC block, which would silently undo the "(none)"
            # default a parent toggle already applied.
            self._clear_initial_conditions()
        if legacy_cdr:
            self.run_card.load_legacy_cdr(legacy_cdr)
        self._forcing_seed = self._forcing_editor.gather()
        self._rebuild()

    def _on_bgc_mode(self, change) -> None:
        """A BGC mode change re-resolves once. An untouched default ForcingSpec
        follows the mode (the BGC-carrying default for MARBL, the physics-only one
        otherwise); a hand-edited or non-default forcing is left alone.
        """
        if self._suspended or self._syncing:
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
        if self._suspended:
            return
        self._rebuild()

    def _on_output_spec(self, _change):
        """Selecting an OutputSpec seeds the output sections. Clear any manual
        overrides on those sections/fields so the selection takes effect cleanly.
        """
        if self._suspended:
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
        dom = self.domain_card.spec_ref()
        # forcing/output are always an explicit catalog selection now (no more
        # "model_default" origin -- ModelSpec no longer provides either as a fallback).
        forcing = SpecRef(name=self.forcing_dd.value, origin="catalog")
        output = SpecRef(name=self.output_dd.value, origin="catalog")
        return Composition(
            model=SpecRef(name=self.model_card.name, origin="catalog"),
            domain=dom,
            forcing=forcing,
            cdr=self.run_card.cdr_spec_ref(),
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
                # CDR is its own independently composable spec (CdrSpec, the Run
                # card's) -- picking a ForcingSpec never touches it, so
                # kw["cdr"] (already set by _gather()) is left as-is here.
                fi, _legacy_cdr = _split_forcing_data(
                    self.catalog.forcing_data(new_name)
                )
                if self.domain_card.parent_enabled:
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
        """Prefill from a cataloged DomainSpec when one is selected: the Grid card
        (see `DomainCard.load_picked_domain`), then the Run card's processor layout
        and run dates; a spec without a parent grid restores what a parent cleared.
        """
        if not self.domain_card.from_catalog:
            self._domain_seed = None
            return
        loaded = self.domain_card.load_picked_domain()
        if loaded is None:
            return
        data, parent_dropped = loaded
        with self._suspend():
            self.run_card.populate_from_domain(data)
            if parent_dropped:
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
        self.domain_card.on_plot(None)

    def _domain_snapshot(self) -> dict[str, Any]:
        """The domain-defining widget values at the moment of a catalog Domain pick:
        the Grid card's, with the Run card's processor layout. Compared against the
        current values in `_rebuild()` to detect an edit made after selection
        (`composition.domain.modified`).
        """
        return self.domain_card.snapshot(self.run_card.layout_snapshot())

    def _domain_spec_data(self) -> dict[str, Any]:
        """Build a DomainSpec-shaped dict from the current widget state (the
        domain-spec extractor for "save modified specs to catalog"). Includes
        topography and nesting -- both results-affecting -- so a saved DomainSpec
        actually round-trips (see ``_verify_spec_roundtrip``); ``register_domain``
        (the ForgeExecutor-driven path) predates these and omits them.
        """
        return self.domain_card.spec_data(
            self._gather(),
            description=self.review_card.spec_description(),
            model_name=self.model_card.name,
            dates=self.run_card.domain_dates(),
        )

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
            self.domain_card.select_custom()
            self.model_card.select_model(cfg, self._load_unknown_specs)
            self.review_card.populate(cfg)
            self.run_card.populate_window(cfg)
            self.domain_card.populate_grid(cfg)
            self.run_card.populate_layout(cfg)
            self.model_card.populate(cfg)
            self.domain_card.populate_topography(cfg)
            self.run_card.populate_cdr(cfg)
            self.domain_card.populate_dt(cfg)
            self.domain_card.populate_nesting(cfg)
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
            if self.domain_card.parent_enabled:
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
            self.run_card.select_cdr_spec(cfg)
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
        kw = dict(
            model_path=self.model_card.model_path(),
            **self.domain_card.gather_grid(),
            partitioning=self.run_card.partitioning(),
            start_date=self.run_card.start_datetime(),
            end_date=self.run_card.end_datetime(),
            **self.review_card.gather(),
            **self.domain_card.gather_derived(),
        )
        kw.update(self.domain_card.gather_topography())
        kw["use_pio"] = self.run_card.use_pio
        kw.update(self.model_card.gather())
        if (ref := self.run_card.model_reference_date()) is not None:
            kw["model_reference_date"] = ref
        kw.update(self.domain_card.gather_file_and_nesting())
        _is_parent, is_child = self.domain_card.nesting_role()
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
        if not self.model_card.uses_marbl:
            # Only MARBL has an air CO2: the hidden "Atmospheric CO2" block keeps its
            # choice for a switch back, but a time-varying one reaches no blueprint.
            surface = kw["forcing_inputs"]["forcing"]["surface"]
            kw["forcing_inputs"]["forcing"]["surface"] = [
                item for item in surface if not _is_co2_item(item)
            ]
        kw["output_settings"] = self._output_settings()
        kw["composition"] = self._composition()
        kw["cdr"] = self.run_card.cdr()
        kw["provenance"] = self._carried_provenance
        return kw

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
        is_parent, is_child = self.domain_card.nesting_role()
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
        if self._suspended:
            return
        # The simple CDR release follows the grid and run window, and the CDR
        # plot is dropped when an input it was drawn from changed (see
        # RunCard.refresh_cdr).
        self.run_card.refresh_cdr()
        self.domain_card.sync_derived_dt()
        # Captured before self.config is reassigned below -- used to detect a
        # real rename (vs. an unrelated edit re-running _rebuild) for the
        # save_path filename re-sync (see ReviewCard.follow_derived).
        prev_name = self.config.name if self.config is not None else None
        self.review_card.clear_results()
        if not self.run_card.has_run_dates:
            self.config = None
            self.review_card.show_unresolved("<i>Set start and end dates…</i>")
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
            problems = _blueprint_problems(exc)
            self.review_card.show_unresolved(
                components.banner(
                    "err",
                    f"<div><b>Invalid blueprint.</b> {_problem_summary(problems)}</div>",
                ),
                error=f"{type(exc).__name__}: {exc}",
            )
            self._show_unresolved_editor()
            self._update_status(None, problems=problems)
            return

        self.domain_card.follow_resolved(cfg)

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
        is_parent, is_child = self.domain_card.nesting_role()
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
            self.domain_card.from_catalog
            and self._domain_seed is not None
            and self._domain_snapshot() != self._domain_seed
        )
        forcing_modified = self._forcing_modified()
        cdr_modified = self.run_card.cdr_deviates_from_spec()

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
        self.review_card.follow_derived(cfg, prev_name)
        self.review_card.refresh_download_links(cfg)
        self.domain_card.warn_boundaries_pending()
        # Suppressed for the same reason as _wizard_settings_cls_for_ref above --
        # _rebuild runs on every keystroke, including typing into the roms_ref
        # override box, so a non-semver ref's UserWarning would otherwise spam.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            problems = validate_run_time_sections(
                cfg.model_settings, roms_ref=effective_roms_ref
            )
        # What the run would use that nothing refuses but its author likely did not
        # mean (zero BGC inputs on an older ucla-roms). The resolver only logs these,
        # which a widget callback never shows, so show them here.
        notes = resolver_warnings(cfg, roms_ref=effective_roms_ref)
        self.review_card.show_resolved(cfg, problems, notes)
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
        if cat == "surface" and self.model_card.uses_marbl:
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
        chips["model"].value = problem_chips.get("model", self.model_card.status_chip())
        chips["run"].value = problem_chips.get("run", self.run_card.status_chip())
        chips["review"].value = self.review_card.status_chip(valid)
        chips["grid"].value = problem_chips.get("grid", self.domain_card.status_chip())
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
            self.editor.set_run_window(self.run_card.window_seconds())
        self.review_card.sync_next_steps()

    # ---- actions -------------------------------------------------------------
    def _save_config(self, path: Path) -> Path:
        """Write the current config to ``path``, stamped as written by the wizard,
        and return the path.

        The stamp becomes the carried provenance, so a later save or rebuild keeps
        it while the content is unchanged (see ``ForgeBlueprint.stamp_provenance``),
        and the Save and Next steps cards are told (`ReviewCard.record_save`): the
        download links point at the file written. Every write of the current config
        to disk goes through here.
        """
        if self.config is None:
            raise RuntimeError("no valid configuration to save")
        stamped = self.config.stamp_provenance(WIZARD_TOOL)
        written = stamped.to_yaml(path)
        self._carried_provenance = stamped.provenance
        self.config = stamped
        self.review_card.record_save(Path(path), written, stamped)
        return written

    # ---- next steps ------------------------------------------------------------
    def set_workplan_hooks(self, hooks: WorkplanHooks | None) -> None:
        """Connect this page to a Workplan page (or disconnect it with ``None``).

        Without hooks only "Save workplan" shows: there is no page to open.
        """
        self.review_card.set_workplan_hooks(hooks)

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
                description=self.review_card.spec_description(),
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
                description=self.review_card.spec_description(),
                # Live widget values, using _gather()'s exact conventions (use_pio/
                # bgc_mode unconditional, roms_ref/marbl_ref only when non-blank) --
                # the round-trip verifier below compares against _gather()'s kw, so
                # any divergence here would be a spurious mismatch.
                bgc_mode=self.model_card.bgc_mode,
                use_pio=self.run_card.use_pio,
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
        if self.domain_card.grid_file_attached:
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
        # topography not sorted out). No ensure_boundaries_derived() call.
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
            self.domain_card.reselect(name if ok else None)
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
                description=self.review_card.spec_description(),
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
        mode = self.run_card.cdr_mode
        if mode == "none":
            self.save_cdr_status.value = components.status_span(
                "warn", "Mode is 'None' — nothing to save."
            )
            return
        current = self.run_card.cdr()
        try:
            self.catalog.register_cdr(
                name,
                description=self.review_card.spec_description(),
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
            self.run_card.reselect_cdr(name if ok else None)
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

        sticky = W.HBox([self.sticky_bar, self.review_card.download_link])
        sticky.add_class("forge-sticky")
        # sticky_bar fills the remaining width (its own content -- step links,
        # summary, spacer, status chip -- pushes the chip to its right edge,
        # see _update_status); download_link stays sized to its fixed caption.
        self.sticky_bar.layout.flex = "1 1 auto"
        self.review_card.download_link.layout.flex = "0 0 auto"

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

        grid_card = components.card(
            W,
            "grid",
            self.card_errors["grid"],
            *self.domain_card.rows(),
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

        run_card = components.card(
            W,
            "run",
            self.card_errors["run"],
            *self.run_card.rows(),
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
            *self.review_card.rows(),
            num=7,
            chips_widget=self.card_chips["review"],
            required_chip=False,
        )

        next_card = components.card(
            W,
            "next",
            *self.review_card.next_rows(),
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
