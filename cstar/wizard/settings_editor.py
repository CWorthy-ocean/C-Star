"""The wizard's settings editor and the tables that place each setting.

:class:`_SettingsEditor` builds the namelist and ``cppdefs`` widgets shown in the
Output, Advanced, Model and Forcing cards from the run-time settings schema. The
output tables, variable grids and placement predicates here decide which card
shows each ``(section, field)``.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from cstar.applications.forge.namelist_model import (
    CDR_OUTPUT_SECTIONS,
    FS_MODE_SPLIT,
    MARBL_ONLY_OUTPUT_FLAGS,
    RunTimeSettings,
    namelist_field_for,
    namelist_schema_for,
    version_gated_section_names,
)
from cstar.applications.forge.resolve import PARTIAL_OUTPUT_SECTIONS
from cstar.wizard.fields import (
    _BOOL_DROPDOWN_FIELDS,
    _base_type,
    _is_optional_number_widget,
    _is_optional_numeric,
    _literal_choices,
    _make_field_widget,
    _read_field_widget,
)
from cstar.wizard.ui import components
from cstar.wizard.ui.labels import known_keys, label_for, section_for

# Label overrides for namelist-section widgets built via _build_section /
# _make_field_widget. Keyed the same way as HELP_TEXT: (section, field_name).
# Falls back to the raw field name when no override is present.
LABEL_TEXT: dict[tuple[str, str], str] = {}


def _namelist_label(section: str, field_name: str) -> str:
    """Look up the display label override for a namelist field, else field_name.

    Prefers the glossary entry ``settings.<section>.<field_name>`` (see
    :mod:`cstar.wizard.ui.labels`); falls back to the legacy :data:`LABEL_TEXT`
    override, then to the raw ``field_name``.
    """
    key = f"settings.{section}.{field_name}"
    if key in known_keys():
        return label_for(key).label
    return LABEL_TEXT.get((section, field_name), field_name)


def _namelist_description_and_tooltip(
    section: str, field_name: str, tooltip: str
) -> tuple[str, str]:
    """The widget ``description``/``tooltip`` pair for one namelist field.

    Starts from :func:`_namelist_label` and the schema-derived ``tooltip``. A
    glossary ``hint`` replaces the schema description: it is curated for the
    wizard (in the wizard's vocabulary, e.g. "used when co2_tvarying is False"),
    and it is the only help for a field with no schema description (every
    ``cppdefs`` flag -- compile-time, never modeled by RomsNamelist), so caveats
    such as "ignored by ucla-roms < 0.8.0" reach the widget instead of living
    only in the YAML. If the glossary (or :data:`LABEL_TEXT`) overrides the
    label away from the raw ``field_name``, the raw name is kept discoverable
    by prefixing it onto the tooltip, and a glossary ``unit``, if present, is
    appended to the description (e.g. ``"Horizontal viscosity (m²/s)"``).
    """
    glossary_key = f"settings.{section}.{field_name}"
    in_glossary = glossary_key in known_keys()
    if in_glossary:
        tooltip = label_for(glossary_key).hint or tooltip
    label = _namelist_label(section, field_name)
    if label == field_name:
        return label, tooltip
    combined_tooltip = f"{field_name} — {tooltip}" if tooltip else field_name
    unit = label_for(glossary_key).unit if in_glossary else None
    description = f"{label} ({unit})" if unit else label
    return description, combined_tooltip


def _namelist_tooltip(
    settings_cls: type[BaseModel], section: str, field_name: str
) -> str:
    """The namelist schema's description of a forge settings field.

    Translates the forge ``section``/``field_name`` to the namelist group and key
    it is written to (:func:`namelist_field_for`, the same table
    :func:`build_namelist` writes through) and returns that field's
    ``description`` from ``settings_cls``'s namelist schema. Empty for anything
    that is not a namelist field of that tier (every ``cppdefs`` flag).
    """
    target = namelist_field_for(settings_cls, section, field_name)
    if target is None:
        return ""
    group, key = target
    group_cls = namelist_schema_for(settings_cls).model_fields[group].annotation
    return group_cls.model_fields[key].description or ""


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
    # extract_data is the parent side of nesting: the child grid, extraction period
    # and file come from the Grid card's nesting widgets (nest_period, the child
    # grid), resolved by build_forge_blueprint. Only nrpf/extract_root_name stay
    # editable in the Output card.
    "extract_data": frozenset(
        {
            "do_extract",
            "extract_file",
            "n_chd",
            "theta_s_chd",
            "theta_b_chd",
            "hc_chd",
            "extract_period",
        }
    ),
}

# The BGC modes each settings key applies in; a key not listed applies in every
# mode. A section key covers all of its fields. The output streams ucla-roms
# compiles only under MARBL come from namelist_model, which owns that fact
# (MARBL_ONLY_OUTPUT_FLAGS, and the requires_marbl rows of CDR_OUTPUT_SECTIONS).
# The rest are wizard-only: the bgc/marbl_bgc/dic_alk_correction groups, NHY/NOX
# forcing and cdr_lite.cdr_online_carbonate_sensitivity read MARBL state,
# cdr_lite_output needs CDR-lite tracers, which only MARBL and bgc_mode "cdr_lite"
# provide, and cdr_lite.cdr_carbonate_sensitivity_time_interpolation applies only
# to the file-based sensitivities of bgc_mode "cdr_lite".
_MARBL_ONLY = frozenset({"marbl"})
_BGC_MODE_SETTINGS: dict[str | tuple[str, str], frozenset[str]] = {
    **dict.fromkeys(
        [section for section, _flag, _why in MARBL_ONLY_OUTPUT_FLAGS]
        + [row[0] for row in CDR_OUTPUT_SECTIONS if row[3]],
        _MARBL_ONLY,
    ),
    "bgc": _MARBL_ONLY,
    "marbl_bgc": _MARBL_ONLY,
    "dic_alk_correction": _MARBL_ONLY,
    "cdr_lite_output": frozenset({"marbl", "cdr_lite"}),
    ("cppdefs", "nhy_forcing"): _MARBL_ONLY,
    ("cppdefs", "nox_forcing"): _MARBL_ONLY,
    ("cdr_lite", "cdr_online_carbonate_sensitivity"): _MARBL_ONLY,
    ("cdr_lite", "cdr_carbonate_sensitivity_time_interpolation"): frozenset(
        {"cdr_lite"}
    ),
}


def _applies_in(section: str, field: str | None, bgc_mode: str) -> bool:
    """Whether the ``(section, field)`` setting (``field=None`` for a scalar
    section) applies in ``bgc_mode`` -- see :data:`_BGC_MODE_SETTINGS`.
    """
    modes = _BGC_MODE_SETTINGS.get(section)
    if modes is None and field is not None:
        modes = _BGC_MODE_SETTINGS.get((section, field))
    return modes is None or bgc_mode in modes


# Settings ``ForgeExecutor.configure_build`` forces on in a BGC mode, whatever the
# blueprint authored: bgc_mode -> {(section, field): why}. The editor shows them
# checked and disabled -- display only, never an override.
_FORCED_ON_BY_BGC_MODE: dict[str, dict[tuple[str, str], str]] = {
    "cdr_lite": {
        ("cdr_lite_output", "do_cdr_lite_output"): (
            "Forced on by bgc_mode cdr_lite: this is the only output the CDR-lite "
            "tracers appear in."
        ),
    },
}


# Modeler-facing consolidation of the raw namelist sections into a few
# Advanced-settings panes, grouped the way an ocean modeler categorizes knobs
# rather than by the exact namelist sub-groups. DISPLAY-ONLY: each widget stays
# keyed by its real ``(section, field)``, so the overrides layer, ``_diff_overrides``,
# and the output/model "modified" tracking are all unaffected by the grouping.
#
# Sections deliberately absent (time_stepping, reference_date_settings, grid,
# s_coord, title, output_root_name, initial, forcing, river_frc) are filled
# dynamically at resolve/run time (ntimes from the run duration, grid/IC/forcing
# paths from generated files, river_frc entirely generation-derived) or edited by a
# dedicated widget elsewhere in the wizard (theta_s/theta_b/hc, dt, reference date,
# PIO/open-boundary checkboxes). ``param`` is shown under CDR for ``nt_passive``
# alone (see :data:`_PARAM_PANE_FIELDS`). Their resolver-composed value still
# flows through untouched -- omitting the pane only removes an editor that would be
# clobbered or duplicated, never the value.
#
# ``cppdefs`` is almost entirely resolver-derived (obc_*/marbl/use_pio/cdr_forcing/
# cdr_lite/co2_tvarying/sal_restore/tides) and stays out of the accordion for those
# fields -- only the handful with no other UI (``sponge_tune``, ``nhy_forcing``/
# ``nox_forcing``, and the ucla-roms >= 0.8.0 advection switches ``parabolic_splines``/
# ``upstream_ts_land_curv``, PR #361) are opted in, via ``_CPPDEFS_PANE_FIELDS``, so
# a user override can never collide with a resolver-owned flag (``diagnostics``, the
# DIAGNOSTICS switch, shows in the Output card's diagnostics row instead). The two
# advection fields only render for ModelSpecs that declare them
# (``roms-marbl-0.8-default``, ``roms-marbl-0.9-default``, ``roms-marbl-0.10-default``,
# ``roms-marbl-0.11-default``, ``roms-marbl-0.12-default``, ``roms-marbl-0.13-default``,
# ``roms-marbl-0.14-default``, ``roms-marbl-0.15-default``, ``roms-marbl-0.16-default``,
# ``pio-dev``) -- the editor type-infers a checkbox from the composed bool, so a spec
# that omits the key shows no widget for it.
#
# ``bgc``/``marbl_bgc`` are SPLIT at field granularity along the existing
# ``PARTIAL_OUTPUT_SECTIONS`` seam (the same split the OutputSpec dropdown seeds):
# their output write-controls live in the Output card (see ``_OUTPUT_MAIN_SECTIONS``),
# the rest under Biogeochemistry.
_ADVANCED_CATEGORIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Physics & subgrid tuning",
        (
            "lateral_visc",
            "vertical_mixing",
            "tracer_diff2",
            "bottom_drag",
            # v_sponge's own field is a first-class domain property with a dedicated
            # widget in "Domain-derived properties" (self.v_sponge), left out of this
            # pane by _ACCORDION_EXCLUDED_FIELDS; the section shows only v_sponge_trc
            # (ucla-roms >= 0.13.0), for ModelSpecs that declare it.
            "v_sponge",
            "sponge_tune",
            "gamma2",
            "ubind",
            "lin_rho_eos",
            # ucla-roms >= 0.15.0, the barotropic scheme; renders only for ModelSpecs
            # that declare it.
            "implicit_fs",
            # pio_settings is ModelSpec-owned (like the rest of this pane), NOT an
            # output section: edits here must land in composition.model / "Save
            # Model spec", so keep it out of the Output card.
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
        ("param", "cdr_frc", "cdr_lite"),
    ),
)

# Settings shown in the Model card (under the carbonate sensitivities) instead of
# their Advanced pane: section -> fields. ``_MODEL_CARD`` names the card as a
# "place" for ``_split_fields``. Display-only like the Output card: each widget stays
# keyed by its real ``(section, field)``, so overrides, load-back and the "modified"
# tracking are unaffected.
_MODEL_CARD = "Model"
_MODEL_CARD_FIELDS: dict[str, frozenset[str]] = {
    "cdr_lite": frozenset({"cdr_carbonate_sensitivity_time_interpolation"}),
}
# Likewise for the Forcing card: MARBL's constant atmospheric xCO2 shows in the
# Surface forcing pane's "Atmospheric CO2" block (see ``_ForcingEditor``), as the
# static alternative to an ``MBL_co2`` surface item.
_FORCING_CARD = "Forcing"
_FORCING_CARD_FIELDS: dict[str, frozenset[str]] = {
    "bgc": frozenset({"xco2air_default"}),
}

# The Output card's placement of the output streams -- the one table the editor build,
# the card's modified-count chip and the section retitling all read. ``_OUTPUT_CARD``
# names the card as a "place" for ``_split_fields``; the main streams are always
# visible in this display order, the rest sit in the collapsed "More output streams"
# accordion. Placement is display-only and independent of OutputSpec ownership
# (``_is_output_key``): ``extract_data``, ``particles``, ``calc_pflx`` and the
# compile-time ``cppdefs.diagnostics`` sit here yet are ModelSpec/Domain-owned.
_OUTPUT_CARD = "Output"
_OUTPUT_MORE_TITLE = "More output streams"
_OUTPUT_GROUPS = (_OUTPUT_CARD, _OUTPUT_MORE_TITLE)
_OUTPUT_MAIN_SECTIONS = (
    "ocean_vars",
    "surf_flux",
    "bgc",
    "marbl_bgc",
    "cdr_output",
    "cdr_lite_output",
    "cdr_gas_exch_output",
    "extract_data",
    "upscale_output",
)
_OUTPUT_MORE_SECTIONS = (
    "diagnostics",
    "frc_output",
    "zslice",
    "random_output",
    "particles",
    "calc_pflx",
    "stdout_diag",
)
# Nesting role of the grid being edited. ``extract_data`` feeds a child from this
# (parent) grid, so it exists only on a parent; ``upscale_output``'s ``_uscl`` files
# feed the parent's upscaled-CDR run, so it is a main stream only on a child and sits
# under "More output streams" otherwise.
_PARENT_ONLY_SECTIONS = frozenset({"extract_data"})
_CHILD_MAIN_SECTIONS = frozenset({"upscale_output"})
# Output-card titles for the sections split along the PARTIAL_OUTPUT_SECTIONS seam,
# whose glossary titles name the whole section (the Advanced BGC pane keeps those).
_OUTPUT_SECTION_TITLES: dict[str, str] = {
    "bgc": "BGC output",
    "marbl_bgc": "MARBL output (tracers & diagnostics to write)",
}
# Per-section note shown under the section header.
_SECTION_NOTES: dict[str, str] = {
    "extract_data": (
        "Child grid, extraction period and file come from the nesting options in "
        "the Grid card."
    ),
}


# What a table or grid shows: a field of its own section by name, or a field hosted
# from another section by its real ``(section, field)`` key (see _HOSTED_FIELDS).
_FieldKey = str | tuple[str, str]
# The diagnostics stream's compile-time switch (its other settings have no effect
# while it is off) and its T/S flags (they matter only for tracer diagnostics), named
# once for the table row below and for ``_apply_field_rules``.
_DIAGNOSTICS_SWITCH = ("cppdefs", "diagnostics")
_TS_DIAGNOSTICS_FLAGS = (("ts_output", "wrt_temp_dia"), ("ts_output", "wrt_salt_dia"))
# The ``implicit_fs`` settings read only with ``fs_mode`` other than split (``fs_verbose``
# also matters in split mode); named for ``_apply_field_rules``.
_IMPLICIT_FS_SOLVER_FIELDS = ("fs_nsub", "fs_rho_inf", "fs_tol")

# List settings long enough to need a wrapping text area, so the whole list shows.
_MULTILINE_LIST_FIELDS = frozenset(
    {
        ("marbl_bgc", "marbl_tracers_to_write"),
        ("marbl_bgc", "marbl_diagnostics_to_write"),
    }
)

# Output-stream tables for the settings editor: each namelist section
# that models one-or-more "write this stream / how often / how many records
# per file" triples renders those fields as a small table (see
# _SettingsEditor._build_section) instead of 3-6 flat rows apiece. Row order
# here is the table's row order. ``write``/``period``/``records`` are field
# names on the section's sub-model (None when the section has no single
# master write flag for that stream, e.g. surf_flux/diagnostics, whose
# per-variable or per-mode flags render elsewhere); ``extra`` lists any other
# small toggles for that stream (rendered as a checkbox/dropdown group in the
# table's last column). A row only forms when every field it names was
# actually built for the active settings_cls/include/exclude -- otherwise its
# fields fall through to the plain flat list unchanged (see
# test_ui_labels.py::test_output_table_fields_exist for schema coverage).
_OUTPUT_TABLES: dict[str, list[dict[str, Any]]] = {
    "ocean_vars": [
        {
            "label": "Instantaneous history",
            "write": "wrt_file_his",
            "period": "output_period_his",
            "records": "nrpf_his",
            "extra": [],
        },
        {
            "label": "Averages",
            "write": "wrt_file_avg",
            "period": "output_period_avg",
            "records": "nrpf_avg",
            "extra": [],
        },
        {
            "label": "Restarts",
            "write": "wrt_file_rst",
            "period": "output_period_rst",
            "records": "nrpf_rst",
            "extra": ["monthly_restarts"],
        },
    ],
    "bgc": [
        {
            "label": "BGC history",
            "write": "wrt_his",
            "period": "output_period_his",
            "records": "nrpf_his",
            "extra": [],
        },
        {
            "label": "BGC averages",
            "write": "wrt_avg",
            "period": "output_period_avg",
            "records": "nrpf_avg",
            "extra": [],
        },
        {
            "label": "BGC diagnostics history",
            "write": "wrt_his_dia",
            "period": "output_period_his_dia",
            "records": "nrpf_his_dia",
            "extra": [],
        },
        {
            "label": "BGC diagnostics averages",
            "write": "wrt_avg_dia",
            "period": "output_period_avg_dia",
            "records": "nrpf_avg_dia",
            "extra": [],
        },
    ],
    "cdr_output": [
        {
            "label": "CDR output",
            "write": "do_cdr_output",
            "period": "output_period",
            "records": "nrpf",
            "extra": ["do_avg", "monthly_averages"],
        },
    ],
    "surf_flux": [
        {
            "label": "Surface fluxes",
            "write": None,
            "period": "output_period",
            "records": "nrpf",
            "extra": ["sflx_avg"],
        },
    ],
    "diagnostics": [
        {
            "label": "Diagnostics",
            "write": None,
            "period": "output_period",
            "records": "nrpf",
            "extra": [
                _DIAGNOSTICS_SWITCH,
                "diag_avg",
                "diag_uv",
                "diag_trc",
                *_TS_DIAGNOSTICS_FLAGS,
            ],
        },
    ],
    "frc_output": [
        {
            "label": "Forcing fields",
            "write": "wrt_frc",
            "period": "output_period",
            "records": "nrpf",
            "extra": ["wrt_frc_avg"],
        },
    ],
    "upscale_output": [
        {
            "label": "Upscaled output",
            "write": "do_upscale",
            "period": "output_period_uscl",
            "records": "nrpf_uscl",
            "extra": [],
        },
    ],
    "random_output": [
        {
            "label": "Random output",
            "write": "do_random",
            "period": "output_period",
            "records": "nrpf",
            "extra": [],
        },
    ],
    "zslice": [
        {
            "label": "Depth slices",
            "write": "do_zslice",
            "period": "output_period",
            "records": "nrpf",
            "extra": ["zslice_avg"],
        },
    ],
    "sponge_tune": [
        {
            "label": "Sponge fields",
            "write": "wrt_sponge",
            "period": "output_period",
            "records": "nrpf",
            "extra": [],
        },
    ],
    "particles": [
        {
            "label": "Particle output",
            "write": "floats",
            "period": "output_period",
            "records": "nrpf",
            "extra": [],
        },
    ],
}

# Per-variable write-flag checkbox grids for the settings editor: a
# (title, field_names) pair renders as a small header plus a 4-column grid of
# checkboxes instead of one flat row per variable. As with _OUTPUT_TABLES, a
# grid only consumes the fields that were actually built; any missing field is
# simply omitted (a section can list more fields here than a given
# settings_cls models).
_VARIABLE_GRIDS: dict[str, list[tuple[str, list[_FieldKey]]]] = {
    "ocean_vars": [
        (
            "Variables in history files",
            [
                "wrt_z",
                "wrt_ub",
                "wrt_vb",
                "wrt_u",
                "wrt_v",
                ("ts_output", "wrt_temp"),
                ("ts_output", "wrt_salt"),
                "wrt_r",
                "wrt_o",
                "wrt_w",
                "wrt_akv",
                "wrt_akt",
                "wrt_aks",
                "wrt_hbls",
                "wrt_hbbl",
            ],
        ),
        (
            "Variables in average files",
            [
                "wrt_avg_z",
                "wrt_avg_ub",
                "wrt_avg_vb",
                "wrt_avg_u",
                "wrt_avg_v",
                ("ts_output", "wrt_avg_temp"),
                ("ts_output", "wrt_avg_salt"),
                "wrt_avg_r",
                "wrt_avg_o",
                "wrt_avg_w",
                "wrt_avg_akv",
                "wrt_avg_akt",
                "wrt_avg_aks",
                "wrt_avg_hbls",
                "wrt_avg_hbbl",
            ],
        ),
    ],
    "surf_flux": [
        ("Fluxes written", ["wrt_smflx", "wrt_stflx", "wrt_rstflx", "wrt_swflx"]),
    ],
    "zslice": [
        ("Fields written", ["wrt_t_zsl", "wrt_u_zsl", "wrt_v_zsl"]),
    ],
}


def _hosted_keys(section: str) -> tuple[tuple[str, str], ...]:
    """The ``(section, field)`` keys ``section``'s table rows and variable grids show
    from other sections: the tuple entries of :data:`_OUTPUT_TABLES`' ``extra`` lists
    and :data:`_VARIABLE_GRIDS`' key lists, the one place a hosted entry is told
    from a field of ``section`` itself.
    """
    entries = [
        *(k for row in _OUTPUT_TABLES.get(section, []) for k in row["extra"]),
        *(k for _title, keys in _VARIABLE_GRIDS.get(section, []) for k in keys),
    ]
    return tuple(dict.fromkeys(k for k in entries if isinstance(k, tuple)))


# Host section -> the fields of other sections it shows. Such a field is built with
# its host (see _SettingsEditor._build_section) yet keyed by its real
# ``(section, field)``, so overrides, sync and load-back never see where it is drawn.
_HOSTED_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    section: hosted
    for section in {*_OUTPUT_TABLES, *_VARIABLE_GRIDS}
    if (hosted := _hosted_keys(section))
}
_HOSTED_KEYS = frozenset(key for keys in _HOSTED_FIELDS.values() for key in keys)


# Output-table rows whose stream writes a record at the run's first step as well
# as one per period (ucla-roms basic_output.F90 and bgc_io.F90 write history at the
# first step; averages and the rest only once a period has passed).
_INITIAL_RECORD_PERIODS = frozenset(
    {
        ("ocean_vars", "output_period_his"),
        ("bgc", "output_period_his"),
        ("bgc", "output_period_his_dia"),
    }
)
# A _VARIABLE_GRIDS list of at least this many variables is a collapsible pane.
_VARIABLE_GRID_COLLAPSE_AT = 8
# (section, _VARIABLE_GRIDS title) -> the output-table Write flag of the stream
# that list belongs to, for the pane's "this stream is off" note.
_VARIABLE_GRID_STREAMS: dict[tuple[str, str], str] = {
    ("ocean_vars", "Variables in history files"): "wrt_file_his",
    ("ocean_vars", "Variables in average files"): "wrt_file_avg",
}


def _model_time_text(seconds: float) -> str:
    """``seconds`` in the largest whole unit (``"1 day"``, ``"6 hours"``,
    ``"90 minutes"``), or ``""`` when it is not a whole number of minutes.
    """
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60)):
        if seconds >= size and seconds % size == 0:
            n = int(seconds // size)
            return f"{n} {unit}{'' if n == 1 else 's'}"
    return ""


def _widget_number(widget: Any) -> float | None:
    """A number widget's value as a float, or ``None`` when it holds none."""
    try:
        return float(widget.value)
    except (TypeError, ValueError):
        return None


class _StreamReadout:
    """The readouts of one output-table row: its period in model time ("= 1 day")
    beside the period box and, once the wizard gives the run window
    (``_SettingsEditor.set_run_window``), how long each file spans and how many
    files the run writes. Both follow the row's widgets as they change, and say
    nothing while the period box is hidden or fixed by a monthly cadence; the file
    count also waits for a written stream with a records-per-file box.
    """

    def __init__(
        self,
        W: Any,
        period: Any,
        records: Any | None,
        write: Any | None,
        *,
        initial_record: bool,
    ) -> None:
        self.period, self.records, self.write = period, records, write
        self.initial_record = initial_record
        self.run_seconds: float | None = None
        self.period_html = W.HTML()
        self.period_html.add_class("forge-out-per")
        self.files_html = W.HTML()
        self.files_html.add_class("forge-out-files")
        for widget in (period, records, write):
            if widget is not None:
                widget.observe(self.render, names="value")
        period.observe(self.render, names="disabled")
        period.layout.observe(self.render, names="display")
        self.render()

    def render(self, _change: Any = None) -> None:
        period = _widget_number(self.period)
        if (
            period is None
            or period <= 0
            or self.period.disabled
            or self.period.layout.display == "none"
        ):
            self.period_html.value = self.files_html.value = ""
            return
        model_time = _model_time_text(period)
        self.period_html.value = f"= {model_time}" if model_time else ""
        self.files_html.value = self._files_text(period)

    def _files_text(self, period: float) -> str:
        n_records = _widget_number(self.records) if self.records is not None else None
        if (
            not n_records
            or n_records < 1
            or not self.run_seconds
            or (self.write is not None and not self.write.value)
        ):
            return ""
        per_file = int(n_records)
        span = period * per_file
        records = int(self.run_seconds // period) + (1 if self.initial_record else 0)
        files = max(1, -(-records // per_file))  # ceiling division
        held = _model_time_text(span) or f"{span:g} s"
        return (
            f"each file holds {held} · {files} file{'' if files == 1 else 's'} "
            "over this run"
        )


def _as_table_cell(widget, *, width: str | None = None, css_class: str | None = None):
    """Prepare an already-built settings widget for a table cell.

    Clears its own ``description`` (the row label column names the stream) and
    optionally pins a width / adds a CSS class -- one place for the tweak every
    Write/Period/Records cell needs, so the columns cannot drift apart.
    """
    widget.description = ""
    if width is not None:
        widget.layout.width = width
    if css_class is not None:
        widget.add_class(css_class)
    return widget


def _build_output_table(
    W,
    section: str,
    built: dict[_FieldKey, Any],
    consumed: set[_FieldKey],
    readouts: list[_StreamReadout] | None = None,
) -> Any | None:
    """A ``W.GridBox`` (class ``forge-out-table``) collecting ``section``'s
    eligible :data:`_OUTPUT_TABLES` rows into one Write / Period / Records
    table, or ``None`` if no row qualifies.

    A row qualifies when its ``period`` field (and its ``write`` flag, when
    it names one) is present in ``built`` -- i.e. was actually constructed
    for the active ``settings_cls``/include/exclude (see
    ``_SettingsEditor._build_section``); a missing ``records``/``extra`` field
    just leaves that cell empty.
    Qualifying rows have their keys added to ``consumed`` so the caller's
    plain flat-list fallback skips them. Widgets are REUSED, never rebuilt:
    the write/period/records widget's ``description``/``layout.width`` are
    tweaked in place (registry identity in ``self._widgets`` is untouched).
    Each row's :class:`_StreamReadout` is appended to ``readouts``.
    """
    # A row qualifies when its period field (and its write flag, if it names
    # one) was built; records/extra cells are optional because some fields
    # only exist for certain settings tiers (e.g. ocean_vars.nrpf_rst is
    # pre-0.5.0 only) -- a missing one leaves that cell empty.
    rows = [
        row
        for row in _OUTPUT_TABLES.get(section, [])
        if row["period"] in built and (row["write"] is None or row["write"] in built)
    ]
    if not rows:
        return None

    header_cells = [
        W.HTML("&nbsp;"),
        W.HTML("Write"),
        W.HTML("Period (s)"),
        W.HTML("Records / file"),
        W.HTML("&nbsp;"),
    ]
    for cell in header_cells:
        cell.add_class("forge-out-th")
    header_cells[1].add_class("forge-out-center")
    header_cells[4].add_class("forge-out-x")
    cells: list[Any] = list(header_cells)

    for row in rows:
        write_key, period_key, records_key, extra_keys = (
            row["write"],
            row["period"],
            row["records"],
            row["extra"],
        )
        symbols = " · ".join(
            k for k in (write_key, period_key, records_key) if k and k in built
        )
        label_cell = W.HTML(f"{row['label']}<span class='sym'>{symbols}</span>")
        label_cell.add_class("forge-out-label")
        cells.append(label_cell)

        present_extras = [k for k in extra_keys if k in built]
        if write_key is not None:
            write_widget = _as_table_cell(
                built[write_key], css_class="forge-out-center"
            )
            cells.append(write_widget)
            consumed.add(write_key)
        else:
            # No single write switch (surface fluxes, diagnostics): the row's
            # labelled flags ARE the write column, stacked, so nothing lands
            # in the unlabelled extras cell.
            write_cell = W.VBox([built[k] for k in present_extras])
            write_cell.add_class("forge-out-write-stack")
            cells.append(write_cell)
            consumed.update(present_extras)
            present_extras = []

        has_records = records_key is not None and records_key in built
        readout = _StreamReadout(
            W,
            built[period_key],
            built[records_key] if has_records else None,
            built[write_key] if write_key is not None else None,
            initial_record=(section, period_key) in _INITIAL_RECORD_PERIODS,
        )
        if readouts is not None:
            readouts.append(readout)
        period_cell = W.HBox(
            [_as_table_cell(built[period_key], width="96px"), readout.period_html]
        )
        period_cell.add_class("forge-out-period")
        cells.append(period_cell)
        consumed.add(period_key)

        if has_records:
            cells.append(_as_table_cell(built[records_key], width="120px"))
            consumed.add(records_key)
        else:
            cells.append(W.HTML("<span class='sym'>—</span>"))

        extras = [built[k] for k in present_extras]
        for extra in extras:
            if not isinstance(extra, W.Checkbox):
                # The label sizes to its text (and ellipsises in the cell).
                extra.style.description_width = "initial"
        consumed.update(present_extras)
        extras_cell = W.HBox([readout.files_html, *extras])
        # In a narrow card this cell moves to a line of its own (see WIZARD_CSS).
        extras_cell.add_class("forge-out-x")
        cells.append(extras_cell)

    # The column template is the stylesheet's (`forge-out-table`), so a narrow
    # card can change it.
    table = W.GridBox(
        cells,
        layout=W.Layout(grid_gap="0 12px", align_items="center"),
    )
    table.add_class("forge-out-table")
    return table


def _build_variable_grids(
    W, section: str, built: dict[_FieldKey, Any], consumed: set[_FieldKey]
) -> list[Any]:
    """A ``W.GridBox`` of checkboxes (class ``forge-var-grid``) per
    :data:`_VARIABLE_GRIDS` entry with at least one field present in ``built``:
    a list of :data:`_VARIABLE_GRID_COLLAPSE_AT` or more variables as a closed
    collapsible pane titled with a live "n of m selected" (and a note while its
    stream, :data:`_VARIABLE_GRID_STREAMS`, is not written), a shorter one under a
    small title, as a flat list of widgets.

    Only the fields actually present are rendered (a grid can list more
    fields than a given ``settings_cls`` models); consumed keys are added to
    ``consumed`` so the caller's flat-list fallback skips them. Checkboxes
    keep their existing widget objects and glossary descriptions unchanged.
    """
    out: list[Any] = []
    for title, keys in _VARIABLE_GRIDS.get(section, []):
        present = [k for k in keys if k in built and k not in consumed]
        if not present:
            continue
        boxes = [built[k] for k in present]
        # Columns come from the stylesheet (`forge-var-grid`): fewer in a narrow card.
        grid = W.GridBox(boxes, layout=W.Layout(grid_gap="2px 16px"))
        grid.add_class("forge-var-grid")
        consumed.update(present)
        if len(boxes) < _VARIABLE_GRID_COLLAPSE_AT:
            out.append(W.HTML(f"<div class='forge-var-title'>{title}</div>"))
            out.append(grid)
            continue
        write_key = _VARIABLE_GRID_STREAMS.get((section, title))
        write = built.get(write_key) if write_key is not None else None
        pane = components.collapsible(W, grid, title)

        def _retitle(
            _change: Any = None, pane=pane, title=title, boxes=boxes, write=write
        ) -> None:
            n = sum(bool(box.value) for box in boxes)
            pills = [components.Pill(f"{n} of {len(boxes)} selected")]
            if write is not None and not write.value:
                pills.append(components.Pill("not written: this stream is off", "off"))
            pane.set_title(components.accordion_title(title, *pills))

        for widget in (*boxes, write):
            if widget is not None:
                widget.observe(_retitle, names="value")
        _retitle()
        out.append(pane)
    return out


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
# has no widget anywhere, except ``diagnostics``, which the Output card's diagnostics
# row hosts; leaving it out of both sets keeps it that way even if a future pane adds
# "cppdefs" without remembering to scope it down.
_CPPDEFS_PANE_FIELDS: dict[str, frozenset[str]] = {
    "Physics & subgrid tuning": frozenset(
        {"sponge_tune", "parabolic_splines", "upstream_ts_land_curv"}
    ),
    "Biogeochemistry (BGC / MARBL)": frozenset({"nhy_forcing", "nox_forcing"}),
}


# The one ``param`` field the accordion shows (in the CDR pane): the passive tracer
# count, which generation checks against the CDR forcing's passive tracers and
# nothing else sets. The grid/tiling dims have dedicated widgets, ``ntrc_bio`` is
# set from the BGC mode, and ``nt_cdr_oae``/``nt_cdr_dor`` are read off the CDR
# forcing's tracer axis when it names its tracers. Display-only (unlike
# _ACCORDION_EXCLUDED_FIELDS): an authored CDR tracer count, needed when the
# forcing file does not name its tracers, still round-trips as an override.
_PARAM_PANE_FIELDS = frozenset({"nt_passive"})


def _split_fields(
    category_title: str, section: str
) -> tuple[frozenset | None, frozenset]:
    """Field filter ``(include, exclude)`` for a section shown in ``category_title``
    (an Advanced pane title, :data:`_OUTPUT_CARD` or :data:`_MODEL_CARD`).

    ``include=None`` means "all fields"; otherwise only those in ``include`` are
    shown. ``exclude`` is always dropped. Sections split across containers (``bgc`` /
    ``marbl_bgc`` via :data:`PARTIAL_OUTPUT_SECTIONS`) keep their output write-
    controls in the Output card and the rest under their feature pane. ``cppdefs`` is
    split the same way via :data:`_CPPDEFS_PANE_FIELDS` -- each pane only sees the
    handful of compile-time flags it owns -- and ``param`` shows only
    :data:`_PARAM_PANE_FIELDS`. The fields of :data:`_MODEL_CARD_FIELDS` show in the
    Model card only, and those of :data:`_FORCING_CARD_FIELDS` in the Forcing card
    only.
    """
    model_card = _MODEL_CARD_FIELDS.get(section, frozenset())
    forcing_card = _FORCING_CARD_FIELDS.get(section, frozenset())
    if category_title == _MODEL_CARD:
        return model_card, frozenset()
    if category_title == _FORCING_CARD:
        return forcing_card, frozenset()
    include: frozenset | None = None
    exclude: frozenset = frozenset()
    parts = PARTIAL_OUTPUT_SECTIONS.get(section)
    if section == "cppdefs":
        include = _CPPDEFS_PANE_FIELDS.get(category_title, frozenset())
    elif section == "param":
        include = _PARAM_PANE_FIELDS
    elif parts is not None and category_title == _OUTPUT_CARD:
        include = frozenset(parts)
    elif parts is not None:
        exclude = frozenset(parts)
    return include, exclude | model_card | forcing_card


def _in_output_card(section: str, field: str | None) -> bool:
    """Whether the ``(section, field)`` setting is placed in the Output card (main or
    "More output streams"), nesting role aside -- the one placement predicate behind
    the Output chip's count, so the Output and Advanced chips partition the overrides.
    Distinct from ``_is_output_key``, which is OutputSpec ownership. A field hosted
    by an Output-card section (see :data:`_HOSTED_FIELDS`) is placed there too.
    """
    if (section, field) in _HOSTED_KEYS:
        return True
    if section not in _OUTPUT_MAIN_SECTIONS and section not in _OUTPUT_MORE_SECTIONS:
        return False
    include, exclude = _split_fields(_OUTPUT_CARD, section)
    return (include is None or field in include) and field not in exclude


def _in_model_card(section: str, field: str | None) -> bool:
    """Whether the ``(section, field)`` setting is placed in the Model card (see
    :data:`_MODEL_CARD_FIELDS`).
    """
    return field in _MODEL_CARD_FIELDS.get(section, frozenset())


def _in_forcing_card(section: str, field: str | None) -> bool:
    """Whether the ``(section, field)`` setting is placed in the Forcing card (see
    :data:`_FORCING_CARD_FIELDS`).
    """
    return field in _FORCING_CARD_FIELDS.get(section, frozenset())


def _output_placement(
    *, is_parent: bool, is_child: bool
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The Output card's ``(main, more)`` sections for a grid's nesting role -- the
    only place the nesting role shapes the editor (see ``_PARENT_ONLY_SECTIONS`` /
    ``_CHILD_MAIN_SECTIONS``).
    """
    main = tuple(
        section
        for section in _OUTPUT_MAIN_SECTIONS
        if (is_parent or section not in _PARENT_ONLY_SECTIONS)
        and (is_child or section not in _CHILD_MAIN_SECTIONS)
    )
    demoted = tuple(s for s in _OUTPUT_MAIN_SECTIONS if s in _CHILD_MAIN_SECTIONS)
    return main, (*_OUTPUT_MORE_SECTIONS, *(() if is_child else demoted))


# Each CDR output stream's master enable flag -- section name -> the field that
# turns the whole stream on/off. ``_apply_field_rules``/``_register_field_rule_
# observers`` loop over this table so ``cdr_output`` (ucla-roms's original
# stream), ``cdr_lite_output``, and ``cdr_gas_exch_output`` (both added by
# ucla-roms PR #351, >= 0.7.0) behave identically: every other field of the
# section (do_avg/monthly_averages/output_period/nrpf, and any stream-specific
# extras such as cdr_lite_output's ``wrt_*`` field-group toggles) hides with
# the master switch, so a new field on a Cfg model is covered without touching
# this file. Extend here -- not by hand in either method -- when ucla-roms adds
# another CDR output stream.
_CDR_STREAM_MASTER_FLAGS: dict[str, str] = {
    "cdr_output": "do_cdr_output",
    "cdr_lite_output": "do_cdr_lite_output",
    "cdr_gas_exch_output": "do_cdr_gas_exch_output",
}


def _section_header_html(
    section: str, n_modified: int = 0, title: str | None = None
) -> str:
    """The header HTML over a settings section: its glossary title (or ``title``)
    and namelist symbol, plus an "N modified" chip once the wizard retitles it with
    a count.
    """
    if title is None:
        title = section_for(f"settings.{section}", default_title=section).title
    chip = (
        " " + components.chip(f"● {n_modified} modified", "info") if n_modified else ""
    )
    return (
        "<div class='forge-settings-sec'>"
        f"<span class='ttl'>{html.escape(title)}</span> "
        f"<span class='sym'>{section}</span>{chip}</div>"
    )


@dataclass(frozen=True)
class _EditorKey:
    """What a built settings editor depends on: it is rebuilt when any of these
    changes. A failed resolve knows all of them but builds an unresolved editor
    (see ``ForgeBlueprintWizard._show_unresolved_editor``), which the next valid
    resolve replaces.
    """

    model: str | None
    settings_cls: type[BaseModel]
    bgc_mode: str
    is_parent: bool
    is_child: bool
    resolved: bool
    """Built from the resolved settings, not the ModelSpec's Model card settings."""


class _SettingsEditor:
    """A collapsible (Accordion) editor over the *editable* model_settings sections,
    consolidated into modeler-facing category panes (see :data:`_ADVANCED_CATEGORIES`)
    plus the output streams the Output card shows (``output_box``, see
    :data:`_OUTPUT_MAIN_SECTIONS`) and the few settings the Model card shows
    (``model_box``, see :data:`_MODEL_CARD_FIELDS`) and the Forcing card shows
    (``forcing_box``, see :data:`_FORCING_CARD_FIELDS`).

    Each pane groups several namelist sections under a sub-header per section, so the
    grouping reads by ocean-modeling concern (physics, forcing, BGC, CDR)
    while every widget is still keyed by its real ``(section, field)`` -- one
    ``_widgets`` registry and one read/sync/on_edit path serve both containers.
    Auto-generates
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
    hiding the widget cannot drop or reset the value. ``is_parent`` / ``is_child``
    give the grid's nesting role, which only decides which output streams the
    Output card places where (see :func:`_output_placement`).
    """

    def __init__(
        self,
        W,
        model_settings: dict[str, Any],
        on_edit=None,
        settings_cls: type[BaseModel] = RunTimeSettings,
        *,
        bgc_mode: str = "marbl",
        is_parent: bool = False,
        is_child: bool = False,
    ):
        self.W = W
        self._settings_cls = settings_cls
        self._bgc_mode = bgc_mode
        self._forced_on = _FORCED_ON_BY_BGC_MODE.get(bgc_mode, {})
        # (section, field|None) -> (widget, base_type)
        self._widgets: dict[Any, Any] = {}
        # group -> {section: fields built there}. A group is an Advanced pane (by
        # title), one of the Output card's two (_OUTPUT_CARD, _OUTPUT_MORE_TITLE) or
        # the Model card (_MODEL_CARD); a
        # section split along PARTIAL_OUTPUT_SECTIONS (bgc/marbl_bgc) sits in a pane
        # and in the Output card, each holding only its own fields. A hosted field
        # (_HOSTED_FIELDS) is listed under its own section, in its host's group.
        self._group_fields: dict[str, dict[str, list[str | None]]] = {}
        # (group, host section) -> {guest section: fields it hosts there}, recorded
        # once by _build_group so a host's header counts its hosted fields.
        self._hosted: dict[tuple[str, str], dict[str, list[str | None]]] = {}
        # (group, section) -> the section's header widget, retitled by the wizard.
        self._headers: dict[tuple[str, str], Any] = {}
        # True while sync() is pushing composed values into widgets -- distinct from
        # the wizard's own _syncing flag, which guards the *wizard-level* on_edit
        # override recording. This one guards the editor-internal forcing rule (see
        # _register_field_rule_observers) so a sync-driven value push never mutates
        # a sibling widget the way a real user edit is allowed to.
        self._syncing_internal = False
        # One per output-table row, refreshed when the run window changes.
        self._readouts: list[_StreamReadout] = []
        panes, titles = [], []
        for title, members in _ADVANCED_CATEGORIES:
            blocks = self._build_group(title, members, model_settings)
            if not blocks:
                continue
            panes.append(W.VBox(blocks))
            titles.append(title)
        # Independently collapsible panes: opening one no longer closes another.
        self.accordion = components.open_accordion(W, panes, titles)
        main, more = _output_placement(is_parent=is_parent, is_child=is_child)
        main_blocks = self._build_group(_OUTPUT_CARD, main, model_settings)
        more_blocks = self._build_group(_OUTPUT_MORE_TITLE, more, model_settings)
        more_panes = [W.VBox(more_blocks)] if more_blocks else []
        self.more_streams = components.open_accordion(
            W, more_panes, [_OUTPUT_MORE_TITLE] * len(more_panes)
        )
        self.output_box = W.VBox([*main_blocks, self.more_streams])
        # The Model card's few settings, bare: the card's own subsection titles them.
        self.model_box = W.VBox(
            self._build_group(
                _MODEL_CARD, tuple(_MODEL_CARD_FIELDS), model_settings, headers=False
            )
        )
        # The Forcing card's (the constant atmospheric xCO2) as field rows, labelled
        # like the rest of its "Atmospheric CO2" block.
        self._build_group(
            _FORCING_CARD, tuple(_FORCING_CARD_FIELDS), model_settings, headers=False
        )
        self.forcing_box = W.VBox(
            [
                components.field_row(
                    W,
                    f"settings.{section}.{field}",
                    self._widgets[(section, field)][0],
                    width="120px",
                )
                for section, fields in self._group_fields.get(_FORCING_CARD, {}).items()
                for field in fields
            ]
        )
        # Before the on_edit observers attach, so locking one is never an edit.
        for key, why in self._forced_on.items():
            if key in self._widgets:
                widget = self._widgets[key][0]
                widget.value = True
                widget.disabled = True
                widget.tooltip = f"{widget.tooltip or ''} {why}".strip()
        if on_edit is not None:
            for (section, field), (widget, _base) in self._widgets.items():
                widget.observe(
                    lambda _ch, s=section, f=field: on_edit(s, f), names="value"
                )
        self._register_field_rule_observers()
        self._apply_field_rules()

    def set_run_window(self, seconds: float | None) -> None:
        """Give the output tables the run's length in seconds (``None`` when the run
        window is not set), for their file-count readouts.
        """
        for readout in self._readouts:
            if readout.run_seconds != seconds:
                readout.run_seconds = seconds
                readout.render()

    @property
    def _pane_sections(self) -> dict[str, list[str]]:
        """Advanced pane title -> the sections shown in it, in the order of (and
        index-aligned with) ``accordion``'s panes.
        """
        return {
            title: list(self._group_fields[title])
            for title, _members in _ADVANCED_CATEGORIES
            if title in self._group_fields
        }

    def header_html(self, group: str, section: str, n_modified: int = 0) -> str:
        """The header HTML of ``section`` in ``group``: the Output card titles the
        split sections itself, every other header uses the glossary's title.
        """
        title = _OUTPUT_SECTION_TITLES.get(section) if group in _OUTPUT_GROUPS else None
        return _section_header_html(section, n_modified, title)

    def _gated_out(self, section: str) -> bool:
        """Whether ``section`` is modeled by some registered settings tier (e.g.
        pio_settings on RunTimeSettingsV0_6_0) but not by the active settings_cls --
        e.g. a pre-0.6.0 roms_ref override with a ModelSpec whose model_settings
        still carries the key. Rendering a type-inferred widget there would let the
        user edit a value the active (extra="ignore") schema silently discards
        downstream. cppdefs is NOT in _VERSION_GATED_SECTIONS (no tier ever models
        it) and so is unaffected -- it keeps rendering via type-inference.
        """
        return (
            section not in self._settings_cls.model_fields
            and section in _VERSION_GATED_SECTIONS
        )

    def _build_group(
        self,
        group: str,
        members: tuple[str, ...],
        model_settings: dict[str, Any],
        *,
        headers: bool = True,
    ) -> list[Any]:
        """Header + (note) + widgets blocks for each of ``members`` that has editable
        fields, flat -- an Advanced pane's (``group`` is its title), the Output
        card's children (``group`` is one of :data:`_OUTPUT_GROUPS`) or the Model
        card's (:data:`_MODEL_CARD`, ``headers=False``: widgets only).
        """
        W = self.W
        place = _OUTPUT_CARD if group in _OUTPUT_GROUPS else group
        blocks: list[Any] = []
        for section in members:
            if section not in model_settings or self._gated_out(section):
                continue
            include, exclude = _split_fields(place, section)
            box, fields, hosted = self._build_section(
                section, model_settings, include, exclude
            )
            if not fields:
                continue
            group_fields = self._group_fields.setdefault(group, {})
            group_fields[section] = fields
            if hosted:
                self._hosted[(group, section)] = hosted
            for guest, guest_fields in hosted.items():
                group_fields.setdefault(guest, []).extend(guest_fields)
            if not headers:
                blocks.append(box)
                continue
            sec_header = W.HTML(self.header_html(group, section))
            # Widget-level class: the CSS draws a bold divider above every
            # section header except the container's first (:first-child).
            sec_header.add_class("forge-settings-sec-w")
            self._headers[(group, section)] = sec_header
            blocks.append(sec_header)
            if note := _SECTION_NOTES.get(section):
                blocks.append(W.HTML(f"<div class='forge-hint'>{note}</div>"))
            blocks.append(box)
        return blocks

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
                if (section, field) in self._forced_on:
                    value = True
                try:
                    labels = _BOOL_DROPDOWN_FIELDS.get((section, field))
                    if labels is not None and value is not None:
                        true_label, false_label = labels
                        widget.value = true_label if bool(value) else false_label
                    elif base is list:
                        # Only rewrite the text when the *parsed* list actually
                        # differs from the value being synced. A list field is a
                        # free-text Text widget, and every keystroke round-trips
                        # through on_edit -> _rebuild -> sync(); re-joining the
                        # parsed list on each pass would rewrite "a, b," back to
                        # "a, b" (the trailing separator parses to nothing),
                        # making it impossible to type a comma at the end of
                        # the field. Leaving the text alone when it already
                        # parses to `value` keeps the in-progress text intact
                        # while a genuine external change (model switch, load,
                        # override reset) still re-renders the field.
                        if _read_field_widget(widget, list) != list(value or []):
                            widget.value = ", ".join(str(x) for x in (value or []))
                    elif _is_optional_number_widget(widget, base):
                        widget.value = "" if value is None else str(value)
                    elif value is not None:
                        # A Dropdown refuses a value outside its options (a
                        # hand-edited blueprint); the validation reports it.
                        options = getattr(widget, "options", None)
                        if options is None or value in options:
                            widget.value = base(value)
                except (ValueError, TypeError):
                    pass
        finally:
            self._syncing_internal = False
        self._apply_field_rules()

    def read(self, section, field):
        widget, base = self._widgets[(section, field)]
        return _read_field_widget(widget, base, key=(section, field))

    # ocean_vars/CDR output-stream cross-field rules -----------------------------
    def _register_field_rule_observers(self) -> None:
        """Wire the controlling widgets to `_apply_field_rules` for instant
        show/hide + disable feedback, plus the one forcing rule (monthly
        restarts implies file restarts) on its own dedicated handler.
        """

        def _rerun(_change=None):
            self._apply_field_rules()

        keys = [
            ("ocean_vars", "wrt_file_rst"),
            ("ocean_vars", "monthly_restarts"),
            ("marbl_bgc", "use_constant_fe"),
            ("implicit_fs", "fs_mode"),
            _DIAGNOSTICS_SWITCH,
            ("diagnostics", "diag_trc"),
        ]
        for section, master_flag in _CDR_STREAM_MASTER_FLAGS.items():
            keys.append((section, master_flag))
            keys.append((section, "do_avg"))
            keys.append((section, "monthly_averages"))
        for key in keys:
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
        """Show/hide and enable/disable ocean_vars/CDR-output-stream widgets (and
        ``marbl_bgc.constant_fe_value``, the ``implicit_fs`` solver settings) from
        their CURRENT values -- never mutates a value, only visibility/``disabled``.

        Rationale: the dependent fields are meaningless (and ignored by the
        namelist) once their master switch is off, and `output_period_*` has no
        effect once a "monthly" cadence fixes the period implicitly -- hiding/
        disabling them keeps the form from offering a control with no effect.
        Applies identically to every stream in :data:`_CDR_STREAM_MASTER_FLAGS`
        (``cdr_output``, ``cdr_lite_output``, ``cdr_gas_exch_output``): every
        field of the section other than the master switch hides with it -- the
        shared do_avg/monthly_averages/output_period/nrpf quartet and any
        stream-specific extras (``cdr_lite_output``'s ``wrt_*`` field-group
        toggles), which are all meaningless once that stream is off.

        The diagnostics stream follows its compile-time switch the same way, when the
        ModelSpec declares one (:data:`_DIAGNOSTICS_SWITCH`): off, every
        ``diagnostics`` widget and the hosted T/S flags hide; on, the T/S flags
        (:data:`_TS_DIAGNOSTICS_FLAGS`) show only with ``diag_trc``. Without the
        switch nothing is hidden.
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

        use_fe = self._widgets.get(("marbl_bgc", "use_constant_fe"))
        if use_fe is not None:
            _show(
                self._widgets.get(("marbl_bgc", "constant_fe_value")),
                bool(use_fe[0].value),
            )

        if (fs_mode := self._widgets.get(("implicit_fs", "fs_mode"))) is not None:
            # fs_verbose also matters in split mode, so it stays.
            for solver_field in _IMPLICIT_FS_SOLVER_FIELDS:
                _show(
                    self._widgets.get(("implicit_fs", solver_field)),
                    fs_mode[0].value != FS_MODE_SPLIT,
                )

        for section, master_flag in _CDR_STREAM_MASTER_FLAGS.items():
            master = self._widgets.get((section, master_flag))
            if master is None:
                continue
            do_avg = self._widgets.get((section, "do_avg"))
            monthly_avg = self._widgets.get((section, "monthly_averages"))
            period = self._widgets.get((section, "output_period"))
            on = bool(master[0].value)
            # Every non-master field of the stream follows the master switch --
            # derived from the widgets actually built for the section (i.e. the
            # Cfg model's fields), not a hardcoded list, so a field added to a
            # stream's Cfg later is covered automatically.
            for (sec, field), entry in self._widgets.items():
                if sec == section and field != master_flag:
                    _show(entry, on)
            if on and do_avg is not None:
                is_avg = self.read(section, "do_avg")
                _show(monthly_avg, is_avg)
                if period is not None:
                    is_monthly = (
                        self.read(section, "monthly_averages")
                        if is_avg and monthly_avg is not None
                        else False
                    )
                    period[0].disabled = bool(is_avg and is_monthly)

        if (switch := self._widgets.get(_DIAGNOSTICS_SWITCH)) is not None:
            diag_on = bool(switch[0].value)
            for (sec, _field), entry in self._widgets.items():
                if sec == "diagnostics":
                    _show(entry, diag_on)
            trc = self._widgets.get(("diagnostics", "diag_trc"))
            for key in _TS_DIAGNOSTICS_FLAGS:
                _show(
                    self._widgets.get(key),
                    diag_on and trc is not None and bool(trc[0].value),
                )

    def _add_field(
        self, section: str, key: str, val: Any, sub: type[BaseModel] | None
    ) -> Any | None:
        """The widget for the ``(section, key)`` field, registered in ``_widgets``, or
        ``None`` when it gets no editor in the active ``settings_cls``/BGC mode.
        """
        if key in _ACCORDION_EXCLUDED_FIELDS.get(section, frozenset()):
            return None
        if not _applies_in(section, key, self._bgc_mode):
            return None
        # Skip metadata-name keys (*_vname/*_tname, cdb_min, ...): the settings
        # sub-models are extra="ignore", so anything not a typed field is dropped
        # by the namelist transform and has no business in the editor.
        if sub is not None and key not in sub.model_fields:
            return None
        ann = (
            sub.model_fields[key].annotation
            if (sub and key in sub.model_fields)
            else None
        )
        base = _base_type(ann, val)
        tip = _namelist_tooltip(self._settings_cls, section, key)
        label, tip = _namelist_description_and_tooltip(section, key, tip)
        bool_dropdown = _BOOL_DROPDOWN_FIELDS.get((section, key))
        w = _make_field_widget(
            self.W,
            label,
            base,
            val,
            tooltip=tip,
            bool_dropdown=bool_dropdown,
            choices=_literal_choices(ann),
            optional=_is_optional_numeric(ann),
            multiline=(section, key) in _MULTILINE_LIST_FIELDS,
        )
        self._widgets[(section, key)] = (w, base)
        return w

    def _build_section(
        self,
        section: str,
        model_settings: dict[str, Any],
        include: frozenset | None = None,
        exclude: frozenset = frozenset(),
    ):
        """``(box, fields, hosted)`` for ``section``: its widgets laid out, the fields
        built for it and, by section, the fields it hosts from others (see
        :data:`_HOSTED_FIELDS`).
        """
        W = self.W
        value = model_settings[section]
        sub = _section_submodel(section, self._settings_cls)
        if not isinstance(value, dict):  # scalar section (e.g. gamma2, ubind)
            if not _applies_in(section, None, self._bgc_mode):
                return W.VBox([]), [], {}
            base = _base_type(None, value)
            tip = _namelist_tooltip(self._settings_cls, section, section)
            label, tip = _namelist_description_and_tooltip(section, section, tip)
            w = _make_field_widget(W, label, base, value, tooltip=tip)
            self._widgets[(section, None)] = (w, base)
            return W.VBox([w]), [None], {}
        built: dict[_FieldKey, Any] = {}
        fields: list[str] = []
        for key, val in value.items():
            if key in exclude:
                continue
            if include is not None and key not in include:
                continue
            if (w := self._add_field(section, key, val, sub)) is not None:
                built[key] = w
                fields.append(key)

        # A hosted field is skipped like a native one when its own section/field is
        # absent, gated out or not applicable; a host that built nothing hosts nothing.
        hosted: dict[str, list[str | None]] = {}
        if built:
            for guest, key in _HOSTED_FIELDS.get(section, ()):
                guest_value = model_settings.get(guest)
                if (
                    self._gated_out(guest)
                    or not isinstance(guest_value, dict)
                    or key not in guest_value
                ):
                    continue
                guest_sub = _section_submodel(guest, self._settings_cls)
                w = self._add_field(guest, key, guest_value[key], guest_sub)
                if w is not None:
                    built[guest, key] = w
                    hosted.setdefault(guest, []).append(key)

        # Re-arrange (never re-build) the widgets just created: output-stream
        # tables and per-variable checkbox grids consume the widgets they
        # cover (by mutating only their description/layout, never their
        # identity or registry entry -- see _build_output_table/
        # _build_variable_grids), and whatever's left over renders as the
        # plain flat list, exactly as before this pane grouping existed.
        consumed: set[_FieldKey] = set()
        table = _build_output_table(W, section, built, consumed, self._readouts)
        grids = _build_variable_grids(W, section, built, consumed)
        rest = [w for key, w in built.items() if key not in consumed]
        body: list[Any] = []
        if table is not None:
            body.append(table)
        body.extend(grids)
        body.extend(rest)
        return W.VBox(body), fields, hosted
