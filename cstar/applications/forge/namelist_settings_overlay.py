"""
C-Star Forge's authoring-vocabulary overlay on :mod:`cstar.roms.namelist_keys`.

Forge does not get a second copy of the namelist fact table. Every group in
:data:`~cstar.roms.namelist_keys.NAMELIST_KEYS` has an entry here recording
only what's genuinely Forge's own:

* :attr:`GroupOverlay.settings_sections` — the ``RunTimeSettings`` field
  name(s) whose Pydantic fields carry this group's values (a tuple because a
  few groups are assembled from more than one Forge section, e.g.
  ``simulation_name_settings`` from ``title`` + ``output_root_name``; empty
  when the key is a bare top-level ``RunTimeSettings`` field rather than a
  nested section, e.g. ``gamma2``/``ubind``).
* :attr:`GroupOverlay.renames` — Forge field name -> Fortran key, for fields
  whose value crosses over unchanged under a different name via a real
  Pydantic ``serialization_alias``.
* :attr:`GroupOverlay.hand_mapped` — the same idea, but where
  ``build_namelist`` performs the rename in Python (an explicit constructor
  kwarg), not via ``serialization_alias``. See the module docstring note
  below — this is a real gap, not modeled the same way as `renames`.
* :attr:`GroupOverlay.pending` — Forge field names that are ``Optional`` /
  `None`-defaulted because input generation fills them in later (grid file,
  IC file, s-coord, forcing paths, casename, output root).
* :attr:`GroupOverlay.required_in_forge` — field names required on the Forge
  side despite carrying a C-Star reference default, because forge's ModelSpec
  YAML is the only source of defaults for that section (deliberate looseness
  reversal — see :mod:`cstar.roms.namelist_keys` group note in the design doc
  for ``marbl_bgc``).
* :attr:`GroupOverlay.synthetic` — Forge-only fields with **no** namelist-key
  counterpart at all: consumed structurally in ``build_namelist`` (regrouped,
  expanded to a per-tracer array, or cross-referenced into a different
  group's key) rather than mapped 1:1.

A group whose Forge section field names are already identical to the
namelist keys still gets a row (for the section <-> group name association
alone) — the "zero rows" case the design describes is about *adding a new
key* needing no overlay change, not about a group having no row at all.

**Known gap** (flagged for maintainer review, not fixed here): two renames
happen by hand in ``build_namelist`` with no ``serialization_alias`` behind
them at all -- ``vertical_mixing.akv`` -> ``vertical_mixing_settings.akv_bak``
and ``title.casename`` -> ``simulation_name_settings.title``. The design's
overlay text says the rename map "doubles as ``serialization_alias``"; these
two don't have one, so they're recorded separately in `hand_mapped` rather
than folded into `renames` (which the equivalence test verifies against a
real alias). See the PR notes for this migration step.

This module must not be imported by anything under ``cstar.roms`` — it is a
one-way overlay onto the C-Star table, never the other way round.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from cstar.roms.namelist_keys import REQUIRED


@dataclass(frozen=True)
class SyntheticField:
    """A Forge-only settings field with no namelist-key counterpart.

    Parameters
    ----------
    name : str
        The Forge field name (as declared on its settings-section class).
    type : type or str
        Its python type, in the same real-type-or-string form as
        :attr:`~cstar.roms.namelist_keys.NamelistKey.type`.
    default : Any
        Its Pydantic default, or :data:`~cstar.roms.namelist_keys.REQUIRED`.
    note : str
        One line on how ``build_namelist`` actually uses this field.
    """

    name: str
    type: type | str
    default: Any = REQUIRED
    note: str = ""


@dataclass(frozen=True)
class GroupOverlay:
    """Forge's authoring-vocabulary overlay for one C-Star namelist group.

    Parameters
    ----------
    group : str
        The :attr:`~cstar.roms.namelist_keys.NamelistKey.group` this overlays.
    settings_sections : tuple[str, ...]
        The ``RunTimeSettings`` field name(s) carrying this group's fields.
        Empty when the key is a bare top-level scalar field instead of a
        nested section (``gamma2``, ``ubind``).
    renames : Mapping[str, str]
        Forge field name -> Fortran key, for fields with a real
        ``serialization_alias`` different from the field name.
    hand_mapped : Mapping[str, str]
        Forge field name -> Fortran key, for fields renamed by hand in
        ``build_namelist`` rather than via ``serialization_alias`` (see the
        module docstring's "Known gap" note).
    pending : frozenset[str]
        Forge field names that are ``Optional``/`None`-defaulted pending
        values filled in later by input generation.
    required_in_forge : frozenset[str]
        Forge field names that are required despite the C-Star table
        recording a default for the same key (the YAML is Forge's only
        source of defaults for that section).
    synthetic : tuple[SyntheticField, ...]
        Forge-only fields with no namelist-key counterpart at all.
    """

    group: str
    settings_sections: tuple[str, ...] = ()
    renames: Mapping[str, str] = field(default_factory=dict)
    hand_mapped: Mapping[str, str] = field(default_factory=dict)
    pending: frozenset[str] = frozenset()
    required_in_forge: frozenset[str] = frozenset()
    synthetic: tuple[SyntheticField, ...] = ()


NAMELIST_SETTINGS_OVERLAY: tuple[GroupOverlay, ...] = (
    # simulation_name_settings <- title + output_root_name (regroup; casename
    # is hand-mapped to `title`, not aliased -- see module docstring).
    GroupOverlay(
        group="simulation_name_settings",
        settings_sections=("title", "output_root_name"),
        hand_mapped={"casename": "title"},
        pending=frozenset({"casename", "output_root_name"}),
    ),
    GroupOverlay(
        group="time_stepping",
        settings_sections=("time_stepping",),
    ),
    GroupOverlay(
        group="reference_date_settings",
        settings_sections=("reference_date_settings",),
    ),
    GroupOverlay(
        group="grid_settings",
        settings_sections=("grid",),
        renames={"grid_file": "grdname"},
        pending=frozenset({"grid_file"}),
    ),
    GroupOverlay(
        group="s_coord",
        settings_sections=("s_coord",),
        renames={"tcline": "hc"},
        pending=frozenset({"theta_s", "theta_b", "tcline"}),
    ),
    GroupOverlay(
        group="param_settings",
        settings_sections=("param",),
        renames={"n": "nz", "ntrc_bio": "nt_bgc"},
    ),
    GroupOverlay(
        group="initial_conditions",
        settings_sections=("initial",),
        renames={"initial_file": "inifile"},
        pending=frozenset({"initial_file"}),
    ),
    # forcing_files <- forcing: every field is consumed structurally (one
    # frcfiles list entry each, in _FORCING_ORDER) -- none maps to a key.
    GroupOverlay(
        group="forcing_files",
        settings_sections=("forcing",),
        pending=frozenset(
            {
                "surface_forcing_path",
                "boundary_forcing_path",
                "tidal_forcing_path",
                "river_path",
            }
        ),
        synthetic=(
            SyntheticField(
                "surface_forcing_path",
                "str | None",
                default=None,
                note="one frcfiles entry, if set (_FORCING_ORDER)",
            ),
            SyntheticField(
                "surface_forcing_bgc_path",
                "list[str]",
                default=(),
                note="sorted, appended as frcfiles entries (_FORCING_ORDER)",
            ),
            SyntheticField(
                "boundary_forcing_path",
                "str | None",
                default=None,
                note="one frcfiles entry, if set (_FORCING_ORDER)",
            ),
            SyntheticField(
                "boundary_forcing_bgc_path",
                "list[str]",
                default=(),
                note="sorted, appended as frcfiles entries (_FORCING_ORDER)",
            ),
            SyntheticField(
                "tidal_forcing_path",
                "str | None",
                default=None,
                note="one frcfiles entry, if set (_FORCING_ORDER)",
            ),
            SyntheticField(
                "river_path",
                "str | None",
                default=None,
                note="one frcfiles entry, if set (_FORCING_ORDER)",
            ),
        ),
    ),
    # surf_frc_settings <- blk_frc + flux_frc (regroup).
    GroupOverlay(
        group="surf_frc_settings",
        settings_sections=("blk_frc", "flux_frc"),
        renames={"interp_frc": "interp_bulk_frc"},
    ),
    GroupOverlay(
        group="river_frc_settings",
        settings_sections=("river_frc",),
        renames={"analytical": "river_analytical"},
    ),
    GroupOverlay(
        group="tidal_frc_settings",
        settings_sections=("tides",),
    ),
    GroupOverlay(
        group="basic_output_settings",
        settings_sections=("ocean_vars",),
    ),
    GroupOverlay(
        group="ts_output_settings",
        settings_sections=("ts_output",),
    ),
    GroupOverlay(
        group="frc_output_settings",
        settings_sections=("frc_output",),
        renames={"output_period": "output_period_frc", "nrpf": "nrpf_frc"},
    ),
    GroupOverlay(
        group="extract_data_settings",
        settings_sections=("extract_data",),
        renames={
            "nrpf": "nrpf_extract",
            "extract_period": "output_period_extract",
        },
    ),
    GroupOverlay(
        group="sponge_tune_settings",
        settings_sections=("sponge_tune",),
        renames={
            "spn_avg": "sponge_avg",
            "sp_timscale": "sponge_timescale",
            "nrpf": "nrpf_sponge",
            "output_period": "output_period_sponge",
        },
    ),
    GroupOverlay(
        group="calc_pflx_settings",
        settings_sections=("calc_pflx",),
        renames={"timescale": "pflx_timescale"},
    ),
    GroupOverlay(
        group="zslice_settings",
        settings_sections=("zslice",),
        renames={
            "wrt_t_zsl": "wrt_t_zslice",
            "wrt_u_zsl": "wrt_u_zslice",
            "wrt_v_zsl": "wrt_v_zslice",
            "output_period": "output_period_zslice",
            "nrpf": "nrpf_zslice",
            "nt_z": "nt_zslice",
        },
    ),
    GroupOverlay(
        group="bgc_settings",
        settings_sections=("bgc",),
        renames={
            "interp_frc": "interp_bgc_frc",
            "wrt_his": "wrt_bgc_his",
            "output_period_his": "output_period_bgc_his",
            "nrpf_his": "nrpf_bgc_his",
            "wrt_avg": "wrt_bgc_avg",
            "output_period_avg": "output_period_bgc_avg",
            "nrpf_avg": "nrpf_bgc_avg",
            "wrt_his_dia": "wrt_bgc_dia_his",
            "output_period_his_dia": "output_period_bgc_his_dia",
            "nrpf_his_dia": "nrpf_bgc_his_dia",
            "wrt_avg_dia": "wrt_bgc_dia_avg",
            "output_period_avg_dia": "output_period_bgc_avg_dia",
            "nrpf_avg_dia": "nrpf_bgc_avg_dia",
        },
    ),
    GroupOverlay(
        group="marbl_biogeochemistry_settings",
        settings_sections=("marbl_bgc",),
        required_in_forge=frozenset(
            {
                "marbl_config_file",
                "marbl_tracers_to_write",
                "marbl_diagnostics_to_write",
                "marbl_timestep",
            }
        ),
    ),
    GroupOverlay(
        group="cdr_frc_settings",
        settings_sections=("cdr_frc",),
        renames={
            "ncdr_parm": "cdr_ncdr_parm",
            "nz_chd": "cdr_nz_chd",
            "forcing_depth_profiles": "cdr_forcing_depth_profiles",
            "forcing_3d": "cdr_forcing_3d",
            "forcing_parameterized": "cdr_forcing_parameterized",
            "time_interpolation": "cdr_time_interpolation",
            "relocate_to_wet_pts": "cdr_relocate_to_wet_pts",
        },
    ),
    GroupOverlay(
        group="cdr_output_settings",
        settings_sections=("cdr_output",),
        renames={
            "do_avg": "wrt_cdr_avg",
            "monthly_averages": "cdr_monthly_averages",
            "output_period": "output_period_cdr",
            "nrpf": "nrpf_cdr",
        },
    ),
    GroupOverlay(
        group="upscale_settings",
        settings_sections=("upscale_output",),
    ),
    GroupOverlay(
        group="lin_rho_eos_settings",
        settings_sections=("lin_rho_eos",),
    ),
    # rho0_settings <- lateral_visc.rho0: a cross-section read in
    # build_namelist, not this group's own section (see lateral_visc_settings
    # below, where `rho0` is the synthetic field).
    GroupOverlay(
        group="rho0_settings",
        settings_sections=(),
        synthetic=(
            SyntheticField(
                "rho0",
                float,
                note="cross-section read from lateral_visc.rho0 in build_namelist",
            ),
        ),
    ),
    GroupOverlay(
        group="gamma2_settings",
        settings_sections=(),
    ),
    # tracer_diff2 <- tracer_diff2.tnu2_default: entirely synthetic (expanded
    # to n_tracers copies as tnu2 in build_namelist).
    GroupOverlay(
        group="tracer_diff2",
        settings_sections=("tracer_diff2",),
        synthetic=(
            SyntheticField(
                "tnu2_default",
                float,
                note="expanded to n_tracers copies as tracer_diff2.tnu2 in build_namelist",
            ),
        ),
    ),
    GroupOverlay(
        group="bottom_drag_settings",
        settings_sections=("bottom_drag",),
    ),
    # vertical_mixing_settings <- vertical_mixing: akv is hand-mapped to
    # akv_bak (no serialization_alias -- see module docstring); akt_default
    # is synthetic (expanded to akt_bak).
    GroupOverlay(
        group="vertical_mixing_settings",
        settings_sections=("vertical_mixing",),
        hand_mapped={"akv": "akv_bak"},
        synthetic=(
            SyntheticField(
                "akt_default",
                float,
                note="expanded to n_tracers copies as vertical_mixing_settings.akt_bak in build_namelist",
            ),
        ),
    ),
    # lateral_visc_settings <- lateral_visc: visc2 maps directly; rho0 is a
    # synthetic extra consumed by the *different* group rho0_settings (see
    # above).
    GroupOverlay(
        group="lateral_visc_settings",
        settings_sections=("lateral_visc",),
        synthetic=(
            SyntheticField(
                "rho0",
                float,
                note="feeds rho0_settings.rho0, not a lateral_visc_settings key",
            ),
        ),
    ),
    GroupOverlay(
        group="ubind_settings",
        settings_sections=(),
    ),
    GroupOverlay(
        group="v_sponge_settings",
        settings_sections=("v_sponge",),
    ),
    GroupOverlay(
        group="sss_correction",
        settings_sections=("sss_correction",),
    ),
    GroupOverlay(
        group="sst_correction",
        settings_sections=("sst_correction",),
    ),
    GroupOverlay(
        group="dic_alk_correction",
        settings_sections=("dic_alk_correction",),
    ),
    GroupOverlay(
        group="diagnostics_settings",
        settings_sections=("diagnostics",),
        renames={"output_period": "output_period_diag", "nrpf": "nrpf_diag"},
    ),
    GroupOverlay(
        group="stdout_diag_settings",
        settings_sections=("stdout_diag",),
    ),
    GroupOverlay(
        group="random_output_settings",
        settings_sections=("random_output",),
        renames={"output_period": "output_period_random", "nrpf": "nrpf_random"},
    ),
    GroupOverlay(
        group="surf_flx_output_settings",
        settings_sections=("surf_flux",),
        renames={"output_period": "output_period_sflx", "nrpf": "nrpf_sflx"},
    ),
    GroupOverlay(
        group="pipe_frc_settings",
        settings_sections=("pipe_frc",),
    ),
    GroupOverlay(
        group="particles_settings",
        settings_sections=("particles",),
        renames={
            # only true from RunTimeSettingsV0_5_0 on (ParticlesCfgV0_5_0);
            # ParticlesCfg (< 0.5.0) keeps output_period/nrpf unaliased.
            "output_period": "output_period_particles",
            "nrpf": "nrpf_particles",
        },
    ),
    GroupOverlay(
        group="pio_settings",
        settings_sections=("pio_settings",),
    ),
    GroupOverlay(
        group="cdr_tracer_output_settings",
        settings_sections=("cdr_tracer_output",),
        renames={
            "do_avg": "wrt_cdr_trc_avg",
            "monthly_averages": "cdr_trc_monthly_averages",
            "output_period": "output_period_cdr_trc",
            "nrpf": "nrpf_cdr_trc",
        },
    ),
    GroupOverlay(
        group="cdr_gas_exch_output_settings",
        settings_sections=("cdr_gas_exch_output",),
        renames={
            "do_avg": "wrt_cdr_gas_avg",
            "monthly_averages": "cdr_gas_monthly_averages",
            "output_period": "output_period_cdr_gas",
            "nrpf": "nrpf_cdr_gas",
        },
    ),
)
