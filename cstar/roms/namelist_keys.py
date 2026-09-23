"""
The single source of truth for every ROMS ``namelist.nml`` fact C-Star and
C-Star Forge care about: group, key, type, reference default, docstring,
ucla-roms version window, and constraint.

This is step 1 of the table-driven namelist schema migration (see
``namelist-tiers-design.md``). Today it is a **read-only fact table**,
transcribed by hand from the existing hand-written classes in
:mod:`cstar.roms.namelist` (the ``_NmlGroup`` subclasses and the
``RomsNamelist*`` tiers) — it changes nothing about how those classes behave.
:mod:`cstar.tests.unit_tests.roms.test_namelist_keys` proves the table and the
existing classes agree; the classes remain authoritative until a later
migration step points generated code at this table instead (see that design
doc's step 3 onward).

C-Star Forge's settings vocabulary does **not** get a second copy of this
table. It gets an overlay, :mod:`cstar.applications.forge.namelist_settings_overlay`,
recording only genuine differences from this table (settings-section names,
renamed/pending/forge-only fields) — this module has no knowledge of forge and
must never import anything under ``cstar.applications``.

Rows are written by hand from reading the source classes, in the same group
order as :class:`~cstar.roms.namelist.RomsNamelistBase` (which itself matches
``write_roms_namelist`` / the reference namelist), and the same field order as
each group class — order matters: ``model_dump()`` order is the ``f90nml``
write order the golden fixtures pin byte-for-byte. Do **not** populate this
table by introspecting the classes at import time; that would make it a
mirror of the classes rather than an independent fact base the equivalence
test can check them against.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final


class _RequiredType:
    """Sentinel marking a :class:`NamelistKey` with no default — the ucla-roms
    namelist has no Fortran initializer for it, so the group can't be
    constructed (or the namelist read) without an explicit value.
    """

    def __repr__(self) -> str:
        return "REQUIRED"


REQUIRED: Final = _RequiredType()


@dataclass(frozen=True)
class NamelistKey:
    """One fact about one key in one ``&group`` of the ROMS ``namelist.nml``.

    Parameters
    ----------
    group : str
        The python field name on :class:`~cstar.roms.namelist.RomsNamelistBase`
        (and the attribute name on its nested group model), e.g.
        ``"extract_data_settings"``.
    nml_group : str
        The ``&GROUP`` header name as ucla-roms writes/reads it. In this
        codebase it is always identical to `group` (``from_f90nml`` matches
        f90nml's group names directly to field names) — kept as its own field
        per the design so a future ucla-roms release that decouples the two
        (a cosmetic Fortran rename, say) doesn't require a table reshape.
    key : str
        The Fortran key name (the name f90nml reads/writes), e.g.
        ``"nrpf_extract"``.
    type : type or str
        The key's python type. A real `type` object (``int``, ``float``,
        ``str``, ``bool``) when the annotation is one of those; a string
        (``"list[float]"``, ``"list[str] | str"``) for generics/unions that
        don't type-check cleanly against `type` under mypy.
    default : Any
        The ucla-roms reference default (as declared on the ``_NmlGroup``
        field — a resolved `default`/`default_factory` value), or
        :data:`REQUIRED` if the field has no default.
    doc : str
        The field's docstring, transcribed verbatim from the ucla-roms
        reference ``src/namelist.nml`` comment.
    since : tuple[int, int, int] or None
        The first ucla-roms version (inclusive) that has this key. `None`
        means "every version this table knows about" (no lower bound).
    until : tuple[int, int, int] or None
        The first ucla-roms version (exclusive) that no longer has this key
        (renamed or removed). `None` means "still current".
    constraint : dict[str, Any] or None
        Pydantic field constraints beyond the type itself, e.g.
        ``{"ge": 1}`` for ``pio_stride``. `None` when the field has none.
    validator : str or None
        The name of the ``field_validator`` hook applied to this key, if any
        (e.g. ``"wrap_scalar_as_list"``, ``"namelist_str_list"``) — a data tag
        for a future small hook registry, not yet wired to real code.
    requires_cppdefs : tuple[str, ...]
        ucla-roms cppdefs this key's group is gated on. Deliberately left
        unpopulated in this migration step (see the design doc's open
        question on ``precheck.py``'s ``_StreamCheck.cppdef_guard``) — every
        row carries the empty default.
    """

    group: str
    nml_group: str
    key: str
    type: type | str
    default: Any = REQUIRED
    doc: str = ""
    since: tuple[int, int, int] | None = None
    until: tuple[int, int, int] | None = None
    constraint: dict[str, Any] | None = None
    validator: str | None = None
    requires_cppdefs: tuple[str, ...] = ()


# ucla-roms breaking-namelist-change versions this table's `since`/`until`
# values reference — kept in sync with `cstar.roms.namelist`'s
# `UCLA_ROMS_0_5_0`/`UCLA_ROMS_0_6_0`/`UCLA_ROMS_0_7_0` (not imported from
# there: this table must stand alone as the source of truth, and
# `test_namelist_keys.py` proves the two agree).
_V0_5_0: Final[tuple[int, int, int]] = (0, 5, 0)
_V0_6_0: Final[tuple[int, int, int]] = (0, 6, 0)
_V0_7_0: Final[tuple[int, int, int]] = (0, 7, 0)


NAMELIST_KEYS: tuple[NamelistKey, ...] = (
    # ---- simulation_name_settings (SimulationNameSettings) ----
    NamelistKey(
        group="simulation_name_settings",
        nml_group="simulation_name_settings",
        key="output_root_name",
        type=str,
        doc="Output file prefix (e.g. `roms_bgc.20120101120000.nc`)",
    ),
    NamelistKey(
        group="simulation_name_settings",
        nml_group="simulation_name_settings",
        key="title",
        type=str,
        doc="Title used in output metadata",
    ),
    # ---- time_stepping (TimeStepping) ----
    NamelistKey(
        group="time_stepping",
        nml_group="time_stepping",
        key="ntimes",
        type=int,
        doc="Number of time steps in this run",
    ),
    NamelistKey(
        group="time_stepping",
        nml_group="time_stepping",
        key="dt",
        type=float,
        doc="Time step (seconds)",
    ),
    NamelistKey(
        group="time_stepping",
        nml_group="time_stepping",
        key="ndtfast",
        type=int,
        doc="Number of fast time-steps per slow timestep",
    ),
    NamelistKey(
        group="time_stepping",
        nml_group="time_stepping",
        key="ninfo",
        type=int,
        doc="Number of steps between runtime diagnostics (STDOUT)",
    ),
    # ---- reference_date_settings (ReferenceDateSettings) ----
    NamelistKey(
        group="reference_date_settings",
        nml_group="reference_date_settings",
        key="reference_date",
        type="list[int]",
        default=(2000, 1, 1),
        doc="Model reference date (t=0): year, month, day",
        validator="wrap_scalar_as_list",
    ),
    # ---- grid_settings (GridSettings) ----
    NamelistKey(
        group="grid_settings",
        nml_group="grid_settings",
        key="grdname",
        type=str,
        doc="Grid file path",
    ),
    # ---- s_coord (SCoord) ----
    NamelistKey(
        group="s_coord",
        nml_group="s_coord",
        key="theta_s",
        type=float,
        doc="S-coordinate surface stretching parameter",
    ),
    NamelistKey(
        group="s_coord",
        nml_group="s_coord",
        key="theta_b",
        type=float,
        doc="S-coordinate bottom stretching parameter",
    ),
    NamelistKey(
        group="s_coord",
        nml_group="s_coord",
        key="hc",
        type=float,
        doc="Critical depth (m)",
    ),
    # ---- param_settings (ParamSettings) ----
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="np_xi",
        type=int,
        doc="Number of processors following X",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="np_eta",
        type=int,
        doc="Number of processors following Y",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="llm",
        type=int,
        doc="Number of grid points in X",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="mmm",
        type=int,
        doc="Number of grid points in Y",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="nz",
        type=int,
        doc="Number of vertical levels",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="nt_passive",
        type=int,
        doc="Number of passive tracers",
    ),
    NamelistKey(
        group="param_settings",
        nml_group="param_settings",
        key="nt_bgc",
        type=int,
        doc="Number of BGC tracers",
    ),
    # ---- initial_conditions (InitialConditions) ----
    NamelistKey(
        group="initial_conditions",
        nml_group="initial_conditions",
        key="inifile",
        type=str,
        doc="Initial conditions (IC) file path",
    ),
    # ---- forcing_files (ForcingFiles) ----
    NamelistKey(
        group="forcing_files",
        nml_group="forcing_files",
        key="frcfiles",
        type="list[str]",
        default=(),
        doc="Forcing file paths (e.g. boundary, surface flux, and river forcing)",
        validator="wrap_scalar_as_list",
    ),
    # ---- surf_frc_settings (SurfFrcSettings) ----
    NamelistKey(
        group="surf_frc_settings",
        nml_group="surf_frc_settings",
        key="interp_bulk_frc",
        type=bool,
        doc="Interpolate forcing from coarser input grid if T",
    ),
    NamelistKey(
        group="surf_frc_settings",
        nml_group="surf_frc_settings",
        key="check_bulk_frc_units",
        type=bool,
        doc="Check units of input vars if T",
    ),
    NamelistKey(
        group="surf_frc_settings",
        nml_group="surf_frc_settings",
        key="interp_flux_frc",
        type=bool,
        doc="Interpolate forcing from coarser input grid if T",
    ),
    # ---- river_frc_settings (RiverFrcSettings) ----
    NamelistKey(
        group="river_frc_settings",
        nml_group="river_frc_settings",
        key="river_source",
        type=bool,
        doc="T if river inputs used, else F",
    ),
    NamelistKey(
        group="river_frc_settings",
        nml_group="river_frc_settings",
        key="river_analytical",
        type=bool,
        doc="T if river inputs specified analytically",
    ),
    NamelistKey(
        group="river_frc_settings",
        nml_group="river_frc_settings",
        key="nriv",
        type=int,
        doc="Number of rivers",
    ),
    # ---- tidal_frc_settings (TidalFrcSettings) ----
    NamelistKey(
        group="tidal_frc_settings",
        nml_group="tidal_frc_settings",
        key="bry_tides",
        type=bool,
        doc="Barotropic tides at domain boundaries",
    ),
    NamelistKey(
        group="tidal_frc_settings",
        nml_group="tidal_frc_settings",
        key="pot_tides",
        type=bool,
        doc="Surface potential tides",
    ),
    NamelistKey(
        group="tidal_frc_settings",
        nml_group="tidal_frc_settings",
        key="ana_tides",
        type=bool,
        doc="Tidal forcing specified analytically",
    ),
    NamelistKey(
        group="tidal_frc_settings",
        nml_group="tidal_frc_settings",
        key="ntides",
        type=int,
        doc="Number of tidal constituents",
    ),
    # ---- basic_output_settings (_BasicOutputSettingsCommon + nrpf_rst pre-0.5.0) ----
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_file_his",
        type=bool,
        doc="Write instantaneous ocean physical state to output",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="output_period_his",
        type=float,
        doc="Frequency of instantaneous output (s)",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="nrpf_his",
        type=int,
        doc="Number of time records in instantaneous output file",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_z",
        type=bool,
        doc="Include `zeta`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_ub",
        type=bool,
        doc="Include `ubar`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_vb",
        type=bool,
        doc="Include `vbar`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_u",
        type=bool,
        doc="Include `u`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_v",
        type=bool,
        doc="Include `v`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_r",
        type=bool,
        doc="Include `rho`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_o",
        type=bool,
        doc="Include `omega`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_w",
        type=bool,
        doc="Include `w`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_akv",
        type=bool,
        doc="Include `Akv`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_akt",
        type=bool,
        doc="Include `Akt`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_aks",
        type=bool,
        doc="Include `Aks`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_hbls",
        type=bool,
        doc="Include `hbls`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_hbbl",
        type=bool,
        doc="Include `hbbl`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_file_avg",
        type=bool,
        doc="Write averages of ocean physical state to output",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="output_period_avg",
        type=float,
        doc="Frequency of averaged output/averaging period (s)",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="nrpf_avg",
        type=int,
        doc="Number of time records in averaged output file",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_z",
        type=bool,
        doc="Include `zeta`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_ub",
        type=bool,
        doc="Include `ubar`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_vb",
        type=bool,
        doc="Include `vbar`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_u",
        type=bool,
        doc="Include `u`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_v",
        type=bool,
        doc="Include `v`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_r",
        type=bool,
        doc="Include `rho`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_o",
        type=bool,
        doc="Include `omega`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_w",
        type=bool,
        doc="Include `w`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_akv",
        type=bool,
        doc="Include `Akv`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_akt",
        type=bool,
        doc="Include `Akt`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_aks",
        type=bool,
        doc="Include `Aks`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_hbls",
        type=bool,
        doc="Include `hbls`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_avg_hbbl",
        type=bool,
        doc="Include `hbbl`",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="wrt_file_rst",
        type=bool,
        doc="Write restart files (containing full model state)",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="monthly_restarts",
        type=bool,
        doc="Write restart files at start of calendar month",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="output_period_rst",
        type=float,
        doc="Write restart files at regular frequency (s)",
    ),
    NamelistKey(
        group="basic_output_settings",
        nml_group="basic_output_settings",
        key="nrpf_rst",
        type=int,
        doc="Number of time records in restart files",
        until=_V0_5_0,
    ),
    # ---- ts_output_settings (TsOutputSettings) ----
    NamelistKey(
        group="ts_output_settings",
        nml_group="ts_output_settings",
        key="wrt_temp",
        type=bool,
        doc="Include temperature in output fields",
    ),
    NamelistKey(
        group="ts_output_settings",
        nml_group="ts_output_settings",
        key="wrt_salt",
        type=bool,
        doc="Include salinity in output fields",
    ),
    NamelistKey(
        group="ts_output_settings",
        nml_group="ts_output_settings",
        key="wrt_temp_dia",
        type=bool,
        doc="Include temperature diagnostics in output fields",
    ),
    NamelistKey(
        group="ts_output_settings",
        nml_group="ts_output_settings",
        key="wrt_salt_dia",
        type=bool,
        doc="Include salinity diagnostics in output fields",
    ),
    # ---- frc_output_settings (FrcOutputSettings) ----
    NamelistKey(
        group="frc_output_settings",
        nml_group="frc_output_settings",
        key="wrt_frc",
        type=bool,
        doc="Write model forcing to its own output file",
    ),
    NamelistKey(
        group="frc_output_settings",
        nml_group="frc_output_settings",
        key="wrt_frc_avg",
        type=bool,
        doc="Forcing output averaged (T) or instantaneous (F)",
    ),
    NamelistKey(
        group="frc_output_settings",
        nml_group="frc_output_settings",
        key="output_period_frc",
        type=float,
        doc="Frequency/averaging period of forcing output",
    ),
    NamelistKey(
        group="frc_output_settings",
        nml_group="frc_output_settings",
        key="nrpf_frc",
        type=int,
        doc="Number of time records in forcing files",
    ),
    # ---- extract_data_settings (ExtractDataSettings) ----
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="do_extract",
        type=bool,
        doc="Generate boundary files for a nested domain",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="output_period_extract",
        type=float,
        doc="How often to output these files",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="nrpf_extract",
        type=int,
        doc="Number of time records per file",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="extract_file",
        type=str,
        doc="File path containing nesting info",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="n_chd",
        type=int,
        doc="Number of vertical levels in nested domain",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="theta_s_chd",
        type=float,
        doc="`theta_s` of nested domain",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="theta_b_chd",
        type=float,
        doc="`theta_b` of nested domain",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="hc_chd",
        type=float,
        doc="`hc` of nested domain",
    ),
    NamelistKey(
        group="extract_data_settings",
        nml_group="extract_data_settings",
        key="extract_root_name",
        type=str,
        default="child",
        doc="Root name (filename prefix) for extracted child boundary files",
    ),
    # ---- sponge_tune_settings (SpongeTuneSettings) ----
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="ub_tune",
        type=bool,
        doc='Tune boundary "sponge" to match parent bry conditions',
    ),
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="sponge_timescale",
        type=float,
        doc="Filtering time scale (s)",
    ),
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="wrt_sponge",
        type=bool,
        doc="Write out sponge tuning values",
    ),
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="sponge_avg",
        type=bool,
        doc="Sponge tuning output averaged (T) or instantaneous (F)",
    ),
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="nrpf_sponge",
        type=int,
        doc="Number of records per sponge file",
    ),
    NamelistKey(
        group="sponge_tune_settings",
        nml_group="sponge_tune_settings",
        key="output_period_sponge",
        type=float,
        doc="Output frequency of sponge tuning file",
    ),
    # ---- calc_pflx_settings (CalcPflxSettings) ----
    NamelistKey(
        group="calc_pflx_settings",
        nml_group="calc_pflx_settings",
        key="calc_pflx",
        type=bool,
        doc="Enable baroclinic pressure flux calculation",
    ),
    NamelistKey(
        group="calc_pflx_settings",
        nml_group="calc_pflx_settings",
        key="pflx_timescale",
        type=float,
        doc="Timescale for filtering pressure fluxes (s)",
    ),
    # ---- zslice_settings (ZsliceSettings) ----
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="do_zslice",
        type=bool,
        doc="Output certain variables on regular z-levels",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="zslice_avg",
        type=bool,
        doc="Averaged output (T) or instantaneous (F)",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="wrt_t_zslice",
        type=bool,
        doc="Write tracers to z-level output",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="wrt_u_zslice",
        type=bool,
        doc="Write zonal velocity to z-level output",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="wrt_v_zslice",
        type=bool,
        doc="Write meridional velocity to z-level output",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="output_period_zslice",
        type=float,
        doc="Frequency of z-level output",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="nrpf_zslice",
        type=int,
        doc="Number of records per file",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="ndep",
        type=int,
        doc="Number of depth levels on which to write",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="vecdep",
        type="list[float]",
        doc="Depths of levels on which to write",
        validator="wrap_scalar_as_list",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="nt_zslice",
        type=int,
        doc="Number of tracers to include",
    ),
    NamelistKey(
        group="zslice_settings",
        nml_group="zslice_settings",
        key="trc2zsc",
        type="list[int]",
        doc="Indices of tracers to include",
        validator="wrap_scalar_as_list",
    ),
    # ---- bgc_settings (BgcSettings) ----
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="interp_bgc_frc",
        type=bool,
        doc="Interpolate forcing from coarser input grid if T",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="wrt_bgc_his",
        type=bool,
        doc="Write instantaneous BGC tracers to output",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="output_period_bgc_his",
        type=float,
        doc="Frequency of instantaneous BGC output (s)",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="nrpf_bgc_his",
        type=int,
        doc="Number of time records per BGC output file",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="wrt_bgc_avg",
        type=bool,
        doc="Write averaged BGC tracers to output",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="output_period_bgc_avg",
        type=float,
        doc="Output frequency/averaging period (s)",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="nrpf_bgc_avg",
        type=int,
        doc="Number of time records per BGC average file",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="wrt_bgc_dia_his",
        type=bool,
        doc="Write instantaneous BGC diagnostics to output",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="output_period_bgc_his_dia",
        type=float,
        doc="Frequency of diagnostics output (s)",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="nrpf_bgc_his_dia",
        type=int,
        doc="Number of time records per BGC diagnostics file",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="wrt_bgc_dia_avg",
        type=bool,
        doc="Write averaged BGC diagnostics to output",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="output_period_bgc_avg_dia",
        type=float,
        doc="Frequency/period of averaged diagnostic output",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="nrpf_bgc_avg_dia",
        type=int,
        doc="Number of time records per BGC diagnostics file",
    ),
    NamelistKey(
        group="bgc_settings",
        nml_group="bgc_settings",
        key="xco2air_default",
        type=float,
        doc="Atmospheric xCO2 (ppm) when PCO2AIR_FORCING is off",
    ),
    # ---- marbl_biogeochemistry_settings (MarblBiogeochemistrySettings) ----
    NamelistKey(
        group="marbl_biogeochemistry_settings",
        nml_group="marbl_biogeochemistry_settings",
        key="marbl_config_file",
        type=str,
        default="marbl_in",
        doc="MARBL configuration file",
    ),
    NamelistKey(
        group="marbl_biogeochemistry_settings",
        nml_group="marbl_biogeochemistry_settings",
        key="marbl_tracers_to_write",
        type="list[str] | str",
        default="",
        doc="MARBL tracers to include in BGC output",
        validator="namelist_str_list",
        constraint={"max_len": 40},
    ),
    NamelistKey(
        group="marbl_biogeochemistry_settings",
        nml_group="marbl_biogeochemistry_settings",
        key="marbl_diagnostics_to_write",
        type="list[str] | str",
        default="",
        doc="MARBL diagnostics to include in BGC output",
        validator="namelist_str_list",
        constraint={"max_len": 64},
    ),
    NamelistKey(
        group="marbl_biogeochemistry_settings",
        nml_group="marbl_biogeochemistry_settings",
        key="marbl_timestep",
        type=float,
        default=3600.0,
        doc="Desired MARBL timestep (s); ROMS derives the step ratio from `dt`",
    ),
    # ---- cdr_frc_settings (CdrFrcSettings) ----
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_source",
        type=bool,
        doc="Apply CDR perturbation (T) or not (F)",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_file",
        type=str,
        doc="File path to CDR perturbation",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_ncdr_parm",
        type=int,
        doc="Number of CDR releases if `3D`/`parameterized`",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_nz_chd",
        type=int,
        doc="Number of vertical levels in CDR forcing",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_forcing_depth_profiles",
        type=bool,
        doc="Apply CDR forcing from a depth profile",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_forcing_3d",
        type=bool,
        doc="Apply CDR forcing from a fully 3D distribution",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_forcing_parameterized",
        type=bool,
        doc="Apply CDR forcing from Gaussian parameters",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_time_interpolation",
        type=bool,
        doc="Interpolate linearly between forcing records",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_relocate_to_wet_pts",
        type=bool,
        doc="Relocate CDR perturbation to sea if on land",
    ),
    NamelistKey(
        group="cdr_frc_settings",
        nml_group="cdr_frc_settings",
        key="cdr_volume",
        type=bool,
        doc="Read in volume flux/tracer concentration",
    ),
    # ---- cdr_output_settings (CdrOutputSettings) ----
    NamelistKey(
        group="cdr_output_settings",
        nml_group="cdr_output_settings",
        key="do_cdr_output",
        type=bool,
        doc="Output CDR-relevant fields",
    ),
    NamelistKey(
        group="cdr_output_settings",
        nml_group="cdr_output_settings",
        key="wrt_cdr_avg",
        type=bool,
        doc="Write averaged (T) or instantaneous (F) output",
    ),
    NamelistKey(
        group="cdr_output_settings",
        nml_group="cdr_output_settings",
        key="cdr_monthly_averages",
        type=bool,
        doc="Write averaged outputs per calendar month",
    ),
    NamelistKey(
        group="cdr_output_settings",
        nml_group="cdr_output_settings",
        key="output_period_cdr",
        type=float,
        doc="Frequency of CDR-relevant output",
    ),
    NamelistKey(
        group="cdr_output_settings",
        nml_group="cdr_output_settings",
        key="nrpf_cdr",
        type=int,
        doc="Time records per output file",
    ),
    # ---- upscale_settings (UpscaleSettings) ----
    NamelistKey(
        group="upscale_settings",
        nml_group="upscale_settings",
        key="do_upscale",
        type=bool,
        doc="Record CDR tracer fluxes thru domain boundaries",
    ),
    NamelistKey(
        group="upscale_settings",
        nml_group="upscale_settings",
        key="nrpf_uscl",
        type=int,
        doc="Number of records per file",
    ),
    NamelistKey(
        group="upscale_settings",
        nml_group="upscale_settings",
        key="output_period_uscl",
        type=float,
        doc="Output frequency",
    ),
    # ---- lin_rho_eos_settings (LinRhoEosSettings) ----
    NamelistKey(
        group="lin_rho_eos_settings",
        nml_group="lin_rho_eos_settings",
        key="tcoef",
        type=float,
        doc="Thermal expansion coefficient (kg/m2/K)",
    ),
    NamelistKey(
        group="lin_rho_eos_settings",
        nml_group="lin_rho_eos_settings",
        key="t0",
        type=float,
        doc="Reference temperature (*C)",
    ),
    NamelistKey(
        group="lin_rho_eos_settings",
        nml_group="lin_rho_eos_settings",
        key="scoef",
        type=float,
        doc="Saline contraction coefficient (kg/m3/psu)",
    ),
    NamelistKey(
        group="lin_rho_eos_settings",
        nml_group="lin_rho_eos_settings",
        key="s0",
        type=float,
        doc="Reference salinity (psu)",
    ),
    # ---- rho0_settings (Rho0Settings) ----
    NamelistKey(
        group="rho0_settings",
        nml_group="rho0_settings",
        key="rho0",
        type=float,
        doc="Boussinesq reference density (kg/m3)",
    ),
    # ---- gamma2_settings (Gamma2Settings) ----
    NamelistKey(
        group="gamma2_settings",
        nml_group="gamma2_settings",
        key="gamma2",
        type=float,
        doc="Slipperiness parameter (free-slip = +1, no-slip = -1)",
    ),
    # ---- tracer_diff2 (TracerDiff2) ----
    NamelistKey(
        group="tracer_diff2",
        nml_group="tracer_diff2",
        key="tnu2",
        type="list[float]",
        doc="Horizontal Laplacian diffusion (m2/s) for each tracer",
        validator="wrap_scalar_as_list",
    ),
    # ---- bottom_drag_settings (BottomDragSettings) ----
    NamelistKey(
        group="bottom_drag_settings",
        nml_group="bottom_drag_settings",
        key="rdrg",
        type=float,
        doc="Linear bottom drag co-efficient (m/s)",
    ),
    NamelistKey(
        group="bottom_drag_settings",
        nml_group="bottom_drag_settings",
        key="rdrg2",
        type=float,
        doc="Quadratic bottom drag co-efficient (dimensionless)",
    ),
    NamelistKey(
        group="bottom_drag_settings",
        nml_group="bottom_drag_settings",
        key="zob",
        type=float,
        doc="Bottom roughness height (m)",
    ),
    # ---- vertical_mixing_settings (VerticalMixingSettings) ----
    NamelistKey(
        group="vertical_mixing_settings",
        nml_group="vertical_mixing_settings",
        key="akv_bak",
        type=float,
        doc="Vertical viscosity (m2/s)",
    ),
    NamelistKey(
        group="vertical_mixing_settings",
        nml_group="vertical_mixing_settings",
        key="akt_bak",
        type="list[float]",
        doc="Vertical mixing (m2/s) for each tracer",
        validator="wrap_scalar_as_list",
    ),
    # ---- lateral_visc_settings (LateralViscSettings) ----
    NamelistKey(
        group="lateral_visc_settings",
        nml_group="lateral_visc_settings",
        key="visc2",
        type=float,
        doc="Horizontal Laplacian kinematic viscosity (m2/s)",
    ),
    # ---- ubind_settings (UbindSettings) ----
    NamelistKey(
        group="ubind_settings",
        nml_group="ubind_settings",
        key="ubind",
        type=float,
        doc="Open boundary binding velocity (m/s)",
    ),
    # ---- v_sponge_settings (VSpongeSettings) ----
    NamelistKey(
        group="v_sponge_settings",
        nml_group="v_sponge_settings",
        key="v_sponge",
        type=float,
        doc="Maximum viscosity in sponge layer (m2/s)",
    ),
    # ---- sss_correction (SssCorrection) ----
    NamelistKey(
        group="sss_correction",
        nml_group="sss_correction",
        key="dsssdt",
        type=float,
        doc="SSS correction co-efficient as piston velocity (cm/day)",
    ),
    # ---- sst_correction (SstCorrection) ----
    NamelistKey(
        group="sst_correction",
        nml_group="sst_correction",
        key="dsstdt",
        type=float,
        doc="SST correction co-efficient as piston velocity (cm/day)",
    ),
    # ---- dic_alk_correction (DicAlkCorrection) ----
    NamelistKey(
        group="dic_alk_correction",
        nml_group="dic_alk_correction",
        key="dcdt",
        type=float,
        doc="DIC/ALK correction co-efficient as piston velocity (cm/day)",
    ),
    # ---- diagnostics_settings (DiagnosticsSettings) ----
    NamelistKey(
        group="diagnostics_settings",
        nml_group="diagnostics_settings",
        key="diag_avg",
        type=bool,
        doc="Output physics diags as avgs (T) or snapshots (F)",
    ),
    NamelistKey(
        group="diagnostics_settings",
        nml_group="diagnostics_settings",
        key="diag_uv",
        type=bool,
        doc="Output momentum diagnostics",
    ),
    NamelistKey(
        group="diagnostics_settings",
        nml_group="diagnostics_settings",
        key="diag_trc",
        type=bool,
        doc="Output tracer diagnostics",
    ),
    NamelistKey(
        group="diagnostics_settings",
        nml_group="diagnostics_settings",
        key="output_period_diag",
        type=float,
        doc="Output frequency (s)",
    ),
    NamelistKey(
        group="diagnostics_settings",
        nml_group="diagnostics_settings",
        key="nrpf_diag",
        type=int,
        doc="Number of records per output file",
    ),
    # ---- stdout_diag_settings (StdoutDiagSettings) ----
    NamelistKey(
        group="stdout_diag_settings",
        nml_group="stdout_diag_settings",
        key="code_check_mode",
        type=bool,
        doc="Diagnostics in stdout formatted for code testing",
    ),
    # ---- random_output_settings (RandomOutputSettings) ----
    NamelistKey(
        group="random_output_settings",
        nml_group="random_output_settings",
        key="do_random",
        type=bool,
        doc="Output user-customized output fields",
    ),
    NamelistKey(
        group="random_output_settings",
        nml_group="random_output_settings",
        key="output_period_random",
        type=float,
        doc="Frequency of custom output (s)",
    ),
    NamelistKey(
        group="random_output_settings",
        nml_group="random_output_settings",
        key="nrpf_random",
        type=int,
        doc="Number of records per output file",
    ),
    # ---- surf_flx_output_settings (SurfFlxOutputSettings) ----
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="wrt_smflx",
        type=bool,
        doc="Output surface momentum flux",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="wrt_stflx",
        type=bool,
        doc="Output surface tracer flux",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="wrt_rstflx",
        type=bool,
        doc="Output surface restoring flux (already accounted in stflx)",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="wrt_swflx",
        type=bool,
        doc="Output surface water flux (P-E)",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="sflx_avg",
        type=bool,
        doc="Output average (T) or instantaneous (F) fields",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="output_period_sflx",
        type=float,
        doc="Frequency of surface flux output (s)",
    ),
    NamelistKey(
        group="surf_flx_output_settings",
        nml_group="surf_flx_output_settings",
        key="nrpf_sflx",
        type=int,
        doc="Number of records per surface flux file",
    ),
    # ---- pipe_frc_settings (PipeFrcSettings) ----
    NamelistKey(
        group="pipe_frc_settings",
        nml_group="pipe_frc_settings",
        key="pipe_source",
        type=bool,
        doc="T if pipe inputs used, else F",
    ),
    NamelistKey(
        group="pipe_frc_settings",
        nml_group="pipe_frc_settings",
        key="p_analytical",
        type=bool,
        doc="T if pipe inputs specified analytically",
    ),
    NamelistKey(
        group="pipe_frc_settings",
        nml_group="pipe_frc_settings",
        key="npip",
        type=int,
        doc="Number of pipe inputs",
    ),
    # ---- particles_settings (_ParticlesSettingsCommon + rename at 0.5.0) ----
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="floats",
        type=bool,
        doc="Release Lagrangian particles (T) or not (F)",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="np",
        type=int,
        doc="Local number of particles",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="extra_space_fac",
        type=float,
        doc="Buffer space to receive extra exchanged particles",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="exchange_facx",
        type=float,
        doc="Maximum number of particles for transfer in N-S",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="exchange_facy",
        type=float,
        doc="Maximum number of particles for transfer in E-W",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="exchange_facc",
        type=float,
        doc="Maximum number of particles for transfer in corners",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="ppm3",
        type=float,
        doc="Target particles per cubic meter",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="pmin",
        type=int,
        doc="Minimum of allocated space for particle array",
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="output_period",
        type=float,
        doc="Frequency of outputs",
        until=_V0_5_0,
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="nrpf",
        type=int,
        doc="Number of records per file",
        until=_V0_5_0,
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="output_period_particles",
        type=float,
        doc="Frequency of outputs",
        since=_V0_5_0,
    ),
    NamelistKey(
        group="particles_settings",
        nml_group="particles_settings",
        key="nrpf_particles",
        type=int,
        doc="Number of records per file",
        since=_V0_5_0,
    ),
    # ---- pio_settings (PioSettings) -- added ucla-roms 0.6.0, PR #346 ----
    NamelistKey(
        group="pio_settings",
        nml_group="pio_settings",
        key="pio_stride",
        type=int,
        default=1,
        doc="Stride between MPI ranks assigned as PIO I/O tasks (requires PARALLEL_IO)",
        since=_V0_6_0,
        constraint={"ge": 1},
    ),
    # ---- cdr_tracer_output_settings (CdrTracerOutputSettings) -- added 0.7.0, PR #351 ----
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="do_cdr_tracer_output",
        type=bool,
        default=False,
        doc="Output dedicated CDR tracers",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_cdr_trc_avg",
        type=bool,
        default=True,
        doc="Write averaged (T) or instantaneous (F)",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="cdr_trc_monthly_averages",
        type=bool,
        default=False,
        doc="Write averaged outputs per calendar month",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="output_period_cdr_trc",
        type=float,
        default=3600.0,
        doc="Frequency of CDR tracer output (s)",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="nrpf_cdr_trc",
        type=int,
        default=4,
        doc="Time records per output file",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_tracers",
        type=bool,
        default=True,
        doc="Write CDR tracer concentrations",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_vertical_integrals",
        type=bool,
        default=True,
        doc="Write int_z_* fields",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_thickness_weighted",
        type=bool,
        default=True,
        doc="Write h* and h*_avg fields",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_sources",
        type=bool,
        default=True,
        doc="Write *_source fields (if cdr_source)",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_alk",
        type=bool,
        default=True,
        doc="Write CDR_OAE_ALK and its variants",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_tracer_output_settings",
        nml_group="cdr_tracer_output_settings",
        key="wrt_dic",
        type=bool,
        default=True,
        doc="Write CDR_OAE_DIC, CDR_DOR_DIC and variants",
        since=_V0_7_0,
    ),
    # ---- cdr_gas_exch_output_settings (CdrGasExchOutputSettings) -- added 0.7.0, PR #351 ----
    NamelistKey(
        group="cdr_gas_exch_output_settings",
        nml_group="cdr_gas_exch_output_settings",
        key="do_cdr_gas_exch_output",
        type=bool,
        default=False,
        doc="Output CDR gas-exchange sensitivities",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_gas_exch_output_settings",
        nml_group="cdr_gas_exch_output_settings",
        key="wrt_cdr_gas_avg",
        type=bool,
        default=True,
        doc="Write averaged (T) or instantaneous (F)",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_gas_exch_output_settings",
        nml_group="cdr_gas_exch_output_settings",
        key="cdr_gas_monthly_averages",
        type=bool,
        default=False,
        doc="Write averaged outputs per calendar month",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_gas_exch_output_settings",
        nml_group="cdr_gas_exch_output_settings",
        key="output_period_cdr_gas",
        type=float,
        default=3600.0,
        doc="Frequency of CDR gas-exch output (s)",
        since=_V0_7_0,
    ),
    NamelistKey(
        group="cdr_gas_exch_output_settings",
        nml_group="cdr_gas_exch_output_settings",
        key="nrpf_cdr_gas",
        type=int,
        default=4,
        doc="Time records per output file",
        since=_V0_7_0,
    ),
)
