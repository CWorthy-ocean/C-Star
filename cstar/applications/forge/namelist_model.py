"""
Forge's run-time settings schema (``RunTimeSettings`` and its
``RunTimeSettingsV0_5_0`` counterpart) and the settings → namelist transform
(``build_namelist``).

* :class:`RunTimeSettings` / :class:`RunTimeSettingsV0_5_0` validate and type
  forge's run-time settings dict — the YAML vocabulary (``tcline``, ``np_xi``,
  ``analytical`` …). Defaults live in each ModelSpec's ``model.yaml``
  (``model_settings``), merged with the catalog's Domain/Forcing/Output specs —
  not here: fields are required so an incomplete settings dict fails loudly;
  only the runtime-filled fields (grid file, IC file, s-coord, forcing paths,
  casename, output root) are ``Optional``. :func:`run_time_settings_for_ref`
  picks the variant matching a pinned ucla-roms ref.
* :func:`build_namelist` transforms a validated run-time settings model into
  the matching :class:`cstar.roms.namelist.RomsNamelistBase` subclass (renames
  via ``serialization_alias``, cross-section regrouping, scalar →
  per-tracer-array expansion, ``frcfiles`` assembly), reading typed fields —
  no ``bool()/int()/float()/str()`` coercion.

The namelist schema itself — ``RomsNamelistBase`` and its versioned
subclasses/``&group`` models — lives in C-Star (:mod:`cstar.roms.namelist`) and
is imported here; that is the reusable read/edit/write schema.
``settings.write_roms_namelist`` is a thin wrapper over ``build_namelist``.
"""

from __future__ import annotations

import os
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from cstar.roms.namelist import (
    BasicOutputSettings,
    BasicOutputSettingsV0_5_0,
    BgcSettings,
    BottomDragSettings,
    CalcPflxSettings,
    CdrFrcSettings,
    CdrGasExchOutputSettings,
    CdrOutputSettings,
    CdrTracerOutputSettings,
    DiagnosticsSettings,
    DicAlkCorrection,
    ExtractDataSettings,
    ForcingFiles,
    FrcOutputSettings,
    Gamma2Settings,
    GridSettings,
    InitialConditions,
    LateralViscSettings,
    LinRhoEosSettings,
    MarblBiogeochemistrySettings,
    ParamSettings,
    ParticlesSettings,
    ParticlesSettingsV0_5_0,
    PioSettings,
    PipeFrcSettings,
    RandomOutputSettings,
    ReferenceDateSettings,
    Rho0Settings,
    RiverFrcSettings,
    RomsNamelist,
    RomsNamelistBase,
    RomsNamelistV0_5_0,
    RomsNamelistV0_6_0,
    RomsNamelistV0_7_0,
    SCoord,
    SimulationNameSettings,
    SpongeTuneSettings,
    SssCorrection,
    SstCorrection,
    StdoutDiagSettings,
    SurfFlxOutputSettings,
    SurfFrcSettings,
    TidalFrcSettings,
    TimeStepping,
    TracerDiff2,
    TsOutputSettings,
    UbindSettings,
    UpscaleSettings,
    VerticalMixingSettings,
    VSpongeSettings,
    ZsliceSettings,
    namelist_schema_for_ref,
)
from cstar.roms.precheck import NamelistConsistencyError as NamelistConsistencyError
from cstar.roms.precheck import applies_to as _output_precheck_applies_to
from cstar.roms.precheck import (
    check_output_streams_divide_rst as _check_output_streams_divide_rst,
)
from cstar.roms.precheck import (
    check_restart_period_divisible_by_dt as _check_restart_period_divisible_by_dt,
)


def _coerce_pathlike(v):
    """Accept ``pathlib.Path``/``os.PathLike`` for path-valued settings fields —
    input generation fills them with ``Path`` objects — coercing to ``str``.
    ``None`` and ``str`` pass through unchanged.
    """
    return os.fspath(v) if isinstance(v, os.PathLike) else v


# An optional path string that also accepts a Path (coerced to str). Used for
# the settings fields that input generation populates with Path objects.
PathStr = Annotated[str | None, BeforeValidator(_coerce_pathlike)]


def _coerce_pathlist(v):
    """Accept ``None`` (-> ``[]``), a single path-like, or a list of them for
    settings fields that now hold *multiple* generated paths (one per bgc source
    -- see ``ForcingCfg.surface_forcing_bgc_path``/``boundary_forcing_bgc_path``).
    Each entry is coerced like ``_coerce_pathlike``.
    """
    if v is None:
        return []
    items = v if isinstance(v, (list, tuple)) else [v]
    return [
        os.fspath(item) if isinstance(item, os.PathLike) else item for item in items
    ]


# A list of path strings that also accepts None/a single path-like/Path objects
# (coerced to str each). Used for the bgc forcing-path settings fields, which may
# now hold one entry per bgc source in a category.
PathListStr = Annotated[list[str], BeforeValidator(_coerce_pathlist)]


# ===========================================================================
# Settings-vocabulary models (forge's run-time dict, validated)
#
# DEFAULTS LIVE IN THE YAML, NOT HERE. Each ModelSpec ships its own
# run-time-defaults.yaml with its own values; these models only *validate and
# type* whatever that YAML (plus overrides + dynamically-set values) provides.
# So fields carry NO value defaults — they are required, and an incomplete
# ModelSpec YAML fails validation loudly (naming the missing field). The only
# exceptions are fields that are intentionally ``null`` in the YAML and filled
# at run time (grid file, initial conditions file, s-coord, forcing paths,
# casename, output root); those are ``Optional[...] = None`` — None means
# "pending", not a configured default.
# ===========================================================================
class _SettingsSection(BaseModel):
    # extra="ignore": settings sections carry metadata keys the namelist drops
    # (sst_vname, cdb_min/max, diag_prec, nbgc_flx, cdr_*_vname, ...).
    model_config = ConfigDict(extra="ignore")


class TitleCfg(_SettingsSection):
    casename: str | None = None  # set dynamically


class OutputRootNameCfg(_SettingsSection):
    output_root_name: str | None = None  # set dynamically


class TimeSteppingCfg(_SettingsSection):
    ntimes: int
    dt: float
    ndtfast: int
    ninfo: int


class ReferenceDateCfg(_SettingsSection):
    reference_date: list[int] = Field(
        default_factory=lambda: [2000, 1, 1]
    )  # set dynamically from the blueprint's model_reference_date


class GridCfg(_SettingsSection):
    grid_file: PathStr = Field(
        default=None, serialization_alias="grdname"
    )  # set from generated grid


class SCoordCfg(_SettingsSection):
    theta_s: float | None = None  # set from grid
    theta_b: float | None = None
    tcline: float | None = Field(default=None, serialization_alias="hc")


class ParamCfg(_SettingsSection):
    llm: int
    mmm: int
    n: int = Field(serialization_alias="nz")
    np_xi: int
    np_eta: int
    nt_passive: int
    ntrc_bio: int = Field(serialization_alias="nt_bgc")


class PioSettingsCfg(_SettingsSection):
    # Deliberate default (like ExtractDataCfg.extract_root_name above): blueprints
    # saved before &PIO_SETTINGS existed bypass the resolver and hit
    # model_validate directly, so the Pydantic default is what keeps them valid.
    pio_stride: int = Field(default=1, ge=1)


class InitialCfg(_SettingsSection):
    initial_file: PathStr = Field(
        default=None, serialization_alias="inifile"
    )  # set from generated IC


class ForcingCfg(_SettingsSection):
    surface_forcing_path: PathStr = None
    surface_forcing_bgc_path: PathListStr = Field(default_factory=list)
    """One path per surface bgc source (e.g. UNIFIED + MBL_co2 both contribute) --
    was a last-write-wins scalar; ROMS's ``frcfiles`` array scans all listed files
    for whichever variables it needs, so all of them must survive into the
    namelist (see ``_FORCING_ORDER`` in ``build_namelist``)."""
    boundary_forcing_path: PathStr = None
    boundary_forcing_bgc_path: PathListStr = Field(default_factory=list)
    """One path per boundary bgc source. See ``surface_forcing_bgc_path``."""
    tidal_forcing_path: PathStr = None
    river_path: PathStr = None


class BlkFrcCfg(_SettingsSection):
    interp_frc: bool = Field(serialization_alias="interp_bulk_frc")
    check_bulk_frc_units: bool


class FluxFrcCfg(_SettingsSection):
    interp_flux_frc: bool


class RiverFrcCfg(_SettingsSection):
    river_source: bool
    analytical: bool = Field(serialization_alias="river_analytical")
    nriv: int


class TidesCfg(_SettingsSection):
    bry_tides: bool
    pot_tides: bool
    ana_tides: bool
    ntides: int


class _OceanVarsCfgCommon(_SettingsSection):
    wrt_file_his: bool
    output_period_his: float
    nrpf_his: int
    wrt_z: bool
    wrt_ub: bool
    wrt_vb: bool
    wrt_u: bool
    wrt_v: bool
    wrt_r: bool
    wrt_o: bool
    wrt_w: bool
    wrt_akv: bool
    wrt_akt: bool
    wrt_aks: bool
    wrt_hbls: bool
    wrt_hbbl: bool
    wrt_file_avg: bool
    output_period_avg: float
    nrpf_avg: int
    wrt_avg_z: bool
    wrt_avg_ub: bool
    wrt_avg_vb: bool
    wrt_avg_u: bool
    wrt_avg_v: bool
    wrt_avg_r: bool
    wrt_avg_o: bool
    wrt_avg_w: bool
    wrt_avg_akv: bool
    wrt_avg_akt: bool
    wrt_avg_aks: bool
    wrt_avg_hbls: bool
    wrt_avg_hbbl: bool
    wrt_file_rst: bool
    monthly_restarts: bool
    output_period_rst: float


class OceanVarsCfg(_OceanVarsCfgCommon):
    """``ocean_vars`` settings for ucla-roms < 0.5.0."""

    nrpf_rst: int


class OceanVarsCfgV0_5_0(_OceanVarsCfgCommon):
    """``ocean_vars`` settings for ucla-roms >= 0.5.0.

    ``nrpf_rst`` was removed in ucla-roms 0.5.0: the restart record count is
    now hardcoded in Fortran rather than namelist-configurable.
    """


def check_rst_period_divisible(
    dt: float | None, ocean_vars: _OceanVarsCfgCommon | dict[str, Any]
) -> None:
    """Raise ``ValueError`` if ``ocean_vars.output_period_rst`` isn't an integer
    multiple of ``dt`` -- restart writes must land on a timestep.

    Forge-vocabulary shim over
    :func:`cstar.roms.precheck.check_restart_period_divisible_by_dt` (the
    single home for this rule): ``ocean_vars``' fields already ARE the real
    Fortran namelist keys (``wrt_file_rst``/``monthly_restarts``/
    ``output_period_rst``, no aliasing -- see :data:`_PRECHECK_SECTION_MAP`),
    so it's passed straight through as the canonical ``basic_output_settings``
    group, whether it's a plain dict (the resolver's world) or an
    ``OceanVarsCfg`` (the pydantic validator's world) -- both forms are
    accepted transparently by the canonical function's own container reader.
    """
    _check_restart_period_divisible_by_dt(
        {"time_stepping": {"dt": dt}, "basic_output_settings": ocean_vars}
    )


def cppdefs_for_precheck(
    cppdefs: dict[str, Any] | None, upscale_output: Any
) -> dict[str, Any]:
    """Build the cppdef-activity mapping :func:`cstar.roms.precheck.check_output_streams_divide_rst`
    expects, from forge's resolved ``cppdefs`` section plus ``upscale_output``.

    Two of that function's guard names aren't literal ``cppdefs.*`` keys in
    forge's settings, but are still real, derivable, compile-time-active flags
    (see ``templates/compile-time/cppdefs.opt.j2``):

    * ``marbl_diags`` -- the template ``#define``s ``MARBL_DIAGS`` exactly
      when it ``#define``s ``MARBL`` (forge has no separate MARBL_DIAGS
      toggle), so this mirrors ``cppdefs["marbl"]``.
    * ``upscaling`` -- the template ``#define``s ``UPSCALING`` exactly when
      ``upscale_output.do_upscale`` is true; there is no ``cppdefs.upscaling``
      key at all, so it's read off the run-time section instead.

    ``diagnostics`` and ``biology_bec2`` need no such derivation: forge's
    template hardcodes ``#undef DIAGNOSTICS``/``#undef BIOLOGY_BEC2`` (no BEC2
    or ROMS term-budget diagnostics support yet), so their absence from
    ``cppdefs`` already correctly reads as "inactive" to the checker.

    Both derived keys are always OVERWRITTEN (not just filled in when absent):
    ``cppdefs`` is an unvalidated dict, so a hand-edited/legacy blueprint could
    carry a stale ``"upscaling"``/``"marbl_diags"`` key that must never mask
    the real, template-derived value.
    """

    def _get(section: Any, key: str) -> Any:
        if section is None:
            return None
        return (
            section.get(key)
            if isinstance(section, dict)
            else getattr(section, key, None)
        )

    result = dict(cppdefs or {})
    result["marbl_diags"] = bool(result.get("marbl", False))
    result["upscaling"] = bool(_get(upscale_output, "do_upscale"))
    return result


def check_output_streams_divide_rst(settings: Any, cppdefs: Any = None) -> None:
    """Forge-side entry to ``cstar.roms.precheck.check_output_streams_divide_rst``.

    The general ucla-roms output-stream / restart-rollover precheck lives in
    C-Star; this keeps the resolver and executor on one forge-side import path
    alongside the forge-shaped helpers above.
    """
    _check_output_streams_divide_rst(settings, cppdefs)


class TsOutputCfg(_SettingsSection):
    wrt_temp: bool
    wrt_salt: bool
    wrt_temp_dia: bool
    wrt_salt_dia: bool


class FrcOutputCfg(_SettingsSection):
    wrt_frc: bool
    wrt_frc_avg: bool
    output_period: float = Field(serialization_alias="output_period_frc")
    nrpf: int = Field(serialization_alias="nrpf_frc")


class ExtractDataCfg(_SettingsSection):
    do_extract: bool
    extract_file: str
    nrpf: int = Field(serialization_alias="nrpf_extract")
    n_chd: int
    theta_s_chd: float
    theta_b_chd: float
    hc_chd: float
    extract_period: float = Field(serialization_alias="output_period_extract")
    extract_root_name: str = "child"


class SpongeTuneCfg(_SettingsSection):
    ub_tune: bool
    spn_avg: bool = Field(serialization_alias="sponge_avg")
    sp_timscale: float = Field(serialization_alias="sponge_timescale")
    wrt_sponge: bool
    nrpf: int = Field(serialization_alias="nrpf_sponge")
    output_period: float = Field(serialization_alias="output_period_sponge")


class CalcPflxCfg(_SettingsSection):
    calc_pflx: bool
    timescale: float = Field(serialization_alias="pflx_timescale")


class ZsliceCfg(_SettingsSection):
    do_zslice: bool
    zslice_avg: bool
    wrt_t_zsl: bool = Field(serialization_alias="wrt_t_zslice")
    wrt_u_zsl: bool = Field(serialization_alias="wrt_u_zslice")
    wrt_v_zsl: bool = Field(serialization_alias="wrt_v_zslice")
    output_period: float = Field(serialization_alias="output_period_zslice")
    nrpf: int = Field(serialization_alias="nrpf_zslice")
    ndep: int
    vecdep: list[float]
    nt_z: int = Field(serialization_alias="nt_zslice")
    trc2zsc: list[int]


class BgcCfg(_SettingsSection):
    interp_frc: bool = Field(serialization_alias="interp_bgc_frc")
    wrt_his: bool = Field(serialization_alias="wrt_bgc_his")
    output_period_his: float = Field(serialization_alias="output_period_bgc_his")
    nrpf_his: int = Field(serialization_alias="nrpf_bgc_his")
    wrt_avg: bool = Field(serialization_alias="wrt_bgc_avg")
    output_period_avg: float = Field(serialization_alias="output_period_bgc_avg")
    nrpf_avg: int = Field(serialization_alias="nrpf_bgc_avg")
    wrt_his_dia: bool = Field(serialization_alias="wrt_bgc_dia_his")
    output_period_his_dia: float = Field(
        serialization_alias="output_period_bgc_his_dia"
    )
    nrpf_his_dia: int = Field(serialization_alias="nrpf_bgc_his_dia")
    wrt_avg_dia: bool = Field(serialization_alias="wrt_bgc_dia_avg")
    output_period_avg_dia: float = Field(
        serialization_alias="output_period_bgc_avg_dia"
    )
    nrpf_avg_dia: int = Field(serialization_alias="nrpf_bgc_avg_dia")
    xco2air_default: float


class CdrFrcCfg(_SettingsSection):
    cdr_source: bool
    cdr_file: str
    ncdr_parm: int = Field(serialization_alias="cdr_ncdr_parm")
    nz_chd: int = Field(serialization_alias="cdr_nz_chd")
    forcing_depth_profiles: bool = Field(
        serialization_alias="cdr_forcing_depth_profiles"
    )
    forcing_3d: bool = Field(serialization_alias="cdr_forcing_3d")
    forcing_parameterized: bool = Field(serialization_alias="cdr_forcing_parameterized")
    time_interpolation: bool = Field(serialization_alias="cdr_time_interpolation")
    relocate_to_wet_pts: bool = Field(serialization_alias="cdr_relocate_to_wet_pts")
    cdr_volume: bool


# MARBL diagnostics ucla-roms' cdr_output module looks up by name with no
# missing-name guard (absence segfaults) — must be in
# marbl_bgc.marbl_diagnostics_to_write whenever do_cdr_output is enabled.
# Defined here so both the resolver and the executor can import them.
CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS = (
    "zsatarag",
    "zsatcalc",
    "CO3",
    "CO3_ALT_CO2",
    "co3_sat_arag",
    "co3_sat_calc",
)


def ensure_cdr_output_marbl_diagnostics(diags: list[str] | None) -> list[str]:
    """Return ``diags`` with every ``CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS`` name
    appended (order-preserving, no duplicates). ``None`` is treated as empty.
    """
    result = list(diags or [])
    existing = set(result)
    for name in CDR_OUTPUT_REQUIRED_MARBL_DIAGNOSTICS:
        if name not in existing:
            result.append(name)
            existing.add(name)
    return result


class CdrOutputCfg(_SettingsSection):
    # ``do_cdr`` is the pre-v5 spelling, accepted so unmigrated dicts validate.
    do_cdr_output: bool = Field(
        validation_alias=AliasChoices("do_cdr_output", "do_cdr")
    )
    do_avg: bool = Field(serialization_alias="wrt_cdr_avg")
    monthly_averages: bool = Field(serialization_alias="cdr_monthly_averages")
    output_period: float = Field(serialization_alias="output_period_cdr")
    nrpf: int = Field(serialization_alias="nrpf_cdr")


class CdrTracerOutputCfg(_SettingsSection):
    """``cdr_tracer_output`` settings -- ucla-roms >= 0.7.0's dedicated
    ``&CDR_TRACER_OUTPUT_SETTINGS`` group (PR #351), a separate output stream
    for the CDR tracers (``CDR_OAE_ALK``/``CDR_OAE_DIC``/``CDR_DOR_DIC``).
    The tracers themselves exist whenever ``nt_cdr_oae``/``nt_cdr_dor`` are
    non-zero, with or without MARBL, so this stream needs ``CDR_FORCING`` but
    not MARBL. ucla-roms 0.7.0 and 0.8.0 nevertheless compile the module only
    under ``MARBL && CDR_FORCING`` (a guard bug reported upstream); C-Star
    encodes the intended rule, so on those releases a no-MARBL run that enables
    this stream aborts at ROMS init instead of at authoring time.

    Every field carries a Pydantic default (the ucla-roms reference default):
    unlike :class:`CdrOutputCfg`, this section is not forced on by an active
    CDR forcing mode (see the resolver's CDR tracer/gas-exchange output
    consistency check) -- a settings dict predating this section, or one that
    simply never enables it, must still validate.
    """

    do_cdr_tracer_output: bool = False
    do_avg: bool = Field(default=True, serialization_alias="wrt_cdr_trc_avg")
    monthly_averages: bool = Field(
        default=False, serialization_alias="cdr_trc_monthly_averages"
    )
    output_period: float = Field(
        default=3600.0, serialization_alias="output_period_cdr_trc"
    )
    nrpf: int = Field(default=4, serialization_alias="nrpf_cdr_trc")
    wrt_tracers: bool = True
    wrt_vertical_integrals: bool = True
    wrt_thickness_weighted: bool = True
    wrt_sources: bool = True
    wrt_alk: bool = True
    wrt_dic: bool = True


class CdrGasExchOutputCfg(_SettingsSection):
    """``cdr_gas_exch_output`` settings -- ucla-roms >= 0.7.0's dedicated
    ``&CDR_GAS_EXCH_OUTPUT_SETTINGS`` group (PR #351), a separate output
    stream for the gas-exchange sensitivities (``ddic_dco2``/``ddic_dalk``),
    active only under MARBL && CDR_FORCING: it reads MARBL's alternative-CO2
    tracers and carbonate-sensitivity code, so MARBL is a genuine requirement
    here. Defaults mirror :class:`CdrTracerOutputCfg`.
    """

    do_cdr_gas_exch_output: bool = False
    do_avg: bool = Field(default=True, serialization_alias="wrt_cdr_gas_avg")
    monthly_averages: bool = Field(
        default=False, serialization_alias="cdr_gas_monthly_averages"
    )
    output_period: float = Field(
        default=3600.0, serialization_alias="output_period_cdr_gas"
    )
    nrpf: int = Field(default=4, serialization_alias="nrpf_cdr_gas")


# (section key, its do-flag, whether the stream needs MARBL) for the two
# ucla-roms >= 0.7.0 CDR output streams that :func:`check_cdr_output_sections`
# enforces. Both compile under CDR_FORCING; only the gas-exchange stream reads
# MARBL state (see the two Cfg docstrings above).
CDR_OUTPUT_SECTIONS: tuple[tuple[str, str, bool], ...] = (
    ("cdr_tracer_output", "do_cdr_tracer_output", False),
    ("cdr_gas_exch_output", "do_cdr_gas_exch_output", True),
)


def check_cdr_output_sections(
    run_time_settings: dict[str, Any],
    *,
    bgc_mode_is_marbl: bool,
) -> bool:
    """Validate ``cdr_tracer_output``/``cdr_gas_exch_output`` (ucla-roms >= 0.7.0's
    dedicated CDR output streams, PR #351) and report whether
    ``cppdefs.cdr_forcing`` must be forced on.

    Unlike ``cdr_output`` (see ``CdrOutputCfg``), these two sections are never
    forced on by an active CDR forcing mode -- they're opt-in extras a user
    enables explicitly, so only the flag actually present in
    ``run_time_settings`` is read here. Either flag being True needs the
    ``CDR_FORCING`` cppdef; the gas-exchange stream additionally needs MARBL,
    the tracer stream does not (``CDR_OUTPUT_SECTIONS`` records which).

    Both the resolver (authoring time) and the executor's ``configure_build``
    (the build-time net for stored blueprints and wizard accordion edits that
    reach the build without re-resolving) call this so the rule and its
    message stay in one place; each caller is responsible for actually
    setting ``cppdefs["cdr_forcing"] = True`` when this returns ``True``, since
    ``cppdefs`` lives in a different dict in each caller.

    Raises ``ValueError`` if a MARBL-requiring flag is set while
    ``bgc_mode_is_marbl`` is False.
    """
    force_cdr_forcing = False
    for section_name, do_flag, requires_marbl in CDR_OUTPUT_SECTIONS:
        section = run_time_settings.get(section_name)
        if not section or not section.get(do_flag):
            continue
        if requires_marbl and not bgc_mode_is_marbl:
            raise ValueError(
                f'{do_flag}=True but bgc_mode != "marbl": ucla-roms compiles the '
                "CDR gas-exchange output module only under MARBL && CDR_FORCING "
                "(it reads MARBL's alternative-CO2 tracers)."
            )
        force_cdr_forcing = True
    return force_cdr_forcing


class UpscaleOutputCfg(_SettingsSection):
    do_upscale: bool
    nrpf_uscl: int
    output_period_uscl: float


class LinRhoEosCfg(_SettingsSection):
    tcoef: float
    t0: float
    scoef: float
    s0: float


class SssCorrectionCfg(_SettingsSection):
    dsssdt: float


class SstCorrectionCfg(_SettingsSection):
    dsstdt: float


class DicAlkCorrectionCfg(_SettingsSection):
    dcdt: float


class DiagnosticsCfg(_SettingsSection):
    diag_avg: bool
    output_period: float = Field(serialization_alias="output_period_diag")
    nrpf: int = Field(serialization_alias="nrpf_diag")
    diag_uv: bool
    diag_trc: bool


class StdoutDiagCfg(_SettingsSection):
    code_check_mode: bool


class RandomOutputCfg(_SettingsSection):
    do_random: bool
    output_period: float = Field(serialization_alias="output_period_random")
    nrpf: int = Field(serialization_alias="nrpf_random")


class SurfFluxCfg(_SettingsSection):
    wrt_smflx: bool
    wrt_stflx: bool
    wrt_rstflx: bool
    wrt_swflx: bool
    sflx_avg: bool
    output_period: float = Field(serialization_alias="output_period_sflx")
    nrpf: int = Field(serialization_alias="nrpf_sflx")


class PipeFrcCfg(_SettingsSection):
    pipe_source: bool
    p_analytical: bool
    npip: int


class _ParticlesCfgCommon(_SettingsSection):
    floats: bool
    np: int
    extra_space_fac: float
    exchange_facx: float
    exchange_facy: float
    exchange_facc: float
    ppm3: float
    pmin: int


class ParticlesCfg(_ParticlesCfgCommon):
    """``particles`` settings for ucla-roms < 0.5.0."""

    output_period: float
    nrpf: int


class ParticlesCfgV0_5_0(_ParticlesCfgCommon):
    """``particles`` settings for ucla-roms >= 0.5.0.

    ucla-roms 0.5.0 renamed the namelist keys ``output_period``/``nrpf`` to
    ``output_period_particles``/``nrpf_particles``; forge's settings
    vocabulary keeps the original names (``output_period``/``nrpf``) and only
    the ``serialization_alias`` changes.
    """

    output_period: float = Field(serialization_alias="output_period_particles")
    nrpf: int = Field(serialization_alias="nrpf_particles")


class LateralViscCfg(_SettingsSection):
    visc2: float
    rho0: float


class VerticalMixingCfg(_SettingsSection):
    akv: float
    akt_default: float


class TracerDiff2Cfg(_SettingsSection):
    tnu2_default: float


class BottomDragCfg(_SettingsSection):
    rdrg: float
    rdrg2: float
    zob: float


class VSpongeCfg(_SettingsSection):
    v_sponge: float


class MarblBgcCfg(_SettingsSection):
    marbl_config_file: str
    marbl_tracers_to_write: list[str] | str
    marbl_diagnostics_to_write: list[str] | str
    marbl_timestep: float


class _RunTimeSettingsCommon(_SettingsSection):
    """Forge's run-time settings dict, typed + validated.

    Sections are required: the (per-ModelSpec) YAML must define them all. There
    are no value defaults here — the YAML is the single source of defaults.

    Not meant to be used directly: the version-varying sections (``ocean_vars``,
    ``particles``) are typed as the loose common models here, and version-varying
    sections that some schemas lack entirely (``pio_settings``, added by
    :class:`RunTimeSettingsV0_6_0`; ``cdr_tracer_output``/``cdr_gas_exch_output``,
    added by :class:`RunTimeSettingsV0_7_0`) are simply absent here; use
    :class:`RunTimeSettings` (ucla-roms < 0.5.0), :class:`RunTimeSettingsV0_5_0`
    (0.5.0 <= ucla-roms < 0.6.0), :class:`RunTimeSettingsV0_6_0`
    (0.6.0 <= ucla-roms < 0.7.0), or :class:`RunTimeSettingsV0_7_0` (>= 0.7.0),
    or select one with :func:`run_time_settings_for_ref`.
    """

    title: TitleCfg
    output_root_name: OutputRootNameCfg
    time_stepping: TimeSteppingCfg
    reference_date_settings: ReferenceDateCfg
    grid: GridCfg
    s_coord: SCoordCfg
    initial: InitialCfg
    forcing: ForcingCfg
    lateral_visc: LateralViscCfg
    vertical_mixing: VerticalMixingCfg
    tracer_diff2: TracerDiff2Cfg
    bottom_drag: BottomDragCfg
    v_sponge: VSpongeCfg
    gamma2: float
    ubind: float
    param: ParamCfg
    bgc: BgcCfg
    blk_frc: BlkFrcCfg
    cdr_output: CdrOutputCfg
    ocean_vars: _OceanVarsCfgCommon
    surf_flux: SurfFluxCfg
    tides: TidesCfg
    river_frc: RiverFrcCfg
    diagnostics: DiagnosticsCfg
    cdr_frc: CdrFrcCfg
    extract_data: ExtractDataCfg
    sponge_tune: SpongeTuneCfg
    upscale_output: UpscaleOutputCfg
    flux_frc: FluxFrcCfg
    ts_output: TsOutputCfg
    frc_output: FrcOutputCfg
    calc_pflx: CalcPflxCfg
    zslice: ZsliceCfg
    stdout_diag: StdoutDiagCfg
    random_output: RandomOutputCfg
    pipe_frc: PipeFrcCfg
    particles: _ParticlesCfgCommon
    lin_rho_eos: LinRhoEosCfg
    sss_correction: SssCorrectionCfg
    sst_correction: SstCorrectionCfg
    dic_alk_correction: DicAlkCorrectionCfg
    marbl_bgc: MarblBgcCfg

    @model_validator(mode="after")
    def _rst_period_divisible_by_dt(self) -> _RunTimeSettingsCommon:
        check_rst_period_divisible(self.time_stepping.dt, self.ocean_vars)
        return self


class RunTimeSettings(_RunTimeSettingsCommon):
    """Forge's run-time settings dict for ucla-roms < 0.5.0, typed + validated.

    Kept unversioned (no suffix) for backward compatibility: this is the name
    historically used by forge.
    """

    ocean_vars: OceanVarsCfg
    particles: ParticlesCfg


class RunTimeSettingsV0_5_0(_RunTimeSettingsCommon):
    """Forge's run-time settings dict for ucla-roms >= 0.5.0, typed + validated."""

    ocean_vars: OceanVarsCfgV0_5_0
    particles: ParticlesCfgV0_5_0


class RunTimeSettingsV0_6_0(RunTimeSettingsV0_5_0):
    """Forge's run-time settings dict for ucla-roms >= 0.6.0, typed + validated.

    Subclasses :class:`RunTimeSettingsV0_5_0` directly (rather than
    ``_RunTimeSettingsCommon``) to inherit its ``ocean_vars``/``particles``
    variants unchanged -- mirrors C-Star's ``RomsNamelistV0_6_0(RomsNamelistV0_5_0)``.
    Adds ``pio_settings`` (ucla-roms PR #346, ``&PIO_SETTINGS``); the field
    carries a default (see :class:`PioSettingsCfg`) so a 0.6.0-pinned blueprint
    saved before this section existed still validates.
    """

    pio_settings: PioSettingsCfg = Field(default_factory=PioSettingsCfg)


class RunTimeSettingsV0_7_0(RunTimeSettingsV0_6_0):
    """Forge's run-time settings dict for ucla-roms >= 0.7.0, typed + validated.

    Subclasses :class:`RunTimeSettingsV0_6_0` directly (rather than
    ``_RunTimeSettingsCommon``) to inherit its ``ocean_vars``/``particles``/
    ``pio_settings`` unchanged -- mirrors C-Star's
    ``RomsNamelistV0_7_0(RomsNamelistV0_6_0)``. Adds ``cdr_tracer_output`` and
    ``cdr_gas_exch_output`` (ucla-roms PR #351, ``&CDR_TRACER_OUTPUT_SETTINGS``/
    ``&CDR_GAS_EXCH_OUTPUT_SETTINGS``); both fields carry defaults (see
    :class:`CdrTracerOutputCfg`/:class:`CdrGasExchOutputCfg`) so a 0.7.0-pinned
    blueprint saved before these sections existed still validates. Unlike
    ``cdr_output``, neither is forced on by an active CDR forcing mode -- see
    the resolver's CDR tracer/gas-exchange output consistency check.
    """

    cdr_tracer_output: CdrTracerOutputCfg = Field(default_factory=CdrTracerOutputCfg)
    cdr_gas_exch_output: CdrGasExchOutputCfg = Field(
        default_factory=CdrGasExchOutputCfg
    )


# Maps each namelist schema class (C-Star, keyed by ucla-roms version range) to
# the matching run-time settings class (forge's settings vocabulary).
_RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA: dict[
    type[RomsNamelistBase], type[_RunTimeSettingsCommon]
] = {
    RomsNamelist: RunTimeSettings,
    RomsNamelistV0_5_0: RunTimeSettingsV0_5_0,
    RomsNamelistV0_6_0: RunTimeSettingsV0_6_0,
    RomsNamelistV0_7_0: RunTimeSettingsV0_7_0,
}

# The inverse of _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA: the namelist schema a
# given run-time settings class corresponds to. Used by
# output_precheck_applies_to to derive the schema class from a settings_cls
# the caller already holds (e.g. from run_time_settings_for_ref), rather than
# re-deriving it via a second, differently-defaulting lookup -- see that
# function's docstring.
_NAMELIST_SCHEMA_BY_RUN_TIME_SETTINGS: dict[
    type[_RunTimeSettingsCommon], type[RomsNamelistBase]
] = {v: k for k, v in _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA.items()}


def output_precheck_applies_to(settings_cls: type[_RunTimeSettingsCommon]) -> bool:
    """True if C-Star's >= 0.5.0 output-stream/restart-rollover precheck
    (:func:`cstar.roms.precheck.check_output_streams_divide_rst`) applies to
    the ucla-roms release ``settings_cls`` (a
    :func:`run_time_settings_for_ref` result) was selected for.

    Derives the namelist schema from ``settings_cls`` itself (inverting
    :data:`_RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA`) rather than re-deriving it
    by calling :func:`cstar.roms.namelist.namelist_schema_for_ref` a second
    time at the call site: ``run_time_settings_for_ref(None | "")`` returns
    the legacy :class:`RunTimeSettings`, but ``namelist_schema_for_ref(None)``
    warns and returns the *latest* schema -- calling the latter fresh here
    would flip the precheck on for a no-ref blueprint that ``settings_cls``
    says is legacy.
    """
    return _output_precheck_applies_to(
        _NAMELIST_SCHEMA_BY_RUN_TIME_SETTINGS[settings_cls]
    )


def version_gated_section_names() -> frozenset[str]:
    """Section names modeled by at least one registered run-time settings tier
    (:data:`_RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA`).

    Distinguishes a section that's *version-gated* -- schema-modeled by some
    tier but not necessarily the active one, e.g. ``pio_settings`` (only on
    :class:`RunTimeSettingsV0_6_0`) -- from a section that's *never*
    schema-modeled by any run-time settings class at all, e.g. ``cppdefs`` (a
    compile-time settings dict with no run-time settings model counterpart).
    The wizard's accordion editor (``_SettingsEditor``, ``forge_blueprint_wizard.py``)
    uses this to decide whether a section absent from the *active* schema
    should still render via type-inference (cppdefs: yes, always) or be
    skipped (a version-gated section under an older ref: yes, skip -- the
    active schema is ``extra="ignore"`` at the top level, so an inferred
    widget's edits would be silently discarded downstream rather than raising).
    """
    return frozenset(
        name
        for cls in _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA.values()
        for name in cls.model_fields
    )


def prune_version_gated_sections(
    run_time_settings: dict[str, Any], settings_cls: type[_RunTimeSettingsCommon]
) -> list[str]:
    """Drop, in place, every ``run_time_settings`` section that is version-gated
    (:func:`version_gated_section_names`) but not modeled by ``settings_cls``,
    the tier selected for the pinned ucla-roms ref; return the dropped names,
    sorted.

    A section the pinned release's namelist schema doesn't model would be
    silently discarded by the top-level ``extra="ignore"`` when the settings
    are validated for ``build_namelist``, yet still steer everything that
    reads the raw settings dict first: the CDR output nets (forcing
    ``cppdefs.cdr_forcing`` on, or rejecting a non-MARBL build, for a stream
    that release can't write) and the resolver's output-stream precheck.
    Both the resolver (authoring time,
    before settings are frozen into the blueprint) and the executor's
    ``configure_build`` (the build-time net for stored blueprints that reach
    the build without re-resolving) call this before those checks, so the
    rule stays in one place; each caller decides whether to report what was
    dropped.
    """
    pruned = sorted(
        (version_gated_section_names() - set(settings_cls.model_fields))
        & set(run_time_settings)
    )
    for name in pruned:
        del run_time_settings[name]
    return pruned


def run_time_settings_for_ref(roms_ref: str | None) -> type[_RunTimeSettingsCommon]:
    """Select the run-time settings class matching a ucla-roms ref.

    Parameters
    ----------
    roms_ref : str or None
        The ucla-roms git ref (tag, branch, or commit hash) the blueprint's
        code is pinned to, e.g. ``code.roms.commit or code.roms.branch``.
        ``None`` preserves forge's historical behavior (the legacy schema),
        so existing callers that don't yet thread a ref through keep working
        unchanged.

    Returns
    -------
    type[_RunTimeSettingsCommon]
        :class:`RunTimeSettings` for ucla-roms < 0.5.0 or when `roms_ref` is
        `None`; :class:`RunTimeSettingsV0_5_0` for 0.5.0 <= ucla-roms < 0.6.0;
        :class:`RunTimeSettingsV0_6_0` for 0.6.0 <= ucla-roms < 0.7.0;
        :class:`RunTimeSettingsV0_7_0` for ucla-roms >= 0.7.0.

    Warns
    -----
    UserWarning
        Propagated unchanged from :func:`cstar.roms.namelist.namelist_schema_for_ref`
        when `roms_ref` isn't a release tag (branch name, commit hash, or
        unparseable) — the latest known schema is used in that case.
    """
    # Falsy covers "" as well as None: a hand-edited blueprint can carry
    # commit=null + branch="", and callers pass `commit or branch` — an empty
    # string must mean "no ref" (legacy), not "unparseable ref" (latest).
    if not roms_ref:
        return RunTimeSettings
    schema = namelist_schema_for_ref(roms_ref)
    try:
        return _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA[schema]
    except KeyError:
        # C-Star is installed from its main branch, so its schema registry can
        # grow a new version before forge maps it — fail with the fix, not a
        # bare KeyError.
        raise ValueError(
            f"C-Star selected namelist schema {schema.__name__} for ucla-roms "
            f"ref {roms_ref!r}, but this cstar-forge version has no matching "
            f"run-time settings model. Update cstar-forge (add the new variant "
            f"to _RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA) or pin an older "
            f"ucla-roms release."
        ) from None


# Canonical forcing order -> frcfiles (matches write_roms_namelist).
_FORCING_ORDER = (
    "surface_forcing_path",
    "surface_forcing_bgc_path",
    "boundary_forcing_path",
    "boundary_forcing_bgc_path",
    "tidal_forcing_path",
    "river_path",
)

# The two *_bgc_path fields hold a list (one path per bgc source in that
# category) rather than a single scalar path -- see ForcingCfg.
_FORCING_LIST_KEYS = frozenset(
    {"surface_forcing_bgc_path", "boundary_forcing_bgc_path"}
)


def build_namelist(rt: _RunTimeSettingsCommon, n_tracers: int) -> RomsNamelistBase:
    """The settings -> namelist transform.

    Most groups map 1:1 from a settings section via ``model_dump(by_alias=True)``
    — the ``serialization_alias`` on each renamed settings field supplies the
    namelist name, and case-only keys were lowercased in both vocabularies. The
    only explicit logic left is genuinely structural: the title/output-root
    regroup, the frcfiles assembly, the scalar -> per-tracer-array expansion, and
    the cross-section read of ``rho0`` from ``lateral_visc``. ``exclude=`` drops
    the settings-only fields with no namelist counterpart.

    ``rt``'s concrete type (:class:`RunTimeSettings`, :class:`RunTimeSettingsV0_5_0`,
    :class:`RunTimeSettingsV0_6_0`, or :class:`RunTimeSettingsV0_7_0`) selects the
    matching namelist schema and ``basic_output_settings``/``particles_settings``
    group classes — the ``ocean_vars``/``particles`` sections already carry the
    right fields and aliases for that variant, so no other branch is needed.
    ``pio_settings`` (added by ``RunTimeSettingsV0_6_0``) and
    ``cdr_tracer_output``/``cdr_gas_exch_output`` (added by
    ``RunTimeSettingsV0_7_0``) are sections a variant can lack entirely rather
    than just carry a different subtype (an older namelist schema rejects the
    group outright, ``extra="forbid"``), so each is added to the constructor
    kwargs only when ``rt`` is an instance of the class that introduced it —
    checked in most-specific-first order (``RunTimeSettingsV0_7_0`` before its
    superclass ``RunTimeSettingsV0_6_0`` before ITS superclass
    ``RunTimeSettingsV0_5_0``) since ``isinstance`` also matches subclasses.
    """
    if isinstance(rt, RunTimeSettingsV0_7_0):
        namelist_cls: type[RomsNamelistBase] = RomsNamelistV0_7_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_6_0):
        namelist_cls = RomsNamelistV0_6_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_5_0):
        namelist_cls = RomsNamelistV0_5_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    else:
        namelist_cls = RomsNamelist
        basic_output_cls = BasicOutputSettings
        particles_cls = ParticlesSettings

    def grp(section) -> dict:
        return section.model_dump(by_alias=True)

    # A *_bgc_path field is a list (one path per bgc source); every other
    # forcing field is a single optional scalar path. ROMS scans all listed
    # frcfiles for whichever variables it needs, so order doesn't matter to it --
    # sorting each bgc list keeps this deterministic regardless of the order bgc
    # sources happened to be generated in.
    frc: list[str] = []
    for k in _FORCING_ORDER:
        v = getattr(rt.forcing, k)
        if k in _FORCING_LIST_KEYS:
            frc.extend(sorted(v))
        elif v is not None:
            frc.append(v)

    kwargs: dict[str, Any] = dict(
        # ---- structural transforms (regroup / computed / cross-section) ----
        simulation_name_settings=SimulationNameSettings(
            output_root_name=rt.output_root_name.output_root_name,
            title=rt.title.casename,
        ),  # regroup; casename -> title
        forcing_files=ForcingFiles(frcfiles=frc),  # 6 *_path -> frcfiles list
        tracer_diff2=TracerDiff2(tnu2=[rt.tracer_diff2.tnu2_default] * n_tracers),
        vertical_mixing_settings=VerticalMixingSettings(
            akv_bak=rt.vertical_mixing.akv,  # akv -> akv_bak
            akt_bak=[rt.vertical_mixing.akt_default] * n_tracers,
        ),
        rho0_settings=Rho0Settings(rho0=rt.lateral_visc.rho0),  # cross-section
        gamma2_settings=Gamma2Settings(gamma2=rt.gamma2),
        ubind_settings=UbindSettings(ubind=rt.ubind),
        diagnostics_settings=DiagnosticsSettings(**grp(rt.diagnostics)),
        basic_output_settings=basic_output_cls(
            **rt.ocean_vars.model_dump(by_alias=True)
        ),
        lateral_visc_settings=LateralViscSettings(
            **rt.lateral_visc.model_dump(by_alias=True, exclude={"rho0"})
        ),
        surf_frc_settings=SurfFrcSettings(**{**grp(rt.blk_frc), **grp(rt.flux_frc)}),
        # ---- 1:1 groups (aliases handle the renames) ----
        time_stepping=TimeStepping(**grp(rt.time_stepping)),
        reference_date_settings=ReferenceDateSettings(
            **grp(rt.reference_date_settings)
        ),
        grid_settings=GridSettings(**grp(rt.grid)),
        s_coord=SCoord(**grp(rt.s_coord)),
        param_settings=ParamSettings(**grp(rt.param)),
        initial_conditions=InitialConditions(**grp(rt.initial)),
        river_frc_settings=RiverFrcSettings(**grp(rt.river_frc)),
        tidal_frc_settings=TidalFrcSettings(**grp(rt.tides)),
        ts_output_settings=TsOutputSettings(**grp(rt.ts_output)),
        frc_output_settings=FrcOutputSettings(**grp(rt.frc_output)),
        extract_data_settings=ExtractDataSettings(**grp(rt.extract_data)),
        sponge_tune_settings=SpongeTuneSettings(**grp(rt.sponge_tune)),
        calc_pflx_settings=CalcPflxSettings(**grp(rt.calc_pflx)),
        zslice_settings=ZsliceSettings(**grp(rt.zslice)),
        bgc_settings=BgcSettings(**grp(rt.bgc)),
        marbl_biogeochemistry_settings=MarblBiogeochemistrySettings(
            **grp(rt.marbl_bgc)
        ),
        cdr_frc_settings=CdrFrcSettings(**grp(rt.cdr_frc)),
        cdr_output_settings=CdrOutputSettings(**grp(rt.cdr_output)),
        upscale_settings=UpscaleSettings(**grp(rt.upscale_output)),
        lin_rho_eos_settings=LinRhoEosSettings(**grp(rt.lin_rho_eos)),
        bottom_drag_settings=BottomDragSettings(**grp(rt.bottom_drag)),
        sss_correction=SssCorrection(**grp(rt.sss_correction)),
        sst_correction=SstCorrection(**grp(rt.sst_correction)),
        dic_alk_correction=DicAlkCorrection(**grp(rt.dic_alk_correction)),
        stdout_diag_settings=StdoutDiagSettings(**grp(rt.stdout_diag)),
        random_output_settings=RandomOutputSettings(**grp(rt.random_output)),
        surf_flx_output_settings=SurfFlxOutputSettings(**grp(rt.surf_flux)),
        pipe_frc_settings=PipeFrcSettings(**grp(rt.pipe_frc)),
        particles_settings=particles_cls(**grp(rt.particles)),
        v_sponge_settings=VSpongeSettings(**grp(rt.v_sponge)),
    )
    if isinstance(rt, RunTimeSettingsV0_6_0):
        kwargs["pio_settings"] = PioSettings(**grp(rt.pio_settings))
    if isinstance(rt, RunTimeSettingsV0_7_0):
        kwargs["cdr_tracer_output_settings"] = CdrTracerOutputSettings(
            **grp(rt.cdr_tracer_output)
        )
        kwargs["cdr_gas_exch_output_settings"] = CdrGasExchOutputSettings(
            **grp(rt.cdr_gas_exch_output)
        )
    return namelist_cls(**kwargs)


def validate_run_time_sections(
    settings: dict, roms_ref: str | None = None
) -> list[str]:
    """Validate the *present* run-time sections of a (possibly partial) settings dict
    against the run-time settings schema, returning a list of human-readable errors
    (empty if all good).

    Unlike ``RunTimeSettings.model_validate``, this does NOT require every section —
    it checks only the sections that are present, so it works on a ``ForgeBlueprint``'s
    flat ``model_settings`` (which omits the processing-filled sections). Keys with no
    run-time-settings counterpart (e.g. ``cppdefs``, a compile-time section) are
    skipped. Use it for fail-fast feedback on hand-edited / loaded configs, where the
    inner values are otherwise opaque (``model_settings`` is ``Dict[str, Any]``).

    Parameters
    ----------
    settings : dict
        The (possibly partial) run-time settings dict to validate.
    roms_ref : str or None
        The ucla-roms ref the blueprint's code is pinned to, forwarded to
        :func:`run_time_settings_for_ref` to select the schema variant.
        ``None`` (default) preserves the legacy schema.
    """
    errors: list[str] = []
    fields = run_time_settings_for_ref(roms_ref).model_fields
    for key, value in (settings or {}).items():
        if key not in fields:
            continue  # not a run-time section (e.g. cppdefs) — nothing to check here
        try:
            TypeAdapter(fields[key].annotation).validate_python(value)
        except ValidationError as exc:
            for err in exc.errors():
                loc = ".".join(str(p) for p in (key, *err["loc"]))
                errors.append(f"{loc}: {err['msg']}")

    # Cross-section invariant: the per-section loop above validates each section
    # independently (TypeAdapter can't see across keys), so it never catches
    # output_period_rst/dt divisibility -- only checkable when both sections are
    # present (both are user-authored, not in _PROCESSING_FILLED_SECTIONS, so a
    # full blueprint's model_settings always carries them). Delegates entirely to
    # check_rst_period_divisible -- no gating logic duplicated here.
    settings = settings or {}
    if "time_stepping" in settings and "ocean_vars" in settings:
        try:
            check_rst_period_divisible(
                (settings["time_stepping"] or {}).get("dt"), settings["ocean_vars"]
            )
        except ValueError as exc:
            errors.append(str(exc))
    return errors


# Maps each forge settings-dict section that check_output_streams_divide_rst's
# canonical table reads to (the forge Cfg class that types/aliases that raw
# section, the C-Star RomsNamelistBase group field name the canonical table
# expects). ``ocean_vars``/``upscale_output`` need no aliasing at all -- their
# forge field names already ARE the real Fortran namelist keys -- but are
# still routed through their Cfg class so a malformed section raises loudly
# rather than silently mismatching field names. Version-pinned to the >= 0.5.0
# variants (``OceanVarsCfgV0_5_0``, ``ParticlesCfgV0_5_0``): both call sites of
# :func:`canonical_output_sections_for_precheck` only run this check under the
# :func:`output_precheck_applies_to` (>= 0.5.0) gate.
_PRECHECK_SECTION_MAP: dict[str, tuple[type[_SettingsSection], str]] = {
    "ocean_vars": (OceanVarsCfgV0_5_0, "basic_output_settings"),
    "frc_output": (FrcOutputCfg, "frc_output_settings"),
    "random_output": (RandomOutputCfg, "random_output_settings"),
    "zslice": (ZsliceCfg, "zslice_settings"),
    "surf_flux": (SurfFluxCfg, "surf_flx_output_settings"),
    "particles": (ParticlesCfgV0_5_0, "particles_settings"),
    "sponge_tune": (SpongeTuneCfg, "sponge_tune_settings"),
    "diagnostics": (DiagnosticsCfg, "diagnostics_settings"),
    "cdr_output": (CdrOutputCfg, "cdr_output_settings"),
    "cdr_tracer_output": (CdrTracerOutputCfg, "cdr_tracer_output_settings"),
    "cdr_gas_exch_output": (CdrGasExchOutputCfg, "cdr_gas_exch_output_settings"),
    "upscale_output": (UpscaleOutputCfg, "upscale_settings"),
    "bgc": (BgcCfg, "bgc_settings"),
    "extract_data": (ExtractDataCfg, "extract_data_settings"),
}

# The inverse of _PRECHECK_SECTION_MAP: canonical RomsNamelistBase group field
# name -> (the forge settings-dict section that maps to it, its Cfg class).
# Used by forge_field_for to point a NamelistConsistencyError's canonical
# section/keys back at the forge settings-dict field the wizard actually
# edits.
_FORGE_SECTION_BY_CANONICAL_GROUP: dict[str, tuple[str, type[_SettingsSection]]] = {
    group_name: (section_name, cfg_cls)
    for section_name, (cfg_cls, group_name) in _PRECHECK_SECTION_MAP.items()
}


def canonical_output_sections_for_precheck(settings: dict[str, Any]) -> dict[str, Any]:
    """Translate the output-stream-relevant sections of a forge run-time
    settings dict into C-Star's canonical namelist vocabulary (RomsNamelistBase
    group field name -> its aliased field dict), for
    :func:`cstar.roms.precheck.check_output_streams_divide_rst` (or this
    module's guarded shim of the same name).

    Deliberately narrower than a full ``RunTimeSettings.model_validate`` +
    :func:`build_namelist`: at resolve time (``build_forge_blueprint``), the
    settings dict is missing the processing-filled sections (``title``/
    ``grid``/``initial``/``forcing``/``s_coord``/``output_root_name``, plus
    ``reference_date_settings`` -- all populated later, at
    ``generate_inputs()``/executor time), so a full run-time-settings
    validation can't succeed yet. None of those sections affect any
    output-stream field, so this only validates+aliases the sections the
    checker actually reads (see :data:`_PRECHECK_SECTION_MAP`). A section
    absent from ``settings`` is simply omitted from the result -- the checker
    already treats an absent section as "skip that stream".

    Always includes ``extract_data`` (the nesting `extract` stream) when
    present in ``settings`` -- it's unconditionally fully populated by
    ``_EXTRACT_DATA_DEFAULT`` before the resolver reaches this call, so
    including it here never trips ``ExtractDataCfg.model_validate`` on a
    missing-required-field it wouldn't otherwise hit. A caller that wants a
    more actionable message for that stream specifically (naming the
    authoring-time knob that produced it) can catch
    ``NamelistConsistencyError`` and check ``exc.section ==
    "extract_data_settings"``.
    """
    out: dict[str, Any] = {}
    for section_name, (cfg_cls, group_name) in _PRECHECK_SECTION_MAP.items():
        section = settings.get(section_name)
        if section is None:
            continue
        out[group_name] = cfg_cls.model_validate(section).model_dump(by_alias=True)
    return out


def forge_field_for(section: str, key: str) -> str | None:
    """Reverse-lookup: a ``NamelistConsistencyError``'s canonical
    ``section``/one of its ``keys`` (a ``RomsNamelistBase`` group field name
    and real Fortran namelist key) -> the forge settings-dict ``"section.field"``
    the wizard actually edits, via :data:`_PRECHECK_SECTION_MAP`'s Cfg classes
    and their ``serialization_alias``. Returns ``None`` if ``section`` isn't
    one of the output-stream-check groups this table maps (not every namelist
    group has a forge settings-dict counterpart via this table).
    """
    entry = _FORGE_SECTION_BY_CANONICAL_GROUP.get(section)
    if entry is None:
        return None
    forge_section, cfg_cls = entry
    for field_name, info in cfg_cls.model_fields.items():
        if (info.serialization_alias or field_name) == key:
            return f"{forge_section}.{field_name}"
    return None
