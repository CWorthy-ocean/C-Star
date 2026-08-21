"""
Forge's run-time settings schema (``RunTimeSettings``) and the settings →
namelist transform (``build_namelist``).

* :class:`RunTimeSettings` validates and types forge's run-time settings dict —
  the YAML vocabulary (``tcline``, ``np_xi``, ``analytical`` …). Defaults live
  in each ModelSpec's ``run-time-defaults.yaml``, not here: fields are required
  so an incomplete YAML fails loudly; only the runtime-filled fields (grid file,
  IC file, s-coord, forcing paths, casename, output root) are ``Optional``.
* :func:`build_namelist` transforms a validated ``RunTimeSettings`` into a
  :class:`cstar.roms.namelist.RomsNamelist` (renames via ``serialization_alias``,
  cross-section regrouping, scalar → per-tracer-array expansion, ``frcfiles``
  assembly), reading typed fields — no ``bool()/int()/float()/str()`` coercion.

The namelist schema itself — ``RomsNamelist`` and its 40 ``&group`` models —
lives in C-Star (:mod:`cstar.roms.namelist`) and is imported here; that is the
reusable read/edit/write schema. ``settings.write_roms_namelist`` is a thin
wrapper over ``build_namelist``.
"""

from __future__ import annotations

import os
from typing import Annotated, Any

from cstar.roms.namelist import (
    BasicOutputSettings,
    BgcSettings,
    BottomDragSettings,
    CalcPflxSettings,
    CdrFrcSettings,
    CdrOutputSettings,
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
    PipeFrcSettings,
    RandomOutputSettings,
    ReferenceDateSettings,
    Rho0Settings,
    RiverFrcSettings,
    RomsNamelist,
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
)
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


def _coerce_pathlike(v):
    """Accept ``pathlib.Path``/``os.PathLike`` for path-valued settings fields —
    input generation fills them with ``Path`` objects — coercing to ``str``.
    ``None`` and ``str`` pass through unchanged.
    """
    return os.fspath(v) if isinstance(v, os.PathLike) else v


# An optional path string that also accepts a Path (coerced to str). Used for
# the settings fields that input generation populates with Path objects.
PathStr = Annotated[str | None, BeforeValidator(_coerce_pathlike)]


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


class InitialCfg(_SettingsSection):
    initial_file: PathStr = Field(
        default=None, serialization_alias="inifile"
    )  # set from generated IC


class ForcingCfg(_SettingsSection):
    surface_forcing_path: PathStr = None
    surface_forcing_bgc_path: PathStr = None
    boundary_forcing_path: PathStr = None
    boundary_forcing_bgc_path: PathStr = None
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


class OceanVarsCfg(_SettingsSection):
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
    nrpf_rst: int


def check_rst_period_divisible(
    dt: float | None, ocean_vars: OceanVarsCfg | dict[str, Any]
) -> None:
    """Raise ``ValueError`` if ``ocean_vars.output_period_rst`` isn't an integer
    multiple of ``dt`` -- restart writes must land on a timestep.

    Enforced only when restarts are written on a fixed period (``wrt_file_rst``
    True and ``monthly_restarts`` False); otherwise ``output_period_rst`` is
    unused and any value is accepted. ``dt`` missing/non-positive skips the
    check (other validation owns ``dt`` sanity). Accepts either a plain dict
    (the resolver's world) or an ``OceanVarsCfg`` (the pydantic validator's
    world) so both enforcement points share one message.
    """
    if isinstance(ocean_vars, dict):
        wrt_file_rst = ocean_vars.get("wrt_file_rst")
        monthly_restarts = ocean_vars.get("monthly_restarts")
        output_period_rst = ocean_vars.get("output_period_rst")
    else:
        wrt_file_rst = ocean_vars.wrt_file_rst
        monthly_restarts = ocean_vars.monthly_restarts
        output_period_rst = ocean_vars.output_period_rst

    if not wrt_file_rst or monthly_restarts:
        return
    if dt is None or output_period_rst is None or dt <= 0:
        return
    ratio = output_period_rst / dt
    if abs(ratio - round(ratio)) > 1e-9:
        raise ValueError(
            f"ocean_vars.output_period_rst ({output_period_rst} s) is not an "
            f"integer multiple of time_stepping.dt ({dt} s): restart writes "
            "must land on a timestep"
        )


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


class ParticlesCfg(_SettingsSection):
    floats: bool
    np: int
    extra_space_fac: float
    exchange_facx: float
    exchange_facy: float
    exchange_facc: float
    output_period: float
    nrpf: int
    ppm3: float
    pmin: int


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


class RunTimeSettings(_SettingsSection):
    """Forge's run-time settings dict, typed + validated.

    Sections are required: the (per-ModelSpec) YAML must define them all. There
    are no value defaults here — the YAML is the single source of defaults.
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
    ocean_vars: OceanVarsCfg
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
    particles: ParticlesCfg
    lin_rho_eos: LinRhoEosCfg
    sss_correction: SssCorrectionCfg
    sst_correction: SstCorrectionCfg
    dic_alk_correction: DicAlkCorrectionCfg
    marbl_bgc: MarblBgcCfg

    @model_validator(mode="after")
    def _rst_period_divisible_by_dt(self) -> RunTimeSettings:
        check_rst_period_divisible(self.time_stepping.dt, self.ocean_vars)
        return self


# Canonical forcing order -> frcfiles (matches write_roms_namelist).
_FORCING_ORDER = (
    "surface_forcing_path",
    "surface_forcing_bgc_path",
    "boundary_forcing_path",
    "boundary_forcing_bgc_path",
    "tidal_forcing_path",
    "river_path",
)


def build_namelist(rt: RunTimeSettings, n_tracers: int) -> RomsNamelist:
    """The settings -> namelist transform.

    Most groups map 1:1 from a settings section via ``model_dump(by_alias=True)``
    — the ``serialization_alias`` on each renamed settings field supplies the
    namelist name, and case-only keys were lowercased in both vocabularies. The
    only explicit logic left is genuinely structural: the title/output-root
    regroup, the frcfiles assembly, the scalar -> per-tracer-array expansion, and
    the cross-section read of ``rho0`` from ``lateral_visc``. ``exclude=`` drops
    the settings-only fields with no namelist counterpart.
    """

    def grp(section) -> dict:
        return section.model_dump(by_alias=True)

    frc = [
        getattr(rt.forcing, k)
        for k in _FORCING_ORDER
        if getattr(rt.forcing, k) is not None
    ]

    return RomsNamelist(
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
        basic_output_settings=BasicOutputSettings(
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
        particles_settings=ParticlesSettings(**grp(rt.particles)),
        v_sponge_settings=VSpongeSettings(**grp(rt.v_sponge)),
    )


def validate_run_time_sections(settings: dict) -> list[str]:
    """Validate the *present* run-time sections of a (possibly partial) settings dict
    against the ``RunTimeSettings`` schema, returning a list of human-readable errors
    (empty if all good).

    Unlike ``RunTimeSettings.model_validate``, this does NOT require every section —
    it checks only the sections that are present, so it works on a ``ForgeBlueprint``'s
    flat ``model_settings`` (which omits the processing-filled sections). Keys with no
    ``RunTimeSettings`` counterpart (e.g. ``cppdefs``, a compile-time section) are
    skipped. Use it for fail-fast feedback on hand-edited / loaded configs, where the
    inner values are otherwise opaque (``model_settings`` is ``Dict[str, Any]``).
    """
    errors: list[str] = []
    fields = RunTimeSettings.model_fields
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
