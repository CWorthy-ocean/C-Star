"""
Forge's run-time settings schema (``RunTimeSettings``) and the settings →
namelist transform (``build_namelist``).

* :class:`RunTimeSettings` validates and types forge's run-time settings dict —
  the YAML vocabulary (``tcline``, ``np_xi``, ``analytical`` …). Defaults live
  in each ModelSpec's ``run-time-defaults.yml``, not here: fields are required
  so an incomplete YAML fails loudly; only the runtime-filled fields (grid file,
  IC file, s-coord, forcing paths, casename, output root) are ``Optional``.
* :func:`build_namelist` transforms a validated ``RunTimeSettings`` into a
  :class:`cstar.roms.namelist.RomsNamelist` (renames via ``serialization_alias``,
  cross-section regrouping, scalar → per-tracer-array expansion, ``frcfile``
  assembly), reading typed fields — no ``bool()/int()/float()/str()`` coercion.

The namelist schema itself — ``RomsNamelist`` and its 40 ``&group`` models —
lives in C-Star (:mod:`cstar.roms.namelist`) and is imported here; that is the
reusable read/edit/write schema. ``settings.write_roms_namelist`` is a thin
wrapper over ``build_namelist``.
"""
from __future__ import annotations

import os
from typing import Annotated, List, Optional, Union

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from cstar.roms.namelist import (
    RomsNamelist,
    SimulationNameSettings, TimeStepping, GridSettings, SCoord, ParamSettings,
    InitialConditions, ForcingFiles, BulkFrcSettings, FluxFrcSettings,
    RiverFrcSettings, TidesSettings, BasicOutputSettings, TsOutputSettings,
    FrcOutputSettings, ExtractDataSettings, SpongeTuneSettings, CalcPflxSettings,
    ZsliceSettings, BgcSettings, MarblBiogeochemistrySettings, CdrFrcSettings,
    CdrOutputSettings, UpscaleSettings, LinRhoEosSettings, Rho0Settings,
    Gamma2Settings, TracerDiff2, BottomDragSettings, VerticalMixingSettings,
    LateralViscSettings, UbindSettings, VSpongeSettings, SssCorrection,
    SstCorrection, DiagnosticsSettings, StdoutDiagSettings, RandomOutputSettings,
    SurfFlxSettings, PipeFrcSettings, ParticlesSettings,
)

def _coerce_pathlike(v):
    """Accept ``pathlib.Path``/``os.PathLike`` for path-valued settings fields —
    input generation fills them with ``Path`` objects — coercing to ``str``.
    ``None`` and ``str`` pass through unchanged."""
    return os.fspath(v) if isinstance(v, os.PathLike) else v


# An optional path string that also accepts a Path (coerced to str). Used for
# the settings fields that input generation populates with Path objects.
PathStr = Annotated[Optional[str], BeforeValidator(_coerce_pathlike)]


# ===========================================================================
# Settings-vocabulary models (forge's run-time dict, validated)
#
# DEFAULTS LIVE IN THE YAML, NOT HERE. Each ModelSpec ships its own
# run-time-defaults.yml with its own values; these models only *validate and
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
    casename: Optional[str] = None       # set dynamically


class OutputRootNameCfg(_SettingsSection):
    output_root_name: Optional[str] = None   # set dynamically


class TimeSteppingCfg(_SettingsSection):
    ntimes: int
    dt: float
    ndtfast: int
    ninfo: int


class GridCfg(_SettingsSection):
    grid_file: PathStr = Field(default=None, serialization_alias="grdname")  # set from generated grid


class SCoordCfg(_SettingsSection):
    theta_s: Optional[float] = None      # set from grid
    theta_b: Optional[float] = None
    tcline: Optional[float] = Field(default=None, serialization_alias="hc")


class ParamCfg(_SettingsSection):
    llm: int
    mmm: int
    n: int
    np_xi: int
    np_eta: int
    nsub_x: int
    nsub_e: int
    nt_passive: int
    ntrc_bio: int


class InitialCfg(_SettingsSection):
    nrrec: int
    initial_file: PathStr = Field(default=None, serialization_alias="ininame")  # set from generated IC


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


class TsOutputCfg(_SettingsSection):
    wrt_temp: bool
    wrt_salt: bool
    wrt_temp_dia: bool
    wrt_salt_dia: bool


class FrcOutputCfg(_SettingsSection):
    wrt_frc: bool
    wrt_frc_avg: bool
    output_period: float
    nrpf: int


class ExtractDataCfg(_SettingsSection):
    do_extract: bool
    extract_file: str
    nrpf: int = Field(serialization_alias="nrpf_extract")
    n_chd: int
    theta_s_chd: float
    theta_b_chd: float
    hc_chd: float
    extract_period: float


class SpongeTuneCfg(_SettingsSection):
    ub_tune: bool
    spn_avg: bool
    sp_timscale: float
    wrt_sponge: bool
    nrpf: int
    output_period: float


class CalcPflxCfg(_SettingsSection):
    calc_pflx: bool
    timescale: float


class ZsliceCfg(_SettingsSection):
    do_zslice: bool
    zslice_avg: bool
    wrt_t_zsl: bool
    wrt_u_zsl: bool
    wrt_v_zsl: bool
    output_period: float
    nrpf: int
    ndep: int
    vecdep: List[float]
    nt_z: int
    trc2zsc: List[int]


class BgcCfg(_SettingsSection):
    interp_frc: bool = Field(serialization_alias="interp_bgc_frc")
    wrt_his: bool = Field(serialization_alias="wrt_bgc_his")
    output_period_his: float
    nrpf_his: int
    wrt_avg: bool = Field(serialization_alias="wrt_bgc_avg")
    output_period_avg: float
    nrpf_avg: int
    wrt_his_dia: bool = Field(serialization_alias="wrt_bgc_dia_his")
    output_period_his_dia: float
    nrpf_his_dia: int
    wrt_avg_dia: bool = Field(serialization_alias="wrt_bgc_dia_avg")
    output_period_avg_dia: float
    nrpf_avg_dia: int


class CdrFrcCfg(_SettingsSection):
    cdr_source: bool
    cdr_file: str
    ncdr_parm: int
    nz_chd: int
    forcing_depth_profiles: bool
    forcing_3d: bool
    forcing_parameterized: bool
    time_interpolation: bool
    relocate_to_wet_pts: bool
    cdr_volume: bool


class CdrOutputCfg(_SettingsSection):
    do_cdr: bool = Field(serialization_alias="do_cdr_output")
    do_avg: bool = Field(serialization_alias="wrt_cdr_avg")
    monthly_averages: bool = Field(serialization_alias="cdr_monthly_averages")
    output_period: float
    nrpf: int


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


class DiagnosticsCfg(_SettingsSection):
    diag_avg: bool
    output_period: float
    nrpf: int
    diag_uv: bool
    diag_trc: bool


class StdoutDiagCfg(_SettingsSection):
    code_check_mode: bool


class RandomOutputCfg(_SettingsSection):
    do_random: bool
    output_period: float
    nrpf: int


class SurfFluxCfg(_SettingsSection):
    wrt_smflx: bool
    wrt_stflx: bool
    wrt_swflx: bool
    sflx_avg: bool
    output_period: float
    nrpf: int


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
    marbl_tracers_to_write: Union[List[str], str]
    marbl_diagnostics_to_write: Union[List[str], str]
    marbl_timestep_ratio: int


class RunTimeSettings(_SettingsSection):
    """Forge's run-time settings dict, typed + validated.

    Sections are required: the (per-ModelSpec) YAML must define them all. There
    are no value defaults here — the YAML is the single source of defaults.
    """
    title: TitleCfg
    output_root_name: OutputRootNameCfg
    time_stepping: TimeSteppingCfg
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
    marbl_bgc: MarblBgcCfg


# Canonical forcing order -> frcfile (matches write_roms_namelist).
_FORCING_ORDER = (
    "surface_forcing_path", "surface_forcing_bgc_path",
    "boundary_forcing_path", "boundary_forcing_bgc_path",
    "tidal_forcing_path", "river_path",
)


def build_namelist(rt: RunTimeSettings, n_tracers: int) -> RomsNamelist:
    """The settings -> namelist transform.

    Most groups map 1:1 from a settings section via ``model_dump(by_alias=True)``
    — the ``serialization_alias`` on each renamed settings field supplies the
    namelist name, and case-only keys were lowercased in both vocabularies. The
    only explicit logic left is genuinely structural: the title/output-root
    regroup, the frcfile assembly, the scalar -> per-tracer-array expansion, and
    the cross-section read of ``rho0`` from ``lateral_visc``. ``exclude=`` drops
    the settings-only fields with no namelist counterpart.
    """
    def grp(section) -> dict:
        return section.model_dump(by_alias=True)

    frc = [getattr(rt.forcing, k) for k in _FORCING_ORDER
           if getattr(rt.forcing, k) is not None]

    return RomsNamelist(
        # ---- structural transforms (regroup / computed / cross-section) ----
        simulation_name_settings=SimulationNameSettings(
            output_root_name=rt.output_root_name.output_root_name,
            title=rt.title.casename),                              # regroup; casename -> title
        forcing_files=ForcingFiles(frcfile=frc),                  # 6 *_path -> frcfile list
        tracer_diff2=TracerDiff2(tnu2=[rt.tracer_diff2.tnu2_default] * n_tracers),
        vertical_mixing_settings=VerticalMixingSettings(
            akv_bak=rt.vertical_mixing.akv,                       # akv -> akv_bak
            akt_bak=[rt.vertical_mixing.akt_default] * n_tracers),
        rho0_settings=Rho0Settings(rho0=rt.lateral_visc.rho0),    # cross-section
        gamma2_settings=Gamma2Settings(gamma2=rt.gamma2),
        ubind_settings=UbindSettings(ubind=rt.ubind),
        diagnostics_settings=DiagnosticsSettings(**grp(rt.diagnostics)),
        basic_output_settings=BasicOutputSettings(
            **rt.ocean_vars.model_dump(by_alias=True)),
        lateral_visc_settings=LateralViscSettings(
            **rt.lateral_visc.model_dump(by_alias=True, exclude={"rho0"})),
        # ---- 1:1 groups (aliases handle the renames) ----
        time_stepping=TimeStepping(**grp(rt.time_stepping)),
        grid_settings=GridSettings(**grp(rt.grid)),
        s_coord=SCoord(**grp(rt.s_coord)),
        param_settings=ParamSettings(**grp(rt.param)),
        initial_conditions=InitialConditions(**grp(rt.initial)),
        bulk_frc_settings=BulkFrcSettings(**grp(rt.blk_frc)),
        flux_frc_settings=FluxFrcSettings(**grp(rt.flux_frc)),
        river_frc_settings=RiverFrcSettings(**grp(rt.river_frc)),
        tides_settings=TidesSettings(**grp(rt.tides)),
        ts_output_settings=TsOutputSettings(**grp(rt.ts_output)),
        frc_output_settings=FrcOutputSettings(**grp(rt.frc_output)),
        extract_data_settings=ExtractDataSettings(**grp(rt.extract_data)),
        sponge_tune_settings=SpongeTuneSettings(**grp(rt.sponge_tune)),
        calc_pflx_settings=CalcPflxSettings(**grp(rt.calc_pflx)),
        zslice_settings=ZsliceSettings(**grp(rt.zslice)),
        bgc_settings=BgcSettings(**grp(rt.bgc)),
        marbl_biogeochemistry_settings=MarblBiogeochemistrySettings(**grp(rt.marbl_bgc)),
        cdr_frc_settings=CdrFrcSettings(**grp(rt.cdr_frc)),
        cdr_output_settings=CdrOutputSettings(**grp(rt.cdr_output)),
        upscale_settings=UpscaleSettings(**grp(rt.upscale_output)),
        lin_rho_eos_settings=LinRhoEosSettings(**grp(rt.lin_rho_eos)),
        bottom_drag_settings=BottomDragSettings(**grp(rt.bottom_drag)),
        sss_correction=SssCorrection(**grp(rt.sss_correction)),
        sst_correction=SstCorrection(**grp(rt.sst_correction)),
        stdout_diag_settings=StdoutDiagSettings(**grp(rt.stdout_diag)),
        random_output_settings=RandomOutputSettings(**grp(rt.random_output)),
        surf_flx_settings=SurfFlxSettings(**grp(rt.surf_flux)),
        pipe_frc_settings=PipeFrcSettings(**grp(rt.pipe_frc)),
        particles_settings=ParticlesSettings(**grp(rt.particles)),
        v_sponge_settings=VSpongeSettings(**grp(rt.v_sponge)),
    )
