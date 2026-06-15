"""
Pydantic models for the ROMS ``namelist.nml`` and forge's run-time settings.

Two models, two vocabularies
----------------------------
* :class:`RunTimeSettings` — forge's settings/YAML vocabulary (``tcline``,
  ``NP_XI``, ``analytical`` …). Forge builds this as a dict today; it is
  validated into this model.
* :class:`RomsNamelist` — the Fortran namelist vocabulary (``hc``, ``np_xi``,
  ``river_analytical`` …), grouped exactly as ``f90nml`` writes ``namelist.nml``.
  Has :meth:`RomsNamelist.read` / :meth:`RomsNamelist.write` for the
  read → edit → write round-trip (reusable by other repos).

:func:`build_namelist` is the settings → namelist transform (renames,
regrouping, scalar→array expansion, ``frcfile`` assembly), reading typed fields
(no ``bool()/int()/float()/str()`` coercion) and assembling validated models.
``settings.write_roms_namelist`` is a thin wrapper over it.

Coverage
--------
Every group ``write_roms_namelist`` emits is modeled (full schema of a
forge-produced namelist, which mirrors the pinned ucla-roms namelist). The
models are strict (``extra="forbid"``): an unknown group/key is rejected, which
catches drift/typos. A namelist that legitimately carries groups forge does not
emit needs those groups added here (the shared-schema is the point); relax a
section/top model to ``extra="allow"`` if pass-through of unknown content is
preferred over strict validation.
"""
from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, List, Optional, Union

import f90nml
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Fortran array bounds declared in ROMS src/marbl_driver.F90 for the namelist
# string lists (pinned ucla-roms commit). A list longer than these would
# overflow the declared array at run time.
MARBL_TRACERS_TO_WRITE_MAX = 40
MARBL_DIAGNOSTICS_TO_WRITE_MAX = 64


def _namelist_str_list(value: Any, *, max_len: Optional[int] = None,
                       name: Optional[str] = None) -> Union[str, List[str]]:
    """
    Normalize a namelist string-list field (e.g. ``marbl_tracers_to_write``).

    ROMS declares these as Fortran string arrays. A YAML sequence becomes a
    list of strings (``f90nml`` emits a multi-element array, ``= 'a', 'b'``); a
    scalar string or ``None`` becomes the string itself, with an empty/absent
    value rendered as ``''`` (ROMS reads an empty first entry as "none").

    If ``max_len`` is given and a list exceeds it, a warning is emitted (the
    list is still written as-is; ROMS would overflow its fixed-size array).
    """
    if isinstance(value, (list, tuple)):
        items = [str(x) for x in value]
        if max_len is not None and len(items) > max_len:
            warnings.warn(
                f"{name or 'namelist list'} has {len(items)} entries but ROMS "
                f"declares it with {max_len}; the model will overflow this array. "
                f"Trim the list in run-time-defaults.yml.",
                UserWarning,
                stacklevel=2,
            )
        return items if items else ""
    if value is None:
        return ""
    return str(value)


def _as_list(v):
    """f90nml collapses single-element arrays to scalars on read; re-wrap."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    return [v]


# ===========================================================================
# Namelist-vocabulary models (one per &group written to namelist.nml)
# ===========================================================================
class _NmlGroup(BaseModel):
    # validate_assignment => edits in the read->edit->write flow are re-checked.
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SimulationNameSettings(_NmlGroup):
    output_root_name: str
    title: str


class TimeStepping(_NmlGroup):
    ntimes: int
    dt: float
    ndtfast: int
    ninfo: int


class GridSettings(_NmlGroup):
    grdname: str


class SCoord(_NmlGroup):
    theta_s: float
    theta_b: float
    hc: float


class ParamSettings(_NmlGroup):
    np_xi: int
    np_eta: int
    nsub_x: int
    nsub_e: int
    llm: int
    mmm: int
    n: int
    nt_passive: int
    ntrc_bio: int


class InitialConditions(_NmlGroup):
    ininame: str
    nrrec: int


class ForcingFiles(_NmlGroup):
    frcfile: List[str] = Field(default_factory=list)

    @field_validator("frcfile", mode="before")
    @classmethod
    def _wrap(cls, v):
        return _as_list(v)


class BulkFrcSettings(_NmlGroup):
    interp_bulk_frc: bool
    check_bulk_frc_units: bool


class FluxFrcSettings(_NmlGroup):
    interp_flux_frc: bool


class RiverFrcSettings(_NmlGroup):
    river_source: bool
    river_analytical: bool
    nriv: int


class TidesSettings(_NmlGroup):
    bry_tides: bool
    pot_tides: bool
    ana_tides: bool
    ntides: int


class BasicOutputSettings(_NmlGroup):
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


class TsOutputSettings(_NmlGroup):
    wrt_temp: bool
    wrt_salt: bool
    wrt_temp_dia: bool
    wrt_salt_dia: bool


class FrcOutputSettings(_NmlGroup):
    wrt_frc: bool
    wrt_frc_avg: bool
    output_period: float
    nrpf: int


class ExtractDataSettings(_NmlGroup):
    do_extract: bool
    extract_period: float
    nrpf_extract: int
    extract_file: str
    n_chd: int
    theta_s_chd: float
    theta_b_chd: float
    hc_chd: float


class SpongeTuneSettings(_NmlGroup):
    ub_tune: bool
    sp_timscale: float
    wrt_sponge: bool
    spn_avg: bool
    nrpf: int
    output_period: float


class CalcPflxSettings(_NmlGroup):
    calc_pflx: bool
    timescale: float


class ZsliceSettings(_NmlGroup):
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

    @field_validator("vecdep", "trc2zsc", mode="before")
    @classmethod
    def _wrap(cls, v):
        return _as_list(v)


class BgcSettings(_NmlGroup):
    interp_bgc_frc: bool
    wrt_bgc_his: bool
    output_period_his: float
    nrpf_his: int
    wrt_bgc_avg: bool
    output_period_avg: float
    nrpf_avg: int
    wrt_bgc_dia_his: bool
    output_period_his_dia: float
    nrpf_his_dia: int
    wrt_bgc_dia_avg: bool
    output_period_avg_dia: float
    nrpf_avg_dia: int


class MarblBiogeochemistrySettings(_NmlGroup):
    marbl_config_file: str = "marbl_in"
    marbl_tracers_to_write: Union[List[str], str] = ""
    marbl_diagnostics_to_write: Union[List[str], str] = ""
    marbl_timestep_ratio: int = 1

    @field_validator("marbl_tracers_to_write", mode="before")
    @classmethod
    def _tracers(cls, v):
        return _namelist_str_list(v, max_len=MARBL_TRACERS_TO_WRITE_MAX,
                                  name="marbl_tracers_to_write")

    @field_validator("marbl_diagnostics_to_write", mode="before")
    @classmethod
    def _diags(cls, v):
        return _namelist_str_list(v, max_len=MARBL_DIAGNOSTICS_TO_WRITE_MAX,
                                  name="marbl_diagnostics_to_write")


class CdrFrcSettings(_NmlGroup):
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


class CdrOutputSettings(_NmlGroup):
    do_cdr_output: bool
    wrt_cdr_avg: bool
    cdr_monthly_averages: bool
    output_period: float
    nrpf: int


class UpscaleSettings(_NmlGroup):
    do_upscale: bool
    nrpf_uscl: int
    output_period_uscl: float


class LinRhoEosSettings(_NmlGroup):
    tcoef: float
    t0: float
    scoef: float
    s0: float


class Rho0Settings(_NmlGroup):
    rho0: float


class Gamma2Settings(_NmlGroup):
    gamma2: float


class TracerDiff2(_NmlGroup):
    tnu2: List[float]

    @field_validator("tnu2", mode="before")
    @classmethod
    def _wrap(cls, v):
        return _as_list(v)


class BottomDragSettings(_NmlGroup):
    rdrg: float
    rdrg2: float
    zob: float


class VerticalMixingSettings(_NmlGroup):
    akv_bak: float
    akt_bak: List[float]

    @field_validator("akt_bak", mode="before")
    @classmethod
    def _wrap(cls, v):
        return _as_list(v)


class LateralViscSettings(_NmlGroup):
    visc2: float


class UbindSettings(_NmlGroup):
    ubind: float


class VSpongeSettings(_NmlGroup):
    v_sponge: float


class SssCorrection(_NmlGroup):
    dsssdt: float


class SstCorrection(_NmlGroup):
    dsstdt: float


class DiagnosticsSettings(_NmlGroup):
    diag_avg: bool
    diag_uv: bool
    diag_trc: bool
    output_period: float
    nrpf: int
    code_check_mode: bool


class StdoutDiagSettings(_NmlGroup):
    code_check_mode: bool


class RandomOutputSettings(_NmlGroup):
    do_random: bool
    output_period: float
    nrpf: int


class SurfFlxSettings(_NmlGroup):
    wrt_smflx: bool
    wrt_stflx: bool
    wrt_swflx: bool
    sflx_avg: bool
    output_period: float
    nrpf: int


class PipeFrcSettings(_NmlGroup):
    pipe_source: bool
    p_analytical: bool
    npip: int


class ParticlesSettings(_NmlGroup):
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


class RomsNamelist(BaseModel):
    """A complete ROMS ``namelist.nml``, round-trippable via ``f90nml``.

    Group order matches ``write_roms_namelist`` / the reference namelist.
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    simulation_name_settings: SimulationNameSettings
    time_stepping: TimeStepping
    grid_settings: GridSettings
    s_coord: SCoord
    param_settings: ParamSettings
    initial_conditions: InitialConditions
    forcing_files: ForcingFiles = Field(default_factory=ForcingFiles)
    bulk_frc_settings: BulkFrcSettings
    flux_frc_settings: FluxFrcSettings
    river_frc_settings: RiverFrcSettings
    tides_settings: TidesSettings
    basic_output_settings: BasicOutputSettings
    ts_output_settings: TsOutputSettings
    frc_output_settings: FrcOutputSettings
    extract_data_settings: ExtractDataSettings
    sponge_tune_settings: SpongeTuneSettings
    calc_pflx_settings: CalcPflxSettings
    zslice_settings: ZsliceSettings
    bgc_settings: BgcSettings
    marbl_biogeochemistry_settings: MarblBiogeochemistrySettings
    cdr_frc_settings: CdrFrcSettings
    cdr_output_settings: CdrOutputSettings
    upscale_settings: UpscaleSettings
    lin_rho_eos_settings: LinRhoEosSettings
    rho0_settings: Rho0Settings
    gamma2_settings: Gamma2Settings
    tracer_diff2: TracerDiff2
    bottom_drag_settings: BottomDragSettings
    vertical_mixing_settings: VerticalMixingSettings
    lateral_visc_settings: LateralViscSettings
    ubind_settings: UbindSettings
    v_sponge_settings: VSpongeSettings
    sss_correction: SssCorrection
    sst_correction: SstCorrection
    diagnostics_settings: DiagnosticsSettings
    stdout_diag_settings: StdoutDiagSettings
    random_output_settings: RandomOutputSettings
    surf_flx_settings: SurfFlxSettings
    pipe_frc_settings: PipeFrcSettings
    particles_settings: ParticlesSettings

    # ---- f90nml round-trip ----
    @classmethod
    def from_f90nml(cls, nml) -> "RomsNamelist":
        return cls.model_validate({k: dict(v) for k, v in nml.items()})

    @classmethod
    def read(cls, path: Union[str, Path]) -> "RomsNamelist":
        return cls.from_f90nml(f90nml.read(str(path)))

    def to_f90nml_dict(self) -> dict:
        d = self.model_dump()
        # match write_roms_namelist: omit frcfile entirely when empty
        if not d["forcing_files"].get("frcfile"):
            d["forcing_files"] = {}
        return d

    def write(self, path: Union[str, Path]) -> None:
        f90nml.Namelist(self.to_f90nml_dict()).write(str(path), force=True)


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
    grid_file: Optional[str] = Field(default=None, serialization_alias="grdname")  # set from generated grid


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
    initial_file: Optional[str] = Field(default=None, serialization_alias="ininame")  # set from generated IC


class ForcingCfg(_SettingsSection):
    surface_forcing_path: Optional[str] = None
    surface_forcing_bgc_path: Optional[str] = None
    boundary_forcing_path: Optional[str] = None
    boundary_forcing_bgc_path: Optional[str] = None
    tidal_forcing_path: Optional[str] = None
    river_path: Optional[str] = None


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
    code_check: bool


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
    the two cross-section reads (``rho0`` from ``lateral_visc``; ``code_check``
    from ``ocean_vars`` into ``diagnostics_settings``). ``exclude=`` drops the
    settings-only fields with no namelist counterpart.
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
        diagnostics_settings=DiagnosticsSettings(
            **grp(rt.diagnostics),
            code_check_mode=rt.ocean_vars.code_check),            # cross-section
        basic_output_settings=BasicOutputSettings(
            **rt.ocean_vars.model_dump(by_alias=True, exclude={"code_check"})),
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
