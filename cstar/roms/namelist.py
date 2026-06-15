"""
Pydantic model of the ROMS Fortran ``namelist.nml``.

:class:`RomsNamelist` mirrors the namelist's ``&group`` structure (one nested
model per group, exact ROMS key names). It round-trips through ``f90nml``:
:meth:`RomsNamelist.read` parses a ``namelist.nml`` into a validated model,
edits are type-checked (``validate_assignment``), and :meth:`RomsNamelist.write`
serializes it back. ``ROMSSimulation.roms_runtime_settings`` uses it to read the
runtime namelist, apply simulation overrides, and write the run-time copy.

Every group of a forge-produced namelist (which mirrors the pinned ucla-roms
namelist) is modeled. The models are strict (``extra="forbid"``): an unknown
group/key is rejected, catching drift/typos. A namelist that legitimately
carries other groups needs them added here; relax a model to ``extra="allow"``
for pass-through if strict validation is not wanted.

(C-Star Forge builds these from its settings dict — see
``cstar_forge.namelist_model.build_namelist`` — and writes a ``namelist.nml``
that this model reads back.)
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

import f90nml
from pydantic import BaseModel, ConfigDict, Field, field_validator

if TYPE_CHECKING:
    from pathlib import Path

# Fortran array bounds declared in ROMS src/marbl_driver.F90 for the namelist
# string lists (pinned ucla-roms commit). A list longer than these would
# overflow the declared array at run time.
MARBL_TRACERS_TO_WRITE_MAX = 40
MARBL_DIAGNOSTICS_TO_WRITE_MAX = 64


def _namelist_str_list(
    value: Any, *, max_len: int | None = None, name: str | None = None
) -> str | list[str]:
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
    frcfile: list[str] = Field(default_factory=list)

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
    vecdep: list[float]
    nt_z: int
    trc2zsc: list[int]

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
    marbl_tracers_to_write: list[str] | str = ""
    marbl_diagnostics_to_write: list[str] | str = ""
    marbl_timestep_ratio: int = 1

    @field_validator("marbl_tracers_to_write", mode="before")
    @classmethod
    def _tracers(cls, v):
        return _namelist_str_list(
            v, max_len=MARBL_TRACERS_TO_WRITE_MAX, name="marbl_tracers_to_write"
        )

    @field_validator("marbl_diagnostics_to_write", mode="before")
    @classmethod
    def _diags(cls, v):
        return _namelist_str_list(
            v, max_len=MARBL_DIAGNOSTICS_TO_WRITE_MAX, name="marbl_diagnostics_to_write"
        )


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
    tnu2: list[float]

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
    akt_bak: list[float]

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
    def from_f90nml(cls, nml) -> RomsNamelist:
        return cls.model_validate({k: dict(v) for k, v in nml.items()})

    @classmethod
    def read(cls, path: str | Path) -> RomsNamelist:
        return cls.from_f90nml(f90nml.read(str(path)))

    def to_f90nml_dict(self) -> dict:
        d = self.model_dump()
        # match write_roms_namelist: omit frcfile entirely when empty
        if not d["forcing_files"].get("frcfile"):
            d["forcing_files"] = {}
        return d

    def write(self, path: str | Path) -> None:
        f90nml.Namelist(self.to_f90nml_dict()).write(str(path), force=True)
