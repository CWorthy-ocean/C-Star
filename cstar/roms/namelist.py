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

Per-field docstrings are transcribed from the ucla-roms reference
``src/namelist.nml`` (``! ...`` comments).

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


def _as_list(v: Any) -> list:
    """f90nml collapses single-element arrays to scalars on read; re-wrap."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    return [v]


class _NmlGroup(BaseModel):
    # validate_assignment => edits in the read->edit->write flow are re-checked.
    # use_attribute_docstrings => the per-field docstrings below become the field
    # descriptions in the generated JSON schema.
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, use_attribute_docstrings=True
    )


class SimulationNameSettings(_NmlGroup):
    output_root_name: str
    """Output file prefix (e.g. `roms_bgc.20120101120000.nc`)"""
    title: str
    """Title used in output metadata"""


class TimeStepping(_NmlGroup):
    ntimes: int
    """Number of time steps in this run"""
    dt: float
    """Time step (seconds)"""
    ndtfast: int
    """Number of fast time-steps per slow timestep"""
    ninfo: int
    """Number of steps between runtime diagnostics (STDOUT)"""


class GridSettings(_NmlGroup):
    grdname: str
    """Grid file path"""


class SCoord(_NmlGroup):
    theta_s: float
    """S-coordinate surface stretching parameter"""
    theta_b: float
    """S-coordinate bottom stretching parameter"""
    hc: float
    """Critical depth (m)"""


class ParamSettings(_NmlGroup):
    np_xi: int
    """Number of processors following X"""
    np_eta: int
    """Number of processors following Y"""
    llm: int
    """Number of grid points in X"""
    mmm: int
    """Number of grid points in Y"""
    nz: int
    """Number of vertical levels"""
    nt_passive: int
    """Number of passive tracers"""
    nt_bgc: int
    """Number of BGC tracers"""


class InitialConditions(_NmlGroup):
    inifile: str
    """Initial conditions (IC) file path"""


class ForcingFiles(_NmlGroup):
    frcfiles: list[str] = Field(default_factory=list)
    """Forcing file paths (e.g. boundary, surface flux, and river forcing)"""

    @field_validator("frcfiles", mode="before")
    @classmethod
    def _wrap(cls, v: Any) -> list:
        """Re-wrap a scalar `frcfiles` value into a list (f90nml read collapse)."""
        return _as_list(v)


class SurfFrcSettings(_NmlGroup):
    interp_bulk_frc: bool
    """Interpolate forcing from coarser input grid if T"""
    check_bulk_frc_units: bool
    """Check units of input vars if T"""
    interp_flux_frc: bool
    """Interpolate forcing from coarser input grid if T"""


class RiverFrcSettings(_NmlGroup):
    river_source: bool
    """T if river inputs used, else F"""
    river_analytical: bool
    """T if river inputs specified analytically"""
    nriv: int
    """Number of rivers"""


class TidalFrcSettings(_NmlGroup):
    bry_tides: bool
    """Barotropic tides at domain boundaries"""
    pot_tides: bool
    """Surface potential tides"""
    ana_tides: bool
    """Tidal forcing specified analytically"""
    ntides: int
    """Number of tidal constituents"""


class BasicOutputSettings(_NmlGroup):
    wrt_file_his: bool
    """Write instantaneous ocean physical state to output"""
    output_period_his: float
    """Frequency of instantaneous output (s)"""
    nrpf_his: int
    """Number of time records in instantaneous output file"""
    wrt_z: bool
    """Include `zeta`"""
    wrt_ub: bool
    """Include `ubar`"""
    wrt_vb: bool
    """Include `vbar`"""
    wrt_u: bool
    """Include `u`"""
    wrt_v: bool
    """Include `v`"""
    wrt_r: bool
    """Include `rho`"""
    wrt_o: bool
    """Include `omega`"""
    wrt_w: bool
    """Include `w`"""
    wrt_akv: bool
    """Include `Akv`"""
    wrt_akt: bool
    """Include `Akt`"""
    wrt_aks: bool
    """Include `Aks`"""
    wrt_hbls: bool
    """Include `hbls`"""
    wrt_hbbl: bool
    """Include `hbbl`"""
    wrt_file_avg: bool
    """Write averages of ocean physical state to output"""
    output_period_avg: float
    """Frequency of averaged output/averaging period (s)"""
    nrpf_avg: int
    """Number of time records in averaged output file"""
    wrt_avg_z: bool
    """Include `zeta`"""
    wrt_avg_ub: bool
    """Include `ubar`"""
    wrt_avg_vb: bool
    """Include `vbar`"""
    wrt_avg_u: bool
    """Include `u`"""
    wrt_avg_v: bool
    """Include `v`"""
    wrt_avg_r: bool
    """Include `rho`"""
    wrt_avg_o: bool
    """Include `omega`"""
    wrt_avg_w: bool
    """Include `w`"""
    wrt_avg_akv: bool
    """Include `Akv`"""
    wrt_avg_akt: bool
    """Include `Akt`"""
    wrt_avg_aks: bool
    """Include `Aks`"""
    wrt_avg_hbls: bool
    """Include `hbls`"""
    wrt_avg_hbbl: bool
    """Include `hbbl`"""
    wrt_file_rst: bool
    """Write restart files (containing full model state)"""
    monthly_restarts: bool
    """Write restart files at start of calendar month"""
    output_period_rst: float
    """Write restart files at regular frequency (s)"""
    nrpf_rst: int
    """Number of time records in restart files"""


class TsOutputSettings(_NmlGroup):
    wrt_temp: bool
    """Include temperature in output fields"""
    wrt_salt: bool
    """Include salinity in output fields"""
    wrt_temp_dia: bool
    """Include temperature diagnostics in output fields"""
    wrt_salt_dia: bool
    """Include salinity diagnostics in output fields"""


class FrcOutputSettings(_NmlGroup):
    wrt_frc: bool
    """Write model forcing to its own output file"""
    wrt_frc_avg: bool
    """Forcing output averaged (T) or instantaneous (F)"""
    output_period_frc: float
    """Frequency/averaging period of forcing output"""
    nrpf_frc: int
    """Number of time records in forcing files"""


class ExtractDataSettings(_NmlGroup):
    do_extract: bool
    """Generate boundary files for a nested domain"""
    output_period_extract: float
    """How often to output these files"""
    nrpf_extract: int
    """Number of time records per file"""
    extract_file: str
    """File path containing nesting info"""
    n_chd: int
    """Number of vertical levels in nested domain"""
    theta_s_chd: float
    """`theta_s` of nested domain"""
    theta_b_chd: float
    """`theta_b` of nested domain"""
    hc_chd: float
    """`hc` of nested domain"""


class SpongeTuneSettings(_NmlGroup):
    ub_tune: bool
    """Tune boundary "sponge" to match parent bry conditions"""
    sponge_timescale: float
    """Filtering time scale (s)"""
    wrt_sponge: bool
    """Write out sponge tuning values"""
    sponge_avg: bool
    """Sponge tuning output averaged (T) or instantaneous (F)"""
    nrpf_sponge: int
    """Number of records per sponge file"""
    output_period_sponge: float
    """Output frequency of sponge tuning file"""


class CalcPflxSettings(_NmlGroup):
    calc_pflx: bool
    """Enable baroclinic pressure flux calculation"""
    pflx_timescale: float
    """Timescale for filtering pressure fluxes (s)"""


class ZsliceSettings(_NmlGroup):
    do_zslice: bool
    """Output certain variables on regular z-levels"""
    zslice_avg: bool
    """Averaged output (T) or instantaneous (F)"""
    wrt_t_zslice: bool
    """Write tracers to z-level output"""
    wrt_u_zslice: bool
    """Write zonal velocity to z-level output"""
    wrt_v_zslice: bool
    """Write meridional velocity to z-level output"""
    output_period_zslice: float
    """Frequency of z-level output"""
    nrpf_zslice: int
    """Number of records per file"""
    ndep: int
    """Number of depth levels on which to write"""
    vecdep: list[float]
    """Depths of levels on which to write"""
    nt_zslice: int
    """Number of tracers to include"""
    trc2zsc: list[int]
    """Indices of tracers to include"""

    @field_validator("vecdep", "trc2zsc", mode="before")
    @classmethod
    def _wrap(cls, v: Any) -> list:
        """Re-wrap scalar `vecdep`/`trc2zsc` values into lists (f90nml read collapse)."""
        return _as_list(v)


class BgcSettings(_NmlGroup):
    interp_bgc_frc: bool
    """Interpolate forcing from coarser input grid if T"""
    wrt_bgc_his: bool
    """Write instantaneous BGC tracers to output"""
    output_period_bgc_his: float
    """Frequency of instantaneous BGC output (s)"""
    nrpf_bgc_his: int
    """Number of time records per BGC output file"""
    wrt_bgc_avg: bool
    """Write averaged BGC tracers to output"""
    output_period_bgc_avg: float
    """Output frequency/averaging period (s)"""
    nrpf_bgc_avg: int
    """Number of time records per BGC average file"""
    wrt_bgc_dia_his: bool
    """Write instantaneous BGC diagnostics to output"""
    output_period_bgc_his_dia: float
    """Frequency of diagnostics output (s)"""
    nrpf_bgc_his_dia: int
    """Number of time records per BGC diagnostics file"""
    wrt_bgc_dia_avg: bool
    """Write averaged BGC diagnostics to output"""
    output_period_bgc_avg_dia: float
    """Frequency/period of averaged diagnostic output"""
    nrpf_bgc_avg_dia: int
    """Number of time records per BGC diagnostics file"""


class MarblBiogeochemistrySettings(_NmlGroup):
    marbl_config_file: str = "marbl_in"
    """MARBL configuration file"""
    marbl_tracers_to_write: list[str] | str = ""
    """MARBL tracers to include in BGC output"""
    marbl_diagnostics_to_write: list[str] | str = ""
    """MARBL diagnostics to include in BGC output"""
    marbl_timestep: float = 3600.0
    """Desired MARBL timestep (s); ROMS derives the step ratio from `dt`"""

    @field_validator("marbl_tracers_to_write", mode="before")
    @classmethod
    def _tracers(cls, v: Any) -> str | list[str]:
        """Normalize `marbl_tracers_to_write` into a namelist string list."""
        return _namelist_str_list(
            v, max_len=MARBL_TRACERS_TO_WRITE_MAX, name="marbl_tracers_to_write"
        )

    @field_validator("marbl_diagnostics_to_write", mode="before")
    @classmethod
    def _diags(cls, v: Any) -> str | list[str]:
        """Normalize `marbl_diagnostics_to_write` into a namelist string list."""
        return _namelist_str_list(
            v, max_len=MARBL_DIAGNOSTICS_TO_WRITE_MAX, name="marbl_diagnostics_to_write"
        )


class CdrFrcSettings(_NmlGroup):
    cdr_source: bool
    """Apply CDR perturbation (T) or not (F)"""
    cdr_file: str
    """File path to CDR perturbation"""
    cdr_ncdr_parm: int
    """Number of CDR releases if `3D`/`parameterized`"""
    cdr_nz_chd: int
    """Number of vertical levels in CDR forcing"""
    cdr_forcing_depth_profiles: bool
    """Apply CDR forcing from a depth profile"""
    cdr_forcing_3d: bool
    """Apply CDR forcing from a fully 3D distribution"""
    cdr_forcing_parameterized: bool
    """Apply CDR forcing from Gaussian parameters"""
    cdr_time_interpolation: bool
    """Interpolate linearly between forcing records"""
    cdr_relocate_to_wet_pts: bool
    """Relocate CDR perturbation to sea if on land"""
    cdr_volume: bool
    """Read in volume flux/tracer concentration"""


class CdrOutputSettings(_NmlGroup):
    do_cdr_output: bool
    """Output CDR-relevant fields"""
    wrt_cdr_avg: bool
    """Write averaged (T) or instantaneous (F) output"""
    cdr_monthly_averages: bool
    """Write averaged outputs per calendar month"""
    output_period_cdr: float
    """Frequency of CDR-relevant output"""
    nrpf_cdr: int
    """Time records per output file"""


class UpscaleSettings(_NmlGroup):
    do_upscale: bool
    """Record CDR tracer fluxes thru domain boundaries"""
    nrpf_uscl: int
    """Number of records per file"""
    output_period_uscl: float
    """Output frequency"""


class LinRhoEosSettings(_NmlGroup):
    tcoef: float
    """Thermal expansion coefficient (kg/m2/K)"""
    t0: float
    """Reference temperature (*C)"""
    scoef: float
    """Saline contraction coefficient (kg/m3/psu)"""
    s0: float
    """Reference salinity (psu)"""


class Rho0Settings(_NmlGroup):
    rho0: float
    """Boussinesq reference density (kg/m3)"""


class Gamma2Settings(_NmlGroup):
    gamma2: float
    """Slipperiness parameter (free-slip = +1, no-slip = -1)"""


class TracerDiff2(_NmlGroup):
    tnu2: list[float]
    """Horizontal Laplacian diffusion (m2/s) for each tracer"""

    @field_validator("tnu2", mode="before")
    @classmethod
    def _wrap(cls, v: Any) -> list:
        """Re-wrap a scalar `tnu2` value into a list (f90nml read collapse)."""
        return _as_list(v)


class BottomDragSettings(_NmlGroup):
    rdrg: float
    """Linear bottom drag co-efficient (m/s)"""
    rdrg2: float
    """Quadratic bottom drag co-efficient (dimensionless)"""
    zob: float
    """Bottom roughness height (m)"""


class VerticalMixingSettings(_NmlGroup):
    akv_bak: float
    """Vertical viscosity (m2/s)"""
    akt_bak: list[float]
    """Vertical mixing (m2/s) for each tracer"""

    @field_validator("akt_bak", mode="before")
    @classmethod
    def _wrap(cls, v: Any) -> list:
        """Re-wrap a scalar `akt_bak` value into a list (f90nml read collapse)."""
        return _as_list(v)


class LateralViscSettings(_NmlGroup):
    visc2: float
    """Horizontal Laplacian kinematic viscosity (m2/s)"""


class UbindSettings(_NmlGroup):
    ubind: float
    """Open boundary binding velocity (m/s)"""


class VSpongeSettings(_NmlGroup):
    v_sponge: float
    """Maximum viscosity in sponge layer (m2/s)"""


class SssCorrection(_NmlGroup):
    dsssdt: float
    """SSS correction co-efficient as piston velocity (cm/day)"""


class SstCorrection(_NmlGroup):
    dsstdt: float
    """SST correction co-efficient as piston velocity (cm/day)"""


class DiagnosticsSettings(_NmlGroup):
    diag_avg: bool
    """Output physics diags as avgs (T) or snapshots (F)"""
    diag_uv: bool
    """Output momentum diagnostics"""
    diag_trc: bool
    """Output tracer diagnostics"""
    output_period_diag: float
    """Output frequency (s)"""
    nrpf_diag: int
    """Number of records per output file"""


class StdoutDiagSettings(_NmlGroup):
    # code_check_mode lives only here (ROMS &STDOUT_DIAG_SETTINGS), not in
    # &DIAGNOSTICS_SETTINGS.
    code_check_mode: bool
    """Diagnostics in stdout formatted for code testing"""


class RandomOutputSettings(_NmlGroup):
    do_random: bool
    """Output user-customized output fields"""
    output_period_random: float
    """Frequency of custom output (s)"""
    nrpf_random: int
    """Number of records per output file"""


class SurfFlxOutputSettings(_NmlGroup):
    wrt_smflx: bool
    """Output surface momentum flux"""
    wrt_stflx: bool
    """Output surface tracer flux"""
    wrt_rstflx: bool
    """Output surface restoring flux (already accounted in stflx)"""
    wrt_swflx: bool
    """Output surface water flux (P-E)"""
    sflx_avg: bool
    """Output average (T) or instantaneous (F) fields"""
    output_period_sflx: float
    """Frequency of surface flux output (s)"""
    nrpf_sflx: int
    """Number of records per surface flux file"""


class PipeFrcSettings(_NmlGroup):
    pipe_source: bool
    """T if pipe inputs used, else F"""
    p_analytical: bool
    """T if pipe inputs specified analytically"""
    npip: int
    """Number of pipe inputs"""


class ParticlesSettings(_NmlGroup):
    floats: bool
    """Release Lagrangian particles (T) or not (F)"""
    np: int
    """Local number of particles"""
    extra_space_fac: float
    """Buffer space to receive extra exchanged particles"""
    exchange_facx: float
    """Maximum number of particles for transfer in N-S"""
    exchange_facy: float
    """Maximum number of particles for transfer in E-W"""
    exchange_facc: float
    """Maximum number of particles for transfer in corners"""
    output_period: float
    """Frequency of outputs"""
    nrpf: int
    """Number of records per file"""
    ppm3: float
    """Target particles per cubic meter"""
    pmin: int
    """Minimum of allocated space for particle array"""


class RomsNamelist(BaseModel):
    """A complete ROMS ``namelist.nml``, round-trippable via ``f90nml``.

    Group order matches ``write_roms_namelist`` / the reference namelist.
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, use_attribute_docstrings=True
    )

    simulation_name_settings: SimulationNameSettings
    time_stepping: TimeStepping
    grid_settings: GridSettings
    s_coord: SCoord
    param_settings: ParamSettings
    initial_conditions: InitialConditions
    forcing_files: ForcingFiles = Field(default_factory=ForcingFiles)
    surf_frc_settings: SurfFrcSettings
    river_frc_settings: RiverFrcSettings
    tidal_frc_settings: TidalFrcSettings
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
    surf_flx_output_settings: SurfFlxOutputSettings
    pipe_frc_settings: PipeFrcSettings
    particles_settings: ParticlesSettings

    # ---- f90nml round-trip ----
    @classmethod
    def from_f90nml(cls, nml: f90nml.Namelist) -> RomsNamelist:
        """Build a validated model from a parsed ``f90nml.Namelist``.

        Each ``&group`` in the namelist becomes a nested model; group and key
        names must match the model exactly, as the schema is strict
        (``extra="forbid"``).

        Parameters
        ----------
        nml : f90nml.Namelist
            A namelist parsed by ``f90nml`` (e.g. via :func:`f90nml.read`).

        Returns
        -------
        RomsNamelist
            The validated model.
        """
        return cls.model_validate({k: dict(v) for k, v in nml.items()})

    @classmethod
    def read(cls, path: str | Path) -> RomsNamelist:
        """Read and validate a ``namelist.nml`` file from disk.

        Parameters
        ----------
        path : str or Path
            Path to the ROMS ``namelist.nml`` file.

        Returns
        -------
        RomsNamelist
            The validated model parsed from the file.
        """
        return cls.from_f90nml(f90nml.read(str(path)))

    def to_f90nml_dict(self) -> dict:
        """Serialize the model to a plain dict suitable for ``f90nml``.

        Mirrors ``write_roms_namelist`` by omitting the ``forcing_files`` group
        entirely when ``frcfiles`` is empty, so no empty ``&forcing_files`` group
        is written.

        Returns
        -------
        dict
            A nested dict suitable for constructing an ``f90nml.Namelist``.
        """
        d = self.model_dump()
        # match write_roms_namelist: omit frcfiles entirely when empty
        if not d["forcing_files"].get("frcfiles"):
            d["forcing_files"] = {}
        return d

    def write(self, path: str | Path) -> None:
        """Write the model to a ``namelist.nml`` file, overwriting if present.

        Parameters
        ----------
        path : str or Path
            Destination path for the written namelist.
        """
        f90nml.Namelist(self.to_f90nml_dict()).write(str(path), force=True)
