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
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Literal

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
    CdrLiteOutputSettings,
    CdrLiteSettings,
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
    ParamSettingsV0_4_0,
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
    RomsNamelistV0_4_0,
    RomsNamelistV0_5_0,
    RomsNamelistV0_6_0,
    RomsNamelistV0_7_0,
    RomsNamelistV0_9_0,
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
    roms_version_from_ref,
)
from cstar.roms.precheck import NamelistConsistencyError as NamelistConsistencyError
from cstar.roms.precheck import applies_to as _output_precheck_applies_to
from cstar.roms.precheck import (
    check_output_streams_divide_rst as _check_output_streams_divide_rst,
)
from cstar.roms.precheck import (
    check_restart_period_divisible_by_dt as _check_restart_period_divisible_by_dt,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


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
    casename: str | None = Field(
        default=None, serialization_alias="title"
    )  # set dynamically


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


# ``param`` keys counting ucla-roms' passive CDR tracers (>= 0.4.0 only), each
# mapped to the tracers one unit adds (an OAE tracer is an ALK/DIC pair).
_CDR_TRACER_WEIGHTS = {"nt_cdr_oae": 2, "nt_cdr_dor": 1}


class _ParamCfgCommon(_SettingsSection):
    llm: int
    mmm: int
    n: int = Field(serialization_alias="nz")
    np_xi: int
    np_eta: int
    nt_passive: int
    ntrc_bio: int = Field(serialization_alias="nt_bgc")


class ParamCfg(_ParamCfgCommon):
    """``param`` settings for ucla-roms < 0.4.0, which has no CDR tracer counts."""

    @model_validator(mode="before")
    @classmethod
    def _reject_cdr_tracer_counts(cls, data: Any) -> Any:
        """Fail loudly on a non-zero CDR tracer count rather than letting
        ``extra="ignore"`` drop it: the tracers would be counted into the
        per-tracer arrays (:func:`n_tracers_from_param`) but never created.
        """

        def _is_set(value: Any) -> bool:
            try:
                return int(value) != 0
            except (TypeError, ValueError):
                return value is not None  # unparseable: not a "no tracers" value

        if isinstance(data, dict):
            if set_keys := [k for k in _CDR_TRACER_WEIGHTS if _is_set(data.get(k))]:
                raise ValueError(
                    f"param.{'/'.join(set_keys)} requires ucla-roms >= 0.4.0; the "
                    f"pinned ucla-roms release has no passive CDR tracers."
                )
        return data


class ParamCfgV0_4_0(_ParamCfgCommon):
    """``param`` settings for ucla-roms >= 0.4.0.

    Deliberate defaults (like :class:`PioSettingsCfg`): blueprints saved before
    these keys existed bypass the resolver and hit ``model_validate`` directly,
    and 0 is the Fortran initializer (no CDR tracers).
    """

    nt_cdr_oae: int = Field(default=0, ge=0)
    nt_cdr_dor: int = Field(default=0, ge=0)


def n_tracers_from_param(param: dict[str, Any]) -> int:
    """Total ROMS tracer count from a ``param`` settings dict, mirroring
    ucla-roms ``param.F90``: T + S + BGC (``ntrc_bio``) + passive
    (``nt_passive``) + ``2*nt_cdr_oae + nt_cdr_dor`` (each OAE tracer is an
    ALK/DIC pair). Missing keys count as 0.
    """
    return (
        2
        + int(param.get("ntrc_bio", 0))
        + int(param.get("nt_passive", 0))
        + sum(w * int(param.get(k, 0)) for k, w in _CDR_TRACER_WEIGHTS.items())
    )


BgcMode = Literal["marbl", "none", "cdr_lite"]
"""The build mode a ModelSpec/blueprint selects for ocean biogeochemistry.

``"marbl"`` compiles MARBL (BGC tracers named by MARBL); ``"none"`` is a
physics-only build; ``"cdr_lite"`` is a build without MARBL whose only extra
tracers are ucla-roms' dedicated CDR-lite tracers (``CDR_LITE`` cppkey). Lives
here, with the rules keyed on it, so the guarded execution modules and
``models``/``resolve``/the catalog share one definition.
"""


def bgc_mode_from_cppdefs(cppdefs: Mapping[str, Any]) -> BgcMode:
    """The :data:`BgcMode` a stored blueprint's ``cppdefs`` encode.

    The one derivation of the three-valued mode from compile-time settings:
    ``marbl`` wins (MARBL with the online CDR-lite sensitivities also sets
    ``cdr_lite``, and is still a MARBL build); ``cdr_lite`` without MARBL is
    the CDR-lite mode; anything else is physics-only. Rules that only ask
    whether MARBL is present keep reading ``cppdefs.marbl`` directly.
    """
    if cppdefs.get("marbl"):
        return "marbl"
    return "cdr_lite" if cppdefs.get("cdr_lite") else "none"


# Names ucla-roms (and roms-tools' CDR forcing tracer axis) gives the generated
# tracers: ``passive_tracer<i>``, ``CDR_OAE_ALK<k>``/``CDR_OAE_DIC<k>`` (one pair
# per OAE release) and ``CDR_DOR_DIC<j>``. ``temp``/``salt`` lead every axis.
_PASSIVE_TRACER_RE = re.compile(r"passive_tracer(\d+)")
_OAE_TRACER_RE = re.compile(r"CDR_OAE_(ALK|DIC)(\d+)")
_DOR_TRACER_RE = re.compile(r"CDR_DOR_DIC(\d+)")
_PHYSICS_TRACER_NAMES = ("temp", "salt")


@dataclass(frozen=True)
class CdrTracerCounts:
    """Tracer counts read off a CDR forcing's tracer axis by :func:`cdr_tracer_counts`."""

    n_passive: int
    """Passive (dye) tracers, ``param.nt_passive``."""
    n_oae_pairs: int
    """OAE ALK/DIC pairs, ``param.nt_cdr_oae``."""
    n_dor: int
    """DOR tracers, ``param.nt_cdr_dor``."""
    other: tuple[str, ...]
    """Axis names that are none of the above (MARBL tracers), in axis order."""


def cdr_tracer_counts(tracer_names: Sequence[str]) -> CdrTracerCounts:
    """Count the generated tracers on a CDR forcing's ``tracer_name`` axis.

    ``temp``/``salt`` are skipped and unrecognized names (MARBL's) are returned
    in ``other``. Each family must be numbered ``1..n`` without gaps or repeats,
    and ``CDR_OAE_ALK<k>``/``CDR_OAE_DIC<k>`` must come as pairs: ROMS names its
    tracers from the counts alone, so any other axis cannot be described by
    ``nt_passive``/``nt_cdr_oae``/``nt_cdr_dor``.

    Raises ``ValueError`` listing every malformed family.
    """
    passive: list[int] = []
    oae: dict[str, list[int]] = {"ALK": [], "DIC": []}
    dor: list[int] = []
    other: list[str] = []
    for name in map(str, tracer_names):
        if name in _PHYSICS_TRACER_NAMES:
            continue
        if match := _PASSIVE_TRACER_RE.fullmatch(name):
            passive.append(int(match[1]))
        elif match := _OAE_TRACER_RE.fullmatch(name):
            oae[match[1]].append(int(match[2]))
        elif match := _DOR_TRACER_RE.fullmatch(name):
            dor.append(int(match[1]))
        else:
            other.append(name)
    problems = [
        f"{label}<k> is numbered {sorted(indices)}, not 1..{len(indices)}"
        for label, indices in (
            ("passive_tracer", passive),
            ("CDR_OAE_ALK", oae["ALK"]),
            ("CDR_OAE_DIC", oae["DIC"]),
            ("CDR_DOR_DIC", dor),
        )
        if sorted(indices) != list(range(1, len(indices) + 1))
    ]
    if len(oae["ALK"]) != len(oae["DIC"]):
        problems.append(
            f"CDR_OAE_ALK<k>/CDR_OAE_DIC<k> do not pair up ({len(oae['ALK'])} ALK, "
            f"{len(oae['DIC'])} DIC)"
        )
    if problems:
        raise ValueError(
            "CDR forcing tracer axis is not a valid ROMS tracer layout: "
            + "; ".join(problems)
            + "."
        )
    return CdrTracerCounts(
        n_passive=len(passive),
        n_oae_pairs=len(oae["ALK"]),
        n_dor=len(dor),
        other=tuple(other),
    )


def check_bgc_tracer_count(param: dict[str, Any], *, bgc_mode_is_marbl: bool) -> None:
    """Reject BGC tracers (``param.ntrc_bio``, the namelist's ``nt_bgc``) in a
    build without MARBL.

    ucla-roms sizes its tracer arrays from ``nt_bgc`` at runtime, but only MARBL
    names the BGC tracer slots: without it they stay uninitialized and ROMS later
    aborts looking up forcing variables under garbage names. Shared by the
    resolver (authoring time), :func:`validate_run_time_sections` (stored
    blueprints, before data staging), and the executor's ``configure_build``
    (the build-time net), like :func:`check_cdr_output_sections`.

    Raises ``ValueError`` if ``ntrc_bio > 0`` while ``bgc_mode_is_marbl`` is False.
    """
    ntrc_bio = int(param.get("ntrc_bio", 0))
    if ntrc_bio > 0 and not bgc_mode_is_marbl:
        raise ValueError(
            f"param.ntrc_bio={ntrc_bio} but MARBL is off (cppdefs.marbl=False): "
            "only MARBL names the BGC tracer slots, so ucla-roms would allocate "
            f"{ntrc_bio} unnamed tracers. Set param.ntrc_bio to 0."
        )


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


# ucla-roms release from which cdr_frc parameterized releases run without MARBL (PR #379).
CDR_FORCING_WITHOUT_MARBL_MIN_ROMS: tuple[int, int, int] = (0, 9, 1)


def check_cdr_forcing_mode(
    cdr_mode: str, *, bgc_mode_is_marbl: bool, roms_ref: str | None
) -> bool:
    """Validate an active CDR forcing mode against MARBL and the pinned ucla-roms
    release, and report whether it implies ``cdr_output.do_cdr_output``.

    ``cdr_output`` (the CDR diagnostics stream) needs MARBL, so an active mode
    implies it only with MARBL. Without MARBL, ucla-roms >= 0.9.1
    (:data:`CDR_FORCING_WITHOUT_MARBL_MIN_ROMS`) still runs the parameterized
    releases of ``cdr_frc`` ("simple"/"yaml"/"netcdf") under ``CDR_FORCING``,
    with ``cdr_output`` left off. ``"upscaled"`` sets
    ``cdr_frc.forcing_depth_profiles``, which ucla-roms rejects at init without
    MARBL, and earlier releases either don't compile ``cdr_frc`` without MARBL
    or silently ignore the release. A ``roms_ref`` that is not a release tag
    (branch, commit hash, ``None``) counts as the latest release, matching
    :func:`run_time_settings_for_ref` (which has no local clone to resolve a
    hash against).

    Both the resolver (authoring time) and the executor's ``configure_build``
    (the build-time net) call this so the rule and its messages stay in one
    place; each caller sets ``cppdefs["cdr_forcing"]`` itself.

    Raises ``ValueError`` for an unsupported combination without MARBL.
    """
    if cdr_mode == "none":
        return False
    if bgc_mode_is_marbl:
        return True
    if cdr_mode == "upscaled":
        raise ValueError(
            f'CDR mode "{cdr_mode}" but bgc_mode != "marbl": upscaled CDR sets '
            "cdr_forcing_depth_profiles, which ucla-roms rejects at init without "
            'MARBL; use a parameterized CDR mode (simple/yaml/netcdf) or bgc_mode "marbl".'
        )
    version = roms_version_from_ref(roms_ref)
    if version is not None and version < CDR_FORCING_WITHOUT_MARBL_MIN_ROMS:
        minimum = ".".join(str(part) for part in CDR_FORCING_WITHOUT_MARBL_MIN_ROMS)
        raise ValueError(
            f'CDR mode "{cdr_mode}" but bgc_mode != "marbl" on ucla-roms '
            f"{roms_ref}: CDR forcing without MARBL needs ucla-roms >= {minimum}; "
            "earlier releases ignore the release or don't compile cdr_frc without MARBL."
        )
    return False


# ucla-roms release from which ``bgc_mode: cdr_lite`` runs: the ``CDR_LITE`` key
# exists from 0.9.0, and CDR forcing without MARBL (which the mode relies on) from
# 0.9.1. To be raised to the release that writes forcing-ready ``_cdrgas`` files and
# zero-fills the ``<CDR tracer>_flx`` surface fluxes a CDR-lite tracer has no
# forcing for (pending ucla-roms work); Forge's CDR-lite mode doesn't generate
# either, so until then a run has to supply them itself.
CDR_LITE_MODE_MIN_ROMS: tuple[int, int, int] = (0, 9, 1)


def check_cdr_lite_mode_roms(roms_ref: str | None) -> None:
    """Reject a ucla-roms pin older than :data:`CDR_LITE_MODE_MIN_ROMS` for
    ``bgc_mode: cdr_lite``. A ``roms_ref`` that is not a release tag (branch,
    commit hash, ``None``) counts as the latest release, like
    :func:`check_cdr_forcing_mode`.

    Both the resolver and the executor's ``configure_build`` (the net for
    stored blueprints) call this so the rule and its message stay in one place.

    Raises ``ValueError`` for a release tag below the minimum.
    """
    version = roms_version_from_ref(roms_ref)
    if version is not None and version < CDR_LITE_MODE_MIN_ROMS:
        minimum = ".".join(str(part) for part in CDR_LITE_MODE_MIN_ROMS)
        raise ValueError(
            f'bgc_mode "cdr_lite" on ucla-roms {roms_ref}: CDR-lite without MARBL '
            f"needs ucla-roms >= {minimum}."
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


class _CdrLiteOutputCfgCommon(_SettingsSection):
    wrt_tracers: bool = True
    wrt_vertical_integrals: bool = True
    wrt_thickness_weighted: bool = True
    wrt_sources: bool = True
    wrt_alk: bool = True
    wrt_dic: bool = True


class CdrLiteOutputCfg(_CdrLiteOutputCfgCommon):
    """``cdr_lite_output`` settings for ucla-roms 0.7.0-0.8.x -- the dedicated
    ``&CDR_TRACER_OUTPUT_SETTINGS`` group (PR #351), a separate output stream
    for the CDR-lite tracers (``CDR_OAE_ALK``/``CDR_OAE_DIC``/``CDR_DOR_DIC``).
    Forge's vocabulary uses the ucla-roms 0.9.0 names (CDR_TRACER was renamed
    CDR_LITE there); the ``serialization_alias`` on each renamed field supplies
    the 0.7/0.8 namelist key, as :class:`ParticlesCfgV0_5_0` does for 0.5.0.
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

    do_cdr_lite_output: bool = Field(
        default=False, serialization_alias="do_cdr_tracer_output"
    )
    do_avg: bool = Field(default=True, serialization_alias="wrt_cdr_trc_avg")
    monthly_averages: bool = Field(
        default=False, serialization_alias="cdr_trc_monthly_averages"
    )
    output_period: float = Field(
        default=3600.0, serialization_alias="output_period_cdr_trc"
    )
    nrpf: int = Field(default=4, serialization_alias="nrpf_cdr_trc")


class CdrLiteOutputCfgV0_9_0(_CdrLiteOutputCfgCommon):
    """``cdr_lite_output`` settings for ucla-roms >= 0.9.0 -- the
    ``&CDR_LITE_OUTPUT_SETTINGS`` group (PR #372), the renamed CDR output
    stream. Compiled and read unconditionally (no ``MARBL``/``CDR_FORCING``
    guard), so enabling it forces no cppdef; ucla-roms aborts at init if it is
    enabled with ``nt_cdr_oae + nt_cdr_dor == 0``. ``wrt_gas_exchange`` (the
    air-sea CO2 flux into each CDR-lite DIC tracer) needs the ``CDR_LITE``
    cppdef (see :class:`CdrLiteCfg`). Defaults as in
    :class:`CdrLiteOutputCfg`.
    """

    do_cdr_lite_output: bool = False
    do_avg: bool = Field(default=True, serialization_alias="wrt_cdr_lite_avg")
    monthly_averages: bool = Field(
        default=False, serialization_alias="cdr_lite_monthly_averages"
    )
    output_period: float = Field(
        default=3600.0, serialization_alias="output_period_cdr_lite"
    )
    nrpf: int = Field(default=4, serialization_alias="nrpf_cdr_lite")
    wrt_gas_exchange: bool = False


class CdrLiteCfg(_SettingsSection):
    """``cdr_lite`` settings -- ucla-roms >= 0.9.0's optional
    ``&CDR_LITE_SETTINGS`` group, read only under the ``CDR_LITE`` cppkey.
    Turning ``cdr_online_carbonate_sensitivity`` on makes the resolver compile
    ``CDR_LITE`` (``cppdefs.cdr_lite``, resolver-owned) and ROMS compute the
    carbonate sensitivities of the CDR-lite air-sea CO2 flux online from
    MARBL's ALT_CO2 state, so it needs MARBL. Off, ROMS reads
    ``ddic_dco2``/``ddic_dalk`` from forcing files; Forge compiles ``CDR_LITE``
    that way only for ``bgc_mode: cdr_lite`` (no MARBL), and not at all
    otherwise.

    The bundled ModelSpecs do not declare this section, so
    ``&CDR_LITE_SETTINGS`` is written at the default (off) -- what
    ``bgc_mode: cdr_lite`` runs with.
    """

    cdr_online_carbonate_sensitivity: bool = False


class CdrGasExchOutputCfg(_SettingsSection):
    """``cdr_gas_exch_output`` settings -- ucla-roms >= 0.7.0's dedicated
    ``&CDR_GAS_EXCH_OUTPUT_SETTINGS`` group (PR #351), a separate output
    stream for the gas-exchange sensitivities (``ddic_dco2``/``ddic_dalk``),
    active only under MARBL && CDR_FORCING: it reads MARBL's alternative-CO2
    tracers and carbonate-sensitivity code, so MARBL is a genuine requirement
    here. Defaults mirror :class:`CdrLiteOutputCfg`.
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


# Enable switch of each version-gated section a user can turn on (forge
# vocabulary: ``cdr_lite_output.do_cdr_lite_output`` serializes to
# ``do_cdr_tracer_output`` on ucla-roms 0.7/0.8, see :class:`CdrLiteOutputCfg`).
# Read by :data:`CDR_OUTPUT_SECTIONS` and :func:`check_cdr_lite_sections` (what
# the switch requires) and :func:`prune_version_gated_sections` (a section with
# its switch on cannot be silently dropped).
_GATED_SECTION_SWITCHES: dict[str, str] = {
    "cdr_lite_output": "do_cdr_lite_output",
    "cdr_gas_exch_output": "do_cdr_gas_exch_output",
    "cdr_lite": "cdr_online_carbonate_sensitivity",
}


def _tier_types_section(
    settings_cls: type[_RunTimeSettingsCommon],
    section: str,
    cfg_cls: type[_SettingsSection],
) -> bool:
    """True if the run-time settings tier ``settings_cls`` types ``section`` as
    exactly ``cfg_cls`` -- the one rule deciding which row of a per-tier section
    table (:data:`CDR_OUTPUT_SECTIONS`, :data:`_PRECHECK_SECTION_MAP`) applies
    to the pinned ucla-roms release.
    """
    info = settings_cls.model_fields.get(section)
    return info is not None and info.annotation is cfg_cls


# (section key, the Cfg class the section must have, its do-flag, whether the
# stream needs MARBL) for the ucla-roms >= 0.7.0 CDR output streams that
# :func:`check_cdr_output_sections` enforces. The CDR-lite tracer stream's Cfg
# class is the 0.7/0.8 one: from 0.9.0 it compiles unconditionally, so it needs
# no cppdef (:class:`CdrLiteOutputCfgV0_9_0`). Both streams compile under
# CDR_FORCING on the tiers listed; only the gas-exchange stream reads MARBL state
# (see the Cfg docstrings above).
CDR_OUTPUT_SECTIONS: tuple[tuple[str, type[_SettingsSection], str, bool], ...] = (
    (
        "cdr_lite_output",
        CdrLiteOutputCfg,
        _GATED_SECTION_SWITCHES["cdr_lite_output"],
        False,
    ),
    (
        "cdr_gas_exch_output",
        CdrGasExchOutputCfg,
        _GATED_SECTION_SWITCHES["cdr_gas_exch_output"],
        True,
    ),
)


def check_cdr_output_sections(
    run_time_settings: dict[str, Any],
    *,
    bgc_mode_is_marbl: bool,
    settings_cls: type[_RunTimeSettingsCommon],
) -> bool:
    """Validate ``cdr_lite_output``/``cdr_gas_exch_output`` (ucla-roms >= 0.7.0's
    dedicated CDR output streams, PR #351) and report whether
    ``cppdefs.cdr_forcing`` must be forced on.

    Unlike ``cdr_output`` (see ``CdrOutputCfg``), these two sections are never
    forced on by an active CDR forcing mode -- they're opt-in extras a user
    enables explicitly, so only the flag actually present in
    ``run_time_settings`` is read here. On the tiers where a stream is compiled
    under ``CDR_FORCING``, its flag being True needs that cppdef; the
    gas-exchange stream additionally needs MARBL, the tracer stream does not
    (``CDR_OUTPUT_SECTIONS`` records which). Only the rows whose Cfg class is the
    one ``settings_cls`` types the section as apply, so the CDR-lite tracer stream
    forces nothing on ucla-roms >= 0.9.0.

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
    for section_name, cfg_cls, do_flag, requires_marbl in CDR_OUTPUT_SECTIONS:
        if not _tier_types_section(settings_cls, section_name, cfg_cls):
            continue
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


def online_carbonate_sensitivity_requested(
    run_time_settings: Mapping[str, Any],
) -> bool:
    """Whether ``cdr_lite.cdr_online_carbonate_sensitivity`` is set in
    ``run_time_settings``: ROMS then computes the carbonate sensitivities online
    from MARBL's ALT_CO2 state instead of reading them from forcing files.
    """
    section = run_time_settings.get("cdr_lite") or {}
    return bool(section.get(_GATED_SECTION_SWITCHES["cdr_lite"]))


def check_cdr_lite_sections(
    run_time_settings: dict[str, Any],
    *,
    bgc_mode: BgcMode,
    settings_cls: type[_RunTimeSettingsCommon],
) -> bool:
    """Validate the ucla-roms >= 0.9.0 CDR-lite sections (``cdr_lite``,
    ``cdr_lite_output``) and report whether ``cppdefs.cdr_lite`` (the
    ``CDR_LITE`` cppkey) must be on: ``bgc_mode == "cdr_lite"`` (CDR-lite
    tracers without MARBL, carbonate sensitivities read from forcing files) or
    ``cdr_lite.cdr_online_carbonate_sensitivity`` set (MARBL computes them from
    its ALT_CO2 state). Only the sections present in ``run_time_settings`` are
    read, like :func:`check_cdr_output_sections`. Shared the same way: by the
    resolver (authoring time), :func:`validate_run_time_sections` (stored
    blueprints) and the executor's ``configure_build`` (the build-time net),
    each of which sets ``cppdefs["cdr_lite"]`` from the result.

    Raises ``ValueError`` for each combination ucla-roms 0.9.0 aborts on at
    init, all reported together: the online sensitivity without MARBL;
    ``cdr_lite_output.wrt_gas_exchange`` without ``CDR_LITE``; the CDR-lite
    output stream or the online sensitivity with no CDR tracers
    (``param.nt_cdr_oae + param.nt_cdr_dor == 0``); and, on a tier that models
    ``cdr_lite`` (``settings_cls``), CDR tracers in a build without ``CDR_LITE``
    ("Forcing type not supported").

    Under ``bgc_mode == "cdr_lite"`` the tracer counts are derived from the CDR
    forcing during generation, so this pre-generation check cannot require them:
    the zero-count rule is skipped there and ``configure_build`` enforces it once
    the counts exist.
    """
    switches = _GATED_SECTION_SWITCHES
    online_flag = f"cdr_lite.{switches['cdr_lite']}"
    stream_flag = f"cdr_lite_output.{switches['cdr_lite_output']}"
    online = online_carbonate_sensitivity_requested(run_time_settings)
    lite_output = run_time_settings.get("cdr_lite_output") or {}
    stream_on = bool(lite_output.get(switches["cdr_lite_output"]))
    needed = online or bgc_mode == "cdr_lite"
    problems: list[str] = []
    if online and bgc_mode != "marbl":
        problems.append(
            f'{online_flag}=True but bgc_mode != "marbl": ucla-roms computes the '
            "CDR-lite carbonate sensitivities from MARBL's ALT_CO2 state."
        )
    if stream_on and lite_output.get("wrt_gas_exchange") and not needed:
        problems.append(
            "cdr_lite_output.wrt_gas_exchange=True needs CDR_LITE, which Forge "
            f'enables via bgc_mode "cdr_lite" or {online_flag} (ucla-roms >= 0.9.0, '
            "MARBL)."
        )
    param = run_time_settings.get("param") or {}
    # ``or 0``: a null count (YAML ``nt_cdr_oae:``) means no tracers.
    n_cdr_tracers = sum(int(param.get(key) or 0) for key in _CDR_TRACER_WEIGHTS)
    if bgc_mode != "cdr_lite" and (
        enabled := [
            flag for flag, on in ((stream_flag, stream_on), (online_flag, online)) if on
        ]
    ):
        if not n_cdr_tracers:
            counts = " + ".join(f"param.{key}" for key in _CDR_TRACER_WEIGHTS)
            problems.append(
                f"{' and '.join(enabled)} set but {counts} == 0: ucla-roms aborts "
                "at init without CDR-lite tracers."
            )
    if n_cdr_tracers and not needed and "cdr_lite" in settings_cls.model_fields:
        counts = " + ".join(f"param.{key}" for key in _CDR_TRACER_WEIGHTS)
        problems.append(
            f"{counts} > 0 but the build has no CDR_LITE: ucla-roms aborts with "
            '"Forcing type not supported" for CDR tracers without it. Use '
            f'bgc_mode "cdr_lite" or enable {online_flag} (MARBL).'
        )
    if problems:
        raise ValueError(" ".join(problems))
    return needed


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
    akv: float = Field(serialization_alias="akv_bak")
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

    Not meant to be used directly: the version-varying sections (``param``,
    ``ocean_vars``, ``particles``) are typed as the loose common models here, and version-varying
    sections that some schemas lack entirely (``pio_settings``, added by
    :class:`RunTimeSettingsV0_6_0`; ``cdr_lite_output``/``cdr_gas_exch_output``,
    added by :class:`RunTimeSettingsV0_7_0`; ``cdr_lite``, added by
    :class:`RunTimeSettingsV0_9_0`) are simply absent here; use
    :class:`RunTimeSettings` (ucla-roms < 0.4.0), :class:`RunTimeSettingsV0_4_0`
    (0.4.0 <= ucla-roms < 0.5.0), :class:`RunTimeSettingsV0_5_0`
    (0.5.0 <= ucla-roms < 0.6.0), :class:`RunTimeSettingsV0_6_0`
    (0.6.0 <= ucla-roms < 0.7.0), :class:`RunTimeSettingsV0_7_0`
    (0.7.0 <= ucla-roms < 0.9.0), or :class:`RunTimeSettingsV0_9_0` (>= 0.9.0),
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
    param: _ParamCfgCommon
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
    """Forge's run-time settings dict for ucla-roms < 0.4.0, typed + validated.

    Kept unversioned (no suffix) for backward compatibility: this is the name
    historically used by forge.
    """

    param: ParamCfg
    ocean_vars: OceanVarsCfg
    particles: ParticlesCfg


class RunTimeSettingsV0_4_0(RunTimeSettings):
    """Forge's run-time settings dict for ucla-roms >= 0.4.0, < 0.5.0.

    Adds the passive CDR tracer counts to ``param`` (see :class:`ParamCfgV0_4_0`);
    mirrors C-Star's ``RomsNamelistV0_4_0(RomsNamelist)``.
    """

    param: ParamCfgV0_4_0


class RunTimeSettingsV0_5_0(_RunTimeSettingsCommon):
    """Forge's run-time settings dict for ucla-roms >= 0.5.0, typed + validated."""

    param: ParamCfgV0_4_0
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
    """Forge's run-time settings dict for ucla-roms >= 0.7.0, < 0.9.0, typed +
    validated.

    Subclasses :class:`RunTimeSettingsV0_6_0` directly (rather than
    ``_RunTimeSettingsCommon``) to inherit its ``ocean_vars``/``particles``/
    ``pio_settings`` unchanged -- mirrors C-Star's
    ``RomsNamelistV0_7_0(RomsNamelistV0_6_0)``. Adds ``cdr_lite_output`` and
    ``cdr_gas_exch_output`` (ucla-roms PR #351, ``&CDR_TRACER_OUTPUT_SETTINGS``/
    ``&CDR_GAS_EXCH_OUTPUT_SETTINGS``); both fields carry defaults (see
    :class:`CdrLiteOutputCfg`/:class:`CdrGasExchOutputCfg`) so a 0.7.0-pinned
    blueprint saved before these sections existed still validates. Unlike
    ``cdr_output``, neither is forced on by an active CDR forcing mode -- see
    the resolver's CDR tracer/gas-exchange output consistency check.
    """

    cdr_lite_output: CdrLiteOutputCfg = Field(default_factory=CdrLiteOutputCfg)
    cdr_gas_exch_output: CdrGasExchOutputCfg = Field(
        default_factory=CdrGasExchOutputCfg
    )


class RunTimeSettingsV0_9_0(RunTimeSettingsV0_6_0):
    """Forge's run-time settings dict for ucla-roms >= 0.9.0, typed + validated.

    Subclasses :class:`RunTimeSettingsV0_6_0`, not :class:`RunTimeSettingsV0_7_0`
    -- mirrors C-Star's ``RomsNamelistV0_9_0(RomsNamelistV0_6_0)``, and a
    subclass cannot retype V0_7_0's ``cdr_lite_output`` field. Adds ``cdr_lite``
    (``&CDR_LITE_SETTINGS``), the renamed ``cdr_lite_output``
    (``&CDR_LITE_OUTPUT_SETTINGS``, see :class:`CdrLiteOutputCfgV0_9_0`) and the
    unchanged ``cdr_gas_exch_output``; all carry defaults, so a blueprint
    saved before they existed still validates.
    """

    cdr_lite: CdrLiteCfg = Field(default_factory=CdrLiteCfg)
    cdr_lite_output: CdrLiteOutputCfgV0_9_0 = Field(
        default_factory=CdrLiteOutputCfgV0_9_0
    )
    cdr_gas_exch_output: CdrGasExchOutputCfg = Field(
        default_factory=CdrGasExchOutputCfg
    )


# Maps each namelist schema class (C-Star, keyed by ucla-roms version range) to
# the matching run-time settings class (forge's settings vocabulary).
_RUN_TIME_SETTINGS_BY_NAMELIST_SCHEMA: dict[
    type[RomsNamelistBase], type[_RunTimeSettingsCommon]
] = {
    RomsNamelist: RunTimeSettings,
    RomsNamelistV0_4_0: RunTimeSettingsV0_4_0,
    RomsNamelistV0_5_0: RunTimeSettingsV0_5_0,
    RomsNamelistV0_6_0: RunTimeSettingsV0_6_0,
    RomsNamelistV0_7_0: RunTimeSettingsV0_7_0,
    RomsNamelistV0_9_0: RunTimeSettingsV0_9_0,
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

    A section whose enable switch (:data:`_GATED_SECTION_SWITCHES`) is on is
    not dropped but rejected -- the pinned release cannot honor the request.
    Switched-off sections (the normal shared-OutputSpec case) are pruned
    silently. Raises ``ValueError`` before mutating anything.
    """
    pruned = sorted(
        (version_gated_section_names() - set(settings_cls.model_fields))
        & set(run_time_settings)
    )
    if requested := [
        f"{name}.{_GATED_SECTION_SWITCHES[name]}"
        for name in pruned
        if name in _GATED_SECTION_SWITCHES
        and isinstance(section := run_time_settings[name], dict)
        and section.get(_GATED_SECTION_SWITCHES[name])
    ]:
        raise ValueError(
            f"{', '.join(requested)} enabled, but the pinned ucla-roms release "
            f"(run-time settings {settings_cls.__name__}) has no such section."
        )
    for name in pruned:
        del run_time_settings[name]
    return pruned


# Section names forge's settings vocabulary used before ucla-roms 0.9.0 renamed
# CDR_TRACER to CDR_LITE: old section -> (new section, {old inner key: new inner
# key}). The only home of the old literals; :func:`normalize_legacy_sections`
# applies it to every settings dict that can predate the rename (stored
# blueprints, catalog specs, overrides).
_LEGACY_SECTION_RENAMES: dict[str, tuple[str, dict[str, str]]] = {
    "cdr_tracer_output": (
        "cdr_lite_output",
        {"do_cdr_tracer_output": "do_cdr_lite_output"},
    ),
}


def normalize_legacy_sections(settings: dict[str, Any]) -> dict[str, str]:
    """Rename, in place and keeping key order, every legacy-named section of a
    (possibly partial) run-time settings dict (:data:`_LEGACY_SECTION_RENAMES`),
    including its renamed inner keys; return the renames applied (legacy section
    name -> current name). Idempotent on current names.

    Raises ``ValueError`` if a dict carries both a legacy section and its
    replacement, or a legacy section carries both an inner key and its new name
    (ambiguous: neither can be dropped silently).
    """
    renamed: dict[str, str] = {}
    for old, (new, inner_keys) in _LEGACY_SECTION_RENAMES.items():
        if old not in settings:
            continue
        if new in settings:
            raise ValueError(
                f"settings carry both the legacy section {old!r} and its "
                f"replacement {new!r}; remove the stale {old!r}."
            )
        section = settings[old]
        if isinstance(section, dict):
            if both := [
                f"{old_k!r} and {new_k!r}"
                for old_k, new_k in inner_keys.items()
                if old_k in section and new_k in section
            ]:
                raise ValueError(
                    f"section {old!r} carries both {', '.join(both)}; remove the "
                    "stale legacy key."
                )
            section = {inner_keys.get(k, k): v for k, v in section.items()}
        items = [
            (new, section) if key == old else (key, value)
            for key, value in settings.items()
        ]
        settings.clear()
        settings.update(items)
        renamed[old] = new
    return renamed


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
        :class:`RunTimeSettings` for ucla-roms < 0.4.0 or when `roms_ref` is
        `None`; :class:`RunTimeSettingsV0_4_0` for 0.4.0 <= ucla-roms < 0.5.0;
        :class:`RunTimeSettingsV0_5_0` for 0.5.0 <= ucla-roms < 0.6.0;
        :class:`RunTimeSettingsV0_6_0` for 0.6.0 <= ucla-roms < 0.7.0;
        :class:`RunTimeSettingsV0_7_0` for 0.7.0 <= ucla-roms < 0.9.0;
        :class:`RunTimeSettingsV0_9_0` for ucla-roms >= 0.9.0.

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
    namelist name (every rename, ``casename`` -> ``title`` and ``akv`` ->
    ``akv_bak`` included), and case-only keys were lowercased in both
    vocabularies. The only explicit logic left is genuinely structural: the
    title/output-root regroup (two sections merged into one group), the frcfiles
    assembly, the scalar -> per-tracer-array expansion (``tnu2_default``,
    ``akt_default``), and the cross-section read of ``rho0`` from
    ``lateral_visc``. ``exclude=`` drops the fields those transforms handle
    instead of the section's own dump.

    ``rt``'s concrete type (:class:`RunTimeSettings`, :class:`RunTimeSettingsV0_4_0`,
    :class:`RunTimeSettingsV0_5_0`, :class:`RunTimeSettingsV0_6_0`,
    :class:`RunTimeSettingsV0_7_0`, or :class:`RunTimeSettingsV0_9_0`) selects the
    matching namelist schema and
    ``param_settings``/``basic_output_settings``/``particles_settings`` group
    classes — the ``param``/``ocean_vars``/``particles`` sections already carry the
    right fields and aliases for that variant, so no other branch is needed.
    ``pio_settings`` (added by ``RunTimeSettingsV0_6_0``),
    ``cdr_lite_output``/``cdr_gas_exch_output`` (added by
    ``RunTimeSettingsV0_7_0``, which writes ``cdr_lite_output`` as the 0.7/0.8
    ``&CDR_TRACER_OUTPUT_SETTINGS`` group) and ``cdr_lite``/``cdr_lite_output``/
    ``cdr_gas_exch_output`` (``RunTimeSettingsV0_9_0``) are sections a variant can
    lack entirely rather than just carry a different subtype (an older namelist
    schema rejects the group outright, ``extra="forbid"``), so each is added to
    the constructor kwargs only when ``rt`` is an instance of the class that
    introduced it — checked in most-specific-first order
    (``RunTimeSettingsV0_9_0``, then ``RunTimeSettingsV0_7_0``, before
    ``RunTimeSettingsV0_6_0`` before ITS superclass ``RunTimeSettingsV0_5_0``)
    since ``isinstance`` also matches subclasses; ``RunTimeSettingsV0_9_0`` is a
    ``RunTimeSettingsV0_6_0`` but not a ``RunTimeSettingsV0_7_0``.
    """
    if isinstance(rt, RunTimeSettingsV0_9_0):
        namelist_cls: type[RomsNamelistBase] = RomsNamelistV0_9_0
        param_cls: type[ParamSettings] = ParamSettingsV0_4_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_7_0):
        namelist_cls = RomsNamelistV0_7_0
        param_cls = ParamSettingsV0_4_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_6_0):
        namelist_cls = RomsNamelistV0_6_0
        param_cls = ParamSettingsV0_4_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_5_0):
        namelist_cls = RomsNamelistV0_5_0
        param_cls = ParamSettingsV0_4_0
        basic_output_cls = BasicOutputSettingsV0_5_0
        particles_cls = ParticlesSettingsV0_5_0
    elif isinstance(rt, RunTimeSettingsV0_4_0):
        namelist_cls = RomsNamelistV0_4_0
        param_cls = ParamSettingsV0_4_0
        basic_output_cls = BasicOutputSettings
        particles_cls = ParticlesSettings
    else:
        namelist_cls = RomsNamelist
        param_cls = ParamSettings
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
            **grp(rt.output_root_name), **grp(rt.title)
        ),  # regroup
        forcing_files=ForcingFiles(frcfiles=frc),  # 6 *_path -> frcfiles list
        tracer_diff2=TracerDiff2(tnu2=[rt.tracer_diff2.tnu2_default] * n_tracers),
        vertical_mixing_settings=VerticalMixingSettings(
            **rt.vertical_mixing.model_dump(by_alias=True, exclude={"akt_default"}),
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
        param_settings=param_cls(**grp(rt.param)),
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
    if isinstance(rt, RunTimeSettingsV0_9_0):
        kwargs["cdr_lite_settings"] = CdrLiteSettings(**grp(rt.cdr_lite))
        kwargs["cdr_lite_output_settings"] = CdrLiteOutputSettings(
            **grp(rt.cdr_lite_output)
        )
        kwargs["cdr_gas_exch_output_settings"] = CdrGasExchOutputSettings(
            **grp(rt.cdr_gas_exch_output)
        )
    elif isinstance(rt, RunTimeSettingsV0_7_0):
        kwargs["cdr_tracer_output_settings"] = CdrTracerOutputSettings(
            **grp(rt.cdr_lite_output)
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
    settings_cls = run_time_settings_for_ref(roms_ref)
    fields = settings_cls.model_fields
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
    # The CDR-lite mode needs a new enough ucla-roms: surfaced here so a stored
    # blueprint pinned below the minimum fails before data staging/generation
    # (configure_build keeps the same check as the net).
    if bgc_mode_from_cppdefs(settings.get("cppdefs") or {}) == "cdr_lite":
        try:
            check_cdr_lite_mode_roms(roms_ref)
        except ValueError as exc:
            errors.append(str(exc))
    # Same for BGC tracers vs MARBL (param vs cppdefs): surfaced here so a stored
    # blueprint fails before data staging/generation, not at configure_build.
    if "param" in settings and "cppdefs" in settings:
        cppdefs = settings["cppdefs"] or {}
        marbl = bool(cppdefs.get("marbl", False))
        try:
            check_bgc_tracer_count(settings["param"] or {}, bgc_mode_is_marbl=marbl)
        except ValueError as exc:
            errors.append(str(exc))
        # And the CDR-lite sections vs MARBL/CDR tracer counts (param vs cppdefs),
        # on the sections the selected tier models only: one it lacks is reported
        # by prune_version_gated_sections (resolve/configure_build), not advised on
        # here with 0.9-specific wording.
        keep = (*fields, "param", "cppdefs")
        modeled = {k: v for k, v in settings.items() if k in keep}
        try:
            check_cdr_lite_sections(
                modeled,
                bgc_mode=bgc_mode_from_cppdefs(cppdefs),
                settings_cls=settings_cls,
            )
        except ValueError as exc:
            errors.append(str(exc))
    return errors


# Rows ``(forge settings-dict section, the forge Cfg class that types/aliases
# that raw section, the C-Star RomsNamelistBase group field name the canonical
# table expects)`` for the sections check_output_streams_divide_rst reads.
# ``ocean_vars``/``upscale_output`` need no aliasing at all -- their forge field
# names already ARE the real Fortran namelist keys -- but are still routed
# through their Cfg class so a malformed section raises loudly rather than
# silently mismatching field names. A section whose Cfg varies by ucla-roms
# release has one row per Cfg; a row applies when the pinned release's settings
# tier types the section as exactly that Cfg (:func:`_tier_types_section`), so
# the ``OceanVarsCfgV0_5_0``/``ParticlesCfgV0_5_0`` rows apply only on the
# >= 0.5.0 tiers :func:`output_precheck_applies_to` gates this check to, and
# ``cdr_lite_output`` maps to the group its release writes.
_PRECHECK_SECTION_MAP: tuple[tuple[str, type[_SettingsSection], str], ...] = (
    ("ocean_vars", OceanVarsCfgV0_5_0, "basic_output_settings"),
    ("frc_output", FrcOutputCfg, "frc_output_settings"),
    ("random_output", RandomOutputCfg, "random_output_settings"),
    ("zslice", ZsliceCfg, "zslice_settings"),
    ("surf_flux", SurfFluxCfg, "surf_flx_output_settings"),
    ("particles", ParticlesCfgV0_5_0, "particles_settings"),
    ("sponge_tune", SpongeTuneCfg, "sponge_tune_settings"),
    ("diagnostics", DiagnosticsCfg, "diagnostics_settings"),
    ("cdr_output", CdrOutputCfg, "cdr_output_settings"),
    ("cdr_lite_output", CdrLiteOutputCfg, "cdr_tracer_output_settings"),
    ("cdr_lite_output", CdrLiteOutputCfgV0_9_0, "cdr_lite_output_settings"),
    ("cdr_gas_exch_output", CdrGasExchOutputCfg, "cdr_gas_exch_output_settings"),
    ("upscale_output", UpscaleOutputCfg, "upscale_settings"),
    ("bgc", BgcCfg, "bgc_settings"),
    ("extract_data", ExtractDataCfg, "extract_data_settings"),
)

# The inverse of _PRECHECK_SECTION_MAP: canonical RomsNamelistBase group field
# name (unique per row) -> (the forge settings-dict section that maps to it,
# its Cfg class). Used by forge_field_for to point a NamelistConsistencyError's
# canonical section/keys back at the forge settings-dict field the wizard
# actually edits.
_FORGE_SECTION_BY_CANONICAL_GROUP: dict[str, tuple[str, type[_SettingsSection]]] = {
    group_name: (section_name, cfg_cls)
    for section_name, cfg_cls, group_name in _PRECHECK_SECTION_MAP
}


def canonical_output_sections_for_precheck(
    settings: dict[str, Any], settings_cls: type[_RunTimeSettingsCommon]
) -> dict[str, Any]:
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
    checker actually reads (see :data:`_PRECHECK_SECTION_MAP`, whose rows are
    filtered by ``settings_cls``, the tier of the pinned ucla-roms release). A
    section absent from ``settings`` is simply omitted from the result -- the
    checker already treats an absent section as "skip that stream".

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
    for section_name, cfg_cls, group_name in _PRECHECK_SECTION_MAP:
        if not _tier_types_section(settings_cls, section_name, cfg_cls):
            continue
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
