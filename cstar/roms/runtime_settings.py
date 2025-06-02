import re
from os import PathLike
from pathlib import Path
from typing import ClassVar, Optional, Self, Union, get_args, get_origin

from pydantic import (
    BaseModel,
    Field,
    ModelWrapValidatorHandler,
    model_serializer,
    model_validator,
)

from cstar.base.log import get_logger
from cstar.base.utils import _list_to_concise_str

log = get_logger(__name__)

################################################################################
# Formatting methods for serializer:


def _format_list_of_floats(float_list: list[float]) -> str:
    joiner = " "
    return joiner.join(_format_float(x) for x in float_list)


def _format_list_of_paths(
    path_list: list[Path], multi_line: Optional[bool] = False
) -> str:
    joiner = "\n    " if multi_line else " "
    return joiner.join(_format_path(x) for x in path_list)


def _format_list_of_other(other_list: list[str | int]) -> str:
    joiner = " "
    return joiner.join(_format_other(x) for x in other_list)


def _format_float(val) -> str:
    if val == 0.0:
        return "0."
    elif abs(val) < 1e-2 or abs(val) >= 1e4:
        return f"{val:.6E}".replace("E+00", "E0")
    else:
        return str(val)


def _format_path(path: Path) -> str:
    return str(path)


def _format_other(other: str | int) -> str:
    return str(other)


def _pascal_to_snake(pascal: str) -> str:
    return re.sub(r"([a-z])([A-Z])", r"\1_\2", pascal).lower()


class ROMSRuntimeSettingsSection(BaseModel):  # , metaclass=ROMSSectionMeta):
    multi_line: ClassVar[bool] = False
    _custom_section_name: ClassVar[str | None] = None

    def __init__(self, *args, **kwargs):
        if args:
            super().__init__(**{k: args[i] for i, k in enumerate(self.key_order)})
        else:
            super().__init__(**kwargs)

    @property
    def section_name(self):
        return self._custom_section_name or _pascal_to_snake(type(self).__name__)

    @property
    def key_order(self):
        return list(self.__pydantic_fields__.keys())

    @model_validator(mode="wrap")
    @classmethod
    def validate_from_lines(cls, data, handler: ModelWrapValidatorHandler):
        # if the class gets a list of strings as it's init, assume it's coming in as a line
        # from the roms.in file, and try to parse it as such. if that fails, or if it's not a list
        # when it comes in, do the usual init process.

        if isinstance(data, list) and all(isinstance(v, str) for v in data):
            try:
                return cls.from_lines(data)
            except Exception as e:
                log.debug(
                    f"{cls.__name__}.validate_from_lines: failed to parse from lines: {data!r} – {e}"
                )
                raise
        return handler(data)

    @property
    def value_joiner(self):
        return "\n    " if self.multi_line else "    "

    @model_serializer
    def default_serializer(self) -> str:
        # Make a dictionary out of the attributes:
        data = {k: getattr(self, k) for k in self.key_order}

        section_values_as_list_of_str = []
        # Check formatting of values
        for value in data.values():
            match value:
                case list() if all(isinstance(v, float) for v in value):
                    section_values_as_list_of_str.append(_format_list_of_floats(value))
                case list() if all(isinstance(v, Path) for v in value):
                    section_values_as_list_of_str.append(
                        _format_list_of_paths(value, multi_line=self.multi_line)
                    )
                case list():
                    section_values_as_list_of_str.append(_format_list_of_other(value))
                case float():
                    section_values_as_list_of_str.append(_format_float(value))
                case Path():
                    section_values_as_list_of_str.append(_format_path(value))
                case None:
                    continue
                case _:
                    section_values_as_list_of_str.append(_format_other(value))
        section_values_as_single_str = self.value_joiner.join(
            section_values_as_list_of_str
        )

        section_header = f"{self.section_name}: {' '.join(data.keys())}\n"
        # Build the serialized string
        serialized = ""
        serialized += section_header
        serialized += f"    {section_values_as_single_str}\n"
        serialized += "\n"
        return serialized

    @classmethod
    def from_lines(cls, lines: Optional[list[str]]) -> Optional[Self]:
        """This takes a list of lines as would be found under the section header of a
        roms.in file and returns a ROMSRuntimeSettingsSection instance.

        It uses the typehints of the ROMSRuntimeSettingsSection subclass under consideration to map
        values from entries in the lines to correctly typed values.

        If the type of an entry is "list", this is either the sole or final entry,
        so we add all the remaining values to the list and break.

        Parameters
        ----------
        lines: list[str]
           Raw lines taken from a UCLA-ROMS runtime settings (`.in`) file.

        Examples
        --------
        >>> LinRhoEos.from_lines(["0.2 1.0 0.822 1.0"])
            LinRhoEos(Tcoef=0.2, T0=1.0, Scoef=0.822, S0=1.0)

        >>> InitialConditions.from_lines(["1", "input_datasets/roms_ini.nc"])
            InitialConditions(nrrec=1, ininame=Path("input_datasets/roms_ini.nc"))
        """
        if not lines:
            return None

        annotations = cls.__annotations__
        kwargs = {}

        # Decide how to flatten the section based on multi_line flag
        flat = lines if cls.multi_line else lines[0].split()

        i = 0  # index of the entry
        key_order = list(cls.__pydantic_fields__.keys())
        for key in key_order:
            annotation = annotations[key]
            annotation_origin = get_origin(annotation)

            # if, e.g. Optional[list[Path]] we just want list[Path]
            if annotation_origin is Union:
                annotation_args = get_args(annotation)
                if (
                    type(None) in annotation_args and len(annotation_args) == 2
                ):  # then Optional
                    expected_type = [t for t in annotation_args if t is not None][0]
                else:
                    expected_type = annotation
            else:
                expected_type = annotation

            if get_origin(expected_type) is not list:
                # This doesn't reformat e.g. 5.0D0 -> 5.0
                if expected_type is float:
                    kwargs[key] = expected_type(flat[i].upper().replace("D", "E"))
                elif (expected_type is str) and (len(key_order) == 1):
                    kwargs[key] = " ".join(flat)
                else:
                    kwargs[key] = expected_type(flat[i])
                i += 1
            # Handle list[...] types
            else:
                item_type = get_args(expected_type)[0]
                values = lines if cls.multi_line else flat[i:]
                kwargs[key] = [item_type(v) for v in values]
                break  # assume list field consumes the rest

        return cls(**kwargs)


################################################################################
## ROMS SECTION SUBCLASSES
################################################################################


class SingleEntryROMSRuntimeSettingsSection(ROMSRuntimeSettingsSection):
    @property
    def section_name(self):
        return self._custom_section_name or list(self.__pydantic_fields__.keys())[0]

    @model_validator(mode="wrap")
    @classmethod
    def single_entry_validator(cls, data, handler: ModelWrapValidatorHandler):
        """Allows a SingleEntryROMSRuntimeSettingsSection to be initialized with just
        the value of the single entry, instead of with a dict or kwargs."""
        field_name, annotation = list(cls.__annotations__.items())[0]
        expected_type = get_origin(annotation) or annotation
        if isinstance(data, expected_type):
            try:
                return cls.__init__(**{field_name: data})
            except Exception as e:
                log.debug(
                    f"single_entry_validator: failed to cast {data!r} to {expected_type} for {cls.__name__}: {e}"
                )
        return handler(data)

    def __repr__(self):
        return repr(getattr(self, self.key_order[0]))

    def __str__(self):
        return str(getattr(self, self.key_order[0]))


class Title(SingleEntryROMSRuntimeSettingsSection):
    title: str


class OutputRootName(SingleEntryROMSRuntimeSettingsSection):
    output_root_name: str


class Rho0(SingleEntryROMSRuntimeSettingsSection):
    rho0: float


class Gamma2(SingleEntryROMSRuntimeSettingsSection):
    gamma2: float


class LateralVisc(SingleEntryROMSRuntimeSettingsSection):
    lateral_visc: float


class TracerDiff2(SingleEntryROMSRuntimeSettingsSection):
    tracer_diff2: list[float]


class SSSCorrection(SingleEntryROMSRuntimeSettingsSection):
    sss_correction: float


class SSTCorrection(SingleEntryROMSRuntimeSettingsSection):
    sst_correction: float


class UBind(SingleEntryROMSRuntimeSettingsSection):
    ubind: float


class VSponge(SingleEntryROMSRuntimeSettingsSection):
    v_sponge: float


class Grid(SingleEntryROMSRuntimeSettingsSection):
    grid: Path


class Climatology(SingleEntryROMSRuntimeSettingsSection):
    climatology: Path


class TimeStepping(ROMSRuntimeSettingsSection):
    ntimes: int
    dt: int
    ndtfast: int
    ninfo: int


class BottomDrag(ROMSRuntimeSettingsSection):
    rdrg: float
    rdrg2: float
    zob: float


class InitialConditions(ROMSRuntimeSettingsSection):
    nrrec: int
    ininame: Optional[Path]

    _custom_section_name = "initial"
    multi_line = True

    @classmethod
    def from_lines(cls, lines: Optional[list[str]]) -> Self:
        """Bespoke `from_lines` for the InitialConditions section, which may have a
        single '0' line, as in, e.g. `$ROMS_ROOT/Examples/Rivers_ana/river_ana.in`

        In this case, set nrrec to 0 and ininame to None
        """
        if (not lines) or (len(lines) == 1) and int(lines[0]) == 0:
            return cls(nrrec=0, ininame=None)
        else:
            return super(InitialConditions, cls).from_lines(lines)


class Forcing(ROMSRuntimeSettingsSection):
    filenames: Optional[list[Path]]

    multi_line = True

    @classmethod
    def from_lines(cls, lines: Optional[list[str]]) -> Self:
        """Bespoke `from_lines` for the Forcing section, which must exist in ROMS but
        may be empty, as in, e.g. `$ROMS_ROOT/Examples/Rivers_ana/river_ana.in`

        In this case, set filenames to None
        """
        if (not lines) or (len(lines) == 0):
            return cls(filenames=None)
        else:
            return super().from_lines(lines)


class VerticalMixing(ROMSRuntimeSettingsSection):
    Akv_bak: float
    Akt_bak: list[float]


class MARBLBiogeochemistry(ROMSRuntimeSettingsSection):
    marbl_namelist_fname: Path
    marbl_tracer_list_fname: Path
    marbl_diag_list_fname: Path

    _custom_section_name = "MARBL_biogeochemistry"
    multi_line = True


class SCoord(ROMSRuntimeSettingsSection):
    theta_s: float
    theta_b: float
    tcline: float

    _custom_section_name = "S-coord"


class LinRhoEos(ROMSRuntimeSettingsSection):
    Tcoef: float
    T0: float
    Scoef: float
    S0: float


class MYBakMixing(ROMSRuntimeSettingsSection):
    Akq_bak: float
    q2nu2: float
    q2nu4: float
    _custom_section_name = "MY_bak_mixing"


################################################################################
## Class to hold all sections:


class ROMSRuntimeSettings(BaseModel):
    # Non-optional:
    title: Title
    time_stepping: TimeStepping
    bottom_drag: BottomDrag
    initial: InitialConditions
    forcing: Forcing
    output_root_name: OutputRootName
    # Optional:
    rho0: Optional[Rho0] = None
    marbl_biogeochemistry: Optional[MARBLBiogeochemistry] = Field(
        alias="MARBL_biogeochemistry", default=None
    )
    s_coord: Optional[SCoord] = Field(alias="S-coord", default=None)
    lin_rho_eos: Optional[LinRhoEos] = None
    lateral_visc: Optional[LateralVisc] = None
    gamma2: Optional[Gamma2] = None
    tracer_diff2: Optional[TracerDiff2] = None
    vertical_mixing: Optional[VerticalMixing] = None
    my_bak_mixing: Optional[MYBakMixing] = Field(alias="MY_bak_mixing", default=None)
    sss_correction: Optional[SSSCorrection] = None
    sst_correction: Optional[SSTCorrection] = None
    ubind: Optional[UBind] = None
    v_sponge: Optional[VSponge] = None
    grid: Optional[Grid] = None
    climatology: Optional[Climatology] = None
    # Pydantic model configuration
    model_config = {"populate_by_name": True}
    """Container for reading, manipulating, and writing ROMS `.in` runtime configuration
    files.

    This class represents the structured input used by ROMS for a single model run.
    It supports loading settings from disk via `from_file()`, editing or inspecting
    values via named attributes, and writing a valid ROMS `.in` file via `to_file()`.

    Each attribute corresponds to a section in the `.in` file, and is an instance of a
    `ROMSRuntimeSettingsSection` subclass corresponding to that section.

    Attributes
    ----------
    title : Title
        Description of the ROMS run.
    time_stepping : TimeStepping
        Time integration parameters: ntimes, dt, ndtfast, ninfo.
    bottom_drag : BottomDrag
        Bottom drag coefficients: rdrg, rdrg2, zob.
    initial : InitialConditions
        Initial condition parameters: nrrec and ininame.
    forcing : Forcing
        List of forcing NetCDF files.
    output_root_name : OutputRootName
        Base name for output NetCDF files.

    Optional Attributes (depending on CPP flags)
    --------------------------------------------
    s_coord : Optional[SCoord]
        S-coordinate transformation parameters:
        - theta_s (surface stretching parameter)
        - theta_b (bottom stretching parameter)
    rho0 : Rho0, optional, default None
        Boussinesq reference density (rho0, kg/m3)
    lin_rho_eos : LinRhoEos, optional, default None
        Linear equation of state parameters:
        - Tcoef (thermal expansion coefficient, kg/m3/K)
        - Scoef (haline contraction coefficient, kg/m3/PSU)
    marbl_biogeochemistry : MARBLBiogeochemistry, optional, default None
        Filenames for MARBL namelist and diagnostics:
        - marbl_namelist_fname
        - marbl_tracer_list_fname
        - marbl_diag_list_fname
    lateral_visc : LateralVisc, optional, default None
        Horizontal Laplacian kinematic viscosity (m2/s)
    gamma2 : Gamma2, optional, default None
        Lateral boundary slipperiness coefficient (free-slip=+1,no-slip=-1)
    tracer_diff2 : TracerDiff2, optional, default None
        Horizontal Laplacian mixing coefficients (one per tracer, m2/s)
    vertical_mixing : VerticalMixing, optional, default None
        Vertical mixing parameters:
        - akv_bak (background vertical viscosity, m2/s)
        - akt_bak (background vertical mixing for tracers, m2/s)
    my_bak_mixing : MYBakMixing, optional, default None
        Background vertical mixing for MY2.5 scheme parameters:
        - akq_bak (background vertical TKE mixing, m2/s)
        - q2nu2 (horizontal Laplacian TKE mixing, m2/s)
        - q2nu4 (horizontal biharmonic TKE mixing, m4/s)
    sss_correction : SSSCorrection, optional, default None
        Surface salinity correction factor.
    sst_correction : SSTCorrection, optional, default None
        Surface temperature correction factor.
    ubind : UBind, optional, default None
        Open boundary binding velocity (m/s)
    v_sponge : VSponge, optional, default None
        Maximum sponge layer viscosity (m2/s)
    grid : Grid, optional, default None
        Grid file path
    climatology : Climatology, optional, default None
        Climatology file path
    """

    @staticmethod
    def _load_raw_sections(filepath: str | Path) -> dict[str, list[str]]:
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File {filepath} does not exist.")

        with filepath.open() as f:
            lines = list(f)

        sections = {}
        current_section = None
        section_lines: list[str] = []

        for line in lines:
            line = line.strip()
            if not line or line.startswith("!"):
                continue

            if ":" in line:
                # save the previous section if one was open
                if current_section is not None:
                    sections[current_section] = section_lines

                # start a new section
                current_section = line.split(":", 1)[0].strip()
                section_lines = []
            else:
                section_lines.append(line)

        # save the last section
        if current_section is not None:
            sections[current_section] = section_lines

        return sections

    @classmethod
    def from_file(cls, filepath: str | Path) -> "ROMSRuntimeSettings":
        """Read ROMS runtime settings from a `.in` file.

        ROMS runtime settings are specified via a `.in` file with sections corresponding
        to different sets of parameters (e.g. time_stepping, bottom_drag). This method
        first parses the file, creating a dict[str,list[str]] where the keys are the
        section names and the values are a list of lines in that section. It then
        translates that dictionary into a ROMSRuntimeSettings instance with properly
        formatted attributes.

        Parameters
        ----------
        - filepath (Path or str):
           The path to the `.in` file

        See Also
        --------
        - `ROMSRuntimeSettings.to_file()`: writes a ROMSRuntimeSettings instance to a
           ROMS-compatible `.in` file
        """

        # Read file
        sections = cls._load_raw_sections(filepath)
        required_fields = {"title", "time_stepping", "bottom_drag", "output_root_name"}
        missing_required_fields = required_fields - sections.keys()
        if missing_required_fields:
            raise ValueError(
                "Required field missing from file. Required fields: "
                "\n- title"
                "\n- time_stepping"
                "\n- bottom_drag"
                "\n- output_root_name"
            )
        return cls(**sections)

    def __str__(self) -> str:
        """Returns a string representation of the input settings.

        Returns
        -------
        str
            A formatted string summarizing the ROMS input settings.
        """
        class_name = self.__class__.__name__
        lines = [f"{class_name}"]
        lines.append("-" * len(class_name))
        lines.append(f"Title (`ROMSRuntimeSettings.title`): {self.title}")
        lines.append(
            f"Output filename prefix (`ROMSRuntimeSettings.output_root_name`): {self.output_root_name}"
        )
        lines.append("Time stepping (`ROMSRuntimeSettings.time_stepping`):")
        lines.append(f"- Number of steps (`ntimes`) = {self.time_stepping.ntimes},")
        lines.append(f"- Time step (`dt`, sec) = {self.time_stepping.dt},")
        lines.append(
            f"- Mode-splitting ratio (`ndtfast`) = {self.time_stepping.ndtfast},"
        )
        lines.append(
            f"- Runtime diagnostic frequency (`ninfo`, steps) = {self.time_stepping.ninfo}"
        )
        lines.append("Bottom drag (`ROMSRuntimeSettings.bottom_drag`):")
        lines.append(
            f"- Linear bottom drag coefficient (`rdrg`, m/s) = {self.bottom_drag.rdrg},"
        )
        lines.append(
            f"- Quadratic bottom drag coefficient (`rdrg2`, nondim) = {self.bottom_drag.rdrg2}"
        )
        lines.append(f"- Bottom roughness height (`zob`,m) = {self.bottom_drag.zob}")
        lines.append(
            f"Grid file (`ROMSRuntimeSettings.grid`): {self.grid if self.grid else 'Not set'}"
        )
        lines.append(
            f"Initial conditions file (`ROMSRuntimeSettings.initial`): {self.initial.ininame}"
        )
        lines.append(
            f"Forcing file(s): {_list_to_concise_str(self.forcing.filenames,pad=10)}"
        )
        if self.s_coord is not None:
            lines.append("S-coordinate parameters (`ROMSRuntimeSettings.s_coord`):")
            lines.append(
                f"- Surface stretching parameter (`theta_s`) = {self.s_coord.theta_s},"
            )
            lines.append(
                f"- Bottom stretching parameter (`theta_b`) = {self.s_coord.theta_b},"
            )
            lines.append(
                f"- Critical depth (`hc` or `tcline`, m) = {self.s_coord.tcline}"
            )
        if self.rho0 is not None:
            lines.append(f"Boussinesq reference density (`rho0`, kg/m3) = {self.rho0}")
        if self.lin_rho_eos is not None:
            lines.append(
                "Linear equation of state parameters (`ROMSRuntimeSettings.lin_rho_eos`):"
            )
            lines.append(
                f"- Thermal expansion coefficient, ⍺ (`Tcoef`, kg/m3/K) = {self.lin_rho_eos.Tcoef},"
            )
            lines.append(f"- Reference temperature (`T0`, °C) = {self.lin_rho_eos.T0},")
            lines.append(
                f"- Haline contraction coefficient, β (`Scoef`, kg/m3/PSU) = {self.lin_rho_eos.Scoef},"
            )
            lines.append(f"- Reference salinity (`S0`, psu) = {self.lin_rho_eos.S0}")

        if self.marbl_biogeochemistry is not None:
            lines.append("MARBL input (`ROMSRuntimeSettings.marbl_biogeochemistry`):")
            lines.append(
                f"- MARBL runtime settings file: {self.marbl_biogeochemistry.marbl_namelist_fname},"
            )
            lines.append(
                f"- MARBL output tracer list: {self.marbl_biogeochemistry.marbl_tracer_list_fname},"
            )
            lines.append(
                f"- MARBL output diagnostics list: {self.marbl_biogeochemistry.marbl_diag_list_fname}"
            )
        if self.lateral_visc is not None:
            lines.append(
                f"Horizontal Laplacian kinematic viscosity (`ROMSRuntimeSettings.lateral_visc`, m2/s) = {self.lateral_visc}"
            )
        if self.gamma2 is not None:
            lines.append(
                f"Boundary slipperiness parameter (`ROMSRuntimeSettings.gamma2`, free-slip=+1, no-slip=-1) = {self.gamma2}"
            )
        if self.tracer_diff2 is not None:
            lines.append(
                f"Horizontal Laplacian mixing coefficients for tracers (`ROMSRuntimeSettings.tracer_diff2`, m2/s) = {self.tracer_diff2}"
            )
        if self.vertical_mixing is not None:
            lines.append(
                "Vertical mixing parameters (`ROMSRuntimeSettings.vertical_mixing`):"
            )
            lines.append(
                f"- Background vertical viscosity (`Akv_bak`, m2/s) = {self.vertical_mixing.Akv_bak},"
            )
            lines.append(
                f"- Background vertical mixing for tracers (`Akt_bak`, m2/s) = {self.vertical_mixing.Akt_bak},"
            )
        if self.my_bak_mixing is not None:
            lines.append(
                "Mellor-Yamada Level 2.5 turbulent closure parameters (`ROMSRuntimeSettings.my_bak_mixing`):"
            )
            lines.append(
                f"- Background vertical TKE mixing [`Akq_bak`, m2/s] = {self.my_bak_mixing.Akq_bak},"
            )
            lines.append(
                f"- Horizontal Laplacian TKE mixing [`q2nu2`, m2/s] = {self.my_bak_mixing.q2nu2},"
            )
            lines.append(
                f"- Horizontal biharmonic TKE mixing [`q2nu4`, m4/s] = {self.my_bak_mixing.q2nu4},"
            )

        if self.sss_correction is not None:
            lines.append(
                f"SSS correction (`ROMSRuntimeSettings.sss_correction`): {self.sss_correction}"
            )
        if self.sst_correction is not None:
            lines.append(
                f"SST correction (`ROMSRuntimeSettings.sst_correction`): {self.sst_correction}"
            )
        if self.ubind is not None:
            lines.append(
                f"Open boundary binding velocity (`ROMSRuntimeSettings.ubind`, m/s) = {self.ubind}"
            )
        if self.v_sponge is not None:
            lines.append(
                f"Maximum sponge layer viscosity (`ROMSRuntimeSettings.v_sponge`, m2/s) = {self.v_sponge}"
            )
        if self.climatology is not None:
            lines.append(
                f"Climatology data files (`ROMSRuntimeSettings.climatology`): {self.climatology}"
            )

        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a full debug-style string representation of the settings.

        Returns
        -------
        str
            A detailed summary of the object, suitable for debugging.
        """
        attrs = {
            "title": self.title,
            "time_stepping": dict(self.time_stepping),
            "bottom_drag": dict(self.bottom_drag),
            "initial": dict(self.initial),
            "forcing": [str(f) for f in self.forcing],
            "output_root_name": self.output_root_name,
            "grid": str(self.grid) if self.grid else None,
            "climatology": str(self.climatology) if self.climatology else None,
            "s_coord": dict(self.s_coord) if self.s_coord else None,
            "rho0": self.rho0,
            "lin_rho_eos": dict(self.lin_rho_eos) if self.lin_rho_eos else None,
            "marbl_biogeochemistry": dict(self.marbl_biogeochemistry)
            if self.marbl_biogeochemistry
            else None,
            "lateral_visc": self.lateral_visc,
            "gamma2": self.gamma2,
            "tracer_diff2": self.tracer_diff2
            if self.tracer_diff2 is not None
            else None,
            "vertical_mixing": dict(self.vertical_mixing)
            if self.vertical_mixing
            else None,
            "my_bak_mixing": dict(self.my_bak_mixing) if self.my_bak_mixing else None,
            "sss_correction": self.sss_correction,
            "sst_correction": self.sst_correction,
            "ubind": self.ubind,
            "v_sponge": self.v_sponge,
        }

        inner = ", ".join(f"{k}={repr(v)}" for k, v in attrs.items() if v is not None)
        return f"{self.__class__.__name__}({inner})"

    def to_file(self, filepath: PathLike[str]) -> None:
        """Write the current settings to a ROMS-compatible `.in` file.

        Parameters
        ----------
        filepath : str or Path
            Path where the output file will be written.
        """

        filepath = Path(filepath)

        output_order = [
            "title",
            "time_stepping",
            "bottom_drag",
            "initial",
            "forcing",
            "output_root_name",
            "s_coord",
            "grid",
            "marbl_biogeochemistry",
            "lateral_visc",
            "rho0",
            "lin_rho_eos",
            "gamma2",
            "tracer_diff2",
            "vertical_mixing",
            "my_bak_mixing",
            "sss_correction",
            "sst_correction",
            "ubind",
            "v_sponge",
            "climatology",
        ]

        with filepath.open("w") as f:
            for field in output_order:
                if getattr(self, field) is None:
                    continue
                fieldname = type(self).model_fields[field].alias or field
                f.write(self.model_dump(by_alias=True)[fieldname])


if __name__ == "__main__":
    orig = "/Users/eilerman/Downloads/sje_pacmed12km_Y2000.in"
    r = ROMSRuntimeSettings.from_file(orig)
    # print(r)
    new = "./new.in"
    r.to_file(new)

    with open(new, "r") as f:
        lines_new = f.readlines()
    with open(orig, "r") as f:
        lines_orig = f.readlines()

    r2 = ROMSRuntimeSettings.from_file(new)
    assert r == r2
    # assert lines_orig == lines_new
