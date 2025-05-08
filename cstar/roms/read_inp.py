from pathlib import Path
from typing import ClassVar, Optional, get_args, get_origin

from pydantic import (
    BaseModel,
    model_serializer,
)

from cstar.base.utils import _list_to_concise_str


################################################################################
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


################################################################################
# # Used to print values in a human-readable, fortran-friendly way
# def _format_value(val: float | str) -> str:
#     if isinstance(val, float):
#         if val == 0.0:
#             return "0."
#         elif abs(val) < 1e-2 or abs(val) >= 1e4:
#             return f"{val:.6E}".replace("E+00", "E0")
#     return str(val)


# def _format_float_list(lst: list) -> str:
#     """Format a list of values for .in file."""
#     return " ".join(_format_value(x) for x in lst)


# Subclassing BaseModel creates something that behaves similarly to (but isn't) a dataclass
class RomsSection(BaseModel):
    # typing sth as a ClassVar tells pydantic it's not supposed to be serialized,
    # and is for internal use.
    # This is the base class, so everything here is a ClassVar. Subclasses define
    # specific attributes /kv pairs
    section_name: ClassVar[str]
    multi_line: ClassVar[bool] = False
    key_order: ClassVar[list[str]]

    # Shouldn't normally need to define __init__ for a BaseModel (~dataclass)
    # This allows positional args instead of kwargs, e.g.
    # TimeStepping(1000, 30, 10, 50) instead of TimeStepping(ntimes=1000, dt=30, ndtfast=10, ninfo=50)
    # which we don't really desire anyway, so this can probs get shitcanned
    def __init__(self, *args, **kwargs):
        if args:
            super().__init__(**{k: args[i] for i, k in enumerate(self.key_order)})
        else:
            super().__init__(**kwargs)

    # This was previously in `write_section`. If `multi_line` (e.g. forcing) the joiner contains `\n`
    # if single line (e.g. time-stepping) it just separates by 4 spaces
    @property
    def value_joiner(self):
        return "\n    " if self.multi_line else "    "

    ################################################################################
    # SERIALIZERS
    ################################################################################
    # Functions used to convert complex structures to Python-native types like str
    # or dict.
    #
    # Typically we don't need to define custom serializers, as pydantic
    # has encoders for int, float, str, list, dict, datetime, Path, Enum, etc.
    # so any class we make which combines these types can cascade down into a
    # standard serialization.
    #
    # Here we have different encoding strategies for different sections, rather than
    # different source types:
    ################################################################################
    # NOTE:
    # SE says this probably isn't needed: only `_kv_serializer` (default) is used
    # with the exception of `_list_serializer` for the ForcingBlock subclass.
    # We could instead just define @model_serializer as `_kv_serializer` in the base
    # class and then override it for the ForcingBlock subclass
    ################################################################################

    @model_serializer
    def default_serializer(self) -> str:
        # Make a dictionary out of the attributes:
        data = {k: getattr(self, k) for k in self.key_order}

        # # Make a line for keys
        # section_header = " ".join(data.keys())

        # Initialize empty list to hold values
        # str_formatted_values_as_list = []
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
                case _:
                    section_values_as_list_of_str.append(_format_other(value))
        section_values_as_single_str = self.value_joiner.join(
            section_values_as_list_of_str
        )

        section_header = f"{self.section_name}: {' '.join(data.keys())}\n"
        # Build the serialized string
        string = ""
        string += section_header
        string += f"    {section_values_as_single_str}\n"
        string += "\n"
        return string

    @classmethod
    def from_lines(cls, lines: Optional[list[str]]) -> Optional["RomsSection"]:
        """This takes a list of lines as would be found under the section header of a
        roms.in file and returns a RomsSection instance.

        It uses the typehints of the RomsSection subclass under consideration to map
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

        >>> InitialBlock.from_lines(["1", "input_datasets/roms_ini.nc"])
            InitialBlock(nrrec=1, ininame=Path("input_datasets/roms_ini.nc"))
        """
        if lines is None:
            return None

        annotations = cls.__annotations__
        kwargs = {}

        # Decide how to flatten the section based on multi_line flag
        flat = lines if cls.multi_line else lines[0].split()

        i = 0  # index of the entry
        for key in cls.key_order:
            expected_type = annotations[key]
            if get_origin(expected_type) is not list:
                # This doesn't reformat e.g. 5.0D0 -> 5.0
                if expected_type is float:
                    kwargs[key] = expected_type(flat[i].replace("D", "E"))
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


################################################################################
## ROMS SECTION SUBCLASSES
################################################################################


class Title(RomsSection):
    title: str
    section_name = "title"
    key_order = [
        "title",
    ]


class OutputRootName(RomsSection):
    output_root_name: str
    section_name = "output_root_name"
    key_order = [
        "output_root_name",
    ]


class Rho0(RomsSection):
    rho0: float
    section_name = "rho0"
    key_order = [
        "rho0",
    ]


class Gamma2(RomsSection):
    gamma2: float
    section_name = "gamma2"
    key_order = [
        "gamma2",
    ]


class LateralVisc(RomsSection):
    lateral_visc: float
    section_name = "lateral_visc"
    key_order = [
        "lateral_visc",
    ]


class TracerDiff2(RomsSection):
    tracer_diff2: list[float]
    section_name = "tracer_diff2"
    key_order = [
        "tracer_diff2",
    ]


class MYBakMixing(RomsSection):
    my_bak_mixing: list[float]
    section_name = "my_bak_mixing"
    key_order = [
        "my_bak_mixing",
    ]


class SSSCorrection(RomsSection):
    sss_correction: float
    section_name = "sss_correction"
    key_order = [
        "sss_correction",
    ]


class SSTCorrection(RomsSection):
    sst_correction: float
    section_name = "sst_correction"
    key_order = [
        "sst_correction",
    ]


class UBind(RomsSection):
    ubind: float
    section_name = "ubind"
    key_order = [
        "ubind",
    ]


class VSponge(RomsSection):
    v_sponge: float
    section_name = "v_sponge"
    key_order = [
        "v_sponge",
    ]


class Grid(RomsSection):
    grid: Path
    section_name = "grid"
    key_order = [
        "grid",
    ]


class Climatology(RomsSection):
    climatology: Path
    section_name = "climatology"
    key_order = [
        "climatology",
    ]


class TimeStepping(RomsSection):
    ntimes: int
    dt: int
    ndtfast: int
    ninfo: int

    section_name = "time_stepping"
    key_order = ["ntimes", "dt", "ndtfast", "ninfo"]


class BottomDrag(RomsSection):
    rdrg: float
    rdrg2: float
    zob: float
    Cdb_min: float | None = None
    Cdb_max: float | None = None

    section_name = "bottom_drag"
    key_order = ["rdrg", "rdrg2", "zob"]


class InitialBlock(RomsSection):
    nrrec: int
    ininame: Path

    section_name = "initial"
    multi_line = True
    key_order = ["nrrec", "ininame"]


class ForcingBlock(RomsSection):
    filenames: list[Path]

    section_name = "forcing"
    multi_line = True
    key_order = ["filenames"]


class VerticalMixing(RomsSection):
    Akv_bak: float
    Akt_bak: list[float]
    section_name = "vertical_mixing"
    key_order = ["Akv_bak", "Akt_bak"]


class MarblBGC(RomsSection):
    marbl_namelist_fname: Path
    marbl_tracer_list_fname: Path
    marbl_diag_list_fname: Path

    section_name = "MARBL_biogeochemistry"
    multi_line = True
    key_order = [
        "marbl_namelist_fname",
        "marbl_tracer_list_fname",
        "marbl_diag_list_fname",
    ]


class SCoord(RomsSection):
    theta_s: float
    theta_b: float
    tcline: float

    section_name = "S-coord"
    key_order = ["theta_s", "theta_b", "tcline"]


class LinRhoEos(RomsSection):
    Tcoef: float
    T0: float
    Scoef: float
    S0: float

    section_name = "lin_rho_eos"
    key_order = [
        "Tcoef",
        "T0",
        "Scoef",
        "S0",
    ]


################################################################################
## THE MAIN BEAST
################################################################################


class ROMSRuntimeSettings(BaseModel):
    # title: str
    title: Title
    time_stepping: TimeStepping
    bottom_drag: BottomDrag
    initial: InitialBlock
    forcing: ForcingBlock
    # output_root_name: str
    output_root_name: OutputRootName
    # rho0: float
    rho0: Rho0
    marbl_biogeochemistry: Optional[MarblBGC]
    s_coord: Optional[SCoord]
    lin_rho_eos: Optional[LinRhoEos]
    # lateral_visc: Optional[float]
    lateral_visc: Optional[LateralVisc]
    # gamma2: Optional[float]
    gamma2: Optional[Gamma2]
    # tracer_diff2: Optional[list[float]]
    tracer_diff2: Optional[TracerDiff2]
    vertical_mixing: Optional[VerticalMixing]
    # my_bak_mixing: Optional[list[float]]
    my_bak_mixing: Optional[MYBakMixing]
    # sss_correction: Optional[float]
    sss_correction: Optional[SSSCorrection]
    # sst_correction: Optional[float]
    sst_correction: Optional[SSTCorrection]
    # ubind: Optional[float]
    ubind: Optional[UBind]
    # v_sponge: Optional[float]
    v_sponge: Optional[VSponge]
    # grid: Optional[Path]
    grid: Optional[Grid]
    # climatology: Optional[str]
    climatology: Optional[Climatology]

    # This line says that types which aren't typically encodable with pydantic
    # (e.g. ndarray, tuple[float, np.ndarray]). Currently these mess up in `to_file`
    # model_config = {"arbitrary_types_allowed": True}

    # file_serializers can be applied to specific fields, e.g. @field_serializer("title","rho0")
    # here, "*" says 'apply to all' and then overrides any ROMSSection instance, on which
    # (see above) individual serializers are defined

    # Define a serializer for any single-entry section that isn't covered by a RomsSection instance:

    # single_entry_sections: ClassVar[list] = [
    #     # "title",
    #     # "output_root_name",
    #     # "rho0",
    #     # "lateral_visc",
    #     # "gamma2",
    #     # "tracer_diff2",
    #     # "my_bak_mixing",
    #     # "sss_correction",
    #     # "sst_correction",
    #     # "ubind",
    #     # "v_sponge",
    #     # "grid",
    #     # "climatology",
    # ]

    # @field_serializer(*single_entry_sections)
    # def single_entry_serializer(
    #     self, data: str | float, info: FieldSerializationInfo
    # ) -> str:
    #     name = info.field_name

    #     # list of values:
    #     if isinstance(data, (list, np.ndarray)):
    #         values = "    " + _format_list_of_floats(data) + "\n"
    #         return f"{name}:\n{values}\n"

    #     # single value
    #     string = f"{name}:\n"
    #     string += f"    {_format_value(data)}\n"
    #     string += "\n"
    #     return string
    """Container for reading, manipulating, and writing ROMS `.in` runtime configuration
    files.

    This class represents the structured input used by ROMS for a single model run.
    It supports loading settings from disk via `from_file()`, editing or inspecting
    values via named attributes, and writing a valid ROMS `.in` file via `to_file()`.

    Attributes
    ----------
    title : str
        Description of the ROMS run.
    time_stepping : OrderedDict
        Time integration parameters: ntimes, dt, ndtfast, ninfo.
    bottom_drag : OrderedDict
        Bottom drag coefficients: rdrg, rdrg2, zob.
    initial : OrderedDict
        Initial condition parameters: nrrec and ininame.
    forcing : list of Path
        List of forcing NetCDF files.
    output_root_name : str
        Base name for output NetCDF files.

    Optional Attributes (depending on CPP flags)
    --------------------------------------------
    s_coord : OrderedDict or None
        S-coordinate transformation parameters.
    rho0 : float or None
        Boussinesq reference density.
    lin_rho_eos : OrderedDict or None
        Linear equation of state parameters.
    marbl_biogeochemistry : OrderedDict or None
        Filenames for MARBL namelist and diagnostics.
    lateral_visc : float or None
        Horizontal viscosity coefficient.
    gamma2 : float or None
        Lateral boundary slipperiness coefficient.
    tracer_diff2 : np.ndarray or None
        Horizontal tracer diffusivities.
    vertical_mixing : OrderedDict or None
        Vertical mixing parameters: Akv_bak and Akt_bak.
    my_bak_mixing : np.ndarray or None
        Background vertical mixing for MY2.5.
    sss_correction : float or None
        Surface salinity correction factor.
    sst_correction : float or None
        Surface temperature correction factor.
    ubind : float or None
        Boundary binding velocity scale.
    v_sponge : float or None
        Maximum sponge layer viscosity.
    grid : Path or None
        Grid file path.
    climatology : Path or None
        Climatology file path.
    """

    def _load_raw_sections(filepath: Path) -> dict:
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File {filepath} does not exist.")

        sections = {}
        with filepath.open() as f:
            lines = list(f)

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if ":" in line and not line.startswith("!"):
                section_name = line.split(":")[0].strip()
                i += 1
                section_lines = []

                while i < len(lines) and ":" not in lines[i]:
                    line = lines[i].strip()
                    if line and not line.startswith("!"):
                        section_lines.append(line)
                    i += 1

                sections[section_name] = section_lines
            else:
                i += 1
        return sections

    @classmethod
    def from_file(cls, filepath: Path | str):
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

        # def _single_line_section_to_list(
        #     section_name: str, expected_type: Any
        # ) -> Optional[list]:
        #     if (section_name not in sections.keys()) or (
        #         len(sections[section_name]) == 0
        #     ):
        #         return None

        #     section = sections[section_name][0].split()
        #     if expected_type == float:
        #         section = [x.upper().replace("D", "E") for x in section]
        #     section = [expected_type(x) for x in section]

        #     return section

        # def _single_line_section_to_scalar(
        #     section_name: str, expected_type: Any
        # ) -> Optional[Any]:
        #     lst = _single_line_section_to_list(section_name, expected_type)
        #     return lst[0] if lst else None

        # Read file
        filepath = Path(filepath)
        sections = cls._load_raw_sections(filepath)

        # Non-optional
        # One-line sections:
        # title = sections["title"][0]
        # time_stepping = _single_line_section_to_list("time_stepping", int) or []
        # bottom_drag = _single_line_section_to_list("bottom_drag", float) or []
        # output_root_name = sections["output_root_name"][0]

        if not all(
            key in sections.keys()
            for key in ["title", "time_stepping", "bottom_drag", "output_root_name"]
        ):
            raise ValueError(
                "Required field missing from file. Required fields: "
                "\n- title"
                "\n- time_steppings"
                "\n- bottom_drag"
                "\n- output_root_name"
            )

        # Multi-line sections:
        # nrrec = int(sections["initial"][0])
        # ini_path = (
        #     Path(sections["initial"][1]) if len(sections["initial"]) > 1 else None
        # )
        # initial = (nrrec, ini_path)
        # forcing: list[Path | str] = [Path(f) for f in sections["forcing"]]

        # Optional
        # One-line sections:
        # s_coord = _single_line_section_to_list("S-coord", float)
        # rho0 = _single_line_section_to_scalar("rho0", float)
        # lin_rho_eos = _single_line_section_to_list("lin_rho_eos", float)
        # lateral_visc = _single_line_section_to_scalar("lateral_visc", float)
        # gamma2 = _single_line_section_to_scalar("gamma2", float)

        # tracer_diff2 = _single_line_section_to_list("tracer_diff2", float)
        # tracer_diff2_list = _single_line_section_to_list("tracer_diff2", float)
        # tracer_diff2 = np.array(tracer_diff2_list) if tracer_diff2_list else None

        # my_bak_mixing = _single_line_section_to_list("MY_bak_mixing", float)

        # vertical_mixing = _single_line_section_to_list("vertical_mixing", float)

        # sss_correction = _single_line_section_to_scalar("SSS_correction", float)
        # sst_correction = _single_line_section_to_scalar("SST_correction", float)
        # ubind = _single_line_section_to_scalar("ubind", float)
        # v_sponge = _single_line_section_to_scalar("v_sponge", float)
        # grid = _single_line_section_to_scalar("grid", Path)
        # climatology = _single_line_section_to_scalar("climatology", Path)

        # Multi-line sections:
        # marbl_biogeochemistry: Optional[list[Path | str]] = (
        #     [Path(x) for x in sections["MARBL_biogeochemistry"]]
        #     if "MARBL_biogeochemistry" in sections
        #     else None
        # )

        return cls(
            # title=title,
            # title = sections.get("title")[0],
            title=Title.from_lines(sections["title"]),
            # time_stepping=TimeStepping(*time_stepping),
            time_stepping=TimeStepping.from_lines(sections["time_stepping"]),
            # marbl_biogeochemistry=MarblBGC(*marbl_biogeochemistry),
            marbl_biogeochemistry=MarblBGC.from_lines(
                sections["MARBL_biogeochemistry"]
            ),
            # s_coord=None if s_coord is None else SCoord(*s_coord),
            s_coord=SCoord.from_lines(sections.get("S-coord")),
            # rho0=rho0,
            # rho0 = float(sections.get("rho0")[0].split()[0].replace("D","E")),
            rho0=Rho0.from_lines(sections.get("rho0")),
            # lin_rho_eos=None if lin_rho_eos is None else LinRhoEos(*lin_rho_eos),
            lin_rho_eos=LinRhoEos.from_lines(sections.get("lin_rho_eos")),
            # lateral_visc=lateral_visc,
            # lateral_visc = float(sections.get("lateral_visc")[0].split()[0].replace("D","E")),
            lateral_visc=LateralVisc.from_lines(sections.get("lateral_visc")),
            # gamma2=gamma2
            gamma2=Gamma2.from_lines(sections.get("gamma2")),
            # gamma2 = float(sections.get("gamma2")[0].split()[0].replace("D","E")),
            # tracer_diff2=tracer_diff2,
            tracer_diff2=TracerDiff2.from_lines(sections.get("tracer_diff2")),
            # bottom_drag=BottomDrag(*bottom_drag),
            bottom_drag=BottomDrag.from_lines(sections.get("bottom_drag")),
            # vertical_mixing=VerticalMixing(
            #     Akv_bak=vertical_mixing[0], Akt_bak=vertical_mixing[1:]
            # ),
            vertical_mixing=VerticalMixing.from_lines(sections.get("vertical_mixing")),
            # my_bak_mixing=my_bak_mixing,
            my_bak_mixing=MYBakMixing.from_lines(sections.get("my_bak_mixing")),
            # sss_correction=sss_correction,
            sss_correction=SSSCorrection.from_lines(sections.get("sss_correction")),
            # sst_correction=sst_correction,
            sst_correction=SSTCorrection.from_lines(sections.get("sst_correction")),
            # ubind=ubind,
            ubind=UBind.from_lines(sections.get("ubind")),
            # v_sponge=v_sponge,
            v_sponge=VSponge.from_lines(sections.get("v_sponge")),
            # grid=grid,
            grid=Grid.from_lines(sections.get("grid")),
            # initial=None if initial is None else InitialBlock(*initial),
            initial=InitialBlock.from_lines(sections.get("initial")),
            # forcing=None if forcing is None else ForcingBlock(forcing),
            forcing=ForcingBlock.from_lines(sections.get("forcing")),
            # climatology=climatology,
            climatology=Climatology.from_lines(sections.get("climatology")),
            # output_root_name=output_root_name,
            # output_root_name = sections.get("output_root_name")[0]
            output_root_name=OutputRootName.from_lines(
                sections.get("output_root_name")
            ),
        )

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
                f"Surface stretching parameter (`theta_s`) = {self.s_coord.theta_s},"
            )
            lines.append(
                f"Bottom stretching parameter (`theta_b`) = {self.s_coord.theta_b},"
            )
            lines.append(
                f"Critical depth (`hc` or `tcline`, m) = {self.s_coord.tcline}"
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
                f"- Backround vertical TKE mixing [`Akq_bak`, m2/s] = {self.my_bak_mixing.Akq_bak},"
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

    def to_file(self, filepath: Path | str) -> None:
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
                f.write(self.model_dump()[field])


if __name__ == "__main__":
    ff = "/Users/dafyddstephenson/Code/my_ucla_roms/Examples/Wales/roms.in"
    ri = ROMSRuntimeSettings.from_file(filepath=ff)
    # print(ri)
    ri.model_dump(serialize_as_any=True)
    ri.to_file("out.in")
