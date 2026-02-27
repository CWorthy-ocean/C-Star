import abc
import types
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, get_args, get_origin

from pydantic import (
    BaseModel,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic.alias_generators import to_snake

from cstar.base.log import get_logger
from cstar.base.utils import DEFAULT_OUTPUT_ROOT_NAME, _list_to_concise_str

if TYPE_CHECKING:
    from pydantic import ModelWrapValidatorHandler

log = get_logger(__name__)


# This lookup is for roms.in sections that have specialized names that are not simply the snake case
# of the class/field name
CUSTOM_ALIAS_LOOKUP = {
    "marbl_biogeochemistry": "MARBL_biogeochemistry",
    "s_coord": "S-coord",
    "my_bak_mixing": "MY_bak_mixing",
    "initial_conditions": "initial",
}

MIN_NUM_TRACERS = 37
"""Vertical mixing requires enough values for all tracers or it raises an error."""


def _format_float(val: float) -> str:
    """Apply special float formatting for 0 and scientific notation."""
    if val == 0.0:
        return "0."

    if abs(val) < 1e-2 or abs(val) >= 1e4:
        return f"{val:.6E}".replace("E+00", "E0")

    return str(val)


def _format_value(val: Any) -> str:
    """Format floats using _format_float, otherwise just return the string of the
    value.
    """
    if isinstance(val, float):
        return _format_float(val)
    return str(val)


def _get_alias(field_name: str) -> str:
    """Get the alias for a given field, checking the custom mappings first, otherwise
    returning the snake_case version of the field name.

    Parameters
    ----------
    field_name: field name to get alias for

    Returns
    -------
    alias for serialization/deserialization
    """
    field_name = to_snake(field_name)
    return CUSTOM_ALIAS_LOOKUP.get(field_name, field_name)


class ROMSRuntimeSettingsSection(BaseModel, abc.ABC):
    """Base class containing common serialization/deserialization methods used by
    subsections of the ROMS runtime input file.
    """

    multi_line: ClassVar[bool] = False
    """If true, multiple values are split across multiple lines in the inputs file; if
    false, they are space-delimited."""

    def __init__(self, *args, **kwargs):
        if args:
            super().__init__(**{k: args[i] for i, k in enumerate(self.key_order)})
        else:
            super().__init__(**kwargs)

    @property
    def key_order(self):
        """Return the pydantic fields (no class vars or properties) in the order they
        are specified.
        """
        return list(self.__pydantic_fields__.keys())

    @model_validator(mode="wrap")
    @classmethod
    def validate_from_lines(
        cls, data: Any, handler: "ModelWrapValidatorHandler"
    ) -> Self:
        """This adapter allows class instantiation from a dict / kwargs, or from lines
        read from a ROMS input file.

        If data is a list of strings, assume it's coming in as raw lines, and create an
        object from the from_lines method. Otherwise, pass to the pydantic handler to
        validate per usual.
        """
        if isinstance(data, list) and all(isinstance(v, str) for v in data):
            return cls.from_lines(data)

        return handler(data)

    @property
    def _intervalue_delimiter(self) -> str:
        """Delimiter that goes between different values in a section. Either a newline +
        spaces for multiline sections, or several spaces.

        For example, TimeStepping is not multi-line, so it's four values will get delimited as:
        ```1    2    3    4```
        whereas InitialConditions is multiline, and will output it's two values as
        ```
            1
            /path/to/ini/file.nc
        ```
        """
        return "\n    " if self.multi_line else "    "

    @property
    def _intravalue_delimiter(self) -> str:
        """Delimiter that goes between multiple parts of a single "value", e.g. multiple
        floats in a list of floats.

        Either a newline + spaces for multiline sections, or a single space.

        Multi-line variations are typically for lists of filenames, e.g. forcing files.
        There is one key-value pair, but each value in the list needs its own line.

        Classes that use this with multi_line=False are arrays of numbers, like
        TracerDiff2, and we use single spaces here to create a visual distinction from
        multiple values (like the TimeStepping example in _intervalue_delimiter).
        """
        return "\n    " if self.multi_line else " "

    def _format_and_join_values(self, *args):
        """Formats any number of args corresponding to a single value, and joins them as
        appropriate.
        """
        # if args is a single, non-list item, just format it (no join needed)
        if len(args) == 1 and not isinstance(args[0], list):
            return _format_value(args[0])

        # otherwise, format each value in args and join them.
        return self._intravalue_delimiter.join(map(_format_value, *args))

    @model_serializer
    def default_serializer(self) -> str:
        """Serializes the object as a string containing the section header and values
        for the roms.in specification.

        Examples
        --------
        >>> t = TimeStepping(1, 2, 3, 4)
        >>> t.model_dump()
        time_stepping: ntimes dt ndtfast ninfo
             1    2    3    4
        """
        # Make a dictionary out of the pydantic attributes:
        data = {k: getattr(self, k) for k in self.key_order}

        section_values = []

        # for each non-None k/v, format and join the values
        for value in [x for x in data.values() if x is not None]:
            section_values.append(self._format_and_join_values(value))

        # combine all the values in this section into a single string
        section_values_as_single_str = self._intervalue_delimiter.join(section_values)

        # single entry sections use the one and only field as the section name
        # multi-value sections use their alias
        if isinstance(self, SingleEntryROMSRuntimeSettingsSection):
            section_name = list(self.__pydantic_fields__.keys())[0]
        else:
            section_name = _get_alias(type(self).__name__)

        # Build the serialized string
        section_header = f"{section_name}: {' '.join(data.keys())}\n"
        serialized = ""
        serialized += section_header
        serialized += f"    {section_values_as_single_str}\n"
        serialized += "\n"
        return serialized

    @classmethod
    def from_lines(cls, lines: list[str]) -> Self:
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
            raise ValueError("Received empty input.")

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
            if annotation_origin is types.UnionType:
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


class SingleEntryROMSRuntimeSettingsSection(ROMSRuntimeSettingsSection):
    @model_validator(mode="after")
    def check_exactly_one_field(self):
        if n_args := len(self.__pydantic_fields__.keys()) != 1:
            raise TypeError(
                f"{type(self)} should only have one pydantic field, but it had {n_args}"
            )
        return self

    @model_validator(mode="wrap")
    @classmethod
    def single_entry_validator(cls, data, handler: "ModelWrapValidatorHandler"):
        """Allows a SingleEntryROMSRuntimeSettingsSection to be initialized with just
        the value of the single entry, instead of only with a dict or kwargs.

        This method verifies that the value passed in at initialization matches the
        expected type for the SingleEntryROMSRuntimeSettings subclass being instantiated
        using a pydantic TypeAdapter, falling back to the pydantic handler (and raising
        a relevant ValidationError) if the type does not strictly match and can't be
        validated as a dict/object either.
        """
        # If no data is provided (None, empty list, etc.), set the whole section to None.
        # We do need to explicitly let 0 through as a valid value, though.
        if data in [None, [], ""]:
            return None

        # If the data passed in is a single value that matches the annotation, initialize the class as if it had been
        # called with the appropriate Class(key=value) syntax
        field_name, annotation = list(cls.__annotations__.items())[0]

        try:
            # TypeAdapter allows you to check annotations or annotated classes against arbitrary objects.
            # Here, we use strict=True because we only want to allow this shortcut syntax if the type
            # matches exactly; we don't allow coercion.
            TypeAdapter(annotation).validate_python(data, strict=True)
            return handler({field_name: data})

        except ValidationError:
            log.debug(
                "Assigning the value to the single-entry field raised a validation error; using regular pydantic handler"
            )

            # Fall back to pydantic handler for dictionaries, objects, etc.
            # This will raise any further ValidationErrors, including the traceback to the above attempt.
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
    ininame: Path | None

    multi_line = True

    @classmethod
    def from_lines(cls, lines: list[str] | None) -> Self:
        """Bespoke `from_lines` for the InitialConditions section, which may have a
        single '0' line, as in, e.g. `$ROMS_ROOT/Examples/Rivers_ana/river_ana.in`

        In this case, set nrrec to 0 and ininame to None
        """
        if (not lines) or (len(lines) == 1) and int(lines[0]) == 0:
            return cls(nrrec=0, ininame=None)
        else:
            return super().from_lines(lines)


class Forcing(ROMSRuntimeSettingsSection):
    filenames: list[Path] | None

    multi_line = True

    @classmethod
    def from_lines(cls, lines: list[str] | None) -> Self:
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

    @field_validator("Akt_bak", mode="after")
    @classmethod
    def zero_fill_tracers(cls, value: list[float]):
        """
        If the list of vertical mixing for tracers is all zeros, make sure it has enough zeros such that
        ROMS can read a zero for every tracer (otherwise it will crash). If any values are non-zero, we can't
        make any assumptions about which tracers the user is trying to assign, so don't do any modification and
        let ROMS use it or fail as appropriate.

        Parameters
        ----------
        value: the field value after running through initial pydantic validation

        Returns
        -------
        modified field value
        """
        list_len = len(value)
        if (num_missing := MIN_NUM_TRACERS - list_len) and all(
            [v == 0 for v in value]
        ) > 0:
            value.extend([0] * num_missing)
        return value


class MARBLBiogeochemistry(ROMSRuntimeSettingsSection):
    marbl_namelist_fname: Path
    marbl_tracer_list_fname: Path
    marbl_diag_list_fname: Path

    multi_line = True


class SCoord(ROMSRuntimeSettingsSection):
    theta_s: float
    theta_b: float
    tcline: float


class LinRhoEos(ROMSRuntimeSettingsSection):
    Tcoef: float
    T0: float
    Scoef: float
    S0: float


class MYBakMixing(ROMSRuntimeSettingsSection):
    Akq_bak: float
    q2nu2: float
    q2nu4: float


class ROMSRuntimeSettings(BaseModel):
    """Container for reading, manipulating, and writing ROMS `.in` runtime configuration
    files.

    This class represents the structured input used by ROMS for a single model run.
    It supports loading settings from disk via `from_file()`, editing or inspecting
    values via named attributes, and writing a valid ROMS `.in` file via `to_file()`.

    Each attribute corresponds to a section in the `.in` file, and is an instance of a
    `ROMSRuntimeSettingsSection` subclass corresponding to that section.
    """

    title: Title
    """Description of the ROMS run."""
    time_stepping: TimeStepping
    """Time integration parameters: ntimes, dt, ndtfast, ninfo."""
    bottom_drag: BottomDrag
    """Bottom drag coefficients: rdrg, rdrg2, zob."""
    initial: InitialConditions
    """Initial condition parameters: nrrec and ininame."""
    forcing: Forcing
    """List of forcing NetCDF files."""
    output_root_name: OutputRootName
    """Base name for output NetCDF files."""

    s_coord: SCoord | None = None
    """S-coordinate transformation parameters:
    - theta_s (surface stretching parameter)
    - theta_b (bottom stretching parameter)
    """
    grid: Grid | None = None
    """Grid file path."""
    marbl_biogeochemistry: MARBLBiogeochemistry | None = None
    """Filenames for MARBL namelist and diagnostics:
    - marbl_namelist_fname
    - marbl_tracer_list_fname
    - marbl_diag_list_fname
    """
    lateral_visc: LateralVisc | None = None
    """Horizontal Laplacian kinematic viscosity (m2/s)."""
    rho0: Rho0 | None = None
    """Boussinesq reference density (rho0, kg/m3)."""
    lin_rho_eos: LinRhoEos | None = None
    """Linear equation of state parameters:
    - Tcoef (thermal expansion coefficient, kg/m3/K)
    - Scoef (haline contraction coefficient, kg/m3/PSU)
    """
    gamma2: Gamma2 | None = None
    """Lateral boundary slipperiness coefficient (free-slip=+1,no-slip=-1)."""
    tracer_diff2: TracerDiff2 | None = None
    """Horizontal Laplacian mixing coefficients (one per tracer, m2/s)."""
    vertical_mixing: VerticalMixing | None = None
    """Vertical mixing parameters:
    - akv_bak (background vertical viscosity, m2/s)
    - akt_bak (background vertical mixing for tracers, m2/s)
    """
    my_bak_mixing: MYBakMixing | None = None
    """Background vertical mixing for MY2.5 scheme parameters:
    - akq_bak (background vertical TKE mixing, m2/s)
    - q2nu2 (horizontal Laplacian TKE mixing, m2/s)
    - q2nu4 (horizontal biharmonic TKE mixing, m4/s)
    """
    sss_correction: SSSCorrection | None = None
    """Surface salinity correction factor."""
    sst_correction: SSTCorrection | None = None
    """Surface temperature correction factor."""
    ubind: UBind | None = None
    """Open boundary binding velocity (m/s)"""
    v_sponge: VSponge | None = None
    """Maximum sponge layer viscosity (m2/s)"""
    climatology: Climatology | None = None
    """Climatology file path"""

    # Pydantic model configuration
    model_config = {"populate_by_name": True, "alias_generator": _get_alias}
    """Pydantic model configuration"""

    @model_validator(mode="after")
    def set_fixed_output_root_name(self) -> "ROMSRuntimeSettings":
        """Apply an "after" validator to automatically change the value of the
        output_root_name attribute to the a fixed name.
        """
        self.output_root_name = OutputRootName(
            output_root_name=DEFAULT_OUTPUT_ROOT_NAME
        )
        return self

    @staticmethod
    def _load_raw_sections(filepath: str | Path) -> dict[str, list[str]]:
        """Read a roms.in text file and parse into a dictionary of sections.

        The keys are the section names and the values are a list of strings that hold
        data for that section (to be further parsed by the pydantic models for each
        section).
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File {filepath} does not exist.")

        all_lines = [line.strip() for line in filepath.read_text().split("\n")]
        lines = [line for line in all_lines if line and not line.startswith("!")]

        sections = defaultdict(list)
        key = ""

        while lines:
            line = lines.pop(0)

            if ":" in line:
                key, _ = [x.strip() for x in line.split(":", maxsplit=1)]
                continue

            if not key:
                continue

            sections[key].append(line)

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
        return cls(**sections)  # type: ignore[arg-type]

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
            f"Forcing file(s): {_list_to_concise_str(self.forcing.filenames, pad=10)}"
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
            "marbl_biogeochemistry": (
                dict(self.marbl_biogeochemistry) if self.marbl_biogeochemistry else None
            ),
            "lateral_visc": self.lateral_visc,
            "gamma2": self.gamma2,
            "tracer_diff2": (
                self.tracer_diff2 if self.tracer_diff2 is not None else None
            ),
            "vertical_mixing": (
                dict(self.vertical_mixing) if self.vertical_mixing else None
            ),
            "my_bak_mixing": dict(self.my_bak_mixing) if self.my_bak_mixing else None,
            "sss_correction": self.sss_correction,
            "sst_correction": self.sst_correction,
            "ubind": self.ubind,
            "v_sponge": self.v_sponge,
        }

        inner = ", ".join(f"{k}={repr(v)}" for k, v in attrs.items() if v is not None)
        return f"{self.__class__.__name__}({inner})"

    @model_serializer()
    def serialize_to_string(self) -> str:
        """Serialize the model (excluding null sections) to a single string as would be
        found in a ROMS-compatible `.in` file.
        """
        output = ""
        for field_name in type(self).model_fields:
            section = getattr(self, field_name)
            if section is None:
                continue
            output += section.model_dump()

        return output

    def to_file(self, filepath: str | Path) -> None:
        """Write the current settings to a ROMS-compatible `.in` file.

        Parameters
        ----------
        filepath : str or Path
            Path where the output file will be written.
        """
        with Path(filepath).open("w") as f:
            f.write(str(self.model_dump()))
