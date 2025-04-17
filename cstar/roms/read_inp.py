import numpy as np
from pathlib import Path
from typing import Optional
from collections import OrderedDict


class ROMSInputSettings:
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

    def __init__(
        self,
        # Non-optional:
        title: str,
        time_stepping: list[int],
        bottom_drag: list[float],
        initial: tuple[int, str | Path | None],
        forcing: list[str | Path],
        output_root_name: str,
        # Optional (cpp-key dependent):
        s_coord: Optional[list[float]] = None,
        rho0: Optional[float] = None,
        lin_rho_eos: Optional[list[float]] = None,
        marbl_biogeochemistry: Optional[list[Path | str]] = None,
        lateral_visc: Optional[float] = None,
        gamma2: Optional[float] = None,
        tracer_diff2: Optional[np.ndarray] = None,
        vertical_mixing: Optional[tuple[float, np.ndarray]] = None,
        my_bak_mixing: Optional[list[float]] = None,
        sss_correction: Optional[float] = None,
        sst_correction: Optional[float] = None,
        ubind: Optional[float] = None,
        v_sponge: Optional[float] = None,
        grid: Optional[str | Path] = None,
        climatology: Optional[str | Path] = None,
    ):
        """Initialize a ROMSInputSettings instance.

        Parameters
        ----------
        title : str
            Description of the ROMS run.
        time_stepping : list[int]
            List containing [ntimes, dt, ndtfast, ninfo].
        bottom_drag : list[float]
            List containing [rdrg, rdrg2, zob].
        initial : tuple[int, str | Path | None]
            A tuple (num. records, filename) for initial conditions.
            Required, but filename can be None and num. records 0.
        forcing : list[str | Path]
            List of paths to forcing files. Required, but can be empty.
        output_root_name : str
            Base name for ROMS output files.
        s_coord : list[float], optional
            List containing vertical co-ordinate params [theta_s, theta_b, tcline].
        rho0 : float, optional
            Boussinesq reference density.
        lin_rho_eos : list[float], optional
            Linear equation of state coefficients [Tcoef, T0, Scoef, S0].
        marbl_biogeochemistry : list[str | Path], optional
            Paths to MARBL input files.
        lateral_visc : float, optional
            Horizontal viscosity parameter.
        gamma2 : float, optional
            Lateral boundary slipperiness parameter.
        tracer_diff2 : np.ndarray, optional
            Array of horizontal tracer diffusivities.
        vertical_mixing : tuple[float, np.ndarray], optional
            Tuple of background viscosity and (one per tracer) mixing coefficients.
        my_bak_mixing : list[float], optional
            List containing [Akq_bak, q2nu2, q2nu4]
            for Mellor-Yamada 2.5 turbulent closure
        sss_correction : float, optional
            Surface salinity correction factor.
        sst_correction : float, optional
            Surface temperature correction factor.
        ubind : float, optional
            Open boundary binding velocity scale.
        v_sponge : float, optional
            Maximum viscosity in sponge layers.
        grid : str or Path, optional
            Path to grid file.
        climatology : str or Path, optional
            Path to climatology file.

        See Also
        --------
        - ROMSInputSettings:
            Container for reading, manipulating, and writing ROMS
            `.in` runtime configuration files.
        """

        self.title: str = title
        self.time_stepping: OrderedDict = OrderedDict(
            [
                ("ntimes", time_stepping[0]),
                ("dt", time_stepping[1]),
                ("ndtfast", time_stepping[2]),
                ("ninfo", time_stepping[3]),
            ]
        )

        self.bottom_drag: OrderedDict = OrderedDict(
            [
                ("rdrg", bottom_drag[0]),
                ("rdrg2", bottom_drag[1]),
                ("zob", bottom_drag[2]),
            ]
        )

        self.initial: OrderedDict = OrderedDict([("nrrec", initial[0])])
        self.initial["ininame"] = Path(initial[1]) if initial[1] else ""

        self.forcing = [Path(f) for f in forcing]
        self.output_root_name = output_root_name

        ################################################################################
        # OPTIONAL

        if marbl_biogeochemistry is not None:
            self.marbl_biogeochemistry: OrderedDict | None = OrderedDict(
                [
                    ("marbl_namelist_fname", marbl_biogeochemistry[0]),
                    ("marbl_tracer_list_fname", marbl_biogeochemistry[1]),
                    ("marbl_diag_list_fname", marbl_biogeochemistry[2]),
                ]
            )
        else:
            self.marbl_biogeochemistry = None

        if s_coord is not None:
            self.s_coord: OrderedDict | None = OrderedDict(
                [
                    ("theta_s", s_coord[0]),
                    ("theta_b", s_coord[1]),
                    ("tcline", s_coord[2]),
                ]
            )
        else:
            self.s_coord = None

        self.rho0 = rho0
        if lin_rho_eos is not None:
            self.lin_rho_eos: OrderedDict | None = OrderedDict(
                [
                    ("Tcoef", lin_rho_eos[0]),
                    ("T0", lin_rho_eos[1]),
                    ("Scoef", lin_rho_eos[2]),
                    ("S0", lin_rho_eos[3]),
                ]
            )
        else:
            self.lin_rho_eos = None

        self.lateral_visc = lateral_visc
        self.gamma2 = gamma2
        self.tracer_diff2 = tracer_diff2

        if vertical_mixing is not None:
            self.vertical_mixing: OrderedDict | None = OrderedDict(
                [
                    ("Akv_bak", vertical_mixing[0]),
                    ("Akt_bak", vertical_mixing[1]),
                ]
            )
        else:
            self.vertical_mixing = None

        if my_bak_mixing is not None:
            self.my_bak_mixing: OrderedDict | None = OrderedDict(
                [
                    ("Akq_bak", my_bak_mixing[0]),
                    ("q2nu2", my_bak_mixing[1]),
                    ("q2nu4", my_bak_mixing[2]),
                ]
            )
        else:
            self.my_bak_mixing = None

        self.sss_correction = sss_correction
        self.sst_correction = sst_correction
        self.ubind = ubind
        self.v_sponge = v_sponge
        self.grid = Path(grid) if grid is not None else None

        self.climatology = climatology

    @classmethod
    def from_file(cls, filepath: Path | str):
        """Read ROMS input settings from a `.in` file.

        ROMS runtime settings are specified via a `.in` file with sections corresponding
        to different sets of parameters (e.g. time_stepping, bottom_drag). This method
        first parses the file, creating a dict[str,list[str]] where the keys are the
        section names and the values are a list of lines in that section. It then
        translates that dictionary into a ROMSInputSettings instance with properly
        formatted attributes.

        Parameters
        ----------
        - filepath (Path or str):
           The path to the `.in` file

        See Also
        --------
        - `ROMSInputSettings.to_file()`: writes a ROMSInputSettings instance to a
           ROMS-compatible `.in` file
        """

        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(filepath)

        sections = {}
        with filepath.open() as f:
            lines = list(f)

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line or line.startswith("!"):
                i += 1
                continue

            if ":" in line:
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

        # import pdb; pdb.set_trace()
        def _single_line_section_to_list(section_name, expected_type):
            if section_name not in sections:
                return None
            else:
                section = sections[section_name][0].split()
                if expected_type == float:
                    section = [x.replace("D", "E") for x in section]
                section = [expected_type(x) for x in section]

                return section

        def _single_line_section_to_scalar(section_name, expected_type):
            lst = _single_line_section_to_list(section_name, expected_type)
            # if len(lst) > 1:
            #     raise ValueError(f"Expected single {expected_type} entry for {section_name} but found {len(lst)}")
            # else:
            return lst[0] if lst else None

        # Non-optional
        # One-line sections:
        title = sections["title"][0]
        time_stepping = _single_line_section_to_list("time_stepping", int)
        bottom_drag = _single_line_section_to_list("bottom_drag", float)
        output_root_name = sections["output_root_name"][0]

        # Multi-line sections:
        nrrec = int(sections["initial"][0])
        ini_path = (
            Path(sections["initial"][1]) if len(sections["initial"]) > 1 else None
        )
        initial = (nrrec, ini_path)
        forcing: list[Path | str] = [Path(f) for f in sections["forcing"]]

        # Optional
        # One-line sections:
        s_coord = _single_line_section_to_list("S-coord", float)
        rho0 = _single_line_section_to_scalar("rho0", float)
        lin_rho_eos = _single_line_section_to_list("lin_rho_eos", float)
        lateral_visc = _single_line_section_to_scalar("lateral_visc", float)
        gamma2 = _single_line_section_to_scalar("gamma2", float)

        tracer_diff2_list = _single_line_section_to_list("tracer_diff2", float)
        tracer_diff2 = np.array(tracer_diff2_list) if tracer_diff2_list else None

        my_bak_mixing = _single_line_section_to_list("MY_bak_mixing", float)

        vmix_list = _single_line_section_to_list("vertical_mixing", float)
        vertical_mixing = (vmix_list[0], np.array(vmix_list[1:])) if vmix_list else None

        sss_correction = _single_line_section_to_scalar("SSS_correction", float)
        sst_correction = _single_line_section_to_scalar("SST_correction", float)
        ubind = _single_line_section_to_scalar("ubind", float)
        v_sponge = _single_line_section_to_scalar("v_sponge", float)
        grid = _single_line_section_to_scalar("grid", Path)
        climatology = _single_line_section_to_scalar("climatology", Path)

        # Multi-line sections:
        marbl_biogeochemistry: Optional[list[Path | str]] = (
            [Path(x) for x in sections["MARBL_biogeochemistry"]]
            if "MARBL_biogeochemistry" in sections
            else None
        )

        return cls(
            title=title,
            time_stepping=time_stepping,
            marbl_biogeochemistry=marbl_biogeochemistry,
            s_coord=s_coord,
            rho0=rho0,
            lin_rho_eos=lin_rho_eos,
            lateral_visc=lateral_visc,
            gamma2=gamma2,
            tracer_diff2=tracer_diff2,
            bottom_drag=bottom_drag,
            vertical_mixing=vertical_mixing,
            my_bak_mixing=my_bak_mixing,
            sss_correction=sss_correction,
            sst_correction=sst_correction,
            ubind=ubind,
            v_sponge=v_sponge,
            grid=grid,
            initial=initial,
            forcing=forcing,
            climatology=climatology,
            output_root_name=output_root_name,
        )

    def to_file(self, filepath: Path | str) -> None:
        """Write the current settings to a ROMS-compatible `.in` file.

        Parameters
        ----------
        filepath : str or Path
            Path where the output file will be written.
        """

        filepath = Path(filepath)

        def _format_value(val):
            """Format a single value for .in file, using exponents for large or small
            values."""

            if isinstance(val, float):
                if val == 0.0:
                    return "0."
                elif abs(val) < 1e-2 or abs(val) >= 1e4:
                    return f"{val:.6E}".replace("E+00", "E0")
                else:
                    return str(val)
            return str(val)

        def _format_float_list(lst):
            """Format a list of values for .in file."""

            return " ".join(_format_value(x) for x in lst)

        def write_section(name: str, data: dict | list | str, multi_line: bool = False):
            """Write a section to file using ROMS input formatting conventions.

            Parameters
            ----------
            name : str
                Name of the ROMS input section.
            data : dict, list, or str
                Section content. Dicts print keys inline, lists print items.
            multi_line : bool, default=False
                Whether to write each value on its own line.
            """

            if multi_line:
                value_joiner = "\n    "
            else:
                value_joiner = "    "

            # OrderedDict with keys = write keys inline for readability
            if isinstance(data, OrderedDict):
                keys_line = " ".join(data.keys())
                values = value_joiner.join(_format_value(v) for v in data.values())
                f.write(f"{name}: {keys_line}\n")
                f.write(f"    {values}\n")

            # list of strings (multi-line section)
            elif isinstance(data, list):
                f.write(f"{name}:\n")
                values = (
                    "    " + value_joiner.join([_format_value(v) for v in data]) + "\n"
                )
                f.write(values)
            # single line string
            else:
                f.write(f"{name}:\n")
                f.write(f"    {data}\n")
            # end with a newline
            f.write("\n")

        with filepath.open("w") as f:
            # Non-optional
            write_section("title", self.title)
            write_section("time_stepping", self.time_stepping)
            write_section("bottom_drag", self.bottom_drag)
            write_section("initial", self.initial, multi_line=True)
            write_section("forcing", self.forcing, multi_line=True)
            write_section("output_root_name", self.output_root_name)

            if self.s_coord:
                write_section("S-coord", self.s_coord)

            if self.grid:
                write_section("grid", str(self.grid))

            if self.marbl_biogeochemistry:
                write_section(
                    "MARBL_biogeochemistry",
                    [
                        str(self.marbl_biogeochemistry[k])
                        for k in self.marbl_biogeochemistry
                    ],
                )

            if self.lateral_visc is not None:
                write_section("lateral_visc", _format_value(self.lateral_visc))

            if self.rho0 is not None:
                write_section("rho0", _format_value(self.rho0))

            if self.lin_rho_eos:
                write_section("lin_rho_eos", self.lin_rho_eos)

            if self.gamma2 is not None:
                write_section("gamma2", _format_value(self.gamma2))

            if self.tracer_diff2 is not None:
                write_section("tracer_diff2", _format_float_list(self.tracer_diff2))

            if self.vertical_mixing:
                write_section(
                    "vertical_mixing",
                    OrderedDict(
                        {
                            "Akv_bak": self.vertical_mixing["Akv_bak"],
                            "Akt_bak": _format_float_list(
                                self.vertical_mixing["Akt_bak"]
                            ),
                        }
                    ),
                )

            if self.my_bak_mixing is not None:
                write_section("MY_bak_mixing", self.my_bak_mixing)

            if self.sss_correction is not None:
                write_section("SSS_correction", _format_value(self.sss_correction))

            if self.sst_correction is not None:
                write_section("SST_correction", _format_value(self.sst_correction))

            if self.ubind is not None:
                write_section("ubind", _format_value(self.ubind))

            if self.v_sponge is not None:
                write_section("v_sponge", _format_value(self.v_sponge))

            if self.climatology is not None:
                write_section("climatology", str(self.climatology))
