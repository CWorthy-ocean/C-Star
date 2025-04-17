import numpy as np
from pathlib import Path
from typing import Optional
from collections import OrderedDict


class ROMSInputSettings:
    def __init__(
        self,
        title: str,
        time_stepping: list[int],
        bottom_drag: list[float],
        initial: tuple[int, str | Path | None],  # not optional, even with ana
        forcing: list[str | Path],
        output_root_name: str,
        # Optional (cpp-key dependent)
        s_coord: Optional[list[float]] = None,
        rho0: Optional[float] = None,
        lin_rho_eos: Optional[list[float]] = None,
        marbl_biogeochemistry: Optional[list[Path | str]] = None,
        lateral_visc: Optional[float] = None,
        gamma2: Optional[float] = None,
        tracer_diff2: Optional[np.ndarray] = None,
        vertical_mixing: Optional[tuple[float, np.ndarray]] = None,  # float, NT
        my_bak_mixing: Optional[np.array] = None,
        sss_correction: Optional[float] = None,
        sst_correction: Optional[float] = None,
        ubind: Optional[float] = None,
        v_sponge: Optional[float] = None,
        grid: Optional[str | Path] = None,  # will run w/o
        climatology: Optional[str | Path] = None,
    ):
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

        # self.initial: OrderedDict = OrderedDict([
        #     ("nrrec", initial[0]),
        #     ("ininame", Path(initial[1]))
        # ])

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

        self.my_bak_mixing = my_bak_mixing
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

        my_bak_mixing_list = _single_line_section_to_list("MY_bak_mixing", float)
        my_bak_mixing = np.array(my_bak_mixing_list) if my_bak_mixing_list else None

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
        filepath = Path(filepath)

        def _format_value(val):
            if isinstance(val, float):
                if val == 0.0:
                    return "0."
                elif abs(val) < 1e-2 or abs(val) >= 1e4:
                    return f"{val:.6E}".replace(
                        "E+00", "E0"
                    )  # force exponent if large/small
                else:
                    return str(val)
            return str(val)

        def _format_float_list(lst):
            return " ".join(_format_value(x) for x in lst)

        def _flatten_dict_values(d: OrderedDict) -> str:
            return " ".join(_format_value(v) for v in d.values())

        def write_section(name: str, data: dict | list | str, multi_line: bool = False):
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
                write_section("MY_bak_mixing", _format_float_list(self.my_bak_mixing))

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
