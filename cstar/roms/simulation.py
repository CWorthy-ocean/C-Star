from pathlib import Path
import subprocess
from datetime import datetime
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.discretization import ROMSDiscretization
from cstar.roms.input_dataset import (
    ROMSModelGrid,
    ROMSInitialConditions,
    ROMSTidalForcing,
    ROMSBoundaryForcing,
    ROMSSurfaceForcing,
    ROMSInputDataset,
)
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.base.additional_code import AdditionalCode
from cstar.execution.handler import ExecutionHandler
from cstar.base.utils import _get_sha256_hash
from cstar import Simulation
from cstar.system.manager import cstar_sysmgr
from typing import Optional, List


class ROMSSimulation(Simulation):
    def __init__(
        self,
        name: str,
        directory: str | Path,
        runtime_code: Optional["AdditionalCode"],
        compile_time_code: Optional["AdditionalCode"],
        discretization: Optional["ROMSDiscretization"],
        codebase: Optional["ROMSExternalCodeBase"] = None,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        valid_start_date: Optional[str | datetime] = None,
        valid_end_date: Optional[str | datetime] = None,
        marbl_codebase: Optional["MARBLExternalCodeBase"] = None,
        model_grid: Optional["ROMSModelGrid"] = None,
        initial_conditions: Optional["ROMSInitialConditions"] = None,
        tidal_forcing: Optional["ROMSTidalForcing"] = None,
        boundary_forcing: Optional[list["ROMSBoundaryForcing"]] = None,
        surface_forcing: Optional[list["ROMSSurfaceForcing"]] = None,
    ):
        super().__init__(
            name=name,
            directory=directory,
            codebase=codebase,
            runtime_code=runtime_code,
            compile_time_code=compile_time_code,
            discretization=discretization,
            start_date=start_date,
            end_date=end_date,
            valid_start_date=valid_start_date,
            valid_end_date=valid_end_date,
        )

        self.model_grid = model_grid
        self.initial_conditions = initial_conditions
        self.tidal_forcing = tidal_forcing
        self.surface_forcing = [] if surface_forcing is None else surface_forcing
        if not all([isinstance(sf, ROMSSurfaceForcing) for sf in self.surface_forcing]):
            raise TypeError(
                "ROMSComponent.surface_forcing must be a list of ROMSSurfaceForcing instances"
            )
        self.boundary_forcing = [] if boundary_forcing is None else boundary_forcing
        if not all(
            [isinstance(bf, ROMSBoundaryForcing) for bf in self.boundary_forcing]
        ):
            raise TypeError(
                "ROMSComponent.boundary_forcing must be a list of ROMSBoundaryForcing instances"
            )

        self.marbl_codebase = (
            marbl_codebase if marbl_codebase is not None else MARBLExternalCodeBase()
        )

        # roms-specific
        self.exe_path: Optional[Path] = None
        self._exe_hash: Optional[str] = None
        self.partitioned_files: List[Path] | None = None

        self._execution_handler: Optional["ExecutionHandler"] = None

    @property
    def default_codebase(self) -> ROMSExternalCodeBase:
        return ROMSExternalCodeBase()

    @property
    def codebases(self) -> list:
        return [self.codebase, self.marbl_codebase]

    @property
    def in_file(self) -> Path:
        in_files = []
        if self.runtime_code is None:
            raise ValueError(
                "ROMSComponent.runtime_code not set."
                + " ROMS reuires a runtime options file "
                + "(typically roms.in)"
            )

        in_files = [
            fname.replace(".in_TEMPLATE", ".in")
            for fname in self.runtime_code.files
            if (fname.endswith(".in") or fname.endswith(".in_TEMPLATE"))
        ]
        if len(in_files) > 1:
            raise ValueError(
                "Multiple '.in' files found:"
                + "\n{in_files}"
                + "\nROMS runtime file choice ambiguous"
            )
        elif len(in_files) == 0:
            raise ValueError(
                "No '.in' file found in ROMSComponent.runtime_code."
                + "ROMS expects a runtime options file with the '.in'"
                + "extension, e.g. roms.in"
            )
        else:
            if self.runtime_code.working_path is not None:
                return self.runtime_code.working_path / in_files[0]
            else:
                return Path(in_files[0])

    @property
    def input_datasets(self) -> list:
        """List all ROMSInputDataset objects associated with this ROMSComponent."""

        input_datasets: List[ROMSInputDataset] = []
        if self.model_grid is not None:
            input_datasets.append(self.model_grid)
        if self.initial_conditions is not None:
            input_datasets.append(self.initial_conditions)
        if self.tidal_forcing is not None:
            input_datasets.append(self.tidal_forcing)
        if len(self.boundary_forcing) > 0:
            input_datasets.extend(self.boundary_forcing)
        if len(self.surface_forcing) > 0:
            input_datasets.extend(self.surface_forcing)
        return input_datasets

    @classmethod
    def from_dict(cls, arg_dict):
        return cls(**arg_dict)

    def to_dict(self) -> dict:
        return {}

    @classmethod
    def from_blueprint(self):
        pass

    def to_blueprint(self) -> None:
        return

    def setup(
        self,
    ) -> None:
        compile_time_code_dir = self.directory / "compile_time_code/ROMS"
        runtime_code_dir = self.directory / "runtime_code/ROMS"
        input_datasets_dir = self.directory / "input_datasets/ROMS"

        # Setup ExternalCodeBase
        infostr = f"Configuring {self.__class__.__name__}"
        print(infostr + "\n" + "-" * len(infostr))
        print(f"Setting up {self.codebase.__class__.__name__}...")
        self.codebase.handle_config_status()

        if self.marbl_codebase is not None:
            print(f"Setting up {self.marbl_codebase.__class__.__name__}...")
            self.marbl_codebase.handle_config_status()

        # Compile-time code
        print(
            "\nFetching compile-time code code..."
            + "\n----------------------------------"
        )
        if self.compile_time_code is not None:
            self.compile_time_code.get(compile_time_code_dir)

        # Runtime code
        print("\nFetching runtime code... " + "\n----------------------")
        if self.runtime_code is not None:
            self.runtime_code.get(runtime_code_dir)

        # InputDatasets
        print("\nFetching input datasets..." + "\n--------------------------")
        for inp in self.input_datasets:
            # Download input dataset if its date range overlaps Case's date range
            if (
                ((inp.start_date is None) or (inp.end_date is None))
                or ((self.start_date is None) or (self.end_date is None))
                or (inp.start_date <= self.end_date)
                and (self.end_date >= self.start_date)
            ):
                inp.get(
                    local_dir=input_datasets_dir,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

    def build(self, rebuild: bool = False) -> None:
        """Compiles any code associated with this configuration of ROMS.

        Compilation occurs in the directory
        `ROMSComponent.additional_source_code.working_path
        This method sets the ROMSComponent `exe_path` attribute.

        Parameters
        ----------
        rebuild (bool, default False):
            Will force the recompilation of ROMS even if the executable
            already exists and there is no apparent reason to recompile.
        """

        if self.compile_time_code is None:
            raise ValueError(
                "Unable to compile ROMSComponent: "
                + "\nROMSComponent.compile_time_code is None."
                + "\n Compile-time files are needed to build ROMS"
            )

        build_dir = self.compile_time_code.working_path
        if build_dir is None:
            raise ValueError(
                "Unable to compile ROMSComponent: "
                + "\nROMSComponent.compile_time_code.working_path is None."
                + "\n Call ROMSComponent.compile_time_code.get() and try again"
            )

        exe_path = build_dir / "roms"
        if (
            (exe_path.exists())
            and (self.compile_time_code.exists_locally)
            and (self._exe_hash is not None)
            and (_get_sha256_hash(exe_path) == self._exe_hash)
            and not rebuild
        ):
            print(
                f"ROMS has already been built at {exe_path}, and "
                "the source code appears not to have changed. "
                "If you would like to recompile, call "
                "ROMSComponent.build(rebuild = True)"
            )
            return

        if (build_dir / "Compile").is_dir():
            make_clean_result = subprocess.run(
                "make compile_clean",
                cwd=build_dir,
                shell=True,
                capture_output=True,
                text=True,
            )
            if make_clean_result.returncode != 0:
                raise RuntimeError(
                    f"Error {make_clean_result.returncode} when compiling ROMS. STDERR stream: "
                    + f"\n {make_clean_result.stderr}"
                )

        print("Compiling UCLA-ROMS configuration...")
        make_roms_result = subprocess.run(
            f"make COMPILER={cstar_sysmgr.environment.compiler}",
            cwd=build_dir,
            shell=True,
            capture_output=True,
            text=True,
        )
        if make_roms_result.returncode != 0:
            raise RuntimeError(
                f"Error {make_roms_result.returncode} when compiling ROMS. STDERR stream: "
                + f"\n {make_roms_result.stderr}"
            )

        print(f"UCLA-ROMS compiled at {build_dir}")

        self.exe_path = exe_path
        self._exe_hash = _get_sha256_hash(exe_path)

    def pre_run(self):
        pass

    def run(self):
        pass

    def post_run(self):
        pass
