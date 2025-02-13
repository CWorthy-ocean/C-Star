import shutil
import warnings
import subprocess
from pathlib import Path
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
from cstar.base.utils import _get_sha256_hash, _replace_text_in_file

from cstar.execution.handler import ExecutionHandler
from cstar.execution.scheduler_job import create_scheduler_job
from cstar.execution.local_process import LocalProcess
from cstar.execution.handler import ExecutionStatus

from cstar import Simulation
from cstar.system.manager import cstar_sysmgr
from typing import Optional, List, cast


class ROMSSimulation(Simulation):
    discretization: ROMSDiscretization

    def __init__(
        self,
        name: str,
        directory: str | Path,
        discretization: "ROMSDiscretization",
        runtime_code: Optional["AdditionalCode"],
        compile_time_code: Optional["AdditionalCode"],
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
            discretization=discretization,
            codebase=codebase,
            runtime_code=runtime_code,
            compile_time_code=compile_time_code,
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
        pass

    def update_runtime_code(self):
        no_template_found = True
        for nl_idx, nl_fname in enumerate(self.runtime_code.files):
            nl_path = self.runtime_code.working_path / nl_fname
            if str(nl_fname)[-9:] == "_TEMPLATE":
                no_template_found = False
                mod_nl_path = Path(str(nl_path)[:-9])
                shutil.copy(nl_path, mod_nl_path)
                for placeholder, replacement in self._runtime_code_modifications[
                    nl_idx
                ].items():
                    _replace_text_in_file(mod_nl_path, placeholder, str(replacement))
                self.runtime_code.modified_files[nl_idx] = mod_nl_path
        if no_template_found:
            warnings.warn(
                "WARNING: No editable runtime code found to set ROMS runtime parameters. "
                + "Expected to find a template in ROMSSimulation.runtime_code"
                + " with the suffix '_TEMPLATE' on which to base the file."
                + "\n********************************************************"
                + "\nANY MODEL PARAMETERS SET IN C-STAR WILL NOT BE APPLIED."
                + "\n********************************************************"
            )

    @property
    def _runtime_code_modifications(self) -> list[dict]:
        # Helper function for formatting:
        def partitioned_files_to_runtime_code_string(input_dataset):
            """Take a ROMSInputDataset that has been partitioned and return a ROMS
            namelist-compatible string pointing to it e.g. path/to/roms_file.232.nc -> '
            path/to/roms_file.nc'."""

            unique_paths = {
                str(Path(f).parent / (Path(Path(f).stem).stem + ".nc"))
                for f in input_dataset.partitioned_files
            }
            return "\n     ".join(sorted(list(unique_paths)))

        # Initialise the list of dictionaries:
        if self.runtime_code is None:
            raise ValueError(
                "attempted to access "
                + "ROMSComponent._runtime_code_modifications, but "
                + "ROMSComponent.runtime_code is None"
            )

        runtime_code_modifications: list[dict] = [{} for f in self.runtime_code.files]

        ################################################################################
        # 'roms.in' file modifications (the only namelist to modify as of 2024-10-01):
        ################################################################################
        # First figure out which namelist is the one to modify
        if "roms.in_TEMPLATE" in self.runtime_code.files:
            nl_idx = self.runtime_code.files.index("roms.in_TEMPLATE")
        else:
            raise ValueError(
                "could not find expected template namelist file "
                + "roms.in_TEMPLATE to modify. "
                + "ROMS requires a namelist file to run."
            )

        # Time step entry
        runtime_code_modifications[nl_idx]["__TIMESTEP_PLACEHOLDER__"] = (
            self.discretization.time_step
        )

        # Grid file entry
        if self.model_grid is not None:
            if len(self.model_grid.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned"
                    + "ROMS grid file. Run ROMSComponent.pre_run() [or "
                    + "Case.pre_run() if running a Case] to partition "
                    + "ROMS input datasets and try again."
                )

            runtime_code_modifications[nl_idx]["__GRID_FILE_PLACEHOLDER__"] = (
                partitioned_files_to_runtime_code_string(self.model_grid)
            )

        # Initial conditions entry
        if self.initial_conditions is not None:
            if len(self.initial_conditions.partitioned_files) == 0:
                raise ValueError(
                    "could not find a local path to a partitioned"
                    + "ROMS initial file. Run ROMSComponent.pre_run() [or "
                    + "Case.pre_run() if running a Case] to partition "
                    + "ROMS input datasets and try again."
                )

            runtime_code_modifications[nl_idx][
                "__INITIAL_CONDITION_FILE_PLACEHOLDER__"
            ] = partitioned_files_to_runtime_code_string(self.initial_conditions)

        # Forcing files entry
        runtime_code_forcing_str = ""
        for sf in self.surface_forcing:
            if len(sf.partitioned_files) > 0:
                runtime_code_forcing_str += (
                    "\n     " + partitioned_files_to_runtime_code_string(sf)
                )
        for bf in self.boundary_forcing:
            if len(bf.partitioned_files) > 0:
                runtime_code_forcing_str += (
                    "\n     " + partitioned_files_to_runtime_code_string(bf)
                )
        if self.tidal_forcing is not None:
            if len(self.tidal_forcing.partitioned_files) == 0:
                raise ValueError(
                    "ROMSComponent has tidal_forcing attribute "
                    + "but could not find a local path to a partitioned "
                    + "tidal forcing file. Run ROMSComponent.pre_run() "
                    + "[or Case.pre_run() if building a Case] "
                    + " to partition ROMS input datasets and try again."
                )
            runtime_code_forcing_str += (
                "\n     " + partitioned_files_to_runtime_code_string(self.tidal_forcing)
            )

        runtime_code_modifications[nl_idx]["__FORCING_FILES_PLACEHOLDER__"] = (
            runtime_code_forcing_str.lstrip()
        )

        # MARBL settings filepaths entries
        ## NOTE: WANT TO RAISE IF PLACEHOLDER IS IN NAMELIST BUT not Path(marbl_file.exists())
        if "marbl_in" in self.runtime_code.files:
            if self.runtime_code.working_path is None:
                raise ValueError(
                    "ROMSComponent.runtime_code does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.runtime_code.get() and try again"
                )
            runtime_code_modifications[nl_idx][
                "__MARBL_SETTINGS_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_in")

        if "marbl_tracer_output_list" in self.runtime_code.files:
            if self.runtime_code.working_path is None:
                raise ValueError(
                    "ROMSComponent.runtime_code does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.runtime_code.get() and try again"
                )

            runtime_code_modifications[nl_idx][
                "__MARBL_TRACER_LIST_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_tracer_output_list")

        if "marbl_diagnostic_output_list" in self.runtime_code.files:
            if self.runtime_code.working_path is None:
                raise ValueError(
                    "ROMSComponent.runtime_code does not have a "
                    + "'working_path' attribute. "
                    + "Run ROMSComponent.runtime_code.get() and try again"
                )

            runtime_code_modifications[nl_idx][
                "__MARBL_DIAG_LIST_FILE_PLACEHOLDER__"
            ] = str(self.runtime_code.working_path / "marbl_diagnostic_output_list")

        return runtime_code_modifications

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

    def pre_run(self) -> None:
        # Partition input datasets and add their paths to namelist
        if self.input_datasets is not None and all(
            [isinstance(a, ROMSInputDataset) for a in self.input_datasets]
        ):
            datasets_to_partition = [d for d in self.input_datasets if d.exists_locally]

            for f in datasets_to_partition:
                f.partition(
                    np_xi=self.discretization.n_procs_x,
                    np_eta=self.discretization.n_procs_y,
                )

    def run(
        self,
        account_key: Optional[str] = None,
        walltime: Optional[str] = None,
        queue_name: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> "ExecutionHandler":
        if self.exe_path is None:
            raise ValueError(
                "C-STAR: ROMSComponent.exe_path is None; unable to find ROMS executable."
                + "\nRun Component.build() first. "
                + "\n If you have already run Component.build(), either run it again or "
                + " add the executable path manually using Component.exe_path='YOUR/PATH'."
            )

        if (self.end_date is not None) and (self.start_date is not None):
            run_length_seconds = int((self.end_date - self.start_date).total_seconds())
            n_time_steps = run_length_seconds // self.discretization.time_step
        else:
            n_time_steps = 1
            warnings.warn(
                "n_time_steps not explicitly set, using default value of 1. "
                "Please call ROMSComponent.run() with the n_time_steps argument "
                "to specify the length of the run.",
                UserWarning,
            )

        if (queue_name is None) and (cstar_sysmgr.scheduler is not None):
            queue_name = cstar_sysmgr.scheduler.primary_queue_name
        if (walltime is None) and (cstar_sysmgr.scheduler is not None):
            walltime = cstar_sysmgr.scheduler.get_queue(queue_name).max_walltime

        output_dir = self.directory / "output"

        # Set run path to output dir for clarity: we are running in the output dir but
        # these are conceptually different:
        run_path = output_dir

        # 1. MODIFY NAMELIST
        # Copy template namelist and add all current known information
        # from this Component instance:
        self.update_runtime_code()

        # Now need to manually update number of time steps as it is unknown
        # outside of the context of this function:

        _replace_text_in_file(
            self.in_file,
            "__NTIMES_PLACEHOLDER__",
            str(n_time_steps),
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        ## 2: RUN ROMS

        roms_exec_cmd = (
            f"{cstar_sysmgr.environment.mpi_exec_prefix} -n {self.discretization.n_procs_tot} {self.exe_path} "
            + f"{self.in_file}"
        )

        if self.discretization.n_procs_tot is None:
            raise ValueError(
                "Unable to calculate node distribution for this Component. "
                + "Component.n_procs_tot is not set"
            )
        if cstar_sysmgr.scheduler is not None:
            if account_key is None:
                raise ValueError(
                    "please call Component.run() with a value for account_key"
                )

            job_instance = create_scheduler_job(
                commands=roms_exec_cmd,
                job_name=job_name,
                cpus=self.discretization.n_procs_tot,
                account_key=account_key,
                run_path=run_path,
                queue_name=queue_name,
                walltime=walltime,
            )

            job_instance.submit()
            self._execution_handler = job_instance
            return job_instance

        else:  # cstar_sysmgr.scheduler is None
            romsprocess = LocalProcess(commands=roms_exec_cmd, run_path=run_path)
            self._execution_handler = romsprocess
            romsprocess.start()
            return romsprocess

    def post_run(self) -> None:
        if self._execution_handler is None:
            raise RuntimeError(
                "Cannot call 'ROMSComponent.post_run()' before calling 'ROMSComponent.run()'"
            )
        elif self._execution_handler.status != ExecutionStatus.COMPLETED:
            raise RuntimeError(
                "Cannot call 'ROMSComponent.post_run()' until the ROMS run is completed, "
                + f"but current execution status is '{self._execution_handler.status}'"
            )

        output_dir = self.directory / "output"
        files = list(output_dir.glob("*.??????????????.*.nc"))
        unique_wildcards = {Path(fname.stem).stem + ".*.nc" for fname in files}
        if not files:
            print("no suitable output found")
        else:
            (output_dir / "PARTITIONED").mkdir(exist_ok=True)
            for wildcard_pattern in unique_wildcards:
                # Want to go from, e.g. myfile.001.nc to myfile.*.nc, so we apply stem twice:

                print(f"Joining netCDF files {wildcard_pattern}...")
                ncjoin_result = subprocess.run(
                    f"ncjoin {wildcard_pattern}",
                    cwd=output_dir,
                    capture_output=True,
                    text=True,
                    shell=True,
                )
                if ncjoin_result.returncode != 0:
                    raise RuntimeError(
                        f"Error {ncjoin_result.returncode} while joining ROMS output. "
                        + f"STDERR stream:\n {ncjoin_result.stderr}"
                    )
                for F in output_dir.glob(wildcard_pattern):
                    F.rename(output_dir / "PARTITIONED" / F.name)

    def restart(self, new_end_date: str | datetime) -> "ROMSSimulation":
        new_sim = cast(ROMSSimulation, super().restart(new_end_date=new_end_date))

        restart_dir = self.directory / "output"

        new_start_date = new_sim.start_date
        restart_date_string = new_start_date.strftime("%Y%m%d%H%M%S")
        restart_wildcard = f"*_rst.{restart_date_string}.nc"
        restart_files = list(restart_dir.glob(restart_wildcard))
        if len(restart_files) == 0:
            raise FileNotFoundError(
                f"No files in {restart_dir} match the pattern "
                + f"'*_rst.{restart_date_string}.nc"
            )

        unique_restarts = {fname for fname in restart_files}

        if len(unique_restarts) > 1:
            raise ValueError(
                "Found multiple distinct restart files corresponding to "
                + f"{restart_date_string}: "
                + "\n ".join([str(rst) for rst in list(unique_restarts)])
            )

        restart_file = restart_dir / list(unique_restarts)[0]
        new_ic = ROMSInitialConditions(
            location=str(restart_file.resolve()), start_date=new_start_date
        )
        new_sim.initial_conditions = new_ic

        # Reset cached data for input datasets
        for inp in new_sim.input_datasets:
            inp._local_file_hash_cache = None
            inp._local_file_stat_cache = None
            inp.working_path = None

        return new_sim
