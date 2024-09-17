import warnings
import subprocess
from pathlib import Path
from typing import Optional, TYPE_CHECKING, List, Sequence

from cstar.base.utils import _calculate_node_distribution, _replace_text_in_file
from cstar.base.component import Component, Discretization

from cstar.roms.input_dataset import (
    ROMSInitialConditions,
    ROMSModelGrid,
    ROMSSurfaceForcing,
    ROMSBoundaryForcing,
    ROMSTidalForcing,
)
from cstar.base.additional_code import AdditionalCode

from cstar.base.environment import (
    _CSTAR_COMPILER,
    _CSTAR_SCHEDULER,
    _CSTAR_SYSTEM,
    _CSTAR_SYSTEM_MAX_WALLTIME,
    _CSTAR_SYSTEM_DEFAULT_PARTITION,
    _CSTAR_SYSTEM_CORES_PER_NODE,
)

if TYPE_CHECKING:
    from cstar.roms import ROMSBaseModel, ROMSInputDataset


class ROMSComponent(Component):
    """
    An implementation of the Component class for the UCLA Regional Ocean Modeling System

    This subclass contains ROMS-specific implementations of the build(), pre_run(), run(), and post_run() methods.

    Attributes:
    -----------
    base_model: ROMSBaseModel
        An object pointing to the unmodified source code of ROMS at a specific commit
    input_datasets: list of ROMSInputDatasets
        Any spatiotemporal data needed to run this instance of ROMS
        e.g. initial conditions, surface forcing, etc.
    additional_code: AdditionalCode
        Additional code contributing to a unique instance of this ROMS run
        e.g. namelists, source modifications, etc.
    discretization: ROMSDiscretization
        Any information related to discretization of this ROMSComponent
        e.g. time step, number of levels, number of CPUs following each direction, etc.

    Methods:
    --------
    build()
        Compiles any code associated with this configuration of ROMS
    pre_run()
        Performs pre-processing steps, such as partitioning input netcdf datasets into one file per core
    run()
        Runs the executable created by `build()`
    post_run()
        Performs post-processing steps, such as joining output netcdf files that are produced one-per-core
    """

    base_model: "ROMSBaseModel"
    additional_code: "AdditionalCode"
    input_datasets: Sequence["ROMSInputDataset"]
    discretization: "ROMSDiscretization"

    def __init__(
        self,
        base_model: "ROMSBaseModel",
        additional_code: "AdditionalCode",
        discretization: "ROMSDiscretization",
        input_datasets: Optional[Sequence["ROMSInputDataset"]] = None,
    ):
        """
        Initialize a ROMSComponent object from a ROMSBaseModel object, code, input datasets, and discretization information

        Parameters:
        -----------
        base_model: ROMSBaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        input_datasets:  list of InputDatasets
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.
        additional_code: AdditionalCode
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        discretization: ROMSDiscretization
            Any information related to discretization of this ROMSComponent
            e.g. time step, number of levels, number of CPUs following each direction, etc.

        Returns:
        --------
        ROMSComponent:
            An intialized ROMSComponent object
        """
        self.base_model = base_model
        self.additional_code = additional_code
        self.input_datasets = [] if input_datasets is None else input_datasets
        self.discretization = discretization

        # roms-specific
        self.exe_path: Optional[Path] = None
        self.partitioned_files: List[Path] | None = None

    def build(self) -> None:
        """
        Compiles any code associated with this configuration of ROMS.

        Compilation occurs in the directory
        `ROMSComponent.additional_code.working_path/ROMS/source_mods/
        This method sets the ROMSComponent `exe_path` attribute.

        """
        working_path = self.additional_code.working_path
        if working_path is None:
            raise ValueError(
                "Unable to compile ROMSComponent: "
                + "\nROMSComponent.additional_code.working_path is None."
                + "\n Call ROMSComponent.additional_code.get() and try again"
            )
        builddir = working_path / "source_mods"
        if (builddir / "Compile").is_dir():
            subprocess.run("make compile_clean", cwd=builddir, shell=True)
        subprocess.run(f"make COMPILER={_CSTAR_COMPILER}", cwd=builddir, shell=True)

        self.exe_path = builddir / "roms"

    def pre_run(self) -> None:
        """
        Performs pre-processing steps associated with this ROMSComponent object.

        This method:
        1. goes through any netcdf files associated with InputDataset objects belonging
           to this ROMSComponent instance and partitions them such that there is one file per processor.
           The partitioned files are stored in a subdirectory `PARTITIONED` of
           InputDataset.working_path

        2. Replaces placeholder strings (if present) representing, e.g. input file paths
           in a template roms namelist file (typically `roms.in_TEMPLATE`) used to run the model with
           the respective paths to input datasets and any MARBL namelists (if this ROMS
           component belongs to a case for which MARBL is also a component).
           The namelist file is sought in
           `ROMSComponent.additional_code.working_path/namelists`.

        """
        from cstar.roms import ROMSInputDataset

        # Partition input datasets
        if self.input_datasets is not None and all(
            [isinstance(a, ROMSInputDataset) for a in self.input_datasets]
        ):
            datasets_to_partition = [d for d in self.input_datasets if d.exists_locally]
            # Preliminary checks
            if self.additional_code.working_path is None:
                raise ValueError(
                    "Unable to prepare ROMSComponent for execution: "
                    + "\nROMSComponent.additional_code.working_path is None."
                    + "\n Call ROMSComponent.additional_code.get() and try again"
                )

            if not hasattr(self.additional_code, "modified_namelists"):
                raise ValueError(
                    "No editable namelist found in which to set ROMS runtime parameters. "
                    + "Expected to find a file in ROMSComponent.additional_code.namelists"
                    + " with the suffix '_TEMPLATE' on which to base the ROMS namelist."
                )
            else:
                mod_namelist = (
                    self.additional_code.working_path
                    / self.additional_code.modified_namelists[0]
                )

            namelist_forcing_str = ""
            # Partition input datasets and add paths to namelist
            if len(datasets_to_partition) > 0:
                from roms_tools.utils import partition_netcdf
            for f in datasets_to_partition:
                # fname = f.source.basename

                if not f.exists_locally:
                    raise ValueError(
                        f"working_path of InputDataset \n{f}\n\n {f.working_path}, "
                        + "refers to a non-existent file"
                        + "\n call InputDataset.get() and try again."
                    )
                # Partitioning step
                if f.working_path is None:
                    # Raise if inputdataset has no local working path
                    raise ValueError(f"InputDataset has no working path: {f}")
                elif isinstance(f.working_path, list):
                    # if single InputDataset corresponds to many files, check they're colocated
                    if all(
                        [d.parent == f.working_path[0].parent for d in f.working_path]
                    ):
                        raise ValueError(
                            f"A single input dataset exists in multiple directories: {f.working_path}."
                        )
                    else:
                        # If they are, we want to partition them all in the same place
                        partdir = f.working_path[0].parent / "PARTITIONED"
                        id_files_to_partition = f.working_path[:]
                else:
                    id_files_to_partition = [
                        f.working_path,
                    ]
                    partdir = f.working_path.parent / "PARTITIONED"

                partdir.mkdir(parents=True, exist_ok=True)
                parted_files = []
                for idfile in id_files_to_partition:
                    parted_files += partition_netcdf(
                        idfile,
                        np_xi=self.discretization.n_procs_x,
                        np_eta=self.discretization.n_procs_y,
                    )

                    # [p.rename(partdir / p.name) for p in parted_files[-1]]
                [p.rename(partdir / p.name) for p in parted_files]
                parted_files = [partdir / p.name for p in parted_files]
                f.partitioned_files = parted_files

                # Namelist modification step
                if isinstance(f, ROMSModelGrid):
                    if f.working_path is None or isinstance(f.working_path, list):
                        raise ValueError(
                            f"ROMS only accepts a single grid file, found list {f.working_path}"
                        )

                    assert isinstance(f.working_path, Path), "silence, linter"

                    namelist_grid_str = f"     {partdir / f.working_path.name} \n"
                    _replace_text_in_file(
                        mod_namelist, "__GRID_FILE_PLACEHOLDER__", namelist_grid_str
                    )
                elif isinstance(f, ROMSInitialConditions):
                    if f.working_path is None or isinstance(f.working_path, list):
                        raise ValueError(
                            f"ROMS only accepts a single initial conditions file, found list {f.working_path}"
                        )
                    assert isinstance(f.working_path, Path), "silence, linter"
                    namelist_ic_str = f"     {partdir / f.working_path.name} \n"
                    _replace_text_in_file(
                        mod_namelist,
                        "__INITIAL_CONDITION_FILE_PLACEHOLDER__",
                        namelist_ic_str,
                    )
                elif type(f) in [
                    ROMSSurfaceForcing,
                    ROMSTidalForcing,
                    ROMSBoundaryForcing,
                ]:
                    if isinstance(f.working_path, Path):
                        dslist = [
                            f.working_path,
                        ]
                    elif isinstance(f.working_path, list):
                        dslist = f.working_path
                    for d in dslist:
                        namelist_forcing_str += f"     {partdir / d.name} \n"

            _replace_text_in_file(
                mod_namelist, "__FORCING_FILES_PLACEHOLDER__", namelist_forcing_str
            )

            _replace_text_in_file(
                mod_namelist,
                "MARBL_NAMELIST_DIR",
                str(self.additional_code.working_path / "namelists"),
            )

    def run(
        self,
        n_time_steps: Optional[int] = None,
        account_key: Optional[str] = None,
        output_dir: Optional[str | Path] = None,
        walltime: Optional[str] = _CSTAR_SYSTEM_MAX_WALLTIME,
        job_name: str = "my_roms_run",
    ) -> None:
        """
        Runs the executable created by `build()`

        This method creates a temporary file to be submitted to the job scheduler (if any)
        on the calling machine, then submits it. By default the job requests the maximum
        walltime. It calculates the number of nodes and cores-per-node to request based on
        the number of cores required by the job, `ROMSComponent.discretization.n_procs_tot`.

        Parameters:
        -----------
        account_key: str, default None
            The users account key on the system
        output_dir: str or Path:
            The path to the directory in which model output will be saved. This is by default
            the directory from which the ROMS executable will be called.
        walltime: str, default _CSTAR_SYSTEM_MAX_WALLTIME
            The requested length of the job, HH:MM:SS
        job_name: str, default 'my_roms_run'
            The name of the job submitted to the scheduler, which also sets the output file name
            `job_name.out`
        """

        if self.exe_path is None:
            raise ValueError(
                "C-STAR: ROMSComponent.exe_path is None; unable to find ROMS executable."
                + "\nRun Component.build() first. "
                + "\n If you have already run Component.build(), either run it again or "
                + " add the executable path manually using Component.exe_path='YOUR/PATH'."
            )
        if output_dir is None:
            output_dir = self.exe_path.parent
        output_dir = Path(output_dir)

        # Set run path to output dir for clarity: we are running in the output dir but
        # these are conceptually different:
        run_path = output_dir

        if self.additional_code is None:
            print(
                "C-STAR: Unable to find AdditionalCode associated with this Component."
            )
            return

        # Add number of timesteps to namelist
        # Check if n_time_steps is None, indicating it was not explicitly set
        if n_time_steps is None:
            n_time_steps = 1
            warnings.warn(
                "n_time_steps not explicitly set, using default value of 1. "
                "Please call ROMSComponent.run() with the n_time_steps argument "
                "to specify the length of the run.",
                UserWarning,
            )
        assert isinstance(n_time_steps, int)

        if hasattr(self.additional_code, "modified_namelists"):
            mod_namelist = (
                self.additional_code.working_path
                / self.additional_code.modified_namelists[0]
            )
            _replace_text_in_file(
                mod_namelist,
                "__NTIMES_PLACEHOLDER__",
                str(n_time_steps),
            )
            _replace_text_in_file(
                mod_namelist,
                "__TIMESTEP_PLACEHOLDER__",
                str(self.discretization.time_step),
            )

        else:
            raise ValueError(
                "No editable namelist found to set ROMS runtime parameters. "
                + "Expected to find a file in ROMSComponent.additional_code.namelists"
                + " with the suffix '_TEMPLATE' on which to base the ROMS namelist."
            )
        output_dir.mkdir(parents=True, exist_ok=True)

        match _CSTAR_SYSTEM:
            case "sdsc_expanse":
                exec_pfx = "srun --mpi=pmi2"
            case "nersc_perlmutter":
                exec_pfx = "srun"
            case "ncar_derecho":
                exec_pfx = "mpirun"
            case "osx_arm64":
                exec_pfx = "mpirun"
            case "linux_x86_64":
                exec_pfx = "mpirun"

        roms_exec_cmd = (
            f"{exec_pfx} -n {self.discretization.n_procs_tot} {self.exe_path} "
            + f"{mod_namelist}"
        )

        if self.discretization.n_procs_tot is not None:
            if _CSTAR_SYSTEM_CORES_PER_NODE is not None:
                nnodes, ncores = _calculate_node_distribution(
                    self.discretization.n_procs_tot, _CSTAR_SYSTEM_CORES_PER_NODE
                )
            else:
                raise ValueError(
                    f"Unable to calculate node distribution for system: {_CSTAR_SYSTEM}."
                    + "\nC-Star is unaware of your system's node configuration (cores per node)."
                    + "\nYour system may be unsupported. Please raise an issue at: "
                    + "\n https://github.com/CWorthy-ocean/C-Star/issues/new"
                    + "\n Thank you in advance for your contribution!"
                )
        else:
            raise ValueError(
                "Unable to calculate node distribution for this Component. "
                + "Component.n_procs_tot is not set"
            )

        match _CSTAR_SCHEDULER:
            case "pbs":
                if account_key is None:
                    raise ValueError(
                        "please call Component.run() with a value for account_key"
                    )
                scheduler_script = "#PBS -S /bin/bash"
                scheduler_script += f"\n#PBS -N {job_name}"
                scheduler_script += f"\n#PBS -o {job_name}.out"
                scheduler_script += f"\n#PBS -A {account_key}"
                scheduler_script += (
                    f"\n#PBS -l select={nnodes}:ncpus={ncores},walltime={walltime}"
                )
                scheduler_script += f"\n#PBS -q {_CSTAR_SYSTEM_DEFAULT_PARTITION}"
                scheduler_script += "\n#PBS -j oe"
                scheduler_script += "\n#PBS -k eod"
                scheduler_script += "\n#PBS -V"
                if _CSTAR_SYSTEM == "ncar_derecho":
                    scheduler_script += "\ncd ${PBS_O_WORKDIR}"
                scheduler_script += f"\n\n{roms_exec_cmd}"

                script_fname = "cstar_run_script.pbs"
                with open(run_path / script_fname, "w") as f:
                    f.write(scheduler_script)
                subprocess.run(f"qsub {script_fname}", shell=True, cwd=run_path)

            case "slurm":
                # TODO: export ALL copies env vars, but will need to handle module load
                if account_key is None:
                    raise ValueError(
                        "please call Component.run() with a value for account_key"
                    )

                scheduler_script = "#!/bin/bash"
                scheduler_script += f"\n#SBATCH --job-name={job_name}"
                scheduler_script += f"\n#SBATCH --output={job_name}.out"
                if _CSTAR_SYSTEM == "nersc_perlmutter":
                    scheduler_script += (
                        f"\n#SBATCH --qos={_CSTAR_SYSTEM_DEFAULT_PARTITION}"
                    )
                    scheduler_script += "\n#SBATCH -C cpu"
                else:
                    scheduler_script += (
                        f"\n#SBATCH --partition={_CSTAR_SYSTEM_DEFAULT_PARTITION}"
                    )
                    # FIXME: This ^^^ is a pretty ugly patch...
                scheduler_script += f"\n#SBATCH --nodes={nnodes}"
                scheduler_script += f"\n#SBATCH --ntasks-per-node={ncores}"
                scheduler_script += f"\n#SBATCH --account={account_key}"
                scheduler_script += "\n#SBATCH --export=ALL"
                scheduler_script += "\n#SBATCH --mail-type=ALL"
                scheduler_script += f"\n#SBATCH --time={walltime}"
                scheduler_script += f"\n\n{roms_exec_cmd}"

                script_fname = "cstar_run_script.sh"
                with open(run_path / script_fname, "w") as f:
                    f.write(scheduler_script)
                subprocess.run(f"sbatch {script_fname}", shell=True, cwd=run_path)

            case None:
                import time

                romsprocess = subprocess.Popen(
                    roms_exec_cmd,
                    shell=True,
                    cwd=run_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                # Process stdout line-by-line
                tstep = 0
                roms_init_string = ""
                T0 = time.time()
                if romsprocess.stdout is None:
                    raise RuntimeError("ROMS is not producing stdout")
                for line in romsprocess.stdout:
                    # Split the line by whitespace
                    parts = line.split()

                    # Check if there are exactly 9 parts and the first one is an integer
                    if len(parts) == 9:
                        try:
                            # Try to convert the first part to an integer
                            tstep = int(parts[0])
                            if tstep == 0:
                                tstep0 = tstep
                            # Capture the first integer and print it
                            ETA = (n_time_steps - (tstep - tstep0)) * (
                                (tstep - tstep0) / (time.time() - T0)
                            )
                            print(
                                f"Running ROMS: time-step {tstep-tstep0} of {n_time_steps} ({time.time()-T0:.1f}s elapsed; ETA {ETA}s)"
                            )
                        except ValueError:
                            pass
                    elif tstep == 0 and len(roms_init_string) == 0:
                        roms_init_string = "Running ROMS: Initializing run..."
                        print(roms_init_string)

                romsprocess.wait()

                if romsprocess.returncode != 0:
                    import datetime as dt

                    errlog = (
                        output_dir
                        / f"ROMS_STDERR_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                    )
                    if romsprocess.stderr is not None:
                        with open(errlog, "w") as F:
                            F.write(romsprocess.stderr.read())
                    raise RuntimeError(
                        f"ROMS terminated with errors. See {errlog} for further information."
                    )

    def post_run(self, output_dir=None) -> None:
        """
        Performs post-processing steps associated with this ROMSComponent object.

        This method goes through any netcdf files produced by the model in
        `output_dir` and joins netcdf files that are produced separately by each processor.

        Parameters:
        -----------
        output_dir: str | Path
            The directory in which output was produced by the run
        """
        output_dir = Path(output_dir)
        files = list(output_dir.glob("*.*0.nc"))
        if not files:
            print("no suitable output found")
        else:
            (output_dir / "PARTITIONED").mkdir(exist_ok=True)
            for f in files:
                print(f)
                # Want to go from, e.g. myfile.001.nc to myfile.*.nc, so we apply stem twice:
                wildcard_pattern = f"{Path(f.stem).stem}.*.nc"
                subprocess.run(
                    f"ncjoin {wildcard_pattern}",
                    cwd=output_dir,
                    shell=True,
                )
                for F in output_dir.glob(wildcard_pattern):
                    F.rename(output_dir / "PARTITIONED" / F.name)


class ROMSDiscretization(Discretization):
    """
    An implementation of the Discretization class for ROMS.


    Additional attributes:
    ----------------------
    n_procs_x: int
        The number of parallel processors over which to subdivide the x axis of the domain.
    n_procs_y: int
        The number of parallel processors over which to subdivide the y axis of the domain.

    Properties:
    -----------
    n_procs_tot: int
        The value of n_procs_x * n_procs_y

    """

    def __init__(
        self,
        time_step: int,
        n_procs_x: int = 1,
        n_procs_y: int = 1,
    ):
        """
        Initialize a ROMSDiscretization object from basic discretization parameters

        Parameters:
        -----------
        time_step: int
            The time step with which to run the Component
        n_procs_x: int
           The number of parallel processors over which to subdivide the x axis of the domain.
        n_procs_y: int
           The number of parallel processors over which to subdivide the y axis of the domain.


        Returns:
        --------
        ROMSDiscretization:
            An initialized ROMSDiscretization object

        """

        super().__init__(time_step)
        self.n_procs_x = n_procs_x
        self.n_procs_y = n_procs_y

    @property
    def n_procs_tot(self) -> int:
        """Total number of processors required by this ROMS configuration"""
        return self.n_procs_x * self.n_procs_y

    def __str__(self) -> str:
        disc_str = super().__str__()

        if hasattr(self, "n_procs_x") and self.n_procs_x is not None:
            disc_str += (
                "\nn_procs_x: "
                + str(self.n_procs_x)
                + " (Number of x-direction processors)"
            )
        if hasattr(self, "n_procs_y") and self.n_procs_y is not None:
            disc_str += (
                "\nn_procs_y: "
                + str(self.n_procs_y)
                + " (Number of y-direction processors)"
            )
        return disc_str

    def __repr__(self) -> str:
        repr_str = super().__repr__().strip(")")
        if hasattr(self, "n_procs_x") and self.n_procs_x is not None:
            repr_str += f", n_procs_x = {self.n_procs_x}, "
        if hasattr(self, "n_procs_y") and self.n_procs_y is not None:
            repr_str += f"n_procs_y = {self.n_procs_y}, "

        repr_str = repr_str.strip(", ")
        repr_str += ")"

        return repr_str
