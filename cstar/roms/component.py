import os
import glob
import warnings
import subprocess
from typing import List, Optional, TYPE_CHECKING

from cstar.base.utils import _calculate_node_distribution, _replace_text_in_file
from cstar.base import Component
from cstar.base.input_dataset import (
    InputDataset,
    InitialConditions,
    ModelGrid,
    SurfaceForcing,
    BoundaryForcing,
    TidalForcing,
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
    from cstar.base import ROMSBaseModel


class ROMSComponent(Component):
    """
    An implementation of the Component class for the UCLA Regional Ocean Modeling System

    This subclass has unique attributes concerning parallelization, and ROMS-specific implementations
    of the build(), pre_run(), run(), and post_run() methods.

    Attributes:
    -----------
    base_model: ROMSBaseModel
        An object pointing to the unmodified source code of ROMS at a specific commit
    additional_code: AdditionalCode or list of AdditionalCodes
        Additional code contributing to a unique instance of this ROMS run
        e.g. namelists, source modifications, etc.
    input_datasets: InputDataset or list of InputDatasets
        Any spatiotemporal data needed to run this instance of ROMS
        e.g. initial conditions, surface forcing, etc.
    time_step: int, Optional, default=1
        The time step with which to run ROMS in this configuration
    nx,ny,n_levels: int
        The number of x and y points and vertical levels in the domain associated with this object
    n_procs_x,n_procs_y: int
        The number of processes following the x and y directions, for running in parallel

    Properties:
    -----------
    n_procs_tot: int
        The value of n_procs_x * n_procs_y

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

    def __init__(
        self,
        base_model: "ROMSBaseModel",
        additional_code: Optional[AdditionalCode] = None,
        input_datasets: Optional[InputDataset | List[InputDataset]] = None,
        time_step: int = 1,
        nx: Optional[int] = None,
        ny: Optional[int] = None,
        n_levels: Optional[int] = None,
        n_procs_x: Optional[int] = None,
        n_procs_y: Optional[int] = None,
    ):
        """
        Initialize a ROMSComponent object from a ROMSBaseModel object, code, input datasets, and discretization information

        Parameters:
        -----------
        base_model: ROMSBaseModel
            An object pointing to the unmodified source code of a model handling an individual
            aspect of the simulation such as biogeochemistry or ocean circulation
        additional_code: AdditionalCode
            Additional code contributing to a unique instance of a base model,
            e.g. namelists, source modifications, etc.
        input_datasets: InputDataset or list of InputDatasets
            Any spatiotemporal data needed to run this instance of the base model
            e.g. initial conditions, surface forcing, etc.
        nx,ny,n_levels: int
            The number of x and y points and vertical levels in the domain associated with this object
        n_procs_x,n_procs_y: int
            The number of processes following the x and y directions, for running in parallel
        exe_path:
            The path to the ROMS executable, set when `build()` is called

        Returns:
        --------
        ROMSComponent:
            An intialized ROMSComponent object
        """

        super().__init__(
            base_model=base_model,
            additional_code=additional_code,
            input_datasets=input_datasets,
        )
        # QUESTION: should all these attrs be passed in as a single "discretization" arg of type dict?
        self.time_step: int = time_step
        self.nx: Optional[int] = nx
        self.ny: Optional[int] = ny
        self.n_levels: Optional[int] = n_levels
        self.n_procs_x: Optional[int] = n_procs_x
        self.n_procs_y: Optional[int] = n_procs_y
        self.exe_path: Optional[str] = None

    @property
    def n_procs_tot(self) -> Optional[int]:
        """Total number of processors required by this ROMS configuration"""
        if (self.n_procs_x is None) or (self.n_procs_y is None):
            return None
        else:
            return self.n_procs_x * self.n_procs_y

    def build(self):
        """
        Compiles any code associated with this configuration of ROMS.

        Compilation occurs in the directory
        `ROMSComponent.additional_code.local_path/source_mods/ROMS`.
        This method sets the ROMSComponent `exe_path` attribute.

        """

        local_path = self.additional_code.local_path

        builddir = local_path + "/source_mods/ROMS/"
        if os.path.isdir(builddir + "Compile"):
            subprocess.run("make compile_clean", cwd=builddir, shell=True)
        subprocess.run(f"make COMPILER={_CSTAR_COMPILER}", cwd=builddir, shell=True)

        self.exe_path = builddir + "roms"

    def pre_run(self):
        """
        Performs pre-processing steps associated with this ROMSComponent object.

        This method:
        1. goes through any netcdf files associated with InputDataset objects belonging
           to this ROMSComponent instance and runs `partit`, a ROMS program used to
           partition netcdf files such that there is one file per processor.
           The partitioned files are stored in a subdirectory `PARTITIONED` of
           InputDataset.local_path

        2. Replaces the template strings INPUT_DIR and MARBL_NAMELIST_DIR (if present)
           in the roms namelist file (typically `roms.in`) used to run the model with
           the respective paths to input datasets and any MARBL namelists (if this ROMS
           component belongs to a case for which MARBL is also a component).
           The namelist file is sought in
           `ROMSComponent.additional_code.local_path/namelists/ROMS`.

        """

        # Partition input datasets
        if self.input_datasets is not None:
            if isinstance(self.input_datasets, InputDataset):
                dataset_list = [
                    self.input_datasets,
                ]
            elif isinstance(self.input_datasets, list):
                dataset_list = self.input_datasets
            else:
                dataset_list = []

            datasets_to_partition = [d for d in dataset_list if d.exists_locally]

            for f in datasets_to_partition:
                dspath = f.local_path
                fname = os.path.basename(f.source)

                os.makedirs(os.path.dirname(dspath) + "/PARTITIONED", exist_ok=True)
                subprocess.run(
                    "partit "
                    + str(self.n_procs_x)
                    + " "
                    + str(self.n_procs_y)
                    + " ../"
                    + fname,
                    cwd=os.path.dirname(dspath) + "/PARTITIONED",
                    shell=True,
                )
            # Edit namelist file to contain dataset paths
            forstr = ""
            namelist_path = (
                self.additional_code.local_path
                + "/"
                + self.additional_code.namelists[0]
            )
            for f in datasets_to_partition:
                partitioned_path = (
                    os.path.dirname(f.local_path)
                    + "/PARTITIONED/"
                    + os.path.basename(f.local_path)
                )
                if isinstance(f, ModelGrid):
                    gridstr = "     " + partitioned_path + "\n"
                    _replace_text_in_file(
                        namelist_path, "__GRID_FILE_PLACEHOLDER__", gridstr
                    )
                elif isinstance(f, InitialConditions):
                    icstr = "     " + partitioned_path + "\n"
                    _replace_text_in_file(
                        namelist_path, "__INITIAL_CONDITION_FILE_PLACEHOLDER__", icstr
                    )
                elif type(f) in [SurfaceForcing, TidalForcing, BoundaryForcing]:
                    forstr += "     " + partitioned_path + "\n"

            _replace_text_in_file(
                namelist_path, "__FORCING_FILES_PLACEHOLDER__", forstr
            )

        ################################################################################
        ## NOTE: we assume that roms.in is the ONLY entry in additional_code.namelists, hence [0]
        _replace_text_in_file(
            self.additional_code.local_path + "/" + self.additional_code.namelists[0],
            "INPUT_DIR",
            self.additional_code.local_path + "/input_datasets/ROMS",
        )

        ##FIXME: it doesn't make any sense to have the next line in ROMSComponent, does it?
        _replace_text_in_file(
            self.additional_code.local_path + "/" + self.additional_code.namelists[0],
            "MARBL_NAMELIST_DIR",
            self.additional_code.local_path + "/namelists/MARBL",
        )

        ################################################################################

    def run(
        self,
        n_time_steps: Optional[int] = None,
        account_key: Optional[str] = None,
        walltime: Optional[str] = _CSTAR_SYSTEM_MAX_WALLTIME,
        job_name: str = "my_roms_run",
    ):
        """
        Runs the executable created by `build()`

        This method creates a temporary file to be submitted to the job scheduler (if any)
        on the calling machine, then submits it. By default the job requests the maximum
        walltime. It calculates the number of nodes and cores-per-node to request based on
        the number of cores required by the job, `ROMSComponent.n_procs_tot`.

        Parameters:
        -----------
        account_key: str, default None
            The users account key on the system
        walltime: str, default _CSTAR_SYSTEM_MAX_WALLTIME
            The requested length of the job, HH:MM:SS
        job_name: str, default 'my_roms_run'
            The name of the job submitted to the scheduler, which also sets the output file name
            `job_name.out`
        """
        if self.additional_code is None:
            print(
                "C-STAR: Unable to find AdditionalCode associated with this Component."
            )
            return
        elif self.additional_code.local_path is None:
            print(
                "C-STAR: Unable to find local copy of AdditionalCode. Run Component.get() first."
                + "\nIf you have already run Component.get(), either run it again or "
                + " add the local path manually using Component.additional_code.local_path='YOUR/PATH'."
            )
            return
        else:
            run_path = self.additional_code.local_path + "/output/PARTITIONED/"

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

        if self.additional_code.namelists is None:
            raise ValueError(
                "A namelist file (typically roms.in) is needed to run ROMS."
                " ROMSComponent.additional_code.namelists should be a non-empty list of filenames."
            )

        _replace_text_in_file(
            self.additional_code.local_path + "/" + self.additional_code.namelists[0],
            "__NTIMES_PLACEHOLDER__",
            str(n_time_steps),
        )

        os.makedirs(run_path, exist_ok=True)
        if self.exe_path is None:
            raise ValueError(
                "C-STAR: ROMSComponent.exe_path is None; unable to find ROMS executable."
                + "\nRun Component.build() first. "
                + "\n If you have already run Component.build(), either run it again or "
                + " add the executable path manually using Component.exe_path='YOUR/PATH'."
            )
        elif self.additional_code.namelists is None:
            raise ValueError(
                "C-STAR: This ROMS Component object's AdditionalCode"
                + " does not contain any namelists."
                + "Cannot run ROMS without a namelist file, e.g. roms.in"
            )
        else:
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
                f"{exec_pfx} -n {self.n_procs_tot} {self.exe_path} "
                + f"{self.additional_code.local_path}/{self.additional_code.namelists[0]}"
            )

            if _CSTAR_SYSTEM_CORES_PER_NODE is not None:
                nnodes, ncores = _calculate_node_distribution(
                    self.n_procs_tot, _CSTAR_SYSTEM_CORES_PER_NODE
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
                    with open(run_path + script_fname, "w") as f:
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
                    with open(run_path + script_fname, "w") as f:
                        f.write(scheduler_script)
                    subprocess.run(f"sbatch {script_fname}", shell=True, cwd=run_path)

                case None:
                    subprocess.run(roms_exec_cmd, shell=True, cwd=run_path)

    def post_run(self):
        """
        Performs post-processing steps associated with this ROMSComponent object.

        This method goes through any netcdf files produced by the model in
        `additional_code.local_path/output/PARTITIONED` and runs `ncjoin`,
        a ROMS program used to join netcdf files that are produced separately by each processor.
        The joined files are saved in
        `additional_code.local_path/output`

        Parameters:
        -----------
        local_path: str
            The path where this ROMS component is being assembled
        """

        out_path = self.additional_code.local_path + "/output/"
        # run_path='/Users/dafyddstephenson/Code/my_c_star/cstar/rme_case/output/'
        files = glob.glob(out_path + "PARTITIONED/*.0.nc")
        if not files:
            print("no suitable output found")
        else:
            for f in files:
                print(f)
                subprocess.run("ncjoin " + f[:-4] + "?.nc", cwd=out_path, shell=True)

