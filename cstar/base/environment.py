import os
import shutil
import platform
import subprocess
import importlib.util
from abc import ABC, abstractmethod
from pathlib import Path
from dotenv import dotenv_values
from typing import Optional, Final, Dict


class CStarEnvironment(ABC):
    """Abstract base class for configuring C-Star environments, managing environment
    variables, system properties, and job scheduler configurations.

    Properties
    ----------
    environment_variables : dict
        Combined dictionary of system and user environment variables.
    system_name : str
        Name of the system, derived from environment variables or platform information.
    root : Path
        Root directory of the top-level package.
    uses_lmod : bool
        Indicates if Linux Environment Modules (Lmod) are used.
    compiler : str
        Compiler type used in the environment, implemented by subclasses.
    mpi_exec_prefix : str
        Command prefix for MPI execution, implemented by subclasses.
    scheduler : str or None
        Type of job scheduler detected on the system, such as 'slurm' or 'pbs'.
    queue_flag : str or None
        Scheduler-specific flag for specifying the job queue.
    primary_queue : str or None
        Primary job queue name.
    other_scheduler_directives : dict
        Additional directives for job scheduling.
    cores_per_node : int or None
        Number of CPU cores per node.
    mem_per_node_gb : int or None
        Memory available per node in GB.
    max_walltime : str or None
        Maximum allowed walltime for jobs in HH:MM:SS format.
    """

    def __init__(self):
        """Initializes the environment by loading any required Lmod modules and updating
        OS environment variables with both system and user configurations.

        - Loads Lmod modules if `uses_lmod` is True.
        - Sets environment variables by combining values from system and user `.env` files.
        """

        if self.uses_lmod:
            self.load_lmod_modules()
        os.environ.update(self.environment_variables)

    def __str__(self) -> str:
        """Provides a structured, readable summary of the environment's configuration.

        Returns
        -------
        str
            Human-readable string representation of the environment's key attributes.
        """
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += f"\nSystem Name: {self.system_name}"
        base_str += f"\nScheduler: {self.scheduler or 'None'}"
        base_str += f"\nCompiler: {self.compiler}"
        base_str += f"\nPrimary Queue: {self.primary_queue or 'None'}"
        base_str += f"\nMPI Exec Prefix: {self.mpi_exec_prefix}"
        base_str += f"\nCores per Node: {self.cores_per_node or 'Not specified'}"
        base_str += f"\nMemory per Node (GB): {self.mem_per_node_gb or 'Not specified'}"
        base_str += f"\nMax Walltime: {self.max_walltime or 'Not specified'}"
        base_str += f"\nUses Lmod: {'Yes' if self.uses_lmod else 'No'}"
        base_str += "\nEnvironment Variables:"
        for key, value in self.environment_variables.items():
            base_str += f"\n    {key}: {value}"
        return base_str

    def __repr__(self):
        """Provides a clear and structured representation of the environment, showing an
        empty initialization call and a separate state section with key properties.

        Returns
        -------
        str
            String representation distinguishing initialization from dynamic state.
        """
        return (
            f"{self.__class__.__name__}() "
            f"\nState: <system_name='{self.system_name}', "
            f"compiler='{self.compiler}', "
            f"scheduler='{self.scheduler}', "
            f"primary_queue='{self.primary_queue}', "
            f"cores_per_node={self.cores_per_node}, "
            f"mem_per_node_gb={self.mem_per_node_gb}, "
            f"max_walltime='{self.max_walltime}', "
            f"uses_lmod={self.uses_lmod}>"
        )

    @property
    def environment_variables(self) -> dict:
        """Loads environment variables from system-specific and user-defined `.env`
        files.

        Returns
        -------
        dict
            Dictionary containing environment variables, where system-specific values
            are loaded first, followed by user-specific overrides.

        Notes
        -----
        - System-specific variables are sourced from an `.env` file under
          `additional_files/env_files/` based on the system name.
        - User-specific variables are loaded from `~/.cstar.env`.
        """

        env_vars = dotenv_values(
            self.root / f"additional_files/env_files/{self.system_name}.env"
        )
        user_env_vars = dotenv_values(Path("~/.cstar.env").expanduser())
        env_vars.update(user_env_vars)
        return env_vars

    # System-level properties
    @property
    def system_name(self) -> str:
        """Determines the system name based on environment variables or platform
        details.

        Checks for Lmod-specific variables (`LMOD_SYSHOST` or `LMOD_SYSTEM_NAME`) if
        `uses_lmod` is True. Otherwise, constructs a system name using `platform.system()` and
        `platform.machine()`.

        Returns
        -------
        str
            The system's name in lowercase.

        Raises
        ------
        EnvironmentError
            If the system name cannot be determined from environment variables or platform information.
        """

        if self.uses_lmod:
            sysname = os.environ.get("LMOD_SYSHOST") or os.environ.get(
                "LMOD_SYSTEM_NAME"
            )
            if sysname is None:
                raise EnvironmentError(
                    "Your system appears to use Linux Environment Modules but C-Star cannot determine "
                    + "the system name as either 'LMOD_SYSHOST' or 'LMOD_SYSTEM_NAME' are not defined "
                    + "in your environment."
                )
        elif (platform.system() is not None) and (platform.machine() is not None):
            sysname = platform.system() + "_" + platform.machine()
        else:
            raise EnvironmentError(
                "C-Star cannot determine your system type using platform.system() and platform.machine()"
            )

        return sysname.casefold()

    @property
    def root(self) -> Path:
        """Identifies the root directory of the top-level package.

        Uses `importlib.util.find_spec` to locate the package directory, enabling
        access to additional configuration files within the package structure.

        Returns
        -------
        Path
            Path to the root directory of the top-level package.

        Raises
        ------
        ImportError
            If the top-level package cannot be located.
        """

        top_level_package_name = __name__.split(".")[0]
        spec = importlib.util.find_spec(top_level_package_name)
        if spec is not None:
            if isinstance(spec.submodule_search_locations, list):
                return Path(spec.submodule_search_locations[0])
        raise ImportError(f"Top-level package '{top_level_package_name}' not found.")

    # Environment management related
    @property
    def uses_lmod(self) -> bool:
        """Checks if the system uses Linux Environment Modules (Lmod) based on OS type
        and presence of `LMOD_DIR` in environment variables.

        Returns
        -------
        bool
            True if the OS is Linux and `LMOD_DIR` is present in environment variables.
        """

        return (platform.system() == "Linux") and ("LMOD_DIR" in list(os.environ))

    def load_lmod_modules(self) -> None:
        """Loads necessary modules for this machine using Linux Environment Modules.

        This function:
        - Resets the current module environment by executing `module reset`.
        - Loads each module listed in the `.lmod` file for the system, located at
          `<root>/additional_files/lmod_lists/<system_name>.lmod`.

        Raises
        ------
        EnvironmentError
            If the system does not use Lmod or `module reset` fails.
        RuntimeError
            If any `module load <module_name>` command fails.
        """

        if not self.uses_lmod:
            raise EnvironmentError(
                "Your system does not appear to use Linux Environment Modules"
            )

        reset_result = subprocess.run(
            "module reset", capture_output=True, shell=True, text=True
        )
        if reset_result.returncode != 0:
            raise RuntimeError(
                f"Error {reset_result.returncode} when attempting to run module reset. Error Messages: "
                + f"\n{reset_result.stderr}"
            )
        with open(
            f"{self.root}/additional_files/lmod_lists/{self.system_name}.lmod"
        ) as F:
            lmod_list = F.readlines()
            for mod in lmod_list:
                lmod_result = subprocess.run(
                    f"module load {mod}", capture_output=True, shell=True, text=True
                )
                if lmod_result.returncode != 0:
                    raise RuntimeError(
                        f"Error {lmod_result.returncode} when attempting to run module load {mod}. Error Messages: "
                        + f"\n{lmod_result.stderr}"
                    )

    @property
    @abstractmethod
    def compiler(self) -> str:
        """Abstract property representing the compiler used in the environment,
        implemented by subclasses.

        Returns
        -------
        str
            Compiler type (e.g., 'gnu', 'intel').
        """

        pass

    # Scheduler/MPI related
    @property
    @abstractmethod
    def mpi_exec_prefix(self) -> str:
        """Abstract property representing the prefix for MPI execution commands,
        implemented by subclasses.

        Returns
        -------
        str
            Prefix for MPI execution commands (e.g., 'mpirun', 'srun').
        """

        pass

    @property
    def scheduler(self) -> Optional[str]:
        """Detects the job scheduler type by checking commands for known schedulers like
        Slurm or PBS.

        Returns
        -------
        str or None
            Scheduler type (e.g., 'slurm', 'pbs') or None if no known scheduler is detected.
        """
        if shutil.which("sinfo") or shutil.which("scontrol"):
            return "slurm"
        elif shutil.which("qstat"):
            return "pbs"
        else:
            return None

    @property
    def queue_flag(self) -> Optional[str]:
        """Flag used by the scheduler for specifying queues in job submissions.

        Returns
        -------
        str or None
            Queue flag, or None if not applicable.
        """

        return None

    @property
    def primary_queue(self) -> Optional[str]:
        """Name of the primary job queue for scheduling jobs.

        Returns
        -------
        str or None
            Name of the primary queue, or None if not specified.
        """

        return None

    @property
    def other_scheduler_directives(self) -> Dict[str, str]:
        """Additional scheduler directives for job submissions, specific to the
        environment.

        Returns
        -------
        dict
            Dictionary of additional scheduler directives.
        """

        return {}

    @property
    def cores_per_node(self) -> Optional[int]:
        """Number of CPU cores per node in the environment.

        Returns
        -------
        int or None
            Number of CPU cores per node, or None if unspecified.
        """

        return None

    @property
    def mem_per_node_gb(self) -> Optional[int]:
        """Memory available per node in gigabytes.

        Returns
        -------
        int or None
            Memory per node in GB, or None if unspecified.
        """

        return None

    @property
    def max_walltime(self) -> Optional[str]:
        """Maximum allowed walltime for jobs in the environment.

        Returns
        -------
        str or None
            Maximum walltime in HH:MM:SS format, or None if unspecified.
        """

        return None


class PerlmutterEnvironment(CStarEnvironment):
    (
        """
    SDSC-Expanse-specific implementation of `CStarEnvironment` (doc below)

    Docstring for CStarEnvironment:
    -------------------------------
    """
    ) + (CStarEnvironment.__doc__ or "")

    # Perlmutter manages jobs with both QoS and partition.

    # The qos is what the user specifies to decide their "queue", and determines max walltime,
    # priority, charges, and node limits. IF UNSPECIFIED, system default is "debug" - restrictive!
    # https://docs.nersc.gov/jobs/policy/#perlmutter-cpu

    # The partition can be specified, but seems to be largely ignored. With "-C cpu",
    # the qos -> partition map seems to be:
    # shared -> shared_milan_ss11
    # regular -> regular_milan_ss11
    # debug -> regular_milan_ss11

    # Whether the user selects -C CPU determines memory per node:
    # https://docs.nersc.gov/jobs/#available-memory-for-applications-on-compute-nodes

    mpi_exec_prefix: Final[str] = "srun"
    compiler: Final[str] = "gnu"
    queue_flag: Final[str] = "qos"
    primary_queue: Final[str] = "regular"
    mem_per_node_gb: Final[int] = 512
    cores_per_node: Final[int] = 128  # for CPU nodes
    max_walltime: Final[str] = "24:00:00"

    @property
    def other_scheduler_directives(self) -> Dict[str, str]:
        """Additional scheduler directives specific to Perlmutter.

        Returns
        -------
        dict
            Dictionary of scheduler directives, e.g., {'-C': 'cpu'}.
        """
        return {"-C": "cpu"}


class ExpanseEnvironment(CStarEnvironment):
    (
        """
    SDSC-Expanse-specific implementation of `CStarEnvironment` (doc below)

    Docstring for CStarEnvironment:
    -------------------------------
    """
    ) + (CStarEnvironment.__doc__ or "")

    mpi_exec_prefix: Final[str] = "srun --mpi=pmi2"
    compiler: Final[str] = "intel"
    queue_flag: Final[str] = "partition"
    primary_queue: Final[str] = "compute"
    mem_per_node_gb: Final[int] = 256
    cores_per_node: Final[int] = 128
    max_walltime: Final[str] = "48:00:00"


class DerechoEnvironment(CStarEnvironment):
    (
        """
    NCAR-Derecho-specific implementation of `CStarEnvironment` (doc below)

    Docstring for CStarEnvironment:
    -------------------------------
    """
    ) + (CStarEnvironment.__doc__ or "")

    mpi_exec_prefix: Final[str] = "mpirun"
    compiler: Final[str] = "intel"
    queue_flag: Final[str] = "q"
    primary_queue: Final[str] = "main"
    cores_per_node: Final[int] = 128
    mem_per_node_gb: Final[int] = 256
    max_walltime: Final[str] = "12:00:00"


class MacOSARMEnvironment(CStarEnvironment):
    (
        """
    MacOS-ARM64-specific implementation of `CStarEnvironment` (doc below)

    Docstring for CStarEnvironment:
    -------------------------------
    """
    ) + (CStarEnvironment.__doc__ or "")

    mpi_exec_prefix: Final[str] = "mpirun"
    compiler: Final[str] = "gnu"

    @property
    def cores_per_node(self) -> int:
        cpu_count = os.cpu_count()
        if cpu_count is not None:
            return cpu_count
        else:
            raise EnvironmentError("unable to determine number of cpus")


class LinuxX86Environment(CStarEnvironment):
    (
        """
    Linux-X86-specific implementation of `CStarEnvironment` (doc below)

    Docstring for CStarEnvironment:
    -------------------------------
    """
    ) + (CStarEnvironment.__doc__ or "")

    mpi_exec_prefix: Final[str] = "mpirun"
    compiler: Final[str] = "gnu"

    @property
    def cores_per_node(self) -> int:
        cpu_count = os.cpu_count()
        if cpu_count is not None:
            return cpu_count
        else:
            raise EnvironmentError("unable to determine number of cpus")


# custom environment stuff here with dotenv


def set_environment() -> CStarEnvironment:
    """Factory function that detects and returns the appropriate environment based on
    system details.

    Returns
    -------
    CStarEnvironment
        An instance of the correct environment subclass.

    Raises
    ------
    EnvironmentError
        If the system is unsupported.
    """

    sysname = os.environ.get("LMOD_SYSHOST", default="") or os.environ.get(
        "LMOD_SYSTEM_NAME", default=""
    )
    match sysname:
        case "expanse":
            return ExpanseEnvironment()
        case "derecho":
            return DerechoEnvironment()
        case "perlmutter":
            return PerlmutterEnvironment()

    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return MacOSARMEnvironment()
    elif platform.system() == "Linux" and platform.machine() == "x86_64":
        return LinuxX86Environment()
    else:
        raise EnvironmentError("Unsupported environment")


environment = set_environment()
