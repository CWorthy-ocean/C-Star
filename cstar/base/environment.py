import io
import os
import platform
from pathlib import Path
from contextlib import contextmanager
import importlib.util
from abc import ABC, abstractmethod
from typing import Optional, Final, Dict
from contextlib import redirect_stdout, redirect_stderr
import subprocess
from dotenv import dotenv_values


class CStarEnvironment(ABC):
    """Base class for C-Star environment configurations."""

    def __init__(self):
        default_env_vars = dotenv_values(
            self.root / f"additional_files/env_files/{self.system_name}.env"
        )
        self.environment_variables: Dict[str, str] = default_env_vars
        user_env_vars = dotenv_values(Path("~/.cstar.env").expanduser())
        self.environment_variables.update(user_env_vars)

    # System-level properties
    @property
    def system_name(self) -> str:
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
        top_level_package_name = __name__.split(".")[0]
        spec = importlib.util.find_spec(top_level_package_name)
        if spec is not None:
            if isinstance(spec.submodule_search_locations, list):
                return Path(spec.submodule_search_locations[0])
        raise ImportError(f"Top-level package '{top_level_package_name}' not found.")

    # Environment management related
    @property
    def uses_lmod(self) -> bool:
        return (platform.system() == "Linux") and ("LMOD_DIR" in list(os.environ))

    def load_lmod_modules(self):
        if not self.uses_lmod:
            raise EnvironmentError(
                "Your system does not appear to use Linux Environment Modules"
            )
        # Dynamically load the env_modules_python module using pathlib
        module_path = (
            Path(os.environ["LMOD_DIR"]).parent / "init" / "env_modules_python.py"
        )
        spec = importlib.util.spec_from_file_location("env_modules_python", module_path)
        if (spec is None) or (spec.loader is None):
            raise EnvironmentError(
                f"Could not find env_modules_python on this machine at {module_path}"
            )
        env_modules = importlib.util.module_from_spec(spec)
        if env_modules is None:
            raise EnvironmentError(
                f"No module found by importlib corresponding to spec {spec}"
            )
        spec.loader.exec_module(env_modules)
        module = env_modules.module

        module_stdout = io.StringIO()
        module_stderr = io.StringIO()

        # Load Linux Environment Modules for this machine:
        with redirect_stdout(module_stdout), redirect_stderr(module_stderr):
            module("reset")
            with open(
                f"{self.root}/additional_files/lmod_lists/{self.system_name}.lmod"
            ) as F:
                lmod_list = F.readlines()
            for mod in lmod_list:
                module("load", mod)
        if any(
            keyword in module_stderr.getvalue().casefold()
            for keyword in ["fail", "error"]
        ):
            raise EnvironmentError(
                "Error with linux environment modules: " + module_stderr.getvalue()
            )

    @property
    @abstractmethod
    def compiler(self) -> str:
        pass

    # @property
    # @abstractmethod
    # def environment_variables(self) -> dict:
    #     pass

    # Scheduler/MPI related
    @property
    @abstractmethod
    def mpi_exec_prefix(self) -> str:
        pass

    @property
    def scheduler(self) -> Optional[str]:
        """Determine the scheduler type by querying system commands."""
        if subprocess.run("sinfo --version", shell=True, text=True).returncode == 0:
            return "slurm"
        elif (
            subprocess.run("scontrol show config", shell=True, text=True).returncode
            == 0
        ):
            return "slurm"
        elif subprocess.run("qstat --version", shell=True, text=True).returncode == 0:
            return "pbs"
        else:
            return None

    @property
    def queue_flag(self) -> Optional[str]:
        return None

    @property
    def primary_queue(self) -> Optional[str]:
        return None

    @property
    def other_scheduler_directives(self) -> Optional[dict]:
        return None

    @property
    def cores_per_node(self) -> Optional[int]:
        return None

    @property
    def mem_per_node_gb(self) -> Optional[int]:
        return None

    @property
    def max_walltime(self) -> Optional[str]:
        return None

    @contextmanager
    def temporary_os_environment(self):
        """Context manager to temporarily apply environment variables at the OS
        level."""
        original_env = os.environ.copy()
        try:
            if self.uses_lmod:
                self.load_lmod_modules()
            os.environ.update(self.environment_variables)
            yield
        finally:
            os.environ.clear()
            os.environ.update(original_env)


class PerlmutterEnvironment(CStarEnvironment):
    """Perlmutter manages jobs with both QoS and partition.

    The qos is what the user specifies to decide their "queue", and determines max walltime,
    priority, charges, and node limits. IF UNSPECIFIED, system default is "debug" - restrictive!
    https://docs.nersc.gov/jobs/policy/#perlmutter-cpu

    The partition can be specified, but seems to be largely ignored. With "-C cpu",
    the qos -> partition map seems to be:
    shared -> shared_milan_ss11
    regular -> regular_milan_ss11
    debug -> regular_milan_ss11

    Whether the user selects -C CPU determines memory per node:
    https://docs.nersc.gov/jobs/#available-memory-for-applications-on-compute-nodes
    """

    mpi_exec_prefix: Final[str] = "srun"
    compiler: Final[str] = "gnu"
    queue_flag: Final[str] = "qos"
    primary_queue: Final[str] = "regular"
    mem_per_node_gb: Final[int] = 512
    cores_per_node: Final[int] = 128  # for CPU nodes
    max_walltime: Final[str] = "24:00:00"

    @property
    def other_scheduler_directives(self) -> dict:
        return {"-C": "cpu"}


class ExpanseEnvironment(CStarEnvironment):
    mpi_exec_prefix: Final[str] = "srun --mpi=pmi2"
    compiler: Final[str] = "intel"
    queue_flag: Final[str] = "partition"
    primary_queue: Final[str] = "compute"
    mem_per_node_gb: Final[int] = 256
    cores_per_node: Final[int] = 128
    max_walltime: Final[str] = "48:00:00"


class DerechoEnvironment(CStarEnvironment):
    mpi_exec_prefix: Final[str] = "mpirun"
    compiler: Final[str] = "intel"
    queue_flag: Final[str] = "q"
    primary_queue: Final[str] = "main"
    cores_per_node: Final[int] = 128
    mem_per_node_gb: Final[int] = 256
    max_walltime: Final[str] = "12:00:00"


class MacOSARMEnvironment(CStarEnvironment):
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
    """Factory function to detect and return the appropriate environment."""
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
