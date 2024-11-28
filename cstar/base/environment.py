import os
import shutil
import platform
import subprocess
import importlib.util
from pathlib import Path
from dotenv import dotenv_values
from typing import Optional, Dict


class CStarEnvironment:
    """Encapsulates the configuration and management of a computing environment for a
    specific system, including compilers, job schedulers, memory, and core settings.

    This class uses properties to avoid attribute modification after initialization.

    This class also provides utilities for interacting with Linux Environment Modules
    (Lmod) and dynamically setting up the environment based on user and system configurations.

    Attributes
    ----------
    system_name : str
        The name of the system (e.g., "expanse", "perlmutter").
    mpi_exec_prefix : str
        The prefix command used for launching MPI jobs.
    compiler : str
        The compiler to be used in the environment (e.g., "intel", "gnu").
    queue_flag : optional, str
        The flag used for specifying the queue in job submissions.
    primary_queue : optional, str
        The default queue for job submissions.
    mem_per_node_gb : optional, float
        Memory available per node in gigabytes.
    cores_per_node : optional, int
        Number of CPU cores available per node.
    max_walltime : optional, str
        The maximum walltime allowed for a job in this environment.
    other_scheduler_directives : optional, dict[str, str]
        Additional directives for the scheduler.
    environment_variables : dict
        A dictionary containing combined environment variables from system and user `.env` files.
    package_root : Path
        The root directory of the package containing configuration files and utilities.
    uses_lmod : bool
        Indicates whether the system uses Linux Environment Modules (Lmod).
    scheduler : Optional[str]
        The type of job scheduler detected on the system (e.g., "slurm", "pbs"), or None if not detected.

    Methods
    -------
    load_lmod_modules(lmod_file: str) -> None
        Loads the necessary Lmod modules for the current system based on a `.lmod` configuration file.

    _call_lmod(*args) -> None
        Executes a Linux Environment Modules command with specified arguments.

    Raises
    ------
    EnvironmentError
        Raised when required resources, modules, or configurations are missing or incompatible.
    RuntimeError
        Raised when a command or operation fails during execution.

    Examples
    --------
    >>> env = CStarEnvironment(
    ...     system_name="expanse",
    ...     mpi_exec_prefix="srun --mpi=pmi2",
    ...     compiler="intel",
    ...     queue_flag="partition",
    ...     primary_queue="compute",
    ...     mem_per_node_gb=256,
    ...     cores_per_node=128,
    ...     max_walltime="48:00:00",
    ...     other_scheduler_directives={},
    ... )
    >>> print(env)
    CStarEnvironment(...)
    """

    def __init__(
        self,
        system_name: str,
        mpi_exec_prefix: str,
        compiler: str,
        queue_flag: Optional[str],
        primary_queue: Optional[str],
        mem_per_node_gb: Optional[float],
        cores_per_node: Optional[int],
        max_walltime: Optional[str],
        other_scheduler_directives: Optional[Dict[str, str]],
    ):
        if other_scheduler_directives is None:
            other_scheduler_directives = {}

        # Initialize private attributes
        self._system_name = system_name
        self._mpi_exec_prefix = mpi_exec_prefix
        self._compiler = compiler
        self._queue_flag = queue_flag
        self._primary_queue = primary_queue
        self._mem_per_node_gb = mem_per_node_gb
        self._cores_per_node = cores_per_node
        self._max_walltime = max_walltime
        self._other_scheduler_directives = other_scheduler_directives

        if self.uses_lmod:
            self.load_lmod_modules(
                lmod_file=f"{self.package_root}/additional_files/lmod_lists/{self._system_name}.lmod"
            )
        os.environ.update(self.environment_variables)

    @property
    def mpi_exec_prefix(self):
        return self._mpi_exec_prefix

    @property
    def compiler(self):
        return self._compiler

    @property
    def queue_flag(self):
        return self._queue_flag

    @property
    def primary_queue(self):
        return self._primary_queue

    @property
    def mem_per_node_gb(self):
        return self._mem_per_node_gb

    @property
    def cores_per_node(self):
        return self._cores_per_node

    @property
    def max_walltime(self):
        return self._max_walltime

    @property
    def other_scheduler_directives(self):
        return self._other_scheduler_directives

    def __str__(self) -> str:
        """Provides a structured, readable summary of the environment's configuration.

        Returns
        -------
        str
            Human-readable string representation of the environment's key attributes.
        """

        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += f"\nScheduler: {self.scheduler or 'None'}"
        base_str += f"\nCompiler: {self.compiler}"
        base_str += f"\nPrimary Queue: {self.primary_queue or 'None'}"
        base_str += f"\nMPI Exec Prefix: {self.mpi_exec_prefix}"
        base_str += f"\nCores per Node: {self.cores_per_node}"
        base_str += f"\nMemory per Node (GB): {self.mem_per_node_gb}"
        base_str += f"\nMax Walltime: {self.max_walltime or 'Not specified'}"
        base_str += f"\nUses Lmod: {True if self.uses_lmod else False}"
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
            f"{self.__class__.__name__}("
            f"system_name={self._system_name!r}, "
            f"compiler={self.compiler!r}, "
            f"scheduler={self.scheduler!r}, "
            f"primary_queue={self.primary_queue!r}, "
            f"cores_per_node={self.cores_per_node!r}, "
            f"mem_per_node_gb={self.mem_per_node_gb!r}, "
            f"max_walltime={self.max_walltime!r}"
            ")\nState: <"
            f"uses_lmod={self.uses_lmod!r}"
            ">"
        )

    @property
    def environment_variables(self) -> dict:
        env_vars = dotenv_values(
            self.package_root / f"additional_files/env_files/{self._system_name}.env"
        )
        user_env_vars = dotenv_values(Path("~/.cstar.env").expanduser())
        env_vars.update(user_env_vars)
        return env_vars

    @property
    def package_root(self) -> Path:
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

        return (platform.system() == "Linux") and ("LMOD_CMD" in list(os.environ))

    def _call_lmod(self, *args) -> None:
        """Calls Linux Environment Modules with specified arguments in python mode.

        This method constructs and executes a command to interface with the Linux Environment
        Modules system (Lmod), equivalently to `module <args>` from the shell.
        The output of the command, which is Python code, is executed
        directly to modify the current process environment persistently. Errors during the
        command's execution are raised as exceptions.

        Parameters
        ----------
        *args : str
            Arguments for the Lmod command. For example, "reset", "load gcc", or "unload gcc".
            These are concatenated into a single command string and passed to Lmod.

        Raises
        ------
        EnvironmentError
            If Lmod is not available on the system or the `LMOD_CMD` environment variable is
            not set.
        RuntimeError
            If the Lmod command returns a non-zero exit code. The error message includes
            details about the command and the stderr output from Lmod.

        Examples
        --------
        Reset the environment managed by Lmod:

        >>> CStarEnvironment._call_lmod("reset")

        Load a module (e.g., gcc):

        >>> CStarEnvironment._call_lmod("load", "gcc")

        Unload a module (e.g., gcc):

        >>> CStarEnvironment._call_lmod("unload", "gcc")
        """

        lmod_path = Path(os.environ.get("LMOD_CMD", ""))
        command = f"{lmod_path} python {' '.join(list(args))}"
        lmod_result = subprocess.run(
            command, shell=True, text=True, capture_output=True
        )
        if lmod_result.returncode != 0:
            raise RuntimeError(
                "Linux Environment Modules command "
                + f"\n{command} "
                + f"\n failed with code {lmod_result.returncode}. STDERR: "
                + f"{lmod_result.stderr}"
            )
        else:
            exec(lmod_result.stdout)

    def load_lmod_modules(self, lmod_file) -> None:
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
        self._call_lmod("reset")
        with open(
            f"{self.package_root}/additional_files/lmod_lists/{self._system_name}.lmod"
        ) as F:
            lmod_list = F.readlines()
            for mod in lmod_list:
                self._call_lmod(f"load {mod}")

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
