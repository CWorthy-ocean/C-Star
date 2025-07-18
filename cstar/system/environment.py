import importlib.util
import os
import platform
from pathlib import Path

from dotenv import dotenv_values, load_dotenv, set_key

from cstar.base.utils import _run_cmd

CSTAR_USER_ENV_PATH = Path("~/.cstar.env").expanduser()


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
    uses_lmod: bool
        True if this system uses Linux Environment Modules for environment management

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
    ... )
    >>> print(env)
    CStarEnvironment(...)
    """

    def __init__(
        self,
        system_name: str,
        mpi_exec_prefix: str,
        compiler: str,
    ):
        # Initialize private attributes
        self._system_name = system_name
        self._mpi_exec_prefix = mpi_exec_prefix
        self._compiler = compiler

        if self.uses_lmod:
            self.load_lmod_modules(
                lmod_file=f"{self.package_root}/additional_files/lmod_lists/{self._system_name}.lmod"
            )
        os.environ.update(self.environment_variables)

    @property
    def mpi_exec_prefix(self) -> str:
        """Get the MPI prefix used when calling mpiexec in the environment."""
        return self._mpi_exec_prefix

    @property
    def compiler(self) -> str:
        """Get the compiler used when building in the environment."""
        return self._compiler

    def __str__(self) -> str:
        """Provides a structured, readable summary of the environment's configuration.

        Returns
        -------
        str
            Human-readable string representation of the environment's key attributes.
        """
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1)
        base_str += f"\nCompiler: {self.compiler}"
        base_str += f"\nMPI Exec Prefix: {self.mpi_exec_prefix}"
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
            f"compiler={self.compiler!r}"
            ")\nState: <"
            f"uses_lmod={self.uses_lmod!r}"
            ">"
        )

    @property
    def environment_variables(self) -> dict:
        env_vars = dotenv_values(
            self.package_root / f"additional_files/env_files/{self._system_name}.env"
        )
        user_env_vars = dotenv_values(CSTAR_USER_ENV_PATH)
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
        stdout = _run_cmd(
            command,
            msg_err=f"Linux Environment Modules command `{command.strip()}` failed.",
            raise_on_error=True,
        )

        exec(stdout)

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
            raise OSError(
                "Your system does not appear to use Linux Environment Modules"
            )
        self._call_lmod("reset")
        with open(
            f"{self.package_root}/additional_files/lmod_lists/{self._system_name}.lmod"
        ) as F:
            lmod_list = F.readlines()
            for mod in lmod_list:
                self._call_lmod(f"load {mod}")

    def set_env_var(self, key: str, value: str) -> None:
        """Set value of an environment variable and store it in the user environment
        file.

        Parameters
        ----------
        key : str
            The environment variable to set.
        value : str
            The value to set for the environment variable.
        """
        set_key(CSTAR_USER_ENV_PATH, key, value)
        load_dotenv(CSTAR_USER_ENV_PATH, override=True)
