import importlib.util
import os
import platform
from pathlib import Path
from typing import ClassVar, Final

from dotenv import dotenv_values
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from cstar.base.utils import _run_cmd


class EnvSettingsBase(BaseSettings):
    """Base class for environment settings - requires no settings."""

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="CSTAR_",
        str_strip_whitespace=True,
        validate_default=True,
    )
    """Configuration altering global model behaviors."""

    @property
    def is_match(self) -> bool:
        """Return `True` if the current system is identified after inspecting
        non-lmod environment variables.
        """
        return False


class LmodEnvSettings(BaseSettings):
    """Environment configuration used by the LMOD system."""

    CMD: str = Field(default="", frozen=True)
    """The LMOD executable command."""
    DIR: str = Field(default="", frozen=True)
    """The LMOD lib directory."""
    PKG: str = Field(default="", frozen=True)
    """The path to the lmod package directory."""
    ROOT: str = Field(default="", frozen=True)
    """The path to the lmod install root directory."""
    SYSHOST: str = Field(default="", frozen=True)
    """The LMOD system host."""
    SYSTEM_DEFAULT_MODULES: str = Field(default="", frozen=True)
    """A colon-delimited list of modules automatically loaded by the system."""
    SYSTEM_NAME: str = Field(default="", frozen=True)
    """The LMOD system name."""
    VERSION: str = Field(default="", frozen=True)
    """The LMOD version."""

    no_default_modules: ClassVar[str] = "__NO_SYSTEM_DEFAULT_MODULES__"

    @classmethod
    def variable(cls, name: str) -> str:
        """Return an aliased/prefixed variable name."""
        return get_envfield_alias(LmodEnvSettings, name)

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="LMOD_",
        str_strip_whitespace=True,
    )
    """Configuration altering global model behaviors."""


class SlurmSettingsBase(EnvSettingsBase):
    """Base class configuring C-Star standard configuration management options."""

    SLURM_ACCOUNT: str = Field(default="", frozen=True, min_length=1)
    """The SLURM account name."""
    SLURM_QUEUE: str = Field(default="", frozen=True, min_length=1)
    """The SLURM queue name."""
    SLURM_MAX_WALLTIME: str = Field(default="48:00:00", frozen=True)
    """The SLURM queue name."""


def get_envfield_alias(
    klass: type[EnvSettingsBase | LmodEnvSettings], field_name: str
) -> str:
    """Retrieve the environment variable name for a given field.

    Parameters
    ----------
    klass : type[BaseSettings]
        A settings class that contains the field.
    field_name : str
        The name of the model field.

    Returns
    -------
    str
    """
    field = klass.model_fields.get(field_name)
    if field and isinstance(field.validation_alias, str):
        return field.validation_alias

    # Fallback logic if validation_alias isn't set: combine prefix and field name
    prefix = klass.model_config.get("env_prefix", "")
    return f"{prefix}{field_name}"


class CStarEnvironment:
    """Encapsulates the configuration and management of a computing environment for a
    specific system, including compilers, job schedulers, memory, and core settings.

    This class uses properties to avoid attribute modification after initialization.

    This class also provides utilities for interacting with Linux Environment Modules
    (Lmod) and dynamically setting up the environment based on user and system configurations.

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

    _system_name: Final[str]
    """The name of the system (e.g., "expanse", "perlmutter")."""
    _mpi_exec_prefix: Final[str]
    """The command prefix used for launching MPI jobs."""
    _compiler: Final[str]
    """The compiler to be used in the environment (e.g., "intel", "gnu")."""
    _package_root: Path | None = None
    """The root directory of the installation of the C-Star package."""
    _lmod_settings: Final[LmodEnvSettings]
    """Environment variables used by the LMOD system."""
    _system_settings_klass: Final[type[EnvSettingsBase] | None]
    """Environment variables used to configure jobs on the system."""

    def __init__(
        self,
        system_name: str,
        mpi_exec_prefix: str,
        compiler: str,
        system_settings_klass: type[EnvSettingsBase] | None = None,
    ):
        """Initialize the instance.

        Parameters
        ----------
        system_name : str
            The name of the hosting platform
        mpi_exec_prefix : str
            The MPI prefix
        compiler : str
            The compiler to use for builds
        """
        self._system_name = system_name
        self._mpi_exec_prefix = mpi_exec_prefix
        self._compiler = compiler

        # Load modules FIRST (if using lmod), then load env file
        # This ensures module-set variables (e.g., CRAY_NETCDF_PREFIX) are available
        # when env file variables are expanded
        self._lmod_settings = LmodEnvSettings()
        if self.uses_lmod:
            self.load_lmod_modules()

        self._system_settings_klass = system_settings_klass

        # Load env file AFTER modules so variables can be expanded
        self._env_vars = self._load_env()

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
        base_str += f"\nUses Lmod: {self.uses_lmod}"
        base_str += "\nEnvironment Variables:"
        for key, value in self._env_vars.items():
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

    def _load_env(self) -> dict[str, str]:
        """Load environment variables from system and user .env files into memory.

        Returns
        -------
        dict[str, str]
            The variables that were loaded
        """
        env_vars = dotenv_values(self.system_env_path)

        env_vars = {k: v for k, v in env_vars.items() if v is not None}

        # Expand shell variables (e.g., ${CRAY_NETCDF_PREFIX}) using os.path.expandvars
        # This allows env file to reference variables set by modules (which are loaded first)
        expanded_vars = {}
        for key, value in env_vars.items():
            # Use os.path.expandvars to expand ${VAR} and $VAR syntax
            expanded_value = os.path.expandvars(value) if value else ""
            expanded_vars[key] = expanded_value

        os.environ.update(expanded_vars)

        return expanded_vars

    @property
    def environment_variables(self) -> dict[str, str]:
        """Return the environment variables that were loaded from .env files.

        Returns
        -------
        dict[str, str]
            The key-value pairs that have been loaded.
        """
        return self._env_vars.copy()

    @property
    def settings_klass(self) -> type[EnvSettingsBase] | None:
        """Return the type that will load and validate the system settings."""
        return self._system_settings_klass

    @classmethod
    def _find_package_root(cls) -> Path:
        """Determine the root directory containing the source for this package.

        Uses `importlib.util.find_spec` to locate the package directory, enabling
        access to additional configuration files within the package structure.

        Returns
        -------
        Path
            The path to the source code directory.

        Raises
        ------
        ImportError
            When the package root directory cannot be identified.
        """
        top_level_package_name = __name__.split(".")[0]
        spec = importlib.util.find_spec(top_level_package_name)
        if spec is not None and isinstance(spec.submodule_search_locations, list):
            return Path(spec.submodule_search_locations[0])

        raise ImportError(f"Top-level package '{top_level_package_name}' not found.")

    @property
    def package_root(self) -> Path:
        """Return the root directory of the top-level package.

        Returns
        -------
        Path
            Path to the root directory of the top-level package.

        Raises
        ------
        ImportError
            If the top-level package cannot be located.
        """
        if self._package_root is None:
            self._package_root = self._find_package_root()
        return self._package_root

    @property
    def uses_lmod(self) -> bool:
        """Checks if the system uses Linux Environment Modules (Lmod) based on OS type
        and presence of `LMOD_DIR` in environment variables.

        Returns
        -------
        bool
            True if the OS is Linux and `LMOD_DIR` is present in environment variables.
        """
        return platform.system() == "Linux" and bool(self._lmod_settings.CMD)

    @property
    def system_env_path(self) -> Path:
        """Identify the expected path to a .env file for the current system.

        Returns
        -------
        Path
            The path to the `.env` file.
        """
        pkg_relative_path = f"additional_files/env_files/{self._system_name}.env"
        return self.package_root / pkg_relative_path

    @property
    def template_root(self) -> Path:
        """The root directory containing CStar templates.

        Returns
        -------
        Path
        """
        pkg_relative_path = "additional_files/templates"
        return self.package_root / pkg_relative_path

    @property
    def lmod_path(self) -> Path:
        """Identify the expected path to a .lmod file for the current system.

        Returns
        -------
        Path
            The complete path to the `.lmod` file.
        """
        pkg_relative_path = f"additional_files/lmod_lists/{self._system_name}.lmod"
        return self.package_root / pkg_relative_path

    def _call_lmod(self, *args: str) -> None:
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
            raise OSError(
                "Your system does not appear to use Linux Environment Modules"
            )

        if not self._lmod_settings.SYSTEM_DEFAULT_MODULES:
            var_name = LmodEnvSettings.variable("SYSTEM_DEFAULT_MODULES")
            os.environ[var_name] = LmodEnvSettings.no_default_modules

        self._call_lmod("reset")

        with open(self.lmod_path) as fp:
            lmod_list = fp.readlines()

        modules = " ".join(x.strip() for x in lmod_list)
        self._call_lmod(f"load {modules}")

    @staticmethod
    def set_env_var(key: str, value: str) -> None:
        """Set value of an environment variable.

        TODO: Remove unless new functionality is needed here.

        Note: after removing the persisted user_env file, this method seems silly. Leaving it for the moment,
        just so we don't have to update everywhere that uses it, and because we may want some other behavior
        around config files or logging to be happening here in the future.

        Parameters
        ----------
        key : str
            The environment variable to set.
        value : str
            The value to set for the environment variable.
        """
        os.environ[key] = value
