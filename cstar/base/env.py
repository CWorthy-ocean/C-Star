import os
import sys
import types
import typing as t
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class EnvVar:
    """Annotation for specifying metadata about an environment variable."""

    description: str
    """Plain-text description of the setting."""
    group: str
    """A group name used to identify the variable use."""
    default: str = ""
    """The default value for the setting."""
    default_factory: t.Callable[["EnvVar"], str | None] | None = None
    """A function used at run-time to generate the default value."""
    indirect_var: str = ""
    """An environment variable name to be used when the primary variable is not set."""


@dataclass(slots=True)
class EnvItem(EnvVar):
    """Runtime wrapper for an `EnvVar` that determines the actual value."""

    name: str = ""
    """The standard environment variable name used for the setting."""

    @property
    def value(self) -> str:
        if env_value := os.getenv(self.name, ""):
            return env_value

        if self.default_factory and (factory_default := self.default_factory(self)):
            return factory_default

        if self.indirect_var and (indirect_value := os.getenv(self.indirect_var, "")):
            return indirect_value

        return self.default

    @classmethod
    def from_env_var(cls, env_var: EnvVar, name: str) -> "EnvItem":
        return EnvItem(
            env_var.description,
            env_var.group,
            env_var.default,
            env_var.default_factory,
            env_var.indirect_var,
            name,
        )


def indirect_default_factory(env_var: EnvVar) -> str:
    """Retrieve the current value of the indirect variable.

    Return empty-string when the indirect variable is not populated.
    Returns
    -------
    str
    """
    var_name = env_var.indirect_var
    return os.environ.get(var_name, "")


_GROUP_FS: t.Final[str] = "File System Configuration"
_GROUP_SIM: t.Final[str] = "Simulation Configuration"
_GROUP_UNK: t.Final[str] = "Uncategorized Configuration"

FLAG_ON: t.Final[str] = "1"
"""Value indicating a feature flag is enabled."""

FLAG_OFF: t.Final[str] = "0"
"""Value indicating a feature flag is disabled."""


def get_env_item(var_name: str, prefix: str = "ENV_") -> EnvItem:
    """Retrieve the metadata for an environment variable constant.

    Parameters:
    -----------
    var_name: str
        The string value of the environment variable (e.g. "CSTAR_CACHE_HOME")

    Returns:
    --------
    env_item: EnvItem
        The metadata associated with the environment variable
    """
    hints = t.get_type_hints(sys.modules[__name__], include_extras=True)

    constant_name = f"{prefix}{var_name}"
    if hint := hints.get(constant_name, None):
        metadata = getattr(hint, "__metadata__", None)
        if not metadata:
            return EnvItem(
                description="unknown",
                group=_GROUP_UNK,
                default="unknown",
                name=var_name,
            )

        meta = metadata[0]
        if isinstance(meta, EnvVar):
            return EnvItem.from_env_var(meta, var_name)

    msg = f"No environment variable metadata found for: {constant_name}"
    raise ValueError(msg)


def hpc_data_directory() -> str | None:
    """A path-locator function that looks for standard scratch file-systems.

    Returns
    -------
    Path | None
        If a scratch file system is identified, return it's paty, otherwise return None.
    """
    scratch_variables = get_env_item(ENV_CSTAR_SCRATCH_DIRS).value.split(",")

    for env_var in scratch_variables:
        if scratch_path := os.getenv(env_var, ""):
            return Path(scratch_path).as_posix()

    return None


def nprocs_factory() -> str:
    """Return the number of processors on the current machine, divided by 3."""
    return str((os.cpu_count() or 3) // 3)


ENV_CSTAR_LOG_LEVEL: t.Annotated[
    t.Literal["CSTAR_LOG_LEVEL"],
    EnvVar(
        "Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
        _GROUP_SIM,
        default="INFO",
    ),
] = "CSTAR_LOG_LEVEL"
"""Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL."""


ENV_CSTAR_CLOBBER_WORKING_DIR: t.Annotated[
    t.Literal["CSTAR_CLOBBER_WORKING_DIR"],
    EnvVar(
        "Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk.",
        _GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_CLOBBER_WORKING_DIR"
""""Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk."""

ENV_CSTAR_FRESH_CODEBASES: t.Annotated[
    t.Literal["CSTAR_FRESH_CODEBASES"],
    EnvVar(
        "Set to `1` to automatically clear codebase directories and create fresh clones during each run. Otherwise, use code found in locations specified in `ROMS_ROOT` and `ROMS_MARBL`.",
        _GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_FRESH_CODEBASES"
"""Set to `1` to automatically clear codebase directories and create fresh clones during each run. Otherwise, use code found in locations specified in `ROMS_ROOT` and `ROMS_MARBL`."""

ENV_CSTAR_IN_ACTIVE_ALLOCATION: t.Annotated[
    t.Literal["CSTAR_IN_ACTIVE_ALLOCATION"],
    EnvVar(
        "Override behavior for launching new jobs via SLURM or simply executing via mpirun. Only set this to 0 if you need to launch new jobs from within an existing allocation.",
        _GROUP_SIM,
        default="",
    ),
] = "CSTAR_IN_ACTIVE_ALLOCATION"
"""""Override behavior for launching new jobs via SLURM or simply executing via mpirun. Only set this to 0 if you need to launch new jobs from within an existing allocation."""

ENV_CSTAR_NPROCS_POST: t.Annotated[
    t.Literal["CSTAR_NPROCS_POST"],
    EnvVar(
        "Specify the number of processes to be used for post-processing simulation output files. Dynamic default ``os.cpu_count() // 3``",
        _GROUP_SIM,
        default_factory=lambda _: nprocs_factory(),  # type: ignore[reportOptionalOperand]
    ),
] = "CSTAR_NPROCS_POST"
"""Specify the number of processes to be used for post-processing simulation output files."""

ENV_CSTAR_SCRATCH_DIRS: t.Annotated[
    t.Literal["CSTAR_SCRATCH_DIRS"],
    EnvVar(
        "A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order.",
        _GROUP_FS,
        "SCRATCH,SCRATCH_DIR,LOCAL_SCRATCH",
    ),
] = "CSTAR_SCRATCH_DIRS"
"""A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order."""

ENV_CSTAR_CACHE_HOME: t.Annotated[
    t.Literal["CSTAR_CACHE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star file cache.",
        _GROUP_FS,
        "~/.cache",
        indirect_var="XDG_CACHE_HOME",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_CACHE_HOME"
"""Environment variable used to override the home directory for C-Star file cache."""

ENV_CSTAR_CONFIG_HOME: t.Annotated[
    t.Literal["CSTAR_CONFIG_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star config storage.",
        _GROUP_FS,
        "~/.config",
        default_factory=indirect_default_factory,
        indirect_var="XDG_CONFIG_HOME",
    ),
] = "CSTAR_CONFIG_HOME"
"""Environment variable used to override the home directory for C-Star config storage."""

ENV_CSTAR_DATA_HOME: t.Annotated[
    t.Literal["CSTAR_DATA_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star dataset storage.",
        _GROUP_FS,
        "~/.local/share",
        indirect_var="XDG_DATA_HOME",
        default_factory=lambda x: hpc_data_directory() or indirect_default_factory(x),
    ),
] = "CSTAR_DATA_HOME"
"""Environment variable used to override the home directory for C-Star dataset storage."""

ENV_CSTAR_STATE_HOME: t.Annotated[
    t.Literal["CSTAR_STATE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star state storage.",
        _GROUP_FS,
        "~/.local/state",
        indirect_var="XDG_STATE_HOME",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_STATE_HOME"
"""Environment variable used to override the home directory for C-Star state storage."""


def discover_env_vars(
    modules: list[types.ModuleType],
    prefix: str = "ENV_",
) -> list[EnvItem]:
    """Locate all constants in a module that represent environment variables."""
    items: list[EnvItem] = []
    for module in modules:
        hints = t.get_type_hints(module, include_extras=True)

        for name, hint in hints.items():
            if name.startswith(prefix):
                metadata = getattr(hint, "__metadata__", None)
                name = name.replace(prefix, "")
                if metadata and isinstance(metadata[0], EnvVar):
                    meta = metadata[0]
                    items.append(EnvItem.from_env_var(meta, name))
                elif not metadata:
                    items.append(
                        EnvItem(
                            description="unknown",
                            group=_GROUP_UNK,
                            default="unknown",
                            name=name,
                        ),
                    )

    return items
