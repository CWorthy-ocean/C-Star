from collections import defaultdict
import os
import sys
import typing as t
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from importlib import import_module
from pathlib import Path

if t.TYPE_CHECKING:
    from types import ModuleType

GROUP_FF: t.Final[str] = "Feature Flags"
"""Group name for feature flag environment variables in documentation."""
GROUP_FS: t.Final[str] = "File System Configuration"
"""Group name for file system related environment variables in documentation."""
GROUP_SIM: t.Final[str] = "Simulation Configuration"
"""Group name for simulation-specific environment variables in documentation."""
GROUP_UNK: t.Final[str] = "Uncategorized Configuration"
"""Group name for uncategorized environment variables in documentation."""

FLAG_ON: t.Final[str] = "1"
"""Value indicating a toggle is enabled."""
FLAG_OFF: t.Final[str] = "0"
"""Value indicating a toggle is disabled."""

ENVVAR_PREFIX: t.Final[str] = "CSTAR_"
"""The common env var prefix that identifies a C-Star configuration setting."""
CONSTANT_PREFIX: t.Final[str] = "ENV_"
"""The common prefix used to name an annotated environment variable constant."""
NOT_SET: t.Final[str] = "<not-set>"
"""Default description and default for variables missing annotations."""


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
        if (env_value := os.getenv(self.name)) is not None:
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


def capture_environment() -> dict[str, str]:
    """Capture the C-Star-owned environment variables at the current time.

    Returns
    -------
    dict[str, str]
    """
    return {k: v for k, v in os.environ.items() if k.startswith(ENVVAR_PREFIX)}


def indirect_default_factory(env_var: EnvVar) -> str:
    """Retrieve the current value of the indirect variable.

    Return empty-string when the indirect variable is not populated.
    Returns
    -------
    str
    """
    var_name = env_var.indirect_var
    return os.environ.get(var_name, "")


def get_env_item(var_name: str) -> EnvItem:
    """Retrieve the metadata for an environment variable constant.

    Parameters:
    -----------
    var_name : str
        The string value of the environment variable (e.g. "CSTAR_CACHE_HOME")

    Returns:
    --------
    env_item: EnvItem
        The metadata associated with the environment variable
    """
    env_vars = discover_env_vars()
    if variable := env_vars.get(var_name, None):
        return variable

    msg = f"No environment variable metadata found for: {var_name}"
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


def generate_run_id() -> str:
    """Generate a unique run identifier based on the current time."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


ENV_CSTAR_LOG_LEVEL: t.Annotated[
    t.Literal["CSTAR_LOG_LEVEL"],
    EnvVar(
        "Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
        GROUP_SIM,
        default="INFO",
    ),
] = "CSTAR_LOG_LEVEL"
"""Specify the logging level for terminal messages. Options: DEBUG, INFO, WARNING, ERROR, CRITICAL."""


ENV_CSTAR_CLI_DRY_RUN: t.Annotated[
    t.Literal["CSTAR_CLI_DRY_RUN"],
    EnvVar(
        "Set to `1` to short-circuit CLI operations after planning steps are completed",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_CLI_DRY_RUN"
"""Set to `1` to short-circuit CLI operations after planning steps are completed."""


ENV_CSTAR_CLI_VERBOSE: t.Annotated[
    t.Literal["CSTAR_CLI_VERBOSE"],
    EnvVar(
        "Set to `1` to produce verbose CLI outputs.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_CLI_VERBOSE"
"""Set to `1` to produce verbose CLI outputs."""

ENV_CSTAR_CLOBBER_WORKING_DIR: t.Annotated[
    t.Literal["CSTAR_CLOBBER_WORKING_DIR"],
    EnvVar(
        "Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_CLOBBER_WORKING_DIR"
"""Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk."""

ENV_CSTAR_FRESH_CODEBASES: t.Annotated[
    t.Literal["CSTAR_FRESH_CODEBASES"],
    EnvVar(
        "Set to `1` to automatically clear codebase directories and create fresh clones during each run. Otherwise, use code found in locations specified in `ROMS_ROOT` and `ROMS_MARBL`.",
        GROUP_SIM,
        default=FLAG_OFF,
    ),
] = "CSTAR_FRESH_CODEBASES"
"""Set to `1` to automatically clear codebase directories and create fresh clones during each run. Otherwise, use code found in locations specified in `ROMS_ROOT` and `ROMS_MARBL`."""

ENV_CSTAR_IN_ACTIVE_ALLOCATION: t.Annotated[
    t.Literal["CSTAR_IN_ACTIVE_ALLOCATION"],
    EnvVar(
        "Override behavior for launching new jobs via SLURM or simply executing via mpirun. Only set this to 0 if you need to launch new jobs from within an existing allocation.",
        GROUP_SIM,
        default="",
    ),
] = "CSTAR_IN_ACTIVE_ALLOCATION"
"""Override behavior for launching new jobs via SLURM or simply executing via mpirun. Only set this to 0 if you need to launch new jobs from within an existing allocation."""

ENV_CSTAR_NPROCS_POST: t.Annotated[
    t.Literal["CSTAR_NPROCS_POST"],
    EnvVar(
        "Specify the number of processes to be used for post-processing simulation output files. Dynamic default ``os.cpu_count() // 3``",
        GROUP_SIM,
        default_factory=lambda _: nprocs_factory(),  # type: ignore[reportOptionalOperand]
    ),
] = "CSTAR_NPROCS_POST"
"""Specify the number of processes to be used for post-processing simulation output files."""

ENV_CSTAR_SCRATCH_DIRS: t.Annotated[
    t.Literal["CSTAR_SCRATCH_DIRS"],
    EnvVar(
        "A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order.",
        GROUP_FS,
        "SCRATCH,SCRATCH_DIR,LOCAL_SCRATCH",
    ),
] = "CSTAR_SCRATCH_DIRS"
"""A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order."""

ENV_CSTAR_CACHE_HOME: t.Annotated[
    t.Literal["CSTAR_CACHE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star file cache.",
        GROUP_FS,
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
        GROUP_FS,
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
        GROUP_FS,
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
        GROUP_FS,
        "~/.local/state",
        indirect_var="XDG_STATE_HOME",
        default_factory=indirect_default_factory,
    ),
] = "CSTAR_STATE_HOME"
"""Environment variable used to override the home directory for C-Star state storage."""

ENV_CSTAR_RUNID: t.Annotated[
    t.Literal["CSTAR_RUNID"],
    EnvVar(
        description="Unique run identifier used by the orchestrator.",
        group=GROUP_SIM,
        default_factory=lambda _: generate_run_id(),
    ),
] = "CSTAR_RUNID"
"""Environment variable containing a unique run identifier used by the orchestrator."""

ENV_CSTAR_SLURM_POST_SUBMIT_DELAY: t.Annotated[
    t.Literal["CSTAR_SLURM_POST_SUBMIT_DELAY"],
    EnvVar(
        "Delay (in seconds) after a submission to ensure status for a SLURM job can be queried.",
        GROUP_SIM,
        default="1.0",
    ),
] = "CSTAR_SLURM_POST_SUBMIT_DELAY"
"""Delay (in seconds) after a submission to ensure status for a SLURM job can be queried."""


@lru_cache
def discover_env_vars() -> dict[str, EnvItem]:
    """Return a mapping from env-var constant to the associated metadata."""
    unknown_meta: t.Final[EnvVar] = EnvVar(NOT_SET, GROUP_UNK, NOT_SET)
    container: dict[str, EnvItem] = {}
    modules: list[ModuleType] = [
        sys.modules[__name__],
        import_module("cstar.orchestration.utils"),
        import_module("cstar.base.feature"),
    ]

    for module in modules:
        hints = t.get_type_hints(module, include_extras=True)
        variables = (x for x in dir(module) if x.startswith(CONSTANT_PREFIX))

        for var_name in variables:
            actual = getattr(module, var_name)
            meta: EnvVar | None = None
            hint = hints.get(var_name, None)

            if hint and (metadata := getattr(hint, "__metadata__", None)):
                meta = next((x for x in metadata if x and isinstance(x, EnvVar)), None)

            if meta:
                container[actual] = EnvItem.from_env_var(meta, actual)
            if not meta and not actual in container:
                container[actual] = EnvItem.from_env_var(unknown_meta, actual)

    return container
