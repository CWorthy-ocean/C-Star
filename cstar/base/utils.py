import datetime as dt
import functools
import hashlib
import os
import re
import subprocess
import sys
import typing as t
from os import PathLike
from pathlib import Path

import dateutil
from attr import dataclass

from cstar.base.log import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class EnvVar:
    """Annotation for specifying metadata about an environment variable."""

    description: str
    """Plain-text description of the setting."""
    group: str
    """A group name used to identify the variable use."""
    default: str = ""
    """The default value for the setting."""
    default_factory: t.Callable[[], str | None] | None = None
    """A function used at run-time to generate the default value."""


@dataclass(slots=True)
class EnvItem(EnvVar):
    """Metadata specifying how to select a specific XDG-compliant setting."""

    name: str = ""
    """The standard environment variable name used for the setting."""

    @property
    def value(self) -> str:
        if self.default_factory is None:
            return self.default

        if factory_default := self.default_factory():
            return factory_default

        return ""


_GROUP_FS: t.Final[str] = "File System Configuration"
_GROUP_FF: t.Final[str] = "Feature Flags"
_GROUP_SIM: t.Final[str] = "Simulation Configuration"

FF_OFF: t.Final[str] = "0"
FF_ON: t.Final[str] = "1"


DEFAULT_OUTPUT_ROOT_NAME: t.Literal["output"] = "output"
"""A fixed `output_root_name` to be used when generating outputs with ROMS."""

SCRATCH_DIRS: t.Final[list[str]] = ["SCRATCH", "SCRATCH_DIR", "LOCAL_SCRATCH"]
"""Common env var names identifying scratch paths on HPC systems, in order of precedence."""


@functools.lru_cache
def get_env_item(var_name: str) -> EnvItem:
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
    for name, hint in hints.items():
        if name.startswith("ENV_") and getattr(sys.modules[__name__], name) == var_name:
            metadata = getattr(hint, "__metadata__", None)
            if metadata and isinstance(metadata[0], EnvVar):
                meta: EnvVar = metadata[0]

                return EnvItem(
                    description=meta.description,
                    group=meta.group,
                    default=meta.default,
                    default_factory=meta.default_factory,
                    name=var_name,
                )

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


ENV_CSTAR_CLOBBER_WORKING_DIR: t.Annotated[
    t.Literal["CSTAR_CLOBBER_WORKING_DIR"],
    EnvVar(
        "Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk.",
        _GROUP_SIM,
        default=FF_OFF,
    ),
] = "CSTAR_CLOBBER_WORKING_DIR"
""""Set to `1` to automatically clear the working directory specified in a blueprint before launching a SLURM job. Use at your own risk."""

ENV_CSTAR_FRESH_CODEBASES: t.Annotated[
    t.Literal["CSTAR_FRESH_CODEBASES"],
    EnvVar(
        "Set to `1` to automatically clear codebase directories and create fresh clones during each run. Otherwise, use code found in locations specified in `ROMS_ROOT` and `ROMS_MARBL`.",
        _GROUP_SIM,
        default=FF_OFF,
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
        default_factory=nprocs_factory,  # type: ignore[reportOptionalOperand]
    ),
] = "CSTAR_NPROCS_POST"
"""Specify the number of processes to be used for post-processing simulation output files."""

ENV_CSTAR_SCRATCH_DIRS: t.Annotated[
    t.Literal["CSTAR_SCRATCH_VARS"],
    EnvVar(
        "A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order.",
        _GROUP_FS,
        "SCRATCH,SCRATCH_DIR,LOCAL_SCRATCH",
    ),
] = "CSTAR_SCRATCH_VARS"
"""A comma-separated list of environment variable names used to identify scratch paths on HPC systems, in search order."""

ENV_CSTAR_CACHE_HOME: t.Annotated[
    t.Literal["CSTAR_CACHE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star file cache.",
        _GROUP_FS,
        "~/.cache",
    ),
] = "CSTAR_CACHE_HOME"
"""Environment variable used to override the home directory for C-Star file cache."""

ENV_CSTAR_CONFIG_HOME: t.Annotated[
    t.Literal["CSTAR_CONFIG_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star config storage.",
        _GROUP_FS,
        "~/.config",
    ),
] = "CSTAR_CONFIG_HOME"
"""Environment variable used to override the home directory for C-Star config storage."""

ENV_CSTAR_DATA_HOME: t.Annotated[
    t.Literal["CSTAR_DATA_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star dataset storage.",
        _GROUP_FS,
        "~/.local/share",
        default_factory=hpc_data_directory,
    ),
] = "CSTAR_DATA_HOME"
"""Environment variable used to override the home directory for C-Star dataset storage."""

ENV_CSTAR_STATE_HOME: t.Annotated[
    t.Literal["CSTAR_STATE_HOME"],
    EnvVar(
        "Environment variable used to override the home directory for C-Star state storage.",
        _GROUP_FS,
        "~/.local/state",
    ),
] = "CSTAR_STATE_HOME"
"""Environment variable used to override the home directory for C-Star state storage."""


ENV_FF_DEVELOPER_MODE: t.Annotated[
    t.Literal["CSTAR_FF_DEVELOPER_MODE"],
    EnvVar(
        "Enable developer mode to enable all feature flags (not recommended).",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_DEVELOPER_MODE"
"""Enable developer mode to enable all feature flags (not recommended)."""

ENV_FF_CLI_ENV_SHOW: t.Annotated[
    t.Literal["CSTAR_FF_CLI_ENV_SHOW"],
    EnvVar(
        "Enable CLI for displaying environment configuration.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_ENV_SHOW"
"""Enable CLI for displaying environment configuration."""

ENV_FF_CLI_TEMPLATE_CREATE: t.Annotated[
    t.Literal["CSTAR_FF_CLI_TEMPLATE_CREATE"],
    EnvVar(
        "Enable CLI for creating blueprints and workplans from standard templates.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_TEMPLATE_CREATE"
"""Enable CLI for creating blueprints and workplans from standard templates."""

ENV_FF_CLI_WORKPLAN_COMPOSE: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_COMPOSE"],
    EnvVar(
        "Enable CLI for composing a workplan from pre-existing blueprints.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_COMPOSE"
"""Enable CLI for composing a workplan from pre-existing blueprints."""

ENV_FF_CLI_WORKPLAN_GEN: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_GEN"],
    EnvVar(
        "Enable CLI for generating a workplan from a directory of blueprints.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_GEN"
"""Enable CLI for generating a workplan from a directory of blueprints."""

ENV_FF_CLI_WORKPLAN_PLAN: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_PLAN"],
    EnvVar(
        "Enable CLI for generating the execution plan of a workplan.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_PLAN"
"""Enable CLI for generating the execution plan of a workplan."""

ENV_FF_CLI_WORKPLAN_STATUS: t.Annotated[
    t.Literal["CSTAR_FF_CLI_WORKPLAN_STATUS"],
    EnvVar(
        "Enable CLI for retrieving status about a workplan run.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_CLI_WORKPLAN_STATUS"
"""Enable CLI for retrieving status about a workplan run."""

ENV_FF_ORCH_TRX_TIMESPLIT: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_TIMESPLIT"],
    EnvVar(
        "Enable automatic time-splitting of simulations.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_TIMESPLIT"
"""Enable automatic time-splitting of simulations."""

ENV_FF_ORCH_TRX_OVERRIDE: t.Annotated[
    t.Literal["CSTAR_FF_ORCH_TRX_OVERRIDE"],
    EnvVar(
        "Enable automatic overrides to blueprints contained in a workplan.",
        _GROUP_FF,
        default=FF_OFF,
    ),
] = "CSTAR_FF_ORCH_TRX_OVERRIDE"
"""Enable automatic overrides to blueprints contained in a workplan."""


def coerce_datetime(datetime: str | dt.datetime) -> dt.datetime:
    """Coerces datetime-like input to a datetime instance.

    Parameters
    ----------
    datetime : str | datetime
       The value to be coerced into a datetime.

    Returns
    -------
    datetime
    """
    if isinstance(datetime, dt.datetime):
        return datetime
    else:
        return dateutil.parser.parse(datetime)


def _get_sha256_hash(file_path: str | Path) -> str:
    """Calculate the 256-bit SHA checksum of a file.

    Parameters
    ----------
    file_path: Path
       Path to the file whose checksum is to be calculated

    Returns
    -------
    file_hash: str
       The SHA-256 checksum of the file at file_path
    """
    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(
            f"Error when calculating file hash: {file_path} is not a valid file"
        )

    sha256_hash = hashlib.sha256()
    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            sha256_hash.update(chunk)

    file_hash = sha256_hash.hexdigest()
    return file_hash


def _replace_text_in_file(file_path: str | Path, old_text: str, new_text: str) -> bool:
    """Find and replace a string in a text file.

    This function creates a temporary file where the changes are written, then
    overwrites the original file.

    Parameters:
    -----------
    file_path: str | Path
        The local path to the text file
    old_text: str
        The text to be replaced
    new_text: str
        The text that will replace `old_text`

    Returns:
    --------
    text_replaced: bool
       True if text was found and replaced, False if not found
    """
    text_replaced = False
    file_path = Path(file_path).resolve()
    temp_file_path = Path(str(file_path) + ".tmp")

    with open(file_path) as read_file, open(temp_file_path, "w") as write_file:
        for line in read_file:
            if old_text in line:
                text_replaced = True
            new_line = line.replace(old_text, new_text)
            write_file.write(new_line)

    temp_file_path.rename(file_path)

    return text_replaced


def _list_to_concise_str(
    input_list, item_threshold=4, pad=16, items_are_strs=True, show_item_count=True
):
    """Take a list and return a concise string representation of it.

    Parameters:
    -----------
    input_list (list of str):
       The list of to be represented
    item_threshold (int, default = 4):
       The number of items beyond which to truncate the str to item0,...itemN
    pad (int, default = 16):
       The number of whitespace characters to prepend newlines with
    items_are_strs (bool, default = True):
       Will use repr formatting ([item1,item2]->['item1','item2']) for lists of strings
    show_item_count (bool, default = True):
       Will add <N items> to the end of a truncated representation

    Returns:
    -------
    list_str: str
       The string representation of the list

    Examples:
    --------
    In: print("my_list: "+_list_to_concise_str(["myitem0","myitem1",
                             "myitem2","myitem3","myitem4"],pad=11))
    my_list: ['myitem0',
              'myitem1',
                  ...
              'myitem4']<5 items>
    """
    list_str = ""
    pad_str = " " * pad
    if show_item_count:
        count_str = f"<{len(input_list)} items>"
    else:
        count_str = ""
    if len(input_list) > item_threshold:
        list_str += f"[{repr(input_list[0]) if items_are_strs else input_list[0]},"
        list_str += (
            f"\n{pad_str}{repr(input_list[1]) if items_are_strs else input_list[1]},"
        )
        list_str += f"\n{pad_str}   ..."
        list_str += f"\n{pad_str}{repr(input_list[-1]) if items_are_strs else input_list[-1]}] {count_str}"
    else:
        list_str += "["
        list_str += f",\n{pad_str}".join(
            (repr(listitem) if items_are_strs else listitem) for listitem in input_list
        )
        list_str += "]"
    return list_str


def _dict_to_tree(input_dict: dict, prefix: str = "") -> str:
    """Recursively converts a dictionary into a tree-like string representation.

    Parameters:
    -----------
     input_dict (dict):
        The dictionary to convert. Takes the form of nested dictionaries with a list
        at the lowest level
    prefix (str, default=""):
        Used for internal recursion to maintain current branch position

    Returns:
    --------
    tree_str:
       A string representing the tree structure.

    Examples:
    ---------
    print(_dict_to_tree({'branch1': {'branch1a': ['twig1ai','twig1aii']},
                         'branch2': {'branch2a': ['twig2ai','twig2aii'],
                                     'branch2b': ['twig2bi',]}
                 }))

    ├── branch1
    │   └── branch1a
    │       ├── twig1ai
    │       └── twig1aii
    └── branch2
        ├── branch2a
        │   ├── twig2ai
        │   └── twig2aii
        └── branch2b
            └── twig2bi
    """
    tree_str = ""
    keys = list(input_dict.keys())

    for i, key in enumerate(keys):
        # Determine if this is the last key at this level
        branch = "└── " if i == len(keys) - 1 else "├── "
        sub_prefix = "    " if i == len(keys) - 1 else "│   "

        # If the value is a dictionary, recurse into it
        if isinstance(input_dict[key], dict):
            tree_str += f"{prefix}{branch}{key}\n"
            tree_str += _dict_to_tree(input_dict[key], prefix + sub_prefix)
        # If the value is a list, print each item in the list
        elif isinstance(input_dict[key], list):
            tree_str += f"{prefix}{branch}{key}\n"
            for j, item in enumerate(input_dict[key]):
                item_branch = "└── " if j == len(input_dict[key]) - 1 else "├── "
                tree_str += f"{prefix}{sub_prefix}{item_branch}{item}\n"

    return tree_str


def _run_cmd(
    cmd: str,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    msg_pre: str | None = None,
    msg_post: str | None = None,
    msg_err: str | None = None,
    raise_on_error: bool = False,
) -> str:
    """Execute a subprocess using default configuration, blocking until it completes.

    Parameters:
    -----------
    cmd (str):
       The command to be executed as a separate process.
    cwd (Path, default = None):
       The working directory for the command. If None, use current working directory.
    env (Dict[str, str], default = None):
       A dictionary of environment variables to be passed to the command.
    msg_pre (str  | None), default = None):
       An overridden message logged before the command is executed.
    msg_post (str | None), default = None):
        An overridden message logged after the command is successfully executed.
    msg_err (str | None), default = None):
        An overridden message logged when a command returns a non-zero code. Logs
        will automatically append the stderr output of the command.
    raise_on_error (bool, default = False):
        If True, raises a RuntimeError if the command returns a non-zero code.

    Returns:
    -------
    stdout: str
       The captured standard output of the process.

    Examples:
    --------
    In: _run_cmd("python foo.py", msg_pre="Running script", msg_post="Script completed")
    Out: Running script
         Script completed

    In: _run_cmd("python foo.py")
    Out: Running command: python foo.py
         Command completed successfully.

    In: _run_cmd("python return_nonzero.py")
    Out: Running command: python return_nonzero.py
         Command `python return_nonzero.py` failed. STDERR: <stderror of foo.py>
    """
    log.debug(msg_pre or f"Running command: {cmd}")
    stdout: str = ""

    fn = functools.partial(
        subprocess.run,
        cmd,
        shell=True,
        text=True,
        capture_output=True,
    )

    kwargs: dict[str, str | PathLike | dict[str, str]] = {}
    if cwd:
        kwargs["cwd"] = cwd
    if env:
        kwargs["env"] = env

    result: subprocess.CompletedProcess[str] = fn(**kwargs)  # type: ignore[reportArgumentType,reportCallIssue]
    stdout = str(result.stdout).strip() if result.stdout is not None else ""
    if result.returncode != 0:
        rc_out = f"Return Code: `{result.returncode}`."
        stderr_out = f"STDERR:\n{result.stderr.strip()}"

        if not msg_err:
            msg_err = f"Command `{cmd}` failed."

        msg = f"{msg_err} {rc_out} {stderr_out}"

        if raise_on_error:
            raise RuntimeError(msg)

        log.error(msg)

    log.debug(msg_post or "Command completed successfully.")
    return stdout


def slugify(source: str) -> str:
    """Convert a source string into a URL-safe slug.

    Parameters
    ----------
    source : str
        The string to be converted.

    Returns
    -------
    str
        The slugified version of the source string.
    """
    if not source:
        raise ValueError

    return re.sub(r"\W+", "-", source.strip().casefold())


def deep_merge(d1: dict[str, t.Any], d2: dict[str, t.Any]) -> dict[str, t.Any]:
    """Deep merge two dictionaries.

    Iterate recursively through keys in dictionary `d2`, replacing
    any leaf values in `d1` with the value from `d2`.

    NOTE: Currently handles leaf values that are scalar or lists. Additional
    leaf types (such as set) may require additional conditional blocks.

    Parameters
    ----------
    d1 : dict[str, t.Any]
        The dictionary that must be updated.
    d2 : dict[str, t.Any]
        The dictionary containing values to be merged.

    Returns
    -------
    dict[str, t.Any]
        The merged dictionaries.
    """
    for k, v in d2.items():
        if isinstance(v, dict):
            d1[k] = deep_merge(d1.get(k, {}), v)
        elif isinstance(v, list):
            list_items = []
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    list_items.append(deep_merge(d1.get(k, {})[i], item))
                else:
                    list_items.append(item)
            d1[k] = list_items
        else:
            d1[k] = v
    return d1


def additional_files_dir() -> Path:
    """Return the path to the additional files directory.

    Returns
    -------
    Path
    """
    return Path(__file__).parent.parent / "additional_files"
