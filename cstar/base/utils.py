import datetime as dt
import functools
import hashlib
import re
import subprocess
import typing as t
from os import PathLike
from pathlib import Path

import dateutil

from cstar.base.log import get_logger

log = get_logger(__name__)


DEFAULT_OUTPUT_ROOT_NAME: t.Literal["output"] = "output"
"""A fixed `output_root_name` to be used when generating outputs with ROMS."""


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
