import errno
import os
import re
import shlex
import stat
import tempfile
import typing as t
from contextlib import contextmanager
from enum import StrEnum, auto
from pathlib import Path

import shellingham
import typer

from cstar.base.env import ENV_CSTAR_LOG_LEVEL
from cstar.base.exceptions import CstarError
from cstar.cli.workplan import ALIAS, NAME
from cstar.cli.workplan.path import COMMAND_CD, COMMAND_PATH
from cstar.entrypoint.utils import (
    ARG_INSTALL,
    ARG_INSTALL_HELP,
    ARG_UNINSTALL,
    ARG_UNINSTALL_HELP,
)
from cstar.execution.file_system import DirectoryManager

if t.TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

app = typer.Typer()

COMMAND_SHELL_INIT: t.Final[str] = "shell-init"
"""Name of the command that prints, installs or removes the shell integration."""

HELP_SHORT = f"Set up the shell function that makes `cstar {NAME} {COMMAND_CD}` work."
HELP_LONG = f"""\
{HELP_SHORT}

A process cannot change the directory of the shell that started it, so
`cstar {NAME} {COMMAND_CD} <run-id> \\[step]` needs a shell function named `cstar`.
The function runs `cstar {NAME} {COMMAND_PATH}` and changes directory to the
path it prints; every other command passes through to the cstar executable.

By default the function is printed, for dotfiles you manage yourself. With
{ARG_INSTALL} it is written to the C-Star config directory and sourced from a
marked block in your rc file (~/.zshrc or ~/.bashrc); re-run it after
upgrading C-Star. {ARG_UNINSTALL} removes both.

The shell is the one running this command, or the one in $SHELL when that cannot
be detected; name it to choose another.
"""

BLOCK_BEGIN: t.Final[str] = "# >>> cstar shell integration >>>"
"""First line of the block that sources the function from an rc file."""
BLOCK_END: t.Final[str] = "# <<< cstar shell integration <<<"
"""Last line of the block that sources the function from an rc file."""
BLOCK_NOTE: t.Final[str] = (
    f"# Managed by cstar env {COMMAND_SHELL_INIT}; edits inside this block are replaced."
)
"""Comment on the second line of the block; nothing that reads the block depends on it."""
_LINE: t.Final[re.Pattern[str]] = re.compile(r"[^\n]*\n|[^\n]+")
"""One line with its ending; only newlines split, so CRLF endings survive."""
_BLANK_LINES: t.Final[frozenset[str]] = frozenset({"\n", "\r\n"})
_ENCODING: t.Final[dict[str, str]] = {"encoding": "utf-8", "errors": "surrogateescape"}
"""Codec settings that round-trip any bytes of an rc file."""


class Shell(StrEnum):
    """Shells the integration can be generated for."""

    ZSH = auto()
    """The Z shell."""
    BASH = auto()
    """The Bourne-again shell."""

    @property
    def rc_file(self) -> Path:
        """The rc file this shell reads for each new interactive session."""
        if self is Shell.ZSH:
            return Path(os.environ.get("ZDOTDIR") or Path.home()) / ".zshrc"
        return Path.home() / ".bashrc"


def detect_shell() -> Shell:
    """Detect the supported shell the user is running.

    The shell running this command is used; `$SHELL` is consulted only when that
    cannot be detected. A detected shell without support is an error, never a
    reason to fall back to another.

    Returns
    -------
    Shell

    Raises
    ------
    typer.BadParameter
        If the detected shell is not supported, or none can be detected.
    """
    try:
        name, origin = shellingham.detect_shell()[0], "running shell"
    except shellingham.ShellDetectionFailure:
        name, origin = Path(os.environ.get("SHELL", "")).name, "$SHELL"

    supported = [s.value for s in Shell]
    if name in supported:
        return Shell(name)

    found = f"the {origin} is {name!r}" if name else "no shell was detected"
    msg = f"Unable to choose a supported shell: {found}; supported: {', '.join(supported)}. Name the shell to use."
    raise typer.BadParameter(msg)


def shell_function(shell: Shell) -> str:
    """Build the shell function that intercepts `cstar workplan cd`.

    The function is portable sh syntax plus `local`, and works under `set -u`
    in bash and `setopt nounset` in zsh. It intercepts only `cstar workplan cd
    <run-id> [step]` (or its alias), and passes everything else, including
    `--help` and an incomplete command, through to the executable. A `cstar`
    alias is removed first, as it would break the function definition.

    The log level is pinned to WARNING for the lookup so that no INFO or DEBUG
    record can reach the stdout the function captures as the directory.

    Parameters
    ----------
    shell : Shell
        The shell the output is generated for; named in the header comment.

    Returns
    -------
    str
        The complete script, ending with a single newline.
    """
    path_command = f"{ENV_CSTAR_LOG_LEVEL}=WARNING command cstar {NAME} {COMMAND_PATH}"

    return f"""\
# cstar shell integration for {shell}. Regenerate after upgrading C-Star.
unalias cstar 2>/dev/null || true
cstar() {{
    if [ "$#" -ge 3 ] && [ "$#" -le 4 ] && [ "$2" = "{COMMAND_CD}" ] \\
        && {{ [ "$1" = "{NAME}" ] || [ "$1" = "{ALIAS}" ]; }}; then
        case "$3" in -*) command cstar "$@"; return ;; esac
        case "${{4-}}" in -*) command cstar "$@"; return ;; esac
        local __cstar_dir
        if [ "$#" -eq 3 ]; then
            __cstar_dir="$({path_command} "$3")" || return
        else
            __cstar_dir="$({path_command} "$3" "$4")" || return
        fi
        builtin cd -- "$__cstar_dir"
    else
        command cstar "$@"
    fi
}}
"""


def _function_file(shell: Shell) -> Path:
    """The file the installed function is written to."""
    return DirectoryManager.config_home() / "shell" / f"cstar.{shell}"


class RcFileError(CstarError):
    """An rc file cannot be edited safely; the message is shown to the user."""


@contextmanager
def _reporting(path: Path) -> "Iterator[None]":
    """Turn a failure to read or edit `path` into a one-line error and exit code 1."""
    try:
        yield
    except RcFileError as ex:
        message = str(ex)
    except OSError as ex:
        message = f"{path}: {ex.strerror or ex}"
    else:
        return

    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(1)


def _write_atomic(path: Path, data: bytes) -> None:
    """Replace the file at `path` with `data`, or leave it exactly as it was.

    The data is written, flushed and synced to a temporary file beside the target,
    given the target's permissions (or the umask default for a new file), and
    renamed over it. A failure at any point, such as a full disk, leaves the
    original intact and removes the temporary file. A symlink at `path` keeps its
    link: its target is replaced. The target becomes a new inode, so hard links to
    it, its ownership and its extended attributes are not preserved.

    Parameters
    ----------
    path : Path
        The file to write; its parent directories are created.
    data : bytes
        The new content.
    """
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        mode = stat.S_IMODE(target.stat().st_mode)
    else:
        umask = os.umask(0)
        os.umask(umask)
        mode = 0o666 & ~umask

    temp = tempfile.NamedTemporaryFile(
        "wb", dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False
    )
    try:
        with temp:
            temp.write(data)
            temp.flush()
            os.fsync(temp.fileno())
        os.chmod(temp.name, mode)
        os.replace(temp.name, target)
    except BaseException:
        Path(temp.name).unlink(missing_ok=True)
        raise


def _rewrite(rc: Path, edit: "Callable[[str], str]") -> bool:
    """Apply `edit` to an rc file's text, writing it back only when it changed.

    The text is decoded losslessly so that every byte `edit` leaves alone survives,
    and `edit` runs before anything is written, so an error it raises changes nothing.

    Parameters
    ----------
    rc : Path
        The rc file; created, with its parent directories, when it needs content.
    edit : Callable[[str], str]
        Maps the current text, empty for a missing file, to the new text.

    Returns
    -------
    bool
        Whether the file was written.
    """
    target = rc.resolve()
    before = target.read_bytes().decode(**_ENCODING) if target.exists() else ""
    after = edit(before)
    if after == before:
        return False

    if target.exists() and not os.access(target, os.W_OK):
        raise PermissionError(errno.EACCES, os.strerror(errno.EACCES), str(target))

    _write_atomic(target, after.encode(**_ENCODING))
    return True


def _ending(line: str) -> str:
    """The line ending, if any, that terminates `line`."""
    return line[len(line.rstrip("\r\n")) :]


def _find_blocks(rc: Path, lines: "Sequence[str]") -> list[tuple[int, int]]:
    """Locate the cstar blocks of an rc file.

    A marker is a line that equals `BLOCK_BEGIN` or `BLOCK_END` once trailing
    whitespace, such as a carriage return, is ignored. The file is well-formed
    when every begin marker is followed by an end marker before the next begin
    marker, and no end marker appears outside a block.

    Parameters
    ----------
    rc : Path
        The rc file, named in the error.
    lines : Sequence[str]
        The lines of the file, each with its ending.

    Returns
    -------
    list[tuple[int, int]]
        The indices of the begin and end lines of each block, inclusive.

    Raises
    ------
    RcFileError
        If the markers are not well-formed, naming the offending line numbers.
    """

    def unbalanced(detail: str) -> RcFileError:
        return RcFileError(
            f"{rc}: the cstar block markers are unbalanced ({detail}). Fix or "
            "remove the block by hand, then run this command again. Nothing was changed."
        )

    blocks: list[tuple[int, int]] = []
    begin: int | None = None
    for index, line in enumerate(lines):
        marker = line.rstrip()
        if marker == BLOCK_BEGIN:
            if begin is not None:
                raise unbalanced(
                    f"line {index + 1} starts a block before the block from "
                    f"line {begin + 1} ends"
                )
            begin = index
        elif marker == BLOCK_END:
            if begin is None:
                raise unbalanced(
                    f"line {index + 1} ends a block that was never started"
                )
            blocks.append((begin, index))
            begin = None

    if begin is not None:
        raise unbalanced(f"line {begin + 1} starts a block that is never closed")

    return blocks


def _without(lines: "Sequence[str]", blocks: "Sequence[tuple[int, int]]") -> list[str]:
    """The lines minus each block and the blank line directly before it, if any."""
    dropped: set[int] = set()
    for begin, end in blocks:
        dropped.update(range(begin, end + 1))
        if begin > 0 and lines[begin - 1] in _BLANK_LINES:
            dropped.add(begin - 1)

    return [line for index, line in enumerate(lines) if index not in dropped]


def _source_block(function_file: Path) -> list[str]:
    """The lines, without endings, of the rc file block that sources `function_file`."""
    path = shlex.quote(str(function_file))
    return [
        BLOCK_BEGIN,
        BLOCK_NOTE,
        f"if [ -f {path} ]; then . {path}; fi",
        BLOCK_END,
    ]


def _upsert_block(rc: Path, function_file: Path) -> bool:
    """Make the rc file contain exactly one block sourcing `function_file`.

    An existing block is replaced in place, keeping its line ending, and any
    further blocks are removed. Otherwise the block is appended after a blank line,
    in the line-ending style of the file.

    Returns
    -------
    bool
        Whether the file changed; `False` when it already had this exact block.

    Raises
    ------
    RcFileError
        If the file's markers are not well-formed; nothing is written.
    """
    body = _source_block(function_file)

    def edit(text: str) -> str:
        lines = _LINE.findall(text)
        blocks = _find_blocks(rc, lines)

        if not blocks:
            eol = next((e for line in lines if (e := _ending(line))), "\n")
            head = text if not text or text.endswith("\n") else text + eol
            return f"{head}{eol if text else ''}{eol.join(body)}{eol}"

        first_begin, first_end = blocks[0]
        eol = _ending(lines[first_begin])
        lines = _without(lines, blocks[1:])
        lines[first_begin : first_end + 1] = [
            *(f"{line}{eol}" for line in body[:-1]),
            f"{body[-1]}{_ending(lines[first_end])}",
        ]
        return "".join(lines)

    return _rewrite(rc, edit)


def _remove_block(rc: Path) -> bool:
    """Remove every cstar block, and the blank line before each, from an rc file.

    Returns
    -------
    bool
        Whether a block was removed.

    Raises
    ------
    RcFileError
        If the file's markers are not well-formed; nothing is written.
    """

    def edit(text: str) -> str:
        lines = _LINE.findall(text)
        return "".join(_without(lines, _find_blocks(rc, lines)))

    return _rewrite(rc, edit)


def _label(rc: Path) -> str:
    """Name an rc file for a report, with the file a symlink points to."""
    return f"{rc} (-> {rc.resolve()})" if rc.is_symlink() else str(rc)


def _install(shell: Shell) -> None:
    """Ensure the rc file sources the function, write the function file, and report.

    The rc file is checked and written first: its block only sources the function
    file when it exists, so it stays inert if writing the function then fails.
    """
    function_file = _function_file(shell)
    rc = shell.rc_file
    with _reporting(rc):
        changed = _upsert_block(rc, function_file)
    with _reporting(function_file):
        _write_atomic(function_file, shell_function(shell).encode())

    typer.echo(f"Shell: {shell}")
    typer.echo(f"Wrote {function_file}")
    typer.echo(
        f"Updated {_label(rc)}" if changed else f"{_label(rc)} already has the block"
    )
    typer.echo(f"Open a new shell, or run: source {shlex.quote(str(function_file))}")
    if shell is Shell.BASH:
        typer.echo("Login shells read ~/.bash_profile: make sure it sources ~/.bashrc.")


def _uninstall(shell: Shell) -> None:
    """Remove the rc file blocks and the function file, and report what was removed.

    The rc file is checked first, so unbalanced markers leave the function file too.
    """
    function_file = _function_file(shell)
    rc = shell.rc_file
    removed: list[str] = []
    with _reporting(rc):
        if _remove_block(rc):
            removed.append(f"Removed the block from {_label(rc)}")
    with _reporting(function_file):
        if function_file.exists():
            function_file.unlink()
            removed.append(f"Removed {function_file}")

    typer.echo(f"Shell: {shell}")
    if removed:
        typer.echo("\n".join(removed))
        typer.echo(
            "Shells that are already open keep the `cstar` function until you "
            "close them or run `unset -f cstar`."
        )
    else:
        typer.echo("Nothing was installed.")


@app.command(name=COMMAND_SHELL_INIT, help=HELP_LONG, short_help=HELP_SHORT)
def shell_init(
    shell: t.Annotated[
        Shell,
        typer.Argument(
            default_factory=detect_shell,
            help="The shell to set up (default: the running shell).",
            show_default=False,
        ),
    ],
    install: t.Annotated[
        bool, typer.Option(ARG_INSTALL, help=ARG_INSTALL_HELP)
    ] = False,
    uninstall: t.Annotated[
        bool, typer.Option(ARG_UNINSTALL, help=ARG_UNINSTALL_HELP)
    ] = False,
) -> None:
    """Print, install or uninstall the shell function that makes `cstar workplan cd` work."""
    if install and uninstall:
        msg = f"{ARG_INSTALL} and {ARG_UNINSTALL} cannot be combined"
        raise typer.BadParameter(msg)

    if install:
        _install(shell)
    elif uninstall:
        _uninstall(shell)
    else:
        typer.echo(shell_function(shell), nl=False)


if __name__ == "__main__":
    typer.run(shell_init)
