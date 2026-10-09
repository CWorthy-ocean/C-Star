import os
import re
import shlex
import sys
import typing as t
from enum import StrEnum, auto
from pathlib import Path

import shellingham
import typer

from cstar.base.env import ENV_CSTAR_LOG_LEVEL
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
    from collections.abc import Callable

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

The shell is the one running this command, else the one in $SHELL; name it to
choose another.
"""

BLOCK_BEGIN: t.Final[str] = "# >>> cstar shell integration >>>"
"""First line of the block that sources the function from an rc file."""
BLOCK_END: t.Final[str] = "# <<< cstar shell integration <<<"
"""Last line of the block that sources the function from an rc file."""
_BLOCK_PATTERN: t.Final[re.Pattern[str]] = re.compile(
    rf"^{re.escape(BLOCK_BEGIN)}\n.*?^{re.escape(BLOCK_END)}(?P<nl>\n|\Z)",
    re.MULTILINE | re.DOTALL,
)
_BLOCK_WITH_BLANK_PATTERN: t.Final[re.Pattern[str]] = re.compile(
    rf"(?:(?<=\n)\n)?{_BLOCK_PATTERN.pattern}", re.MULTILINE | re.DOTALL
)
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

    The shell running this command wins, then the one named by `$SHELL`.

    Returns
    -------
    Shell

    Raises
    ------
    typer.BadParameter
        If neither source names a supported shell.
    """
    found: list[str] = []
    try:
        found.append(shellingham.detect_shell()[0])
    except shellingham.ShellDetectionFailure:
        pass
    if login_shell := os.environ.get("SHELL"):
        found.append(Path(login_shell).name)

    supported = [s.value for s in Shell]
    if candidate := next((name for name in found if name in supported), None):
        return Shell(candidate)

    msg = (
        f"No supported shell was detected (found: {', '.join(found) or 'nothing'}; "
        f"supported: {', '.join(supported)}). Name the shell to use."
    )
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


def _rewrite(rc: Path, edit: "Callable[[str], str]") -> bool:
    """Apply `edit` to an rc file's text, writing it back only when it changed.

    A symlinked rc file keeps its link: the file it points to is rewritten in place.
    Its text is decoded losslessly so that every byte `edit` leaves alone survives.

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

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(after.encode(**_ENCODING))
    return True


def _upsert_block(rc: Path, block: str) -> bool:
    """Replace the cstar block of an rc file in place, or append it after a blank line.

    Returns
    -------
    bool
        Whether the file changed; `False` when it already had this exact block.
    """

    def edit(text: str) -> str:
        if _BLOCK_PATTERN.search(text):
            return _BLOCK_PATTERN.sub(lambda m: block + m.group("nl"), text, count=1)
        # a blank line separates the block from existing content
        gap = "" if not text else ("" if text.endswith("\n") else "\n") + "\n"
        return f"{text}{gap}{block}\n"

    return _rewrite(rc, edit)


def _remove_block(rc: Path) -> bool:
    """Remove the cstar block, and the blank line before it, from an rc file.

    Returns
    -------
    bool
        Whether a block was removed.
    """
    return _rewrite(rc, lambda text: _BLOCK_WITH_BLANK_PATTERN.sub("", text))


def _source_block(function_file: Path) -> str:
    """The rc file block that sources `function_file` when it exists."""
    path = shlex.quote(str(function_file))
    return f"{BLOCK_BEGIN}\nif [ -f {path} ]; then . {path}; fi\n{BLOCK_END}"


def _install(shell: Shell) -> None:
    """Write the function file, ensure the rc file sources it, and report both."""
    function_file = _function_file(shell)
    function_file.parent.mkdir(parents=True, exist_ok=True)
    function_file.write_text(shell_function(shell))

    rc = shell.rc_file
    changed = _upsert_block(rc, _source_block(function_file))

    typer.echo(f"Shell: {shell}")
    typer.echo(f"Wrote {function_file}")
    typer.echo(f"Updated {rc}" if changed else f"{rc} already has the block")
    typer.echo(f"Open a new shell, or run: source {shlex.quote(str(function_file))}")
    if shell is Shell.BASH and sys.platform == "darwin":
        typer.echo(
            "Terminal starts login shells, which read ~/.bash_profile: "
            "make sure it sources ~/.bashrc."
        )


def _uninstall(shell: Shell) -> None:
    """Remove the rc file block and the function file, and report what was removed."""
    function_file = _function_file(shell)
    removed: list[str] = []
    if _remove_block(shell.rc_file):
        removed.append(f"Removed the block from {shell.rc_file}")
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
            help="The shell to set up (default: the running shell, else $SHELL).",
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
