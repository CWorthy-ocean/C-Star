import typing as t
from enum import StrEnum, auto

import typer

from cstar.base.env import ENV_CSTAR_LOG_LEVEL
from cstar.cli.workplan import ALIAS, NAME
from cstar.cli.workplan.path import COMMAND_CD, COMMAND_PATH

app = typer.Typer()

COMMAND_SHELL_INIT: t.Final[str] = "shell-init"
"""Name of the command that prints the shell integration."""

HELP_SHORT = f"Print the shell function that makes `cstar {NAME} {COMMAND_CD}` work."
HELP_LONG = f"""\
{HELP_SHORT}

A process cannot change the directory of the shell that started it, so
`cstar {NAME} {COMMAND_CD} <run-id> \\[step]` needs a shell function named `cstar`.
The function runs `cstar {NAME} {COMMAND_PATH}` and changes directory to the
path it prints; every other command passes through to the cstar executable.

Save the output once, source it from your shell's rc file, and regenerate it
after upgrading C-Star.
"""


class Shell(StrEnum):
    """Shells the integration can be generated for."""

    ZSH = auto()
    """The Z shell."""
    BASH = auto()
    """The Bourne-again shell."""


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


@app.command(name=COMMAND_SHELL_INIT, help=HELP_LONG, short_help=HELP_SHORT)
def shell_init(
    shell: t.Annotated[
        Shell,
        typer.Argument(help="The shell to generate the function for."),
    ],
) -> None:
    """Print the shell function that makes `cstar workplan cd` work."""
    typer.echo(shell_function(shell), nl=False)


if __name__ == "__main__":
    typer.run(shell_init)
