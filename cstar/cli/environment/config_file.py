import os
import re
import typing as t

import typer

from cstar.base.env import (
    USER_CONFIG_HEADER,
    format_user_config_value,
    get_env_item,
    load_user_config_for_repair,
    read_user_config,
    render_setting_block,
    user_config_problems,
)

if t.TYPE_CHECKING:
    from pathlib import Path

app_set = typer.Typer()
app_unset = typer.Typer()

NameArg = t.Annotated[str, typer.Argument(help="The setting to change.")]
"""The setting's name, as exported in the shell (e.g. CSTAR_LOG_LEVEL)."""


def _active_pattern(name: str) -> re.Pattern[str]:
    """Match a line that sets `name`, as the dotenv format reads it (an ``export``
    prefix is accepted), or names it without ``=``, which the loader refuses.
    """
    return re.compile(rf"^\s*(?:export\s+)?{re.escape(name)}\s*(?:=|#|$)")


def _placeholder_pattern(name: str) -> re.Pattern[str]:
    """Match the commented-out line ``# NAME=`` that the renderer writes for a
    setting without a value.
    """
    return re.compile(rf"^\s*#\s*{re.escape(name)}\s*=\s*$")


def _validated_name(name: str) -> str:
    """Strip `name` and check that the configuration file may hold it.

    Parameters
    ----------
    name : str
        The setting name as typed.

    Returns
    -------
    str
        The name without surrounding whitespace.

    Raises
    ------
    typer.BadParameter
        When `user_config_problems` rejects the name.
    """
    name = name.strip()
    if problems := user_config_problems([name]):
        raise typer.BadParameter(problems[0])
    return name


def _read_lines(path: "Path") -> list[str]:
    """The file's lines, with their line endings.

    Raises
    ------
    typer.Exit
        With code 1 when the file cannot be read: rewriting it would lose what
        it holds.
    """
    try:
        return path.read_text().splitlines(keepends=True)
    except (OSError, UnicodeDecodeError) as ex:
        typer.echo(f"Error: cannot read {path}: {ex}", err=True)
        raise typer.Exit(1) from ex


def set_setting_lines(lines: list[str], name: str, value: str) -> list[str] | None:
    """Rewrite file lines so that `name` is set to `value`.

    The first active line for `name` (one without ``=`` included) takes the new
    value and later active duplicates are dropped; with no active line, the
    first placeholder is filled in place. Every other line is kept as it was.

    Parameters
    ----------
    lines : list[str]
        The file's lines, with their line endings.
    name : str
        The setting to set.
    value : str
        The value to give it.

    Returns
    -------
    list[str] | None
        The rewritten lines, or `None` when `name` has neither an active line nor
        a placeholder, so the caller must append its block.
    """
    new_line = f"{name}={format_user_config_value(value)}\n"
    active = _active_pattern(name)
    placeholder = _placeholder_pattern(name)

    if any(active.match(line) for line in lines):
        result: list[str] = []
        replaced = False
        for line in lines:
            if not active.match(line):
                result.append(line)
            elif not replaced:
                result.append(new_line)
                replaced = True
        return result

    for index, line in enumerate(lines):
        if placeholder.match(line):
            return [*lines[:index], new_line, *lines[index + 1 :]]
    return None


def unset_setting_lines(lines: list[str], name: str) -> list[str] | None:
    """Rewrite file lines so that `name` is commented out.

    Every active line for `name` becomes the placeholder ``# NAME=``, which
    leaves the setting's comment block in place for a later ``cstar env set``.

    Parameters
    ----------
    lines : list[str]
        The file's lines, with their line endings.
    name : str
        The setting to unset.

    Returns
    -------
    list[str] | None
        The rewritten lines, or `None` when no line sets `name`.
    """
    active = _active_pattern(name)
    if not any(active.match(line) for line in lines):
        return None
    return [f"# {name}=\n" if active.match(line) else line for line in lines]


@app_set.command(name="set", help="Set a value in the configuration file.")
def set_value(
    name: NameArg,
    value: t.Annotated[str, typer.Argument(help="The value to give it.")],
) -> None:
    """Set a setting in the configuration file, creating the file if needed.

    The file is located through the loader's record, so the path is the one
    this process read. A shell that exports the same setting still takes
    precedence over the file. The name is checked strictly, so a typo is caught
    here rather than skipped by every later command.
    """
    name = _validated_name(name)
    config = load_user_config_for_repair()
    path = config.path

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        block = render_setting_block(get_env_item(name), value)
        path.write_text(f"{USER_CONFIG_HEADER}\n{block}")
    else:
        current = _read_lines(path)
        text = "".join(current)
        lines = set_setting_lines(current, name, value)
        if lines is not None:
            path.write_text("".join(lines))
        else:
            separator = "" if not text or text.endswith("\n") else "\n"
            block = render_setting_block(get_env_item(name), value)
            path.write_text(f"{text}{separator}\n{block}")

    effective = read_user_config(path).get(name, value)
    expansion = f" (reads back as {effective!r})" if effective != value else ""
    typer.echo(f"Set {name}={value} in {path}{expansion}")
    # a value the loader took from the file is not a shell export
    exported = os.environ.get(name)
    shell_exported = exported is not None and config.applied.get(name) != exported
    if shell_exported and exported != value:
        typer.echo(
            f"{name} is exported in the shell as {exported!r}, which takes "
            f"precedence over the file until it is unset.",
            err=True,
        )


@app_unset.command(name="unset", help="Remove a value from the configuration file.")
def unset_value(name: NameArg) -> None:
    """Comment a setting out of the configuration file, keeping its description.

    Any name is accepted, so a line the loader skips (a removed setting, a name
    this version does not know) can be taken out as well.
    """
    name = name.strip()
    path = load_user_config_for_repair().path

    lines = unset_setting_lines(_read_lines(path), name) if path.is_file() else None
    if lines is None:
        typer.echo(f"{name} is not set in {path}")
        return

    path.write_text("".join(lines))
    typer.echo(f"Unset {name} in {path}")
