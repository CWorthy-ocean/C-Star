import os
import textwrap
import typing as t
from enum import StrEnum, auto

import typer
from rich import print
from rich.markup import escape

from cstar.base.env import NOT_SET, env_var_groups, load_user_config_for_repair

if t.TYPE_CHECKING:
    from cstar.base.env import EnvItem, UserConfig

app = typer.Typer()


H_CONFIG_ALL: t.Final[str] = "C-Star Environment Configuration"
"""Main header displayed before all configuration sections."""


SOURCE_ENV: t.Final[str] = "env"
"""Marker for a value that is exported in the environment."""
SOURCE_CONFIG: t.Final[str] = "config"
"""Marker for a value the configuration file supplied."""
SOURCE_DEFAULT: t.Final[str] = "default"
"""Marker for a value that fell through to a built-in or system default (a
scratch convention, an ``XDG_*`` or ``PROJECT`` variable)."""


def value_source(item: "EnvItem", config: "UserConfig") -> str:
    """Name where a setting's effective value came from.

    Parameters
    ----------
    item : EnvItem
        The setting.
    config : UserConfig
        What the loader exported from the configuration file.

    Returns
    -------
    str
        `SOURCE_CONFIG` when the file supplied the exported value, `SOURCE_ENV`
        when the shell (or a CLI option) did, else `SOURCE_DEFAULT`.
    """
    if (exported := os.environ.get(item.name)) is None:
        return SOURCE_DEFAULT
    if config.applied.get(item.name) == exported:
        return SOURCE_CONFIG
    return SOURCE_ENV


def _interactive(all_config: dict[str, list["EnvItem"]], config: "UserConfig") -> None:
    """Format configuration for an interactive user."""
    show_headers = len(all_config) > 1

    if show_headers:
        print(f"[underline2]{H_CONFIG_ALL}[/underline2]\n")

    state = "" if config.path.is_file() else " (not found)"
    print(escape(f"Configuration file: {config.path}{state}"))
    print(
        escape(
            f"Each value is marked with its source: [{SOURCE_ENV}] exported in the "
            f"shell, [{SOURCE_CONFIG}] from the configuration file, "
            f"[{SOURCE_DEFAULT}] a built-in or system default.\n"
        )
    )

    for group_name, items in all_config.items():
        if show_headers:
            print(f"[underline]{group_name}[/underline]")

        for item in sorted(items, key=lambda x: x.name):
            val_in = "[bold red]" if item.value != item.default else "[green]"
            val_out = "[/bold red]" if "red" in val_in else "[/green]"
            def_in, def_out = "[blue]", "[/blue]"
            desc_in, desc_out = "[yellow italic]", "[/yellow italic]"

            kvp_value = item.value or NOT_SET
            def_value = item.default or NOT_SET

            kvp_detail = f"{item.name}: {val_in}{kvp_value}{val_out}"
            source_detail = escape(f"[{value_source(item, config)}]")
            default_detail = f"(default: {def_in}{def_value}{def_out})"
            desc_detail = f"{desc_in}{item.description}{desc_out}"
            details = f"{default_detail}, {desc_detail}"
            print(f"- {kvp_detail} {source_detail} {details}")

        print("\n")


def _export(all_config: dict[str, list["EnvItem"]]) -> None:
    """Format configuration as environment variable export statements."""
    header_sep = "#" * 80
    item_sep = f"# {'-' * 68}"

    for group_name, items in all_config.items():
        print(f"{header_sep}\n# {group_name}\n{header_sep}\n")

        for item in sorted(items, key=lambda x: x.name):
            default_detail = f"(default: {item.default or NOT_SET})"
            wrapped_header = textwrap.fill(
                item.description, width=68, initial_indent="# ", subsequent_indent="# "
            )
            export = f'export {item.name}="{item.value}"\n'
            print(
                textwrap.dedent(
                    f"""\
                    {item_sep}
                    # {item.name} {default_detail}
                    <H>
                    {item_sep}
                    {export}
                    """,
                ).replace("<H>", wrapped_header),
            )


class DisplayFormat(StrEnum):
    """Supported display formats."""

    INTERACTIVE = auto()
    """Display a human-readable format."""
    EXPORT = auto()
    """Display a series of export statements."""


ALL_GROUPS = t.Literal["all", "flags", "file", "orch", "sim", "dev", "internal"]


@app.command(name="show", help="Display the active environment configuration.")
def show(
    group: t.Annotated[
        ALL_GROUPS,
        typer.Option(
            "-g",
            "--group",
            help=(
                "Specify a group key to retrieve a subset of available configuration. "
                "'all' omits the internal group, which C-Star sets for its own processes."
            ),
        ),
    ] = "all",
    display: t.Annotated[
        DisplayFormat,
        typer.Option(
            "-d",
            "--display",
            help="Specify the desired format for displaying the configuration.",
        ),
    ] = DisplayFormat.INTERACTIVE,
) -> None:
    """Display the active environment configuration.

    The internal group is shown only when requested by name, and never as
    export statements: C-Star sets those variables for its own processes.
    """
    if group == "internal" and display == DisplayFormat.EXPORT:
        print("[bold red]The internal group is never exported[/bold red]")
        return

    group_map = env_var_groups(include_internal=group == "internal")

    if group != "all":
        group_map = {k: v for k, v in group_map.items() if group in k.lower()}

    if not group_map:
        msg = f"[bold red]No environment variables found for group '{group}'[/bold red]"
        print(msg)
        return

    if display == DisplayFormat.EXPORT:
        _export(group_map)
        return

    _interactive(group_map, load_user_config_for_repair())


if __name__ == "__main__":
    typer.run(show)
