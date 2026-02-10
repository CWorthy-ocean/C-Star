import typing as t
from collections import defaultdict
from enum import StrEnum, auto

import typer
from rich import print  # noqa: A004, ignore shadowing of built-in print

from cstar.base import utils as base_utils
from cstar.base.utils import EnvItem, discover_env_vars
from cstar.orchestration import utils as orch_utils

app = typer.Typer()


H_CONFIG_ALL: t.Final[str] = "C-Star Environment Configuration"
"""Main header displayed before all configuration sections."""


def _interactive(all_config: dict[str, list[EnvItem]]) -> None:
    """Format configuration for an interactive user."""
    show_headers = len(all_config) > 1

    if show_headers:
        print(f"[underline2]{H_CONFIG_ALL}[/underline2]\n")

    for group_name, items in sorted(all_config.items()):
        if show_headers:
            print(f"[underline]{group_name}[/underline]")

        for item in sorted(items, key=lambda x: x.name):
            val_in = "[bold red]" if item.value != item.default else ""
            val_out = "[/bold red]" if val_in else ""

            default = f"(default: {item.default}, {item.description})"
            print(f"- {item.name}: {val_in}{item.value}{val_out} {default}")

        print("\n")


def _export(all_config: dict[str, list[EnvItem]]) -> None:
    """Format configuration as environment variable export statements."""
    header_sep = "#" * 80
    item_sep = f"# {'-' * 68}"

    for group_name, items in sorted(all_config.items()):
        print(f"{header_sep}\n# {group_name}\n{header_sep}\n")

        for item in sorted(items, key=lambda x: x.name):
            purpose = f"# {item.description}\n# default: {item.default}"
            export = f'export {item.name}="{item.value}"\n'
            print(f"{item_sep}\n{purpose}\n{item_sep}\n{export}")


class DisplayFormat(StrEnum):
    """Supported display formats."""

    INTERACTIVE = auto()
    """Display a human-readable format."""
    EXPORT = auto()
    """Display a series of export statements."""


@app.command()
def show(
    group: str = "all",
    display: DisplayFormat = DisplayFormat.INTERACTIVE,
) -> None:
    """Display the active environment configuration."""
    all_items = discover_env_vars([base_utils, orch_utils])

    if group != "all":
        all_items = [item for item in all_items if group.lower() in item.group.lower()]

    # Group items by their group name for display
    group_map: dict[str, list[EnvItem]] = defaultdict(list)
    for item in all_items:
        group_map[item.group].append(item)

    if not group_map:
        msg = f"[bold red]No environment variables found for group '{group}'[/bold red]"
        print(msg)
        return

    if display == DisplayFormat.EXPORT:
        _export(group_map)
        return

    _interactive(group_map)


if __name__ == "__main__":
    typer.run(app)
