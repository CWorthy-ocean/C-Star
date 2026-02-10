import types
import typing as t
from enum import StrEnum, auto

import typer
from rich import print  # noqa: A004, ignore shadowing of built-in print

from cstar.base import utils as base_utils
from cstar.base.utils import EnvItem
from cstar.execution import file_system as fs
from cstar.orchestration import utils as orch_utils

app = typer.Typer()


H_CONFIG_ALL: t.Final[str] = "C-Star Environment Configuration"
"""Main header displayed before all configuration sections."""


def _interactive(all_config: dict[str, list[EnvItem]]) -> None:
    """Format configuration for an interactive user."""
    show_headers = len(all_config) > 1

    if show_headers:
        print(f"[underline2]{H_CONFIG_ALL}[/underline2]\n")

    for group_name, items in all_config.items():
        if show_headers:
            print(f"[underline]{group_name}[/underline]")

        for item in items:
            val_in = "[bold red]" if item.value != item.default else ""
            val_out = "[/bold red]" if val_in else ""

            default = f"(default: {item.default}, {item.description})"
            print(f"- {item.name}: {val_in}{item.value}{val_out} {default}")

        print("\n")


def _export(all_config: dict[str, list[EnvItem]]) -> None:
    """Format configuration as environment variable export statements."""
    header_sep = "#" * 80
    item_sep = f"# {'-' * 68}"

    for group_name, items in all_config.items():
        print(f"{header_sep}\n# {group_name}\n{header_sep}\n")

        for item in items:
            purpose = f"# {item.description}\n# default: {item.default}"
            export = f'export {item.name}="{item.value}"\n'
            print(f"{item_sep}\n{purpose}\n{item_sep}\n{export}")


def _adapt_xdg_meta_to_env_item(
    xdg_metadata: fs.XdgMetaContainer,
) -> t.Iterable[EnvItem]:
    return [
        EnvItem(
            description=x.purpose,
            group=x.env_item.group,
            default=x.default_value,
            name=x.var_name,
            default_factory=fs.DirectoryManager.xdg_dir(x).as_posix,
        )
        for x in xdg_metadata
    ]


def _discover_env_vars(
    module: types.ModuleType,
    prefix: str = "ENV_",
    is_ff: bool = False,
) -> list[EnvItem]:
    """Locate all constants in a module that represent environment variables."""
    items = []
    hints = t.get_type_hints(module, include_extras=True)

    for name, hint in hints.items():
        if name.startswith(prefix):
            metadata = getattr(hint, "__metadata__", None)
            if metadata and isinstance(metadata[0], base_utils.EnvVar):
                meta: base_utils.EnvVar = metadata[0]
                var_name = getattr(module, name)
                default = meta.default
                if meta.default_factory and (factory_default := meta.default_factory()):
                    default = factory_default

                # value = os.getenv(var_name, default)
                # if is_ff:
                #     value = FF_ON if is_feature_enabled(var_name) else FF_OFF

                items.append(
                    EnvItem(
                        description=meta.description,
                        group=meta.group,
                        default=default
                        if not meta.default_factory
                        else f"{default} or <generated>"
                        if default
                        else "<generated>",
                        default_factory=meta.default_factory,
                        name=var_name,
                        # value=value,
                    ),
                )
    return items


def _load_flags() -> t.Iterable[EnvItem]:
    """Load all feature flags declared in the cstar.base.utils module.

    Returns
    -------
    t.Iterable[EnvItem]
    """
    return _discover_env_vars(base_utils, prefix="ENV_FF_", is_ff=True)


def _load_orchestration() -> t.Iterable[EnvItem]:
    """Load all environment variables declared in the cstar.orchestration.utils module.

    Returns
    -------
    t.Iterable[EnvItem]
    """
    return _discover_env_vars(orch_utils)


def _load_xdg_meta() -> t.Iterable[EnvItem]:
    """Load all XDG environment variables and convert XDG metadata into generic EnvItem.

    Returns
    -------
    t.Iterable[EnvItem]
    """
    all_metadata = fs.load_xdg_metadata()
    return _adapt_xdg_meta_to_env_item(all_metadata)


def _load_all() -> t.Iterable[EnvItem]:
    """Load all environment variables from all groups.

    Returns
    -------
    t.Iterable[EnvItem]
    """
    yield from _load_xdg_meta()
    yield from _load_flags()
    yield from _load_orchestration()


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
    all_items = list(_load_all())

    if group != "all":
        all_items = [
            item
            for item in all_items
            if item.group.lower().replace(" ", "-") == group.lower()
        ]

    # Group items by their group name for display
    group_map: dict[str, list[EnvItem]] = {}
    for item in all_items:
        if item.group not in group_map:
            group_map[item.group] = []
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
