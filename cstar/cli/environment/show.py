import typing as t
from dataclasses import dataclass
from enum import StrEnum, auto

from rich import print
from typer import Typer

from cstar.base.feature import FF_OFF, FF_ON, is_feature_enabled
from cstar.base.utils import (
    ENV_FF_CLI_ENV_SHOW,
    ENV_FF_CLI_TEMPLATE_CREATE,
    ENV_FF_CLI_WORKPLAN_COMPOSE,
    ENV_FF_CLI_WORKPLAN_GEN,
    ENV_FF_CLI_WORKPLAN_PLAN,
    ENV_FF_CLI_WORKPLAN_STATUS,
    ENV_FF_ORCH_TRX_OVERRIDE,
    ENV_FF_ORCH_TRX_TIMESPLIT,
    get_ff_descriptions,
)
from cstar.execution import file_system as fs

app = Typer()


CONFIG_HEADER: t.Final[str] = "C-Star Environment Configuration"
"""Main header displayed before all configuration sections."""

FS_HEADER: t.Final[str] = "File System"
"""Header for file system configuration section."""


@dataclass(slots=True)
class EnvItem:
    """Metadata specifying how to select a specific XDG-compliant setting."""

    purpose: str
    """Plain-text description of a setting."""
    name: str
    """The standard environment variable name used for the setting."""
    default: str
    """The default value for a setting."""
    value: str
    """The actual value for a setting."""


class VariableGroup(StrEnum):
    ALL = auto()
    FILE_SYSTEM = auto()
    FEATURE_FLAGS = auto()


@dataclass
class Group:
    header: str
    """The header to display for the group."""
    loader: t.Callable[[], t.Iterable[EnvItem]]
    """The callable to load the group."""

    def __iter__(self) -> t.Iterator[EnvItem]:
        yield from self.loader()


def _interactive(all_config: dict[VariableGroup, Group]) -> None:
    """Format configuration for an interactive user."""
    print(f"[underline2]{CONFIG_HEADER}[/underline2]\n")

    for group in all_config.values():
        print(f"[underline]{group.header}[/underline]")

        for item in group:
            val_in = "[bold red]" if item.value != item.default else ""
            val_out = "[/bold red]" if val_in else ""

            print(
                f"- {item.name}: {val_in}{item.value}{val_out} (default: {item.default}, {item.purpose})"
            )

        print("\n")


def _export(all_config: dict[VariableGroup, Group]) -> None:
    """Format configuration as environment variable export statements."""
    header_sep = "#" * 80
    item_sep = f"# {'-' * 68}"

    for group in all_config.values():
        print(f"{header_sep}\n# {group.header}\n{header_sep}\n")

        for item in group:
            print(
                f"{item_sep}\n# {item.purpose}\n# default: {item.default}\n{item_sep}"
            )
            print(f'export {item.name}="{item.value}"\n')


def _adapt_xdg_meta_to_env_item(
    xdg_metadata: fs.XdgMetaContainer,
) -> t.Iterable[EnvItem]:
    return [
        EnvItem(
            x.purpose,
            x.var_name,
            x.default_value,
            fs.DirectoryManager.xdg_dir(x).as_posix(),
        )
        for x in xdg_metadata
    ]


def _load_flags() -> t.Iterable[EnvItem]:
    all_keys = [
        ENV_FF_ORCH_TRX_TIMESPLIT,
        ENV_FF_ORCH_TRX_OVERRIDE,
        ENV_FF_CLI_ENV_SHOW,
        ENV_FF_CLI_TEMPLATE_CREATE,
        ENV_FF_CLI_WORKPLAN_GEN,
        ENV_FF_CLI_WORKPLAN_PLAN,
        ENV_FF_CLI_WORKPLAN_STATUS,
        ENV_FF_CLI_WORKPLAN_COMPOSE,
    ]
    descriptions = get_ff_descriptions()

    return [
        EnvItem(
            purpose=descriptions[ff_var_name],
            name=ff_var_name,
            default=FF_OFF,
            value=FF_ON if is_feature_enabled(ff_var_name) else FF_OFF,
        )
        for ff_var_name in all_keys
    ]


def _load_xdg_meta() -> t.Iterable[EnvItem]:
    """Load all XDG environment variables and convert XDG metadata into generic EnvItem."""
    all_metadata = fs.load_xdg_metadata()
    return _adapt_xdg_meta_to_env_item(all_metadata)


class DisplayFormat(StrEnum):
    """Supported display formats."""

    INTERACTIVE = auto()
    """Print in a human-readable format."""
    EXPORT = auto()
    """Print a series of env var export statements."""


def _create_group_meta_map() -> dict[VariableGroup, Group]:
    return {
        VariableGroup.FILE_SYSTEM: Group("File-System", _load_xdg_meta),
        VariableGroup.FEATURE_FLAGS: Group("Feature Flags", _load_flags),
    }


@app.command()
def show(
    group: VariableGroup = VariableGroup.ALL,
    format: DisplayFormat = DisplayFormat.INTERACTIVE,
) -> None:
    """Display the active environment configuration."""
    group_meta_map = _create_group_meta_map()

    if group != VariableGroup.ALL:
        group_meta_map = {group: group_meta_map[group]}

    if format == DisplayFormat.EXPORT:
        _export(group_meta_map)
        return

    _interactive(group_meta_map)
