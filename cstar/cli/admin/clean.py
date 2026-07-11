import shutil
import typing as t
from pathlib import Path

import typer
from rich.console import Console
from rich.prompt import Prompt

from cstar.execution.file_system import DirectoryManager

app = typer.Typer()
console = Console()

short_help: t.Final[str] = "Clean up leftover data and files written to disk."
help: t.Final[str] = (
    f"{short_help}. WARNING: This will execute a destructive wipe "
    "of C-Star directories. Your data may be lost."
)

yes_help: t.Final[str] = "Execute clean operations without confirmation."


@app.command(
    name="clean",
    help="Clean up leftover data and files written to disk.",
    short_help=short_help,
)
def clean(
    yes: t.Annotated[bool, typer.Option("--yes", help=yes_help)] = False,
) -> None:
    """Clean up leftover data and files written to disk.

    Returns
    -------
    None
    """
    cstar_dir: t.Final[str] = "cstar"

    actions = {
        "C-Star package cache": [DirectoryManager.cache_home() / cstar_dir],
        "C-Star state files": [
            DirectoryManager.state_home() / cstar_dir,
            Path("~/.prefect/storage").expanduser().resolve(),
        ],
        "C-Star outputs and input datasets": [DirectoryManager.data_home() / cstar_dir],
        "C-Star user-level configuration": [DirectoryManager.config_home() / cstar_dir],
    }

    for prompt, paths in actions.items():
        if not yes:
            answer = Prompt.ask(
                f"Type y(es) and <enter> to delete {prompt!r} in ({', '.join(str(p) for p in paths)})"
            )
            if "y" not in answer:
                console.print(f"\t[yellow]Skipping[/yellow] {prompt!r} deletion\n")
                continue

        for path in paths:
            shutil.rmtree(path, ignore_errors=True)
            console.print(f"[red]Removed[/red] {str(path)} directory\n")
