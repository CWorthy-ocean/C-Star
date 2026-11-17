import os
import typing as t
from pathlib import Path

import typer

from cstar.base.env import (
    ENV_CSTAR_CATALOG,
    ENV_CSTAR_SYSTEM,
    CatalogRoots,
    catalog_entries,
    catalog_roots,
    load_user_config_for_repair,
)
from cstar.base.exceptions import CstarError
from cstar.entrypoint.utils import (
    ARG_CATALOG,
    ARG_CATALOG_HELP,
    ARG_REPLACE,
    ARG_REPLACE_HELP,
)
from cstar.system.layers import SystemFileConflict, install_system_file
from cstar.system.manager import (
    detection_would_select,
    display_path,
    reload_system_files,
    system_stack,
)
from cstar.system.spec import (
    DRAFT_LAYER,
    SYSTEM_FILE_SUFFIX,
    SystemFile,
    SystemFileError,
    read_system_file,
)

app = typer.Typer()

URL_PREFIXES: t.Final = ("http://", "https://", "git@")
"""What a URL or a git remote starts with; C-Star never fetches a system file."""


def looks_like_path(argument: str) -> bool:
    """Tell whether an argument names a system file rather than a system: it holds
    a path separator or ends in ``.yaml`` (a file in the current directory is
    ``./name``).
    """
    return (
        os.sep in argument or "/" in argument or argument.endswith(SYSTEM_FILE_SUFFIX)
    )


def read_draft(path: str) -> SystemFile:
    """Read a system file given by path (it need not be named after its system).

    Raises
    ------
    SystemFileError
        If the argument is a URL, which C-Star never fetches, or the file is
        refused.
    """
    if path.startswith(URL_PREFIXES):
        raise SystemFileError(
            Path(path),
            "it is a URL, and C-Star does not fetch system files: download and "
            "review it, then give its local path",
        )
    return read_system_file(Path(path).expanduser(), DRAFT_LAYER, check_name=False)


def current_catalog(roots: CatalogRoots) -> str:
    """The value `CSTAR_CATALOG` has now, spelled as a variable would hold it.

    An unset variable means your own catalog, the default root.
    """
    if entries := catalog_entries():
        return os.pathsep.join(entries)
    return roots.user.as_posix() if roots.user else ""


@app.command(name="add", help="Copy a system file into a catalog C-Star reads.")
def add(
    path: t.Annotated[
        str,
        typer.Argument(
            help="A system file on this machine; it is installed as <name>.yaml."
        ),
    ],
    catalog: t.Annotated[
        Path | None,
        typer.Option(ARG_CATALOG, help=ARG_CATALOG_HELP, file_okay=False),
    ] = None,
    replace: t.Annotated[
        bool, typer.Option(ARG_REPLACE, help=ARG_REPLACE_HELP)
    ] = False,
) -> None:
    """Validate a system file and copy it to ``<catalog>/systems/<name>.yaml``.

    Without `--catalog` the file goes to your own catalog, the first entry of
    ``CSTAR_CATALOG``. `--catalog` puts it into another catalog root, such as a
    group's shared one that you can write to; it is a checked copy and does not
    make a shared layer writable in any other sense.

    Parameters
    ----------
    path : str
        The system file, read from this machine.
    catalog : Path | None
        The catalog root to install into.
    replace : bool
        Whether to replace a different file of the same name.

    Raises
    ------
    typer.Exit
        Code 1 if the file is refused, no catalog can be chosen, or a different
        file is there and `replace` is not given.
    """
    load_user_config_for_repair()
    try:
        source = read_draft(path)
    except SystemFileError as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex
    roots = catalog_roots()
    root = catalog.expanduser().resolve() if catalog else roots.user
    if root is None:
        typer.echo(
            f"Error: the first {ENV_CSTAR_CATALOG} entry is not a local directory; "
            f"name the catalog to use with {ARG_CATALOG}.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        installed = install_system_file(source, root, replace)
    except SystemFileConflict as ex:
        typer.echo(f"Error: {ex}; pass {ARG_REPLACE} to replace it.", err=True)
        raise typer.Exit(1) from ex
    except (CstarError, OSError) as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1) from ex
    reload_system_files()

    name = source.spec.name
    where = display_path(installed.path)
    if installed.written:
        typer.echo(f"Added the system {name!r} as {where}")
    else:
        typer.echo(f"{where} is already identical; nothing to do.")

    if root not in roots.local:
        typer.echo(
            f"C-Star does not read {display_path(root)} yet. Add it after your own "
            "entries with:"
        )
        typer.echo(
            f"  cstar env set {ENV_CSTAR_CATALOG} {current_catalog(roots)}{os.pathsep}{root}"
        )
    else:
        layers = system_stack().layers
        in_effect = layers.files.get(name)
        below = layers.shadowed.get(name)
        if in_effect is not None and in_effect.path == installed.path and below:
            typer.echo(f"It shadows {display_path(below[0].path)}.")

    if detection_would_select(source.spec):
        typer.echo(f"C-Star selects {name!r} on this machine.")
    else:
        typer.echo(
            f"C-Star does not select {name!r} on this machine by itself. To use it "
            "here, run:"
        )
        typer.echo(f"  cstar env set {ENV_CSTAR_SYSTEM} {name}")
