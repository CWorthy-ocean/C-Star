"""The system files C-Star can see: yours, your group's, and the bundled ones.

A ``systems/`` directory under each local `CSTAR_CATALOG` root holds system
files, one ``<name>.yaml`` each, above the files C-Star ships. The first file
found for a name wins, top first, and replaces any below it whole. A file that
cannot be used is refused, with its reason, and skipped.

This module reads files and reports what it found; it logs nothing, so the
caller decides what to warn about and how often. Like `cstar.system.spec`, it
imports nothing from `cstar.system.manager` or `cstar.catalog`.
"""

import os
import stat
import typing as t
from dataclasses import dataclass
from pathlib import Path

from cstar.base.env import CatalogRoots
from cstar.system.spec import (
    BUNDLED_SYSTEMS_DIR,
    SHARED_LAYER,
    SYSTEM_FILE_SUFFIX,
    SYSTEMS_DIR_NAME,
    USER_LAYER,
    SystemFile,
    SystemFileError,
    read_system_file,
)

if t.TYPE_CHECKING:
    from collections.abc import Sequence

REASON_WORLD_WRITABLE: t.Final = "it is writable by every user"
"""Why a world-writable system file is refused (see `world_writable`)."""
REASON_DIR_WORLD_WRITABLE: t.Final = (
    "its directory {directory} is writable by every user"
)
"""Why a system file in a world-writable directory is refused."""


@dataclass(frozen=True)
class RefusedSystemFile:
    """A system file C-Star found and does not use."""

    path: Path
    """The file."""
    layer: str
    """The layer it was found in."""
    reason: str
    """Why it is not used."""

    @property
    def name(self) -> str:
        """The system the file would describe: its name without ``.yaml``."""
        return self.path.stem


@dataclass(frozen=True)
class SystemLayers:
    """Every system file C-Star found, and which of them it uses."""

    searched: tuple[Path, ...]
    """The ``systems/`` directories read, top first, the bundled one last."""
    files: dict[str, SystemFile]
    """The file in effect for each name, the top layer's first."""
    shadowed: dict[str, tuple[SystemFile, ...]]
    """For a name found in more than one layer, the files below the one in
    effect, top first."""
    refused: tuple[RefusedSystemFile, ...]
    """The files found and not used, top first."""

    def refusals(self, name: str) -> list[RefusedSystemFile]:
        """The refused files that would describe `name`, top first."""
        return [r for r in self.refused if r.name == name]


def systems_dir(root: Path) -> Path:
    """Return the directory of system files under a catalog root."""
    return root / SYSTEMS_DIR_NAME


def world_writable(path: Path) -> bool:
    """Tell whether every user may write `path` (following a symbolic link).

    Raises
    ------
    OSError
        If the path cannot be examined.
    """
    return bool(os.stat(path).st_mode & stat.S_IWOTH)


def read_layer(
    directory: Path, layer: str
) -> tuple[list[SystemFile], list[RefusedSystemFile]]:
    """Read the system files of one layer, in file-name order.

    A file is refused when it cannot be read or validated (see
    `read_system_file`), when every user may write it, or when every user may
    write its directory: such a file runs commands for whoever reads it.

    Parameters
    ----------
    directory : Path
        The layer's ``systems/`` directory; nothing is found when it does not exist.
    layer : str
        The layer's name, for the files read.

    Returns
    -------
    tuple[list[SystemFile], list[RefusedSystemFile]]
        The files that can be used, and those refused with their reasons.
    """
    try:
        if not directory.is_dir():
            return [], []
        paths = sorted(directory.glob(f"*{SYSTEM_FILE_SUFFIX}"))
        open_dir = world_writable(directory)
    except OSError as ex:
        return [], [RefusedSystemFile(directory, layer, str(ex))]

    found: list[SystemFile] = []
    refused: list[RefusedSystemFile] = []
    for path in paths:
        try:
            if open_dir:
                reason = REASON_DIR_WORLD_WRITABLE.format(directory=directory)
            elif world_writable(path):
                reason = REASON_WORLD_WRITABLE
            else:
                found.append(read_system_file(path, layer))
                continue
        except SystemFileError as ex:
            reason = ex.reason
        except OSError as ex:
            reason = str(ex)
        refused.append(RefusedSystemFile(path, layer, reason))
    return found, refused


def read_system_layers(
    roots: CatalogRoots, bundled: "Sequence[SystemFile]"
) -> SystemLayers:
    """Read the system files of every local catalog root, over the bundled ones.

    Parameters
    ----------
    roots : CatalogRoots
        The local catalog roots, top first (see `cstar.base.env.catalog_roots`).
    bundled : Sequence[SystemFile]
        The system files C-Star ships, already read.

    Returns
    -------
    SystemLayers
    """
    layers = [(USER_LAYER, roots.user)] if roots.user else []
    layers += [(SHARED_LAYER, root) for root in roots.shared]

    searched: list[Path] = []
    files: dict[str, SystemFile] = {}
    shadowed: dict[str, list[SystemFile]] = {}
    refused: list[RefusedSystemFile] = []

    def place(found: "Sequence[SystemFile]") -> None:
        for f in found:
            if (name := f.spec.name) in files:
                shadowed.setdefault(name, []).append(f)
            else:
                files[name] = f

    for layer, root in layers:
        directory = systems_dir(root)
        searched.append(directory)
        found, rejected = read_layer(directory, layer)
        place(found)
        refused += rejected
    searched.append(BUNDLED_SYSTEMS_DIR)
    place(bundled)
    return SystemLayers(
        searched=tuple(searched),
        files=files,
        shadowed={name: tuple(below) for name, below in shadowed.items()},
        refused=tuple(refused),
    )
