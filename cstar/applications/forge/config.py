"""Forge's host-resolution layer.

:func:`get_data_paths` reports the durable source-data cache (owned by
:meth:`cstar.execution.file_system.DirectoryManager.source_data_home`) and the
user catalog; :func:`resolve_host` packages the cache with the run's working
directory and C-Star's name for this machine into
:class:`cstar.applications.forge.host.HostPaths`.
"""

from __future__ import annotations

import json
import os
import platform
import socket
from dataclasses import dataclass
from pathlib import Path

from cstar.applications.forge.host import HostPaths
from cstar.catalog.domain_catalog import user_catalog_root
from cstar.execution.file_system import DirectoryManager
from cstar.system.manager import HostNameEvaluator


def _ensure_dir(path: Path) -> Path:
    """Create directory if needed and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


@dataclass(frozen=True)
class DataPaths:
    """Central object holding key paths for data and local assets.

    Includes:
    - source_data
    - catalog (durable, user-registered content; see ``user_catalog_root``)
    """

    source_data: Path
    catalog: Path


def get_data_paths() -> DataPaths:
    """Return the source-data cache and catalog paths for this machine.

    Only builds ``Path`` objects; :func:`ensure_data_dirs` creates the directories.
    Importing this module must not have filesystem side effects.
    """
    # The catalog is deliberately home-anchored (unlike source_data, which follows
    # the project, then scratch, file system): catalog entries are durable,
    # user-registered content. See user_catalog_root's docstring.
    return DataPaths(
        source_data=DirectoryManager.source_data_home(), catalog=user_catalog_root()
    )


def ensure_data_dirs() -> DataPaths:
    """Create the source-data cache and catalog directories and return their paths.

    Call this from entry points that actually write data (e.g. ``run.py``'s
    ``main()``); importing :mod:`cstar.applications.forge.config` must not create
    directories. The cache is created by
    :meth:`DirectoryManager.ensure_source_data_home`, which adopts a cache at the
    legacy ``cstar-forge-data/source-data`` location.
    """
    DirectoryManager.ensure_source_data_home()
    dp = get_data_paths()
    _ensure_dir(dp.catalog)
    return dp


# Initialize canonical instance
paths = get_data_paths()


def resolve_host(working_dir: str | Path) -> HostPaths:
    """Build the forge application's ``HostPaths`` for this machine.

    ``working_dir`` is the per-run artifact root: the blueprint's effective working
    directory or a ``--working-dir`` override. It is used as written (after ``~``
    expansion), like every other C-Star application; inside a workplan the step's
    assigned directory arrives here already applied. The source-data cache comes from
    ``paths`` and the machine name from C-Star's ``HostNameEvaluator``.
    """
    return HostPaths(
        working_dir=Path(working_dir).expanduser(),
        source_data_cache=paths.source_data,
        system=HostNameEvaluator().name,
    )


def _paths_to_dict(dp: DataPaths) -> dict:
    return {k: str(v) for k, v in dp.__dict__.items()}


def _hostname() -> str:
    """Best-effort hostname for display; never raises (containers may lack one)."""
    return (
        socket.gethostname()
        or platform.node()
        or os.environ.get("HOSTNAME")
        or "unknown"
    )


def format_paths(*, as_json: bool = False) -> str:
    """Render the detected system and configured data paths as a string.

    Backs the ``cstar forge show-paths`` CLI command.
    """
    system_tag = HostNameEvaluator().name
    hostname = _hostname()
    dp = paths

    if as_json:
        payload = {
            "system": system_tag,
            "hostname": hostname,
            "paths": _paths_to_dict(dp),
        }
        return json.dumps(payload, indent=2)

    lines = [
        f"System tag : {system_tag}",
        f"Hostname   : {hostname}",
        "",
        "Paths:",
    ]
    lines.extend(f"  {key:12s} -> {value}" for key, value in _paths_to_dict(dp).items())
    return "\n".join(lines)
