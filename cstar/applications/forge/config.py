"""Forge's host-resolution layer.

:func:`detect_system` is the single seam that asks C-Star's own system layer
(:mod:`cstar.system.manager`) who we're running on; ``SYSTEM_LAYOUT_REGISTRY`` maps
that name onto the durable source-data cache; and :func:`resolve_host` packages the
result with the run's working directory into
:class:`cstar.applications.forge.host.HostPaths`.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from cstar.applications.forge.host import HostPaths
from cstar.catalog.domain_catalog import user_catalog_root
from cstar.system.manager import (
    HostNameEvaluator,
    current_user,
    find_bouchet_scratch_root,
)

logger = logging.getLogger(__name__)

USER = current_user()


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


# --------------------------------------------------------
# System identity
# --------------------------------------------------------


def detect_system() -> str:
    """Return C-Star's name for the current compute environment.

    The single seam onto C-Star's own machine identity
    (:class:`cstar.system.manager.HostNameEvaluator`): the LMOD-reported
    ``SYSHOST``/``SYSTEM_NAME`` when set, else the first registered
    ``SystemContext`` whose ``is_match()`` is true (e.g. ``"anvil"``,
    ``"perlmutter"``, ``"bouchet"``), else a platform-derived tag such as
    ``"darwin_arm64"`` or ``"linux_x86_64"``. No other detection heuristic may
    exist alongside this one -- see ``SYSTEM_LAYOUT_REGISTRY`` below.
    """
    return HostNameEvaluator().name


# --------------------------------------------------------
# System layout registry (pluggable)
# --------------------------------------------------------

# Each layout returns just source_data; catalog is always user_catalog_root().
SystemLayoutFn = Callable[[Path, Mapping[str, str]], Path]
SYSTEM_LAYOUT_REGISTRY: dict[str, SystemLayoutFn] = {}


def register_system(tag: str) -> Callable[[SystemLayoutFn], SystemLayoutFn]:
    """
    Decorator to register a system-specific path layout.

    The decorated function must accept (home: Path, env: Mapping[str, str])
    and return source_data.
    """

    def decorator(func: SystemLayoutFn) -> SystemLayoutFn:
        SYSTEM_LAYOUT_REGISTRY[tag] = func
        return func

    return decorator


# --------------------------------------------------------
# Default system layouts
# --------------------------------------------------------


def _layout_home_anchored(home: Path, env: Mapping[str, str]) -> Path:
    """Home-anchored source-data layout.

    Registered directly under the local/dev names C-Star reports for macOS
    and Linux workstations (``"darwin_arm64"``, ``"linux_x86_64"``,
    ``"linux_aarch64"``), and also used as the fallback
    ``SYSTEM_LAYOUT_REGISTRY.get(name, ...)`` default for any other system
    name C-Star reports that has no dedicated HPC layout below (a deliberate,
    documented default -- not a second detection heuristic).
    """
    return home / "cstar-forge-data" / "source-data"


for _tag in ("darwin_arm64", "linux_x86_64", "linux_aarch64"):
    SYSTEM_LAYOUT_REGISTRY[_tag] = _layout_home_anchored


# $PROJECT is the standard cross-machine env var naming the (usually
# group-shared) project directory the data base lives under: when set, the
# data base is $PROJECT/cstar-forge-data on every HPC layout below. Anvil
# exports it natively (as the same directory as $WORK, which is deliberately
# NOT consulted: a user-overridden $PROJECT must move everything with it);
# elsewhere users set it.
@register_system("anvil")
def _layout_anvil(home: Path, env: Mapping[str, str]) -> Path:
    project = Path(env.get("PROJECT", home / "work"))
    return project / "cstar-forge-data" / "source-data"


@register_system("perlmutter")
def _layout_perlmutter(home: Path, env: Mapping[str, str]) -> Path:
    if "PROJECT" in env:
        base = Path(env["PROJECT"]) / "cstar-forge-data"
    else:
        scratch_root = Path(env.get("SCRATCH", home / "scratch"))
        base = scratch_root / "cstar-forge-data"
    return base / "source-data"


@register_system("bouchet")
def _layout_bouchet(home: Path, env: Mapping[str, str]) -> Path:
    """Path layout for Yale's Bouchet cluster.

    Bouchet has no ``$SCRATCH`` env var, so the scratch root is discovered via
    C-Star's :func:`cstar.system.manager.find_bouchet_scratch_root` (the
    ``scratch_pi_*`` directories under home) unless an explicit ``$SCRATCH``
    override is set (consistent with the other HPC layouts above). ``$PROJECT``,
    when set, moves the data base to
    ``$PROJECT/cstar-forge-data``, like the other layouts. Falls back to the
    home-anchored layout -- ignoring ``$PROJECT`` -- if no scratch root can be
    found.
    """
    scratch_root: Path | None
    if "SCRATCH" in env:
        scratch_root = Path(env["SCRATCH"])
    else:
        scratch_root = find_bouchet_scratch_root(home, USER)

    if scratch_root is None:
        logger.warning(
            "No scratch_pi_* directory found under %s on Bouchet; falling back "
            "to a home-anchored layout. Set $SCRATCH to override.",
            home,
        )
        return _layout_home_anchored(home, env)

    if "PROJECT" in env:
        base = Path(env["PROJECT"]) / "cstar-forge-data"
    else:
        # Per-user scratch: the root discovered by find_bouchet_scratch_root
        # already ends in the username, so no extra USER layer is added. This
        # also means source_data is per-user in this mode (not project-shared
        # as on Anvil) -- set $PROJECT to share it.
        base = scratch_root / "cstar-forge-data"

    return base / "source-data"


# --------------------------------------------------------
# Path factory
# --------------------------------------------------------


def get_data_paths(create: bool = False) -> DataPaths:
    """Return canonical data and project paths adapted to the system we're running on.

    Only builds ``Path`` objects by default; pass ``create=True`` (or call
    :func:`ensure_data_dirs` afterwards) to also create the directories on disk.
    Importing this module must not have filesystem side effects.
    """
    env = os.environ
    home = Path(env.get("SCRATCH", str(Path.home())))
    system_tag = detect_system()

    layout_fn = SYSTEM_LAYOUT_REGISTRY.get(system_tag, _layout_home_anchored)
    source_data = layout_fn(home, env)

    # The catalog is deliberately home-anchored (unlike source_data above,
    # which gets rebased onto HPC $SCRATCH/$WORK): catalog entries are
    # durable, user-registered content that must survive scratch purges, not
    # job-scoped working data. See user_catalog_root's docstring.
    catalog = user_catalog_root()

    if create:
        for p in (source_data, catalog):
            _ensure_dir(p)

    return DataPaths(source_data=source_data, catalog=catalog)


def ensure_data_dirs(dp: DataPaths | None = None) -> DataPaths:
    """Create the on-disk directories for *dp* (default: the module-level ``paths``).

    Call this from entry points that actually write data (e.g. ``run.py``'s
    ``main()``); importing :mod:`cstar.applications.forge.config` must not create directories.
    """
    if dp is None:
        dp = paths
    for p in (dp.source_data, dp.catalog):
        _ensure_dir(p)
    return dp


# Initialize canonical instance
paths = get_data_paths()
system = detect_system()


def resolve_host(working_dir: str | Path) -> HostPaths:
    """Build the forge application's ``HostPaths`` for this machine.

    ``working_dir`` is the per-run artifact root: the blueprint's effective working
    directory or a ``--working-dir`` override. It is used as written (after ``~``
    expansion), like every other C-Star application; inside a workplan the step's
    assigned directory arrives here already applied. The source-data cache and machine
    identity come from this module's system detection.
    """
    return HostPaths(
        working_dir=Path(working_dir).expanduser(),
        source_data_cache=paths.source_data,
        system=system,
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
    system_tag = detect_system()
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
