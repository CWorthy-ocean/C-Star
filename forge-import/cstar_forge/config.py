"""Forge's disposable host-resolution layer.

A thin adapter over C-Star's own system layer (:mod:`cstar.system.manager`,
:mod:`cstar.base.env`): :func:`detect_system` is the single seam that asks
C-Star who we're running on, and everything below it only maps that name onto
the on-disk paths Forge has always used. When Forge relocates into C-Star this
module is dropped and callers take C-Star's equivalent host resolution
instead -- see :mod:`cstar_forge.forge.host`.
"""

from __future__ import annotations

import getpass
import json
import logging
import os
import platform
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from cstar.system.manager import HostNameEvaluator

from cstar_forge.domain_catalog import user_catalog_root

logger = logging.getLogger(__name__)


def _detect_user() -> str:
    """Best-effort current username; never raises (containers/CI may lack $USER)."""
    user = os.environ.get("USER")
    if user:
        return user
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


USER = _detect_user()


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


def _bouchet_scratch_root(home: Path) -> Path | None:
    """Best-effort per-user scratch root on Yale's Bouchet cluster.

    Bouchet exposes no ``$SCRATCH`` env var. Instead, each user's home carries
    per-project symlinks named ``scratch_pi_<pi-netid>`` (the suffix is
    unpredictable), and inside each of those the user has a subdirectory named
    after their own username. We glob ``home/scratch_pi_*``, keep only
    directories (``is_dir()`` follows symlinks, so the per-project symlinks
    themselves qualify), sort for determinism, and take the first match,
    appending the current username. Returns ``None`` if no such directory is
    found or the scan fails (e.g. a stale/permission-restricted mount behind
    one of the symlinks) -- this runs at module import via ``get_data_paths``,
    so it must never raise. C-Star's own ``BouchetSystemContext`` (see
    ``cstar/system/manager.py``) has no equivalent scratch helper of its own,
    so this heuristic stays forge-local.
    """
    try:
        candidates = sorted(p for p in home.glob("scratch_pi_*") if p.is_dir())
    except OSError:
        logger.warning(
            "Failed to scan %s for scratch_pi_* directories; falling back to a "
            "home-anchored layout. Set $SCRATCH to override.",
            home,
        )
        return None
    if not candidates:
        return None
    return candidates[0] / USER


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
    :func:`_bouchet_scratch_root`'s ``scratch_pi_*`` glob heuristic unless an
    explicit ``$SCRATCH`` override is set (consistent with the other HPC
    layouts above). ``$PROJECT``, when set, moves the data base to
    ``$PROJECT/cstar-forge-data``, like the other layouts. Falls back to the
    home-anchored layout -- ignoring ``$PROJECT`` -- if no scratch root can be
    found.
    """
    scratch_root: Path | None
    if "SCRATCH" in env:
        scratch_root = Path(env["SCRATCH"])
    else:
        scratch_root = _bouchet_scratch_root(home)

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
        # Per-user scratch: the root discovered by _bouchet_scratch_root
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
    ``main()``); importing :mod:`cstar_forge.config` must not create directories.
    """
    if dp is None:
        dp = paths
    for p in (dp.source_data, dp.catalog):
        _ensure_dir(p)
    return dp


# Initialize canonical instance
paths = get_data_paths()
system = detect_system()


def _hpc_scratch_root(
    system_tag: str, env: Mapping[str, str], home: Path
) -> Path | None:
    """Bare scratch root for HPC systems, ``None`` elsewhere.

    Per-system conventions, unchanged from before the C-Star system layer was
    adopted for machine identity: ``$SCRATCH`` (falling back to ``~/scratch``) on
    Perlmutter; ``$SCRATCH`` falling back to ``$PROJECT/scratch`` (or
    ``~/work/scratch``) on Anvil; ``$SCRATCH`` falling back to the globbed
    ``scratch_pi_*/<user>`` root on Bouchet, which exports no scratch env var at
    all. ``$SCRATCH`` is per-user on all of these machines, so no extra username
    layer is inserted. Non-HPC names (``"darwin_arm64"``, ``"linux_x86_64"``)
    return ``None`` even if the environment happens to carry ``$SCRATCH``.

    Adopting C-Star's ``CSTAR_SCRATCH_DIRS`` search (``$SCRATCH_DIR``,
    ``$LOCAL_SCRATCH``) is deferred to the relocation, when the forge data
    locations move under ``CSTAR_DATA_HOME`` anyway.
    """
    if system_tag == "perlmutter":
        return Path(env.get("SCRATCH", home / "scratch"))
    if system_tag == "anvil":
        project = Path(env.get("PROJECT", home / "work"))
        return Path(env.get("SCRATCH", project / "scratch"))
    if system_tag == "bouchet":
        if "SCRATCH" in env:
            return Path(env["SCRATCH"])
        return _bouchet_scratch_root(home)
    return None


# Home-relative default working roots a stored ``working_dir`` may carry, all
# rebased onto ``$SCRATCH/cstar/_forge_bp_runs/<relative part>`` on HPC. The current
# default (``~/cstar/_forge_bp_runs``) plus the two legacy sentinels from blueprints
# authored before this rename (``~/cstar-forge-run``, current since commit 3826bbee)
# and before that one (``~/cstar-forge-data/cstar-forge-run``), which the current
# prefix would otherwise miss -- leaving those runs writing into home. The roots are
# disjoint, so match order is irrelevant. Kept intentionally narrow: a bare
# ``~/cstar-forge-data`` match would also rebase the home-anchored source_data
# cache, which lives under that same base.
_DEFAULT_WORKING_ROOTS: tuple[str, ...] = (
    "cstar/_forge_bp_runs",
    "cstar-forge-run",
    "cstar-forge-data/cstar-forge-run",
)
_SCRATCH_WORKING_ROOT = "cstar/_forge_bp_runs"


def relocate_working_dir(
    working_dir,
    *,
    system_tag: str | None = None,
    env: dict | None = None,
    home: Path | None = None,
) -> Path:
    """Rebase a default-form ``working_dir`` onto the host's scratch data root.

    The ForgeBlueprint stores ``working_dir`` with a home-rooted default
    (``~/cstar/_forge_bp_runs/<name>``, or a legacy root -- ``~/cstar-forge-run`` or
    ``~/cstar-forge-data/cstar-forge-run`` -- from older blueprints). On HPC systems
    that path belongs on scratch, so any path under one of those default roots is
    rebased to ``$SCRATCH/cstar/_forge_bp_runs/<same relative part>``. Paths outside
    the default roots are a deliberate user choice and pass through untouched
    (expanded only).

    This is a stand-in for C-Star's eventual runtime override of the spec's
    ``working_dir``; keyword args exist for tests and default to the live host.
    """
    env = dict(os.environ) if env is None else env
    home = Path.home() if home is None else Path(home)
    system_tag = system if system_tag is None else system_tag

    wd = Path(working_dir).expanduser()
    scratch_root = _hpc_scratch_root(system_tag, env, home)
    if scratch_root is None:
        return wd
    for root in _DEFAULT_WORKING_ROOTS:
        try:
            rel = wd.relative_to(home / root)
        except ValueError:
            continue
        return scratch_root / _SCRATCH_WORKING_ROOT / rel
    if wd.is_relative_to(home):
        # HPC, but the path is home-rooted and matched no default root, so it is left
        # in home instead of being relocated to scratch. Usually a deliberate choice;
        # occasionally an unrecognized (e.g. very old) default that should have landed
        # on scratch -- worth a heads-up either way.
        logger.warning(
            "working_dir %s is under $HOME on an HPC system and was not relocated to "
            "scratch (%s); generated data will be written to home. If this was not "
            "intended, set working_dir under %s.",
            wd,
            scratch_root / _SCRATCH_WORKING_ROOT,
            home / _SCRATCH_WORKING_ROOT,
        )
    return wd


def resolve_host(working_dir):
    """Build the forge application's ``HostPaths`` from auto-detected Forge config.

    ``working_dir`` is the per-run artifact root (typically the spec's ``working_dir``,
    expanded, or a host override); everything the executor produces lands under it.
    Default-form paths (under ``~/cstar/_forge_bp_runs``) are rebased onto host
    scratch on HPC systems via :func:`relocate_working_dir`.

    This is Forge's **disposable** host provider: it auto-detects the machine (via
    C-Star's own :func:`detect_system`) for the source-data cache + machine identity.
    When the forge application relocates into C-Star, C-Star supplies an equivalent
    ``HostPaths`` from its own host resolution and this function is not carried over.
    """
    from cstar_forge.forge.host import HostPaths

    return HostPaths(
        working_dir=relocate_working_dir(working_dir),
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

    Backs the ``cstar forge show-paths`` CLI command (``cstar_forge/cli.py``).
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
