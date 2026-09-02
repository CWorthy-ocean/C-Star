from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import platform
import socket
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

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
    """
    Central object holding key paths for data and local assets.

    Includes:
    - source_data
    - input_data
    - scratch
    - catalog (inner directory that directly contains the ``blueprints`` subdirectory)
    - blueprints (under ``catalog / "blueprints"`` by default)
    - models_yaml
    - builds_yaml
    """

    here: Path
    source_data: Path
    input_data: Path
    scratch: Path
    catalog: Path
    blueprints: Path
    models_yaml: Path
    builds_yaml: Path


# --------------------------------------------------------
# Hostname / system detection helpers
# --------------------------------------------------------


def _get_hostname() -> str:
    """Return lowercase hostname from multiple sources."""
    return (
        socket.gethostname()
        or platform.node()
        or os.environ.get("HOSTNAME")
        or "unknown"
    ).lower()


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
    so it must never raise. Cross-reference: C-Star's
    ``BouchetSystemContext.scratch_directory`` implements the same heuristic.
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


def _detect_system() -> str:
    """
    Return a tag for the current compute environment.

    Tags:
        - "MacOS"
        - "RCAC_anvil"
        - "NERSC_perlmutter"
        - "YCRC_bouchet"
        - "unknown"

    Extendable via SYSTEM_LAYOUT_REGISTRY.
    """
    system = platform.system().lower()
    if system == "darwin":
        return "MacOS"

    host = _get_hostname()
    if "anvil" in host:
        return "RCAC_anvil"

    # Check NERSC_HOST environment variable for Perlmutter
    if os.environ.get("NERSC_HOST", "").lower() == "perlmutter":
        return "NERSC_perlmutter"

    # Bouchet exports no distinguishing hostname substring; mirror C-Star's
    # BouchetSystemContext.is_match, which matches CLUSTER or SLURM_CLUSTER_NAME
    # exactly (no case folding -- both tools must agree on the detected system).
    if (
        os.environ.get("CLUSTER", "") == "bouchet"
        or os.environ.get("SLURM_CLUSTER_NAME", "") == "bouchet"
    ):
        return "YCRC_bouchet"

    return "unknown"


# --------------------------------------------------------
# System layout registry (pluggable)
# --------------------------------------------------------

# Now each layout returns 3 paths:
# (source_data, input_data, scratch)
SystemLayoutFn = Callable[[Path, dict], tuple[Path, Path, Path]]
SYSTEM_LAYOUT_REGISTRY: dict[str, SystemLayoutFn] = {}


def register_system(tag: str) -> Callable[[SystemLayoutFn], SystemLayoutFn]:
    """
    Decorator to register a system-specific path layout.

    The decorated function must accept (home: Path, env: dict)
    and return (source_data, input_data, scratch).
    """

    def decorator(func: SystemLayoutFn) -> SystemLayoutFn:
        SYSTEM_LAYOUT_REGISTRY[tag] = func
        return func

    return decorator


# --------------------------------------------------------
# Default system layouts
# --------------------------------------------------------


@register_system("MacOS")
def _layout_mac(home: Path, env: dict) -> tuple[Path, Path, Path]:
    base = home / "cstar-forge-data"
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = home / "cstar" / "_forge_bp_runs"
    return source_data, input_data, scratch


# $PROJECT is the standard cross-machine env var naming the (usually
# group-shared) project directory the data base lives under: when set, the
# data base is $PROJECT/cstar-forge-data on every HPC layout below. Anvil
# exports it natively (as the same directory as $WORK, which is deliberately
# NOT consulted: a user-overridden $PROJECT must move everything with it);
# elsewhere users set it.
@register_system("RCAC_anvil")
def _layout_RCAC_anvil(home: Path, env: dict) -> tuple[Path, Path, Path]:
    project = Path(env.get("PROJECT", home / "work"))
    scratch_root = Path(env.get("SCRATCH", project / "scratch"))

    base = project / "cstar-forge-data"
    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = scratch_root / "cstar" / "_forge_bp_runs"
    return source_data, input_data, scratch


@register_system("NERSC_perlmutter")
def _layout_NERSC_perlmutter(home: Path, env: dict) -> tuple[Path, Path, Path]:
    scratch_root = Path(env.get("SCRATCH", home / "scratch"))
    if "PROJECT" in env:
        base = Path(env["PROJECT"]) / "cstar-forge-data"
    else:
        base = scratch_root / "cstar-forge-data"

    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = scratch_root / "cstar" / "_forge_bp_runs"
    return source_data, input_data, scratch


@register_system("YCRC_bouchet")
def _layout_YCRC_bouchet(home: Path, env: dict) -> tuple[Path, Path, Path]:
    """Path layout for Yale's Bouchet cluster.

    Bouchet has no ``$SCRATCH`` env var, so the scratch root is discovered via
    :func:`_bouchet_scratch_root`'s ``scratch_pi_*`` glob heuristic unless an
    explicit ``$SCRATCH`` override is set (consistent with the other HPC
    layouts above). ``$PROJECT``, when set, moves the data base (not the run
    scratch) to ``$PROJECT/cstar-forge-data``, like the other layouts. Falls
    back to the home-anchored layout -- ignoring ``$PROJECT`` -- if no scratch
    root can be found.
    """
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
        return _layout_unknown(home, env)

    if "PROJECT" in env:
        # Shared project dir: source-data is group-shared, so input-data
        # needs the per-user layer the other HPC layouts carry.
        base = Path(env["PROJECT"]) / "cstar-forge-data"
        input_data = base / USER / "input-data"
    else:
        # Per-user scratch: the root discovered by _bouchet_scratch_root
        # already ends in the username, so no extra USER layer is added. That
        # also means source_data is per-user in this mode (not project-shared
        # as on Anvil) -- set $PROJECT to share it.
        base = scratch_root / "cstar-forge-data"
        input_data = base / "input-data"

    source_data = base / "source-data"
    scratch = scratch_root / "cstar" / "_forge_bp_runs"
    return source_data, input_data, scratch


@register_system("unknown")
def _layout_unknown(home: Path, env: dict) -> tuple[Path, Path, Path]:
    base = home / "cstar-forge-data"
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = home / "cstar" / "_forge_bp_runs"
    return source_data, input_data, scratch


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
    system_tag = _detect_system()

    layout_fn = SYSTEM_LAYOUT_REGISTRY.get(
        system_tag, SYSTEM_LAYOUT_REGISTRY["unknown"]
    )

    source_data, input_data, scratch = layout_fn(home, env)

    here = Path(__file__).resolve().parent
    # The catalog is deliberately home-anchored (unlike source_data/input_data/
    # scratch above, which get rebased onto HPC $SCRATCH/$WORK): catalog entries
    # are durable, user-registered content that must survive scratch purges, not
    # job-scoped working data. See user_catalog_root's docstring.
    catalog = user_catalog_root()
    blueprints_dir = catalog / "blueprints"
    models_yaml = here / "models.yaml"
    builds_yaml = here / "builds.yaml"

    if create:
        for p in (source_data, input_data, scratch, catalog, blueprints_dir):
            _ensure_dir(p)

    return DataPaths(
        here=here,
        source_data=source_data,
        input_data=input_data,
        scratch=scratch,
        catalog=catalog,
        blueprints=blueprints_dir,
        models_yaml=models_yaml,
        builds_yaml=builds_yaml,
    )


def ensure_data_dirs(dp: DataPaths | None = None) -> DataPaths:
    """Create the on-disk directories for *dp* (default: the module-level ``paths``).

    Call this from entry points that actually write data (e.g. ``run.py``'s
    ``main()``); importing :mod:`cstar_forge.config` must not create directories.
    """
    if dp is None:
        dp = paths
    for p in (dp.source_data, dp.input_data, dp.scratch, dp.catalog, dp.blueprints):
        _ensure_dir(p)
    return dp


def with_catalog(paths: DataPaths, catalog: Path) -> DataPaths:
    """
    Return a copy of *paths* with ``catalog`` and ``blueprints`` rooted under *catalog*.

    ``blueprints`` is set to ``catalog / "blueprints"``.
    Other fields (``here``, data roots, YAML paths) are unchanged.

    Intended for relocating the on-disk catalog without editing ``get_data_paths``;
    assign the result to ``cstar_forge.config.paths`` (and create directories as needed).
    """
    catalog = Path(catalog)
    return replace(
        paths,
        catalog=catalog,
        blueprints=catalog / "blueprints",
    )


# =========================================================
# Model execution (run) functions
# =========================================================


class ClusterType:
    """Constants for cluster/scheduler types."""

    LOCAL = "LocalCluster"
    SLURM = "SLURMCluster"
    PBS = "PBSCluster"  # For future extensibility


def _default_cluster_type(system_tag: str) -> str:
    """
    Return the default cluster type based on the system tag.

    Parameters
    ----------
    system_tag : str
        System tag (e.g., "MacOS", "NERSC_perlmutter").

    Returns
    -------
    str
        "LocalCluster" for MacOS/unknown, "SLURMCluster" for other systems.
    """
    if system_tag in ["MacOS", "unknown"]:
        return ClusterType.LOCAL
    elif system_tag in ["RCAC_anvil", "NERSC_perlmutter", "YCRC_bouchet"]:
        return ClusterType.SLURM
    else:
        raise NotImplementedError(
            f"Cluster type not implemented for system: {system_tag}"
        )


# --------------------------------------------------------
# Environment and Machine Information
# --------------------------------------------------------


@dataclass
class EnvironmentInfo:
    """Information about the execution environment and machine."""

    hostname: str
    system_tag: str
    os_info: str
    python_version: str
    python_executable: str
    conda_env: str | None
    conda_prefix: str | None
    kernel_name: str | None
    kernel_version: str | None

    @property
    def env_info(self) -> str:
        """Formatted conda/micromamba environment information."""
        if self.conda_env:
            return f"{self.conda_env} ({self.conda_prefix})"
        return "Not in conda/micromamba environment"

    @property
    def kernel_spec(self) -> str:
        """Formatted kernel information."""
        if self.kernel_name and self.kernel_version:
            return f"{self.kernel_name} ({self.kernel_version})"
        elif self.kernel_name:
            return self.kernel_name
        return "unknown"


def get_environment_info() -> EnvironmentInfo:
    """
    Collect and return information about the execution environment and machine.

    Returns:
        EnvironmentInfo: Dataclass containing machine and environment details.
    """
    # Get machine information
    hostname = (
        socket.gethostname() or platform.node() or os.environ.get("HOSTNAME", "unknown")
    )
    system_tag = _detect_system()
    os_info = f"{platform.system()} {platform.release()} ({platform.machine()})"

    # Get environment information
    python_version = sys.version.split()[0]
    python_executable = sys.executable

    # Try to get kernel information
    kernel_name = None
    kernel_version = None
    try:
        from jupyter_client.kernelspec import KernelSpecManager

        KernelSpecManager()  # verifies jupyter_client is importable
        # Try to get current kernel name from environment or kernel spec
        kernel_name = os.environ.get("JPY_KERNEL_NAME", None)
        if not kernel_name:
            # Try to infer from Python executable path
            if "cstar-forge" in python_executable:
                kernel_name = "cstar-forge-env"
            else:
                kernel_name = None
        try:
            import ipykernel

            kernel_version = f"ipykernel {ipykernel.__version__}"
        except Exception:
            kernel_version = None
    except Exception:
        pass

    # Try to get conda/micromamba environment
    conda_env = os.environ.get("CONDA_DEFAULT_ENV", None)
    conda_prefix = None
    if conda_env:
        conda_prefix = os.environ.get(
            "CONDA_PREFIX", os.environ.get("MAMBA_ROOT_PREFIX", None)
        )

    # Import the class from the current module to ensure it's accessible
    # This handles autoreload issues where the class might not be in scope
    current_module = sys.modules[__name__]
    EnvironmentInfo = current_module.EnvironmentInfo

    return EnvironmentInfo(
        hostname=hostname,
        system_tag=system_tag,
        os_info=os_info,
        python_version=python_version,
        python_executable=python_executable,
        conda_env=conda_env,
        conda_prefix=conda_prefix,
        kernel_name=kernel_name,
        kernel_version=kernel_version,
    )


# --------------------------------------------------------
# CLI
# --------------------------------------------------------


def _paths_to_dict(dp: DataPaths) -> dict:
    return {k: str(v) for k, v in dp.__dict__.items()}


def main(argv: list[str] | None = None) -> int:
    """CLI for inspecting detected compute environment and configured paths."""
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Inspect cstar-forge data path configuration."
    )

    subparsers = parser.add_subparsers(dest="command")

    # show-paths command
    show_parser = subparsers.add_parser(
        "show-paths",
        help="Show detected system and configured data paths.",
    )
    show_parser.add_argument(
        "--json",
        action="store_true",
        help="Output paths as JSON instead of human-readable text.",
    )

    if not argv:
        argv = ["show-paths"]

    args = parser.parse_args(argv)

    if args.command == "show-paths":
        system_tag = _detect_system()
        hostname = _get_hostname()
        dp = paths

        if args.json:
            payload = {
                "system": system_tag,
                "hostname": hostname,
                "paths": _paths_to_dict(dp),
            }
            print(json.dumps(payload, indent=2))
        else:
            print(f"System tag : {system_tag}")
            print(f"Hostname   : {hostname}")
            print()
            print("Paths:")
            for key, value in _paths_to_dict(dp).items():
                print(f"  {key:12s} -> {value}")

        return 0

    parser.print_help()
    return 1


# Initialize canonical instance
paths = get_data_paths()
system = _detect_system()
system_id = system  # Alias for compatibility
cluster_type = _default_cluster_type(system)


def _hpc_scratch_root(system_tag: str, env: dict, home: Path) -> Path | None:
    """Bare scratch root for HPC systems, ``None`` elsewhere.

    Mirrors the env-var conventions of the system layouts above ($SCRATCH on
    Perlmutter; $SCRATCH falling back to $PROJECT/scratch on Anvil; $SCRATCH
    falling back to a globbed ``scratch_pi_*/<user>`` root on Bouchet, which
    exports no $SCRATCH at all). $SCRATCH is per-user on all of these
    machines, so no extra username layer is inserted.
    """
    if system_tag == "NERSC_perlmutter":
        return Path(env.get("SCRATCH", home / "scratch"))
    if system_tag == "RCAC_anvil":
        project = Path(env.get("PROJECT", home / "work"))
        return Path(env.get("SCRATCH", project / "scratch"))
    if system_tag == "YCRC_bouchet":
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
# ``~/cstar-forge-data`` match would also rebase the mac/unknown source_data and
# input_data caches, which live under that same base.
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

    This is Forge's **disposable** host provider: it auto-detects the machine (NERSC /
    RCAC / local) for the source-data cache + machine identity. When the forge
    application relocates into C-Star, C-Star supplies an equivalent ``HostPaths`` from
    its own host resolution and this function is not carried over.
    """
    from cstar_forge.forge.host import HostPaths

    return HostPaths(
        working_dir=relocate_working_dir(working_dir),
        source_data_cache=paths.source_data,
        system=system,
    )


if __name__ == "__main__":
    raise SystemExit(main())
