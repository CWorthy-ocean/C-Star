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

import yaml

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
    machines_yaml: Path


@dataclass(frozen=True)
class MachineConfig:
    """
    Machine-specific configuration loaded from machines.yaml.

    Attributes
    ----------
    account : str, optional
        Account/project name for job submission.
    pes_per_node : int, optional
        Processing elements (cores) per node.
    queues : dict, optional
        Dictionary of queue names, with 'default' and optionally 'premium' keys.
    """

    account: str | None = None
    pes_per_node: int | None = None
    queues: dict[str, str] | None = None


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


def _detect_system() -> str:
    """
    Return a tag for the current compute environment.

    Tags:
        - "MacOS"
        - "RCAC_anvil"
        - "NERSC_perlmutter"
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
    scratch = base / "cstar-forge-run"
    return source_data, input_data, scratch


@register_system("RCAC_anvil")
def _layout_RCAC_anvil(home: Path, env: dict) -> tuple[Path, Path, Path]:
    work = Path(env.get("WORK", home / "work"))
    scratch_root = Path(env.get("SCRATCH", work / "scratch"))

    base = work / "cstar-forge-data"
    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = scratch_root / "cstar-forge-run"
    return source_data, input_data, scratch


@register_system("NERSC_perlmutter")
def _layout_NERSC_perlmutter(home: Path, env: dict) -> tuple[Path, Path, Path]:
    scratch_root = Path(env.get("SCRATCH", home / "scratch"))
    base = scratch_root / "cstar-forge-data"

    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = base / "cstar-forge-run"
    return source_data, input_data, scratch


@register_system("unknown")
def _layout_unknown(home: Path, env: dict) -> tuple[Path, Path, Path]:
    base = home / "cstar-forge-data"
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = base / "cstar-forge-run"
    return source_data, input_data, scratch


# --------------------------------------------------------
# Path factory
# --------------------------------------------------------


def default_catalog_inner_dir(input_data: Path) -> Path:
    """
    Default inner *catalog* directory: the folder that directly contains ``blueprints/``.

    The catalog lives alongside ``input-data`` inside the base data directory, e.g.
    ``~/cstar-forge-data/catalog/blueprints/``.
    """
    return input_data.parent.resolve() / "catalog"


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
    # Inner catalog dir: .../cstar_forge_data/catalog/blueprints/
    catalog = default_catalog_inner_dir(input_data)
    blueprints_dir = catalog / "blueprints"
    models_yaml = here / "models.yaml"
    builds_yaml = here / "builds.yaml"
    machines_yaml = here / "machines.yaml"

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
        machines_yaml=machines_yaml,
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


# --------------------------------------------------------
# Machine configuration loader
# --------------------------------------------------------


def load_machine_config(system_tag: str, machines_yaml_path: Path) -> MachineConfig:
    """
    Load machine-specific configuration from machines.yaml.

    Parameters
    ----------
    system_tag : str
        System tag (e.g., "NERSC_perlmutter", "RCAC_anvil").
    machines_yaml_path : Path
        Path to the machines.yaml file.

    Returns
    -------
    MachineConfig
        Machine configuration object. Returns empty config if machine not found
        or file doesn't exist.
    """
    if not machines_yaml_path.exists():
        return MachineConfig()

    try:
        with machines_yaml_path.open("r") as f:
            machines_data = yaml.safe_load(f) or {}

        machine_data = machines_data.get(system_tag, {})
        if not isinstance(machine_data, dict):
            machine_data = {}

        return MachineConfig(
            account=machine_data.get("account"),
            pes_per_node=machine_data.get("pes_per_node"),
            queues=machine_data.get("queues"),
        )
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "Failed to load machine config for %r from %s: %s",
            system_tag,
            machines_yaml_path,
            exc,
        )
        return MachineConfig()


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
    elif system_tag in ["RCAC_anvil", "NERSC_perlmutter"]:
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
                kernel_name = "cstar-forge-v0"
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


def _load_machine_config_from_catalog(system_tag: str) -> MachineConfig:
    """Load machine config from the default DomainCatalog (internal cstar-forge catalog)."""
    from cstar_forge.domain_catalog import default_catalog

    try:
        data = default_catalog.machine_data(system_tag)
    except (KeyError, OSError, yaml.YAMLError) as exc:
        # Machine not in the catalog, or its YAML is missing/unreadable -- fall
        # back to an empty config rather than failing import-time module setup.
        logger.warning(
            "Failed to load machine config for %r from catalog: %s", system_tag, exc
        )
        return MachineConfig()

    return MachineConfig(
        account=data.get("account"),
        pes_per_node=data.get("pes_per_node"),
        queues=data.get("queues"),
    )


# Initialize canonical instance
paths = get_data_paths()
system = _detect_system()
system_id = system  # Alias for compatibility
machine_config = _load_machine_config_from_catalog(system)
cluster_type = _default_cluster_type(system)


def _hpc_scratch_data_root(system_tag: str, env: dict, home: Path) -> Path | None:
    """Scratch-rooted ``cstar-forge-data`` base for HPC systems, ``None`` elsewhere.

    Mirrors the env-var conventions of the system layouts above ($SCRATCH on
    Perlmutter; $SCRATCH falling back to $WORK/scratch on Anvil). $SCRATCH is
    per-user on both machines, so no extra username layer is inserted.
    """
    if system_tag == "NERSC_perlmutter":
        return Path(env.get("SCRATCH", home / "scratch")) / "cstar-forge-data"
    if system_tag == "RCAC_anvil":
        work = Path(env.get("WORK", home / "work"))
        return Path(env.get("SCRATCH", work / "scratch")) / "cstar-forge-data"
    return None


def relocate_working_dir(
    working_dir,
    *,
    system_tag: str | None = None,
    env: dict | None = None,
    home: Path | None = None,
) -> Path:
    """Rebase a default-form ``working_dir`` onto the host's scratch data root.

    The ForgeBlueprint stores ``working_dir`` with a home-rooted default
    (``~/cstar-forge-data/<name>``). On HPC systems that path belongs on scratch, so
    any path under ``~/cstar-forge-data`` is rebased to
    ``$SCRATCH/cstar-forge-data/<same relative part>``. Paths outside the default
    root are a deliberate user choice and pass through untouched (expanded only).

    This is a stand-in for C-Star's eventual runtime override of the spec's
    ``working_dir``; keyword args exist for tests and default to the live host.
    """
    env = dict(os.environ) if env is None else env
    home = Path.home() if home is None else Path(home)
    system_tag = system if system_tag is None else system_tag

    wd = Path(working_dir).expanduser()
    scratch_root = _hpc_scratch_data_root(system_tag, env, home)
    if scratch_root is None:
        return wd
    try:
        rel = wd.relative_to(home / "cstar-forge-data")
    except ValueError:
        return wd
    return scratch_root / rel


def resolve_host(working_dir):
    """Build the forge application's ``HostPaths`` from auto-detected Forge config.

    ``working_dir`` is the per-run artifact root (typically the spec's ``working_dir``,
    expanded, or a host override); everything the executor produces lands under it.
    Default-form paths (under ``~/cstar-forge-data``) are rebased onto host scratch on
    HPC systems via :func:`relocate_working_dir`.

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
        machine_config=machine_config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
