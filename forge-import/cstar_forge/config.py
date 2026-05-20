from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import os
import platform
import socket
from typing import Callable, Dict, Tuple, Optional, Any
import argparse
import json
import sys
import yaml

USER = os.environ.get("USER", None)
if USER is None:
    raise ValueError("USER environment variable is not set")

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
    - catalog (root for generated blueprints and rendered build trees; inner directory
      that directly contains ``blueprints`` and ``builds`` subdirectories)
    - blueprints (under ``catalog / "blueprints"`` by default)
    - builds (under ``catalog / "builds"`` by default)
    - models_yaml
    - builds_yaml
    """

    here: Path
    model_configs: Path
    source_data: Path
    input_data: Path
    scratch: Path
    catalog: Path
    blueprints: Path
    builds: Path
    models_yaml: Path
    builds_yaml: Path
    machines_yaml: Path


@dataclass(frozen=True)
class MachineConfig:
    """
    Machine-specific configuration loaded from machines.yml.

    Attributes
    ----------
    account : str, optional
        Account/project name for job submission.
    pes_per_node : int, optional
        Processing elements (cores) per node.
    queues : dict, optional
        Dictionary of queue names, with 'default' and optionally 'premium' keys.
    """
    account: Optional[str] = None
    pes_per_node: Optional[int] = None
    queues: Optional[Dict[str, str]] = None


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
SystemLayoutFn = Callable[[Path, dict], Tuple[Path, Path, Path]]
SYSTEM_LAYOUT_REGISTRY: Dict[str, SystemLayoutFn] = {}


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
def _layout_mac(home: Path, env: dict) -> Tuple[Path, Path, Path]:
    base = home / "cson-forge-data"
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = base / "cson-forge-run"
    return source_data, input_data, scratch


@register_system("RCAC_anvil")
def _layout_RCAC_anvil(home: Path, env: dict) -> Tuple[Path, Path, Path]:
    work = Path(env.get("WORK", home / "work"))
    scratch_root = Path(env.get("SCRATCH", work / "scratch"))

    base = work / "cson-forge-data"
    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = scratch_root / "cson-forge-run"
    return source_data, input_data, scratch


@register_system("NERSC_perlmutter")
def _layout_NERSC_perlmutter(home: Path, env: dict) -> Tuple[Path, Path, Path]:
    scratch_root = Path(env.get("SCRATCH", home / "scratch"))
    base = scratch_root / "cson-forge-data"

    source_data = base / "source-data"
    input_data = base / USER / "input-data"
    scratch = base / "cson-forge-run"
    return source_data, input_data, scratch


@register_system("unknown")
def _layout_unknown(home: Path, env: dict) -> Tuple[Path, Path, Path]:
    base = home / "cson-forge-data"
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = base / "cson-forge-run"
    return source_data, input_data, scratch


# --------------------------------------------------------
# Path factory
# --------------------------------------------------------

def default_catalog_inner_dir(source_data: Path) -> Path:
    """
    Default inner *catalog* directory: the folder that directly contains ``blueprints/`` and ``builds/``.

    The catalog lives alongside ``source-data`` inside the base data directory, e.g.
    ``~/cstar-forge-data/catalog/{blueprints,builds}``.
    """
    return source_data.parent.resolve() / "catalog"


def get_data_paths() -> DataPaths:
    """
    Return canonical data and project paths adapted to the system we're running on.
    """
    env = os.environ
    home = Path(env.get("SCRATCH", str(Path.home())))
    system_tag = _detect_system()

    layout_fn = SYSTEM_LAYOUT_REGISTRY.get(
        system_tag, SYSTEM_LAYOUT_REGISTRY["unknown"]
    )

    source_data, input_data, scratch = layout_fn(home, env)

    here = Path(__file__).resolve().parent
    model_configs = here / "model-configs"
    # Inner catalog dir: .../cstar_forge_data/catalog/{blueprints,builds}
    catalog = default_catalog_inner_dir(source_data)
    blueprints_dir = catalog / "blueprints"
    builds_dir = catalog / "builds"
    models_yaml = here / "models.yml"
    builds_yaml = here / "builds.yml"
    machines_yaml = here / "machines.yml"

    # ensure everything exists
    for p in (source_data, input_data, scratch, catalog, blueprints_dir, builds_dir, model_configs):
        _ensure_dir(p)

    return DataPaths(
        here=here,
        model_configs=model_configs,
        source_data=source_data,
        input_data=input_data,
        scratch=scratch,
        catalog=catalog,
        blueprints=blueprints_dir,
        builds=builds_dir,
        models_yaml=models_yaml,
        builds_yaml=builds_yaml,
        machines_yaml=machines_yaml,
    )


def with_catalog(paths: DataPaths, catalog: Path) -> DataPaths:
    """
    Return a copy of *paths* with ``catalog``, ``blueprints``, and ``builds`` rooted under *catalog*.

    ``blueprints`` and ``builds`` are set to ``catalog / "blueprints"`` and ``catalog / "builds"``
    respectively. Other fields (``here``, data roots, YAML paths) are unchanged.

    Intended for relocating the on-disk catalog without editing ``get_data_paths``;
    assign the result to ``cstar_forge.config.paths`` (and create directories as needed).
    """
    catalog = Path(catalog)
    return replace(
        paths,
        catalog=catalog,
        blueprints=catalog / "blueprints",
        builds=catalog / "builds",
    )


# --------------------------------------------------------
# Machine configuration loader
# --------------------------------------------------------

def load_machine_config(system_tag: str, machines_yaml_path: Path) -> MachineConfig:
    """
    Load machine-specific configuration from machines.yml.

    Parameters
    ----------
    system_tag : str
        System tag (e.g., "NERSC_perlmutter", "RCAC_anvil").
    machines_yaml_path : Path
        Path to the machines.yml file.

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

        return MachineConfig(
            account=machine_data.get("account"),
            pes_per_node=machine_data.get("pes_per_node"),
            queues=machine_data.get("queues"),
        )
    except Exception:
        # If there's any error loading the config, return empty config
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
        raise NotImplementedError(f"Cluster type not implemented for system: {system_tag}")

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
    conda_env: Optional[str]
    conda_prefix: Optional[str]
    kernel_name: Optional[str]
    kernel_version: Optional[str]

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
    hostname = socket.gethostname() or platform.node() or os.environ.get("HOSTNAME", "unknown")
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
        ksm = KernelSpecManager()
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
        except:
            kernel_version = None
    except Exception:
        pass

    # Try to get conda/micromamba environment
    conda_env = os.environ.get("CONDA_DEFAULT_ENV", None)
    conda_prefix = None
    if conda_env:
        conda_prefix = os.environ.get("CONDA_PREFIX", os.environ.get("MAMBA_ROOT_PREFIX", None))

    # Import the class from the current module to ensure it's accessible
    # This handles autoreload issues where the class might not be in scope
    current_module = sys.modules[__name__]
    EnvironmentInfo = getattr(current_module, 'EnvironmentInfo')

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
    """
    CLI for inspecting detected compute environment and configured paths.
    """
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
            print("")
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
machine_config = load_machine_config(system, paths.machines_yaml)
cluster_type = _default_cluster_type(system)


if __name__ == "__main__":
    raise SystemExit(main())
