# Machine configuration

C-STAR Forge uses a configuration system to manage paths and system-specific settings.

## System Detection

The system is automatically detected based on the hostname and platform. Supported systems:

- `MacOS` - macOS systems (detected via `platform.system() == "darwin"`)
- `RCAC_anvil` - Anvil HPC system (detected via hostname containing "anvil")
- `NERSC_perlmutter` - Perlmutter HPC system (detected via `NERSC_HOST` environment variable)
- `unknown` - Fallback for other systems

## Data Paths

Data paths are automatically configured based on the detected system. The `config.paths` object (of type `DataPaths`) provides access to all configured paths:

- **Source data** (`config.paths.source_data`): External datasets (GLORYS, UNIFIED_BGC, SRTM15, etc.)
- **Input data** (`config.paths.input_data`): Generated ROMS-MARBL input files
- **Scratch directory** (`config.paths.scratch`): Model execution directories
- **Model configs** (`config.paths.model_configs`): Model configuration templates and defaults
    - **Catalog root** (`config.paths.catalog`): Inner directory that directly contains ``blueprints/`` and ``builds/`` (default: ``<cson-forge-data base>/cson_forge_data/catalog`` when ``source-data`` lives under the usual layout; avoids ``cson_forge_data/cson_forge_data`` if ``source_data`` is already under ``cson_forge_data``)
- **Blueprints** (`config.paths.blueprints`): Generated blueprint YAML files (default: `config.paths.catalog / "blueprints"`)
- **Builds** (`config.paths.builds`): Rendered compile-time and run-time code directories (default: `config.paths.catalog / "builds"`)
- **YAML files** (`config.paths.models_yaml`, `config.paths.builds_yaml`, `config.paths.machines_yaml`): Configuration files

### Relocating the catalog

To point blueprints and builds at another directory (for example scratch or a shared drive), build a new `DataPaths` with `config.with_catalog` and assign it to `config.paths`:

```python
from pathlib import Path
from cson_forge import config

config.paths = config.with_catalog(config.paths, Path("/scratch/me/cson-catalog"))
```

Create the new `blueprints` and `builds` directories if needed before running workflows.

Per-domain or per-builder overrides use ``CstarSpecBuilder(catalog_root=...)`` (outer anchor:
``<catalog_root>/catalog/blueprints`` and ``<catalog_root>/catalog/builds``) or
``catalog_root="local"`` for the in-repo ``cson_forge/catalog`` layout. See ``CstarSpecEngine`` for a
default ``catalog_root`` applied to all domains from YAML.

### Accessing Configuration in Code

```python
from cstar_forge import config

# Access paths
source_data_path = config.paths.source_data
input_data_path = config.paths.input_data

# Access system information
system_tag = config.system  # e.g., "MacOS", "RCAC_anvil", "NERSC_perlmutter"
hostname = config.system_id  # Alias for system

# Access machine configuration
machine_config = config.machine  # MachineConfig object with account, pes_per_node, queues
cluster_type = config.cluster_type  # "LocalCluster" or "SLURMCluster"
```

### Inspecting Configuration

You can inspect the detected system and configured paths using the `config` module CLI:

```bash
python -m cstar_forge.config show-paths
```

This will display:
- The detected system tag (e.g., `MacOS`, `RCAC_anvil`, `NERSC_perlmutter`)
- The hostname
- All configured data paths (source_data, input_data, scratch, model_configs, blueprints, etc.)

To output the paths in JSON format:

```bash
python -m cstar_forge.config show-paths --json
```

## Machine Configuration

Machine-specific settings (account, processing elements per node, queue names) are loaded from `cstar_forge/machines.yml`. The `config.machine` object provides access to these settings:

```python
from cstar_forge import config

# Access machine configuration
account = config.machine.account  # Account/project name for job submission
pes_per_node = config.machine.pes_per_node  # Cores per node
default_queue = config.machine.queues.get("default")  # Default queue name
```

If a machine is not found in `machines.yml` or the file doesn't exist, an empty `MachineConfig` is returned.

### Cluster Types

The system automatically determines the cluster type based on the detected system:

- **LocalCluster**: Used for `MacOS` and `unknown` systems (local execution)
- **SLURMCluster**: Used for `RCAC_anvil` and `NERSC_perlmutter` systems (HPC job submission)

The cluster type is accessible via `config.cluster_type` and is used by the execution system to determine how to submit and manage jobs.

## Customization

### Adding a New System

To customize paths or add a new system, edit `cstar_forge/config.py` and:

1. Create a layout function that returns `(source_data, input_data, scratch)` paths
2. Register it using the `@register_system(tag)` decorator

Example:

```python
@register_system("MY_SYSTEM")
def _layout_my_system(home: Path, env: dict) -> Tuple[Path, Path, Path]:
    base = Path(env.get("MY_DATA_ROOT", home / "data"))
    source_data = base / "source-data"
    input_data = base / "input-data"
    scratch = base / "runs"
    return source_data, input_data, scratch
```

The system detection logic in `_detect_system()` will need to be updated to recognize your system tag based on hostname or environment variables.

### System-Specific Path Layouts

Each system layout function receives:
- `home`: The user's home directory (from `$HOME` environment variable)
- `env`: Dictionary of environment variables

Layout functions should return a tuple of three paths:
1. `source_data`: Location for external datasets
2. `input_data`: Location for generated input files
3. `scratch`: Location for model execution directories

The `get_data_paths()` function automatically creates these directories if they don't exist.

## Reference

For further reference, see:
- [API Reference: Core](api-core.md) - Configuration module source code
- [Machines (machines.yml)](reference-machines.md) - Machine-specific settings



