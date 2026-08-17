# System detection and data paths

C-Star Forge uses a configuration system to manage paths and system-specific settings.

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
- **Catalog root** (`config.paths.catalog`): Inner directory that directly contains ``blueprints/`` — equal to `user_catalog_root()` (default: `~/cstar-forge-data/catalog`), deliberately home-anchored rather than rebased onto `$SCRATCH`/`$WORK` like `source_data`/`input_data`/`scratch` above: catalog entries are durable, user-registered content that must survive HPC scratch purges. Override with the `CSTAR_FORGE_CATALOG` environment variable (an `os.pathsep`-separated list of catalog roots; the first entry is this writable layer).
- **Blueprints** (`config.paths.blueprints`): Generated blueprint YAML files (default: `config.paths.catalog / "blueprints"`)
- **Builds**: rendered compile-time/run-time code directories under the per-run `HostPaths.working_dir / "builds"/{compile-time,run-time}` — not under the catalog.
- **YAML files** (`config.paths.models_yaml`, `config.paths.builds_yaml`): *(vestigial: these two `DataPaths` fields survive but the files no longer ship and nothing reads them — model data comes from the layered default catalog instead.)*

### Relocating the catalog

The catalog the wizard and `domain_catalog.default_catalog` actually read from and
save into is controlled independently of `config.paths`, via the
`CSTAR_FORGE_CATALOG` environment variable (see the **Catalog root** bullet
above) — set it in your environment before launching the wizard (or before
importing `cstar_forge` at all) to relocate the writable layer, for example
onto a shared drive.

`config.with_catalog` is a narrower, in-process utility: it returns a new
`DataPaths` with `.catalog`/`.blueprints` overridden, for code that reads
`config.paths.catalog` directly. It does **not** reroute
`domain_catalog.default_catalog`, so it will not change where the wizard
saves:

```python
from pathlib import Path
from cstar_forge import config

config.paths = config.with_catalog(config.paths, Path("/scratch/me/cstar-catalog"))
```

Create the new `blueprints` directory if needed before running workflows that read `config.paths.blueprints` directly.

At processing time, `cstar_forge.config.resolve_host(working_dir)` builds the forge
application's `HostPaths` (`cstar_forge.forge.host.HostPaths`) from this auto-detected
config: `source_data_cache` comes from `config.paths.source_data`, plus the detected
`system` tag. `working_dir` (the per-run artifact root that
`ForgeExecutor` writes everything under) is supplied separately — see
`docs/architecture-details.md` §2–4 for the full authoring/execution split. This is Forge's
own *disposable* host provider; when the forge application relocates into C-Star,
C-Star supplies its own `HostPaths` and this resolver is not carried over.

### Accessing Configuration in Code

```python
from cstar_forge import config

# Access paths
source_data_path = config.paths.source_data
input_data_path = config.paths.input_data

# Access system information
system_tag = config.system  # e.g., "MacOS", "RCAC_anvil", "NERSC_perlmutter"
system_tag_alias = config.system_id  # alias for config.system (a tag like "MacOS", not a hostname)
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
- All configured data paths (source_data, input_data, scratch, catalog, blueprints, etc.)

To output the paths in JSON format:

```bash
python -m cstar_forge.config show-paths --json
```

## Cluster Types

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

The system detection logic in `_detect_system()` will need to be updated to recognize your system tag based on hostname or environment variables. You must also add the tag to `_default_cluster_type()` in `config.py`, which raises `NotImplementedError` for unknown tags at module-import time.

### System-Specific Path Layouts

Each system layout function receives:
- `home`: The user's home directory (from `$HOME` environment variable)
- `env`: Dictionary of environment variables

Layout functions should return a tuple of three paths:
1. `source_data`: Location for external datasets
2. `input_data`: Location for generated input files
3. `scratch`: Location for model execution directories

The `get_data_paths()` function builds `Path` objects only; pass `create=True`, or call `config.ensure_data_dirs()` from an entry point that writes data (as `run.py`'s `main()` does).

## Reference

For further reference, see:
- [Architecture Details](architecture-details.md) - module map, including `config.py` (`DataPaths`/`resolve_host()`)



