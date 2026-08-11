# Source Data Example

Normally you don't call `SourceData` directly: `cstar forge run <forge_blueprint.yaml>` (or `python -m cstar_forge.run …`) runs source-data preparation as one step of executing a `ForgeBlueprint`, auto-detecting the host's shared download cache for you. The snippet below is the lower-level API that `ForgeExecutor` calls internally — useful for pre-staging data outside of a full blueprint run.

`SourceData` no longer resolves its cache location from `cstar_forge.config` internally; the caller must inject it via `source_data_dir`. `cstar_forge.config.resolve_host()` builds the same `HostPaths` the forge application would use, whose `source_data_cache` is the shared download cache root.

```python
from datetime import datetime
from cstar_forge.forge.source_data import SourceData
from cstar_forge.config import resolve_host

host = resolve_host(working_dir="~/cstar-forge-run/my_domain")

start_time = datetime(2012, 1, 1)
end_time = datetime(2012, 1, 2)

domain_grid = roms_tools.Grid(...)  # the domain's Grid (as built by the executor)

src = SourceData(
    datasets=["GLORYS", "SRTM15", "UNIFIED_BGC"],
    clobber=True,
    grid=domain_grid,
    grid_name="my_domain",
    start_time=start_time,
    end_time=end_time,
    source_data_dir=host.source_data_cache,
)

# Prepares and caches the datasets needed
src.prepare_all()
# Paths to prepared files are available as: src.paths[<DATASET_KEY>]
# Note: src.paths["GLORYS_REGIONAL"] is a List[Path] (one file per day, window padded ±1 day)
# For streamable sources (e.g., ERA5), use: src.prepare_all(include_streamable=True)
# (ERA5 still stages no file; paths["ERA5"] is None)
# You can also use: src.path_for_source("GLORYS") to get the path using the logical name
```
