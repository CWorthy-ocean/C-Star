# Source Data Example

Normally you don't call `SourceData` directly: `python -m cstar_forge.run <forge_blueprint.yaml>` runs source-data preparation as one step of executing a `ForgeBlueprint`, auto-detecting the host's shared download cache for you. The snippet below is the lower-level API that `ForgeExecutor` calls internally — useful for pre-staging data outside of a full blueprint run.

`SourceData` no longer resolves its cache location from `cstar_forge.config` internally; the caller must inject it via `source_data_dir`. `cstar_forge.config.resolve_host()` builds the same `HostPaths` the forge application would use, whose `source_data_cache` is the shared download cache root.

```python
from datetime import datetime
from cstar_forge.forge.source_data import SourceData
from cstar_forge.config import resolve_host

host = resolve_host(working_dir="~/cstar-forge-data/my_domain")

start_time = datetime(2012, 1, 1)
end_time = datetime(2012, 1, 2)

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
# Note: For GLORYS with multiple days, src.paths["GLORYS_REGIONAL"] may be a List[Path]
# For streamable sources (e.g., ERA5), use: src.prepare_all(include_streamable=True)
# You can also use: src.path_for_source("GLORYS") to get the path using the logical name
```
