# Source Data Example

To pre-stage model input data, you can use a call to `SourceData` like this.

```python
from datetime import datetime
from cson_forge.source_data import SourceData

start_time = datetime(2012, 1, 1)
end_time = datetime(2012, 1, 2)

src = SourceData(
    datasets=["GLORYS", "SRTM15", "UNIFIED_BGC"],
    clobber=True,
    grid=domain_grid,
    grid_name="my_domain",
    start_time=start_time,
    end_time=end_time,
)

# Prepares and caches the datasets needed
src.prepare_all()
# Paths to prepared files are available as: src.paths[<DATASET_KEY>]
# Note: For GLORYS with multiple days, src.paths["GLORYS_REGIONAL"] may be a List[Path]
# For streamable sources (e.g., ERA5), use: src.prepare_all(include_streamable=True)
# You can also use: src.path_for_source("GLORYS") to get the path using the logical name
```
