# Source Data Module Developer's Guide

## Module Design Philosophy

The `source_data` module provides a **registry-based framework** for managing heterogeneous source datasets used in ROMS preprocessing. The design emphasizes:

- **Extensibility**: New datasets can be added by registering handler functions without modifying core logic
- **Dependency Management**: Each dataset declares its requirements (grid, time range, etc.) explicitly
- **Caching**: Datasets are downloaded/cached locally and reused across runs
- **Abstraction**: User-facing logical names (e.g., "GLORYS") map to implementation-specific dataset keys (e.g., "GLORYS_REGIONAL")

## Core Architecture

The module consists of three main components:

1. **Registry System**: Decorator-based registration of dataset handlers
2. **SourceData Class**: Main interface for preparing and accessing datasets
3. **Source Name Mapping**: Translation layer between logical names and dataset keys

```
User Request ("GLORYS") 
    ↓
SOURCE_ALIAS mapping
    ↓
Dataset Key ("GLORYS_REGIONAL")
    ↓
DATASET_REGISTRY lookup
    ↓
DatasetHandler (function + requirements)
    ↓
Handler execution → Path(s) to prepared data
```

## Core Objects

### `SourceData` (Dataclass)

The main interface for preparing and accessing source datasets.

**Constructor:**
```python
SourceData(
    datasets: List[str],           # Dataset names to prepare
    clobber: bool = False,         # Force re-download if True
    grid: Optional[GridType] = None,
    grid_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
)
```

**Key Attributes:**
- `datasets`: Normalized list of dataset keys (after alias resolution)
- `paths`: Dictionary mapping dataset keys to prepared file paths
- Optional attributes (`grid`, `start_time`, etc.) are only required if a dataset handler declares them

**Key Methods:**
- `prepare_all(include_streamable=False)`: Prepare all requested datasets
- `path_for_source(logical_name)`: Get path for a logical source name (e.g., "GLORYS")
- `dataset_key_for_source(logical_name)`: Map logical name to dataset key

**Lifecycle:**
1. **Initialization**: Normalizes dataset names through `SOURCE_ALIAS`, validates against `DATASET_REGISTRY`
2. **Preparation**: `prepare_all()` iterates through datasets, checks requirements, and calls handler functions
3. **Storage**: Handler return values (Path or List[Path]) are stored in `self.paths`

### `DatasetHandler` (Class)

Container for a dataset preparation function and its dependency requirements.

```python
class DatasetHandler:
    def __init__(self, func: Callable["SourceData", Path], requires: List[str]):
        self.func = func      # Handler function
        self.requires = requires  # Required SourceData attributes
```

**Purpose:**
- Encapsulates handler function and metadata
- Enables dependency checking before handler execution
- Stored in `DATASET_REGISTRY` keyed by dataset name

## Registry Framework

### Registration Decorator

The `@register_dataset` decorator registers dataset preparation functions:

```python
@register_dataset(
    name: str,                    # Dataset key (e.g., "GLORYS_REGIONAL")
    requires: Optional[List[str]] = None  # Required SourceData attributes
)
def _prepare_dataset(self: SourceData) -> Union[Path, List[Path], Dict]:
    """Handler function that prepares the dataset."""
    # Implementation...
    return path_or_paths
```

**Registration Process:**
1. Decorator captures function and requirements
2. Creates `DatasetHandler` instance
3. Stores in `DATASET_REGISTRY` with uppercase key

**Example:**
```python
@register_dataset(
    "GLORYS_REGIONAL",
    requires=["grid", "grid_name", "start_time", "end_time"]
)
def _prepare_glorys_regional(self: SourceData) -> List[Path]:
    """Download daily regional GLORYS subsets."""
    bounds = rt.get_glorys_bounds(self.grid)
    paths = self._prepare_glorys_daily(is_regional=True, bounds=bounds)
    self.paths["GLORYS_REGIONAL"] = paths[0] if len(paths) == 1 else paths
    return paths
```

### Registry Dictionary

`DATASET_REGISTRY: Dict[str, DatasetHandler]` maps dataset keys to their handlers.

- Keys are **uppercase** (normalized during registration)
- Values are `DatasetHandler` instances
- Populated at module import time via decorators

### Handler Function Signature

Handler functions must:
- Accept `self: SourceData` as first parameter (bound to `SourceData` instance)
- Return `Path`, `List[Path]`, or `Dict[str, Path]` (stored in `self.paths[dataset_key]`)
- Access required attributes via `self` (e.g., `self.grid`, `self.start_time`)
- Use `self.clobber` to determine if re-download is needed
- Store result in `self.paths[dataset_key]` (convention, not required)

## Source Name Mapping

### Logical Names vs. Dataset Keys

Users specify **logical source names** in configuration (e.g., `models.yml`):
- `"GLORYS"` → maps to `"GLORYS_REGIONAL"` or `"GLORYS_GLOBAL"` (platform-dependent)
- `"UNIFIED"` → maps to `"UNIFIED_BGC"`
- `"SRTM15"` → maps to `"SRTM15_V2.7"` (version-specific)

### `SOURCE_ALIAS` Dictionary

Maps logical names to dataset registry keys:

```python
SOURCE_ALIAS: Dict[str, str] = {
    "GLORYS": glorys_dataset_key,  # Platform-dependent
    "UNIFIED": "UNIFIED_BGC",
    "SRTM15": f"SRTM15_{SRTM15_VERSION}".upper(),
    "ERA5": "ERA5",
    "TPXO": "TPXO",
    "DAI": "DAI",
}
```

**Mapping Function:**
```python
def map_source_to_dataset_key(name: str) -> str:
    """Map logical name to dataset key, or return uppercased name if no alias."""
    return SOURCE_ALIAS.get(name.upper(), name.upper())
```

**Normalization:**
- `SourceData.__post_init__()` normalizes all dataset names through `SOURCE_ALIAS`
- Unknown names are uppercased and used as-is (must exist in `DATASET_REGISTRY`)

### Streamable Sources

Some datasets don't require local caching (e.g., ERA5, DAI). Listed in `STREAMABLE_SOURCES`:

```python
STREAMABLE_SOURCES: List[str] = ["ERA5", "DAI"]
```

- Skipped by default in `prepare_all()` unless `include_streamable=True`
- `path_for_source()` returns `None` for streamable sources if not prepared

## Adding a New Dataset

### Step 1: Implement Handler Function

```python
@register_dataset("MY_DATASET", requires=["grid", "grid_name"])
def _prepare_my_dataset(self: SourceData) -> Path:
    """
    Prepare MY_DATASET for the given grid.
    
    Returns
    -------
    Path
        Path to the prepared dataset file.
    """
    dataset_dir = config.paths.source_data / "MY_DATASET"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / f"my_dataset_{self.grid_name}.nc"
    
    needs_download = self.clobber or (not path.exists())
    
    if needs_download:
        if path.exists():
            print(f"⚠️  Clobber=True: removing existing file {path.name}")
            path.unlink()
        
        print(f"⬇️  Downloading MY_DATASET → {path}")
        # Download/prepare logic here...
    else:
        print(f"✔️  Using existing MY_DATASET: {path}")
    
    self.paths["MY_DATASET"] = path
    return path
```

### Step 2: Add Source Alias (if needed)

If users should reference it by a logical name:

```python
SOURCE_ALIAS["MY_SOURCE"] = "MY_DATASET"
```

### Step 3: Add to Streamable Sources (if applicable)

If the dataset doesn't need local caching:

```python
STREAMABLE_SOURCES.append("MY_DATASET")
```

## Example Dataset Handlers

### Simple Download Handler (SRTM15)

```python
@register_dataset("SRTM15")
def _prepare_srtm15(self: SourceData) -> Path:
    """Download SRTM15 bathymetry."""
    path = config.paths.source_data / "SRTM15" / f"SRTM15_{SRTM15_VERSION}.nc"
    
    if self.clobber or not path.exists():
        # Download logic...
        pass
    
    self.srtm15_path = path
    return path
```

**Characteristics:**
- No requirements (works for any grid/time)
- Returns single `Path`
- Stores in both `self.paths` and convenience attribute

### Time-Dependent Handler (GLORYS)

```python
@register_dataset(
    "GLORYS_REGIONAL",
    requires=["grid", "grid_name", "start_time", "end_time"]
)
def _prepare_glorys_regional(self: SourceData) -> List[Path]:
    """Download daily regional GLORYS subsets."""
    bounds = rt.get_glorys_bounds(self.grid)
    paths = self._prepare_glorys_daily(is_regional=True, bounds=bounds)
    # Store single path or list depending on count
    self.paths["GLORYS_REGIONAL"] = paths[0] if len(paths) == 1 else paths
    return paths
```

**Characteristics:**
- Requires grid and time range
- Returns `List[Path]` (one per day)
- Uses helper method `_prepare_glorys_daily()` for iteration

### User-Provided Dataset Handler (TPXO)

```python
@register_dataset("TPXO")
def _prepare_tpxo(self: SourceData) -> Dict[str, Path]:
    """Verify user-provided TPXO tidal data exists."""
    tpxo_path = config.paths.source_data / "TPXO" / "TPXO10.v2"
    
    # Verify files exist
    tpxo_dict = {
        "grid": tpxo_path / "grid_tpxo10v2.nc",
        "h": tpxo_path / "h_tpxo10.v2.nc",
        "u": tpxo_path / "u_tpxo10.v2.nc",
    }
    
    # Validation logic...
    
    self.paths["TPXO"] = tpxo_path
    return tpxo_dict
```

**Characteristics:**
- No download (user must provide)
- Returns `Dict[str, Path]` (multiple files)
- Validates file existence

## Usage Pattern

```python
from cson_forge.source_data import SourceData
from datetime import datetime

# Create SourceData instance
src = SourceData(
    datasets=["GLORYS", "UNIFIED", "SRTM15"],  # Logical names
    clobber=False,
    grid=my_grid,
    grid_name="my-grid",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 1, 2)
)

# Prepare all datasets
src.prepare_all()

# Access prepared paths
glorys_path = src.path_for_source("GLORYS")  # Returns Path or List[Path]
unified_path = src.path_for_source("UNIFIED")  # Returns Path
srtm15_path = src.path_for_source("SRTM15")  # Returns Path

# Or access directly
glorys_path = src.paths["GLORYS_REGIONAL"]
```

## Design Patterns

### Dependency Injection

Required attributes are injected into `SourceData` and accessed by handlers via `self`. This enables:
- Lazy evaluation (attributes only needed if dataset is requested)
- Clear dependency declaration via `requires` parameter
- Runtime validation before handler execution

### Caching Strategy

- Files are cached in `config.paths.source_data / {dataset_name} /`
- Existence check: `if self.clobber or (not path.exists())`
- Clobber mode: Remove existing file before download

### Return Value Flexibility

Handlers can return:
- `Path`: Single file
- `List[Path]`: Multiple files (e.g., daily time series)
- `Dict[str, Path]`: Named file collection (e.g., TPXO with grid/h/u files)

All are stored in `self.paths[dataset_key]` for uniform access.
