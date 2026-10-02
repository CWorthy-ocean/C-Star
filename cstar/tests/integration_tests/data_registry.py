"""Pooch registry for the upstream source data used by the integration suite.

Files come from the roms-tools test-data repository, pinned to a commit rather than
``main`` so that a data bump is a visible one-line change to ``TEST_DATA_COMMIT``
(and, if the files changed, to ``REGISTRY``). ETOPO5 and the ERA5 correction file are
not listed here: roms-tools fetches them itself into its own pooch cache.
"""

from pathlib import Path

import pooch

TEST_DATA_COMMIT = "76c7ad5f1448202eac9e5b7de3a21373d700c51d"
TEST_DATA_BASE_URL = (
    f"https://github.com/CWorthy-ocean/roms-tools-test-data/raw/{TEST_DATA_COMMIT}/"
)
CACHE_NAME = "cstar_integration_test_data"
"""Name passed to ``pooch.os_cache``; CI caches this directory."""

REGISTRY = {
    "GLORYS_NA_2012.nc": "b862add892f5d6e0d670c8f7fa698f4af5290ac87077ca812a6795e120d0ca8c",
    "ERA5_NA_2012.nc": "d07fa7450869dfd3aec54411777a5f7de3cb3ec21492eec36f4980e220c51757",
    "coarsened_UNIFIED_bgc_dataset_v2_1.nc": "732af496416cbfe181a87d056012363d5456dd268023de53646cd9ebf54cf712",
    "regional_grid_tpxo10v2.nc": "0789b6a24ecb2ced522481dfcfb7282e32f999984747b9b9f46f044a8898d0ac",
    "regional_h_tpxo10v2.nc": "202fd0c197490ac460af12cd9fa1156aa40023c0023c705f145c596de5b5ad3d",
    "regional_u_tpxo10v2.nc": "3b0849473cbb7f9076ca907e4fc39eceda3c7d64659c121fa0692024d59dcdb3",
}


def test_data_pooch() -> pooch.Pooch:
    """Return the pooch that manages the pinned integration-test source data."""
    return pooch.create(
        path=pooch.os_cache(CACHE_NAME),
        base_url=TEST_DATA_BASE_URL,
        registry=REGISTRY,
        retry_if_failed=3,
    )


def fetch(filename: str) -> Path:
    """Download (or find in the cache) one registry file and return its path."""
    return Path(test_data_pooch().fetch(filename))


def fetch_all() -> dict[str, Path]:
    """Fetch every registry file and return a ``{filename: path}`` mapping."""
    return {name: fetch(name) for name in REGISTRY}
