"""
Diagnostics package for model evaluation and comparison.

This package provides utilities for comparing model outputs to observational datasets
and computing diagnostic metrics.
"""

from . import glodap

# Export public API
__all__ = [
    "compute_grid_area",
    "depth_bnds",
    "glodap",
    "known_products",
    "lat_weights_regular_grid",
    # glodap module exports
    "open_glodap",
]

# Import commonly used functions for convenience
from .glodap import (
    compute_grid_area,
    depth_bnds,
    known_products,
    lat_weights_regular_grid,
    open_glodap,
)
