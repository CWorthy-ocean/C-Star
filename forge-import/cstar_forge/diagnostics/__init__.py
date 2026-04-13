"""
Diagnostics package for model evaluation and comparison.

This package provides utilities for comparing model outputs to observational datasets
and computing diagnostic metrics.
"""

from . import glodap

# Export public API
__all__ = [
    "glodap",
    # glodap module exports
    "open_glodap",
    "lat_weights_regular_grid",
    "compute_grid_area",
    "known_products",
    "depth_bnds",
]

# Import commonly used functions for convenience
from .glodap import (
    open_glodap,
    lat_weights_regular_grid,
    compute_grid_area,
    known_products,
    depth_bnds,
)