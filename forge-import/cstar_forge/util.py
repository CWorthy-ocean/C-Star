"""
Utility functions for CSFORGE Forge.

This module contains utility functions used across the codebase.
"""
from __future__ import annotations

import inspect
import math
import warnings
from typing import Literal

import xarray as xr

PhaseSpeedMode = Literal["baroclinic", "barotropic"]
"""Wave phase speed mode for the CFL time step calculation."""

def compute_timestep_from_cfl(
    grid_size_x: float,
    grid_size_y: float,
    grid_nx: int,
    grid_ny: int,
    grid_ds: xr.Dataset,
    cfl: float = 0.7,
    default_depth: float = 4000.0,
    reduced_gravity: float = 0.04,
    equivalent_depth: float = 2000.0,
    mode: PhaseSpeedMode = "baroclinic",
) -> float:
    """
    
    [functionally validated template: requires review for accuracy]

    Compute timestep based on CFL criterion for numerical stability.
    
    The CFL (Courant-Friedrichs-Lewy) condition uses ``dt = CFL * dx_min / c``
    for the chosen phase speed ``c``.
    
    Parameters
    ----------
    grid_size_x : float
        Domain size in x-direction (kilometers).
    grid_size_y : float
        Domain size in y-direction (kilometers).
    grid_nx : int
        Number of grid points in x-direction.
    grid_ny : int
        Number of grid points in y-direction.
    grid_ds : xr.Dataset
        Grid dataset; bathymetry (``h``) is read only when
        ``mode`` is ``"barotropic"``.
    cfl : float, optional
        CFL number (typically 0.5-0.8 for stability). Default is 0.7.
    default_depth : float, optional
        Default ocean depth in meters if ``h`` is missing in barotropic mode.
        Default is 4000.0 m.
    reduced_gravity : float, optional
        Reduced gravity ``g'`` (m/s^2) for baroclinic mode, ``c = sqrt(g' * h_e)``.
        Default 0.04 m/s^2 a typical thermocline-scale value.
    equivalent_depth : float, optional
        Equivalent depth ``h_e`` (m) for baroclinic mode. Default 2000.0 m with
        default ``g'`` gives ``c`` ~ 9.0 m/s, but is tuned for a timestep of ~900s
        @ a 12km grid size. A typical internal wave speed is ~3 m/s but the timestep
        generated for that speed is longer than current ROMS-MARBL stability requires.
    mode : {"baroclinic", "barotropic"}, optional
        ``"baroclinic"`` (default): ``c = sqrt(reduced_gravity * equivalent_depth)``.
        ``"barotropic"``: ``c = sqrt(g * H_max)`` from ``grid_ds['h']`` or
        ``default_depth``.
    
    Returns
    -------
    float
        Timestep in seconds, rounded to nearest integer, with minimum of 1.0.
        The timestep is adjusted to be an even divisor of 86400 (seconds per day)
        to ensure timesteps align with daily boundaries.
    
    Notes
    -----
    The calculation follows these steps:
    1. Compute minimum grid spacing (dx, dy) from domain size and grid points
    2. Estimate fastest {Baroclinic,Barotropic} wave speed
    3. Apply CFL condition: dt = CFL * dx_min / c
    4. Round to nearest integer
    5. Adjust to nearest divisor of 86400 (ensures daily alignment)
    
    Logic: grid spacing -> phase speed ``c`` -> ``dt = CFL * dx_min / c`` ->
    round -> snap to a divisor of 86400. Baroclinic ``c`` is a reduced-gravity
    scale for the baroclinic timestep; barotropic stability and
    other limits may still require a smaller ``dt``. The fastest gravity wave speed 
	is the barotropic wave speed for shallow water waves, which depends on the 
	maximum depth in the domain.
    
    The timestep is constrained to be an even divisor of 86400 (seconds per day)
    to ensure that model timesteps align with daily boundaries, which is important
    for forcing data interpolation and output timing.
    """
    # Compute grid spacing in kilometers
    dx_km = grid_size_x / grid_nx
    dy_km = grid_size_y / grid_ny
    dx_min_km = min(dx_km, dy_km)
    
    # Convert to meters
    dx_min_m = dx_min_km * 1000.0
    
    if mode == "baroclinic":
        if reduced_gravity <= 0.0 or equivalent_depth <= 0.0:
            raise ValueError(
                "reduced_gravity and equivalent_depth must be positive for "
                "baroclinic phase speed; got "
                f"reduced_gravity={reduced_gravity!r}, "
                f"equivalent_depth={equivalent_depth!r}"
            )
        c = math.sqrt(reduced_gravity * equivalent_depth)
    elif mode == "barotropic":
        if "h" in grid_ds:
            H_max = float(grid_ds["h"].max().values)
        else:
            H_max = default_depth
            warnings.warn(
                "Grid dataset does not contain 'h' variable. "
                f"Using default depth of {H_max} m for CFL calculation.",
                UserWarning,
                stacklevel=2,
            )
        g = 9.81
        c = math.sqrt(g * H_max)
    else:
        raise ValueError(
            "'mode' must be 'baroclinic' or 'barotropic'; "
            f"got {mode!r}"
        )
    
    # Compute timestep from CFL condition: dt = CFL * dx / c
    dt = cfl * dx_min_m / c
    
    # Round to nearest integer (timesteps are typically integers in seconds)
    dt = round(dt)
    
    # Ensure minimum timestep (avoid extremely small values)
    dt = max(dt, 1.0)
    
    # Adjust dt to be an even divisor of 86400 (seconds per day)
    # This ensures that timesteps align with daily boundaries
    # Find the nearest divisor of 86400 that's close to the current dt
    dt_original = int(round(dt))  # Ensure integer
    target_seconds_per_day = 86400
    
    # Collect all divisors of 86400
    divisors = set()
    sqrt_target = int(target_seconds_per_day**0.5) + 1
    for d in range(1, sqrt_target):
        if target_seconds_per_day % d == 0:
            divisors.add(d)
            divisors.add(target_seconds_per_day // d)
    
    # Find the closest divisor
    if divisors:
        best_dt = min(divisors, key=lambda x: abs(x - dt_original))
    else:
        # Fallback (should never happen, but be safe)
        best_dt = 1
    
    dt = int(best_dt)
    
    return dt


def roms_tools_nesting_writer():
    """
    Return the roms_tools API that writes parent/child nesting metadata to disk.

    Newer roms_tools exposes ``make_edata``; older releases used ``make_nesting_info``.
    Both accept ``(parent_grid, child_grid, filepath, **kwargs)`` with compatible kwargs
    such as ``period``.
    """
    import roms_tools as rt

    writer = getattr(rt, "make_nesting_info", None) or getattr(rt, "make_edata", None)
    if writer is None:
        raise AttributeError(
            "roms_tools must provide make_edata or make_nesting_info for nested grids."
        )
    return writer


def roms_tools_default_nesting_period_seconds() -> float:
    """Default ``period`` (seconds) for nesting extract, from roms_tools writer signature."""
    writer = roms_tools_nesting_writer()
    params = inspect.signature(writer).parameters
    if "period" in params:
        default = params["period"].default
        if default is not inspect.Parameter.empty:
            return float(default)
    return 3600.0

