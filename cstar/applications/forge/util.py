"""
Utility functions for CSFORGE Forge.

This module contains utility functions used across the codebase.
"""

from __future__ import annotations

import gc
import logging
import math
import os
import sys
import time
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import roms_tools as rt
    import xarray as xr

log = logging.getLogger(__name__)

PhaseSpeedMode = Literal["baroclinic", "barotropic"]
"""Wave phase speed mode for the CFL time step calculation."""


def compute_timestep_from_cfl(
    grid_size_x: float,
    grid_size_y: float,
    grid_nx: int,
    grid_ny: int,
    grid_ds: xr.Dataset | None,
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
    grid_ds : xr.Dataset or None
        Grid dataset; bathymetry (``h``) is read only when
        ``mode`` is ``"barotropic"``, which requires it.
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
        generated for that speed is longer than current UCLA-ROMS stability requires.
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
        if grid_ds is None:
            raise ValueError("barotropic mode needs grid_ds to read the bathymetry")
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
        raise ValueError(f"'mode' must be 'baroclinic' or 'barotropic'; got {mode!r}")

    # Compute timestep from CFL condition: dt = CFL * dx / c
    dt = cfl * dx_min_m / c

    # Round to nearest integer (timesteps are typically integers in seconds)
    dt = round(dt)

    # Ensure minimum timestep (avoid extremely small values)
    dt = max(dt, 1.0)

    # Adjust dt to be an even divisor of 86400 (seconds per day)
    # This ensures that timesteps align with daily boundaries
    # Find the nearest divisor of 86400 that's close to the current dt
    dt_original = round(dt)  # Ensure integer
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


def compute_grid_spacing_m(grid_size_x: float, grid_nx: int) -> float:
    """
    Horizontal grid spacing in meters from domain size and point count.

    ``grid_spacing`` is ``size_x / nx`` with ``size_x`` in kilometers, converted
    to meters.
    """
    if grid_nx <= 0:
        raise ValueError(f"grid_nx must be positive, got {grid_nx!r}")
    return (grid_size_x / grid_nx) * 1000.0


def compute_v_sponge_from_grid(grid_size_x: float, grid_nx: int) -> float:
    """Default sponge viscosity: grid spacing (m) divided by 10."""
    return compute_grid_spacing_m(grid_size_x, grid_nx) / 10.0


_CDR_EARTH_RADIUS_M = 6.371e6
"""The Earth radius ucla-roms' ``cdr_frc`` measures release distances with."""

_CDR_HSC_CUTOFF = 1e-3
"""ucla-roms spreads a release with a horizontal scale over the cells whose
Gaussian weight ``exp(-(dist/hsc)**2)`` exceeds this."""


def check_cdr_release_positions(
    grid: rt.Grid,
    releases: Sequence[Mapping[str, Any]],
    *,
    relocate_to_wet_pts: bool,
) -> None:
    """Refuse CDR releases ucla-roms cannot place on ``grid``, and warn about the ones
    it moves.

    ``releases`` are the release dicts of a roms-tools ``CDRForcing`` kwargs dict.
    ucla-roms (``cdr_frc.F90``) centres each release on the nearest interior cell of
    the whole grid, with no distance limit. With ``cdr_frc.relocate_to_wet_pts`` it
    leaves out land cells; without it, it stops at start-up on a release whose cell is
    land. It rescales each release's vertical profile to deliver the full flux, unless
    the profile is zero at every level of every column the release reaches.

    Refused, in one ``ValueError`` naming every release:

    * a release outside the domain: farther from every grid point than the largest
      half cell diagonal plus 10%, roms-tools' own rule. ucla-roms would move it to
      the nearest cell however far away.
    * a release on land while ``relocate_to_wet_pts`` is off, or on a grid without
      a wet interior cell.
    * a release so far below the seafloor, for its vertical scale, that its profile
      is zero at every level, so ROMS would deliver none of it.

    Warned, in one ``UserWarning``: a release on the grid's boundary row (ROMS uses
    the nearest interior cell), on land with relocation on (ROMS moves it to the
    nearest wet cell), or below the seafloor (ROMS delivers its flux near the bottom).
    roms-tools refuses all of these when given a grid, so Forge builds the forcing
    without one and checks here instead.
    """
    import numpy as np

    ds = grid.ds
    lat = np.deg2rad(np.asarray(ds["lat_rho"].values, dtype=float))
    lon = np.deg2rad(np.asarray(ds["lon_rho"].values, dtype=float))
    wet = np.asarray(ds["mask_rho"].values) > 0
    h = np.asarray(ds["h"].values, dtype=float)
    pm = np.asarray(ds["pm"].values, dtype=float)
    pn = np.asarray(ds["pn"].values, dtype=float)
    sigma_r = np.asarray(ds["sigma_r"].values, dtype=float)
    cs_r = np.asarray(ds["Cs_r"].values, dtype=float)
    interior = np.zeros(wet.shape, dtype=bool)
    interior[1:-1, 1:-1] = True
    outside_m = 1.1 * float(np.max(np.hypot(1 / pm, 1 / pn) / 2))

    refused: list[str] = []
    warned: list[str] = []
    for index, release in enumerate(releases):
        name = str(release.get("name", f"#{index}"))
        try:
            r_lat = float(release["lat"])
            r_lon = float(release["lon"])
            depth = float(release["depth"])
            hsc = float(release.get("hsc") or 0.0)
            vsc = float(release.get("vsc") or 0.0)
        except (KeyError, TypeError, ValueError):
            continue  # roms-tools refuses a release without a usable position
        here = f"release {name!r} (lat {r_lat:g}, lon {r_lon:g}, depth {depth:g} m)"

        # haversine; periodic in longitude, so no wrapping is needed
        a = (
            np.sin((lat - np.deg2rad(r_lat)) / 2) ** 2
            + np.cos(lat)
            * np.cos(np.deg2rad(r_lat))
            * np.sin((lon - np.deg2rad(r_lon)) / 2) ** 2
        )
        dist = 2 * _CDR_EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))

        if float(dist.min()) > outside_m:
            refused.append(
                f"{here} is outside the domain, {dist.min() / 1000:.0f} km from the "
                "nearest grid point; ROMS would move it to the nearest cell"
            )
            continue
        on_edge = not interior.flat[int(np.argmin(dist))]

        candidates = interior & wet if relocate_to_wet_pts else interior
        if not candidates.any():
            refused.append(f"{here}: the grid has no wet interior cell")
            continue
        nearest = np.unravel_index(np.argmin(np.where(interior, dist, np.inf)), h.shape)
        cell = np.unravel_index(np.argmin(np.where(candidates, dist, np.inf)), h.shape)
        if not wet[nearest]:
            if not relocate_to_wet_pts:
                refused.append(
                    f"{here} is on land, and cdr_frc.relocate_to_wet_pts is off; "
                    "ROMS stops at start-up"
                )
                continue
            warned.append(
                f"{here} is on land; ROMS moves it to the nearest wet cell, "
                f"{dist[cell] / 1000:.1f} km away"
            )
        elif on_edge:
            warned.append(
                f"{here} is on the edge of the grid; ROMS centres it on the nearest "
                "interior cell"
            )

        if depth <= h[cell]:
            continue
        reached = np.zeros(h.shape, dtype=bool)
        reached[cell] = True
        if hsc > 0:
            reached |= (
                candidates & wet & (np.exp(-((dist / hsc) ** 2)) > _CDR_HSC_CUTOFF)
            )
        deepest = float(h[reached].max())
        # level depths (m, positive down) of the deepest column, as ROMS sets them;
        # ROMS's double-precision weights count as zero below the smallest normal
        # number, where a build that flushes subnormals to zero loses them
        levels = -deepest * (grid.hc * sigma_r + deepest * cs_r) / (grid.hc + deepest)
        weights = np.exp(-(((depth - levels) / vsc) ** 2)) if vsc > 0 else None
        if weights is not None and not np.any(weights > np.finfo(float).tiny):
            refused.append(
                f"{here} is {depth - h[cell]:.0f} m below the seafloor "
                f"({h[cell]:.0f} m), {(depth - h[cell]) / vsc:.0f} times its vertical "
                "scale: its profile is zero at every level, so ROMS would deliver none "
                "of its flux"
            )
            continue
        warned.append(
            f"{here} is below the seafloor ({h[cell]:.0f} m); ROMS delivers its "
            "flux near the bottom"
        )

    if refused:
        raise ValueError(
            "CDR releases ROMS cannot place on the grid:\n  - " + "\n  - ".join(refused)
        )
    if warned:
        warnings.warn(
            "CDR releases ROMS will move:\n  - " + "\n  - ".join(warned),
            UserWarning,
            stacklevel=2,
        )


# --- memory / timing instrumentation (formerly cstar_forge.utils) ----------
_PLATFORM = sys.platform  # 'linux', 'darwin', 'win32', ...

# --- instantaneous RSS -----------------------------------------------------
try:
    import psutil

    _proc = psutil.Process(os.getpid())

    def rss_mb():
        return _proc.memory_info().rss / 1024**2
except ImportError:
    if _PLATFORM == "linux":

        def rss_mb():
            with open("/proc/self/statm") as f:
                pages = int(f.read().split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE") / 1024**2
    else:
        import resource

        def rss_mb():
            # no cheap instantaneous RSS without psutil on macOS;
            # fall back to peak as an approximation
            return _peak_rss_mb()


# --- peak RSS --------------------------------------------------------------
if _PLATFORM == "linux":

    def _peak_rss_mb():
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024  # kB -> MB
        return float("nan")

    def _reset_peak():
        # kernel 3.11+; resets VmHWM to current RSS. RHEL 8 (Anvil) is fine.
        try:
            with open("/proc/self/clear_refs", "w") as f:
                f.write("5")
            return True
        except (OSError, PermissionError):
            return False

else:  # macOS (darwin), and a reasonable default elsewhere
    import resource

    def _peak_rss_mb():
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes; Linux reports kB. We're in the non-linux branch,
        # so assume bytes on darwin, kB otherwise.
        divisor = 1024**2 if _PLATFORM == "darwin" else 1024
        return maxrss / divisor

    def _reset_peak():
        return False  # no way to reset the high-water mark here


# --- context manager -------------------------------------------------------
@contextmanager
def mem_log(name, level=logging.INFO, collect=True, enabled=True):
    """Time a block and log RSS before/after, plus peak if resettable.

    On Linux (incl. Anvil) reports a true per-block peak.
    On macOS reports process-lifetime peak (cannot be reset).

    If ``enabled`` is False, this is a no-op: no timing, no RSS reads, no
    ``gc.collect()``, no log line. Use this to keep the fast path free of
    instrumentation overhead when verbose diagnostics aren't requested.
    """
    if not enabled:
        yield
        return
    log.debug("%s: starting", name)
    resettable = _reset_peak()
    before = rss_mb()
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        if collect:
            gc.collect()
        after = rss_mb()
        peak = _peak_rss_mb()
        delta = after - before
        if resettable:
            log.log(
                level,
                "%s: %.2fs, %.0f -> %.0f MB, peak %.0f (delta %+.0f)",
                name,
                elapsed,
                before,
                after,
                peak,
                delta,
            )
        else:
            log.log(
                level,
                "%s: %.2fs, %.0f -> %.0f MB (delta %+.0f); peak %.0f is process-lifetime max",
                name,
                elapsed,
                before,
                after,
                delta,
                peak,
            )
