A physics surface forcing item accepts a ``blend`` block that fills every grid point a regional source misses from a global one such as ERA5, using ROMS-Tools' surface-forcing blend.
- ``fallback`` takes its own ``source``, ``correct_radiation`` and ``options``; ``width_km`` (``0``, the default, switches at the footprint's edge) and ``profile`` (``smoothstep`` or ``linear``) shape the band between the two.
- When any surface item has a blend, every surface item must set the same ``coarse_grid_mode``, ``never`` or ``always``.
- Forge writes one blended file, ``<name>_surface-physics-<primary>-<fallback>_*.nc``.
