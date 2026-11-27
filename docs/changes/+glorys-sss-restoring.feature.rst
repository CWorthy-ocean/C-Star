Forge can restore sea-surface salinity to GLORYS: give a surface item ``type: restoring``, ``restoring_forces: [sss]`` and a ``GLORYS`` source, with an optional ``time_resolution`` of ``monthly`` or ``daily``.
- The restoring reuses the GLORYS files staged for the run and writes ``surface-restoring-glorys*.nc``.
- It needs a ROMS-Tools release with GLORYS SSS restoring; ``cstar blueprint check`` refuses it on an older one.
