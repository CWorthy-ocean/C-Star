Forge and the wizard can run ucla-roms 0.15.0's implicit free surface: set ``implicit_fs.fs_mode`` to ``implicit`` (in the wizard, under Advanced settings, Physics & subgrid tuning).
- ``split``, the default, keeps the split-explicit barotropic scheme and the results of earlier releases.
- ``fs_nsub``, ``fs_rho_inf``, ``fs_tol`` and ``fs_verbose`` set the implicit scheme's sub-steps, damping, solver tolerance and log detail; the wizard shows the first three only in implicit mode.
- Pins below 0.15.0 keep the 0.14 namelist tier, and Forge rejects ``fs_mode: implicit`` there with an error naming 0.15.0. A run pinned to a ucla-roms branch without ucla-roms #420 gets the ``&IMPLICIT_FS`` group in its namelist, ignores it and keeps the split-explicit scheme.
