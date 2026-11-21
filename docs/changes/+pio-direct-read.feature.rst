With ParallelIO, ucla-roms 0.16.0 reads the grid and initial (or restart) files directly with O_DIRECT, which shortens start-up on large domains; Forge and the wizard set this with ``pio_settings.pio_direct_read`` (in the wizard, under Advanced settings, Physics & subgrid tuning, Parallel I/O).
- ``on``, the default in ``roms-marbl-0.16-default``, gives the same results bit for bit as reading through ParallelIO.
- Set ``off`` on a file system that rejects or slows down O_DIRECT reads; ``verify`` reads both ways and reports values that differ.
