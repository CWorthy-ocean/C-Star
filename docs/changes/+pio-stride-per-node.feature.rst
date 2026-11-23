With ParallelIO, C-Star can choose ``pio_stride`` for the machine a run lands on: an empty ``pio_stride`` in the namelist becomes the number of MPI ranks on one node when the run starts, so each node has one I/O rank instead of every rank doing I/O.
- The bundled ``roms-marbl-0.15-default`` and ``roms-marbl-0.16-default`` models leave ``pio_stride`` empty, so Forge blueprints made from them get one I/O rank per node. Older bundled models, existing blueprints and namelists that set a number keep that number.
- Set ``namelist_overrides: {pio_settings: {pio_stride: null}}`` to have C-Star choose for an existing ``ucla_roms`` blueprint. In the wizard, a blank PIO stride box means the same.
- Forge blueprints are saved as version 11; an older C-Star asks to be upgraded before reading one.
