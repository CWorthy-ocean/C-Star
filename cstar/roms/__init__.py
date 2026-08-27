"""ROMS-specific simulation, codebase, dataset, and namelist components.

Import from the concrete submodules (e.g. ``cstar.roms.simulation``,
``cstar.roms.input_dataset``): this package deliberately re-exports nothing,
so that importing a leaf module such as ``cstar.roms.namelist`` does not drag
in ``ROMSSimulation`` and its application-layer dependencies.
"""
