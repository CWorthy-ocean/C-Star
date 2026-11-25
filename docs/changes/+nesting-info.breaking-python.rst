Nesting info is keyed by child name in the Python API.
- ``UclaRomsBlueprint.nesting_info`` and ``ROMSSimulation.nesting_info`` are mappings from child name to nesting dataset.
- Forge's ``Domain.grid_kwargs_child``, ``metadata_child`` and ``nesting_include_pressure_fluxes`` are replaced by ``children`` and ``nest_period``.
- ``cstar.roms.outputs.EXTRACT_ROOT_NAME`` is removed; use ``DEFAULT_CHILD_NAME``.
