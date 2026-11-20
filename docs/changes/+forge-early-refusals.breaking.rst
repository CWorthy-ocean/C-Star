Forge refuses, when a blueprint is resolved or validated and before any input is generated, settings that used to be ignored or to stop the run only after the ROMS build; the wizard's settings check lists them too.
- An unknown section or key in ``model_settings``, overrides or OutputSpecs, such as ``botom_drag``, ``ocean_vars.wrt_fiel_his`` or ``cppdefs.diagnostic``, is refused with the nearest valid name. A key the pinned ucla-roms release does not have still gets its message naming the release that adds it.
- ``pipe_frc.pipe_source: true`` is refused: Forge writes no pipe forcing, so ROMS stopped at initialisation.
- ``n_procs_x`` or ``n_procs_y`` that leave a tile with no grid points (16 × 16 tiles on an 8 × 8 grid, or 9 tiles across 10 points) are refused with the largest count that works.
- ``run.model_reference_date`` with a time of day is refused when the blueprint loads; use midnight. ROMS refused such a run after the build.
