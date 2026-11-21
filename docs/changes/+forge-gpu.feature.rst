Forge and the blueprint wizard can build inputs and a ``ucla_roms_gpu`` blueprint for roms-marbl-gpu v0.1.3-beta, which is experimental: choose ``roms-marbl-gpu-0.1.3-default`` as the ModelSpec (in the wizard, set **Runs on** to GPU in the model section).
- For now Forge refuses surface restoring (use the ForcingSpec ``glorys-era5-unified-no-restoring``), ParallelIO, automatic tiling, compile-time overrides, nesting, and the namelist settings the GPU model ignores or stops on.
- The workplan builder sizes a GPU step by its GPUs and nodes, and the forge-then-run recipe submits the GPU run.
