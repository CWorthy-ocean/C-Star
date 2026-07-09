# Domain generation overview

> This page described `CstarSpecBuilder`, which no longer exists — it was decomposed
> into `ForgeBlueprint` (the input) + `ForgeExecutor` (the processing engine, in
> `cstar_forge/forge/executor.py`). See `docs/developer-guide.md` for the current
> architecture and module map; the stage-by-stage flow below is otherwise unchanged.

## Workflow

The C-STAR Forge workflow progresses through distinct stages, transforming a model specification into an executable simulation:

```{mermaid}
flowchart TD
    S[settings-defaults.yml] --> A[model.yml]
    T["templates:<br/>cppdefs.opt.j2"] --> A[model.yml]
    A -->|build_forge_blueprint| B[ForgeBlueprint]

    C["User input / wizard UI"] -->|domain, forcing, run window| B

    B -->|from_forge_blueprint| D[ForgeExecutor]

    D -->|model_post_init| E["PRECONFIG<br/>Create Grid<br/>Init Blueprint<br/>Load Default Settings<br/>."]
    E -->|persist| F[B_preconfig.yml]
    
    F -->|ensure_source_data| G["Source Data<br/>GLORYS, UNIFIED<br/>SRTM15, etc.<br/>."]
    
    G -->|generate_inputs| H["POSTCONFIG<br/>"]
    P1[Pre-existing<br/>Blueprint] -->|load| H
    H -->|persist| I[B_postconfig.yml]
    
    I -->|configure_build| J["BUILD<br/>Render Templates<br/>Create Simulation<br/>."]

    J -->|persist| K[B_build.yml]
    
    K -->|build| L["Compile<br/>Setup C-Star<br/>Build ROMS/MARBL<br/>."]
    L --> M[Executable]

    M -->|run| N["RUN<br/>run-time settings<br/>run<br/>."]
    N -->|persist| O[B_run.yml]
    
    style E fill:#e1f5ff
    style H fill:#fff4e1
    style J fill:#e8f5e9
    style N fill:#fce4ec
```

### Workflow Stages

1. **PRECONFIG** (Initialization)
   - Load `ModelSpec` from the model's `model.yml` (under `catalog/ModelSpec/<model>/`)
   - Build a `ForgeBlueprint` (`build_forge_blueprint`) from the catalog pieces + domain/run
     inputs, then construct a `ForgeExecutor` from it (`ForgeExecutor.from_forge_blueprint`)
   - Initialize grid object from `grid_kwargs`
   - Create blueprint structure with placeholder data
   - Load default settings from the resolved `ForgeBlueprint.model_settings`
   - Persist blueprint to `B_{name}_preconfig.yml`

2. **POSTCONFIG** (Input Generation)
   - Prepare source datasets (`ensure_source_data()`)
   - Generate all input files (`generate_inputs()`):
     - Grid NetCDF files
     - Initial conditions
     - Surface forcing
     - Boundary forcing
     - Tidal forcing
     - River forcing
   - Update blueprint with actual file paths
   - Update settings with input-specific values
   - Persist blueprint to `B_{name}_postconfig.yml`

3. **BUILD** (Configuration)
   - Render Jinja2 templates (`configure_build()`):
     - Compile-time template → `cppdefs.opt`
     - Run-time → `namelist.nml` (via f90nml) + `marbl_in`
   - Update blueprint with rendered code locations
   - Create `ROMSSimulation` instance
   - Persist blueprint to `B_{name}_build.yml`
   - Compile model executable (`build()`)

4. **RUN** (Execution)
   - Prepare run directory (`pre_run()`)
   - Execute model simulation (`run()`)
   - Clean up (`post_run()`)
   - Persist blueprint to `B_{name}_run_{datestr}.yml`
