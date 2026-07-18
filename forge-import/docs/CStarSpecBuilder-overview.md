# Domain generation overview

> This page described `CstarSpecBuilder`, which no longer exists — it was decomposed
> into `ForgeBlueprint` (the input) + `ForgeExecutor` (the processing engine, in
> `cstar_forge/forge/executor.py`). See `docs/developer-guide.md` for the current
> architecture and module map.
>
> The executor no longer models a "preconfig/postconfig/build/run" blueprint stage
> machine. It builds the blueprint up in memory across three steps and persists it
> to disk exactly once, at the end of `configure_build()`, as a single `B_{name}.yaml`
> (+ `settings_B_{name}.yaml` sidecar). The flow below reflects that.

## Workflow

The C-STAR Forge workflow progresses through distinct steps, transforming a model specification into an executable simulation:

```{mermaid}
flowchart TD
    S[settings-defaults.yaml] --> A[model.yaml]
    T["templates:<br/>cppdefs.opt.j2"] --> A[model.yaml]
    A -->|build_forge_blueprint| B[ForgeBlueprint]

    C["User input / wizard UI"] -->|domain, forcing, run window| B

    B -->|from_forge_blueprint| D[ForgeExecutor]

    D -->|model_post_init| E["Initialize<br/>Create Grid<br/>Init Blueprint (in memory)<br/>Load Default Settings<br/>."]

    E -->|ensure_source_data| G["Source Data<br/>GLORYS, UNIFIED<br/>SRTM15, etc.<br/>."]

    G -->|generate_inputs| H["Generate Inputs<br/>(in memory)"]

    H -->|configure_build| J["Configure Build<br/>Render Templates<br/>Create Simulation<br/>."]
    J -->|persist| K[B_name.yaml]

    K -->|build| L["Compile<br/>Setup C-Star<br/>Build ROMS/MARBL<br/>."]
    L --> M[Executable]

    M -->|run| N["Run<br/>run-time settings<br/>run<br/>."]

    style E fill:#e1f5ff
    style H fill:#fff4e1
    style J fill:#e8f5e9
    style N fill:#fce4ec
```

### Workflow Steps

1. **Initialize** (`model_post_init()` / `_initialize_roms_marbl_blueprint()`)
   - Load `ModelSpec` from the model's `model.yaml` (under `catalog/ModelSpec/<model>/`)
   - Build a `ForgeBlueprint` (`build_forge_blueprint`) from the catalog pieces + domain/run
     inputs, then construct a `ForgeExecutor` from it (`ForgeExecutor.from_forge_blueprint`)
   - Initialize grid object from `grid_kwargs`
   - Create the in-memory blueprint structure with placeholder data
   - Load default settings from the resolved `ForgeBlueprint.model_settings`
   - Nothing is persisted yet

2. **Generate inputs** (`generate_inputs()`)
   - Prepare source datasets (`ensure_source_data()`)
   - Generate all input files:
     - Grid NetCDF files
     - Initial conditions
     - Surface forcing
     - Boundary forcing
     - Tidal forcing
     - River forcing
   - Update the in-memory blueprint with actual file paths
   - Update in-memory settings with input-specific values
   - Nothing is persisted yet

3. **Configure build** (`configure_build()`)
   - Render Jinja2 templates:
     - Compile-time template → `cppdefs.opt`
     - Run-time → `namelist.nml` (via f90nml) + `marbl_in`
   - Update blueprint with rendered code locations
   - Create `ROMSSimulation` instance
   - **Persist the blueprint to `B_{name}.yaml`** (+ `settings_B_{name}.yaml` sidecar)
     — the only time it is written to disk
   - Compile model executable (`build()`)

4. **Run** (`run()`)
   - Prepare run directory (`prep_cstar_environment()`)
   - Hand `B_{name}.yaml`'s path to C-Star and execute the model simulation
