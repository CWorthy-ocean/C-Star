# Domain generation overview

C-STAR Forge turns a set of reusable catalog pieces (plus optional wizard input) into
a `ForgeBlueprint`, then processes that blueprint into ROMS-MARBL input files, compiled
code, and a downstream C-Star blueprint that C-Star runs. See `docs/developer-guide.md`
for the full module map and call chains; this page covers the workflow at a high level.

## Workflow

The C-STAR Forge workflow progresses through distinct steps, transforming catalog
specs into an executable simulation:

```{mermaid}
flowchart TD
    MS["ModelSpec:<br/>model.yaml"] -->|build_forge_blueprint| B[ForgeBlueprint]
    DS["DomainSpec:<br/>Domain.yaml"] -->|build_forge_blueprint| B
    FS["ForcingSpec"] -->|build_forge_blueprint| B
    OS["OutputSpec"] -->|build_forge_blueprint| B

    C["User input / wizard UI"] -->|domain, forcing, run window, overrides| B

    B -->|"python -m cstar_forge.run<br/>(from_forge_blueprint)"| D[ForgeExecutor]

    D -->|model_post_init| E["Initialize<br/>Create Grid<br/>Init Blueprint (in memory)<br/>Load Default Settings<br/>."]

    E -->|ensure_source_data| G["Source Data<br/>GLORYS, UNIFIED<br/>SRTM15, etc.<br/>."]

    G -->|generate_inputs| H["Generate Inputs<br/>(in memory)"]

    T["templates:<br/>cppdefs.opt.j2"] -.->|fetched via C-Star<br/>AdditionalCode| J

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

Model defaults (namelist sections, cppdefs, code refs) live directly in each model's
`model.yaml` under `catalog/ModelSpec/<model>/` — there is no separate
`settings-defaults.yaml`; the resolver overlays domain-, forcing-, and output-derived
values on top of that single file.

### Workflow Steps

0. **Resolve** (`build_forge_blueprint()`, `cstar_forge/forge_blueprint_resolve.py`)
   - Assemble a `ForgeBlueprint` from the catalog pieces (`ModelSpec`, `DomainSpec`,
     `ForcingSpec`, `OutputSpec`) plus any wizard-supplied domain/forcing/run overrides
   - This step, and everything before it, is dependency-light: no ROMS/C-Star/roms-tools
     required, so a UI backend can call it directly

1. **Initialize** (`model_post_init()` / `_initialize_roms_marbl_blueprint()`)
   - Construct a `ForgeExecutor` from the resolved `ForgeBlueprint`
     (`ForgeExecutor.from_forge_blueprint`), invoked via
     `python -m cstar_forge.run forge_blueprint.yaml`
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
   - Render Jinja2 templates (fetched via C-Star's `AdditionalCode` from the forge git ref):
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
