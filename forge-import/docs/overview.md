# Overview

C-STAR Forge streamlines the creation of ROMS-MARBL domains by automating the generation of all required input files using [ROMS Tools](https://roms-tools.readthedocs.io/en/latest/index.html).
The files include grids, initial conditions, boundary and surface forcing, rivers, and tidal forcing—from a variety of observational and reanalysis datasets.

## The workflow at a glance

1. **Author a forge blueprint.** An interactive **wizard** (run in Jupyter, or served as
   a standalone [Voilà](https://voila.readthedocs.io/) web app) helps you assemble a
   `ForgeBlueprint` from catalog **specs** — model (`ModelSpec`), domain (`DomainSpec`),
   forcing (`ForcingSpec`), and output (`OutputSpec`). The result is a single YAML file
   that fully describes what to generate. Every value is editable in the wizard, and the
   blueprint records the provenance of each choice.
2. **Process the blueprint.** The **forge application** consumes the forge blueprint:

   ```bash
   python -m cstar_forge.run path/to/forge_blueprint.yaml
   ```

   It downloads/prepares source data, generates all ROMS input files, renders model
   settings, and emits a **ROMS-MARBL blueprint** (`RomsMarblBlueprint`) — a YAML file
   capturing the complete configuration and file paths of the resulting setup.
3. **Run the simulation.** [C-Star](https://c-star.readthedocs.io) consumes the
   ROMS-MARBL blueprint to build the model and execute the actual ROMS-MARBL simulation.

## Key Features

- **Interactive blueprint wizard**: Assemble, review, and save a `ForgeBlueprint` from
  catalog specs — in a notebook or a code-free web form
- **Automated Input Generation**: Generate all ROMS input files (grid, initial conditions, forcing, boundaries, rivers, tidal forcing) from source datasets
- **Multi-Dataset Support**: Integrates with multiple data sources including:
  - GLORYS (ocean reanalysis)
  - ERA5 (atmospheric reanalysis)
  - UNIFIED_BGC (biogeochemical climatology)
  - SRTM15 (bathymetry)
  - DAI / GLOFAS (river discharge)
  - TPXO (tidal forcing)
- **ROMS-MARBL Blueprint System**: Automatically generates YAML blueprints that document:
  - Complete model specification (repositories, code versions, input configurations)
  - All generated input file paths (both full and partitioned)
  - Domain configuration (grid name, time ranges, boundaries, processor layout)
  - Source data provenance
- **Reproducible Workflows**: Blueprints serve as complete descriptors that enable:
  - Exact reproduction of model configurations
  - Integration with C-Star workflow management
  - Version control and sharing of domain setups

## Project Structure

```
cstar-forge/
├── cstar_forge/                 # Main package directory
│   ├── forge_blueprint_resolve.py  # resolver: build_forge_blueprint(...)
│   ├── forge_blueprint_wizard.py   # ForgeBlueprintWizard (ipywidgets UI) +
│   │                               # ForgeBlueprintWizardApp (adds catalog-location bar)
│   ├── forge-blueprint-wizard.ipynb     # wizard notebook (run in Jupyter)
│   ├── forge-blueprint-wizard-app.ipynb # wizard app notebook (served by Voilà)
│   ├── models.py               # Spec classes (ModelSpec, etc.)
│   ├── domain_catalog.py       # DomainCatalog: scans the catalog, exposes accessors
│   ├── config.py               # Path management and system detection
│   ├── run.py                  # CLI entry point: python -m cstar_forge.run forge_blueprint.yaml
│   ├── forge/                  # The forge application (execution engine; see
│   │   │                       # docs/developer-guide.md — relocates into C-Star as one unit)
│   │   ├── app.py                  # ForgeRunner/ForgeApplication (C-Star application)
│   │   ├── forge_blueprint.py      # ForgeBlueprint — the forge application's blueprint
│   │   ├── forge_blueprint_engine.py # process_forge_blueprint(); ForgeBlueprintExecutor Protocol
│   │   ├── executor.py         # ForgeExecutor — the processing engine
│   │   ├── input_data.py       # Input file generation
│   │   ├── source_data.py      # Dataset download and preparation
│   │   ├── settings.py         # Template rendering
│   │   └── namelist_model.py   # RunTimeSettings + build_namelist
│   └── catalog/                # Bundled spec catalog (+ BlueprintCatalog API)
│       ├── ModelSpec/{model}/model.yaml    # Code repos, templates, settings, defaults
│       ├── DomainSpec/{grid}/Domain.yaml   # Grid definitions
│       ├── ForcingSpec/{name}/Forcing.yaml # Forcing source configurations
│       ├── OutputSpec/{name}/Output.yaml   # Output configurations
│       ├── Machines/{system}.yaml          # Machine descriptions
│       └── blueprints/                     # Example/saved blueprints (legacy/ holds
│                                           # pre-refactor layouts)
├── templates/                  # Render templates (cppdefs.opt.j2, marbl_in), decoupled
│                                # from ModelSpec — fetched by ForgeExecutor via C-Star's
│                                # AdditionalCode
├── workflows/                 # Example notebooks and workflows
│   ├── computing-benchmarks/
│   ├── generate-models/       # Domain generation notebooks
│   ├── skill-assessment/
│   ├── source-data/
│   └── visualization/
├── docs/                      # Documentation (dev-notes/ holds historical planning docs)
└── README.md
```

See `docs/developer-guide.md` for the current module map, the authoring/execution split,
and the end-to-end call chain (catalog pick → `ForgeBlueprint` → `ForgeExecutor`).

## ROMS-MARBL Blueprint System

ROMS-MARBL blueprints (`RomsMarblBlueprint`) are YAML files that capture the complete state of a domain configuration. Blueprints enable:
1. **Reproducibility**: Exact recreation of model setups from a single YAML file
2. **C-Star Integration**: Blueprints can be consumed by C-Star workflows to orchestrate model runs
3. **Documentation**: Self-documenting domain configurations with full provenance
4. **Version Control**: Track domain evolution and share configurations across teams
