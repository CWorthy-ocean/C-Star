# Overview

C-SON Forge streamlines the creation of ROMS-MARBL domains by automating the generation of all required input files using [ROMS Tools](https://roms-tools.readthedocs.io/en/latest/index.html). 
The files include grids, initial conditions, boundary and surface forcing, rivers, and tidal forcing—from a variety of observational and reanalysis datasets. 


The tool produces **blueprint** YAML files that capture the complete configuration and file paths for each domain, enabling reproducible model setups that can be integrated into C-Star workflows.

## Key Features

- **Automated Input Generation**: Generate all ROMS input files (grid, initial conditions, forcing, boundaries, rivers, tidal forcing) from source datasets
- **Multi-Dataset Support**: Integrates with multiple data sources including:
  - GLORYS (ocean reanalysis)
  - ERA5 (atmospheric reanalysis)
  - UNIFIED_BGC (biogeochemical climatology)
  - SRTM15 (bathymetry)
  - DAI (river discharge)
  - TPXO (tidal forcing)
- **Blueprint System**: Automatically generates YAML blueprints that document:
  - Complete model specification (repositories, conda environments, input configurations)
  - All generated input file paths (both full and partitioned)
  - Domain configuration (grid name, time ranges, boundaries, processor layout)
  - Source data provenance
- **Reproducible Workflows**: Blueprints serve as complete descriptors that enable:
  - Exact reproduction of model configurations
  - Integration with C-Star workflow management
  - Version control and sharing of domain setups
- **Model Building**: Automated compilation of ROMS and MARBL executables with support for multiple compilers and MPI configurations
- **Execution Management**: Run models locally or submit to HPC clusters (SLURM, PBS) with automatic log file management

## Project Structure

```
cson-forge/
├── cson_forge/                 # Main package directory
│   ├── _core.py                # Core classes (CstarSpecBuilder, CstarSpecEngine)
│   ├── models.py               # Model specification classes (ModelSpec, etc.)
│   ├── source_data.py          # Dataset download and preparation
│   ├── input_data.py           # Input file generation
│   ├── settings.py             # Template rendering
│   ├── config.py               # Path management and system detection
│   ├── catalog.py              # Blueprint catalog
│   ├── models.yml              # Model configuration specifications
│   ├── domains.yml             # Established domain configurations
│   ├── machines.yml            # Machine-specific settings
│   ├── blueprints/             # Generated blueprint YAML files
│   │   └── {model}_{grid}/
│   │       ├── B_{name}_{stage}.yml      # Blueprint files (preconfig, postconfig, build, run)
│   │       ├── settings_B_{name}_{stage}.yml  # Settings sidecar files (same directory)
│   │       └── _{input_type}.yml         # Input-specific blueprints
│   ├── builds/                 # Model compilation directories
│   │   └── {model}_{grid}/
│   │       └── opt/            # Rendered configuration files
│   │           ├── *.opt       # Compile-time options (cppdefs, param, etc.)
│   │           ├── roms.in     # Run-time configuration
│   │           └── Makefile    # Build configuration
│   └── model-configs/          # Model templates and defaults
│       └── {model}/
│           └── templates/
│               ├── compile-time/        # Jinja2 templates for compile-time configs
│               │   └── *.j2
│               ├── compile-time-defaults.yml
│               ├── run-time/           # Jinja2 templates for run-time configs
│               │   └── *.j2
│               └── run-time-defaults.yaml
├── workflows/                  # Example notebooks and workflows
│   ├── generate-models/       # Domain generation notebooks
│   └── analysis/              # Analysis notebooks
├── docs/                      # Documentation
└── README.md
```

## Blueprint System

Blueprints are YAML files that capture the complete state of a domain configuration. Blueprints enable:
1. **Reproducibility**: Exact recreation of model setups from a single YAML file
2. **C-Star Integration**: Blueprints can be consumed by C-Star workflows to orchestrate model runs
3. **Documentation**: Self-documenting domain configurations with full provenance
4. **Version Control**: Track domain evolution and share configurations across teams

