# C-Star

[![Conda Version](https://img.shields.io/conda/vn/conda-forge/cstar-ocean.svg)](https://anaconda.org/conda-forge/cstar-ocean)
[![PyPI](https://img.shields.io/pypi/v/cstar-ocean.svg)](https://pypi.org/project/cstar-ocean/)
[![Documentation Status](https://readthedocs.org/projects/c-star/badge/?version=latest)](https://c-star.readthedocs.io/en/latest/)
[![Unit tests](https://github.com/CWorthy-ocean/C-Star/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/CWorthy-ocean/C-Star/actions/workflows/unit_tests.yaml)
[![codecov](https://codecov.io/gh/CWorthy-ocean/C-Star/graph/badge.svg?token=HAPZGL2LWF)](https://codecov.io/gh/CWorthy-ocean/C-Star)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/CWorthy-ocean/C-Star/blob/main/LICENSE)

**C-Star** is an open-source system for building, running and sharing regional
ocean simulations. It is developed by ocean and biogeochemical modelers and
scientific software engineers at [[C]Worthy](https://cworthy.org) to support
Monitoring, Reporting and Verification (MRV) of ocean-based carbon dioxide
removal. Today it runs [UCLA-ROMS](https://github.com/CWorthy-ocean/ucla-roms)
coupled to the [MARBL](https://marbl-ecosys.github.io) biogeochemistry model,
on a laptop or on a supported HPC system, from the same description of the run.

## How it works

C-Star is organized around two ideas. An **application** is something you can
run: the ROMS-MARBL model, Forge (the tool that builds a new ROMS-MARBL
domain), or a smaller data-transformation or analysis step. A **blueprint** is
a YAML file holding every input an application needs to produce its result:
which model code to build, which input files to use, which settings to apply.
Given the same blueprint, an application produces the same result on any
supported machine, which is what makes a C-Star simulation shareable and
reproducible.

A simple ocean modeling project moves through three steps:

1. **Describe the domain.** The Forge wizard, a form that runs in your browser
   or as a Jupyter notebook, walks you through choosing a model configuration,
   a region and grid, forcing datasets, and a run window. It writes a
   *forge blueprint*.
2. **Generate the inputs.** Running that forge blueprint downloads the source
   datasets, builds the grid, initial conditions, boundary and surface forcing,
   and rivers and tides where requested, and writes the ROMS namelist. Its
   output is a *ROMS-MARBL blueprint* pointing at everything the model needs.
3. **Run the simulation.** Running the ROMS-MARBL blueprint fetches and
   compiles the model code and executes the simulation.

To run several simulations as one unit, a **workplan** lists blueprints as
*steps* with dependencies between them. C-Star schedules the steps, submits
them to the cluster's job scheduler, and tracks their status, so a chain such
as a spin-up followed by an experiment and its control can be launched and
monitored with a single run ID. Reusable pieces of a domain description (model
configurations, domains, forcing selections, output settings and saved
blueprints) live in a **catalog**; C-Star ships a small bundled catalog to
start from.

## Installation

C-Star is published on [conda-forge](https://anaconda.org/conda-forge/cstar-ocean)
as `cstar-ocean`. One install gives you the `cstar` command line, the
ROMS-MARBL application and Forge, including its wizard.

On a laptop or workstation, install `cstar-ocean-standalone`, which bundles
the compilers, MPI and netCDF toolchain that ROMS needs:

```
conda create -n cstar-env -c conda-forge cstar-ocean-standalone
conda activate cstar-env
cstar --version
```

On a supported HPC system, install `cstar-ocean` alone and let the site's
environment modules provide the toolchain. The
[installation guide](https://c-star.readthedocs.io/en/latest/installation.html)
covers the HPC variant, installing from source, and what to set up after
installing.

## Quick start

The three steps above, on a laptop, with a deliberately tiny Western Indian
Ocean domain that processes in minutes:

```
# 1. Build a forge blueprint in your browser; save or download it when done.
cstar forge wizard

# 2. Generate the domain. The last lines of output name the ROMS-MARBL
#    blueprint Forge wrote and the command to run it.
cstar blueprint run forge_blueprint.yaml

# 3. Compile ROMS-MARBL and run the simulation.
cstar blueprint run ~/cstar/blueprint_runs/forge/<name>/blueprints/B_<name>.yaml
```

The same command runs every blueprint; the blueprint's `application` field
decides which application handles it. GLORYS ocean reanalysis supplies the
initial and boundary conditions, so
[register for Copernicus Marine](https://c-star.readthedocs.io/en/latest/data_access.html)
before the first run. The
[end-to-end tutorial](https://c-star.readthedocs.io/en/latest/tutorials/end_to_end.html)
walks through each step in detail.

## Documentation

The full documentation lives at <https://c-star.readthedocs.io>.

- [Getting started](https://c-star.readthedocs.io/en/latest/installation.html):
  installation, dataset registration and configuration
- [Terminology and concepts](https://c-star.readthedocs.io/en/latest/terminology.html)
- User guide:
  [blueprints](https://c-star.readthedocs.io/en/latest/blueprints.html),
  [workplans](https://c-star.readthedocs.io/en/latest/workplans.html),
  [catalog](https://c-star.readthedocs.io/en/latest/catalog.html),
  [wizard](https://c-star.readthedocs.io/en/latest/wizard.html)
- [Domain generation with Forge](https://c-star.readthedocs.io/en/latest/forge/index.html)
- [Supported systems](https://c-star.readthedocs.io/en/latest/machines.html)
  and [running on HPC](https://c-star.readthedocs.io/en/latest/hpc.html)
- [API reference](https://c-star.readthedocs.io/en/latest/api.html)
- [Contributor guide](https://c-star.readthedocs.io/en/latest/contributing.html)

## Related projects

- [UCLA-ROMS](https://github.com/CWorthy-ocean/ucla-roms): the Regional Ocean
  Modeling System with MARBL biogeochemistry, the model C-Star builds and runs
- [ROMS-Tools](https://github.com/CWorthy-ocean/roms-tools): generates ROMS
  input files from source datasets and analyzes model output; Forge drives it
- [MARBL](https://marbl-ecosys.github.io): the Marine Biogeochemistry Library
  coupled to ROMS

## Feedback and contributions

Bug reports and feature requests are welcome as
[GitHub Issues](https://github.com/CWorthy-ocean/C-Star/issues). We also
accept contributions as pull requests; the
[contributor guide](https://c-star.readthedocs.io/en/latest/contributing.html)
covers the development environment, the test suite and building the
documentation.

## License

C-Star is openly available and permissively licensed under the
[Apache License 2.0](https://github.com/CWorthy-ocean/C-Star/blob/main/LICENSE).
Copyright 2026 [C]Worthy LLC.
