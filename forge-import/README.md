# C-Star Forge

A utility for generating new regional oceanographic modeling domains and creating reproducible [C-Star](https://c-star.readthedocs.io) workflows through blueprint descriptors.

[![Run Tests](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml/badge.svg)](https://github.com/CWorthy-ocean/cstar-forge/actions/workflows/tests.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/CWorthy-ocean/cstar-forge/graph/badge.svg)](https://codecov.io/gh/CWorthy-ocean/cstar-forge)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/cstar-forge.svg)](https://anaconda.org/conda-forge/cstar-forge)

```{image} docs/assets/csforge.png
:alt: C-Star Forge Logo
:class: csforge-logo
:align: center
```

```{warning}
This project is still in an early phase of development.

You are welcome to try out using the package, but be aware that development is ongoing and we cannot yet guarantee backwards compatibility.
```

## What is C-Star Forge?

[C-Star](https://c-star.readthedocs.io) is built on a system of **applications**
(a model or computation you want to run) and **blueprints** (the inputs to an
application that make its result reproducible). C-Star Forge is the application
for *creating* new ROMS-MARBL domains.

Setting up a regional ocean simulation has traditionally meant weeks of bespoke
work: designing a grid, collecting and regridding forcing datasets, hand-editing
model configuration files, and hoping the result is reproducible on the next
machine. C-Star Forge automates that path for ROMS-MARBL domains. You describe
*what* you want — a region, a resolution, a time window, forcing sources — and
Forge produces everything the model needs to run, in a form that
[C-Star](https://c-star.readthedocs.io) can build and execute anywhere.

The whole workflow revolves around two YAML documents:

- A **forge blueprint** describes the domain you want. It is the single input to
  Forge, and it is complete: given the same forge blueprint, Forge generates the
  same setup.
- A **ROMS-MARBL blueprint** describes the setup Forge generated — the model
  code, input files, and runtime settings of a concrete, runnable simulation.
  It is Forge's output, and C-Star's input.

## How it works

C-Star Forge takes you from "I want a regional ROMS-MARBL domain here" to a
running simulation in three conceptual steps:

1. **Build a forge blueprint.** An interactive **wizard** — a point-and-click
   web form, also usable inside Jupyter — walks you through the choices: a model
   spec, a domain from the bundled catalog (or your own), forcing sources,
   output settings. The result is saved as a single `forge_blueprint.yaml`.
   Because the wizard is just a front-end for writing this file, you can also
   start from an example blueprint and edit it by hand.

2. **Process the blueprint.** The Forge executor consumes the forge blueprint
   on the machine where the data should live. It fetches and prepares the
   source datasets, generates every ROMS input file (grid, initial conditions,
   surface and boundary forcing, rivers, tides), renders the model settings,
   and emits the ROMS-MARBL blueprint describing the finished setup.

3. **Run the simulation.** C-Star consumes the ROMS-MARBL blueprint to fetch
   and compile the model code and execute the simulation — on your laptop or on
   a supported HPC system. Forge is out of the picture at this point: the
   handoff is the blueprint file alone.

The input files are generated with
[ROMS Tools](https://roms-tools.readthedocs.io/en/latest/index.html), drawing on
GLORYS (ocean reanalysis), ERA5 (atmospheric reanalysis), UNIFIED_BGC
(biogeochemical climatology), SRTM15 (bathymetry), DAI/GLOFAS (river
discharge), and TPXO (tides).

Each step can happen on a different machine. A common pattern is building the
blueprint in a browser on your laptop, processing it on the cluster where the
forcing data lives, and running the simulation through C-Star's scheduler
support on that same cluster.

## Where to go next

- **[Getting Started](https://cworthy-ocean.github.io/cstar-forge/getting-started/)** —
  install with one conda command and take a small toy domain from wizard to
  running simulation.
- **[Installation](https://cworthy-ocean.github.io/cstar-forge/installation/)** —
  HPC installs, developer setups, and reproducible locked environments.
- **[Documentation](https://cworthy-ocean.github.io/cstar-forge/)** — concepts,
  the domain catalog, machine configuration, and developer guides.
