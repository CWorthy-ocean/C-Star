C-Star
======

**C-Star** is an open-source system for building, running and sharing regional
ocean simulations. It is developed by ocean and biogeochemical modelers and
scientific software engineers at `[C]Worthy <https://cworthy.org>`_ to support
Monitoring, Reporting and Verification (MRV) of ocean-based carbon dioxide
removal. Today it runs `UCLA-ROMS <https://github.com/CWorthy-ocean/ucla-roms>`_
coupled to the `MARBL <https://marbl-ecosys.github.io>`_ biogeochemistry model,
on a laptop or on a supported HPC system, from the same description of the run.

How it works
------------

C-Star is organized around two ideas.

An **application** is something you can run:
- the ROMS-MARBL model
- Forge, the tool that builds a new ROMS-MARBL domain
- any number of smaller data transformation steps or analysis packages

A **blueprint** is a YAML file holding every input an application needs to produce its result: which model
code to build, which input files to use, which settings to apply. Given the
same blueprint, an application produces the same result on any supported
machine, which is what makes a C-Star simulation shareable and reproducible.

A simple ocean modeling project moves through three basic steps:

1. **Describe the domain.** The Forge :ref:`wizard <wizard>`, a form that runs in your
   browser or as a Jupyter notebook, walks you through choosing a model configuration, a region and
   grid, forcing datasets, and a run window. It writes a *forge blueprint*.
2. **Generate the inputs.** Running that forge blueprint downloads the source
   datasets, builds the grid, initial conditions, boundary and surface forcing,
   and rivers and tides where requested, and writes the ROMS namelist. Its
   output is a *ROMS-MARBL blueprint* pointing at everything the model needs.
3. **Run the simulation.** Running the ROMS-MARBL blueprint fetches and
   compiles the model code and executes the simulation. Output lands in the
   blueprint's working directory.

The wizard can be run on any machine, even your laptop, and the Forge blueprint can
then be moved to a different machine, like an HPC, where you want heavier computations to take place,
or where collections of source data may be staged among your working group.

Running any blueprint independently can be done with the command  ``cstar blueprint run <blueprint.yaml>``.
To run several simulations as one unit, a **workplan** lists the blueprints
to run as *steps*, with dependencies between them. C-Star schedules the steps,
submits them to the cluster's job scheduler, and tracks their status, so a
chain such as a spin-up followed by an experiment and its control can be
launched and monitored with a single run ID. Steps can pass information to one
another at run time: for example, a restart file from one simulation becoming
the initial conditions of the next.

Reusable pieces of a domain description live in a **catalog**: model
configurations, domains, forcing selections and output settings, plus saved
blueprints. C-Star ships a small bundled catalog to start from, and everything
you save from the wizard goes into your own catalog layer.

Principles
----------

C-Star is built with these principles in mind:

- **Scientific integrity.** The models are trusted, community-developed
  codes with decades of development behind them, and community involvement
  keeps the system tracking the best available science.
- **Transparency and accessibility.** The code is open, so both academic and
  commercial users can inspect and trust what it does.
- **Reproducibility and auditability.** Simulations that underpin carbon
  removal claims must be shareable and reproducible by others. Blueprints
  make a simulation a complete, reviewable document.
- **Ease of use.** A consistent workflow that diverse users can apply the
  same way.
- **Standardization.** A common framework gives a consistent level of quality
  across projects.

.. toctree::
    :maxdepth: 1
    :caption: Getting Started

    Installing C-Star <installation>
    Registering for datasets <data_access>
    configuration

.. toctree::
    :maxdepth: 1
    :caption: Terminology and Concepts

    terminology

.. toctree::
    :maxdepth: 1
    :caption: Laptop-Runnable Examples

    End to end: a new domain to a running simulation <tutorials/end_to_end>
    Understanding a ROMS-MARBL blueprint <tutorials/tutorial_bp>
    Understanding the basics of a workplan <tutorials/tutorial_wp>

.. toctree::
    :maxdepth: 2
    :caption: User Guide

    blueprints
    workplans
    catalog
    wizard

.. toctree::
    :maxdepth: 1
    :caption: Domain generation (Forge)

    forge/index
    forge/specs
    forge/source_datasets

.. toctree::
    :maxdepth: 1
    :caption: Deployment

    machines
    hpc

.. toctree::
    :maxdepth: 1
    :caption: Reference

    api-blueprint
    api-orchestration
    api-forge
    api
    schemas/index

.. toctree::
    :maxdepth: 1
    :caption: For Developers

    contributing
    custom_applications
    system-registration
    developers/forge_internals
    developers/forge_templates
    developers/forge_source_data
    developers/forge_input_data
    developers/catalog_design
    releases
