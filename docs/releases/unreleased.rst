.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- ``--resume`` can no longer be combined with ``--var`` or ``--varfile``; a resumed run always continues with the variables it was started with. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)

New features
~~~~~~~~~~~~


- ``cstar workplan run my_workplan.yaml --resume`` resumes the run that workplan started, deriving the run-id from the workplan name; ``--run-id <id> my_workplan.yaml --resume`` uses the explicit id. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- A workplan path given with ``--resume`` is checked against the run's recorded original workplan; a changed, missing or unreadable record is reported as a usage error naming ``--run-id <id>`` as the way to resume the run as recorded. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)

Bug Fixes
~~~~~~~~~


- Reading or writing a file with an unrecognized extension no longer prints "Using default persistence mode ``yaml`` for file ``{path}``" to the terminal. (`#703 <https://github.com/CWorthy-ocean/C-Star/pull/703>`_)

Improvements
~~~~~~~~~~~~


- The run-directory backup of the original workplan now has a single owner shared by the run preparation and the resume check. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)

Miscellaneous
~~~~~~~~~~~~~

- Documentation overhaul, pulling in Forge docs: (`#701 <https://github.com/CWorthy-ocean/C-Star/pull/701>`_)

  - Landing page: "How it works" (applications and blueprints, the three-step forge -> ROMS-MARBL flow, workplans, the catalog) plus the original principles; the sidebar is reorganized (see below).
  - Getting Started: installation rewritten around conda (laptop/HPC tabs, verify, after-install steps, brief source install); new "Registering for datasets" page (Copernicus Marine, TPXO, user-staged sources, where files go).
  - Terminology: entries for application (with the built-in ones), ROMS-MARBL blueprint, Forge, forge blueprint, spec, catalog, wizard, step, directive and working directory; the HPC deployment walkthrough tightened.
  - Laptop examples: new "End to end: a new domain to a running simulation" (wizard -> ``cstar blueprint run`` forge blueprint -> ``cstar blueprint run`` ROMS-MARBL blueprint); the two notebooks reframed as "Understanding a ROMS-MARBL blueprint" and "Understanding the basics of a workplan", with trimmed prerequisites (``cstar env register-kernel``), C-SON Forge references replaced, stale claims corrected, links pointing at source files so nbsphinx resolves them, and a stray copied code cell removed.
  - User Guide: Blueprints split into the core page plus ``blueprints/roms_marbl`` and a new ``blueprints/forge`` (sections, example, ``cstar forge run`` options, what Forge writes); Workplans refreshed (``Step.directives``, ``--var``/``--varfile`` runtime variables, ``{{<scope>: <step>}}`` directory placeholders, ``clobber``/``resume`` workflow overrides) with a new ``workplans/directives`` subpage (``apply-overrides``, ``continue-from``, ``nest-from``, deprecated keys, ordering, example, ``cstar blueprint run --directives``); new ``catalog`` page (contents, layers, ``CSTAR_CATALOG``, sharing, adding entries, legacy location); new ``wizard`` page (web app, login node over SSH, notebook form; each card; where saves go).
  - Forge section reduced to an overview, ``forge/specs`` (the five spec kinds, ``model.yaml`` in detail, the bundled model specs, the example notebook) and ``forge/source_datasets`` (downloaded, streamed and user-provided sources, pre-staging). Removed pages: getting_started, installation_hpc, machine_config, installation_changes, catalog, model_spec, reference, source_data.
  - Deployment: new ``hpc`` page (scheduler configuration, per-system source-data cache and working-directory table, ``PYTHONNOUSERSITE``, Jupyter kernels, wizard over SSH).

- Reference: new ``api-forge`` autosummary page (blueprint models, application, resolver and ModelSpec, catalog, execution). (`#701 <https://github.com/CWorthy-ocean/C-Star/pull/701>`_)

  - For Developers: ``developers/forge_internals``, ``developers/forge_input_data``, ``developers/forge_source_data``, ``developers/catalog_design`` (moved or extracted from the Forge pages) and a new ``developers/forge_templates`` (rendering, commit/hash pinning, bundled-vs-fetch staging); ``custom_applications`` points at the internals page as its worked example.
  - Banners: the Sphinx prolog "early phase of development" attention block and the README warning are removed, along with the "in development" snippet; the workplan state and compute-environment sections describe the current behaviour factually instead.

- Workplan guide and ``--resume`` help text describe resuming by workplan path and the ``--var``/``--varfile`` restriction. (`#702 <https://github.com/CWorthy-ocean/C-Star/pull/702>`_)
- Added a test that an unrecognized extension resolves to YAML without printing anything. (`#703 <https://github.com/CWorthy-ocean/C-Star/pull/703>`_)

