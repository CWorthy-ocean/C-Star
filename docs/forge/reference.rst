.. _forge-reference:

Reference
=========

Models (``model.yaml``)
------------------------

Example per-model ``model.yaml`` (at
``cstar/catalog/bundled/ModelSpec/cson_roms-marbl_v0.1/model.yaml``):

.. literalinclude:: ../../cstar/catalog/bundled/ModelSpec/cson_roms-marbl_v0.1/model.yaml
   :language: yaml

A second bundled model, ``pio-dev``, is the reference example for the PIO path
(``use_pio: true``, ucla-roms pinned to ``main``):

.. literalinclude:: ../../cstar/catalog/bundled/ModelSpec/pio-dev/model.yaml
   :language: yaml

The wizard's default model is ``roms-marbl-0.8-default`` (ucla-roms 0.8.0).

Domains (``DomainSpec/{grid}/Domain.yaml``)
---------------------------------------------

Current domains live in the catalog as one directory per grid, each holding a
``Domain.yaml``. Example
(``cstar/catalog/bundled/DomainSpec/wio-toy/Domain.yaml``, the toy domain used
in :doc:`index`):

.. literalinclude:: ../../cstar/catalog/bundled/DomainSpec/wio-toy/Domain.yaml
   :language: yaml

ROMS-MARBL blueprint (``B_{name}.yaml``)
-------------------------------------------

Processing a forge blueprint (``cstar forge run``, or ``cstar blueprint run``)
emits a **ROMS-MARBL blueprint** -- the YAML handoff that C-Star builds and
runs (``cstar blueprint run B_{name}.yaml``; see :doc:`../blueprints`). It is
written to the blueprint's working directory alongside a settings sidecar
(see `Blueprint settings sidecar`_ below).

What a current blueprint contains:

- **``run_time`` code payload**: ``namelist.nml`` (the single generated
  namelist) plus the model's static run-time files (today just ``marbl_in``).
- **``compile_time`` code payload**: ``cppdefs.opt`` -- the only rendered
  compile-time file; all other former ``*.opt`` outputs were absorbed into
  the namelist.
- **Input datasets**: one ``Resource`` entry (location + partitioned flag) per
  generated NetCDF input -- grid, initial conditions, surface/boundary
  forcing, tides, rivers, CDR.
- **``partitioning``**: ``n_procs_x``/``n_procs_y``, ``use_pio``, and -- when
  auto-tiling is enabled -- ``auto_tiling: true`` with ``n_cores`` instead of
  the explicit processor grid (ROMS then picks the tiling at runtime via
  ``MPI_MASKING``).
- **``runtime_params``**: ``start_date``/``end_date``, and the blueprint
  ``working_dir`` (there is no ``output_dir`` -- it was a pre-2.0.0 field
  superseded by ``working_dir``). There is no ``model_params`` section as of
  schema 3.0.0: the time step lives only in the namelist
  (``time_stepping.dt``), and ``use_pio`` moved into ``partitioning``. Forge
  leaves the schema's ``namelist_overrides`` map empty -- it emits the
  complete ``namelist.nml`` directly.

To see a complete, current example, process the bundled toy domain and
inspect the result:

.. code-block:: console

   cstar forge run docs/forge-blueprint-example.wio-toy.yaml
   # emits ~/cstar/_forge_bp_runs/cson_roms-marbl_v0.1_wio-toy_10procs/B_*.yaml

Blueprint settings sidecar
-----------------------------

Next to every emitted ROMS-MARBL blueprint, the executor persists a settings
sidecar (``settings_B_{name}.yaml``) -- the fully-resolved model settings,
stored separately so the blueprint itself stays uncluttered by configuration
detail.

Current structure (two top-level keys):

.. code-block:: yaml

   compile_time:
     cppdefs: { ... }        # the ONLY compile-time section
   run_time:
     title: ...
     output_root_name: ...
     reference_date_settings: { ... }
     param: { ... }           # namelist sections sit DIRECTLY under run_time --
     tides: { ... }           # there is no intermediate "roms.in:" grouping key
     marbl_bgc: { ... }
     # ... every other resolved model_settings section ...

The split rule is simple: ``cppdefs`` is the sole compile-time section; every
other section of the resolved ``model_settings`` is a run-time (namelist)
section. Generate a current example by processing the bundled toy domain
(``cstar forge run docs/forge-blueprint-example.wio-toy.yaml``) and
inspecting ``settings_B_*.yaml`` in the working directory.
