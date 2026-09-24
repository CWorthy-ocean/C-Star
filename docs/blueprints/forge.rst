.. _blueprints-forge:

Forge blueprint
===============

A forge blueprint is the input to Forge, the domain-generation application:
one YAML file describing the domain you want. Processing it produces every
input file a ROMS-MARBL simulation needs and a :doc:`ROMS-MARBL blueprint
<roms_marbl>` that runs it.

Most forge blueprints are written by the :doc:`wizard <../wizard>`, which
assembles one from catalog specs and your choices, then shows the result for
review. You can also start from a saved blueprint and edit the YAML directly.
Values are written out in full rather than referenced back to the catalog, so
a saved blueprint keeps producing the same domain even if the catalog entries
it came from are edited later.

What it contains
----------------

Besides the core fields every blueprint has (``name``, ``description``,
``application: forge``, ``working_dir``), a forge blueprint has these
sections:

``run``
   The simulation window: ``start_date``, ``end_date`` and the model reference
   date.
``domain``
   The grid: the named domain it came from, the grid parameters (``nx``,
   ``ny``, size, center, rotation, number of levels), topography source, open
   boundaries, processor layout, the time step and sponge viscosity derived
   from the grid, and optional nesting relationships or a pre-made grid file.
``forcing``
   The datasets behind the initial conditions and the surface, boundary,
   tidal and river forcing, including biogeochemical sources, and how each
   is interpolated onto the grid.
``cdr``
   Optional carbon dioxide removal forcing: none, a hand-authored
   description, or an imported netCDF file.
``datasets``
   The list of source datasets Forge must stage before generating inputs
   (derived from ``forcing``; see :doc:`../forge/source_datasets`).
``model_settings``
   The resolved ROMS-MARBL settings, one section per namelist group plus the
   compile-time ``cppdefs`` switches. These start from the model spec's
   defaults and are adjusted by the resolver for the chosen domain and
   forcing; the *Advanced settings* section of the wizard edits them.
``code``
   The pinned ROMS, MARBL and PIO repositories and the render templates, by
   commit.
``composition`` and ``provenance``
   Which catalog specs the blueprint was built from and with what overrides,
   the C-Star version that wrote it, and a content hash. Both are
   informational; the executor never reads the catalog.

A ``forge_blueprint_version`` field records the schema version. Blueprints
written by older versions are migrated when loaded; a blueprint newer than
the installed C-Star is rejected with a message saying so.

Example
-------

The bundled toy domain used in the :doc:`end-to-end tutorial
<../tutorials/end_to_end>`:

.. literalinclude:: ../forge-blueprint-example.wio-toy.yaml
   :language: yaml
   :lines: 1-60

:download:`Download the full file <../forge-blueprint-example.wio-toy.yaml>`.

Checking validity
-----------------

.. code-block:: console

   cstar blueprint check forge_blueprint.yaml

Processing
----------

.. code-block:: console

   cstar blueprint run forge_blueprint.yaml

The ``application: forge`` field routes the blueprint to Forge, which runs
three stages: stage the source data, generate the input files, and configure
the build (render ``cppdefs.opt`` and ``namelist.nml`` and write the
ROMS-MARBL blueprint). ``cstar forge run`` runs the same stages with Forge's
full option set:

.. code-block:: console

   cstar forge run forge_blueprint.yaml --help

The options you are most likely to want:

``--only-inputs grid,tidal``
   Generate only the named input categories (``grid``,
   ``initial_conditions``, ``surface``, ``boundary``, ``tidal``, ``river``,
   ``cdr``) and stop before the build configuration. Useful for a slow input
   you want to check by hand before generating the rest.
``--clobber``
   Regenerate inputs that already exist. Without it, existing files are
   reused.
``--no-data``, ``--no-generate``, ``--no-configure``
   Skip a stage.
``--working-dir PATH``
   Put this run's outputs somewhere other than the blueprint's
   ``working_dir``.
``--verbose``
   Timestamped logging and timing and memory instrumentation around the
   ROMS-Tools calls.
``--dask-num-workers N``, ``--no-dask``, ``--dask ...``
   Control how input generation is parallelized. The defaults suit a laptop
   and a single HPC node; ``--dask-num-workers`` caps the thread count on
   nodes with many cores.

What Forge writes
-----------------

Everything goes under the blueprint's ``working_dir``, used exactly as
written. The wizard fills it in for the machine it runs on
(``~/cstar/_forge_bp_runs/<name>`` on a laptop, the scratch data home on a
cluster), and a workplan assigns the step its own directory; see
:doc:`../hpc`.

.. code-block:: text

   <working_dir>/
     input_data/              grid, initial conditions, forcing netCDF files
     builds/compile-time/     cppdefs.opt
     builds/run-time/         namelist.nml, marbl_in
     blueprints/
       B_<name>.yaml          the ROMS-MARBL blueprint
       settings_B_<name>.yaml the resolved model settings, for reference

The ROMS-MARBL blueprint points at the generated files with absolute paths,
pins the same model code the forge blueprint did, carries the processor
layout and run window, and lists the rendered ``cppdefs.opt``, ``namelist.nml``
and ``marbl_in`` as its compile-time and run-time code. Run it with
``cstar blueprint run blueprints/B_<name>.yaml``; the last lines of Forge's
output print the exact command.

The settings sidecar is the complete resolved ``model_settings`` split into
the single compile-time section (``cppdefs``) and the run-time namelist
sections. It is informational; the simulation reads ``namelist.nml``.
