.. _blueprints-forge:

Forge blueprint
===============

A forge blueprint is the input to Forge, the domain-generation application:
one YAML file describing the domain you want. Processing it produces every
input file a UCLA-ROMS simulation needs and a :doc:`UCLA-ROMS blueprint
<../ucla_roms/blueprint>` that runs it. The Forge blueprint is particularly
interesting, in that it encapsulates all of the choices needed to set up and
run a full ocean model, all in a lightweight YAML file. It is entirely portable
and shareable: by simply handing a Forge blueprint to a colleague, they can reproduce
the same model on their own machine.

Most forge blueprints are written by the :doc:`wizard <../../catalog_wizard/blueprints>`, which
assembles one from catalog specs and your choices, then shows the result for
review. You can also start from a saved blueprint and edit the YAML directly.
Values are written out in full rather than referenced back to the catalog, so
a saved blueprint keeps producing the same domain even if the catalog entries
it came from are edited later.

What it contains
----------------

Besides the core fields every blueprint has (``name``, ``description``,
``application: forge``, and an optional ``working_dir`` that a forge blueprint
normally omits), a forge blueprint has these sections:

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
``carbonate_sensitivity``
   Optional, and only for ``bgc_mode: cdr_lite``: the carbonate sensitivity
   files (``ddic_dco2`` and ``ddic_dalk``) a CDR-lite build reads as surface
   forcing, as a ``files:`` list of ``location`` and ``content_hash``
   entries. It is normally left out, and the ``carbonate-sensitivity-from``
   :doc:`workplan directive <../../workplans/directives>` supplies the files at
   run time from an earlier UCLA-ROMS run with MARBL. When it is given, Forge
   checks the files and copies them into ``input_data/``.
``datasets``
   The list of source datasets Forge must stage before generating inputs
   (derived from ``forcing``; see :doc:`source_datasets`).
``model_settings``
   The resolved UCLA-ROMS settings, one section per namelist group plus the
   compile-time ``cppdefs`` switches. These start from the model spec's
   defaults and are adjusted by the resolver for the chosen domain and
   forcing; the *Advanced settings* section of the wizard edits them.
``code``
   The pinned ROMS, MARBL and PIO repositories and the render templates, by
   commit.
``composition`` and ``provenance``
   Which catalog specs the blueprint was built from and with what overrides,
   and what produced the file: when (``generated_at``) and by which tool
   (``generated_by``, holding the tool, a unique id, the system and the
   versions of C-Star and roms-tools), plus a content hash. Both are
   informational; Forge never reads the catalog when it processes the
   blueprint.

A ``forge_blueprint_version`` field records the schema version. Blueprints
written by older versions of C-Star are migrated automatically when loaded.

Example
-------

The bundled toy domain used in the :doc:`end-to-end tutorial
<../../tutorials/end_to_end>`:

.. literalinclude:: forge-blueprint-example.wio-toy.yaml
   :language: yaml
   :lines: 1-60

:download:`Download the full file <forge-blueprint-example.wio-toy.yaml>`.

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
UCLA-ROMS blueprint). ``cstar forge run`` runs the same stages with Forge's
full option set:

.. code-block:: console

   cstar forge run forge_blueprint.yaml --help

The options you are most likely to want:

``--only-inputs grid,tidal``
   Generate only the named input categories (``grid``,
   ``initial_conditions``, ``surface``, ``boundary``, ``tidal``, ``river``,
   ``cdr``, ``carbonate_sensitivity``) and stop before the build
   configuration. Useful for a slow input you want to check by hand before
   generating the rest.
``--clobber``
   Regenerate inputs that already exist. Without it, existing files are
   reused. To run the emitted UCLA-ROMS blueprint again in the same
   directory, pass ``--clobber`` to ``cstar blueprint run``, as for any
   application.
``--no-data``, ``--no-generate``, ``--no-configure``
   Skip a stage.
``--working-dir PATH``
   Put this run's outputs somewhere other than the blueprint's working
   directory.
``--verbose``
   Timestamped logging and timing and memory instrumentation around the
   ROMS-Tools calls.
``--dask-num-workers N``, ``--no-dask``, ``--dask ...``
   Control how input generation is parallelized. The defaults suit a laptop
   and a single HPC node; ``--dask-num-workers`` caps the thread count on
   nodes with many cores.

What Forge writes
-----------------

Everything goes under the blueprint's working directory: by default
``CSTAR_DATA_HOME/blueprint_runs/forge/<name>`` (see :doc:`../../deployments/hpc`), or the
``working_dir`` you set, used exactly as written.

.. code-block:: text

   <working directory>/
     input_data/              grid, initial conditions, forcing netCDF files
     builds/compile-time/     cppdefs.opt
     builds/run-time/         namelist.nml, marbl_in
     blueprints/
       B_<name>.yaml          the UCLA-ROMS blueprint
       settings_B_<name>.yaml the resolved model settings, for reference
     output/
       B_<name>.yaml          a copy of the UCLA-ROMS blueprint

The UCLA-ROMS blueprint points at the generated files with paths relative to
its own directory (``../input_data/<file>``, ``../builds/run-time``), so the
working directory can be moved or copied as a unit; the copy in ``output/``
sits at the same depth and names the same files. It pins the same model code
the forge blueprint did, carries the processor layout and run window, and
lists the rendered ``cppdefs.opt``, ``namelist.nml`` and ``marbl_in`` as its
compile-time and run-time code. It has no
``working_dir`` of its own, so it runs under
``CSTAR_DATA_HOME/blueprint_runs/ucla_roms/<name>`` unless a workplan places
it. Both ``cstar blueprint run`` and ``cstar forge run`` copy it into
``output/``, where a later step of a workplan looks for it; run that copy with
``cstar blueprint run output/B_<name>.yaml``. ``cstar forge run`` prints the
exact command as its last lines.

Its ``description`` names the forge blueprint it came from and repeats that
blueprint's description. Its :ref:`provenance <blueprint_provenance>` records
the Forge run that generated it, so a copy of the file still says where it came
from:

``generated_at``, ``generated_by``
   When it was generated and by what: ``tool`` (``forge``), an ``id`` unique to
   the run, the ``system``, the ``versions`` of C-Star and roms-tools, the
   ``run_id`` of the workplan run when a workplan step ran Forge (empty
   otherwise) and the ``working_dir``.
``derived_from``
   The forge blueprint, by name and ``content_hash``, followed by each catalog
   spec it was composed from that has a name, as ``ModelSpec``, ``DomainSpec``,
   ``ForcingSpec``, ``CdrSpec`` or ``OutputSpec`` entries with their ``origin``
   and whether they were ``modified``.

Every run records its own provenance, with a new ``id``, even when it reuses the
input files of an earlier one.

The settings sidecar is the complete resolved ``model_settings`` split into
the single compile-time section (``cppdefs``) and the run-time namelist
sections. It is informational; the simulation reads ``namelist.nml``.
