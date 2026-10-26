.. _tutorial-blueprint:

Using blueprints to set up and run an ocean model
=================================================

This walkthrough takes you from an installed environment to a running toy
simulation on a laptop or workstation, using all three C-Star steps: build a
forge blueprint with the wizard, run Forge to generate the domain, then run the
simulation Forge produced. The domain is **wio-toy**, a deliberately tiny
(20 by 20 cells, 10 levels) Western Indian Ocean grid that processes in minutes.

Before you start
----------------

- :doc:`Install C-Star <../getting_started/installation>` and activate the environment.
- :doc:`Register for Copernicus Marine <../getting_started/data_access>` and run
  ``copernicusmarine login``. GLORYS supplies the initial and boundary
  conditions for this domain.

This tutorial leaves out tides and rivers, so there is nothing else to
register for. The first run downloads the source data it needs into a cache:
a few hundred megabytes of GLORYS ocean state, plus the 10 GB
biogeochemistry climatology, which takes a while on a slow connection. Later
runs of this or any other domain reuse the cache.

1. Build a forge blueprint with the wizard
------------------------------------------

.. code-block:: console

   cstar wizard

This serves the wizard at ``http://localhost:8866`` and opens it in your
browser. You should see the following interface:

.. figure:: ../images/wizard-overview.png
   :alt: The wizard's header with the step navigation, the Start and Model sections
   :width: 100%

Work down the sections:

- **Model**: keep the default model preset.
- **Domain and grid**: choose ``wio-toy (bundled)`` from the domain
  dropdown.
- **Boundaries and forcing**: choose ``simple-bgc-demo (bundled)`` from the
  forcing dropdown. It is the default forcing without tides, rivers or
  surface-salinity restoring, which keeps this first run short and needs no
  data you would have to download by hand.
- **Run setup**: the defaults for this domain are fine.
- **Review and export**: read through the resolved YAML, then **Save** it
  to your catalog or **Download** ``forge_blueprint.yaml``.

:doc:`../catalog_wizard/blueprints` describes each section. If you would rather skip the wizard, the
same blueprint ships with C-Star as ``wio-toy-simple``: pick it in the wizard's
**Start** section to load it, or download
:download:`wio-toy-simple.yaml <../../cstar/catalog/bundled/blueprints/forge/wio-toy-simple.yaml>`.

Blueprints you save from the wizard land in your own catalog layer at
``~/cstar/catalog/blueprints/``, never inside the installed package. Set
``CSTAR_CATALOG`` to put it elsewhere (see :doc:`../catalog_wizard/catalog`).

2. Run Forge
------------

.. code-block:: console

   cstar blueprint run path/to/forge_blueprint.yaml

C-Star reads the ``application: forge`` line and hands the blueprint to
Forge, which fetches the source data (most of the time in a first run goes
here), builds the grid, generates every input file, renders the model
settings, and writes a UCLA-ROMS blueprint. Everything lands under the
blueprint's working directory. A forge blueprint normally omits
``working_dir``, so for the wizard's default name,
``roms-marbl-0.13-default_wio-toy_10procs``, that is
``~/cstar/blueprint_runs/forge/roms-marbl-0-13-default_wio-toy_10procs/`` (the
directory name is the blueprint name, slugified):

.. code-block:: text

   input_data/            grid, initial conditions and forcing netCDF files
   builds/compile-time/   cppdefs.opt
   builds/run-time/       namelist.nml, marbl_in
   blueprints/            B_<name>.yaml and its settings sidecar
   output/                a copy of B_<name>.yaml

The copy in ``output/`` is the blueprint to run next, and the log reports where
it is, in a line like this:

.. code-block:: text

   Forge blueprint published for downstream steps: ~/cstar/blueprint_runs/forge/roms-marbl-0-13-default_wio-toy_10procs/output/B_roms-marbl-0.13-default_wio-toy_10procs.yaml

If you started from the bundled ``wio-toy-simple`` blueprint instead, it keeps
that name, so the paths on the rest of this page use ``wio-toy-simple`` in
both places: the blueprint to run next is
``~/cstar/blueprint_runs/forge/wio-toy-simple/output/B_wio-toy-simple.yaml``.


3. Run the simulation
---------------------

.. code-block:: console

   cstar blueprint run ~/cstar/blueprint_runs/forge/roms-marbl-0-13-default_wio-toy_10procs/output/B_roms-marbl-0.13-default_wio-toy_10procs.yaml

This time, when C-Star opens the blueprint you passed, it sees the ``application: ucla_roms``
line and therefore executes the UCLA-ROMS application instead of Forge.
C-Star clones and compiles the model code pinned in
the blueprint and runs the simulation. The blueprint has no ``working_dir``,
so results land under the C-Star default location:
``~/cstar/blueprint_runs/ucla_roms/roms-marbl-0-13-default_wio-toy_10procs/``.

:doc:`../applications/ucla_roms/blueprint`
explains each section of the UCLA-ROMS blueprint Forge just wrote. The
:doc:`next page <workplan>` runs these same two steps as a workplan, so you
submit once and let C-Star carry the blueprint from one step to the next, without
needing to worry about all of the intermediate paths.

On a cluster
------------

Each step can run on a different machine. A common pattern is to build the
blueprint in a browser on your laptop, copy the YAML to the cluster, and run
steps 2 and 3 there, where the forcing data and the compute are. On an HPC
system, both steps' working directories land under ``CSTAR_DATA_HOME``, which
resolves to scratch. Run step 3 with ``cstar blueprint submit`` instead of
``cstar blueprint run`` so the simulation runs on a compute node, or run both
steps as a workplan (next page). A submitted run keeps its results in the
run's directory, which ``cstar run path <run-id> ucla_roms`` prints; the run ID
is the blueprint's name made into a slug. See :doc:`../deployments/hpc`.
