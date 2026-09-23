.. _tutorial-end-to-end:

End to end: a new domain to a running simulation
================================================

This walkthrough takes you from an installed environment to a running toy
simulation on a laptop or workstation, using all three C-Star steps: build a
forge blueprint with the wizard, run Forge to generate the domain, then run the
simulation Forge produced. The domain is **wio-toy**, a deliberately tiny
(20 by 20 cells, 10 levels) Western Indian Ocean grid that processes in minutes.

Before you start
----------------

- :doc:`Install C-Star <../installation>` and activate the environment.
- :doc:`Register for Copernicus Marine <../data_access>` and run
  ``copernicusmarine login``. GLORYS supplies the initial and boundary
  conditions for this domain. TPXO tides are not needed here.

The first run downloads a few hundred megabytes of source data. Later runs of
this or any other domain reuse it.

1. Build a forge blueprint with the wizard
------------------------------------------

.. code-block:: console

   cstar forge wizard

This serves the wizard at ``http://localhost:8866`` and opens it in your
browser. Work down the cards:

- **Model**: keep the default model preset.
- **Domain and grid**: choose ``wio-toy (bundled)`` from the domain
  dropdown.
- **Boundaries and forcing** and **Run setup**: the defaults for this
  domain are fine.
- **Review and export**: read through the resolved YAML, then **Save** it
  to your catalog or **Download** ``forge_blueprint.yaml``.

:doc:`../wizard` describes each card. If you would rather skip the wizard, a
ready-made blueprint for this domain ships with the documentation:
:download:`forge-blueprint-example.wio-toy.yaml <../forge-blueprint-example.wio-toy.yaml>`.

Blueprints you save from the wizard land in your own catalog layer at
``~/cstar/catalog/blueprints/``, never inside the installed package. Set
``CSTAR_CATALOG`` to put it elsewhere (see :doc:`../catalog`).

2. Run Forge
------------

.. code-block:: console

   cstar blueprint run path/to/forge_blueprint.yaml

C-Star reads the ``application: forge`` line and hands the blueprint to
Forge, which fetches the source data (most of the time in a first run goes
here), builds the grid, generates every input file, renders the model
settings, and writes a ROMS-MARBL blueprint. Everything lands under the
blueprint's ``working_dir``, for wio-toy
``~/cstar/_forge_bp_runs/cson_roms-marbl_v0.1_wio-toy_10procs/``:

.. code-block:: text

   input_data/            grid, initial conditions and forcing netCDF files
   builds/compile-time/   cppdefs.opt
   builds/run-time/       namelist.nml, marbl_in
   blueprints/            B_<name>.yaml and its settings sidecar

The last lines of output tell you what to do next:

.. code-block:: text

   Blueprint: ~/cstar/_forge_bp_runs/.../blueprints/B_cson_roms-marbl_v0.1_wio-toy_10procs.yaml
   Run it with:  cstar blueprint run <path>

``cstar forge run <forge_blueprint.yaml>`` runs the same thing with Forge's
full option set: regenerate only some inputs, overwrite existing files, tune
dask, or change the working directory. See :doc:`../blueprints/forge`.

3. Run the simulation
---------------------

.. code-block:: console

   cstar blueprint run ~/cstar/_forge_bp_runs/.../blueprints/B_cson_roms-marbl_v0.1_wio-toy_10procs.yaml

This time the ``application: roms_marbl`` line routes the blueprint to the
ROMS-MARBL application. C-Star clones and compiles the model code pinned in
the blueprint, partitions the inputs, runs the simulation, and joins the
output. Results land under that blueprint's own working directory.

Both steps use the same command; the blueprint's ``application`` field
decides which application handles it. :doc:`tutorial_bp` walks through what
is in the ROMS-MARBL blueprint Forge just wrote, and :doc:`tutorial_wp` shows
how to chain simulations into a workplan.

On a cluster
------------

Each step can run on a different machine. A common pattern is to build the
blueprint in a browser on your laptop, copy the YAML to the cluster, and run
steps 2 and 3 there, where the forcing data and the compute are. On an HPC
system, Forge places default-form working directories on scratch, and the
ROMS-MARBL step should be submitted through a workplan so it runs on compute
nodes rather than the login node. See :doc:`../hpc`.
