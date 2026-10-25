.. _ucla-roms-example:

Example: a toy domain from a ready-made blueprint
=================================================

This page runs a UCLA-ROMS blueprint whose inputs already exist, so you can
see how the sections of the file map to what C-Star does, without generating
a domain first. The domain is ``wio_toy``, a coarse Western Indian Ocean grid
(20 by 20 cells of 400 km, 10 vertical levels) with MARBL biogeochemistry,
tides and rivers. The inputs were made with ROMS-Tools from ERA5, GLORYS, the
unified biogeochemical climatology, TPXO and the Dai and Trenberth river
dataset, and live in a public `examples repository
<https://github.com/CWorthy-ocean/cstar_blueprint_roms_marbl_example>`__.
The run is 30 simulated minutes, so it finishes in a few minutes on a laptop.

To make a domain and blueprint of your own, use :doc:`Forge <../forge/index>`;
the :doc:`end-to-end tutorial <../../tutorials/end_to_end>` walks through it.

Before you start
----------------

- :doc:`Install C-Star <../../installation>` and activate the environment.
- Download :download:`wio_toy_blueprint.yaml <../../tutorials/wio_toy_blueprint.yaml>`.

Nothing needs to be registered for: every input is fetched from the examples
repository.

The blueprint
-------------

.. literalinclude:: ../../tutorials/wio_toy_blueprint.yaml
   :language: yaml

Reading it top to bottom:

``code``
   ``roms`` and ``marbl`` pin the model code to a commit and a tag. The
   ``run_time`` and ``compile_time`` sections point at the examples
   repository and list the files to take from it. ``namelist.nml`` under
   ``run_time`` is the namelist template: C-Star keeps its scientific
   settings and fills in the file paths, output location and run window from
   the blueprint. ``cppdefs.opt`` under ``compile_time`` is compiled into the
   model, and is where tides and rivers are switched on.
``grid``, ``initial_conditions``, ``forcing``
   Each input file by URL. None is partitioned, so C-Star partitions them
   according to ``partitioning`` before the run; see :doc:`blueprint` for
   using files you have already partitioned.
``partitioning``
   2 by 5 tiles, so the model runs on 10 MPI ranks.
``namelist_overrides``
   The time step, 900 seconds, set in the ``time_stepping`` group of the
   namelist.
``runtime_params``
   The run window, which must lie within ``valid_start_date`` and
   ``valid_end_date``.

Check it
--------

.. code-block:: console

   cstar blueprint check wio_toy_blueprint.yaml

prints ``The blueprint is valid``. The check reads the file and validates
its structure; it does not fetch anything.

Run it
------

.. code-block:: console

   cstar blueprint run wio_toy_blueprint.yaml

The blueprint sets ``working_dir`` to ``~/cstar/wio_toy_blueprint``, so
everything goes there. The log shows the stages
described in :doc:`index` going by: the codebases are set up, the
compile-time and run-time code and the input datasets are fetched, each
input is partitioned into 2 by 5 tiles, and the model is launched:

.. code-block:: text

   Executing blueprint in a worker service
   [INFO] - 🛠️ Configuring ROMSSimulation
   [INFO] - 🔧 Setting up ROMSExternalCodeBase...
   [INFO] - 🔧 Setting up MARBLExternalCodeBase...
   [INFO] - 📦 Fetching compile-time code...
   [INFO] - 📦 Fetching runtime code...
   [INFO] - 📦 Fetching input datasets...
   [INFO] - Partitioning .../input/input_datasets/cson_roms-marbl_v0.1_wio-toy_10procs_grid.nc into (2,5)
   ...
   [INFO] - Running mpirun -n 10 .../input/compile_time_code/roms .../work/cstar_generated_roms.nml

The first run compiles ucla-roms and MARBL, which takes a few minutes; the
model itself runs in well under a minute. To run the blueprint again in the
same directory, add ``--clobber``.

Look at the results
-------------------

Under the working directory:

``input/``
   What the run was built from: ``codebases/`` (the cloned model source),
   ``compile_time_code/`` (``cppdefs.opt`` and the compiled ``roms``
   executable), ``runtime_code/`` (``marbl_in`` and the namelist template)
   and ``input_datasets/`` (the partitioned inputs).
``work/``
   The namelist C-Star generated from the template, ``cstar_generated_roms.nml``,
   which is what the model actually read.
``logs/``
   The model's own log. An error from ROMS or MARBL shows up here.
``output/``
   The joined output files, one per stream and time: ``output_his.*.nc``,
   ``output_rst.*.nc``, ``output_bgc.*.nc`` and so on.
``cstar-step.yaml``
   The :term:`step record`: when the run started and finished, on what, and
   the output files it left.

The output files open with `ROMS-Tools
<https://roms-tools.readthedocs.io/en/latest/plotting_roms_output.html>`__,
``ncview`` or any netCDF tool. A first look at the sea surface temperature:

.. code-block:: python

   from roms_tools import Grid, ROMSOutput

   grid = Grid.from_file(
       "~/cstar/wio_toy_blueprint/input/input_datasets/cson_roms-marbl_v0.1_wio-toy_10procs_grid.nc"
   )
   roms_output = ROMSOutput(
       grid=grid,
       path=["~/cstar/wio_toy_blueprint/output/output_his.20120101000000.nc"],
   )
   roms_output.plot("temp", time=0, s=-1)

Where to go next
----------------

A second ready-made blueprint, :download:`wales_toy_blueprint.yaml
<../../tutorials/wales_toy_blueprint.yaml>`, covers a small domain off Wales
and runs with ParallelIO. The same blueprint ships in C-Star's bundled catalog
as the UCLA-ROMS blueprint ``wales-toy``, so the wizard's Workplan page can
start a workplan from it without a local copy.

To run several simulations as one unit, or to chain a spin-up and an
experiment, put the blueprints in a :doc:`workplan <../../workplans/index>`;
the :doc:`workplan tutorial <../../tutorials/workplan>` does that with the
domain from the end-to-end tutorial.
