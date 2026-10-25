.. _nest-ic:

Creating initial conditions for a nest
======================================

A nested child grid starts from the state of its parent. The ``nest_ic``
application takes a restart file that the parent run wrote and regrids it
onto the child grid, producing the child's initial conditions file. The
child run then continues from that file, and takes its boundary forcing from
the parent's output through the ``nest-from`` directive, so the two runs
agree at the moment the child starts.

What it does
------------

Given the parent grid, one parent restart file, and the child grid,
``nest_ic`` uses `ROMS-Tools <https://roms-tools.readthedocs.io>`__ to
interpolate every field of the restart onto the child grid at the restart's
time. It reads the date from the restart file's name, carries the parent's
model reference date over so that the child's clock matches, and includes
the MARBL tracers whenever the restart has them. The result is written as
``ic_from_parent_rst.<timestamp>.nc`` in the step's ``output`` directory,
where the timestamp is the restart's, for example
``ic_from_parent_rst.20120102060000.nc``.

By default the file is written for a ROMS build with ParallelIO, which
needs CDF-5 netCDF; set ``pio: false`` for a build without it.

The blueprint
-------------

``parent_grid``
   The parent's grid file.
``parent_rst``
   The parent restart to start the child from. The restart's timestamp
   becomes the child's start date, so pick the restart that falls on the
   time you want the child to begin.
``child_grid``
   The child's grid file.
``pio`` (optional, default ``true``)
   Whether the child's ROMS build reads its inputs through ParallelIO.

Example
-------

In a workplan the step is written inline, with its paths reaching into the
other steps' directories. Here ``forge_parent`` and ``forge_child`` are the
Forge steps that generated the two grids and ``parent`` is the parent run,
which writes a restart every six hours; ``child`` continues from the file
``nest_ic`` writes and takes its boundaries from the parent:

.. code-block:: yaml

   steps:
     - name: nest_ic
       application: nest_ic
       blueprint: inline
       depends_on:
         - parent
         - forge_child
       blueprint_overrides:
         parent_grid: "{{root_dir: forge_parent}}/input_data/parent_grid.nc"
         parent_rst: "{{output_dir: parent}}/output_rst.20120102060000.nc"
         child_grid: "{{root_dir: forge_child}}/input_data/child_grid.nc"

     - name: child
       application: ucla_roms
       blueprint:
         from_step: forge_child
       depends_on:
         - forge_child
         - nest_ic
         - parent
       directives:
         continue-from:
           step: nest_ic
         nest-from:
           step: parent

``continue-from`` finds the initial conditions file in the ``nest_ic`` step's
record, and ``nest-from`` the ``child_bry.*.nc`` files the parent run wrote.
Both directives are described under :doc:`../../workplans/directives`. The
:doc:`nesting and upscaling tutorial <../../tutorials/nesting_upscaling>` runs
this sequence end to end.

The application can also run on its own, from a blueprint file, when the
parent's files are already on disk:

.. code-block:: yaml

   name: child-initial-conditions
   description: Initial conditions for the child grid from the parent's restart at the child's start
   application: nest_ic
   parent_grid: /data/parent/parent_grid.nc
   parent_rst: /data/parent/output/output_rst.20120102060000.nc
   child_grid: /data/child/child_grid.nc

.. code-block:: console

   cstar blueprint run child_ic.yaml
