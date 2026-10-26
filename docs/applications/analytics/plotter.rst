.. _plotter:

Plotter
=======

The ``plotter`` application makes a set of 2D plots from a run's output
files. It is a demonstration, not an analysis tool: it exists to show what a
small application that reads a run's output and writes figures looks like,
and to give workplan examples a step to put after a simulation. It will be
replaced by a more complete plotting suite in the near future; for real
analysis, open the output with `ROMS-Tools
<https://roms-tools.readthedocs.io/en/latest/plotting_roms_output.html>`__
directly.

What it does
------------

For every combination of the variables, time indices and depth levels in its
blueprint, the plotter opens the matching output files with ROMS-Tools and
saves one PNG, named ``<variable>_s<level>_t<time>.png``, in its working
directory. A combination that cannot be plotted is logged and skipped. The
figures are listed as products in the step's record.

The blueprint
-------------

``input_dir``
   The directory holding the output files (a UCLA-ROMS step's ``output``
   directory).
``file_glob``
   Which files in it to open, ``*rst*.nc`` by default.
``grid_file_path``
   The grid file matching the output.
``variables``
   The variables to plot.
``time_indices``, ``s_indices``
   The time records and vertical levels to plot, as indices; ``-1`` is the
   last.

Example
-------

A workplan that runs the bundled ``wales-toy`` blueprint and then plots two
variables from its output. The plotter step's paths reach into the
simulation step's directories with placeholders, and the time index comes
from a runtime variable, so the same workplan can plot a different record
each time it is submitted:

.. literalinclude:: ../../tutorials/workplan_laptop_example_w_plotter.yaml
   :language: yaml

.. literalinclude:: ../../tutorials/wales_toy_plotter_blueprint.yaml
   :language: yaml
   :caption: wales_toy_plotter_blueprint.yaml

.. code-block:: console

   cstar workplan submit workplan.yaml --var time_slice=-1

The application's source, ``cstar/applications/plotter.py``, is short and is
a reasonable template for an analysis step of your own; see
:doc:`../../developers/custom_applications`.
