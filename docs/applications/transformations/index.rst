.. _transformations-index:

Data transformations
====================

Two small applications sit between model runs in a nested experiment. Each
takes files that one UCLA-ROMS run wrote and turns them into an input for
another, so that a workplan can carry a simulation from a coarse grid to a
fine one and back without any hand-editing in between.

.. toctree::
   :maxdepth: 1

   Creating initial conditions for a nest <nest_ic>
   Upscaling CDR signals <upscaler>

Both have blueprints of only a few fields, all of them paths into other
steps' directories, so in a workplan they are written inline: the step says
``blueprint: inline`` and gives the fields under ``blueprint_overrides``,
using placeholders such as ``{{output_dir: parent}}`` to reach the other
steps' files. See :doc:`../../workplans/index` for inline blueprints and
:doc:`../../workplans/references` for the placeholders.

The :doc:`nesting and upscaling tutorial <../../tutorials/nesting_upscaling>`
runs both applications in one workplan on a laptop-sized pair of grids.
