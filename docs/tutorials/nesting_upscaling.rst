.. _tutorial-nesting-upscaling:

Nesting and upscaling: a tiny nested CDR run
============================================

A carbon dioxide removal (CDR) release, such as an alkalinity addition, is
best resolved on a fine grid, but its effect reaches well beyond that grid. A
nested workflow runs the release in a small **child** domain inside a larger
**parent** run, then **upscales** the child's result: the boundary fluxes of
the CDR tracers that the child writes become a CDR forcing file for the parent
grid, and the parent is run again with that forcing so it sees what the child
exported. One workplan describes the whole chain:

.. code-block:: text

   forge_parent  forge_child  forge_parent_up
        |             |              |
      parent ---> nest_ic ---> child ---> upscale ---> parent_up

- ``forge_parent``, ``forge_child`` and ``forge_parent_up`` are Forge steps
  that generate the inputs of the three ROMS runs.
- ``parent`` runs the coarse grid and writes the boundary data for the child
  (``child_bry.*.nc``) and restart files.
- ``nest_ic`` regrids the parent's restart at the child's start onto the
  child grid: the child's initial conditions.
- ``child`` runs the fine grid with the CDR release. It takes its initial
  state from ``nest_ic`` and its boundary forcing from ``parent``, and writes
  the boundary fluxes of the CDR tracers (``*_uscl.*.nc``).
- ``upscale`` turns those fluxes into ``upscaled_cdr.nc``, a CDR forcing file
  for the parent grid.
- ``parent_up`` reruns the parent from its own restart at the child's start,
  with ``upscaled_cdr.nc`` as its CDR forcing.

Run it
------

The example lives in the repository, in ``examples/nesting_upscaling/``, not in
the installed package. ``examples/nesting_upscaling/tiny/`` is a laptop-sized
case: 24 by 24 cells in each grid, about 8 minutes on a 10-core laptop, and a
download of about 40 MB of pinned test data. The case's README lists the files,
the prerequisites (a source checkout of C-Star with its environment installed
and activated, and GNU ``timeout``), the time each step takes, and what to look
at afterwards.

From the ``tiny`` directory, with the environment active:

.. code-block:: console

   cstar workplan check workplan.yaml --var example_data=$(python fetch_data.py)
   cstar workplan run   workplan.yaml --var example_data=$(python fetch_data.py) --run-id nest-tiny
   cstar run status nest-tiny

``fetch_data.py`` downloads the pinned source files and prints the directory
that holds them, which the workplan takes as the runtime variable
``example_data``. The first run also clones ROMS, MARBL and ParallelIO and
compiles a ROMS executable for each of the three ROMS runs, which are
different builds.

A larger case on the California Current, ``examples/nesting_upscaling/ccs/``,
uses the same eight steps with real forcing: a 168 by 336 parent at about 16 km
with a 70 by 140 child at about 4 km and one alkalinity release near Monterey
Bay, on a workstation or HPC node with up to 64 cores. Its run windows are
runtime variables: ``smoke.vars`` (4 days of parent, 2 of child) has passed
end to end, and ``full.vars`` (a year of parent, 184 days of child, about 12
hours with margin) is proposed and has not been run. See
``examples/nesting_upscaling/ccs/README.md`` for its prerequisites, timings and
known issues.

Workplan features it uses
-------------------------

The example is a worked use of the features described in :doc:`../workplans`
(see also :ref:`workplan_examples`) and :doc:`../workplans/directives`:

- ``continue-from`` takes a step's initial conditions from the restart file
  of an earlier step. ``child`` continues from ``nest_ic``; ``parent_up``
  continues from ``parent`` with a ``timestamp``, so it starts from the
  restart at the child's start rather than the latest one.
- ``nest-from`` supplies a child's open-boundary forcing from the parent's
  ``child_bry.*.nc`` files.
- ``{{output_dir: <step>}}`` placeholders (and ``{{root_dir: <step>}}``) point
  one step at another step's files, such as the restart ``nest_ic`` reads or
  the ``upscaled_cdr.nc`` that ``parent_up`` takes as its CDR forcing.
- ``runtime_vars`` declares ``example_data``, which ``--var`` supplies when the
  workplan is run, so the workplan holds no machine-specific path.
- Deferred blueprints (``blueprint: {from_step: ...}``) run what a Forge step
  generated, and inline blueprints (``blueprint: inline``) give the
  ``nest_ic`` and ``upscaler`` steps all their fields through
  ``blueprint_overrides``.
