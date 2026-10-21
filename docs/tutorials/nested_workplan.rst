.. _tutorial-nested-workplan:

Spin-up, continuation and a nested grid
=======================================

This page puts the pieces of a typical regional modeling project into one
workplan: generate the inputs for two grids, spin the outer grid up, continue
it for the period of interest, and run a finer grid nested inside it over that
period. Six steps, one command. Along the way it introduces the two ways steps
influence each other beyond ordering: a step can **override** values in its
blueprint, and it can take files written by an earlier step through a
**directive**.

The grids are the ``wio-toy`` domain from the previous pages (20 by 20 cells,
400 km each, 8000 km across) and a child grid with the same number of cells
covering the central 2000 km of it, so each child cell is 100 km. The whole
workplan takes around a quarter of an hour on a recent laptop.

Before you start
----------------

Download the three files into an empty directory:

- :download:`workplan_nested.yaml <workplan_nested.yaml>`
- :download:`forge_blueprint_wio_outer.yaml <forge_blueprint_wio_outer.yaml>`
- :download:`forge_blueprint_wio_inner.yaml <forge_blueprint_wio_inner.yaml>`

The forge blueprints are ready-made. They use the same GLORYS, ERA5 and
biogeochemistry sources as the previous pages, with tides and rivers turned
off so there is nothing further to register for or download. If you have run
the previous pages, the GLORYS data for the outer grid is already cached; the
inner grid fetches its own, smaller cut.

The two grids
-------------

Nesting is set up in Forge, before anything runs. Open the two forge
blueprints and look at the ``domain`` section of each.

The outer blueprint describes the ``wio-toy`` grid and, under
``grid_kwargs_child``, the grid of the child it will feed. Forge needs this to
work out which outer cells sit on the child's edges; it writes that mapping to
a ``nesting.nc`` file, and switches on the model's ``extract_data`` output
stream so the outer run writes hourly boundary values for the child at those
cells. Everything else is an ordinary domain.

The inner blueprint describes the child grid and, under ``grid_kwargs_parent``,
the outer grid it sits in. Forge uses the parent to blend the child's land
mask and bathymetry toward the parent's along the edges, so the two agree
where they meet. Its ``forcing`` section has no ``boundary`` entry at all: the
child's boundary values will come from the outer run, not from a reanalysis.
It still has initial conditions from GLORYS, because every ROMS-MARBL blueprint
needs some, but the workplan replaces them before the run.

In the wizard, the same two blueprints come from the nesting part of the
*Domain and grid* section: tick *This grid is a parent* on the outer domain
and enter the child grid, or *This is a child grid* on the inner one and enter
the parent. The run windows differ on purpose: the outer blueprint covers
2012-01-01 to 2012-01-03, the inner one only the second day.

The workplan
------------

.. literalinclude:: workplan_nested.yaml
   :language: yaml

The first two steps are Forge runs with nothing to wait for, so they run at
the same time. Each of the four steps after them adds one idea.

**spinup** runs day one of the outer grid, and shows **blueprint overrides**.
Its blueprint is the one ``make_outer`` writes, but ``blueprint_overrides``
changes two values in it before the model runs: the end date is pulled back
by a day, and the history output is switched off, since nobody is going to
look at a spin-up. The override keys follow the ROMS-MARBL blueprint:
``runtime_params`` is the run window, and ``namelist_overrides`` is keyed by
namelist group and setting (see :doc:`../blueprints/roms_marbl`). The
blueprint itself is not edited; the overrides are applied to a copy when the
step starts, and that copy is kept in the step's ``work`` directory. The
spin-up leaves behind a restart file at the end of 2012-01-01 and, because
the extraction is on, a day of hourly boundary values for the child.

**outer** runs day two of the outer grid, and shows the **continue-from**
directive. A directive is an instruction that runs just before the model
starts, using something that only exists by then. ``continue-from: step:
spinup`` looks in the spin-up's output for its latest restart file, makes it
this step's initial conditions, and sets the start date to the restart's time.
The end date stays as the blueprint has it, so this run covers 2012-01-02 to
2012-01-03, this time with the history output on. The step lists both
``make_outer`` (it reads that step's blueprint) and ``spinup`` (it reads that
step's restart) under ``depends_on``; a directive that names a step you did
not depend on is refused when you submit.

**inner_ic** is the one step here that is not ROMS or Forge. The inner grid
cannot start from the GLORYS initial conditions Forge made for it, because they
would not match the outer run it is nested in. The small ``nest_ic``
application interpolates a parent restart file onto the child grid. Its
blueprint is three paths, so instead of a file the step says ``blueprint:
inline`` and gives the paths as overrides. Each path uses a placeholder for
another step's directory: ``{{root_dir: make_outer}}`` is the outer Forge
step's directory (its ``input_data/`` holds the grid it wrote),
``{{output_dir: spinup}}`` is the spin-up's output, where ROMS put the restart
file for the end of day one. Placeholders are filled in when the workplan is
submitted, which is why the step depends on all three steps it points into.

**inner** runs the child grid over day two, with two directives. ``continue-from:
step: inner_ic`` picks up the initial-conditions file ``inner_ic`` wrote, since
it is named like a restart. ``nest-from: step: spinup;outer`` collects the
hourly boundary files both outer-grid runs extracted (``child_bry.<time>.nc``
in their outputs) and makes them this step's boundary forcing. Two sources,
separated by ``;``, because ROMS needs boundary values on both sides of every
time it steps through, including the first one: the spin-up's last record is
at 2012-01-02 00:00, exactly where the inner run starts, and the outer run's
first is an hour later. That is the whole hand-off: the outer grid never knows
the inner one exists beyond the extraction it was asked for, and the inner run
is an ordinary ROMS-MARBL run with unusual sources for two of its inputs.

Read the ``depends_on`` lists again with this in mind. Every entry is a file
the step reads:

- ``spinup`` reads the blueprint ``make_outer`` wrote;
- ``outer`` reads that blueprint and the spin-up's restart;
- ``inner_ic`` reads the outer grid from ``make_outer``, the inner grid from
  ``make_inner`` and the spin-up's restart;
- ``inner`` reads the blueprint ``make_inner`` wrote, the initial conditions
  from ``inner_ic``, and boundary values from ``spinup`` and ``outer``.

The two Forge steps overlap, and so would ``outer`` and ``inner_ic`` if the
machine had the cores, since neither needs the other.

Run it
------

.. code-block:: console

   cstar workplan check workplan_nested.yaml
   cstar workplan run workplan_nested.yaml

Then follow it as on the :doc:`previous page <workplan>`:

.. code-block:: console

   cstar run status wio-nested
   cstar run log wio-nested outer --monitor

Each ROMS step fetches and compiles the model in its own directory, so
expect a minute or two of compiling before each one starts stepping, and
about as long again for the simulation itself. If a step fails, the steps
after it are not run, and ``cstar run log wio-nested <step>`` has the reason.
Fix the cause and submit the workplan again; finished steps are kept and the
failed one is retried.

What to look at
---------------

When ``cstar run status`` shows every step as *Done*:

- ``spinup``'s output has one restart file and one ``child_bry.*.nc``
  extraction file, and no history files; ``outer``'s has all three kinds.
- ``inner_ic``'s output has one file, the inner grid's initial conditions at
  2012-01-02 00:00.
- ``inner``'s ``work`` directory holds a copy of its blueprint after each
  directive was applied; the one with the longest name is what actually
  ran. Its ``initial_conditions`` points into ``inner_ic`` and its
  ``forcing.boundary`` lists the outer run's extraction files. Compare it
  with the blueprint in ``make_inner``'s output to see exactly what the
  directives changed.
- ``cstar run show wio-nested`` lists all six steps with their timings, and
  ``cstar run gather wio-nested`` links every step's output files into one
  ``gathered_output`` directory under the run, if you prefer to browse them
  in one place.

Where to go from here
---------------------

This is the skeleton of most projects built with C-Star: a spin-up, a
production segment, and grids nested one inside the other, all described in
one file that someone else can re-run. The same tools stretch further than
this page shows. A ``continue-from`` can name a step of an *earlier* run
instead of one in the same workplan, so a long spin-up is done once and reused
(:ref:`workplan_external_runs`). Long runs are usually split into chunks that
each continue from the previous one, and the wizard's Workplan page has a
generator for that. Carbon dioxide removal experiments add a forcing file and
a control run alongside the experiment, and nested CDR runs can pass their
effect back up to the parent. :doc:`../workplans` and
:doc:`../workplans/directives` describe every option.
