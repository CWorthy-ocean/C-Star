.. _developers-nesting-upscaling-ccs:

Nesting and upscaling on the California Current System
======================================================

This page describes the same workflow as the :doc:`tiny nested tutorial
<../tutorials/nesting_upscaling>` on a realistic case: a 16 km parent and a 4 km
child in the California Current System, with real forcing. It is too large for
a laptop. It runs on an HPC cluster or on a workstation. The tiling of
``full.vars`` (4 by 4, 5 by 10 and 4 by 4) starts up to 50 MPI ranks in one
step on either, so a workstation needs that many cores, or a smaller tiling
with a matching ``num_cpus`` (see "Running a shorter window").
Read the tiny tutorial first: it explains nesting, ``continue-from``,
upscaling and the metrics step, and this page does not repeat it.

The case
--------

- The **parent** is a 168 by 336 cell grid with 20 levels, about 16 km
  resolution, covering the California Current System.
- The **child** is a 70 by 140 cell grid with 20 levels, about 4 km
  resolution, nested in the parent.
- One **alkalinity release** (ALK) is placed in the child, off San Simeon.
- The chain is the one of the tiny tutorial, nine steps in all:

  .. code-block:: text

     forge_parent -> parent -----------.--> child --> upscale --> parent_up
     forge_child  -> nest_ic ----------'      |                     ^   |
     forge_parent_up -------------------------|---------------------'   |
                                              '--> cdr_metrics <--------'

  The arrows show the main flow. The workplan also makes ``nest_ic`` and
  ``parent_up`` wait for ``parent`` (the restart each of them reads), and
  ``child`` wait for ``forge_child`` (its inputs).

The three forge blueprints use the bundled ``roms-marbl-0.11-default`` model
(ucla-roms 0.11.0 with MARBL and ParallelIO) and write daily restarts.

The release values are placeholders, to be confirmed. The release is 1e4 mol/s
of ALK (1e7 meq/s) at 35.52 N, 121.22 W and 2 m depth, constant for 30 days from
the child's start, then zero.

Before you start
----------------

You need:

- A source checkout of C-Star, with the conda environment from
  ``environment-laptop.yml`` or ``environment-hpc.yml`` created, activated, and
  C-Star installed into it (see "Install from source" on the
  :doc:`installation page <../installation>`). The commands below run from the
  root of the checkout.
- roms-tools from its main branch, at or after commit ``f9b07d42``, until a
  roms-tools release carries it. The 5.2.1 release does not. roms-tools `#705
  <https://github.com/CWorthy-ocean/roms-tools/pull/705>`__ fixed the edge
  labels in ``compute_boundary_distance``, so that ``align_grids`` blends the
  child's boundary with the parent's. The fix matters only when a child
  boundary is closed. This child's east boundary is closed
  (``open_boundaries.east: false`` in ``forge_child.yaml``). The tiny
  tutorial's child has four open boundaries, so it does not need the fix.
  Install it into the C-Star environment:

  .. code-block:: console

     pip install git+https://github.com/CWorthy-ocean/roms-tools.git

- The source datasets below, some of which you stage yourself.

Source datasets
^^^^^^^^^^^^^^^

The case uses GLORYS, ERA5, GLODAP, WOA, UNIFIED BGC, TPXO, GloFAS rivers (with
RIVR2O) and MBL CO2, plus SRTM15 bathymetry. They come in three ways:

- **Forge downloads them** into the shared source-data cache: GLORYS, SRTM15,
  UNIFIED BGC, the WOA nutrients and oxygen, and MBL CO2. GLORYS needs a free
  Copernicus Marine login (see :ref:`forge-data-access-glorys`) unless the
  days you need are already in the cache.
- **ERA5 is streamed** by roms-tools when the inputs are generated.
- **You stage them** in the source-data cache: TPXO tides (registration
  required, see :ref:`forge-data-access-tpxo`), the GLODAP climatology, the WOA
  salinity used for surface salinity restoring, and the GloFAS river discharge
  with the RIVR2O river chemistry. Forge stops and lists the paths it
  expected if a staged file is missing.

``cstar forge show-paths`` prints the cache location. See
:doc:`../forge/source_datasets` for the layout each staged source expects and
:doc:`../data_access` for registration.

Run it
------

``full.vars`` holds the run windows for a full run: a 365-day parent
(2010-01-01 to 2011-01-01), and a child and ``parent_up`` that run for the last
184 days (2010-07-01 to 2011-01-01), with the alkalinity released for the first
30 of them.

.. literalinclude:: nesting_upscaling_ccs/full.vars
   :language: text
   :caption: docs/developers/nesting_upscaling_ccs/full.vars

From the root of the repository, with the environment active:

.. code-block:: console

   cstar workplan submit docs/developers/nesting_upscaling_ccs/workplan.yaml --varfile docs/developers/nesting_upscaling_ccs/full.vars

``cstar workplan check`` validates the workplan with the same
``--var``/``--varfile`` options (``check`` has no ``--run-id``). The run ID
defaults to the workplan's name; give each window its own ``--run-id``,
because reusing one attaches to the earlier run. Follow a run with
``cstar run status <run-id>``.

The workplan names no launcher, so C-Star picks one for the machine: SLURM
where there is a scheduler, local processes otherwise. Every step carries a
``local`` and a ``slurm`` entry in ``compute_overrides``. Under SLURM,
``cstar workplan submit`` submits all nine jobs, chained with SLURM dependencies,
and returns. The account and queue come from your environment, as described on
the :doc:`HPC page <../hpc>`.

Running a shorter window
^^^^^^^^^^^^^^^^^^^^^^^^

The same files serve any window, because the dates are runtime variables. A
month is a good first test: 31 days of parent, and a child and ``parent_up``
for the last 16 days with an 8-day release.

``--var`` and ``--varfile`` cannot be combined, so give all twelve variables
one way or the other. The simplest way is to copy the directory, so that you
can also edit the workplan, and edit a copy of ``full.vars``:

.. code-block:: console

   cp -r docs/developers/nesting_upscaling_ccs ~/ccs-month
   cd ~/ccs-month
   cp full.vars month.vars

This block is the whole of ``month.vars``, all twelve variables. Only the six
date variables can differ from ``full.vars`` (``parent_start`` happens to be
the same); the tiling and output variables keep their values:

.. code-block:: text

   parent_start=2010-01-01T00:00:00
   parent_end=2010-02-01T00:00:00
   child_start=2010-01-16T00:00:00
   child_start_stamp=20100116000000
   child_end=2010-02-01T00:00:00
   cdr_end=2010-01-24T00:00:00
   parent_np_x=4
   parent_np_y=4
   child_np_x=5
   child_np_y=10
   cdr_output_period=86400
   cdr_output_nrpf=1

Then run it:

.. code-block:: console

   cstar workplan check workplan.yaml --varfile month.vars
   cstar workplan submit workplan.yaml --varfile month.vars --run-id ccs-month

The variables, with their values for the full run and for the month:

.. list-table::
   :header-rows: 1
   :widths: 24 46 15 15

   * - Variable
     - Meaning
     - ``full.vars``
     - Month
   * - ``parent_start``
     - Start of the parent run and of ``forge_parent``'s forcing
     - 2010-01-01
     - 2010-01-01
   * - ``parent_end``
     - End of the parent run
     - 2011-01-01
     - 2010-02-01
   * - ``child_start``
     - Start of the child and ``parent_up``; the parent has a restart here
     - 2010-07-01
     - 2010-01-16
   * - ``child_start_stamp``
     - ``child_start`` as ``YYYYMMDDHHMMSS``, the parent restart's file name
     - 20100701000000
     - 20100116000000
   * - ``child_end``
     - End of the child and ``parent_up``
     - 2011-01-01
     - 2010-02-01
   * - ``cdr_end``
     - End of the alkalinity release
     - 2010-07-31
     - 2010-01-24

The dates must satisfy ``parent_start`` < ``child_start`` < ``child_end`` <=
``parent_end``. ``child_start`` must be a day boundary, because the parent
writes daily restarts, ``child_start_stamp`` must spell the same instant, and
``cdr_end`` must lie between ``child_start`` and ``child_end``.
``cstar workplan check`` does not test these rules.

The tiling variables (``parent_np_x`` and ``parent_np_y``, ``child_np_x`` and
``child_np_y``) must divide the grid sizes (168 and 336 for the parent, 70 and
140 for the child). They set the number of MPI ranks of each ROMS step. Under
SLURM the ``num_cpus`` of the three ROMS steps in the workplan must equal
those ranks (16, 50 and 16 for ``full.vars``), so change both together.
``cdr_output_period`` and ``cdr_output_nrpf`` set the averaging period (in
seconds) and the number of records per file of the CDR output; their product
must divide the 86400 s restart period. Hourly CDR output on the parent grid
is 5.9 GB per simulated day, so keep daily output (86400 and 1) for long runs.

Lower the SLURM time limits as well. The ``slurm.max_walltime`` values in the
workplan are sized for the full run, at about three times the estimated time.
Shorter limits also let the scheduler start a job sooner, but a job that hits
its limit is killed, and so is everything after it. SLURM lets you lower a
queued job's limit but not raise it. These limits were enough for a month run
on one cluster:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Step
     - Limit for the month
     - Limit in the workplan
   * - ``forge_parent``
     - 2:00:00
     - 6:00:00
   * - ``forge_child``
     - 1:00:00
     - 2:30:00
   * - ``forge_parent_up``
     - 1:30:00
     - 3:00:00
   * - ``parent``
     - 1:30:00
     - 11:00:00
   * - ``nest_ic``, ``upscale``
     - 0:30:00
     - 0:30:00
   * - ``child``
     - 1:00:00
     - 8:30:00
   * - ``parent_up``
     - 1:00:00
     - 8:00:00

The limit of ``cdr_metrics`` (2:00:00) can stay as it is. A workstation run
uses the ``local`` limits, which are generous upper bounds.

Resources
---------

The numbers in this section were measured on 2026-10-15 on one cluster, with
earlier versions than these files use: a ucla-roms commit before 0.11.0 and
roms-tools 5.1.2.dev. Treat them as a guide. A rerun with these files will
refresh them. They also predate the upscaler's time-convention fix:
``parent_up`` now receives the final hour of the child's export too. On the
one-month CCS run of 2026-10-21 that hour carried 1.5 % of the export, because
the plume reached the child's boundary late, so a month's alkalinity is about
1.5 % higher.

CPUs per step, as in the workplan:

.. list-table::
   :header-rows: 1
   :widths: 50 20 30

   * - Step
     - CPUs
     - Why
   * - ``forge_parent``, ``forge_child``, ``forge_parent_up``
     - 32
     - Memory: on many clusters a job's memory scales with its CPUs, and
       forge peaks near 20 GB
   * - ``parent``, ``parent_up``
     - 16
     - 4 by 4 tiles
   * - ``child``
     - 50
     - 5 by 10 tiles
   * - ``nest_ic``, ``upscale``
     - 4
     - Small steps
   * - ``cdr_metrics``
     - 8
     - Reads the output of two runs

- **A month** (the window above) took about 1 hour of compute along the
  critical path (``forge_parent``, ``parent``, ``nest_ic``, ``child``,
  ``upscale``, ``parent_up``) and about 34 CPU-hours (CPUs times hours).
  ``forge_child`` and ``forge_parent_up`` run beside ``parent``. Queue waits
  come on top. The measurement predates the ``cdr_metrics`` step, so it covers
  the other eight steps. Its scratch use was 46 GB.
- **A year** (``full.vars``) is estimated at about 11 hours along the critical
  path and about 355 CPU-hours, and at about 530 GB of scratch. This is an
  extrapolation from the month, not a run.

The metrics step
----------------

``cdr_metrics`` reads the ``_cdr`` output of ``child`` and ``parent_up`` and
writes ``cdr_metrics.nc``, ``cdr_metrics.png`` and ``cdr_domains.png`` to
``tasks/cdr_metrics/output/``. The child holds the release and ``parent_up``
has ``source_role: transfer``, so the combined efficiency is measured against
the child's release only. The outputs and the meaning of the combined uptake,
the efficiency and ``closure_residual`` are the same as in the tiny tutorial:
see :ref:`tutorial-nesting-upscaling-outputs`.

The workplan
------------

The workplan's header comments explain each step, the runtime variables and the
SLURM settings.

.. dropdown:: workplan.yaml

   .. literalinclude:: nesting_upscaling_ccs/workplan.yaml
      :language: yaml
      :caption: docs/developers/nesting_upscaling_ccs/workplan.yaml
