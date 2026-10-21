.. _tutorial-nesting-upscaling:

Nesting and upscaling: a tiny nested CDR run
============================================

A carbon dioxide removal (CDR) release, such as an alkalinity addition, is best
resolved on a fine grid, but its effect spreads well beyond that grid. A
*nested* workflow runs the release in a small **child** domain inside a larger
**parent** run. It then **upscales** the child's result, so that the parent
sees the alkalinity that left the child, and it computes the CO2 uptake and the
CDR efficiency of the whole release.

This tutorial runs a tiny version of that workflow on a laptop, from one
workplan, in about 10 minutes. The parent is a 24 by 24 cell grid over 400 km
north-west of Scotland. The child is a 24 by 24 cell grid over 100 km inside it, with
one alkalinity (ALK) release near its east boundary. A
:doc:`second page <nesting_upscaling_ccs>` shows the same workflow on a
realistic case, the California Current.

How the workflow fits together
------------------------------

One workplan describes the whole chain:

.. code-block:: text

   forge_parent  forge_child                            forge_parent_up
        |             |                                        |
      parent  -->  nest_ic  -->  child  -->  upscale  -->  parent_up  -->  cdr_metrics

``forge_parent``, ``forge_child`` and ``forge_parent_up`` are Forge steps that
generate the inputs of the three ROMS runs. ``parent`` runs the coarse grid.
``nest_ic`` and ``child`` set up and run the fine grid with the release.
``upscale`` and ``parent_up`` pass the result back to the coarse grid, and
``cdr_metrics`` analyses both domains. The arrows show the main flow; the
workplan also makes ``child`` wait for ``parent`` (its boundary data) and
``parent_up`` for ``parent`` (its restart), and ``cdr_metrics`` reads ``child``
as well as ``parent_up``. Four ideas make this work.

**Nesting.** A child domain takes its open-boundary forcing from the parent
instead of from a global dataset. The parent run writes the child's boundary
data (``child_bry.*.nc``) as it runs, because its forge blueprint asks for it.
The ``nest-from`` directive on the child's step names the parent step, and
C-Star hands those files to the child. See :doc:`../workplans/directives`.

**Continuing from a restart.** The ``continue-from`` directive takes a step's
initial conditions from a restart file of an earlier step. The child continues
from ``nest_ic``, a small step that regrids the parent's restart at the child's
start time onto the child grid. ``parent_up`` continues from ``parent``
itself, with a ``timestamp`` that picks the restart at the child's start
rather than the latest one.

**Upscaling.** The child writes the boundary fluxes of the CDR tracers
(alkalinity and DIC) through its open boundary, in ``*_uscl.*.nc`` files. The
``upscaler`` application turns them into ``upscaled_cdr.nc``, a CDR forcing
file for the parent grid. ``parent_up`` is the parent grid run again from its
own restart at the child's start, with that file as its CDR forcing, so the
parent receives what the child exported. Forge builds ``parent_up`` from a
blueprint with the CDR mode ``upscaled``, which compiles ROMS to read its CDR
forcing from a file.

**CDR metrics.** The ``cdr_metrics`` application computes the CO2 uptake and
the CDR efficiency from the ``_cdr`` output of the child and of ``parent_up``.
It analyses each domain on its own grid and combines them. The
:doc:`workplans page <../workplans>` describes the application in its
sections "Computing CDR metrics from segmented runs" and "Nested (upscaled)
experiments".

Before you start
----------------

- A source checkout of C-Star, with the conda environment from
  ``environment-laptop.yml`` created, activated, and C-Star installed into it
  (see "Install from source" on the :doc:`installation page
  <../installation>`). The example files and ``fetch_data.py`` live in the
  repository and are not part of the installed package. ROMS builds find
  NetCDF and MPI through the activated environment, so activate it in every
  shell where you use the ``cstar`` commands, including ``cstar run status``.
  The environment file also installs GNU coreutils, which C-Star needs to
  enforce the local time limits on macOS; the ``cstar`` commands that read the
  workplan, ``cstar run status`` among them, look for it too.
- Network access on every run. For each ROMS step C-Star refreshes its cached
  clones of ROMS, MARBL and ParallelIO (it clones them the first time) before
  it compiles ROMS. Only the test data and the auxiliary files that roms-tools
  fetches itself, such as the ETOPO5 topography, are downloaded just once.

Run it
------

From the root of the repository, with the environment active:

.. code-block:: console

   cstar workplan run docs/tutorials/nesting_upscaling_tiny/workplan.yaml --var example_data=$(python docs/tutorials/nesting_upscaling_tiny/fetch_data.py)

``fetch_data.py`` downloads about 44 MB of pinned test data the first time and
prints the folder that holds it. The workplan takes that folder as its runtime
variable ``example_data``. The script writes its progress to stderr and only
the folder to stdout, so it is safe inside ``$(...)``.

The example uses these files because they need no account and are small. The
GLORYS file (the ocean state) and the ERA5 file (the atmosphere) are small
regional extracts. The UNIFIED biogeochemical climatology is global, coarsened
to 2 degrees, and is most of the 44 MB. The files come from C-Star's
integration-test data registry, which pins their version and checksums.
Forge's own ocean source, GLORYS, needs a free Copernicus Marine login (see
:ref:`forge-data-access-glorys`), and a real domain would use it. The three
forge blueprints in this example carry placeholder source paths, and each
forge step swaps in the real files through ``blueprint_overrides``.

``cstar workplan run`` returns once it has scheduled the steps. They run
detached, each one waiting for the steps it depends on, so follow the run with
``cstar run status nest-tiny``. The run ID defaults to the workplan's name,
``nest-tiny``. Each step's files are under
``$CSTAR_DATA_HOME/workplan_runs/nest-tiny/tasks/<step>/``, which is
``~/cstar/workplan_runs/nest-tiny/`` by default, and ``cstar run path nest-tiny
<step>`` prints a step's directory. To check the workplan without running it,
use ``cstar workplan check`` with the same ``--var``/``--varfile`` options
(``check`` has no ``--run-id``). Running the ``run`` command again attaches to
the existing run; give a new ``--run-id`` to start over.

What each step does
-------------------

The ROMS steps use ucla-roms 0.11.0 with MARBL and ParallelIO, and each runs
on 4 MPI ranks (a 2 by 2 tiling). The times are from a 10-core laptop with the
source data already downloaded; the whole workplan takes about 10 minutes. The
time of a ROMS step includes building its own ROMS executable: the three ROMS
steps are three different builds.

.. list-table::
   :header-rows: 1
   :widths: 16 14 58 12

   * - Step
     - Application
     - What it does
     - Wall time
   * - ``forge_parent``
     - ``forge``
     - Builds the parent grid and its initial, surface and boundary forcing
       (2012-01-01 12:00 to 01-03 07:00), and extracts hourly boundary data
       for the child
     - 9 s
   * - ``forge_child``
     - ``forge``
     - Builds the child grid, its surface forcing and the ALK release (a
       nested child has no initial or boundary forcing of its own)
     - 6 s
   * - ``forge_parent_up``
     - ``forge``
     - Builds the parent grid again, with the CDR mode ``upscaled``
     - 10 s
   * - ``parent``
     - ``roms_marbl``
     - Runs the parent, writes the child's boundary data and a restart every
       6 hours
     - 3:01
   * - ``nest_ic``
     - ``nest_ic``
     - Regrids the parent's restart at 2012-01-02 06:00 onto the child grid
     - 6 s
   * - ``child``
     - ``roms_marbl``
     - Runs 2012-01-02 06:00 to 01-03 06:00 with ``continue-from: nest_ic`` and
       ``nest-from: parent``; releases ALK and writes the ``_uscl`` and
       ``_cdr`` output
     - 3:49
   * - ``upscale``
     - ``upscaler``
     - Turns the child's ``_uscl`` files into ``upscaled_cdr.nc``
     - 1 s
   * - ``parent_up``
     - ``roms_marbl``
     - Reruns the parent grid from the parent's restart at the child's start,
       with ``upscaled_cdr.nc`` as its CDR forcing, to the child's end
     - 2:02
   * - ``cdr_metrics``
     - ``cdr_metrics``
     - Computes the CO2 uptake and CDR efficiency of the release from the
       child's and ``parent_up``'s ``_cdr`` output
     - 3 s

The release is alkalinity at 2e6 meq/s for four hours, from the child's
start. The workplan's three forge steps share nothing, but
``forge_child`` and ``forge_parent_up`` wait for ``forge_parent`` so that
first-time downloads happen once; see the comments in the workplan below.

.. _tutorial-nesting-upscaling-outputs:

What to look at afterwards
--------------------------

``cstar run status nest-tiny`` should show nine steps done. Under
``tasks/<step>/output/``:

- ``parent``: ``child_bry.*.nc``, the boundary data for the child, and
  ``output_rst.*.nc``, the 6-hourly restarts.
- ``nest_ic``: ``ic_from_parent_rst.20120102060000.nc``, the child's initial
  conditions.
- ``child``: ``*_uscl.*.nc``, the boundary fluxes to upscale, and
  ``*_cdr.*.nc``, whose ``ALK_source`` is the release.
- ``upscale``: ``upscaled_cdr.nc``, the CDR forcing for the parent grid.
- ``parent_up``: ``*_cdr.*.nc``, in which ``ALK_source`` is the upscaled
  alkalinity entering along the child's boundary.
- ``cdr_metrics``: ``cdr_metrics.nc``, ``cdr_metrics.png`` and
  ``cdr_domains.png``.

The last step is the result. ``tasks/cdr_metrics/output/cdr_metrics.nc`` holds
time series of the CO2 uptake in tonnes and of the CDR efficiency, each from
the CO2 flux and from the DIC difference. ``cdr_metrics.png`` plots the
combined efficiency and uptake, and ``cdr_domains.png`` maps the two domains.

The two domains are combined in a specific way:

- **Uptake** is summed over the child and ``parent_up``. Each domain removes
  the part of the perturbation that is inside it.
- **Efficiency** is the combined uptake divided by the alkalinity released in
  the child only. ``parent_up`` receives the same alkalinity through its
  boundary, so its source is marked ``source_role: transfer`` and is not counted
  a second time.
- **Closure residual** (``closure_residual``) is the combined uptake from the
  flux minus the combined uptake from the DIC difference. The two are
  independent views of the same perturbation, so a residual that is small next
  to the uptake is the sign that the domains were combined consistently. The
  two methods can disagree for a single domain: DIC that the child took up and
  then exported shows up as a DIC difference in ``parent_up``. The two agree
  to within a few percent on the sum.

``cdr_metrics.nc`` also holds each domain's own uptake, such as
``child_carbon_uptake_from_flux`` and ``parent_up_carbon_uptake_from_flux``.

At the end of the run (2012-01-03 06:00), a reference run with ucla-roms 0.11.0
gave these values (rounded):

.. list-table::
   :header-rows: 1
   :widths: 40 20 20 20

   * - Quantity
     - Child
     - ``parent_up``
     - Combined
   * - CO2 uptake from the flux (tonnes)
     - 7.00
     - 0.75
     - 7.75
   * - CO2 uptake from the DIC difference (tonnes)
     - 5.31
     - 2.62
     - 7.92
   * - CDR efficiency from the flux
     -
     -
     - 0.0061
   * - ``closure_residual`` (tonnes)
     -
     -
     - -0.17 (2.2 % of the flux uptake)

Your numbers should be close to these but need not match them exactly: they
depend on the ucla-roms and roms-tools versions and on the compiler.

The workplan
------------

This is the workplan you ran, as it is in the repository. The header comments
explain each step.

.. literalinclude:: nesting_upscaling_tiny/workplan.yaml
   :language: yaml
   :caption: docs/tutorials/nesting_upscaling_tiny/workplan.yaml

Where to go next
----------------

The weekly extended integration test runs this workplan, so it is kept working
as C-Star changes. For the same chain on a 16 km parent and a 4 km child with
real forcing, see :doc:`nesting_upscaling_ccs`.
