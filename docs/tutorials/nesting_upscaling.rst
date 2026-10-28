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
workplan, in about 20 minutes. The parent is a 24 by 24 cell grid over 400 km
north-west of Scotland. The child is a 24 by 24 cell grid over 100 km inside it, with
one alkalinity (ALK) release near its east boundary, in its southern half. The
current there carries the alkalinity out of the child within the day, so the
run shows the release being handed from the child to the parent. If workplans,
``continue-from`` or the CDR metrics are new to you, :doc:`cdr_workplan`
introduces them on a simpler workplan without nesting.

.. figure:: ../images/nest-tiny-domains.png
   :alt: Map of the seas north-west of Scotland, with the 400 km parent grid outlined in orange and the 100 km child grid outlined in blue inside it
   :width: 60%

   The parent grid (orange) and the child grid (blue). The workplan's last step
   draws this map, ``cdr_domains.png``, labelling the parent by its
   ``parent_up`` run.

Before you start
----------------

- :doc:`Install C-Star <../getting_started/installation>` and activate the environment.
- :doc:`Register for Copernicus Marine <../getting_started/data_access>` and run
  ``copernicusmarine login``. GLORYS supplies the initial and boundary
  conditions of the parent grid.
- Download the four files into an empty directory:

  - :download:`workplan.yaml <nesting_upscaling_tiny/workplan.yaml>`
  - :download:`forge_parent.yaml <nesting_upscaling_tiny/forge_parent.yaml>`
  - :download:`forge_child.yaml <nesting_upscaling_tiny/forge_child.yaml>`
  - :download:`forge_parent_up.yaml <nesting_upscaling_tiny/forge_parent_up.yaml>`

The forge blueprints use the same sources as the :doc:`earlier tutorials
<blueprint_tutorial>`: GLORYS for the ocean state, ERA5 for the atmosphere, and the
UNIFIED biogeochemical climatology. Forge downloads GLORYS and UNIFIED into
its source-data cache, and ERA5 is read over the network as the inputs are
generated. If you have run the earlier tutorials, UNIFIED is already in the
cache; otherwise the first run downloads it, about 10 GB, and the first forge
step takes correspondingly longer.

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
file for the parent grid, with each hour's flux stamped at the start of that
hour so the parent applies it in the hour it left the child. ``parent_up`` is
the parent grid run again from its own restart at the child's start, with that
file as its CDR forcing, so the parent receives what the child exported. Forge
builds ``parent_up`` from a blueprint with the CDR mode ``upscaled``, which
compiles ROMS to read its CDR forcing from a file.

**CDR metrics.** The ``cdr_metrics`` application computes the CO2 uptake and
the CDR efficiency from the ``_cdr`` output of the child and of ``parent_up``.
It analyses each domain on its own grid and combines them.
:doc:`../applications/analytics/cdr_metrics` describes the application and its blueprint.

Run it
------

From the directory with the four files:

.. code-block:: console

   cstar workplan check workplan.yaml
   cstar workplan submit workplan.yaml

``cstar workplan submit`` returns once it has scheduled the steps. They run
detached, each one waiting for the steps it depends on, and closing the
terminal does not stop them, so follow the run with
``cstar run status nest-tiny``. The run ID defaults to the workplan's name,
``nest-tiny``. Each step's files are under
``$CSTAR_DATA_HOME/workplan_runs/nest-tiny/tasks/<step>/``, which is
``~/cstar/workplan_runs/nest-tiny/`` by default, and ``cstar run path nest-tiny
<step>`` prints a step's directory. Running the ``submit`` command again
attaches to the existing run; give a new ``--run-id`` to start over.

What each step does
-------------------

This is the workplan you ran. The header comments explain each step.

.. literalinclude:: nesting_upscaling_tiny/workplan.yaml
   :language: yaml
   :caption: workplan.yaml

The ROMS steps use ucla-roms 0.13.0 with MARBL and ParallelIO, and each runs
on 4 MPI ranks (a 2 by 2 tiling). Each ROMS step builds its own ROMS
executable: the three ROMS steps are three different builds. On a 10-core
laptop that was also busy with other work, with the source data already
downloaded, the whole workplan took about 20 minutes; it takes less on an
idle machine.

The child's sponge layer acts on currents only (``v_sponge_trc: 0.0`` in
``forge_child.yaml``): the child is upscaled, and tracer diffusion in its sponge
band would change the plume handed to ``parent_up``.

The release is alkalinity at 2e6 meq/s for four hours, from the child's
start. The workplan's three forge steps share nothing, but
``forge_child`` and ``forge_parent_up`` wait for ``forge_parent`` so that
first-time downloads happen once.

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
combined efficiency and uptake, and ``cdr_domains.png`` maps the two domains,
as at the top of this page.

The two domains are combined in a specific way:

- **Uptake** is summed over the child and ``parent_up``. Each domain removes
  the part of the perturbation that is inside it.
- **Efficiency** is the combined uptake divided by the alkalinity released in
  the child only. ``parent_up`` receives the same alkalinity through its
  boundary, so its source is marked ``source_role: transfer`` and is not counted
  a second time.
- **Closure residual** (``closure_residual``) is the combined uptake from the
  flux minus the combined uptake from the DIC difference. The two are
  independent views of the same perturbation. They can disagree for a single
  domain: DIC that the child took up and then exported shows up as a DIC
  difference in ``parent_up``. On the sum the residual should be small next to
  the flux-based uptake.

``cdr_metrics.nc`` also holds each domain's own uptake, such as
``child_carbon_uptake_from_flux`` and ``parent_up_carbon_uptake_from_flux``.
It holds one diagnostic as well, ``parent_up_DIC_difference_inside_child``: the
DIC perturbation that ``parent_up`` carries inside the child's footprint. It is
small but normally not zero, because anomaly that enters ``parent_up`` along
the child's boundary can spread into the footprint; ``closure_residual`` is the
consistency check.
:ref:`blueprints-cdr-metrics-nested` explains the diagnostic.

At the child's end (2012-01-03 06:00), a reference run with ucla-roms 0.13.0
and roms-tools 5.3.0 gave these combined values (rounded):

.. list-table::
   :header-rows: 1
   :widths: 60 20 20

   * - Quantity
     - From the flux
     - From the DIC difference
   * - CO2 uptake (tonnes)
     - 5.03
     - 5.03
   * - CDR efficiency
     - 0.0040
     - 0.0040

``closure_residual`` was below 0.001 tonnes, and
``parent_up_DIC_difference_inside_child`` was 0.08 tonnes. Your numbers should
be close to these but need not match them exactly.

.. figure:: ../images/nest-tiny-metrics.png
   :alt: Combined CDR efficiency and CO2 uptake against time. The flux and DIC-difference curves overlap and reach about 5 tonnes; the child's flux-based uptake levels off near 3.8 tonnes while parent_up's rises to about 1.3 tonnes
   :width: 90%

   ``cdr_metrics.png`` from the reference run. Top: the combined efficiency,
   from the CO2 flux and from the DIC difference, which lie on top of each
   other. Bottom: the combined uptake the same two ways, and the flux-based
   uptake of each domain: the child's levels off as the alkalinity leaves it,
   and ``parent_up``'s rises.

Each domain's own series show the hand-off. Until about midday on the 2nd the
alkalinity stays in the child, and its two uptake curves rise together. Then
the current carries the alkalinity east across the child's boundary, and the
two views part. The DIC difference counts the perturbation where it is now, so
the child's falls back to zero as its DIC leaves, and ``parent_up``'s rises to
nearly the whole 5 tonnes. The flux counts where the CO2 entered the ocean, so
the child keeps the 3.8 tonnes it took up while it held the alkalinity, and
``parent_up`` adds 1.3 tonnes after it arrives. Summed over both domains the
two views agree, which is what ``closure_residual`` checks.

.. figure:: ../images/nest-tiny-domain-uptake.png
   :alt: Two panels of CO2 uptake against time. Child: the flux-based curve rises to 3.8 tonnes and levels off; the DIC-based curve peaks near 2.6 tonnes and falls back to zero. parent_up: the flux-based curve rises to 1.3 tonnes and the DIC-based curve to 5.1 tonnes, both starting around midday
   :width: 100%

   Each domain's CO2 uptake in the reference run, from the CO2 flux (solid) and
   from the DIC difference (dashed): the per-domain series in
   ``cdr_metrics.nc``.

Where to go next
----------------

:doc:`../workplans/directives` describes ``nest-from`` and ``continue-from`` in
full, and :doc:`../applications/analytics/cdr_metrics` the metrics step, including the
metrics of a run split into several segments.
