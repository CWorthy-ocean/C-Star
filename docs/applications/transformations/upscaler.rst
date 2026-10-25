.. _upscaler:

Upscaling CDR signals
=====================

A carbon dioxide removal (CDR) release is best resolved on a fine grid, but
its effect spreads well beyond that grid. In a nested experiment the release
happens in a child domain, and the alkalinity and DIC perturbation that
leaves the child through its open boundaries has to be handed on to the
parent, or the parent never sees it. The ``upscaler`` application does the
handing on: it turns the child's boundary fluxes into a CDR forcing file for
the parent grid, so that a second run of the parent receives what the child
exported, where and when it left.

What it does
------------

A child run with upscaled output switched on (the ``upscale_output`` stream
in its output settings, which the wizard lists among the main streams for a
child grid) writes
``*_uscl.*.nc`` files: the flux of the CDR tracers through each open
boundary, averaged over each output window, column by column along the
boundary. The upscaler reads every ``_uscl`` file of the run, in time order,
and writes ``upscaled_cdr.nc`` in its ``output`` directory. That file is a
ROMS CDR forcing file: one release profile per boundary column, at the
column's position, with the flux of each tracer through each layer and the
layer thicknesses, and one record per averaging window.

Alkalinity and DIC are always included. When the child run also carried
ucla-roms' dedicated CDR-LiTE tracers, their fluxes follow, and the file
names each tracer so that the parent applies each column to the tracer of
that name. Each record is stamped at the start of its window, so the parent
applies the flux in the hour it left the child.

The upscaler checks the ``_uscl`` files as it reads them. Files from a run
that was split into several steps must be given together, and a missing
file shows up as a gap between windows, which is reported rather than
silently upscaled. By default the file is written for a ROMS build with
ParallelIO, which needs CDF-5 netCDF; set ``pio: false`` for a build
without it.

The blueprint
-------------

``uscl_file_location``
   The directory holding the child's ``_uscl`` files: the child step's
   ``output`` directory. When that step's record lists the files, the record
   is used and a file it lists that is missing stops the step; otherwise the
   directory is searched.
``pio`` (optional, default ``true``)
   Whether the parent's ROMS build reads its inputs through ParallelIO.

Example
-------

The upscaler sits between the child run and a second run of the parent. The
parent is run again from its own restart at the child's start, with
``upscaled_cdr.nc`` as its CDR forcing. Forge builds that second parent from
a blueprint whose CDR mode is ``upscaled``, which compiles ROMS to read its
CDR forcing from a file:

.. code-block:: yaml

   steps:
     - name: upscale
       application: upscaler
       blueprint: inline
       depends_on:
         - child
       blueprint_overrides:
         uscl_file_location: "{{output_dir: child}}"

     - name: parent_up
       application: ucla_roms
       blueprint:
         from_step: forge_parent_up
       depends_on:
         - forge_parent_up
         - upscale
         - parent
       blueprint_overrides:
         cdr_forcing:
           data:
             - location: "{{output_dir: upscale}}/upscaled_cdr.nc"
       directives:
         continue-from:
           step: parent
           timestamp: "2012-01-02 06:00:00"   # the child's start

The ``timestamp`` on ``continue-from`` picks the parent restart at the
child's start rather than the latest one; see
:doc:`../../workplans/directives`. The :doc:`CDR metrics <../analytics/cdr_metrics>`
application then combines the child and ``parent_up`` into one set of
metrics, counting the parent's source as a transfer rather than a second
release. The :doc:`nesting and upscaling tutorial
<../../tutorials/nesting_upscaling>` runs the whole chain on a laptop.

On its own, from a blueprint file:

.. code-block:: yaml

   name: upscale-child
   description: CDR forcing for the parent grid from the child's boundary fluxes
   application: upscaler
   uscl_file_location: /data/child/output

.. code-block:: console

   cstar blueprint run upscale.yaml
