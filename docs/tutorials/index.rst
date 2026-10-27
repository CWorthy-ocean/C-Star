:orphan:

.. _tutorials-index:

Guided tutorials
================

These tutorials take you from a fresh install to a nested carbon dioxide
removal (CDR) experiment, one idea at a time. Each one runs on a laptop, and
each builds on the one before it, so work through them in order.

Before you start
----------------

Do these once, before the first tutorial:

1. :doc:`Install C-Star <../getting_started/installation>` and activate the
   environment.
2. Run ``cstar init``. It sets up where C-Star keeps run output, source data
   and your catalog, offers to install the shell function behind
   ``cstar run cd`` (used in the workplan tutorials), and ends by listing the
   datasets that need a one-time registration. See
   :doc:`../getting_started/configuration`.
3. :ref:`Register for Copernicus Marine <forge-data-access-glorys>` and run
   ``copernicusmarine login``. Every tutorial uses GLORYS for the ocean state.
   None of them uses tides, so you can skip the TPXO registration for now.

The first tutorial you run downloads the source data into a shared cache,
including a 10 GB biogeochemistry climatology, so expect it to take a while
on a slow connection. Later tutorials reuse the cache.

The tutorials
-------------

:doc:`blueprint_tutorial`
   Build a forge blueprint with the wizard, run Forge to generate the inputs
   of a tiny Western Indian Ocean domain, then run the UCLA-ROMS simulation
   Forge produced. Introduces blueprints and the basic building blocks of
   running an ocean model with C-Star.

:doc:`workplan`
   Run the same two steps as one **workplan**: a YAML file that lists the
   steps, orders them, and hands Forge's output to the simulation. Covers
   submitting a run, checking its status and finding its output.

:doc:`cdr_workplan`
   A five-step workplan that generates inputs, spins the domain up, continues
   it with an alkalinity release, and computes the release's CO2 uptake and
   CDR efficiency. Introduces step overrides and directives, the two ways steps
   pass information to each other.

:doc:`nesting_upscaling`
   Run the release in a fine **child** grid nested inside a coarser
   **parent**, then upscale the child's result back into the parent and
   compute the CDR metrics of the whole release.
