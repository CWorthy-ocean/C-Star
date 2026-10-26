.. _analytics-index:

Analytics
=========

Analysis applications read the output of UCLA-ROMS runs and write results
of their own: numbers, time series, figures. They usually run as the last
step of a workplan, after the runs they analyse, but each can also run on
its own on output from anywhere.

.. toctree::
   :maxdepth: 1

   CDR metrics <cdr_metrics>
   Plotter <plotter>

:doc:`cdr_metrics` computes the CO2 uptake and the CDR efficiency of a carbon
dioxide removal release, across one domain or a nested set. :doc:`plotter` is
a small demonstration of a plotting step, kept as an example of how an
analysis application is put together.
