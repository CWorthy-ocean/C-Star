.. _blueprints-cdr-metrics:

CDR metrics
===========

The ``cdr_metrics`` application computes the carbon dioxide removal (CDR)
metrics of a release from the ``_cdr`` output of UCLA-ROMS runs with MARBL: the CO2
uptake in tonnes and the CDR efficiency, each from the air-sea CO2 flux and
from the difference in dissolved inorganic carbon (DIC). The metrics are
roms-tools' ``compute_cdr_metrics``; the application finds the output, checks
it, and writes the results and plots.

It usually runs as the last step of a workplan, after the runs it analyses,
but it can also run on its own, on output from anywhere:

.. code-block:: console

   cstar blueprint run cdr_metrics_blueprint.yaml

What it contains
----------------

Besides the core fields every blueprint has (``name``, ``description``,
``application: cdr_metrics`` and an optional ``working_dir``), a CDR metrics
blueprint has one field, ``domains``. It maps a domain name to:

``grid``
   The ``location`` of the domain's ROMS grid file.
``sources``
   The ordered sources of the domain's ``_cdr`` output, each a ``location``
   that is a step's ``output`` directory, another directory of ``_cdr``
   files, or a single ``_cdr`` file. The first source must hold the segment
   containing the release, because the cumulative sums start at its first
   record.
``source_role``
   ``release`` (the default) for the domain holding the CDR release, or
   ``transfer`` for a parent domain that receives the release from a nested
   child (see :ref:`blueprints-cdr-metrics-nested`).

One domain gives the metrics of a single run. Several domains, listed child
first, are combined into one set of metrics. In a workplan the whole blueprint
is usually given inline, with ``blueprint: inline`` and ``domains`` under the
step's ``blueprint_overrides``, as in the examples below. The published JSON
schema is listed under :doc:`../../schemas/index`.

Example
-------

A single domain whose run was split into two segments:

.. code-block:: yaml

    name: wio-cdr-metrics
    description: CDR metrics for the western Indian Ocean release
    application: cdr_metrics
    domains:
      wio:
        grid:
          location: input/wio_grid.nc
        sources:
          - location: runs/segment-01/output
          - location: runs/segment-02/output

What it writes
--------------

The application writes to its ``output`` directory:

- ``cdr_metrics.nc``, a time series of the CDR efficiencies and the uptake in
  tonnes of CO2. With several domains it also holds each domain's uptake and
  two diagnostics (see :ref:`blueprints-cdr-metrics-nested`).
- ``cdr_metrics.png``, a plot of them.
- ``cdr_domains.png``, with several domains only, a map of the domains.

Its step record lists the series as an ``analysis`` product with
``metadata["role"]`` ``cdr_metrics`` and the plots as ``figure`` products (the
map with ``metadata["role"]`` ``domains``), so a later step can find the series
by kind and ``cstar run check`` reports any of the files if it goes missing.

The producing runs must be MARBL runs with a CDR forcing; forge turns on
``cdr_output`` for an active MARBL CDR forcing. A domain whose ``_cdr`` output
lacks a variable the metrics need is reported with the ROMS setting that writes
it.

Segmented runs
--------------

The application stitches the segments of a run that was split into several
steps, such as a run chained with ``continue-from``: list each segment's output
under ``sources``, in order, from the segment containing the release onward.

.. code:: yaml

    name: cdr-metrics-workplan
    description: CDR metrics over two consecutive segments
    state: draft

    steps:
    - name: seg-01
      application: ucla_roms
      blueprint: /path/to/segment_01.yaml

    - name: seg-02
      application: ucla_roms
      blueprint: /path/to/segment_02.yaml
      depends_on:
      - seg-01
      directives:
        continue-from:
          step: seg-01

    - name: metrics
      application: cdr_metrics
      blueprint: inline
      depends_on:
      - seg-01
      - seg-02
      blueprint_overrides:
        domains:
          wio:
            grid:
              location: /path/to/wio_grid.nc
            sources:
            - location: "{{output_dir: seg-01}}"
            - location: "{{output_dir: seg-02}}"

The records are sorted by time after loading; a gap or an overlap between the
averaging windows of consecutive records is an error, so a segment missing from
``sources`` (or listed twice) is reported rather than silently skewing the
cumulative sums.

.. note::

   With ucla-roms 0.10.1 and 0.11.0, the averaged air-sea CO2 flux diagnostics
   (``FG_CO2``, ``FG_ALT_CO2``) written after a ``continue-from`` restart differ from
   those of the same run without a restart, although the model state is identical. The
   flux-based metrics of a segmented run can therefore differ from the unsegmented
   run's; the metrics from DIC differences match it exactly.

.. _blueprints-cdr-metrics-nested:

Nested (upscaled) experiments
-----------------------------

In a nested experiment the release takes place in a child domain and the perturbation
is handed on to one or more parent domains with the ``upscaler`` application. A
parent's ``ALK_source`` and ``DIC_source`` are then a *transfer* of the same
alkalinity, not a new release. List every domain in ``domains``, child first, and
mark each parent with ``source_role: transfer``:

.. code:: yaml

    - name: metrics
      application: cdr_metrics
      blueprint: inline
      depends_on:
      - child
      - parent_up
      blueprint_overrides:
        domains:
          child:
            grid:
              location: "{{input_dir: child}}/input_datasets/child_grid.nc"
            sources:
            - location: "{{output_dir: child}}"
          parent:
            source_role: transfer
            grid:
              location: "{{input_dir: parent_up}}/input_datasets/parent_grid.nc"
            sources:
            - location: "{{output_dir: parent_up}}"

Each domain is analysed on its own grid, and the results are combined with roms-tools'
``UpscaledCDR``: the CO2 uptake of all domains is summed, and the efficiency is the total
uptake divided by the cumulative source of the release domain only, so the transferred
alkalinity is not counted twice. ``cdr_metrics.nc`` then also holds each domain's
uptake series and two diagnostics. ``closure_residual`` is the combined flux-based minus
DIC-difference-based uptake; the two are independent views of the same perturbation, so a
residual small against the uptake is the sign that the domains were combined
consistently. ``<parent>_DIC_difference_inside_<child>`` is the DIC perturbation a parent
carries in its cells inside the child's footprint. It is normally nonzero: the upscaler
re-injects what leaves the child along the child's boundary, which lies inside that
footprint, and the child's own inventory no longer holds it. A parent must be a MARBL
run writing ``_cdr`` output; a CDR-lite parent writes ``_cdrtrc`` output, which the
metrics cannot use.

The :doc:`nesting and upscaling tutorial <../../tutorials/nesting_upscaling>` runs a
nested experiment end to end, with this step last.
