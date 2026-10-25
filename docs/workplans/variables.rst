.. _workplan-variables:

User variables
==============

A workplan can leave some values to be filled in when it is submitted: the
run to continue from, an end date, a time index to plot. Declare the names
under ``runtime_vars`` and use them as ``{{name}}`` anywhere in a step (its
blueprint path, ``blueprint_overrides`` and ``directives``) or in the
``runs`` table. The same file then serves as a template that is submitted
with different values each time.

.. code-block:: yaml

    name: production
    description: Continue a spin-up run and plot the last record
    runtime_vars:
      - spinup_run
      - time_slice
    runs:
      spinup: "{{spinup_run}}"

    steps:
      - name: simulate
        application: ucla_roms
        blueprint: ./production.yaml
        depends_on:
          - simulate@spinup
        directives:
          continue-from:
            step: simulate@spinup

      - name: plot
        application: plotter
        blueprint: ./plotter.yaml
        depends_on:
          - simulate
        blueprint_overrides:
          input_dir: "{{output_dir: simulate}}"
          time_indices:
            - "{{time_slice}}"

Supply the values when you check or submit the workplan, one ``--var`` per
name, or all of them in a file with one ``key=value`` per line:

.. code-block:: console

   cstar workplan check  production.yaml --var spinup_run=spinup-2012 --var time_slice=-1
   cstar workplan submit production.yaml --varfile production.vars

Every placeholder is replaced when the workplan is scheduled, before any
step runs. A name that is supplied but not declared in ``runtime_vars`` is
rejected, and so is a declared name that is left without a value, so a typo
on the command line is caught before anything is submitted.

The values a run was submitted with are kept with the run: the attempt's
``workplan.vars`` under ``attempts/`` in the run directory records them, and
the transformed workplan holds the filled-in result. Re-entering a run with
``--resume`` continues with the variables it was started with, so ``--var``
and ``--varfile`` cannot be combined with it. See
:doc:`../runs/moving/manifest`.

The other kind of placeholder, ``{{<scope>: <step>}}``, refers to another
step's directories rather than to a value you supply; see
:doc:`references`.
