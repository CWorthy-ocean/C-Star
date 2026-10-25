.. _workplan-references:

Path references and run aliases
===============================

Steps need to find one another's files: the restart a simulation wrote, the
grid a Forge step generated, the output an analysis step reads. Rather than
spelling out paths into the run directory, which would require the user to
understand and predict C-Star's directory handling and also tie the workplan to
one machine and one run, a step names the other step and the kind of
directory it wants. The same mechanism can reach into entirely different runs that
were executed from different workplans, through an alias declared at the top of the workplan.

Reaching another step's directories
-----------------------------------

Anywhere in a step (its blueprint path, its ``blueprint_overrides`` and its
``directives``), ``{{<scope>: <step>}}`` is replaced with a directory of
another step's layout when the workplan is scheduled. ``<step>`` is any
step in the workplan and ``<scope>`` is one of:

``root_dir``
   The step's root directory, containing everything below.
``input_dir``
   Inputs staged for the step: datasets, code and runtime configuration.
``run_dir``
   Generated run scripts and other work items.
``tasks_dir``
   Per-subtask working directories, for steps split into subtasks.
``logs_dir``
   Log files written during the step.
``output_dir``
   The step's final outputs.

For example, ``{{output_dir: outer}}`` is the ``output`` directory of the
step named ``outer``, and ``{{input_dir: outer}}/input_datasets/grid.nc`` is
a file in its staged inputs:

.. code-block:: yaml

    - name: nest_conversion
      application: nest_ic
      blueprint: inline
      depends_on:
        - outer
      blueprint_overrides:
        parent_grid: "{{input_dir: outer}}/input_datasets/parent_grid.nc"
        parent_rst: "{{output_dir: outer}}/output_rst.20100103000000.nc"
        child_grid: ./child_grid.nc

A placeholder is a reference to a directory, not to a particular file, so
when the file you want is one a step records as a product (a restart, the
boundary files for a nested child, the carbonate sensitivities), prefer a
:doc:`directive <directives>`, which looks the file up in the step's record
and fails loudly if it is missing.

.. _workplan_external_runs:

Referencing other runs
----------------------

A large workflow is often split into several workplans: a spin-up run, then
a production run that continues from it, or an outer-grid run and a nested
run. A workplan can reference the steps of a run that was executed earlier
(or is still running, see below) instead of spelling out paths into its run
directory. Declare the other runs under ``runs``, keyed by an alias of your
choosing, and reference one of their steps as ``<step>@<alias>`` wherever a
step name is accepted: ``depends_on``, a deferred ``blueprint: {from_step:
...}``, a directive's ``step`` key and a ``{{<scope>: <step>}}``
placeholder.

.. code-block:: yaml

    name: production
    description: Continue the spin-up run on the same grid
    runs:
      spinup: "{{spinup_run}}"      # a run, or a runtime variable holding one
    runtime_vars:
      - spinup_run

    steps:
      - name: nest_conversion
        application: nest_ic
        blueprint: inline
        depends_on:
          - outer@spinup
        blueprint_overrides:
          parent_grid: "{{input_dir: outer@spinup}}/input_datasets/parent_grid.nc"
          parent_rst: "{{output_dir: outer@spinup}}/output_rst.20100103000000.nc"
          child_grid: /path/to/child_grid.nc

      - name: child
        application: ucla_roms
        blueprint: ./child.yaml
        depends_on:
          - nest_conversion
          - outer@spinup
        directives:
          continue-from:
            step: nest_conversion
          nest-from:
            step: outer@spinup

Naming a run
^^^^^^^^^^^^

A run is named in one of three ways:

.. code-block:: yaml

    runs:
      spinup: "{{spinup_run}}"                            # its run-id
      base: run://6f1c2d3e-8a4b-4c57-9e21-3b7d5a0c4f18    # its uuid
      colleague: /path/to/their/run                       # its directory

- A run-id is the one shown by ``cstar run ls``: the value given to
  ``--run-id``, or derived from the workplan name.
- ``run://<uuid>`` is the ``id`` in the run's :doc:`manifest
  <../runs/moving/manifest>`, which the ``Id`` row of ``cstar run show``
  prints. The scheme is not case-sensitive. A uuid names one run; two runs
  can share a run-id.
- A directory is the run directory, the one that holds ``cstar-run.yaml``. Text
  with a ``/``, or one that starts with ``.`` or ``~``, is read as a directory,
  so ``./spinup`` is a directory and ``spinup`` is a run-id. In ``runs``, a
  relative directory is resolved against the directory of the workplan file (on
  the command line, against the current directory), ``~`` is expanded, and a
  ``{{name}}`` placeholder is filled first.

A run is found as follows:

- A run-id goes to the runs tracked on this machine first, and a tracked run is
  the one used. When the project runs area (see :doc:`../hpc`) holds another run
  with that run-id, a different uuid, C-Star warns, names it, and says to pass
  ``run://<uuid>`` or its directory to use that one. When the tracked run's
  directory is gone, as after a purge of scratch, a copy of the same run (the
  same uuid) in a directory named for the run-id, in your data home or the
  project runs area, is used.
- A run-id that is not tracked here is looked up in the manifests of the
  directories of that name under ``CSTAR_DATA_HOME/workplan_runs/``,
  ``CSTAR_DATA_HOME/blueprint_runs/<application>/`` and the project runs area,
  ``cstar/runs/`` under ``CSTAR_PROJECT_HOME``. Two different runs under one
  run-id are an error that lists them; C-Star does not pick the more recent.
  Name the one you mean by ``run://<uuid>`` or by its directory.
- ``run://<uuid>`` is looked up in the manifests of the runs under those same
  three directories, one level down. A run tracked here is used as tracked.
  The directories are scanned at most once per command, and no index file is
  written.
- A directory is read directly, wherever it is; when it is the directory of a
  run tracked here, that run is used as tracked.

Tracking is kept per machine, under ``CSTAR_STATE_HOME``. A run started on
another machine, or under another state home, is not tracked here. It is
reached by the path of its directory, or :doc:`attached
<../runs/moving/attach>`. When that directory is in one of the places scanned
above, the run is also reached by ``run://<uuid>``, and by its run-id when
the directory is named for it.

The rules
^^^^^^^^^

- Every alias used in a token must be declared under ``runs``; an undeclared
  alias is rejected when the workplan is loaded. Because ``@`` separates the
  step from the alias, step names and aliases may not contain it.
- An external step a step reads from must be listed in that step's
  ``depends_on`` (directly or through an earlier step), exactly as a local
  step must be. ``cstar workplan check`` and ``cstar workplan submit`` report
  every missing dependency.
- When the workplan is scheduled, each external step is looked up in the
  other run's records: the run must be found, the step must exist in it, and
  the step must have **completed** (``Done``). For a run tracked on this
  machine, a step that is still running is accepted only when both runs use
  the SLURM launcher; the new step is then submitted with a SLURM dependency
  on the other run's job. A step that failed, was cancelled, was never
  submitted, or is running under the local launcher is refused with a message
  naming the run, step and status. If a status looks stale (the other run
  finished but its orchestrator was not polled since), ``cstar run status
  <run-id>`` refreshes it.
- A run that is not tracked here is known only by its manifest: a copy, a run
  in the project runs area, a colleague's run reached by its directory, or a
  run in your data home that was not started on this machine. A run attached
  from elsewhere is read the same way. It has no job to wait for, so only a
  step its manifest records as ``Done`` can be depended on; any other status is
  refused with the recorded status named. A run with no
  workplan, such as a standalone blueprint run, cannot be referenced.
- The transformed workplan written into the run directory records, for each
  alias that a step refers to, the run it resolved to: the run's run-id
  (``run_id``), uuid (``id``), start time (``start_at``) and, for a reference
  by directory, that directory (``path``). The run's inputs stay traceable even
  if the same run-id is used again later, and later lookups, such as ``cstar
  run status`` or a step that depends on the run, go straight to that run. A
  workplan you write does not set ``id`` or ``path``; the :doc:`workplan
  schema <../schemas/index>` has them from version 1.1.0. The run manifest
  records the uuid of each referenced run as well.

.. _workplan_extending_run:

Extending a finished run
------------------------

Extending a run that you do not own, or one that was moved, starts a new run
that references it; nothing in the base run is modified. Give the new workplan
a run ID of its own (its ``name``, or ``--run-id``), declare the base run under
``runs`` by ``run://<uuid>``, or by its directory when it is not in your data
home or the project runs area, and take what you need from its steps. The
``Id`` row of ``cstar run show <run>`` prints the uuid.

.. code-block:: yaml

    name: production-year2
    description: Run a second year from where the finished run stopped
    runs:
      base: run://6f1c2d3e-8a4b-4c57-9e21-3b7d5a0c4f18

    steps:
      - name: simulate_year2
        application: ucla_roms
        blueprint:
          from_step: make_inputs@base
        depends_on:
          - make_inputs@base
          - simulate@base
        directives:
          continue-from:
            step: simulate@base

The step takes its blueprint from the ``make_inputs`` step of the base run and
its initial conditions from the restart file the ``simulate`` step wrote. A
``{{output_dir: simulate@base}}`` placeholder reaches the base run's output
directory in the same way. A base run that is not tracked here must have
completed the steps used (see above). The new run's manifest records the base
run by its uuid.

Re-entering the base run's own workplan is for a run you own that is tracked
on this machine: pass ``--clobber <step>`` to run steps again (see
:ref:`workplan_rerun`). A run you were given or moved is generally not
re-entered. Re-entry, with ``--run-id`` or ``--resume``, acts only on a run
tracked on this machine and not attached from elsewhere, and the run's recorded plan names the blueprint
files you wrote at the paths they had when the run was scheduled, outside the
run directory. A blueprint generated inside the run, or declared ``inline``,
is in the run directory. The copies under ``attempts/`` are a record.
