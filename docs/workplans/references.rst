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
      spinup: "the-spinup-run-id-you-ran-earlier"

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
      run-ref-1: "the-spinup-run-id-you-ran-earlier"         # its run-id
      run-ref-2: run://6f1c2d3e-8a4b-4c57-9e21-3b7d5a0c4f18    # its uuid
      run-ref-3: /path/to/their/run                       # its directory

- A run-id is the one shown by ``cstar run ls``: the value given to
  ``--run-id``, or derived from the workplan name.
- ``run://<uuid>`` is the ``id`` in the run's :doc:`manifest
  <../runs/moving/manifest>`, which the ``Id`` row of ``cstar run show``
  prints. The scheme is not case-sensitive. A uuid names one run; two runs
  can share a run-id. When copies of the run are tracked here under several
  run IDs, the uuid resolves to the run started here first, then to an
  attached copy.
- A directory is the run directory, the one that holds ``cstar-run.yaml``.
  Anything with a ``/``, or starting with ``.`` or ``~``, is read as a
  directory, so ``./spinup`` is a directory and ``spinup`` is a run ID. A
  relative directory is resolved against the directory of the workplan file.

Any of these formats can also utilize :doc:`runtime variables <variables>`
to dynamically reference a run passed via the command line.

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


Further details on run referencing
----------------------------------

How run references are resolved
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- A run ID is looked up among the runs tracked on this machine first, then
  in directories of that name in your data home and the project runs area.
  If two different runs share the run ID, C-Star stops and lists them; name
  the one you mean by ``run://<uuid>`` or by its directory.
- ``run://<uuid>`` is looked up in the same places; when copies of the run
  are tracked under several run IDs, it resolves to the run started here
  first, then to an owned attached copy, then to a read-only one, with a
  warning naming the copies passed over (name a copy by its run ID or
  directory to use it).
- A directory is read directly, wherever it is.

The rules
^^^^^^^^^

- Every alias used in a step must be declared under ``runs``. Step names
  and aliases may not contain ``@``.
- A step that reads from another run's step must list it in ``depends_on``,
  exactly as for a step of its own workplan.
- The other run's step must have **completed** when the workplan is
  submitted. The one exception is a run on this machine whose step is still
  running under SLURM: the new step is then submitted to wait for that job.
- The run directory records which run each alias resolved to, so the inputs
  stay traceable even if the same run ID is used again later.
