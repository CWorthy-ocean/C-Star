Workplans
=========

Workplans define the contract for requesting the execution of one or more
:doc:`blueprints`. A user-configured workplan informs C-Star which ``Blueprint``
to execute, and in what order.


Workplan Schema
---------------

Workplans are defined in :class:`cstar.orchestration.models.Workplan`.

.. rubric:: Workplan Attributes

.. autosummary::

  ~cstar.orchestration.models.Workplan.name
  ~cstar.orchestration.models.Workplan.description
  ~cstar.orchestration.models.Workplan.steps
  ~cstar.orchestration.models.Workplan.state
  ~cstar.orchestration.models.Workplan.compute_environment
  ~cstar.orchestration.models.Workplan.runtime_vars
  ~cstar.orchestration.models.Workplan.runs


State
^^^^^

A workplan is marked *draft* or *validated* with :attr:`~cstar.orchestration.models.Workplan.state`.
A draft workplan can be edited freely and submitted for execution. The validated
state is reserved for workplans whose modification will be restricted to preserve
reproducibility; C-Star does not yet enforce that restriction, so today the field
is informational.


.. _workplan_compute_environment:

Compute Environment
^^^^^^^^^^^^^^^^^^^

The compute environment a workplan expects is described with
:attr:`~cstar.orchestration.models.Workplan.compute_environment`. Every key is
optional, and unknown keys are rejected when the workplan is loaded:

.. code-block:: yaml

    compute_environment:
      launcher: slurm          # "local" | "slurm" | omitted
      system: anvil            # optional, informational
      slurm:                   # workplan-wide SLURM defaults
        account_name: x-ees250129
        queue_name: wholenode
        max_walltime: "04:00:00"

- ``launcher`` selects how steps are executed. Omitted, C-Star picks the
  launcher for the current system: SLURM when the system has a scheduler,
  otherwise local. ``slurm`` on a system with no scheduler stops the run
  with an error. ``local`` on a system with a scheduler runs the steps as
  local processes and logs a warning.
- ``system`` names the machine the workplan was written for. A mismatch with
  the system C-Star detects produces a single warning; it does not stop the run.
- ``slurm`` takes the same keys as a step's ``compute_overrides.slurm`` (see
  below) and applies to every step. SLURM settings layer, lowest to highest
  precedence: the ``CSTAR_SLURM_*`` environment variables, then
  ``compute_environment.slurm``, then the step's own ``compute_overrides``.
  The merged result is recorded on each step of the transformed workplan
  written into the run directory.

.. note::

   On systems whose settings require a SLURM account and queue (such as Anvil
   and Bouchet), values the workplan supplies here, or in every step's
   ``compute_overrides``, stand in for ``CSTAR_SLURM_ACCOUNT`` and
   ``CSTAR_SLURM_QUEUE``; only the ones it leaves out must be set in the
   environment.

Per-step compute requirements are set with each step's ``compute_overrides``
(see below).


Runtime Variables
^^^^^^^^^^^^^^^^^

C-Star fills ``{{ }}`` placeholders anywhere in a step -- its blueprint path,
``blueprint_overrides`` and ``directives`` values -- when the workplan is
scheduled. Two forms are supported:

- ``{{name}}`` is replaced with a value supplied at runtime. Declare the allowed
  names in :attr:`~cstar.orchestration.models.Workplan.runtime_vars`, then supply
  values when running the workplan with ``cstar workplan run --var name=value``
  (repeatable) or ``--varfile path`` (a file with one ``key=value`` pair per
  line). A name supplied that was not declared in ``runtime_vars`` is rejected.
- ``{{<scope>: <step>}}`` is replaced with a directory path from another step's
  file layout, where ``<step>`` names any step in the workplan and ``<scope>``
  is one of:

  - ``root_dir`` -- the step's root directory, containing everything else below
  - ``input_dir`` -- inputs staged for the step, such as datasets, code, and runtime configuration
  - ``run_dir`` -- generated run scripts and other work items
  - ``tasks_dir`` -- per-subtask working directories, for steps split into subtasks
  - ``logs_dir`` -- log files written during the step
  - ``output_dir`` -- the step's final outputs

  For example, ``{{output_dir: outer}}`` resolves to the ``output`` directory
  of the step named ``outer``. ``<step>`` may also name a step of another
  workplan run; see :ref:`workplan_external_runs`.


.. _workplan_external_runs:

Referencing other runs
^^^^^^^^^^^^^^^^^^^^^^

A large workflow is often split into several workplans -- a spin-up run, then
a production run that continues from it, or an outer-grid run and a nested
run. A workplan can reference the steps of a run that was executed earlier
(or is still running, see below) instead of spelling out paths into its run
directory. Declare the other runs under
:attr:`~cstar.orchestration.models.Workplan.runs`, keyed by an alias of your
choosing, and reference one of their steps as ``<step>@<alias>`` wherever a
step name is accepted: ``depends_on``, a deferred ``blueprint: {from_step:
...}``, a directive's ``step`` key and a ``{{<scope>: <step>}}`` placeholder.

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
        application: roms_marbl
        blueprint: ./child.yaml
        depends_on:
          - nest_conversion
          - outer@spinup
        directives:
          continue-from:
            step: nest_conversion
          nest-from:
            step: outer@spinup

A run is named in one of three ways:

.. code-block:: yaml

    runs:
      spinup: "{{spinup_run}}"                            # its run-id
      base: run://6f1c2d3e-8a4b-4c57-9e21-3b7d5a0c4f18    # its uuid
      colleague: /path/to/their/run                       # its directory

- A run-id is the one shown by ``cstar run ls``: the value given to
  ``--run-id``, or derived from the workplan name.
- ``run://<uuid>`` is the ``id`` in the run's :ref:`manifest
  <workplan_run_manifest>`, which the ``Id`` row of ``cstar run show`` prints.
  The scheme is not case-sensitive. A uuid names one run; two runs can share a
  run-id.
- A directory is the run directory, the one that holds ``cstar-run.yaml``. Text
  with a ``/``, or one that starts with ``.`` or ``~``, is read as a directory,
  so ``./spinup`` is a directory and ``spinup`` is a run-id. In ``runs``, a
  relative directory is resolved against the directory of the workplan file (on
  the command line, against the current directory), ``~`` is expanded, and a
  ``{{name}}`` placeholder is filled first.

A run is found as follows:

- A run-id goes to the runs tracked on this machine first, and a tracked run is
  the one used. When the project runs area (see :doc:`hpc`) holds another run
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

The rules:

- Every alias used in a token must be declared under ``runs``; an undeclared
  alias is rejected when the workplan is loaded. Because ``@`` separates the
  step from the alias, step names and aliases may not contain it.
- An external step a step reads from must be listed in that step's
  ``depends_on`` (directly or through an earlier step), exactly as a local
  step must be. ``cstar workplan check`` and ``cstar workplan run`` report
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
  schema <schemas/index>` has them from version 1.1.0. The run manifest
  records the uuid of each referenced run as well.

Tracking is kept per machine, under ``CSTAR_STATE_HOME``. A run started on
another machine, or under another state home, is not tracked here. It is
reached by the path of its directory. When that directory is in one of the
places scanned above, the run is also reached by ``run://<uuid>``, and by its
run-id when the directory is named for it.

The transformed workplan holds absolute paths into the run directory, and
records the run directory it was written under (``root``). Every command that
reads it rebases the paths that begin with the recorded root onto the directory
the file now sits in, so a run directory stays valid wherever it is moved or
copied; a value that merely contains the root, such as ``--out=<root>/x``, is
left as written. The run record in the state home follows a moved run only into
the current data home: when the run directory it names no longer exists, it is
read as ``workplan_runs/<run-id>`` under ``CSTAR_DATA_HOME``. Moving one run
from one data home to another on the same machine (move
``<old>/workplan_runs/<id>`` to ``<new>/workplan_runs/<id>`` and point
``CSTAR_DATA_HOME`` at ``<new>``) therefore keeps ``status``, ``gather``,
``continue-from`` and ``--resume`` working. A run copied to any other path, or
to another machine, has valid internal paths and is read through its directory,
but it is not tracked there: ``cstar run status`` cannot act on it until it is
registered with ``cstar run attach`` (see :ref:`workplan_attach_run`). Even
then, ``--resume`` and ``--run-id`` stay refused: an attached run is inspected
and referenced, never relaunched.
``cstar workplan run --resume`` re-exports the environment captured when the
run started, except ``CSTAR_DATA_HOME`` and ``CSTAR_STATE_HOME``, and warns
about every other captured setting that differs from your shell. A transformed
workplan written by an earlier release records no root and is loaded as it is,
so it stays pinned to the directory it was written in.

.. _workplan_extending_run:

Extending a finished run
""""""""""""""""""""""""

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
        application: roms_marbl
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


Steps
^^^^^

The heart of a workplan is the collection of steps found in :attr:`~cstar.orchestration.models.Workplan.steps`.
Steps have a 1-to-1 relationship with blueprints - each step must specify the path to a blueprint file,
reference a blueprint that will be generated by an earlier step (see the *Deferred Blueprints* example below),
or declare the blueprint ``inline``, built entirely from the step's ``blueprint_overrides`` (see the
*Inline Blueprints* example below).
The step also specifies the application type to use for its execution.

A relative ``blueprint`` path is resolved against the directory containing the
workplan file, not the directory ``cstar`` is run from, so a workplan can be
checked and run from anywhere and moved together with the blueprints beside it.
The transformed workplan written into the run directory records the resolved
absolute path.


Step Schema
-----------

See :class:`cstar.orchestration.models.Step` for complete details on configuring steps.

.. rubric:: Step Attributes

.. autosummary::

  ~cstar.orchestration.models.Step.name
  ~cstar.orchestration.models.Step.application
  ~cstar.orchestration.models.Step.blueprint_path
  ~cstar.orchestration.models.Step.depends_on
  ~cstar.orchestration.models.Step.blueprint_overrides
  ~cstar.orchestration.models.Step.compute_overrides
  ~cstar.orchestration.models.Step.workflow_overrides
  ~cstar.orchestration.models.Step.directives

``workflow_overrides`` recognizes three keys: ``clobber`` (clear the step's prior
state and re-execute it from scratch), ``resume`` (continue the step's
failed prior attempt in place instead) and ``pre_run`` (perform every stage
before the model launch, then stop; see :ref:`workplan_pre_run`). ``resume``
is mutually exclusive with both ``clobber`` and ``pre_run`` on a single step.

Compute overrides
^^^^^^^^^^^^^^^^^

By default every step is submitted with the account, queue and walltime
from your environment (``CSTAR_SLURM_ACCOUNT``, ``CSTAR_SLURM_QUEUE``,
``CSTAR_SLURM_MAX_WALLTIME``; see :doc:`hpc`). A step's ``compute_overrides``
replaces any of them, under a key naming the launcher:

.. code-block:: yaml

    steps:
    - name: make_inputs
      application: forge
      blueprint: forge_blueprint.yaml
      compute_overrides:
        slurm:
          queue_name: day          # a general-purpose queue for data processing
          max_walltime: "04:00:00"
          num_cpus: 8
    - name: simulate
      application: roms_marbl
      blueprint:
        from_step: make_inputs
      depends_on: [make_inputs]
      compute_overrides:
        slurm:
          queue_name: mpi
          account_name: my-allocation
          num_cpus: 128

The ``slurm`` keys are ``account_name``, ``queue_name``, ``max_walltime``
(``HH:MM:SS`` or SLURM's ``D-HH:MM:SS``, so ``30:00:00`` and ``1-06:00:00``
are the same request), ``num_cpus``, ``num_nodes``, ``cpus_per_node`` and
``single_node``. The ``local`` launcher accepts ``max_walltime`` and
``force_kill_timeout``, enforced with GNU ``timeout`` (on macOS, install
``coreutils`` from conda-forge or Homebrew, which provides ``gtimeout``).
``num_cpus`` is also where a deferred-blueprint step
declares its allocation, since C-Star cannot read the blueprint at submit time.
Defaults shared by every step belong in the workplan's
:ref:`compute environment <workplan_compute_environment>`.

Directives
^^^^^^^^^^

A step's ``directives`` run on the compute node just before the application
starts, and modify the blueprint using information that only exists at run
time -- such as a restart file written by an earlier step. See
:doc:`workplans/directives` for the available directives and how to configure
them.


.. _workplan_examples:

Workplan Examples
-----------------

.. tab-set::

   .. tab-item:: Single-step

    The following example demonstrates the minimum possible workplan.

    It contains a single step to be executed.

    .. code:: yaml

        name: simple-workplan
        description: Run a simulation
        state: draft

        steps:
        - name: job1
          application: roms_marbl
          blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml

   .. tab-item:: Multi-step

    The following example demonstrates a workplan with multiple steps. Note that each step
    can reference different blueprints.

    Additionally, this example introduces a simple dependency with ``depends_on: job1``. A
    dependency must be specified if the steps of the workplan require specific ordering. Here,
    *job1* must complete successfully before *job2* will start.

    .. important::
        A multi-step workplan without dependencies has no ordering guarantees.

        Jobs are scheduled immediately and executed as the system launcher permits.

    .. code:: yaml

        name: multi-step-workplan
        description: Run multiple ROMS-MARBL simulations
        state: draft

        steps:
        - name: job1
          application: roms_marbl
          blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml
        - name: job2
          application: roms_marbl
          blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_b.yaml
          depends_on:
          - job1
        - name: job3
          application: roms_marbl
          blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_c.yaml

   .. tab-item:: Overriding Blueprints

    The following example demonstrates how to override configuration in a
    blueprint from the workplan. Overriding blueprints enables the same
    blueprint to be used with different inputs, data sources, etc.

    .. tip::
        Blueprint overrides are supplied as a dictionary with
        :ref:`Blueprint schema<blueprint_schema>`

    .. code:: yaml

        name: workplan-overriding-a-blueprint
        description: Run a blueprint ensemble varying parameters with overrides
        state: draft

        steps:
        - name: step 1
          application: roms_marbl
          blueprint: blueprint.yaml
          blueprint_overrides:
          - name: Run blueprint with development UCLA-ROMS branch
          - code:
              roms:
                location: https://github.com/CWorthy-ocean/ucla-roms.git
                branch: develop

        - name: step 2
          application: roms_marbl
          blueprint: blueprint.yaml
          blueprint_overrides:
          - name: Run blueprint with custom UCLA-ROMS fork
          - code:
              roms:
                location: https://github.com/github-user/ucla-roms.git
                branch: main

   .. tab-item:: Deferred Blueprints

    The following example demonstrates a step consuming a blueprint that is
    *generated* by an earlier step and does not exist when the workplan is
    scheduled. Instead of a path, the ``blueprint`` field is given a mapping
    naming the producing step. At runtime, C-Star locates the generated
    blueprint in the producing step's output directory.

    .. note::
        - The producing step named in ``from_step`` must also be listed in
          ``depends_on``.
        - ``filename`` is optional. When omitted, the blueprint the producing
          step recorded (its ``emitted_blueprint`` product) is used; for a run
          recorded by C-Star 0.17.0 or earlier, whose step record lists no
          products, exactly one blueprint file must be present in the producing
          step's output directory.
        - ``blueprint_overrides`` are still supported; they are applied at
          runtime, once the generated blueprint exists.
        - The generated blueprint must declare the same application as the
          step, and applications with schedule-time transforms (e.g. ROMS-MARBL
          time splitting) cannot be used with a deferred blueprint.
        - When running under SLURM, the CPU allocation for a deferred step
          defaults to 1 because the blueprint cannot be inspected at submit
          time. Declare the expected allocation with ``compute_overrides``.

    .. code:: yaml

        name: deferred-blueprint-workplan
        description: Run a simulation from a generated blueprint
        state: draft

        steps:
        - name: make_blueprint
          application: nest_ic
          blueprint: /path/to/nest_ic_blueprint.yaml

        - name: run_generated
          application: roms_marbl
          depends_on:
          - make_blueprint
          blueprint:
            from_step: make_blueprint
            filename: blueprint.yaml
          compute_overrides:
            slurm:
              num_cpus: 64
          blueprint_overrides:
            runtime_params:
              end_date: "2025-02-01 00:00:00"

   .. tab-item:: Inline Blueprints

    The following example demonstrates a step with no blueprint file. Setting
    ``blueprint`` to the literal token ``inline`` builds the step's blueprint
    from its ``blueprint_overrides`` alone. This suits small applications, such
    as ``nest_ic``, whose fields are all set per step.

    .. note::
        - The ``blueprint`` field is still required; a step that omits it fails
          schema validation. A relative blueprint file literally named
          ``inline`` must be written ``./inline``.
        - The step's ``name``, ``application``, and run directory seed the
          blueprint, and ``blueprint_overrides`` are merged on top, so any
          field can be set there.
        - ``blueprint_overrides`` must supply every field the application's
          blueprint requires. This is validated by ``cstar workplan check`` and
          when the workplan is run, and every missing field is reported.
        - Placeholders such as ``{{input_dir: outer}}`` are filled as usual.
        - The resulting blueprint is written to the step's work directory
          (``tasks/<step>/work/blueprint.yaml`` under the run directory) when
          the workplan is scheduled, recording exactly what ran.
        - Applications with schedule-time transforms (e.g. ROMS-MARBL time
          splitting) cannot be used with an inline blueprint.

    .. code:: yaml

        name: inline-blueprint-workplan
        description: Convert a restart file to initial conditions for a nested grid
        state: draft

        steps:
        - name: outer
          application: roms_marbl
          blueprint: /path/to/outer_blueprint.yaml

        - name: nest_conversion
          application: nest_ic
          blueprint: inline
          depends_on:
          - outer
          blueprint_overrides:
            parent_grid: "{{input_dir: outer}}/input_datasets/parent_grid.nc"
            parent_rst: "{{root_dir: outer}}/output/output_rst.20100103000000.nc"
            child_grid: /path/to/child_grid.nc


Checking validity
-----------------

Workplans can be checked for errors using the CLI and in code.

.. tab-set::

   .. tab-item:: Validating via CLI

    Use the ``check`` command from the ``cstar`` CLI.

    .. code-block:: console

        cstar workplan check my_workplan.yaml

    By default ``check`` performs the same resolution that ``run`` performs
    before submitting anything: it validates the file's structure, then
    imports each step's application, loads and validates each blueprint,
    merges ``blueprint_overrides``, and validates every directive (unknown
    directive keys, malformed configuration, and ``step`` references that do
    not name an upstream dependency are all reported together). Nothing is
    written to disk. If the workplan declares ``runtime_vars``, supply them
    with ``--var``/``--varfile`` exactly as you would for ``run``. Pass
    ``--schema-only`` to validate only the file's structure, for example
    when the referenced blueprints are not available on the current machine.

   .. tab-item:: Programmatic Validation

    Use the ``deserialize`` method to validate a YAML file in Python.

    .. code-block:: python

        from cstar.orchestration.models import Workplan
        from cstar.orchestration.serialization import deserialize

        deserialize("my_workplan.yaml", Workplan)


Execution
---------

.. include:: snippets/review-config.rst

.. attention::
    An error will occur if the SLURM **account** and **queue** are not configured when running on a HPC.


.. tab-set::

   .. tab-item:: Run via CLI

    Use the ``run`` command from the ``cstar`` CLI to execute the workplan.

    .. code-block:: console

        cstar workplan run --run-id <my-unique-id> my_workplan.yaml

    .. tip::
        Executing the command again with the same :term:`run ID` will attach to the
        previous execution and schedule any steps not yet scheduled. Like the first
        run, it returns without waiting for the steps to finish; use
        ``cstar run status <run-id>`` to follow their progress.

        Specify a different :term:`run ID` to re-run the workplan from scratch.

    Re-entering a prior run (``--run-id`` and no workplan path) accepts two
    mutually-exclusive controls for handling failed or stale steps instead of
    reusing them as-is:

    - ``--clobber <step-name|all>`` clears the named step(s) (or every step,
      with the literal value ``all``) and re-executes them from scratch.
    - ``--resume`` continues every *failed* step in place, from its last usable
      restart, instead of clearing it -- provided the step's application
      declares itself resumable; other failed steps are re-run from scratch
      with a warning, and completed steps are left untouched.

    ``--resume`` also accepts the workplan path the run was started from in
    place of ``--run-id``. The :term:`run ID` is derived from the workplan
    ``name`` exactly as on the first run, and the file is used only to identify
    that run: it must be unchanged since the latest attempt that read it, whose
    :ref:`carried copy <workplan_carried_inputs>` it is compared with (edits,
    and blueprint schema migrations applied on load, both count), otherwise the
    command refuses and points you at ``--run-id``. ``--var`` and ``--varfile``
    cannot be combined with ``--resume``; the run continues with the variables
    it was started with.

    .. code-block:: console

        cstar workplan run my_workplan.yaml --resume


   .. tab-item:: Programmatic Execution

    Use the ``dag_runner`` module to execute a Workplan in python.

    .. code-block:: python

        from pathlib import Path
        from cstar.orchestration.dag_runner import build_and_run_dag

        path = Path("/path/to/my/workplan.yaml")
        await run_workplan(path, run_id="my-unique-id")


.. _workplan_rerun:

Re-running a workplan
^^^^^^^^^^^^^^^^^^^^^

Running a workplan under a :term:`run ID` that already exists re-enters that run. By default the run ID is the workplan ``name``, so running the same file again re-enters its previous run. Each step is handled according to its recorded state:

- A step that is done is reused as it is, unless you pass ``--clobber <step-name|all>``, which clears it and runs it again from scratch.
- A step that is done is not re-run under a changed definition: if its blueprint file, its ``blueprint_overrides``, its ``directives``, its dependencies or its application changed since the step ran, the re-entry is refused, and the message names the steps and the ``--clobber <step-name>`` to pass. Pass ``--run-id <other-id>`` to run the changed workplan separately instead. A step whose producer changed through ``from_step`` is named too. Runtime ``--var`` values and ``compute_overrides`` / ``workflow_overrides`` (resources, clobber, resume) are not part of a step's definition, so changing them re-enters the run without a refusal. A run recorded by an earlier release has no definition hashes in its :ref:`run manifest <workplan_run_manifest>` and is not checked.
- A step that failed is run again from scratch, unless you pass ``--resume``, which continues it in place from its last usable restart when its application supports that.
- A step that is still in progress is adopted when the launcher that started it matches the current launcher; otherwise the run is refused unless that step is clobbered.
- A workplan with the same ``name`` as an earlier run but different content than the workplan that run started from is refused, because the two would share one run directory; the same workplan arriving by another path (a remote file, a moved checkout) is not. Pass ``--run-id <other-id>`` to run it separately, or rename the workplan.
- Each re-entry appends an attempt to the :ref:`run manifest <workplan_run_manifest>`, which records when it started, on which system, with which package versions, and which steps it clobbered or resumed.
- Each attempt that reads the workplan file leaves its own directory of :ref:`carried inputs <workplan_carried_inputs>` (the workplan as given, its variables, the plan it prepared and each step's blueprint) under ``attempts/`` in the run directory, so earlier attempts are never overwritten. Runs scheduled by earlier releases keep their ``<stem>_original.bak``, ``<stem>.vars`` and rotated ``.1``, ``.2`` copies; new attempts no longer write them.
- Step directories and state files that no step of the workplan accounts for any more (for example after a step was renamed) are listed in a warning and left in place; remove them by hand once you have confirmed they are not needed.
- The transformed workplan saved in the run directory (``<stem>_transformed.yaml``) keeps the workplan ``name`` and is the plan the run executes. Passing it to ``cstar workplan run`` re-enters its run exactly as ``--run-id <id>`` does; a copy of it anywhere else is refused, because only the plan a run's record points at can be re-entered.

.. _workplan_pre_run:

Pre-running a workplan
^^^^^^^^^^^^^^^^^^^^^^

``--pre-run`` performs every stage before the model launch for each step --
staging inputs, cloning and compiling the codebases, partitioning inputs
(when ParallelIO is not in use) and generating and validating the namelist --
and then exits successfully without launching ROMS. It catches start-up
problems (missing or malformed inputs, compile failures, namelist errors)
without waiting in a scheduler queue, and moves cloning and compilation out
of the eventual allocation. Unlike ``cstar workplan check``, it is not free of
side effects: it writes each step's full working directory, which can hold
many GB of staged inputs.

.. code-block:: console

    cstar workplan run my_workplan.yaml --pre-run
    cstar workplan run my_workplan.yaml

The second command uses the same :term:`run ID` -- derived from the workplan
``name`` as usual, or pass the same ``--run-id`` to both. The orchestrator
records on each step that it was prepared; on the second command it
re-launches those steps, which attach to the prepared directories, reuse the
staged inputs, compiled executable and partitions, and launch the model
inside the scheduler job.

The same :term:`run ID` also works after a failed real run. If a scheduled
run fails, fix the cause and run the workplan again with ``--pre-run``: the
failed steps are cleared and prepared from scratch on the login node, and the
following plain run attaches to them as above.

During the pre-run every step runs locally, even on a system with a job
scheduler, so the command is suitable for a login node. Steps that cannot be
prepared are skipped and reported with the reason:

- the step's application does not support pre-run (for example ``forge`` or
  ``hello_world``);
- the step's blueprint is produced by another step (``blueprint: {from_step:
  ...}``);
- one of the step's directives (``continue-from``, ``nest-from`` or
  ``carbonate-sensitivity-from``) takes its input from another step's output.

Every step downstream of a skipped step is skipped as well, since a
``depends_on`` edge may carry data the planner cannot see. Skipped steps run
from scratch on the second command, exactly as they would without a pre-run.

``--pre-run`` cannot be combined with ``--resume``. Running ``--pre-run``
again on the same :term:`run ID` leaves already-prepared steps as they are;
to prepare from scratch, add ``--clobber all`` (or ``--clobber <step-name>``).
A pre-run is a property of the whole run, so a hand-written ``pre_run`` key
must appear on every step or on none; use ``--pre-run`` instead.
The ``pre_run`` key is recorded in the transformed workplan, so re-entering the
run with ``--run-id`` re-enters it as a pre-run.

When a step uses ParallelIO (``use_pio``), the pre-run only validates its
inputs, so preparation is light. Without it, input partitioning also runs on
the machine performing the pre-run.

ROMS itself is not started, so errors that only surface when ROMS opens its
boundary or tide files, or writes its first restart, are not caught.

Inspecting a Run
----------------

Once a workplan has been run, the ``cstar run`` commands operate on the run: they take the :term:`run ID` you gave to ``--run-id`` (or the one derived from the workplan name, shown by ``cstar run ls``). ``show``, ``check``, ``log``, ``path`` and ``cd`` also take ``run://<uuid>`` or the path of a run directory (see :ref:`workplan_external_runs`). ``status`` and ``gather`` take only the run ID of a run tracked on this machine: ``status`` reads the run's live state, and ``gather`` writes into the run's directory, which a copy or a run in the project area may not be yours to change. A run :ref:`attached <workplan_attach_run>` from elsewhere counts as tracked; one attached read-only is read but never written into, so ``gather`` refuses it. ``cstar workplan run`` still launches a run, and ``cstar workplan check`` still validates a workplan file.

.. note::

   These commands are also available as ``cstar workplan status``, ``log``, ``gather``, ``path``, ``cd``, ``ls`` and ``list`` (or ``cstar wp ...``), where they lived before ``cstar run`` existed. The two spellings run the same command.

Checking Workplan Status
^^^^^^^^^^^^^^^^^^^^^^^^

.. tab-set::

   .. tab-item:: CLI Status Check

    Use the ``status`` command from the ``cstar`` CLI to retrieve the current status of steps in a running workplan.

    .. code-block:: console

        cstar run status <my-unique-id>

    ``cstar workplan status <my-unique-id>`` is equivalent. ``status`` takes the run ID of a run tracked on this machine; to see any other run, use ``cstar run show``. For a run attached from elsewhere, ``status`` shows what its manifest and step records say, since it has no scheduler state here.

    ``status`` also refreshes the run's :ref:`manifest <workplan_run_manifest>`: it folds the step statuses and records into it, and closes the latest attempt once every step is terminal. Pass ``--no-refresh`` to read the run as last recorded without writing.

.. _workplan_show_run:

Showing a Run
^^^^^^^^^^^^^

Use the ``show`` command to see what a run was and what it produced: each attempt to run the workplan, each step's status and outcome, and what the run depends on outside itself. Give it a :term:`run ID`, ``run://<uuid>``, or the path of any directory that holds a run manifest, such as a copy of a run made on another machine:

.. code-block:: console

    cstar run show <my-unique-id>
    cstar run show run://<uuid>
    cstar run show /path/to/run/directory

The ``Jobs`` table shows each step's job and how long it waited and ran, derived from its submission and its step record (see :ref:`workplan_execution_facts`). A dash means a time is missing, or the record is from an earlier run of the step.

The ``Id`` row of the first table is the run's uuid. For a run tracked on this machine, named by its run ID or uuid, ``show`` refreshes the manifest first, as ``status`` does; pass ``--no-refresh`` to show it as last recorded. A directory, and a run found only by its manifest in your data home or the project runs area, are read as they are. Step records written since the manifest was last refreshed are shown, and nothing is written, so the command is safe on a run that is not yours. It does not need the run to be tracked on the machine you run it on.

Pick the output with ``--format``: ``table`` (the default), ``yaml`` or ``json``. The last two print the manifest itself, ready to pipe into another tool:

.. code-block:: console

    cstar run show <my-unique-id> --format json

The exit code is 0 when the run is shown, 1 when it has no readable manifest and 2 for bad arguments.

.. _workplan_attach_run:

Attaching a run that arrived from elsewhere
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A run copied, rsynced or transferred with Globus from another machine, or from another data home, is read through its directory but is not tracked here (see :ref:`workplan_external_runs`). Neither is a run whose tracking record was lost. Use ``attach`` to register it:

.. code-block:: console

    cstar run attach /path/to/copy/of/the/run

``attach`` reads the run's manifest, step records and transformed workplan, and writes one tracking record in your state home. It writes nothing into the run directory.

It first checks the run as ``cstar run check`` does (see :ref:`workplan_check_run`). A product that the step records list and that is missing, or whose size differs, shows that the transfer is incomplete: the run is not attached and the command exits with code 1. Finish the transfer and run it again. Any other finding is listed and the run is attached all the same, also with exit code 1.

Once attached, the run is known here: ``status``, ``show``, ``gather``, ``ls``, ``path``, a bare run ID and ``run://<uuid>`` all find it, and a workplan can take its finished steps through a ``runs`` alias (see :ref:`workplan_external_runs`). The run has no scheduler state on this machine, so ``status`` shows what its manifest and step records say, and ``show`` and ``check`` read its directory as it is.

The run is attached under the run ID its manifest names. If that run ID is already tracked here for the same run (the same uuid) in another directory, the run is re-attached and the record now names this directory. If it is tracked for another run, ``attach`` refuses; pass ``--as <run-id>`` to attach the run under a free one. ``run://<uuid>`` finds the run either way.

An attached run is **owned** or **read-only**, which decides whether a command may write into its directory. It is read-only by default when its directory is in the project runs area or belongs to another user, and owned otherwise. Pass ``--own`` or ``--read-only`` to choose; on a run that is already attached, they change the mode and nothing else. A run attached read-only refuses ``cstar run gather``, ``cstar run check --repair``, ``cstar run check --record-digests`` and ``cstar admin clean --run-id``. The last case removes the run's tracking record and leaves its directory (the project runs area is left alone whatever the mode).

An attached run is never relaunched: ``--run-id`` and ``--resume`` refuse it. To continue from it, start a new run that references it (see :ref:`workplan_extending_run`).

Pass ``--verify`` to also compare the content of each product that records a digest (see :ref:`workplan_check_run`), and ``--dry-run`` to see what ``attach`` would do without writing the record. The exit code is 0 when the run is attached or already was, 1 when problems were found or the transfer is incomplete, and 2 for bad arguments, a run ID that is taken or a directory that is not a workplan run.

.. _workplan_promote_run:

Promoting a run
^^^^^^^^^^^^^^^

A run in your data home lives on scratch, which is purged. Use ``promote`` to place a finished run in the project runs area (``cstar/runs`` under ``CSTAR_PROJECT_HOME``; see :doc:`hpc`), where it outlasts your scratch space and your collaborators find it:

.. code-block:: console

    cstar run promote <my-unique-id>
    cstar run promote <my-unique-id> --dry-run

The run is named by its run ID, by ``run://<uuid>`` or by its directory. The copy goes to ``<project runs area>/<run-id>``; pass ``--to <dir>`` to put it at ``<dir>/<run-id>`` instead. Without a project runs area and without ``--to``, ``promote`` stops with an error.

What is copied is a plan read from the run's manifest, not the whole directory: the manifest, the transformed workplan, the ``attempts/`` directories, each step's record, logs and the small files in its ``work/`` directory, and every product the step records list. The layout is kept, so every path in the manifest stays valid, and nothing in the files is rewritten. The large parts of a ROMS step are left behind unless you ask for them: ``--include builds`` adds each step's compile-time code, runtime code and cloned codebases, ``--include temp-output`` the per-rank output pieces, and ``--include all`` the whole directory. ``--dry-run`` prints the plan, with the number of files and the size of each group, and copies nothing.

``promote`` never overwrites. A destination that holds another run (a manifest with another uuid) is refused, and so is one that already holds this run. A product the step records list that is missing from the source, a run directory holding more than one transformed workplan, and a destination file system without room for the plan are refused before anything is copied: ``cstar run check`` the run first. Symlinks are copied as links: a link inside the run stays relative, and a link that leaves the run is written absolute so it still names the file where it is.

After the copy, the destination is checked as ``cstar run check`` does (see :ref:`workplan_check_run`), and its ``gathered_output`` is rebuilt when the source has one. Absolute paths inside the copied files are the source's (the transformed workplan records the directory it was written in, which C-Star rebases when it loads it) and do not fail the promotion; a product that is missing or whose size changed, a carried input that differs, a step directory that is missing or a dangling link does, with exit code 1. Pass ``--verify`` to compare the content of every product too: the digest of each product that has none is computed from the source, recorded in the copy's step records and manifest, and the copy is compared against every recorded digest (see :ref:`workplan_check_run` for what a digest reads). The source is never written into.

``--mode`` chooses what promote does; the default is ``copy``:

- ``copy``: a copy of the plan's files. The run in your data home is untouched and stays tracked here; the copy in the project runs area is found by its run ID, by its uuid or by its directory (see :ref:`workplan_external_runs`), and ``cstar run ls --shared`` lists it as ``project``.
- ``link``: no copy; ``<project runs area>/<run-id>`` becomes a symlink to the run where it is, which registers the run with the project without spending the space. ``cstar run ls --shared`` lists it as ``project, linked``: it is as durable as its target.
- ``materialize``: replaces that link with a copy under the same uuid. The run the link pointed at is left in place; delete it yourself, or promote with ``--mode move`` to begin with.
- ``move``: a copy of the whole directory, whatever ``--include`` says, after which the source directory is removed, once the destination passed the check; a move loses nothing. A run tracked here is then tracked as :ref:`attached <workplan_attach_run>`: it is inspected and referenced, never relaunched, and it is read-only when the destination is in the project runs area (``cstar run attach <dir> --own`` takes ownership). ``move`` refuses a run that is attached read-only, a run that is not tracked here and was found by its run ID or uuid (pass its directory to move it deliberately) and a run whose latest attempt has not finished; the other modes warn about an unfinished attempt and go ahead.

A copy or a link carries the file permissions of the source. The project runs area is shared, so set your umask, or the group and permissions of the area, so that your collaborators can read what you promote; C-Star changes no permissions.

The exit code is 0 when the run was promoted, 1 when it was promoted but the copy has problems of its own (or the source misses products and nothing was copied), and 2 for bad arguments, a destination that is taken and a move that is refused.

.. _workplan_run_manifest:

The run manifest
^^^^^^^^^^^^^^^^

Every run records itself in two kinds of file.

``cstar-run.yaml`` is the **run manifest**, in the run directory. It holds:

- an ``id``, the run's uuid, minted once when the run directory is first scheduled, with the :term:`run ID`, the workplan name, whether the run is a ``workplan`` or a ``blueprint``, and the system it was first scheduled on;
- the workplan's name, content hash and path;
- one :term:`attempt <Attempt>` for each time the run was scheduled (``cstar workplan run``, a re-entry with ``--run-id``, ``--resume`` or ``--clobber``), with its start time, system, package versions (``cstar-ocean`` and ``roms-tools``), the steps it clobbered or resumed, its status and when it finished, and, when it carried inputs, their directory (``directory``) and a sha256 digest of each file (``carried``);
- one entry per step: its name, application, working directory relative to the run directory, a definition hash of what the step computes, its blueprint, the copy of its blueprint file the latest attempt carried (``carried_blueprint``), the launcher, job id and time of its latest submission (``launcher``, ``job_id``, ``submitted_at``), its status as of the time the manifest was last refreshed, and its step record once it has one;
- what the run depends on outside itself: referenced runs (with the uuid each resolved to), source data locations, codebases with their commit or branch, and the catalog entries and producer blueprints named in the blueprints' provenance.

``cstar-step.yaml`` is the :term:`step record`, in each step's working directory (``tasks/<step>/``). The step process writes it when it shuts down: the outcome (``completed``, ``failed``, ``cancelled`` or ``unsubmitted``), start and finish times, the system, package versions, what ran the step (``execution``), the blueprint file the step ran relative to its working directory (``blueprint_file``, for example ``work/<name>.ovrd.yaml`` once its directives were applied), the :term:`products <Product>` the step left and any errors. ``execution`` holds the scheduler (``slurm`` or ``local``), the host and the process id of the step; inside a SLURM job it also holds the job id and the allocation the job was granted: its nodes, tasks, CPUs on the step's node, partition and account. These are what was granted, read from the step's own environment, not what was requested: the request is in the transformed workplan. They describe the step's own process: a model run the step submits as a job of its own, as ROMS is when its step runs on a login node outside an allocation, is not described. The products are the files a user keeps, each with its size in bytes and, once recorded (see :ref:`workplan_check_run`), a content digest: for a ROMS step, its joined outputs by stream, its namelist, cppdefs and MARBL configuration, its executable and its log; for a forge step, its generated inputs, published blueprint, rendered configuration and log; for a ``cdr_metrics`` step, its metrics series and figures. The size ``cstar run ls`` shows for each step (``step_sizes``) is a separate measurement of the whole directory, not derived from the products.

The scheduling command writes the manifest, under a lock, so concurrent commands do not overwrite each other. It also records each step's submission as it makes it. The steps write their own records and never touch the manifest. Keeping these records never stops a run: a step that cannot write its record, or a command that cannot update the manifest (or waits more than a minute for its lock), logs a warning and carries on. ``cstar run status``, ``cstar run show`` of a run tracked on this machine and ``cstar workplan run --resume`` refresh the manifest: they fold the step statuses and records into it, and once every step is terminal they set ``finished_at`` on the latest attempt. That field is the completion marker. A run that finishes while nobody is watching is closed by the next ``status`` or ``show``.

Because the manifest records working directories relative to the run directory, a copied or moved run stays readable with ``cstar run show <directory>``. A standalone ``cstar blueprint run`` writes the same two files in its working directory, with ``kind: blueprint`` and one step whose working directory is ``.``.

Runs scheduled with an earlier C-Star release have no manifest until they are re-entered: ``cstar workplan run --run-id <my-unique-id>`` records one, and steps that are done are reused.

The files are described by published schemas, listed in the :doc:`schema reference <schemas/index>`.

.. _workplan_execution_facts:

Execution facts
"""""""""""""""

The manifest records each step's latest submission (``launcher``, ``job_id``, ``submitted_at``), and the step record says what ran the step (``execution``). ``cstar run show`` derives two times from these timestamps: the time the step waited, from its submission to the step process starting, and its wall time, from that start to the recorded outcome. Neither is stored. The wait includes the time a step was held for the steps it depends on, since a dependent step is submitted as soon as they are; SLURM's ``Eligible`` time separates the two (see below). ``show`` leaves both times out when the step's record is from an earlier run than its latest submission: it started before that submission, or it names another SLURM job.

The manifest keeps only the latest submission of each step, and ``launcher`` says whether ``job_id`` is a SLURM job id (``slurm``) or the process id of the step's launch script (``local``). A step that an attempt clobbers or resumes loses its earlier submission.

Anything else is SLURM accounting's to keep: an earlier submission, CPU time, the memory high-water mark, and the exit state of a job that was killed before its step could write a record. Query it by job id while your site retains it:

.. code-block:: console

    sacct -j <job-id> --format=JobID,Submit,Eligible,Start,End,Elapsed,State,MaxRSS

A standalone ``cstar blueprint run`` has no submission. Its record still says what ran it, and inside an ``salloc`` that is the allocation.

.. _workplan_carried_inputs:

Carried inputs
""""""""""""""

Each attempt that reads the workplan file (``cstar workplan run <workplan>``, also when it re-enters a run) leaves what it was asked in the run directory, under ``attempts/<YYYYMMDDTHHMMSS>/``, named after the attempt's start in UTC (``-2``, ``-3`` and so on when two attempts start in the same second):

- ``workplan.yaml``: the workplan file exactly as given;
- ``workplan.vars``: the runtime variables, when any were given;
- ``workplan_transformed.yaml``: a copy of the plan the attempt prepared;
- ``blueprints/<step>/<file>``: each step's blueprint file, downloaded first when it is remote. A deferred or inline blueprint has no file at that point and is not carried; an inline one is written into its step's directory.

The run manifest records the directory on the attempt with a sha256 digest of each file, and points each step at the copy of its blueprint (``carried_blueprint``). A re-entry by ``--run-id <my-unique-id>`` alone, or with ``--resume``, replays the recorded plan and carries nothing. The plan a run executes stays in the run directory as ``<stem>_transformed.yaml``, where ``<stem>`` is the workplan file's name without its extension; each attempt overwrites it.

Only the latest attempt that carried inputs is ever read: ``--resume <workplan>``, and a re-run under a run ID derived from the workplan ``name``, compare the workplan you give with its ``workplan.yaml``. If the run's manifest cannot be read, both are refused. Older attempt directories are a record and can be deleted by hand; ``cstar run check`` lists a missing one for information only. If the latest one is gone too, ``--resume <workplan>`` warns and resumes the run as recorded, while a re-run under a derived run ID is refused until you pass ``--run-id``.


Listing Runs
^^^^^^^^^^^^

Use the ``ls`` command (also spelled ``list``) to list the runs started by a user, with their workplan, disk usage and start time.

.. code-block:: console

    cstar run ls

``cstar workplan ls`` is equivalent.

``cstar run ls`` lists the runs tracked on this machine. Two flags also read the run manifests in your data home and in the project runs area (``cstar/runs`` under ``CSTAR_PROJECT_HOME``; see :doc:`hpc`), and add a ``where`` column to the table (a ``where`` field to the CSV and JSON output):

.. code-block:: console

    cstar run ls --shared     # the runs in the project runs area only
    cstar run ls --all        # the runs tracked here and every other run found there

``--shared`` needs ``CSTAR_PROJECT_HOME`` (which defaults to ``$PROJECT``) and stops with an error without it. Given together, the two flags list what ``--all`` lists. The ``where`` column says:

- ``local``: a run tracked on this machine.
- ``local, untracked``: a run in your data home that this machine does not track.
- ``project``: a run in the project runs area.
- ``project, linked``: a link in the project runs area to a run kept elsewhere.
- ``attached``: a run :ref:`attached <workplan_attach_run>` from elsewhere. ``attached, read-only`` is one that nothing may write into.

The disk space of a run that was not started here is not measured, and the table shows ``-``. With ``--all``, a copy in the project runs area of a run tracked here is not listed again.


Reading a Step's Log
^^^^^^^^^^^^^^^^^^^^

Use the ``log`` command to print the log of one step of a run. Add ``--monitor`` to keep streaming it, like ``tail -f``.

.. code-block:: console

    cstar run log <my-unique-id> <step-name>

``cstar workplan log <my-unique-id> <step-name>`` is equivalent.


.. _workplan_run_cd:

Finding a Run's Directory
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``path`` command from the ``cstar`` CLI to print the directory of a run, or of one of its steps. The path is the only output, so it composes with other commands in any shell. A failed lookup prints nothing and exits non-zero, so guard the ``cd`` with ``&&``:

.. code-block:: console

    cstar run path <my-unique-id>
    dir=$(cstar run path <my-unique-id> <step-name>) && cd "$dir"

A program cannot change the directory of the shell that started it, so ``cstar run cd <my-unique-id> [step-name]`` works only once your shell has a small ``cstar`` function. Install it once, then open a new shell:

.. code-block:: console

    cstar env shell-init --install

The command detects whether you run zsh or bash (pass ``zsh`` or ``bash`` to choose), writes the function under the C-Star config directory, and adds a marked block to ``~/.zshrc`` or ``~/.bashrc`` that sources it. For bash, login shells, such as SSH sessions and the macOS Terminal, read ``~/.bash_profile`` instead, so make sure that file sources ``~/.bashrc``. Run the command again after upgrading C-Star to refresh the function, and add ``--uninstall`` to remove the block and the function. The command stops without changing anything if the block's marker lines have been edited so that they no longer pair up.

To manage your dotfiles yourself, omit ``--install``: the command prints the function instead, for example ``cstar env shell-init zsh > <file>``, ready to source from your own configuration.

``cstar workplan path`` and ``cstar workplan cd`` are equivalent, and the shell function accepts both spellings. Regenerate the function after upgrading so that it knows ``cstar run cd``.


Gathering Workplan Outputs
^^^^^^^^^^^^^^^^^^^^^^^^^^

Every step writes its final outputs to its own ``output`` directory. ROMS steps that run without ParallelIO first write partitioned files to ``temp_output``; C-Star joins those into whole files in ``output`` after the run and removes the partition pieces.

Use the ``gather`` command from the ``cstar`` CLI to consolidate every step's ``output`` files into a single run-level ``gathered_output`` directory of symlinks, inside the run directory. It takes the run ID of a run tracked on this machine; a run attached read-only is refused (see :ref:`workplan_attach_run`). It is safe to re-run while a workplan is still in progress. When two steps produce a file with the same name, each link is prefixed with its step name (``<step>__<file>``) so both remain reachable.

.. code-block:: console

    cstar run gather <my-unique-id>

``cstar workplan gather <my-unique-id>`` is equivalent.

Runs completed with an earlier C-Star release kept joined files in a ``joined_output`` directory next to ``output``. Move such a run to the current layout before continuing from it or gathering it:

.. code-block:: console

    cstar admin migrate-outputs <path-to-run-or-step-directory> --dry-run
    cstar admin migrate-outputs <path-to-run-or-step-directory>


.. _workplan_check_run:

Checking a Run Directory
^^^^^^^^^^^^^^^^^^^^^^^^

A run directory keeps working after it is copied or moved as a unit only while the files that describe it refer to each other by relative path. Use the ``check`` command to look for what does not, in a run (by run ID or ``run://<uuid>``) or in any run directory (by path, a directory that contains ``tasks/``, or one that holds ``cstar-run.yaml`` and no ``tasks/``, a standalone blueprint run):

.. code-block:: console

    cstar run check <my-unique-id>
    cstar run check /path/to/copy/of/the/run

The command reports, kind by kind, with one line per finding:

- absolute paths under the run, or under the C-Star data home, in the files each step emits (``output/*.yaml``, ``blueprints/B_*.yaml``, ``work/cstar_generated_roms.nml``) and in the run's ``*_transformed.yaml``. Pass ``--all`` to scan the launch artifacts too (override and directive files, ``script.sh`` and namelists); launchers regenerate those, so they are skipped by default.
- symlinks with an absolute target inside the run, and symlinks whose target does not exist.
- steps of the transformed workplan with no ``tasks/<slug>`` directory. A step that has not started has none yet.
- products that are missing from the run, or whose size changed, compared with the step records folded into the manifest. A log is reported only when it shrank, since a step's own log is still written after its record is taken. Pass ``--verify`` to compare the content too, where a digest is recorded: for a netCDF file, the structure and a bounded sample of the data, which does not depend on how the file is stored on disk and reads little however large the file is; for any other file, the sha256 of its bytes. A product recorded without a digest is skipped. A record that predates digests and holds a bare sha256 is still compared.
- :ref:`carried inputs <workplan_carried_inputs>` whose content differs from the digest the manifest recorded when the attempt carried them. Only the files the manifest lists are read, always: they are small.

Symlinks with a target outside the run (absolute, or relative and climbing out of it), step directories that no step accounts for, sentinel files of the run that no step accounts for (found for a run tracked on this machine that you name by run ID or uuid, so not for a path), steps that are done without a step record, a latest attempt that has not finished, and an attempt directory or carried file that is missing (older attempts may be deleted by hand) are listed for information and do not fail the check. Nothing is ever deleted.

For a run tracked on this machine, named by its run ID or uuid, the manifest is refreshed first, as ``cstar run show`` does; pass ``--no-refresh`` to read it as last recorded. Any other run is checked as its directory.

The exit code is 0 when there is nothing to fix, 1 when findings remain and 2 for bad arguments. A workplan file is refused: validate it with ``cstar workplan check <file>``.

Pass ``--repair`` to rewrite each absolute symlink that points inside the run as a relative link, and rebuild ``gathered_output`` if the run has one and its transformed workplan describes this directory. ``--repair`` changes nothing else: a file that names absolute paths is regenerated by running the workplan again, and a dangling link is reported, never touched. Because it writes into the run, ``--repair`` refuses a run that is attached read-only, and a run that is not tracked here and was found by its run ID or ``run://<uuid>``, such as a run in the project runs area; pass that run's directory to repair it deliberately.

A digest is recorded by ``--record-digests``, which computes one for each product that has none and writes it into the step records and the run manifest. Run it on the machine that has the run before you transfer it, so that ``cstar run attach --verify`` and ``cstar run check --verify`` can compare the files on arrival; a netCDF file costs a bounded read, any other file is read whole. It writes into the run, so it needs a run you own that is tracked here, or the run's directory passed explicitly, and it refuses a run attached read-only and a run found only by its run ID or ``run://<uuid>``, as ``--repair`` does.


.. toctree::
   :hidden:

   workplans/directives

   tutorials/tutorial_wp
