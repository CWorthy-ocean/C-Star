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
      spinup: "{{spinup_run}}"      # a run-id, or a runtime variable holding one
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

The run-id is the one shown by ``cstar run ls`` (the value given to
``--run-id``, or derived from the workplan name). The rules:

- Every alias used in a token must be declared under ``runs``; an undeclared
  alias is rejected when the workplan is loaded. Because ``@`` separates the
  step from the alias, step names and aliases may not contain it.
- An external step a step reads from must be listed in that step's
  ``depends_on`` (directly or through an earlier step), exactly as a local
  step must be. ``cstar workplan check`` and ``cstar workplan run`` report
  every missing dependency.
- When the workplan is scheduled, each external step is looked up in the
  other run's records on this machine: the run must exist, the step must
  exist in it, and the step must have **completed** (``Done``). A step that
  is still running is accepted only when both runs use the SLURM launcher;
  the new step is then submitted with a SLURM dependency on the other run's
  job. A step that failed, was cancelled, was never submitted, or is running
  under the local launcher is refused with a message naming the run, step
  and status. If a status looks stale (the other run finished but its
  orchestrator was not polled since), ``cstar run status <run-id>``
  refreshes it.
- The transformed workplan written into the run directory records, for each
  alias, the exact run record that was resolved (its start time), so the
  run's inputs stay traceable even if the same run-id is used again later.

A run-id resolves only on the machine, and under the same ``CSTAR_STATE_HOME``,
where that run was executed.

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
to another machine, has valid internal paths but is not reachable by run-id
until it is registered there, which a later release adds.
``cstar workplan run --resume`` re-exports the environment captured when the
run started, except ``CSTAR_DATA_HOME`` and ``CSTAR_STATE_HOME``, and warns
about every other captured setting that differs from your shell. A transformed
workplan written by an earlier release records no root and is loaded as it is,
so it stays pinned to the directory it was written in.


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
        - ``filename`` is optional. When omitted, exactly one blueprint file
          must be present in the producing step's output directory.
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


Computing CDR metrics from segmented runs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``cdr_metrics`` application computes carbon dioxide removal (CDR) metrics
from the ``_cdr`` output of ROMS-MARBL runs, stitching the segments of a run that
was split into several steps. Each source is a directory searched recursively for
``_cdr`` output (or a single ``_cdr`` file), and the sources must cover the run
from the release onward: the first holds the segment containing the release,
because the cumulative sums start at the first record.

.. code:: yaml

    name: cdr-metrics-workplan
    description: CDR metrics over two consecutive segments
    state: draft

    steps:
    - name: seg-01
      application: roms_marbl
      blueprint: /path/to/segment_01.yaml

    - name: seg-02
      application: roms_marbl
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
cumulative sums. The step writes ``output/cdr_metrics.nc``, a time series of the
CDR efficiencies and the uptake in tonnes of CO2, and ``output/cdr_metrics.png``,
a plot of them.

The producing runs must be MARBL runs with a CDR forcing; forge turns on
``cdr_output`` for an active MARBL CDR forcing. ``domains`` maps a domain name to its
grid and sources. A domain whose ``_cdr`` output lacks a variable the metrics need is
reported with the ROMS setting that writes it.

.. note::

   With ucla-roms 0.10.1 and 0.11.0, the averaged air-sea CO2 flux diagnostics
   (``FG_CO2``, ``FG_ALT_CO2``) written after a ``continue-from`` restart differ from
   those of the same run without a restart, although the model state is identical. The
   flux-based metrics of a segmented run can therefore differ from the unsegmented
   run's; the metrics from DIC differences match it exactly.

Nested (upscaled) experiments
"""""""""""""""""""""""""""""

In a nested experiment the release takes place in a child domain and the perturbation
is handed on to one or more parent domains with the ``upscaler`` application. A parent's ``ALK_source`` and ``DIC_source`` are then
a *transfer* of the same alkalinity, not a new release. List every domain in
``domains``, child first, and mark each parent with ``source_role: transfer``:

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
alkalinity is not counted twice. ``output/cdr_metrics.nc`` then also holds each domain's
uptake series and two diagnostics. ``closure_residual`` is the combined flux-based minus
DIC-difference-based uptake; the two are independent views of the same perturbation, so a
residual small against the uptake is the sign that the domains were combined
consistently. ``<parent>_DIC_difference_inside_<child>`` is the DIC perturbation a parent
carries in its cells inside the child's footprint. It is normally nonzero: the upscaler
re-injects what leaves the child along the child's boundary, which lies inside that
footprint, and the child's own inventory no longer holds it. ``output/cdr_domains.png``
maps the domains. A parent must be a MARBL run writing ``_cdr`` output; a CDR-lite parent
writes ``_cdrtrc`` output, which the metrics cannot use.


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
    that run: it must be unchanged since (edits, and blueprint schema
    migrations applied on load, both count), otherwise the command refuses and
    points you at ``--run-id``. ``--var`` and ``--varfile`` cannot be combined
    with ``--resume``; the run continues with the variables it was started with.

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
- Each attempt keeps the files of the attempts before it: the original workplan, the transformed workplan and the runtime variables saved in the run directory (also when a later attempt saves none) are rotated to ``<stem>_original.bak.1``, ``<stem>_transformed.yaml.1``, ``<stem>.vars.1`` and so on, where ``<stem>`` is the workplan file's name without its extension, and nothing reads the rotated copies.
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

Once a workplan has been run, the ``cstar run`` commands operate on the run: they take the :term:`run ID` you gave to ``--run-id`` (or the one derived from the workplan name, shown by ``cstar run ls``). ``cstar workplan run`` still launches a run, and ``cstar workplan check`` still validates a workplan file.

.. note::

   These commands are also available as ``cstar workplan status``, ``log``, ``gather``, ``path``, ``cd``, ``ls`` and ``list`` (or ``cstar wp ...``), where they lived before ``cstar run`` existed. The two spellings run the same command.

Checking Workplan Status
^^^^^^^^^^^^^^^^^^^^^^^^

.. tab-set::

   .. tab-item:: CLI Status Check

    Use the ``status`` command from the ``cstar`` CLI to retrieve the current status of steps in a running workplan.

    .. code-block:: console

        cstar run status <my-unique-id>

    ``cstar workplan status <my-unique-id>`` is equivalent.

    ``status`` also refreshes the run's :ref:`manifest <workplan_run_manifest>`: it folds the step statuses and records into it, and closes the latest attempt once every step is terminal. Pass ``--no-refresh`` to read the run as last recorded without writing.

.. _workplan_show_run:

Showing a Run
^^^^^^^^^^^^^

Use the ``show`` command to see what a run was and what it produced: each attempt to run the workplan, each step's status and outcome, and what the run depends on outside itself. Give it a :term:`run ID`, or the path of any directory that holds a run manifest, such as a copy of a run made on another machine:

.. code-block:: console

    cstar run show <my-unique-id>
    cstar run show /path/to/run/directory

For a run ID, ``show`` refreshes the manifest first, as ``status`` does; pass ``--no-refresh`` to show it as last recorded. A directory is read as it is. Step records written since the manifest was last refreshed are shown, and nothing is written, so the command is safe on a run that is not yours. It does not need the run to be tracked on the machine you run it on.

Pick the output with ``--format``: ``table`` (the default), ``yaml`` or ``json``. The last two print the manifest itself, ready to pipe into another tool:

.. code-block:: console

    cstar run show <my-unique-id> --format json

The exit code is 0 when the run is shown, 1 when it has no readable manifest and 2 for bad arguments.

.. _workplan_run_manifest:

The run manifest
^^^^^^^^^^^^^^^^

Every run records itself in two kinds of file.

``cstar-run.yaml`` is the **run manifest**, in the run directory. It holds:

- an ``id``, minted once when the run directory is first scheduled, with the :term:`run ID`, the workplan name, whether the run is a ``workplan`` or a ``blueprint``, and the system it was first scheduled on;
- the workplan's name, content hash and path;
- one :term:`attempt <Attempt>` for each time the run was scheduled (``cstar workplan run``, a re-entry with ``--run-id``, ``--resume`` or ``--clobber``), with its start time, system, package versions (``cstar-ocean`` and ``roms-tools``), the steps it clobbered or resumed, its status and when it finished;
- one entry per step: its name, application, working directory relative to the run directory, a definition hash of what the step computes, its blueprint, its status as of the time the manifest was last refreshed, and its step record once it has one;
- what the run depends on outside itself: referenced runs, source data locations, codebases with their commit or branch, and the catalog entries and producer blueprints named in the blueprints' provenance.

``cstar-step.yaml`` is the :term:`step record`, in each step's working directory (``tasks/<step>/``). The step process writes it when it shuts down: the outcome (``completed``, ``failed``, ``cancelled`` or ``unsubmitted``), start and finish times, the system, package versions, the :term:`products <Product>` the step left and any errors. The products are the files a user keeps, each with its size in bytes: for a ROMS step, its joined outputs by stream, its namelist, cppdefs and MARBL configuration, its executable and its log; for a forge step, its generated inputs, published blueprint, rendered configuration and log. The size ``cstar run ls`` shows for each step (``step_sizes``) is a separate measurement of the whole directory, not derived from the products.

The scheduling command writes the manifest, under a lock, so concurrent commands do not overwrite each other. The steps write their own records and never touch the manifest. ``cstar run status``, ``cstar run show <run-id>`` and ``cstar workplan run --resume`` refresh the manifest: they fold the step statuses and records into it, and once every step is terminal they set ``finished_at`` on the latest attempt. That field is the completion marker. A run that finishes while nobody is watching is closed by the next ``status`` or ``show``.

Because the manifest records working directories relative to the run directory, a copied or moved run stays readable with ``cstar run show <directory>``. A standalone ``cstar blueprint run`` writes the same two files in its working directory, with ``kind: blueprint`` and one step whose working directory is ``.``.

Runs scheduled with an earlier C-Star release have no manifest until they are re-entered: ``cstar workplan run --run-id <my-unique-id>`` records one, and steps that are done are reused.

The files are described by published schemas, listed in the :doc:`schema reference <schemas/index>`.


Listing Runs
^^^^^^^^^^^^

Use the ``ls`` command (also spelled ``list``) to list the runs started by a user, with their workplan, disk usage and start time.

.. code-block:: console

    cstar run ls

``cstar workplan ls`` is equivalent.


Reading a Step's Log
^^^^^^^^^^^^^^^^^^^^

Use the ``log`` command to print the log of one step of a run. Add ``--monitor`` to keep streaming it, like ``tail -f``.

.. code-block:: console

    cstar run log <my-unique-id> <step-name>

``cstar workplan log <my-unique-id> <step-name>`` is equivalent.


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

Use the ``gather`` command from the ``cstar`` CLI to consolidate every step's ``output`` files into a single run-level ``gathered_output`` directory of symlinks. It is safe to re-run while a workplan is still in progress. When two steps produce a file with the same name, each link is prefixed with its step name (``<step>__<file>``) so both remain reachable.

.. code-block:: console

    cstar run gather <my-unique-id>

``cstar workplan gather <my-unique-id>`` is equivalent.

Runs completed with an earlier C-Star release kept joined files in a ``joined_output`` directory next to ``output``. Move such a run to the current layout before continuing from it or gathering it:

.. code-block:: console

    cstar admin migrate-outputs <path-to-run-or-step-directory> --dry-run
    cstar admin migrate-outputs <path-to-run-or-step-directory>


Checking a Run Directory
^^^^^^^^^^^^^^^^^^^^^^^^

A run directory keeps working after it is copied or moved as a unit only while the files that describe it refer to each other by relative path. Use the ``check`` command to look for what does not, in a tracked run (by run ID) or in any run directory (by path, a directory that contains ``tasks/``, or one that holds ``cstar-run.yaml`` and no ``tasks/``, a standalone blueprint run):

.. code-block:: console

    cstar run check <my-unique-id>
    cstar run check /path/to/copy/of/the/run

The command reports, kind by kind, with one line per finding:

- absolute paths under the run, or under the C-Star data home, in the files each step emits (``output/*.yaml``, ``blueprints/B_*.yaml``, ``work/cstar_generated_roms.nml``) and in the run's ``*_transformed.yaml``. Pass ``--all`` to scan the launch artifacts too (override and directive files, ``script.sh`` and namelists); launchers regenerate those, so they are skipped by default.
- symlinks with an absolute target inside the run, and symlinks whose target does not exist.
- steps of the transformed workplan with no ``tasks/<slug>`` directory. A step that has not started has none yet.
- products that are missing from the run, or whose size changed, compared with the step records folded into the manifest. A log is reported only when it shrank, since a step's own log is still written after its record is taken. Pass ``--verify`` to compare the content too, where a sha256 is recorded (none is yet).

Symlinks with a target outside the run (absolute, or relative and climbing out of it), step directories that no step accounts for, sentinel files of the run that no step accounts for (found by run ID, so not for a bare path), steps that are done without a step record, and a latest attempt that has not finished are listed for information and do not fail the check. Nothing is ever deleted.

For a run ID the manifest is refreshed first, as ``cstar run show`` does; pass ``--no-refresh`` to read it as last recorded.

The exit code is 0 when there is nothing to fix, 1 when findings remain and 2 for bad arguments. A workplan file is refused: validate it with ``cstar workplan check <file>``.

Pass ``--repair`` to rewrite each absolute symlink that points inside the run as a relative link, and rebuild ``gathered_output`` if the run has one and its transformed workplan describes this directory. ``--repair`` changes nothing else: a file that names absolute paths is regenerated by running the workplan again, and a dangling link is reported, never touched.


.. toctree::
   :hidden:

   workplans/directives

   tutorials/tutorial_wp
