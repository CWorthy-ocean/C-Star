.. _workplans:

Workplans
=========

A workplan is a YAML file that lists the things to run as **steps**, says
which steps wait for which, and lets steps pass files and settings to one
another. You submit the whole thing once, and C-Star schedules the steps,
submits them to the job scheduler on a cluster or runs them as processes on
a laptop, and keeps track of how they are doing under a single **run ID**.
Any multi-step sequence might be a workplan: a Forge step feeding a simulation,
a domain spin-up followed by an experiment and its control, an ensemble that varies one
setting across steps, an analysis of an earlier step's output, or all of the above,
all in sequence.

This page covers the shape of a workplan and the commands that check and
submit one. :doc:`directives` describes how steps hand run-time files to one
another, :doc:`references` describes how a step points at another step's directories
and at earlier runs, and :doc:`variables` describes how a workplan takes values from
the command line. Following a run after it is submitted is covered under
:doc:`../runs/monitoring`.

A first workplan
----------------

The smallest useful workplan runs Forge, then the simulation Forge produced:

.. literalinclude:: ../tutorials/workplan_forge_then_run.yaml
   :language: yaml

The top of the file describes the workplan itself. ``name`` doubles as the
run ID, so it must be a slug (lowercase letters, digits, ``-`` and ``_``).
``description`` is for the next reader. ``state`` is ``draft`` or
``validated``; the validated state is reserved for workplans whose
modification will be restricted to preserve reproducibility, and C-Star does
not enforce that yet, so today the field is informational.

Everything else is the list of ``steps``. A step is one thing to run. It has
a ``name``, the ``application`` that runs it, and the ``blueprint`` that
application gets. ``depends_on`` is the ordering: a step does not start
until every step it lists has finished successfully, a step that depends on
nothing starts at once, and if a step fails, the steps that depend on it are
not run. Steps with no unmet dependencies may run at the same time.

.. important::
    A multi-step workplan without dependencies has no ordering guarantees.
    Steps are scheduled immediately and executed as the launcher permits.

Steps
-----

Each step runs one application on one blueprint. The ``blueprint`` field
takes one of three forms:

A path
   A blueprint file. A relative path is resolved against the directory that
   holds the workplan file, not the directory you run ``cstar`` from, so a
   workplan can be checked and run from anywhere and moved together with the
   blueprints beside it. A URL works too.
A step that will produce it (``from_step``)
   The blueprint does not exist yet when the workplan is submitted, because
   an earlier step writes it: Forge writes the UCLA-ROMS blueprint of the
   domain it generates. ``blueprint: {from_step: make_inputs}`` tells C-Star
   to look in that step's output when this step starts. The producing step
   must be listed in ``depends_on``. ``filename`` is optional: without it the
   blueprint the producing step recorded is used. Because the blueprint
   cannot be read at submit time, a deferred step under SLURM is allocated
   one CPU unless its ``compute_overrides`` say otherwise.
``inline``
   No file at all: the blueprint is built from the step's
   ``blueprint_overrides`` alone, seeded with the step's name, application
   and working directory. This suits the small applications, such as
   ``nest_ic`` and ``cdr_metrics``, whose fields are all set per step. Every
   field the application requires must be supplied, and ``cstar workplan
   check`` reports any that are missing. The resulting blueprint is written
   into the step's directory when the workplan is scheduled, so the run
   records exactly what ran. (A relative blueprint file literally named
   ``inline`` must be written ``./inline``.)


Besides ``name``, ``application``, ``blueprint`` and ``depends_on``, a step
can carry:

``blueprint_overrides``
   Values to change in the blueprint before it runs, in the blueprint's own
   layout. The same blueprint can then serve several steps that differ by
   only a few settings, and a deferred blueprint can be adjusted once it exists.
   Placeholders such as ``{{output_dir: parent}}`` are filled in (see
   :doc:`references`).
``compute_overrides``
   The resources this step asks for; see below.
``directives``
   Run-time instructions that modify the blueprint with files written by
   earlier steps; see :doc:`directives`.
``workflow_overrides``
   How the step is re-entered: ``clobber``, ``resume`` or ``pre_run``. These
   are normally set by the command-line options of ``cstar workplan submit``
   rather than written by hand; see :doc:`../runs/monitoring`.

The model behind a step is :class:`cstar.orchestration.models.Step`, and
behind the file :class:`cstar.orchestration.models.Workplan`.

.. _workplan_examples:

Examples
--------

.. tab-set::

   .. tab-item:: Single step

    The minimum workplan: one simulation, submitted to the cluster.

    .. code:: yaml

        name: simple-workplan
        description: Run a simulation
        state: draft

        steps:
        - name: job1
          application: ucla_roms
          blueprint: ./blueprints/simulation.yaml

   .. tab-item:: Several steps

    Three simulations. ``job2`` waits for ``job1``; ``job3`` depends on
    nothing and starts alongside ``job1``.

    .. code:: yaml

        name: multi-step-workplan
        description: Run multiple UCLA-ROMS simulations
        state: draft

        steps:
        - name: job1
          application: ucla_roms
          blueprint: ./blueprints/simulation_a.yaml
        - name: job2
          application: ucla_roms
          blueprint: ./blueprints/simulation_b.yaml
          depends_on:
          - job1
        - name: job3
          application: ucla_roms
          blueprint: ./blueprints/simulation_c.yaml

   .. tab-item:: Overriding a blueprint

    One blueprint, two steps, each building a different ROMS code. The
    overrides use the blueprint's own layout (see the :ref:`blueprint fields
    <blueprint_schema>`).

    .. code:: yaml

        name: workplan-overriding-a-blueprint
        description: Run a blueprint ensemble varying parameters with overrides
        state: draft

        steps:
        - name: develop
          application: ucla_roms
          blueprint: blueprint.yaml
          blueprint_overrides:
            name: Run blueprint with development UCLA-ROMS branch
            code:
              roms:
                location: https://github.com/CWorthy-ocean/ucla-roms.git
                branch: develop

        - name: fork
          application: ucla_roms
          blueprint: blueprint.yaml
          blueprint_overrides:
            name: Run blueprint with custom UCLA-ROMS fork
            code:
              roms:
                location: https://github.com/github-user/ucla-roms.git
                branch: main

   .. tab-item:: A blueprint from an earlier step

    The second step runs the blueprint that ``make_blueprint`` writes.
    ``filename`` names it when the producing step does not record one, and
    the overrides are applied once the blueprint exists.

    .. code:: yaml

        name: deferred-blueprint-workplan
        description: Run a simulation from a generated blueprint
        state: draft

        steps:
        - name: make_blueprint
          application: forge
          blueprint: ./forge_blueprint.yaml

        - name: run_generated
          application: ucla_roms
          depends_on:
          - make_blueprint
          blueprint:
            from_step: make_blueprint
          compute_overrides:
            slurm:
              num_cpus: 64
          blueprint_overrides:
            runtime_params:
              end_date: "2025-02-01 00:00:00"

   .. tab-item:: An inline blueprint

    A ``nest_ic`` step with no blueprint file: its three fields are given
    under ``blueprint_overrides``, reaching into the ``outer`` step's
    directories with placeholders.

    .. code:: yaml

        name: inline-blueprint-workplan
        description: Convert a restart file to initial conditions for a nested grid
        state: draft

        steps:
        - name: outer
          application: ucla_roms
          blueprint: ./outer_blueprint.yaml

        - name: nest_conversion
          application: nest_ic
          blueprint: inline
          depends_on:
          - outer
          blueprint_overrides:
            parent_grid: "{{input_dir: outer}}/input_datasets/parent_grid.nc"
            parent_rst: "{{output_dir: outer}}/output_rst.20100103000000.nc"
            child_grid: ./child_grid.nc

The tutorials have complete, runnable workplans: :doc:`Forge then a
simulation <../tutorials/workplan>`, :doc:`a spin-up, a CDR release and its
metrics <../tutorials/cdr_workplan>`, and :doc:`a nested run with upscaling
<../tutorials/nesting_upscaling>`.

Checking a workplan
-------------------

.. code-block:: console

   cstar workplan check my_workplan.yaml

This performs the same resolution that submitting does, without submitting
anything: it validates the file's structure, imports each step's
application, loads and validates each blueprint, merges
``blueprint_overrides``, and validates every directive. Unknown directive
keys, malformed configuration, and ``step`` references that do not name an
upstream dependency are reported together. Nothing is written to disk. If
the workplan declares :doc:`runtime variables <variables>`, supply them with
``--var`` or ``--varfile`` exactly as you would when submitting. Pass
``--schema-only`` to validate only the file's structure, for example when
the referenced blueprints are not available on the current machine. To
load and check a workplan from Python, see :doc:`../reference/python`.

Submitting a workplan
---------------------

.. code-block:: console

   cstar workplan submit my_workplan.yaml

The command sets up a directory for the run, schedules every step, prints a
summary with the log file of each step, and returns. On a laptop the steps
run as background processes, each waiting for the steps it depends on; on a
cluster each step becomes a scheduler job with a dependency on the jobs
before it, so the command only submits and you can log out. The run ID is
the workplan's ``name`` unless you pass ``--run-id``:

.. code-block:: console

   cstar workplan submit --run-id <my-unique-id> my_workplan.yaml

The run directory is ``CSTAR_DATA_HOME/workplan_runs/<run-id>/``
(``~/cstar/workplan_runs/<run-id>/`` on a laptop), with one directory per
step under ``tasks/``:

.. code-block:: text

   workplan_runs/<run-id>/
     cstar-run.yaml                       the run manifest
     <workplan>_transformed.yaml          the plan as actually run
     attempts/<start time>/               the workplan and blueprints as given
     tasks/<step>/                        input/, output/, logs/, work/ ... as in a blueprint run

.. attention::
    Submitting on an HPC system needs the SLURM settings the system requires: the **account** and **queue** on Anvil and Bouchet, the **account** on Expanse (see :doc:`../deployments/hpc`). Run ``cstar init`` once on the system to write them to your configuration file. A workplan run with the SLURM launcher checks them before it schedules any step, and stops if one the system requires is neither set in your environment or configuration file nor supplied by the workplan.

A path to a blueprint file can be given instead of a workplan: it is run as
a single-step workplan, the same as ``cstar blueprint submit`` (see
:doc:`../applications/blueprints`).

Submitting the same workplan again does not start a second copy. The run ID
is the same, so the command re-enters the existing run: finished steps are
left alone and whatever did not finish is picked up. :doc:`../runs/monitoring`
covers following a run and re-entering it after a failure, including
``--clobber`` to run steps again and ``--resume`` to continue a failed
UCLA-ROMS step from its last restart.


.. _workplan_compute_environment:

Where the steps run
-------------------

The workplan's ``compute_environment`` says what the workplan expects of the
machine, and provides scheduler defaults for every step. Every key is
optional:

.. code-block:: yaml

    compute_environment:
      launcher: slurm          # "local" | "slurm" | omitted
      system: anvil            # optional, informational
      slurm:                   # workplan-wide SLURM defaults
        account_name: my-allocation
        queue_name: wholenode
        max_walltime: "04:00:00"

``launcher`` selects how steps are executed. Omitted, C-Star picks the
launcher for the current system: SLURM when the system has a scheduler,
otherwise local. ``system`` names the machine the workplan was written for,
for the record.

Per step, ``compute_overrides`` replaces any of the scheduler settings under
a key naming the launcher. A common pattern sends Forge steps to a
general-purpose queue and ROMS steps to an MPI queue:

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
      application: ucla_roms
      blueprint:
        from_step: make_inputs
      depends_on: [make_inputs]
      compute_overrides:
        slurm:
          queue_name: mpi
          account_name: my-allocation
          num_cpus: 128

The ``slurm`` keys are ``account_name``, ``queue_name``, ``max_walltime``
(``HH:MM:SS`` or SLURM's ``D-HH:MM:SS``), ``num_cpus``, ``num_nodes``,
``cpus_per_node`` and ``single_node``. The ``local`` launcher accepts
``max_walltime`` and ``force_kill_timeout``, enforced with GNU ``timeout``
(on macOS, install ``coreutils`` from conda-forge or Homebrew, which
provides ``gtimeout``). ``num_cpus`` is also where a deferred-blueprint step
declares its allocation.

The SLURM settings layer, lowest to highest precedence: the
``CSTAR_SLURM_*`` settings from your :doc:`configuration <../getting_started/configuration>`,
then ``compute_environment.slurm``, then the step's ``compute_overrides``.
The merged result is recorded on each step of the transformed workplan in the
run directory. On systems that require a SLURM account and queue (Anvil and
Bouchet, for example), values the workplan supplies stand in for
``CSTAR_SLURM_ACCOUNT`` and ``CSTAR_SLURM_QUEUE``; only the ones it leaves
out must be set in your environment. See :doc:`../deployments/hpc`.

