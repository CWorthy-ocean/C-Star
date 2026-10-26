.. _runs-pre-run-resume:

Pre-run and resume for UCLA-ROMS runs
=====================================

A UCLA-ROMS run does a lot before the model starts: it stages inputs,
clones and compiles the model code, partitions the inputs and generates the
namelist. Most of the ways a run can go wrong show up in those stages, and
on a cluster they would otherwise show up only after the job has waited in
the queue. The model run itself can also be lost part way through, to a
walltime limit or a node failure, with hours of restarts already on disk.
Two options address these: ``--pre-run`` does everything up to the launch
and stops, and ``--resume`` picks a run up from where it left off. Both
exist for ``cstar blueprint run`` and for ``cstar workplan submit``.

.. _workplan_pre_run:

Pre-run: prepare everything, launch nothing
-------------------------------------------

``--pre-run`` performs every stage before the model launch (staging inputs,
cloning and compiling the codebases, partitioning inputs when ParallelIO is
not in use, and generating and validating the namelist) and then exits
successfully without launching ROMS. It catches missing or malformed
inputs, compile failures and namelist errors without waiting in a scheduler
queue, and it moves cloning and compilation out of the eventual allocation.
Unlike ``cstar workplan check``, it is not free of side effects: it writes
the full working directory, which can hold many GB of staged inputs.

For a single blueprint:

.. code-block:: console

   cstar blueprint run my_blueprint.yaml --pre-run
   cstar blueprint run my_blueprint.yaml --resume

The first command leaves the prepared working directory in place. The
second attaches to it, reusing the staged inputs, compiled executable and
partitions, and launches the model; on a scheduler system it submits ROMS as
a job, exactly as a plain ``blueprint run`` does. ``--pre-run`` cannot be
combined with ``--resume``; combined with ``--clobber`` it starts the
preparation from scratch.

For a workplan:

.. code-block:: console

   cstar workplan submit my_workplan.yaml --pre-run
   cstar workplan submit my_workplan.yaml

The second command uses the same run ID, derived from the workplan ``name``
as usual (or pass the same ``--run-id`` to both). The orchestrator records
on each step that it was prepared; on the second command it re-launches
those steps, which attach to the prepared directories and launch the model
inside the scheduler job.

During a pre-run every step runs locally, even on a system with a job
scheduler, so the command is suitable for a login node. Steps that cannot
be prepared are skipped and reported with the reason:

- the step's application does not support pre-run (for example ``forge`` or
  ``hello_world``);
- the step's blueprint is produced by another step (``blueprint: {from_step:
  ...}``);
- one of the step's directives (``continue-from``, ``nest-from`` or
  ``carbonate-sensitivity-from``) takes its input from another step's output.

Every step downstream of a skipped step is skipped as well, since a
``depends_on`` edge may carry data the planner cannot see. Skipped steps run
from scratch on the second command, exactly as they would without a
pre-run. In a typical Forge-then-simulate workplan this means the pre-run
prepares nothing, because the simulation's blueprint comes from the Forge
step; the pre-run earns its keep on workplans whose blueprints exist up
front.

The same run ID also works after a failed real run: fix the cause and
submit the workplan again with ``--pre-run``, and the failed steps are
prepared again from scratch on the login node. To re-prepare steps that
already succeeded, add ``--clobber all`` (or ``--clobber <step-name>``).

Two things a pre-run does not do. When a step uses ParallelIO, the inputs
are not partitioned, so the pre-run only validates them and preparation is
light. And ROMS itself is not started, so errors that only surface when
ROMS opens its boundary or tide files, or writes its first restart, are not
caught. Future work is planned to expand pre-run capabilities to include ROMS
start-up.

Resume: continue from the last restart
--------------------------------------

If a run is lost or interrupted mid-execution, ``--resume`` continues it in
place from the blueprint's working directory, rather than starting over.
For a single blueprint:

.. code-block:: console

   cstar blueprint run my_blueprint.yaml --resume

A resumed UCLA-ROMS run reuses the codebases, compiled executable, staged
inputs and partitions already in the working directory, and continues from
the last usable restart file the previous attempt wrote. When no restart was written, it starts from the original
initial conditions. ``--resume`` cannot be combined with ``--clobber``, and
only applications that declare themselves resumable accept it (today,
``ucla_roms``).

For a workplan, ``--resume`` re-enters the run and continues every failed
step in place:

.. code-block:: console

   cstar workplan submit my_workplan.yaml --resume

Failed steps whose application supports resume continue from their last
usable restart; failed steps of other applications are run again from
scratch, with a warning; completed steps are reused.

Resume and clobber are the two ways to handle a failed step when a run is
re-entered, and they cannot be combined on one step. The default, without
either, is to run a failed step again from scratch; see
:doc:`monitoring`.
