.. _runs-manifest:

The run manifest and moving runs
================================

A run keeps its own record. Two kinds of file, written as the run goes,
say what the run was, what each step did and what each step left behind.
They are what ``cstar run show`` reads, what ``cstar run check`` compares a
directory against, and what lets a copy of a run be understood on another
machine where nothing else about it is known.

.. _workplan_run_manifest:

The two files
-------------

``cstar-run.yaml``, in the run directory, is the **run manifest**. It holds:

- the run's identity: an ``id``, the run's uuid, minted once when the run
  directory is first scheduled; the :term:`run ID`; the workplan name;
  whether the run is a ``workplan`` or a ``blueprint``; and the system it
  was first scheduled on;
- the workplan's name, content hash and path;
- one :term:`attempt <Attempt>` for each time the run was scheduled
  (``cstar workplan submit``, or a re-entry with ``--run-id``, ``--resume``
  or ``--clobber``), with its start time, system, package versions
  (``cstar-ocean`` and ``roms-tools``), the steps it clobbered or resumed,
  its status and when it finished, and, when it carried inputs, their
  directory and a sha256 digest of each file;
- one entry per step: its name, application, working directory relative to
  the run directory, a definition hash of what the step computes, its
  blueprint, the launcher, job id and time of its latest submission, its
  status as of the time the manifest was last refreshed, and its step record
  once it has one;
- what the run depends on outside itself: referenced runs (with the uuid
  each resolved to), source data locations, codebases with their commit or
  branch, and the catalog entries and producer blueprints named in the
  blueprints' provenance.

``cstar-step.yaml``, in each step's working directory (``tasks/<step>/``),
is the :term:`step record`. The step process writes it when it shuts down:
the outcome (``completed``, ``failed``, ``cancelled`` or ``unsubmitted``),
start and finish times, the system, package versions, what ran the step
(the scheduler, host and process id, and inside a SLURM job the job id and
the allocation it was granted), the blueprint file the step ran, the
:term:`products <Product>` the step left and any errors. The products are
the files a user keeps, each with its size and, once recorded, a content
digest: for a ROMS step, its joined outputs by stream, its namelist, cppdefs
and MARBL configuration, its executable and its log; for a Forge step, its
generated inputs, published blueprint, rendered configuration and log; for
a ``cdr_metrics`` step, its metrics series and figures.

The scheduling command writes the manifest, under a lock, and records each
step's submission as it makes it. The steps write their own records and
never touch the manifest. Keeping these records never stops a run: a step
that cannot write its record, or a command that cannot update the manifest,
logs a warning and carries on. ``cstar run status`` and ``cstar run show``
refresh the manifest: they fold the step statuses and records into it, and
once every step is terminal they set ``finished_at`` on the latest attempt,
which is the completion marker. A run that finishes while nobody is watching
is closed by the next ``status`` or ``show``.

A standalone ``cstar blueprint run`` writes the same two files in its
working directory, with ``kind: blueprint`` and one step. Runs scheduled
with an earlier C-Star release have no manifest until they are re-entered.
The files are described by published schemas, listed in the :doc:`schema
reference <../../schemas/index>`.

.. _workplan_execution_facts:

What the manifest says about timing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The manifest records each step's latest submission (``launcher``,
``job_id``, ``submitted_at``), and the step record says what ran the step.
``cstar run show`` derives two times from these: the time the step waited,
from its submission to the step process starting, and its wall time, from
that start to the recorded outcome. The wait includes the time a step was
held for the steps it depends on, since a dependent step is submitted as
soon as they are. ``show`` leaves both times out when the step's record is
from an earlier run than its latest submission.

The manifest keeps only the latest submission of each step. Anything else
is SLURM accounting's to keep: an earlier submission, CPU time, the memory
high-water mark, and the exit state of a job that was killed before its
step could write a record. Query it by job id while your site retains it:

.. code-block:: console

   sacct -j <job-id> --format=JobID,Submit,Eligible,Start,End,Elapsed,State,MaxRSS

.. _workplan_carried_inputs:

What the run was asked
^^^^^^^^^^^^^^^^^^^^^^

Each attempt that reads the workplan file leaves what it was asked in the
run directory, under ``attempts/<YYYYMMDDTHHMMSS>/``, named after the
attempt's start in UTC:

- ``workplan.yaml``: the workplan file exactly as given;
- ``workplan.vars``: the runtime variables, when any were given;
- ``workplan_transformed.yaml``: a copy of the plan the attempt prepared;
- ``blueprints/<step>/<file>``: each step's blueprint file, downloaded first
  when it is remote. A deferred or inline blueprint has no file at that
  point and is not carried; an inline one is written into its step's
  directory.

The run manifest records the directory on the attempt with a sha256 digest
of each file. A re-entry by ``--run-id`` alone, or with ``--resume``,
replays the recorded plan and carries nothing. The plan a run executes stays
in the run directory as ``<stem>_transformed.yaml``, where ``<stem>`` is the
workplan file's name without its extension; each attempt overwrites it.
Only the latest attempt that carried inputs is ever read: ``--resume
<workplan>``, and a re-run under a run ID derived from the workplan
``name``, compare the workplan you give with its ``workplan.yaml``. Older
attempt directories are a record and can be deleted by hand.

Moving a run directory
----------------------

Because the manifest and the step records refer to the run's files by
relative path, a run directory stays readable wherever it is moved or
copied as a unit. ``cstar run show <directory>`` reads a copy anywhere, and
``cstar run check <directory>`` reports anything that did not survive the
move: a missing product, a changed size, a symlink that points back at the
old location.

The transformed workplan holds absolute paths into the run directory, and
records the run directory it was written under. Every command that reads
it rebases the paths that begin with the recorded root onto the directory
the file now sits in, so the plan stays valid too. The run record in the
state home follows a moved run only into the current data home: moving one
run from one data home to another on the same machine (move
``<old>/workplan_runs/<id>`` to ``<new>/workplan_runs/<id>`` and point
``CSTAR_DATA_HOME`` at ``<new>``) keeps ``status``, ``gather``,
``continue-from`` and ``--resume`` working.

A run copied to any other path, or to another machine, has valid internal
paths and is read through its directory, but it is not tracked there:
``cstar run status`` cannot act on it until it is registered with
:doc:`cstar run attach <attach>`. Even then, ``--resume`` and ``--run-id``
stay refused: an attached run is inspected and referenced, never relaunched.
To continue from it, start a new workplan that references it (see
:ref:`workplan_extending_run`).

Verifying a transfer
^^^^^^^^^^^^^^^^^^^^

A run's products can be verified after a transfer when their content
digests were recorded before it. Run this on the machine that has the run:

.. code-block:: console

   cstar run check <run-id> --record-digests

It computes a digest for each product that has none and writes it into the
step records and the run manifest. For a netCDF file the digest covers the
structure and a bounded sample of the data, which does not depend on how
the file is stored on disk and reads little however large the file is; for
any other file it is the sha256 of its bytes. ``--record-digests`` writes
into the run, so it needs a run you own that is tracked here, or the run's
directory passed explicitly.

On arrival, ``cstar run attach --verify`` and ``cstar run check --verify``
compare every product that has a digest against it, so an incomplete or
corrupted transfer is reported rather than discovered later. ``cstar run
promote --verify`` does the same for the copy it makes.
