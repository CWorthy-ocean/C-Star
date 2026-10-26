.. _runs-monitoring:

Monitoring runs
===============

Once a workplan is submitted, the ``cstar run`` commands are how you follow
it, find its files, and pick it up again when something goes wrong. They
take the :term:`run ID` the one derived from
the workplan's name in most cases, or the one you deliberately gave to ``--run-id``
when you submitted it. ``cstar run ls`` lists your tracked run IDs, in case you forget.

.. note::

   Earlier releases listed these commands under ``cstar workplan`` (``cstar
   workplan status``, ``log``, ``gather``, ``path``, ``cd``, ``ls`` and
   ``list``, or ``cstar wp ...``), and called the launch command ``cstar
   workplan run`` rather than ``cstar workplan submit``. Those spellings
   still work and run the same command; they are no longer listed in
   ``--help``.

Checking run status
-------------------

.. code-block:: console

   cstar run status <run-id>

prints one row per step with a tick in its current column:

.. code-block:: text

                                    Run wio-workplan-tutorial Results
   ┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━┓
   ┃                  Step ┃ Submitted ┃ In Queue ┃ Running ┃ Done ┃ Failed ┃ Cancelled ┃ Dependencies┃
   ┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━┩
   │1. make_inputs (forge) │           │          │    ✔    │      │        │           │             │
   ├───────────────────────┼───────────┼──────────┼─────────┼──────┼────────┼───────────┼─────────────┤
   │           2. simulate │           │          │    ✔    │      │        │           │      1      │
   │           (ucla_roms) │           │          │         │      │        │           │             │
   └───────────────────────┴───────────┴──────────┴─────────┴──────┴────────┴───────────┴─────────────┘
               Disk usage not measured. Run: cstar run status wio-workplan-tutorial --size

On a laptop, a step that is waiting for its dependencies shows as *Running*,
because its process has started and is waiting; on a cluster it sits *In
Queue* until the scheduler releases it. ``--size`` measures each step's disk
usage as well, which can take a while on a large run.

``status`` reads the run's live state from the scheduler, so it takes the
run ID of a run tracked on this machine; to look at any other run (a copy, a
colleague's run, a run in the project runs area), use ``show`` below. It
also refreshes the run's :doc:`manifest <moving/manifest>`, folding the
step statuses and records into it and closing the latest attempt once every
step is terminal; pass ``--no-refresh`` to read the run as last recorded
without writing.

Viewing individual logs
-----------------------

.. code-block:: console

   cstar run log <run-id> <step-name>

prints the log of any step, whether it's running, crashed, or completed.
For an active step, add ``--monitor`` to keep streaming it (and press ``Ctrl-C``
to stop). When a step has failed, this is the first place to look.


Listing all known runs
----------------------

.. code-block:: console

   cstar run ls

lists the runs started on this machine, with their workplan, disk usage and
start time:

.. code-block:: text

   ┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
   ┃                run-id ┃ workplan              ┃ disk space                    ┃    start time    ┃
   ┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
   │ wio-workplan-tutorial │ wio-workplan-tutorial │ not measured (cstar run       │ 2026-10-21 16:50 │
   │                       │                       │ status wio-workplan-tutorial  │                  │
   │                       │                       │ --size)                       │                  │
   └───────────────────────┴───────────────────────┴───────────────────────────────┴──────────────────┘

Without ``--all`` or ``--shared`` it reads only the tracking records in your
state home and scans no run directories. Two flags also read the run
manifests in your data home and in the project runs area (``cstar/runs``
under ``CSTAR_PROJECT_HOME``; see :doc:`../hpc`), and add a ``where`` column:

.. code-block:: console

   cstar run ls --shared     # the runs in the project runs area only
   cstar run ls --all        # the runs tracked here and every other run found there

``--shared`` needs ``CSTAR_PROJECT_HOME`` (which defaults to ``$PROJECT``)
and stops with an error without it. The ``where`` column says ``local`` for
a run tracked on this machine, ``local, untracked`` for a run in your data
home that this machine does not track, ``project`` for a run in the project
runs area (``project, linked`` for a link there to a run kept elsewhere), and
``attached`` for a run :doc:`attached <moving/attach>` from elsewhere
(``attached, read-only`` for one that nothing may write into). The disk
space of a run that was not started here is not measured, and the table
shows ``-``.

.. _workplan_run_cd:

Navigating to the run directory
-------------------------------

.. code-block:: console

   cstar run path <run-id>
   cstar run path <run-id> <step-name>

prints the directory of a run, or of one of its steps. Assuming you
installed the additional shell hook when you first ran `cstar init`,
you can use

.. code-block:: console

   cstar run cd <run-id>
   cstar run cd <run-id> <step-name>

to move your active terminal directly to that directory. If you didn't
install the shell hook or need to update it, you can do so at any time with
``cstar env shell-init``.

Every step writes its final outputs to its own ``output`` directory. To see
them all in one place, ``gather`` collects every step's ``output`` files
into a single ``gathered_output`` directory of symlinks inside the run
directory:

.. code-block:: console

   cstar run gather <run-id>

It is safe to re-run while a workplan is still in progress. When two steps
produce a file with the same name, each link is prefixed with its step name
(``<step>__<file>``) so both remain reachable.

Runs completed with an earlier C-Star release kept joined files in a
``joined_output`` directory next to ``output``. Move such a run to the
current layout before continuing from it or gathering it:

.. code-block:: console

   cstar admin migrate-outputs <path-to-run-or-step-directory> --dry-run
   cstar admin migrate-outputs <path-to-run-or-step-directory>

.. _workplan_rerun:

When a step fails
-----------------

Steps fail: a dataset was missing, a queue limit was hit, the model blew
up. ``cstar run status`` shows the step as *Failed*, the steps that depended
on it are not run, and ``cstar run log`` has the reason. Once you have fixed
the cause, submit the workplan again:

.. code-block:: console

   cstar workplan submit my_workplan.yaml

Running a workplan under a run ID that already exists re-enters that run.
By default the run ID is the workplan ``name``, so running the same file
again re-enters its previous run; ``--run-id <id>`` on its own, without a
workplan path, does the same for a run started under an explicit ID. Each
step is handled according to its recorded state:

- A step that is **done** is reused as it is. Nothing is recomputed.
- A step that **failed** is run again from scratch.
- A step that is **still in progress** has its status refreshed.
- A step that was never reached, because something before it failed, is
  scheduled as usual.

So after a failure, fixing the cause and submitting again is usually all
there is to it. Two options change the default handling:

``--clobber <step-name>``, ``--clobber all``
   Clear the named step (or every step) and run it again from scratch, even
   if it is done. This is how you rerun a step after changing its inputs.
   The ``--clobber`` flag is repeatable if you need to clear multiple steps at once.
``--resume``
   Continue every failed step in place, from its last usable restart,
   instead of clearing it. Only applications that declare themselves
   resumable support this (today, ``ucla_roms``); other failed steps are run
   again from scratch with a warning, and completed steps are left
   untouched. See :doc:`pre_run_resume`.

Additional reentry rules
^^^^^^^^^^^^^^^^^^^^^^^^

A few more rules keep a re-entry safe and reproducible:

- A step that is done is not re-run under a changed definition: if its
  blueprint file, its ``blueprint_overrides``, its ``directives``, its
  dependencies or its application changed since the step ran, the re-entry
  is refused, and the message names the steps and the ``--clobber
  <step-name>`` to pass. Pass ``--run-id <other-id>`` to run the changed
  workplan separately instead. Runtime ``--var`` values and
  ``compute_overrides`` / ``workflow_overrides`` are not part of a step's
  definition, so changing them re-enters the run without a refusal.
- A workplan with the same ``name`` as an earlier run but different content
  is refused, because the two would share one run directory. Pass
  ``--run-id <other-id>`` to run it separately, or rename the workplan.
- When the run directory is gone (after a purge of scratch, or after you
  deleted it) while the run ID still has step state on this machine,
  running the workplan again is refused, and the message names the state
  directory. ``cstar admin clean --run-id <id>`` clears it, or pass
  ``--run-id <other-id>`` to start a new run.
- Step directories and state files that no step of the workplan accounts
  for any more (after a step was renamed, for example) are listed in a
  warning and left in place; remove them by hand once you have confirmed
  they are not needed.
- Each re-entry appends an attempt to the run's :doc:`manifest
  <moving/manifest>`, which records when it started, on which system, with
  which package versions, and which steps it clobbered or resumed. Each
  attempt that reads the workplan file also leaves its own copy of the
  workplan and blueprints under ``attempts/`` in the run directory, so
  earlier attempts are never overwritten.
- The transformed workplan saved in the run directory
  (``<stem>_transformed.yaml``) is the plan the run executes. Passing it to
  ``cstar workplan submit`` re-enters its run exactly as ``--run-id <id>``
  does; a copy of it anywhere else is refused.

Submitting again with a **different** run ID starts the workplan from
scratch in a new directory, which is the right thing when you want to keep
the first run as it is.



.. _workplan_show_run:

Viewing run history and metadata
--------------------------------

.. code-block:: console

   cstar run show <run-id>

shows what a run was and what it produced: each time the workplan was
submitted, on which machine and with which package versions, each step's
status and outcome, and what the run depends on outside itself.

.. code-block:: text

                Run wio-workplan-tutorial
   ┌──────────┬──────────────────────────────────────┐
   │     Name │ wio-workplan-tutorial                │
   │     Kind │ workplan                             │
   │   Run-id │ wio-workplan-tutorial                │
   │       Id │ b3881b52-933a-4db9-82cc-d79feb9de9fc │
   │   System │ darwin_arm64                         │
   │  Created │ 2026-10-21 16:50                     │
   │ Observed │ 2026-10-21 16:57                     │
   └──────────┴──────────────────────────────────────┘
                                                        Attempts
   ┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
   ┃ Started          ┃ Finished         ┃ Status ┃ Clobbered ┃ Resumed ┃ Pre-run ┃ cstar-ocean                      ┃
   ┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
   │ 2026-10-21 16:50 │ 2026-10-21 16:56 │ done   │ -         │ -       │ no      │ 0.17.0                           │
   └──────────────────┴──────────────────┴────────┴───────────┴─────────┴─────────┴──────────────────────────────────┘
                                       Steps
   ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━┳━━━━━━━━┓
   ┃ Step        ┃ Application ┃ Status ┃ Outcome   ┃ Products ┃ Size ┃ Record ┃
   ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━╇━━━━━━━━┩
   │ make_inputs │ forge       │ Done   │ completed │ 0        │ 0 B  │ yes    │
   │ simulate    │ ucla_roms   │ Done   │ completed │ 0        │ 0 B  │ yes    │
   └─────────────┴─────────────┴────────┴───────────┴──────────┴──────┴────────┘

Unlike ``status``, ``show`` works on any run: give it a run ID,
``run://<uuid>``, or the path of any directory that holds a run manifest,
such as a copy of a run made on another machine. The ``Id`` row is the run's
uuid, which is how another workplan can refer to this run unambiguously (see
:doc:`../workplans/references`). A ``Jobs`` table shows each step's job and
how long it waited and ran, derived from its submission and its step record;
a dash means a time is missing, or the record is from an earlier run of the
step.

For a run tracked on this machine, ``show`` refreshes the manifest first, as
``status`` does; pass ``--no-refresh`` to show it as last recorded. A
directory, and a run found only by its manifest, are read as they are, and
nothing is written, so the command is safe on a run that is not yours.
``--format yaml`` or ``--format json`` prints the manifest itself, ready to
pipe into another tool. The exit code is 0 when the run is shown, 1 when it
has no readable manifest and 2 for bad arguments.

.. _workplan_check_run:

Is the run directory intact?
----------------------------

.. code-block:: console

   cstar run check <run-id>
   cstar run check /path/to/copy/of/the/run

looks for what would stop a run directory from being read, continued from,
or moved as a unit, and reports kind by kind, one line per finding:

- absolute paths under the run, or under the C-Star data home, in the files
  each step emits and in the run's ``*_transformed.yaml``. Pass ``--all`` to
  scan the launch artifacts too (override and directive files,
  ``script.sh`` and namelists); launchers regenerate those, so they are
  skipped by default.
- symlinks with an absolute target inside the run, and symlinks whose target
  does not exist.
- steps of the transformed workplan with no ``tasks/<slug>`` directory. A
  step that has not started has none yet.
- products that are missing from the run, or whose size changed, compared
  with the step records folded into the manifest. A log is reported only
  when it shrank. Pass ``--verify`` to compare the content too, where a
  digest is recorded (see :doc:`moving/manifest`).
- carried inputs under ``attempts/`` whose content differs from the digest
  the manifest recorded.

Symlinks with a target outside the run, step directories that no step
accounts for, sentinel files that no step accounts for, steps that are done
without a step record, a latest attempt that has not finished, and an
attempt directory or carried file that is missing are listed for
information and do not fail the check. Nothing is ever deleted.

For a run tracked on this machine, named by its run ID or uuid, the
manifest is refreshed first, as ``show`` does; pass ``--no-refresh`` to read
it as last recorded. Any other run is checked as its directory. The exit
code is 0 when there is nothing to fix, 1 when findings remain and 2 for
bad arguments. A workplan file is refused: validate it with ``cstar
workplan check <file>``.

Pass ``--repair`` to rewrite each absolute symlink that points inside the
run as a relative link, and rebuild ``gathered_output`` if the run has one.
``--repair`` changes nothing else: a file that names absolute paths is
regenerated by running the workplan again, and a dangling link is reported,
never touched. Because it writes into the run, ``--repair`` refuses a run
attached read-only, and a run that is not tracked here and was found by its
run ID or ``run://<uuid>``; pass that run's directory to repair it
deliberately.

``--record-digests`` computes a content digest for each product that has
none and writes it into the step records and the run manifest, so that the
files can be verified after a transfer; see :doc:`moving/index`.

.. _workplan_clean_up:

Cleaning up unneeded runs
-------------------------

.. code-block:: console

   cstar admin clean --run-id <run-id> --dry-run
   cstar admin clean --run-id <run-id>

removes one run: its tracking record and its run directory. Add
``--dry-run`` first to see what would go. The command asks before each
removal; ``--yes`` answers for you. A run that is attached read-only, or
whose directory is in the project runs area, loses its tracking record and
keeps its directory; remove that directory yourself if you want it gone. A
run ID with nothing to remove is reported, and the exit code is 0.

``cstar admin clean --all`` is the other choice, and should be used with caution:
C-Star's data, state, cache and config directories are emptied, including
the output of every run in your data home. The shared source-data cache and
your catalog are kept, so the next run does not download its inputs again.
``--dry-run`` lists what ``--all`` would remove. Without ``--all`` or
``--run-id``, the command stops with a usage error and removes nothing.

.. _workplan_known_limitations:

Known limitations
-----------------

These edges of re-entering, promoting and recording runs are known in this
release. Each entry says what you see and what to do about it.

- **Moving a run whose directory is a symlink.** ``cstar run promote --mode move --to <dir>`` given a run whose directory is itself a symbolic link copies and checks the run, then cannot remove the source: it prints ``Moved the copy to <dir> ..., but the source at <link> was not removed`` and exits with code 1. The copy is complete and checked, and is tracked here when the source was. Once you have confirmed it, remove the link, and the directory it points at, yourself. To avoid this, name the run by the directory the link points at. A link that ``--mode link`` made is replaced with a copy by ``--mode materialize``, not moved (see :ref:`workplan_linked_promotions`).
- **Referenced runs listed twice after a re-entry.** When a run whose manifest was written by C-Star 0.17.0 or earlier, and whose workplan references other runs through ``runs``, is re-entered, its manifest keeps the earlier entry for each referenced run beside the new one. ``cstar run show`` then lists each alias twice under ``External runs``, once with an empty ``Uuid``. The entry with a uuid is the current one; ignore the other.
- **Attempt directories the manifest does not list.** An attempt that reads the workplan file writes its :ref:`carried inputs <workplan_carried_inputs>` under ``attempts/`` before it records them in the run manifest. When that record is not written, the directory stays without a manifest entry: either the manifest write failed, which logs a warning and lets the run go on, or a ``--pre-run`` found no step it could prepare and printed ``No step can be prepared in pre-run mode; nothing was scheduled.`` ``cstar run show`` does not list such a directory and ``cstar run check`` does not report it, since it reads only the attempt directories the manifest lists. Nothing reads it, so you can delete it by hand. After a failed manifest write, ``--resume <workplan>`` compares your workplan with the latest attempt the manifest does list; if it refuses, pass ``--run-id`` instead.
- **A run directory purged in part.** When a purge of scratch removes a run's step outputs but leaves its manifest (``cstar-run.yaml``) or its transformed workplan, running the workplan again under the same run ID reuses the steps that were done, although their outputs are gone. ``cstar run check <run-id>`` lists the missing products; pass ``--clobber all`` (or ``--clobber <step-name>``) to run those steps again, or ``--run-id <other-id>`` to start a new run.
- **Locks on an NFS state home.** C-Star serializes writes to a run's manifest and tracking record with lock files in your state home (``CSTAR_STATE_HOME``, ``~/.local/state/cstar`` by default). On NFS, Linux implements these ``flock`` locks with POSIX record locks, which belong to a process: they keep separate processes apart, such as two ``cstar`` commands, but not two threads of one process. A program that calls C-Star's Python API from several threads on the same run can lose one of two simultaneous updates to its manifest or tracking record. Make such calls from one thread at a time, or keep the state home on a local disk or a parallel file system such as GPFS or Lustre (not on scratch, which is purged). ``cstar run status <run-id>`` refreshes a manifest from the step records.
