.. _runs-monitoring:

Monitoring runs
===============

Once a workplan is submitted, the ``cstar run`` commands are how you follow
it, find its files, and pick it up again when something goes wrong. They
take the :term:`run ID`: the one derived from
the workplan's name in most cases, or the one you deliberately gave to ``--run-id``
when you submitted it. ``cstar run ls`` lists your tracked run IDs, in case you forget.

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

To also see runs that were not started here, add a flag:

.. code-block:: console

   cstar run ls --shared     # the runs in your project's shared runs area
   cstar run ls --all        # your runs plus every other run C-Star can find

Both add a ``where`` column saying where each run lives: ``local`` for your
own runs, ``project`` for the shared runs area (see :doc:`../deployments/hpc`), and
``attached`` for a run :doc:`attached <moving/attach>` from elsewhere.

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

Two things stop a re-entry
^^^^^^^^^^^^^^^^^^^^^^^^^^

- **A finished step whose definition changed.** If a done step's blueprint,
  ``blueprint_overrides``, ``directives``, dependencies or application
  changed since it ran, the re-entry is refused, and the message names the
  steps. Pass ``--clobber <step-name>`` to rerun them, or ``--run-id
  <other-id>`` to run the changed workplan separately. Changing ``--var``
  values or ``compute_overrides`` does not count.
- **A different workplan with the same name.** Two workplans that share a
  ``name`` would share one run directory, so the second is refused. Pass
  ``--run-id <other-id>``, or rename the workplan.

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

``show`` never writes into a run you don't own, so it is safe on a
colleague's run. ``--format yaml`` or ``--format json`` prints the manifest
itself, ready to pipe into another tool.

.. _workplan_check_run:

Is the run directory intact?
----------------------------

.. code-block:: console

   cstar run check <run-id>
   cstar run check /path/to/copy/of/the/run

looks for anything that would stop a run directory from being read,
continued from, or moved as a unit, and lists what it finds:

- output files and other products that are missing, or whose size changed
  since the step recorded them (add ``--verify`` to compare their content
  too; see :doc:`moving/manifest`);
- files and symlinks that point at absolute paths, which break when the run
  is moved;
- symlinks whose target no longer exists;
- steps of the workplan with no directory.

Nothing is ever deleted. ``--repair`` rewrites absolute symlinks inside the
run as relative ones and rebuilds ``gathered_output``; it changes nothing
else.

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
removal; ``--yes`` answers for you. A run in the shared project runs area
keeps its directory; only your record of it is removed.

``cstar admin clean --all`` is the other choice, and should be used with caution:
C-Star's data, state, cache and config directories are emptied, including
the output of every run in your data home. The shared source-data cache and
your catalog are kept, so the next run does not download its inputs again.
``--dry-run`` lists what ``--all`` would remove.
