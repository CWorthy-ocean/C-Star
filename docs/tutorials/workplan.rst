.. _tutorial-workplan:

Automating the previous model with a workplan
=============================================

The :doc:`previous page <end_to_end>` ran Forge and then the simulation as two
separate commands, and you carried the path of the blueprint Forge wrote from
the first command to the second. A **workplan** does that for you. It is a
small YAML file that lists the things to run as *steps*, says which steps wait
for which, and lets a step pick up a blueprint that an earlier step produced.
You submit the whole thing once and check on it while it runs.

This page runs the same two steps as a simple workplan to show you the key elements.
The next tutorial will show how workplans can be extended with a number of features
that help you manage more complex ocean modeling workflows.

Before you start
----------------

- Finish the :doc:`previous page <end_to_end>`, or at least step 1 of it: you
  need a forge blueprint. Save it (or copy it) as ``forge_blueprint.yaml``
  in an empty directory.
- Download :download:`workplan_forge_then_run.yaml <workplan_forge_then_run.yaml>`
  into the same directory.

1. Read the workplan
--------------------

.. literalinclude:: workplan_forge_then_run.yaml
   :language: yaml

The top of the file describes the workplan itself. The ``name`` doubles as the
:term:`run ID`, the handle you use to check on the run later. ``state: draft`` means
the file may still change; see :doc:`../workplans/index` for the ``validated`` state.

Everything else is the list of ``steps``. A step is one thing to run: it has a
``name``, the ``application`` that runs it, and the ``blueprint`` that application
gets. The two steps here are exactly the two commands from the previous page:

``make_inputs``
   Runs the Forge *application* against your Forge *blueprint*. The ``blueprint`` path is relative to
   the workplan file, not to where you run ``cstar`` from, so the two files
   can be moved around together.

``simulate``
   Runs the UCLA-ROMS application. Its blueprint does not exist yet when you
   submit the workplan, because Forge has not written it. Instead of a path,
   ``blueprint`` names the step that will produce it: ``from_step:
   make_inputs``. When the step starts, C-Star knows where Forge will output
   the UCLA-ROMS blueprint, and looks there for its instructions.

``depends_on`` is the ordering. A step does not start until every step it
lists has finished successfully, and a step that depends on nothing starts at
once. A deferred blueprint always needs this: ``simulate`` must list
``make_inputs``, or C-Star refuses the workplan. If a step fails, the steps
that depend on it are not run.

That is the whole vocabulary for now. Steps can also override values in their
blueprint and pass run-time information to each other; the :doc:`next page
<cdr_workplan>` uses both.

2. Check it (optional)
----------------------

.. code-block:: console

   cstar workplan check workplan_forge_then_run.yaml

This reads the workplan and every blueprint it names, and reports problems
such as a misspelled step name, a missing dependency, or a blueprint that does
not validate. Nothing is written to disk.

Submitting the workplan in the next step also performs the check before proceeding,
but doing it manually may help you catch errors after drafting or modifying a workplan you're
not yet ready to submit.

3. Submit it
------------

.. code-block:: console

   cstar workplan submit workplan_forge_then_run.yaml

The command sets up a directory for the run, starts the steps, prints a
summary with the log file of each step, and returns. The steps keep running in
the background. On a laptop both steps are started immediately and
``simulate`` waits for ``make_inputs`` to finish; on a cluster each step
becomes a scheduler job with a dependency on the jobs before it.

The run directory is ``~/cstar/workplan_runs/<run-id>/`` (or under
``CSTAR_DATA_HOME`` if you have set it), and each step gets its own directory
beneath it:

.. code-block:: text

   workplan_runs/wio-workplan-tutorial/
     cstar-run.yaml                                the run manifest
     workplan_forge_then_run_transformed.yaml      the plan as actually run
     tasks/make_inputs/   input_data/, output/, logs/, ...   as in a blueprint run
     tasks/simulate/      input/, output/, logs/, ...

Each step directory has the same layout as the working directory of a
blueprint run on the previous page, so everything you found there is here too.

Submitting the same file again does not start a second copy: the run ID is the
workplan name, so the command re-enters the existing run, leaves finished steps
alone and picks up whatever did not finish. To start over from scratch, give
the run a new ID with ``--run-id my-second-try``, or clear it with
``--clobber all``.

4. Keep an eye on it
--------------------

Three commands cover most of what you want to know. All of them take the run
ID.

.. code-block:: console

   cstar run status wio-workplan-tutorial

prints one row per step with a tick in its current column: *Submitted*,
*Running*, *Done*, *Failed* and so on. A minute or two after submitting, it
looks like this:

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

While Forge works, ``simulate`` shows as running too, because its process is
already started and waiting for its dependency. When both steps have finished,
both ticks sit in the *Done* column.

.. code-block:: console

   cstar run log wio-workplan-tutorial make_inputs

prints the log of one step, the same text you saw scroll past when you ran
the blueprint directly. If you want to keep track of an actively running job,
add ``--monitor`` to stream the log output as it's generated (and press ``Ctrl-C``
to stop).

.. code-block:: console

   cstar run show wio-workplan-tutorial

shows what the run was and what it produced: each time it was submitted, on
which machine and with which package versions, and each step's status and
outcome. Once the run is over:

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

Finally,

.. code-block:: console

   cstar run ls

lists every run you have started on this machine, in case you forget a run
ID:

.. code-block:: text

   ┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
   ┃                run-id ┃ workplan              ┃ disk space                    ┃    start time    ┃
   ┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
   │ wio-workplan-tutorial │ wio-workplan-tutorial │ not measured (cstar run       │ 2026-10-21 16:50 │
   │                       │                       │ status wio-workplan-tutorial  │                  │
   │                       │                       │ --size)                       │                  │
   └───────────────────────┴───────────────────────┴───────────────────────────────┴──────────────────┘

5. Look at the results
----------------------

When ``status`` shows both steps as *Done*, the simulation's output is in the
``simulate`` step's ``output`` directory. ``cstar run path wio-workplan-tutorial
simulate`` prints that step's directory (leave out the step name for the run's
own directory), so you don't have to remember the layout. You can also use

.. code-block:: console

   cstar run cd wio-workplan-tutorial simulate

to go straight to a given run or step.

.. note:: A program cannot change the directory of the shell that started it,
   so this command needs a small shell function. The first time you run it,
   it tells you how: run ``cstar env shell-init --install`` once and open a new
   shell. If you would rather not, use the output of ``cstar run path`` however
   you like, e.g. ``cd $(cstar run path wio-workplan-tutorial simulate)``.

The files are the same ones the previous page produced, and you can open them
with `roms-tools <https://roms-tools.readthedocs.io/en/latest/plotting_roms_output.html>`_
or any netCDF tool exactly as before. The UCLA-ROMS blueprint that
``simulate`` ran is in the ``output`` directory of ``make_inputs``.

What you gained
---------------

Nothing changed in the science: the same forge blueprint, the same generated
inputs, the same simulation. What changed is that the sequence is now a file.
It can be submitted with one command, checked on with one ID, re-entered if a
step fails, and handed to someone else to reproduce. That matters more as the
sequence grows, which is where the :doc:`next page <cdr_workplan>` goes.

On a cluster
------------

A workplan is also how a run reaches the compute nodes of an HPC system.
There, ``cstar workplan submit`` submits each step as a scheduler job and
returns, and the ``cstar run`` commands work the same way from the login node.

One thing to add first. C-Star sizes each job from the step's blueprint, but
``simulate``'s blueprint doesn't exist until ``make_inputs`` writes it, so the
job would get a single CPU. The wio-toy domain is split into 2 by 5 tiles and
runs on 10 MPI ranks, so ask for 10 CPUs with ``compute_overrides``:

.. code-block:: yaml

   - name: simulate
     application: ucla_roms
     blueprint:
       from_step: make_inputs
     depends_on:
       - make_inputs
     compute_overrides:
       slurm:
         num_cpus: 10

``compute_overrides`` is also where a step asks for its own queue and
walltime; see :doc:`../workplans/index` and :doc:`../deployments/hpc`.
