.. _blueprints:

Blueprints
==========

A blueprint is a YAML file that tells an application what to do. It is the
only input an application takes, so a blueprint has to be complete: a
colleague who has the same blueprint, on another machine, gets the same
result. This page covers what every blueprint has in common and the commands
that work on any of them. Each application's own fields are described on its
pages.

What every blueprint has
------------------------

The smallest application that ships with C-Star, ``hello_world``, prints a
greeting. Its blueprint shows the fields every blueprint shares, plus one
field of its own:

.. code-block:: yaml

   name: say-hello
   description: Greet the oceanographers
   application: hello_world
   target: oceanographers

``name``
   What the blueprint is called. It names the working directory when you do
   not give one, and a workplan step names it in its logs.
``description``
   A sentence or two for the next person who opens the file.
``application``
   Which application runs this blueprint. ``cstar blueprint run`` reads this
   line to decide what to do with the file, so a forge blueprint and a
   UCLA-ROMS blueprint are run with the same command.
``working_dir`` (optional)
   Where the application writes its results. Left out, the working directory
   is ``CSTAR_DATA_HOME/blueprint_runs/<application>/<name>``: under
   ``~/cstar`` on a laptop, on scratch on an HPC system (see :doc:`../hpc`).
   A relative path is resolved against the directory you run ``cstar`` from.

A few more core fields are rarely written by hand. ``state`` marks a
blueprint as ``draft`` or ``validated``; today the field is informational.
``schema_version`` records the version of the application's blueprint
schema the file was written with (see below). ``cpus_needed`` lets a
workplan size a step's allocation. ``provenance`` records what produced the
blueprint and what it was derived from; Forge fills it in on the UCLA-ROMS
blueprints it writes, and you can leave it out of your own.

The core fields come from :class:`cstar.orchestration.models.Blueprint`;
every application's blueprint is a subclass that adds its own. The
:doc:`UCLA-ROMS blueprint <ucla_roms/blueprint>` adds the model code, input
datasets and run settings of a simulation, the :doc:`forge blueprint
<forge/blueprint>` adds a domain description, and the small applications add
a handful of paths each.

.. _blueprint_schema:

Schema versions
^^^^^^^^^^^^^^^

Each application's blueprint schema has a version, and a blueprint records
the version it was written with in ``schema_version``. The version is semver
read from the file's point of view: a file with an older minor or patch
version loads unchanged; a file with an older major version is migrated
automatically when the application provides a migration (``cstar blueprint
migrate`` writes the migrated file out); and a file newer than the installed
C-Star is refused with a message to upgrade. The published schemas are listed
under :doc:`../schemas/index`, and the rules are spelled out in
:ref:`versioning_blueprint_schema`.

.. _blueprint_provenance:

Provenance
^^^^^^^^^^

A blueprint can record what produced it and what it was derived from in its
``provenance`` block: when and by which tool it was generated, on which
system and with which package versions, and the blueprints and catalog
entries it came from. The block is optional, and an application may extend
it with fields of its own. Forge fills it in on every UCLA-ROMS blueprint it
writes, so a copy of the file still says where it came from; see
:doc:`forge/blueprint`.

.. rubric:: Provenance models

.. autosummary::

  ~cstar.orchestration.models.Provenance
  ~cstar.orchestration.models.GeneratedBy
  ~cstar.orchestration.models.BlueprintIdentity
  ~cstar.orchestration.models.CatalogSpecRef

Where blueprints come from
--------------------------

There are three ways to get a blueprint:

1. **Build it in the wizard.** For a new domain, the :doc:`wizard
   <../wizard/launching>` writes a forge blueprint from your choices, and
   running that blueprint produces the UCLA-ROMS blueprint of the simulation.
   This is the usual path.
2. **Write it by hand.** For input files you already have, or for one of the
   small applications, write the YAML directly. The example on each
   application's page is a good starting point.
3. **Let a step produce it.** In a workplan, a step can take the blueprint
   that an earlier step generated, or be given its blueprint inline. See
   :doc:`../workplans/index`.

Checking a blueprint
--------------------

.. code-block:: console

   cstar blueprint check my_blueprint.yaml

This reads the file, validates it against the application's schema and
reports every problem it finds: a misspelled field, a missing required
value, a date that does not parse. It does not check that the files the
blueprint points at exist. A valid file prints ``The blueprint is valid``.

The same check is available from Python, which is handy in a notebook:

.. code-block:: python

   from cstar.applications.ucla_roms.models import UclaRomsBlueprint
   from cstar.orchestration.serialization import deserialize

   deserialize("my_blueprint.yaml", UclaRomsBlueprint)

Running a blueprint
--------------------

.. code-block:: console

   cstar blueprint run my_blueprint.yaml

C-Star reads the ``application`` line, hands the blueprint to that
application, and runs it in the foreground, printing the application's log
as it goes. Results land in the working directory described above. Pass
``--clobber`` to empty the working directory first; without it, a
non-empty working directory is refused so a previous result is never
overwritten by accident.

Where the work happens depends on the application:

- The ``ucla_roms`` application submits the model run to the job scheduler
  itself when one is available, so on a cluster it is fine to invoke it from
  a login node: the code is fetched and compiled there, and the simulation
  runs in a SLURM job. Without a scheduler it runs the model in place.
- The ``forge`` application does no submitting. Downloading source data,
  building the grid and generating input files all happen wherever you run
  the command. For anything beyond a toy domain, run it from a compute node
  (an interactive allocation, or your own batch script that calls
  ``cstar blueprint run``), or put it in a :doc:`workplan <../workplans/index>`
  and let C-Star submit it.

.. warning::
    The post-processing step that joins partitioned model output may use every core of a login node and be terminated (and make the admins angry).

    - Set ``CSTAR_NPROCS_POST`` to a small number (about 2) when running a UCLA-ROMS blueprint directly on an HPC login node.
    - Better, make a :ref:`single-step workplan <workplan_examples>` to run a simulation entirely on the compute cluster.

Two more options matter for UCLA-ROMS runs. ``--pre-run`` performs every
stage before the model launch, so start-up problems surface without waiting
in a scheduler queue, and ``--resume`` picks an interrupted run up in place.
Both are described in :doc:`../runs/pre_run_resume`.

A standalone run records itself in its working directory, exactly as a
workplan step does: ``cstar-step.yaml`` holds what the run did and
``cstar-run.yaml`` is a :term:`run manifest` with ``kind: blueprint`` and a
single step. Read them with ``cstar run show <working directory>``. See
:doc:`../runs/moving/manifest`.

Running from Python
^^^^^^^^^^^^^^^^^^^

The command line is the usual interface, but a blueprint can also be run
from Python, for example from a notebook or a script of your own. A
``UclaRomsRunner`` executes a UCLA-ROMS blueprint:

.. code-block:: python

   from cstar.entrypoint.config import JobConfig, ServiceConfiguration
   from cstar.applications.ucla_roms.app import UclaRomsRunner
   from cstar.applications.ucla_roms.models import UclaRomsBlueprint
   from cstar.applications.core import RunnerRequest

   request = RunnerRequest("my_blueprint.yaml", UclaRomsBlueprint)
   service_cfg = ServiceConfiguration()
   job_cfg = JobConfig(account_id="your-account-id", walltime="00:90:00", priority="wholenode")

   runner = UclaRomsRunner(request, service_cfg, job_cfg)
   await runner.execute()
