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
   Any information you'd like to include about what the blueprint contains or
   where it came from
``application``
   Which application runs this blueprint. ``cstar blueprint run`` reads this
   line to decide what to do with the file, so a forge blueprint and a
   UCLA-ROMS blueprint are run with the same command.
``working_dir`` (optional)
   Where the application writes its results. Left out, the working directory
   is ``CSTAR_DATA_HOME/blueprint_runs/<application>/<name>``: under
   ``~/cstar`` on a laptop, on scratch on an HPC system (see :doc:`../deployments/hpc`).
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
the version it was written with in ``schema_version``. Users can largely ignore
these versions; they are used by C-Star to know how to read and adapt older
files. Developers should increment the schema version when adding or changing
fields in a given blueprint.

The schema version is based on `semantic versioning <https://semver.org/>`_
, read from the file's point of view: a file with an older minor or patch
version loads unchanged; a file with an older major version is migrated
automatically when the application provides a migration (``cstar blueprint
migrate`` writes the migrated file out); a file with a newer minor or patch
version loads unless it sets a field the installed C-Star does not know; and a
file with a newer major version is refused with a message to upgrade. The published schemas are listed
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
   <../catalog_wizard/launching>` writes a forge blueprint from your choices, and
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
To check a blueprint from a notebook or script instead, see
:doc:`../reference/python`.

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

Everything happens on the machine you run the command on, whatever the
application: Forge downloads source data and generates input files there,
and UCLA-ROMS compiles the model and runs it there. On a cluster, run it
inside an allocation (an interactive job, or your own batch script that
calls ``cstar blueprint run``), or submit it as described below. UCLA-ROMS
refuses to launch the model on a login node; see :doc:`../deployments/hpc`.

Two more options matter for UCLA-ROMS runs. ``--pre-run`` performs every
stage before the model launch, so start-up problems surface without waiting
in a scheduler queue, and ``--resume`` picks an interrupted run up in place.
Both are described in :doc:`../runs/pre_run_resume`.

A standalone run records itself in its working directory, exactly as a
workplan step does: ``cstar-step.yaml`` holds what the run did and
``cstar-run.yaml`` is a :term:`run manifest` with ``kind: blueprint`` and a
single step. Read them with ``cstar run show <working directory>``. See
:doc:`../runs/moving/manifest`.

Submitting a blueprint
----------------------

On a cluster, submit the blueprint to the job scheduler instead:

.. code-block:: console

   cstar blueprint submit my_blueprint.yaml

C-Star runs it as a single-step :doc:`workplan <../workplans/index>`: the
command schedules the job and returns, so you can log off while it waits in
the queue and runs. The run ID is the blueprint's ``name`` made into a slug
(``My spin-up`` becomes ``my-spin-up``), and the step is named after the
application. Follow the run with the ``cstar run`` commands:

.. code-block:: console

   cstar run status my-spin-up
   cstar run log my-spin-up ucla_roms

Results land in the run's directory, ``workplan_runs/my-spin-up/tasks/ucla_roms/``
under ``CSTAR_DATA_HOME``, laid out as in a ``cstar blueprint run``. The job
needs your cluster's scheduler settings; see :doc:`../deployments/hpc`.

Submitting the same blueprint again re-enters its run instead of starting a
second one, as for a workplan (see :doc:`../runs/monitoring`). ``--run-id``
names the run yourself, and ``--clobber`` empties its working directory
first. ``--pre-run`` and ``--resume`` are described in
:doc:`../runs/pre_run_resume`.

A blueprint can also be run from Python, for example from a notebook; see
:doc:`../reference/python`.
