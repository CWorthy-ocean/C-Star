.. _python:

Using C-Star from Python
========================

The ``cstar`` command line is the main way to use C-Star, and everything in
the rest of this documentation is written for it. The same pieces are also
importable from Python. That is handy when you already work in a notebook or a
script: you can check a blueprint you just edited, look inside a workplan,
load a catalog spec, or run a blueprint without leaving the session.

Where a snippet stands in for a ``cstar`` command, the section names the
command, so you can read the command's page for what it does and come here
for how to do it from Python.

Before you start
----------------

Import C-Star from the environment you installed it into. For a notebook,
register that environment as a Jupyter kernel once and pick it in the
notebook's kernel menu:

.. code-block:: console

   cstar env register-kernel

C-Star reads the same configuration from Python as it does on the command
line, so the settings in your :doc:`configuration file <../getting_started/configuration>`
apply here too.

Checking a blueprint
--------------------

``deserialize`` reads a blueprint file into the application's blueprint
class and validates it. A problem with the file raises an error that lists
every field at fault. This is the content check that ``cstar blueprint check``
performs (see :doc:`../applications/blueprints`).

.. code-block:: python

   from cstar.applications.ucla_roms.models import UclaRomsBlueprint
   from cstar.orchestration.serialization import deserialize

   blueprint = deserialize("my_blueprint.yaml", UclaRomsBlueprint)
   print(blueprint.runtime_params.start_date, blueprint.partitioning.n_procs_x)

The result is an ordinary object, so its fields can be read and inspected in
the notebook. A forge blueprint loads the same way, with its own class:

.. code-block:: python

   from cstar.applications.forge.blueprint import ForgeBlueprint

   forge_blueprint = ForgeBlueprint.from_yaml("forge_blueprint.yaml")

Checking a workplan
-------------------

A workplan file loads the same way, into the ``Workplan`` class:

.. code-block:: python

   from cstar.orchestration.models import Workplan
   from cstar.orchestration.serialization import deserialize

   workplan = deserialize("my_workplan.yaml", Workplan)
   print([step.name for step in workplan.steps])

This checks the workplan file's own structure, much like ``cstar workplan
check --schema-only``. The full ``cstar workplan check`` goes further: it
loads every step's blueprint, merges the overrides and validates the
directives (see :doc:`../workplans/index`).

Loading a model spec
--------------------

The model specs in the :doc:`catalog <../catalog_wizard/catalog>` are ``model.yaml`` files
(see :doc:`../applications/forge/specs`). ``load_models_yaml`` reads one into a
validated ``ModelSpec``, which is a quick way to see what code and settings
a spec pins:

.. code-block:: python

   from pathlib import Path
   from cstar.applications.forge.models import load_models_yaml

   spec = load_models_yaml(
       Path("~/cstar/catalog/ModelSpec/my-model/model.yaml").expanduser(), "my-model"
   )
   print(spec.code.roms.location, spec.code.roms.commit, spec.bgc_mode, spec.use_pio)

:class:`~cstar.applications.forge.models.ModelSpec` and the rest of the Forge
classes are described in :doc:`api-forge`.

Running a blueprint
-------------------

A blueprint can also be run from Python. ``cstar blueprint run`` runs the
same runner (see :doc:`../applications/blueprints`). A
``UclaRomsRunner`` executes a UCLA-ROMS blueprint, with the job and service
settings taken from your configuration, as the command takes them:

.. code-block:: python

   from cstar.applications.core import RunnerRequest
   from cstar.applications.ucla_roms.app import UclaRomsRunner
   from cstar.applications.ucla_roms.models import UclaRomsBlueprint
   from cstar.entrypoint.config import get_job_config, get_service_config

   request = RunnerRequest("my_blueprint.yaml", UclaRomsBlueprint)
   runner = UclaRomsRunner(
       request,
       get_service_config("INFO", name="UclaRomsRunner"),
       get_job_config(),
   )
   await runner.execute()

``get_job_config()`` reads the scheduler account, queue and walltime from
your ``CSTAR_SLURM_*`` settings. To set them for this run only, build a
``JobConfig`` yourself instead:

.. code-block:: python

   from cstar.entrypoint.config import JobConfig

   job_cfg = JobConfig(account_id="my-allocation", walltime="01:30:00", priority="wholenode")

``RunnerRequest`` takes the same choices as the command's options:
``clobber=True``, ``resume=True`` and ``pre_run=True`` match ``--clobber``,
``--resume`` and ``--pre-run`` (see :doc:`../runs/pre_run_resume`).

Two differences from the command are worth knowing:

- ``execute()`` waits until the simulation finishes, and so does the
  notebook cell that calls it. On a cluster the model still runs as a
  scheduler job, but the cell is busy for the whole time the job waits in
  the queue and runs. For long runs, submit a :doc:`workplan
  <../workplans/index>` from the command line instead, and follow it with
  ``cstar run status``.
- ``await`` at the top level works in a Jupyter notebook. In a script, wrap
  the call in ``asyncio.run(runner.execute())``.

The classes behind these snippets are documented in the rest of this
Reference section: :doc:`api-blueprint` for the blueprint models,
:doc:`api-orchestration` for workplans and runs, and :doc:`api-forge` for
Forge.
