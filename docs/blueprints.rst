Blueprints
==========

Blueprints define the contract for communicating the available behaviors of an
application. A user-configured blueprint informs C-Star which behaviors are
desired.

.. _blueprint_schema:

Core Blueprint Schema
---------------------

The core attributes of a blueprint come from :class:`cstar.orchestration.models.Blueprint`.
The example on the :doc:`ROMS-MARBL blueprint <blueprints/roms_marbl>` page shows them in use.

.. rubric:: Core Blueprint Attributes

.. autosummary::

  ~cstar.orchestration.models.Blueprint.name
  ~cstar.orchestration.models.Blueprint.description
  ~cstar.orchestration.models.Blueprint.application
  ~cstar.orchestration.models.Blueprint.state
  ~cstar.orchestration.models.Blueprint.schema_version
  ~cstar.orchestration.models.Blueprint.working_dir
  ~cstar.orchestration.models.Blueprint.cpus_needed

The core blueprint attributes do not contain enough information to be executed alone.

Customizing Blueprints
-----------------------

Each supported application defines its own ``Blueprint`` subclass, adding the
attributes needed to configure that application. See :doc:`blueprints/roms_marbl`
for the ROMS-MARBL blueprint and :doc:`blueprints/forge` for the blueprint
produced by Forge.


Preparing a Blueprint
---------------------

A blueprint can be prepared for execution in a few ways:

1. If you are creating a brand new domain, use Forge (``cstar forge wizard``) to
   prepare your input files and blueprint together. See :doc:`wizard` and
   :doc:`blueprints/forge`.
2. For an existing set of inputs, manually write a YAML file with the desired
   blueprint configuration.
3. Write Python code to define a blueprint instance and export it to YAML.


Checking validity
-----------------

Blueprints can be checked for errors using the CLI and in code.

.. tab-set::

   .. tab-item:: Validating via CLI

    Use the ``check`` command from the ``cstar`` CLI.

    .. code-block:: console

        cstar blueprint check my_blueprint.yaml

   .. tab-item:: Programmatic Validation

    Use the ``deserialize`` method to validate a YAML file in Python.

    .. code-block:: python

        from cstar.orchestration.models import RomsMarblBlueprint
        from cstar.orchestration.serialization import deserialize

        deserialize("my_blueprint.yaml", RomsMarblBlueprint)


Execution
---------

.. include:: snippets/review-config.rst

.. warning::
    The post-processing step joining partitioned data may consume all available cores of a login node and be terminated (and make the admins angry).

    - We *strongly* recommend setting ``CSTAR_NPROCS_POST`` to a small number (~2) when running a ROMS-MARBL blueprint directly on a HPC login node.
    - Consider making a :ref:`single-step workplan <workplan_examples>` to run a simulation entirely on the compute cluster.

CLI
^^^

Use the ``run`` command from the ``cstar`` CLI to execute a blueprint.


.. tab-set::

   .. tab-item:: Run via CLI

    Use the ``run`` command from the ``cstar`` CLI.

    .. code-block:: console

        cstar blueprint run my_blueprint.yaml

   .. tab-item:: Programmatic Execution

    Use a ``RomsMarblRunner`` to execute the blueprint.

    .. code-block:: python
      :caption: Executing a blueprint YAML file in python.

        from cstar.entrypoint.config import JobConfig, ServiceConfiguration
        from cstar.applications.roms_marbl.app import RomsMarblRunner
        from cstar.applications.roms_marbl.models import RomsMarblBlueprint
        from cstar.applications.core import RunnerRequest

        account_id = "your-account-id"
        queue_id = "wholenode"

        request = RunnerRequest("my_blueprint.yaml", RomsMarblBlueprint)
        service_cfg = ServiceConfiguration()
        job_cfg = JobConfig(account_id=account_id, walltime="00:90:00", priority=queue_id)

        runner = RomsMarblRunner(request, service_cfg, job_cfg)
        await runner.execute()

Resuming an interrupted run
""""""""""""""""""""""""""""

If a run is lost or interrupted mid-execution, pass ``--resume`` to continue it in
place from the blueprint's working directory, rather than starting over:

.. code-block:: console

    cstar blueprint run my_blueprint.yaml --resume

``--resume`` cannot be combined with ``--clobber``, and only applications that
declare themselves resumable accept it -- the CLI rejects the flag for any other
application before execution begins. No workplan or run-id is needed; this is a
standalone alternative to running a blueprint fresh.

.. toctree::
   :hidden:

   blueprints/roms_marbl
   blueprints/forge
   tutorials/tutorial_bp
