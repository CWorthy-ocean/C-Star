Workplans
=========

Workplans define the contract for requesting the execution of one or more 
:doc:`blueprints`. A user-configured workplan informs C-Star which `Blueprint` 
to execute, and in what order.


Workplan Schema
---------------

Workplans are defined in :class:`cstar.orchestration.models.Workplan`.

.. rubric:: Workplan Attributes

.. autosummary::
    
  ~cstar.orchestration.models.Workplan.name
  ~cstar.orchestration.models.Workplan.description
  ~cstar.orchestration.models.Workplan.steps
  ~cstar.orchestration.models.Workplan.state
  ~cstar.orchestration.models.Workplan.compute_environment
  ~cstar.orchestration.models.Workplan.runtime_vars


State
^^^^^

.. include:: snippets/in-development.rst

A workplan may be configured in a *draft* or *validated* state using :attr:`~cstar.orchestration.models.Workplan.state`

- a *draft* workplan can be freely edited and submitted for execution.
- modifications to a *validated* workplan are restricted to ensure reproducibility.


Compute Environment
^^^^^^^^^^^^^^^^^^^

.. include:: snippets/in-development.rst

The desired compute environment characteristic are specified using :attr:`~cstar.orchestration.models.Workplan.compute_environment`


Runtime Variables
^^^^^^^^^^^^^^^^^

C-Star performs some simple templating for user convenience. The key-value pairs in
:attr:`~cstar.orchestration.models.Workplan.runtime_vars` define the original value
and the value that will replace it at run time.


Steps
^^^^^

The heart of a workplan is the collection of steps found in :attr:`~cstar.orchestration.models.Workplan.steps`.
Steps have a 1-to-1 relationship with blueprints - each step must specify the path to a blueprint file.
The step also specifies the application type to use for it's execution.


Step Schema
-----------

See :class:`cstar.orchestration.models.Step` for complete details on configuring steps.

.. rubric:: Step Attributes

.. autosummary::
    
  ~cstar.orchestration.models.Step.name
  ~cstar.orchestration.models.Step.application
  ~cstar.orchestration.models.Step.blueprint
  ~cstar.orchestration.models.Step.depends_on
  ~cstar.orchestration.models.Step.blueprint_overrides
  ~cstar.orchestration.models.Step.compute_overrides
  ~cstar.orchestration.models.Step.workflow_overrides


.. _workplan_examples:

Workplan Examples
-----------------

.. tab-set::

   .. tab-item:: Single-step

    The following example demonstrates the minimum possible workplan.
    
    It contains a single step to be executed.

    .. code:: yaml

        name: Simple Workplan
        description: Run a simulation
        state: draft

        steps:
        - name: job1
            application: roms_marbl
            blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml

   .. tab-item:: Multi-step

    The following example demonstrates a workplan with multiple steps. Note that each step
    can reference different blueprints.

    Additionally, this example introduces a simple dependency with ``depends_on: job1``. A
    dependency must be specified if the steps of the workplan require specific ordering. Here,
    *job1* must complete successfully before *job2* will start.

    .. important::
        A multi-step workplan without dependencies has no ordering guarantees. 
        
        Jobs are scheduled immediately and executed as the system launcher permits.

    .. code:: yaml

        name: Multi-step Workplan Example
        description: Run multiple ROMS-MARBL simulations
        state: draft

        steps:
        - name: job1
            application: roms_marbl
            blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml
        - name: job2
            application: roms_marbl
            blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_b.yaml
            depends_on:
            - job1
        - name: job3
            application: roms_marbl
            blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_c.yaml

   .. tab-item:: Overriding Blueprints

    The following example demonstrates how to override configuration in a 
    blueprint from the workplan. Overriding blueprints enables the same
    blueprint to be used with different inputs, data sources, etc.

    .. tip::
        Blueprint overrides are supplied as a dictionary with 
        :ref:`Blueprint schema<blueprint_schema>`

    .. code:: yaml

        name: Workplan Overriding a Blueprint
        description: Run a blueprint ensemble varying parameters with overrides
        state: draft

        steps:
        - name: step 1
            application: roms_marbl
            blueprint: blueprint.yaml
            blueprint_overrides: 
            - name: Run blueprint with development UCLA-ROMS branch
            - code:
                roms:
                  location: https://github.com/CWorthy-ocean/ucla-roms.git
                  branch: develop

        - name: step 2
            application: roms_marbl
            blueprint: blueprint.yaml
            blueprint_overrides: 
            - name: Run blueprint with custom UCLA-ROMS fork
            - code:
                roms:
                  location: https://github.com/github-user/ucla-roms.git
                  branch: main


Checking validity
-----------------

Workplans can be checked for errors using the CLI and in code.

.. tab-set::

   .. tab-item:: Validating via CLI

    Use the `check` command from the `cstar CLI`.

    .. code-block:: console

        cstar workplan check my_workplan.yaml

   .. tab-item:: Programmatic Validation

    Use the `deserialize` method to validate a `YAML` file in Python.

    .. code-block:: python

        from cstar.orchestration.models import Workplan
        from cstar.orchestration.serialization import deserialize

        deserialize("my_workplan.yaml", Workplan)


Execution
---------

.. include:: snippets/review-config.rst

.. attention::
    An error will occur if the `SLURM` **account** and **queue** are not configured when running on a HPC.
    
CLI
^^^

.. code::

    cstar workplan run my_workplan.yaml --run-id my-unique-id

You can run the same command later, with the same :term:`run ID` to check the status of your running jobs. Or, you can use a different run ID to re-run the workplan from scratch.

In Python
^^^^^^^^^


.. code:: python

    from pathlib import Path
    from cstar.cli.workplan.run import run as run_workplan

    run_workplan(Path("/path/to/my/workplan.yaml"), run_id = "my-unique-id")
