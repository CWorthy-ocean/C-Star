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



.. _single-step-wp-ex:
Example - Single Step Workplan
-------

The following example demonstrates the minimum possible workplan containing
a single step:

.. code:: yaml

    name: Simple Workplan
    description: Run a simulation
    state: draft

    steps:
      - name: job1
        application: roms_marbl
        blueprint: /home/x-seilerman/wp_testing/2node_1wk_new_a.yaml


.. _multi-step-wp-ex:
Example - Multi-step Workplan
-------

The following example demonstrates a workplan with multiple steps. Note that each step
can reference different blueprints.

Additionally, this example introduces a simple dependency with ``depends_on: job1``. A
dependency must be specified if the steps of the workplan require specific ordering. Here,
*job1* must complete successfully before *job2* will start.

.. tip::
    A multi-step workplan without dependencies does not guarantee any ordering. All jobs
    will be executed as the scheduler on the system sees fit.

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

Schema details
--------------

TODO

Checking validity
-----------------


CLI
^^^

.. code::

    cstar workplan check my_workplan.yaml

In Python
^^^^^^^^^

.. code:: python

    from pathlib import Path
    from cstar.cli.workplan.check import check as check_workplan

    check_workplan(Path("/path/to/my/workplan.yaml"))


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
