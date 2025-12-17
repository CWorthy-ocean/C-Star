Terminology and Concepts
========================

Definitions
-----------
.. glossary::

    Application
      An **application** is a piece of software we want to run.

      Currently, the only supported application is ROMS-MARBL. Upcoming work will add applications to support domain generation and post-processing applications.

    Blueprint
     A **blueprint** is a yaml file detailing all of the information needed to run the application, such as where the code is to run it, any input data, any user-settable parameters, etc.

     The blueprint will have a different schema for different applications.

     See the `blueprints page <blueprints.rst>`_ for examples and more info.

    Workplan
     A workplan is a YAML file that represents a series of **steps**, each of which represents a logical unit of work: one blueprint/application to be executed.

     A workplan can define basic dependencies between steps, so that one step must wait for one or more previous steps to complete before being executed.

     Any steps that are not waiting on a dependency can be submitted/executed in parallel.

     In future releases, workplans can be designated as “validated,” which means additional checks and restrictions are enforced to ensure reproducibility and auditability.

     See the `workplans page <workplans.rst>`_ for more examples and more info.

    Orchestrator
     The orchestrator is a process that can read workplans, create a DAG (directed acyclic graph) of tasks, schedule their execution, and monitor their status. This process typically runs on a HPC login node and submits workloads to appropriate compute resources using a Launcher

    Run ID
     Whenever uses the orchestrator to run a workplan, they must specify a **run ID**. This identifier represents a unique execution of a given workplan, and can later be used to check the status of a previously submitted workplan. Specifying a different run ID allows the user to run a new instantiation of the same workplan.

    Worker
     The worker is a process that runs on a compute resource, reads a single blueprint, and executes the designated application with the inputs specified in the blueprint.



Example HPC Deployment
----------------------
.. image:: images/hpc_arch_diagram.png

This diagram illustrates an example of a user initiating a workflow on a HPC. The user has pre-prepared their blueprints and a workplan, with all relevant input data in their HPC storage.

The user logs into the login node and initiate the workplan via the ``cstar cli``. This creates an orchestrator instance (an ephemeral prefect server) that reads their workplan, identifies the tasks that need to be executed, organizes per-task blueprints, and submits the entire set of tasks to SLURM.

SLURM schedules and allocates the needed compute resources and manages job status and dependencies. Each task gets its own allocation and initiates a worker to read the blueprint for that task and execute the appropriate application.

The user can log off and return later to monitor the overall workplan status by re-calling the ``cstar cli`` with the same run ID.