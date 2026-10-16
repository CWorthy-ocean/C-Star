Orchestration API
#################

The Orchestration API is used to manage and execute workplans.

.. seealso::

    :doc:`api-blueprint`

Core
----

.. autosummary::
   :toctree: generated/

   cstar.orchestration.orchestration.ProcessHandle
   cstar.orchestration.orchestration.Status
   cstar.orchestration.orchestration.Task

Execution
---------

.. autosummary::
   :toctree: generated/

   cstar.orchestration.models.Step
   cstar.orchestration.models.Workplan
   cstar.orchestration.orchestration.Planner
   cstar.orchestration.orchestration.Launcher
   cstar.orchestration.launch.local.LocalLauncher
   cstar.orchestration.launch.slurm.SlurmLauncher
   cstar.orchestration.orchestration.Orchestrator

Run Directory Checks
--------------------

.. autosummary::
   :toctree: generated/

   cstar.orchestration.run_check.check_run
   cstar.orchestration.run_check.repair_symlinks
   cstar.orchestration.run_check.RunCheckReport
   cstar.orchestration.run_check.Finding
   cstar.orchestration.run_check.FindingKind
   cstar.orchestration.run_check.Orphans

Custom Application Building
---------------------------

.. autosummary::
   :toctree: generated/

   cstar.applications.core.ApplicationDefinition
   cstar.orchestration.models.Blueprint
   cstar.entrypoint.runner.BlueprintRunner
   cstar.execution.handler.ExecutionStatus