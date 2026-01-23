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
   cstar.orchestration.orchestration.Orchestrator
