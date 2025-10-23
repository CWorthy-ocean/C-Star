API Reference
#############

Simulation
----------

.. autosummary::
   :toctree: generated/
	     
   cstar.Simulation
   cstar.roms.ROMSSimulation

External Codebases
------------------------

.. autosummary::
   :toctree: generated/

   cstar.base.ExternalCodeBase
   cstar.roms.ROMSExternalCodeBase
   cstar.marbl.MARBLExternalCodeBase

Additional Code
------------------

.. autosummary::
   :toctree: generated/

   cstar.base.AdditionalCode

Input Datasets
----------------

.. autosummary::
   :toctree: generated/

   cstar.base.InputDataset
   cstar.roms.ROMSInputDataset
   cstar.roms.ROMSModelGrid
   cstar.roms.ROMSInitialConditions
   cstar.roms.ROMSTidalForcing
   cstar.roms.ROMSRiverForcing
   cstar.roms.ROMSBoundaryForcing
   cstar.roms.ROMSSurfaceForcing
   cstar.roms.ROMSForcingCorrections
   cstar.roms.ROMSRuntimeSettings

Discretization
----------------

.. autosummary::
   :toctree: generated/

   cstar.base.Discretization

Scheduler Job
----------------

.. autosummary::
   :toctree: generated/

   cstar.execution.handler.ExecutionHandler
   cstar.execution.local_process.LocalProcess
   cstar.execution.scheduler_job.SchedulerJob
   cstar.execution.scheduler_job.SlurmJob
   cstar.execution.scheduler_job.PBSJob

io
--
.. autosummary::
   :toctree: generated/

   cstar.io.source_data.SourceData
   cstar.io.source_data.SourceDataCollection
   cstar.io.staged_data.StagedData
   cstar.io.staged_data.StagedDataCollection
   cstar.io.stager.Stager
   cstar.io.retriever.Retriever

   
System
------
.. autosummary::
   :toctree: generated/

   cstar.system.manager.CStarSystemManager
   cstar.system.scheduler.Scheduler
   cstar.system.environment.CStarEnvironment

