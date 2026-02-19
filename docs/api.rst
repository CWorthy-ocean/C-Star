Simulation API
##############

Simulation
----------

.. autosummary::
   :toctree: generated/
	     
   cstar.simulation.Simulation
   cstar.roms.ROMSSimulation

External Codebases
------------------------

.. autosummary::
   :toctree: generated/

   cstar.base.external_codebase.ExternalCodeBase
   cstar.roms.external_codebase.ROMSExternalCodeBase
   cstar.marbl.external_codebase.MARBLExternalCodeBase

Additional Code
------------------

.. autosummary::
   :toctree: generated/

   cstar.base.additional_code.AdditionalCode

Input Datasets
----------------

.. autosummary::
   :toctree: generated/

   cstar.base.input_dataset.InputDataset
   cstar.roms.input_dataset.ROMSInputDataset
   cstar.roms.input_dataset.ROMSModelGrid
   cstar.roms.input_dataset.ROMSInitialConditions
   cstar.roms.input_dataset.ROMSTidalForcing
   cstar.roms.input_dataset.ROMSRiverForcing
   cstar.roms.input_dataset.ROMSBoundaryForcing
   cstar.roms.input_dataset.ROMSSurfaceForcing
   cstar.roms.input_dataset.ROMSForcingCorrections
   cstar.roms.runtime_settings.ROMSRuntimeSettings

Discretization
----------------

.. autosummary::
   :toctree: generated/

   cstar.base.discretization.Discretization

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

