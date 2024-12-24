API Reference
#############


Cases
------------------------

.. autosummary::
   :toctree: generated/

   cstar.Case


Components
------------------------

.. autosummary::
   :toctree: generated/

   cstar.base.Component
   cstar.roms.ROMSComponent
   cstar.marbl.MARBLComponent

Base Models
------------------------

.. autosummary::
   :toctree: generated/

   cstar.base.BaseModel
   cstar.roms.ROMSBaseModel
   cstar.marbl.MARBLBaseModel

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
   cstar.roms.ROMSBoundaryForcing
   cstar.roms.ROMSSurfaceForcing

Discretization
----------------

.. autosummary::
   :toctree: generated/

   cstar.base.Discretization

Scheduler Job
----------------

.. autosummary::
   :toctree: generated/

   cstar.execution..scheduler_job.SchedulerJob
   cstar.execution.scheduler_job.SlurmJob
   cstar.execution.scheduler_job.PBSJob
   cstar.execution.local_process.LocalProcess
   
System
------
.. autosummary::
   :toctree: generated/

   cstar.system.manager.CStarSystemManager
   cstar.system.scheduler.Scheduler
   cstar.system.environment.CStarEnvironment

