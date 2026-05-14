Blueprint API
#############

The Blueprint API is used to create and manage blueprints.

.. seealso::

    :doc:`api-orchestration`

Blueprint Components
--------------------

.. autosummary::
   :toctree: generated/

   cstar.orchestration.models.ConfiguredBaseModel
   cstar.orchestration.models.Resource
   cstar.orchestration.models.VersionedResource
   cstar.orchestration.models.Dataset
   cstar.orchestration.models.PathFilter
   cstar.orchestration.models.CodeRepository
   cstar.orchestration.models.ParameterSet
   cstar.orchestration.models.Blueprint

Simulation Components
---------------------

.. autosummary::
   :toctree: generated/

   cstar.applications.roms_marbl.models.ForcingConfiguration
   cstar.applications.roms_marbl.models.BlueprintState
   cstar.applications.roms_marbl.models.RuntimeParameterSet
   cstar.applications.roms_marbl.models.PartitioningParameterSet
   cstar.applications.roms_marbl.models.ModelParameterSet

ROMS-MARBL
----------

.. autosummary::
   :toctree: generated/

   cstar.applications.roms_marbl.models.ROMSCompositeCodeRepository
   cstar.applications.roms_marbl.models.RomsMarblBlueprint
