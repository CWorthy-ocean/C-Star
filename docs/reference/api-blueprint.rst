Blueprint API
#############

The Blueprint API is used to create and manage blueprints.

.. seealso::

    :doc:`api-orchestration`

Blueprint Components
--------------------

.. autosummary::
   :toctree: ../generated/

   cstar.orchestration.models.ConfiguredBaseModel
   cstar.orchestration.models.Resource
   cstar.orchestration.models.VersionedResource
   cstar.orchestration.models.Dataset
   cstar.orchestration.models.PathFilter
   cstar.orchestration.models.CodeRepository
   cstar.orchestration.models.ParameterSet
   cstar.orchestration.models.Blueprint
   cstar.orchestration.models.Provenance
   cstar.orchestration.models.GeneratedBy
   cstar.orchestration.models.BlueprintIdentity
   cstar.orchestration.models.CatalogSpecRef

Simulation Components
---------------------

.. autosummary::
   :toctree: ../generated/

   cstar.applications.ucla_roms.models.ForcingConfiguration
   cstar.applications.ucla_roms.models.BlueprintState
   cstar.applications.ucla_roms.models.RuntimeParameterSet
   cstar.applications.ucla_roms.models.PartitioningParameterSet

UCLA-ROMS
---------

.. autosummary::
   :toctree: ../generated/

   cstar.applications.ucla_roms.models.ROMSCompositeCodeRepository
   cstar.applications.ucla_roms.models.UclaRomsBlueprint
