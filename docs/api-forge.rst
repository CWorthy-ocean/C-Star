Forge API
#########

The Forge API is used to author and execute domain-generation blueprints.

.. seealso::

    - :doc:`api-blueprint`
    - :doc:`api-orchestration`

Blueprint
---------

.. autosummary::
   :toctree: generated/

   cstar.applications.forge.blueprint.ForgeBlueprint
   cstar.applications.forge.blueprint.RunWindow
   cstar.applications.forge.blueprint.Domain
   cstar.applications.forge.blueprint.Forcing
   cstar.applications.forge.blueprint.InitialConditions
   cstar.applications.forge.blueprint.BoundaryForcing
   cstar.applications.forge.blueprint.SurfaceForcingItem
   cstar.applications.forge.blueprint.TidalForcingItem
   cstar.applications.forge.blueprint.RiverForcingItem
   cstar.applications.forge.blueprint.CdrSpec
   cstar.applications.forge.blueprint.Code
   cstar.applications.forge.blueprint.TemplateRepo
   cstar.applications.forge.blueprint.Composition
   cstar.applications.forge.blueprint.Provenance

Application
-----------

.. autosummary::
   :toctree: generated/

   cstar.applications.forge.app.ForgeApplication
   cstar.applications.forge.app.ForgeRunner

Authoring
---------

.. autosummary::
   :toctree: generated/

   cstar.applications.forge.resolve.build_forge_blueprint
   cstar.applications.forge.models.ModelSpec
   cstar.applications.forge.models.ModelCode
   cstar.applications.forge.models.ModelTemplates

Catalog
-------

.. autosummary::
   :toctree: generated/

   cstar.catalog.domain_catalog.DomainCatalog
   cstar.catalog.domain_catalog.LayeredCatalog
   cstar.catalog.domain_catalog.build_catalog_stack
   cstar.catalog.domain_catalog.user_catalog_root

Execution
---------

.. autosummary::
   :toctree: generated/

   cstar.applications.forge.engine.process_forge_blueprint
   cstar.applications.forge.executor.ForgeExecutor
   cstar.applications.forge.host.HostPaths
   cstar.applications.forge.runtime.run_blueprint
   cstar.applications.forge.source_datasets.SourceDatasets
